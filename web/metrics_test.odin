package web

import "../common"
import queuelib "../queue"
import "core:strconv"
import "core:strings"
import "core:testing"

// metrics_values parses a Prometheus text body. Every sample line must be
// `name value` with an integer value; anything else is counted as malformed.
metrics_values :: proc(body: string) -> (values: map[string]i64, malformed: int) {
	rest := body
	for len(rest) > 0 {
		nl := strings.index_byte(rest, '\n')
		line := rest
		if nl >= 0 {
			line = rest[:nl]
			rest = rest[nl+1:]
		} else {
			rest = ""
		}
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		sp := strings.index_byte(line, ' ')
		if sp <= 0 {
			malformed += 1
			continue
		}
		v, ok := strconv.parse_int(line[sp+1:], 10)
		if !ok {
			// Prometheus allows float samples (e.g. the stalled-seconds counter);
			// keep the parser strict about the format but accept a decimal value.
			if f, fok := strconv.parse_f64(line[sp+1:]); fok {
				values[line[:sp]] = i64(f)
				continue
			}
			malformed += 1
			continue
		}
		values[line[:sp]] = i64(v)
	}
	return values, malformed
}

metrics_must :: proc(t: ^testing.T, values: map[string]i64, name: string) -> i64 {
	v, ok := values[name]
	testing.expectf(t, ok, "metric %s is missing", name)
	return v
}

metrics_expect :: proc(t: ^testing.T, values: map[string]i64, name: string, want: i64) {
	v, ok := values[name]
	testing.expectf(t, ok, "metric %s is missing", name)
	if ok {
		testing.expectf(t, v == want, "metric %s = %d, want %d", name, v, want)
	}
}

metrics_snapshot :: proc(t: ^testing.T, env: Handlers_Test_Env, arena: ^common.Arena) -> map[string]i64 {
	r := handlers_test_do(env, arena, "GET", "/metrics", "", "")
	testing.expect_value(t, r.code, 200)
	values, _ := metrics_values(r.body)
	return values
}

METRICS_NAMES :: [27]string{
	"oboe_ingested_rows_total",
	"oboe_ingested_bytes_total",
	"oboe_queries_total",
	"oboe_flushed_parts_total",
	"oboe_flush_rows_total",
	"oboe_flush_ms_last",
	"oboe_merged_parts_total",
	"oboe_parts",
	"oboe_partitions",
	"oboe_mem_rows",
	"oboe_mem_bytes",
	"oboe_mem_limit_rows",
	"oboe_mem_limit_bytes",
	"oboe_ingest_stalled_seconds_total",
	"oboe_mem_limit_rejected_total",
	"oboe_rows",
	"oboe_disk_bytes",
	"oboe_http_requests_total",
	"oboe_http_connections_total",
	"oboe_cache_entries",
	"oboe_cache_bytes",
	"oboe_cache_hits_total",
	"oboe_cache_misses_total",
	"oboe_cache_evictions_total",
	"oboe_cache_expirations_total",
	"oboe_cache_collisions_total",
	"oboe_uptime_seconds",
}

@(test)
test_metrics_names_and_format :: proc(t: ^testing.T) {
	env := handlers_test_open(handlers_test_tmpdir("web_metrics_names"))
	defer handlers_test_close(env)
	arena: common.Arena
	defer common.arena_destroy(&arena)

	r := handlers_test_do(env, &arena, "GET", "/metrics", "", "")
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.ctype, "text/plain"))
	testing.expect(t, strings.contains(r.ctype, "version=0.0.4"))
	testing.expect(t, strings.contains(r.body, "# HELP oboe_ingested_rows_total"))
	testing.expect(t, strings.contains(r.body, "# TYPE oboe_ingested_rows_total counter"))

	values, malformed := metrics_values(r.body)
	defer delete(values)
	testing.expect_value(t, malformed, 0)
	for name in METRICS_NAMES {
		metrics_must(t, values, name)
	}
	// The queue metrics only appear when a job queue is configured.
	testing.expect(t, !strings.contains(r.body, "oboe_queue_"))
	// Every metric exposed must use the oboe_ prefix.
	for name, _ in values {
		testing.expectf(t, strings.has_prefix(name, "oboe_"), "unexpected metric %s", name)
	}
}

@(test)
test_metrics_counters_monotonic :: proc(t: ^testing.T) {
	env := handlers_test_open(handlers_test_tmpdir("web_metrics_counters"))
	defer handlers_test_close(env)
	arena: common.Arena
	defer common.arena_destroy(&arena)

	values := metrics_snapshot(t, env, &arena)
	defer delete(values)
	testing.expect_value(t, values["oboe_ingested_rows_total"], i64(0))
	testing.expect_value(t, values["oboe_queries_total"], i64(0))
	testing.expect_value(t, values["oboe_cache_hits_total"], i64(0))

	first := `{"_time":"2026-10-26T06:00:00Z","level":"info","service":"api","_msg":"one"}
{"_time":"2026-10-26T06:00:01Z","level":"info","service":"api","_msg":"two"}
`
	r := handlers_test_do(env, &arena, "POST", "/insert/jsonline", "", first)
	testing.expect_value(t, r.code, 200)
	values2 := metrics_snapshot(t, env, &arena)
	defer delete(values2)
	ingested2 := metrics_must(t, values2, "oboe_ingested_rows_total")
	testing.expect_value(t, ingested2, i64(2))
	testing.expect(t, metrics_must(t, values2, "oboe_ingested_bytes_total") > 0)
	testing.expect_value(t, metrics_must(t, values2, "oboe_mem_rows"), i64(2))

	second := `{"_time":"2026-10-26T06:00:02Z","level":"warn","service":"db","_msg":"three"}
{"_time":"2026-10-26T06:00:03Z","level":"warn","service":"db","_msg":"four"}
{"_time":"2026-10-26T06:00:04Z","level":"warn","service":"db","_msg":"five"}
`
	r = handlers_test_do(env, &arena, "POST", "/insert/jsonline", "", second)
	testing.expect_value(t, r.code, 200)
	values3 := metrics_snapshot(t, env, &arena)
	defer delete(values3)
	ingested3 := metrics_must(t, values3, "oboe_ingested_rows_total")
	testing.expect_value(t, ingested3, i64(5))
	testing.expect(t, ingested3 > ingested2)
	testing.expect(t, metrics_must(t, values3, "oboe_ingested_bytes_total") > metrics_must(t, values2, "oboe_ingested_bytes_total"))
	testing.expect(t, metrics_must(t, values3, "oboe_uptime_seconds") >= metrics_must(t, values2, "oboe_uptime_seconds"))

	// Each executed query bumps oboe_queries_total by one.
	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=level%3Ainfo&limit=10", "")
	testing.expect_value(t, r.code, 200)
	values4 := metrics_snapshot(t, env, &arena)
	defer delete(values4)
	testing.expect_value(t, metrics_must(t, values4, "oboe_queries_total"), i64(1))

	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=*&limit=10", "")
	testing.expect_value(t, r.code, 200)
	values5 := metrics_snapshot(t, env, &arena)
	defer delete(values5)
	testing.expect_value(t, metrics_must(t, values5, "oboe_queries_total"), i64(2))

	// Cache hits and entries are exposed and only grow.
	r = handlers_test_do(env, &arena, "PUT", "/cache/k", "", "v")
	testing.expect_value(t, r.code, 200)
	r = handlers_test_do(env, &arena, "GET", "/cache/k", "", "")
	testing.expect_value(t, r.code, 200)
	values6 := metrics_snapshot(t, env, &arena)
	defer delete(values6)
	testing.expect_value(t, metrics_must(t, values6, "oboe_cache_entries"), i64(1))
	testing.expect(t, metrics_must(t, values6, "oboe_cache_bytes") > 0)
	testing.expect_value(t, metrics_must(t, values6, "oboe_cache_hits_total"), i64(1))

	r = handlers_test_do(env, &arena, "GET", "/cache/k", "", "")
	testing.expect_value(t, r.code, 200)
	values7 := metrics_snapshot(t, env, &arena)
	defer delete(values7)
	testing.expect_value(t, metrics_must(t, values7, "oboe_cache_hits_total"), i64(2))
	testing.expect_value(t, metrics_must(t, values7, "oboe_cache_entries"), i64(1))

	// A miss is counted as well and never decreases a counter.
	r = handlers_test_do(env, &arena, "GET", "/cache/absent", "", "")
	testing.expect_value(t, r.code, 404)
	values8 := metrics_snapshot(t, env, &arena)
	defer delete(values8)
	testing.expect_value(t, metrics_must(t, values8, "oboe_cache_misses_total"), i64(1))
	for name in METRICS_NAMES {
		prev, okp := values7[name]
		if !okp {
			continue
		}
		cur, okc := values8[name]
		testing.expectf(t, okc && cur >= prev, "counter %s went backwards (%d -> %d)", name, prev, cur)
	}
}

@(test)
test_metrics_queue_names :: proc(t: ^testing.T) {
	env := handlers_test_open(handlers_test_tmpdir("web_metrics_queue"))
	defer handlers_test_close(env)

	arena: common.Arena
	defer common.arena_destroy(&arena)

	// Without a configured queue the endpoint must not expose queue metrics.
	r := handlers_test_do(env, &arena, "GET", "/metrics", "", "")
	testing.expect_value(t, r.code, 200)
	testing.expect(t, !strings.contains(r.body, "oboe_queue_"))

	q := queuelib.queue_init(queuelib.default_queue_config())
	defer queuelib.queue_destroy(q)
	env.srv.jobq = q

	body := transmute([]u8)string("job")
	_, res := queuelib.queue_put(q, queuelib.QUEUE_DEFAULT_TUBE, 100, 0, i64(30_000_000_000), body)
	testing.expect_value(t, res, queuelib.Queue_Put_Result.OK)

	values := metrics_snapshot(t, env, &arena)
	defer delete(values)

	names := []string{
		"oboe_queue_jobs",
		"oboe_queue_ready",
		"oboe_queue_reserved",
		"oboe_queue_delayed",
		"oboe_queue_buried",
		"oboe_queue_connections",
		"oboe_queue_puts_total",
	}
	for name in names {
		metrics_must(t, values, name)
	}
	testing.expect_value(t, metrics_must(t, values, "oboe_queue_jobs"), i64(1))
	testing.expect_value(t, metrics_must(t, values, "oboe_queue_ready"), i64(1))
	testing.expect_value(t, metrics_must(t, values, "oboe_queue_reserved"), i64(0))
	// Fresh queue plus exactly one put; a shared or double-counted counter fails.
	testing.expect_value(t, metrics_must(t, values, "oboe_queue_puts_total"), i64(1))
}
