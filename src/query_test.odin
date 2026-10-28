package oboe

import "core:os"
import "core:testing"

@(test)
test_query_parse :: proc(t: ^testing.T) {
	now := i64(1_700_000_000_000_000_000)

	q, ok := parse_query_string("level:error service:api | limit 10", now)
	testing.expect(t, ok)
	if !ok {
		return
	}
	defer query_destroy(&q)
	testing.expect(t, q.filter != nil)
	testing.expect_value(t, q.filter.kind, Filter_Kind.AND)
	testing.expect_value(t, len(q.filter.kids), 2)
	testing.expect_value(t, q.filter.kids[0].field, "level")
	testing.expect_value(t, q.filter.kids[0].value, "error")
	testing.expect_value(t, len(q.pipes), 1)
	testing.expect_value(t, q.pipes[0].kind, Pipe_Kind.LIMIT)
	testing.expect_value(t, q.pipes[0].limit, 10)

	q2, ok2 := parse_query_string("_time:[2026-10-26T10:00:00Z, 2026-10-26T10:10:00Z] level:error", now)
	testing.expect(t, ok2)
	if ok2 {
		defer query_destroy(&q2)
		start, _ := parse_iso_ns("2026-10-26T10:00:00Z")
		end, _ := parse_iso_ns("2026-10-26T10:10:00Z")
		testing.expect(t, q2.has_start)
		testing.expect(t, q2.has_end)
		testing.expect_value(t, q2.start, start)
		testing.expect_value(t, q2.end, end)
	}

	q3, ok3 := parse_query_string("level:error | stats by (service) count() as total, avg(duration) as avg_d", now)
	testing.expect(t, ok3)
	if ok3 {
		defer query_destroy(&q3)
		testing.expect_value(t, len(q3.pipes), 1)
		p := q3.pipes[0]
		testing.expect_value(t, p.kind, Pipe_Kind.STATS)
		testing.expect_value(t, len(p.by), 1)
		testing.expect_value(t, p.by[0], "service")
		testing.expect_value(t, len(p.aggs), 2)
		testing.expect_value(t, p.aggs[0].kind, Agg_Kind.COUNT)
		testing.expect_value(t, p.aggs[0].out, "total")
		testing.expect_value(t, p.aggs[1].kind, Agg_Kind.AVG)
		testing.expect_value(t, p.aggs[1].field, "duration")
		testing.expect_value(t, p.aggs[1].out, "avg_d")
	}

	q4, ok4 := parse_query_string("_time:5m level:err* | sort by (_time) desc", now)
	testing.expect(t, ok4)
	if ok4 {
		defer query_destroy(&q4)
		testing.expect_value(t, q4.start, now - 300_000_000_000)
		testing.expect_value(t, q4.end, now)
		f := q4.filter.kids[1]
		testing.expect_value(t, f.kind, Filter_Kind.PREFIX)
		testing.expect_value(t, q4.pipes[0].kind, Pipe_Kind.SORT)
		testing.expect(t, q4.pipes[0].sort[0].desc)
	}

	_, bad := parse_query_string("(level:error", now)
	testing.expect(t, !bad)
	_, bad2 := parse_query_string("level:error | nonsense 1", now)
	testing.expect(t, !bad2)
}

@(test)
test_query_execute :: proc(t: ^testing.T) {
	dir := "/tmp/oboe_query_test"
	os.remove_all(dir)
	cfg := default_store_config()
	cfg.data_path = dir
	cfg.flush_interval = 1 << 62
	cfg.flush_max_rows = 100000
	cfg.flush_max_bytes = 1 << 40
	s := store_init(cfg, false)
	defer {
		store_close(s)
		os.remove_all(dir)
	}

	base, _ := parse_iso_ns("2026-10-26T10:00:00Z")
	now := base + 5_000_000_000
	sec :: i64(1_000_000_000)
	minute :: i64(60) * sec

	fs := make([]Field, 4 * 4)
	defer delete(fs)
	fs[0] = {name = "level", value = "error"}
	fs[1] = {name = "service", value = "api"}
	fs[2] = {name = "duration", value = "600"}
	fs[3] = {name = "zone", value = "old"}
	fs[4] = {name = "level", value = "error"}
	fs[5] = {name = "service", value = "api"}
	fs[6] = {name = "duration", value = "120"}
	fs[7] = {name = "zone", value = "a"}
	fs[8] = {name = "level", value = "info"}
	fs[9] = {name = "service", value = "api"}
	fs[10] = {name = "duration", value = "15"}
	fs[11] = {name = "zone", value = "a"}
	fs[12] = {name = "level", value = "error"}
	fs[13] = {name = "service", value = "db"}
	fs[14] = {name = "duration", value = "900"}
	fs[15] = {name = "zone", value = "a"}

	rows := make([]Ingest_Row, 4)
	defer delete(rows)
	rows[0] = Ingest_Row{ts = base - 30 * minute, stream = 1, msg = "ancient failure", fields = fs[0:4]}
	rows[1] = Ingest_Row{ts = base - 3 * sec, stream = 1, msg = "request failed for user alice", fields = fs[4:8]}
	rows[2] = Ingest_Row{ts = base - 2 * sec, stream = 1, msg = "request ok", fields = fs[8:12]}
	rows[3] = Ingest_Row{ts = base - 1 * sec, stream = 1, msg = "connection timeout", fields = fs[12:16]}
	testing.expect_value(t, store_ingest(s, rows), 4)
	store_flush_all(s)

	// sorted error search, time-bounded
	{
		q, ok := parse_query_string("_time:5m level:error | sort by (_time) desc", now)
		testing.expect(t, ok)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 2)
		if len(r.rows) == 2 {
			testing.expect_value(t, r.rows[0].msg, "connection timeout")
			testing.expect_value(t, r.rows[1].msg, "request failed for user alice")
			testing.expect_value(t, r.rows[0].ts, base - sec)
			testing.expect(t, r.rows[0].has_time)
		}
		query_destroy(&q)
		result_destroy(&r)
	}

	// early stop at limit
	{
		q, _ := parse_query_string("* | limit 1", now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 1)
		query_destroy(&q)
		result_destroy(&r)
	}

	// bloom pruning removes the whole scan
	{
		q, _ := parse_query_string("level:nomatchxyz", now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 0)
		testing.expect_value(t, r.matched_rows, 0)
		testing.expect(t, r.pruned_blocks > 0)
		testing.expect_value(t, r.scanned_blocks, 0)
		query_destroy(&q)
		result_destroy(&r)
	}

	// prefix, phrase, range, negation, exists
	{
		q, _ := parse_query_string("_time:5m level:err*", now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 2)
		query_destroy(&q)
		result_destroy(&r)
	}
	{
		q, _ := parse_query_string(`_msg:"connection timeout"`, now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 1)
		query_destroy(&q)
		result_destroy(&r)
	}
	{
		q, _ := parse_query_string("_time:5m duration:>100", now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 2)
		query_destroy(&q)
		result_destroy(&r)
	}
	{
		q, _ := parse_query_string("_time:5m -level:error", now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 1)
		if len(r.rows) == 1 {
			testing.expect_value(t, r.rows[0].msg, "request ok")
		}
		query_destroy(&q)
		result_destroy(&r)
	}
	{
		q, _ := parse_query_string("level:*", now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 4)
		query_destroy(&q)
		result_destroy(&r)
	}

	// boolean or + time window
	{
		q, _ := parse_query_string("_time:5m service:api or service:db", now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 3)
		query_destroy(&q)
		result_destroy(&r)
	}

	// field projection
	{
		q, _ := parse_query_string("_time:5m level:error | fields service", now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 2)
		if len(r.rows) == 2 {
			testing.expect_value(t, len(r.rows[0].fields), 1)
			testing.expect_value(t, r.rows[0].fields[0].name, "service")
		}
		query_destroy(&q)
		result_destroy(&r)
	}

	// stats grouped
	{
		q, _ := parse_query_string("_time:5m level:error | stats by (service) count() as total, avg(duration) as avg_d", now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 2)
		if len(r.rows) == 2 {
			for &row in r.rows {
				svc, _ := out_row_get(&row, "service")
				total, _ := out_row_get(&row, "total")
				if svc == "db" {
					testing.expect_value(t, total, "1")
				}
				if svc == "api" {
					testing.expect_value(t, total, "1")
				}
			}
		}
		query_destroy(&q)
		result_destroy(&r)
	}

	// stats without by, plus sort on the aggregate
	{
		q, _ := parse_query_string("_time:5m level:error | stats count() as total", now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 1)
		if len(r.rows) == 1 {
			total, _ := out_row_get(&r.rows[0], "total")
			testing.expect_value(t, total, "2")
		}
		query_destroy(&q)
		result_destroy(&r)
	}
	{
		q, _ := parse_query_string("level:error | stats by (service) count() as total | sort by (total) desc", now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 2)
		query_destroy(&q)
		result_destroy(&r)
	}

	// in-memory (unflushed) rows are visible to queries
	{
		extra := make([]Ingest_Row, 1)
		efs := make([]Field, 1)
		efs[0] = {name = "level", value = "warn"}
		extra[0] = Ingest_Row{ts = base, stream = 2, msg = "memory only", fields = efs}
		store_ingest(s, extra)
		q, _ := parse_query_string("level:warn", now)
		r := execute_query(s, &q, 100)
		testing.expect_value(t, len(r.rows), 1)
		if len(r.rows) == 1 {
			testing.expect_value(t, r.rows[0].msg, "memory only")
		}
		query_destroy(&q)
		result_destroy(&r)
		delete(extra)
		delete(efs)
	}
}
