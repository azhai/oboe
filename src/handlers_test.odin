package oboe

import "core:fmt"
import "core:os"
import "core:strings"
import "core:testing"

HANDLERS_TEST_STREAM_FIELDS := [2]string{"level", "service"}

Handlers_Test_Env :: struct {
	srv:   ^Server,
	store: ^Store,
	cache: ^Cache,
	dir:   string,
}

handlers_test_open :: proc(dir: string) -> Handlers_Test_Env {
	os.remove_all(dir)
	cfg := default_store_config()
	cfg.data_path = dir
	cfg.flush_interval = 1 << 62
	cfg.flush_max_rows = 100_000
	cfg.flush_max_bytes = 1 << 40
	cfg.stream_fields = HANDLERS_TEST_STREAM_FIELDS[:]
	store := store_init(cfg, false)

	cc := default_cache_config()
	cc.max_bytes = 1 << 20
	cc.max_entries = 1024
	cache := cache_init(cc)

	srv := server_init(default_server_config(), store, cache, nil)
	srv.cfg.stream_fields = cfg.stream_fields
	return Handlers_Test_Env{srv = srv, store = store, cache = cache, dir = dir}
}

handlers_test_close :: proc(env: Handlers_Test_Env) {
	cache_destroy(env.cache)
	store_close(env.store)
	free(env.srv)
	os.remove_all(env.dir)
}

Handlers_Test_Response :: struct {
	code:  int,
	body:  string,
	extra: string,
	ctype: string,
}

// handlers_test_do runs dispatch in-process and copies the response into the
// arena, so callers never hold pointers into the response buffer.
handlers_test_do :: proc(env: Handlers_Test_Env, arena: ^Arena, method, path, query, body: string) -> Handlers_Test_Response {
	req := Http_Request{
		method  = method,
		path    = path,
		query   = query,
		version = "HTTP/1.1",
		body    = transmute([]u8)body,
	}
	resp := dispatch(env.srv, &req, arena)
	defer buf_destroy(&resp.body)
	return Handlers_Test_Response{
		code  = resp.code,
		body  = arena_str(arena, string(resp.body.data[:])),
		extra = arena_str(arena, resp.extra),
		ctype = arena_str(arena, resp.ctype),
	}
}

@(test)
test_dispatch_ingest_query_metrics :: proc(t: ^testing.T) {
	env := handlers_test_open("/tmp/oboe_handlers_ingest")
	defer handlers_test_close(env)
	arena: Arena
	defer arena_destroy(&arena)

	payload := `{"_time":"2026-10-26T04:00:00Z","level":"error","service":"api","_msg":"boom"}
{"_time":"2026-10-26T04:00:01Z","level":"info","service":"api","_msg":"ok"}
{"_time":"2026-10-26T04:00:02Z","level":"error","service":"db","_msg":"timeout"}
`
	r := handlers_test_do(env, &arena, "POST", "/insert/jsonline", "", payload)
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.body, `"rows":3`))
	testing.expect(t, strings.contains(r.body, `"errors":0`))

	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=level%3Aerror&limit=10", "")
	testing.expect_value(t, r.code, 200)
	testing.expect_value(t, strings.count(r.body, "\n"), 2)
	testing.expect(t, strings.contains(r.body, `"_msg":"boom"`))
	testing.expect(t, strings.contains(r.body, `"_msg":"timeout"`))
	testing.expect(t, !strings.contains(r.body, `"_msg":"ok"`))
	testing.expect(t, strings.contains(r.extra, "X-Oboe-Scanned-Rows:"))
	testing.expect(t, strings.contains(r.extra, "X-Oboe-Matched-Rows:"))
	testing.expect(t, strings.contains(r.ctype, "application/stream+json"))

	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=*%20%7C%20stats%20by%20(service)%20count()%20as%20total", "")
	testing.expect_value(t, r.code, 200)
	testing.expect_value(t, strings.count(r.body, "\n"), 2)
	testing.expect(t, strings.contains(r.body, `"service":"api","total":"2"`))
	testing.expect(t, !strings.contains(r.body, "_msg"))

	r = handlers_test_do(env, &arena, "GET", "/select/logsql/field_names", "query=*", "")
	testing.expect_value(t, r.code, 200)
	names := []string{"_time", "_msg", "level", "service"}
	for name in names {
		testing.expectf(t, strings.contains(r.body, fmt.tprintf(`"value":%q`, name)), "field_names missing %q: %s", name, r.body)
	}

	r = handlers_test_do(env, &arena, "GET", "/select/logsql/field_values", "field=level&query=*", "")
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.body, `"value":"error","hits":2`))
	testing.expect(t, strings.contains(r.body, `"value":"info","hits":1`))

	r = handlers_test_do(env, &arena, "GET", "/metrics", "", "")
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.body, "oboe_ingested_rows_total 3"))
	testing.expect(t, strings.contains(r.body, "oboe_mem_rows 3"))
	testing.expect(t, strings.contains(r.body, "oboe_cache_entries 0"))
	testing.expect(t, !strings.contains(r.body, "odlog_"))

	r = handlers_test_do(env, &arena, "GET", "/", "", "")
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.body, "<title>oboe"))

	r = handlers_test_do(env, &arena, "GET", "/health", "", "")
	testing.expect_value(t, r.code, 200)
	testing.expect_value(t, r.body, "OK")

	r = handlers_test_do(env, &arena, "GET", "/nope", "", "")
	testing.expect_value(t, r.code, 404)
	r = handlers_test_do(env, &arena, "GET", "/insert/jsonline", "", "")
	testing.expect_value(t, r.code, 405)
	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=%28level%3Aerror", "")
	testing.expect_value(t, r.code, 400)
}

@(test)
test_dispatch_cache_api :: proc(t: ^testing.T) {
	env := handlers_test_open("/tmp/oboe_handlers_cache")
	defer handlers_test_close(env)
	arena: Arena
	defer arena_destroy(&arena)

	r := handlers_test_do(env, &arena, "PUT", "/cache/greeting", "ttl=30s", "hello")
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.extra, "X-Cache: stored"))
	testing.expect(t, strings.contains(r.body, `"ok":true`))

	r = handlers_test_do(env, &arena, "GET", "/cache/greeting", "", "")
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.extra, "X-Cache: hit"))
	testing.expect_value(t, r.body, "hello")
	testing.expect(t, strings.contains(r.ctype, "application/octet-stream"))

	r = handlers_test_do(env, &arena, "DELETE", "/cache/greeting", "", "")
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.extra, "X-Cache: deleted"))
	testing.expect(t, strings.contains(r.body, `"deleted":true`))

	r = handlers_test_do(env, &arena, "GET", "/cache/greeting", "", "")
	testing.expect_value(t, r.code, 404)
	testing.expect(t, strings.contains(r.extra, "X-Cache: miss"))

	r = handlers_test_do(env, &arena, "GET", "/cache/stats", "", "")
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.body, `"entries":0`))
	testing.expect(t, strings.contains(r.body, `"sets":1`))
	testing.expect(t, strings.contains(r.body, `"deletes":1`))
	testing.expect(t, strings.contains(r.body, `"hits":1`))
	testing.expect(t, strings.contains(r.body, `"misses":1`))

	r = handlers_test_do(env, &arena, "PUT", "/cache/bad", "ttl=nope", "x")
	testing.expect_value(t, r.code, 400)

	long := make([]u8, CACHE_MAX_KEY+1)
	defer delete(long)
	for i in 0..<len(long) {
		long[i] = 'k'
	}
	path := fmt.aprintf("/cache/%s", string(long))
	defer delete(path)
	r = handlers_test_do(env, &arena, "PUT", path, "", "x")
	testing.expect_value(t, r.code, 400)

	// /cache/save: requires POST and a configured file.
	r = handlers_test_do(env, &arena, "POST", "/cache/save", "", "")
	testing.expect_value(t, r.code, 400)
	testing.expect(t, strings.contains(r.body, "cache file not configured"))

	save_path := "/tmp/oboe_handlers_cache/cache.bin"
	env.srv.cache_file = save_path
	r = handlers_test_do(env, &arena, "GET", "/cache/save", "", "")
	testing.expect_value(t, r.code, 405)

	r = handlers_test_do(env, &arena, "PUT", "/cache/saved-key", "", "persist")
	testing.expect_value(t, r.code, 200)
	r = handlers_test_do(env, &arena, "POST", "/cache/save", "", "")
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.body, `"saved":true`))
	testing.expect(t, os.exists(save_path))
	env.srv.cache_file = ""

	env.srv.cache = nil
	r = handlers_test_do(env, &arena, "GET", "/cache/anything", "", "")
	testing.expect_value(t, r.code, 503)
	env.srv.cache = env.cache
}

@(test)
test_dispatch_elastic_and_loki :: proc(t: ^testing.T) {
	env := handlers_test_open("/tmp/oboe_handlers_formats")
	defer handlers_test_close(env)
	arena: Arena
	defer arena_destroy(&arena)

	es := `{"index":{}}
{"@timestamp":"2026-10-26T05:00:00Z","message":"es line","level":"info","service":"es"}
`
	r := handlers_test_do(env, &arena, "POST", "/insert/elasticsearch/_bulk", "", es)
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.body, `"errors":false`))

	loki := `{"streams":[{"stream":{"level":"error","service":"loki"},"values":[["1700000000000000000","loki line"]]}]}`
	r = handlers_test_do(env, &arena, "POST", "/insert/loki/api/v1/push", "", loki)
	testing.expect_value(t, r.code, 204)
	testing.expect_value(t, r.body, "")

	r = handlers_test_do(env, &arena, "POST", "/insert/loki/api/v1/push", "", `not json`)
	testing.expect_value(t, r.code, 400)

	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=*&limit=10", "")
	testing.expect_value(t, r.code, 200)
	testing.expect_value(t, strings.count(r.body, "\n"), 2)
	testing.expect(t, strings.contains(r.body, `"_msg":"es line"`))
	testing.expect(t, strings.contains(r.body, `"level":"info"`))
	testing.expect(t, strings.contains(r.body, `"_msg":"loki line"`))
	testing.expect(t, strings.contains(r.body, `"service":"loki"`))

	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=service%3Aloki", "")
	testing.expect_value(t, strings.count(r.body, "\n"), 1)
}

@(test)
test_params_and_line_helpers :: proc(t: ^testing.T) {
	env := handlers_test_open("/tmp/oboe_handlers_helpers")
	defer handlers_test_close(env)
	arena: Arena
	defer arena_destroy(&arena)

	params := params_parse("a=1&b=hello+world&c=%E4%BD%A0&flag", &arena)
	defer delete(params)
	v, ok := params_get(params, "b")
	testing.expect(t, ok)
	testing.expect_value(t, v, "hello world")
	v, ok = params_get(params, "c")
	testing.expect(t, ok)
	testing.expect_value(t, v, "你")
	_, has_value := params_get(params, "flag")
	testing.expect(t, has_value)
	testing.expect_value(t, params_int(params, "a", 0), 1)
	testing.expect_value(t, params_get_or(params, "missing", "def"), "def")

	msg_field, time_field, stream_fields := prepare_query_params(env.srv, params, &arena)
	testing.expect_value(t, msg_field, "_msg")
	testing.expect_value(t, time_field, "_time")
	testing.expect_value(t, len(stream_fields), 2)
	testing.expect_value(t, stream_fields[0], "level")

	custom := params_parse("_msg_field=message&_time_field=ts&_stream_fields=x,y", &arena)
	defer delete(custom)
	msg_field, time_field, stream_fields = prepare_query_params(env.srv, custom, &arena)
	testing.expect_value(t, msg_field, "message")
	testing.expect_value(t, time_field, "ts")
	testing.expect_value(t, len(stream_fields), 2)
	testing.expect_value(t, stream_fields[1], "y")

	lines: [dynamic]string
	defer delete(lines)
	split_lines(transmute([]u8)string("a\r\n\nb\nc"), &lines)
	testing.expect_value(t, len(lines), 3)
	testing.expect_value(t, lines[0], "a")
	testing.expect_value(t, lines[1], "b")
	testing.expect_value(t, lines[2], "c")
}
