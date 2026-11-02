package web

import "../common"
import "core:strings"
import "core:testing"

// The Elasticsearch and Loki handlers live in the web package (they need the
// server and store), so their wire-format coverage lives here as well.

@(test)
test_web_jsonline_more_elastic_bulk :: proc(t: ^testing.T) {
	env := handlers_test_open(handlers_test_tmpdir("web_jsonline_bulk"))
	defer handlers_test_close(env)
	arena: common.Arena
	defer common.arena_destroy(&arena)

	// CRLF line endings, blank lines, an index action pair and a delete action.
	bulk := "\r\n{\"index\":{\"_index\":\"logs\"}}\r\n{\"message\":\"first\",\"@timestamp\":\"2026-10-26T06:00:00Z\",\"level\":\"info\",\"service\":\"es\"}\r\n\r\n{\"delete\":{\"_index\":\"logs\",\"_id\":\"1\"}}\r\n\r\n{\"index\":{}}\r\n{\"message\":\"second\",\"@timestamp\":\"2026-10-26T06:00:01Z\",\"level\":\"warn\",\"service\":\"es\"}\r\n"
	r := handlers_test_do(env, &arena, "POST", "/insert/elasticsearch/_bulk", "", bulk)
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.body, `"took":`))
	testing.expect(t, strings.contains(r.body, `"errors":false`))
	testing.expect(t, strings.contains(r.body, `"items":[]`))

	// The delete action contributes no document; only the two index docs land.
	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=*&limit=10", "")
	testing.expect_value(t, r.code, 200)
	testing.expect_value(t, strings.count(r.body, "\n"), 2)
	testing.expect(t, strings.contains(r.body, `"_msg":"first"`))
	testing.expect(t, strings.contains(r.body, `"_msg":"second"`))
	testing.expect(t, strings.contains(r.body, `"_time":"2026-10-26T06:00:00Z"`))
	testing.expect(t, strings.contains(r.body, `"_time":"2026-10-26T06:00:01Z"`))
	testing.expect(t, strings.contains(r.body, `"level":"info"`))
	testing.expect(t, strings.contains(r.body, `"level":"warn"`))
	// "message" and "@timestamp" are mapped to _msg/_time, not kept as fields.
	testing.expect(t, !strings.contains(r.body, `"message"`))
	testing.expect(t, !strings.contains(r.body, `"@timestamp"`))

	// An empty bulk body is accepted and ingests nothing.
	r = handlers_test_do(env, &arena, "POST", "/insert/elasticsearch/_bulk", "", "")
	testing.expect_value(t, r.code, 200)
	testing.expect(t, strings.contains(r.body, `"errors":false`))
	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=*&limit=10", "")
	testing.expect_value(t, strings.count(r.body, "\n"), 2)

	// Explicit field parameters disable the ES defaults.
	custom := `{"index":{}}
{"msg":"custom msg","ts":"2026-10-26T06:00:02Z","level":"trace","service":"es"}
`
	r = handlers_test_do(env, &arena, "POST", "/insert/elasticsearch", "_msg_field=msg&_time_field=ts", custom)
	testing.expect_value(t, r.code, 200)
	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=*&limit=10", "")
	testing.expect_value(t, strings.count(r.body, "\n"), 3)
	testing.expect(t, strings.contains(r.body, `"_msg":"custom msg"`))
	testing.expect(t, strings.contains(r.body, `"_time":"2026-10-26T06:00:02Z"`))
	testing.expect(t, !strings.contains(r.body, `"msg"`))
	testing.expect(t, !strings.contains(r.body, `"ts"`))

	r = handlers_test_do(env, &arena, "GET", "/insert/elasticsearch/_bulk", "", "")
	testing.expect_value(t, r.code, 405)
}

@(test)
test_web_jsonline_more_loki_streams :: proc(t: ^testing.T) {
	env := handlers_test_open(handlers_test_tmpdir("web_jsonline_loki"))
	defer handlers_test_close(env)
	arena: common.Arena
	defer common.arena_destroy(&arena)

	// Two streams, one with a second-resolution timestamp and one with
	// nanosecond timestamps that must keep their sub-second part.
	loki := `{"streams":[{"stream":{"level":"error","service":"api"},"values":[["1700000000","sec line"],["1700000000000000001","ns line"]]},{"stream":{"level":"info","service":"worker"},"values":[["1700000000000000002","worker line"]]}]}`
	r := handlers_test_do(env, &arena, "POST", "/insert/loki/api/v1/push", "", loki)
	testing.expect_value(t, r.code, 204)
	testing.expect_value(t, r.body, "")

	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=*&limit=10", "")
	testing.expect_value(t, r.code, 200)
	testing.expect_value(t, strings.count(r.body, "\n"), 3)
	testing.expect(t, strings.contains(r.body, `"_msg":"sec line"`))
	testing.expect(t, strings.contains(r.body, `"_msg":"ns line"`))
	testing.expect(t, strings.contains(r.body, `"_msg":"worker line"`))
	// 1700000000 is seconds (10 digits) and expands to the same instant as the
	// 19 digit nanosecond timestamp.
	testing.expect(t, strings.contains(r.body, `"_time":"2023-11-14T22:13:20Z"`))
	testing.expect(t, strings.contains(r.body, `"_time":"2023-11-14T22:13:20.000000001Z"`))
	testing.expect(t, strings.contains(r.body, `"_time":"2023-11-14T22:13:20.000000002Z"`))
	testing.expect(t, strings.contains(r.body, `"service":"api"`))
	testing.expect(t, strings.contains(r.body, `"service":"worker"`))

	// Each stream keeps its own label set, so field filters separate them.
	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=service%3Aapi&limit=10", "")
	testing.expect_value(t, strings.count(r.body, "\n"), 2)
	testing.expect(t, !strings.contains(r.body, "worker line"))
	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=level%3Ainfo&limit=10", "")
	testing.expect_value(t, strings.count(r.body, "\n"), 1)
	testing.expect(t, strings.contains(r.body, "worker line"))
}

@(test)
test_web_jsonline_more_loki_errors :: proc(t: ^testing.T) {
	env := handlers_test_open(handlers_test_tmpdir("web_jsonline_loki_err"))
	defer handlers_test_close(env)
	arena: common.Arena
	defer common.arena_destroy(&arena)

	r := handlers_test_do(env, &arena, "POST", "/insert/loki/api/v1/push", "", "not json")
	testing.expect_value(t, r.code, 400)
	testing.expect(t, strings.contains(r.body, "invalid json"))

	r = handlers_test_do(env, &arena, "POST", "/insert/loki/api/v1/push", "", `[]`)
	testing.expect_value(t, r.code, 400)
	testing.expect(t, strings.contains(r.body, "expected object"))

	r = handlers_test_do(env, &arena, "POST", "/insert/loki/api/v1/push", "", `{}`)
	testing.expect_value(t, r.code, 400)
	testing.expect(t, strings.contains(r.body, "missing streams"))

	r = handlers_test_do(env, &arena, "POST", "/insert/loki/api/v1/push", "", `{"streams":{}}`)
	testing.expect_value(t, r.code, 400)
	testing.expect(t, strings.contains(r.body, "streams must be an array"))

	// Malformed entries are skipped instead of failing the whole push.
	partial := `{"streams":[{"stream":"not-an-object","values":[]},{"stream":{"service":"api"},"values":[["1700000000000000003","ok line"],["1700000000000000004"],["1700000000000000005",42],["bad-ts","bad ts line"]]}]}`
	r = handlers_test_do(env, &arena, "POST", "/insert/loki/api/v1/push", "", partial)
	testing.expect_value(t, r.code, 204)
	r = handlers_test_do(env, &arena, "GET", "/select/logsql/query", "query=*&limit=10", "")
	testing.expect_value(t, strings.count(r.body, "\n"), 2)
	testing.expect(t, strings.contains(r.body, `"_msg":"ok line"`))
	testing.expect(t, strings.contains(r.body, `"_msg":"bad ts line"`))
	testing.expect(t, !strings.contains(r.body, "not-an-object"))

	// An empty stream list is a successful no-op.
	r = handlers_test_do(env, &arena, "POST", "/insert/loki/api/v1/push", "", `{"streams":[]}`)
	testing.expect_value(t, r.code, 204)
	testing.expect_value(t, r.body, "")

	r = handlers_test_do(env, &arena, "GET", "/insert/loki/api/v1/push", "", "")
	testing.expect_value(t, r.code, 405)
}
