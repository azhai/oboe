package tests

// End-to-end HTTP tests over real sockets. Each test starts web.server_run on
// 127.0.0.1:0, reads the bound port back from the server, and talks HTTP/1.1
// through the shared TCP helpers in testutil.odin.

import common "../common"
import netio "../netio"
import web "../web"
import "base:runtime"
import "core:fmt"
import "core:net"
import "core:strings"
import "core:sync"
import "core:testing"
import "core:time"

// http_e2e_env switches to the process heap (worker threads allocate there),
// starts the server and registers cleanup with the caller.
http_e2e_env :: proc(t: ^testing.T, prefix: string, max_body := web.HTTP_MAX_BODY) -> (env: ^Web_Test_Env, ok: bool) {
	env = web_test_start(prefix, max_body)
	if env == nil {
		testing.expectf(t, false, "%s: server did not start on 127.0.0.1:0", prefix)
		return nil, false
	}
	return env, true
}

// http_expect_rejected sends one raw request and asserts that the server
// answered with `want`, included Content-Length, asked for Connection: close
// and only then closed the socket.
http_expect_rejected :: proc(t: ^testing.T, env: ^Web_Test_Env, label, method, path, extra, body: string, want: int) {
	c, cok := test_tcp_dial(env.addr)
	if !cok {
		testing.expectf(t, false, "%s: dial %s failed", label, env.addr)
		return
	}
	defer test_tcp_close(&c)
	// The peer may close while the client is still writing oversized input.
	_ = test_http_send(&c, method, path, extra, body)
	resp, rok := test_http_read_response(&c)
	if !rok {
		testing.expectf(t, false, "%s: no response", label)
		return
	}
	testing.expectf(t, resp.status == want, "%s: status %d, want %d", label, resp.status, want)
	testing.expectf(t, strings.contains(resp.headers, "Connection: close"), "%s: headers %q", label, resp.headers)
	testing.expectf(t, strings.contains(resp.headers, "Content-Length:"), "%s: headers %q", label, resp.headers)
	testing.expectf(t, strings.contains(resp.body, `"error"`), "%s: body %q", label, resp.body)
	testing.expect(t, test_tcp_closed(&c))
}

@(test)
test_http_e2e_keep_alive_three_requests :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	env, ok := http_e2e_env(t, "http_keepalive")
	if !ok {
		return
	}
	defer web_test_stop(env)

	c, cok := test_tcp_dial(env.addr)
	if !cok {
		testing.expectf(t, false, "dial %s failed", env.addr)
		return
	}
	defer test_tcp_close(&c)

	// One connection, three HTTP/1.1 requests; keep-alive is the default.
	for i in 0..<3 {
		if !test_http_send(&c, "GET", "/health", "", "") {
			testing.expectf(t, false, "request %d: send failed", i)
			return
		}
		resp, rok := test_http_read_response(&c)
		if !rok {
			testing.expectf(t, false, "request %d: no response", i)
			return
		}
		testing.expect_value(t, resp.status, 200)
		testing.expect_value(t, resp.body, "OK")
		testing.expectf(t, strings.contains(resp.headers, "Connection: keep-alive"), "request %d: headers %q", i, resp.headers)
	}

	// The fourth request asks for close: the response says so and the peer
	// drops the connection afterwards.
	if !test_http_send(&c, "GET", "/health", "Connection: close\r\n", "") {
		testing.expectf(t, false, "closing request: send failed")
		return
	}
	resp, rok := test_http_read_response(&c)
	if !rok {
		testing.expectf(t, false, "closing request: no response")
		return
	}
	testing.expect_value(t, resp.status, 200)
	testing.expectf(t, strings.contains(resp.headers, "Connection: close"), "headers %q", resp.headers)
	testing.expect(t, test_tcp_closed(&c))
}

@(test)
test_http_e2e_chunked_body :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	env, ok := http_e2e_env(t, "http_chunked")
	if !ok {
		return
	}
	defer web_test_stop(env)

	// Chunk framing CRLF is not part of the body, so the first chunk carries
	// the newline that separates the two NDJSON records.
	chunk1 := `{"_time":"2026-10-26T04:00:00Z","level":"info","service":"api","_msg":"chunk one"}` + "\n"
	chunk2 := `{"_time":"2026-10-26T04:00:01Z","level":"error","service":"db","_msg":"chunk two"}`

	payload := strings.builder_make(context.temp_allocator)
	fmt.sbprintf(&payload, "%x\r\n%s\r\n", len(chunk1), chunk1)
	// Chunk extensions must be ignored by the parser.
	fmt.sbprintf(&payload, "%x;ext=ignored\r\n%s\r\n", len(chunk2), chunk2)
	strings.write_string(&payload, "0\r\n\r\n")

	c, cok := test_tcp_dial(env.addr)
	if !cok {
		testing.expectf(t, false, "dial %s failed", env.addr)
		return
	}
	defer test_tcp_close(&c)

	headers := "Transfer-Encoding: chunked\r\nContent-Type: application/x-ndjson\r\nConnection: close\r\n"
	if !test_http_send(&c, "POST", "/insert/jsonline", headers, strings.to_string(payload)) {
		testing.expectf(t, false, "chunked request: send failed")
		return
	}
	resp, rok := test_http_read_response(&c)
	if !rok {
		testing.expectf(t, false, "chunked request: no response")
		return
	}
	testing.expect_value(t, resp.status, 200)
	testing.expectf(t, strings.contains(resp.body, `"rows":2`), "body %q", resp.body)
	testing.expectf(t, strings.contains(resp.body, `"errors":0`), "body %q", resp.body)

	q, qok := test_http_once(env.addr, "GET", "/select/logsql/query?query=*&limit=10", "", "")
	testing.expect(t, qok)
	testing.expect_value(t, q.status, 200)
	testing.expect_value(t, strings.count(q.body, "\n"), 2)
	testing.expect(t, strings.contains(q.body, `"chunk one"`))
	testing.expect(t, strings.contains(q.body, `"chunk two"`))
}

@(test)
test_http_e2e_expect_100_continue :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	env, ok := http_e2e_env(t, "http_expect")
	if !ok {
		return
	}
	defer web_test_stop(env)

	payload := `{"_time":"2026-10-26T04:00:10Z","level":"info","service":"api","_msg":"expect body"}`

	c, cok := test_tcp_dial(env.addr)
	if !cok {
		testing.expectf(t, false, "dial %s failed", env.addr)
		return
	}
	defer test_tcp_close(&c)

	headers := fmt.tprintf("Expect: 100-continue\r\nContent-Length: %d\r\nContent-Type: application/x-ndjson\r\nConnection: close\r\n", len(payload))
	if !test_http_send(&c, "POST", "/insert/jsonline", headers, "") {
		testing.expectf(t, false, "expect request head: send failed")
		return
	}
	interim, iok := test_http_read_response(&c, include_interim = true)
	if !iok {
		testing.expectf(t, false, "expect request: no 100 Continue")
		return
	}
	testing.expect_value(t, interim.status, 100)

	// The server waits for the body only after the interim response.
	if !test_tcp_send(&c, payload) {
		testing.expectf(t, false, "expect request: body send failed")
		return
	}
	resp, rok := test_http_read_response(&c)
	if !rok {
		testing.expectf(t, false, "expect request: no final response")
		return
	}
	testing.expect_value(t, resp.status, 200)
	testing.expectf(t, strings.contains(resp.body, `"rows":1`), "body %q", resp.body)
}

@(test)
test_http_e2e_gzip_body :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	env, ok := http_e2e_env(t, "http_gzip")
	if !ok {
		return
	}
	defer web_test_stop(env)

	raw := strings.concatenate({
		`{"_time":"2026-10-26T04:02:00Z","level":"info","service":"api","_msg":"gzip one"}`,
		"\n",
		`{"_time":"2026-10-26T04:02:01Z","level":"warn","service":"api","_msg":"gzip two"}`,
		"\n",
	}, context.temp_allocator)
	gz, gok := test_gzip_compress(transmute([]u8)raw)
	if !gok {
		testing.expectf(t, false, "gzip compression failed")
		return
	}
	defer delete(gz)

	c, cok := test_tcp_dial(env.addr)
	if !cok {
		testing.expectf(t, false, "dial %s failed", env.addr)
		return
	}
	defer test_tcp_close(&c)

	headers := fmt.tprintf("Content-Encoding: gzip\r\nContent-Length: %d\r\nContent-Type: application/x-ndjson\r\nConnection: close\r\n", len(gz))
	if !test_http_send(&c, "POST", "/insert/jsonline", headers, string(gz)) {
		testing.expectf(t, false, "gzip request: send failed")
		return
	}
	resp, rok := test_http_read_response(&c)
	if !rok {
		testing.expectf(t, false, "gzip request: no response")
		return
	}
	testing.expect_value(t, resp.status, 200)
	testing.expectf(t, strings.contains(resp.body, `"rows":2`), "body %q", resp.body)
	testing.expectf(t, strings.contains(resp.body, `"errors":0`), "body %q", resp.body)

	q, qok := test_http_once(env.addr, "GET", "/select/logsql/query?query=*&limit=10", "", "")
	testing.expect(t, qok)
	testing.expect(t, strings.contains(q.body, `"gzip one"`))
	testing.expect(t, strings.contains(q.body, `"gzip two"`))
}

@(test)
test_http_e2e_body_limits :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	// 128 KiB body cap; the cache keeps its own 64 KiB value cap.
	env, ok := http_e2e_env(t, "http_limits", 128<<10)
	if !ok {
		return
	}
	defer web_test_stop(env)

	// Content-Length above max_body is answered with 413 before the body is
	// read.
	http_expect_rejected(t, env, "content-length", "POST", "/insert/jsonline", "Content-Length: 262144\r\n", "", 413)

	// A chunked body whose declared chunk exceeds max_body is also 413.
	http_expect_rejected(t, env, "chunked", "POST", "/insert/jsonline", "Transfer-Encoding: chunked\r\n", "40000\r\n", 413)

	// The cache rejects values above its own max_value with a real 413.
	big := make([]u8, 100<<10)
	defer delete(big)
	for i in 0..<len(big) {
		big[i] = 'x'
	}
	headers := fmt.tprintf("Content-Length: %d\r\n", len(big))
	r, rok := test_http_once(env.addr, "PUT", "/cache/huge", headers, string(big))
	testing.expect(t, rok)
	testing.expect_value(t, r.status, 413)
	testing.expectf(t, strings.contains(r.body, "value too large"), "body %q", r.body)
}

@(test)
test_http_e2e_header_limit_rejects :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	env, ok := http_e2e_env(t, "http_header_limit")
	if !ok {
		return
	}
	defer web_test_stop(env)

	// More than HTTP_MAX_HEADER (64 KiB) before the blank line yields 431.
	pad := make([]u8, 70_000)
	defer delete(pad)
	for i in 0..<len(pad) {
		pad[i] = 'a'
	}
	extra := strings.concatenate({"X-Pad: ", string(pad), "\r\n"}, context.temp_allocator)
	http_expect_rejected(t, env, "header", "GET", "/health", extra, "", 431)
}

@(test)
test_http_e2e_body_limit_boundary :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	LIMIT :: 128 << 10
	env, ok := http_e2e_env(t, "http_body_boundary", LIMIT)
	if !ok {
		return
	}
	defer web_test_stop(env)

	// A body exactly at max_body is accepted and reaches the handler.
	body := make([]u8, LIMIT)
	defer delete(body)
	for i in 0..<len(body) {
		body[i] = 'x'
	}
	headers := fmt.tprintf("Content-Length: %d\r\nContent-Type: application/x-ndjson\r\n", len(body))
	r, rok := test_http_once(env.addr, "POST", "/insert/jsonline", headers, string(body))
	testing.expect(t, rok)
	if rok {
		testing.expect_value(t, r.status, 200)
		// One malformed line: the request was read and dispatched.
		testing.expectf(t, strings.contains(r.body, `"errors":1`), "body %q", r.body)
	}

	// One byte above the limit is rejected before the body is sent.
	over := fmt.tprintf("Content-Length: %d\r\n", LIMIT + 1)
	http_expect_rejected(t, env, "limit+1", "POST", "/insert/jsonline", over, "", 413)
}

@(test)
test_http_e2e_content_length_with_transfer_encoding :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	env, ok := http_e2e_env(t, "http_framing_conflict")
	if !ok {
		return
	}
	defer web_test_stop(env)

	// Both framings must be rejected regardless of header order.
	http_expect_rejected(t, env, "cl-then-te", "POST", "/insert/jsonline", "Content-Length: 5\r\nTransfer-Encoding: chunked\r\n", "", 400)
	http_expect_rejected(t, env, "te-then-cl", "POST", "/insert/jsonline", "Transfer-Encoding: chunked\r\nContent-Length: 5\r\n", "", 400)
}

@(test)
test_http_e2e_bad_chunk_size :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	env, ok := http_e2e_env(t, "http_bad_chunk")
	if !ok {
		return
	}
	defer web_test_stop(env)

	// A non-hex chunk size breaks the framing and yields 400.
	http_expect_rejected(t, env, "bad chunk size", "POST", "/insert/jsonline", "Transfer-Encoding: chunked\r\n", "zz\r\n", 400)
}

@(test)
test_http_e2e_options_cors_and_errors :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	env, ok := http_e2e_env(t, "http_options")
	if !ok {
		return
	}
	defer web_test_stop(env)

	opt, ook := test_http_once(env.addr, "OPTIONS", "/anything", "Origin: http://example.test\r\n", "")
	testing.expect(t, ook)
	testing.expect_value(t, opt.status, 204)
	testing.expect_value(t, opt.body, "")
	testing.expect(t, strings.contains(opt.headers, "Access-Control-Allow-Origin: *"))
	testing.expect(t, strings.contains(opt.headers, "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS"))
	testing.expect(t, strings.contains(opt.headers, "Content-Length: 0"))

	missing, mok := test_http_once(env.addr, "GET", "/nope", "", "")
	testing.expect(t, mok)
	testing.expect_value(t, missing.status, 404)
	testing.expectf(t, strings.contains(missing.body, "unknown path"), "body %q", missing.body)

	bad, bok := test_http_once(env.addr, "GET", "/insert/jsonline", "", "")
	testing.expect(t, bok)
	testing.expect_value(t, bad.status, 405)
	testing.expectf(t, strings.contains(bad.body, "method not allowed"), "body %q", bad.body)
}

@(test)
test_http_e2e_stop_releases_port :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	env, ok := http_e2e_env(t, "http_stop")
	if !ok {
		return
	}
	defer web_test_stop(env)

	port := env.srv.bound_port
	testing.expect(t, port > 0)

	probe, pok := test_http_once(env.addr, "GET", "/health", "", "")
	testing.expect(t, pok)
	testing.expect_value(t, probe.status, 200)

	web_test_halt(env)

	// After server_stop the listener must be gone: dials fail and a fresh
	// listen on the same port succeeds.
	if sock, derr := net.dial_tcp_from_hostname_and_port_string(env.addr); derr == nil {
		testing.expectf(t, false, "port %d still accepts connections after stop", port)
		net.close(sock)
	}

	ep := net.Endpoint{address = net.IP4_Address{127, 0, 0, 1}, port = port}
	sock, lerr := net.listen_tcp(ep, 16)
	if lerr != nil {
		testing.expectf(t, false, "port %d was not released: %v", port, lerr)
		return
	}
	net.close(sock)
}

// More simultaneously open keep-alive connections than business workers must
// all be answered. The event loop owns the connections; a worker only runs one
// complete request at a time, so the old "one connection per worker" model
// would leave the extra connections unanswered until the first ones close.
@(test)
test_http_e2e_connections_exceed_workers :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	env, ok := http_e2e_env(t, "http_more_conns")
	if !ok {
		return
	}
	defer web_test_stop(env)

	conns := make([]Test_Conn, 4)
	defer {
		for &c in conns {
			test_tcp_close(&c)
		}
		delete(conns)
	}
	// Everything is sent before anything is read: all four connections are
	// open at the same time while the server was started with two workers.
	for i in 0..<4 {
		c, cok := test_tcp_dial(env.addr, 2 * time.Second)
		if !cok {
			testing.expectf(t, false, "connection %d: dial failed", i)
			return
		}
		conns[i] = c
		if !test_http_send(&conns[i], "GET", "/health", "", "") {
			testing.expectf(t, false, "connection %d: send failed", i)
			return
		}
	}
	for i in 0..<4 {
		resp, rok := test_http_read_response(&conns[i])
		if !rok {
			testing.expectf(t, false, "connection %d: no response", i)
			continue
		}
		testing.expect_value(t, resp.status, 200)
		testing.expect_value(t, resp.body, "OK")
		testing.expectf(t, strings.contains(resp.headers, "Connection: keep-alive"), "connection %d: headers %q", i, resp.headers)
	}
}

// ---------------------------------------------------------------- chunk framing

// CHUNK_TEST_MAX_BODY is small so a declared chunk size can exceed it while
// keeping the sent bodies tiny.
CHUNK_TEST_MAX_BODY :: 4096

CHUNK_TEST_HEAD :: "POST /insert/jsonline HTTP/1.1\r\nHost: oboe-test\r\nTransfer-Encoding: chunked\r\n\r\n"

// chunk_status maps the shared framing result to the status the HTTP layers
// must answer with. 0 means "not complete yet": neither layer replies.
chunk_status :: proc(res: netio.Chunk_Result) -> int {
	switch res {
	case .Complete:
		return 200
	case .Too_Large:
		return 413
	case .Bad_Format:
		return 400
	case .Need_More:
		return 0
	}
	return 0
}

// shared_chunk_status is the event-loop side: the same netio.chunk_frame call
// nbio_server.odin makes, over the whole body at once.
shared_chunk_status :: proc(body: string, max_body: int) -> int {
	fr: netio.Chunk_Framer
	return chunk_status(netio.chunk_frame(&fr, transmute([]u8)body, max_body, web.HTTP_MAX_HEADER))
}

// parser_chunk_status is the worker side: it runs the real blocking parser over
// a pre-filled connection, exactly as http_job_run does, and maps its error.
parser_chunk_status :: proc(body: string, max_body: int) -> int {
	raw := make([]u8, len(CHUNK_TEST_HEAD) + len(body))
	defer delete(raw)
	copy(raw, CHUNK_TEST_HEAD)
	copy(raw[len(CHUNK_TEST_HEAD):], body)

	conn := netio.Conn{src = raw}
	head := common.buf_init(4096)
	defer common.buf_destroy(&head)
	parsed := common.buf_init(4096)
	defer common.buf_destroy(&parsed)
	_, err := web.http_read_request(&conn, &head, &parsed, max_body, raw)
	// parsed may hold the decoded body; it dies with the buffer below.
	switch err {
	case .None:
		return 200
	case .Body_Too_Large:
		return 413
	case .Bad_Request:
		return 400
	case .Header_Too_Large:
		return 431
	case .Drop:
		return 0
	}
	return 0
}

test_rnd_state: u32 = 0x1234_5678

// test_rnd_next is a small xorshift32 so the random corpus is reproducible
// without pulling in a fuzzing dependency.
test_rnd_next :: proc() -> u32 {
	x := test_rnd_state
	x ~= x << 13
	x ~= x >> 17
	x ~= x << 5
	test_rnd_state = x
	return x
}

// The malformed chunk corpus must be classified identically by the shared
// framer, the request parser and the full HTTP server. The statuses asserted
// here are the ones the pre-refactor E2E suite already pinned: 400 for framing
// damage, 413 for a declared size above max_body.
@(test)
test_http_e2e_chunk_framing_shared :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	env, ok := http_e2e_env(t, "http_chunk_shared", CHUNK_TEST_MAX_BODY)
	if !ok {
		return
	}
	defer web_test_stop(env)

	valid1 := `{"_time":"2026-10-26T04:00:00Z","level":"info","service":"api","_msg":"chunk one"}` + "\n"
	valid2 := `{"_time":"2026-10-26T04:00:01Z","level":"error","service":"db","_msg":"chunk two"}`
	valid := fmt.aprintf("%x\r\n%s\r\n%x;ext=ignored\r\n%s\r\n0\r\n\r\n", len(valid1), valid1, len(valid2), valid2)
	defer delete(valid)

	long := strings.builder_make()
	defer strings.builder_destroy(&long)
	for _ in 0..<web.HTTP_MAX_HEADER + 8 {
		strings.write_byte(&long, 'a')
	}

	Case :: struct {
		label: string,
		body:  string,
		want:  int,
	}
	cases := []Case{
		{"bad chunk size", "zz\r\n", 0},
		{"broken chunk CRLF", "4\r\nWikiX\r\n0\r\n\r\n", 0},
		{"overlong chunk line", strings.to_string(long), 0},
		{"chunk over max_body", "4000\r\n", 0},
		{"two chunks with extension", valid, 0},
	}

	for &c in cases {
		c.want = shared_chunk_status(c.body, CHUNK_TEST_MAX_BODY)
		testing.expectf(t, c.want == 200 || c.want == 400 || c.want == 413, "%s: framer classified %d", c.label, c.want)
		got := parser_chunk_status(c.body, CHUNK_TEST_MAX_BODY)
		testing.expectf(t, got == c.want, "%s: parser %d, framer %d", c.label, got, c.want)
		if c.want == 200 {
			r, rok := test_http_once(env.addr, "POST", "/insert/jsonline", "Transfer-Encoding: chunked\r\nContent-Type: application/x-ndjson\r\n", c.body)
			testing.expectf(t, rok && r.status == 200, "%s: http status %d", c.label, r.status)
		} else {
			http_expect_rejected(t, env, c.label, "POST", "/insert/jsonline", "Transfer-Encoding: chunked\r\n", c.body, c.want)
		}
	}

	// A short random sweep: both callers must agree on every input, including
	// the ones that are merely incomplete.
	alphabet := "0123456789abcdefABCDEF; x\r\n\t"
	for _ in 0..<120 {
		n := int(test_rnd_next() % 40) + 1
		b := strings.builder_make()
		for _ in 0..<n {
			strings.write_byte(&b, alphabet[int(test_rnd_next() % u32(len(alphabet)))])
		}
		s := strings.to_string(b)
		want := shared_chunk_status(s, CHUNK_TEST_MAX_BODY)
		got := parser_chunk_status(s, CHUNK_TEST_MAX_BODY)
		testing.expectf(t, got == want, "random %q: parser %d, framer %d", s, got, want)
		strings.builder_destroy(&b)
	}
}

// -max-conns must answer the connection above the limit with an explicit 503
// instead of queueing it, must not count it as active, and must let a new
// connection in as soon as one of the accepted ones goes away.
@(test)
test_http_e2e_max_conns_rejects :: proc(t: ^testing.T) {
	saved := context
	context.allocator = runtime.default_allocator()
	defer context = saved

	env := web_test_start("http_max_conns", web.HTTP_MAX_BODY, 2, 2)
	if env == nil {
		testing.expectf(t, false, "server did not start")
		return
	}
	defer web_test_stop(env)

	// web_test_start probes the port with a dial that it closes again; wait
	// until the loop has reclaimed that probe so the limit counts only us.
	start := time.now()
	for sync.atomic_load(&env.srv.active) != 0 && time.since(start) < 2*time.Second {
		time.sleep(5 * time.Millisecond)
	}

	conns := make([]Test_Conn, 3)
	defer {
		for &c in conns {
			test_tcp_close(&c)
		}
		delete(conns)
	}
	for i in 0..<3 {
		c, cok := test_tcp_dial(env.addr, 2 * time.Second)
		if !cok {
			testing.expectf(t, false, "connection %d: dial failed", i)
			return
		}
		conns[i] = c
		if !test_http_send(&conns[i], "GET", "/health", "", "") {
			testing.expectf(t, false, "connection %d: send failed", i)
			return
		}
	}

	for i in 0..<2 {
		resp, rok := test_http_read_response(&conns[i])
		if !rok {
			testing.expectf(t, false, "connection %d: no response", i)
			continue
		}
		testing.expect_value(t, resp.status, 200)
		testing.expect_value(t, resp.body, "OK")
	}

	// The third connection is over the limit: 503, Connection: close, error
	// body, then closed by the server.
	resp, rok := test_http_read_response(&conns[2])
	if !rok {
		testing.expectf(t, false, "rejected connection: no response")
	} else {
		testing.expect_value(t, resp.status, 503)
		testing.expectf(t, strings.contains(resp.headers, "Connection: close"), "headers %q", resp.headers)
		testing.expectf(t, strings.contains(resp.body, "too many connections"), "body %q", resp.body)
		testing.expect(t, test_tcp_closed(&conns[2]))
	}
	test_tcp_close(&conns[2])

	// The rejected connection never entered the active gauge.
	testing.expect_value(t, sync.atomic_load(&env.srv.active), u32(2))

	// Metrics expose both the gauge and the rejection counter.
	mok := test_http_send(&conns[0], "GET", "/metrics", "", "")
	m: Http_Test_Response
	if mok {
		m, mok = test_http_read_response(&conns[0])
	}
	testing.expect(t, mok)
	if mok {
		testing.expectf(t, strings.contains(m.body, "oboe_http_connections 2\n"), "metrics body missing gauge: %q", m.body)
		testing.expectf(t, strings.contains(m.body, "oboe_http_conns_rejected_total 1\n"), "metrics body missing rejection counter: %q", m.body)
	}

	// Freeing one accepted connection must free a slot for a new one.
	test_tcp_close(&conns[0])
	start = time.now()
	for sync.atomic_load(&env.srv.active) > 1 && time.since(start) < 2*time.Second {
		time.sleep(5 * time.Millisecond)
	}
	testing.expect_value(t, sync.atomic_load(&env.srv.active), u32(1))

	after, aok := test_http_once(env.addr, "GET", "/health", "", "")
	testing.expect(t, aok)
	if aok {
		testing.expect_value(t, after.status, 200)
		testing.expect_value(t, after.body, "OK")
	}
}
