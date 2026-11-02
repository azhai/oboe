package tests

// End-to-end HTTP tests over real sockets. Each test starts web.server_run on
// 127.0.0.1:0, reads the bound port back from the server, and talks HTTP/1.1
// through the shared TCP helpers in testutil.odin.

import web "../web"
import "base:runtime"
import "core:fmt"
import "core:net"
import "core:strings"
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
