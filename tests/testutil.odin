package tests

// Shared helpers for the cross-package suites. They live only in the test
// package so library packages stay free of test scaffolding.

import "../common"
import "../logdb"
import cachelib "../cache"
import queuelib "../queue"
import web "../web"
import "core:fmt"
import "core:net"
import "core:os"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"
import "vendor:zlib"

test_dir_seq: u32

// test_tmpdir returns a fresh, per-process, per-test directory. The pid and
// an atomic counter keep concurrent `odin test` runs from sharing state.
test_tmpdir :: proc(prefix: string) -> string {
	n := sync.atomic_add(&test_dir_seq, u32(1))
	dir := fmt.aprintf("/tmp/oboe_%s_%d_%d", prefix, os.get_pid(), n)
	os.remove_all(dir)
	os.make_directory_all(dir)
	return dir
}

test_tmpdir_cleanup :: proc(dir: string) {
	os.remove_all(dir)
	delete(dir)
}

// ---------------------------------------------------------------- ports

// test_wait_port dials addr until it accepts or the timeout expires. It
// returns true once a TCP connection could be established.
test_wait_port :: proc(addr: string, timeout := 3 * time.Second) -> bool {
	deadline := time.now()
	wait := 5 * time.Millisecond
	for time.since(deadline) < timeout {
		if sock, err := net.dial_tcp_from_hostname_and_port_string(addr); err == nil {
			net.close(sock)
			return true
		}
		time.sleep(wait)
	}
	return false
}

// ---------------------------------------------------------------- tcp client

TEST_TCP_BUF :: 1 << 16

Test_Conn :: struct {
	sock:  net.TCP_Socket,
	buf:   [TEST_TCP_BUF]u8,
	start: int,
	end:   int,
}

test_tcp_dial :: proc(addr: string, timeout := 5 * time.Second) -> (c: Test_Conn, ok: bool) {
	sock, err := net.dial_tcp_from_hostname_and_port_string(addr)
	if err != nil {
		return c, false
	}
	_ = net.set_option(sock, .Receive_Timeout, timeout)
	_ = net.set_option(sock, .Send_Timeout, timeout)
	_ = net.set_option(sock, .TCP_Nodelay, true)
	c.sock = sock
	return c, true
}

test_tcp_close :: proc(c: ^Test_Conn) {
	if c.sock != 0 {
		net.close(c.sock)
		c.sock = 0
	}
}

test_tcp_send :: proc(c: ^Test_Conn, data: string) -> bool {
	remaining := transmute([]u8)data
	for len(remaining) > 0 {
		n, err := net.send_tcp(c.sock, remaining)
		if err != nil || n <= 0 {
			return false
		}
		remaining = remaining[n:]
	}
	return true
}

test_tcp_fill :: proc(c: ^Test_Conn) -> bool {
	n, err := net.recv_tcp(c.sock, c.buf[:])
	if err != nil || n <= 0 {
		return false
	}
	c.start = 0
	c.end = n
	return true
}

// Strings returned by these helpers are allocated with the test thread's temp
// allocator; they stay valid for the duration of the test.
test_tcp_read_line :: proc(c: ^Test_Conn) -> (string, bool) {
	line := make([dynamic]u8, 0, 256, context.temp_allocator)
	for {
		if c.start >= c.end {
			if !test_tcp_fill(c) {
				return "", false
			}
		}
		b := c.buf[c.start]
		c.start += 1
		if b == '\n' {
			if len(line) > 0 && line[len(line)-1] == '\r' {
				resize(&line, len(line)-1)
			}
			return string(line[:]), true
		}
		append(&line, b)
	}
}

test_tcp_read_exact :: proc(c: ^Test_Conn, n: int) -> (string, bool) {
	if n <= 0 {
		return "", true
	}
	out := make([dynamic]u8, 0, n, context.temp_allocator)
	remaining := n
	for remaining > 0 {
		if c.start >= c.end {
			if !test_tcp_fill(c) {
				return "", false
			}
		}
		avail := c.end - c.start
		take := min(avail, remaining)
		append(&out, ..c.buf[c.start:c.start+take])
		c.start += take
		remaining -= take
	}
	return string(out[:]), true
}

// test_tcp_closed reports whether the peer closed the connection without any
// further data. It also asserts that error responses are followed by a close.
test_tcp_closed :: proc(c: ^Test_Conn, timeout := 2 * time.Second) -> bool {
	_ = net.set_option(c.sock, .Receive_Timeout, timeout)
	if c.start < c.end {
		return false
	}
	n, err := net.recv_tcp(c.sock, c.buf[:])
	return err != nil || n == 0
}

// ---------------------------------------------------------------- http

Http_Test_Response :: struct {
	status:  int,
	headers: string,
	body:    string,
}

test_http_parse_status :: proc(line: string) -> (int, bool) {
	fields := strings.fields(line)
	if len(fields) < 2 {
		return 0, false
	}
	n, ok := strconv.parse_int(fields[1], 10)
	return n, ok
}

// test_http_read_response reads one response. 1xx interim responses (such as
// 100 Continue) are skipped unless include_interim is set.
test_http_read_response :: proc(c: ^Test_Conn, include_interim := false) -> (resp: Http_Test_Response, ok: bool) {
	for {
		status_line, lok := test_tcp_read_line(c)
		if !lok {
			return resp, false
		}
		status, sok := test_http_parse_status(status_line)
		if !sok {
			return resp, false
		}

		header_lines := make([dynamic]u8, 0, 512, context.temp_allocator)
		content_length := 0
		for {
			line, hok := test_tcp_read_line(c)
			if !hok {
				return resp, false
			}
			if len(line) == 0 {
				break
			}
			if len(header_lines) > 0 {
				append(&header_lines, '\n')
			}
			append(&header_lines, ..transmute([]u8)line)
			colon := strings.index_byte(line, ':')
			if colon > 0 && common.token_eq_ci(strings.trim_space(line[:colon]), "content-length") {
				n, nok := strconv.parse_int(strings.trim_space(line[colon+1:]), 10)
				if nok && n >= 0 {
					content_length = n
				}
			}
		}

		if status >= 100 && status < 200 && !include_interim {
			// Interim response has no body; read the real one.
			continue
		}
		body, bok := test_tcp_read_exact(c, content_length)
		if !bok {
			return resp, false
		}
		resp.status = status
		resp.headers = string(header_lines[:])
		resp.body = body
		return resp, true
	}
}

// test_http_send writes a request. extra is a header block where each line
// already ends with CRLF; it may be empty.
test_http_send :: proc(c: ^Test_Conn, method, path, extra, body: string) -> bool {
	b := strings.builder_make(context.temp_allocator)
	fmt.sbprintf(&b, "%s %s HTTP/1.1\r\nHost: oboe-test\r\n", method, path)
	strings.write_string(&b, extra)
	strings.write_string(&b, "\r\n")
	strings.write_string(&b, body)
	return test_tcp_send(c, strings.to_string(b))
}

// test_http_once opens a connection, sends one request with Connection: close
// and reads the response.
test_http_once :: proc(addr, method, path, extra, body: string) -> (Http_Test_Response, bool) {
	c, cok := test_tcp_dial(addr)
	if !cok {
		return {}, false
	}
	defer test_tcp_close(&c)
	headers := strings.concatenate({"Connection: close\r\n", extra}, context.temp_allocator)
	if !test_http_send(&c, method, path, headers, body) {
		return {}, false
	}
	return test_http_read_response(&c)
}

// ---------------------------------------------------------------- web service

// TEST_STREAM_FIELDS must have static lifetime: the server keeps the slice in
// cfg.stream_fields and worker threads read it long after start returns.
TEST_STREAM_FIELDS := [2]string{"level", "service"}

// Web_Test_Env owns a real HTTP server, its store and cache. The caller is
// responsible for switching context.allocator to the process heap before
// start: worker threads allocate and free through this structures.
Web_Test_Env :: struct {
	srv:    ^web.Server,
	store:  ^logdb.Store,
	cache:  ^cachelib.Cache,
	dir:    string,
	addr:   string,
	runner: ^thread.Thread,
}

web_test_runner :: proc(data: rawptr) {
	env := cast(^Web_Test_Env)data
	_ = web.server_run(env.srv)
}

// web_test_start launches web.server_run on 127.0.0.1:0 and waits until the
// ephemeral port accepts connections. Returns nil if the server never came up.
web_test_start :: proc(prefix: string, max_body := web.HTTP_MAX_BODY, workers := 2, max_conns := web.DEFAULT_MAX_CONNS) -> ^Web_Test_Env {
	env := new(Web_Test_Env)
	env.dir = test_tmpdir(prefix)

	store_cfg := logdb.default_store_config()
	store_cfg.data_path = env.dir
	// Disable the background flusher so tests do not race disk IO.
	store_cfg.flush_interval = time.Duration(1 << 62)
	store_cfg.flush_max_rows = 1 << 30
	store_cfg.flush_max_bytes = 1 << 40
	store_cfg.stream_fields = TEST_STREAM_FIELDS[:]
	env.store = logdb.store_init(store_cfg, false)

	cache_cfg := cachelib.default_cache_config()
	cache_cfg.max_bytes = 4 << 20
	cache_cfg.max_entries = 4096
	cache_cfg.max_value = 64 << 10
	env.cache = cachelib.cache_init(cache_cfg)

	srv_cfg := web.default_server_config()
	srv_cfg.listen = "127.0.0.1:0"
	srv_cfg.workers = workers
	srv_cfg.max_conns = max_conns
	srv_cfg.max_body = max_body
	srv_cfg.stream_fields = store_cfg.stream_fields
	env.srv = web.server_init(srv_cfg, env.store, env.cache, nil)

	env.runner = thread.create_and_start_with_data(env, web_test_runner, self_cleanup = false, name = "oboe-http-test")
	deadline := time.now()
	for env.srv.bound_port == 0 && time.since(deadline) < 3*time.Second {
		time.sleep(time.Millisecond)
	}
	if env.srv.bound_port == 0 {
		web_test_stop(env)
		return nil
	}
	env.addr = fmt.aprintf("127.0.0.1:%d", env.srv.bound_port)
	if !test_wait_port(env.addr) {
		web_test_stop(env)
		return nil
	}
	return env
}

// web_test_halt stops the server and joins the accept thread, but keeps the
// store/cache/dir alive. It is idempotent, so web_test_stop can be called later.
web_test_halt :: proc(env: ^Web_Test_Env) {
	if env == nil {
		return
	}
	if env.srv != nil && env.srv.listener != 0 {
		web.server_stop(env.srv)
	}
	if env.runner != nil {
		thread.join(env.runner)
		thread.destroy(env.runner)
		env.runner = nil
	}
}

web_test_stop :: proc(env: ^Web_Test_Env) {
	if env == nil {
		return
	}
	web_test_halt(env)
	if env.srv != nil {
		web.server_destroy(env.srv)
		env.srv = nil
	}
	if env.cache != nil {
		cachelib.cache_destroy(env.cache)
		env.cache = nil
	}
	if env.store != nil {
		logdb.store_close(env.store)
		env.store = nil
	}
	if len(env.addr) > 0 {
		delete(env.addr)
	}
	test_tmpdir_cleanup(env.dir)
	free(env)
}

// ---------------------------------------------------------------- queue service

// Queue_Test_Env owns a real beanstalk-protocol server listening on an
// ephemeral port. As with Web_Test_Env, switch to the process heap first.
Queue_Test_Env :: struct {
	q:    ^queuelib.Queue,
	qs:   ^queuelib.Queue_Server,
	addr: string,
}

queue_test_start :: proc() -> ^Queue_Test_Env {
	env := new(Queue_Test_Env)
	cfg := queuelib.default_queue_config()
	cfg.listen = "127.0.0.1:0"
	env.q = queuelib.queue_init(cfg)
	env.qs = queuelib.queue_server_start(env.q)
	if env.qs == nil {
		queuelib.queue_destroy(env.q)
		free(env)
		return nil
	}
	env.addr = fmt.aprintf("127.0.0.1:%d", env.qs.bound_port)
	if !test_wait_port(env.addr) {
		queue_test_stop(env)
		return nil
	}
	return env
}

queue_test_stop :: proc(env: ^Queue_Test_Env) {
	if env == nil {
		return
	}
	// Callers close their sockets before deferring into here; give the
	// per-connection threads a moment to unwind before freeing the queue.
	queue_test_wait_connections(env.q, 0)
	queuelib.queue_server_stop(env.qs)
	queuelib.queue_destroy(env.q)
	delete(env.addr)
	free(env)
}

// queue_test_wait_connections waits until the server reports want live client
// connections. It is used to let connection threads unwind after the test
// closes its sockets, so queue_destroy never races them.
queue_test_wait_connections :: proc(q: ^queuelib.Queue, want: int, timeout := 2*time.Second) -> bool {
	deadline := time.now()
	for time.since(deadline) < timeout {
		if queuelib.queue_stats(q).connections == want {
			return true
		}
		time.sleep(5 * time.Millisecond)
	}
	return false
}

// queue_test_wait_ready waits until ready and reserved counts settle.
queue_test_wait_ready :: proc(q: ^queuelib.Queue, ready, reserved: int, timeout := 2*time.Second) -> bool {
	deadline := time.now()
	for time.since(deadline) < timeout {
		st := queuelib.queue_stats(q)
		if st.ready == ready && st.reserved == reserved {
			return true
		}
		time.sleep(5 * time.Millisecond)
	}
	return false
}

// ---------------------------------------------------------------- queue protocol

// test_queue_cmd writes one command line and reads one reply line.
test_queue_cmd :: proc(c: ^Test_Conn, cmd: string) -> (string, bool) {
	if !test_tcp_write_line(c, cmd) {
		return "", false
	}
	return test_tcp_read_line(c)
}

test_tcp_write_line :: proc(c: ^Test_Conn, line: string) -> bool {
	b := strings.builder_make(context.temp_allocator)
	strings.write_string(&b, line)
	strings.write_string(&b, "\r\n")
	return test_tcp_send(c, strings.to_string(b))
}

// test_queue_put sends put + body and parses "INSERTED <id>".
test_queue_put :: proc(c: ^Test_Conn, pri, delay, ttr: int, body: string) -> (id: u64, reply: string, ok: bool) {
	b := strings.builder_make(context.temp_allocator)
	fmt.sbprintf(&b, "put %d %d %d %d\r\n", pri, delay, ttr, len(body))
	strings.write_string(&b, body)
	strings.write_string(&b, "\r\n")
	if !test_tcp_send(c, strings.to_string(b)) {
		return 0, "", false
	}
	line, lok := test_tcp_read_line(c)
	if !lok {
		return 0, "", false
	}
	fields := strings.fields(line, context.temp_allocator)
	if len(fields) != 2 || fields[0] != "INSERTED" {
		return 0, line, false
	}
	n, nok := strconv.parse_u64(fields[1], 10)
	return n, line, nok
}

// test_queue_read_job reads "RESERVED|FOUND <id> <bytes>\r\n<body>\r\n".
test_queue_read_job :: proc(c: ^Test_Conn, verb: string) -> (id: u64, body: string, ok: bool) {
	line, lok := test_tcp_read_line(c)
	if !lok {
		return 0, "", false
	}
	fields := strings.fields(line, context.temp_allocator)
	if len(fields) != 3 || fields[0] != verb {
		return 0, line, false
	}
	i, iok := strconv.parse_u64(fields[1], 10)
	n, nok := strconv.parse_int(fields[2], 10)
	if !iok || !nok || n < 0 {
		return 0, line, false
	}
	payload, pok := test_tcp_read_exact(c, n)
	if !pok {
		return 0, line, false
	}
	tail, tok := test_tcp_read_exact(c, 2)
	if !tok || tail != "\r\n" {
		return 0, line, false
	}
	return i, payload, true
}

// test_queue_read_ok_yaml reads "OK <bytes>\r\n<yaml>\r\n".
test_queue_read_ok_yaml :: proc(c: ^Test_Conn) -> (string, bool) {
	line, lok := test_tcp_read_line(c)
	if !lok {
		return "", false
	}
	fields := strings.fields(line, context.temp_allocator)
	if len(fields) != 2 || fields[0] != "OK" {
		return "", false
	}
	n, nok := strconv.parse_int(fields[1], 10)
	if !nok || n < 0 {
		return "", false
	}
	payload, pok := test_tcp_read_exact(c, n)
	if !pok {
		return "", false
	}
	tail, tok := test_tcp_read_exact(c, 2)
	if !tok || tail != "\r\n" {
		return "", false
	}
	return payload, true
}

// test_gzip_compress produces a gzip (windowBits=31) stream so request bodies
// can be exercised without relying on external tools.
test_gzip_compress :: proc(src: []u8) -> ([]u8, bool) {
	strm: zlib.z_stream
	rc := zlib.deflateInit2(&strm, 1, zlib.DEFLATED, 31, 8, zlib.DEFAULT_STRATEGY)
	if rc != zlib.OK {
		return nil, false
	}
	defer zlib.deflateEnd(&strm)
	strm.next_in = raw_data(src)
	strm.avail_in = u32(len(src))
	out := make([dynamic]u8, 0, len(src) + 64)
	for {
		old := len(out)
		resize(&out, old + 4096)
		strm.next_out = raw_data(out[old:])
		strm.avail_out = 4096
		rc = zlib.deflate(&strm, zlib.FINISH)
		produced := 4096 - int(strm.avail_out)
		resize(&out, old + produced)
		if rc == zlib.STREAM_END {
			break
		}
		if rc != zlib.OK {
			delete(out)
			return nil, false
		}
	}
	return out[:], true
}
