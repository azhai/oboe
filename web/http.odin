package web

import "../common"
import "../logdb"
import "../netio"
import cachelib "../cache"
import queuelib "../queue"
import "core:fmt"
import "core:net"
import "core:nbio"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:sys/posix"
import "core:thread"
import "core:time"

HTTP_MAX_HEADER :: 64 << 10
HTTP_MAX_BODY :: 256 << 20
// DEFAULT_MAX_CONNS bounds the connections the event loop keeps open at once.
// Each live connection owns a 16 KiB receive buffer plus its request bytes, so
// 4096 × 16 KiB ≈ 64 MiB of receive buffers before any body is buffered.
DEFAULT_MAX_CONNS :: 4096
HTTP_IDLE_TIMEOUT :: 60 * time.Second

// Http_Read_Error tells the caller why a request could not be read. Only the
// limit and framing cases map to an HTTP status; .Drop keeps the old behaviour
// of closing the connection without a response for I/O errors and malformed
// request lines.
Http_Read_Error :: enum {
	None,
	Drop,
	Bad_Request,       // 400
	Header_Too_Large,  // 431
	Body_Too_Large,    // 413
}

// ---------------------------------------------------------------- requests

Http_Response :: struct {
	code:  int,
	ctype: string,
	extra: string,
	body:  common.Buf,
	// close_conn forces `Connection: close` even when the client asked for
	// keep-alive. Ingest overload uses it so a stalled request does not tie up a
	// session that the client is about to retry on anyway.
	close_conn: bool,
}

Http_Request :: struct {
	method:      string,
	path:        string,
	query:       string,
	version:     string,
	keep_alive:  bool,
	content_len: int,
	chunked:     bool,
	gzip:        bool,
	expect_continue: bool,
	body:        []u8,
	body_owned:  []u8,
}

http_read_head :: proc(c: ^netio.Conn, head: ^common.Buf) -> Http_Read_Error {
	common.buf_reset(head)
	for {
		b, ok := netio.conn_read_byte(c)
		if !ok {
			return .Drop
		}
		common.buf_byte(head, b)
		n := len(head.data)
		if n > HTTP_MAX_HEADER {
			return .Header_Too_Large
		}
		if n >= 4 && head.data[n-1] == '\n' && head.data[n-2] == '\r' && head.data[n-3] == '\n' && head.data[n-4] == '\r' {
			return .None
		}
	}
}

next_crlf :: proc(s: string, pos: ^int) -> (string, bool) {
	start := pos^
	for i := start; i + 1 < len(s); i += 1 {
		if s[i] == '\r' && s[i+1] == '\n' {
			pos^ = i + 2
			return s[start:i], true
		}
	}
	return "", false
}

// read_chunked_body frames and decodes the chunked body with netio.chunk_frame,
// the same state machine the event loop uses to decide completeness. `src` is
// the body slice (starting right after the head) when the caller already has
// all bytes; nil falls back to reading bytes off the connection.
read_chunked_body :: proc(c: ^netio.Conn, body: ^common.Buf, max_body: int, src: []u8) -> Http_Read_Error {
	fr: netio.Chunk_Framer
	if src != nil {
		switch netio.chunk_frame(&fr, src, max_body, HTTP_MAX_HEADER, body) {
		case .Complete:
			return .None
		case .Too_Large:
			return .Body_Too_Large
		case .Bad_Format:
			return .Bad_Request
		case .Need_More:
			return .Drop
		}
	}
	scratch := common.buf_init(4096)
	defer common.buf_destroy(&scratch)
	for {
		switch netio.chunk_frame(&fr, scratch.data[:], max_body, HTTP_MAX_HEADER, body) {
		case .Complete:
			return .None
		case .Too_Large:
			return .Body_Too_Large
		case .Bad_Format:
			return .Bad_Request
		case .Need_More:
			b, ok := netio.conn_read_byte(c)
			if !ok {
				return .Drop
			}
			common.buf_byte(&scratch, b)
		}
	}
}

http_read_request :: proc(c: ^netio.Conn, head, body: ^common.Buf, max_body: int, raw: []u8 = nil) -> (req: Http_Request, err: Http_Read_Error) {
	common.buf_reset(body)
	err = http_read_head(c, head)
	if err != .None {
		return
	}
	s := string(head.data[:])
	pos := 0
	line, lok := next_crlf(s, &pos)
	if !lok {
		return req, .Drop
	}
	// request line: METHOD SP TARGET SP VERSION
	sp1 := strings.index_byte(line, ' ')
	if sp1 <= 0 {
		return req, .Drop
	}
	rest := line[sp1+1:]
	sp2 := strings.index_byte(rest, ' ')
	if sp2 <= 0 {
		return req, .Drop
	}
	req.method = line[:sp1]
	target := rest[:sp2]
	req.version = rest[sp2+1:]
	if qmark := strings.index_byte(target, '?'); qmark >= 0 {
		req.path = target[:qmark]
		req.query = target[qmark+1:]
	} else {
		req.path = target
	}
	// default framing
	req.keep_alive = req.version == "HTTP/1.1"
	connection_close := false
	connection_keep := false
	connection_hdr := false
	content_len_hdr := false
	transfer_hdr := false
	for {
		h, hok := next_crlf(s, &pos)
		if !hok {
			return req, .Drop
		}
		if len(h) == 0 {
			break
		}
		colon := strings.index_byte(h, ':')
		if colon <= 0 {
			continue
		}
		name := strings.trim_space(h[:colon])
		value := strings.trim_space(h[colon+1:])
		switch {
		case common.token_eq_ci(name, "content-length"):
			content_len_hdr = true
			n, nok := strconv.parse_int(value, 10)
			if !nok || n < 0 {
				return req, .Bad_Request
			}
			req.content_len = n
		case common.token_eq_ci(name, "transfer-encoding"):
			transfer_hdr = true
			req.chunked = common.contains_ci(value, "chunked")
		case common.token_eq_ci(name, "content-encoding"):
			req.gzip = common.contains_ci(value, "gzip")
		case common.token_eq_ci(name, "expect"):
			req.expect_continue = common.contains_ci(value, "100-continue")
		case common.token_eq_ci(name, "connection"):
			connection_hdr = true
			connection_close = common.contains_ci(value, "close")
			connection_keep = common.contains_ci(value, "keep-alive")
		}
	}
	if connection_hdr {
		req.keep_alive = !connection_close
	} else if req.version != "HTTP/1.1" {
		req.keep_alive = connection_keep
	}

	// Both framings are validated after the whole head is parsed so the
	// smuggling case wins over the size check regardless of header order.
	if content_len_hdr && transfer_hdr {
		return req, .Bad_Request
	}
	if content_len_hdr && req.content_len > max_body {
		return req, .Body_Too_Large
	}

	if req.expect_continue && c.src == nil {
		http_send_raw(c.sock, transmute([]u8)string("HTTP/1.1 100 Continue\r\n\r\n"))
	}
	if req.chunked {
		// head.data holds exactly the head (including the final CRLFCRLF), so
		// its length is where the body starts inside `raw`.
		body_src: []u8
		if raw != nil && len(raw) >= len(head.data) {
			body_src = raw[len(head.data):]
		}
		cerr := read_chunked_body(c, body, max_body, body_src)
		if cerr != .None {
			return req, cerr
		}
	} else if req.content_len > 0 {
		if !netio.conn_read_exact(c, req.content_len, body) {
			return req, .Drop
		}
	}
	req.body = body.data[:]
	if req.gzip && len(req.body) > 0 {
		raw, dok := common.inflate_any(req.body, max_body)
		if !dok {
			return req, .Drop
		}
		req.body_owned = raw
		req.body = raw
	}
	return req, .None
}

// ---------------------------------------------------------------- response

status_text :: proc(code: int) -> string {
	switch code {
	case 200:
		return "OK"
	case 201:
		return "Created"
	case 204:
		return "No Content"
	case 400:
		return "Bad Request"
	case 401:
		return "Unauthorized"
	case 404:
		return "Not Found"
	case 405:
		return "Method Not Allowed"
	case 413:
		return "Payload Too Large"
	case 431:
		return "Request Header Fields Too Large"
	case 429:
		return "Too Many Requests"
	case 500:
		return "Internal Server Error"
	case 503:
		return "Service Unavailable"
	}
	return "Unknown"
}

http_send_raw :: proc(sock: net.TCP_Socket, data: []u8) -> bool {
	remaining := data
	for len(remaining) > 0 {
		n, err := net.send_tcp(sock, remaining)
		if err != nil || n <= 0 {
			return false
		}
		remaining = remaining[n:]
	}
	return true
}

CORS_HEADERS :: "Access-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: *\r\nAccess-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n"

// http_build_response renders a complete response (head and body) into one
// buffer. The nbio server writes it from the event loop; http_send is the
// blocking variant used by the parser's error replies.
http_build_response :: proc(code: int, ctype: string, body: []u8, keep_alive: bool, extra: string = "") -> common.Buf {
	hdr := common.buf_init(256 + len(body))
	ct := ctype
	if ct == "" {
		ct = "text/plain; charset=utf-8"
	}
	common.buf_addf(&hdr, "HTTP/1.1 %d %s\r\n", code, status_text(code))
	common.buf_addf(&hdr, "Content-Type: %s\r\n", ct)
	common.buf_addf(&hdr, "Content-Length: %d\r\n", len(body))
	common.buf_addf(&hdr, "Connection: %s\r\n", keep_alive ? "keep-alive" : "close")
	common.buf_string(&hdr, CORS_HEADERS)
	common.buf_string(&hdr, "Server: oboe\r\n")
	if len(extra) > 0 {
		common.buf_string(&hdr, extra)
	}
	common.buf_string(&hdr, "\r\n")
	if len(body) > 0 {
		common.buf_write(&hdr, body)
	}
	return hdr
}

http_send :: proc(sock: net.TCP_Socket, code: int, ctype: string, body: []u8, keep_alive: bool, extra: string = "") -> bool {
	hdr := http_build_response(code, ctype, body, keep_alive, extra)
	defer common.buf_destroy(&hdr)
	return http_send_raw(sock, hdr.data[:])
}

// http_build_read_error renders the response for a request that failed before
// dispatch. The response always carries Content-Length and Connection: close;
// the caller closes the connection right after it, without draining the
// unread request.
http_build_read_error :: proc(err: Http_Read_Error) -> common.Buf {
	switch err {
	case .Header_Too_Large:
		return http_build_response(431, "application/json", transmute([]u8)string(`{"error":"request header too large"}`), false)
	case .Body_Too_Large:
		return http_build_response(413, "application/json", transmute([]u8)string(`{"error":"request body too large"}`), false)
	case .Bad_Request:
		return http_build_response(400, "application/json", transmute([]u8)string(`{"error":"bad request"}`), false)
	case .None, .Drop:
	}
	return {}
}

http_send_read_error :: proc(sock: net.TCP_Socket, err: Http_Read_Error) -> bool {
	hdr := http_build_read_error(err)
	if hdr.data == nil {
		return false
	}
	defer common.buf_destroy(&hdr)
	return http_send_raw(sock, hdr.data[:])
}

// ---------------------------------------------------------------- server

Server_Config :: struct {
	listen:        string,
	workers:       int,
	// max_conns caps simultaneously open connections. 0 means unlimited.
	max_conns:     int,
	max_body:      int,
	stream_fields: []string,
}

default_server_config :: proc() -> Server_Config {
	return Server_Config{
		listen  = "0.0.0.0:9428",
		workers = 8,
		max_conns = DEFAULT_MAX_CONNS,
		max_body = HTTP_MAX_BODY,
	}
}

Server :: struct {
	cfg:        Server_Config,
	store:      ^logdb.Store,
	cache:      ^cachelib.Cache,
	cache_file: string,
	jobq:       ^queuelib.Queue,
	listener:   net.TCP_Socket,
	// bound_port is the port the listener actually bound. It differs from the
	// configured port when listen uses ":0" for an ephemeral port.
	bound_port: int,
	// loop, live and accept_op are owned by the thread that runs server_run.
	// Connections are no longer tied to worker threads: the event loop moves
	// bytes and only complete requests are handed to the business workers.
	loop:       ^nbio.Event_Loop,
	live:       [dynamic]^Http_Conn,
	accept_op:  ^nbio.Operation,
	// mu guards the business-work queue shared with the worker pool.
	mu:         sync.Mutex,
	cond:       sync.Cond,
	jobs:       [dynamic]^Http_Job,
	workers:    [dynamic]^thread.Thread,
	// pending counts jobs handed to a worker and not yet answered. The event
	// loop drains it before tearing the loop down.
	pending:    u32,
	stopping:   u32,
	start_ns:   i64,
	requests:   u64,
	errors:     u64,
	conns:      u64,
	// active is the gauge of open connections owned by the event loop; rejected
	// connections are never counted. conns_rejected grows by one per 503.
	active:         u32,
	conns_rejected: u64,
	// rejects counts 503 replies still in flight; shutdown ticks until they are
	// closed so no send/close operation outlives the loop.
	rejects: u32,
}

server_init :: proc(cfg: Server_Config, store: ^logdb.Store, cache: ^cachelib.Cache = nil, queue: ^queuelib.Queue = nil) -> ^Server {
	srv := new(Server)
	srv.cfg = cfg
	srv.store = store
	srv.cache = cache
	srv.jobq = queue
	srv.start_ns = common.now_ns()
	return srv
}

// server_request_stop asks a running server to stop accepting connections.
// It is async-signal-safe: it only stores an atomic flag and closes the
// listening socket (close(2)). Business workers and the event loop are not
// touched; the thread that owns server_run finishes the teardown itself, and
// the caller joins the workers through server_stop.
server_request_stop :: proc "contextless" (srv: ^Server) {
	if srv == nil {
		return
	}
	sync.atomic_store(&srv.stopping, 1)
	if srv.listener != 0 {
		posix.close(cast(posix.FD) srv.listener)
		srv.listener = 0
	}
}

// server_stop stops a running server and joins the business workers. It is
// safe to call from a thread that is not running the event loop. The live
// connections are closed by server_run itself (see nbio_server.odin), so this
// function never touches loop-owned state.
server_stop :: proc(srv: ^Server) {
	if srv == nil {
		return
	}
	server_request_stop(srv)
	sync.mutex_lock(&srv.mu)
	sync.cond_broadcast(&srv.cond)
	sync.mutex_unlock(&srv.mu)
	for w in srv.workers {
		thread.join(w)
	}
	delete(srv.workers)
	srv.workers = nil
}

// server_destroy stops a running server, closes connections that were still
// live, and frees the Server itself. It is safe to call on a server that was
// never started.
server_destroy :: proc(srv: ^Server) {
	if srv == nil {
		return
	}
	if srv.listener != 0 {
		server_stop(srv)
	} else {
		delete(srv.workers)
		srv.workers = nil
	}
	delete(srv.live)
	srv.live = nil
	delete(srv.jobs)
	srv.jobs = nil
	free(srv)
}
