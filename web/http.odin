package web

import "../common"
import "../logdb"
import "../netio"
import cachelib "../cache"
import queuelib "../queue"
import "core:fmt"
import "core:net"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:sys/posix"
import "core:thread"
import "core:time"

HTTP_MAX_HEADER :: 64 << 10
HTTP_MAX_BODY :: 256 << 20
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

read_chunked_body :: proc(c: ^netio.Conn, body: ^common.Buf, max_body: int) -> Http_Read_Error {
	line := common.buf_init(64)
	defer common.buf_destroy(&line)
	for {
		if !netio.conn_read_line(c, &line, HTTP_MAX_HEADER) {
			// conn_read_line only leaves an oversized buffer behind when it
			// rejects a line for length; otherwise the peer went away.
			if len(line.data) > HTTP_MAX_HEADER {
				return .Bad_Request
			}
			return .Drop
		}
		size_str := string(line.data[:])
		// strip chunk extensions
		if semi := strings.index_byte(size_str, ';'); semi >= 0 {
			size_str = size_str[:semi]
		}
		size_str = strings.trim_space(size_str)
		if len(size_str) == 0 {
			return .Bad_Request
		}
		size, ok := strconv.parse_int(size_str, 16)
		if !ok || size < 0 {
			return .Bad_Request
		}
		if size == 0 {
			// trailers
			for {
				if !netio.conn_read_line(c, &line, HTTP_MAX_HEADER) {
					if len(line.data) > HTTP_MAX_HEADER {
						return .Bad_Request
					}
					return .Drop
				}
				if len(line.data) == 0 {
					return .None
				}
			}
		}
		// The declared chunk size already decides whether the body can fit, so
		// answer 413 before consuming any of it.
		if size > max_body - len(body.data) {
			return .Body_Too_Large
		}
		if !netio.conn_read_exact(c, size, body) {
			return .Drop
		}
		// trailing CRLF
		cr, cok1 := netio.conn_read_byte(c)
		if !cok1 {
			return .Drop
		}
		lf, cok2 := netio.conn_read_byte(c)
		if !cok2 {
			return .Drop
		}
		if cr != '\r' || lf != '\n' {
			return .Bad_Request
		}
	}
}

http_read_request :: proc(c: ^netio.Conn, head, body: ^common.Buf, max_body: int) -> (req: Http_Request, err: Http_Read_Error) {
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

	if req.expect_continue {
		http_send_raw(c.sock, transmute([]u8)string("HTTP/1.1 100 Continue\r\n\r\n"))
	}
	if req.chunked {
		cerr := read_chunked_body(c, body, max_body)
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

http_send :: proc(sock: net.TCP_Socket, code: int, ctype: string, body: []u8, keep_alive: bool, extra: string = "") -> bool {
	hdr := common.buf_init(256)
	defer common.buf_destroy(&hdr)
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
	if !http_send_raw(sock, hdr.data[:]) {
		return false
	}
	if len(body) > 0 && !http_send_raw(sock, body) {
		return false
	}
	return true
}

// http_send_read_error answers a request that failed before dispatch. The
// response always carries Content-Length and Connection: close; the caller
// closes the socket right after it, without draining the unread request.
http_send_read_error :: proc(sock: net.TCP_Socket, err: Http_Read_Error) -> bool {
	switch err {
	case .Header_Too_Large:
		return http_send(sock, 431, "application/json", transmute([]u8)string(`{"error":"request header too large"}`), false)
	case .Body_Too_Large:
		return http_send(sock, 413, "application/json", transmute([]u8)string(`{"error":"request body too large"}`), false)
	case .Bad_Request:
		return http_send(sock, 400, "application/json", transmute([]u8)string(`{"error":"bad request"}`), false)
	case .None, .Drop:
	}
	return false
}

// ---------------------------------------------------------------- server

Server_Config :: struct {
	listen:        string,
	workers:       int,
	max_body:      int,
	stream_fields: []string,
}

default_server_config :: proc() -> Server_Config {
	return Server_Config{
		listen  = "0.0.0.0:9428",
		workers = 8,
		max_body = HTTP_MAX_BODY,
	}
}

Server :: struct {
	cfg:       Server_Config,
	store:     ^logdb.Store,
	cache:     ^cachelib.Cache,
	cache_file: string,
	jobq:      ^queuelib.Queue,
	listener:  net.TCP_Socket,
	// bound_port is the port the listener actually bound. It differs from the
	// configured port when listen uses ":0" for an ephemeral port.
	bound_port: int,
	mu:        sync.Mutex,
	cond:      sync.Cond,
	queue:     [dynamic]net.TCP_Socket,
	// active holds sockets currently owned by a worker. server_stop shuts them
	// down so workers blocked on an idle keep-alive connection return promptly.
	active:    [dynamic]net.TCP_Socket,
	stopping:  u32,
	workers:   [dynamic]^thread.Thread,
	start_ns:  i64,
	requests:  u64,
	errors:    u64,
	conns:     u64,
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

http_worker :: proc(data: rawptr) {
	srv := cast(^Server)data
	for {
		sync.mutex_lock(&srv.mu)
		for len(srv.queue) == 0 && srv.stopping == 0 {
			sync.cond_wait(&srv.cond, &srv.mu)
		}
		if srv.stopping != 0 {
			sync.mutex_unlock(&srv.mu)
			return
		}
		sock := pop(&srv.queue)
		sync.mutex_unlock(&srv.mu)
		if !conn_begin(srv, sock) {
			net.close(sock)
			continue
		}
		serve_connection(srv, sock)
		conn_end(srv, sock)
		net.close(sock)
		sync.atomic_add(&srv.conns, 1)
	}
}

// conn_begin registers a socket as active. It refuses when the server is
// already stopping, in which case the worker must close the socket itself.
conn_begin :: proc(srv: ^Server, sock: net.TCP_Socket) -> bool {
	sync.mutex_lock(&srv.mu)
	if sync.atomic_load(&srv.stopping) != 0 {
		sync.mutex_unlock(&srv.mu)
		return false
	}
	append(&srv.active, sock)
	sync.mutex_unlock(&srv.mu)
	return true
}

conn_end :: proc(srv: ^Server, sock: net.TCP_Socket) {
	sync.mutex_lock(&srv.mu)
	for s, i in srv.active {
		if s == sock {
			unordered_remove(&srv.active, i)
			break
		}
	}
	sync.mutex_unlock(&srv.mu)
}

serve_connection :: proc(srv: ^Server, sock: net.TCP_Socket) {
	_ = net.set_option(sock, .Receive_Timeout, HTTP_IDLE_TIMEOUT)
	_ = net.set_option(sock, .Send_Timeout, 30 * time.Second)
	_ = net.set_option(sock, .TCP_Nodelay, true)

	conn := netio.Conn{sock = sock}
	head := common.buf_init(4096)
	body := common.buf_init(4096)
	defer common.buf_destroy(&head)
	defer common.buf_destroy(&body)

	for {
		req, rerr := http_read_request(&conn, &head, &body, srv.cfg.max_body)
		if rerr != .None {
			if req.body_owned != nil {
				delete(req.body_owned)
			}
			http_send_read_error(sock, rerr)
			return
		}
		sync.atomic_add(&srv.requests, 1)
		arena: common.Arena
		resp := dispatch(srv, &req, &arena)
		sent := http_send(sock, resp.code, resp.ctype, resp.body.data[:], req.keep_alive && !resp.close_conn, resp.extra)
		common.buf_destroy(&resp.body)
		common.arena_destroy(&arena)
		if req.body_owned != nil {
			delete(req.body_owned)
		}
		if !sent || !req.keep_alive {
			return
		}
	}
}

server_run :: proc(srv: ^Server) -> bool {
	ep, ok := netio.parse_listen(srv.cfg.listen)
	if !ok {
		fmt.eprintf("oboe: invalid listen address %q\n", srv.cfg.listen)
		return false
	}
	sock, lerr := net.listen_tcp(ep, 4096)
	if lerr != nil {
		fmt.eprintf("oboe: cannot listen on %s: %v\n", srv.cfg.listen, lerr)
		return false
	}
	srv.listener = sock
	if bound, berr := net.bound_endpoint(sock); berr == nil {
		srv.bound_port = bound.port
	} else {
		srv.bound_port = ep.port
	}
	workers := srv.cfg.workers
	if workers < 1 {
		workers = 1
	}
	for _ in 0..<workers {
		w := thread.create_and_start_with_data(srv, http_worker, self_cleanup = false, name = "oboe-http")
		append(&srv.workers, w)
	}
	fmt.printf("oboe: listening on http://%s (workers=%d, data=%s)\n", srv.cfg.listen, workers, srv.store.cfg.data_path)
	for sync.atomic_load(&srv.stopping) == 0 {
		client, _, aerr := net.accept_tcp(sock)
		if aerr != nil {
			if sync.atomic_load(&srv.stopping) != 0 {
				break
			}
			time.sleep(5 * time.Millisecond)
			continue
		}
		sync.mutex_lock(&srv.mu)
		if sync.atomic_load(&srv.stopping) != 0 || len(srv.queue) >= 1 << 18 {
			sync.mutex_unlock(&srv.mu)
			net.close(client)
			continue
		}
		append(&srv.queue, client)
		sync.cond_signal(&srv.cond)
		sync.mutex_unlock(&srv.mu)
	}
	return true
}

// server_request_stop asks a running server to stop accepting connections.
// It is async-signal-safe: it only stores an atomic flag and closes the
// listening socket (close(2)). Worker threads are not joined and the connection
// queues are not touched; the thread that owns server_run must call
// server_stop afterwards to finish the teardown.
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

server_stop :: proc(srv: ^Server) {
	if srv == nil {
		return
	}
	server_request_stop(srv)
	sync.mutex_lock(&srv.mu)
	// shutdown (not close) fds owned by workers: the worker still closes them
	// after serve_connection returns, avoiding a double close on a reused fd.
	for sock in srv.active {
		net.shutdown(sock, .Both)
	}
	// Sockets queued but never handed to a worker are closed here; workers
	// observe stopping under the same mutex and will not pop them.
	for sock in srv.queue {
		net.close(sock)
	}
	clear(&srv.queue)
	sync.cond_broadcast(&srv.cond)
	sync.mutex_unlock(&srv.mu)
	for w in srv.workers {
		thread.join(w)
	}
	delete(srv.active)
	srv.active = nil
	delete(srv.workers)
	srv.workers = nil
}

// server_destroy stops a running server, closes connections that were still
// queued for a worker, and frees the Server itself. It is safe to call on a
// server that was never started.
server_destroy :: proc(srv: ^Server) {
	if srv == nil {
		return
	}
	if srv.listener != 0 {
		server_stop(srv)
	} else {
		delete(srv.active)
		srv.active = nil
		delete(srv.workers)
		srv.workers = nil
	}
	for sock in srv.queue {
		net.close(sock)
	}
	delete(srv.queue)
	srv.queue = nil
	free(srv)
}
