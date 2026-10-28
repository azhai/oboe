package oboe

import "core:fmt"
import "core:net"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"

HTTP_MAX_HEADER :: 64 << 10
HTTP_MAX_BODY :: 256 << 20
HTTP_IDLE_TIMEOUT :: 60 * time.Second

// ---------------------------------------------------------------- connection

Conn :: struct {
	sock:  net.TCP_Socket,
	buf:   [1 << 16]u8,
	start: int,
	end:   int,
}

conn_fill :: proc(c: ^Conn) -> bool {
	n, err := net.recv_tcp(c.sock, c.buf[:])
	if err != nil || n <= 0 {
		return false
	}
	c.start = 0
	c.end = n
	return true
}

conn_read_byte :: proc(c: ^Conn) -> (u8, bool) {
	if c.start >= c.end {
		if !conn_fill(c) {
			return 0, false
		}
	}
	b := c.buf[c.start]
	c.start += 1
	return b, true
}

conn_read_exact :: proc(c: ^Conn, n: int, out: ^Buf) -> bool {
	remaining := n
	for remaining > 0 {
		if c.start >= c.end {
			if !conn_fill(c) {
				return false
			}
		}
		avail := c.end - c.start
		take := min(avail, remaining)
		buf_write(out, c.buf[c.start:c.start+take])
		c.start += take
		remaining -= take
	}
	return true
}

conn_read_line :: proc(c: ^Conn, out: ^Buf) -> bool {
	buf_reset(out)
	for {
		b, ok := conn_read_byte(c)
		if !ok {
			return false
		}
		if b == '\n' {
			if len(out.data) > 0 && out.data[len(out.data)-1] == '\r' {
				resize(&out.data, len(out.data)-1)
			}
			return true
		}
		buf_byte(out, b)
		if len(out.data) > HTTP_MAX_HEADER {
			return false
		}
	}
}

// ---------------------------------------------------------------- requests

Http_Response :: struct {
	code:  int,
	ctype: string,
	extra: string,
	body:  Buf,
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

http_read_head :: proc(c: ^Conn, head: ^Buf) -> bool {
	buf_reset(head)
	for {
		b, ok := conn_read_byte(c)
		if !ok {
			return false
		}
		buf_byte(head, b)
		n := len(head.data)
		if n > HTTP_MAX_HEADER {
			return false
		}
		if n >= 4 && head.data[n-1] == '\n' && head.data[n-2] == '\r' && head.data[n-3] == '\n' && head.data[n-4] == '\r' {
			return true
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

read_chunked_body :: proc(c: ^Conn, body: ^Buf, max_body: int) -> bool {
	line := buf_init(64)
	defer buf_destroy(&line)
	for {
		if !conn_read_line(c, &line) {
			return false
		}
		size_str := string(line.data[:])
		// strip chunk extensions
		if semi := strings.index_byte(size_str, ';'); semi >= 0 {
			size_str = size_str[:semi]
		}
		size_str = strings.trim_space(size_str)
		if len(size_str) == 0 {
			return false
		}
		size, ok := strconv.parse_int(size_str, 16)
		if !ok || size < 0 {
			return false
		}
		if size == 0 {
			// trailers
			for {
				if !conn_read_line(c, &line) {
					return false
				}
				if len(line.data) == 0 {
					return true
				}
			}
		}
		if len(body.data) + size > max_body {
			return false
		}
		if !conn_read_exact(c, size, body) {
			return false
		}
		// trailing CRLF
		if _, cok1 := conn_read_byte(c); !cok1 {
			return false
		}
		if _, cok2 := conn_read_byte(c); !cok2 {
			return false
		}
	}
}

http_read_request :: proc(c: ^Conn, head, body: ^Buf, max_body: int) -> (req: Http_Request, ok: bool) {
	buf_reset(body)
	if !http_read_head(c, head) {
		return req, false
	}
	s := string(head.data[:])
	pos := 0
	line, lok := next_crlf(s, &pos)
	if !lok {
		return req, false
	}
	// request line: METHOD SP TARGET SP VERSION
	sp1 := strings.index_byte(line, ' ')
	if sp1 <= 0 {
		return req, false
	}
	rest := line[sp1+1:]
	sp2 := strings.index_byte(rest, ' ')
	if sp2 <= 0 {
		return req, false
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
	for {
		h, hok := next_crlf(s, &pos)
		if !hok {
			return req, false
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
		case token_eq_ci(name, "content-length"):
			n, nok := strconv.parse_int(value, 10)
			if !nok || n < 0 || n > max_body {
				return req, false
			}
			req.content_len = n
		case token_eq_ci(name, "transfer-encoding"):
			req.chunked = contains_ci(value, "chunked")
		case token_eq_ci(name, "content-encoding"):
			req.gzip = contains_ci(value, "gzip")
		case token_eq_ci(name, "expect"):
			req.expect_continue = contains_ci(value, "100-continue")
		case token_eq_ci(name, "connection"):
			connection_hdr = true
			connection_close = contains_ci(value, "close")
			connection_keep = contains_ci(value, "keep-alive")
		}
	}
	if connection_hdr {
		req.keep_alive = !connection_close
	} else if req.version != "HTTP/1.1" {
		req.keep_alive = connection_keep
	}

	if req.expect_continue {
		http_send_raw(c.sock, transmute([]u8)string("HTTP/1.1 100 Continue\r\n\r\n"))
	}
	if req.chunked {
		if !read_chunked_body(c, body, max_body) {
			return req, false
		}
	} else if req.content_len > 0 {
		if !conn_read_exact(c, req.content_len, body) {
			return req, false
		}
	}
	req.body = body.data[:]
	if req.gzip && len(req.body) > 0 {
		raw, dok := inflate_any(req.body, max_body)
		if !dok {
			return req, false
		}
		req.body_owned = raw
		req.body = raw
	}
	return req, true
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
	hdr := buf_init(256)
	defer buf_destroy(&hdr)
	ct := ctype
	if ct == "" {
		ct = "text/plain; charset=utf-8"
	}
	buf_addf(&hdr, "HTTP/1.1 %d %s\r\n", code, status_text(code))
	buf_addf(&hdr, "Content-Type: %s\r\n", ct)
	buf_addf(&hdr, "Content-Length: %d\r\n", len(body))
	buf_addf(&hdr, "Connection: %s\r\n", keep_alive ? "keep-alive" : "close")
	buf_string(&hdr, CORS_HEADERS)
	buf_string(&hdr, "Server: oboe\r\n")
	if len(extra) > 0 {
		buf_string(&hdr, extra)
	}
	buf_string(&hdr, "\r\n")
	if !http_send_raw(sock, hdr.data[:]) {
		return false
	}
	if len(body) > 0 && !http_send_raw(sock, body) {
		return false
	}
	return true
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
	store:     ^Store,
	cache:     ^Cache,
	cache_file: string,
	jobq:      ^Queue,
	listener:  net.TCP_Socket,
	mu:        sync.Mutex,
	cond:      sync.Cond,
	queue:     [dynamic]net.TCP_Socket,
	stopping:  u32,
	workers:   [dynamic]^thread.Thread,
	start_ns:  i64,
	requests:  u64,
	errors:    u64,
	conns:     u64,
}

parse_listen :: proc(addr: string) -> (ep: net.Endpoint, ok: bool) {
	colon := strings.last_index_byte(addr, ':')
	host := ""
	port_str := addr
	if colon >= 0 {
		host = addr[:colon]
		port_str = addr[colon+1:]
	}
	port, pok := strconv.parse_int(port_str, 10)
	if !pok || port <= 0 || port > 65535 {
		return ep, false
	}
	ep.port = port
	if len(host) == 0 || host == "0.0.0.0" || host == "*" {
		ep.address = net.IP4_Address{0, 0, 0, 0}
		return ep, true
	}
	if host == "localhost" {
		ep.address = net.IP4_Address{127, 0, 0, 1}
		return ep, true
	}
	if ip4, iok := net.parse_ip4_address(host); iok {
		ep.address = ip4
		return ep, true
	}
	if ip6, iok := net.parse_ip6_address(host); iok {
		ep.address = ip6
		return ep, true
	}
	return ep, false
}

server_init :: proc(cfg: Server_Config, store: ^Store, cache: ^Cache = nil, queue: ^Queue = nil) -> ^Server {
	srv := new(Server)
	srv.cfg = cfg
	srv.store = store
	srv.cache = cache
	srv.jobq = queue
	srv.start_ns = now_ns()
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
		serve_connection(srv, sock)
		net.close(sock)
		sync.atomic_add(&srv.conns, 1)
	}
}

serve_connection :: proc(srv: ^Server, sock: net.TCP_Socket) {
	_ = net.set_option(sock, .Receive_Timeout, HTTP_IDLE_TIMEOUT)
	_ = net.set_option(sock, .Send_Timeout, 30 * time.Second)
	_ = net.set_option(sock, .TCP_Nodelay, true)

	conn := Conn{sock = sock}
	head := buf_init(4096)
	body := buf_init(4096)
	defer buf_destroy(&head)
	defer buf_destroy(&body)

	for {
		req, ok := http_read_request(&conn, &head, &body, srv.cfg.max_body)
		if !ok {
			if req.body_owned != nil {
				delete(req.body_owned)
			}
			return
		}
		sync.atomic_add(&srv.requests, 1)
		arena: Arena
		resp := dispatch(srv, &req, &arena)
		sent := http_send(sock, resp.code, resp.ctype, resp.body.data[:], req.keep_alive, resp.extra)
		buf_destroy(&resp.body)
		arena_destroy(&arena)
		if req.body_owned != nil {
			delete(req.body_owned)
		}
		if !sent || !req.keep_alive {
			return
		}
	}
}

server_run :: proc(srv: ^Server) -> bool {
	ep, ok := parse_listen(srv.cfg.listen)
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
			if srv.stopping != 0 {
				break
			}
			time.sleep(5 * time.Millisecond)
			continue
		}
		sync.mutex_lock(&srv.mu)
		if srv.stopping != 0 || len(srv.queue) >= 1 << 18 {
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

server_stop :: proc(srv: ^Server) {
	sync.atomic_store(&srv.stopping, 1)
	if srv.listener != 0 {
		net.close(srv.listener)
		srv.listener = 0
	}
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
// queued for a worker, and frees the Server itself. It is safe to call on a
// server that was never started.
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
	for sock in srv.queue {
		net.close(sock)
	}
	delete(srv.queue)
	srv.queue = nil
	free(srv)
}
