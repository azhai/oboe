// nbio_server.odin: the event-loop half of the HTTP server.
//
// The thread running server_run owns an nbio event loop and moves bytes only.
// A request is accumulated (head + body) until the framing says it is complete,
// then handed to the business worker pool, which runs the existing blocking
// parser over a pre-filled netio.Conn (netio.Conn.src). Responses are built by
// the worker and sent back through nbio, so a slow logdb query never blocks
// other connections.
package web

import "../common"
import "../netio"
import "core:fmt"
import "core:net"
import "core:nbio"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"

HTTP_RECV_CHUNK :: 16 << 10
HTTP_TICK_INTERVAL :: 50 * time.Millisecond
HTTP_SEND_TIMEOUT :: 30 * time.Second
// How long shutdown waits for workers to hand back their responses before
// closing whatever is left.
HTTP_DRAIN_TIMEOUT :: 5 * time.Second

CONTINUE_RESPONSE :: "HTTP/1.1 100 Continue\r\n\r\n"

Frame_Mode :: enum {
	Head,
	Content_Length,
	Chunked,
	Complete,
}

Http_Scan :: enum {
	Need_More,
	Complete,
	Failed,
	Interim,
}

Http_Conn :: struct {
	srv:     ^Server,
	loop:    ^nbio.Event_Loop,
	sock:    net.TCP_Socket,
	raw:     common.Buf,
	req_end: int,
	closed:  bool,
	closing: bool,

	recv_buf: [HTTP_RECV_CHUNK]u8,
	recv_op:  ^nbio.Operation,
	send_op:  ^nbio.Operation,
	close_op: ^nbio.Operation,

	head_done: bool,
	head_scan: int,
	head_end:  int,

	frame:       Frame_Mode,
	content_len: int,
	// framer is the shared chunked state machine from netio. Its positions are
	// relative to raw.data[head_end:], and the parser in the worker runs the
	// same function over the same bytes, so both halves classify an input
	// identically.
	framer: netio.Chunk_Framer,

	expect_continue: bool,
	continue_sent:   bool,
}

Http_Job :: struct {
	conn: ^Http_Conn,
}

// Http_Send owns the bytes of one response until the send callback runs.
Http_Send :: struct {
	conn:       ^Http_Conn,
	owned:      common.Buf,
	data:       []u8,
	keep_alive: bool,
	from_job:   bool,
	interim:    bool,
}

server_run :: proc(srv: ^Server) -> bool {
	if !nbio.FULLY_SUPPORTED {
		fmt.eprintf("oboe: core:nbio is not fully supported on this platform\n")
		return false
	}
	ep, ok := netio.parse_listen(srv.cfg.listen)
	if !ok {
		fmt.eprintf("oboe: invalid listen address %q\n", srv.cfg.listen)
		return false
	}
	if err := nbio.acquire_thread_event_loop(); err != nil {
		fmt.eprintf("oboe: cannot acquire nbio event loop: %v\n", err)
		return false
	}
	defer nbio.release_thread_event_loop()
	srv.loop = nbio.current_thread_event_loop()

	sock, lerr := nbio.listen_tcp(ep, 4096)
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
	// Published only after the workers exist so callers that wait for
	// bound_port never race with worker creation during shutdown.
	if bound, berr := net.bound_endpoint(sock); berr == nil {
		srv.bound_port = bound.port
	} else {
		srv.bound_port = ep.port
	}
	fmt.printf("oboe: listening on http://%s (workers=%d, data=%s)\n", srv.cfg.listen, workers, srv.store.cfg.data_path)

	if sync.atomic_load(&srv.stopping) == 0 && srv.listener != 0 {
		srv.accept_op = nbio.accept_poly(srv.listener, srv, on_accept, l = srv.loop)
	}
	for sync.atomic_load(&srv.stopping) == 0 {
		_ = nbio.tick(HTTP_TICK_INTERVAL)
	}
	server_loop_shutdown(srv)
	return true
}

// server_loop_shutdown stops the accept side, waits for in-flight business work
// to be handed back, then closes every remaining connection. Only the thread
// that owns the loop calls this.
server_loop_shutdown :: proc(srv: ^Server) {
	if srv.accept_op != nil {
		nbio.remove(srv.accept_op)
		srv.accept_op = nil
	}
	// Wake up blocked reads and sends so none of them outlives the loop.
	for c in srv.live {
		if c.sock != 0 {
			net.shutdown(c.sock, .Both)
		}
	}
	drain_start := time.tick_now()
	for sync.atomic_load(&srv.pending) > 0 && time.tick_since(drain_start) < HTTP_DRAIN_TIMEOUT {
		_ = nbio.tick(10 * time.Millisecond)
	}
	// Give the send/close callbacks one more turn before tearing down.
	for (len(srv.live) > 0 || srv.rejects > 0) && time.tick_since(drain_start) < HTTP_DRAIN_TIMEOUT {
		for c in srv.live {
			http_conn_close(c)
		}
		_ = nbio.tick(time.Millisecond)
	}
}

on_accept :: proc(op: ^nbio.Operation, srv: ^Server) {
	srv.accept_op = nil
	if sync.atomic_load(&srv.stopping) != 0 || srv.listener == 0 {
		return
	}
	if op.accept.err != nil {
		// A persistent accept error (e.g. EMFILE) must not spin the loop.
		srv.accept_op = nbio.timeout_poly(5 * time.Millisecond, srv, on_accept_retry, l = srv.loop)
		return
	}
	srv.accept_op = nbio.accept_poly(srv.listener, srv, on_accept, l = srv.loop)

	client := op.accept.client
	// The limit is enforced here, on the loop thread, before a connection
	// object or a business job exists. A rejected connection never becomes
	// "live", so it cannot be counted as active and cannot displace a job.
	if srv.cfg.max_conns > 0 && len(srv.live) >= srv.cfg.max_conns {
		sync.atomic_add(&srv.conns_rejected, 1)
		http_conn_reject(srv, client)
		return
	}

	c := new(Http_Conn)
	c.srv = srv
	c.loop = srv.loop
	c.sock = client
	c.raw = common.buf_init(4096)
	append(&srv.live, c)
	sync.atomic_add(&srv.active, 1)
	http_conn_start(c)
}

// Http_Reject owns a 503 reply until it is written and the socket is closed.
Http_Reject :: struct {
	srv:      ^Server,
	loop:     ^nbio.Event_Loop,
	sock:     net.TCP_Socket,
	owned:    common.Buf,
	send_op:  ^nbio.Operation,
	close_op: ^nbio.Operation,
}

// http_conn_reject answers a connection that exceeded -max-conns with an
// explicit 503 and closes it, so operators see throttling instead of a hang.
http_conn_reject :: proc(srv: ^Server, sock: net.TCP_Socket) {
	r := new(Http_Reject)
	r.srv = srv
	r.loop = srv.loop
	r.sock = sock
	srv.rejects += 1
	r.owned = http_build_response(
		503,
		"application/json",
		transmute([]u8)string(`{"error":"too many connections"}`),
		false,
		"Retry-After: 1\r\n",
	)
	r.send_op = nbio.send_poly(sock, [][]byte{r.owned.data[:]}, r, on_reject_sent, all = true, timeout = HTTP_SEND_TIMEOUT, l = srv.loop)
}

on_reject_sent :: proc(_: ^nbio.Operation, r: ^Http_Reject) {
	r.send_op = nil
	r.close_op = nbio.close_poly(r.sock, r, on_reject_closed, l = r.loop)
}

on_reject_closed :: proc(_: ^nbio.Operation, r: ^Http_Reject) {
	r.close_op = nil
	r.srv.rejects -= 1
	common.buf_destroy(&r.owned)
	free(r)
}

on_accept_retry :: proc(_: ^nbio.Operation, srv: ^Server) {
	srv.accept_op = nil
	if sync.atomic_load(&srv.stopping) != 0 || srv.listener == 0 {
		return
	}
	srv.accept_op = nbio.accept_poly(srv.listener, srv, on_accept, l = srv.loop)
}

// ---------------------------------------------------------------- connection

http_conn_start :: proc(c: ^Http_Conn) {
	switch http_scan(c) {
	case .Need_More:
		http_conn_recv(c)
	case .Complete:
		http_conn_dispatch(c)
	case .Interim:
		// The 100 Continue reply is in flight; its callback resumes the scan.
	case .Failed:
		// http_conn_fail already replied or closed.
	}
}

http_conn_resume :: proc(c: ^Http_Conn) {
	// Keep bytes that arrived after the previous request (HTTP pipelining).
	if c.req_end > 0 && c.req_end <= len(c.raw.data) {
		rest := len(c.raw.data) - c.req_end
		if rest > 0 {
			copy(c.raw.data[:rest], c.raw.data[c.req_end:])
			resize(&c.raw.data, rest)
		} else {
			clear(&c.raw.data)
		}
	}
	c.req_end = 0
	c.head_done = false
	c.head_scan = 0
	c.head_end = 0
	c.frame = .Head
	c.content_len = 0
	c.framer = {}
	c.expect_continue = false
	c.continue_sent = false
	http_conn_start(c)
}

http_conn_recv :: proc(c: ^Http_Conn) {
	c.recv_op = nbio.recv_poly(
		c.sock,
		[][]byte{c.recv_buf[:]},
		c,
		on_recv,
		timeout = HTTP_IDLE_TIMEOUT,
		l = c.loop,
	)
}

on_recv :: proc(op: ^nbio.Operation, c: ^Http_Conn) {
	c.recv_op = nil
	if op.recv.err != nil || op.recv.received == 0 {
		http_conn_close(c)
		return
	}
	common.buf_write(&c.raw, c.recv_buf[:op.recv.received])
	switch http_scan(c) {
	case .Need_More:
		http_conn_recv(c)
	case .Complete:
		http_conn_dispatch(c)
	case .Interim:
	case .Failed:
	}
}

// http_conn_send hands a response to the event loop. `owned` is freed in the
// send callback; for static bytes it is left empty.
http_conn_send :: proc(c: ^Http_Conn, owned: common.Buf, data: []u8, keep_alive, from_job, interim: bool) {
	st := new(Http_Send)
	st.conn = c
	st.owned = owned
	st.data = data
	st.keep_alive = keep_alive
	st.from_job = from_job
	st.interim = interim
	c.send_op = nbio.send_poly(
		c.sock,
		[][]byte{data},
		st,
		on_send,
		all = true,
		timeout = HTTP_SEND_TIMEOUT,
		l = c.loop,
	)
}

on_send :: proc(op: ^nbio.Operation, st: ^Http_Send) {
	c := st.conn
	ok := op.send.err == nil
	keep_alive := st.keep_alive
	interim := st.interim
	from_job := st.from_job
	owned := st.owned
	free(st)
	c.send_op = nil
	if owned.data != nil {
		common.buf_destroy(&owned)
	}
	if from_job {
		sync.atomic_sub(&c.srv.pending, 1)
	}
	stopping := sync.atomic_load(&c.srv.stopping) != 0

	if interim {
		if !ok || stopping {
			http_conn_close(c)
			return
		}
		switch http_scan_body(c) {
		case .Need_More:
			http_conn_recv(c)
		case .Complete:
			http_conn_dispatch(c)
		case .Interim, .Failed:
		}
		return
	}
	if ok && keep_alive && !stopping {
		http_conn_resume(c)
		return
	}
	http_conn_close(c)
}

http_conn_close :: proc(c: ^Http_Conn) {
	if !c.closed {
		c.closed = true
		if c.recv_op != nil {
			nbio.remove(c.recv_op)
			c.recv_op = nil
		}
	}
	// A send in flight keeps the socket (and this connection) alive until its
	// callback; that callback closes the connection.
	if c.send_op != nil || c.closing {
		return
	}
	c.closing = true
	if c.sock != 0 {
		sock := c.sock
		c.sock = 0
		c.close_op = nbio.close_poly(sock, c, on_conn_closed, l = c.loop)
		return
	}
	on_conn_closed(nil, c)
}

on_conn_closed :: proc(_: ^nbio.Operation, c: ^Http_Conn) {
	c.close_op = nil
	for live, i in c.srv.live {
		if live == c {
			unordered_remove(&c.srv.live, i)
			break
		}
	}
	common.buf_destroy(&c.raw)
	sync.atomic_sub(&c.srv.active, 1)
	sync.atomic_add(&c.srv.conns, 1)
	free(c)
}

// ---------------------------------------------------------------- framing

http_scan :: proc(c: ^Http_Conn) -> Http_Scan {
	raw := c.raw.data[:]
	if !c.head_done {
		start := c.head_scan
		if start > 3 {
			start -= 3
		} else {
			start = 0
		}
		idx := strings.index(string(raw[start:]), "\r\n\r\n")
		if idx < 0 {
			c.head_scan = len(raw)
			if len(raw) > HTTP_MAX_HEADER {
				http_conn_fail(c, .Header_Too_Large)
				return .Failed
			}
			return .Need_More
		}
		head_end := start + idx + 4
		if head_end > HTTP_MAX_HEADER {
			http_conn_fail(c, .Header_Too_Large)
			return .Failed
		}
		c.head_end = head_end
		c.head_done = true
		switch http_scan_framing(c) {
		case .Failed:
			return .Failed
		case .Interim:
			return .Interim
		case .Ok:
		}
	}
	return http_scan_body(c)
}

Frame_Scan :: enum {
	Ok,
	Interim,
	Failed,
}

// http_scan_framing parses just enough of the head to know where the body
// ends. Full validation (including the CL+TE conflict) stays in the parser the
// worker runs over these same bytes.
http_scan_framing :: proc(c: ^Http_Conn) -> Frame_Scan {
	s := string(c.raw.data[:c.head_end])
	pos := 0
	if _, ok := next_crlf(s, &pos); !ok {
		http_conn_fail(c, .Drop)
		return .Failed
	}
	cl_present := false
	te_present := false
	chunked := false
	cl := 0
	for {
		h, ok := next_crlf(s, &pos)
		if !ok || len(h) == 0 {
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
			cl_present = true
			n, nok := strconv.parse_int(value, 10)
			if !nok || n < 0 {
				http_conn_fail(c, .Bad_Request)
				return .Failed
			}
			cl = n
		case common.token_eq_ci(name, "transfer-encoding"):
			te_present = true
			chunked = common.contains_ci(value, "chunked")
		case common.token_eq_ci(name, "expect"):
			c.expect_continue = common.contains_ci(value, "100-continue")
		}
	}
	if cl_present && te_present {
		// The parser rejects this after the head; the head alone is complete.
		c.frame = .Complete
		return .Ok
	}
	if cl_present && cl > c.srv.cfg.max_body {
		http_conn_fail(c, .Body_Too_Large)
		return .Failed
	}
	if te_present && chunked {
		c.frame = .Chunked
		c.framer = {}
	} else if cl_present {
		c.frame = .Content_Length
		c.content_len = cl
	} else {
		c.frame = .Complete
	}
	if c.expect_continue && !c.continue_sent {
		c.continue_sent = true
		http_conn_send(c, {}, transmute([]u8)string(CONTINUE_RESPONSE), true, false, true)
		return .Interim
	}
	return .Ok
}

http_scan_body :: proc(c: ^Http_Conn) -> Http_Scan {
	raw := c.raw.data[:]
	switch c.frame {
	case .Complete:
		c.req_end = c.head_end
		return .Complete
	case .Content_Length:
		if len(raw) - c.head_end >= c.content_len {
			c.req_end = c.head_end + c.content_len
			return .Complete
		}
		return .Need_More
	case .Chunked:
		return http_scan_chunks(c)
	case .Head:
	}
	return .Need_More
}

// http_scan_chunks defers every chunked decision to netio.chunk_frame, the same
// function the request parser decodes with. The loop only needs the end offset;
// no second copy of the state machine lives here.
http_scan_chunks :: proc(c: ^Http_Conn) -> Http_Scan {
	switch netio.chunk_frame(&c.framer, c.raw.data[c.head_end:], c.srv.cfg.max_body, HTTP_MAX_HEADER, nil) {
	case .Complete:
		c.req_end = c.head_end + c.framer.end
		return .Complete
	case .Too_Large:
		http_conn_fail(c, .Body_Too_Large)
		return .Failed
	case .Bad_Format:
		http_conn_fail(c, .Bad_Request)
		return .Failed
	case .Need_More:
		return .Need_More
	}
	return .Need_More
}

http_conn_fail :: proc(c: ^Http_Conn, err: Http_Read_Error) {
	sync.atomic_add(&c.srv.errors, 1)
	if err == .Drop {
		http_conn_close(c)
		return
	}
	out := http_build_read_error(err)
	if out.data == nil {
		http_conn_close(c)
		return
	}
	http_conn_send(c, out, out.data[:], false, false, false)
}

// ---------------------------------------------------------------- dispatch

http_conn_dispatch :: proc(c: ^Http_Conn) {
	srv := c.srv
	job := new(Http_Job)
	job.conn = c
	sync.mutex_lock(&srv.mu)
	if sync.atomic_load(&srv.stopping) != 0 {
		sync.mutex_unlock(&srv.mu)
		free(job)
		http_conn_close(c)
		return
	}
	append(&srv.jobs, job)
	sync.atomic_add(&srv.pending, 1)
	sync.cond_signal(&srv.cond)
	sync.mutex_unlock(&srv.mu)
}

// http_worker is a business thread: it never touches the network. It parses a
// complete request from memory, runs the query, and hands the response bytes
// back to the owning event loop.
http_worker :: proc(data: rawptr) {
	srv := cast(^Server)data
	conn := new(netio.Conn)
	head := common.buf_init(4096)
	body := common.buf_init(4096)
	defer {
		free(conn)
		common.buf_destroy(&head)
		common.buf_destroy(&body)
	}
	for {
		sync.mutex_lock(&srv.mu)
		for len(srv.jobs) == 0 && sync.atomic_load(&srv.stopping) == 0 {
			sync.cond_wait(&srv.cond, &srv.mu)
		}
		if len(srv.jobs) == 0 {
			// Stopping and nothing queued: exit. Jobs queued before the stop
			// are still drained below.
			sync.mutex_unlock(&srv.mu)
			return
		}
		job := pop(&srv.jobs)
		sync.mutex_unlock(&srv.mu)
		http_job_run(srv, job, conn, &head, &body)
		free(job)
	}
}

http_job_run :: proc(srv: ^Server, job: ^Http_Job, conn: ^netio.Conn, head, body: ^common.Buf) {
	conn.src = job.conn.raw.data[:]
	conn.pos = 0
	conn.start = 0
	conn.end = 0
	common.buf_reset(head)
	common.buf_reset(body)

	req, rerr := http_read_request(conn, head, body, srv.cfg.max_body, job.conn.raw.data[:])
	if rerr != .None {
		if req.body_owned != nil {
			delete(req.body_owned)
		}
		http_job_fail(job.conn, rerr)
		return
	}
	sync.atomic_add(&srv.requests, 1)
	arena: common.Arena
	resp := dispatch(srv, &req, &arena)
	// Ingest overload requests a hard close, so the client cannot queue another
	// attempt on the same session while the memtable cap is full.
	keep_alive := req.keep_alive && !resp.close_conn
	out := http_build_response(resp.code, resp.ctype, resp.body.data[:], keep_alive, resp.extra)
	common.buf_destroy(&resp.body)
	common.arena_destroy(&arena)
	if req.body_owned != nil {
		delete(req.body_owned)
	}
	http_conn_send(job.conn, out, out.data[:], keep_alive, true, false)
}

http_job_fail :: proc(c: ^Http_Conn, err: Http_Read_Error) {
	sync.atomic_add(&c.srv.errors, 1)
	if err == .Drop {
		http_job_drop(c)
		return
	}
	out := http_build_read_error(err)
	if out.data == nil {
		http_job_drop(c)
		return
	}
	http_conn_send(c, out, out.data[:], false, true, false)
}

// http_job_drop closes a connection without a reply. The close must happen on
// the loop thread, so it goes through a zero-cost timeout operation.
http_job_drop :: proc(c: ^Http_Conn) {
	nbio.timeout_poly(time.Millisecond, c, on_job_drop, l = c.loop)
}

on_job_drop :: proc(_: ^nbio.Operation, c: ^Http_Conn) {
	sync.atomic_sub(&c.srv.pending, 1)
	http_conn_close(c)
}
