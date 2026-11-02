package queue

import "../common"
import "../netio"
import "core:fmt"
import "core:net"
import "core:os"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"

// beanstalkd wire protocol subset. One thread per client connection; multiple
// workers can block in reserve without holding up others.

Queue_Server :: struct {
	q:        ^Queue,
	listener: net.TCP_Socket,
	// bound_port is the port the listener actually bound, which differs from
	// cfg.listen when it uses ":0" for an ephemeral port.
	bound_port: int,
	acceptor: ^thread.Thread,
}

Queue_Conn_Args :: struct {
	q:    ^Queue,
	id:   u64,
	sock: net.TCP_Socket,
}

QConn :: struct {
	q:        ^Queue,
	id:       u64,
	sock:     net.TCP_Socket,
	conn:     netio.Conn,
	body:     common.Buf,
	reply:    common.Buf,
	used:     string,
	watched:  [dynamic]string,
	reserved: [dynamic]u64,
}

queue_server_start :: proc(q: ^Queue) -> ^Queue_Server {
	ep, ok := netio.parse_listen(q.cfg.listen)
	if !ok {
		fmt.eprintf("oboe: invalid queue listen address %q\n", q.cfg.listen)
		return nil
	}
	sock, lerr := net.listen_tcp(ep, 1024)
	if lerr != nil {
		fmt.eprintf("oboe: cannot listen for queue on %s: %v\n", q.cfg.listen, lerr)
		return nil
	}
	qs := new(Queue_Server)
	qs.q = q
	qs.listener = sock
	if bound, berr := net.bound_endpoint(sock); berr == nil {
		qs.bound_port = bound.port
	} else {
		qs.bound_port = ep.port
	}
	qs.acceptor = thread.create_and_start_with_data(qs, queue_accept_loop, self_cleanup = false, name = "oboe-queue")
	fmt.printf("oboe: queue listening on tcp://%s (beanstalk protocol)\n", q.cfg.listen)
	return qs
}

queue_server_stop :: proc(qs: ^Queue_Server) {
	if qs == nil {
		return
	}
	sync.atomic_store(&qs.q.stopping, 1)
	net.close(qs.listener)
	if qs.acceptor != nil {
		thread.join(qs.acceptor)
		thread.destroy(qs.acceptor)
		qs.acceptor = nil
	}
	free(qs)
}

queue_accept_loop :: proc(data: rawptr) {
	qs := cast(^Queue_Server)data
	q := qs.q
	for sync.atomic_load(&q.stopping) == 0 {
		client, _, aerr := net.accept_tcp(qs.listener)
		if aerr != nil {
			if sync.atomic_load(&q.stopping) != 0 {
				break
			}
			time.sleep(5 * time.Millisecond)
			continue
		}
		if !queue_conn_opened(q) {
			net.close(client)
			continue
		}
		args := new(Queue_Conn_Args)
		args^ = Queue_Conn_Args{q = q, id = queue_new_conn(q), sock = client}
		thread.create_and_start_with_data(args, queue_conn_thread, self_cleanup = true, name = "oboe-queue-conn")
	}
}

queue_conn_thread :: proc(data: rawptr) {
	args := cast(^Queue_Conn_Args)data
	q := args.q
	id := args.id
	sock := args.sock
	free(args)

	_ = net.set_option(sock, .Send_Timeout, 30 * time.Second)
	_ = net.set_option(sock, .TCP_Nodelay, true)

	c := QConn{
		q    = q,
		id   = id,
		sock = sock,
		body = common.buf_init(4096),
		reply = common.buf_init(1024),
		used = strings.clone(QUEUE_DEFAULT_TUBE),
	}
	c.conn = netio.Conn{sock = sock}
	append(&c.watched, strings.clone(QUEUE_DEFAULT_TUBE))
	queue_use(q, "", c.used)
	queue_watch(q, c.used)

	line := common.buf_init(4096)
	for {
		if !netio.conn_read_line(&c.conn, &line, 64 << 10) {
			break
		}
		if len(line.data) == 0 {
			continue
		}
		keep := queue_handle(&c, string(line.data[:]))
		free_all(context.temp_allocator)
		if !keep {
			break
		}
	}

	queue_conn_closed(q, c.used, c.watched[:], c.reserved[:])
	for name in c.watched {
		delete(name)
	}
	delete(c.watched)
	delete(c.reserved)
	delete(c.used)
	common.buf_destroy(&line)
	common.buf_destroy(&c.body)
	common.buf_destroy(&c.reply)
	net.close(sock)
}

queue_send_bytes :: proc(sock: net.TCP_Socket, data: []u8) -> bool {
	sent := 0
	for sent < len(data) {
		n, err := net.send_tcp(sock, data[sent:])
		if err != nil || n <= 0 {
			return false
		}
		sent += n
	}
	return true
}

queue_send_str :: #force_inline proc(sock: net.TCP_Socket, s: string) -> bool {
	return queue_send_bytes(sock, transmute([]u8)s)
}

queue_send_buf :: proc(c: ^QConn, b: ^common.Buf) -> bool {
	return queue_send_bytes(c.sock, b.data[:])
}

queue_send_job :: proc(c: ^QConn, verb: string, id: u64, body: []u8) -> bool {
	hdr := common.buf_init(64)
	defer common.buf_destroy(&hdr)
	common.buf_addf(&hdr, "%s %d %d\r\n", verb, id, len(body))
	return queue_send_bytes(c.sock, hdr.data[:]) &&
	       queue_send_bytes(c.sock, body) &&
	       queue_send_str(c.sock, "\r\n")
}

queue_parse_u64 :: proc(s: string) -> (u64, bool) {
	return strconv.parse_u64(s, 10)
}

queue_parse_i64 :: proc(s: string) -> (i64, bool) {
	return strconv.parse_i64(s, 10)
}

queue_secs_ns :: proc(secs: i64) -> i64 {
	if secs <= 0 {
		return 0
	}
	if secs > 9_000_000_000 {
		return i64(0x7fffffffffffffff)
	}
	return secs * 1_000_000_000
}

queue_crlf :: proc(c: ^QConn) -> bool {
	a, ok1 := netio.conn_read_byte(&c.conn)
	b, ok2 := netio.conn_read_byte(&c.conn)
	return ok1 && ok2 && a == '\r' && b == '\n'
}

qconn_forget :: proc(c: ^QConn, id: u64) {
	for v, i in c.reserved {
		if v == id {
			for k in i..<len(c.reserved)-1 {
				c.reserved[k] = c.reserved[k+1]
			}
			resize(&c.reserved, len(c.reserved)-1)
			return
		}
	}
}

qconn_watches :: proc(c: ^QConn, name: string) -> bool {
	for w in c.watched {
		if w == name {
			return true
		}
	}
	return false
}

queue_state_name :: proc(state: Job_State) -> string {
	switch state {
	case .READY:
		return "ready"
	case .RESERVED:
		return "reserved"
	case .DELAYED:
		return "delayed"
	case .BURIED:
		return "buried"
	}
	return "unknown"
}

queue_global_yaml :: proc(q: ^Queue, out: ^common.Buf) {
	st := queue_stats(q)
	up := (common.now_ns() - q.created_ns) / 1_000_000_000
	common.buf_addf(out, "current-jobs-urgent: %d\n", st.urgent)
	common.buf_addf(out, "current-jobs-ready: %d\n", st.ready)
	common.buf_addf(out, "current-jobs-reserved: %d\n", st.reserved)
	common.buf_addf(out, "current-jobs-delayed: %d\n", st.delayed)
	common.buf_addf(out, "current-jobs-buried: %d\n", st.buried)
	common.buf_addf(out, "current-connections: %d\n", st.connections)
	common.buf_addf(out, "current-jobs: %d\n", st.current_jobs)
	common.buf_addf(out, "total-jobs: %d\n", st.total_jobs)
	common.buf_addf(out, "current-using: %d\n", st.using_n)
	common.buf_addf(out, "current-watching: %d\n", st.watching)
	common.buf_addf(out, "current-waiting: %d\n", st.waiting)
	common.buf_addf(out, "cmd-put: %d\n", st.cmd_put)
	common.buf_addf(out, "cmd-reserve: %d\n", st.cmd_reserve)
	common.buf_addf(out, "cmd-delete: %d\n", st.cmd_delete)
	common.buf_addf(out, "cmd-release: %d\n", st.cmd_release)
	common.buf_addf(out, "cmd-bury: %d\n", st.cmd_bury)
	common.buf_addf(out, "cmd-kick: %d\n", st.cmd_kick)
	common.buf_addf(out, "cmd-touch: %d\n", st.cmd_touch)
	common.buf_addf(out, "job-timeouts: %d\n", st.timeouts)
	common.buf_addf(out, "total-connections: %d\n", st.total_connections)
	common.buf_addf(out, "max-job-size: %d\n", q.cfg.max_job_bytes)
	common.buf_addf(out, "uptime: %d\n", up)
	common.buf_addf(out, "pid: %d\n", os.get_pid())
	common.buf_addf(out, "version: \"1.13\"\n")
}

queue_tube_yaml :: proc(st: Tube_Stats, out: ^common.Buf) {
	common.buf_addf(out, "---\nname: %s\n", st.name)
	common.buf_addf(out, "current-jobs-urgent: %d\n", st.urgent)
	common.buf_addf(out, "current-jobs-ready: %d\n", st.ready)
	common.buf_addf(out, "current-jobs-reserved: %d\n", st.reserved)
	common.buf_addf(out, "current-jobs-delayed: %d\n", st.delayed)
	common.buf_addf(out, "current-jobs-buried: %d\n", st.buried)
	common.buf_addf(out, "total-jobs: %d\n", st.total_jobs)
	common.buf_addf(out, "current-using: %d\n", st.using_n)
	common.buf_addf(out, "current-watching: %d\n", st.watching)
	common.buf_addf(out, "current-waiting: %d\n", st.waiting)
	common.buf_addf(out, "cmd-put: %d\n", st.cmd_put)
	common.buf_addf(out, "cmd-reserve: %d\n", st.cmd_reserve)
	common.buf_addf(out, "cmd-delete: %d\n", st.cmd_delete)
	common.buf_addf(out, "cmd-release: %d\n", st.cmd_release)
	common.buf_addf(out, "cmd-bury: %d\n", st.cmd_bury)
	common.buf_addf(out, "cmd-kick: %d\n", st.cmd_kick)
	common.buf_addf(out, "cmd-touch: %d\n", st.cmd_touch)
	common.buf_addf(out, "pause: %d\n", st.pause_seconds)
	common.buf_addf(out, "pause-time-left: %d\n", st.pause_time_left)
}

queue_job_yaml :: proc(st: Job_Stats, out: ^common.Buf) {
	common.buf_addf(out, "---\nid: %d\n", st.id)
	common.buf_addf(out, "tube: %s\n", st.tube)
	common.buf_addf(out, "state: %s\n", queue_state_name(st.state))
	common.buf_addf(out, "pri: %d\n", st.pri)
	common.buf_addf(out, "age: %d\n", st.age)
	common.buf_addf(out, "delay: %d\n", st.delay)
	common.buf_addf(out, "ttr: %d\n", st.ttr)
	common.buf_addf(out, "time-left: %d\n", st.time_left)
	common.buf_addf(out, "file: 0\n")
	common.buf_addf(out, "reserves: %d\n", st.reserves)
	common.buf_addf(out, "timeouts: %d\n", st.timeouts)
	common.buf_addf(out, "releases: %d\n", st.releases)
	common.buf_addf(out, "buries: %d\n", st.buries)
	common.buf_addf(out, "kicks: %d\n", st.kicks)
}

queue_send_ok_yaml :: proc(c: ^QConn, yaml: ^common.Buf) -> bool {
	common.buf_reset(&c.reply)
	common.buf_addf(&c.reply, "OK %d\r\n", len(yaml.data))
	common.buf_write(&c.reply, yaml.data[:])
	common.buf_string(&c.reply, "\r\n")
	return queue_send_buf(c, &c.reply)
}

queue_discard_exact :: proc(c: ^QConn, n: int) -> bool {
	remaining := n
	for remaining > 0 {
		if c.conn.start >= c.conn.end {
			if !netio.conn_fill(&c.conn) {
				return false
			}
		}
		avail := c.conn.end - c.conn.start
		take := avail
		if take > remaining {
			take = remaining
		}
		c.conn.start += take
		remaining -= take
	}
	return true
}

queue_handle :: proc(c: ^QConn, cmdline: string) -> bool {
	fields, _ := strings.fields(cmdline, context.temp_allocator)
	if len(fields) == 0 {
		return true
	}
	cmd := fields[0]
	q := c.q
	switch cmd {
	case "put":
		if len(fields) != 5 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		pri, pok := strconv.parse_u64(fields[1], 10)
		delay_s, dok := queue_parse_i64(fields[2])
		ttr_s, tok := queue_parse_i64(fields[3])
		bytes_i, bok := strconv.parse_int(fields[4], 10)
		if !pok || !dok || !tok || !bok || pri > 0xffffffff || delay_s < 0 || ttr_s < 0 || bytes_i < 0 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		too_big := q.cfg.max_job_bytes > 0 && bytes_i > q.cfg.max_job_bytes
		if too_big {
			if !queue_discard_exact(c, bytes_i) {
				return false
			}
			if !queue_crlf(c) {
				queue_send_str(c.sock, "EXPECTED_CRLF\r\n")
				return false
			}
			return queue_send_str(c.sock, "JOB_TOO_BIG\r\n")
		}
		common.buf_reset(&c.body)
		if !netio.conn_read_exact(&c.conn, bytes_i, &c.body) {
			return false
		}
		if !queue_crlf(c) {
			queue_send_str(c.sock, "EXPECTED_CRLF\r\n")
			return false
		}
		id, res := queue_put(q, c.used, u32(pri), queue_secs_ns(delay_s), queue_secs_ns(ttr_s), c.body.data[:])
		switch res {
		case .OK:
			common.buf_reset(&c.reply)
			common.buf_addf(&c.reply, "INSERTED %d\r\n", id)
			return queue_send_buf(c, &c.reply)
		case .TOO_BIG:
			return queue_send_str(c.sock, "JOB_TOO_BIG\r\n")
		case .DRAINING:
			return queue_send_str(c.sock, "DRAINING\r\n")
		}
	case "use":
		if len(fields) != 2 || !queue_valid_tube_name(fields[1]) {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		if fields[1] != c.used {
			queue_use(q, c.used, fields[1])
			delete(c.used)
			c.used = strings.clone(fields[1])
		}
		common.buf_reset(&c.reply)
		common.buf_addf(&c.reply, "USING %s\r\n", c.used)
		return queue_send_buf(c, &c.reply)
	case "watch":
		if len(fields) != 2 || !queue_valid_tube_name(fields[1]) {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		if !qconn_watches(c, fields[1]) {
			name := strings.clone(fields[1])
			append(&c.watched, name)
			queue_watch(q, name)
		}
		common.buf_reset(&c.reply)
		common.buf_addf(&c.reply, "WATCHING %d\r\n", len(c.watched))
		return queue_send_buf(c, &c.reply)
	case "ignore":
		if len(fields) != 2 || !queue_valid_tube_name(fields[1]) {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		idx := -1
		for w, i in c.watched {
			if w == fields[1] {
				idx = i
				break
			}
		}
		if idx < 0 || len(c.watched) <= 1 {
			return queue_send_str(c.sock, "NOT_IGNORED\r\n")
		}
		queue_unwatch(q, c.watched[idx])
		delete(c.watched[idx])
		for k in idx..<len(c.watched)-1 {
			c.watched[k] = c.watched[k+1]
		}
		resize(&c.watched, len(c.watched)-1)
		common.buf_reset(&c.reply)
		common.buf_addf(&c.reply, "WATCHING %d\r\n", len(c.watched))
		return queue_send_buf(c, &c.reply)
	case "reserve":
		if len(fields) != 1 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		return queue_do_reserve(c, -1)
	case "reserve-with-timeout":
		if len(fields) != 2 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		secs, ok := queue_parse_i64(fields[1])
		if !ok {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		timeout := i64(-1)
		if secs >= 0 {
			timeout = queue_secs_ns(secs)
		}
		return queue_do_reserve(c, timeout)
	case "delete":
		if len(fields) != 2 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		id, ok := queue_parse_u64(fields[1])
		if !ok {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		if queue_delete(q, id, c.id) {
			qconn_forget(c, id)
			return queue_send_str(c.sock, "DELETED\r\n")
		}
		return queue_send_str(c.sock, "NOT_FOUND\r\n")
	case "release":
		if len(fields) != 4 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		id, iok := queue_parse_u64(fields[1])
		pri, pok := strconv.parse_u64(fields[2], 10)
		delay_s, dok := queue_parse_i64(fields[3])
		if !iok || !pok || !dok || pri > 0xffffffff || delay_s < 0 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		if queue_release(q, id, c.id, u32(pri), queue_secs_ns(delay_s)) {
			qconn_forget(c, id)
			return queue_send_str(c.sock, "RELEASED\r\n")
		}
		return queue_send_str(c.sock, "NOT_FOUND\r\n")
	case "bury":
		if len(fields) != 2 && len(fields) != 3 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		id, iok := queue_parse_u64(fields[1])
		set_pri := len(fields) == 3
		pri: u64
		pok := true
		if set_pri {
			pri, pok = strconv.parse_u64(fields[2], 10)
		}
		if !iok || !pok || pri > 0xffffffff {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		if queue_bury(q, id, c.id, u32(pri), set_pri) {
			qconn_forget(c, id)
			return queue_send_str(c.sock, "BURIED\r\n")
		}
		return queue_send_str(c.sock, "NOT_FOUND\r\n")
	case "touch":
		if len(fields) != 2 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		id, ok := queue_parse_u64(fields[1])
		if !ok {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		if queue_touch(q, id, c.id) {
			return queue_send_str(c.sock, "TOUCHED\r\n")
		}
		return queue_send_str(c.sock, "NOT_FOUND\r\n")
	case "peek":
		if len(fields) != 2 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		id, ok := queue_parse_u64(fields[1])
		if !ok {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		return queue_do_peek(c, .ID, id)
	case "peek-ready":
		return queue_do_peek(c, .READY, 0)
	case "peek-delayed":
		return queue_do_peek(c, .DELAYED, 0)
	case "peek-buried":
		return queue_do_peek(c, .BURIED, 0)
	case "kick":
		if len(fields) != 2 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		bound, ok := strconv.parse_int(fields[1], 10)
		if !ok || bound < 0 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		n := queue_kick(q, c.used, bound)
		common.buf_reset(&c.reply)
		common.buf_addf(&c.reply, "KICKED %d\r\n", n)
		return queue_send_buf(c, &c.reply)
	case "kick-job":
		if len(fields) != 2 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		id, ok := queue_parse_u64(fields[1])
		if !ok {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		if queue_kick_job(q, id) {
			return queue_send_str(c.sock, "KICKED\r\n")
		}
		return queue_send_str(c.sock, "NOT_FOUND\r\n")
	case "stats":
		yaml := common.buf_init(1024)
		defer common.buf_destroy(&yaml)
		queue_global_yaml(q, &yaml)
		return queue_send_ok_yaml(c, &yaml)
	case "stats-tube":
		if len(fields) != 2 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		st, ok := queue_tube_stats(q, fields[1])
		if !ok {
			return queue_send_str(c.sock, "NOT_FOUND\r\n")
		}
		yaml := common.buf_init(1024)
		defer common.buf_destroy(&yaml)
		queue_tube_yaml(st, &yaml)
		return queue_send_ok_yaml(c, &yaml)
	case "stats-job":
		if len(fields) != 2 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		id, ok := queue_parse_u64(fields[1])
		if !ok {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		st, found := queue_job_stats(q, id)
		if !found {
			return queue_send_str(c.sock, "NOT_FOUND\r\n")
		}
		yaml := common.buf_init(512)
		defer common.buf_destroy(&yaml)
		queue_job_yaml(st, &yaml)
		return queue_send_ok_yaml(c, &yaml)
	case "list-tubes":
		names := queue_list_tubes(q, context.temp_allocator)
		slice.sort_by(names, proc(a, b: string) -> bool { return a < b })
		yaml := common.buf_init(256)
		defer common.buf_destroy(&yaml)
		common.buf_string(&yaml, "---\n")
		for name in names {
			common.buf_addf(&yaml, "- %s\n", name)
		}
		return queue_send_ok_yaml(c, &yaml)
	case "list-tube-used":
		common.buf_reset(&c.reply)
		common.buf_addf(&c.reply, "USING %s\r\n", c.used)
		return queue_send_buf(c, &c.reply)
	case "list-tubes-watched":
		yaml := common.buf_init(256)
		defer common.buf_destroy(&yaml)
		common.buf_string(&yaml, "---\n")
		for name in c.watched {
			common.buf_addf(&yaml, "- %s\n", name)
		}
		return queue_send_ok_yaml(c, &yaml)
	case "pause-tube":
		if len(fields) != 3 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		secs, ok := strconv.parse_int(fields[2], 10)
		if !ok || secs < 0 {
			return queue_send_str(c.sock, "BAD_FORMAT\r\n")
		}
		if queue_pause_tube(q, fields[1], secs) {
			return queue_send_str(c.sock, "PAUSED\r\n")
		}
		return queue_send_str(c.sock, "NOT_FOUND\r\n")
	case "quit":
		return false
	}
	return queue_send_str(c.sock, "UNKNOWN_COMMAND\r\n")
}

queue_do_reserve :: proc(c: ^QConn, timeout_ns: i64) -> bool {
	id, ok := queue_reserve_into(c.q, c.watched[:], c.id, timeout_ns, &c.body)
	if !ok {
		return queue_send_str(c.sock, "TIMED_OUT\r\n")
	}
	append(&c.reserved, id)
	return queue_send_job(c, "RESERVED", id, c.body.data[:])
}

queue_do_peek :: proc(c: ^QConn, mode: Peek_Mode, id: u64) -> bool {
	jid, ok := queue_peek_into(c.q, c.used, mode, id, &c.body)
	if !ok {
		return queue_send_str(c.sock, "NOT_FOUND\r\n")
	}
	return queue_send_job(c, "FOUND", jid, c.body.data[:])
}
