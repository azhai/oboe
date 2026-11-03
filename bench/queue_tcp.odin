// TCP queue benchmark against a running `oboe -queue-listen`. It speaks the
// beanstalk subset directly so the numbers include protocol parsing, socket
// round-trips and the per-connection server thread.
package main

import "core:fmt"
import "core:net"
import "core:os"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"

BUF_SIZE :: 1 << 16
JOB_BODY := "bench job payload: index=0 method=POST path=/api/v1/items status=200 latency=12ms trace=0123456789abcdef"

QConn :: struct {
	sock:  net.TCP_Socket,
	buf:   [BUF_SIZE]u8,
	start: int,
	end:   int,
	reply: [256]u8,
}

q_fill :: proc(c: ^QConn) -> bool {
	n, err := net.recv_tcp(c.sock, c.buf[:])
	if err != nil || n <= 0 {
		return false
	}
	c.start = 0
	c.end = n
	return true
}

q_read_line :: proc(c: ^QConn, line: ^[256]u8, n: ^int) -> bool {
	n^ = 0
	for {
		if c.start >= c.end {
			if !q_fill(c) {
				return false
			}
		}
		b := c.buf[c.start]
		c.start += 1
		if b == '\n' {
			if n^ > 0 && line[n^-1] == '\r' {
				n^ -= 1
			}
			return true
		}
		if n^ >= len(line) {
			return false
		}
		line[n^] = b
		n^ += 1
	}
}

q_skip :: proc(c: ^QConn, count: int) -> bool {
	remaining := count
	for remaining > 0 {
		if c.start >= c.end {
			if !q_fill(c) {
				return false
			}
		}
		avail := c.end - c.start
		take := min(avail, remaining)
		c.start += take
		remaining -= take
	}
	return true
}

q_send :: proc(c: ^QConn, s: string) -> bool {
	rem := transmute([]u8)s
	for len(rem) > 0 {
		n, err := net.send_tcp(c.sock, rem)
		if err != nil || n <= 0 {
			return false
		}
		rem = rem[n:]
	}
	return true
}

q_connect :: proc(ep: net.Endpoint) -> (^QConn, bool) {
	sock, err := net.dial_tcp(ep)
	if err != nil {
		return nil, false
	}
	_ = net.set_option(sock, .Receive_Timeout, 5 * time.Second)
	_ = net.set_option(sock, .TCP_Nodelay, true)
	c := new(QConn)
	c.sock = sock
	return c, true
}

q_close :: proc(c: ^QConn) {
	net.close(c.sock)
	free(c)
}

// q_command sends one line command and returns the reply line (without CRLF),
// backed by the connection's reply buffer.
q_command :: proc(c: ^QConn, cmd: string) -> (string, bool) {
	if !q_send(c, cmd) {
		return "", false
	}
	n: int
	if !q_read_line(c, &c.reply, &n) {
		return "", false
	}
	return string(c.reply[:n]), true
}

q_use :: proc(c: ^QConn, tube: string) -> bool {
	cmd := fmt.aprintf("use %s\r\n", tube)
	defer delete(cmd)
	reply, ok := q_command(c, cmd)
	return ok && strings.has_prefix(reply, "USING ")
}

q_watch :: proc(c: ^QConn, tube: string) -> bool {
	cmd := fmt.aprintf("watch %s\r\n", tube)
	defer delete(cmd)
	reply, ok := q_command(c, cmd)
	return ok && strings.has_prefix(reply, "WATCHING ")
}

q_put :: proc(c: ^QConn, body: string) -> bool {
	hdr := fmt.aprintf("put 1 0 60 %d\r\n", len(body))
	defer delete(hdr)
	if !q_send(c, hdr) || !q_send(c, body) || !q_send(c, "\r\n") {
		return false
	}
	line: [256]u8
	n: int
	if !q_read_line(c, &line, &n) {
		return false
	}
	return strings.has_prefix(string(line[:n]), "INSERTED ")
}

// q_reserve reserves one job and consumes its body. id is 0 for TIMED_OUT.
q_reserve :: proc(c: ^QConn) -> (id: u64, ok: bool) {
	if !q_send(c, "reserve-with-timeout 0\r\n") {
		return 0, false
	}
	line: [256]u8
	n: int
	if !q_read_line(c, &line, &n) {
		return 0, false
	}
	s := string(line[:n])
	if strings.has_prefix(s, "TIMED_OUT") || strings.has_prefix(s, "DEADLINE_SOON") {
		return 0, false
	}
	if !strings.has_prefix(s, "RESERVED ") {
		return 0, false
	}
	rest := s[9:]
	sp := strings.index_byte(rest, ' ')
	if sp <= 0 {
		return 0, false
	}
	ids, iok := strconv.parse_u64(rest[:sp])
	bytes_n, bok := strconv.parse_int(rest[sp+1:])
	if !iok || !bok || bytes_n < 0 {
		return 0, false
	}
	if !q_skip(c, bytes_n + 2) {
		return 0, false
	}
	return ids, true
}

q_delete :: proc(c: ^QConn, id: u64) -> bool {
	cmd := fmt.aprintf("delete %d\r\n", id)
	defer delete(cmd)
	reply, ok := q_command(c, cmd)
	return ok && strings.has_prefix(reply, "DELETED")
}

q_bury :: proc(c: ^QConn, id: u64) -> bool {
	cmd := fmt.aprintf("bury %d\r\n", id)
	defer delete(cmd)
	reply, ok := q_command(c, cmd)
	return ok && strings.has_prefix(reply, "BURIED")
}

q_kick :: proc(c: ^QConn, bound: int) -> (kicked: int, ok: bool) {
	cmd := fmt.aprintf("kick %d\r\n", bound)
	defer delete(cmd)
	reply, rok := q_command(c, cmd)
	if !rok || !strings.has_prefix(reply, "KICKED ") {
		return 0, false
	}
	k, kok := strconv.parse_int(reply[7:])
	return k, kok
}

Queue_Mode :: enum {
	Put,
	Get,
	Cycle,
	Bury,
}

Queue_Args :: struct {
	ep:       net.Endpoint,
	mode:     Queue_Mode,
	tube:     string,
	start:    ^u32,
	per_job:  int,
	rounds:   int,
	body:        string,
	samples:     ^Samples,
	kick_samples: ^Samples,
	ops:         ^u64,
	errors:      ^u64,
	res_fail:    ^u64,
	bury_fail:   ^u64,
	kick_fail:   ^u64,
	target:      u64,
}

queue_client :: proc(data: rawptr) {
	a := cast(^Queue_Args)data
	c, ok := q_connect(a.ep)
	if !ok {
		sync.atomic_add(a.errors, 1)
		return
	}
	defer q_close(c)
	if !q_use(c, a.tube) || !q_watch(c, a.tube) {
		sync.atomic_add(a.errors, 1)
		return
	}
	spin_until(a.start)

	switch a.mode {
	case .Put:
		for _ in 0..<a.per_job {
			t0 := time.tick_now()
			if q_put(c, a.body) {
				samples_add(a.samples, f64(time.tick_since(t0)) / 1e6)
				sync.atomic_add(a.ops, 1)
			} else {
				sync.atomic_add(a.errors, 1)
			}
		}
	case .Get:
		for {
			if sync.atomic_load(a.ops) >= a.target {
				break
			}
			t0 := time.tick_now()
			id, rok := q_reserve(c)
			if !rok {
				break
			}
			if !q_delete(c, id) {
				sync.atomic_add(a.errors, 1)
				continue
			}
			samples_add(a.samples, f64(time.tick_since(t0)) / 1e6)
			if sync.atomic_add(a.ops, 1) + 1 >= a.target {
				break
			}
		}
	case .Cycle:
		for _ in 0..<a.per_job {
			t0 := time.tick_now()
			ok_cycle := q_put(c, a.body)
			if ok_cycle {
				id, rok := q_reserve(c)
				ok_cycle = rok && q_delete(c, id)
			}
			if ok_cycle {
				samples_add(a.samples, f64(time.tick_since(t0)) / 1e6)
				sync.atomic_add(a.ops, 1)
			} else {
				sync.atomic_add(a.errors, 1)
			}
		}
	case .Bury:
		for _ in 0..<a.rounds {
			for _ in 0..<a.per_job {
				t0 := time.tick_now()
				id, rok := q_reserve(c)
				if !rok {
					sync.atomic_add(a.res_fail, 1)
					sync.atomic_add(a.errors, 1)
					continue
				}
				if !q_bury(c, id) {
					sync.atomic_add(a.bury_fail, 1)
					sync.atomic_add(a.errors, 1)
					continue
				}
				samples_add(a.samples, f64(time.tick_since(t0)) / 1e6)
				sync.atomic_add(a.ops, 1)
			}
			k0 := time.tick_now()
			_, kok := q_kick(c, a.per_job)
			if !kok {
				sync.atomic_add(a.kick_fail, 1)
				sync.atomic_add(a.errors, 1)
			}
			samples_add(a.kick_samples, f64(time.tick_since(k0)) / 1e6)
		}
	}
}

// preload_jobs fills the tube so reserve/delete and bury/kick measurements do
// not include producer cost. The preload is reported separately.
preload_jobs :: proc(ep: net.Endpoint, tube: string, jobs: int) -> (time.Duration, int) {
	c, ok := q_connect(ep)
	if !ok {
		return 0, 0
	}
	defer q_close(c)
	_ = q_use(c, tube)
	_ = q_watch(c, tube)
	begin := time.now()
	done := 0
	for _ in 0..<jobs {
		if !q_put(c, JOB_BODY) {
			break
		}
		done += 1
	}
	return time.since(begin), done
}

run_queue_tcp :: proc(p: Params) {
	ep, ok := net.parse_endpoint(p.addr)
	if !ok {
		fmt.eprintf("bench: invalid queue address %q\n", p.addr)
		return
	}
	mode := Queue_Mode.Cycle
	switch p.qmode {
	case "put":
		mode = .Put
	case "get":
		mode = .Get
	case "cycle":
		mode = .Cycle
	case "bury":
		mode = .Bury
	case:
		fmt.eprintf("bench: unknown -qmode %q\n", p.qmode)
		return
	}
	body := JOB_BODY

	// A per-process tube keeps leftovers from earlier runs out of this run.
	run_tube := fmt.aprintf("%s-%d", p.tube, os.get_pid())
	defer delete(run_tube)

	preload_d: time.Duration
	// For get, every client stops when the shared counter reaches the preloaded
	// total; for the other modes each client owns an equal share.
	target := u64(p.jobs)
	per_client := p.jobs / p.clients
	if per_client < 1 {
		per_client = 1
	}
	tubes := make([]string, p.clients)
	defer {
		for t in tubes {
			if t != run_tube {
				delete(t)
			}
		}
		delete(tubes)
	}
	for i in 0..<p.clients {
		tubes[i] = run_tube
	}
	if mode == .Bury {
		// Isolated tube per client: a shared tube lets clients steal jobs
		// another client is about to bury, which makes kick counts racy.
		for i in 0..<p.clients {
			t := fmt.aprintf("%s-%d", run_tube, i)
			tubes[i] = t
			preload_jobs(ep, t, per_client)
		}
	} else if mode == .Get {
		preload_d, _ = preload_jobs(ep, run_tube, p.clients * per_client)
		target = u64(p.clients * per_client)
	}

	start: u32
	args := make([]Queue_Args, p.clients)
	workers := make([]^thread.Thread, p.clients)
	samples := make([]Samples, p.clients)
	kick_samples := make([]Samples, p.clients)
	ops, errors, res_fail, bury_fail, kick_fail: u64
	for i in 0..<p.clients {
		samples[i] = samples_init()
		kick_samples[i] = samples_init()
		args[i] = Queue_Args{
			ep       = ep,
			mode     = mode,
			tube     = tubes[i],
			start    = &start,
			per_job  = per_client,
			rounds   = p.rounds,
			body     = body,
			samples  = &samples[i],
			kick_samples = &kick_samples[i],
			ops      = &ops,
			errors   = &errors,
			res_fail = &res_fail,
			bury_fail = &bury_fail,
			kick_fail = &kick_fail,
			target   = target,
		}
		workers[i] = thread.create_and_start_with_data(&args[i], queue_client, self_cleanup = false)
	}
	begin := time.now()
	sync.atomic_store(&start, 1)
	wait_workers(workers)
	d := time.since(begin)

	merged := samples_init()
	kick_merged := samples_init()
	for i in 0..<p.clients {
		samples_merge(&merged, &samples[i])
		samples_merge(&kick_merged, &kick_samples[i])
		samples_destroy(&samples[i])
		samples_destroy(&kick_samples[i])
	}
	samples_finish(&merged)
	samples_finish(&kick_merged)
	op_s := throughput(int(ops), d)
	if p.json {
		fmt.printf(
			`{{"scenario":"queue_tcp","qmode":"%s","clients":%d,"jobs":%d,"rounds":%d,"ops":%d,"ops_per_s":%.0f,"p50_ms":%.3f,"p95_ms":%.3f,"p99_ms":%.3f,"max_ms":%.3f,"kick_p50_ms":%.3f,"kick_p99_ms":%.3f,"preload_s":%.3f,"errors":%d,"res_fail":%d,"bury_fail":%d,"kick_fail":%d}}`,
			p.qmode, p.clients, p.jobs, p.rounds, ops, op_s,
			samples_pct(&merged, 0.50), samples_pct(&merged, 0.95), samples_pct(&merged, 0.99),
			samples_max(&merged), samples_pct(&kick_merged, 0.50), samples_pct(&kick_merged, 0.99),
			millis(preload_d) / 1000.0, errors, res_fail, bury_fail, kick_fail,
		)
		fmt.println()

	} else {
		fmt.printf("[queue-tcp] qmode=%s clients=%d jobs=%d rounds=%d preload=%.1fms ops=%d ops/s=%.0f p50=%.3fms p95=%.3fms p99=%.3fms kick_p50=%.3fms errors=%d\n",
			p.qmode, p.clients, p.jobs, p.rounds, millis(preload_d), ops, op_s,
			samples_pct(&merged, 0.50), samples_pct(&merged, 0.95), samples_pct(&merged, 0.99),
			samples_pct(&kick_merged, 0.50), errors)
	}
	samples_destroy(&merged)
	samples_destroy(&kick_merged)
	delete(samples)
	delete(kick_samples)
	delete(args)
	delete(workers)
}
