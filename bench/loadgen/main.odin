// HTTP load generator for the oboe server.
//
// One OS thread per virtual user. A user either keeps one keep-alive connection
// and issues requests back-to-back, or opens a new connection for every request
// (per-request mode), until the measurement window ends.
//
// The output separates throughput from saturation: `served` is the number of
// virtual users that completed at least one measured request, `stalled` is the
// remainder. When concurrency exceeds the server's worker count, keep-alive
// requests are queued behind busy workers and `served` stays at the worker
// count no matter how high -conc goes. `wait_*` reports how long a served user
// waited for its first response, which is where that queueing shows up.
//
// Build:
//   odin build bench/loadgen -out:oboe_loadgen
//
// Examples:
//   oboe_loadgen -addr 127.0.0.1:9428 -path /health -conc 64 -dur 5000 -warm 2000
//   oboe_loadgen -path /health -conc 512 -conns per-request -dur 5000
//   oboe_loadgen -path /insert/jsonline -method POST -body-file /tmp/lines.ndjson \
//               -conc 16 -dur 5000 -rows-per-req 100
//   oboe_loadgen -path '/select/logsql/query?query=level%3Aerror&limit=100' \
//               -conc 8 -dur 5000 -json
package main

import "core:fmt"
import "core:math"
import "core:net"
import "core:os"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"

BUF_SIZE :: 1 << 16

Conn_Mode :: enum {
	Keep_Alive,
	Per_Request,
}

Args :: struct {
	addr:        string,
	path:        string,
	method:      string,
	body_file:   string,
	body_text:   string,
	body:        []u8,
	conc:        int,
	warm:        time.Duration,
	dur:         time.Duration,
	requests:    int,
	conns:       Conn_Mode,
	timeout:     time.Duration,
	expect:      int,
	rows_per_req: int,
	rotate:      int,
	label:       string,
	json:        bool,
}

Reader :: struct {
	sock:  net.TCP_Socket,
	buf:   [BUF_SIZE]u8,
	start: int,
	end:   int,
}

Worker :: struct {
	args:         Args,
	ep:           net.Endpoint,
	req_keep:     []u8,
	req_close:    []u8,
	start:        ^u32,
	lat:          [dynamic]f64,
	ok:           int,
	http_err:     int,
	transport:    int,
	dial_err:     int,
	status_2xx:   int,
	status_3xx:   int,
	status_4xx:   int,
	status_5xx:   int,
	bytes:        i64,
	first_resp_ms: f64,
	rot_index:    int,
}

// ---------------------------------------------------------------- io helpers

fill :: proc(r: ^Reader) -> bool {
	n, err := net.recv_tcp(r.sock, r.buf[:])
	if err != nil || n <= 0 {
		return false
	}
	r.start = 0
	r.end = n
	return true
}

read_line :: proc(r: ^Reader, line: ^[1024]u8, n: ^int) -> bool {
	n^ = 0
	for {
		if r.start >= r.end {
			if !fill(r) {
				return false
			}
		}
		b := r.buf[r.start]
		r.start += 1
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

skip_body :: proc(r: ^Reader, n: int) -> bool {
	remaining := n
	for remaining > 0 {
		if r.start >= r.end {
			if !fill(r) {
				return false
			}
		}
		avail := r.end - r.start
		take := min(avail, remaining)
		r.start += take
		remaining -= take
	}
	return true
}

send_all :: proc(sock: net.TCP_Socket, data: []u8) -> bool {
	rem := data
	for len(rem) > 0 {
		n, err := net.send_tcp(sock, rem)
		if err != nil || n <= 0 {
			return false
		}
		rem = rem[n:]
	}
	return true
}

// one_exchange sends one request and consumes exactly one response. It returns
// the status code and the total response bytes (headers + body); ok is false on
// a transport error.
one_exchange :: proc(r: ^Reader, req: []u8) -> (code: int, resp_bytes: i64, ok: bool) {
	if !send_all(r.sock, req) {
		return 0, 0, false
	}
	line: [1024]u8
	n: int
	if !read_line(r, &line, &n) {
		return 0, 0, false
	}
	resp_bytes = i64(n) + 1
	// "HTTP/1.1 200 OK"
	if n >= 12 {
		if c, cok := strconv.parse_int(string(line[9:12])); cok {
			code = c
		}
	}
	content_len := -1
	for {
		if !read_line(r, &line, &n) {
			return code, resp_bytes, false
		}
		resp_bytes += i64(n) + 2
		if n == 0 {
			break
		}
		h := string(line[:n])
		if strings.has_prefix(h, "Content-Length:") || strings.has_prefix(h, "content-length:") {
			if v, vok := strconv.parse_int(strings.trim_space(h[15:])); vok {
				content_len = v
			}
		}
	}
	if content_len > 0 {
		if !skip_body(r, content_len) {
			return code, resp_bytes, false
		}
		resp_bytes += i64(content_len)
	}
	return code, resp_bytes, true
}

// ---------------------------------------------------------------- worker

spin_until :: #force_inline proc(flag: ^u32) {
	for sync.atomic_load(flag) == 0 {
	}
}

tick_ms :: #force_inline proc(t: time.Tick) -> f64 {
	return f64(time.tick_since(t)) / 1e6
}

record :: proc(w: ^Worker, code: int, ok: bool, dt_ms: f64, resp_bytes: i64, base: time.Tick) {
	append(&w.lat, dt_ms)
	if ok {
		w.ok += 1
		if w.ok == 1 {
			// Time from the start of the measurement window, not from the
			// start of warmup, so the value reflects queueing after warmup.
			wait := tick_ms(base) - f64(w.args.warm) / 1e6
			w.first_resp_ms = wait > 0 ? wait : 0
		}
	} else {
		w.http_err += 1
	}
	w.bytes += resp_bytes
	switch {
	case code >= 500:
		w.status_5xx += 1
	case code >= 400:
		w.status_4xx += 1
	case code >= 300:
		w.status_3xx += 1
	case code >= 200:
		w.status_2xx += 1
	}
}

// Open (or re-open) the session's socket. Keeping this separate lets the
// keep-alive loop recover from a server-side close instead of parking the
// worker: with the memtable cap a write can come back as `503 Connection:
// close`, and every real client redials after that.
keep_alive_open :: proc(w: ^Worker, rr: ^Reader) -> bool {
	sock, derr := net.dial_tcp(w.ep)
	if derr != nil {
		w.dial_err += 1
		return false
	}
	_ = net.set_option(sock, .Receive_Timeout, w.args.timeout)
	_ = net.set_option(sock, .TCP_Nodelay, true)
	rr.sock = sock
	rr.start = 0
	rr.end = 0
	return true
}

keep_alive_user :: proc(w: ^Worker) {
	base := time.tick_now()
	warm := w.args.warm
	window := warm + w.args.dur

	rr := new(Reader)
	defer free(rr)
	if !keep_alive_open(w, rr) {
		return
	}
	opened := true
	defer {
		if opened {
			net.close(rr.sock)
		}
	}

	measured := 0
	for {
		elapsed := time.tick_since(base)
		if elapsed >= window {
			break
		}
		if w.args.requests > 0 && measured >= w.args.requests {
			break
		}
		measure := elapsed >= warm
		t0 := time.tick_now()
		req, owned := request_for(w, true)
		code, rbytes, ok := one_exchange(rr, req)
		if owned {
			delete(req)
		}
		dt := tick_ms(t0)
		if measure && ok {
			measured += 1
			record(w, code, w.args.expect == 0 || code == w.args.expect, dt, rbytes, base)
		}
		if !ok {
			// The session died (503 overload closes it, or the server is
			// shutting down). Redial and keep measuring; a run that ends here
			// would report zero throughput instead of a throttled one.
			w.transport += 1
			net.close(rr.sock)
			opened = false
			time.sleep(time.Millisecond)
			if !keep_alive_open(w, rr) {
				break
			}
			opened = true
		}
	}
}

per_request_user :: proc(w: ^Worker) {
	base := time.tick_now()
	warm := w.args.warm
	window := warm + w.args.dur

	measured := 0
	for {
		elapsed := time.tick_since(base)
		if elapsed >= window {
			break
		}
		if w.args.requests > 0 && measured >= w.args.requests {
			break
		}
		measure := elapsed >= warm
		t0 := time.tick_now()
		sock, derr := net.dial_tcp(w.ep)
		if derr != nil {
			if measure {
				w.dial_err += 1
			}
			time.sleep(500 * time.Microsecond)
			continue
		}
		_ = net.set_option(sock, .Receive_Timeout, w.args.timeout)
		_ = net.set_option(sock, .TCP_Nodelay, true)
		r := Reader{sock = sock}
		req, owned := request_for(w, false)
		code, rbytes, ok := one_exchange(&r, req)
		if owned {
			delete(req)
		}
		net.close(sock)
		dt := tick_ms(t0)
		if measure {
			if ok {
				measured += 1
				record(w, code, w.args.expect == 0 || code == w.args.expect, dt, rbytes, base)
			} else {
				w.transport += 1
			}
		}
		if !ok {
			time.sleep(500 * time.Microsecond)
		}
	}
}

user_main :: proc(data: rawptr) {
	w := cast(^Worker)data
	spin_until(w.start)
	switch w.args.conns {
	case .Keep_Alive:
		keep_alive_user(w)
	case .Per_Request:
		per_request_user(w)
	}
}

// ---------------------------------------------------------------- args

usage :: proc() {
	fmt.println(`oboe_loadgen — HTTP load generator

  -addr          host:port                      (default 127.0.0.1:9428)
  -path          request target, query included (default /health)
  -method        HTTP method                    (default GET, POST when -body* is set)
  -body          inline request body
  -body-file     read the request body from a file
  -conc          concurrent virtual users        (default 8)
  -warm          warmup milliseconds             (default 1000)
  -dur           measurement milliseconds        (default 5000)
  -requests      measured requests per user      (0 = run for -dur)
  -conns         keep-alive|per-request          (default keep-alive)
  -timeout       socket receive timeout ms       (default 2000)
  -expect        expected status                 (default 200, 0 = any)
  -rows-per-req  rows represented by one request (adds rows/s, default 0)
  -rotate        expand {i} in -path over this many values (default 0)
  -label         label in the output             (default derived from -path)
  -json          emit one JSON object instead of a human line`)
}

next_arg :: proc(args: []string, i: ^int) -> (string, bool) {
	if i^ + 1 >= len(args) {
		return "", false
	}
	i^ += 1
	return args[i^], true
}

parse_args :: proc() -> (Args, bool) {
	a := Args{
		addr     = "127.0.0.1:9428",
		path     = "/health",
		method   = "",
		conc     = 8,
		warm     = 1000 * time.Millisecond,
		dur      = 5000 * time.Millisecond,
		timeout  = 2000 * time.Millisecond,
		expect   = 200,
		conns    = .Keep_Alive,
		label    = "",
	}
	args := os.args
	i := 1
	for i < len(args) {
		switch args[i] {
		case "-addr":
			v, ok := next_arg(args, &i)
			if ok { a.addr = v }
		case "-path", "-url":
			v, ok := next_arg(args, &i)
			if ok { a.path = v }
		case "-method":
			v, ok := next_arg(args, &i)
			if ok { a.method = v }
		case "-body":
			v, ok := next_arg(args, &i)
			if ok { a.body_text = v }
		case "-body-file":
			v, ok := next_arg(args, &i)
			if ok { a.body_file = v }
		case "-conc":
			v, ok := next_arg(args, &i)
			if ok {
				if n, nok := strconv.parse_int(v); nok && n > 0 { a.conc = n }
			}
		case "-warm":
			v, ok := next_arg(args, &i)
			if ok {
				if n, nok := strconv.parse_int(v); nok && n >= 0 { a.warm = time.Duration(n) * time.Millisecond }
			}
		case "-dur":
			v, ok := next_arg(args, &i)
			if ok {
				if n, nok := strconv.parse_int(v); nok && n > 0 { a.dur = time.Duration(n) * time.Millisecond }
			}
		case "-requests":
			v, ok := next_arg(args, &i)
			if ok {
				if n, nok := strconv.parse_int(v); nok && n >= 0 { a.requests = n }
			}
		case "-conns":
			v, ok := next_arg(args, &i)
			if ok {
				switch v {
				case "keep-alive", "ka":
					a.conns = .Keep_Alive
				case "per-request", "new":
					a.conns = .Per_Request
				case:
					fmt.eprintf("loadgen: unknown -conns %q\n", v)
					return a, false
				}
			}
		case "-timeout":
			v, ok := next_arg(args, &i)
			if ok {
				if n, nok := strconv.parse_int(v); nok && n > 0 { a.timeout = time.Duration(n) * time.Millisecond }
			}
		case "-expect":
			v, ok := next_arg(args, &i)
			if ok {
				if n, nok := strconv.parse_int(v); nok && n >= 0 { a.expect = n }
			}
		case "-rows-per-req":
			v, ok := next_arg(args, &i)
			if ok {
				if n, nok := strconv.parse_int(v); nok && n >= 0 { a.rows_per_req = n }
			}
		case "-rotate":
			v, ok := next_arg(args, &i)
			if ok {
				if n, nok := strconv.parse_int(v); nok && n > 0 { a.rotate = n }
			}
		case "-label":
			v, ok := next_arg(args, &i)
			if ok { a.label = v }
		case "-json":
			a.json = true
		case "-h", "-help", "--help":
			usage()
			os.exit(0)
		case:
			fmt.eprintf("loadgen: unknown flag %q\n", args[i])
			usage()
			return a, false
		}
		i += 1
	}

	if len(a.body_file) > 0 {
		if data, err := os.read_entire_file(a.body_file, context.allocator); err == nil {
			a.body = data
		} else {
			fmt.eprintf("loadgen: cannot read %q\n", a.body_file)
			return a, false
		}
	} else if len(a.body_text) > 0 {
		a.body = transmute([]u8)a.body_text
	}
	if len(a.method) == 0 {
		a.method = "GET"
		if len(a.body) > 0 {
			a.method = "POST"
		}
	}
	if len(a.label) == 0 {
		a.label = a.path
	}
	return a, true
}

build_request :: proc(a: Args, keep_alive: bool, path: string) -> []u8 {
	conn := keep_alive ? "keep-alive" : "close"
	sb := strings.builder_make(0, 512)
	fmt.sbprintf(&sb, "%s %s HTTP/1.1\r\n", a.method, path)
	fmt.sbprintf(&sb, "Host: %s\r\n", a.addr)
	fmt.sbprintf(&sb, "User-Agent: oboe-loadgen\r\n")
	fmt.sbprintf(&sb, "Accept: */*\r\n")
	fmt.sbprintf(&sb, "Connection: %s\r\n", conn)
	if len(a.body) > 0 {
		fmt.sbprintf(&sb, "Content-Type: application/json\r\n")
		fmt.sbprintf(&sb, "Content-Length: %d\r\n", len(a.body))
	}
	fmt.sbprintf(&sb, "\r\n")
	s := strings.to_string(sb)
	out := make([]u8, len(s) + len(a.body))
	for i in 0..<len(s) {
		out[i] = s[i]
	}
	if len(a.body) > 0 {
		copy(out[len(s):], a.body)
	}
	strings.builder_destroy(&sb)
	return out
}

rotate_path :: proc(template: string, n: int) -> string {
	i := strings.index(template, "{i}")
	if i < 0 {
		return fmt.aprintf("%s", template)
	}
	return fmt.aprintf("%s%d%s", template[:i], n, template[i+3:])
}

// request_for returns the bytes for the next request. When -rotate is set the
// caller owns (and must delete) the returned buffer.
request_for :: proc(w: ^Worker, keep_alive: bool) -> (req: []u8, owned: bool) {
	if w.args.rotate <= 0 {
		return keep_alive ? w.req_keep : w.req_close, false
	}
	path := rotate_path(w.args.path, w.rot_index % w.args.rotate)
	w.rot_index += 1
	defer delete(path)
	return build_request(w.args, keep_alive, path), true
}

// ---------------------------------------------------------------- stats

percentile :: proc(sorted: []f64, p: f64) -> f64 {
	if len(sorted) == 0 {
		return f64(0)
	}
	// nearest-rank: ceil(p * n) - 1
	rank := int(math.ceil(p * f64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank - 1]
}

print_human :: proc(a: Args, lat: []f64, w: []Worker, elapsed_s: f64, total_ok, total_http_err, total_transport, total_dial_err: int, total_bytes: i64, served, stalled: int, wait: []f64) {
	p50 := percentile(lat, 0.50)
	p95 := percentile(lat, 0.95)
	p99 := percentile(lat, 0.99)
	pmax := f64(0)
	if len(lat) > 0 {
		pmax = lat[len(lat)-1]
	}
	mode := a.conns == .Keep_Alive ? "keep-alive" : "per-request"
	qps := f64(total_ok) / elapsed_s
	fmt.printf(
		"label=%s mode=%s conc=%d requests=%d ok=%d http_err=%d transport_err=%d dial_err=%d qps=%.1f p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms served=%d/%d stalled=%d wait_p50=%.2fms wait_p99=%.2fms bytes=%.1fMiB",
		a.label, mode, a.conc, total_ok + total_http_err, total_ok, total_http_err,
		total_transport, total_dial_err, qps, p50, p95, p99, pmax,
		served, a.conc, stalled, percentile(wait, 0.50), percentile(wait, 0.99),
		f64(total_bytes) / (1 << 20),
	)
	if a.rows_per_req > 0 {
		fmt.printf(" rows/s=%.0f", qps * f64(a.rows_per_req))
	}
	fmt.println()
}

print_json :: proc(a: Args, lat: []f64, elapsed_s: f64, total_ok, total_http_err, total_transport, total_dial_err: int, total_bytes: i64, served, stalled: int, wait: []f64) {
	p50 := percentile(lat, 0.50)
	p95 := percentile(lat, 0.95)
	p99 := percentile(lat, 0.99)
	pmax := f64(0)
	if len(lat) > 0 {
		pmax = lat[len(lat)-1]
	}
	mode := a.conns == .Keep_Alive ? "keep-alive" : "per-request"
	qps := f64(total_ok) / elapsed_s
	fmt.printf(
		`{{"label":%q,"mode":"%s","conc":%d,"warm_ms":%d,"dur_ms":%d,"elapsed_s":%.3f,"requests":%d,"ok":%d,"http_err":%d,"transport_err":%d,"dial_err":%d,"qps":%.1f,"p50_ms":%.3f,"p95_ms":%.3f,"p99_ms":%.3f,"max_ms":%.3f,"served":%d,"stalled":%d,"wait_p50_ms":%.3f,"wait_p99_ms":%.3f,"bytes":%d,"req_body_bytes":%d,"rows_per_req":%d,"rows_per_s":%.0f}}`,
		a.label, mode, a.conc, int(a.warm / time.Millisecond), int(a.dur / time.Millisecond),
		elapsed_s, total_ok + total_http_err, total_ok, total_http_err, total_transport, total_dial_err,
		qps, p50, p95, p99, pmax, served, stalled,
		percentile(wait, 0.50), percentile(wait, 0.99), total_bytes, len(a.body), a.rows_per_req,
		qps * f64(a.rows_per_req),
	)
	fmt.println()
}

// ---------------------------------------------------------------- main

main :: proc() {
	a, ok := parse_args()
	if !ok {
		os.exit(2)
	}
	ep, epok := net.parse_endpoint(a.addr)
	if !epok {
		fmt.eprintf("loadgen: invalid address %q\n", a.addr)
		os.exit(2)
	}
	req_keep := build_request(a, true, a.path)
	req_close := build_request(a, false, a.path)
	defer delete(req_keep)
	defer delete(req_close)
	if len(a.body) > 0 && len(a.body_file) > 0 {
		delete(a.body)
	}

	workers := make([]Worker, a.conc)
	threads := make([]^thread.Thread, a.conc)
	defer {
		for i in 0..<a.conc {
			delete(workers[i].lat)
		}
		delete(workers)
		delete(threads)
	}
	start: u32

	for i in 0..<a.conc {
		workers[i].args = a
		workers[i].ep = ep
		workers[i].req_keep = req_keep
		workers[i].req_close = req_close
		workers[i].start = &start
		workers[i].rot_index = i
		threads[i] = thread.create_and_start_with_data(&workers[i], user_main, self_cleanup = false)
	}
	sync.atomic_store(&start, 1)
	for t in threads {
		thread.join(t)
		thread.destroy(t)
	}

	lat: [dynamic]f64
	wait: [dynamic]f64
	served := 0
	stalled := 0
	total_ok, total_http_err, total_transport, total_dial_err := 0, 0, 0, 0
	total_bytes: i64
	for i in 0..<a.conc {
		w := &workers[i]
		total_ok += w.ok
		total_http_err += w.http_err
		total_transport += w.transport
		total_dial_err += w.dial_err
		total_bytes += w.bytes
		if len(w.lat) > 0 {
			served += 1
			append(&wait, w.first_resp_ms)
		}
		for v in w.lat {
			append(&lat, v)
		}
		if len(w.lat) == 0 && w.dial_err == 0 {
			stalled += 1
		}
	}
	slice.sort(lat[:])
	slice.sort(wait[:])

	elapsed_s := f64(a.dur) / 1e9

	if a.json {
		print_json(a, lat[:], elapsed_s, total_ok, total_http_err, total_transport, total_dial_err, total_bytes, served, stalled, wait[:])
	} else {
		print_human(a, lat[:], workers, elapsed_s, total_ok, total_http_err, total_transport, total_dial_err, total_bytes, served, stalled, wait[:])
	}
	delete(lat)
	delete(wait)
}
