// Shared helpers for the oboe benchmark suite: CLI parsing, worker plumbing,
// latency samples and output formatting.
package main

import "core:fmt"
import "core:math"
import "core:os"
import "core:slice"
import "core:strconv"
import "core:sync"
import "core:thread"
import "core:time"

// ---------------------------------------------------------------- parameters

Params :: struct {
	mode:       string,
	rows:       int,
	threads:    int,
	batch:      int,
	reps:       int,
	ops:        int,
	ops_large:  int,
	data:       string,
	keep:       bool,
	reuse:      bool,
	warm:       int,
	json:       bool,
	addr:       string,
	qmode:      string,
	clients:    int,
	jobs:       int,
	tube:       string,
	rounds:     int,
	value_size: int,
}

parse_params :: proc() -> Params {
	p := Params{
		mode      = "all",
		rows      = 200_000,
		threads   = 8,
		batch     = 500,
		reps      = 20,
		ops       = 100_000,
		ops_large = 2_000,
		data      = "/tmp/oboe_bench",
		warm      = 0,
		addr      = "127.0.0.1:11300",
		qmode     = "cycle",
		clients   = 1,
		jobs      = 100_000,
		tube      = "bench",
		rounds    = 20,
		value_size = 64,
	}
	args := os.args
	i := 1
	for i < len(args) {
		a := args[i]
		switch a {
		case "-mode":
			if v, ok := next_arg(args, &i); ok { p.mode = v }
		case "-rows":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v); nok { p.rows = n }
			}
		case "-threads":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v); nok { p.threads = n }
			}
		case "-batch":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v); nok { p.batch = n }
			}
		case "-reps":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v); nok { p.reps = n }
			}
		case "-ops":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v); nok { p.ops = n }
			}
		case "-ops-large":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v); nok { p.ops_large = n }
			}
		case "-data":
			if v, ok := next_arg(args, &i); ok { p.data = v }
		case "-keep":
			p.keep = true
		case "-reuse":
			p.reuse = true
		case "-warm":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v); nok && n >= 0 { p.warm = n }
			}
		case "-json":
			p.json = true
		case "-addr", "-qaddr":
			if v, ok := next_arg(args, &i); ok { p.addr = v }
		case "-qmode":
			if v, ok := next_arg(args, &i); ok { p.qmode = v }
		case "-clients":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v); nok && n > 0 { p.clients = n }
			}
		case "-jobs":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v); nok && n > 0 { p.jobs = n }
			}
		case "-tube":
			if v, ok := next_arg(args, &i); ok { p.tube = v }
		case "-rounds":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v); nok && n > 0 { p.rounds = n }
			}
		case "-value-size":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v); nok && n > 0 { p.value_size = n }
			}
		case "-h", "-help", "--help":
			print_usage()
			os.exit(0)
		case:
			fmt.eprintf("bench: unknown flag %q\n", a)
			print_usage()
			os.exit(2)
		}
		i += 1
	}
	p.threads = max(1, p.threads)
	p.batch = max(1, p.batch)
	p.reps = max(1, p.reps)
	p.clients = max(1, p.clients)
	p.jobs = max(1, p.jobs)
	p.rounds = max(1, p.rounds)
	return p
}

next_arg :: proc(args: []string, i: ^int) -> (string, bool) {
	if i^ + 1 >= len(args) {
		return "", false
	}
	i^ += 1
	return args[i^], true
}

print_usage :: proc() {
	fmt.println(`oboe bench — library and TCP benchmark suite

  -mode        ingest|query|cache|queue-tcp|all   (default all)
  -rows        rows to ingest                     (default 200000)
  -threads     concurrency for ingest/query/cache (default 8)
  -batch       rows per store_ingest call         (default 500)
  -reps        measured query calls per thread    (default 20)
  -ops         small-value cache ops per thread   (default 100000)
  -ops-large   large-value cache ops per thread   (default 2000)
  -warm        warmup operations per thread       (default 0)
  -data        scratch directory                  (default /tmp/oboe_bench)
  -keep        keep the scratch directory
  -reuse       load the existing data directory instead of reseeding
  -json        emit one JSON line per scenario
  -addr        queue TCP address                  (default 127.0.0.1:11300)
  -qmode       queue-tcp mode: put|get|cycle|bury (default cycle)
  -clients     queue TCP connections              (default 1)
  -jobs        jobs to put/reserve                (default 100000)
  -rounds      bury+kick rounds                   (default 20)
  -tube        queue tube                         (default bench)
  -value-size  small cache value bytes            (default 64)`)
}

// ---------------------------------------------------------------- threads

spin_until :: #force_inline proc(flag: ^u32) {
	for sync.atomic_load(flag) == 0 {
	}
}

wait_workers :: proc(workers: []^thread.Thread) {
	for w in workers {
		thread.join(w)
		thread.destroy(w)
	}
}

throughput :: #force_inline proc(n: int, d: time.Duration) -> f64 {
	secs := f64(d) / 1e9
	if secs <= 0 {
		return 0
	}
	return f64(n) / secs
}

millis :: #force_inline proc(d: time.Duration) -> f64 {
	return f64(d) / 1e6
}

// ---------------------------------------------------------------- samples

Samples :: struct {
	v: [dynamic]f64,
}

samples_init :: proc() -> Samples {
	return Samples{v = make([dynamic]f64, 0, 1024)}
}

samples_add :: proc(s: ^Samples, ms: f64) {
	append(&s.v, ms)
}

samples_merge :: proc(dst: ^Samples, src: ^Samples) {
	for x in src.v {
		append(&dst.v, x)
	}
}

samples_finish :: proc(s: ^Samples) {
	slice.sort(s.v[:])
}

samples_pct :: proc(s: ^Samples, p: f64) -> f64 {
	if len(s.v) == 0 {
		return f64(0)
	}
	rank := int(math.ceil(p * f64(len(s.v))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(s.v) {
		rank = len(s.v)
	}
	return s.v[rank-1]
}

samples_max :: proc(s: ^Samples) -> f64 {
	if len(s.v) == 0 {
		return f64(0)
	}
	return s.v[len(s.v)-1]
}

samples_destroy :: proc(s: ^Samples) {
	delete(s.v)
	s.v = nil
}

// ---------------------------------------------------------------- output
