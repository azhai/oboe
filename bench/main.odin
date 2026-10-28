// Standalone benchmark/stress driver for oboe.
//
// Run with:
//   odin run bench -- -mode ingest -rows 200000 -threads 8 -batch 500
//   odin run bench -- -mode query  -rows 200000 -threads 8 -reps 20
//   odin run bench -- -mode cache  -ops 100000 -threads 16
//   odin run bench -- -mode queue  -rows 200000 -threads 8
//   odin run bench -- -mode all
package main

import "core:fmt"
import "core:os"
import "core:strconv"
import "core:sync"
import "core:thread"
import "core:time"

import oboe "../src"

Params :: struct {
	mode:    string,
	rows:    int,
	threads: int,
	batch:   int,
	reps:    int,
	ops:     int,
	data:    string,
	keep:    bool,
}

parse_params :: proc() -> Params {
	p := Params{
		mode    = "all",
		rows    = 200_000,
		threads = 8,
		batch   = 500,
		reps    = 20,
		ops     = 100_000,
		data    = "/tmp/oboe_bench",
	}
	args := os.args
	i := 1
	for i < len(args) {
		switch args[i] {
		case "-mode":
			i += 1
			if i < len(args) {
				p.mode = args[i]
			}
		case "-rows":
			i += 1
			if i < len(args) {
				if v, ok := strconv.parse_int(args[i]); ok {
					p.rows = v
				}
			}
		case "-threads":
			i += 1
			if i < len(args) {
				if v, ok := strconv.parse_int(args[i]); ok {
					p.threads = v
				}
			}
		case "-batch":
			i += 1
			if i < len(args) {
				if v, ok := strconv.parse_int(args[i]); ok {
					p.batch = v
				}
			}
		case "-reps":
			i += 1
			if i < len(args) {
				if v, ok := strconv.parse_int(args[i]); ok {
					p.reps = v
				}
			}
		case "-ops":
			i += 1
			if i < len(args) {
				if v, ok := strconv.parse_int(args[i]); ok {
					p.ops = v
				}
			}
		case "-data":
			i += 1
			if i < len(args) {
				p.data = args[i]
			}
		case "-keep":
			p.keep = true
		case "-h", "--help":
			print_usage()
			os.exit(0)
		case:
			fmt.eprintf("unknown flag: %s\n", args[i])
			print_usage()
			os.exit(2)
		}
		i += 1
	}
	if p.threads < 1 {
		p.threads = 1
	}
	if p.batch < 1 {
		p.batch = 1
	}
	if p.reps < 1 {
		p.reps = 1
	}
	return p
}

print_usage :: proc() {
	fmt.println(`oboe bench
  -mode    ingest|query|cache|queue|all   (default all)
  -rows    rows to ingest / jobs to queue  (default 200000)
  -threads concurrent workers              (default 8)
  -batch   rows per store_ingest call      (default 500)
  -reps    query executions per worker     (default 20)
  -ops     cache operations per worker     (default 100000)
  -data    scratch directory               (default /tmp/oboe_bench)
  -keep    keep the scratch directory`)
}

// ---------------------------------------------------------------- corpus

Corpus :: struct {
	msgs:      []string,
	levels:    [4]string,
	services:  [4]string,
	zones:     [3]string,
	durations: []string,
}

corpus_make :: proc() -> Corpus {
	c := Corpus{
		msgs      = make([]string, 32),
		levels    = [4]string{"error", "info", "warn", "debug"},
		services  = [4]string{"api", "db", "cache", "auth"},
		zones     = [3]string{"a", "b", "c"},
		durations = make([]string, 256),
	}
	for i in 0..<len(c.msgs) {
		c.msgs[i] = fmt.aprintf(
			"request id=%d path=/api/v1/items/%d status=%d latency=%dms peer=10.0.%d.%d",
			i,
			i * 7,
			200 + i % 3 * 100,
			i % 250,
			i % 16,
			(i * 3) % 255,
		)
	}
	for i in 0..<len(c.durations) {
		c.durations[i] = fmt.aprintf("%d", (i * 37) % 5000)
	}
	return c
}

corpus_destroy :: proc(c: ^Corpus) {
	for s in c.msgs {
		delete(s)
	}
	delete(c.msgs)
	for s in c.durations {
		delete(s)
	}
	delete(c.durations)
	c^ = {}
}

// ---------------------------------------------------------------- shared helpers

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

// ---------------------------------------------------------------- ingest

Ingest_Args :: struct {
	s:       ^oboe.Store,
	corpus:  ^Corpus,
	start:   ^u32,
	seq:     ^u64,
	base_ts: i64,
	rows:    int,
	batch:   int,
}

ingest_worker :: proc(data: rawptr) {
	a := cast(^Ingest_Args)data
	spin_until(a.start)
	rows := make([]oboe.Ingest_Row, a.batch)
	defer delete(rows)
	fields := make([]oboe.Field, a.batch * 4)
	defer delete(fields)
	done := 0
	for done < a.rows {
		count := min(a.batch, a.rows - done)
		for b in 0..<count {
			gi := done + b
			seq := sync.atomic_add(a.seq, 1)
			ts := a.base_ts + i64(seq) * 1_000
			svc := int(gi % 4)
			fi := b * 4
			fields[fi+0] = oboe.Field{name = "level", value = a.corpus.levels[gi % 4]}
			fields[fi+1] = oboe.Field{name = "service", value = a.corpus.services[svc]}
			fields[fi+2] = oboe.Field{name = "duration", value = a.corpus.durations[gi % 256]}
			fields[fi+3] = oboe.Field{name = "zone", value = a.corpus.zones[gi % 3]}
			rows[b] = oboe.Ingest_Row{
				ts     = ts,
				stream = u64(svc + 1),
				msg    = a.corpus.msgs[gi % len(a.corpus.msgs)],
				fields = fields[fi:fi+4],
			}
		}
		oboe.store_ingest(a.s, rows[:count])
		done += count
	}
}

// fill_store ingests `rows` rows with `threads` writers and returns the wall
// time of the concurrent ingest phase.
fill_store :: proc(s: ^oboe.Store, rows, threads, batch: int) -> time.Duration {
	corpus := corpus_make()
	defer corpus_destroy(&corpus)

	start: u32
	seq: u64
	base_ts := time.to_unix_nanoseconds(time.now()) - 3_600_000_000_000
	args := make([]Ingest_Args, threads)
	workers := make([]^thread.Thread, threads)
	defer {
		delete(args)
		delete(workers)
	}
	per := rows / threads
	rem := rows % threads
	for i in 0..<threads {
		n := per
		if i < rem {
			n += 1
		}
		args[i] = Ingest_Args{
			s       = s,
			corpus  = &corpus,
			start   = &start,
			seq     = &seq,
			base_ts = base_ts,
			rows    = n,
			batch   = batch,
		}
		workers[i] = thread.create_and_start_with_data(&args[i], ingest_worker, self_cleanup = false)
	}
	begin := time.now()
	sync.atomic_store(&start, 1)
	wait_workers(workers)
	return time.since(begin)
}

run_ingest :: proc(p: Params) {
	dir := fmt.aprintf("%s/ingest", p.data)
	defer delete(dir)
	os.remove_all(dir)

	cfg := oboe.default_store_config()
	cfg.data_path = dir
	// Keep everything in memory during the timed ingest, then flush once.
	cfg.flush_interval = time.Duration(1 << 62)
	cfg.flush_max_rows = 1 << 30
	cfg.flush_max_bytes = 1 << 40
	s := oboe.store_init(cfg, false)
	defer oboe.store_close(s)

	fmt.printf("[ingest] rows=%d threads=%d batch=%d\n", p.rows, p.threads, p.batch)
	d := fill_store(s, p.rows, p.threads, p.batch)
	fmt.printf("  ingest   %.0f rows/s  %.1f ms\n", throughput(p.rows, d), millis(d))

	st := oboe.store_stats(s)
	fmt.printf("  mem      %d rows in %d partitions\n", st.mem_rows, st.partitions)

	begin := time.now()
	oboe.store_flush_all(s)
	fd := time.since(begin)
	st = oboe.store_stats(s)
	mib := f64(st.disk_bytes) / (1 << 20)
	fmt.printf("  flush    %.0f rows/s  %.1f ms  %d parts  %.1f MiB on disk\n",
		throughput(p.rows, fd), millis(fd), st.parts, mib)
}

// ---------------------------------------------------------------- query

Query_Args :: struct {
	s:      ^oboe.Store,
	start:  ^u32,
	qstr:   string,
	now:    i64,
	reps:   int,
	rows:   ^u64,
	errors: ^u32,
}

query_worker :: proc(data: rawptr) {
	a := cast(^Query_Args)data
	q, ok := oboe.parse_query_string(a.qstr, a.now)
	if !ok {
		sync.atomic_add(a.errors, 1)
		return
	}
	spin_until(a.start)
	for _ in 0..<a.reps {
		r := oboe.execute_query(a.s, &q, 100)
		sync.atomic_add(a.rows, u64(len(r.rows)))
		oboe.result_destroy(&r)
	}
	oboe.query_destroy(&q)
}

run_query :: proc(p: Params) {
	dir := fmt.aprintf("%s/query", p.data)
	defer delete(dir)
	os.remove_all(dir)

	cfg := oboe.default_store_config()
	cfg.data_path = dir
	cfg.flush_interval = time.Duration(1 << 62)
	cfg.flush_max_rows = 1 << 30
	cfg.flush_max_bytes = 1 << 40
	s := oboe.store_init(cfg, false)
	defer oboe.store_close(s)

	fill_store(s, p.rows, p.threads, p.batch)
	oboe.store_flush_all(s)
	now := time.to_unix_nanoseconds(time.now())

	qstrs := []string{
		"level:error | limit 100",
		"service:api and level:error | limit 100",
		"level:error | stats by (service) count() as total",
		"* | sort by (_time) desc | limit 100",
	}

	fmt.printf("[query] rows=%d threads=%d reps=%d\n", p.rows, p.threads, p.reps)
	for qstr in qstrs {
		start: u32
		rows_out: u64
		errors: u32
		args := make([]Query_Args, p.threads)
		workers := make([]^thread.Thread, p.threads)
		for i in 0..<p.threads {
			args[i] = Query_Args{
				s      = s,
				start  = &start,
				qstr   = qstr,
				now    = now,
				reps   = p.reps,
				rows   = &rows_out,
				errors = &errors,
			}
			workers[i] = thread.create_and_start_with_data(&args[i], query_worker, self_cleanup = false)
		}
		begin := time.now()
		sync.atomic_store(&start, 1)
		wait_workers(workers)
		d := time.since(begin)
		delete(args)
		delete(workers)

		total := p.threads * p.reps
		avg_rows := f64(rows_out) / f64(total)
		fmt.printf("  %s  %.1f q/s  %.3f ms/op  %.0f rows  errors=%d\n",
			qstr,
			throughput(total, d),
			millis(d) / f64(total),
			avg_rows,
			errors)
	}
}

// ---------------------------------------------------------------- cache

CACHE_KEYS :: 8192

Cache_Args :: struct {
	c:     ^oboe.Cache,
	start: ^u32,
	keys:  []string,
	value: []u8,
	ops:   int,
	idx:   int,
	done:  ^u64,
}

cache_worker :: proc(data: rawptr) {
	a := cast(^Cache_Args)data
	buf := oboe.buf_init(256)
	defer oboe.buf_destroy(&buf)
	spin_until(a.start)
	ki := a.idx % len(a.keys)
	for i in 0..<a.ops {
		k := a.keys[ki]
		if i % 10 == 0 {
			oboe.cache_set(a.c, k, a.value, 0)
		} else {
			oboe.buf_reset(&buf)
			oboe.cache_get_into(a.c, k, &buf)
		}
		if i % 97 == 0 {
			oboe.cache_delete(a.c, k)
		}
		ki += 1
		if ki == len(a.keys) {
			ki = 0
		}
	}
	sync.atomic_add(a.done, u64(a.ops))
}

run_cache :: proc(p: Params) {
	cfg := oboe.default_cache_config()
	cfg.max_bytes = 0
	cfg.max_entries = 0
	c := oboe.cache_init(cfg)
	defer oboe.cache_destroy(c)

	keys := make([]string, CACHE_KEYS)
	defer {
		for k in keys {
			delete(k)
		}
		delete(keys)
	}
	for i in 0..<CACHE_KEYS {
		keys[i] = fmt.aprintf("bench:key:%d", i)
	}
	value := make([]u8, 128)
	defer delete(value)
	for i in 0..<len(value) {
		value[i] = u8('a' + i % 26)
	}
	// Prefill so the measured mix is dominated by the read path.
	for k in keys {
		oboe.cache_set(c, k, value, 0)
	}

	start: u32
	done: u64
	total_ops := p.threads * p.ops
	args := make([]Cache_Args, p.threads)
	workers := make([]^thread.Thread, p.threads)
	defer {
		delete(args)
		delete(workers)
	}
	for i in 0..<p.threads {
		args[i] = Cache_Args{
			c     = c,
			start = &start,
			keys  = keys,
			value = value,
			ops   = p.ops,
			idx   = i,
			done  = &done,
		}
		workers[i] = thread.create_and_start_with_data(&args[i], cache_worker, self_cleanup = false)
	}
	fmt.printf("[cache] threads=%d ops/thread=%d keys=%d\n", p.threads, p.ops, CACHE_KEYS)
	begin := time.now()
	sync.atomic_store(&start, 1)
	wait_workers(workers)
	d := time.since(begin)

	st := oboe.cache_stats(c)
	fmt.printf("  cache    %.0f ops/s  %.1f ms  entries=%d hits=%d misses=%d sets=%d deletes=%d\n",
		throughput(total_ops, d), millis(d), st.entries, st.hits, st.misses, st.sets, st.deletes)
}

// ---------------------------------------------------------------- queue

Queue_Prod_Args :: struct {
	q:      ^oboe.Queue,
	start:  ^u32,
	body:   []u8,
	jobs:   int,
	put_ok: ^u64,
	failed: ^u64,
}

queue_producer :: proc(data: rawptr) {
	a := cast(^Queue_Prod_Args)data
	spin_until(a.start)
	for _ in 0..<a.jobs {
		_, res := oboe.queue_put(a.q, "bench", 0, 0, i64(30_000_000_000), a.body)
		if res == .OK {
			sync.atomic_add(a.put_ok, 1)
		} else {
			sync.atomic_add(a.failed, 1)
		}
	}
}

Queue_Cons_Args :: struct {
	q:        ^oboe.Queue,
	start:    ^u32,
	total:    int,
	consumed: ^u64,
}

queue_consumer :: proc(data: rawptr) {
	a := cast(^Queue_Cons_Args)data
	conn := oboe.queue_new_conn(a.q)
	oboe.queue_conn_opened(a.q)
	watched := []string{"bench"}
	buf := oboe.buf_init(256)
	defer oboe.buf_destroy(&buf)
	empty := make([]u64, 0)
	defer delete(empty)
	defer oboe.queue_conn_closed(a.q, "bench", watched, empty)

	spin_until(a.start)
	for sync.atomic_load(a.consumed) < u64(a.total) {
		id, ok := oboe.queue_reserve_into(a.q, watched, conn, i64(50_000_000), &buf)
		if ok {
			oboe.queue_delete(a.q, id, conn)
			sync.atomic_add(a.consumed, 1)
		}
	}
}

run_queue :: proc(p: Params) {
	cfg := oboe.default_queue_config()
	cfg.max_jobs = p.rows + 1024
	q := oboe.queue_init(cfg)
	defer oboe.queue_destroy(q)

	producers := max(1, p.threads / 2)
	consumers := max(1, p.threads - producers)
	if producers == 0 {
		producers = 1
	}
	body_str := "bench job payload: index=0 method=POST path=/api/v1/items status=200 latency=12ms trace=0123456789abcdef"
	body := transmute([]u8)body_str

	start: u32
	put_ok: u64
	failed: u64
	consumed: u64

	prod_args := make([]Queue_Prod_Args, producers)
	prod_workers := make([]^thread.Thread, producers)
	cons_args := make([]Queue_Cons_Args, consumers)
	cons_workers := make([]^thread.Thread, consumers)
	defer {
		delete(prod_args)
		delete(prod_workers)
		delete(cons_args)
		delete(cons_workers)
	}

	per := p.rows / producers
	rem := p.rows % producers
	for i in 0..<producers {
		n := per
		if i < rem {
			n += 1
		}
		prod_args[i] = Queue_Prod_Args{
			q      = q,
			start  = &start,
			body   = body,
			jobs   = n,
			put_ok = &put_ok,
			failed = &failed,
		}
		prod_workers[i] = thread.create_and_start_with_data(&prod_args[i], queue_producer, self_cleanup = false)
	}
	for i in 0..<consumers {
		cons_args[i] = Queue_Cons_Args{
			q        = q,
			start    = &start,
			total    = p.rows,
			consumed = &consumed,
		}
		cons_workers[i] = thread.create_and_start_with_data(&cons_args[i], queue_consumer, self_cleanup = false)
	}

	fmt.printf("[queue] jobs=%d producers=%d consumers=%d\n", p.rows, producers, consumers)
	begin := time.now()
	sync.atomic_store(&start, 1)
	wait_workers(prod_workers)
	wait_workers(cons_workers)
	d := time.since(begin)

	st := oboe.queue_stats(q)
	fmt.printf("  queue    %.0f jobs/s  %.1f ms  put=%d consumed=%d failed=%d ready=%d reserved=%d\n",
		throughput(int(consumed), d), millis(d), put_ok, consumed, failed,
		st.ready, st.reserved)
}

// ---------------------------------------------------------------- main

main :: proc() {
	p := parse_params()
	defer {
		if !p.keep {
			os.remove_all(p.data)
		}
	}

	switch p.mode {
	case "ingest":
		run_ingest(p)
	case "query":
		run_query(p)
	case "cache":
		run_cache(p)
	case "queue":
		run_queue(p)
	case "all":
		run_ingest(p)
		fmt.println()
		run_query(p)
		fmt.println()
		run_cache(p)
		fmt.println()
		run_queue(p)
	case:
		fmt.eprintf("unknown mode: %s\n", p.mode)
		print_usage()
		os.exit(2)
	}
}
