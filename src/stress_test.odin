package oboe

// Concurrency stress tests. Excluded from the default run; enable with:
//
//   odin test src -define:ODIN_STRESS=1
import "core:fmt"
import "core:os"
import "base:runtime"
import "core:strings"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"

STRESS :: #config(ODIN_STRESS, 0)

when STRESS != 0 {
	STRESS_THREADS :: 8
STRESS_ROWS :: 10_000
STRESS_BATCH :: 200
STRESS_CACHE_KEYS :: 64
STRESS_QUEUE_PRODUCERS :: 4
STRESS_QUEUE_JOBS :: 5_000

stress_spin_until :: #force_inline proc(flag: ^u32) {
	for sync.atomic_load(flag) == 0 {
	}
}

stress_join_all :: proc(workers: []^thread.Thread) {
	for w in workers {
		thread.join(w)
		thread.destroy(w)
	}
}

// ------------------------------------------------------------ store

Stress_Writer_Args :: struct {
	s:        ^Store,
	start:    ^u32,
	base_ts:  i64,
	tid:      int,
	rows:     int,
	batch:    int,
	failures: ^u32,
}

stress_writer :: proc(data: rawptr) {
	a := cast(^Stress_Writer_Args)data
	stress_spin_until(a.start)
	levels := [2]string{"error", "info"}
	services := [4]string{"api", "db", "cache", "auth"}
	rows := make([]Ingest_Row, a.batch)
	defer delete(rows)
	fields := make([]Field, a.batch * 3)
	defer delete(fields)
	done := 0
	for done < a.rows {
		n := min(a.batch, a.rows - done)
		for b in 0..<n {
			i := done + b
			fi := b * 3
			fields[fi + 0] = Field{name = "level", value = levels[i % 3 == 0 ? 0 : 1]}
			fields[fi + 1] = Field{name = "service", value = services[i % 4]}
			fields[fi + 2] = Field{name = "zone", value = "z"}
			rows[b] = Ingest_Row{
				ts     = a.base_ts + i64(i) * 1_000 + i64(a.tid),
				stream = u64(a.tid + 1),
				msg    = "stress request completion event",
				fields = fields[fi:fi + 3],
			}
		}
		if store_ingest(a.s, rows[:n]) != n {
			sync.atomic_add(a.failures, 1)
		}
		done += n
	}
}

Stress_Reader_Args :: struct {
	s:          ^Store,
	start:      ^u32,
	stop:       ^u32,
	now:        i64,
	iterations: ^u64,
	failures:   ^u32,
}

stress_reader :: proc(data: rawptr) {
	a := cast(^Stress_Reader_Args)data
	stress_spin_until(a.start)
	queries := []string{"* | limit 10", "level:error | limit 5"}
	for sync.atomic_load(a.stop) == 0 {
		for qs in queries {
			q, ok := parse_query_string(qs, a.now)
			if !ok {
				sync.atomic_add(a.failures, 1)
				continue
			}
			r := execute_query(a.s, &q, 100)
			// Readers start before the first rows land, so fewer than 10 rows
			// is expected early on; more than the limit is a bug.
			if qs == queries[0] && len(r.rows) > 10 {
				sync.atomic_add(a.failures, 1)
			}
			query_destroy(&q)
			result_destroy(&r)
		}
		sync.atomic_add(a.iterations, 1)
		time.sleep(50 * time.Microsecond)
	}
}

Stress_Flusher_Args :: struct {
	s:        ^Store,
	start:    ^u32,
	stop:     ^u32,
	failures: ^u32,
}

stress_flusher :: proc(data: rawptr) {
	a := cast(^Stress_Flusher_Args)data
	stress_spin_until(a.start)
	for sync.atomic_load(a.stop) == 0 {
		store_flush_due(a.s)
		store_merge_all(a.s)
		time.sleep(time.Millisecond)
	}
	store_flush_due(a.s)
}

stress_expect_count :: proc(t: ^testing.T, s: ^Store, now: i64, qstr: string, expected: int) {
	q, ok := parse_query_string(qstr, now)
	if !ok {
		testing.expectf(t, false, "%s: query did not parse", qstr)
		return
	}
	defer query_destroy(&q)
	r := execute_query(s, &q, 100)
	defer result_destroy(&r)
	if len(r.rows) != 1 {
		testing.expectf(t, false, "%s: got %d result rows, want 1 (matched=%d)", qstr, len(r.rows), r.matched_rows)
		return
	}
	total, _ := out_row_get(&r.rows[0], "total")
	want := fmt.tprintf("%d", expected)
	testing.expectf(t, total == want, "%s: total=%s want %s (matched=%d scanned=%d)", qstr, total, want, r.matched_rows, r.scanned_rows)
}

@(test)
test_stress_store_concurrent_ingest_flush_query :: proc(t: ^testing.T) {
	// The test runner's per-task allocator is not thread-safe, and these
	// structures are shared with worker threads, so use the process heap.
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	dir := "/tmp/oboe_stress_store"
	os.remove_all(dir)
	cfg := default_store_config()
	cfg.data_path = dir
	cfg.flush_interval = time.Duration(1 << 62)
	cfg.flush_max_rows = 2_000
	cfg.flush_max_bytes = 1 << 40
	cfg.block_rows = 512
	cfg.merge_threshold = 4
	cfg.merge_max_rows = 1_000_000
	s := store_init(cfg, false)
	defer {
		store_close(s)
		os.remove_all(dir)
	}

	start: u32
	flusher_stop: u32
	reader_stop: u32
	writer_failures: u32
	reader_failures: u32
	iterations: u64
	base_ts := time.to_unix_nanoseconds(time.now()) - 3_600_000_000_000
	now := time.to_unix_nanoseconds(time.now())

	wargs := make([]Stress_Writer_Args, STRESS_THREADS)
	writers := make([]^thread.Thread, STRESS_THREADS)
	rargs := make([]Stress_Reader_Args, STRESS_THREADS)
	readers := make([]^thread.Thread, STRESS_THREADS)
	fargs := Stress_Flusher_Args{s = s, start = &start, stop = &flusher_stop, failures = &reader_failures}
	defer {
		delete(wargs)
		delete(writers)
		delete(rargs)
		delete(readers)
	}

	for i in 0..<STRESS_THREADS {
		wargs[i] = Stress_Writer_Args{
			s        = s,
			start    = &start,
			base_ts  = base_ts,
			tid      = i,
			rows     = STRESS_ROWS,
			batch    = STRESS_BATCH,
			failures = &writer_failures,
		}
		writers[i] = thread.create_and_start_with_data(&wargs[i], stress_writer, self_cleanup = false)
		rargs[i] = Stress_Reader_Args{
			s          = s,
			start      = &start,
			stop       = &reader_stop,
			now        = now,
			iterations = &iterations,
			failures   = &reader_failures,
		}
		readers[i] = thread.create_and_start_with_data(&rargs[i], stress_reader, self_cleanup = false)
	}
	flusher := thread.create_and_start_with_data(&fargs, stress_flusher, self_cleanup = false)

	sync.atomic_store(&start, 1)
	stress_join_all(writers[:])
	sync.atomic_store(&reader_stop, 1)
	sync.atomic_store(&flusher_stop, 1)
	stress_join_all(readers[:])
	thread.join(flusher)
	thread.destroy(flusher)

	testing.expect_value(t, writer_failures, u32(0))
	testing.expect_value(t, reader_failures, u32(0))
	testing.expectf(t, iterations > 0, "readers never completed a query")

	store_flush_all(s)
	st := store_stats(s)
	total := STRESS_THREADS * STRESS_ROWS
	testing.expect_value(t, st.mem_rows, 0)
	testing.expect_value(t, st.rows, total)
	testing.expectf(t, st.parts > 0, "no parts were written")

	errors, api, errors_db: int
	for i in 0..<STRESS_ROWS {
		if i % 3 == 0 {
			errors += 1
		}
		if i % 4 == 0 {
			api += 1
		}
		if i % 3 == 0 && i % 4 == 1 {
			errors_db += 1
		}
	}
	stress_expect_count(t, s, now, "* | stats count() as total", total)
	stress_expect_count(t, s, now, "level:error | stats count() as total", errors * STRESS_THREADS)
	stress_expect_count(t, s, now, "level:info | stats count() as total", (STRESS_ROWS - errors) * STRESS_THREADS)
	stress_expect_count(t, s, now, "service:api | stats count() as total", api * STRESS_THREADS)
	stress_expect_count(t, s, now, "level:error and service:db | stats count() as total", errors_db * STRESS_THREADS)
}

// ------------------------------------------------------------ cache

Stress_Cache_Args :: struct {
	c:        ^Cache,
	start:    ^u32,
	tid:      int,
	failures: ^u32,
}

stress_cache_worker :: proc(data: rawptr) {
	a := cast(^Stress_Cache_Args)data
	stress_spin_until(a.start)
	buf := buf_init(128)
	defer buf_destroy(&buf)
	keys := make([]string, STRESS_CACHE_KEYS)
	vals := make([]string, STRESS_CACHE_KEYS)
	defer {
		for k in keys {
			delete(k)
		}
		for v in vals {
			delete(v)
		}
		delete(keys)
		delete(vals)
	}
	for k in 0..<len(keys) {
		keys[k] = fmt.aprintf("stress:cache:%d:%d", a.tid, k)
		vals[k] = fmt.aprintf("value-%d-%d", a.tid, k)
	}
	for _ in 0..<4 {
		for k in 0..<len(keys) {
			if !cache_set(a.c, keys[k], transmute([]u8)vals[k], 0) {
				sync.atomic_add(a.failures, 1)
			}
			buf_reset(&buf)
			if !cache_get_into(a.c, keys[k], &buf) || string(buf.data[:]) != vals[k] {
				sync.atomic_add(a.failures, 1)
			}
		}
		for k in 0..<len(keys) {
			if k % 2 == 0 {
				cache_delete(a.c, keys[k])
			}
		}
	}
	for k in 0..<len(keys) {
		buf_reset(&buf)
		found := cache_get_into(a.c, keys[k], &buf)
		if k % 2 == 0 {
			if found {
				sync.atomic_add(a.failures, 1)
			}
		} else if !found || string(buf.data[:]) != vals[k] {
			sync.atomic_add(a.failures, 1)
		}
	}
}

@(test)
test_stress_cache_concurrent :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	c := cache_init(default_cache_config())
	defer cache_destroy(c)

	shared_key := "stress:cache:shared"
	shared_val := "shared-value"
	testing.expect(t, cache_set(c, shared_key, transmute([]u8)string(shared_val), 0))

	start: u32
	failures: u32
	args := make([]Stress_Cache_Args, STRESS_THREADS)
	workers := make([]^thread.Thread, STRESS_THREADS)
	defer {
		delete(args)
		delete(workers)
	}
	for i in 0..<STRESS_THREADS {
		args[i] = Stress_Cache_Args{c = c, start = &start, tid = i, failures = &failures}
		workers[i] = thread.create_and_start_with_data(&args[i], stress_cache_worker, self_cleanup = false)
	}
	sync.atomic_store(&start, 1)
	stress_join_all(workers[:])
	testing.expect_value(t, failures, u32(0))

	v, found := cache_get(c, shared_key)
	testing.expect(t, found)
	if found {
		testing.expect_value(t, string(v), shared_val)
		delete(v)
	}

	ttl_key := "stress:cache:ttl"
	testing.expect(t, cache_set(c, ttl_key, transmute([]u8)string("short"), i64(30 * time.Millisecond)))
	time.sleep(60 * time.Millisecond)
	if v, ok := cache_get(c, ttl_key); ok {
		testing.expectf(t, false, "ttl entry survived expiration")
		delete(v)
	}
}

// ------------------------------------------------------------ queue

Stress_Queue_Prod_Args :: struct {
	q:      ^Queue,
	start:  ^u32,
	pid:    int,
	jobs:   int,
	put_ok: ^u64,
	failed: ^u64,
}

stress_queue_producer :: proc(data: rawptr) {
	a := cast(^Stress_Queue_Prod_Args)data
	stress_spin_until(a.start)
	buf: [64]u8
	for i in 0..<a.jobs {
		body := fmt.bprintf(buf[:], "stress-job:%d:%d", a.pid, i)
		_, res := queue_put(a.q, "stress", 0, 0, i64(30 * time.Second), transmute([]u8)body)
		if res == .OK {
			sync.atomic_add(a.put_ok, 1)
		} else {
			sync.atomic_add(a.failed, 1)
		}
	}
}

Stress_Queue_Cons_Args :: struct {
	q:              ^Queue,
	start:          ^u32,
	producers_done: ^u32,
	total:          int,
	consumed:       ^u64,
	seen:           []u32,
	failures:       ^u32,
}

stress_queue_consumer :: proc(data: rawptr) {
	a := cast(^Stress_Queue_Cons_Args)data
	conn := queue_new_conn(a.q)
	queue_conn_opened(a.q)
	watched := []string{"stress"}
	buf := buf_init(128)
	defer buf_destroy(&buf)
	empty := make([]u64, 0)
	defer delete(empty)
	defer queue_conn_closed(a.q, "stress", watched, empty)

	stress_spin_until(a.start)
	idle := 0
	for sync.atomic_load(a.consumed) < u64(a.total) {
		id, ok := queue_reserve_into(a.q, watched, conn, i64(20 * time.Millisecond), &buf)
		if !ok {
			if sync.atomic_load(a.producers_done) == 1 {
				idle += 1
				if idle > 200 {
					sync.atomic_add(a.failures, 1)
					break
				}
			}
			continue
		}
		idle = 0
		if !strings.has_prefix(string(buf.data[:]), "stress-job:") {
			sync.atomic_add(a.failures, 1)
		}
		if id < u64(len(a.seen)) {
			sync.atomic_add(&a.seen[id], 1)
		} else {
			sync.atomic_add(a.failures, 1)
		}
		if !queue_delete(a.q, id, conn) {
			sync.atomic_add(a.failures, 1)
		}
		sync.atomic_add(a.consumed, 1)
	}
}

@(test)
test_stress_queue_concurrent :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	total := STRESS_QUEUE_PRODUCERS * STRESS_QUEUE_JOBS
	cfg := default_queue_config()
	cfg.max_jobs = total + 1024
	q := queue_init(cfg)
	defer queue_destroy(q)

	start: u32
	producers_done: u32
	put_ok: u64
	failed: u64
	consumed: u64
	failures: u32
	seen := make([]u32, total + 2)
	defer delete(seen)

	pargs := make([]Stress_Queue_Prod_Args, STRESS_QUEUE_PRODUCERS)
	producers := make([]^thread.Thread, STRESS_QUEUE_PRODUCERS)
	cargs := make([]Stress_Queue_Cons_Args, STRESS_QUEUE_PRODUCERS)
	consumers := make([]^thread.Thread, STRESS_QUEUE_PRODUCERS)
	defer {
		delete(pargs)
		delete(producers)
		delete(cargs)
		delete(consumers)
	}

	for i in 0..<STRESS_QUEUE_PRODUCERS {
		pargs[i] = Stress_Queue_Prod_Args{
			q      = q,
			start  = &start,
			pid    = i,
			jobs   = STRESS_QUEUE_JOBS,
			put_ok = &put_ok,
			failed = &failed,
		}
		producers[i] = thread.create_and_start_with_data(&pargs[i], stress_queue_producer, self_cleanup = false)
		cargs[i] = Stress_Queue_Cons_Args{
			q              = q,
			start          = &start,
			producers_done = &producers_done,
			total          = total,
			consumed       = &consumed,
			seen           = seen,
			failures       = &failures,
		}
		consumers[i] = thread.create_and_start_with_data(&cargs[i], stress_queue_consumer, self_cleanup = false)
	}

	sync.atomic_store(&start, 1)
	stress_join_all(producers[:])
	sync.atomic_store(&producers_done, 1)
	stress_join_all(consumers[:])

	testing.expect_value(t, put_ok, u64(total))
	testing.expect_value(t, failed, u64(0))
	testing.expect_value(t, failures, u32(0))
	testing.expect_value(t, consumed, u64(total))

	duplicates := 0
	missing := 0
	for id in 1..=total {
		switch seen[id] {
		case 1:
		case 0:
			missing += 1
		case:
			duplicates += 1
		}
	}
	testing.expectf(t, missing == 0, "%d jobs were never consumed", missing)
	testing.expectf(t, duplicates == 0, "%d jobs were consumed more than once", duplicates)
	testing.expect_value(t, seen[total + 1], u32(0))

	st := queue_stats(q)
	testing.expect_value(t, st.current_jobs, 0)
	testing.expect_value(t, st.ready, 0)
	testing.expect_value(t, st.reserved, 0)
}

}
