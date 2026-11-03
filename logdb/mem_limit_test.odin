package logdb

// The memtable cap is the only thing that bounds RSS when ingest outruns flush.
// These tests cover both halves of the contract: a capped store under
// concurrent ingest + flush (nothing lost, nothing duplicated, the cap holds),
// and shutdown while a writer is blocked on the cap (the waiter must wake
// without sitting out its whole budget).
import "../common"
import "base:runtime"
import "core:os"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"

CAP_TEST_THREADS :: 6
CAP_TEST_BATCHES :: 30
CAP_TEST_BATCH :: 40
CAP_TEST_ROWS :: CAP_TEST_THREADS * CAP_TEST_BATCHES * CAP_TEST_BATCH
CAP_TEST_MAX_ROWS :: 200
CAP_TEST_MAX_ATTEMPTS :: 5000

Cap_Test_Writer :: struct {
	s:        ^Store,
	start:    ^u32,
	base:     i64,
	tid:      int,
	accepted: u64,
	// Set once the writer sees stop and gives up, so a shutdown test can prove
	// it drained instead of sitting in the cap wait.
	drained_ns: u64,
	done:     ^u32,
}

cap_test_writer :: proc(data: rawptr) {
	a := cast(^Cap_Test_Writer)data
	for sync.atomic_load(a.start) == 0 {}
	fields := []Field{{name = "level", value = "info"}, {name = "service", value = "api"}}
	rows: [CAP_TEST_BATCH]Ingest_Row
	for b in 0..<CAP_TEST_BATCHES {
		for i in 0..<CAP_TEST_BATCH {
			n := a.tid * CAP_TEST_BATCHES * CAP_TEST_BATCH + b * CAP_TEST_BATCH + i
			rows[i] = Ingest_Row{
				ts     = a.base + i64(n) * 1000,
				stream = u64(n % 7),
				msg    = "cap row",
				fields = fields,
			}
		}
		// Retry a rejected batch: the cap is a queue of work, not a drop.
		for _ in 0..<CAP_TEST_MAX_ATTEMPTS {
			accepted, _, rejected := store_ingest_bounded(a.s, rows[:], 1 * time.Second)
			if !rejected {
				a.accepted += u64(accepted)
				break
			}
			if sync.atomic_load(&a.s.stop) != 0 {
				sync.atomic_add(a.done, 1)
				return
			}
			time.sleep(200 * time.Microsecond)
		}
	}
	sync.atomic_add(a.done, 1)
}

Cap_Test_Flusher :: struct {
	s:     ^Store,
	start: ^u32,
	stop:  ^u32,
	peak:  ^u64
}

cap_test_flusher :: proc(data: rawptr) {
	a := cast(^Cap_Test_Flusher)data
	for sync.atomic_load(a.start) == 0 {}
	for sync.atomic_load(a.stop) == 0 {
		used := sync.atomic_load(&a.s.mem_rows)
		if used > sync.atomic_load(a.peak) {
			// Sampled peak, not a linearisable maximum: good enough to prove the
			// cap holds, and it cannot hide a big overshoot for a whole flush cycle.
			sync.atomic_store(a.peak, used)
		}
		// Slower than the writers on purpose so the cap actually binds.
		time.sleep(1 * time.Millisecond)
		store_flush_due(a.s)
		store_flush_all(a.s)
	}
}

@(test)
test_store_mem_cap_concurrent_ingest_flush :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	dir := sm_tmpdir("store_mem_cap")
	os.remove_all(dir)
	defer os.remove_all(dir)

	cfg := sm_store_config(dir)
	cfg.block_rows = 64
	cfg.flush_max_rows = 300
	cfg.flush_max_bytes = 1 << 40
	cfg.flush_interval = time.Duration(1 << 62)
	cfg.merge_threshold = 1 << 20
	cfg.ingest_shards = 4
	cfg.mem_max_rows = CAP_TEST_MAX_ROWS
	cfg.mem_max_bytes = 0
	cfg.mem_wait = 1 * time.Second
	s := store_init(cfg, false)

	base, _ := common.parse_iso_ns("2026-10-26T00:00:00Z")
	start: u32
	stop: u32
	done: u32
	peak: u64
	peak_ptr := &peak

	fargs := Cap_Test_Flusher{s = s, start = &start, stop = &stop, peak = peak_ptr}
	flusher := thread.create_and_start_with_data(&fargs, cap_test_flusher, self_cleanup = false)
	wargs := make([]Cap_Test_Writer, CAP_TEST_THREADS)
	writers := make([]^thread.Thread, CAP_TEST_THREADS)
	defer {
		delete(wargs)
		delete(writers)
		thread.destroy(flusher)
	}
	for i in 0..<CAP_TEST_THREADS {
		wargs[i] = Cap_Test_Writer{s = s, start = &start, base = base, tid = i, done = &done}
		writers[i] = thread.create_and_start_with_data(&wargs[i], cap_test_writer, self_cleanup = false)
	}
	sync.atomic_store(&start, 1)
	stop_at := time.now()
	for w in writers {
		thread.join(w)
		thread.destroy(w)
	}
	sync.atomic_store(&stop, 1)
	thread.join(flusher)
	elapsed := time.since(stop_at)

	accepted: u64
	for &w in wargs {
		accepted += w.accepted
	}
	testing.expect_value(t, accepted, u64(CAP_TEST_ROWS))
	testing.expect_value(t, int(sync.atomic_load(&s.ingested_rows)), CAP_TEST_ROWS)
	// The reservation is all-or-nothing, so the accounting may never exceed the
	// cap even though a flush is subtracting concurrently.
	testing.expectf(t, peak <= CAP_TEST_MAX_ROWS, "memtable peaked at %d rows, cap is %d", peak, CAP_TEST_MAX_ROWS)
	testing.expectf(t, elapsed < 30 * time.Second, "capped ingest took %v", elapsed)
	testing.expectf(t, sync.atomic_load(&s.ingest_stalled_ns) > 0, "no stall was recorded: the cap never bound")

	store_flush_all(s)
	st := store_stats(s)
	testing.expect_value(t, st.mem_rows, 0)
	testing.expect_value(t, st.rows, CAP_TEST_ROWS)

	// Every row exactly once.
	seen := make(map[i64]int, CAP_TEST_ROWS)
	defer delete(seen)
	counted := 0
	for p in s.partitions {
		for part in p.parts {
			for &bi in part.index {
				d, ok := read_block(part, &bi)
				if !ok {
					continue
				}
				for r in d.rows {
					seen[r.ts] += 1
					counted += 1
				}
				decoded_destroy(&d)
			}
		}
	}
	duplicates := 0
	for _, n in seen {
		if n > 1 {
			duplicates += 1
		}
	}
	testing.expect_value(t, counted, CAP_TEST_ROWS)
	testing.expect_value(t, len(seen), CAP_TEST_ROWS)
	testing.expect_value(t, duplicates, 0)

	close_at := time.now()
	store_close(s)
	close_ns := time.since(close_at)
	testing.expectf(t, close_ns < 2 * time.Second, "store_close took %v with a %d row cap", close_ns, CAP_TEST_MAX_ROWS)
}

// A full cap with no flush to free it must produce a bounded wait and a
// rejection, not an unbounded hang. This is the store-level contract that the
// HTTP layer turns into 503 + Retry-After.
@(test)
test_store_mem_cap_rejects_after_bounded_wait :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	dir := sm_tmpdir("store_mem_cap_reject")
	os.remove_all(dir)
	defer os.remove_all(dir)

	cfg := sm_store_config(dir)
	cfg.block_rows = 64
	cfg.flush_interval = time.Duration(1 << 62)
	cfg.merge_threshold = 1 << 20
	cfg.mem_max_rows = 100
	cfg.mem_max_bytes = 0
	s := store_init(cfg, false)

	fields := []Field{{name = "level", value = "info"}}
	base, _ := common.parse_iso_ns("2026-10-26T00:00:00Z")
	fill := make([]Ingest_Row, 100)
	defer delete(fill)
	for &r, i in fill {
		r = Ingest_Row{ts = base + i64(i) * 1000, stream = 1, msg = "fill row", fields = fields}
	}
	accepted, _, rejected := store_ingest_bounded(s, fill[:], 1 * time.Second)
	testing.expect_value(t, accepted, 100)
	testing.expect(t, !rejected)

	more := make([]Ingest_Row, 40)
	defer delete(more)
	for &r, i in more {
		r = Ingest_Row{ts = base + i64(100 + i) * 1000, stream = 1, msg = "fill row", fields = fields}
	}
	budget := 200 * time.Millisecond
	t_wait := time.now()
	accepted2, stalled_ns, rejected2 := store_ingest_bounded(s, more[:], budget)
	waited := time.since(t_wait)
	testing.expect_value(t, accepted2, 0)
	testing.expectf(t, rejected2, "a capped store with no flusher must reject")
	testing.expectf(t, stalled_ns > 0, "the rejection must record the wait time")
	// Bounded: it must not wait meaningfully longer than the budget.
	testing.expectf(t, waited >= budget && waited < budget + 500 * time.Millisecond, "wait was %v", waited)
	testing.expect_value(t, sync.atomic_load(&s.mem_limit_rejected), u64(1))
	testing.expect_value(t, int(sync.atomic_load(&s.mem_rows)), 100)

	store_flush_all(s)
	testing.expect_value(t, store_stats(s).mem_rows, 0)
	testing.expect_value(t, store_stats(s).rows, 100)
	store_close(s)
}

// Shutdown must not wait out the cap budget: store_close wakes every blocked
// ingest, so a writer parked with a 5 s wait returns right away.
@(test)
test_store_mem_cap_shutdown_wakes_waiters :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	dir := sm_tmpdir("store_mem_cap_shutdown")
	os.remove_all(dir)
	defer os.remove_all(dir)

	cfg := sm_store_config(dir)
	cfg.block_rows = 64
	cfg.flush_max_rows = 300
	cfg.flush_max_bytes = 1 << 40
	cfg.flush_interval = time.Duration(1 << 62)
	cfg.merge_threshold = 1 << 20
	cfg.ingest_shards = 4
	cfg.mem_max_rows = 100
	cfg.mem_max_bytes = 0
	cfg.mem_wait = 5 * time.Second
	s := store_init(cfg, false)

	// Fill the cap with no flusher running so the next batch has to wait.
	fields := []Field{{name = "level", value = "info"}}
	rows := make([]Ingest_Row, 100)
	defer delete(rows)
	base, _ := common.parse_iso_ns("2026-10-26T00:00:00Z")
	for &r, i in rows {
		r = Ingest_Row{ts = base + i64(i) * 1000, stream = 1, msg = "shutdown row", fields = fields}
	}
	accepted, _, rejected := store_ingest_bounded(s, rows[:], 1 * time.Second)
	testing.expect_value(t, accepted, 100)
	testing.expect(t, !rejected)

	blocked := make([]Ingest_Row, 40)
	defer delete(blocked)
	for &r, i in blocked {
		r = Ingest_Row{ts = base + i64(100 + i) * 1000, stream = 1, msg = "shutdown row", fields = fields}
	}
	start: u32
	done: u32
	wargs2 := Shutdown_Writer{s = s, start = &start, done = &done, rows = blocked}
	w := thread.create_and_start_with_data(&wargs2, shutdown_writer, self_cleanup = false)
	sync.atomic_store(&start, 1)
	// Give it time to reach the cap wait, then shut down.
	time.sleep(100 * time.Millisecond)
	testing.expectf(t, sync.atomic_load(&done) == 0, "writer finished before the shutdown test started")

	// This is exactly the first two steps store_close takes; doing them by hand
	// keeps the store alive until the writer has drained, which store_close
	// cannot guarantee because it frees the store.
	wake_at := time.now()
	sync.atomic_store(&s.stop, 1)
	wake_mem_waiters(s)
	thread.join(w)
	thread.destroy(w)
	wake_ns := time.since(wake_at)
	testing.expectf(t, sync.atomic_load(&done) == 1, "writer did not drain on shutdown")
	testing.expectf(t, wake_ns < 2 * time.Second, "a cap-blocked writer took %v to drain", wake_ns)

	store_close(s)
}

Shutdown_Writer :: struct {
	s:     ^Store,
	start: ^u32,
	done:  ^u32,
	rows:  []Ingest_Row
}

shutdown_writer :: proc(data: rawptr) {
	a := cast(^Shutdown_Writer)data
	for sync.atomic_load(a.start) == 0 {}
	// One blocking attempt: it must return as soon as the store stops, not after
	// the 5 s budget.
	_, _, rejected := store_ingest_bounded(a.s, a.rows, 5 * time.Second)
	_ = rejected
	sync.atomic_add(a.done, 1)
}
