package logdb

// The memtable is striped into independently locked shards, so ingest batches
// append without excluding each other while a flush steals shards underneath
// them. This test hammers that pair: concurrent ingest vs a flusher calling
// store_flush_due / store_flush_all, then checks that every row survived
// exactly once.
import "../common"
import "base:runtime"
import "core:os"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"

SHARD_TEST_THREADS :: 6
SHARD_TEST_BATCHES :: 50
SHARD_TEST_BATCH :: 40
SHARD_TEST_ROWS_PER_DAY :: 7_000
SHARD_TEST_ROWS :: SHARD_TEST_THREADS * SHARD_TEST_BATCHES * SHARD_TEST_BATCH

Shard_Test_Writer :: struct {
	s:     ^Store,
	start: ^u32,
	base:  i64,
	tid:   int,
}

shard_test_writer :: proc(data: rawptr) {
	a := cast(^Shard_Test_Writer)data
	for sync.atomic_load(a.start) == 0 {}
	fields := []Field{{name = "level", value = "info"}, {name = "service", value = "api"}}
	rows: [SHARD_TEST_BATCH]Ingest_Row
	for b in 0..<SHARD_TEST_BATCHES {
		for i in 0..<SHARD_TEST_BATCH {
			n := a.tid * SHARD_TEST_BATCHES * SHARD_TEST_BATCH + b * SHARD_TEST_BATCH + i
			ts := a.base + i64(n % SHARD_TEST_ROWS_PER_DAY) * 1_000_000
			if n >= SHARD_TEST_ROWS_PER_DAY {
				ts += common.DAY_NS
			}
			rows[i] = Ingest_Row{ts = ts, stream = u64(n % 7), msg = "shard row", fields = fields}
		}
		_ = store_ingest(a.s, rows[:])
		// Stagger the batches so the round-robin shard cursor actually interleaves.
		time.sleep(time.Duration(b % 4) * 10 * time.Microsecond)
	}
}

Shard_Test_Flusher :: struct {
	s:     ^Store,
	start: ^u32,
	stop:  ^u32,
}

shard_test_flusher :: proc(data: rawptr) {
	a := cast(^Shard_Test_Flusher)data
	for sync.atomic_load(a.start) == 0 {}
	for sync.atomic_load(a.stop) == 0 {
		store_flush_due(a.s)
		store_flush_all(a.s)
		time.sleep(300 * time.Microsecond)
	}
}

@(test)
test_store_ingest_shards_concurrent_flush :: proc(t: ^testing.T) {
	// Workers share the store, so allocations must not go through the test
	// runner's per-task allocator.
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	dir := sm_tmpdir("store_shards_concurrent")
	os.remove_all(dir)
	defer delete(dir)
	defer os.remove_all(dir)

	cfg := sm_store_config(dir)
	cfg.block_rows = 64
	cfg.flush_max_rows = 300
	cfg.flush_max_bytes = 1 << 40
	cfg.flush_interval = time.Duration(1 << 62)
	cfg.merge_threshold = 1 << 20
	cfg.ingest_shards = 8
	s := store_init(cfg, false)
	defer store_close(s)

	base, _ := common.parse_iso_ns("2026-10-26T00:00:00Z")
	start: u32
	stop: u32

	fargs := Shard_Test_Flusher{s = s, start = &start, stop = &stop}
	flusher := thread.create_and_start_with_data(&fargs, shard_test_flusher, self_cleanup = false)
	wargs := make([]Shard_Test_Writer, SHARD_TEST_THREADS)
	writers := make([]^thread.Thread, SHARD_TEST_THREADS)
	defer {
		delete(wargs)
		delete(writers)
	}
	for i in 0..<SHARD_TEST_THREADS {
		wargs[i] = Shard_Test_Writer{s = s, start = &start, base = base, tid = i}
		writers[i] = thread.create_and_start_with_data(&wargs[i], shard_test_writer, self_cleanup = false)
	}
	sync.atomic_store(&start, 1)
	for w in writers {
		thread.join(w)
		thread.destroy(w)
	}
	sync.atomic_store(&stop, 1)
	thread.join(flusher)
	thread.destroy(flusher)

	store_flush_all(s)
	testing.expect_value(t, int(sync.atomic_load(&s.ingested_rows)), SHARD_TEST_ROWS)
	st := store_stats(s)
	testing.expect_value(t, st.mem_rows, 0)
	testing.expect_value(t, st.rows, SHARD_TEST_ROWS)
	testing.expectf(t, st.parts > 0, "no parts were written")

	// Every row must appear exactly once: a lost append leaves a gap, a double
	// append after the swap would duplicate one.
	seen := make(map[i64]int, SHARD_TEST_ROWS)
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
	testing.expect_value(t, counted, SHARD_TEST_ROWS)
	testing.expect_value(t, len(seen), SHARD_TEST_ROWS)
	testing.expect_value(t, duplicates, 0)
}
