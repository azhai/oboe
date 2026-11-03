// Parallel memtable sort. A flush needs rows ordered by (stream, ts): the part
// writer closes a block whenever the stream changes, so the order defines both
// the block boundaries and the index. Rows arrive from ingest interleaved
// across a handful of streams, which splits the sort into three cheap steps:
//
//   1. count rows per stream,
//   2. scatter each stream into a contiguous range (in input order, so the
//      relative order of equal keys inside a stream is preserved),
//   3. sort every range by ts in parallel and concatenate them in ascending
//      stream order.
//
// Step 3 uses the same in-place algorithm on the same data no matter how many
// threads run, and the partition never depends on the thread count, so a part
// flushed with sort_threads 1 and sort_threads N is byte-identical.
package logdb

import "core:slice"
import "core:sync"
import "core:thread"

// Below this row count the scatter pass and the thread handoff cost more than
// the sort saves; a 100k-row flush is a dozen blocks and sorts in a few
// milliseconds, while a 5M-row flush is over 600 blocks.
SORT_PARALLEL_MIN_ROWS :: 100_000

// A pathological stream cardinality turns the counting pass into a lookup
// problem; fall back to the plain sort rather than growing the bookkeeping.
SORT_MAX_STREAMS :: 64

Stream_Count :: struct {
	id: u64,
	n:  int,
}

Sort_Range :: struct {
	off:   int,
	nrows: int,
}

Sort_Ctx :: struct {
	ranges: []Sort_Range,
	rows:   []Row,
	cursor: u32,
}

// row_ts_less orders a single stream's rows; the stream is constant inside a
// range, so this is the same order as row_less there.
row_ts_less :: proc(a, b: Row) -> bool {
	return a.ts < b.ts
}

stream_count_less :: proc(a, b: Stream_Count) -> bool {
	return a.id < b.id
}

sort_range_worker :: proc(data: rawptr) {
	c := cast(^Sort_Ctx)data
	for {
		i := int(sync.atomic_add(&c.cursor, 1))
		if i >= len(c.ranges) {
			break
		}
		r := c.ranges[i]
		slice.sort_by(c.rows[r.off:r.off + r.nrows], row_ts_less)
	}
}

// sort_rows_parallel sorts by (stream, ts) using up to `threads` threads.
// Small memtables are sorted in place without the partition pass.
sort_rows_parallel :: proc(rows: []Row, threads: int) {
	n := len(rows)
	if n < SORT_PARALLEL_MIN_ROWS {
		sort_rows(rows)
		return
	}

	active := flush_profile_enabled()
	ph: ^Phase_Stats
	if active {
		ph = &flush_prof.phase[int(Prof_Scope.Flush)]
	}

	t_part := prof_stage_start(active)

	// Pass 1: rows per stream. Streams are few (they come from the configured
	// stream fields), so a linear scan beats a map lookup per row.
	counts := make([dynamic]Stream_Count, 0, 8)
	defer delete(counts)
	for r in rows {
		idx := -1
		for c, i in counts {
			if c.id == r.stream {
				idx = i
				break
			}
		}
		if idx < 0 {
			if len(counts) >= SORT_MAX_STREAMS {
				if active {
					ph.sort_scatter_ns += prof_stage_end(true, t_part)
				}
				sort_rows(rows)
				return
			}
			append(&counts, Stream_Count{id = r.stream})
			idx = len(counts) - 1
		}
		counts[idx].n += 1
	}
	slice.sort_by(counts[:], stream_count_less)

	// Pass 2: stable scatter into range order. The destination is recomputed
	// per row with the same linear scan, which keeps the pass allocation-free
	// apart from the range table.
	tmp := make([]Row, n)
	defer delete(tmp)
	ranges := make([]Sort_Range, len(counts))
	defer delete(ranges)
	writes := make([]int, len(counts))
	defer delete(writes)
	off := 0
	for c, i in counts {
		ranges[i] = Sort_Range{off = off, nrows = c.n}
		writes[i] = off
		off += c.n
	}
	for r in rows {
		idx := -1
		for c, i in counts {
			if c.id == r.stream {
				idx = i
				break
			}
		}
		tmp[writes[idx]] = r
		writes[idx] += 1
	}
	if active {
		ph.sort_scatter_ns += prof_stage_end(true, t_part)
	}

	// Pass 3: one range per stream, sorted concurrently. A single stream
	// followed by a single worker is still the same algorithm, just serial.
	t_seg := prof_stage_start(active)
	ctx := Sort_Ctx{ranges = ranges, rows = tmp}
	wanted := clamp(threads, 1, len(ranges))
	spawned := make([dynamic]^thread.Thread, 0, max(wanted - 1, 0))
	defer delete(spawned)
	for _ in 0 ..< wanted - 1 {
		t := thread.create_and_start_with_data(&ctx, sort_range_worker, self_cleanup = false, name = "oboe-sort")
		if t == nil {
			break
		}
		append(&spawned, t)
	}
	sort_range_worker(&ctx)
	for t in spawned {
		thread.join(t)
		thread.destroy(t)
	}
	if active {
		ph.sort_segment_ns += prof_stage_end(true, t_seg)
	}

	copy(rows, tmp)
}
