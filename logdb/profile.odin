// Flush pipeline profiler. The instrumentation is opt-in: every hook starts
// with one relaxed atomic load and, while the profiler is off, does no
// allocation and no per-block bookkeeping. `bench -mode flush-profile` turns it
// on for the measured run only.
//
// The counters are deliberately flat u64s so the bench can aggregate them as a
// word array (see flush_profile_words) without a field-by-field copy.
package logdb

import "core:mem"
import "core:slice"
import "core:sync"
import "core:time"

// Prof_Scope separates a memtable flush from a multi-part merge. Both go
// through the same part writer, but their costs answer different questions.
Prof_Scope :: enum u8 {
	Flush,
	Merge,
}

Phase_Stats :: struct {
	// blocks and rows covered by this scope.
	blocks:      u64,
	parts:       u64,
	rows:        u64,
	sorts:       u64,
	// memtable order -> part order
	sort_ns:     u64,
	// rows -> column arrays + bloom token hashing, and timestamp bounds
	collect_ns:  u64,
	// column arrays -> uncompressed block bytes
	serialize_ns:  u64,
	// uncompressed block -> zlib stream
	zlib_ns:       u64,
	// bloom dedupe/sizing/bit setting
	bloom_finish_ns: u64,
	// ordered writes of the block
	write_data_ns:   u64,
	write_bloom_ns:  u64,
	// index record accumulation and the part-level finish steps
	index_ns:        u64,
	finish_index_ns: u64,
	finish_meta_ns:  u64,
	finish_open_ns:  u64,
	// bytes
	raw_bytes:     u64,
	comp_bytes:    u64,
	bloom_bytes:   u64,
	data_bytes:    u64,
	// per-block spread (max(0) is meaningless, min stays at all-ones when empty)
	min_rows: u64,
	max_rows: u64,
	min_raw:  u64,
	max_raw:  u64,
	// rows per block: full (= block_rows), >= half, < half
	row_hist: [3]u64,
	// raw block bytes: <=64K, <=256K, <=512K, <=1M, <=2M, >2M
	size_hist: [6]u64,
}

Flush_Profile :: struct {
	// ingest side: time blocked on the store write lock vs time holding it
	ingest_calls:   u64,
	ingest_rows:    u64,
	ingest_wait_ns: u64,
	ingest_hold_ns: u64,
	ingest_total_ns: u64,

	// flush side: lock wait for the memtable swap, the swap itself, and the
	// short lock taken when the finished part is attached
	flush_calls:        u64,
	flush_rows:         u64,
	flush_wait_ns:      u64,
	flush_swap_ns:      u64,
	flush_append_wait_ns: u64,
	flush_total_ns:     u64,

	// merge: total, the k-way cursor loop, and how many parts/rows it moved
	merge_calls:      u64,
	merge_rows:       u64,
	merged_parts:     u64,
	merge_total_ns:   u64,
	merge_loop_ns:    u64,
	// merge input: reading compressed blocks + inflate + decode
	merge_read_ns:     u64,
	merge_read_blocks: u64,
	merge_read_bytes:  u64,

	block_rows: u64,
	phase:      [2]Phase_Stats,
}

@(private)
flush_prof_on: u32

@(private)
flush_prof: Flush_Profile

flush_profile_enable :: proc(block_rows: int) {
	flush_prof = {}
	flush_prof.block_rows = u64(block_rows)
	for i in 0 ..< len(flush_prof.phase) {
		flush_prof.phase[i].min_rows = ~u64(0)
		flush_prof.phase[i].min_raw = ~u64(0)
	}
	sync.atomic_store_explicit(&flush_prof_on, 1, .Release)
}

flush_profile_disable :: proc() {
	sync.atomic_store_explicit(&flush_prof_on, 0, .Release)
}

// flush_profile_enabled is the only check on the hot path while profiling is
// off: one relaxed load, no allocation and no branch into the timing code.
flush_profile_enabled :: #force_inline proc() -> bool {
	return sync.atomic_load_explicit(&flush_prof_on, .Relaxed) != 0
}

// flush_profile_snapshot must be called after flush_profile_disable, so all
// flush-side writers are quiesced.
flush_profile_snapshot :: proc() -> Flush_Profile {
	return flush_prof
}

// flush_profile_words exposes the counter block as u64 words. Every field of
// Flush_Profile (and Phase_Stats) is a u64, so this view is exact and lets the
// bench compute medians across repetitions generically.
flush_profile_words :: proc(p: ^Flush_Profile) -> []u64 {
	bytes := mem.ptr_to_bytes(p)
	return slice.from_ptr(cast(^u64)raw_data(bytes), len(bytes) / size_of(u64))
}

// flush_profile_record_block keeps the per-block size distribution. Called once
// per block, only while profiling.
flush_profile_record_block :: proc(scope: Prof_Scope, nrows, raw, comp: int) {
	ph := &flush_prof.phase[int(scope)]
	r := u64(nrows)
	b := u64(raw)
	ph.blocks += 1
	ph.rows += r
	ph.raw_bytes += b
	ph.comp_bytes += u64(comp)
	if r < ph.min_rows {
		ph.min_rows = r
	}
	if r > ph.max_rows {
		ph.max_rows = r
	}
	if b < ph.min_raw {
		ph.min_raw = b
	}
	if b > ph.max_raw {
		ph.max_raw = b
	}
	switch {
	case flush_prof.block_rows > 0 && r >= flush_prof.block_rows:
		ph.row_hist[0] += 1
	case flush_prof.block_rows > 0 && r * 2 >= flush_prof.block_rows:
		ph.row_hist[1] += 1
	case:
		ph.row_hist[2] += 1
	}
	switch {
	case b <= 64 << 10:
		ph.size_hist[0] += 1
	case b <= 256 << 10:
		ph.size_hist[1] += 1
	case b <= 512 << 10:
		ph.size_hist[2] += 1
	case b <= 1 << 20:
		ph.size_hist[3] += 1
	case b <= 2 << 20:
		ph.size_hist[4] += 1
	case:
		ph.size_hist[5] += 1
	}
}

// prof_stage_start/prof_stage_end avoid repeating the `if active` dance at
// every site while keeping the disabled path to a single predictable branch.
prof_stage_start :: proc(active: bool) -> time.Time {
	if active {
		return time.now()
	}
	return time.Time{}
}

prof_stage_end :: proc(active: bool, t: time.Time) -> u64 {
	if active {
		return u64(time.since(t))
	}
	return 0
}
