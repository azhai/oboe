package logdb

import "../common"
import "core:fmt"
import "core:os"
import "core:slice"
import "core:testing"

@(test)
test_block_spans_follow_stream_and_block_size :: proc(t: ^testing.T) {
	rows := make([]Row, 6)
	defer delete(rows)
	for i in 0 ..< 3 {
		rows[i] = Row{ts = i64(i), stream = 1}
	}
	for i in 0 ..< 3 {
		rows[3 + i] = Row{ts = i64(i), stream = 2}
	}
	spans := block_spans(rows, 2)
	defer delete(spans)
	testing.expect_value(t, len(spans), 4)
	if len(spans) != 4 {
		return
	}
	expected := [4]Block_Span{
		{off = 0, nrows = 2, stream = 1},
		{off = 2, nrows = 1, stream = 1},
		{off = 3, nrows = 2, stream = 2},
		{off = 5, nrows = 1, stream = 2},
	}
	for s, i in spans {
		testing.expect_value(t, s.off, expected[i].off)
		testing.expect_value(t, s.nrows, expected[i].nrows)
		testing.expect_value(t, s.stream, expected[i].stream)
	}
}

fp_files_equal :: proc(a, b: string) -> bool {
	da, ea := os.read_entire_file_from_path(a, context.allocator)
	if ea != nil {
		return false
	}
	defer delete(da)
	db, eb := os.read_entire_file_from_path(b, context.allocator)
	if eb != nil {
		return false
	}
	defer delete(db)
	return slice.equal(da, db)
}

// A parallel part must be byte-for-byte identical to the serial one: only the
// encoding work moves to other threads, the write order and index layout stay
// the same.
@(test)
test_parallel_part_matches_serial :: proc(t: ^testing.T) {
	base := sm_tmpdir("flush_pool")
	defer delete(base)
	serial_dir := fmt.aprintf("%s/serial", base)
	par_dir := fmt.aprintf("%s/par", base)
	defer delete(serial_dir)
	defer delete(par_dir)
	os.remove_all(base)
	defer os.remove_all(base)

	msgs := [8]string{
		"alpha line one", "beta line two", "gamma line three", "delta line four",
		"epsilon line five", "zeta line six", "eta line seven", "theta line eight",
	}
	levels := [4]string{"error", "info", "warn", "debug"}
	hosts := [8]string{"host-0", "host-1", "host-2", "host-3", "host-4", "host-5", "host-6", "host-7"}
	const_rows :: 900
	const_fields :: 2
	rows := make([]Row, const_rows)
	defer delete(rows)
	fields := make([]Field, const_rows * const_fields)
	defer delete(fields)
	for i in 0 ..< const_rows {
		fields[i * const_fields + 0] = Field{name = "level", value = levels[i % 4]}
		fields[i * const_fields + 1] = Field{name = "host", value = hosts[i % 8]}
		rows[i] = Row{
			ts     = i64(i) * 1000,
			stream = u64(i % 3 + 1),
			msg    = msgs[i % 8],
			fields = fields[i * const_fields:i * const_fields + const_fields],
		}
	}
	sort_rows(rows)

	p_serial, ok_serial := write_part(serial_dir, rows, 64)
	testing.expect(t, ok_serial)
	if !ok_serial {
		return
	}
	defer close_part(p_serial)
	p_par, ok_par := write_part_parallel(par_dir, rows, 64, 4)
	testing.expect(t, ok_par)
	if !ok_par {
		return
	}
	defer close_part(p_par)

	testing.expect_value(t, p_par.meta.rows, p_serial.meta.rows)
	testing.expect_value(t, p_par.meta.blocks, p_serial.meta.blocks)
	testing.expect_value(t, p_par.meta.min_ts, p_serial.meta.min_ts)
	testing.expect_value(t, p_par.meta.max_ts, p_serial.meta.max_ts)
	testing.expect_value(t, p_par.meta.data_size, p_serial.meta.data_size)
	testing.expect_value(t, p_par.meta.bloom_size, p_serial.meta.bloom_size)
	testing.expect_value(t, p_par.meta.index_size, p_serial.meta.index_size)

	names := [3]string{"data.bin", "bloom.bin", "index.bin"}
	for name in names {
		a := fmt.aprintf("%s/%s", serial_dir, name)
		b := fmt.aprintf("%s/%s", par_dir, name)
		testing.expect(t, fp_files_equal(a, b), name)
		delete(a)
		delete(b)
	}
}

// Parallel sorting must produce the same permutation as the serial sort: keys
// here are unique, so any difference in order is a real bug in the chunk/merge
// boundaries rather than an artifact of an unstable sort.
@(test)
test_parallel_sort_matches_serial :: proc(t: ^testing.T) {
	const_rows :: 20_003
	rows := make([]Row, const_rows)
	defer delete(rows)
	serial := make([]Row, const_rows)
	defer delete(serial)
	for i in 0 ..< const_rows {
		// ts is unique, so every (stream, ts) key is unique too.
		rows[i] = Row{ts = i64(i) * 3, stream = u64((i * 7) % 13 + 1)}
	}
	copy(serial, rows)
	slice.sort_by(serial, row_less)

	sort_rows(rows, 8)

	for i in 0 ..< len(rows) {
		// Keys are unique, so a matching stream/ts pair at every index proves
		// the parallel sort produced exactly the serial permutation.
		if rows[i].ts != serial[i].ts || rows[i].stream != serial[i].stream {
			testing.expectf(t, false, "parallel sort differs from serial sort at row %d", i)
			return
		}
	}
}

// A merge larger than MERGE_WINDOW_BLOCKS goes through several encode windows;
// the resulting part must still hold every row ordered by (stream, ts).
@(test)
test_merge_windows_cover_all_rows :: proc(t: ^testing.T) {
	dir := sm_tmpdir("merge_windows")
	defer delete(dir)
	os.remove_all(dir)
	defer os.remove_all(dir)

	cfg := sm_store_config(dir)
	cfg.block_rows = 8
	cfg.merge_threshold = 3
	cfg.merge_max_rows = 100_000_000
	s := store_init(cfg, false)
	defer store_close(s)

	base, _ := common.parse_iso_ns("2026-10-26T00:00:00Z")
	fields := []Field{{name = "level", value = "info"}}
	const_batches :: 4
	const_rows :: 300
	// MERGE_WINDOW_BLOCKS * block_rows is 256 rows, so this merge spans five
	// windows and exercises the window hand-off.
	for b in 0 ..< const_batches {
		batch := make([]Ingest_Row, const_rows)
		for i in 0 ..< const_rows {
			n := b * const_rows + i
			batch[i] = Ingest_Row{
				ts     = base + i64(n) * 1_000_000,
				stream = u64(i % 3),
				msg    = "window row",
				fields = fields,
			}
		}
		testing.expect_value(t, store_ingest(s, batch), const_rows)
		store_flush_all(s)
		delete(batch)
	}

	store_merge_all(s)
	st := store_stats(s)
	testing.expect_value(t, st.parts, 1)
	testing.expect_value(t, st.rows, const_batches * const_rows)

	merged := s.partitions[0].parts[0]
	testing.expect_value(t, merged.meta.rows, const_batches * const_rows)

	seen := 0
	first := true
	prev_stream: u64
	prev_ts: i64
	for &bi in merged.index {
		d, ok := read_block(merged, &bi)
		testing.expect(t, ok)
		if !ok {
			return
		}
		for ri in 0 ..< d.nrows {
			r := &d.rows[ri]
			if !first {
				ordered := r.stream > prev_stream || (r.stream == prev_stream && r.ts >= prev_ts)
				testing.expectf(t, ordered, "merge output out of order at row %d", seen)
			}
			prev_stream = r.stream
			prev_ts = r.ts
			first = false
			seen += 1
		}
		decoded_destroy(&d)
	}
	testing.expect_value(t, seen, const_batches * const_rows)
}
