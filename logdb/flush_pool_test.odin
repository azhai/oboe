package logdb

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
