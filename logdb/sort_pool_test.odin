package logdb

import "core:fmt"
import "core:os"
import "core:slice"
import "core:testing"

// The parallel sort must not change what a part contains: the partition only
// depends on the streams, and each stream range is sorted with the same
// in-place algorithm, so 1 and N threads have to produce the same order.

sp_make_rows :: proc(n, streams, ts_mod: int) -> []Row {
	rows := make([]Row, n)
	for i in 0 ..< n {
		rows[i] = Row{
			ts     = i64((i * 7919) % ts_mod) + 1,
			stream = u64(i % streams + 1),
			msg    = fmt.aprintf("%d", i),
		}
	}
	return rows
}

// sp_make_skewed_rows weights one stream at 90% and leaves gaps in the stream
// ids (2, 9 present, 3..8 absent), which is the shape of a real partition where
// one service logs far more than the others.
sp_make_skewed_rows :: proc(n: int) -> []Row {
	rows := make([]Row, n)
	for i in 0 ..< n {
		stream := i % 10 == 0 ? 9 : 7
		if i % 20 == 1 {
			stream = 2
		}
		rows[i] = Row{
			ts     = i64((i * 7919) % 1_000) + 1,
			stream = u64(stream),
			msg    = fmt.aprintf("%d", i),
		}
	}
	return rows
}

sp_destroy_rows :: proc(rows: []Row) {
	for r in rows {
		delete(r.msg)
	}
	delete(rows)
}

sp_same_order :: proc(a, b: []Row) -> bool {
	if len(a) != len(b) {
		return false
	}
	for i in 0 ..< len(a) {
		if a[i].ts != b[i].ts || a[i].stream != b[i].stream || a[i].msg != b[i].msg {
			return false
		}
	}
	return true
}

// sp_is_sorted checks the invariant every part relies on: streams ascending,
// timestamps ascending inside a stream, and no row lost.
sp_is_sorted :: proc(t: ^testing.T, rows: []Row, want: int) -> bool {
	testing.expect_value(t, len(rows), want)
	if len(rows) != want {
		return false
	}
	for i in 1 ..< len(rows) {
		if rows[i].stream < rows[i - 1].stream {
			testing.expectf(t, false, "stream out of order at %d: %d then %d", i, rows[i - 1].stream, rows[i].stream)
			return false
		}
		if rows[i].stream == rows[i - 1].stream && rows[i].ts < rows[i - 1].ts {
			testing.expectf(t, false, "ts out of order at %d: %d then %d", i, rows[i - 1].ts, rows[i].ts)
			return false
		}
	}
	return true
}

sp_check_pair :: proc(t: ^testing.T, src: []Row, threads: int) {
	a := slice.clone(src)
	b := slice.clone(src)
	defer delete(a)
	defer delete(b)
	sort_rows_parallel(a, 1)
	sort_rows_parallel(b, threads)
	sp_is_sorted(t, a, len(src))
	sp_is_sorted(t, b, len(src))
	if !sp_same_order(a, b) {
		for i in 0 ..< len(a) {
			if a[i].ts != b[i].ts || a[i].stream != b[i].stream || a[i].msg != b[i].msg {
				testing.expectf(t, false, "threads 1 vs %d differ at row %d", threads, i)
				break
			}
		}
	}
}

@(test)
test_sort_parallel_uniform_streams :: proc(t: ^testing.T) {
	rows := sp_make_rows(SORT_PARALLEL_MIN_ROWS + 20_000, 4, 1_000)
	defer sp_destroy_rows(rows)
	sp_check_pair(t, rows, 4)
}

@(test)
test_sort_parallel_skewed_streams :: proc(t: ^testing.T) {
	rows := sp_make_skewed_rows(SORT_PARALLEL_MIN_ROWS + 20_000)
	defer sp_destroy_rows(rows)
	sp_check_pair(t, rows, 4)
}

@(test)
test_sort_parallel_duplicate_timestamps :: proc(t: ^testing.T) {
	// 16 distinct timestamps over 120k rows: every block is full of ties, so
	// any tie-break that depended on the thread count would show up here.
	rows := sp_make_rows(SORT_PARALLEL_MIN_ROWS + 20_000, 4, 16)
	defer sp_destroy_rows(rows)
	sp_check_pair(t, rows, 4)
	sp_check_pair(t, rows, 3)
}

@(test)
test_sort_parallel_small_and_empty :: proc(t: ^testing.T) {
	// Below SORT_PARALLEL_MIN_ROWS the plain sort runs for any thread count.
	small := sp_make_rows(1_000, 4, 100)
	defer sp_destroy_rows(small)
	sp_check_pair(t, small, 4)

	// An empty memtable and a single row must not spawn anything.
	none := make([]Row, 0)
	defer delete(none)
	sort_rows_parallel(none, 4)

	one := sp_make_rows(1, 4, 100)
	defer sp_destroy_rows(one)
	sort_rows_parallel(one, 4)
	testing.expect_value(t, len(one), 1)
}

// A part built from a 1-thread sort has to be byte-identical to one built from
// a 4-thread sort: the order, block boundaries and index all come from the
// sorted rows.
@(test)
test_sort_parallel_part_bytes_match :: proc(t: ^testing.T) {
	base := sm_tmpdir("sort_parallel")
	defer delete(base)
	os.remove_all(base)
	defer os.remove_all(base)

	rows := sp_make_rows(SORT_PARALLEL_MIN_ROWS + 20_000, 4, 1_000)
	defer sp_destroy_rows(rows)

	serial := slice.clone(rows)
	defer delete(serial)
	parallel := slice.clone(rows)
	defer delete(parallel)
	sort_rows_parallel(serial, 1)
	sort_rows_parallel(parallel, 4)

	serial_dir := fmt.aprintf("%s/serial", base)
	par_dir := fmt.aprintf("%s/parallel", base)
	defer delete(serial_dir)
	defer delete(par_dir)
	p_serial, ok_serial := write_part(serial_dir, serial, 8192)
	testing.expect(t, ok_serial, "serial part write failed")
	if !ok_serial {
		return
	}
	defer close_part(p_serial)
	p_par, ok_par := write_part(par_dir, parallel, 8192)
	testing.expect(t, ok_par, "parallel part write failed")
	if !ok_par {
		return
	}
	defer close_part(p_par)

	names := [3]string{"data.bin", "bloom.bin", "index.bin"}
	for name in names {
		a := fmt.aprintf("%s/%s", serial_dir, name)
		b := fmt.aprintf("%s/%s", par_dir, name)
		defer delete(a)
		defer delete(b)
		testing.expect(t, fp_files_equal(a, b), name)
	}
}
