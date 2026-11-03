package logdb

import "../common"
import "core:fmt"
import "core:os"
import "core:slice"
import "core:strings"
import "core:testing"

// Storage tests that need directories keep them unique per process and per test
// so concurrent test runners never share state.
sm_tmpdir :: proc(name: string) -> string {
	return fmt.aprintf("/tmp/oboe_%s_%d", name, os.get_pid())
}

sm_store_config :: proc(dir: string) -> Store_Config {
	cfg := default_store_config()
	cfg.data_path = dir
	cfg.flush_interval = 1 << 62
	cfg.flush_max_rows = 1_000_000
	cfg.flush_max_bytes = 1 << 40
	return cfg
}

sm_part_path :: proc(data_path: string, day: i64, name: string) -> string {
	day_str := common.format_day(day)
	defer delete(day_str)
	return fmt.aprintf("%s/partitions/%s/parts/%s", data_path, day_str, name)
}

@(test)
test_store_more_retention :: proc(t: ^testing.T) {
	dir := sm_tmpdir("store_more_retention")
	os.remove_all(dir)
	defer delete(dir)
	defer os.remove_all(dir)

	cfg := sm_store_config(dir)
	cfg.retention_days = 3
	s := store_init(cfg, false)
	defer store_close(s)

	fields := []Field{{name = "level", value = "info"}}
	now := common.now_ns()
	old_ts := now - 10 * common.DAY_NS
	old_day := common.utc_day(old_ts)
	today := common.utc_day(now)

	old_rows := []Ingest_Row{
		{ts = old_ts, stream = 7, msg = "old one", fields = fields},
		{ts = old_ts + 3_600_000_000_000, stream = 7, msg = "old two", fields = fields},
	}
	store_ingest(s, old_rows)
	store_flush_all(s)

	recent_rows := []Ingest_Row{{ts = now, stream = 7, msg = "recent", fields = fields}}
	store_ingest(s, recent_rows)
	store_flush_all(s)

	// A row still held in the old partition's memtable must be dropped with the
	// partition, not only flushed parts.
	mem_rows := []Ingest_Row{{ts = old_ts + 1, stream = 7, msg = "old mem", fields = fields}}
	store_ingest(s, mem_rows)

	before := store_stats(s)
	testing.expect_value(t, before.partitions, 2)
	testing.expect_value(t, before.parts, 2)
	testing.expect_value(t, before.mem_rows, 1)
	testing.expect_value(t, before.rows, 3)

	store_apply_retention(s)

	after := store_stats(s)
	testing.expect_value(t, after.partitions, 1)
	testing.expect_value(t, after.parts, 1)
	testing.expect_value(t, after.mem_rows, 0)
	testing.expect_value(t, after.rows, 1)
	old_day_str := common.format_day(old_day)
	defer delete(old_day_str)
	old_partition := fmt.aprintf("%s/partitions/%s", dir, old_day_str)
	defer delete(old_partition)
	testing.expect(t, !os.exists(old_partition))
	today_str := common.format_day(today)
	defer delete(today_str)
	today_partition := fmt.aprintf("%s/partitions/%s", dir, today_str)
	defer delete(today_partition)
	testing.expect(t, os.exists(today_partition))

	// retention_days=0 means "keep everything", including old partitions.
	dir2 := sm_tmpdir("store_more_retention_zero")
	os.remove_all(dir2)
	defer delete(dir2)
	defer os.remove_all(dir2)
	cfg2 := sm_store_config(dir2)
	cfg2.retention_days = 0
	s2 := store_init(cfg2, false)
	defer store_close(s2)
	store_ingest(s2, old_rows)
	store_flush_all(s2)
	store_apply_retention(s2)
	st2 := store_stats(s2)
	testing.expect_value(t, st2.partitions, 1)
	testing.expect_value(t, st2.rows, 2)
	testing.expect_value(t, st2.min_ts, old_ts)
}

@(test)
test_store_more_merge_multiway :: proc(t: ^testing.T) {
	dir := sm_tmpdir("store_more_merge")
	os.remove_all(dir)
	defer delete(dir)
	defer os.remove_all(dir)

	cfg := sm_store_config(dir)
	cfg.block_rows = 32
	cfg.merge_threshold = 3
	cfg.merge_max_rows = 10_000_000
	s := store_init(cfg, false)
	defer store_close(s)

	base, _ := common.parse_iso_ns("2026-10-26T00:00:00Z")
	fields := []Field{{name = "level", value = "info"}, {name = "service", value = "api"}}
	const_batches :: 5
	const_rows :: 20
	for b in 0..<const_batches {
		batch := make([]Ingest_Row, const_rows)
		for i in 0..<const_rows {
			n := b * const_rows + i
			batch[i] = Ingest_Row{
				ts = base + i64(n) * 1_000_000,
				stream = u64(i % 2),
				msg = "merged row",
				fields = fields,
			}
		}
		testing.expect_value(t, store_ingest(s, batch), const_rows)
		store_flush_all(s)
		delete(batch)
	}

	before := store_stats(s)
	testing.expect_value(t, before.parts, const_batches)
	testing.expect_value(t, before.rows, const_batches * const_rows)

	store_merge_all(s)

	after := store_stats(s)
	testing.expect_value(t, after.parts, 1)
	testing.expect_value(t, after.rows, const_batches * const_rows)
	testing.expect_value(t, after.min_ts, base)
	testing.expect_value(t, after.max_ts, base + i64(const_batches*const_rows-1)*1_000_000)

	merged := s.partitions[0].parts[0]
	testing.expect_value(t, merged.meta.rows, const_batches * const_rows)

	marker_path := fmt.aprintf("%s/merged_from", merged.dir)
	defer delete(marker_path)
	marker, merr := os.read_entire_file_from_path(marker_path, context.allocator)
	testing.expect(t, merr == nil)
	if merr == nil {
		ids, _ := strings.split_lines(string(marker))
		listed := 0
		for id in ids {
			if len(id) > 0 {
				listed += 1
			}
		}
		testing.expect_value(t, listed, const_batches)
		delete(ids)
		delete(marker)
	}

	// The merged part must still be sorted by (stream, ts).
	prev_stream: u64
	prev_ts: i64
	first := true
	for &bi in merged.index {
		d, ok := read_block(merged, &bi)
		testing.expect(t, ok)
		if !ok {
			continue
		}
		for r in d.rows {
			if !first {
				ordered := r.stream > prev_stream || (r.stream == prev_stream && r.ts >= prev_ts)
				testing.expect(t, ordered)
			}
			prev_stream = r.stream
			prev_ts = r.ts
			first = false
		}
		decoded_destroy(&d)
	}
}

@(test)
test_store_more_merged_from_restart :: proc(t: ^testing.T) {
	dir := sm_tmpdir("store_more_merged_from")
	os.remove_all(dir)
	defer delete(dir)
	defer os.remove_all(dir)

	cfg := sm_store_config(dir)
	cfg.block_rows = 32
	cfg.merge_threshold = 2
	cfg.merge_max_rows = 10_000_000
	base, _ := common.parse_iso_ns("2026-10-26T00:00:00Z")
	fields := []Field{{name = "level", value = "info"}}

	s := store_init(cfg, false)
	for b in 0..<3 {
		batch := make([]Ingest_Row, 30)
		for i in 0..<30 {
			n := b * 30 + i
			batch[i] = Ingest_Row{ts = base + i64(n) * 1_000_000, stream = 1, msg = "original", fields = fields}
		}
		store_ingest(s, batch)
		store_flush_all(s)
		delete(batch)
	}
	store_merge_all(s)
	st := store_stats(s)
	testing.expect_value(t, st.rows, 90)
	testing.expect_value(t, st.parts, 1)

	merged := s.partitions[0].parts[0]
	marker_path := fmt.aprintf("%s/merged_from", merged.dir)
	defer delete(marker_path)
	marker, merr := os.read_entire_file_from_path(marker_path, context.allocator)
	testing.expect(t, merr == nil)
	if merr != nil {
		store_close(s)
		return
	}
	ids, _ := strings.split_lines(string(marker))
	old_id: string
	for id in ids {
		if len(id) > 0 {
			old_id = id
			break
		}
	}
	testing.expect(t, len(old_id) == 18)
	store_close(s)

	// Simulate a crash after the merged part became durable but before the old
	// input directories were removed: recreate one of them as a valid part.
	day := common.utc_day(base)
	dup_dir := sm_part_path(dir, day, old_id)
	dup_rows := make([]Row, 30)
	for i in 0..<30 {
		dup_rows[i] = Row{ts = base + i64(i) * 1_000_000, stream = 1, msg = "duplicate", fields = fields}
	}
	dup, dup_ok := write_part(dup_dir, dup_rows[:], cfg.block_rows)
	testing.expect(t, dup_ok)
	if dup_ok {
		close_part(dup)
	}
	delete(dup_rows)

	s = store_init(cfg, false)
	defer store_close(s)
	st = store_stats(s)
	testing.expect_value(t, st.rows, 90)
	testing.expect_value(t, st.parts, 1)
	testing.expect(t, !os.exists(dup_dir))

	delete(dup_dir)
	delete(ids)
	delete(marker)
}

@(test)
test_store_more_incomplete_part_cleanup :: proc(t: ^testing.T) {
	dir := sm_tmpdir("store_more_incomplete")
	os.remove_all(dir)
	defer delete(dir)
	defer os.remove_all(dir)

	cfg := sm_store_config(dir)
	s := store_init(cfg, false)
	fields := []Field{{name = "level", value = "info"}}
	base, _ := common.parse_iso_ns("2026-10-26T00:00:00Z")
	rows := make([]Ingest_Row, 20)
	for i in 0..<20 {
		rows[i] = Ingest_Row{ts = base + i64(i), stream = 1, msg = "kept", fields = fields}
	}
	store_ingest(s, rows)
	store_flush_all(s)
	before := store_stats(s)
	store_close(s)
	delete(rows)

	day := common.utc_day(base)
	bad_name := "p_00000000deadbeef"
	bad_dir := sm_part_path(dir, day, bad_name)
	defer delete(bad_dir)
	os.make_directory_all(bad_dir)
	bad_data := fmt.aprintf("%s/data.bin", bad_dir)
	defer delete(bad_data)
	testing.expect(t, os.write_entire_file(bad_data, []u8{1, 2, 3}) == nil)

	s = store_init(cfg, false)
	defer store_close(s)
	after := store_stats(s)
	testing.expect_value(t, after.rows, before.rows)
	testing.expect_value(t, after.parts, before.parts)
	testing.expect_value(t, after.partitions, before.partitions)
	testing.expect(t, !os.exists(bad_dir))
}

sm_corrupt_part :: proc(t: ^testing.T, data_path: string, day: i64, name: string, kind: string) {
	dir := sm_part_path(data_path, day, name)
	defer delete(dir)
	fields := []Field{{name = "level", value = "info"}}
	rows := []Row{
		{ts = 1000, stream = 1, msg = "corrupt me", fields = fields},
		{ts = 2000, stream = 1, msg = "corrupt me too", fields = fields},
		{ts = 3000, stream = 1, msg = "and me", fields = fields},
	}
	p, ok := write_part(dir, rows, 16)
	testing.expect(t, ok)
	if !ok {
		os.remove_all(dir)
		return
	}
	close_part(p)

	data_file := fmt.aprintf("%s/data.bin", dir)
	index_file := fmt.aprintf("%s/index.bin", dir)
	bloom_file := fmt.aprintf("%s/bloom.bin", dir)
	defer {
		delete(data_file)
		delete(index_file)
		delete(bloom_file)
	}
	switch kind {
	case "data_truncated":
		bytes, err := os.read_entire_file_from_path(data_file, context.allocator)
		testing.expect(t, err == nil)
		if err == nil && len(bytes) > 1 {
			testing.expect(t, os.write_entire_file(data_file, bytes[:len(bytes)-1]) == nil)
		}
		delete(bytes)
	case "data_appended":
		bytes, err := os.read_entire_file_from_path(data_file, context.allocator)
		testing.expect(t, err == nil)
		if err == nil {
			extra := make([]u8, len(bytes) + 1)
			copy(extra, bytes)
			testing.expect(t, os.write_entire_file(data_file, extra) == nil)
			delete(extra)
		}
		delete(bytes)
	case "index_magic":
		bytes, err := os.read_entire_file_from_path(index_file, context.allocator)
		testing.expect(t, err == nil)
		if err == nil && len(bytes) >= 4 {
			bytes[0] = 'X'
			testing.expect(t, os.write_entire_file(index_file, bytes) == nil)
		}
		delete(bytes)
	case "index_bad_descriptor":
		bytes, err := os.read_entire_file_from_path(index_file, context.allocator)
		testing.expect(t, err == nil)
		if err == nil && len(bytes) >= 12 + INDEX_RECORD_SIZE {
			// comp_len lives at record offset 32; make it point past data.bin.
			for i in 0..<4 {
				bytes[12+32+i] = 0xFF
			}
			testing.expect(t, os.write_entire_file(index_file, bytes) == nil)
		}
		delete(bytes)
	case "bloom_truncated":
		bytes, err := os.read_entire_file_from_path(bloom_file, context.allocator)
		testing.expect(t, err == nil)
		if err == nil && len(bytes) > 1 {
			testing.expect(t, os.write_entire_file(bloom_file, bytes[:len(bytes)-1]) == nil)
		}
		delete(bytes)
	}

	p2, pok := open_part(dir)
	testing.expectf(t, !pok, "open_part accepted corrupted %s part", kind)
	if pok {
		close_part(p2)
	}
	os.remove_all(dir)
}

@(test)
test_store_more_corrupt_parts :: proc(t: ^testing.T) {
	dir := sm_tmpdir("store_more_corrupt")
	os.remove_all(dir)
	defer delete(dir)
	defer os.remove_all(dir)
	day := common.utc_day(common.now_ns())

	kinds := []string{"data_truncated", "data_appended", "index_magic", "index_bad_descriptor", "bloom_truncated"}
	for kind, i in kinds {
		name := fmt.aprintf("p_%016x", u64(1000 + i))
		sm_corrupt_part(t, dir, day, name, kind)
		delete(name)
	}
}

@(test)
test_store_more_part_seq_no_reuse :: proc(t: ^testing.T) {
	dir := sm_tmpdir("store_more_seq")
	os.remove_all(dir)
	defer delete(dir)
	defer os.remove_all(dir)

	cfg := sm_store_config(dir)
	base, _ := common.parse_iso_ns("2026-10-26T00:00:00Z")
	fields := []Field{{name = "level", value = "info"}}

	s := store_init(cfg, false)
	rows := make([]Ingest_Row, 20)
	for i in 0..<20 {
		rows[i] = Ingest_Row{ts = base + i64(i), stream = 1, msg = "before reopen", fields = fields}
	}
	store_ingest(s, rows)
	store_flush_all(s)
	delete(rows)

	before_ids := make(map[string]bool)
	max_seq: u64
	defer delete_map(before_ids)
	for p in s.partitions {
		for part in p.parts {
			before_ids[part.meta.id] = true
			if seq, ok := part_seq(part.meta.id); ok && seq > max_seq {
				max_seq = seq
			}
		}
	}
	testing.expect(t, len(before_ids) > 0)
	store_close(s)

	s = store_init(cfg, false)
	defer store_close(s)
	rows = make([]Ingest_Row, 20)
	for i in 0..<20 {
		rows[i] = Ingest_Row{ts = base + i64(10_000 + i), stream = 1, msg = "after reopen", fields = fields}
	}
	store_ingest(s, rows)
	store_flush_all(s)
	delete(rows)

	found_new := false
	for p in s.partitions {
		for part in p.parts {
			if before_ids[part.meta.id] {
				continue
			}
			found_new = true
			seq, ok := part_seq(part.meta.id)
			testing.expect(t, ok)
			testing.expectf(t, seq > max_seq, "new part seq %d must exceed loaded max %d", seq, max_seq)
		}
	}
	testing.expect(t, found_new)
	st := store_stats(s)
	testing.expect_value(t, st.rows, 40)
}

// sm_merge_order_less mirrors the merge comparison: stream first, then ts.
sm_merge_order_less :: proc(a, b: Row) -> bool {
	if a.stream != b.stream {
		return a.stream < b.stream
	}
	return a.ts < b.ts
}

// The merge no longer copies rows into an arena: the output block references the
// decoded source blocks instead, which are pinned until the block is encoded.
// What lands on disk must not change, so the merged part is compared byte for
// byte against a part written directly from the same rows in merge order.
@(test)
test_store_more_merge_zero_copy_part_bytes_match :: proc(t: ^testing.T) {
	dir := sm_tmpdir("store_more_merge_bytes")
	ref_dir := sm_tmpdir("store_more_merge_bytes_ref")
	os.remove_all(dir)
	os.remove_all(ref_dir)
	defer delete(dir)
	defer delete(ref_dir)
	defer os.remove_all(dir)
	defer os.remove_all(ref_dir)

	cfg := sm_store_config(dir)
	cfg.block_rows = 32
	cfg.merge_threshold = 3
	cfg.merge_max_rows = 10_000_000
	s := store_init(cfg, false)
	defer store_close(s)

	base, _ := common.parse_iso_ns("2026-10-26T00:00:00Z")
	fields := []Field{{name = "level", value = "info"}, {name = "service", value = "api"}}
	const_batches :: 3
	const_rows :: 40
	expected := make([dynamic]Row, 0, const_batches * const_rows)
	defer delete(expected)
	for b in 0..<const_batches {
		batch := make([]Ingest_Row, const_rows)
		for i in 0..<const_rows {
			n := b * const_rows + i
			batch[i] = Ingest_Row{
				ts = base + i64(n) * 1_000_000,
				stream = u64(i % 3),
				msg = "merged row",
				fields = fields,
			}
			append(&expected, Row{ts = batch[i].ts, stream = batch[i].stream, msg = batch[i].msg, fields = batch[i].fields})
		}
		testing.expect_value(t, store_ingest(s, batch), const_rows)
		store_flush_all(s)
		delete(batch)
	}

	store_merge_all(s)
	st := store_stats(s)
	testing.expect_value(t, st.parts, 1)
	testing.expect_value(t, st.rows, const_batches * const_rows)

	slice.sort_by(expected[:], sm_merge_order_less)
	ref, ref_ok := write_part(ref_dir, expected[:], cfg.block_rows)
	testing.expect(t, ref_ok, "reference part write failed")
	if !ref_ok {
		return
	}
	defer close_part(ref)

	merged := s.partitions[0].parts[0]
	names := [3]string{"data.bin", "bloom.bin", "index.bin"}
	for name in names {
		a := fmt.aprintf("%s/%s", merged.dir, name)
		b := fmt.aprintf("%s/%s", ref_dir, name)
		defer delete(a)
		defer delete(b)
		testing.expectf(t, fp_files_equal(a, b), "merged %s differs from the reference part", name)
	}
}
