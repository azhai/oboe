package odlog

import "core:fmt"
import "core:os"
import "core:sync"
import "core:testing"

@(test)
test_block_roundtrip :: proc(t: ^testing.T) {
	rows := make([]Row, 3)
	fs := make([]Field, 6)
	fs[0] = {name = "level", value = "error"}
	fs[1] = {name = "service", value = "api"}
	fs[2] = {name = "level", value = "info"}
	fs[3] = {name = "service", value = "api"}
	fs[4] = {name = "level", value = "error"}
	fs[5] = {name = "service", value = "worker"}
	rows[0] = Row{ts = 100, stream = 7, msg = "hello world", fields = fs[0:2]}
	rows[1] = Row{ts = 200, stream = 7, msg = "second line", fields = fs[2:4]}
	rows[2] = Row{ts = 300, stream = 7, msg = "third", fields = fs[4:6]}
	buf := buf_init(1024)
	defer buf_destroy(&buf)
	bb: Bloom_Builder
	defer delete(bb.hashes)
	min_ts, max_ts, ok := encode_block(rows, &buf, &bb)
	testing.expect(t, ok)
	testing.expect_value(t, min_ts, i64(100))
	testing.expect_value(t, max_ts, i64(300))
	raw := make([]u8, len(buf.data))
	copy(raw, buf.data[:])
	d, dok := decode_block(raw)
	testing.expect(t, dok)
	if !dok {
		return
	}
	defer decoded_destroy(&d)
	testing.expect_value(t, d.nrows, 3)
	testing.expect_value(t, d.rows[0].msg, "hello world")
	v, vok := row_field(&d.rows[0], "level")
	testing.expect(t, vok)
	testing.expect_value(t, v, "error")
	v2, _ := row_field(&d.rows[2], "service")
	testing.expect_value(t, v2, "worker")
	bloom := bloom_builder_finish(&bb)
	defer delete(bloom)
	b := bloom_from_bytes(bloom)
	testing.expect(t, bloom_check_scoped(&b, "level", "error", true))
	testing.expect(t, bloom_check_scoped(&b, "service", "worker", true))
	testing.expect(t, !bloom_check_scoped(&b, "level", "debug", true))
}

@(test)
test_store_roundtrip :: proc(t: ^testing.T) {
	dir := "/tmp/odlog_store_test"
	os.remove_all(dir)
	cfg := default_store_config()
	cfg.data_path = dir
	cfg.flush_interval = 1 << 62
	cfg.flush_max_rows = 100000
	cfg.flush_max_bytes = 1 << 40
	cfg.merge_threshold = 2
	s := store_init(cfg, false)
	defer {
		store_close(s)
		os.remove_all(dir)
	}
	count := 5000
	rows := make([]Ingest_Row, count)
	defer delete(rows)
	fields := make([]Field, count * 3)
	defer delete(fields)
	for i in 0..<count {
		level := i % 3 == 0 ? "error" : "info"
		fields[i*3+0] = {name = "level", value = level}
		fields[i*3+1] = {name = "service", value = i % 2 == 0 ? "api" : "worker"}
		fields[i*3+2] = {name = "n", value = fmt.aprintf("%d", i)}
		rows[i] = Ingest_Row{
			ts = i64(1_700_000_000_000_000_000) + i64(i) * 1_000_000,
			stream = u64(i % 2),
			msg = fmt.aprintf("message number %d level=%s", i, level),
			fields = fields[i*3 : i*3+3],
		}
	}
	n := store_ingest(s, rows[:2500])
	testing.expect_value(t, n, 2500)
	store_flush_all(s)
	store_ingest(s, rows[2500:])
	store_flush_all(s)
	st := store_stats(s)
	testing.expect_value(t, st.rows, count)
	testing.expect(t, st.parts >= 2)
	// read everything back
	seen := 0
	sync.rw_mutex_shared_lock(&s.mu)
	for p in s.partitions {
		for part in p.parts {
			for i in 0..<len(part.index) {
				d, ok := read_block(part, &part.index[i])
				testing.expect(t, ok)
				if ok {
					seen += d.nrows
					decoded_destroy(&d)
				}
			}
		}
	}
	sync.rw_mutex_shared_unlock(&s.mu)
	testing.expect_value(t, seen, count)
	// force a merge
	store_merge_all(s)
	st2 := store_stats(s)
	testing.expect_value(t, st2.rows, count)
	testing.expect(t, st2.parts < st.parts)
}

@(test)
test_store_reload_after_merge :: proc(t: ^testing.T) {
	dir := "/tmp/odlog_store_reload"
	os.remove_all(dir)
	cfg := default_store_config()
	cfg.data_path = dir
	cfg.flush_interval = 1 << 62
	cfg.flush_max_rows = 100000
	cfg.flush_max_bytes = 1 << 40
	cfg.block_rows = 64
	cfg.merge_threshold = 2
	s := store_init(cfg, false)
	fields := []Field{{name = "level", value = "info"}, {name = "service", value = "api"}}
	rows := make([]Ingest_Row, 500)
	defer delete(rows)
	base := i64(1_700_000_000_000_000_000)
	for i in 0..<500 {
		rows[i] = Ingest_Row{ts = base + i64(i) * 1_000_000, stream = u64(i % 2), msg = "reload test", fields = fields}
	}
	store_ingest(s, rows[0:200])
	store_flush_all(s)
	store_ingest(s, rows[200:350])
	store_flush_all(s)
	store_ingest(s, rows[350:500])
	store_flush_all(s)
	store_merge_all(s)
	st := store_stats(s)
	testing.expect_value(t, st.rows, 500)
	parts_before := st.parts
	store_close(s)

	s = store_init(cfg, false)
	st = store_stats(s)
	testing.expect_value(t, st.rows, 500)
	testing.expect(t, st.parts <= parts_before)

	// A new part must not reuse the name of a loaded one.
	extra := []Ingest_Row{{ts = base + 1000 * 1_000_000, stream = 0, msg = "after reopen", fields = fields}}
	store_ingest(s, extra)
	store_flush_all(s)
	st = store_stats(s)
	testing.expect_value(t, st.rows, 501)
	store_close(s)
	os.remove_all(dir)
}
