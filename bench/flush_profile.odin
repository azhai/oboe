// Flush profiling. A memtable flush spends its time in four separable phases:
// sorting the rows, column encoding (including bloom token hashing), zlib
// compression, and the ordered writes. This mode times each of them on the same
// corpus the rest of the suite uses, and then compares the serial part writer
// with the parallel one, so the numbers stay comparable with the HTTP ingest
// benchmarks.
package main

import "core:fmt"
import "core:os"
import "core:slice"
import "core:time"

import "../common"
import "../logdb"

run_flush_profile :: proc(p: Params) {
	corpus := corpus_make()
	defer corpus_destroy(&corpus)

	base_ts := common.now_ns() - SPREAD_NS
	rows := make([]logdb.Row, p.rows)
	defer delete(rows)
	fields := make([]logdb.Field, p.rows * FIELDS_PER_ROW)
	defer delete(fields)
	for i in 0 ..< p.rows {
		f := fields[i * FIELDS_PER_ROW:(i + 1) * FIELDS_PER_ROW]
		rows[i] = corpus_fill_row(&corpus, i, p.rows, base_ts, f)
	}

	// The sort pass leaves the rows in part order; every later pass works on
	// that sorted layout, exactly like a real flush does.
	sort_ctx := Profile_Sort_Ctx{rows = rows, rng = 0x9e3779b97f4a7c15}
	sort_ms := profile_median(p.reps, profile_do_sort, &sort_ctx)
	logdb.sort_rows(rows)

	spans_begin := time.now()
	spans := logdb.block_spans(rows, p.block_rows)
	spans_ms := millis(time.since(spans_begin))
	defer delete(spans)

	profile_emit(p, "sort", sort_ms, f64(p.rows), 0, 0)
	profile_emit(p, "spans", spans_ms, 0, 0, 0)

	buf := common.buf_init(1 << 16)
	defer common.buf_destroy(&buf)
	bb := logdb.Bloom_Builder{}
	defer {
		delete(bb.hashes)
		delete(bb.table)
	}

	raws := make([][]u8, len(spans))
	defer {
		for r in raws {
			delete(r)
		}
		delete(raws)
	}
	encode_ctx := Profile_Encode_Ctx{rows = rows, spans = spans, buf = &buf, bb = &bb, raws = raws}
	encode_ms := profile_median(p.reps, profile_do_encode, &encode_ctx)
	profile_emit(p, "encode", encode_ms, f64(p.rows), encode_ctx.encoded_bytes, 0)

	bloom_ctx := Profile_Bloom_Ctx{rows = rows, spans = spans, bb = &bb}
	bloom_ms := profile_median(p.reps, profile_do_bloom, &bloom_ctx)
	profile_emit(p, "bloom", bloom_ms, f64(p.rows), 0, 0)

	compress_ctx := Profile_Compress_Ctx{raws = raws}
	compress_ms := profile_median(p.reps, profile_do_compress, &compress_ctx)
	ratio := f64(0)
	if compress_ctx.comp_bytes > 0 {
		ratio = f64(encode_ctx.encoded_bytes) / f64(compress_ctx.comp_bytes)
	}
	profile_emit(p, "compress", compress_ms, f64(p.rows), encode_ctx.encoded_bytes, compress_ctx.comp_bytes)
	fmt.printf("  compress ratio %.2fx\n", ratio)

	// End-to-end writers: the serial path is the baseline the parallel path
	// has to match byte-for-byte (see logdb/flush_pool_test.odin).
	tmp := fmt.aprintf("%s/flush_profile", p.data)
	defer delete(tmp)
	os.remove_all(tmp)
	defer os.remove_all(tmp)

	serial_ctx := Profile_Write_Ctx{rows = rows, dir = tmp, block_rows = p.block_rows}
	serial_ms := profile_median(p.reps, profile_do_write_serial, &serial_ctx)
	profile_emit_write(p, 1, serial_ms, f64(p.rows))

	threads := profile_threads(p.threads)
	defer delete(threads)
	for n in threads {
		par_ctx := Profile_Write_Ctx{rows = rows, dir = tmp, block_rows = p.block_rows, threads = n}
		par_ms := profile_median(p.reps, profile_do_write_parallel, &par_ctx)
		profile_emit_write(p, n, par_ms, f64(p.rows))
	}
}

// ---------------------------------------------------------------- phases

Profile_Sort_Ctx :: struct {
	rows: []logdb.Row,
	rng:  u64,
}

profile_do_sort :: proc(rep: int, ctx: rawptr) -> f64 {
	c := cast(^Profile_Sort_Ctx)ctx
	profile_shuffle(c.rows, &c.rng)
	begin := time.now()
	logdb.sort_rows(c.rows)
	return millis(time.since(begin))
}

Profile_Encode_Ctx :: struct {
	rows:          []logdb.Row,
	spans:         []logdb.Block_Span,
	buf:           ^common.Buf,
	bb:            ^logdb.Bloom_Builder,
	raws:          [][]u8,
	encoded_bytes: i64,
	captured:      bool,
}

profile_do_encode :: proc(rep: int, ctx: rawptr) -> f64 {
	c := cast(^Profile_Encode_Ctx)ctx
	begin := time.now()
	for sp, i in c.spans {
		logdb.bloom_builder_reset(c.bb)
		_, _, ok := logdb.encode_block(c.rows[sp.off:sp.off + sp.nrows], c.buf, c.bb)
		if !ok {
			return -1
		}
		if !c.captured {
			c.raws[i] = make([]u8, len(c.buf.data))
			copy(c.raws[i], c.buf.data[:])
			c.encoded_bytes += i64(len(c.buf.data))
		}
	}
	c.captured = true
	return millis(time.since(begin))
}

Profile_Bloom_Ctx :: struct {
	rows:  []logdb.Row,
	spans: []logdb.Block_Span,
	bb:    ^logdb.Bloom_Builder,
}

profile_do_bloom :: proc(rep: int, ctx: rawptr) -> f64 {
	c := cast(^Profile_Bloom_Ctx)ctx
	begin := time.now()
	for sp in c.spans {
		logdb.bloom_builder_reset(c.bb)
		for ri in sp.off ..< sp.off + sp.nrows {
			r := &c.rows[ri]
			logdb.bloom_builder_add_value(c.bb, "_msg", r.msg)
			for f in r.fields {
				logdb.bloom_builder_add_value(c.bb, f.name, f.value)
			}
		}
	}
	return millis(time.since(begin))
}

Profile_Compress_Ctx :: struct {
	raws:       [][]u8,
	comp_bytes: i64,
}

profile_do_compress :: proc(rep: int, ctx: rawptr) -> f64 {
	c := cast(^Profile_Compress_Ctx)ctx
	begin := time.now()
	c.comp_bytes = 0
	for raw in c.raws {
		if len(raw) == 0 {
			continue
		}
		comp, ok := common.zlib_compress(raw, 1)
		if !ok {
			return -1
		}
		c.comp_bytes += i64(len(comp))
		delete(comp)
	}
	return millis(time.since(begin))
}

Profile_Write_Ctx :: struct {
	rows:       []logdb.Row,
	dir:        string,
	block_rows: int,
	threads:    int,
}

profile_do_write_serial :: proc(rep: int, ctx: rawptr) -> f64 {
	c := cast(^Profile_Write_Ctx)ctx
	dir := fmt.aprintf("%s/serial_%d", c.dir, rep)
	defer delete(dir)
	os.remove_all(dir)
	begin := time.now()
	part, ok := logdb.write_part(dir, c.rows, c.block_rows)
	ms := millis(time.since(begin))
	if ok {
		logdb.close_part(part)
	}
	return ms
}

profile_do_write_parallel :: proc(rep: int, ctx: rawptr) -> f64 {
	c := cast(^Profile_Write_Ctx)ctx
	dir := fmt.aprintf("%s/par_%d_%d", c.dir, c.threads, rep)
	defer delete(dir)
	os.remove_all(dir)
	begin := time.now()
	part, ok := logdb.write_part_parallel(dir, c.rows, c.block_rows, c.threads)
	ms := millis(time.since(begin))
	if ok {
		logdb.close_part(part)
	}
	return ms
}

// ---------------------------------------------------------------- helpers

profile_shuffle :: proc(rows: []logdb.Row, state: ^u64) {
	for i := len(rows) - 1; i > 0; i -= 1 {
		state^ = state^ * 6364136223846793005 + 1442695040888963407
		j := int((state^ >> 33) % u64(i + 1))
		rows[i], rows[j] = rows[j], rows[i]
	}
}

profile_median :: proc(reps: int, body: proc(rep: int, ctx: rawptr) -> f64, ctx: rawptr) -> f64 {
	vals := make([]f64, reps)
	defer delete(vals)
	for i in 0 ..< reps {
		vals[i] = body(i, ctx)
	}
	slice.sort(vals)
	return vals[reps / 2]
}

// profile_threads returns the thread counts worth measuring: 2, 4 and 8 when
// the machine allows them, plus the requested maximum.
profile_threads :: proc(max_threads: int) -> [dynamic]int {
	candidates := [3]int{2, 4, 8}
	out := make([dynamic]int, 0, 4)
	for n in candidates {
		if n <= max_threads && n > 1 {
			append(&out, n)
		}
	}
	append(&out, max(1, max_threads))
	slice.sort(out[:])
	uniq := make([dynamic]int, 0, len(out))
	for v in out {
		if len(uniq) == 0 || uniq[len(uniq) - 1] != v {
			append(&uniq, v)
		}
	}
	delete(out)
	return uniq
}

profile_emit :: proc(p: Params, phase: string, ms, rows: f64, raw_bytes, comp_bytes: i64) {
	rows_per_s := f64(0)
	if ms > 0 {
		rows_per_s = rows / (ms / 1000.0)
	}
	if p.json {
		fmt.printf(
			`{{"scenario":"flush_profile","phase":"%s","rows":%d,"ms":%.3f,"rows_per_s":%.0f,"raw_bytes":%d,"comp_bytes":%d}}`,
			phase, p.rows, ms, rows_per_s, raw_bytes, comp_bytes,
		)
		fmt.println()
	} else {
		fmt.printf("  %-9s %.2f ms  %.0f rows/s\n", phase, ms, rows_per_s)
	}
}

profile_emit_write :: proc(p: Params, threads: int, ms, rows: f64) {
	rows_per_s := f64(0)
	if ms > 0 {
		rows_per_s = rows / (ms / 1000.0)
	}
	if p.json {
		fmt.printf(
			`{{"scenario":"flush_write","threads":%d,"rows":%d,"ms":%.3f,"rows_per_s":%.0f}}`,
			threads, p.rows, ms, rows_per_s,
		)
		fmt.println()
	} else {
		fmt.printf("  write/%-3d %.2f ms  %.0f rows/s\n", threads, ms, rows_per_s)
	}
}
