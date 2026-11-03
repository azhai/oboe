// Parallel part writing. Sorting a memtable already groups rows by stream, so
// the block boundaries are known before any encoding starts: a block closes
// when the stream changes or block_rows rows are reached, exactly like
// part_writer_add. Blocks share no state, which lets the CPU-heavy work
// (column encoding, bloom hashing, zlib) run on several threads while a single
// thread writes the results out in block order.
package logdb

import "../common"
import "core:mem"
import "core:sync"
import "core:thread"

// FLUSH_CHUNK_BLOCKS caps how many encoded blocks are buffered between the
// encoder and the writer. At the default block_rows of 8192 this is roughly
// 1M rows of compressed output, which keeps a long flush bounded in memory
// without giving up parallelism on ordinary flushes.
FLUSH_CHUNK_BLOCKS :: 128

Block_Span :: struct {
	off:    int,
	nrows:  int,
	stream: u64,
}

Block_Out :: struct {
	comp:    []u8,
	bloom:   []u8,
	raw_len: u32,
	nrows:   u32,
	stream:  u64,
	min_ts:  i64,
	max_ts:  i64,
	ok:      bool,
}

// block_spans partitions already sorted rows into the blocks that
// part_writer_add would emit. The caller owns the returned slice.
block_spans :: proc(rows: []Row, block_rows: int) -> []Block_Span {
	if len(rows) == 0 || block_rows <= 0 {
		return nil
	}
	spans := make([dynamic]Block_Span, 0, len(rows) / block_rows + 2)
	start := 0
	stream := rows[0].stream
	for i in 1 ..< len(rows) {
		if rows[i].stream != stream || i - start >= block_rows {
			append(&spans, Block_Span{off = start, nrows = i - start, stream = stream})
			start = i
			stream = rows[i].stream
		}
	}
	append(&spans, Block_Span{off = start, nrows = len(rows) - start, stream = stream})
	return spans[:]
}

Encode_Ctx :: struct {
	rows:   []Row,
	spans:  []Block_Span,
	outs:   []Block_Out,
	cursor: u32,
	// alloc comes from the calling thread. Buffers are allocated in the worker
	// and freed by the writer, so both sides must use the same allocator.
	alloc: mem.Allocator,
}

encode_worker :: proc(data: rawptr) {
	c := cast(^Encode_Ctx)data
	context.allocator = c.alloc
	buf := common.buf_init(1 << 16)
	defer common.buf_destroy(&buf)
	bb := Bloom_Builder{}
	defer {
		delete(bb.hashes)
		delete(bb.table)
	}
	// Each worker keeps its own counters and folds them into the shared
	// profile once, so blocks encoded in parallel do not race on it.
	active := flush_profile_enabled()
	local: Phase_Stats
	if active {
		local.min_rows = ~u64(0)
		local.min_raw = ~u64(0)
	}
	for {
		i := int(sync.atomic_add(&c.cursor, 1))
		if i >= len(c.spans) {
			break
		}
		sp := c.spans[i]
		block := c.rows[sp.off:sp.off + sp.nrows]
		bloom_builder_reset(&bb)
		prof_var: ^Phase_Stats
		if active {
			prof_var = &local
		}
		min_ts, max_ts, eok := encode_block(block, &buf, &bb, prof_var)
		if !eok {
			c.outs[i] = Block_Out{ok = false}
			continue
		}
		t := prof_stage_start(active)
		comp, cok := common.zlib_compress(buf.data[:], 1)
		if active {
			local.zlib_ns += prof_stage_end(true, t)
		}
		if !cok {
			c.outs[i] = Block_Out{ok = false}
			continue
		}
		t = prof_stage_start(active)
		bits := bloom_builder_finish(&bb)
		if active {
			local.bloom_finish_ns += prof_stage_end(true, t)
			local.bloom_bytes += u64(len(bits))
			flush_profile_record_local_block(&local, sp.nrows, len(buf.data), len(comp))
		}
		c.outs[i] = Block_Out{
			comp    = comp,
			bloom   = bits,
			raw_len = u32(len(buf.data)),
			nrows   = u32(sp.nrows),
			stream  = sp.stream,
			min_ts  = min_ts,
			max_ts  = max_ts,
			ok      = true,
		}
	}
	if active {
		flush_profile_merge_phase(.Flush, &local)
	}
}

// encode_blocks fills outs[i] for every span i. Work is claimed with an atomic
// cursor, so a failed thread_create only lowers parallelism: the calling thread
// keeps encoding instead of stalling the flush.
encode_blocks :: proc(rows: []Row, spans: []Block_Span, outs: []Block_Out, threads: int) -> bool {
	if len(spans) == 0 {
		return true
	}
	ctx := Encode_Ctx{rows = rows, spans = spans, outs = outs, alloc = context.allocator}
	wanted := clamp(threads, 1, len(spans))
	spawned := make([dynamic]^thread.Thread, 0, wanted - 1)
	defer delete(spawned)
	for _ in 0 ..< wanted - 1 {
		t := thread.create_and_start_with_data(&ctx, encode_worker, self_cleanup = false, name = "oboe-encode")
		if t == nil {
			break
		}
		append(&spawned, t)
	}
	encode_worker(&ctx)
	for t in spawned {
		thread.join(t)
		thread.destroy(t)
	}
	for i in 0 ..< len(spans) {
		if !outs[i].ok {
			return false
		}
	}
	return true
}

release_block_outs :: proc(outs: []Block_Out) {
	for &o in outs {
		delete(o.comp)
		delete(o.bloom)
		o = {}
	}
}

// part_writer_add_encoded appends one encoded block. The index record layout
// and offset bookkeeping mirror part_writer_flush_block, so a parallel part is
// byte-for-byte identical to the serial one.
part_writer_add_encoded :: proc(pw: ^Part_Writer, out: Block_Out) -> bool {
	if !pw.ok || !out.ok {
		pw.ok = false
		return false
	}
	active := flush_profile_enabled()
	ph: ^Phase_Stats
	if active {
		ph = &flush_prof.phase[int(pw.prof_scope)]
	}
	t := prof_stage_start(active)
	data_ok := write_all(pw.data_file, out.comp)
	if active {
		ph.write_data_ns += prof_stage_end(true, t)
		ph.data_bytes += u64(len(out.comp))
	}
	t = prof_stage_start(active)
	bloom_ok := write_all(pw.bloom_file, out.bloom)
	if active {
		ph.write_bloom_ns += prof_stage_end(true, t)
		ph.bloom_bytes += u64(len(out.bloom))
	}
	if !data_ok || !bloom_ok {
		pw.ok = false
		return false
	}
	t = prof_stage_start(active)
	common.buf_u64le(&pw.index_buf, out.stream)
	common.buf_i64le(&pw.index_buf, out.min_ts)
	common.buf_i64le(&pw.index_buf, out.max_ts)
	common.buf_u32le(&pw.index_buf, out.nrows)
	common.buf_u32le(&pw.index_buf, out.raw_len)
	common.buf_u32le(&pw.index_buf, u32(len(out.comp)))
	common.buf_u32le(&pw.index_buf, 0)
	common.buf_u64le(&pw.index_buf, pw.data_off)
	common.buf_u64le(&pw.index_buf, pw.bloom_off)
	common.buf_u32le(&pw.index_buf, u32(len(out.bloom)))
	common.buf_u32le(&pw.index_buf, 0)
	if active {
		ph.index_ns += prof_stage_end(true, t)
	}
	pw.data_off += u64(len(out.comp))
	pw.bloom_off += u64(len(out.bloom))
	pw.nblocks += 1
	pw.total_rows += int(out.nrows)
	if out.min_ts < pw.min_ts {
		pw.min_ts = out.min_ts
	}
	if out.max_ts > pw.max_ts {
		pw.max_ts = out.max_ts
	}
	return true
}

// write_part_parallel writes a part using `threads` encode threads. Rows must
// already be sorted by (stream, ts), as store_flush_partition guarantees.
write_part_parallel :: proc(part_dir: string, rows: []Row, block_rows, threads: int) -> (^Disk_Part, bool) {
	if len(rows) == 0 {
		return nil, false
	}
	pw := part_writer_begin(part_dir, block_rows)
	if pw == nil {
		return nil, false
	}
	spans := block_spans(rows, block_rows)
	if len(spans) == 0 {
		part_writer_abort(pw)
		return nil, false
	}
	defer delete(spans)
	chunk := min(len(spans), FLUSH_CHUNK_BLOCKS)
	outs := make([]Block_Out, chunk)
	defer delete(outs)
	for base := 0; base < len(spans); base += chunk {
		n := min(chunk, len(spans) - base)
		if !encode_blocks(rows, spans[base:base + n], outs[:n], threads) {
			release_block_outs(outs[:n])
			part_writer_abort(pw)
			return nil, false
		}
		wrote := true
		for i in 0 ..< n {
			if !part_writer_add_encoded(pw, outs[i]) {
				wrote = false
				break
			}
		}
		release_block_outs(outs[:n])
		if !wrote {
			part_writer_abort(pw)
			return nil, false
		}
	}
	return part_writer_finish(pw)
}
