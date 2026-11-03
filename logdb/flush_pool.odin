// Parallel part writing. Sorting a memtable already groups rows by stream, so
// the block boundaries are known before any encoding starts: a block closes
// when the stream changes or block_rows rows are reached, exactly like
// part_writer_add. Blocks share no state, which lets the CPU-heavy work
// (column encoding, bloom hashing, zlib) run on several threads while a single
// thread writes the results out in block order.
package logdb

import "../common"
import "core:mem"
import "core:slice"
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
	for {
		i := int(sync.atomic_add(&c.cursor, 1))
		if i >= len(c.spans) {
			break
		}
		sp := c.spans[i]
		block := c.rows[sp.off:sp.off + sp.nrows]
		bloom_builder_reset(&bb)
		min_ts, max_ts, eok := encode_block(block, &buf, &bb)
		if !eok {
			c.outs[i] = Block_Out{ok = false}
			continue
		}
		comp, cok := common.zlib_compress(buf.data[:], 1)
		if !cok {
			c.outs[i] = Block_Out{ok = false}
			continue
		}
		bits := bloom_builder_finish(&bb)
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
	if !write_all(pw.data_file, out.comp) || !write_all(pw.bloom_file, out.bloom) {
		pw.ok = false
		return false
	}
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

// MERGE_WINDOW_BLOCKS bounds how many blocks a merge accumulates before
// handing them to the parallel encoder. A window is 32 blocks (262k rows at the
// default block size), which is plenty of work for the encoder while keeping
// the copied rows and their strings to a few tens of megabytes.
MERGE_WINDOW_BLOCKS :: 32

// Part_Stream writes a single part from a stream of already sorted rows whose
// source is decoded block by block (a merge). Rows are copied into a window
// with their own arena because the cursors free their decoded blocks as they
// advance; when the window fills up it goes through the same parallel
// encode/write path as a flush, keeping the part's block layout identical.
Part_Stream :: struct {
	pw:         ^Part_Writer,
	rows:       [dynamic]Row,
	arena:      common.Arena,
	block_rows: int,
	threads:    int,
	window:     int,
	ok:         bool,
}

part_stream_begin :: proc(part_dir: string, block_rows, threads: int) -> Part_Stream {
	ps := Part_Stream{}
	ps.pw = part_writer_begin(part_dir, block_rows, false)
	if ps.pw == nil {
		return ps
	}
	ps.block_rows = block_rows
	ps.threads = threads
	ps.window = max(block_rows, MERGE_WINDOW_BLOCKS * block_rows)
	ps.rows = make([dynamic]Row, 0, 4096)
	ps.ok = true
	return ps
}

part_stream_add :: proc(ps: ^Part_Stream, row: Row) -> bool {
	if !ps.ok {
		return false
	}
	fields := common.arena_alloc(&ps.arena, len(row.fields) * size_of(Field), align_of(Field))
	fs := slice.from_ptr(cast(^Field)raw_data(fields), len(row.fields))
	for i in 0..<len(row.fields) {
		fs[i] = Field{
			name  = common.arena_str(&ps.arena, row.fields[i].name),
			value = common.arena_str(&ps.arena, row.fields[i].value),
		}
	}
	append(&ps.rows, Row{
		ts     = row.ts,
		stream = row.stream,
		msg    = common.arena_str(&ps.arena, row.msg),
		fields = fs,
	})
	if len(ps.rows) >= ps.window {
		return part_stream_flush(ps)
	}
	return true
}

part_stream_flush :: proc(ps: ^Part_Stream) -> bool {
	if !ps.ok {
		return false
	}
	if len(ps.rows) == 0 {
		return true
	}
	spans := block_spans(ps.rows[:], ps.block_rows)
	if len(spans) == 0 {
		delete(spans)
		return true
	}
	defer delete(spans)
	outs := make([]Block_Out, len(spans))
	defer delete(outs)
	ok := encode_blocks(ps.rows[:], spans, outs, ps.threads)
	if ok {
		for i in 0..<len(spans) {
			if !part_writer_add_encoded(ps.pw, outs[i]) {
				ok = false
				break
			}
		}
	}
	release_block_outs(outs)
	ps.ok = ok
	clear(&ps.rows)
	common.arena_reset(&ps.arena)
	return ok
}

part_stream_finish :: proc(ps: ^Part_Stream) -> (^Disk_Part, bool) {
	if !ps.ok || !part_stream_flush(ps) {
		part_stream_abort(ps)
		return nil, false
	}
	part, ok := part_writer_finish(ps.pw)
	part_stream_destroy(ps)
	return part, ok
}

part_stream_abort :: proc(ps: ^Part_Stream) {
	if ps.pw != nil {
		part_writer_abort(ps.pw)
	}
	part_stream_destroy(ps)
}

part_stream_destroy :: proc(ps: ^Part_Stream) {
	delete(ps.rows)
	common.arena_destroy(&ps.arena)
	ps.pw = nil
	ps.ok = false
}

// write_part_parallel writes a part using `threads` encode threads. Rows must
// already be sorted by (stream, ts), as store_flush_partition guarantees.
write_part_parallel :: proc(part_dir: string, rows: []Row, block_rows, threads: int) -> (^Disk_Part, bool) {
	if len(rows) == 0 {
		return nil, false
	}
	pw := part_writer_begin(part_dir, block_rows, false)
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
