package logdb

import "../common"
import "core:encoding/json"
import "core:fmt"
import "core:os"
import "core:slice"
import "core:strings"
import "core:sync"
import "core:time"

INDEX_MAGIC :: "ODLI"
INDEX_VERSION :: u16(1)
INDEX_RECORD_SIZE :: 64

Part_Meta :: struct {
	version:    int    `json:"version"`,
	id:         string `json:"id"`,
	rows:       int    `json:"rows"`,
	blocks:     int    `json:"blocks"`,
	min_ts:     i64    `json:"min_ts"`,
	max_ts:     i64    `json:"max_ts"`,
	data_size:  i64    `json:"data_size"`,
	bloom_size: i64    `json:"bloom_size"`,
	index_size: i64    `json:"index_size"`,
	created_ns: i64    `json:"created_ns"`,
}

Block_Index :: struct {
	stream:    u64,
	min_ts:    i64,
	max_ts:    i64,
	rows:      u32,
	raw_len:   u32,
	comp_len:  u32,
	data_off:  u64,
	bloom_off: u64,
	bloom_len: u32,
}

Disk_Part :: struct {
	meta:      Part_Meta,
	dir:       string,
	data_file: ^os.File,
	bloom:     []u8,
	index:     []Block_Index,
	// refs counts readers that captured the part pointer and still need to scan
	// it. unlinked marks a part that has been removed from its partition (by a
	// merge or retention); its last reader is responsible for closing it, so a
	// query can keep scanning after the store lock is released.
	refs:     i64,
	unlinked: bool,
}

write_all :: proc(f: ^os.File, data: []u8) -> bool {
	remaining := data
	for len(remaining) > 0 {
		n, err := os.write(f, remaining)
		if err != nil || n <= 0 {
			return false
		}
		remaining = remaining[n:]
	}
	return true
}

read_full_at :: proc(f: ^os.File, data: []u8, offset: i64) -> bool {
	remaining := data
	off := offset
	for len(remaining) > 0 {
		n, err := os.read_at(f, remaining, off)
		if err != nil || n <= 0 {
			return false
		}
		remaining = remaining[n:]
		off += i64(n)
	}
	return true
}

base_name :: proc(p: string) -> string {
	i := strings.last_index_byte(p, '/')
	if i < 0 {
		return p
	}
	return p[i+1:]
}

// ---------------------------------------------------------------- part writer

Part_Writer :: struct {
	dir:        string,
	data_path:  string,
	bloom_path: string,
	index_path: string,
	meta_path:  string,
	data_file:  ^os.File,
	bloom_file: ^os.File,
	index_buf:  common.Buf,
	block_buf:  common.Buf,
	bloom_b:    Bloom_Builder,
	rows:       [dynamic]Row,
	block_rows: int,
	stream:     u64,
	has_stream: bool,
	data_off:   u64,
	bloom_off:  u64,
	nblocks:    u32,
	total_rows: int,
	min_ts:     i64,
	max_ts:     i64,
	ok:         bool,
	prof_scope: Prof_Scope,
}

// The writer only buffers the rows handed to part_writer_add; it never owns or
// copies their strings. Callers either keep the rows alive until the block is
// flushed (write_part) or pin the decoded source blocks they point into (merge).
part_writer_begin :: proc(part_dir: string, block_rows: int, prof_scope := Prof_Scope.Flush) -> ^Part_Writer {
	os.make_directory_all(part_dir)
	pw := new(Part_Writer)
	pw.dir = strings.clone(part_dir)
	pw.block_rows = block_rows
	pw.prof_scope = prof_scope
	pw.data_path = fmt.aprintf("%s/data.bin", part_dir)
	pw.bloom_path = fmt.aprintf("%s/bloom.bin", part_dir)
	pw.index_path = fmt.aprintf("%s/index.bin", part_dir)
	pw.meta_path = fmt.aprintf("%s/meta.json", part_dir)
	pw.index_buf = common.buf_init(4096)
	pw.block_buf = common.buf_init(1 << 16)
	pw.min_ts = i64(0x7fffffffffffffff)
	pw.max_ts = i64(-0x7fffffffffffffff - 1)
	derr: os.Error
	pw.data_file, derr = os.open(pw.data_path, {.Write, .Create, .Trunc})
	if derr != nil {
		part_writer_abort(pw)
		return nil
	}
	pw.bloom_file, derr = os.open(pw.bloom_path, {.Write, .Create, .Trunc})
	if derr != nil {
		part_writer_abort(pw)
		return nil
	}
	common.buf_u32le(&pw.index_buf, u32(0x494c444f))
	common.buf_u16le(&pw.index_buf, INDEX_VERSION)
	common.buf_u16le(&pw.index_buf, INDEX_RECORD_SIZE)
	common.buf_u32le(&pw.index_buf, u32(0))
	pw.ok = true
	return pw
}

part_writer_add :: proc(pw: ^Part_Writer, row: Row) -> bool {
	if !pw.ok {
		return false
	}
	if pw.has_stream && (row.stream != pw.stream || len(pw.rows) >= pw.block_rows) {
		if !part_writer_flush_block(pw) {
			return false
		}
	}
	r := row
	pw.stream = row.stream
	pw.has_stream = true
	append(&pw.rows, r)
	return true
}

part_writer_flush_block :: proc(pw: ^Part_Writer) -> bool {
	if len(pw.rows) == 0 {
		return true
	}
	active := flush_profile_enabled()
	ph: ^Phase_Stats
	if active {
		ph = &flush_prof.phase[int(pw.prof_scope)]
	}
	bloom_builder_reset(&pw.bloom_b)
	bmin, bmax, eok := encode_block(pw.rows[:], &pw.block_buf, &pw.bloom_b, ph)
	if !eok {
		pw.ok = false
		return false
	}
	t := prof_stage_start(active)
	comp, cok := common.zlib_compress(pw.block_buf.data[:], 1)
	if active {
		ph.zlib_ns += prof_stage_end(true, t)
	}
	if !cok {
		pw.ok = false
		return false
	}
	defer delete(comp)
	t = prof_stage_start(active)
	data_ok := write_all(pw.data_file, comp)
	if active {
		ph.write_data_ns += prof_stage_end(true, t)
		ph.data_bytes += u64(len(comp))
	}
	if !data_ok {
		pw.ok = false
		return false
	}
	t = prof_stage_start(active)
	bloom_bits := bloom_builder_finish(&pw.bloom_b)
	if active {
		ph.bloom_finish_ns += prof_stage_end(true, t)
		ph.bloom_bytes += u64(len(bloom_bits))
	}
	// bloom_builder_finish returns freshly allocated bits; the write must
	// happen before the deferred delete.
	defer delete(bloom_bits)
	t = prof_stage_start(active)
	bloom_ok := write_all(pw.bloom_file, bloom_bits)
	if active {
		ph.write_bloom_ns += prof_stage_end(true, t)
	}
	if !bloom_ok {
		pw.ok = false
		return false
	}
	t = prof_stage_start(active)
	common.buf_u64le(&pw.index_buf, pw.stream)
	common.buf_i64le(&pw.index_buf, bmin)
	common.buf_i64le(&pw.index_buf, bmax)
	common.buf_u32le(&pw.index_buf, u32(len(pw.rows)))
	common.buf_u32le(&pw.index_buf, u32(len(pw.block_buf.data)))
	common.buf_u32le(&pw.index_buf, u32(len(comp)))
	common.buf_u32le(&pw.index_buf, 0)
	common.buf_u64le(&pw.index_buf, pw.data_off)
	common.buf_u64le(&pw.index_buf, pw.bloom_off)
	common.buf_u32le(&pw.index_buf, u32(len(bloom_bits)))
	common.buf_u32le(&pw.index_buf, 0)
	if active {
		ph.index_ns += prof_stage_end(true, t)
	}
	pw.data_off += u64(len(comp))
	pw.bloom_off += u64(len(bloom_bits))
	pw.nblocks += 1
	pw.total_rows += len(pw.rows)
	if bmin < pw.min_ts {
		pw.min_ts = bmin
	}
	if bmax > pw.max_ts {
		pw.max_ts = bmax
	}
	if active {
		flush_profile_record_block(pw.prof_scope, len(pw.rows), len(pw.block_buf.data), len(comp))
	}
	clear(&pw.rows)
	return true
}

part_writer_finish :: proc(pw: ^Part_Writer) -> (^Disk_Part, bool) {
	if !pw.ok || !part_writer_flush_block(pw) {
		part_writer_abort(pw)
		return nil, false
	}
	active := flush_profile_enabled()
	ph: ^Phase_Stats
	if active {
		ph = &flush_prof.phase[int(pw.prof_scope)]
	}
	os.close(pw.data_file)
	pw.data_file = nil
	os.close(pw.bloom_file)
	pw.bloom_file = nil
	common.buf_put_u32_at(&pw.index_buf, 8, pw.nblocks)
	t := prof_stage_start(active)
	index_file, ierr := os.open(pw.index_path, {.Write, .Create, .Trunc})
	if ierr != nil {
		part_writer_abort(pw)
		return nil, false
	}
	iok := write_all(index_file, pw.index_buf.data[:])
	os.close(index_file)
	if active {
		ph.finish_index_ns += prof_stage_end(true, t)
	}
	if !iok {
		part_writer_abort(pw)
		return nil, false
	}
	meta := Part_Meta{
		version = 1,
		id = base_name(pw.dir),
		rows = pw.total_rows,
		blocks = int(pw.nblocks),
		min_ts = pw.min_ts,
		max_ts = pw.max_ts,
		data_size = i64(pw.data_off),
		bloom_size = i64(pw.bloom_off),
		index_size = i64(len(pw.index_buf.data)),
		created_ns = common.now_ns(),
	}
	t = prof_stage_start(active)
	meta_bytes, merr := json.marshal(meta)
	if merr != nil {
		part_writer_abort(pw)
		return nil, false
	}
	defer delete(meta_bytes)
	if werr := os.write_entire_file(pw.meta_path, meta_bytes); werr != nil {
		part_writer_abort(pw)
		return nil, false
	}
	if active {
		ph.finish_meta_ns += prof_stage_end(true, t)
	}
	dir := strings.clone(pw.dir)
	part_writer_destroy(pw)
	t = prof_stage_start(active)
	p, pok := open_part(dir)
	if active {
		ph.finish_open_ns += prof_stage_end(true, t)
	}
	if !pok {
		os.remove_all(dir)
		delete(dir)
		return nil, false
	}
	if active {
		ph.parts += 1
	}
	delete(dir)
	return p, true
}

part_writer_abort :: proc(pw: ^Part_Writer) {
	if pw.data_file != nil {
		os.close(pw.data_file)
		pw.data_file = nil
	}
	if pw.bloom_file != nil {
		os.close(pw.bloom_file)
		pw.bloom_file = nil
	}
	os.remove_all(pw.dir)
	part_writer_destroy(pw)
}

part_writer_destroy :: proc(pw: ^Part_Writer) {
	common.buf_destroy(&pw.index_buf)
	common.buf_destroy(&pw.block_buf)
	delete(pw.bloom_b.hashes)
	delete(pw.bloom_b.table)
	delete(pw.rows)
	delete(pw.dir)
	delete(pw.data_path)
	delete(pw.bloom_path)
	delete(pw.index_path)
	delete(pw.meta_path)
	free(pw)
}

write_part :: proc(part_dir: string, rows: []Row, block_rows: int) -> (^Disk_Part, bool) {
	if len(rows) == 0 {
		return nil, false
	}
	pw := part_writer_begin(part_dir, block_rows)
	if pw == nil {
		return nil, false
	}
	for i in 0..<len(rows) {
		if !part_writer_add(pw, rows[i]) {
			part_writer_abort(pw)
			return nil, false
		}
	}
	p2, ok2 := part_writer_finish(pw)
	return p2, ok2
}

// ---------------------------------------------------------------- part reader

open_part :: proc(dir: string) -> (^Disk_Part, bool) {
	meta_path := fmt.aprintf("%s/meta.json", dir)
	defer delete(meta_path)
	meta_bytes, merr := os.read_entire_file_from_path(meta_path, context.allocator)
	if merr != nil {
		return nil, false
	}
	defer delete(meta_bytes)
	meta: Part_Meta
	if json.unmarshal(meta_bytes, &meta) != nil {
		return nil, false
	}
	// json.unmarshal allocates meta.id; every rejection path below must release
	// it again or a corrupt part leaks one string per startup.
	opened := false
	defer if !opened { delete(meta.id) }
	if meta.version != 1 {
		return nil, false
	}
	index_path := fmt.aprintf("%s/index.bin", dir)
	bloom_path := fmt.aprintf("%s/bloom.bin", dir)
	data_path := fmt.aprintf("%s/data.bin", dir)
	defer {
		delete(index_path)
		delete(bloom_path)
		delete(data_path)
	}
	index_bytes, ierr := os.read_entire_file_from_path(index_path, context.allocator)
	if ierr != nil {
		return nil, false
	}
	defer delete(index_bytes)
	if len(index_bytes) < 12 || string(index_bytes[0:4]) != INDEX_MAGIC {
		return nil, false
	}
	if common.read_u16le(index_bytes, 4) != INDEX_VERSION || common.read_u16le(index_bytes, 6) != INDEX_RECORD_SIZE {
		return nil, false
	}
	nblocks := int(common.read_u32le(index_bytes, 8))
	if nblocks < 0 || nblocks > 10_000_000 || 12 + nblocks * INDEX_RECORD_SIZE > len(index_bytes) {
		return nil, false
	}
	if meta.blocks != nblocks || meta.index_size != i64(len(index_bytes)) {
		return nil, false
	}
	index := make([]Block_Index, nblocks)
	for k in 0..<nblocks {
		at := 12 + k * INDEX_RECORD_SIZE
		bi := Block_Index{
			stream = common.read_u64le(index_bytes, at),
			min_ts = common.read_i64le(index_bytes, at + 8),
			max_ts = common.read_i64le(index_bytes, at + 16),
			rows = common.read_u32le(index_bytes, at + 24),
			raw_len = common.read_u32le(index_bytes, at + 28),
			comp_len = common.read_u32le(index_bytes, at + 32),
			data_off = common.read_u64le(index_bytes, at + 40),
			bloom_off = common.read_u64le(index_bytes, at + 48),
			bloom_len = common.read_u32le(index_bytes, at + 56),
		}
		// Refuse block descriptors that point outside the data/bloom files; a
		// truncated or corrupted part must not be served.
		if bi.rows == 0 || bi.raw_len == 0 || bi.comp_len == 0 ||
		   bi.data_off + u64(bi.comp_len) > u64(meta.data_size) ||
		   bi.bloom_off + u64(bi.bloom_len) > u64(meta.bloom_size) {
			delete(index)
			return nil, false
		}
		index[k] = bi
	}
	bloom_bytes, berr := os.read_entire_file_from_path(bloom_path, context.allocator)
	if berr != nil {
		delete(index)
		return nil, false
	}
	if meta.bloom_size != i64(len(bloom_bytes)) {
		delete(index)
		delete(bloom_bytes)
		return nil, false
	}
	data_file, derr := os.open(data_path, {.Read})
	if derr != nil {
		delete(index)
		delete(bloom_bytes)
		return nil, false
	}
	if size, serr := os.file_size(data_file); serr != nil || size != meta.data_size {
		os.close(data_file)
		delete(index)
		delete(bloom_bytes)
		return nil, false
	}
	p := new(Disk_Part)
	p.meta = meta
	p.dir = strings.clone(dir)
	p.data_file = data_file
	p.bloom = bloom_bytes
	p.index = index
	opened = true
	return p, true
}

close_part :: proc(p: ^Disk_Part) {
	if p.data_file != nil {
		os.close(p.data_file)
	}
	delete(p.bloom)
	delete(p.index)
	delete(p.dir)
	delete(p.meta.id)
	free(p)
}

// part_retain must be called while holding the store's shared lock, so it
// cannot race with the write-locked unlink.
part_retain :: proc(p: ^Disk_Part) {
	sync.atomic_add(&p.refs, 1)
}

// part_release drops a reader's reference and closes the part when it was
// unlinked in the meantime. Only one caller can observe the transition to zero,
// so the directory removal and close happen exactly once.
part_release :: proc(p: ^Disk_Part) {
	// atomic_sub returns the value before the subtraction.
	if sync.atomic_sub(&p.refs, 1) == 1 && sync.atomic_load(&p.unlinked) {
		os.remove_all(p.dir)
		close_part(p)
	}
}

// part_unlink detaches a part from the store. Callers must hold the exclusive
// lock; readers that already captured the pointer keep it alive until they
// release it.
part_unlink :: proc(p: ^Disk_Part) {
	sync.atomic_store(&p.unlinked, true)
	if sync.atomic_load(&p.refs) == 0 {
		os.remove_all(p.dir)
		close_part(p)
	}
}

part_bloom :: proc(p: ^Disk_Part, bi: ^Block_Index) -> Bloom {
	off := int(bi.bloom_off)
	n := int(bi.bloom_len)
	if off < 0 || n <= 0 || off + n > len(p.bloom) {
		return Bloom{}
	}
	return bloom_from_bytes(p.bloom[off:off+n])
}

read_block :: proc(p: ^Disk_Part, bi: ^Block_Index) -> (Decoded_Block, bool) {
	if bi.comp_len == 0 || bi.raw_len == 0 {
		return {}, false
	}
	comp := make([]u8, int(bi.comp_len))
	defer delete(comp)
	if !read_full_at(p.data_file, comp, i64(bi.data_off)) {
		return {}, false
	}
	raw, ok := common.zlib_decompress(comp, int(bi.raw_len))
	if !ok {
		return {}, false
	}
	d, dok := decode_block(raw)
	if !dok {
		delete(raw)
		return {}, false
	}
	return d, true
}

part_total_size :: proc(p: ^Disk_Part) -> i64 {
	return p.meta.data_size + p.meta.bloom_size + p.meta.index_size
}
