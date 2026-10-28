package oboe

import "core:encoding/json"
import "core:fmt"
import "core:os"
import "core:slice"
import "core:strings"

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
	index_buf:  Buf,
	block_buf:  Buf,
	bloom_b:    Bloom_Builder,
	rows:       [dynamic]Row,
	arena:      Arena,
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
	copy_rows:  bool,
}

part_writer_begin :: proc(part_dir: string, block_rows: int, copy_rows: bool) -> ^Part_Writer {
	os.make_directory_all(part_dir)
	pw := new(Part_Writer)
	pw.dir = strings.clone(part_dir)
	pw.block_rows = block_rows
	pw.copy_rows = copy_rows
	pw.data_path = fmt.aprintf("%s/data.bin", part_dir)
	pw.bloom_path = fmt.aprintf("%s/bloom.bin", part_dir)
	pw.index_path = fmt.aprintf("%s/index.bin", part_dir)
	pw.meta_path = fmt.aprintf("%s/meta.json", part_dir)
	pw.index_buf = buf_init(4096)
	pw.block_buf = buf_init(1 << 16)
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
	buf_u32le(&pw.index_buf, u32(0x494c444f))
	buf_u16le(&pw.index_buf, INDEX_VERSION)
	buf_u16le(&pw.index_buf, INDEX_RECORD_SIZE)
	buf_u32le(&pw.index_buf, u32(0))
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
	if pw.copy_rows {
		r.msg = arena_str(&pw.arena, row.msg)
		fields := arena_alloc(&pw.arena, len(row.fields) * size_of(Field), align_of(Field))
		fs := slice.from_ptr(cast(^Field)raw_data(fields), len(row.fields))
		for i in 0..<len(row.fields) {
			fs[i] = Field{
				name = arena_str(&pw.arena, row.fields[i].name),
				value = arena_str(&pw.arena, row.fields[i].value),
			}
		}
		r.fields = fs
	}
	pw.stream = row.stream
	pw.has_stream = true
	append(&pw.rows, r)
	return true
}

part_writer_flush_block :: proc(pw: ^Part_Writer) -> bool {
	if len(pw.rows) == 0 {
		return true
	}
	bloom_builder_reset(&pw.bloom_b)
	bmin, bmax, eok := encode_block(pw.rows[:], &pw.block_buf, &pw.bloom_b)
	if !eok {
		pw.ok = false
		return false
	}
	comp, cok := zlib_compress(pw.block_buf.data[:], 1)
	if !cok {
		pw.ok = false
		return false
	}
	defer delete(comp)
	if !write_all(pw.data_file, comp) {
		pw.ok = false
		return false
	}
	bloom_bits := bloom_builder_finish(&pw.bloom_b)
	defer delete(bloom_bits)
	if !write_all(pw.bloom_file, bloom_bits) {
		pw.ok = false
		return false
	}
	buf_u64le(&pw.index_buf, pw.stream)
	buf_i64le(&pw.index_buf, bmin)
	buf_i64le(&pw.index_buf, bmax)
	buf_u32le(&pw.index_buf, u32(len(pw.rows)))
	buf_u32le(&pw.index_buf, u32(len(pw.block_buf.data)))
	buf_u32le(&pw.index_buf, u32(len(comp)))
	buf_u32le(&pw.index_buf, 0)
	buf_u64le(&pw.index_buf, pw.data_off)
	buf_u64le(&pw.index_buf, pw.bloom_off)
	buf_u32le(&pw.index_buf, u32(len(bloom_bits)))
	buf_u32le(&pw.index_buf, 0)
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
	clear(&pw.rows)
	arena_reset(&pw.arena)
	return true
}

part_writer_finish :: proc(pw: ^Part_Writer) -> (^Disk_Part, bool) {
	if !pw.ok || !part_writer_flush_block(pw) {
		part_writer_abort(pw)
		return nil, false
	}
	os.close(pw.data_file)
	pw.data_file = nil
	os.close(pw.bloom_file)
	pw.bloom_file = nil
	buf_put_u32_at(&pw.index_buf, 8, pw.nblocks)
	index_file, ierr := os.open(pw.index_path, {.Write, .Create, .Trunc})
	if ierr != nil {
		part_writer_abort(pw)
		return nil, false
	}
	iok := write_all(index_file, pw.index_buf.data[:])
	os.close(index_file)
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
		created_ns = now_ns(),
	}
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
	dir := strings.clone(pw.dir)
	part_writer_destroy(pw)
	p, pok := open_part(dir)
	if !pok {
		os.remove_all(dir)
		delete(dir)
		return nil, false
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
	buf_destroy(&pw.index_buf)
	buf_destroy(&pw.block_buf)
	delete(pw.bloom_b.hashes)
	delete(pw.bloom_b.table)
	delete(pw.rows)
	arena_destroy(&pw.arena)
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
	pw := part_writer_begin(part_dir, block_rows, false)
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
	if read_u16le(index_bytes, 4) != INDEX_VERSION || read_u16le(index_bytes, 6) != INDEX_RECORD_SIZE {
		return nil, false
	}
	nblocks := int(read_u32le(index_bytes, 8))
	if nblocks < 0 || nblocks > 10_000_000 || 12 + nblocks * INDEX_RECORD_SIZE > len(index_bytes) {
		return nil, false
	}
	index := make([]Block_Index, nblocks)
	for k in 0..<nblocks {
		at := 12 + k * INDEX_RECORD_SIZE
		index[k] = Block_Index{
			stream = read_u64le(index_bytes, at),
			min_ts = read_i64le(index_bytes, at + 8),
			max_ts = read_i64le(index_bytes, at + 16),
			rows = read_u32le(index_bytes, at + 24),
			raw_len = read_u32le(index_bytes, at + 28),
			comp_len = read_u32le(index_bytes, at + 32),
			data_off = read_u64le(index_bytes, at + 40),
			bloom_off = read_u64le(index_bytes, at + 48),
			bloom_len = read_u32le(index_bytes, at + 56),
		}
	}
	bloom_bytes, berr := os.read_entire_file_from_path(bloom_path, context.allocator)
	if berr != nil {
		delete(index)
		return nil, false
	}
	data_file, derr := os.open(data_path, {.Read})
	if derr != nil {
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
	raw, ok := zlib_decompress(comp, int(bi.raw_len))
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
