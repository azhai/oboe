package logdb

import "../common"
import "core:time"

Row :: struct {
	ts:     i64,
	stream: u64,
	msg:    string,
	fields: []Field,
}

BLOCK_MAGIC :: "ODLB"
BLOCK_VERSION :: u16(1)

// Column-name lookup table and per-column value cache sizes. Both are
// direct-mapped: a miss simply falls back to the full path, so collisions cost
// time, never correctness.
BLOCK_COL_SLOTS :: 32
BLOCK_VALUE_SLOTS :: 64

Enc_Col :: struct {
	name: string,
	vals: [dynamic]string,
	// fnv(name) + separator, computed once per column instead of once per value.
	base: u64,
	// Values already indexed into this block's bloom filter. The filter keeps
	// only the *set* of hashes (bloom_builder_finish collapses duplicates), so a
	// repeated (column, value) can be skipped without changing one output bit.
	keys:   [BLOCK_VALUE_SLOTS]u64,
	cached: [BLOCK_VALUE_SLOTS]string,
	used:   [BLOCK_VALUE_SLOTS]bool,
}

Enc_Schema :: struct {
	cols:  [dynamic]Enc_Col,
	slots: [BLOCK_COL_SLOTS]int, // 0 = empty, else column index + 1
}

// enc_key folds at most the last eight bytes plus the length. Field names and
// repeated field values share a suffix far more often than a prefix, so this
// separates them cheaply; a colliding key falls through to a string compare.
enc_key :: #force_inline proc(s: string) -> u64 {
	n := len(s)
	start := 0
	if n > 8 {
		start = n - 8
	}
	h := common.fnv(common.FNV_BASIS, s[start:n])
	return h ~ (u64(n) << 56)
}

// enc_schema_col resolves the column for `name`, appending it on first sight so
// the serialized column order stays first-appearance order.
enc_schema_col :: proc(sc: ^Enc_Schema, name: string, nrows: int) -> int {
	k := enc_key(name)
	slot := int(k) & (BLOCK_COL_SLOTS - 1)
	for _ in 0 ..< BLOCK_COL_SLOTS {
		v := sc.slots[slot]
		if v == 0 {
			idx := len(sc.cols)
			append(&sc.cols, Enc_Col{
				name = name,
				vals = make([dynamic]string, nrows),
				base = common.fnv_continue(common.fnv(common.FNV_BASIS, name), "\x00"),
			})
			sc.slots[slot] = idx + 1
			return idx
		}
		if sc.cols[v - 1].name == name {
			return v - 1
		}
		slot = (slot + 1) & (BLOCK_COL_SLOTS - 1)
	}
	// More distinct columns than lookup slots: fall back to a linear scan so the
	// behaviour stays correct for very wide blocks.
	for i in 0 ..< len(sc.cols) {
		if sc.cols[i].name == name {
			return i
		}
	}
	idx := len(sc.cols)
	append(&sc.cols, Enc_Col{
		name = name,
		vals = make([dynamic]string, nrows),
		base = common.fnv_continue(common.fnv(common.FNV_BASIS, name), "\x00"),
	})
	return idx
}

// enc_bloom_value indexes `value` for column `c` unless this block already did,
// in which case the hash set would be unchanged and the work is skipped.
enc_bloom_value :: #force_inline proc(bb: ^Bloom_Builder, c: ^Enc_Col, value: string) {
	k := enc_key(value)
	slot := int(k) & (BLOCK_VALUE_SLOTS - 1)
	if c.used[slot] && c.keys[slot] == k && c.cached[slot] == value {
		return
	}
	c.used[slot] = true
	c.keys[slot] = k
	c.cached[slot] = value
	bloom_builder_add_value_base(bb, c.base, value)
}

// encode_block serializes rows into a columnar block. All rows must belong to
// the same stream. The returned timestamp bounds are also written in the header.
// One pass over each row fills every column slot and indexes the bloom filter:
// a field name is resolved once (not twice) and its hash prefix reused.
encode_block :: proc(rows: []Row, out: ^common.Buf, bloom: ^Bloom_Builder, prof: ^Phase_Stats = nil) -> (min_ts, max_ts: i64, ok: bool) {
	if len(rows) == 0 {
		return 0, 0, false
	}
	t := prof_stage_start(prof != nil)
	min_ts = rows[0].ts
	max_ts = rows[0].ts
	for r in rows {
		if r.ts < min_ts {
			min_ts = r.ts
		}
		if r.ts > max_ts {
			max_ts = r.ts
		}
	}
	nrows := len(rows)
	sc: Enc_Schema
	defer {
		for &c in sc.cols {
			delete(c.vals)
		}
		delete(sc.cols)
	}
	// _msg has always been the first column, so resolve it once and write it
	// without a lookup in the hot loop.
	reserve(&sc.cols, 8)
	msg_ci := enc_schema_col(&sc, "_msg", nrows)

	t = prof_stage_start(prof != nil)
	for ri in 0 ..< nrows {
		r := &rows[ri]
		c := &sc.cols[msg_ci]
		c.vals[ri] = r.msg
		enc_bloom_value(bloom, c, r.msg)
		for f in r.fields {
			ci := enc_schema_col(&sc, f.name, nrows)
			c = &sc.cols[ci]
			c.vals[ri] = f.value
			enc_bloom_value(bloom, c, f.value)
		}
	}
	if prof != nil {
		prof.collect_ns += prof_stage_end(true, t)
	}

	t = prof_stage_start(prof != nil)
	common.buf_reset(out)
	common.buf_string(out, BLOCK_MAGIC)
	common.buf_u16le(out, BLOCK_VERSION)
	common.buf_u32le(out, u32(nrows))
	common.buf_u32le(out, u32(len(sc.cols)))
	common.buf_u64le(out, rows[0].stream)
	common.buf_i64le(out, min_ts)
	common.buf_i64le(out, max_ts)
	for r in rows {
		common.buf_i64le(out, r.ts)
	}
	for &c in sc.cols {
		common.buf_u16le(out, u16(len(c.name)))
		common.buf_string(out, c.name)
		is_const := true
		for v in c.vals {
			if v != c.vals[0] {
				is_const = false
				break
			}
		}
		if is_const {
			common.buf_byte(out, 0)
			common.buf_u32le(out, u32(len(c.vals[0])))
			common.buf_string(out, c.vals[0])
		} else {
			common.buf_byte(out, 1)
			for v in c.vals {
				common.buf_u32le(out, u32(len(v)))
			}
			for v in c.vals {
				common.buf_string(out, v)
			}
		}
	}
	if prof != nil {
		prof.serialize_ns += prof_stage_end(true, t)
	}
	return min_ts, max_ts, true
}

// ---------------------------------------------------------------- decoding

Dec_Col :: struct {
	name:     string,
	is_const: bool,
	const_v:  string,
	lens_off: int,
	data_off: int,
}

Decoded_Block :: struct {
	buf:     []u8,
	rows:    []Row,
	fields:  []Field,
	stream:  u64,
	min_ts:  i64,
	max_ts:  i64,
	nrows:   int,
}

decode_block :: proc(raw: []u8) -> (d: Decoded_Block, ok: bool) {
	if len(raw) < 38 || string(raw[0:4]) != BLOCK_MAGIC {
		return {}, false
	}
	at := 4
	if common.read_u16le(raw, at) != BLOCK_VERSION {
		return {}, false
	}
	at += 2
	nrows := int(common.read_u32le(raw, at))
	at += 4
	ncols := int(common.read_u32le(raw, at))
	at += 4
	if nrows <= 0 || ncols <= 0 || nrows > 1 << 24 || ncols > 4096 {
		return {}, false
	}
	stream := common.read_u64le(raw, at)
	at += 8
	min_ts := common.read_i64le(raw, at)
	at += 8
	max_ts := common.read_i64le(raw, at)
	at += 8
	if at + nrows * 8 > len(raw) {
		return {}, false
	}
	ts_off := at
	at += nrows * 8
	scratch := make([]i32, nrows * 3)
	defer delete(scratch)
	counts := scratch[0:nrows]
	starts := scratch[nrows:2*nrows]
	fill := scratch[2*nrows:3*nrows]

	cols := make([dynamic]Dec_Col, 0, ncols)
	defer delete(cols)
	for _ in 0..<ncols {
		if at + 2 > len(raw) {
			return {}, false
		}
		nlen := int(common.read_u16le(raw, at))
		at += 2
		if at + nlen + 1 > len(raw) {
			return {}, false
		}
		c := Dec_Col{name = string(raw[at:at+nlen])}
		at += nlen
		kind := raw[at]
		at += 1
		if kind == 0 {
			if at + 4 > len(raw) {
				return {}, false
			}
			vlen := int(common.read_u32le(raw, at))
			at += 4
			if at + vlen > len(raw) {
				return {}, false
			}
			c.is_const = true
			c.const_v = string(raw[at:at+vlen])
			at += vlen
			if len(c.const_v) > 0 && c.name != "_msg" {
				for i in 0..<nrows {
					counts[i] += 1
				}
			}
		} else {
			c.lens_off = at
			at += nrows * 4
			c.data_off = at
			if at > len(raw) {
				return {}, false
			}
			// Sum the length table here, validating the data range and counting
			// non-empty non-message fields in the same pass.
			total := 0
			if c.name == "_msg" {
				for i in 0..<nrows {
					total += int(common.read_u32le(raw, c.lens_off + i * 4))
				}
			} else {
				for i in 0..<nrows {
					vlen := int(common.read_u32le(raw, c.lens_off + i * 4))
					total += vlen
					if vlen > 0 {
						counts[i] += 1
					}
				}
			}
			if at + total > len(raw) {
				return {}, false
			}
			at += total
		}
		append(&cols, c)
	}

	rows := make([]Row, nrows)
	total_fields := 0
	for i in 0..<nrows {
		starts[i] = i32(total_fields)
		total_fields += int(counts[i])
		rows[i].ts = common.read_i64le(raw, ts_off + i * 8)
		rows[i].stream = stream
	}
	fields := make([]Field, total_fields)
	// pass 2: place values into each row's field slice
	for &c in cols {
		if c.is_const {
			v := c.const_v
			if len(v) == 0 {
				continue
			}
			if c.name == "_msg" {
				for i in 0..<nrows {
					rows[i].msg = v
				}
				continue
			}
			for i in 0..<nrows {
				pos := int(starts[i] + fill[i])
				fields[pos] = Field{name = c.name, value = v}
				fill[i] += 1
			}
			continue
		}
		offset := c.data_off
		if c.name == "_msg" {
			for i in 0..<nrows {
				vlen := int(common.read_u32le(raw, c.lens_off + i * 4))
				if vlen > 0 {
					rows[i].msg = string(raw[offset:offset+vlen])
				}
				offset += vlen
			}
			continue
		}
		for i in 0..<nrows {
			vlen := int(common.read_u32le(raw, c.lens_off + i * 4))
			if vlen > 0 {
				pos := int(starts[i] + fill[i])
				fields[pos] = Field{name = c.name, value = string(raw[offset:offset+vlen])}
				fill[i] += 1
			}
			offset += vlen
		}
	}
	for i in 0..<nrows {
		rows[i].fields = fields[int(starts[i]):int(starts[i] + counts[i])]
	}
	return Decoded_Block{
		buf = raw,
		rows = rows,
		fields = fields,
		stream = stream,
		min_ts = min_ts,
		max_ts = max_ts,
		nrows = nrows,
	}, true
}

decoded_destroy :: proc(d: ^Decoded_Block) {
	delete(d.rows)
	delete(d.fields)
	delete(d.buf)
	d^ = {}
}

// ---------------------------------------------------------------- row helpers

row_field :: proc(r: ^Row, name: string) -> (string, bool) {
	if name == "_msg" {
		return r.msg, true
	}
	if name == "_time" {
		return "", false
	}
	for f in r.fields {
		if f.name == name {
			return f.value, true
		}
	}
	return "", false
}
