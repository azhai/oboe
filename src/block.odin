package odlog

Row :: struct {
	ts:     i64,
	stream: u64,
	msg:    string,
	fields: []Field,
}

BLOCK_MAGIC :: "ODLB"
BLOCK_VERSION :: u16(1)

Enc_Col :: struct {
	name: string,
	vals: [dynamic]string,
}

// encode_block serializes rows into a columnar block. All rows must belong to
// the same stream. The returned timestamp bounds are also written in the header.
encode_block :: proc(rows: []Row, out: ^Buf, bloom: ^Bloom_Builder) -> (min_ts, max_ts: i64, ok: bool) {
	if len(rows) == 0 {
		return 0, 0, false
	}
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
	cols: [dynamic]Enc_Col
	defer {
		for &c in cols {
			delete(c.vals)
		}
		delete(cols)
	}
	for ri in 0..<len(rows) {
		r := &rows[ri]
		// message column
		ci := find_enc_col(&cols, "_msg", ri)
		if len(cols[ci].vals) == ri {
			append(&cols[ci].vals, r.msg)
		} else {
			cols[ci].vals[ri] = r.msg
		}
		for f in r.fields {
			ci = find_enc_col(&cols, f.name, ri)
			if len(cols[ci].vals) == ri {
				append(&cols[ci].vals, f.value)
			} else {
				cols[ci].vals[ri] = f.value
			}
		}
		for &c in cols {
			for len(c.vals) < len(rows) {
				append(&c.vals, "")
			}
		}
		bloom_builder_add_value(bloom, "_msg", r.msg)
		for f in r.fields {
			bloom_builder_add_value(bloom, f.name, f.value)
		}
	}
	buf_reset(out)
	buf_string(out, BLOCK_MAGIC)
	buf_u16le(out, BLOCK_VERSION)
	buf_u32le(out, u32(len(rows)))
	buf_u32le(out, u32(len(cols)))
	buf_u64le(out, rows[0].stream)
	buf_i64le(out, min_ts)
	buf_i64le(out, max_ts)
	for r in rows {
		buf_i64le(out, r.ts)
	}
	for &c in cols {
		buf_u16le(out, u16(len(c.name)))
		buf_string(out, c.name)
		is_const := true
		for v in c.vals {
			if v != c.vals[0] {
				is_const = false
				break
			}
		}
		if is_const {
			buf_byte(out, 0)
			buf_u32le(out, u32(len(c.vals[0])))
			buf_string(out, c.vals[0])
		} else {
			buf_byte(out, 1)
			for v in c.vals {
				buf_u32le(out, u32(len(v)))
			}
			for v in c.vals {
				buf_string(out, v)
			}
		}
	}
	return min_ts, max_ts, true
}

find_enc_col :: proc(cols: ^[dynamic]Enc_Col, name: string, ri: int) -> int {
	for i in 0..<len(cols) {
		if cols[i].name == name {
			return i
		}
	}
	c := Enc_Col{name = name}
	c.vals = make([dynamic]string, 0, ri + 1)
	for _ in 0..<ri {
		append(&c.vals, "")
	}
	append(cols, c)
	return len(cols) - 1
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
	if len(raw) < 4 || string(raw[0:4]) != BLOCK_MAGIC {
		return {}, false
	}
	at := 4
	if read_u16le(raw, at) != BLOCK_VERSION {
		return {}, false
	}
	at += 2
	nrows := int(read_u32le(raw, at))
	at += 4
	ncols := int(read_u32le(raw, at))
	at += 4
	if nrows <= 0 || ncols <= 0 || nrows > 1 << 24 || ncols > 4096 {
		return {}, false
	}
	stream := read_u64le(raw, at)
	at += 8
	min_ts := read_i64le(raw, at)
	at += 8
	max_ts := read_i64le(raw, at)
	at += 8
	if at + nrows * 8 > len(raw) {
		return {}, false
	}
	ts_off := at
	at += nrows * 8
	cols := make([dynamic]Dec_Col, 0, ncols)
	defer delete(cols)
	for _ in 0..<ncols {
		if at + 2 > len(raw) {
			return {}, false
		}
		nlen := int(read_u16le(raw, at))
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
			vlen := int(read_u32le(raw, at))
			at += 4
			if at + vlen > len(raw) {
				return {}, false
			}
			c.is_const = true
			c.const_v = string(raw[at:at+vlen])
			at += vlen
		} else {
			c.lens_off = at
			at += nrows * 4
			c.data_off = at
			if at > len(raw) {
				return {}, false
			}
			total := 0
			for i in 0..<nrows {
				total += int(read_u32le(raw, c.lens_off + i * 4))
			}
			if at + total > len(raw) {
				return {}, false
			}
			at += total
		}
		append(&cols, c)
	}

	// pass 1: count non-empty fields per row (excluding _msg)
	counts := make([]i32, nrows)
	defer delete(counts)
	for i in 0..<nrows {
		for c in cols {
			vlen: int
			if c.is_const {
				vlen = len(c.const_v)
			} else {
				vlen = int(read_u32le(raw, c.lens_off + i * 4))
			}
			if vlen > 0 && c.name != "_msg" {
				counts[i] += 1
			}
		}
	}
	starts := make([]i32, nrows)
	defer delete(starts)
	total_fields := 0
	for i in 0..<nrows {
		starts[i] = i32(total_fields)
		total_fields += int(counts[i])
	}
	rows := make([]Row, nrows)
	fields := make([]Field, total_fields)
	fill := make([]i32, nrows)
	defer delete(fill)
	for i in 0..<nrows {
		rows[i].ts = read_i64le(raw, ts_off + i * 8)
		rows[i].stream = stream
	}
	// pass 2
	for c in cols {
		offset := c.data_off
		for i in 0..<nrows {
			vlen: int
			v: string
			if c.is_const {
				vlen = len(c.const_v)
				v = c.const_v
			} else {
				vlen = int(read_u32le(raw, c.lens_off + i * 4))
				v = string(raw[offset:offset+vlen])
				offset += vlen
			}
			if vlen == 0 {
				continue
			}
			if c.name == "_msg" {
				rows[i].msg = v
				continue
			}
			pos := int(starts[i] + fill[i])
			fields[pos] = Field{name = c.name, value = v}
			fill[i] += 1
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
