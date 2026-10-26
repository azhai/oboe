package odlog

MAX_JSON_FIELDS :: 128

Field :: struct {
	name:  string,
	value: string,
}

Parsed_Line :: struct {
	fields:   [MAX_JSON_FIELDS]Field,
	nfields:  int,
	msg:      string,
	msg_set:  bool,
	time_raw: string,
	time_set: bool,
}

// ---------------------------------------------------------------- json parser

skip_ws :: #force_inline proc(s: string, i: ^int) {
	for i^ < len(s) {
		c := s[i^]
		if c != ' ' && c != '\t' && c != '\r' && c != '\n' {
			break
		}
		i^ += 1
	}
}

utf8_encode :: proc(out: []u8, pos: ^int, r: u32) {
	if r < 0x80 {
		out[pos^] = u8(r)
		pos^ += 1
	} else if r < 0x800 {
		out[pos^] = u8(0xC0 | (r >> 6))
		out[pos^+1] = u8(0x80 | (r & 0x3F))
		pos^ += 2
	} else if r < 0x10000 {
		out[pos^] = u8(0xE0 | (r >> 12))
		out[pos^+1] = u8(0x80 | ((r >> 6) & 0x3F))
		out[pos^+2] = u8(0x80 | (r & 0x3F))
		pos^ += 3
	} else {
		out[pos^] = u8(0xF0 | (r >> 18))
		out[pos^+1] = u8(0x80 | ((r >> 12) & 0x3F))
		out[pos^+2] = u8(0x80 | ((r >> 6) & 0x3F))
		out[pos^+3] = u8(0x80 | (r & 0x3F))
		pos^ += 4
	}
}

hex4 :: proc(s: string, at: int) -> (u32, bool) {
	if at + 4 > len(s) {
		return 0, false
	}
	v: u32
	for i in 0..<4 {
		h := unhex(s[at+i])
		if h < 0 {
			return 0, false
		}
		v = v << 4 | u32(h)
	}
	return v, true
}

// parse_json_string parses a quoted JSON string at s[i^]. Unescaped strings are
// returned as views into s; escaped ones are decoded into the arena.
parse_json_string :: proc(s: string, i: ^int, arena: ^Arena) -> (string, bool) {
	if i^ >= len(s) || s[i^] != '"' {
		return "", false
	}
	i^ += 1
	start := i^
	escaped := false
	for i^ < len(s) {
		c := s[i^]
		if c == '"' {
			if !escaped {
				out := s[start:i^]
				i^ += 1
				return out, true
			}
			break
		}
		if c == '\\' {
			escaped = true
			i^ += 2
			continue
		}
		i^ += 1
	}
	if !escaped || i^ >= len(s) {
		return "", false
	}
	// slow path: decode escapes
	out := arena_alloc(arena, len(s)-start, 1)
	op := 0
	j := start
	for j < len(s) {
		c := s[j]
		if c == '"' {
			j += 1
			i^ = j
			return string(out[:op]), true
		}
		if c != '\\' {
			out[op] = c
			op += 1
			j += 1
			continue
		}
		if j + 1 >= len(s) {
			return "", false
		}
		e := s[j+1]
		j += 2
		switch e {
		case '"':
			out[op] = '"'
			op += 1
		case '\\':
			out[op] = '\\'
			op += 1
		case '/':
			out[op] = '/'
			op += 1
		case 'b':
			out[op] = 8
			op += 1
		case 'f':
			out[op] = 12
			op += 1
		case 'n':
			out[op] = '\n'
			op += 1
		case 'r':
			out[op] = '\r'
			op += 1
		case 't':
			out[op] = '\t'
			op += 1
		case 'u':
			r, ok := hex4(s, j)
			if !ok {
				return "", false
			}
			j += 4
			if r >= 0xD800 && r <= 0xDBFF && j + 6 <= len(s) && s[j] == '\\' && s[j+1] == 'u' {
				lo, lok := hex4(s, j+2)
				if lok && lo >= 0xDC00 && lo <= 0xDFFF {
					r = 0x10000 + ((r - 0xD800) << 10) + (lo - 0xDC00)
					j += 6
				}
			}
			if op + 4 > len(out) {
				return "", false
			}
			utf8_encode(out, &op, r)
		case:
			out[op] = e
			op += 1
		}
	}
	return "", false
}

skip_json_value :: proc(s: string, i: ^int) -> bool {
	if i^ >= len(s) {
		return false
	}
	c := s[i^]
	if c == '{' || c == '[' {
		open := c
		close := c == '{' ? u8('}') : u8(']')
		depth := 0
		for i^ < len(s) {
			ch := s[i^]
			if ch == '"' {
				// skip string
				i^ += 1
				for i^ < len(s) {
					if s[i^] == '\\' {
						i^ += 2
						continue
					}
					if s[i^] == '"' {
						i^ += 1
						break
					}
					i^ += 1
				}
				continue
			}
			if ch == open {
				depth += 1
			} else if ch == close {
				depth -= 1
				if depth == 0 {
					i^ += 1
					return true
				}
			}
			i^ += 1
		}
		return false
	}
	start := i^
	for i^ < len(s) {
		ch := s[i^]
		if ch == ',' || ch == '}' || ch == ']' || ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' {
			break
		}
		i^ += 1
	}
	return i^ > start
}

add_field :: proc(p: ^Parsed_Line, name, value: string) {
	for i in 0..<p.nfields {
		if p.fields[i].name == name {
			p.fields[i].value = value
			return
		}
	}
	if p.nfields >= MAX_JSON_FIELDS {
		return
	}
	p.fields[p.nfields] = Field{name = name, value = value}
	p.nfields += 1
}

// parse_json_line parses a single flat/nested JSON object. Nested values are
// kept as raw JSON text. _time/_msg are extracted as timestamp and message.
parse_json_line :: proc(line: string, arena: ^Arena, msg_field, time_field: string, p: ^Parsed_Line) -> bool {
	p^ = {}
	i := 0
	skip_ws(line, &i)
	if i >= len(line) || line[i] != '{' {
		return false
	}
	i += 1
	for {
		skip_ws(line, &i)
		if i >= len(line) {
			return false
		}
		if line[i] == '}' {
			i += 1
			break
		}
		if line[i] != '"' {
			return false
		}
		key, kok := parse_json_string(line, &i, arena)
		if !kok {
			return false
		}
		skip_ws(line, &i)
		if i >= len(line) || line[i] != ':' {
			return false
		}
		i += 1
		skip_ws(line, &i)
		if i >= len(line) {
			return false
		}
		value: string
		c := line[i]
		if c == '"' {
			v, vok := parse_json_string(line, &i, arena)
			if !vok {
				return false
			}
			value = v
		} else if c == '{' || c == '[' {
			start := i
			if !skip_json_value(line, &i) {
				return false
			}
			value = line[start:i]
		} else {
			start := i
			for i < len(line) {
				ch := line[i]
				if ch == ',' || ch == '}' || ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' {
					break
				}
				i += 1
			}
			if i == start {
				return false
			}
			value = line[start:i]
		}
		if key == "_time" || (len(time_field) > 0 && key == time_field) {
			p.time_raw = value
			p.time_set = true
		} else if key == "_msg" || (len(msg_field) > 0 && key == msg_field) {
			p.msg = value
			p.msg_set = true
		} else {
			add_field(p, key, value)
		}
		skip_ws(line, &i)
		if i >= len(line) {
			return false
		}
		if line[i] == ',' {
			i += 1
			continue
		}
		if line[i] == '}' {
			i += 1
			break
		}
		return false
	}
	skip_ws(line, &i)
	return i == len(line)
}

// ---------------------------------------------------------------- helpers

parsed_field :: proc(p: ^Parsed_Line, name: string) -> (string, bool) {
	for i in 0..<p.nfields {
		if p.fields[i].name == name {
			return p.fields[i].value, true
		}
	}
	return "", false
}

compute_stream_id :: proc(fields: []Field, stream_fields: []string) -> u64 {
	if len(stream_fields) == 0 {
		return 0
	}
	h := FNV_BASIS
	for name in stream_fields {
		h = fnv_continue(h, name)
		h = fnv_continue(h, "=")
		for f in fields {
			if f.name == name {
				h = fnv_continue(h, f.value)
				break
			}
		}
		h = fnv_continue(h, "\x00")
	}
	return h
}

find_field :: proc(fields: []Field, name: string) -> (string, bool) {
	for f in fields {
		if f.name == name {
			return f.value, true
		}
	}
	return "", false
}
