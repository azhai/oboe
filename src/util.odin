package odlog

import "core:fmt"
import "core:mem"
import "core:strconv"
import "core:strings"
import "core:time"

DAY_NS :: i64(86_400_000_000_000)

// ---------------------------------------------------------------- bytes buffer

Buf :: struct {
	data: [dynamic]u8,
}

buf_init :: proc(cap: int = 1024) -> Buf {
	return Buf{data = make([dynamic]u8, 0, cap)}
}

buf_destroy :: proc(b: ^Buf) {
	delete(b.data)
	b.data = nil
}

buf_reset :: proc(b: ^Buf) {
	clear(&b.data)
}

buf_write :: proc(b: ^Buf, s: []u8) {
	append(&b.data, ..s)
}

buf_string :: proc(b: ^Buf, s: string) {
	append(&b.data, ..transmute([]u8)s)
}

buf_byte :: proc(b: ^Buf, v: u8) {
	append(&b.data, v)
}

arena_aprintf :: proc(a: ^Arena, format: string, args: ..any) -> string {
	s := fmt.aprintf(format, ..args)
	out := arena_str(a, s)
	delete(s)
	return out
}

buf_put_digits :: proc(b: ^Buf, v: i64, width: int) {
	buf: [24]u8
	n := 0
	x := v
	if x < 0 {
		buf_byte(b, '-')
		x = -x
	}
	if x == 0 {
		buf[n] = '0'
		n = 1
	}
	for x > 0 {
		buf[n] = u8('0' + x % 10)
		n += 1
		x /= 10
	}
	for n < width {
		buf[n] = '0'
		n += 1
	}
	for i := n - 1; i >= 0; i -= 1 {
		buf_byte(b, buf[i])
	}
}

// buf_time_rfc3339 formats nanoseconds since the epoch as RFC3339 UTC without
// allocating.
buf_time_rfc3339 :: proc(b: ^Buf, ts: i64) {
	sec := floor_div(ts, 1_000_000_000)
	nsec := ts - sec * 1_000_000_000
	day := floor_div(sec, 86_400)
	sod := sec - day * 86_400
	y, m, d := civil_from_days(day)
	buf_put_digits(b, y, 4)
	buf_byte(b, '-')
	buf_put_digits(b, m, 2)
	buf_byte(b, '-')
	buf_put_digits(b, d, 2)
	buf_byte(b, 'T')
	buf_put_digits(b, sod / 3600, 2)
	buf_byte(b, ':')
	buf_put_digits(b, (sod % 3600) / 60, 2)
	buf_byte(b, ':')
	buf_put_digits(b, sod % 60, 2)
	if nsec != 0 {
		frac: [9]u8
		x := nsec
		for i in 0..<9 {
			frac[8-i] = u8('0' + x % 10)
			x /= 10
		}
		n := 9
		for n > 0 && frac[n-1] == '0' {
			n -= 1
		}
		buf_byte(b, '.')
		buf_write(b, frac[:n])
	}
	buf_byte(b, 'Z')
}

buf_addf :: proc(b: ^Buf, format: string, args: ..any) {
	sb := strings.builder_make(0, 256)
	fmt.sbprintf(&sb, format, ..args)
	buf_write(b, transmute([]u8)strings.to_string(sb))
	strings.builder_destroy(&sb)
}

buf_u16le :: proc(b: ^Buf, v: u16) {
	append(&b.data, u8(v), u8(v >> 8))
}

buf_u32le :: proc(b: ^Buf, v: u32) {
	append(&b.data, u8(v), u8(v >> 8), u8(v >> 16), u8(v >> 24))
}

buf_u64le :: proc(b: ^Buf, v: u64) {
	buf_u32le(b, u32(v))
	buf_u32le(b, u32(v >> 32))
}

buf_i64le :: proc(b: ^Buf, v: i64) {
	buf_u64le(b, u64(v))
}

buf_uvarint :: proc(b: ^Buf, v: u64) {
	x := v
	for x >= 0x80 {
		append(&b.data, u8(x) | 0x80)
		x >>= 7
	}
	append(&b.data, u8(x))
}

buf_put_u32_at :: proc(b: ^Buf, at: int, v: u32) {
	b.data[at+0] = u8(v)
	b.data[at+1] = u8(v >> 8)
	b.data[at+2] = u8(v >> 16)
	b.data[at+3] = u8(v >> 24)
}

read_u16le :: proc(p: []u8, at: int) -> u16 {
	return u16(p[at]) | u16(p[at+1]) << 8
}

read_u32le :: proc(p: []u8, at: int) -> u32 {
	return u32(p[at]) | u32(p[at+1]) << 8 | u32(p[at+2]) << 16 | u32(p[at+3]) << 24
}

read_u64le :: proc(p: []u8, at: int) -> u64 {
	return u64(read_u32le(p, at)) | u64(read_u32le(p, at+4)) << 32
}

read_i64le :: proc(p: []u8, at: int) -> i64 {
	return i64(read_u64le(p, at))
}

// ---------------------------------------------------------------- arena

Arena :: struct {
	chunks:    [dynamic][]u8,
	chunk_idx: int,
	used:      int,
}

arena_alloc :: proc(a: ^Arena, n: int, align := 8) -> []u8 {
	off := (a.used + align - 1) & ~(align - 1)
	if a.chunk_idx >= len(a.chunks) {
		size := max(n + align, 1 << 20)
		chunk := make([]u8, size)
		append(&a.chunks, chunk)
	}
	chunk := a.chunks[a.chunk_idx]
	if off + n > len(chunk) {
		a.chunk_idx += 1
		a.used = 0
		if a.chunk_idx >= len(a.chunks) {
			size := max(n + align, 1 << 20)
			append(&a.chunks, make([]u8, size))
		}
		return arena_alloc(a, n, align)
	}
	out := chunk[off:off+n]
	a.used = off + n
	return out
}

arena_str :: proc(a: ^Arena, s: string) -> string {
	if len(s) == 0 {
		return ""
	}
	dst := arena_alloc(a, len(s), 1)
	copy(dst, transmute([]u8)s)
	return string(dst)
}

arena_reset :: proc(a: ^Arena) {
	a.chunk_idx = 0
	a.used = 0
}

arena_destroy :: proc(a: ^Arena) {
	for c in a.chunks {
		delete(c)
	}
	delete(a.chunks)
	a.chunks = nil
	a.chunk_idx = 0
	a.used = 0
}

// ---------------------------------------------------------------- hashing

FNV_BASIS :: u64(14695981039346656037)
FNV_PRIME :: u64(1099511628211)

fnv_continue :: proc(h: u64, s: string, fold := false) -> u64 {
	x := h
	for i := 0; i < len(s); i += 1 {
		b := s[i]
		if fold && b >= 'A' && b <= 'Z' {
			b += 32
		}
		x = (x ~ u64(b)) * FNV_PRIME
	}
	return x
}

fnv :: proc(seed: u64, s: string, fold := false) -> u64 {
	return fnv_continue(seed, s, fold)
}

mix64 :: #force_inline proc(x0: u64) -> u64 {
	x := x0
	x ~= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ~= x >> 27
	x *= 0x94d049bb133111eb
	return x ~ (x >> 31)
}

// ---------------------------------------------------------------- time

now_ns :: proc() -> i64 {
	return time.to_unix_nanoseconds(time.now())
}

floor_div :: proc(a, b: i64) -> i64 {
	q := a / b
	if (a % b != 0) && ((a < 0) != (b < 0)) {
		q -= 1
	}
	return q
}

utc_day :: proc(ts_ns: i64) -> i64 {
	return floor_div(ts_ns, DAY_NS)
}

days_from_civil :: proc(y0: i64, m: i64, d: i64) -> i64 {
	y := y0
	if m <= 2 {
		y -= 1
	}
	era := floor_div(y, 400)
	yoe := y - era * 400
	mp := m + (m > 2 ? -3 : 9)
	doy := (153 * mp + 2) / 5 + d - 1
	doe := yoe * 365 + yoe / 4 - yoe / 100 + doy
	return era * 146097 + doe - 719468
}

civil_from_days :: proc(z0: i64) -> (y: i64, m: i64, d: i64) {
	z := z0 + 719468
	era := floor_div(z, 146097)
	doe := z - era * 146097
	yoe := (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365
	yy := yoe + era * 400
	doy := doe - (365 * yoe + yoe / 4 - yoe / 100)
	mp := (5 * doy + 2) / 153
	d = doy - (153 * mp + 2) / 5 + 1
	m = mp + (mp < 10 ? 3 : -9)
	y = yy + (m <= 2 ? 1 : 0)
	return
}

format_day :: proc(day: i64) -> string {
	y, m, d := civil_from_days(day)
	buf: [16]u8
	return strings.clone(fmt.bprintf(buf[:], "%04d%02d%02d", y, m, d))
}

parse_digits :: proc(s: string, i: ^int, n: int, out: ^i64) -> bool {
	x: i64
	for _ in 0..<n {
		if i^ >= len(s) {
			return false
		}
		c := s[i^]
		if c < '0' || c > '9' {
			return false
		}
		x = x * 10 + i64(c - '0')
		i^ += 1
	}
	out^ = x
	return true
}

// parse_iso_ns parses RFC3339-like timestamps with full nanosecond precision.
parse_iso_ns :: proc(s: string) -> (i64, bool) {
	if len(s) < 10 {
		return 0, false
	}
	i := 0
	y, m, d, hh, mm, ss: i64
	if !parse_digits(s, &i, 4, &y) {
		return 0, false
	}
	if i >= len(s) || s[i] != '-' {
		return 0, false
	}
	i += 1
	if !parse_digits(s, &i, 2, &m) {
		return 0, false
	}
	if i >= len(s) || s[i] != '-' {
		return 0, false
	}
	i += 1
	if !parse_digits(s, &i, 2, &d) {
		return 0, false
	}
	if m < 1 || m > 12 || d < 1 || d > 31 {
		return 0, false
	}
	if i < len(s) {
		if s[i] == 'T' || s[i] == 't' || s[i] == ' ' {
			i += 1
			if !parse_digits(s, &i, 2, &hh) {
				return 0, false
			}
			if i >= len(s) || s[i] != ':' {
				return 0, false
			}
			i += 1
			if !parse_digits(s, &i, 2, &mm) {
				return 0, false
			}
			if i < len(s) && s[i] == ':' {
				i += 1
				if !parse_digits(s, &i, 2, &ss) {
					return 0, false
				}
			}
		}
	}
	frac: i64
	frac_digits := 0
	if i < len(s) && (s[i] == '.' || s[i] == ',') {
		i += 1
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			if frac_digits < 9 {
				frac = frac * 10 + i64(s[i] - '0')
				frac_digits += 1
			}
			i += 1
		}
		for frac_digits < 9 {
			frac *= 10
			frac_digits += 1
		}
	}
	offset: i64
	if i < len(s) {
		c := s[i]
		if c == 'Z' || c == 'z' {
			i += 1
		} else if c == '+' || c == '-' {
			sign: i64 = c == '-' ? -1 : 1
			i += 1
			oh, om: i64
			if !parse_digits(s, &i, 2, &oh) {
				return 0, false
			}
			if i < len(s) && s[i] == ':' {
				i += 1
			}
			if i < len(s) {
				if !parse_digits(s, &i, 2, &om) {
					return 0, false
				}
			}
			offset = sign * (oh * 3600 + om * 60)
		}
	}
	if hh > 23 || mm > 59 || ss > 60 {
		return 0, false
	}
	days := days_from_civil(y, m, d)
	secs := days * 86400 + hh * 3600 + mm * 60 + ss - offset
	return secs * 1_000_000_000 + frac, true
}

parse_duration_ns :: proc(s: string) -> (i64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	i := 0
	sign: i64 = 1
	if s[0] == '+' {
		i += 1
	} else if s[0] == '-' {
		sign = -1
		i += 1
	}
	start := i
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i += 1
	}
	if i == start {
		return 0, false
	}
	n, ok := strconv.parse_int(s[start:i], 10)
	if !ok {
		return 0, false
	}
	unit := s[i:]
	mult: i64
	switch unit {
	case "", "ns":
		mult = 1
	case "us", "µs":
		mult = 1_000
	case "ms":
		mult = 1_000_000
	case "s":
		mult = 1_000_000_000
	case "m":
		mult = 60 * 1_000_000_000
	case "h":
		mult = 3600 * 1_000_000_000
	case "d":
		mult = 86400 * 1_000_000_000
	case "w":
		mult = 7 * 86400 * 1_000_000_000
	case:
		return 0, false
	}
	return sign * i64(n) * mult, true
}

// parse_time_ns accepts RFC3339, unix timestamps, "now", "now-1h" and "-1h".
parse_time_ns :: proc(s: string, now: i64) -> (i64, bool) {
	v := strings.trim_space(s)
	if len(v) == 0 {
		return 0, false
	}
	if v == "now" {
		return now, true
	}
	if strings.has_prefix(v, "now-") || strings.has_prefix(v, "now+") {
		d, ok := parse_duration_ns(v[3:])
		if !ok {
			return 0, false
		}
		return now + d, true
	}
	if v[0] == '-' || v[0] == '+' {
		if d, ok := parse_duration_ns(v); ok {
			return now + d, true
		}
	}
	all_digits := true
	for i := 0; i < len(v); i += 1 {
		if v[i] < '0' || v[i] > '9' {
			all_digits = false
			break
		}
	}
	if all_digits {
		n, ok := strconv.parse_int(v, 10)
		if !ok {
			return 0, false
		}
		switch len(v) {
		case 1..=10:
			return i64(n) * 1_000_000_000, true
		case 11..=13:
			return i64(n) * 1_000_000, true
		case 14..=16:
			return i64(n) * 1_000, true
		case:
			return i64(n), true
		}
	}
	return parse_iso_ns(v)
}

format_time_ns :: proc(ts: i64) -> string {
	sec := floor_div(ts, 1_000_000_000)
	nsec := ts - sec * 1_000_000_000
	t := time.unix(sec, nsec)
	s, _ := time.time_to_rfc3339(t, 0, true)
	return s
}

// ---------------------------------------------------------------- strings

is_word_byte :: #force_inline proc(b: u8) -> bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b >= 0x80
}

Token_Iter :: struct {
	s: string,
	i: int,
}

next_token :: proc(it: ^Token_Iter) -> (string, bool) {
	for it.i < len(it.s) && !is_word_byte(it.s[it.i]) {
		it.i += 1
	}
	if it.i >= len(it.s) {
		return "", false
	}
	start := it.i
	for it.i < len(it.s) && is_word_byte(it.s[it.i]) {
		it.i += 1
	}
	return it.s[start:it.i], true
}

ascii_eq_ci :: #force_inline proc(a, b: u8) -> bool {
	aa := a
	bb := b
	if aa >= 'A' && aa <= 'Z' {
		aa += 32
	}
	if bb >= 'A' && bb <= 'Z' {
		bb += 32
	}
	return aa == bb
}

token_eq_ci :: proc(a, b: string) -> bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i += 1 {
		if !ascii_eq_ci(a[i], b[i]) {
			return false
		}
	}
	return true
}

contains_ci :: proc(haystack, needle: string) -> bool {
	if len(needle) == 0 {
		return true
	}
	if len(needle) > len(haystack) {
		return false
	}
	n := len(needle)
	for i := 0; i+n <= len(haystack); i += 1 {
		match := true
		for j := 0; j < n; j += 1 {
			if !ascii_eq_ci(haystack[i+j], needle[j]) {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

starts_with_ci :: proc(s, prefix: string) -> bool {
	if len(prefix) > len(s) {
		return false
	}
	for i := 0; i < len(prefix); i += 1 {
		if !ascii_eq_ci(s[i], prefix[i]) {
			return false
		}
	}
	return true
}

has_token_ci :: proc(value, token: string) -> bool {
	if len(token) == 0 {
		return true
	}
	it := Token_Iter{s = value}
	for tok in next_token(&it) {
		if token_eq_ci(tok, token) {
			return true
		}
	}
	return false
}

json_escape_into :: proc(b: ^Buf, s: string) {
	hex := "0123456789abcdef"
	buf_byte(b, '"')
	for i := 0; i < len(s); i += 1 {
		c := s[i]
		switch c {
		case '"':
			buf_write(b, transmute([]u8)string("\\\""))
		case '\\':
			buf_write(b, transmute([]u8)string("\\\\"))
		case '\n':
			buf_write(b, transmute([]u8)string("\\n"))
		case '\r':
			buf_write(b, transmute([]u8)string("\\r"))
		case '\t':
			buf_write(b, transmute([]u8)string("\\t"))
		case:
			if c < 0x20 {
				buf_write(b, transmute([]u8)string("\\u00"))
				buf_byte(b, hex[c >> 4])
				buf_byte(b, hex[c & 0xf])
			} else {
				buf_byte(b, c)
			}
		}
	}
	buf_byte(b, '"')
}

percent_decode :: proc(s: string) -> string {
	if !strings.contains(s, "%") && !strings.contains(s, "+") {
		return s
	}
	out := make([dynamic]u8, 0, len(s))
	i := 0
	for i < len(s) {
		c := s[i]
		if c == '+' {
			append(&out, u8(' '))
			i += 1
		} else if c == '%' && i + 2 < len(s) {
			hi := unhex(s[i+1])
			lo := unhex(s[i+2])
			if hi >= 0 && lo >= 0 {
				append(&out, u8(hi << 4 | lo))
				i += 3
			} else {
				append(&out, c)
				i += 1
			}
		} else {
			append(&out, c)
			i += 1
		}
	}
	return string(out[:])
}

unhex :: proc(c: u8) -> int {
	switch c {
	case '0'..='9':
		return int(c - '0')
	case 'a'..='f':
		return int(c - 'a' + 10)
	case 'A'..='F':
		return int(c - 'A' + 10)
	}
	return -1
}

// ---------------------------------------------------------------- misc

clamp_i64 :: proc(v, lo, hi: i64) -> i64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

mem_zero :: proc(p: rawptr, n: int) {
	mem.set(p, 0, n)
}

parse_size_bytes :: proc(s: string) -> (i64, bool) {
	str := strings.trim_space(s)
	if len(str) == 0 {
		return 0, false
	}
	lower, _ := strings.to_lower(str, context.temp_allocator)
	num_end := len(lower)
	for num_end > 0 && (lower[num_end-1] < '0' || lower[num_end-1] > '9') {
		num_end -= 1
	}
	if num_end == 0 {
		return 0, false
	}
	n, ok := strconv.parse_i64(lower[:num_end], 10)
	if !ok || n < 0 {
		return 0, false
	}
	mult: i64
	switch lower[num_end:] {
	case "", "b":
		mult = 1
	case "k", "kb":
		mult = 1000
	case "ki", "kib":
		mult = 1024
	case "m", "mb":
		mult = 1_000_000
	case "mi", "mib":
		mult = 1 << 20
	case "g", "gb":
		mult = 1_000_000_000
	case "gi", "gib":
		mult = 1 << 30
	case:
		return 0, false
	}
	if n > (i64(0x7fffffffffffffff) - 1) / mult {
		return 0, false
	}
	return n * mult, true
}
