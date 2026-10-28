package oboe

import "core:testing"

Size_Case :: struct {
	input: string,
	want: i64,
	ok:   bool,
}

@(test)
test_parse_size_bytes :: proc(t: ^testing.T) {
	cases := []Size_Case{
		{"", 0, false},
		{"0", 0, true},
		{"512", 512, true},
		{"1KB", 1000, true},
		{" 2KiB ", 2048, true},
		{"3MiB", 3 << 20, true},
		{"4GB", 4_000_000_000, true},
		{"5gib", 5 << 30, true},
		{"1.5GB", 0, false},
		{"-1", 0, false},
		{"12XB", 0, false},
		{"MB", 0, false},
	}
	for c in cases {
		got, ok := parse_size_bytes(c.input)
		testing.expectf(t, ok == c.ok, "parse_size_bytes(%q) ok=%v, want %v", c.input, ok, c.ok)
		if ok && c.ok {
			testing.expectf(t, got == c.want, "parse_size_bytes(%q)=%d, want %d", c.input, got, c.want)
		}
	}
}

Duration_Case :: struct {
	input: string,
	want: i64,
	ok:   bool,
}

@(test)
test_parse_duration_ns :: proc(t: ^testing.T) {
	cases := []Duration_Case{
		{"10", 10, true},
		{"10ns", 10, true},
		{"10us", 10_000, true},
		{"10µs", 10_000, true},
		{"10ms", 10_000_000, true},
		{"2s", 2_000_000_000, true},
		{"3m", 180_000_000_000, true},
		{"1h", 3_600_000_000_000, true},
		{"1d", 86_400_000_000_000, true},
		{"1w", 604_800_000_000_000, true},
		{"-1h", -3_600_000_000_000, true},
		{"+5s", 5_000_000_000, true},
		{"", 0, false},
		{"abc", 0, false},
		{"s", 0, false},
		{"10x", 0, false},
	}
	for c in cases {
		got, ok := parse_duration_ns(c.input)
		testing.expectf(t, ok == c.ok, "parse_duration_ns(%q) ok=%v, want %v", c.input, ok, c.ok)
		if ok && c.ok {
			testing.expectf(t, got == c.want, "parse_duration_ns(%q)=%d, want %d", c.input, got, c.want)
		}
	}
}

@(test)
test_parse_time_ns :: proc(t: ^testing.T) {
	now := i64(1_700_000_000_000_000_000)
	sec := i64(1_000_000_000)

	{
		got, ok := parse_time_ns("now", now)
		testing.expect(t, ok)
		testing.expect_value(t, got, now)
	}
	{
		got, ok := parse_time_ns("now-1h", now)
		testing.expect(t, ok)
		testing.expect_value(t, got, now-3600*sec)
	}
	{
		got, ok := parse_time_ns("-30m", now)
		testing.expect(t, ok)
		testing.expect_value(t, got, now-1800*sec)
	}
	{
		got, ok := parse_time_ns("1700000000", 0)
		testing.expect(t, ok)
		testing.expect_value(t, got, i64(1_700_000_000_000_000_000))
	}
	{
		got, ok := parse_time_ns("1700000000123", 0)
		testing.expect(t, ok)
		testing.expect_value(t, got, i64(1_700_000_000_123_000_000))
	}
	{
		got, ok := parse_time_ns("1700000000123456", 0)
		testing.expect(t, ok)
		testing.expect_value(t, got, i64(1_700_000_000_123_456_000))
	}
	{
		got, ok := parse_time_ns("1700000000123456789", 0)
		testing.expect(t, ok)
		testing.expect_value(t, got, i64(1_700_000_000_123_456_789))
	}
	{
		utc, _ := parse_iso_ns("2026-10-26T02:00:00Z")
		got, ok := parse_time_ns("2026-10-26T10:00:00+08:00", 0)
		testing.expect(t, ok)
		testing.expect_value(t, got, utc)
	}
	{
		date, _ := parse_iso_ns("2026-10-26T00:00:00Z")
		got, ok := parse_time_ns("2026-10-26", 0)
		testing.expect(t, ok)
		testing.expect_value(t, got, date)
	}
	bad_times := []string{"", "not-a-time", "2026-13-01T00:00:00Z", "2026-10-32T00:00:00Z", "2026-10-26T25:00:00Z"}
	for bad in bad_times {
		_, ok := parse_time_ns(bad, now)
		testing.expectf(t, !ok, "parse_time_ns(%q) should fail", bad)
	}
}

@(test)
test_time_format_and_calendar :: proc(t: ^testing.T) {
	testing.expect_value(t, days_from_civil(1970, 1, 1), i64(0))
	y, m, d := civil_from_days(0)
	testing.expect_value(t, y, i64(1970))
	testing.expect_value(t, m, i64(1))
	testing.expect_value(t, d, i64(1))
	testing.expect_value(t, floor_div(-7, 2), i64(-4))
	testing.expect_value(t, floor_div(7, 2), i64(3))
	testing.expect_value(t, utc_day(0), i64(0))
	testing.expect_value(t, utc_day(-1), i64(-1))
	testing.expect_value(t, utc_day(DAY_NS-1), i64(0))

	timestamps := []string{
		"2026-10-26T10:00:00Z",
		"1969-12-31T23:59:59Z",
		"2026-02-28T23:59:59.123456789Z",
	}
	for s in timestamps {
		ts, ok := parse_iso_ns(s)
		if !ok {
			testing.expectf(t, false, "parse_iso_ns(%q) failed", s)
			continue
		}
		got := format_time_ns(ts)
		testing.expectf(t, got == s, "format_time_ns(%q)=%q", s, got)
		delete(got)
	}

	b := buf_init(64)
	defer buf_destroy(&b)
	ts, _ := parse_iso_ns("2026-10-26T10:00:00.12Z")
	buf_time_rfc3339(&b, ts)
	testing.expect_value(t, string(b.data[:]), "2026-10-26T10:00:00.12Z")

	buf_reset(&b)
	buf_time_rfc3339(&b, 0)
	testing.expect_value(t, string(b.data[:]), "1970-01-01T00:00:00Z")

	buf_reset(&b)
	buf_time_rfc3339(&b, -1_000_000_000)
	testing.expect_value(t, string(b.data[:]), "1969-12-31T23:59:59Z")
}

@(test)
test_string_helpers :: proc(t: ^testing.T) {
	testing.expect_value(t, token_eq_ci("Error", "eRRor"), true)
	testing.expect_value(t, token_eq_ci("Error", "error!"), false)
	testing.expect_value(t, contains_ci("Hello World", "o w"), true)
	testing.expect_value(t, contains_ci("Hello World", "HW"), false)
	testing.expect_value(t, starts_with_ci("Content-Length", "content-"), true)
	testing.expect_value(t, starts_with_ci("Content", "content-length"), false)
	testing.expect_value(t, has_token_ci("GET /api/v1/users", "api"), true)
	testing.expect_value(t, has_token_ci("GET /api/v1/users", "ap"), false)
	testing.expect_value(t, ascii_eq_ci('A', 'a'), true)
	testing.expect_value(t, ascii_eq_ci('A', 'b'), false)

	it := Token_Iter{s = "a-b_c d!"}
	tokens: [dynamic]string
	defer delete(tokens)
	for tok in next_token(&it) {
		append(&tokens, tok)
	}
	testing.expect_value(t, len(tokens), 3)
	testing.expect_value(t, tokens[0], "a")
	testing.expect_value(t, tokens[1], "b_c")
	testing.expect_value(t, tokens[2], "d")

	decoded := percent_decode("%E4%BD%A0%E5%A5%BD+a%25")
	defer delete(decoded)
	testing.expect_value(t, decoded, "你好 a%")
	invalid := percent_decode("%zz")
	defer delete(invalid)
	testing.expect_value(t, invalid, "%zz")
	testing.expect_value(t, unhex('f'), 15)
	testing.expect_value(t, unhex('F'), 15)
	testing.expect_value(t, unhex('g'), -1)
}

@(test)
test_buffer_encoding :: proc(t: ^testing.T) {
	b := buf_init(16)
	defer buf_destroy(&b)

	buf_u16le(&b, 0x1234)
	testing.expect_value(t, read_u16le(b.data[:], 0), u16(0x1234))
	buf_u32le(&b, 0x89abcdef)
	testing.expect_value(t, read_u32le(b.data[:], 2), u32(0x89abcdef))
	buf_u64le(&b, 0x0123456789abcdef)
	testing.expect_value(t, read_u64le(b.data[:], 6), u64(0x0123456789abcdef))
	buf_i64le(&b, -2)
	testing.expect_value(t, read_i64le(b.data[:], 14), i64(-2))

	buf_reset(&b)
	buf_uvarint(&b, 0)
	buf_uvarint(&b, 127)
	buf_uvarint(&b, 128)
	buf_uvarint(&b, 300)
	testing.expect_value(t, len(b.data), 6)
	testing.expect_value(t, b.data[0], u8(0))
	testing.expect_value(t, b.data[1], u8(127))
	testing.expect_value(t, b.data[2], u8(0x80))
	testing.expect_value(t, b.data[3], u8(0x01))
	testing.expect_value(t, b.data[4], u8(0xac))
	testing.expect_value(t, b.data[5], u8(0x02))

	buf_reset(&b)
	json_escape_into(&b, "a\"b\\c\n\r\t\x01é")
	testing.expect_value(t, string(b.data[:]), `"a\"b\\c\n\r\t\u0001é"`)

	buf_reset(&b)
	buf_u32le(&b, 0)
	buf_put_u32_at(&b, 0, 0x01020304)
	testing.expect_value(t, read_u32le(b.data[:], 0), u32(0x01020304))
}

@(test)
test_arena :: proc(t: ^testing.T) {
	a: Arena
	defer arena_destroy(&a)

	p1 := arena_alloc(&a, 1, 8)
	p2 := arena_alloc(&a, 1, 8)
	testing.expect(t, raw_data(p1) != raw_data(p2))
	testing.expect_value(t, uintptr(raw_data(p2)) % 8, uintptr(0))

	s := arena_str(&a, "hello arena")
	testing.expect_value(t, s, "hello arena")
	testing.expect_value(t, arena_str(&a, ""), "")

	big := arena_alloc(&a, (1 << 20) + 16, 8)
	testing.expect_value(t, len(big), (1 << 20) + 16)

	arena_reset(&a)
	after := arena_alloc(&a, 8, 8)
	testing.expect_value(t, len(after), 8)
}
