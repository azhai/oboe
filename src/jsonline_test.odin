package oboe

import "core:testing"

@(test)
test_parse_json_line_basic :: proc(t: ^testing.T) {
	arena: Arena
	defer arena_destroy(&arena)
	p: Parsed_Line
	line := `{"_time":"2026-10-26T04:00:00Z","level":"error","service":"api","_msg":"boom","n":42,"ok":true,"nested":{"a":1},"list":[1,2]}`
	ok := parse_json_line(line, &arena, "_msg", "_time", &p)
	testing.expect(t, ok)
	if !ok {
		return
	}
	testing.expect(t, p.time_set)
	testing.expect_value(t, p.time_raw, "2026-10-26T04:00:00Z")
	testing.expect(t, p.msg_set)
	testing.expect_value(t, p.msg, "boom")
	testing.expect_value(t, p.nfields, 6)

	v, vok := parsed_field(&p, "level")
	testing.expect(t, vok)
	testing.expect_value(t, v, "error")
	v, _ = parsed_field(&p, "n")
	testing.expect_value(t, v, "42")
	v, _ = parsed_field(&p, "ok")
	testing.expect_value(t, v, "true")
	v, _ = parsed_field(&p, "nested")
	testing.expect_value(t, v, `{"a":1}`)
	v, _ = parsed_field(&p, "list")
	testing.expect_value(t, v, "[1,2]")
	_, missing := parsed_field(&p, "_msg")
	testing.expect(t, !missing)
}

@(test)
test_parse_json_line_escapes_and_custom_fields :: proc(t: ^testing.T) {
	arena: Arena
	defer arena_destroy(&arena)
	p: Parsed_Line

	line := `{"_msg":"a\nb\"c\\d\/e\tA:\u0041","emoji":"\ud83d\ude00"}`
	ok := parse_json_line(line, &arena, "_msg", "_time", &p)
	testing.expect(t, ok)
	if !ok {
		return
	}
	testing.expect_value(t, p.msg, "a\nb\"c\\d/e\tA:A")
	emoji, eok := parsed_field(&p, "emoji")
	testing.expect(t, eok)
	testing.expect_value(t, emoji, "😀")

	custom := `{"ts":"2026-10-26T05:00:00Z","message":"custom msg","level":"warn"}`
	ok = parse_json_line(custom, &arena, "message", "ts", &p)
	testing.expect(t, ok)
	if ok {
		testing.expect(t, p.time_set)
		testing.expect_value(t, p.time_raw, "2026-10-26T05:00:00Z")
		testing.expect(t, p.msg_set)
		testing.expect_value(t, p.msg, "custom msg")
		testing.expect_value(t, p.nfields, 1)
	}
}

@(test)
test_parse_json_line_duplicates_and_limits :: proc(t: ^testing.T) {
	arena: Arena
	defer arena_destroy(&arena)
	p: Parsed_Line

	dup := `{"level":"a","level":"b"}`
	testing.expect(t, parse_json_line(dup, &arena, "_msg", "_time", &p))
	testing.expect_value(t, p.nfields, 1)
	v, _ := parsed_field(&p, "level")
	testing.expect_value(t, v, "b")

	b := buf_init(4096)
	defer buf_destroy(&b)
	buf_byte(&b, '{')
	for i in 0..<(MAX_JSON_FIELDS + 10) {
		if i > 0 {
			buf_byte(&b, ',')
		}
		buf_addf(&b, `"k%d":"v%d"`, i, i)
	}
	buf_byte(&b, '}')
	testing.expect(t, parse_json_line(string(b.data[:]), &arena, "_msg", "_time", &p))
	testing.expect_value(t, p.nfields, MAX_JSON_FIELDS)
	v, _ = parsed_field(&p, "k0")
	testing.expect_value(t, v, "v0")
	_, last := parsed_field(&p, "k129")
	testing.expect(t, !last)
}

@(test)
test_parse_json_line_malformed :: proc(t: ^testing.T) {
	arena: Arena
	defer arena_destroy(&arena)
	p: Parsed_Line
	bad_lines := []string{
		``,
		`level:error`,
		`[]`,
		`{"a":}`,
		`{"a":1`,
		`{"a":"unterminated}`,
		`{"a":1} extra`,
		`{"a" 1}`,
	}
	for bad in bad_lines {
		ok := parse_json_line(bad, &arena, "_msg", "_time", &p)
		testing.expectf(t, !ok, "parse_json_line(%q) should fail", bad)
	}
}

@(test)
test_compute_stream_id :: proc(t: ^testing.T) {
	fields := []Field{
		{name = "level", value = "error"},
		{name = "service", value = "api"},
		{name = "n", value = "1"},
	}
	reversed := []Field{
		{name = "n", value = "1"},
		{name = "service", value = "api"},
		{name = "level", value = "error"},
	}
	stream_fields := []string{"level", "service"}

	base := compute_stream_id(fields, stream_fields)
	testing.expect_value(t, compute_stream_id(reversed, stream_fields), base)
	testing.expect_value(t, compute_stream_id(fields, nil), u64(0))

	changed := []Field{
		{name = "level", value = "info"},
		{name = "service", value = "api"},
	}
	testing.expect(t, compute_stream_id(changed, stream_fields) != base)

	v, ok := find_field(fields, "service")
	testing.expect(t, ok)
	testing.expect_value(t, v, "api")
	_, missing := find_field(fields, "missing")
	testing.expect(t, !missing)
}
