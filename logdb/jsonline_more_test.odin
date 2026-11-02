package logdb

import "../common"
import "core:testing"

@(test)
test_jsonline_more_field_boundary :: proc(t: ^testing.T) {
	arena: common.Arena
	defer common.arena_destroy(&arena)
	p: Parsed_Line

	// Exactly MAX_JSON_FIELDS user fields must all survive.
	b := common.buf_init(1 << 16)
	defer common.buf_destroy(&b)
	common.buf_byte(&b, '{')
	for i in 0..<MAX_JSON_FIELDS {
		if i > 0 {
			common.buf_byte(&b, ',')
		}
		common.buf_addf(&b, `"k%d":"v%d"`, i, i)
	}
	common.buf_byte(&b, '}')
	testing.expect(t, parse_json_line(string(b.data[:]), &arena, "_msg", "_time", &p))
	testing.expect_value(t, p.nfields, MAX_JSON_FIELDS)
	first, fok := parsed_field(&p, "k0")
	testing.expect(t, fok)
	testing.expect_value(t, first, "v0")
	last, lok := parsed_field(&p, "k127")
	testing.expect(t, lok)
	testing.expect_value(t, last, "v127")

	// One past the limit is dropped, and the 129th/130th names never appear.
	common.buf_reset(&b)
	common.buf_byte(&b, '{')
	for i in 0..<(MAX_JSON_FIELDS + 2) {
		if i > 0 {
			common.buf_byte(&b, ',')
		}
		common.buf_addf(&b, `"k%d":"v%d"`, i, i)
	}
	common.buf_byte(&b, '}')
	testing.expect(t, parse_json_line(string(b.data[:]), &arena, "_msg", "_time", &p))
	testing.expect_value(t, p.nfields, MAX_JSON_FIELDS)
	_, over1 := parsed_field(&p, "k128")
	_, over2 := parsed_field(&p, "k129")
	testing.expect(t, !over1)
	testing.expect(t, !over2)

	// _msg/_time are extracted before add_field, so they do not consume slots.
	line := `{"_msg":"hello","_time":"2026-10-26T04:00:00Z","a":1}`
	testing.expect(t, parse_json_line(line, &arena, "_msg", "_time", &p))
	testing.expect_value(t, p.nfields, 1)
	testing.expect(t, p.msg_set)
	testing.expect_value(t, p.msg, "hello")
	testing.expect(t, p.time_set)
	testing.expect_value(t, p.time_raw, "2026-10-26T04:00:00Z")

	// Duplicate names overwrite in place instead of consuming extra slots,
	// including duplicates whose value is itself the result of an escape.
	dup := `{"a":"one","b":"two","a":"\u006f\u006e\u0065"}`
	testing.expect(t, parse_json_line(dup, &arena, "_msg", "_time", &p))
	testing.expect_value(t, p.nfields, 2)
	v, _ := parsed_field(&p, "a")
	testing.expect_value(t, v, "one")
}

@(test)
test_jsonline_more_duplicate_and_nested :: proc(t: ^testing.T) {
	arena: common.Arena
	defer common.arena_destroy(&arena)
	p: Parsed_Line

	// Duplicate _msg/_time: last assignment wins.
	dup_msg := `{"_msg":"first","_time":"1","_msg":"second","_time":"2"}`
	testing.expect(t, parse_json_line(dup_msg, &arena, "_msg", "_time", &p))
	testing.expect_value(t, p.msg, "second")
	testing.expect_value(t, p.time_raw, "2")

	// A nested value followed by a scalar duplicate keeps only the scalar.
	mixed := `{"a":{"x":1},"a":"plain"}`
	testing.expect(t, parse_json_line(mixed, &arena, "_msg", "_time", &p))
	v, _ := parsed_field(&p, "a")
	testing.expect_value(t, v, "plain")

	// Nested objects and arrays are kept as raw JSON, including commas and
	// brackets inside strings.
	line := `{"obj":{"a":{"b":2},"c":[1,[2],{"d":3}]},"str":"x,y}","arr":[[],[{"k":[1,2,3]}]],"empty":{}}`
	testing.expect(t, parse_json_line(line, &arena, "_msg", "_time", &p))
	testing.expect_value(t, p.nfields, 4)
	obj, _ := parsed_field(&p, "obj")
	testing.expect_value(t, obj, `{"a":{"b":2},"c":[1,[2],{"d":3}]}`)
	str, _ := parsed_field(&p, "str")
	testing.expect_value(t, str, "x,y}")
	arr, _ := parsed_field(&p, "arr")
	testing.expect_value(t, arr, `[[],[{"k":[1,2,3]}]]`)
	empty, _ := parsed_field(&p, "empty")
	testing.expect_value(t, empty, "{}")

	// Scalars are kept verbatim, with no type coercion.
	scalars := `{"i":-12,"f":1.5e3,"yes":true,"no":false,"nil":null,"ws":" padded "}`
	testing.expect(t, parse_json_line(scalars, &arena, "_msg", "_time", &p))
	testing.expect_value(t, p.nfields, 6)
	iv, _ := parsed_field(&p, "i")
	testing.expect_value(t, iv, "-12")
	fv, _ := parsed_field(&p, "f")
	testing.expect_value(t, fv, "1.5e3")
	nv, _ := parsed_field(&p, "nil")
	testing.expect_value(t, nv, "null")
	wv, _ := parsed_field(&p, "ws")
	testing.expect_value(t, wv, " padded ")

	// Whitespace around every token is accepted.
	pretty := "{\n\t\"a\" : 1 ,\n\t\"b\" : [ 1 , 2 ]\n}"
	testing.expect(t, parse_json_line(pretty, &arena, "_msg", "_time", &p))
	testing.expect_value(t, p.nfields, 2)
}

@(test)
test_jsonline_more_escapes_and_special :: proc(t: ^testing.T) {
	arena: common.Arena
	defer common.arena_destroy(&arena)
	p: Parsed_Line

	// Escaped keys are decoded before deduplication.
	line := `{"\u006b\u0065\u0079":1,"key":2}`
	testing.expect(t, parse_json_line(line, &arena, "_msg", "_time", &p))
	testing.expect_value(t, p.nfields, 1)
	v, _ := parsed_field(&p, "key")
	testing.expect_value(t, v, "2")

	// Every one-byte escape plus a BMP code point and a surrogate pair.
	esc := `{"_msg":"a\"b\\c\/d\be\ff\ng\rh\ti\u0041\ud83d\ude00"}`
	testing.expect(t, parse_json_line(esc, &arena, "_msg", "_time", &p))
	testing.expect(t, p.msg_set)
	testing.expect_value(t, p.msg, "a\"b\\c/d\be\ff\ng\rh\tiA😀")

	// A lone high surrogate is encoded as raw UTF-8 rather than rejected.
	lone := `{"x":"\ud83d"}`
	testing.expect(t, parse_json_line(lone, &arena, "_msg", "_time", &p))
	xv, _ := parsed_field(&p, "x")
	testing.expect_value(t, xv, "\xED\xA0\xBD")

	// Invalid hex in a \u escape rejects the whole line.
	bad_hex := `{"x":"\uZZZZ"}`
	testing.expect(t, !parse_json_line(bad_hex, &arena, "_msg", "_time", &p))
	short_hex := `{"x":"\u12"}`
	testing.expect(t, !parse_json_line(short_hex, &arena, "_msg", "_time", &p))

	// Unknown escapes are passed through (matching the permissive lexer).
	unknown := `{"x":"a\q"}`
	testing.expect(t, parse_json_line(unknown, &arena, "_msg", "_time", &p))
	uv, _ := parsed_field(&p, "x")
	testing.expect_value(t, uv, "aq")

	// A custom message/time field name is honoured, and _msg/_time still win
	// when both are present.
	custom := `{"ts":"2026-10-26T05:00:00Z","message":"custom","_msg":"explicit","_time":"9"}`
	testing.expect(t, parse_json_line(custom, &arena, "message", "ts", &p))
	testing.expect_value(t, p.msg, "explicit")
	testing.expect_value(t, p.time_raw, "9")
	testing.expect_value(t, p.nfields, 0)
}
