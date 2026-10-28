package oboe

import "core:fmt"
import "core:testing"

@(test)
test_bloom_no_false_negatives :: proc(t: ^testing.T) {
	bb: Bloom_Builder
	defer delete(bb.hashes)
	defer delete(bb.table)
	values := []string{"error", "info", "warn", "GET /api/v1/users", "你好世界"}
	for v in values {
		bloom_builder_add_value(&bb, "msg", v)
	}
	bits := bloom_builder_finish(&bb)
	defer delete(bits)
	testing.expect_value(t, len(bits), 128)

	b := bloom_from_bytes(bits)
	missing := 0
	for v in values {
		// Full values are indexed exact-case; tokens are indexed case-folded.
		if !bloom_check_scoped(&b, "msg", v, false) {
			missing += 1
		}
		it := Token_Iter{s = v}
		for tok in next_token(&it) {
			if !bloom_check_scoped(&b, "msg", tok, true) {
				missing += 1
			}
		}
	}
	testing.expect_value(t, missing, 0)

	// Tokens are indexed case-folded; full values stay exact.
	testing.expect(t, bloom_check_scoped(&b, "msg", "API", true))
	testing.expect(t, bloom_check_scoped(&b, "msg", "error", false))
	testing.expect(t, !bloom_check_scoped(&b, "msg", "debug", true))
}

@(test)
test_bloom_false_positive_rate :: proc(t: ^testing.T) {
	bb: Bloom_Builder
	defer delete(bb.hashes)
	defer delete(bb.table)
	values := make([dynamic]string)
	defer {
		for v in values {
			delete(v)
		}
		delete(values)
	}
	for i in 0..<1000 {
		v := fmt.aprintf("value-%d", i)
		append(&values, v)
		bloom_builder_add_value(&bb, "msg", v)
	}
	bits := bloom_builder_finish(&bb)
	defer delete(bits)
	b := bloom_from_bytes(bits)

	missing := 0
	for v in values {
		if !bloom_check_scoped(&b, "msg", v, false) {
			missing += 1
		}
	}
	testing.expect_value(t, missing, 0)

	false_positives := 0
	for i in 0..<1000 {
		v := fmt.aprintf("absent-%d", i)
		if bloom_check_scoped(&b, "msg", v, false) {
			false_positives += 1
		}
		delete(v)
	}
	testing.expectf(t, false_positives <= 20, "false positives=%d/1000", false_positives)
}

@(test)
test_bloom_builder_size_cap :: proc(t: ^testing.T) {
	bb: Bloom_Builder
	defer delete(bb.hashes)
	defer delete(bb.table)
	for i in 0..<30_000 {
		bloom_builder_add_hash(&bb, mix64(u64(i)))
	}
	bits := bloom_builder_finish(&bb)
	defer delete(bits)
	testing.expect_value(t, len(bits), int(BLOOM_MAX_BITS / 8))

	bloom_builder_reset(&bb)
	empty := bloom_builder_finish(&bb)
	defer delete(empty)
	testing.expect_value(t, len(empty), 128)
	eb := bloom_from_bytes(empty)
	testing.expect(t, !bloom_check_scoped(&eb, "msg", "anything", false))
}
