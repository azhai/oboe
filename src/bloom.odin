package odlog

import "core:sort"

BLOOM_K :: 7
BLOOM_MAX_BITS :: 262144 // 32 KiB per block

Bloom :: struct {
	bits:  []u8,
	nbits: u64,
	k:     u32,
}

bloom_from_bytes :: proc(bits: []u8) -> Bloom {
	return Bloom{bits = bits, nbits = u64(len(bits)) * 8, k = BLOOM_K}
}

bloom_hash :: proc(field, value: string, fold: bool) -> u64 {
	h := fnv(FNV_BASIS, field)
	h = fnv_continue(h, "\x00")
	h = fnv_continue(h, value, fold)
	return mix64(h)
}

bloom_pos :: #force_inline proc(b: ^Bloom, h: u64, i: u32) -> u64 {
	h2 := (h >> 32) | (h << 32) | 1
	return (h + u64(i) * h2) & (b.nbits - 1)
}

bloom_add_hash :: proc(b: ^Bloom, h: u64) {
	for i in 0..<b.k {
		p := bloom_pos(b, h, i)
		b.bits[int(p >> 3)] |= u8(1) << u8(p & 7)
	}
}

bloom_check_hash :: proc(b: ^Bloom, h: u64) -> bool {
	for i in 0..<b.k {
		p := bloom_pos(b, h, i)
		if b.bits[int(p >> 3)] & (u8(1) << u8(p & 7)) == 0 {
			return false
		}
	}
	return true
}

bloom_check_scoped :: proc(b: ^Bloom, field, value: string, fold: bool) -> bool {
	return bloom_check_hash(b, bloom_hash(field, value, fold))
}

Bloom_Builder :: struct {
	hashes: [dynamic]u64,
}

bloom_builder_reset :: proc(bb: ^Bloom_Builder) {
	clear(&bb.hashes)
}

bloom_builder_add_hash :: proc(bb: ^Bloom_Builder, h: u64) {
	append(&bb.hashes, h)
}

// bloom_builder_add_value indexes all tokens of value, plus the full value for
// exact-match pruning. Tokens are case-folded; full values are not.
bloom_builder_add_value :: proc(bb: ^Bloom_Builder, field, value: string) {
	it := Token_Iter{s = value}
	for tok in next_token(&it) {
		append(&bb.hashes, bloom_hash(field, tok, true))
	}
	if len(value) > 0 && len(value) <= 512 {
		append(&bb.hashes, bloom_hash(field, value, false))
	}
}

bloom_builder_finish :: proc(bb: ^Bloom_Builder) -> []u8 {
	sort.quick_sort(bb.hashes[:])
	// unique in place
	uniq := 0
	for i in 0..<len(bb.hashes) {
		if i == 0 || bb.hashes[i] != bb.hashes[i-1] {
			bb.hashes[uniq] = bb.hashes[i]
			uniq += 1
		}
	}
	target := i64(uniq) * 10
	if target < 1024 {
		target = 1024
	}
	nbits: u64 = 1024
	for nbits < u64(target) && nbits < BLOOM_MAX_BITS {
		nbits <<= 1
	}
	if nbits > BLOOM_MAX_BITS {
		nbits = BLOOM_MAX_BITS
	}
	bits := make([]u8, int(nbits / 8))
	b := Bloom{bits = bits, nbits = nbits, k = BLOOM_K}
	for i in 0..<uniq {
		bloom_add_hash(&b, bb.hashes[i])
	}
	return bits
}
