package oboe

import "core:mem"

BLOOM_K :: 7
BLOOM_MAX_BITS :: 262144 // 32 KiB per block
BLOOM_DEDUPE_MAX :: 1 << 22

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

bloom_pos :: #force_inline proc(b: ^Bloom, h: u64, i: u32) -> u64 #no_bounds_check {
	h2 := (h >> 32) | (h << 32) | 1
	return (h + u64(i) * h2) & (b.nbits - 1)
}

bloom_add_hash :: proc(b: ^Bloom, h: u64) #no_bounds_check {
	if b.nbits == 0 {
		return
	}
	for i in 0..<b.k {
		p := bloom_pos(b, h, i)
		b.bits[int(p >> 3)] |= u8(1) << u8(p & 7)
	}
}

bloom_check_hash :: proc(b: ^Bloom, h: u64) -> bool #no_bounds_check {
	if b.nbits == 0 {
		return false
	}
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

BLOOM_MEMO_SIZE :: 256

Bloom_Memo :: struct {
	h:     u64,
	off:   int,
	n:     int,
	value: string,
	valid: bool,
}

Bloom_Builder :: struct {
	hashes: [dynamic]u64,
	// Scratch space reused across blocks to de-duplicate hashes in linear time.
	table: [dynamic]u64,
	// Direct-mapped memo of recently added values. Log fields repeat heavily
	// (levels, services, message templates), so the token hashes of a value are
	// usually already known within a block.
	memo: [BLOOM_MEMO_SIZE]Bloom_Memo,
}

bloom_builder_reset :: proc(bb: ^Bloom_Builder) {
	clear(&bb.hashes)
	bb.memo = {}
}

bloom_builder_add_hash :: proc(bb: ^Bloom_Builder, h: u64) {
	append(&bb.hashes, h)
}

// bloom_builder_add_value indexes all tokens of value, plus the full value for
// exact-match pruning. Tokens are case-folded; full values are not.
bloom_builder_add_value :: proc(bb: ^Bloom_Builder, field, value: string) {
	base := fnv(FNV_BASIS, field)
	base = fnv_continue(base, "\x00")
	full_h: u64
	keyed := len(value) > 0 && len(value) <= 512
	if keyed {
		full_h = mix64(fnv_continue(base, value, false))
		e := &bb.memo[full_h & (BLOOM_MEMO_SIZE - 1)]
		if e.valid && e.h == full_h && e.value == value {
			n := e.n
			old := len(bb.hashes)
			reserve(&bb.hashes, old + n)
			resize(&bb.hashes, old + n)
			copy(bb.hashes[old:], bb.hashes[e.off:e.off+n])
			return
		}
		start := len(bb.hashes)
		it := Token_Iter{s = value}
		for tok in next_token(&it) {
			append(&bb.hashes, mix64(fnv_continue(base, tok, true)))
		}
		append(&bb.hashes, full_h)
		e^ = Bloom_Memo{h = full_h, off = start, n = len(bb.hashes) - start, value = value, valid = true}
		return
	}
	it := Token_Iter{s = value}
	for tok in next_token(&it) {
		append(&bb.hashes, mix64(fnv_continue(base, tok, true)))
	}
}

// bloom_builder_finish builds the filter. Duplicate hashes only need to be set
// once, so they are collapsed with an open-addressed table first: this is O(n)
// versus sorting the whole hash list, and it keeps the filter sized from the
// unique item count instead of the emitted token count.
bloom_builder_finish :: proc(bb: ^Bloom_Builder) -> []u8 {
	n := len(bb.hashes)
	uniq := n
	// De-duplicating more than this would need an unreasonable scratch table;
	// such a block is large enough that the maximum-size filter is free.
	if n <= BLOOM_DEDUPE_MAX / 2 {
		size := 1024
		for size < n * 2 {
			size <<= 1
		}
		if cap(bb.table) < size {
			bb.table = make([dynamic]u64, size)
		} else {
			resize(&bb.table, size)
			mem.zero_slice(bb.table[:])
		}
		mask := u64(size - 1)
		uniq = 0
		zero_seen := false
		for h in bb.hashes {
			if h == 0 {
				zero_seen = true
				continue
			}
			i := h & mask
			for bb.table[i] != 0 && bb.table[i] != h {
				i = (i + 1) & mask
			}
			if bb.table[i] == 0 {
				bb.table[i] = h
				bb.hashes[uniq] = h
				uniq += 1
			}
		}
		if zero_seen {
			bb.hashes[uniq] = 0
			uniq += 1
		}
		clear(&bb.table)
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
