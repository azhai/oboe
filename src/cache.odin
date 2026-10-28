package oboe

import "core:mem"
import "core:strings"
import "core:sync"

// The cache follows the VictoriaMetrics/fastcache layout: each bucket packs
// keys and values into lazily allocated 64 KiB chunks that form a ring. A
// generation counter invalidates a whole lap at once, so reads only take a
// shared lock and never mutate LRU state. Entries too large for a chunk live in
// a per-bucket FIFO area.
//
// Buckets are spread by hash; capacity is split evenly between them, so a
// single write only locks one bucket.

CACHE_BUCKETS :: 512
CACHE_MIN_BUCKETS :: 16
CACHE_CHUNK_SIZE :: 64 << 10
CACHE_MAX_KEY :: 1024

// Packed slot: gen in the high bits, byte offset inside the bucket ring in the
// low bits. A zero slot is never stored because generations start at 1.
CACHE_REC_HDR :: 12 // key_len u16 | val_len u16 | expires_ns i64
CACHE_IDX_BITS :: 40
CACHE_IDX_MASK :: (u64(1) << CACHE_IDX_BITS) - 1
CACHE_GEN_BITS :: 64 - CACHE_IDX_BITS
CACHE_GEN_MASK :: (u64(1) << CACHE_GEN_BITS) - 1

Cache_Config :: struct {
	max_bytes:   i64,
	max_entries: int,
	max_value:   int,
}

default_cache_config :: proc() -> Cache_Config {
	return Cache_Config{
		max_bytes   = 256 << 20,
		max_entries = 1_000_000,
		max_value   = 16 << 20,
	}
}

// Bucket geometry derived from the config; also stored in cache files so a
// load with different flags is rejected instead of silently reshaped.
Cache_Layout :: struct {
	bucket_count:   int,
	max_chunks:     int, // per bucket, 0 = grow without a ring limit
	bucket_bytes:   i64,
	bucket_entries: int,
}

Cache_Big :: struct {
	h:          u64,
	key:        string,
	value:      []u8,
	expires_ns: i64,
	prev:       ^Cache_Big,
	next:       ^Cache_Big,
}

Cache_Bucket :: struct {
	mu:        sync.RW_Mutex,
	m:         map[u64]u64,
	chunks:    [][]u8,
	gen:       u64,
	idx:       u64,
	count:     int,
	big:       map[u64]^Cache_Big,
	big_head:  ^Cache_Big,
	big_tail:  ^Cache_Big,
	big_bytes: i64,

	max_chunks:      int,
	max_entries:     int,
	max_big_bytes:   i64,
}

Cache :: struct {
	cfg:              Cache_Config,
	layout:           Cache_Layout,
	// Keys, values and chunks may be written by any thread but are freed by
	// whichever thread evicts them, so allocation is pinned to the owner.
	allocator:        mem.Allocator,
	buckets:          []Cache_Bucket,
	// Serializes file saves, which the HTTP handler and the periodic saver can
	// trigger at the same time.
	save_mu:          sync.Mutex,
	hits:             u64,
	misses:           u64,
	evictions:        u64,
	expirations:      u64,
	sets:             u64,
	deletes:          u64,
	collisions:       u64,
}

Cache_Stats :: struct {
	entries:     int,
	bytes:       i64,
	hits:        u64,
	misses:      u64,
	evictions:   u64,
	expirations: u64,
	sets:        u64,
	deletes:     u64,
	collisions:  u64,
}

cache_layout :: proc(cfg: Cache_Config) -> Cache_Layout {
	lay := Cache_Layout{bucket_count = CACHE_BUCKETS}
	// Keep at least a few entries per bucket when max_entries is small.
	if cfg.max_entries > 0 {
		for lay.bucket_count > CACHE_MIN_BUCKETS && lay.bucket_count > cfg.max_entries / 4 {
			lay.bucket_count >>= 1
		}
	}
	// Avoid reserving 64 KiB of chunk per bucket for tiny caches.
	if cfg.max_bytes > 0 {
		for lay.bucket_count > CACHE_MIN_BUCKETS && i64(lay.bucket_count) * CACHE_CHUNK_SIZE > cfg.max_bytes * 4 {
			lay.bucket_count >>= 1
		}
		lay.bucket_bytes = (cfg.max_bytes + i64(lay.bucket_count) - 1) / i64(lay.bucket_count)
		lay.max_chunks = int((lay.bucket_bytes + CACHE_CHUNK_SIZE - 1) / CACHE_CHUNK_SIZE)
	}
	if cfg.max_entries > 0 {
		lay.bucket_entries = (cfg.max_entries + lay.bucket_count - 1) / lay.bucket_count
	}
	return lay
}

cache_init :: proc(cfg: Cache_Config) -> ^Cache {
	return cache_init_ex(cfg, cache_layout(cfg))
}

cache_init_ex :: proc(cfg: Cache_Config, lay: Cache_Layout) -> ^Cache {
	c := new(Cache)
	c.cfg = cfg
	c.layout = lay
	c.allocator = context.allocator
	c.buckets = make([]Cache_Bucket, lay.bucket_count, c.allocator)
	for i in 0..<lay.bucket_count {
		b := &c.buckets[i]
		b.m = make(map[u64]u64)
		b.big = make(map[u64]^Cache_Big)
		b.gen = 1
		b.max_chunks = lay.max_chunks
		b.max_entries = lay.bucket_entries
		b.max_big_bytes = lay.bucket_bytes
	}
	return c
}

cache_destroy :: proc(c: ^Cache) {
	for i in 0..<len(c.buckets) {
		b := &c.buckets[i]
		for chunk in b.chunks {
			if chunk != nil {
				delete(chunk, c.allocator)
			}
		}
		delete(b.chunks, c.allocator)
		delete(b.m)
		delete(b.big)
		e := b.big_head
		for e != nil {
			next := e.next
			delete(e.key, c.allocator)
			delete(e.value, c.allocator)
			free(e, c.allocator)
			e = next
		}
	}
	delete(c.buckets, c.allocator)
	free(c)
}

// ---------------------------------------------------------------- encoding

cache_put_u16 :: #force_inline proc(p: []u8, off: int, v: u16) #no_bounds_check {
	p[off] = u8(v)
	p[off + 1] = u8(v >> 8)
}

cache_put_i64 :: #force_inline proc(p: []u8, off: int, v: i64) #no_bounds_check {
	u := u64(v)
	for i in 0..<8 {
		p[off + i] = u8(u >> uint(8 * i))
	}
}

cache_get_u16 :: #force_inline proc(p: []u8, off: int) -> u16 #no_bounds_check {
	return u16(p[off]) | u16(p[off + 1]) << 8
}

cache_get_i64 :: #force_inline proc(p: []u8, off: int) -> i64 #no_bounds_check {
	u: u64
	for i in 0..<8 {
		u |= u64(p[off + i]) << uint(8 * i)
	}
	return i64(u)
}

cache_hash :: #force_inline proc(key: string) -> u64 {
	return mix64(fnv(FNV_BASIS, key))
}

cache_bucket_for :: #force_inline proc(c: ^Cache, h: u64) -> ^Cache_Bucket {
	return &c.buckets[int(h & u64(len(c.buckets) - 1))]
}

cache_expired :: #force_inline proc(expires_ns, now: i64) -> bool {
	return expires_ns != 0 && now >= expires_ns
}

// A slot is live when it belongs to the current generation below the write
// cursor, or to the previous generation past it (records from the older lap
// that the ring has not reached yet).
cache_slot_valid :: #force_inline proc(b: ^Cache_Bucket, slot: u64) -> bool #no_bounds_check {
	gen := slot >> CACHE_IDX_BITS
	idx := slot & CACHE_IDX_MASK
	bgen := b.gen & CACHE_GEN_MASK
	if gen == bgen {
		return idx < b.idx
	}
	if gen + 1 == bgen {
		return idx >= b.idx
	}
	if gen == CACHE_GEN_MASK && bgen == 1 {
		return idx >= b.idx
	}
	return false
}

Cache_Rec :: struct {
	chunk:      []u8,
	off:        int,
	key_len:    int,
	val_len:    int,
	expires_ns: i64,
}

bucket_record :: #force_inline proc(b: ^Cache_Bucket, slot: u64) -> (rec: Cache_Rec, ok: bool) #no_bounds_check {
	idx := int(slot & CACHE_IDX_MASK)
	ci := idx / CACHE_CHUNK_SIZE
	if ci >= len(b.chunks) {
		return {}, false
	}
	chunk := b.chunks[ci]
	if chunk == nil {
		return {}, false
	}
	off := idx % CACHE_CHUNK_SIZE
	if off + CACHE_REC_HDR > CACHE_CHUNK_SIZE {
		return {}, false
	}
	klen := int(cache_get_u16(chunk, off))
	vlen := int(cache_get_u16(chunk, off + 2))
	if off + CACHE_REC_HDR + klen + vlen > CACHE_CHUNK_SIZE {
		return {}, false
	}
	return Cache_Rec{chunk = chunk, off = off, key_len = klen, val_len = vlen, expires_ns = cache_get_i64(chunk, off + 4)}, true
}

// ---------------------------------------------------------------- bucket ops

cache_ensure_chunk :: proc(c: ^Cache, b: ^Cache_Bucket, ci: int) -> bool {
	if ci < len(b.chunks) {
		return b.chunks[ci] != nil
	}
	if b.max_chunks > 0 && ci >= b.max_chunks {
		return false
	}
	grown := make([][]u8, ci + 1, c.allocator)
	copy(grown, b.chunks)
	if len(b.chunks) > 0 {
		delete(b.chunks, c.allocator)
	}
	b.chunks = grown
	b.chunks[ci] = make([]u8, CACHE_CHUNK_SIZE, c.allocator)
	return true
}

cache_clean_locked :: proc(c: ^Cache, b: ^Cache_Bucket) {
	victims: [dynamic]u64
	defer delete(victims)
	for h, slot in b.m {
		if !cache_slot_valid(b, slot) {
			append(&victims, h)
		}
	}
	n := len(victims)
	for h in victims {
		delete_key(&b.m, h)
	}
	b.count -= n
	if n > 0 {
		sync.atomic_add(&c.evictions, u64(n))
	}
}

cache_write_record_locked :: proc(c: ^Cache, b: ^Cache_Bucket, h: u64, key: string, value: []u8, expires_ns: i64) -> bool #no_bounds_check {
	rec_len := CACHE_REC_HDR + len(key) + len(value)
	if rec_len >= CACHE_CHUNK_SIZE {
		return false
	}
	ci := int(b.idx) / CACHE_CHUNK_SIZE
	new_idx := b.idx + u64(rec_len)
	new_ci := int(new_idx) / CACHE_CHUNK_SIZE
	wrapped := false
	if new_ci > ci {
		if b.max_chunks > 0 && new_ci >= b.max_chunks {
			b.gen += 1
			if b.gen & CACHE_GEN_MASK == 0 {
				b.gen += 1
			}
			b.idx = 0
			ci = 0
			new_idx = u64(rec_len)
			wrapped = true
		} else {
			ci = new_ci
			b.idx = u64(ci * CACHE_CHUNK_SIZE)
			new_idx = b.idx + u64(rec_len)
		}
	}
	if !cache_ensure_chunk(c, b, ci) {
		return false
	}
	off := int(b.idx) % CACHE_CHUNK_SIZE
	chunk := b.chunks[ci]
	cache_put_u16(chunk, off, u16(len(key)))
	cache_put_u16(chunk, off + 2, u16(len(value)))
	cache_put_i64(chunk, off + 4, expires_ns)
	koff := off + CACHE_REC_HDR
	copy(chunk[koff:koff + len(key)], transmute([]u8)key)
	copy(chunk[koff + len(key):koff + len(key) + len(value)], value)
	b.m[h] = (b.gen << CACHE_IDX_BITS) | b.idx
	b.idx = new_idx
	if wrapped {
		cache_clean_locked(c, b)
	}
	return true
}

// bucket_lookup_locked returns the packed value of key. collided is set when the
// hash is present but belongs to a different key, which means the old entry was
// shadowed by a collision.
bucket_lookup_locked :: proc(b: ^Cache_Bucket, h: u64, key: string) -> (value: []u8, expires_ns: i64, ok: bool, collided: bool) #no_bounds_check {
	slot, has := b.m[h]
	if !has || !cache_slot_valid(b, slot) {
		return nil, 0, false, false
	}
	rec, rok := bucket_record(b, slot)
	if !rok {
		return nil, 0, false, true
	}
	koff := rec.off + CACHE_REC_HDR
	if string(rec.chunk[koff:koff + rec.key_len]) != key {
		return nil, 0, false, true
	}
	return rec.chunk[koff + rec.key_len:koff + rec.key_len + rec.val_len], rec.expires_ns, true, false
}

cache_big_remove :: proc(c: ^Cache, b: ^Cache_Bucket, e: ^Cache_Big, eviction: bool) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		b.big_head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		b.big_tail = e.prev
	}
	delete_key(&b.big, e.h)
	b.big_bytes -= i64(len(e.value))
	b.count -= 1
	if eviction {
		sync.atomic_add(&c.evictions, 1)
	}
	delete(e.key, c.allocator)
	delete(e.value, c.allocator)
	free(e, c.allocator)
}

cache_big_push_locked :: proc(c: ^Cache, b: ^Cache_Bucket, h: u64, key: string, value: []u8, expires_ns: i64) {
	e := new(Cache_Big, c.allocator)
	e.h = h
	e.key = strings.clone(key, c.allocator)
	e.value = make([]u8, len(value), c.allocator)
	copy(e.value, value)
	e.expires_ns = expires_ns
	e.next = b.big_head
	if b.big_head != nil {
		b.big_head.prev = e
	} else {
		b.big_tail = e
	}
	b.big_head = e
	b.big[h] = e
	b.count += 1
	b.big_bytes += i64(len(value))
	// Evict older big values down to the bucket budget, but keep the value that
	// was just stored even when it alone exceeds the budget. Otherwise a single
	// value larger than max_bytes/bucket_count could never be cached.
	for b.max_big_bytes > 0 && b.big_bytes > b.max_big_bytes && b.big_tail != nil && b.big_tail != e {
		cache_big_remove(c, b, b.big_tail, true)
	}
}

cache_make_room_locked :: proc(c: ^Cache, b: ^Cache_Bucket) {
	if b.max_entries <= 0 {
		return
	}
	for b.count >= b.max_entries {
		if b.big_tail != nil {
			cache_big_remove(c, b, b.big_tail, true)
			continue
		}
		dropped := 0
		for h, slot in b.m {
			if cache_slot_valid(b, slot) {
				dropped += 1
			}
		}
		if len(b.m) > 0 {
			clear(&b.m)
		}
		b.count -= dropped
		if dropped > 0 {
			sync.atomic_add(&c.evictions, u64(dropped))
		}
		if dropped == 0 {
			break
		}
	}
}

// ---------------------------------------------------------------- public API

cache_get_into :: proc(c: ^Cache, key: string, out: ^Buf) -> bool #no_bounds_check {
	h := cache_hash(key)
	b := cache_bucket_for(c, h)
	found := false
	expired_small := false
	expired_big: ^Cache_Big

	sync.rw_mutex_shared_lock(&b.mu)
	value, expires_ns, ok, collided := bucket_lookup_locked(b, h, key)
	if ok {
		// Most entries have no TTL, so only read the clock when needed.
		if expires_ns != 0 && cache_expired(expires_ns, now_ns()) {
			expired_small = true
			ok = false
		} else {
			buf_write(out, value)
			found = true
		}
	}
	if !ok && !expired_small {
		if e, has := b.big[h]; has && e.key == key {
			if e.expires_ns != 0 && cache_expired(e.expires_ns, now_ns()) {
				expired_big = e
			} else {
				buf_write(out, e.value)
				found = true
			}
		}
	}
	sync.rw_mutex_shared_unlock(&b.mu)

	if collided {
		sync.atomic_add(&c.collisions, 1)
	}
	if expired_small || expired_big != nil {
		sync.rw_mutex_lock(&b.mu)
		// Another writer may have refreshed the entry between the shared and
		// exclusive sections, so resolve the removal against current state.
		_, expires2, ok2, _ := bucket_lookup_locked(b, h, key)
		if ok2 && cache_expired(expires2, now_ns()) {
			delete_key(&b.m, h)
			b.count -= 1
			sync.atomic_add(&c.expirations, 1)
		}
		if expired_big != nil {
			if e, has := b.big[h]; has && e == expired_big {
				if cache_expired(e.expires_ns, now_ns()) {
					cache_big_remove(c, b, e, false)
					sync.atomic_add(&c.expirations, 1)
				}
			}
		}
		sync.rw_mutex_unlock(&b.mu)
	}
	if found {
		sync.atomic_add(&c.hits, 1)
	} else {
		sync.atomic_add(&c.misses, 1)
	}
	return found
}

// cache_get returns a caller-owned copy of the value.
cache_get :: proc(c: ^Cache, key: string) -> ([]u8, bool) {
	out := buf_init(64)
	defer buf_destroy(&out)
	if !cache_get_into(c, key, &out) {
		return nil, false
	}
	value := make([]u8, len(out.data))
	copy(value, out.data[:])
	return value, true
}

cache_set :: proc(c: ^Cache, key: string, value: []u8, ttl_ns: i64) -> bool {
	if len(key) == 0 || len(key) > CACHE_MAX_KEY {
		return false
	}
	if c.cfg.max_value > 0 && len(value) > c.cfg.max_value {
		return false
	}
	expires_ns: i64
	if ttl_ns > 0 {
		now := now_ns()
		expires_ns = now + ttl_ns
		if expires_ns < now {
			expires_ns = i64(0x7fffffffffffffff)
		}
	}
	h := cache_hash(key)
	b := cache_bucket_for(c, h)
	small := CACHE_REC_HDR + len(key) + len(value) < CACHE_CHUNK_SIZE
	ok := false

	sync.rw_mutex_lock(&b.mu)
	if small {
		_, _, exists, _ := bucket_lookup_locked(b, h, key)
		if !exists {
			cache_make_room_locked(c, b)
		}
		// Drop a big entry shadowed by the same hash.
		if e, has := b.big[h]; has {
			cache_big_remove(c, b, e, false)
		}
		ok = cache_write_record_locked(c, b, h, key, value, expires_ns)
		if ok && !exists {
			b.count += 1
		}
	} else {
		replace := false
		if e, has := b.big[h]; has && e.key == key {
			replace = true
		}
		if !replace {
			cache_make_room_locked(c, b)
		}
		// Drop a small entry shadowed by the same hash.
		if slot, has := b.m[h]; has {
			if cache_slot_valid(b, slot) {
				b.count -= 1
			}
			delete_key(&b.m, h)
		}
		if e, has := b.big[h]; has {
			if e.key == key {
				b.big_bytes += i64(len(value)) - i64(len(e.value))
				delete(e.value, c.allocator)
				e.value = make([]u8, len(value), c.allocator)
				copy(e.value, value)
				e.expires_ns = expires_ns
				ok = true
			} else {
				cache_big_remove(c, b, e, false)
			}
		}
		if !ok {
			cache_big_push_locked(c, b, h, key, value, expires_ns)
			ok = true
		}
	}
	sync.rw_mutex_unlock(&b.mu)
	if ok {
		sync.atomic_add(&c.sets, 1)
	}
	return ok
}

cache_delete :: proc(c: ^Cache, key: string) -> bool {
	h := cache_hash(key)
	b := cache_bucket_for(c, h)
	deleted := false
	sync.rw_mutex_lock(&b.mu)
	if slot, has := b.m[h]; has {
		if cache_slot_valid(b, slot) {
			if rec, rok := bucket_record(b, slot); rok {
				koff := rec.off + CACHE_REC_HDR
				if string(rec.chunk[koff:koff + rec.key_len]) == key {
					delete_key(&b.m, h)
					b.count -= 1
					deleted = true
				}
			}
		}
	}
	if e, has := b.big[h]; has && e.key == key {
		cache_big_remove(c, b, e, false)
		deleted = true
	}
	sync.rw_mutex_unlock(&b.mu)
	if deleted {
		sync.atomic_add(&c.deletes, 1)
	}
	return deleted
}

cache_stats :: proc(c: ^Cache) -> Cache_Stats {
	st := Cache_Stats{
		hits        = sync.atomic_load(&c.hits),
		misses      = sync.atomic_load(&c.misses),
		evictions   = sync.atomic_load(&c.evictions),
		expirations = sync.atomic_load(&c.expirations),
		sets        = sync.atomic_load(&c.sets),
		deletes     = sync.atomic_load(&c.deletes),
		collisions  = sync.atomic_load(&c.collisions),
	}
	now := now_ns()
	expired_total := u64(0)
	for i in 0..<len(c.buckets) {
		b := &c.buckets[i]
		sync.rw_mutex_lock(&b.mu)
		victims: [dynamic]u64
		small_live := 0
		for h, slot in b.m {
			if !cache_slot_valid(b, slot) {
				append(&victims, h)
				continue
			}
			rec, rok := bucket_record(b, slot)
			if !rok {
				append(&victims, h)
				continue
			}
			if cache_expired(rec.expires_ns, now) {
				append(&victims, h)
				expired_total += 1
				continue
			}
			small_live += 1
			st.bytes += i64(rec.val_len)
		}
		for h in victims {
			delete_key(&b.m, h)
		}
		delete(victims)
		big_live := 0
		e := b.big_head
		for e != nil {
			next := e.next
			if cache_expired(e.expires_ns, now) {
				cache_big_remove(c, b, e, false)
				expired_total += 1
			} else {
				big_live += 1
				st.bytes += i64(len(e.value))
			}
			e = next
		}
		b.count = small_live + big_live
		st.entries += b.count
		sync.rw_mutex_unlock(&b.mu)
	}
	st.expirations += expired_total
	if expired_total > 0 {
		sync.atomic_add(&c.expirations, expired_total)
	}
	return st
}
