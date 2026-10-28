package oboe

import "core:fmt"
import "core:os"
import "core:sync"
import "core:time"

// Cache files are a single artifact written next to the log data. Each bucket is
// serialized (generation, write cursor, hash slots, chunk ring and large-value
// FIFO) and zlib-compressed independently, so a corrupted bucket only costs
// that bucket. Saving goes through `<path>.tmp` and a rename to stay atomic.

CACHE_FILE_MAGIC :: "ODCA"
CACHE_FILE_VERSION :: u16(1)

cache_save_file :: proc(c: ^Cache, path: string) -> (bytes: i64, ok: bool) {
	if c == nil || len(path) == 0 {
		return 0, false
	}
	sync.mutex_lock(&c.save_mu)
	defer sync.mutex_unlock(&c.save_mu)
	tmp := fmt.aprintf("%s.tmp", path)
	defer delete(tmp)
	f, ferr := os.open(tmp, {.Write, .Create, .Trunc})
	if ferr != nil {
		return 0, false
	}
	closed := false
	committed := false
	defer {
		if !closed {
			os.close(f)
		}
		if !committed {
			os.remove(tmp)
		}
	}

	header := buf_init(32)
	defer buf_destroy(&header)
	buf_string(&header, CACHE_FILE_MAGIC)
	buf_u16le(&header, CACHE_FILE_VERSION)
	buf_u32le(&header, u32(len(c.buckets)))
	buf_u32le(&header, u32(CACHE_CHUNK_SIZE))
	buf_u32le(&header, u32(c.layout.max_chunks))
	if n, werr := os.write(f, header.data[:]); werr != nil {
		return 0, false
	} else {
		bytes += i64(n)
	}

	for i in 0..<len(c.buckets) {
		blob, bok := cache_bucket_blob(c, &c.buckets[i])
		if !bok {
			return 0, false
		}
		raw_len := len(blob.data)
		comp, cok := zlib_compress(blob.data[:], 1)
		buf_destroy(&blob)
		if !cok {
			return 0, false
		}
		sizes := buf_init(16)
		buf_u64le(&sizes, u64(raw_len))
		buf_u64le(&sizes, u64(len(comp)))
		n1, e1 := os.write(f, sizes.data[:])
		n2, e2 := os.write(f, comp)
		buf_destroy(&sizes)
		delete(comp)
		if e1 != nil || e2 != nil {
			return 0, false
		}
		bytes += i64(n1) + i64(n2)
	}

	if serr := os.sync(f); serr != nil {
		return 0, false
	}
	if cerr := os.close(f); cerr != nil {
		return 0, false
	}
	closed = true
	if rerr := os.rename(tmp, path); rerr != nil {
		return 0, false
	}
	committed = true
	return bytes, true
}

cache_bucket_blob :: proc(c: ^Cache, b: ^Cache_Bucket) -> (blob: Buf, ok: bool) {
	blob = buf_init(CACHE_CHUNK_SIZE)
	now := now_ns()
	sync.rw_mutex_lock(&b.mu)
	defer sync.rw_mutex_unlock(&b.mu)

	buf_u64le(&blob, b.gen)
	buf_u64le(&blob, b.idx)
	pair_slot := len(blob.data)
	buf_u32le(&blob, 0)
	pairs := u32(0)
	for h, slot in b.m {
		if !cache_slot_valid(b, slot) {
			continue
		}
		rec, rok := bucket_record(b, slot)
		if !rok || cache_expired(rec.expires_ns, now) {
			continue
		}
		buf_u64le(&blob, h)
		buf_u64le(&blob, slot)
		pairs += 1
	}
	buf_put_u32_at(&blob, pair_slot, pairs)

	buf_u32le(&blob, u32(len(b.chunks)))
	for i in 0..<len(b.chunks) {
		if b.chunks[i] == nil {
			b.chunks[i] = make([]u8, CACHE_CHUNK_SIZE, c.allocator)
		}
		buf_write(&blob, b.chunks[i])
	}

	big_slot := len(blob.data)
	buf_u32le(&blob, 0)
	big_count := u32(0)
	for e := b.big_head; e != nil; e = e.next {
		if cache_expired(e.expires_ns, now) {
			continue
		}
		buf_u16le(&blob, u16(len(e.key)))
		buf_u32le(&blob, u32(len(e.value)))
		buf_i64le(&blob, e.expires_ns)
		buf_write(&blob, transmute([]u8)e.key)
		buf_write(&blob, e.value)
		big_count += 1
	}
	buf_put_u32_at(&blob, big_slot, big_count)
	return blob, true
}

// cache_load_file loads a cache written by cache_save_file. It returns false
// when the file is missing, corrupt, or was written with a different geometry.
cache_load_file :: proc(cfg: Cache_Config, path: string) -> (^Cache, bool) {
	data, ferr := os.read_entire_file_from_path(path, context.allocator)
	if ferr != nil {
		return nil, false
	}
	defer delete(data)

	r := Cache_Reader{data = data, ok = true}
	magic := cache_rd_bytes(&r, len(CACHE_FILE_MAGIC))
	if !r.ok || string(magic) != CACHE_FILE_MAGIC {
		return nil, false
	}
	version := cache_rd_u16(&r)
	bucket_count := cache_rd_u32(&r)
	chunk_size := cache_rd_u32(&r)
	max_chunks := cache_rd_u32(&r)
	if !r.ok || version != CACHE_FILE_VERSION || chunk_size != u32(CACHE_CHUNK_SIZE) {
		return nil, false
	}
	lay := cache_layout(cfg)
	if int(bucket_count) != lay.bucket_count || int(max_chunks) != lay.max_chunks {
		return nil, false
	}

	c := cache_init_ex(cfg, lay)
	for i in 0..<lay.bucket_count {
		raw_len := cache_rd_u64(&r)
		comp_len := cache_rd_u64(&r)
		if !r.ok || comp_len > u64(len(r.data) - r.pos) || raw_len < 16 || raw_len > u64(1 << 31) {
			cache_destroy(c)
			return nil, false
		}
		comp := r.data[r.pos:r.pos + int(comp_len)]
		r.pos += int(comp_len)
		raw, zok := zlib_decompress(comp, int(raw_len))
		if !zok {
			cache_destroy(c)
			return nil, false
		}
		bok := cache_load_bucket(c, &c.buckets[i], raw)
		delete(raw)
		if !bok {
			cache_destroy(c)
			return nil, false
		}
	}
	return c, true
}

cache_load_bucket :: proc(c: ^Cache, b: ^Cache_Bucket, raw: []u8) -> bool {
	r := Cache_Reader{data = raw, ok = true}
	gen := cache_rd_u64(&r)
	idx := cache_rd_u64(&r)
	pair_count := cache_rd_u32(&r)
	if !r.ok || int(pair_count) > (len(r.data) - r.pos) / 16 {
		return false
	}
	pairs := make([]u64, int(pair_count) * 2)
	defer delete(pairs)
	for i in 0..<int(pair_count) {
		pairs[i * 2] = cache_rd_u64(&r)
		pairs[i * 2 + 1] = cache_rd_u64(&r)
	}
	chunk_count := cache_rd_u32(&r)
	if !r.ok {
		return false
	}
	if int(chunk_count) * CACHE_CHUNK_SIZE > len(r.data) - r.pos {
		return false
	}
	chunks := make([][]u8, int(chunk_count), c.allocator)
	for ci in 0..<int(chunk_count) {
		chunk := make([]u8, CACHE_CHUNK_SIZE, c.allocator)
		copy(chunk, r.data[r.pos:r.pos + CACHE_CHUNK_SIZE])
		r.pos += CACHE_CHUNK_SIZE
		chunks[ci] = chunk
	}
	big_count := cache_rd_u32(&r)
	if !r.ok {
		for chunk in chunks {
			delete(chunk, c.allocator)
		}
		delete(chunks, c.allocator)
		return false
	}
	if idx > u64(int(chunk_count) * CACHE_CHUNK_SIZE) {
		for chunk in chunks {
			delete(chunk, c.allocator)
		}
		delete(chunks, c.allocator)
		return false
	}

	b.gen = gen
	b.idx = idx
	b.chunks = chunks
	m := make(map[u64]u64, int(pair_count))
	now := now_ns()
	for i in 0..<int(pair_count) {
		h := pairs[i * 2]
		slot := pairs[i * 2 + 1]
		if !cache_slot_valid(b, slot) {
			continue
		}
		rec, rok := bucket_record(b, slot)
		if !rok || cache_expired(rec.expires_ns, now) {
			continue
		}
		m[h] = slot
	}
	b.m = m

	for _ in 0..<int(big_count) {
		klen := int(cache_rd_u16(&r))
		vlen := int(cache_rd_u32(&r))
		expires := cache_rd_i64(&r)
		key := cache_rd_bytes(&r, klen)
		value := cache_rd_bytes(&r, vlen)
		if !r.ok {
			return false
		}
		if cache_expired(expires, now) {
			continue
		}
		cache_big_push_locked(c, b, cache_hash(string(key)), string(key), value, expires)
	}
	b.count = len(b.m) + len(b.big)
	return true
}

// ---------------------------------------------------------------- reader

// ---------------------------------------------------------------- periodic saver

Cache_Saver :: struct {
	c:        ^Cache,
	path:     string,
	interval: time.Duration,
	stop:     u32,
}

cache_saver_run :: proc(data: rawptr) {
	s := cast(^Cache_Saver)data
	if s.interval <= 0 {
		return
	}
	// Sleep in slices so shutdown joins quickly instead of waiting a full
	// interval before the thread notices stop.
	step := time.Duration(100 * time.Millisecond)
	if s.interval < step {
		step = s.interval
	}
	waited := time.Duration(0)
	for sync.atomic_load(&s.stop) == 0 {
		time.sleep(step)
		waited += step
		if waited < s.interval {
			continue
		}
		waited = 0
		if sync.atomic_load(&s.stop) != 0 {
			break
		}
		if _, ok := cache_save_file(s.c, s.path); !ok {
			fmt.eprintf("oboe: periodic cache save to %s failed\n", s.path)
		}
	}
}

// ---------------------------------------------------------------- reader

Cache_Reader :: struct {
	data: []u8,
	pos:  int,
	ok:   bool,
}

cache_rd_bytes :: proc(r: ^Cache_Reader, n: int) -> []u8 {
	if !r.ok || n < 0 || r.pos + n > len(r.data) {
		r.ok = false
		return nil
	}
	out := r.data[r.pos:r.pos + n]
	r.pos += n
	return out
}

cache_rd_u16 :: proc(r: ^Cache_Reader) -> u16 {
	b := cache_rd_bytes(r, 2)
	if b == nil {
		return 0
	}
	return cache_get_u16(b, 0)
}

cache_rd_u32 :: proc(r: ^Cache_Reader) -> u32 {
	b := cache_rd_bytes(r, 4)
	if b == nil {
		return 0
	}
	return u32(b[0]) | u32(b[1]) << 8 | u32(b[2]) << 16 | u32(b[3]) << 24
}

cache_rd_u64 :: proc(r: ^Cache_Reader) -> u64 {
	b := cache_rd_bytes(r, 8)
	if b == nil {
		return 0
	}
	u: u64
	for i in 0..<8 {
		u |= u64(b[i]) << uint(8 * i)
	}
	return u
}

cache_rd_i64 :: proc(r: ^Cache_Reader) -> i64 {
	return i64(cache_rd_u64(r))
}
