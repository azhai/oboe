package odlog

import "core:strings"
import "core:sync"

// Keys and values live in process memory only; a restart starts with an empty
// cache. Capacity is split evenly across shards so each write needs one lock.

CACHE_SHARDS :: 16
CACHE_MAX_KEY :: 1024

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

Cache_Entry :: struct {
	key:        string,
	value:      []u8,
	expires_ns: i64,
	prev:       ^Cache_Entry,
	next:       ^Cache_Entry,
}

Cache_Shard :: struct {
	mu:          sync.Mutex,
	entries:     map[string]^Cache_Entry,
	head:        ^Cache_Entry,
	tail:        ^Cache_Entry,
	bytes:       i64,
	max_bytes:   i64,
	max_entries: int,
}

Cache :: struct {
	cfg:          Cache_Config,
	shards:       [CACHE_SHARDS]Cache_Shard,
	hits:         u64,
	misses:       u64,
	evictions:    u64,
	expirations:  u64,
	sets:         u64,
	deletes:      u64,
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
}

cache_init :: proc(cfg: Cache_Config) -> ^Cache {
	c := new(Cache)
	c.cfg = cfg
	for i in 0..<CACHE_SHARDS {
		sh := &c.shards[i]
		sh.entries = make(map[string]^Cache_Entry)
		if cfg.max_bytes > 0 {
			sh.max_bytes = (cfg.max_bytes + CACHE_SHARDS - 1) / CACHE_SHARDS
		}
		if cfg.max_entries > 0 {
			sh.max_entries = (cfg.max_entries + CACHE_SHARDS - 1) / CACHE_SHARDS
		}
	}
	return c
}

cache_destroy :: proc(c: ^Cache) {
	for i in 0..<CACHE_SHARDS {
		sh := &c.shards[i]
		e := sh.head
		for e != nil {
			next := e.next
			delete(e.key)
			if e.value != nil {
				delete(e.value)
			}
			free(e)
			e = next
		}
		delete(sh.entries)
	}
	free(c)
}

cache_shard :: #force_inline proc(c: ^Cache, key: string) -> ^Cache_Shard {
	h := fnv(FNV_BASIS, key)
	return &c.shards[h % CACHE_SHARDS]
}

cache_expired :: #force_inline proc(e: ^Cache_Entry, now: i64) -> bool {
	return e.expires_ns != 0 && now >= e.expires_ns
}

lru_unlink :: proc(sh: ^Cache_Shard, e: ^Cache_Entry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		sh.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		sh.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

lru_push_front :: proc(sh: ^Cache_Shard, e: ^Cache_Entry) {
	e.prev = nil
	e.next = sh.head
	if sh.head != nil {
		sh.head.prev = e
	}
	sh.head = e
	if sh.tail == nil {
		sh.tail = e
	}
}

lru_touch :: proc(sh: ^Cache_Shard, e: ^Cache_Entry) {
	if sh.head == e {
		return
	}
	lru_unlink(sh, e)
	lru_push_front(sh, e)
}

entry_remove :: proc(sh: ^Cache_Shard, e: ^Cache_Entry) {
	lru_unlink(sh, e)
	delete_key(&sh.entries, e.key)
	sh.bytes -= i64(len(e.value))
	delete(e.key)
	if e.value != nil {
		delete(e.value)
	}
	free(e)
}

	cache_get_into :: proc(c: ^Cache, key: string, out: ^Buf) -> bool {
	now := now_ns()
	sh := cache_shard(c, key)
	sync.mutex_lock(&sh.mu)
	e, ok := sh.entries[key]
	if !ok {
		sync.mutex_unlock(&sh.mu)
		sync.atomic_add(&c.misses, 1)
		return false
	}
	if cache_expired(e, now) {
		entry_remove(sh, e)
		sync.mutex_unlock(&sh.mu)
		sync.atomic_add(&c.expirations, 1)
		sync.atomic_add(&c.misses, 1)
		return false
	}
	lru_touch(sh, e)
	buf_write(out, e.value)
	sync.mutex_unlock(&sh.mu)
	sync.atomic_add(&c.hits, 1)
	return true
}

// cache_get returns a caller-owned copy of the value.
cache_get :: proc(c: ^Cache, key: string) -> ([]u8, bool) {
	value: []u8
	found := false
	sh := cache_shard(c, key)
	sync.mutex_lock(&sh.mu)
	e, ok := sh.entries[key]
	now := now_ns()
	if ok {
		if cache_expired(e, now) {
			entry_remove(sh, e)
			sync.atomic_add(&c.expirations, 1)
		} else {
			lru_touch(sh, e)
			value = make([]u8, len(e.value))
			copy(value, e.value)
			found = true
		}
	}
	sync.mutex_unlock(&sh.mu)
	if found {
		sync.atomic_add(&c.hits, 1)
	} else {
		sync.atomic_add(&c.misses, 1)
	}
	return value, found
}

cache_set :: proc(c: ^Cache, key: string, value: []u8, ttl_ns: i64) -> bool {
	if len(key) == 0 || len(key) > CACHE_MAX_KEY {
		return false
	}
	if c.cfg.max_value > 0 && len(value) > c.cfg.max_value {
		return false
	}
	now := now_ns()
	sh := cache_shard(c, key)
	if sh.max_bytes > 0 && i64(len(value)) > sh.max_bytes {
		return false
	}
	expires: i64
	if ttl_ns > 0 {
		expires = now + ttl_ns
		if expires < now {
			expires = i64(0x7fffffffffffffff)
		}
	}
	val := make([]u8, len(value))
	copy(val, value)

	sync.mutex_lock(&sh.mu)
	if e, ok := sh.entries[key]; ok {
		sh.bytes += i64(len(val)) - i64(len(e.value))
		if e.value != nil {
			delete(e.value)
		}
		e.value = val
		e.expires_ns = expires
		lru_touch(sh, e)
	} else {
		e := new(Cache_Entry)
		e.key = strings.clone(key)
		e.value = val
		e.expires_ns = expires
		sh.entries[e.key] = e
		sh.bytes += i64(len(val))
		lru_push_front(sh, e)
	}
	// Evict the least recently used entry; count it as an expiration when stale.
	for (sh.max_bytes > 0 && sh.bytes > sh.max_bytes) ||
	    (sh.max_entries > 0 && len(sh.entries) > sh.max_entries) {
		victim := sh.tail
		if victim == nil {
			break
		}
		if cache_expired(victim, now) {
			sync.atomic_add(&c.expirations, 1)
		} else {
			sync.atomic_add(&c.evictions, 1)
		}
		entry_remove(sh, victim)
	}
	sync.mutex_unlock(&sh.mu)
	sync.atomic_add(&c.sets, 1)
	return true
}

cache_delete :: proc(c: ^Cache, key: string) -> bool {
	sh := cache_shard(c, key)
	sync.mutex_lock(&sh.mu)
	e, ok := sh.entries[key]
	if ok {
		entry_remove(sh, e)
	}
	sync.mutex_unlock(&sh.mu)
	if ok {
		sync.atomic_add(&c.deletes, 1)
	}
	return ok
}

cache_stats :: proc(c: ^Cache) -> Cache_Stats {
	st := Cache_Stats{
		hits        = sync.atomic_load(&c.hits),
		misses      = sync.atomic_load(&c.misses),
		evictions   = sync.atomic_load(&c.evictions),
		expirations = sync.atomic_load(&c.expirations),
		sets        = sync.atomic_load(&c.sets),
		deletes     = sync.atomic_load(&c.deletes),
	}
	for i in 0..<CACHE_SHARDS {
		sh := &c.shards[i]
		sync.mutex_lock(&sh.mu)
		st.entries += len(sh.entries)
		st.bytes += sh.bytes
		sync.mutex_unlock(&sh.mu)
	}
	return st
}
