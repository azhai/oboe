package oboe

import "core:fmt"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"

@(test)
test_cache_basic :: proc(t: ^testing.T) {
	cfg := default_cache_config()
	cfg.max_bytes = 1 << 20
	cfg.max_entries = 1024
	c := cache_init(cfg)
	defer cache_destroy(c)

	value := transmute([]u8)string("hello cache")
	testing.expect(t, cache_set(c, "alpha", value, 0))

	got, ok := cache_get(c, "alpha")
	testing.expect(t, ok)
	testing.expect_value(t, string(got), "hello cache")
	delete(got)

	out := buf_init(32)
	defer buf_destroy(&out)
	testing.expect(t, cache_get_into(c, "alpha", &out))
	testing.expect_value(t, string(out.data[:]), "hello cache")

	testing.expect(t, cache_delete(c, "alpha"))
	testing.expect(t, !cache_delete(c, "alpha"))

	_, ok = cache_get(c, "alpha")
	testing.expect(t, !ok)

	st := cache_stats(c)
	testing.expect_value(t, st.entries, 0)
	testing.expect_value(t, st.sets, u64(1))
	testing.expect_value(t, st.deletes, u64(1))
	testing.expect_value(t, st.hits, u64(2))
	testing.expect_value(t, st.misses, u64(1))
}

@(test)
test_cache_ttl :: proc(t: ^testing.T) {
	c := cache_init(default_cache_config())
	defer cache_destroy(c)

	value := transmute([]u8)string("short lived")
	testing.expect(t, cache_set(c, "ttl-key", value, i64(10*time.Millisecond)))
	if v, ok := cache_get(c, "ttl-key"); ok {
		testing.expect_value(t, string(v), "short lived")
		delete(v)
	} else {
		testing.expect(t, false)
	}

	time.sleep(30 * time.Millisecond)
	_, ok := cache_get(c, "ttl-key")
	testing.expect(t, !ok)

	st := cache_stats(c)
	testing.expect_value(t, st.entries, 0)
	testing.expect_value(t, st.expirations, u64(1))
	testing.expect_value(t, st.misses, u64(1))
}

@(test)
test_cache_limits :: proc(t: ^testing.T) {
	cfg := default_cache_config()
	cfg.max_value = 4
	c := cache_init(cfg)
	defer cache_destroy(c)

	testing.expect(t, !cache_set(c, "big", transmute([]u8)string("12345"), 0))
	testing.expect(t, cache_set(c, "fit", transmute([]u8)string("1234"), 0))
	testing.expect(t, !cache_set(c, "", transmute([]u8)string("x"), 0))

	long_key := make([]u8, CACHE_MAX_KEY + 1)
	defer delete(long_key)
	testing.expect(t, !cache_set(c, string(long_key), transmute([]u8)string("x"), 0))
}

@(test)
test_cache_lru_eviction :: proc(t: ^testing.T) {
	cfg := default_cache_config()
	cfg.max_bytes = 0
	cfg.max_entries = 32 // two entries per shard
	c := cache_init(cfg)
	defer cache_destroy(c)

	keys: [3]string
	found := 0
	for i := 0; i < 100000 && found < 3; i += 1 {
		k := fmt.aprintf("key-%d", i)
		if found == 0 || cache_shard(c, k) == cache_shard(c, keys[0]) {
			keys[found] = k
			found += 1
		} else {
			delete(k)
		}
	}
	testing.expect_value(t, found, 3)
	if found < 3 {
		for i in 0..<found {
			delete(keys[i])
		}
		return
	}
	defer {
		for i in 0..<3 {
			delete(keys[i])
		}
	}

	value := transmute([]u8)string("v")
	for k in keys {
		testing.expect(t, cache_set(c, k, value, 0))
	}
	_, ok := cache_get(c, keys[0])
	testing.expect(t, !ok)
	v1, ok1 := cache_get(c, keys[1])
	testing.expect(t, ok1)
	delete(v1)
	v2, ok2 := cache_get(c, keys[2])
	testing.expect(t, ok2)
	delete(v2)

	st := cache_stats(c)
	testing.expect_value(t, st.entries, 2)
	testing.expect_value(t, st.evictions, u64(1))
	testing.expect_value(t, st.bytes, i64(2))
}

Cache_Thread_Args :: struct {
	c:    ^Cache,
	base: int,
	fail: ^u32,
}

cache_thread_work :: proc(data: rawptr) {
	a := cast(^Cache_Thread_Args)data
	value := transmute([]u8)string("shared payload")
	for i in 0..<5000 {
		k := fmt.aprintf("k-%d-%d", a.base, i % 64)
		if !cache_set(a.c, k, value, 0) {
			sync.atomic_add(a.fail, 1)
		}
		if v, ok := cache_get(a.c, k); ok {
			delete(v)
		} else {
			sync.atomic_add(a.fail, 1)
		}
		if i % 7 == 0 {
			cache_delete(a.c, k)
		}
		delete(k)
		free_all(context.temp_allocator)
	}
}

@(test)
test_cache_concurrent :: proc(t: ^testing.T) {
	cfg := default_cache_config()
	cfg.max_bytes = 0
	cfg.max_entries = 0
	c := cache_init(cfg)
	defer cache_destroy(c)

	fail: u32
	args: [4]Cache_Thread_Args
	workers: [4]^thread.Thread
	for i in 0..<4 {
		args[i] = Cache_Thread_Args{c = c, base = i, fail = &fail}
		workers[i] = thread.create_and_start_with_data(&args[i], cache_thread_work, self_cleanup = false)
	}
	for w in workers {
		thread.join(w)
		thread.destroy(w)
	}
	testing.expect_value(t, fail, u32(0))
	testing.expect_value(t, cache_stats(c).sets, u64(4 * 5000))
}
