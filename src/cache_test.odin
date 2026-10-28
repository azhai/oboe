package oboe

import "core:fmt"
import "core:os"
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

// cache_keys_in_bucket collects keys that hash to the same bucket.
cache_keys_in_bucket :: proc(c: ^Cache, prefix: string, want: int) -> [dynamic]string {
	out: [dynamic]string
	target := -1
	for i := 0; i < 100000 && len(out) < want; i += 1 {
		k := fmt.aprintf("%s-%d", prefix, i)
		idx := int(cache_hash(k) & u64(len(c.buckets) - 1))
		if target < 0 {
			target = idx
		}
		if idx != target {
			delete(k)
			continue
		}
		append(&out, k)
	}
	return out
}

@(test)
test_cache_eviction :: proc(t: ^testing.T) {
	cfg := default_cache_config()
	cfg.max_bytes = 0
	cfg.max_entries = 32 // 16 buckets with 2 entries each
	c := cache_init(cfg)
	defer cache_destroy(c)

	keys := cache_keys_in_bucket(c, "evict", 3)
	defer {
		for k in keys {
			delete(k)
		}
		delete(keys)
	}
	testing.expect_value(t, len(keys), 3)
	if len(keys) < 3 {
		return
	}

	value := transmute([]u8)string("v")
	for k in keys {
		testing.expect(t, cache_set(c, k, value, 0))
	}
	_, ok0 := cache_get(c, keys[0])
	testing.expect(t, !ok0)
	v1, ok1 := cache_get(c, keys[1])
	testing.expect(t, !ok1)
	if ok1 {
		delete(v1)
	}
	v2, ok2 := cache_get(c, keys[2])
	testing.expect(t, ok2)
	if ok2 {
		testing.expect_value(t, string(v2), "v")
		delete(v2)
	}

	st := cache_stats(c)
	testing.expect_value(t, st.entries, 1)
	testing.expect_value(t, st.evictions, u64(2))
	testing.expect_value(t, st.bytes, i64(1))
}

@(test)
test_cache_entry_cap :: proc(t: ^testing.T) {
	cfg := default_cache_config()
	cfg.max_bytes = 0
	cfg.max_entries = 64
	c := cache_init(cfg)
	defer cache_destroy(c)

	value := transmute([]u8)string("payload")
	for i in 0..<2000 {
		k := fmt.aprintf("cap-%d", i)
		testing.expect(t, cache_set(c, k, value, 0))
		delete(k)
	}
	st := cache_stats(c)
	testing.expect(t, st.entries <= cfg.max_entries)
	testing.expect(t, st.entries > 0)
	testing.expect(t, st.evictions > 0)
}

@(test)
test_cache_big_values :: proc(t: ^testing.T) {
	cfg := default_cache_config()
	cfg.max_bytes = 8 << 20
	cfg.max_entries = 64
	c := cache_init(cfg)
	defer cache_destroy(c)

	big := make([]u8, 100<<10)
	defer delete(big)
	for i in 0..<len(big) {
		big[i] = u8('a' + i % 26)
	}
	testing.expect(t, cache_set(c, "big", big, 0))
	got, ok := cache_get(c, "big")
	testing.expect(t, ok)
	testing.expect_value(t, len(got), len(big))
	testing.expect(t, string(got) == string(big))
	delete(got)

	st := cache_stats(c)
	testing.expect_value(t, st.entries, 1)
	testing.expect_value(t, st.bytes, i64(len(big)))

	testing.expect(t, cache_delete(c, "big"))
	testing.expect_value(t, cache_stats(c).entries, 0)
}

@(test)
test_cache_oversized_big_value :: proc(t: ^testing.T) {
	cfg := default_cache_config()
	cfg.max_bytes = 8 << 20 // about 16 KiB per bucket
	cfg.max_entries = 0
	c := cache_init(cfg)
	defer cache_destroy(c)

	keys := cache_keys_in_bucket(c, "huge", 2)
	defer {
		for k in keys {
			delete(k)
		}
		delete(keys)
	}
	testing.expect_value(t, len(keys), 2)
	if len(keys) < 2 {
		return
	}

	// Larger than the per-bucket big-value budget: it must still be stored.
	value := make([]u8, 1 << 20)
	defer delete(value)
	for i in 0..<len(value) {
		value[i] = u8('a' + i % 26)
	}
	testing.expect(t, cache_set(c, keys[0], value, 0))
	got, ok := cache_get(c, keys[0])
	testing.expect(t, ok)
	testing.expect_value(t, len(got), len(value))
	if ok {
		delete(got)
	}

	// A newer big value in the same bucket evicts the older one.
	testing.expect(t, cache_set(c, keys[1], value, 0))
	_, first := cache_get(c, keys[0])
	testing.expect(t, !first)
	second, sok := cache_get(c, keys[1])
	testing.expect(t, sok)
	if sok {
		testing.expect_value(t, len(second), len(value))
		delete(second)
	}

	st := cache_stats(c)
	testing.expect_value(t, st.entries, 1)
	testing.expect_value(t, st.bytes, i64(len(value)))
}

@(test)
test_cache_save_load :: proc(t: ^testing.T) {
	dir := "/tmp/oboe_cache_file"
	os.remove_all(dir)
	defer os.remove_all(dir)

	cfg := default_cache_config()
	cfg.max_bytes = 64 << 20
	cfg.max_entries = 0
	path := fmt.aprintf("%s/cache.bin", dir)
	defer delete(path)
	os.make_directory_all(dir)

	c := cache_init(cfg)
	value := transmute([]u8)string("persisted")
	for i in 0..<20 {
		k := fmt.aprintf("p-%d", i)
		testing.expect(t, cache_set(c, k, value, 0))
		delete(k)
	}
	testing.expect(t, cache_set(c, "with-ttl", transmute([]u8)string("alive"), i64(10*time.Second)))

	big := make([]u8, 100<<10)
	defer delete(big)
	for i in 0..<len(big) {
		big[i] = u8('A' + i % 26)
	}
	testing.expect(t, cache_set(c, "big", big, 0))

	before := cache_stats(c)
	bytes, sok := cache_save_file(c, path)
	testing.expect(t, sok)
	testing.expect(t, bytes > 0)

	loaded, lok := cache_load_file(cfg, path)
	testing.expect(t, lok)
	defer {
		if loaded != nil {
			cache_destroy(loaded)
		}
	}
	if !lok {
		cache_destroy(c)
		return
	}
	after := cache_stats(loaded)
	testing.expect_value(t, after.entries, before.entries)
	testing.expect_value(t, after.bytes, before.bytes)

	got, gok := cache_get(loaded, "p-7")
	testing.expect(t, gok)
	if gok {
		testing.expect_value(t, string(got), "persisted")
		delete(got)
	}
	if ttl_value, tok := cache_get(loaded, "with-ttl"); tok {
		testing.expect_value(t, string(ttl_value), "alive")
		delete(ttl_value)
	} else {
		testing.expect(t, false)
	}
	big_got, bok := cache_get(loaded, "big")
	testing.expect(t, bok)
	if bok {
		testing.expect_value(t, len(big_got), len(big))
		delete(big_got)
	}

	// The loaded cache keeps working normally.
	testing.expect(t, cache_set(loaded, "fresh", transmute([]u8)string("ok"), 0))
	if fresh, fok := cache_get(loaded, "fresh"); fok {
		testing.expect_value(t, string(fresh), "ok")
		delete(fresh)
	} else {
		testing.expect(t, false)
	}

	// A different geometry must not load the file.
	other_cfg := cfg
	other_cfg.max_bytes = 128 << 20
	_, bad := cache_load_file(other_cfg, path)
	testing.expect(t, !bad)

	cache_destroy(c)

	// Expired entries are dropped when the cache is loaded.
	c2 := cache_init(cfg)
	defer cache_destroy(c2)
	testing.expect(t, cache_set(c2, "gone", transmute([]u8)string("x"), i64(10*time.Millisecond)))
	time.sleep(30 * time.Millisecond)
	testing.expect(t, cache_set(c2, "kept", transmute([]u8)string("y"), 0))
	_, save_ok := cache_save_file(c2, path)
	testing.expect(t, save_ok)
	c3, c3ok := cache_load_file(cfg, path)
	testing.expect(t, c3ok)
	if c3ok {
		defer cache_destroy(c3)
		_, gone := cache_get(c3, "gone")
		testing.expect(t, !gone)
		if kept, kok := cache_get(c3, "kept"); kok {
			testing.expect_value(t, string(kept), "y")
			delete(kept)
		} else {
			testing.expect(t, false)
		}
	}

	// Missing and corrupt files are rejected.
	missing := fmt.aprintf("%s/missing.bin", dir)
	defer delete(missing)
	c4, mok := cache_load_file(cfg, missing)
	testing.expect(t, !mok)
	testing.expect(t, c4 == nil)

	corrupt := fmt.aprintf("%s/corrupt.bin", dir)
	defer delete(corrupt)
	testing.expect(t, os.write_entire_file(corrupt, []u8{'O', 'D', 'C', 'A', 1}) == nil)
	c5, cok := cache_load_file(cfg, corrupt)
	testing.expect(t, !cok)
	testing.expect(t, c5 == nil)

	// Saving without a path is a no-op failure.
	_, nok := cache_save_file(c2, "")
	testing.expect(t, !nok)
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
