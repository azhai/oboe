// Cache benchmark: PUT/GET hit/GET miss with small and large values. The large
// value defaults to 8 MiB, half of the default 16 MiB per-value limit, and the
// cache is sized so the measured ring of keys stays resident.
package main

import "core:fmt"
import "core:sync"
import "core:thread"
import "core:time"

import "../common"
import cachelib "../cache"

SMALL_KEYS :: 4096
LARGE_KEYS :: 8

Cache_Mode :: enum {
	Put_Small,
	Put_Large,
	Get_Hit_Small,
	Get_Hit_Large,
	Get_Miss_Small,
	Get_Miss_Large,
}

Cache_Def :: struct {
	mode: Cache_Mode,
	name: string,
}

CACHE_MODES := [?]Cache_Def{
	{.Put_Small, "put_small"},
	{.Put_Large, "put_large"},
	{.Get_Hit_Small, "get_hit_small"},
	{.Get_Hit_Large, "get_hit_large"},
	{.Get_Miss_Small, "get_miss_small"},
	{.Get_Miss_Large, "get_miss_large"},
}

Cache_Args :: struct {
	c:       ^cachelib.Cache,
	start:   ^u32,
	mode:    Cache_Mode,
	keys:    []string,
	mvalue:  []u8,
	ops:     int,
	idx:     int,
	warm:    int,
	samples: ^Samples,
	hits:    ^u64,
	misses:  ^u64,
	errors:  ^u64,
}

cache_worker :: proc(data: rawptr) {
	a := cast(^Cache_Args)data
	buf := common.buf_init(256)
	defer common.buf_destroy(&buf)

	for _ in 0..<a.warm {
		cache_op(a, &buf, false)
	}
	spin_until(a.start)
	for _ in 0..<a.ops {
		cache_op(a, &buf, true)
	}
}

cache_op :: proc(a: ^Cache_Args, buf: ^common.Buf, measure: bool) {
	ki := a.idx % len(a.keys)
	key := a.keys[ki]
	t0 := time.tick_now()
	switch a.mode {
	case .Put_Small:
		if !cachelib.cache_set(a.c, key, a.mvalue, 0) {
			if measure { sync.atomic_add(a.errors, 1) }
		}
	case .Put_Large:
		if !cachelib.cache_set(a.c, key, a.mvalue, 0) {
			if measure { sync.atomic_add(a.errors, 1) }
		}
	case .Get_Hit_Small, .Get_Hit_Large:
		common.buf_reset(buf)
		if cachelib.cache_get_into(a.c, key, buf) {
			if measure { sync.atomic_add(a.hits, 1) }
		} else {
			if measure { sync.atomic_add(a.misses, 1) }
		}
	case .Get_Miss_Small, .Get_Miss_Large:
		common.buf_reset(buf)
		if cachelib.cache_get_into(a.c, key, buf) {
			if measure { sync.atomic_add(a.hits, 1) }
		} else {
			if measure { sync.atomic_add(a.misses, 1) }
		}
	}
	if measure {
		samples_add(a.samples, f64(time.tick_since(t0)) / 1e6)
	}
	a.idx += 1
	if a.idx >= len(a.keys) {
		a.idx = 0
	}
}

run_cache :: proc(p: Params) {
	cfg := cachelib.default_cache_config()
	cfg.max_bytes = 512 << 20
	cfg.max_entries = 1_000_000
	cfg.max_value = 16 << 20
	c := cachelib.cache_init(cfg)
	defer cachelib.cache_destroy(c)

	large_size := 8 << 20
	small_value := make([]u8, p.value_size)
	defer delete(small_value)
	for i in 0..<len(small_value) {
		small_value[i] = u8('a' + i % 26)
	}
	large_value := make([]u8, large_size)
	defer delete(large_value)
	for i in 0..<len(large_value) {
		large_value[i] = u8('a' + i % 26)
	}

	small_keys := make([]string, SMALL_KEYS)
	defer {
		for k in small_keys {
			delete(k)
		}
		delete(small_keys)
	}
	for i in 0..<SMALL_KEYS {
		small_keys[i] = fmt.aprintf("bench:key:%d", i)
	}
	large_keys := make([]string, LARGE_KEYS)
	defer {
		for k in large_keys {
			delete(k)
		}
		delete(large_keys)
	}
	for i in 0..<LARGE_KEYS {
		large_keys[i] = fmt.aprintf("bench:large:%d", i)
	}
	miss_keys := make([]string, SMALL_KEYS)
	defer {
		for k in miss_keys {
			delete(k)
		}
		delete(miss_keys)
	}
	for i in 0..<SMALL_KEYS {
		miss_keys[i] = fmt.aprintf("bench:absent:%d", i)
	}
	miss_large_keys := make([]string, LARGE_KEYS)
	defer {
		for k in miss_large_keys {
			delete(k)
		}
		delete(miss_large_keys)
	}
	for i in 0..<LARGE_KEYS {
		miss_large_keys[i] = fmt.aprintf("bench:absent:large:%d", i)
	}

	if !p.json {
		fmt.printf("[cache] threads=%d small=%dB large=%dMiB ops=%d ops_large=%d\n",
			p.threads, p.value_size, large_size >> 20, p.ops, p.ops_large)
	}

	for def in CACHE_MODES {
		// Pre-fill only the hit scenarios. Put scenarios also start from a
		// resident ring so the workload is a steady overwrite, not a cold fill.
		keys := small_keys
		value := small_value
		ops := p.ops
		switch def.mode {
		case .Put_Small, .Get_Hit_Small:
			keys = small_keys
			value = small_value
		case .Get_Miss_Small:
			keys = miss_keys
			value = small_value
		case .Put_Large, .Get_Hit_Large:
			keys = large_keys
			value = large_value
			ops = p.ops_large
		case .Get_Miss_Large:
			keys = miss_large_keys
			value = large_value
			ops = p.ops_large
		}
		if def.mode != .Get_Miss_Small && def.mode != .Get_Miss_Large {
			for k in keys {
				cachelib.cache_set(c, k, value, 0)
			}
		}

		start: u32
		samples := make([]Samples, p.threads)
		args := make([]Cache_Args, p.threads)
		workers := make([]^thread.Thread, p.threads)
		hits, misses, errors: u64
		for i in 0..<p.threads {
			samples[i] = samples_init()
			args[i] = Cache_Args{
				c       = c,
				start   = &start,
				mode    = def.mode,
				keys    = keys,
				mvalue  = value,
				ops     = ops,
				idx     = i,
				warm    = p.warm,
				samples = &samples[i],
				hits    = &hits,
				misses  = &misses,
				errors  = &errors,
			}
			workers[i] = thread.create_and_start_with_data(&args[i], cache_worker, self_cleanup = false)
		}
		begin := time.now()
		sync.atomic_store(&start, 1)
		wait_workers(workers)
		d := time.since(begin)

		merged := samples_init()
		for i in 0..<p.threads {
			samples_merge(&merged, &samples[i])
			samples_destroy(&samples[i])
		}
		samples_finish(&merged)
		total := p.threads * ops
		op_s := throughput(total, d)
		// Bytes only move on puts and hits; a miss is an index lookup.
		transfers := true
		if def.mode == .Get_Miss_Small || def.mode == .Get_Miss_Large {
			transfers = false
		}
		mb_s := f64(0)
		if transfers {
			mb_s = op_s * f64(len(value)) / (1 << 20)
		}
		if p.json {
			fmt.printf(
				`{{"scenario":"cache","class":"%s","threads":%d,"ops":%d,"value_bytes":%d,"ops_per_s":%.0f,"mib_per_s":%.1f,"p50_ms":%.4f,"p95_ms":%.4f,"p99_ms":%.4f,"max_ms":%.4f,"hits":%d,"misses":%d,"errors":%d}}`,
				def.name, p.threads, total, len(value), op_s, mb_s,
				samples_pct(&merged, 0.50), samples_pct(&merged, 0.95), samples_pct(&merged, 0.99),
				samples_max(&merged), hits, misses, errors,
			)
			fmt.println()
		} else {
			fmt.printf("  %-14s threads=%-2d ops=%7d value=%-6d ops/s=%10.0f %8.1f MiB/s p50=%8.4fms p95=%8.4fms p99=%8.4fms hits=%d misses=%d errors=%d\n",
				def.name, p.threads, total, len(value), op_s, mb_s,
				samples_pct(&merged, 0.50), samples_pct(&merged, 0.95), samples_pct(&merged, 0.99),
				hits, misses, errors)
		}
		samples_destroy(&merged)
		delete(samples)
		delete(args)
		delete(workers)
	}
}
