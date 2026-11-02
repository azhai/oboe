package cache

import "../common"
import "base:runtime"
import "core:fmt"
import "core:os"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"

cm_tmpdir :: proc(name: string) -> string {
	return fmt.aprintf("/tmp/oboe_%s_%d", name, os.get_pid())
}

cm_path :: proc(dir, name: string) -> string {
	return fmt.aprintf("%s/%s", dir, name)
}

cm_write :: proc(t: ^testing.T, path: string, bytes: []u8) {
	testing.expectf(t, os.write_entire_file(path, bytes) == nil, "write %s", path)
}

cm_cfg :: proc() -> Cache_Config {
	cfg := default_cache_config()
	cfg.max_bytes = 1 << 20
	cfg.max_entries = 1024
	return cfg
}

cm_populate :: proc(t: ^testing.T, c: ^Cache) {
	value := transmute([]u8)string("persisted value")
	for i in 0..<12 {
		k := fmt.aprintf("k-%d", i)
		testing.expectf(t, cache_set(c, k, value, 0), "set %s", k)
		delete(k)
	}
	// One big value so the file also exercises the large-value FIFO path.
	big := make([]u8, 100<<10)
	defer delete(big)
	for i in 0..<len(big) {
		big[i] = u8('a' + i % 26)
	}
	testing.expect(t, cache_set(c, "big", big, 0))
}

// cm_check_patch writes a copy of the source snapshot with `patch` applied and
// expects cache_load_file to refuse it.
cm_check_patch :: proc(t: ^testing.T, cfg: Cache_Config, src_path, dir, label: string, patch: proc(b: []u8)) {
	src, serr := os.read_entire_file_from_path(src_path, context.allocator)
	testing.expectf(t, serr == nil, "read %s", src_path)
	if serr != nil {
		return
	}
	defer delete(src)

	out := make([]u8, len(src))
	defer delete(out)
	copy(out, src)
	patch(out)

	p := cm_path(dir, label)
	defer delete(p)
	cm_write(t, p, out)
	c, ok := cache_load_file(cfg, p)
	testing.expectf(t, !ok, "%s should be rejected", label)
	testing.expect(t, c == nil)
}

cm_patch_bad_version :: proc(b: []u8) {
	b[4] = 2
	b[5] = 0
}

cm_patch_bad_chunk :: proc(b: []u8) {
	b[10] = 0
	b[11] = 0
	b[12] = 0
	b[13] = 0
}

cm_patch_bad_buckets :: proc(b: []u8) {
	b[6] = 0xFF
	b[7] = 0xFF
	b[8] = 0xFF
	b[9] = 0xFF
}

// The first bucket's zlib stream starts at byte 34 (18 byte header + 16 byte
// size pair). A damaged byte inside it must fail the decompression (zlib
// verifies its adler32 trailer).
cm_patch_bad_payload :: proc(b: []u8) {
	if len(b) <= 34 {
		return
	}
	comp_len := int(common.read_u64le(b, 26))
	at := 34 + comp_len / 2
	if at >= len(b) {
		at = len(b) - 1
	}
	b[at] = b[at] == 0xFF ? 0x00 : b[at] + 1
}

@(test)
test_cache_file_more_truncated_snapshots :: proc(t: ^testing.T) {
	dir := cm_tmpdir("cache_file_trunc")
	defer delete(dir)
	os.remove_all(dir)
	defer os.remove_all(dir)
	os.make_directory_all(dir)

	cfg := cm_cfg()
	c := cache_init(cfg)
	defer cache_destroy(c)
	cm_populate(t, c)

	path := cm_path(dir, "cache.bin")
	defer delete(path)
	bytes, sok := cache_save_file(c, path)
	testing.expect(t, sok)
	testing.expect(t, bytes > 0)
	if !sok {
		return
	}

	snapshot, rerr := os.read_entire_file_from_path(path, context.allocator)
	testing.expect(t, rerr == nil)
	if rerr != nil {
		return
	}
	defer delete(snapshot)
	testing.expect(t, len(snapshot) > 34)

	// Every prefix shorter than the full snapshot must be refused.
	n := len(snapshot)
	cuts := [15]int{0, 1, 4, 5, 6, 10, 14, 17, 18, 19, 26, 33, 34, n / 2, n - 1}
	for cut in cuts {
		if cut < 0 || cut >= n {
			continue
		}
		p := cm_path(dir, fmt.tprintf("trunc-%d.bin", cut))
		defer delete(p)
		cm_write(t, p, snapshot[:cut])
		lc, lok := cache_load_file(cfg, p)
		testing.expectf(t, !lok, "snapshot truncated to %d bytes should be rejected", cut)
		testing.expect(t, lc == nil)
	}

	// A leftover half-written .tmp must never be read as the snapshot.
	tmp := cm_path(dir, "cache.bin.tmp")
	defer delete(tmp)
	cm_write(t, tmp, []u8{'O', 'D', 'C', 'A', 1})
	lc2, lok2 := cache_load_file(cfg, path)
	testing.expect(t, lok2)
	if lok2 {
		testing.expect_value(t, cache_stats(lc2).entries, 13)
		cache_destroy(lc2)
	}
	// Saving again replaces the stale tmp atomically and removes it.
	_, sok2 := cache_save_file(c, path)
	testing.expect(t, sok2)
	testing.expect(t, !os.exists(tmp))

	// A snapshot that only exists as a .tmp is not loadable.
	only_tmp := cm_path(dir, "only.bin.tmp")
	defer delete(only_tmp)
	cm_write(t, only_tmp, snapshot)
	only := cm_path(dir, "only.bin")
	defer delete(only)
	lc3, lok3 := cache_load_file(cfg, only)
	testing.expect(t, !lok3)
	testing.expect(t, lc3 == nil)

	// Header and payload corruption are refused.
	cm_check_patch(t, cfg, path, dir, "bad-version.bin", cm_patch_bad_version)
	cm_check_patch(t, cfg, path, dir, "bad-chunk.bin", cm_patch_bad_chunk)
	cm_check_patch(t, cfg, path, dir, "bad-buckets.bin", cm_patch_bad_buckets)
	cm_check_patch(t, cfg, path, dir, "bad-payload.bin", cm_patch_bad_payload)
}

@(test)
test_cache_file_more_geometry_mismatch :: proc(t: ^testing.T) {
	dir := cm_tmpdir("cache_file_geom")
	defer delete(dir)
	os.remove_all(dir)
	defer os.remove_all(dir)
	os.make_directory_all(dir)

	base_cfg := cm_cfg()
	c := cache_init(base_cfg)
	defer cache_destroy(c)
	cm_populate(t, c)

	path := cm_path(dir, "cache.bin")
	defer delete(path)
	_, sok := cache_save_file(c, path)
	testing.expect(t, sok)
	if !sok {
		return
	}

	// Changing bucket_count (via max_entries) must be rejected.
	other_entries := base_cfg
	other_entries.max_entries = 64
	lc, lok := cache_load_file(other_entries, path)
	testing.expect(t, !lok)
	testing.expect(t, lc == nil)

	// Changing max_chunks (via max_bytes) must be rejected.
	other_bytes := base_cfg
	other_bytes.max_bytes = 64 << 20
	lc2, lok2 := cache_load_file(other_bytes, path)
	testing.expect(t, !lok2)
	testing.expect(t, lc2 == nil)

	// max_value is not part of the file geometry: loading still succeeds.
	other_value := base_cfg
	other_value.max_value = 1 << 10
	lc3, lok3 := cache_load_file(other_value, path)
	testing.expect(t, lok3)
	if lok3 {
		testing.expect_value(t, cache_stats(lc3).entries, 13)
		v, gok := cache_get(lc3, "k-3")
		testing.expect(t, gok)
		if gok {
			testing.expect_value(t, string(v), "persisted value")
			delete(v)
		}
		cache_destroy(lc3)
	}
}

Cm_Concurrent_Args :: struct {
	c:    ^Cache,
	cfg:  Cache_Config,
	path: string,
	fail: ^u32,
}

cm_concurrent_save :: proc(data: rawptr) {
	a := cast(^Cm_Concurrent_Args)data
	context.allocator = runtime.default_allocator()
	for _ in 0..<20 {
		if !cache_set(a.c, "hot", transmute([]u8)string("hot value"), 0) {
			sync.atomic_add(a.fail, 1)
		}
		if _, ok := cache_save_file(a.c, a.path); !ok {
			sync.atomic_add(a.fail, 1)
		}
	}
}

cm_concurrent_load :: proc(data: rawptr) {
	a := cast(^Cm_Concurrent_Args)data
	context.allocator = runtime.default_allocator()
	for _ in 0..<20 {
		l, ok := cache_load_file(a.cfg, a.path)
		if !ok {
			sync.atomic_add(a.fail, 1)
			continue
		}
		if v, gok := cache_get(l, "baseline"); gok {
			delete(v)
		} else {
			sync.atomic_add(a.fail, 1)
		}
		cache_destroy(l)
	}
}

@(test)
test_cache_file_more_concurrent_save_load :: proc(t: ^testing.T) {
	dir := cm_tmpdir("cache_file_concurrent")
	defer delete(dir)
	os.remove_all(dir)
	defer os.remove_all(dir)
	os.make_directory_all(dir)

	cfg := cm_cfg()
	c := cache_init(cfg)
	defer cache_destroy(c)

	path := cm_path(dir, "cache.bin")
	defer delete(path)

	testing.expect(t, cache_set(c, "baseline", transmute([]u8)string("baseline value"), 0))
	_, sok := cache_save_file(c, path)
	testing.expect(t, sok)
	if !sok {
		return
	}

	fail: u32
	WORKERS :: 4
	args: [WORKERS]Cm_Concurrent_Args
	workers: [WORKERS]^thread.Thread
	for i in 0..<WORKERS {
		args[i] = Cm_Concurrent_Args{c = c, cfg = cfg, path = path, fail = &fail}
		work: proc(rawptr)
		if i < 2 {
			work = cm_concurrent_save
		} else {
			work = cm_concurrent_load
		}
		workers[i] = thread.create_and_start_with_data(&args[i], work, self_cleanup = false)
	}
	for w in workers {
		thread.join(w)
		thread.destroy(w)
	}
	testing.expect_value(t, fail, u32(0))

	// The final snapshot is still loadable and contains the baseline.
	lc, lok := cache_load_file(cfg, path)
	testing.expect(t, lok)
	if lok {
		if v, gok := cache_get(lc, "baseline"); gok {
			testing.expect_value(t, string(v), "baseline value")
			delete(v)
		} else {
			testing.expect(t, false)
		}
		cache_destroy(lc)
	}
}

@(test)
test_cache_file_more_expired_filter :: proc(t: ^testing.T) {
	dir := cm_tmpdir("cache_file_expired")
	defer delete(dir)
	os.remove_all(dir)
	defer os.remove_all(dir)
	os.make_directory_all(dir)

	cfg := default_cache_config()
	cfg.max_bytes = 8 << 20
	cfg.max_entries = 0
	path := cm_path(dir, "cache.bin")
	defer delete(path)

	c := cache_init(cfg)
	cache_set(c, "small-live", transmute([]u8)string("small"), 0)
	cache_set(c, "small-exp", transmute([]u8)string("small expired"), i64(10*time.Millisecond))
	big := make([]u8, 100<<10)
	defer delete(big)
	for i in 0..<len(big) {
		big[i] = u8('A' + i % 26)
	}
	cache_set(c, "big-live", big, 0)
	cache_set(c, "big-exp", big, i64(10*time.Millisecond))
	time.sleep(30 * time.Millisecond)

	_, sok := cache_save_file(c, path)
	testing.expect(t, sok)
	cache_destroy(c)
	if !sok {
		return
	}

	lc, lok := cache_load_file(cfg, path)
	testing.expect(t, lok)
	if !lok {
		return
	}
	defer cache_destroy(lc)
	testing.expect_value(t, cache_stats(lc).entries, 2)

	if v, ok := cache_get(lc, "small-live"); ok {
		testing.expect_value(t, string(v), "small")
		delete(v)
	} else {
		testing.expect(t, false)
	}
	if v, ok := cache_get(lc, "big-live"); ok {
		testing.expect_value(t, len(v), len(big))
		testing.expect_value(t, string(v[:16]), string(big[:16]))
		delete(v)
	} else {
		testing.expect(t, false)
	}
	for gone in ([]string{"small-exp", "big-exp"}) {
		_, ok := cache_get(lc, gone)
		testing.expectf(t, !ok, "expired entry %s should not survive a load", gone)
	}
}
