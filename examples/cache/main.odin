// Cache TTL, get_into, and an optional file snapshot. The cache is memory-only
// until cache_save_file is called explicitly.
//
// Run with:
//
//	odin run examples/cache
package main

import "core:fmt"
import "core:os"
import "core:time"

import "../../common"
import cachelib "../../cache"

DATA_DIR :: "/tmp/oboe_example_cache"
SNAPSHOT :: "/tmp/oboe_example_cache/cache.bin"

main :: proc() {
	os.remove_all(DATA_DIR)
	defer os.remove_all(DATA_DIR)
	os.make_directory_all(DATA_DIR)

	cfg := cachelib.default_cache_config()
	cfg.max_bytes = 8 << 20
	cfg.max_entries = 1000
	cfg.max_value = 1 << 20

	c := cachelib.cache_init(cfg)
	defer cachelib.cache_destroy(c)

	// No TTL: the value stays until it is overwritten, deleted or evicted.
	cachelib.cache_set(c, "greeting", transmute([]u8)string("hello"), 0)

	out := common.buf_init(64)
	defer common.buf_destroy(&out)
	ok := cachelib.cache_get_into(c, "greeting", &out)
	fmt.printfln("cache: greeting hit=%v value=%s", ok, out.data[:])

	// TTL is a duration in nanoseconds; expired entries are removed lazily.
	cachelib.cache_set(c, "session", transmute([]u8)string("token"), i64(30 * time.Millisecond))
	time.sleep(60 * time.Millisecond)
	common.buf_reset(&out)
	if cachelib.cache_get_into(c, "session", &out) {
		fmt.printfln("cache: session still present (unexpected)")
	} else {
		fmt.printfln("cache: session expired")
	}

	st := cachelib.cache_stats(c)
	fmt.printfln(
		"cache: entries=%d bytes=%d hits=%d misses=%d expirations=%d",
		st.entries,
		st.bytes,
		st.hits,
		st.misses,
		st.expirations,
	)

	// Snapshots are optional and need an existing directory.
	if bytes, ok := cachelib.cache_save_file(c, SNAPSHOT); ok {
		fmt.printfln("cache: saved %d bytes to %s", bytes, SNAPSHOT)
		if loaded, lok := cachelib.cache_load_file(cfg, SNAPSHOT); lok {
			defer cachelib.cache_destroy(loaded)
			common.buf_reset(&out)
			if cachelib.cache_get_into(loaded, "greeting", &out) {
				fmt.printfln("cache: reloaded greeting=%s", out.data[:])
			}
			lst := cachelib.cache_stats(loaded)
			fmt.printfln("cache: reloaded entries=%d bytes=%d", lst.entries, lst.bytes)
		} else {
			fmt.eprintln("cache: snapshot could not be loaded")
		}
	} else {
		fmt.eprintln("cache: snapshot save failed")
	}

	if cachelib.cache_delete(c, "greeting") {
		fmt.printfln("cache: greeting deleted")
	}
}
