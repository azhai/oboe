// Entry point of the oboe benchmark suite. See bench/README.md and
// docs/benchmarks.md for the scenarios and the matrix runner.
package main

import "core:fmt"
import "core:os"

import "../logdb"

main :: proc() {
	p := parse_params()
	defer if !p.keep && !p.reuse && p.mode != "cache" && p.mode != "queue-tcp" {
		os.remove_all(p.data)
	}

	switch p.mode {
	case "ingest":
		run_ingest(p)
	case "query":
		s := open_store(p, p.reuse)
		defer logdb.store_close(s)
		run_query(p, s)
	case "cache":
		run_cache(p)
	case "queue-tcp":
		run_queue_tcp(p)
	case "all":
		s := open_store(p, p.reuse)
		run_query(p, s)
		logdb.store_close(s)
		run_cache(p)
		run_queue_tcp(p)
	case:
		fmt.eprintf("bench: unknown -mode %q\n", p.mode)
		print_usage()
		os.exit(2)
	}
}
