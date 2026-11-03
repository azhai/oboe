// LogsQL query benchmark: four query shapes chosen to stress different parts
// of the execution path. Each class runs with `-threads` workers and `-reps`
// measured calls per worker after `-warm` unmeasured calls.
package main

import "core:fmt"
import "core:sync"
import "core:thread"
import "core:time"

import "../common"
import "../logdb"

Query_Def :: struct {
	name: string,
	qstr: string,
}

QUERY_DEFS :: [?]Query_Def{
	// filter: matches most blocks and returns a bounded stream.
	{"filter", "level:error | limit 500"},
	// stats: full scan with a small group-by; CPU bound.
	{"stats", "level:error | stats by (level) count() as total"},
	// sort: top-N heap over the whole data set.
	{"sort", "_time:10m | sort by (_time) desc | limit 100"},
	// narrow: time-window pruning plus a high-cardinality group.
	{"narrow", "_time:2m | stats by (host) count() as total"},
}

Query_Args :: struct {
	s:       ^logdb.Store,
	start:   ^u32,
	q:       logdb.Query,
	reps:    int,
	warm:    int,
	samples: ^Samples,
	scanned: ^u64,
	matched: ^u64,
	out_rows: ^u64,
	pruned:  ^u64,
	errors:  ^u32,
}

query_worker :: proc(data: rawptr) {
	a := cast(^Query_Args)data
	for _ in 0..<a.warm {
		r := logdb.execute_query(a.s, &a.q, 1000)
		logdb.result_destroy(&r)
	}
	spin_until(a.start)
	for _ in 0..<a.reps {
		t0 := time.tick_now()
		r := logdb.execute_query(a.s, &a.q, 1000)
		dt := f64(time.tick_since(t0)) / 1e6
		samples_add(a.samples, dt)
		sync.atomic_add(a.scanned, u64(r.scanned_rows))
		sync.atomic_add(a.matched, u64(r.matched_rows))
		sync.atomic_add(a.out_rows, u64(len(r.rows)))
		sync.atomic_add(a.pruned, u64(r.pruned_blocks))
		logdb.result_destroy(&r)
	}
}

run_query :: proc(p: Params, s: ^logdb.Store) {
	now := common.now_ns()
	st := logdb.store_stats(s)
	if !p.json {
		fmt.printf("[query] rows=%d parts=%d threads=%d reps=%d\n", st.rows + st.mem_rows, st.parts, p.threads, p.reps)
	}
	for def in QUERY_DEFS {
		q, ok := logdb.parse_query_string(def.qstr, now)
		if !ok {
			fmt.eprintf("bench: cannot parse query %q\n", def.qstr)
			continue
		}
		start: u32
		scanned, matched, out_rows, pruned: u64
		errors: u32
		args := make([]Query_Args, p.threads)
		workers := make([]^thread.Thread, p.threads)
		samples := make([]Samples, p.threads)
		for i in 0..<p.threads {
			samples[i] = samples_init()
			args[i] = Query_Args{
				s       = s,
				start   = &start,
				q       = q,
				reps    = p.reps,
				warm    = p.warm,
				samples = &samples[i],
				scanned = &scanned,
				matched = &matched,
				out_rows = &out_rows,
				pruned  = &pruned,
				errors  = &errors,
			}
			workers[i] = thread.create_and_start_with_data(&args[i], query_worker, self_cleanup = false)
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
		total := p.threads * p.reps
		qps := throughput(total, d)
		n := u64(total)
		if p.json {
			fmt.printf(
				`{{"scenario":"query","class":"%s","threads":%d,"reps":%d,"rows":%d,"parts":%d,"qps":%.1f,"p50_ms":%.3f,"p95_ms":%.3f,"p99_ms":%.3f,"max_ms":%.3f,"scanned_rows":%d,"matched_rows":%d,"out_rows":%d,"pruned_blocks":%d,"errors":%d}}`,
				def.name, p.threads, p.reps, st.rows + st.mem_rows, st.parts,
				qps, samples_pct(&merged, 0.50), samples_pct(&merged, 0.95), samples_pct(&merged, 0.99),
				samples_max(&merged), scanned / n, matched / n, out_rows / n, pruned / n, errors,
			)
			fmt.println()
		} else {
			fmt.printf("  %-7s threads=%-2d qps=%7.1f p50=%7.2fms p95=%7.2fms p99=%7.2fms scanned=%9d matched=%8d out=%5d pruned=%6d errors=%d\n",
				def.name, p.threads, qps,
				samples_pct(&merged, 0.50), samples_pct(&merged, 0.95), samples_pct(&merged, 0.99),
				scanned / n, matched / n, out_rows / n, pruned / n, errors)
		}
		samples_destroy(&merged)
		delete(args)
		delete(workers)
		delete(samples)
		logdb.query_destroy(&q)
	}
}
