// Flush profile: the in-pipeline breakdown of a memtable flush and of a
// multi-part merge. Unlike the per-phase microbenchmarks, these numbers come
// from the real `store_flush_partition` / `store_merge_partition` path, so lock
// waits and block sizes are measured where they actually happen.
//
// The profiler lives in logdb and is off unless this mode enables it. Both
// tiers use the same corpus, block size and ingest parameters as the rest of
// the suite, so the results line up with docs/benchmarks.md.
package main

import "core:fmt"
import "core:os"
import "core:slice"
import "core:time"

import "../common"
import "../logdb"

FLUSH_PROFILE_TIERS :: [2]int{200_000, 5_000_000}
MERGE_CHUNKS :: 8

Profile_Run :: struct {
	tier:      string,
	rows:      int,
	wall_ms:   f64,
	ingest_ms: f64,
	drain_ms:  f64,
	prof:      logdb.Flush_Profile,
}

run_flush_profile :: proc(p: Params) {
	for rows in FLUSH_PROFILE_TIERS {
		tier := rows == 200_000 ? "small" : "big"
		runs := make([dynamic]Profile_Run, 0, p.profile_reps)
		defer delete(runs)
		for rep in 0 ..< p.profile_reps {
			r := profile_flush_run(p, tier, rows, rep)
			append(&runs, r)
			if p.json {
				profile_json_line(tier, "flush", rep, r)
			}
		}
		profile_print_flush(p, tier, rows, runs[:])

		// Control: identical run with the profiler disabled, to show that the
		// instrumentation itself is not moving the wall clock.
		control := make([dynamic]Profile_Run, 0, p.profile_reps)
		defer delete(control)
		for rep in 0 ..< p.profile_reps {
			append(&control, profile_flush_run(p, tier, rows, 1000 + rep, false))
		}
		profile_print_control(runs[:], control[:])

		merge_runs := make([dynamic]Profile_Run, 0, p.profile_reps)
		defer delete(merge_runs)
		for rep in 0 ..< p.profile_reps {
			r := profile_merge_run(p, tier, rows, rep)
			append(&merge_runs, r)
			if p.json {
				profile_json_line(tier, "merge", rep, r)
			}
		}
		profile_print_merge(p, tier, rows, merge_runs[:])
	}
}

// profile_flush_run ingests the whole tier while the background worker may
// flush concurrently, then closes the store (which drains the memtable). The
// profiler stays enabled across close so the final flush is included.
profile_flush_run :: proc(p: Params, tier: string, rows, rep: int, enabled := true) -> Profile_Run {
	dir := fmt.aprintf("%s/flush_%s_%d", p.data, tier, rep)
	defer delete(dir)
	os.remove_all(dir)

	cfg := store_config(dir)
	cfg.block_rows = p.block_rows
	s := logdb.store_init(cfg, true)
	base_ts := common.now_ns() - SPREAD_NS

	if enabled {
		logdb.flush_profile_enable(cfg.block_rows)
	}
	begin := time.now()
	d := fill_store(s, rows, p.threads, p.batch, base_ts)
	ingest_ms := millis(d)
	c0 := time.now()
	logdb.store_close(s)
	drain_ms := millis(time.since(c0))
	prof: logdb.Flush_Profile
	if enabled {
		logdb.flush_profile_disable()
		prof = logdb.flush_profile_snapshot()
	}
	wall_ms := millis(time.since(begin))
	os.remove_all(dir)
	return Profile_Run{
		tier = tier,
		rows = rows,
		wall_ms = wall_ms,
		ingest_ms = ingest_ms,
		drain_ms = drain_ms,
		prof = prof,
	}
}

// profile_merge_run forces MERGE_CHUNKS parts (one per chunk) with the worker
// disabled, then runs a real merge. The merge threshold is 8 parts, so this is
// the same k-way merge a live store performs after enough flushes.
profile_merge_run :: proc(p: Params, tier: string, rows, rep: int) -> Profile_Run {
	dir := fmt.aprintf("%s/merge_%s_%d", p.data, tier, rep)
	defer delete(dir)
	os.remove_all(dir)

	cfg := store_config(dir)
	cfg.block_rows = p.block_rows
	s := logdb.store_init(cfg, false)
	base_ts := common.now_ns() - SPREAD_NS

	logdb.flush_profile_enable(cfg.block_rows)
	begin := time.now()
	ingest: f64
	chunk := rows / MERGE_CHUNKS
	offset := 0
	for k in 0 ..< MERGE_CHUNKS {
		n := chunk
		if k == MERGE_CHUNKS - 1 {
			n = rows - offset
		}
		fill_store_range(s, n, p.threads, p.batch, base_ts, offset, rows)
		logdb.store_flush_all(s)
		offset += n
	}
	ingest = millis(time.since(begin))
	m0 := time.now()
	logdb.store_merge_all(s)
	drain_ms := millis(time.since(m0))
	logdb.flush_profile_disable()
	wall_ms := millis(time.since(begin))
	prof := logdb.flush_profile_snapshot()
	logdb.store_close(s)
	os.remove_all(dir)
	return Profile_Run{
		tier = tier,
		rows = rows,
		wall_ms = wall_ms,
		ingest_ms = ingest,
		drain_ms = drain_ms,
		prof = prof,
	}
}

// ---------------------------------------------------------------- output

profile_json_line :: proc(tier, scenario: string, rep: int, r: Profile_Run) {
	ph := r.prof.phase[0]
	fmt.printf(
		`{{"scenario":"flush_profile","kind":"%s","tier":"%s","rep":%d,"rows":%d,"wall_ms":%.1f,"ingest_ms":%.1f,"drain_ms":%.1f,"blocks":%d,"zlib_ms":%.2f,"sort_ms":%.2f,"write_ms":%.2f,"flush_wait_ms":%.2f,"raw_bytes":%d,"comp_bytes":%d}}`,
		scenario, tier, rep, r.rows, r.wall_ms, r.ingest_ms, r.drain_ms,
		ph.blocks, f64(ph.zlib_ns)/1e6, f64(ph.sort_ns)/1e6,
		f64(ph.write_data_ns+ph.write_bloom_ns)/1e6, f64(r.prof.flush_wait_ns)/1e6,
		ph.raw_bytes, ph.comp_bytes,
	)
	fmt.println()
}

profile_print_flush :: proc(p: Params, tier: string, rows: int, runs: []Profile_Run) {
	med, lo, hi := profile_aggregate(runs)
	ph := med.phase[int(logdb.Prof_Scope.Flush)]
	lph := lo.phase[int(logdb.Prof_Scope.Flush)]
	hph := hi.phase[int(logdb.Prof_Scope.Flush)]
	blocks := ph.blocks

	pipeline := ph.sort_ns + ph.collect_ns + ph.serialize_ns + ph.zlib_ns +
		ph.bloom_finish_ns + ph.write_data_ns + ph.write_bloom_ns + ph.index_ns +
		ph.finish_index_ns + ph.finish_meta_ns + ph.finish_open_ns
	waits := med.ingest_wait_ns + med.flush_wait_ns + med.flush_append_wait_ns

	fmt.printf("\n=== flush %s: %d rows, %d reps, threads=%d batch=%d block_rows=%d ===\n",
		tier, rows, len(runs), p.threads, p.batch, p.block_rows)
	fmt.printf("  ingest %.1f ms (%.0f rows/s) | flush 累计 %.1f ms（%d 次） | drain+close %.1f ms（含 worker 退出等待） | wall %.1f ms | %d blocks | %.1f MiB raw -> %.1f MiB on disk (%.2fx)\n",
		profile_median_f64(runs, .Ingest), profile_rows_per_s(rows, profile_median_f64(runs, .Ingest) * 1e6),
		f64(med.flush_total_ns)/1e6, med.flush_calls,
		profile_median_f64(runs, .Drain), profile_median_f64(runs, .Wall), blocks,
		f64(ph.raw_bytes)/(1<<20), f64(ph.data_bytes + ph.bloom_bytes)/(1<<20),
		profile_ratio(ph.raw_bytes, ph.comp_bytes))

	fmt.printf("  %-24s %-8s %-10s %-7s %-11s %-12s %-8s\n", "阶段", "次数", "总耗时ms", "占比", "每blockµs", "rows/s", "压缩比")
	stage_row("sort", ph.sorts, ph.sort_ns, lph.sort_ns, hph.sort_ns, 0, ph.rows, pipeline, 0, 0)
	stage_row("encode:行→列+bloom哈希", blocks, ph.collect_ns, lph.collect_ns, hph.collect_ns, blocks, ph.rows, pipeline, 0, 0)
	stage_row("encode:serialize", blocks, ph.serialize_ns, lph.serialize_ns, hph.serialize_ns, blocks, ph.rows, pipeline, 0, 0)
	stage_row("zlib (level 1)", blocks, ph.zlib_ns, lph.zlib_ns, hph.zlib_ns, blocks, ph.rows, pipeline, ph.raw_bytes, ph.comp_bytes)
	stage_row("bloom:finish", blocks, ph.bloom_finish_ns, lph.bloom_finish_ns, hph.bloom_finish_ns, blocks, ph.rows, pipeline, 0, 0)
	stage_row("write data.bin", blocks, ph.write_data_ns, lph.write_data_ns, hph.write_data_ns, blocks, ph.rows, pipeline, 0, 0)
	stage_row("write bloom.bin", blocks, ph.write_bloom_ns, lph.write_bloom_ns, hph.write_bloom_ns, blocks, ph.rows, pipeline, 0, 0)
	stage_row("index:record", blocks, ph.index_ns, lph.index_ns, hph.index_ns, blocks, ph.rows, pipeline, 0, 0)
	finish_row("finish:index.bin", ph.parts, ph.finish_index_ns, lph.finish_index_ns, hph.finish_index_ns, pipeline)
	finish_row("finish:meta.json", ph.parts, ph.finish_meta_ns, lph.finish_meta_ns, hph.finish_meta_ns, pipeline)
	finish_row("finish:open+verify", ph.parts, ph.finish_open_ns, lph.finish_open_ns, hph.finish_open_ns, pipeline)
	fmt.printf("  %-24s %-8s % 10.2f %-7s\n", "流水线合计", "", f64(pipeline)/1e6, "100.0%")
	fmt.printf("  pipeline %.1f ms -> %.0f rows/s；锁等待合计 %.2f ms，占（流水线+锁等待）%.1f%%\n",
		f64(pipeline)/1e6, profile_rows_per_s(int(ph.rows), f64(pipeline)),
		f64(waits)/1e6, profile_share(waits, pipeline + waits))

	fmt.printf("  %-24s %-8s %-10s %-10s %-14s\n", "锁/切换", "次数", "总耗时ms", "每次µs", "占流水线+锁")
	lock_row("ingest 等写锁", med.ingest_calls, med.ingest_wait_ns, pipeline + waits)
	lock_row("flush 等写锁(swap)", med.flush_calls, med.flush_wait_ns, pipeline + waits)
	lock_row("flush 追加 part 等锁", med.flush_calls, med.flush_append_wait_ns, pipeline + waits)
	lock_row("memtable swap(持锁)", med.flush_calls, med.flush_swap_ns, pipeline + waits)
	fmt.printf("  ingest 调用累计 %.1f ms（%d 线程合计），其中 %.1f ms 等锁（%.1f%%），%.1f ms 持锁\n",
		f64(med.ingest_total_ns)/1e6, p.threads, f64(med.ingest_wait_ns)/1e6,
		profile_share(med.ingest_wait_ns, med.ingest_total_ns),
		f64(med.ingest_hold_ns)/1e6)

	block_distribution("flush blocks", ph, lph, hph)
}

profile_print_merge :: proc(p: Params, tier: string, rows: int, runs: []Profile_Run) {
	med, lo, hi := profile_aggregate(runs)
	ph := med.phase[int(logdb.Prof_Scope.Merge)]
	lph := lo.phase[int(logdb.Prof_Scope.Merge)]
	hph := hi.phase[int(logdb.Prof_Scope.Merge)]
	total := med.merge_total_ns
	blocks := ph.blocks

	// Input reading (compressed block read + inflate + decode) is timed at the
	// block boundary inside cursor_advance, so it is not derived by subtraction.
	fmt.printf("\n=== merge %s: %d rows, %d reps, %d chunks, block_rows=%d ===\n",
		tier, rows, len(runs), MERGE_CHUNKS, p.block_rows)
	fmt.printf("  merge %.1f ms (%.0f rows/s), %d parts -> 1 part, %d blocks, %.1f MiB raw -> %.1f MiB (%.2fx)\n",
		f64(total)/1e6, profile_rows_per_s(int(med.merge_rows), f64(total)),
		med.merged_parts, blocks, f64(ph.raw_bytes)/(1<<20),
		f64(ph.data_bytes + ph.bloom_bytes)/(1<<20),
		profile_ratio(ph.raw_bytes, ph.comp_bytes))
	fmt.printf("  输入 %.1f MiB / %d blocks，读+inflate+decode %.2f ms (%.0f MiB/s)\n",
		f64(med.merge_read_bytes)/(1<<20), med.merge_read_blocks,
		f64(med.merge_read_ns)/1e6, profile_throughput_mib(med.merge_read_bytes, med.merge_read_ns))
	fmt.printf("  %-24s %-8s %-10s %-7s %-11s %-12s %-8s\n", "阶段", "次数", "总耗时ms", "占比", "每blockµs", "rows/s", "压缩比")
	stage_row("merge 总耗时", med.merge_calls, total, lo.merge_total_ns, hi.merge_total_ns, 0, med.merge_rows, total, 0, 0)
	stage_row("k-way 主循环", med.merge_calls, med.merge_loop_ns, lo.merge_loop_ns, hi.merge_loop_ns, 0, med.merge_rows, total, 0, 0)
	stage_row("输入读取+inflate", med.merge_read_blocks, med.merge_read_ns, lo.merge_read_ns, hi.merge_read_ns, 0, med.merge_rows, total, 0, 0)
	stage_row("encode:行→列+bloom哈希", blocks, ph.collect_ns, lph.collect_ns, hph.collect_ns, blocks, ph.rows, total, 0, 0)
	stage_row("encode:serialize", blocks, ph.serialize_ns, lph.serialize_ns, hph.serialize_ns, blocks, ph.rows, total, 0, 0)
	stage_row("zlib (level 1)", blocks, ph.zlib_ns, lph.zlib_ns, hph.zlib_ns, blocks, ph.rows, total, ph.raw_bytes, ph.comp_bytes)
	stage_row("bloom:finish", blocks, ph.bloom_finish_ns, lph.bloom_finish_ns, hph.bloom_finish_ns, blocks, ph.rows, total, 0, 0)
	stage_row("write data.bin", blocks, ph.write_data_ns, lph.write_data_ns, hph.write_data_ns, blocks, ph.rows, total, 0, 0)
	stage_row("write bloom.bin", blocks, ph.write_bloom_ns, lph.write_bloom_ns, hph.write_bloom_ns, blocks, ph.rows, total, 0, 0)
	stage_row("index:record", blocks, ph.index_ns, lph.index_ns, hph.index_ns, blocks, ph.rows, total, 0, 0)
	finish_row("finish:index.bin", ph.parts, ph.finish_index_ns, lph.finish_index_ns, hph.finish_index_ns, total)
	finish_row("finish:meta.json", ph.parts, ph.finish_meta_ns, lph.finish_meta_ns, hph.finish_meta_ns, total)
	finish_row("finish:open+verify", ph.parts, ph.finish_open_ns, lph.finish_open_ns, hph.finish_open_ns, total)
	block_distribution("merge blocks", ph, lph, hph)
}

// ---------------------------------------------------------------- table helpers

profile_print_control :: proc(on, off: []Profile_Run) {
	on_w := profile_median_f64(on, .Wall)
	off_w := profile_median_f64(off, .Wall)
	pct := 0.0
	if off_w > 0 {
		pct = (on_w - off_w) / off_w * 100.0
	}
	fmt.printf("  prof-off 对照（%d reps）: ingest %.1f ms, drain %.1f ms, wall %.1f ms；开启相对关闭 %+.1f%%\n",
		len(off), profile_median_f64(off, .Ingest), profile_median_f64(off, .Drain), off_w, pct)
}

stage_row :: proc(name: string, n, ns, lo_ns, hi_ns, blocks, rows, pipeline, raw, comp: u64) {
	per_block := "-"
	if blocks > 0 {
		per_block = fmt.tprintf("%.1f", f64(ns) / f64(blocks) / 1000.0)
	}
	rps := "-"
	if ns > 0 && rows > 0 {
		rps = fmt.tprintf("%.0f", profile_rows_per_s(int(rows), f64(ns)))
	}
	ratio := "-"
	if comp > 0 {
		ratio = fmt.tprintf("%.2fx", f64(raw) / f64(comp))
	}
	star := profile_span_star(ns, lo_ns, hi_ns)
	fmt.printf("  %-24s % 8d % 10.2f % 6.1f%% %-11s %-12s %-8s%s\n",
		name, n, f64(ns)/1e6, profile_share(ns, pipeline), per_block, rps, ratio, star)
}

finish_row :: proc(name: string, n, ns, lo_ns, hi_ns, total: u64) {
	star := profile_span_star(ns, lo_ns, hi_ns)
	fmt.printf("  %-24s % 8d % 10.2f % 6.1f%% %-11s %-12s %-8s%s\n",
		name, n, f64(ns)/1e6, profile_share(ns, total), "-", "-", "-", star)
}

lock_row :: proc(name: string, n, ns, total: u64) {
	per_call := "-"
	if n > 0 {
		per_call = fmt.tprintf("%.1f", f64(ns) / f64(n) / 1000.0)
	}
	fmt.printf("  %-24s % 8d % 10.2f %-10s % 13.1f%%\n",
		name, n, f64(ns)/1e6, per_call, profile_share(ns, total))
}

block_distribution :: proc(label: string, ph, lph, hph: logdb.Phase_Stats) {
	avg_rows := f64(0)
	if ph.blocks > 0 {
		avg_rows = f64(ph.rows) / f64(ph.blocks)
	}
	min_rows := ph.min_rows
	max_rows := ph.max_rows
	if ph.blocks == 0 {
		min_rows = 0
	}
	min_raw := ph.min_raw
	if ph.blocks == 0 {
		min_raw = 0
	}
	star := profile_span_star(ph.zlib_ns, lph.zlib_ns, hph.zlib_ns)
	fmt.printf("  %s: %d blocks, %d parts, rows/block avg %.0f (min %d max %d), raw/block avg %.0f KiB (min %.0f max %.0f KiB), comp/block avg %.0f KiB%s\n",
		label, ph.blocks, ph.parts, avg_rows, min_rows, max_rows,
		profile_avg(ph.raw_bytes, ph.blocks) / 1024.0, f64(min_raw)/1024.0, f64(ph.max_raw)/1024.0,
		profile_avg(ph.comp_bytes, ph.blocks) / 1024.0, star)
	fmt.printf("    block rows: full=%d half+=%d small=%d | raw size: <=64K=%d <=256K=%d <=512K=%d <=1M=%d <=2M=%d >2M=%d\n",
		ph.row_hist[0], ph.row_hist[1], ph.row_hist[2],
		ph.size_hist[0], ph.size_hist[1], ph.size_hist[2], ph.size_hist[3], ph.size_hist[4], ph.size_hist[5])
}

// ---------------------------------------------------------------- aggregation

profile_aggregate :: proc(runs: []Profile_Run) -> (med, lo, hi: logdb.Flush_Profile) {
	n := len(runs)
	if n == 0 {
		return
	}
	words := make([][]u64, n)
	defer delete(words)
	for i in 0 ..< n {
		words[i] = logdb.flush_profile_words(&runs[i].prof)
	}
	med = {}
	lo = {}
	hi = {}
	mw := logdb.flush_profile_words(&med)
	lw := logdb.flush_profile_words(&lo)
	hw := logdb.flush_profile_words(&hi)
	buf := make([]u64, n)
	defer delete(buf)
	for wi in 0 ..< len(mw) {
		for i in 0 ..< n {
			buf[i] = words[i][wi]
		}
		slice.sort(buf)
		lw[wi] = buf[0]
		hw[wi] = buf[n-1]
		mw[wi] = buf[n/2]
	}
	return
}

Profile_Wall_Field :: enum {
	Wall,
	Ingest,
	Drain,
}

profile_median_f64 :: proc(runs: []Profile_Run, field: Profile_Wall_Field) -> f64 {
	vals := make([]f64, len(runs))
	defer delete(vals)
	for r, i in runs {
		vals[i] = field == .Wall ? r.wall_ms : field == .Ingest ? r.ingest_ms : r.drain_ms
	}
	slice.sort(vals)
	return vals[len(vals)/2]
}

profile_span_star :: proc(med, lo, hi: u64) -> string {
	if med == 0 {
		return ""
	}
	if f64(hi - lo) / f64(med) > 0.3 {
		return "*"
	}
	return ""
}

profile_share :: proc(part, total: u64) -> f64 {
	if total == 0 {
		return 0
	}
	return f64(part) / f64(total) * 100.0
}

profile_avg :: proc(total, count: u64) -> f64 {
	if count == 0 {
		return 0
	}
	return f64(total) / f64(count)
}

profile_rows_per_s :: proc(rows: int, ns: f64) -> f64 {
	if ns <= 0 {
		return 0
	}
	return f64(rows) / (ns / 1e9)
}

profile_ratio :: proc(raw, comp: u64) -> f64 {
	if comp == 0 {
		return 0
	}
	return f64(raw) / f64(comp)
}

profile_throughput_mib :: proc(bytes, ns: u64) -> f64 {
	if ns == 0 {
		return 0
	}
	return f64(bytes) / (1 << 20) / (f64(ns) / 1e9)
}
