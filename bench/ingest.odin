// Ingest benchmark and data seeding. Rows are written with the background
// flusher enabled, so the reported rate includes concurrent memtable flushes
// instead of only in-memory appends.
package main

import "core:fmt"
import "core:os"
import "core:sync"
import "core:thread"
import "core:time"

import "../common"
import "../logdb"

Ingest_Args :: struct {
	s:       ^logdb.Store,
	corpus:  ^Corpus,
	start:   ^u32,
	base_ts: i64,
	offset:  int,
	rows:    int,
	total:   int,
	batch:   int,
}

ingest_worker :: proc(data: rawptr) {
	a := cast(^Ingest_Args)data
	spin_until(a.start)
	rows := make([]logdb.Ingest_Row, a.batch)
	defer delete(rows)
	fields := make([]logdb.Field, a.batch * 6)
	defer delete(fields)
	done := 0
	for done < a.rows {
		count := min(a.batch, a.rows - done)
		for b in 0..<count {
			gi := a.offset + done + b
			fi := b * 6
			fields[fi+0] = logdb.Field{name = "level", value = a.corpus.levels[gi % 4]}
			fields[fi+1] = logdb.Field{name = "service", value = a.corpus.services[gi % 4]}
			fields[fi+2] = logdb.Field{name = "zone", value = a.corpus.zones[gi % 3]}
			fields[fi+3] = logdb.Field{name = "host", value = a.corpus.hosts[gi % HOSTS]}
			fields[fi+4] = logdb.Field{name = "user", value = a.corpus.users[gi % USERS]}
			fields[fi+5] = logdb.Field{name = "duration", value = a.corpus.durations[gi % DURATIONS]}
			rows[b] = logdb.Ingest_Row{
				ts     = row_ts(a.base_ts, gi, a.total),
				stream = u64(gi % 4 + 1),
				msg    = a.corpus.msgs[gi % len(a.corpus.msgs)],
				fields = fields[fi:fi+6],
			}
		}
		logdb.store_ingest(a.s, rows[:count])
		done += count
	}
}

// fill_store writes `rows` rows concurrently and returns the wall time.
fill_store :: proc(s: ^logdb.Store, rows, threads, batch: int, base_ts: i64) -> time.Duration {
	return fill_store_range(s, rows, threads, batch, base_ts, 0, rows)
}

// fill_store_range writes rows [offset, offset+rows) of a corpus of `total`
// rows, so a tier can be ingested in successive chunks with timestamps spread
// over the same window.
fill_store_range :: proc(s: ^logdb.Store, rows, threads, batch: int, base_ts: i64, offset0, total: int) -> time.Duration {
	corpus := corpus_make()
	defer corpus_destroy(&corpus)

	start: u32
	args := make([]Ingest_Args, threads)
	workers := make([]^thread.Thread, threads)
	defer {
		delete(args)
		delete(workers)
	}
	per := rows / threads
	rem := rows % threads
	offset := offset0
	for i in 0..<threads {
		n := per
		if i < rem {
			n += 1
		}
		args[i] = Ingest_Args{
			s      = s,
			corpus = &corpus,
			start  = &start,
			base_ts = base_ts,
			offset = offset,
			rows   = n,
			total  = total,
			batch  = batch,
		}
		offset += n
		workers[i] = thread.create_and_start_with_data(&args[i], ingest_worker, self_cleanup = false)
	}
	begin := time.now()
	sync.atomic_store(&start, 1)
	wait_workers(workers)
	return time.since(begin)
}

store_config :: proc(dir: string) -> logdb.Store_Config {
	cfg := logdb.default_store_config()
	cfg.data_path = dir
	return cfg
}

// open_store loads an existing directory or seeds a fresh one. Seeding is
// skipped when reuse is set; the directory is wiped otherwise.
open_store :: proc(p: Params, reuse: bool) -> ^logdb.Store {
	dir := p.data
	if !reuse {
		os.remove_all(dir)
	}
	cfg := store_config(dir)
	s := logdb.store_init(cfg, !reuse)
	if !reuse {
		base_ts := common.now_ns() - SPREAD_NS
		d := fill_store(s, p.rows, p.threads, p.batch, base_ts)
		if p.rows > 0 {
			if p.json {
				fmt.printf(
					`{{"scenario":"seed","rows":%d,"threads":%d,"batch":%d,"ingest_s":%.3f,"rows_per_s":%.0f,"base_ts":%d,"end_ts":%d}}`,
					p.rows, p.threads, p.batch, millis(d) / 1000.0, throughput(p.rows, d),
					base_ts, base_ts + SPREAD_NS,
				)
				fmt.println()
			} else {
				fmt.printf("[seed] rows=%d threads=%d batch=%d  %.0f rows/s  %.1f ms\n",
					p.rows, p.threads, p.batch, throughput(p.rows, d), millis(d))
			}
		}
		begin := time.now()
		logdb.store_flush_all(s)
		fd := time.since(begin)
		st := logdb.store_stats(s)
		if p.json {
			fmt.printf(
				`{{"scenario":"seed_flush","rows":%d,"flush_s":%.3f,"rows_per_s":%.0f,"parts":%d,"partitions":%d,"disk_bytes":%d}}`,
				p.rows, millis(fd) / 1000.0, throughput(p.rows, fd), st.parts, st.partitions, st.disk_bytes,
			)
			fmt.println()
		} else {
			fmt.printf("[flush] %.0f rows/s  %.1f ms  %d parts  %d partitions  %.1f MiB\n",
				throughput(p.rows, fd), millis(fd), st.parts, st.partitions, f64(st.disk_bytes) / (1 << 20))
		}
	}
	return s
}

run_ingest :: proc(p: Params) {
	s := open_store(p, false)
	defer logdb.store_close(s)
	if p.keep {
		return
	}
}
