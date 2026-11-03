package logdb

import "../common"
import "base:runtime"
import "core:fmt"
import "core:os"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:sys/info"
import "core:thread"
import "core:time"

Store_Config :: struct {
	data_path:        string,
	retention_days:   int,
	stream_fields:    []string,
	flush_interval:   time.Duration,
	flush_max_rows:   int,
	flush_max_bytes:  i64,
	block_rows:       int,
	merge_threshold:  int,
	merge_max_rows:   int,
	// flush_threads is the number of threads encoding/compressing a part.
	// 0 picks a value from the logical core count.
	flush_threads:    int,
	// sort_threads is the number of threads sorting a memtable by stream.
	// 0 picks a value from the logical core count; small memtables always sort
	// serially (see sort_rows).
	sort_threads:     int,
	// ingest_shards stripes a partition's memtable into this many independently
	// locked append buffers. 0 picks a value from the logical core count.
	ingest_shards:    int,
	// mem_max_rows / mem_max_bytes bound the whole memtable (all partitions).
	// An ingest batch reserves its rows and bytes before copying; when the
	// reservation does not fit it waits up to mem_wait for a flush to free
	// space, and rejects the batch if it still does not fit. 0 disables a cap.
	mem_max_rows:  int,
	mem_max_bytes: i64,
	// mem_wait is the longest an ingest batch may block on the cap before the
	// caller (the HTTP layer) turns it into 503 + Retry-After.
	mem_wait:      time.Duration,
}

default_store_config :: proc() -> Store_Config {
	return Store_Config{
		data_path = "./data",
		retention_days = 30,
		flush_interval = 1 * time.Second,
		flush_max_rows = 200_000,
		flush_max_bytes = 128 << 20,
		block_rows = 8192,
		merge_threshold = 8,
		merge_max_rows = 4_000_000,
		flush_threads = 0,
		sort_threads = 0,
		ingest_shards = 0,
		// 1 GiB of memtable and 6M rows. The row cap is the smaller of the two
		// for the measured ~165 B/row of the JSON corpus (6M rows ≈ 0.97 GiB),
		// so both limits engage at the same order and either one alone is enough
		// to bound RSS. They are 30x flush_max_rows, so a flush that keeps up
		// never blocks ingest, and a flush that does not leaves at most ~1 GiB
		// resident instead of growing without a ceiling.
		mem_max_rows = 6_000_000,
		mem_max_bytes = 1 << 30,
		mem_wait = 2 * time.Second,
	}
}

// resolve_flush_threads caps the default at half the logical cores so that
// HTTP workers and the encoder do not fight over every core, and at eight
// because zlib saturates memory bandwidth well before that.
resolve_flush_threads :: proc(requested: int) -> int {
	if requested > 0 {
		return requested
	}
	_, logical, ok := info.cpu_core_count()
	if !ok || logical <= 2 {
		return 1
	}
	n := logical / 2
	return clamp(n, 2, 8)
}

// resolve_sort_threads caps the default at four: the sort is bandwidth bound,
// and a flush only has one range per stream (four for the default stream
// fields), so more threads would only add handoff cost.
resolve_sort_threads :: proc(requested: int) -> int {
	if requested > 0 {
		return requested
	}
	_, logical, ok := info.cpu_core_count()
	if !ok || logical <= 2 {
		return 1
	}
	return clamp(logical / 2, 2, 4)
}

// resolve_ingest_shards makes the shard count at least four so ingest batches
// never queue behind each other, and at most sixteen because each shard keeps
// its own arena around.
resolve_ingest_shards :: proc(requested: int) -> int {
	if requested > 0 {
		return requested
	}
	_, logical, ok := info.cpu_core_count()
	if !ok || logical <= 3 {
		return 4
	}
	return clamp(logical, 4, 16)
}

// Ingest_Shard is one stripe of a partition's memtable. It owns its lock, row
// array and arena, so batches that land on different stripes never serialise.
Ingest_Shard :: struct {
	mu:    sync.Mutex,
	rows:  [dynamic]Row,
	arena: common.Arena,
	bytes: i64,
}

Partition :: struct {
	day:        i64,
	dir:        string,
	shards:     []Ingest_Shard,
	next_shard: u32,
	// memtable totals: written under the owning shard's lock, read without one
	// (flush_due, store_stats), so they are atomics.
	mem_rows:  u64,
	mem_bytes: i64,
	since_ns:  i64,
	parts:     [dynamic]^Disk_Part,
}

Store :: struct {
	cfg:        Store_Config,
	mu:         sync.RW_Mutex,
	partitions: [dynamic]^Partition,
	// parts_mu guards every partition's parts list. A flush appends its parts and
	// a merge picks, replaces and unlinks parts under this mutex instead of mu's
	// write side: ingest holds mu shared for whole batches, and a reader-preferring
	// rw lock then starves a part append for hundreds of milliseconds, which
	// throttles the flush and lets the memtable backlog grow. Lock order is
	// mu -> parts_mu; parts_mu is never held while taking mu.
	parts_mu: sync.Mutex,
	// retired partitions have been removed from partitions by retention but a
	// flush or merge may still hold their pointer, so their containers (and dirs)
	// are freed at close instead of here.
	retired: [dynamic]^Partition,
	worker:     ^thread.Thread,
	// merge_worker compacts parts independently of the flush worker so a large
	// merge never delays a flush that ingest is waiting on.
	merge_worker: ^thread.Thread,
	// merge_mu serialises merge rounds: two concurrent merges could pick the same
	// parts and append both results, duplicating rows.
	merge_mu: sync.Mutex,
	// alloc is the allocator the store was created with. Worker threads do not
	// inherit the creator's context, so they install this one before touching any
	// memory the main thread (or another worker) may free later.
	alloc: runtime.Allocator,
	seq:   u64,
	stop:       u32,
	// metrics
	ingested_rows:  u64,
	ingested_bytes: u64,
	flushed_parts:  u64,
	// flushed_rows and flush_ms_last describe the most recent flush, so
	// operators can tell whether the flush size is bounded by flush_max_rows.
	flushed_rows:  u64,
	flush_ms_last: u64,
	// active_scans counts queries that captured a part snapshot; store_close
	// waits for them so it never frees a part a scan is still reading.
	active_scans:   i64,
	merged_parts:   u64,
	queries:        u64,
	dropped_rows:   u64,
	// store-wide memtable accounting, in rows and in row_bytes. Ingest adds its
	// reservation before copying and a flush subtracts what it stole, so the
	// sums never include a batch that was rejected.
	mem_rows:  u64,
	mem_bytes: i64,
	// space_mu guards the reservation check-and-add against a concurrent flush
	// subtracting, and is the mutex space_cond waits on. Lock order is
	// `mu` -> `space_mu`: ingest takes space_mu on its own (never while holding
	// mu), a flush takes mu and then space_mu. No cycle.
	space_mu:   sync.Mutex,
	space_cond: sync.Cond,
	// metrics
	ingest_stalled_ns: u64,
	mem_limit_rejected: u64,
}

Ingest_Row :: struct {
	ts:     i64,
	stream: u64,
	msg:    string,
	fields: []Field,
}

row_less :: proc(a, b: Row) -> bool {
	if a.stream != b.stream {
		return a.stream < b.stream
	}
	return a.ts < b.ts
}

// Sorting dominates the flush of a large memtable, so above
// SORT_PARALLEL_MIN_ROWS the slice is split into contiguous chunks, one per
// thread. Each chunk is sorted independently and the sorted runs are merged
// into a scratch buffer afterwards. The merge is serial but visits each row
// once with at most `chunks` (i.e. thread count) comparisons, which is far
// cheaper than the sort it replaces.
SORT_PARALLEL_MIN_ROWS :: 8192
SORT_CHUNK_MIN_ROWS :: 2048

Sort_Ctx :: struct {
	rows:   []Row,
	bounds: []int,
	cursor: u32,
}

sort_chunk_worker :: proc(data: rawptr) {
	c := cast(^Sort_Ctx)data
	for {
		i := int(sync.atomic_add(&c.cursor, 1))
		if i + 1 >= len(c.bounds) {
			break
		}
		slice.sort_by(c.rows[c.bounds[i]:c.bounds[i + 1]], row_less)
	}
}

// sort_rows orders rows by (stream, ts), the layout every part relies on.
sort_rows :: proc(rows: []Row, threads := 0) {
	n := len(rows)
	parallel := resolve_flush_threads(threads)
	if parallel <= 1 || n < SORT_PARALLEL_MIN_ROWS {
		slice.sort_by(rows, row_less)
		return
	}
	chunks := clamp(n / SORT_CHUNK_MIN_ROWS, 1, parallel)
	if chunks <= 1 {
		slice.sort_by(rows, row_less)
		return
	}
	bounds := make([]int, chunks + 1)
	defer delete(bounds)
	base := n / chunks
	extra := n % chunks
	off := 0
	for i in 0 ..< chunks {
		bounds[i] = off
		off += base
		if i < extra {
			off += 1
		}
	}
	bounds[chunks] = n
	ctx := Sort_Ctx{rows = rows, bounds = bounds}
	spawned := make([dynamic]^thread.Thread, 0, chunks - 1)
	defer delete(spawned)
	for _ in 0 ..< chunks - 1 {
		t := thread.create_and_start_with_data(&ctx, sort_chunk_worker, self_cleanup = false, name = "oboe-sort")
		if t == nil {
			// A failed spawn only lowers parallelism: the calling thread keeps
			// claiming chunks until every run is sorted.
			break
		}
		append(&spawned, t)
	}
	sort_chunk_worker(&ctx)
	for t in spawned {
		thread.join(t)
		thread.destroy(t)
	}
	merge_sorted_chunks(rows, bounds)
}

merge_sorted_chunks :: proc(rows: []Row, bounds: []int) {
	k := len(bounds) - 1
	if k <= 1 {
		return
	}
	tmp := make([]Row, len(rows))
	defer delete(tmp)
	cursors := make([]int, k)
	defer delete(cursors)
	copy(cursors, bounds[:k])
	for dst in 0 ..< len(rows) {
		best := -1
		for i in 0 ..< k {
			if cursors[i] >= bounds[i + 1] {
				continue
			}
			// `<` on purpose: on a tie the lowest chunk wins, so equal keys stay
			// ordered by chunk index and a sorted chunk's internal order is
			// preserved as far as smoothsort allows.
			if best < 0 || row_less(rows[cursors[i]], rows[cursors[best]]) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		tmp[dst] = rows[cursors[best]]
		cursors[best] += 1
	}
	copy(rows, tmp)
}

// ---------------------------------------------------------------- lifecycle

store_init :: proc(cfg: Store_Config, start_worker := true) -> ^Store {
	s := new(Store)
	s.cfg = cfg
	s.alloc = context.allocator
	s.cfg.flush_threads = resolve_flush_threads(cfg.flush_threads)
	s.cfg.sort_threads = resolve_sort_threads(cfg.sort_threads)
	s.cfg.ingest_shards = resolve_ingest_shards(cfg.ingest_shards)
	os.make_directory_all(s.cfg.data_path)
	partitions_dir := fmt.aprintf("%s/partitions", s.cfg.data_path)
	os.make_directory_all(partitions_dir)
	entries, derr := os.read_directory_by_path(partitions_dir, -1, context.allocator)
	if derr == nil {
		for fi in entries {
			if fi.type != .Directory {
				continue
			}
			if len(fi.name) != 8 {
				continue
			}
			day, ok := parse_day_string(fi.name)
			if !ok {
				continue
			}
			p := new(Partition)
			p.day = day
			p.dir = strings_clone_dir(fi.fullpath)
			partition_alloc_shards(p, s.cfg.ingest_shards)
			parts_dir := fmt.aprintf("%s/parts", p.dir)
			part_entries, perr := os.read_directory_by_path(parts_dir, -1, context.allocator)
			// Parts superseded by a completed merge are still referenced by
			// newer parts until their directories are removed; drop them here
			// so a crash mid-cleanup cannot resurrect duplicate rows.
			superseded := make(map[string]bool)
			if perr == nil {
				for pe in part_entries {
					if pe.type != .Directory {
						continue
					}
					if seq, sok := part_seq(pe.name); sok && seq >= s.seq {
						s.seq = seq + 1
					}
					part, pok := open_part(pe.fullpath)
					if !pok {
						fmt.eprintf("oboe: dropping incomplete part %s\n", pe.fullpath)
						os.remove_all(pe.fullpath)
						continue
					}
					append(&p.parts, part)
					marker_path := fmt.aprintf("%s/merged_from", pe.fullpath)
					if marker, merr := os.read_entire_file_from_path(marker_path, context.allocator); merr == nil {
						ids, _ := strings.split_lines(string(marker))
						for id in ids {
							if len(id) > 0 {
								superseded[id] = true
							}
						}
						delete(ids)
						delete(marker)
					}
					delete(marker_path)
				}
				os.file_info_slice_delete(part_entries, context.allocator)
			}
			if len(superseded) > 0 {
				kept := make([dynamic]^Disk_Part, 0, len(p.parts))
				for part in p.parts {
					if superseded[part.meta.id] {
						os.remove_all(part.dir)
						close_part(part)
					} else {
						append(&kept, part)
					}
				}
				delete(p.parts)
				p.parts = kept
			}
			delete_map(superseded)
			delete(parts_dir)
			append(&s.partitions, p)
		}
		os.file_info_slice_delete(entries, context.allocator)
	}
	delete(partitions_dir)
	if start_worker {
		s.worker = thread.create_and_start_with_data(s, store_worker, self_cleanup = false, name = "oboe-store")
		s.merge_worker = thread.create_and_start_with_data(s, store_merge_worker, self_cleanup = false, name = "oboe-merge")
	}
	return s
}

// strings_clone_dir avoids importing core:strings just for one clone.
strings_clone_dir :: proc(s: string) -> string {
	b := make([]u8, len(s))
	copy(b, transmute([]u8)s)
	return string(b)
}

parse_day_string :: proc(name: string) -> (i64, bool) {
	if len(name) != 8 {
		return 0, false
	}
	y, m, d: i64
	for i in 0..<8 {
		c := name[i]
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	y = i64(name[0]-'0')*1000 + i64(name[1]-'0')*100 + i64(name[2]-'0')*10 + i64(name[3]-'0')
	m = i64(name[4]-'0')*10 + i64(name[5]-'0')
	d = i64(name[6]-'0')*10 + i64(name[7]-'0')
	if m < 1 || m > 12 || d < 1 || d > 31 {
		return 0, false
	}
	return common.days_from_civil(y, m, d), true
}

// part_seq extracts the monotonically increasing suffix from a part directory
// name ("p_<16 hex digits>"), so fresh parts never overwrite loaded ones.
part_seq :: proc(name: string) -> (u64, bool) {
	if len(name) != 18 || name[0] != 'p' || name[1] != '_' {
		return 0, false
	}
	return strconv.parse_u64(name[2:], 16)
}

store_worker :: proc(data: rawptr) {
	s := cast(^Store)data
	context.allocator = s.alloc
	last_retention := i64(0)
	for sync.atomic_load(&s.stop) == 0 {
		store_flush_due(s)
		now := common.now_ns()
		if now - last_retention >= 60 * 1_000_000_000 {
			store_apply_retention(s)
			last_retention = now
		}
		// Poll often so a memtable is swapped out as soon as it crosses
		// flush_max_rows. With a long poll interval the whole backlog is swapped
		// at once, which makes each flush grow with ingest rate instead of
		// staying bounded by the configured threshold.
		time.sleep(5 * time.Millisecond)
	}
}

// Chunked flushes produce one part per flush_max_rows, so merges have to run
// continuously to keep the part count bounded; a merge also takes far longer
// than the flush worker's poll interval, which is why it lives on its own thread.
store_merge_worker :: proc(data: rawptr) {
	s := cast(^Store)data
	context.allocator = s.alloc
	for sync.atomic_load(&s.stop) == 0 {
		merged := store_merge_all(s, 0)
		if merged == 0 {
			time.sleep(50 * time.Millisecond)
		} else {
			// Yield so a merge backlog cannot starve the flush worker of a core.
			time.sleep(1 * time.Millisecond)
		}
	}
}


store_close :: proc(s: ^Store) {
	t_close := time.now()
	sync.atomic_store(&s.stop, 1)
	// Release cap-blocked writers first: they must not sit out mem_wait while we
	// wait for the worker and the final flush.
	wake_mem_waiters(s)
	if s.worker != nil {
		thread.join(s.worker)
		thread.destroy(s.worker)
		s.worker = nil
	}
	if s.merge_worker != nil {
		thread.join(s.merge_worker)
		thread.destroy(s.merge_worker)
		s.merge_worker = nil
	}
	flush_start := time.now()
	store_flush_all(s)
	flush_ns := time.since(flush_start)
	// Scans released the store lock but may still be reading parts, so wait for
	// them before anything is freed.
	for sync.atomic_load(&s.active_scans) > 0 {
		time.sleep(1 * time.Millisecond)
	}
	sync.rw_mutex_lock(&s.mu)
	for p in s.partitions {
		// Shutdown keeps the parts on disk; just close the handles. Unlinking
		// here would delete the freshly flushed data.
		for part in p.parts {
			close_part(part)
		}
		delete(p.parts)
		partition_free_shards(p)
		delete(p.dir)
		free(p)
	}
	for p in s.retired {
		delete(p.parts)
		partition_free_shards(p)
		delete(p.dir)
		free(p)
	}
	delete(s.retired)
	delete(s.partitions)
	sync.rw_mutex_unlock(&s.mu)
	// Shutdown cost is dominated by draining the memtable, so log it: it is how
	// operators check that the cap is keeping close bounded. stderr keeps the
	// bench's `-json` stdout stream machine-readable.
	fmt.fprintf(os.stderr, "oboe: store closed in %.1f ms (final flush %.1f ms, %d rows left)\n",
		f64(time.since(t_close)) / 1e6, f64(flush_ns) / 1e6, sync.atomic_load(&s.mem_rows))
	free(s)
}

// ---------------------------------------------------------------- ingestion

// reserve_mem claims a batch's rows and bytes in the store-wide memtable
// accounting, waiting up to max_wait for a flush to make room. The cap check and
// the add both happen under space_mu, so a flush cannot subtract between them
// and let two batches overfill by the same amount twice. Returns stalled=true
// when it had to wait at all.
reserve_mem :: proc(s: ^Store, rows: int, bytes: i64, max_wait: time.Duration) -> (ok: bool, stalled: bool) {
	if rows <= 0 {
		return true, false
	}
	wait := max_wait
	if wait < 0 {
		wait = 0
	}
	deadline := common.now_ns() + i64(wait)
	limit_rows := i64(s.cfg.mem_max_rows)
	limit_bytes := s.cfg.mem_max_bytes
	sync.mutex_lock(&s.space_mu)
	defer sync.mutex_unlock(&s.space_mu)
	for {
		if sync.atomic_load(&s.stop) != 0 {
			// Shutting down: fail fast so every blocked writer drains instead of
			// sitting out the full budget while store_close waits on it.
			return false, stalled
		}
		used_rows := i64(sync.atomic_load(&s.mem_rows))
		used_bytes := sync.atomic_load(&s.mem_bytes)
		rows_fit := limit_rows <= 0 || used_rows + i64(rows) <= limit_rows
		bytes_fit := limit_bytes <= 0 || used_bytes + bytes <= limit_bytes
		if rows_fit && bytes_fit {
			sync.atomic_add(&s.mem_rows, u64(rows))
			sync.atomic_add(&s.mem_bytes, bytes)
			return true, stalled
		}
		now := common.now_ns()
		if now >= deadline {
			return false, stalled
		}
		stalled = true
		_ = sync.cond_wait_with_timeout(&s.space_cond, &s.space_mu, time.Duration(deadline - now))
	}
}

// release_mem returns reserved space and wakes blocked ingests so they re-check
// the cap. A flush calls it after stealing a memtable; ingest calls it when a
// batch appended fewer rows than it reserved.
release_mem :: proc(s: ^Store, rows: int, bytes: i64) {
	if rows > 0 {
		sync.atomic_sub(&s.mem_rows, u64(rows))
	}
	if bytes > 0 {
		sync.atomic_sub(&s.mem_bytes, bytes)
	}
	sync.mutex_lock(&s.space_mu)
	sync.cond_broadcast(&s.space_cond)
	sync.mutex_unlock(&s.space_mu)
}

// wake_mem_waiters releases blocked ingests without changing the accounting;
// store_close uses it so a cap-blocked writer returns immediately.
wake_mem_waiters :: proc(s: ^Store) {
	sync.mutex_lock(&s.space_mu)
	sync.cond_broadcast(&s.space_cond)
	sync.mutex_unlock(&s.space_mu)
}

// store_ingest_bounded is store_ingest with an explicit cap budget. `accepted`
// is 0 when the batch was rejected because the memtable stayed full for the
// whole `max_wait`; `stalled_ns` is how long the batch waited on the cap.
store_ingest_bounded :: proc(s: ^Store, rows: []Ingest_Row, max_wait: time.Duration) -> (accepted: int, stalled_ns: i64, rejected: bool) {
	if len(rows) == 0 {
		return 0, 0, false
	}
	active := flush_profile_enabled()
	t_entry := prof_stage_start(active)
	t_locked: time.Time
	now := common.now_ns()
	for i in 0..<len(rows) {
		if rows[i].ts <= 0 {
			rows[i].ts = now
		}
	}

	// Reserve before taking the store lock: a cap wait must never be able to
	// block a flush's exclusive lock, so the reservation finishes (or times
	// out) first, and space_mu is released before mu is taken.
	// row_bytes is a pure length sum, so this pass costs far less than the copy
	// it protects, and it uses the same formula the memtable accounting uses,
	// which keeps the flush subtraction exact.
	add_bytes := i64(0)
	for i in 0..<len(rows) {
		add_bytes += row_bytes(&rows[i])
	}
	t_wait := time.now()
	ok, stalled := reserve_mem(s, len(rows), add_bytes, max_wait)
	if stalled {
		stalled_ns = i64(time.since(t_wait))
		sync.atomic_add(&s.ingest_stalled_ns, u64(stalled_ns))
	}
	if !ok {
		sync.atomic_add(&s.mem_limit_rejected, 1)
		if active {
			sync.atomic_add(&flush_prof.ingest_total_ns, u64(time.since(t_entry)))
		}
		return 0, stalled_ns, true
	}

	// The store lock stays shared for the whole batch: it keeps partitions alive
	// against retention, but concurrent batches no longer exclude each other.
	t_lock_start := time.now()
	sync.rw_mutex_shared_lock(&s.mu)
	if !batch_partitions_present(s, rows) {
		// A day without a partition is rare (first batch of a day), so the
		// exclusive upgrade is off the hot path.
		sync.rw_mutex_shared_unlock(&s.mu)
		sync.rw_mutex_lock(&s.mu)
		for i := 0; i < len(rows); {
			day := common.utc_day(rows[i].ts)
			_ = get_partition(s, day)
			i = day_run_end(rows, i, day)
		}
		sync.rw_mutex_unlock(&s.mu)
		sync.rw_mutex_shared_lock(&s.mu)
	}
	if active {
		t_locked = time.now()
		sync.atomic_add(&flush_prof.ingest_wait_ns, u64(time.since(t_lock_start)))
		sync.atomic_add(&flush_prof.ingest_calls, 1)
		sync.atomic_add(&flush_prof.ingest_rows, u64(len(rows)))
	}

	// Contiguous day runs share one shard lock, so a normal batch takes exactly
	// one append lock. The round-robin cursor spreads concurrent batches over
	// different shards even when they all target the same partition.
	total_bytes: i64
	appended := 0
	i := 0
	for i < len(rows) {
		day := common.utc_day(rows[i].ts)
		j := day_run_end(rows, i, day)
		if p := find_partition(s, day); p != nil {
			idx := int(sync.atomic_add(&p.next_shard, u32(1))) % len(p.shards)
			total_bytes += append_shard_rows(p, &p.shards[idx], rows[i:j], now)
			appended += j - i
		}
		i = j
	}
	sync.atomic_add(&s.ingested_rows, u64(appended))
	sync.atomic_add(&s.ingested_bytes, u64(total_bytes))
	sync.rw_mutex_shared_unlock(&s.mu)
	// Give back what the reservation claimed but nothing appended (a day run
	// whose partition disappeared); done after dropping mu to keep the lock
	// order mu -> space_mu one-directional.
	if appended < len(rows) {
		release_mem(s, len(rows) - appended, add_bytes - total_bytes)
	}
	if active {
		sync.atomic_add(&flush_prof.ingest_hold_ns, u64(time.since(t_locked)))
		sync.atomic_add(&flush_prof.ingest_total_ns, u64(time.since(t_entry)))
	}
	return appended, stalled_ns, false
}

store_ingest :: proc(s: ^Store, rows: []Ingest_Row) -> int {
	accepted, _, _ := store_ingest_bounded(s, rows, s.cfg.mem_wait)
	return accepted
}

// batch_partitions_present reports whether every day in the batch already has a
// partition, so the common case never needs the exclusive lock.
batch_partitions_present :: proc(s: ^Store, rows: []Ingest_Row) -> bool {
	i := 0
	for i < len(rows) {
		day := common.utc_day(rows[i].ts)
		i = day_run_end(rows, i, day)
		if find_partition(s, day) == nil {
			return false
		}
	}
	return true
}

find_partition :: proc(s: ^Store, day: i64) -> ^Partition {
	for p in s.partitions {
		if p.day == day {
			return p
		}
	}
	return nil
}

day_run_end :: proc(rows: []Ingest_Row, i: int, day: i64) -> int {
	j := i + 1
	for j < len(rows) && common.utc_day(rows[j].ts) == day {
		j += 1
	}
	return j
}

// append_shard_rows copies a run of rows into one shard under a single lock.
append_shard_rows :: proc(p: ^Partition, sh: ^Ingest_Shard, rows: []Ingest_Row, now: i64) -> i64 {
	active := flush_profile_enabled()
	t := prof_stage_start(active)
	sync.mutex_lock(&sh.mu)
	if active {
		sync.atomic_add(&flush_prof.ingest_shard_wait_ns, u64(time.since(t)))
		sync.atomic_add(&flush_prof.ingest_shard_calls, 1)
	}
	if sync.atomic_load(&p.since_ns) == 0 {
		sync.atomic_store(&p.since_ns, now)
	}
	bytes: i64
	for i in 0..<len(rows) {
		bytes += append_mem_row(sh, &rows[i])
	}
	sync.atomic_add(&p.mem_rows, u64(len(rows)))
	sync.atomic_add(&p.mem_bytes, bytes)
	sync.mutex_unlock(&sh.mu)
	return bytes
}

get_partition :: proc(s: ^Store, day: i64) -> ^Partition {
	if p := find_partition(s, day); p != nil {
		return p
	}
	p := new(Partition)
	p.day = day
	day_str := common.format_day(day)
	dir := fmt.aprintf("%s/partitions/%s", s.cfg.data_path, day_str)
	delete(day_str)
	p.dir = dir
	parts_dir := fmt.aprintf("%s/parts", dir)
	os.make_directory_all(parts_dir)
	delete(parts_dir)
	partition_alloc_shards(p, s.cfg.ingest_shards)
	append(&s.partitions, p)
	return p
}

partition_alloc_shards :: proc(p: ^Partition, n: int) {
	p.shards = make([]Ingest_Shard, n)
	for i in 0..<n {
		p.shards[i].rows = make([dynamic]Row, 0, 256)
	}
}

partition_free_shards :: proc(p: ^Partition) {
	for &sh in p.shards {
		delete(sh.rows)
		common.arena_destroy(&sh.arena)
	}
	delete(p.shards)
}

row_bytes :: proc(r: ^Ingest_Row) -> i64 {
	b := i64(len(r.msg) + len(r.fields) * size_of(Field))
	for f in r.fields {
		b += i64(len(f.name) + len(f.value))
	}
	return b
}

append_mem_row :: proc(sh: ^Ingest_Shard, r: ^Ingest_Row) -> i64 {
	fields := common.arena_alloc(&sh.arena, len(r.fields) * size_of(Field), align_of(Field))
	fs := slice.from_ptr(cast(^Field)raw_data(fields), len(r.fields))
	for i in 0..<len(r.fields) {
		fs[i] = Field{
			name = common.arena_str(&sh.arena, r.fields[i].name),
			value = common.arena_str(&sh.arena, r.fields[i].value),
		}
	}
	row := Row{
		ts = r.ts,
		stream = r.stream,
		msg = common.arena_str(&sh.arena, r.msg),
		fields = fs,
	}
	b := row_bytes(r)
	append(&sh.rows, row)
	sh.bytes += b
	return b
}

// ---------------------------------------------------------------- flush

store_partition_due :: proc(s: ^Store, p: ^Partition, now: i64, pressure := false) -> bool {
	n := int(sync.atomic_load(&p.mem_rows))
	if n == 0 {
		return false
	}
	return pressure || n >= s.cfg.flush_max_rows || sync.atomic_load(&p.mem_bytes) >= s.cfg.flush_max_bytes || now - sync.atomic_load(&p.since_ns) >= i64(s.cfg.flush_interval)
}

store_flush_due :: proc(s: ^Store) {
	now := common.now_ns()
	// Hard-cap pressure: once the memtable is over half of mem_max_*, flush
	// everything with rows even if the normal triggers have not fired, so an
	// ingest blocked on the cap gets space on the next worker tick instead of
	// waiting for flush_interval or flush_max_rows.
	limit_rows := i64(s.cfg.mem_max_rows)
	limit_bytes := s.cfg.mem_max_bytes
	pressure := (limit_rows > 0 && i64(sync.atomic_load(&s.mem_rows)) * 2 >= limit_rows) ||
		(limit_bytes > 0 && sync.atomic_load(&s.mem_bytes) * 2 >= limit_bytes)
	due: [dynamic]^Partition
	sync.rw_mutex_shared_lock(&s.mu)
	for p in s.partitions {
		if store_partition_due(s, p, now, pressure) {
			append(&due, p)
		}
	}
	sync.rw_mutex_shared_unlock(&s.mu)
	for p in due {
		store_flush_partition(s, p)
	}
	delete(due)
}

// store_flush_pending reports whether any memtable is due, without flushing it.
store_flush_pending :: proc(s: ^Store) -> bool {
	now := common.now_ns()
	pending := false
	sync.rw_mutex_shared_lock(&s.mu)
	for p in s.partitions {
		if store_partition_due(s, p, now) {
			pending = true
			break
		}
	}
	sync.rw_mutex_shared_unlock(&s.mu)
	return pending
}

store_flush_all :: proc(s: ^Store) {
	sync.rw_mutex_shared_lock(&s.mu)
	all := make([]^Partition, len(s.partitions))
	copy(all, s.partitions[:])
	sync.rw_mutex_shared_unlock(&s.mu)
	for p in all {
		store_flush_partition(s, p)
	}
	delete(all)
}

// store_flush_partition swaps the whole memtable and writes it out in parts of
// at most flush_max_rows rows. A memtable can grow far past flush_max_rows
// while a previous flush runs (or after a burst), and sorting/encoding it as
// one unit is both slower per row and unbounded in peak memory. Writing
// bounded chunks keeps every part at the configured size and keeps the sort
// scratch buffer and encoder working sets small.
store_flush_partition :: proc(s: ^Store, p: ^Partition) -> bool {
	active := flush_profile_enabled()
	t_entry := prof_stage_start(active)
	t_locked: time.Time
	begin := common.now_ns()
	stolen: [dynamic][]Row
	old_arenas: [dynamic]common.Arena
	defer {
		delete(stolen)
		delete(old_arenas)
	}
	// The swap still takes the write lock: it keeps readers (queries scan the
	// memtable under a shared lock) from observing rows leave the memtable
	// without the part they were flushed into being visible yet. Ingest only
	// holds a shared lock, so ingest-vs-ingest is no longer serialised here.
	sync.rw_mutex_lock(&s.mu)
	if active {
		t_locked = time.now()
		flush_prof.flush_wait_ns += u64(time.since(t_entry))
	}
	// Steal every shard's contents. Only the owning shard lock is taken, so an
	// ingest batch appending to another shard never waits for this swap.
	total := 0
	stolen_bytes: i64
	for &sh in p.shards {
		sync.mutex_lock(&sh.mu)
		n := len(sh.rows)
		if n > 0 {
			append(&stolen, sh.rows[:])
			append(&old_arenas, sh.arena)
			sync.atomic_sub(&p.mem_rows, u64(n))
			sync.atomic_sub(&p.mem_bytes, sh.bytes)
			total += n
			stolen_bytes += sh.bytes
			sh.rows = make([dynamic]Row, 0, 4096)
			sh.arena = {}
			sh.bytes = 0
		}
		sync.mutex_unlock(&sh.mu)
	}
	if sync.atomic_load(&p.mem_rows) == 0 {
		sync.atomic_store(&p.since_ns, 0)
	}
	if total > 0 {
		// Return the stolen space to the store-wide cap and wake any ingest
		// blocked on it. Still under mu: the lock order is mu -> space_mu.
		sync.mutex_lock(&s.space_mu)
		sync.atomic_sub(&s.mem_rows, u64(total))
		sync.atomic_sub(&s.mem_bytes, stolen_bytes)
		sync.cond_broadcast(&s.space_cond)
		sync.mutex_unlock(&s.space_mu)
	}
	sync.rw_mutex_unlock(&s.mu)
	if active {
		flush_prof.flush_swap_ns += u64(time.since(t_locked))
	}
	if total == 0 {
		if active {
			flush_prof.flush_calls += 1
			flush_prof.flush_total_ns += u64(time.since(t_entry))
		}
		return true
	}
	old_rows := make([]Row, total)
	defer delete(old_rows)
	off := 0
	for sr in stolen {
		copy(old_rows[off:], sr[:])
		off += len(sr)
		delete(sr)
	}
	clear(&stolen)

	chunk := s.cfg.flush_max_rows
	if chunk <= 0 {
		chunk = len(old_rows)
	}
	// done counts the rows already written to durable parts; only the rest is
	// restored into the memtable if a chunk fails.
	done := 0
	ok := true
	for done < len(old_rows) {
		n := min(chunk, len(old_rows) - done)
		rows := old_rows[done:done + n]
		t := prof_stage_start(active)
		sort_rows(rows, s.cfg.flush_threads)
		if active {
			ph := &flush_prof.phase[int(Prof_Scope.Flush)]
			ph.sort_ns += prof_stage_end(true, t)
			ph.sorts += 1
		}
		seq := sync.atomic_add(&s.seq, u64(1))
		part_dir := fmt.aprintf("%s/parts/p_%016x", p.dir, seq)
		part, wok := write_part_parallel(part_dir, rows, s.cfg.block_rows, s.cfg.flush_threads)
		delete(part_dir)
		if !wok {
			ok = false
			fmt.eprintf("oboe: flush failed for %s (%d rows, %d of %d done)\n", p.dir, n, done, len(old_rows))
			break
		}
		sync.mutex_lock(&s.parts_mu)
		append(&p.parts, part)
		sync.mutex_unlock(&s.parts_mu)
		sync.atomic_add(&s.flushed_parts, 1)
		sync.atomic_add(&s.flushed_rows, u64(n))
		done += n
	}
	if done < len(old_rows) {
		failed := make([]Ingest_Row, len(old_rows) - done)
		for i in 0 ..< len(failed) {
			r := &old_rows[done + i]
			failed[i] = Ingest_Row{ts = r.ts, stream = r.stream, msg = r.msg, fields = r.fields}
		}
		sync.rw_mutex_shared_lock(&s.mu)
		now := common.now_ns()
		back_bytes := append_shard_rows(p, &p.shards[0], failed, now)
		// The rows are live again, so put their space back in the cap. The flush
		// just released it, which is why this must not be treated as overshoot.
		sync.mutex_lock(&s.space_mu)
		sync.atomic_add(&s.mem_rows, u64(len(failed)))
		sync.atomic_add(&s.mem_bytes, back_bytes)
		sync.mutex_unlock(&s.space_mu)
		sync.rw_mutex_shared_unlock(&s.mu)
		delete(failed)
	}
	for &a in old_arenas {
		common.arena_destroy(&a)
	}
	if active {
		flush_prof.flush_calls += 1
		flush_prof.flush_rows += u64(len(old_rows))
		flush_prof.flush_total_ns += u64(time.since(t_entry))
	}
	sync.atomic_store(&s.flush_ms_last, u64((common.now_ns() - begin) / 1_000_000))
	return ok
}

// ---------------------------------------------------------------- merge

// store_merge_all merges partitions that have accumulated merge_threshold
// parts. max_partitions > 0 caps how many partitions are merged in this round.
// It returns the number of merges performed so the merge worker can keep going
// until the backlog is drained.
store_merge_all :: proc(s: ^Store, max_partitions := 0) -> int {
	// Merging fewer than two parts is a no-op, and a threshold below two would
	// make every poll rewrite the same data.
	if s.cfg.merge_threshold < 2 {
		return 0
	}
	sync.mutex_lock(&s.merge_mu)
	defer sync.mutex_unlock(&s.merge_mu)
	sync.rw_mutex_shared_lock(&s.mu)
	candidates := make([]^Partition, len(s.partitions))
	copy(candidates, s.partitions[:])
	sync.rw_mutex_shared_unlock(&s.mu)
	merged := 0
	for p in candidates {
		if max_partitions > 0 && merged >= max_partitions {
			break
		}
		if store_partition_parts_count(s, p) >= s.cfg.merge_threshold {
			store_merge_partition(s, p)
			merged += 1
		}
	}
	delete(candidates)
	return merged
}

store_partition_parts_count :: proc(s: ^Store, p: ^Partition) -> int {
	sync.mutex_lock(&s.parts_mu)
	defer sync.mutex_unlock(&s.parts_mu)
	return len(p.parts)
}

store_merge_partition :: proc(s: ^Store, p: ^Partition) {
	sync.mutex_lock(&s.parts_mu)
	if len(p.parts) < 2 {
		sync.mutex_unlock(&s.parts_mu)
		return
	}
	chosen := make([dynamic]^Disk_Part, 0, len(p.parts))
	total_rows := 0
	for part in p.parts {
		total_rows += part.meta.rows
	}
	if total_rows <= s.cfg.merge_max_rows {
		append(&chosen, ..p.parts[:])
	} else {
		used := make([]bool, len(p.parts))
		for _ in 0..<s.cfg.merge_threshold {
			best := -1
			for i in 0..<len(p.parts) {
				if used[i] {
					continue
				}
				if best < 0 || p.parts[i].meta.rows < p.parts[best].meta.rows {
					best = i
				}
			}
			if best < 0 {
				break
			}
			used[best] = true
			append(&chosen, p.parts[best])
		}
		delete(used)
	}
	sync.mutex_unlock(&s.parts_mu)
	if len(chosen) < 2 {
		delete(chosen)
		return
	}
	// Keep the sources readable for the whole merge: retention may drop their
	// partition meanwhile, and an unlinked part with no reference closes its files.
	for m in chosen {
		part_retain(m)
	}
	defer {
		for m in chosen {
			part_release(m)
		}
		delete(chosen)
	}
	// A merge can run for many seconds; letting it finish on the way out is how a
	// SIGTERM turns into a multi-second shutdown. Bail before doing any work, and
	// again at every block boundary, so store_close waits for at most one block.
	if sync.atomic_load(&s.stop) != 0 {
		return
	}
	active := flush_profile_enabled()
	t_entry := prof_stage_start(active)
	chosen_rows: u64
	chosen_n := len(chosen)
	for m in chosen {
		chosen_rows += u64(m.meta.rows)
	}
	defer {
		if active {
			flush_prof.merge_calls += 1
			flush_prof.merge_rows += chosen_rows
			flush_prof.merged_parts += u64(chosen_n)
			flush_prof.merge_total_ns += u64(time.since(t_entry))
		}
	}
	seq := sync.atomic_add(&s.seq, u64(1))
	part_dir := fmt.aprintf("%s/parts/p_%016x", p.dir, seq)
	defer delete(part_dir)
	ps := part_stream_begin(part_dir, s.cfg.block_rows, s.cfg.flush_threads)
	if !ps.ok {
		return
	}
	// Rows are copied into the stream window as they are consumed, so a cursor can
	// free its decoded block immediately; the window is encoded in parallel when
	// it fills up, keeping the part layout identical to a flush.
	cursors := make([]Part_Cursor, len(chosen))
	any := false
	failed := false
	for i in 0..<len(chosen) {
		cursor_init(&cursors[i], chosen[i])
		if cursor_advance(&cursors[i]) {
			any = true
		}
		if cursors[i].err {
			failed = true
		}
	}
	if failed {
		// Leave the sources in place: a merged part built from unreadable blocks
		// would silently lose the rows they hold.
		for i in 0..<len(cursors) {
			cursor_destroy(&cursors[i])
		}
		delete(cursors)
		part_stream_abort(&ps)
		return
	}
	t_loop := prof_stage_start(active)
	for any {
		best := -1
		for i in 0..<len(cursors) {
			c := &cursors[i]
			if !c.has_row {
				continue
			}
			if best < 0 || c.row.stream < cursors[best].row.stream || (c.row.stream == cursors[best].row.stream && c.row.ts < cursors[best].row.ts) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		if sync.atomic_load(&s.stop) != 0 {
			// Shutdown: abandon the merged part. The source parts are still in
			// place, so dropping this merge loses no data; the next start (or the
			// next merge tick) redoes it.
			for i in 0..<len(cursors) {
				cursor_destroy(&cursors[i])
			}
			delete(cursors)
			part_stream_abort(&ps)
			return
		}
		if !part_stream_add(&ps, cursors[best].row) {
			for i in 0..<len(cursors) {
				cursor_destroy(&cursors[i])
			}
			delete(cursors)
			part_stream_abort(&ps)
			return
		}
		cursor_advance(&cursors[best])
		if cursors[best].err {
			for i in 0..<len(cursors) {
				cursor_destroy(&cursors[i])
			}
			delete(cursors)
			part_stream_abort(&ps)
			return
		}
		any = false
		for i in 0..<len(cursors) {
			if cursors[i].has_row {
				any = true
				break
			}
		}
	}
	if active {
		flush_prof.merge_loop_ns += prof_stage_end(true, t_loop)
	}
	for i in 0..<len(cursors) {
		cursor_destroy(&cursors[i])
	}
	delete(cursors)
	part, ok := part_stream_finish(&ps)
	if !ok {
		return
	}
	// Record which parts this one replaces. If the process dies after the
	// merged part is durable but before the old directories are unlinked,
	// the next start uses this list to skip the stale inputs.
	marker := common.buf_init(len(chosen) * 20)
	for m in chosen {
		common.buf_string(&marker, m.meta.id)
		common.buf_byte(&marker, '\n')
	}
	marker_path := fmt.aprintf("%s/merged_from", part.dir)
	if werr := os.write_entire_file(marker_path, marker.data[:]); werr != nil {
		fmt.eprintf("oboe: failed to write merge marker for %s\n", part.dir)
	}
	delete(marker_path)
	common.buf_destroy(&marker)
	sync.mutex_lock(&s.parts_mu)
	kept: [dynamic]^Disk_Part
	dropped: [dynamic]^Disk_Part
	for old in p.parts {
		drop := false
		for m in chosen {
			if m == old {
				drop = true
				break
			}
		}
		if drop {
			append(&dropped, old)
		} else {
			append(&kept, old)
		}
	}
	delete(p.parts)
	p.parts = kept
	append(&p.parts, part)
	// Readers that captured the old parts keep them alive until they release
	// the reference; otherwise the directories are removed here. Only parts still
	// in the list are unlinked: retention may have removed them already.
	for old in dropped {
		part_unlink(old)
	}
	sync.mutex_unlock(&s.parts_mu)
	delete(dropped)
	sync.atomic_add(&s.merged_parts, u64(len(chosen)))
}

// ---------------------------------------------------------------- query iterators

Part_Cursor :: struct {
	part:      ^Disk_Part,
	block_idx: int,
	decoded:   Decoded_Block,
	next_row:  int,
	row:       Row,
	has_row:   bool,
	// err records that a source block could not be decoded. A merge must abort on
	// it: dropping the rows silently and then unlinking the source parts would
	// lose data whenever a read fails (for example under file-descriptor pressure).
	err: bool,
}

cursor_init :: proc(c: ^Part_Cursor, p: ^Disk_Part) {
	c.part = p
	c.block_idx = 0
	c.next_row = 0
	c.has_row = false
	c.err = false
}

cursor_destroy :: proc(c: ^Part_Cursor) {
	if c.decoded.buf != nil {
		decoded_destroy(&c.decoded)
	}
	c.has_row = false
}

cursor_advance :: proc(c: ^Part_Cursor, pins: ^[dynamic]Decoded_Block = nil) -> bool {
	for {
		if c.decoded.buf != nil && c.next_row < c.decoded.nrows {
			c.row = c.decoded.rows[c.next_row]
			c.next_row += 1
			c.has_row = true
			return true
		}
		if c.decoded.buf != nil {
			// A merge hands the exhausted block over instead of freeing it, because
			// rows already buffered in the part writer still point into it.
			if pins != nil {
				append(pins, c.decoded)
				if flush_profile_enabled() {
					flush_prof.merge_pin_blocks += 1
					flush_prof.merge_pin_bytes += u64(len(c.decoded.buf))
				}
			} else {
				decoded_destroy(&c.decoded)
			}
			c.decoded = {}
		}
		if c.block_idx >= len(c.part.index) {
			c.has_row = false
			return false
		}
		bi := &c.part.index[c.block_idx]
		c.block_idx += 1
		active := flush_profile_enabled()
		t := prof_stage_start(active)
		d, ok := read_block(c.part, bi)
		if active {
			flush_prof.merge_read_ns += prof_stage_end(true, t)
			flush_prof.merge_read_blocks += 1
			flush_prof.merge_read_bytes += u64(bi.raw_len)
		}
		if !ok {
			c.err = true
			c.has_row = false
			return false
		}
		c.decoded = d
		c.next_row = 0
	}
}

// ---------------------------------------------------------------- retention & stats

store_apply_retention :: proc(s: ^Store) {
	if s.cfg.retention_days <= 0 {
		return
	}
	cutoff := common.utc_day(common.now_ns()) - i64(s.cfg.retention_days)
	sync.rw_mutex_lock(&s.mu)
	kept: [dynamic]^Partition
	removed: [dynamic]^Partition
	for p in s.partitions {
		if p.day < cutoff {
			append(&removed, p)
		} else {
			append(&kept, p)
		}
	}
	delete(s.partitions)
	s.partitions = kept
	// Parts still referenced by an in-flight scan are closed by that scan. The
	// partition directory can only go away when no scan holds any of them. A
	// flush or merge may still hold the partition pointer after it left the
	// catalogue, so its container is only freed at close (see retired).
	sync.mutex_lock(&s.parts_mu)
	for p in removed {
		busy := false
		for part in p.parts {
			if sync.atomic_load(&part.refs) > 0 {
				busy = true
				break
			}
		}
		for part in p.parts {
			part_unlink(part)
		}
		if !busy {
			os.remove_all(p.dir)
		}
		clear(&p.parts)
	}
	sync.mutex_unlock(&s.parts_mu)
	append(&s.retired, ..removed[:])
	sync.rw_mutex_unlock(&s.mu)
	released_rows: u64
	released_bytes: i64
	for p in removed {
		released_rows += sync.atomic_load(&p.mem_rows)
		released_bytes += sync.atomic_load(&p.mem_bytes)
		// part_unlink above already closed (or deferred the close of) every part.
		// The shards and the container stay allocated until close: a flush may still
		// be writing a part for this partition and touches both.
	}
	delete(removed)
	// Dropping a partition drops whatever it still held in its memtable, so the
	// store-wide cap accounting has to let go of it too.
	if released_rows > 0 || released_bytes > 0 {
		release_mem(s, int(released_rows), released_bytes)
	}
}

Store_Stats :: struct {
	partitions: int,
	parts:      int,
	mem_rows:   int,
	rows:       int,
	disk_bytes: i64,
	min_ts:     i64,
	max_ts:     i64,
}

store_stats :: proc(s: ^Store) -> Store_Stats {
	st := Store_Stats{min_ts = i64(0x7fffffffffffffff), max_ts = 0}
	sync.rw_mutex_shared_lock(&s.mu)
	sync.mutex_lock(&s.parts_mu)
	for p in s.partitions {
		st.partitions += 1
		st.mem_rows += int(sync.atomic_load(&p.mem_rows))
		for part in p.parts {
			st.parts += 1
			st.rows += part.meta.rows
			st.disk_bytes += part_total_size(part)
			if part.meta.min_ts < st.min_ts {
				st.min_ts = part.meta.min_ts
			}
			if part.meta.max_ts > st.max_ts {
				st.max_ts = part.meta.max_ts
			}
		}
	}
	sync.mutex_unlock(&s.parts_mu)
	sync.rw_mutex_shared_unlock(&s.mu)
	if st.min_ts == i64(0x7fffffffffffffff) {
		st.min_ts = 0
	}
	return st
}
