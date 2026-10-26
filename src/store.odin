package odlog

import "core:fmt"
import "core:os"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:sync"
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
	}
}

Mem_Part :: struct {
	rows:     [dynamic]Row,
	arena:    Arena,
	bytes:    i64,
	since_ns: i64,
}

Partition :: struct {
	day:   i64,
	dir:   string,
	mem:   Mem_Part,
	parts: [dynamic]^Disk_Part,
}

Store :: struct {
	cfg:        Store_Config,
	mu:         sync.RW_Mutex,
	partitions: [dynamic]^Partition,
	worker:     ^thread.Thread,
	seq:        u64,
	stop:       u32,
	// metrics
	ingested_rows:  u64,
	ingested_bytes: u64,
	flushed_parts:  u64,
	merged_parts:   u64,
	queries:        u64,
	dropped_rows:   u64,
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

// ---------------------------------------------------------------- lifecycle

store_init :: proc(cfg: Store_Config, start_worker := true) -> ^Store {
	s := new(Store)
	s.cfg = cfg
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
						fmt.eprintf("odlog: dropping incomplete part %s\n", pe.fullpath)
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
	}
	delete(partitions_dir)
	if start_worker {
		s.worker = thread.create_and_start_with_data(s, store_worker, self_cleanup = false, name = "odlog-store")
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
	return days_from_civil(y, m, d), true
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
	last_merge := now_ns()
	last_retention := i64(0)
	for sync.atomic_load(&s.stop) == 0 {
		store_flush_due(s)
		now := now_ns()
		if now - last_merge >= 10 * 1_000_000_000 {
			store_merge_all(s)
			last_merge = now
		}
		if now - last_retention >= 60 * 1_000_000_000 {
			store_apply_retention(s)
			last_retention = now
		}
		time.sleep(250 * time.Millisecond)
	}
}

store_close :: proc(s: ^Store) {
	sync.atomic_store(&s.stop, 1)
	if s.worker != nil {
		thread.join(s.worker)
		s.worker = nil
	}
	store_flush_all(s)
	sync.rw_mutex_lock(&s.mu)
	for p in s.partitions {
		for part in p.parts {
			close_part(part)
		}
		delete(p.parts)
		delete(p.mem.rows)
		arena_destroy(&p.mem.arena)
		delete(p.dir)
		free(p)
	}
	delete(s.partitions)
	sync.rw_mutex_unlock(&s.mu)
	free(s)
}

// ---------------------------------------------------------------- ingestion

store_ingest :: proc(s: ^Store, rows: []Ingest_Row) -> int {
	if len(rows) == 0 {
		return 0
	}
	total_bytes: i64
	sync.rw_mutex_lock(&s.mu)
	defer sync.rw_mutex_unlock(&s.mu)
	now := now_ns()
	for i in 0..<len(rows) {
		r := &rows[i]
		if r.ts <= 0 {
			r.ts = now
		}
		day := utc_day(r.ts)
		p := get_partition(s, day)
		append_mem_row(p, r, now)
		total_bytes += i64(len(r.msg))
		for f in r.fields {
			total_bytes += i64(len(f.name) + len(f.value))
		}
	}
	sync.atomic_add(&s.ingested_rows, u64(len(rows)))
	sync.atomic_add(&s.ingested_bytes, u64(total_bytes))
	return len(rows)
}

get_partition :: proc(s: ^Store, day: i64) -> ^Partition {
	for p in s.partitions {
		if p.day == day {
			return p
		}
	}
	p := new(Partition)
	p.day = day
	day_str := format_day(day)
	dir := fmt.aprintf("%s/partitions/%s", s.cfg.data_path, day_str)
	delete(day_str)
	p.dir = dir
	parts_dir := fmt.aprintf("%s/parts", dir)
	os.make_directory_all(parts_dir)
	delete(parts_dir)
	append(&s.partitions, p)
	return p
}

append_mem_row :: proc(p: ^Partition, r: ^Ingest_Row, now: i64) {
	fields := arena_alloc(&p.mem.arena, len(r.fields) * size_of(Field), align_of(Field))
	fs := slice.from_ptr(cast(^Field)raw_data(fields), len(r.fields))
	for i in 0..<len(r.fields) {
		fs[i] = Field{
			name = arena_str(&p.mem.arena, r.fields[i].name),
			value = arena_str(&p.mem.arena, r.fields[i].value),
		}
	}
	row := Row{
		ts = r.ts,
		stream = r.stream,
		msg = arena_str(&p.mem.arena, r.msg),
		fields = fs,
	}
	if len(p.mem.rows) == 0 {
		p.mem.since_ns = now
	}
	append(&p.mem.rows, row)
	p.mem.bytes += i64(len(r.msg) + len(r.fields) * size_of(Field))
	for f in r.fields {
		p.mem.bytes += i64(len(f.name) + len(f.value))
	}
}

// ---------------------------------------------------------------- flush

store_flush_due :: proc(s: ^Store) {
	now := now_ns()
	due: [dynamic]^Partition
	sync.rw_mutex_shared_lock(&s.mu)
	for p in s.partitions {
		n := len(p.mem.rows)
		if n == 0 {
			continue
		}
		if n >= s.cfg.flush_max_rows || p.mem.bytes >= s.cfg.flush_max_bytes || now - p.mem.since_ns >= i64(s.cfg.flush_interval) {
			append(&due, p)
		}
	}
	sync.rw_mutex_shared_unlock(&s.mu)
	for p in due {
		store_flush_partition(s, p)
	}
	delete(due)
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

store_flush_partition :: proc(s: ^Store, p: ^Partition) -> bool {
	sync.rw_mutex_lock(&s.mu)
	if len(p.mem.rows) == 0 {
		sync.rw_mutex_unlock(&s.mu)
		return true
	}
	old_rows := p.mem.rows
	old_arena := p.mem.arena
	p.mem = Mem_Part{rows = make([dynamic]Row, 0, 4096)}
	sync.rw_mutex_unlock(&s.mu)

	slice.sort_by(old_rows[:], row_less)
	seq := sync.atomic_add(&s.seq, u64(1))
	part_dir := fmt.aprintf("%s/parts/p_%016x", p.dir, seq)
	part, ok := write_part(part_dir, old_rows[:], s.cfg.block_rows)
	delete(part_dir)
	if ok {
		sync.rw_mutex_lock(&s.mu)
		append(&p.parts, part)
		sync.rw_mutex_unlock(&s.mu)
		sync.atomic_add(&s.flushed_parts, 1)
	} else {
		fmt.eprintf("odlog: flush failed for %s (%d rows)\n", p.dir, len(old_rows))
		sync.rw_mutex_lock(&s.mu)
		now := now_ns()
		for i in 0..<len(old_rows) {
			r := &old_rows[i]
			append_mem_row(p, &Ingest_Row{ts = r.ts, stream = r.stream, msg = r.msg, fields = r.fields}, now)
		}
		sync.rw_mutex_unlock(&s.mu)
	}
	arena_destroy(&old_arena)
	delete(old_rows)
	return ok
}

// ---------------------------------------------------------------- merge

store_merge_all :: proc(s: ^Store) {
	sync.rw_mutex_shared_lock(&s.mu)
	candidates := make([]^Partition, len(s.partitions))
	copy(candidates, s.partitions[:])
	sync.rw_mutex_shared_unlock(&s.mu)
	for p in candidates {
		if len(p.parts) >= s.cfg.merge_threshold {
			store_merge_partition(s, p)
		}
	}
	delete(candidates)
}

store_merge_partition :: proc(s: ^Store, p: ^Partition) {
	sync.rw_mutex_shared_lock(&s.mu)
	if len(p.parts) < 2 {
		sync.rw_mutex_shared_unlock(&s.mu)
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
	sync.rw_mutex_shared_unlock(&s.mu)
	if len(chosen) < 2 {
		delete(chosen)
		return
	}
	seq := sync.atomic_add(&s.seq, u64(1))
	part_dir := fmt.aprintf("%s/parts/p_%016x", p.dir, seq)
	defer delete(part_dir)
	pw := part_writer_begin(part_dir, s.cfg.block_rows, true)
	if pw == nil {
		delete(chosen)
		return
	}
	cursors := make([]Part_Cursor, len(chosen))
	any := false
	for i in 0..<len(chosen) {
		cursor_init(&cursors[i], chosen[i])
		if cursor_advance(&cursors[i]) {
			any = true
		}
	}
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
		if !part_writer_add(pw, cursors[best].row) {
			for i in 0..<len(cursors) {
				cursor_destroy(&cursors[i])
			}
			delete(cursors)
			part_writer_abort(pw)
			delete(chosen)
			return
		}
		cursor_advance(&cursors[best])
		any = false
		for i in 0..<len(cursors) {
			if cursors[i].has_row {
				any = true
				break
			}
		}
	}
	for i in 0..<len(cursors) {
		cursor_destroy(&cursors[i])
	}
	delete(cursors)
	part, ok := part_writer_finish(pw)
	if !ok {
		delete(chosen)
		return
	}
	// Record which parts this one replaces. If the process dies after the
	// merged part is durable but before the old directories are unlinked,
	// the next start uses this list to skip the stale inputs.
	marker := buf_init(len(chosen) * 20)
	for m in chosen {
		buf_string(&marker, m.meta.id)
		buf_byte(&marker, '\n')
	}
	marker_path := fmt.aprintf("%s/merged_from", part.dir)
	if werr := os.write_entire_file(marker_path, marker.data[:]); werr != nil {
		fmt.eprintf("odlog: failed to write merge marker for %s\n", part.dir)
	}
	delete(marker_path)
	buf_destroy(&marker)
	sync.rw_mutex_lock(&s.mu)
	kept: [dynamic]^Disk_Part
	for old in p.parts {
		drop := false
		for m in chosen {
			if m == old {
				drop = true
				break
			}
		}
		if !drop {
			append(&kept, old)
		}
	}
	delete(p.parts)
	p.parts = kept
	append(&p.parts, part)
	sync.rw_mutex_unlock(&s.mu)
	// No readers can hold the old parts after this exclusive section.
	for old in chosen {
		os.remove_all(old.dir)
		close_part(old)
	}
	sync.atomic_add(&s.merged_parts, u64(len(chosen)))
	delete(chosen)
}

// ---------------------------------------------------------------- query iterators

Part_Cursor :: struct {
	part:      ^Disk_Part,
	block_idx: int,
	decoded:   Decoded_Block,
	next_row:  int,
	row:       Row,
	has_row:   bool,
}

cursor_init :: proc(c: ^Part_Cursor, p: ^Disk_Part) {
	c.part = p
	c.block_idx = 0
	c.next_row = 0
	c.has_row = false
}

cursor_destroy :: proc(c: ^Part_Cursor) {
	if c.decoded.buf != nil {
		decoded_destroy(&c.decoded)
	}
	c.has_row = false
}

cursor_advance :: proc(c: ^Part_Cursor) -> bool {
	for {
		if c.decoded.buf != nil && c.next_row < c.decoded.nrows {
			c.row = c.decoded.rows[c.next_row]
			c.next_row += 1
			c.has_row = true
			return true
		}
		if c.decoded.buf != nil {
			decoded_destroy(&c.decoded)
			c.decoded = {}
		}
		if c.block_idx >= len(c.part.index) {
			c.has_row = false
			return false
		}
		bi := &c.part.index[c.block_idx]
		c.block_idx += 1
		d, ok := read_block(c.part, bi)
		if !ok {
			continue
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
	cutoff := utc_day(now_ns()) - i64(s.cfg.retention_days)
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
	sync.rw_mutex_unlock(&s.mu)
	for p in removed {
		for part in p.parts {
			close_part(part)
		}
		delete(p.parts)
		delete(p.mem.rows)
		arena_destroy(&p.mem.arena)
		os.remove_all(p.dir)
		delete(p.dir)
		free(p)
	}
	delete(removed)
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
	for p in s.partitions {
		st.partitions += 1
		st.mem_rows += len(p.mem.rows)
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
	sync.rw_mutex_shared_unlock(&s.mu)
	if st.min_ts == i64(0x7fffffffffffffff) {
		st.min_ts = 0
	}
	return st
}
