package odlog

import "core:fmt"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:sync"

Out_Field :: struct {
	name:      string,
	value:     string,
	is_number: bool,
}

Out_Row :: struct {
	ts:       i64,
	has_time: bool,
	stream:   u64,
	msg:      string,
	fields:   []Out_Field,
}

Query_Result :: struct {
	rows:           []Out_Row,
	arena:          Arena,
	scanned_rows:   int,
	matched_rows:   int,
	scanned_blocks: int,
	pruned_blocks:  int,
	groups:         int,
	truncated:      bool,
	elapsed_ns:     i64,
}

result_destroy :: proc(r: ^Query_Result) {
	if r.rows != nil {
		delete(r.rows)
	}
	arena_destroy(&r.arena)
	r^ = {}
}

// ---------------------------------------------------------------- matching

str_eq_ci :: proc(a, b: string) -> bool {
	if len(a) != len(b) {
		return false
	}
	for i in 0..<len(a) {
		if !ascii_eq_ci(a[i], b[i]) {
			return false
		}
	}
	return true
}

row_get_field :: proc(r: ^Row, name: string) -> (string, bool) {
	if name == "_msg" {
		return r.msg, true
	}
	if name == "_time" {
		return "", true
	}
	return row_field(r, name)
}

filter_bloom_may_match :: proc(f: ^Filter, b: ^Bloom) -> bool {
	switch f.kind {
	case .AND:
		for k in f.kids {
			if !filter_bloom_may_match(k, b) {
				return false
			}
		}
		return true
	case .OR:
		for k in f.kids {
			if filter_bloom_may_match(k, b) {
				return true
			}
		}
		return false
	case .WORD:
		if len(f.value) == 0 || len(f.value) > 512 {
			return true
		}
		return bloom_check_scoped(b, f.field, f.value, true)
	case .EXACT:
		if len(f.value) == 0 || len(f.value) > 512 {
			return true
		}
		return bloom_check_scoped(b, f.field, f.value, false)
	case .NOT, .PHRASE, .PREFIX, .EXISTS, .RANGE, .TIME_RANGE, .ALL:
		return true
	}
	return true
}

numeric :: proc(s: string) -> (f64, bool) {
	return strconv.parse_f64(s)
}

range_match :: proc(v: string, op: Compare_Op, want: string) -> bool {
	if vn, vok := numeric(v); vok {
		if wn, wok := numeric(want); wok {
			switch op {
			case .EQ:
				return vn == wn
			case .GT:
				return vn > wn
			case .GTE:
				return vn >= wn
			case .LT:
				return vn < wn
			case .LTE:
				return vn <= wn
			}
		}
	}
	c := strings.compare(v, want)
	switch op {
	case .EQ:
		return c == 0
	case .GT:
		return c > 0
	case .GTE:
		return c >= 0
	case .LT:
		return c < 0
	case .LTE:
		return c <= 0
	}
	return false
}

exec_match :: proc(f: ^Filter, r: ^Row) -> bool {
	switch f.kind {
	case .ALL:
		return true
	case .AND:
		for k in f.kids {
			if !exec_match(k, r) {
				return false
			}
		}
		return true
	case .OR:
		for k in f.kids {
			if exec_match(k, r) {
				return true
			}
		}
		return false
	case .NOT:
		if len(f.kids) == 0 {
			return true
		}
		return !exec_match(f.kids[0], r)
	case .TIME_RANGE:
		if f.has_start && r.ts < f.ts_start {
			return false
		}
		if f.has_end && r.ts > f.ts_end {
			return false
		}
		return true
	case .EXISTS:
		v, ok := row_get_field(r, f.field)
		return ok && len(v) > 0
	case .WORD, .PHRASE, .EXACT, .PREFIX, .RANGE:
		v, ok := row_get_field(r, f.field)
		if !ok {
			return false
		}
		#partial switch f.kind {
		case .WORD:
			return has_token_ci(v, f.value)
		case .PHRASE:
			return contains_ci(v, f.value)
		case .EXACT:
			return str_eq_ci(v, f.value)
		case .PREFIX:
			return starts_with_ci(v, f.value)
		case .RANGE:
			return range_match(v, f.cmp, f.value)
		}
	}
	return false
}

// ---------------------------------------------------------------- output copy

arena_fields :: proc(a: ^Arena, n: int) -> []Out_Field {
	if n <= 0 {
		return nil
	}
	bytes := arena_alloc(a, n * size_of(Out_Field), align_of(Out_Field))
	return slice.from_ptr(cast(^Out_Field)raw_data(bytes), n)
}

intern_name :: proc(names: ^map[string]string, a: ^Arena, name: string) -> string {
	if v, ok := names[name]; ok {
		return v
	}
	clone := arena_str(a, name)
	names[clone] = clone
	return clone
}

copy_row_out :: proc(r: ^Row, a: ^Arena, names: ^map[string]string) -> Out_Row {
	out := Out_Row{
		ts       = r.ts,
		has_time = true,
		stream   = r.stream,
		msg      = arena_str(a, r.msg),
	}
	out.fields = arena_fields(a, len(r.fields))
	for i in 0..<len(r.fields) {
		out.fields[i] = Out_Field{
			name  = intern_name(names, a, r.fields[i].name),
			value = arena_str(a, r.fields[i].value),
		}
	}
	return out
}

// ---------------------------------------------------------------- sorting

out_row_get :: proc(r: ^Out_Row, name: string) -> (string, bool) {
	if name == "_msg" {
		return r.msg, true
	}
	for f in r.fields {
		if f.name == name {
			return f.value, true
		}
	}
	return "", false
}

compare_out_rows :: proc(a, b: ^Out_Row, name: string) -> int {
	if name == "_time" {
		if a.ts < b.ts {
			return -1
		}
		if a.ts > b.ts {
			return 1
		}
		return 0
	}
	av, _ := out_row_get(a, name)
	bv, _ := out_row_get(b, name)
	an, aok := numeric(av)
	bn, bok := numeric(bv)
	if aok && bok && an != bn {
		if an < bn {
			return -1
		}
		return 1
	}
	return strings.compare(av, bv)
}

out_row_less :: proc(a, b: ^Out_Row, specs: []Sort_Spec) -> bool {
	for s in specs {
		c := compare_out_rows(a, b, s.name)
		if c != 0 {
			if s.desc {
				return c > 0
			}
			return c < 0
		}
	}
	return false
}

sort_out_rows :: proc(rows: []Out_Row, specs: []Sort_Spec) {
	if len(rows) < 2 || len(specs) == 0 {
		return
	}
	tmp := make([]Out_Row, len(rows))
	defer delete(tmp)
	width := 1
	for width < len(rows) {
		for lo := 0; lo < len(rows); lo += 2 * width {
			mid := min(lo + width, len(rows))
			hi := min(lo + 2 * width, len(rows))
			i, j, k := lo, mid, lo
			for i < mid && j < hi {
				if out_row_less(&rows[j], &rows[i], specs) {
					tmp[k] = rows[j]
					j += 1
				} else {
					tmp[k] = rows[i]
					i += 1
				}
				k += 1
			}
			for i < mid {
				tmp[k] = rows[i]
				i += 1
				k += 1
			}
			for j < hi {
				tmp[k] = rows[j]
				j += 1
				k += 1
			}
		}
		copy(rows, tmp[:len(rows)])
		width *= 2
	}
}

// ---------------------------------------------------------------- pipes

project_row :: proc(r: ^Out_Row, keep: []string, a: ^Arena) {
	if len(keep) == 0 {
		return
	}
	n := 0
	for f in r.fields {
		for k in keep {
			if f.name == k {
				n += 1
				break
			}
		}
	}
	if n == len(r.fields) {
		return
	}
	out := arena_fields(a, n)
	oi := 0
	for f in r.fields {
		for k in keep {
			if f.name == k {
				out[oi] = f
				oi += 1
				break
			}
		}
	}
	r.fields = out
}

// ---------------------------------------------------------------- stats

Agg_State :: struct {
	count: u64,
	n:     u64,
	sum:   f64,
	min:   f64,
	max:   f64,
	seen:  bool,
}

Stats_Group :: struct {
	by:     []Out_Field,
	states: []Agg_State,
}

// ---------------------------------------------------------------- execution

DEFAULT_QUERY_LIMIT :: 1000
MAX_QUERY_LIMIT :: 1_000_000

Scan_Tally :: struct {
	scanned_rows:   int,
	matched_rows:   int,
	scanned_blocks: int,
	pruned_blocks:  int,
}

Exec_State :: struct {
	res:      ^Query_Result,
	names:    ^map[string]string,
	rows:     ^[dynamic]Out_Row,
	groups:   ^[dynamic]Stats_Group,
	group_index: ^map[string]int,
	keybuf:   ^Buf,
	by:       []string,
	aggs:     []Agg_Spec,
	stats:    bool,
	has_sort: bool,
	specs:    []Sort_Spec,
	trim_at:  int,
	limit:    int,
}

ensure_group :: proc(st: ^Exec_State, r: ^Row) -> int {
	keybuf := st.keybuf
	buf_reset(keybuf)
	for name in st.by {
		v, _ := row_get_field(r, name)
		buf_string(keybuf, v)
		buf_byte(keybuf, 0)
	}
	ks := string(keybuf.data[:])
	if gi, ok := st.group_index[ks]; ok {
		return gi
	}
	owned := arena_str(&st.res.arena, ks)
	gi := len(st.groups)
	st.group_index[owned] = gi
	g := Stats_Group{states = make([]Agg_State, len(st.aggs))}
	g.by = arena_fields(&st.res.arena, len(st.by))
	for k in 0..<len(st.by) {
		v, _ := row_get_field(r, st.by[k])
		g.by[k] = Out_Field{
			name  = intern_name(st.names, &st.res.arena, st.by[k]),
			value = arena_str(&st.res.arena, v),
		}
	}
	append(st.groups, g)
	return gi
}

stats_update :: proc(g: ^Stats_Group, r: ^Row, aggs: []Agg_Spec) {
	for i in 0..<len(aggs) {
		st := &g.states[i]
		a := &aggs[i]
		st.count += 1
		if a.kind == .COUNT {
			continue
		}
		v, ok := row_get_field(r, a.field)
		if !ok {
			continue
		}
		n, nok := numeric(v)
		if !nok {
			continue
		}
		st.sum += n
		st.n += 1
		if !st.seen {
			st.min = n
			st.max = n
			st.seen = true
		} else {
			if n < st.min {
				st.min = n
			}
			if n > st.max {
				st.max = n
			}
		}
	}
}

// exec_consume reports whether the scan may stop early.
exec_consume :: proc(st: ^Exec_State, r: ^Row) -> bool {
	if st.stats {
		gi := ensure_group(st, r)
		stats_update(&st.groups[gi], r, st.aggs)
		return false
	}
	append(st.rows, copy_row_out(r, &st.res.arena, st.names))
	if st.has_sort {
		if len(st.rows) >= st.trim_at {
			sort_out_rows(st.rows[:], st.specs)
			resize(st.rows, st.limit)
		}
		return false
	}
	return len(st.rows) >= st.limit
}

execute_query :: proc(s: ^Store, q: ^Query, default_limit := DEFAULT_QUERY_LIMIT) -> Query_Result {
	res: Query_Result
	t0 := now_ns()

	limit, _ := query_limit(q, default_limit)
	if limit <= 0 {
		limit = default_limit
	}
	if limit <= 0 {
		limit = DEFAULT_QUERY_LIMIT
	}
	if limit > MAX_QUERY_LIMIT {
		limit = MAX_QUERY_LIMIT
	}

	has_sort := false
	sort_specs: []Sort_Spec
	for p in q.pipes {
		if p.kind == .SORT {
			has_sort = true
			sort_specs = p.sort
			break
		}
	}
	stats_idx := -1
	for p, i in q.pipes {
		if p.kind == .STATS {
			stats_idx = i
			break
		}
	}
	fields_pipe := -1
	for p, i in q.pipes {
		if p.kind == .FIELDS {
			fields_pipe = i
		}
	}
	want_stats := stats_idx >= 0

	rows: [dynamic]Out_Row
	names: map[string]string
	defer delete_map(names)

	groups: [dynamic]Stats_Group
	group_index: map[string]int
	defer {
		for g in groups {
			delete(g.states)
		}
		delete(groups)
		delete_map(group_index)
	}

	by: []string
	aggs: []Agg_Spec
	if want_stats {
		by = q.pipes[stats_idx].by
		aggs = q.pipes[stats_idx].aggs
	}

	keybuf := buf_init(256)
	defer buf_destroy(&keybuf)

	st := Exec_State{
		res      = &res,
		names    = &names,
		rows     = &rows,
		groups   = &groups,
		group_index = &group_index,
		keybuf   = &keybuf,
		by       = by,
		aggs     = aggs,
		stats    = want_stats,
		has_sort = has_sort,
		specs    = sort_specs,
		limit    = limit,
		trim_at  = max(limit * 2, 4096),
	}
	// A stats pipe without a `by` clause must still report a zero group when
	// nothing matches.
	if want_stats && len(by) == 0 {
		g := Stats_Group{states = make([]Agg_State, len(aggs))}
		append(&groups, g)
		group_index[""] = 0
	}

	tally := Scan_Tally{}
	stop := false

	sync.rw_mutex_shared_lock(&s.mu)
	for p in s.partitions {
		if stop {
			break
		}
		day_start := p.day * DAY_NS
		if q.has_start && day_start + DAY_NS - 1 < q.start {
			continue
		}
		if q.has_end && day_start > q.end {
			continue
		}
		for part in p.parts {
			if stop {
				break
			}
			for &bi in part.index {
				if stop {
					break
				}
				blk := &bi
				if q.has_start && blk.max_ts < q.start {
					continue
				}
				if q.has_end && blk.min_ts > q.end {
					continue
				}
				if q.filter != nil {
					bl := part_bloom(part, blk)
					if bl.bits != nil && !filter_bloom_may_match(q.filter, &bl) {
						tally.pruned_blocks += 1
						continue
					}
				}
				d, ok := read_block(part, blk)
				if !ok {
					continue
				}
				tally.scanned_blocks += 1
				tally.scanned_rows += d.nrows
				for ri in 0..<d.nrows {
					r := &d.rows[ri]
					if q.has_start && r.ts < q.start {
						continue
					}
					if q.has_end && r.ts > q.end {
						continue
					}
					if q.filter != nil && !exec_match(q.filter, r) {
						continue
					}
					tally.matched_rows += 1
					if exec_consume(&st, r) {
						stop = true
						break
					}
				}
				decoded_destroy(&d)
			}
		}
		if stop {
			break
		}
		for ri in 0..<len(p.mem.rows) {
			r := &p.mem.rows[ri]
			if q.has_start && r.ts < q.start {
				continue
			}
			if q.has_end && r.ts > q.end {
				continue
			}
			if q.filter != nil && !exec_match(q.filter, r) {
				continue
			}
			tally.matched_rows += 1
			if exec_consume(&st, r) {
				stop = true
				break
			}
		}
	}
	sync.rw_mutex_shared_unlock(&s.mu)

	if want_stats {
		for i in 0..<len(groups) {
			g := &groups[i]
			out := Out_Row{fields = arena_fields(&res.arena, len(by) + len(aggs))}
			for k in 0..<len(by) {
				out.fields[k] = g.by[k]
			}
			for k in 0..<len(aggs) {
				a := &aggs[k]
				state := &g.states[k]
				value: string
				is_num := true
				switch a.kind {
				case .COUNT:
					value = fmt.tprintf("%d", i64(state.count))
				case .SUM:
					if state.n == 0 {
						is_num = false
					} else {
						value = fmt.tprintf("%v", state.sum)
					}
				case .AVG:
					if state.n == 0 {
						is_num = false
					} else {
						value = fmt.tprintf("%v", state.sum / f64(state.n))
					}
				case .MIN:
					if !state.seen {
						is_num = false
					} else {
						value = fmt.tprintf("%v", state.min)
					}
				case .MAX:
					if !state.seen {
						is_num = false
					} else {
						value = fmt.tprintf("%v", state.max)
					}
				}
				out.fields[len(by) + k] = Out_Field{
					name      = arena_str(&res.arena, a.out),
					value     = arena_str(&res.arena, value),
					is_number = is_num,
				}
			}
			append(&rows, out)
		}
	}

	if has_sort {
		sort_out_rows(rows[:], sort_specs)
	}
	if fields_pipe >= 0 {
		keep := q.pipes[fields_pipe].fields
		for i in 0..<len(rows) {
			project_row(&rows[i], keep, &res.arena)
		}
	}
	if len(rows) > limit {
		resize(&rows, limit)
		res.truncated = true
	}

	res.rows = rows[:]
	res.scanned_rows = tally.scanned_rows
	res.matched_rows = tally.matched_rows
	res.scanned_blocks = tally.scanned_blocks
	res.pruned_blocks = tally.pruned_blocks
	res.elapsed_ns = now_ns() - t0
	if want_stats {
		res.groups = len(res.rows)
	}
	return res
}
