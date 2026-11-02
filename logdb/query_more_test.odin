package logdb

import "../common"
import "core:fmt"
import "core:mem"
import "core:os"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:testing"

qm_tmpdir :: proc(name: string) -> string {
	return fmt.aprintf("/tmp/oboe_%s_%d", name, os.get_pid())
}

// Deterministic LCG so a failing case is always reproducible.
Qm_LCG :: struct {
	state: u64,
}

qm_next :: proc(g: ^Qm_LCG) -> u64 {
	g.state = g.state * 6364136223846793005 + 1442695040888963407
	return g.state >> 33
}

Qm_Rec :: struct {
	ts:        i64,
	level:     string,
	level_idx: int,
	svc:       string,
	svc_idx:   int,
	duration:  int,
	msg:       string,
}

qm_records :: proc(n: int, base: i64) -> []Qm_Rec {
	levels := [3]string{"error", "warn", "info"}
	svcs := [4]string{"api", "db", "cache", "worker"}
	recs := make([]Qm_Rec, n)
	g := Qm_LCG{state = 0x9e3779b97f4a7c15}
	for i in 0..<n {
		l := int(qm_next(&g) % 3)
		s := int(qm_next(&g) % 4)
		dur := i + 1
		ts := base + i64(i) * 1_000_000
		if i >= n / 2 {
			ts += common.DAY_NS
		}
		msg := fmt.aprintf("req %d", i)
		if qm_next(&g) % 7 == 0 {
			delete(msg)
			msg = fmt.aprintf("req %d needle", i)
		}
		recs[i] = Qm_Rec{
			ts        = ts,
			level     = levels[l],
			level_idx = l,
			svc       = svcs[s],
			svc_idx   = s,
			duration  = dur,
			msg       = msg,
		}
	}
	return recs
}

Qm_Pred :: enum {
	ALWAYS,
	LEVEL_ERROR,
	LEVEL_ERROR_API,
	LEVEL_PREFIX_ERR,
	DUR_GT_500,
	NOT_ERROR,
	MSG_NEEDLE,
	ERROR_OR_WARN_API,
}

qm_pred :: proc(k: Qm_Pred, r: ^Qm_Rec) -> bool {
	switch k {
	case .ALWAYS:
		return true
	case .LEVEL_ERROR:
		return r.level == "error"
	case .LEVEL_ERROR_API:
		return r.level == "error" && r.svc == "api"
	case .LEVEL_PREFIX_ERR:
		return strings.has_prefix(r.level, "err")
	case .DUR_GT_500:
		return r.duration > 500
	case .NOT_ERROR:
		return r.level != "error"
	case .MSG_NEEDLE:
		return strings.contains(r.msg, "needle")
	case .ERROR_OR_WARN_API:
		return (r.level == "error" || r.level == "warn") && r.svc == "api"
	}
	return false
}

qm_check_filter :: proc(t: ^testing.T, s: ^Store, recs: []Qm_Rec, qs: string, k: Qm_Pred, now: i64) {
	q, ok := parse_query_string(qs, now)
	testing.expectf(t, ok, "query should parse: %s", qs)
	if !ok {
		return
	}
	defer query_destroy(&q)
	res := execute_query(s, &q, 2000)
	defer result_destroy(&res)

	expected := 0
	seen := make(map[string]bool, len(recs))
	defer delete(seen)
	for &r in recs {
		if qm_pred(k, &r) {
			expected += 1
			seen[r.msg] = true
		}
	}

	testing.expectf(t, len(res.rows) == expected, "%s: got %d rows, want %d", qs, len(res.rows), expected)
	testing.expect_value(t, res.matched_rows, expected)
	testing.expect(t, !res.truncated)
	got := 0
	for &row in res.rows {
		if row.msg in seen {
			got += 1
		} else {
			testing.expectf(t, false, "%s: unexpected row %q", qs, row.msg)
		}
	}
	testing.expect_value(t, got, expected)
}

Qm_Sort_Item :: struct {
	msg: string,
	ts:  i64,
	dur: int,
	svc: string,
}

qm_less_time_desc :: proc(a, b: Qm_Sort_Item) -> bool {
	return a.ts > b.ts
}

qm_less_dur_asc :: proc(a, b: Qm_Sort_Item) -> bool {
	if a.dur != b.dur {
		return a.dur < b.dur
	}
	return a.ts < b.ts
}

qm_less_svc_dur_desc :: proc(a, b: Qm_Sort_Item) -> bool {
	if a.svc != b.svc {
		return a.svc < b.svc
	}
	if a.dur != b.dur {
		return a.dur > b.dur
	}
	return a.ts < b.ts
}

qm_check_sort :: proc(
	t: ^testing.T,
	s: ^Store,
	recs: []Qm_Rec,
	qs: string,
	k: Qm_Pred,
	limit: int,
	less: proc(a, b: Qm_Sort_Item) -> bool,
	now: i64,
) {
	q, ok := parse_query_string(qs, now)
	testing.expectf(t, ok, "sort query should parse: %s", qs)
	if !ok {
		return
	}
	defer query_destroy(&q)
	res := execute_query(s, &q, 2000)
	defer result_destroy(&res)

	items := make([dynamic]Qm_Sort_Item, 0, len(recs))
	defer delete(items)
	for &r in recs {
		if qm_pred(k, &r) {
			append(&items, Qm_Sort_Item{msg = r.msg, ts = r.ts, dur = r.duration, svc = r.svc})
		}
	}
	slice.sort_by(items[:], less)
	want := min(limit, len(items))
	testing.expectf(t, len(res.rows) == want, "%s: got %d rows, want %d", qs, len(res.rows), want)
	for i in 0..<min(want, len(res.rows)) {
		testing.expectf(t, res.rows[i].msg == items[i].msg, "%s: row %d is %q, want %q", qs, i, res.rows[i].msg, items[i].msg)
	}
}

qm_abs :: proc(v: f64) -> f64 {
	return v < 0 ? -v : v
}

qm_close :: proc(a, b: f64) -> bool {
	scale := max(1.0, qm_abs(b))
	return qm_abs(a - b) <= 1e-9 * scale
}

@(test)
test_query_more_random_oracle :: proc(t: ^testing.T) {
	N :: 240
	base, _ := common.parse_iso_ns("2026-10-26T00:00:00Z")
	now := base + common.DAY_NS + 60_000_000_000

	dir := qm_tmpdir("query_more_random")
	os.remove_all(dir)
	defer delete(dir)
	defer os.remove_all(dir)

	cfg := default_store_config()
	cfg.data_path = dir
	cfg.flush_interval = 1 << 62
	cfg.flush_max_rows = 1_000_000
	cfg.flush_max_bytes = 1 << 40
	cfg.block_rows = 32
	s := store_init(cfg, false)
	defer store_close(s)

	recs := qm_records(N, base)
	defer {
		for &r in recs {
			delete(r.msg)
		}
		delete(recs)
	}

	fields := make([]Field, N * 3)
	defer delete(fields)
	dur_strs := make([]string, N)
	defer {
		for &d in dur_strs {
			delete(d)
		}
		delete(dur_strs)
	}
	rows := make([]Ingest_Row, N)
	defer delete(rows)
	for i in 0..<N {
		r := &recs[i]
		ds := fmt.aprintf("%d", r.duration)
		dur_strs[i] = ds
		fields[i*3] = Field{name = "level", value = r.level}
		fields[i*3+1] = Field{name = "service", value = r.svc}
		fields[i*3+2] = Field{name = "duration", value = ds}
		rows[i] = Ingest_Row{ts = r.ts, stream = 1, msg = r.msg, fields = fields[i*3:i*3+3]}
	}
	testing.expect_value(t, store_ingest(s, rows), N)
	store_flush_all(s)
	testing.expect_value(t, store_stats(s).rows, N)
	testing.expect_value(t, store_stats(s).partitions, 2)

	// Filter oracle: every predicate is checked against the brute-force record
	// list, never against the storage scan order.
	qm_check_filter(t, s, recs, "*", .ALWAYS, now)
	qm_check_filter(t, s, recs, "level:error", .LEVEL_ERROR, now)
	qm_check_filter(t, s, recs, "level:error service:api", .LEVEL_ERROR_API, now)
	qm_check_filter(t, s, recs, "level:err*", .LEVEL_PREFIX_ERR, now)
	qm_check_filter(t, s, recs, "duration:>500", .DUR_GT_500, now)
	qm_check_filter(t, s, recs, "-level:error", .NOT_ERROR, now)
	qm_check_filter(t, s, recs, "_msg:needle", .MSG_NEEDLE, now)
	qm_check_filter(t, s, recs, "(level:error or level:warn) service:api", .ERROR_OR_WARN_API, now)

	// A first-day-only window must not leak rows from the second-day partition.
	{
		q, ok := parse_query_string("_time:[2026-10-26T00:00:00Z, 2026-10-26T12:00:00Z]", now)
		testing.expect(t, ok)
		if ok {
			res := execute_query(s, &q, 2000)
			defer result_destroy(&res)
			window_end, _ := common.parse_iso_ns("2026-10-26T12:00:00Z")
			want := 0
			for &r in recs {
				if r.ts <= window_end {
					want += 1
				}
			}
			testing.expect_value(t, len(res.rows), want)
			testing.expect_value(t, res.matched_rows, want)
			query_destroy(&q)
		}
	}

	// Sort + limit oracle.
	qm_check_sort(t, s, recs, "* | sort by (_time) desc | limit 5", .ALWAYS, 5, qm_less_time_desc, now)
	qm_check_sort(t, s, recs, "level:error | sort by (duration) | limit 7", .LEVEL_ERROR, 7, qm_less_dur_asc, now)
	qm_check_sort(t, s, recs, "* | sort by (service, duration desc) | limit 25", .ALWAYS, 25, qm_less_svc_dur_desc, now)

	// Stats oracle.
	{
		q, ok := parse_query_string(
			"level:error | stats by (service) count() as total, avg(duration) as avg_d, min(duration) as min_d, max(duration) as max_d, sum(duration) as sum_d",
			now,
		)
		testing.expect(t, ok)
		if ok {
			res := execute_query(s, &q, 2000)
			defer result_destroy(&res)
			counts: [4]int
			seen: [4]bool
			sums: [4]f64
			mins: [4]f64
			maxs: [4]f64
			matched := 0
			for &r in recs {
				if r.level != "error" {
					continue
				}
				matched += 1
				si := r.svc_idx
				if !seen[si] {
					seen[si] = true
					mins[si] = f64(r.duration)
					maxs[si] = f64(r.duration)
				}
				counts[si] += 1
				sums[si] += f64(r.duration)
				if f64(r.duration) < mins[si] {
					mins[si] = f64(r.duration)
				}
				if f64(r.duration) > maxs[si] {
					maxs[si] = f64(r.duration)
				}
			}
			testing.expect_value(t, res.matched_rows, matched)
			groups := 0
			for v in seen {
				if v {
					groups += 1
				}
			}
			testing.expect_value(t, len(res.rows), groups)
			for &row in res.rows {
				svc, _ := out_row_get(&row, "service")
				si := -1
				for name, idx in ([4]string{"api", "db", "cache", "worker"}) {
					if name == svc {
						si = idx
					}
				}
				testing.expectf(t, si >= 0, "stats produced unknown service %q", svc)
				if si < 0 {
					continue
				}
				total, _ := out_row_get(&row, "total")
				testing.expect_value(t, total, fmt.tprintf("%d", counts[si]))
				avg_s, _ := out_row_get(&row, "avg_d")
				min_s, _ := out_row_get(&row, "min_d")
				max_s, _ := out_row_get(&row, "max_d")
				sum_s, _ := out_row_get(&row, "sum_d")
				avg, aok := strconv.parse_f64(avg_s)
				if counts[si] > 0 {
					testing.expect(t, aok)
					testing.expect(t, qm_close(avg, sums[si] / f64(counts[si])))
				}
				mn, mok := strconv.parse_f64(min_s)
				testing.expect(t, mok)
				testing.expect(t, qm_close(mn, mins[si]))
				mx, xok := strconv.parse_f64(max_s)
				testing.expect(t, xok)
				testing.expect(t, qm_close(mx, maxs[si]))
				sm, sok := strconv.parse_f64(sum_s)
				testing.expect(t, sok)
				testing.expect(t, qm_close(sm, sums[si]))
			}
			query_destroy(&q)
		}
	}

	// Stats without `by` on an empty match still reports one zero group.
	{
		q, ok := parse_query_string("level:nosuchxyz | stats count() as total, avg(duration) as avg_d", now)
		testing.expect(t, ok)
		if ok {
			res := execute_query(s, &q, 2000)
			defer result_destroy(&res)
			testing.expect_value(t, len(res.rows), 1)
			testing.expect_value(t, res.groups, 1)
			if len(res.rows) == 1 {
				total, _ := out_row_get(&res.rows[0], "total")
				testing.expect_value(t, total, "0")
				testing.expect_value(t, len(res.rows[0].fields), 2)
				testing.expect(t, !res.rows[0].fields[1].is_number)
				testing.expect_value(t, res.rows[0].fields[1].value, "")
			}
			query_destroy(&q)
		}
	}

	// Stats + sort keeps groups ordered by the aggregate.
	{
		q, ok := parse_query_string("level:error | stats by (service) count() as total | sort by (total) desc", now)
		testing.expect(t, ok)
		if ok {
			res := execute_query(s, &q, 2000)
			defer result_destroy(&res)
			for i in 1..<len(res.rows) {
				prev, _ := out_row_get(&res.rows[i-1], "total")
				cur, _ := out_row_get(&res.rows[i], "total")
				pi, _ := strconv.parse_int(prev, 10)
				ci, _ := strconv.parse_int(cur, 10)
				testing.expect(t, pi >= ci)
			}
			query_destroy(&q)
		}
	}
}

@(test)
test_query_more_limit_bounds :: proc(t: ^testing.T) {
	N :: 240
	base, _ := common.parse_iso_ns("2026-10-26T00:00:00Z")
	now := base + 60_000_000_000

	dir := qm_tmpdir("query_more_limit")
	os.remove_all(dir)
	defer delete(dir)
	defer os.remove_all(dir)

	cfg := default_store_config()
	cfg.data_path = dir
	cfg.flush_interval = 1 << 62
	cfg.flush_max_rows = 1_000_000
	cfg.flush_max_bytes = 1 << 40
	s := store_init(cfg, false)
	defer store_close(s)

	fields_info := []Field{{name = "level", value = "info"}}
	fields_warn := []Field{{name = "level", value = "warn"}}
	rows := make([]Ingest_Row, N)
	defer delete(rows)
	for i in 0..<N {
		fs := fields_info
		if i % 2 == 1 {
			fs = fields_warn
		}
		rows[i] = Ingest_Row{ts = base + i64(i) * 1_000_000, stream = 1, msg = "limit row", fields = fs}
	}
	testing.expect_value(t, store_ingest(s, rows), N)
	store_flush_all(s)

	Qm_Limit_Case :: struct {
		q:          string,
		default_lim: int,
		rows:       int,
	}
	// limit 0 falls back to the caller's default; anything past the row count
	// is not truncated; MAX_QUERY_LIMIT is only a clamp, not an error.
	cases := []Qm_Limit_Case{
		{"* | limit 3", 2000, 3},
		{"* | limit 240", 2000, 240},
		{"* | limit 241", 2000, 240},
		{"* | limit 0", 5, 5},
		{"*", 7, 7},
		{"* | limit 2000000", 2000, 240},
		{"* | limit 1000000000", 2000, 240},
	}
	for c in cases {
		q, ok := parse_query_string(c.q, now)
		testing.expectf(t, ok, "limit query should parse: %s", c.q)
		if !ok {
			continue
		}
		res := execute_query(s, &q, c.default_lim)
		testing.expectf(t, len(res.rows) == c.rows, "%s: got %d rows, want %d", c.q, len(res.rows), c.rows)
		result_destroy(&res)
		query_destroy(&q)
	}

	// Limit after stats resizes the group set and flags truncation.
	q, ok := parse_query_string("level:* | stats by (level) count() as total | limit 1", now)
	testing.expect(t, ok)
	if ok {
		res := execute_query(s, &q, 2000)
		testing.expect_value(t, len(res.rows), 1)
		testing.expect(t, res.truncated)
		result_destroy(&res)
		query_destroy(&q)
	}

	// Invalid limit values are rejected by the parser.
	for bad in ([]string{"* | limit -1", "* | limit abc", "* | limit 1.5"}) {
		_, pok := parse_query_string(bad, now)
		testing.expectf(t, !pok, "limit query should be rejected: %s", bad)
	}
}

@(test)
test_query_more_malformed_corpus :: proc(t: ^testing.T) {
	now := i64(1_800_000_000_000_000_000)
	bad := []string{
		"(", "(level:error", "level:", "level:error |", "level:error | nonsense 1",
		"level:error | limit", "level:error | limit -1", "level:error | limit abc",
		"level:error | sort by", "level:error | sort by (", "level:error | sort by ()",
		"level:error | stats", "level:error | stats by (service) bogus",
		"level:error | stats by (service", "level:error | stats by ()",
		"level:error | stats by (service) count() as",
		"level:error | stats by (service) count( as x",
		"level:error | fields", "level:error or",
		"level:>", "level:error:extra",
		`"\uZZZZ"`, `_msg:"\uZZZZ"`,
		"_time:5x", "_time:[5x, *]", "_time:[*, 5x]", "_time:[2026-10-26T10:00:00Z]",
		"_time:[2026-10-26T10:00:00Z 2026-10-26T10:10:00Z]", "_time:>=",
	}

	// Swap in a tracking allocator so a parser leak fails the test instead of
	// only printing a runner warning.
	track: mem.Tracking_Allocator
	mem.tracking_allocator_init(&track, context.allocator)
	prev := context.allocator
	context.allocator = mem.tracking_allocator(&track)

	for qs in bad {
		q, ok := parse_query_string(qs, now)
		// A case that unexpectedly parses must still release its query.
		if ok {
			query_destroy(&q)
		}
		testing.expectf(t, !ok, "malformed query should be rejected: %s", qs)
	}
	good := []string{
		"*", "level:error", "level:error service:api", "level:err*",
		"_time:5m level:error | sort by (_time) desc | limit 10",
		"level:error | stats by (service) count() as total, avg(duration) as avg_d",
		"level:error | fields service", `_msg:"hello world"`, "level:error | limit 0",
		"a or b", "(level:error or level:warn) service:api", "_time:[*, 2026-10-26T10:00:00Z]",
	}
	for qs in good {
		q, ok := parse_query_string(qs, now)
		testing.expectf(t, ok, "valid query should parse: %s", qs)
		if ok {
			query_destroy(&q)
		}
	}

	leaked := 0
	for _, entry in track.allocation_map {
		leaked += 1
	}
	context.allocator = prev
	testing.expect_value(t, leaked, 0)
	mem.tracking_allocator_destroy(&track)
}

@(test)
test_query_more_time_expressions :: proc(t: ^testing.T) {
	now := i64(1_800_000_000_000_000_000)
	iso10, _ := common.parse_iso_ns("2026-10-26T10:00:00Z")
	iso20, _ := common.parse_iso_ns("2026-10-26T10:10:00Z")
	default_end := now + common.DAY_NS

	Qm_Time_Case :: struct {
		q:         string,
		has_start: bool,
		has_end:   bool,
		start:     i64,
		end:       i64,
	}
	cases := []Qm_Time_Case{
		{"_time:5m", true, true, now - 300_000_000_000, now},
		{"_time:1h", true, true, now - 3_600_000_000_000, now},
		{"_time:90m", true, true, now - 5_400_000_000_000, now},
		{"_time:[2026-10-26T10:00:00Z, 2026-10-26T10:10:00Z]", true, true, iso10, iso20},
		{"_time:[*, 2026-10-26T10:00:00Z]", true, true, 0, iso10},
		{"_time:>=2026-10-26T10:00:00Z", true, true, iso10, iso10},
		{"_time:=2026-10-26T10:00:00Z", true, true, iso10, iso10},
		{"_time:>2026-10-26T10:00:00Z", true, false, iso10 + 1, default_end},
		{"_time:<2026-10-26T10:00:00Z", false, true, 0, iso10 - 1},
		{"_time:<=2026-10-26T10:00:00Z", false, true, 0, iso10},
		{"_time:1700000000", true, true, 1_700_000_000_000_000_000, 1_700_000_000_000_000_000},
		{"_time:1700000000000", true, true, 1_700_000_000_000_000_000, 1_700_000_000_000_000_000},
		{"_time:now-10m", true, true, now - 600_000_000_000, now - 600_000_000_000},
	}
	for c in cases {
		q, ok := parse_query_string(c.q, now)
		testing.expectf(t, ok, "time query should parse: %s", c.q)
		if !ok {
			continue
		}
		testing.expectf(t, q.has_start == c.has_start, "%s: has_start=%v, want %v", c.q, q.has_start, c.has_start)
		testing.expectf(t, q.has_end == c.has_end, "%s: has_end=%v, want %v", c.q, q.has_end, c.has_end)
		testing.expectf(t, q.start == c.start, "%s: start=%d, want %d", c.q, q.start, c.start)
		testing.expectf(t, q.end == c.end, "%s: end=%d, want %d", c.q, q.end, c.end)
		query_destroy(&q)
	}

	for bad in ([]string{"_time:5x", "_time:", "_time:[2026-10-26T10:00:00Z]", "_time:[*, 5x]"}) {
		_, ok := parse_query_string(bad, now)
		testing.expectf(t, !ok, "time query should be rejected: %s", bad)
	}
}
