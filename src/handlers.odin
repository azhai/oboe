package oboe

import "core:fmt"
import "core:encoding/json"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:sync"

// ---------------------------------------------------------------- params

Param :: struct {
	name:  string,
	value: string,
}

Params :: [dynamic]Param

pct_decode :: proc(s: string, a: ^Arena) -> string {
	if !strings.contains(s, "%") && !strings.contains(s, "+") {
		return s
	}
	out := arena_alloc(a, len(s), 1)
	n := 0
	i := 0
	for i < len(s) {
		c := s[i]
		if c == '+' {
			out[n] = ' '
			n += 1
			i += 1
		} else if c == '%' && i + 2 < len(s) {
			hi := unhex(s[i+1])
			lo := unhex(s[i+2])
			if hi >= 0 && lo >= 0 {
				out[n] = u8(hi << 4 | lo)
				n += 1
				i += 3
			} else {
				out[n] = c
				n += 1
				i += 1
			}
		} else {
			out[n] = c
			n += 1
			i += 1
		}
	}
	return string(out[:n])
}

params_parse :: proc(qstr: string, a: ^Arena) -> Params {
	params: [dynamic]Param
	rest := qstr
	for len(rest) > 0 {
		amp := strings.index_byte(rest, '&')
		kv := rest
		if amp >= 0 {
			kv = rest[:amp]
			rest = rest[amp+1:]
		} else {
			rest = ""
		}
		if len(kv) == 0 {
			continue
		}
		eq := strings.index_byte(kv, '=')
		if eq >= 0 {
			append(&params, Param{name = pct_decode(kv[:eq], a), value = pct_decode(kv[eq+1:], a)})
		} else {
			append(&params, Param{name = pct_decode(kv, a)})
		}
	}
	return params
}

params_get :: proc(params: Params, name: string) -> (string, bool) {
	for p in params {
		if p.name == name {
			return p.value, true
		}
	}
	return "", false
}

params_get_or :: proc(params: Params, name, def: string) -> string {
	v, ok := params_get(params, name)
	if ok && len(v) > 0 {
		return v
	}
	return def
}

params_int :: proc(params: Params, name: string, def: int) -> int {
	v, ok := params_get(params, name)
	if !ok {
		return def
	}
	n, nok := strconv.parse_int(v, 10)
	if !nok {
		return def
	}
	return n
}

split_stream_fields :: proc(s: string, a: ^Arena) -> []string {
	if len(s) == 0 {
		return nil
	}
	out: [dynamic]string
	defer delete(out)
	rest := s
	for len(rest) > 0 {
		c := strings.index_byte(rest, ',')
		item := rest
		if c >= 0 {
			item = rest[:c]
			rest = rest[c+1:]
		} else {
			rest = ""
		}
		item = strings.trim_space(item)
		if len(item) > 0 {
			append(&out, arena_str(a, item))
		}
	}
	if len(out) == 0 {
		return nil
	}
	bytes := arena_alloc(a, len(out) * size_of(string), align_of(string))
	fields := slice.from_ptr(cast(^string)raw_data(bytes), len(out))
	copy(fields, out[:])
	return fields
}

// ---------------------------------------------------------------- dispatch

dispatch :: proc(srv: ^Server, req: ^Http_Request, arena: ^Arena) -> (resp: Http_Response) {
	resp.body = buf_init(4096)
	resp.ctype = "application/json"
	params := params_parse(req.query, arena)
	defer delete(params)

	if req.method == "OPTIONS" {
		resp.code = 204
		return
	}

	path := req.path
	if len(path) > 1 && path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}

	if strings.has_prefix(path, "/cache/") || path == "/cache" {
		key := path[6:]
		if len(key) > 0 && key[0] == '/' {
			key = key[1:]
		}
		resp.code = handle_cache(srv, req, key, params, arena, &resp)
		return
	}

	switch path {
	case "", "/", "/index.html", "/select":
		resp.code = 200
		resp.ctype = "text/html; charset=utf-8"
		resp.extra = "Cache-Control: no-cache\r\n"
		buf_string(&resp.body, UI_HTML)
	case "/health":
		resp.code = 200
		resp.ctype = "text/plain; charset=utf-8"
		buf_string(&resp.body, "OK")
	case "/metrics":
		resp.code = 200
		resp.ctype = "text/plain; version=0.0.4; charset=utf-8"
		handle_metrics(srv, &resp.body)
	case "/insert/jsonline":
		if req.method != "POST" {
			resp.code = 405
			buf_string(&resp.body, `{"error":"method not allowed"}`)
			return
		}
		resp.code = handle_jsonline(srv, req.body, params, arena, &resp.body)
	case "/insert/elasticsearch/_bulk", "/insert/elasticsearch":
		if req.method != "POST" {
			resp.code = 405
			buf_string(&resp.body, `{"error":"method not allowed"}`)
			return
		}
		resp.code = handle_elastic_bulk(srv, req.body, params, arena, &resp.body)
	case "/insert/loki/api/v1/push":
		if req.method != "POST" {
			resp.code = 405
			buf_string(&resp.body, `{"error":"method not allowed"}`)
			return
		}
		resp.code = handle_loki(srv, req.body, params, arena, &resp.body)
	case "/select/logsql/query":
		resp.code = handle_query(srv, params, arena, &resp, &resp.body)
	case "/select/logsql/field_names":
		resp.code = handle_field_names(srv, params, arena, &resp.body)
	case "/select/logsql/field_values":
		resp.code = handle_field_values(srv, params, arena, &resp.body)
	case:
		resp.code = 404
		buf_addf(&resp.body, `{{"error":"unknown path","path":%q}}`, req.path)
	}
	return
}

// ---------------------------------------------------------------- ingest

prepare_query_params :: proc(srv: ^Server, params: Params, arena: ^Arena) -> (msg_field, time_field: string, stream_fields: []string) {
	msg_field = params_get_or(params, "_msg_field", "_msg")
	time_field = params_get_or(params, "_time_field", "_time")
	if sf, ok := params_get(params, "_stream_fields"); ok {
		stream_fields = split_stream_fields(sf, arena)
	} else {
		stream_fields = srv.cfg.stream_fields
	}
	return
}

ingest_json_line :: proc(srv: ^Server, lines: []string, msg_field, time_field: string, stream_fields: []string, arena: ^Arena, body: ^Buf) -> int {
	rows: [dynamic]Ingest_Row
	defer delete(rows)
	errors := 0
	now := now_ns()
	parsed: Parsed_Line
	for line in lines {
		if len(line) == 0 {
			continue
		}
		if !parse_json_line(line, arena, msg_field, time_field, &parsed) {
			errors += 1
			continue
		}
		ts: i64
		if parsed.time_set {
			t, tok := parse_time_ns(parsed.time_raw, now)
			if !tok {
				errors += 1
				continue
			}
			ts = t
		}
		n := parsed.nfields
		fs: []Field
		if n > 0 {
			bytes := arena_alloc(arena, n * size_of(Field), align_of(Field))
			fs = slice.from_ptr(cast(^Field)raw_data(bytes), n)
			copy(fs, parsed.fields[:n])
		}
		append(&rows, Ingest_Row{
			ts = ts,
			stream = compute_stream_id(fs, stream_fields),
			msg = parsed.msg,
			fields = fs,
		})
	}
	store_ingest(srv.store, rows[:])
	buf_addf(body, `{{"rows":%d,"errors":%d}}`, len(rows), errors)
	return 200
}

split_lines :: proc(body: []u8, out: ^[dynamic]string) {
	s := string(body)
	start := 0
	for i in 0..<len(s) {
		if s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			if len(line) > 0 {
				append(out, line)
			}
			start = i + 1
		}
	}
	if start < len(s) {
		line := s[start:]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if len(line) > 0 {
			append(out, line)
		}
	}
}

handle_jsonline :: proc(srv: ^Server, body: []u8, params: Params, arena: ^Arena, out: ^Buf) -> int {
	msg_field, time_field, stream_fields := prepare_query_params(srv, params, arena)
	lines: [dynamic]string
	defer delete(lines)
	split_lines(body, &lines)
	if len(lines) == 0 {
		buf_string(out, `{"rows":0,"errors":0}`)
		return 200
	}
	return ingest_json_line(srv, lines[:], msg_field, time_field, stream_fields, arena, out)
}

handle_elastic_bulk :: proc(srv: ^Server, body: []u8, params: Params, arena: ^Arena, out: ^Buf) -> int {
	msg_field, time_field, stream_fields := prepare_query_params(srv, params, arena)
	if msg_field == "_msg" {
		msg_field = "message"
	}
	if time_field == "_time" {
		time_field = "@timestamp"
	}
	lines: [dynamic]string
	defer delete(lines)
	split_lines(body, &lines)
	docs: [dynamic]string
	defer delete(docs)
	expect_action := true
	for line in lines {
		if expect_action {
			if contains_ci(line, `"delete"`) {
				expect_action = true
			} else {
				expect_action = false
			}
			continue
		}
		append(&docs, line)
		expect_action = true
	}
	start := now_ns()
	ingest_json_line(srv, docs[:], msg_field, time_field, stream_fields, arena, out)
	took := (now_ns() - start) / 1_000_000
	buf_reset(out)
	buf_addf(out, `{{"took":%d,"errors":false,"items":[]}}`, took)
	return 200
}

handle_loki :: proc(srv: ^Server, body: []u8, params: Params, arena: ^Arena, out: ^Buf) -> int {
	val, perr := json.parse_string(string(body), .JSON)
	if perr != nil {
		buf_string(out, `{"error":"invalid json"}`)
		return 400
	}
	defer json.destroy_value(val)
	obj, ok := val.(json.Object)
	if !ok {
		buf_string(out, `{"error":"expected object"}`)
		return 400
	}
	streams_v, sok := obj["streams"]
	if !sok {
		buf_string(out, `{"error":"missing streams"}`)
		return 400
	}
	streams, arr_ok := streams_v.(json.Array)
	if !arr_ok {
		buf_string(out, `{"error":"streams must be an array"}`)
		return 400
	}
	msg_field, time_field, cfg_stream_fields := prepare_query_params(srv, params, arena)
	_ = msg_field
	_ = time_field
	rows: [dynamic]Ingest_Row
	defer delete(rows)
	now := now_ns()
	for stream_v in streams {
		sobj, sook := stream_v.(json.Object)
		if !sook {
			continue
		}
		labels_v, _ := sobj["stream"]
		labels, _ := labels_v.(json.Object)
		values_v, _ := sobj["values"]
		values, _ := values_v.(json.Array)
		// stream identity comes from labels when no stream fields are configured
		stream_fields := cfg_stream_fields
		names: [dynamic]string
		if len(stream_fields) == 0 {
			for name, _ in labels {
				append(&names, name)
			}
			stream_fields = names[:]
		}
		label_fields: [dynamic]Field
		for name, value in labels {
			v, vok := value.(json.String)
			if !vok {
				continue
			}
			append(&label_fields, Field{name = name, value = v})
		}
		stream := compute_stream_id(label_fields[:], stream_fields)
		clear(&names)
		for value in values {
			pair, pok := value.(json.Array)
			if !pok || len(pair) < 2 {
				continue
			}
			ts_s, tsok := pair[0].(json.String)
			line_s, loki_ok := pair[1].(json.String)
			if !tsok || !loki_ok {
				continue
			}
			ts: i64
			if n, nok := strconv.parse_int(ts_s, 10); nok {
				ts = i64(n)
				if len(ts_s) <= 13 {
					ts *= 1_000_000_000
				}
			} else if t, tok := parse_time_ns(ts_s, now); tok {
				ts = t
			} else {
				ts = 0
			}
			fs_bytes := arena_alloc(arena, len(label_fields) * size_of(Field), align_of(Field))
			fs := slice.from_ptr(cast(^Field)raw_data(fs_bytes), len(label_fields))
			copy(fs, label_fields[:])
			append(&rows, Ingest_Row{ts = ts, stream = stream, msg = line_s, fields = fs})
		}
		delete(label_fields)
		delete(names)
	}
	store_ingest(srv.store, rows[:])
	buf_reset(out)
	return 204
}

// ---------------------------------------------------------------- query

write_out_row :: proc(b: ^Buf, r: ^Out_Row, with_msg := true) {
	buf_byte(b, '{')
	first := true
	if r.has_time {
		buf_string(b, "\"_time\":\"")
		buf_time_rfc3339(b, r.ts)
		buf_byte(b, '"')
		first = false
	}
	if with_msg {
		if !first {
			buf_byte(b, ',')
		}
		buf_string(b, "\"_msg\":")
		json_escape_into(b, r.msg)
		first = false
	}
	for f in r.fields {
		if f.name == "_time" || f.name == "_msg" {
			continue
		}
		if !first {
			buf_byte(b, ',')
		}
		json_escape_into(b, f.name)
		buf_byte(b, ':')
		json_escape_into(b, f.value)
		first = false
	}
	buf_byte(b, '}')
	buf_byte(b, '\n')
}

prepare_query :: proc(params: Params, now: i64) -> (q: Query, ok: bool) {
	qstr := params_get_or(params, "query", "*")
	q, ok = parse_query_string(qstr, now)
	if !ok {
		return q, false
	}
	start, has_start := params_get(params, "start")
	end, has_end := params_get(params, "end")
	if !has_start {
		start = ""
	}
	if !has_end {
		end = ""
	}
	if !apply_query_bounds(&q, start, end) {
		query_destroy(&q)
		return q, false
	}
	return q, true
}

handle_query :: proc(srv: ^Server, params: Params, arena: ^Arena, resp: ^Http_Response, out: ^Buf) -> int {
	now := now_ns()
	q, ok := prepare_query(params, now)
	if !ok {
		buf_string(out, `{"error":"invalid query"}`)
		return 400
	}
	defer query_destroy(&q)
	limit := params_int(params, "limit", DEFAULT_QUERY_LIMIT)
	r := execute_query(srv.store, &q, limit)
	defer result_destroy(&r)

	resp.ctype = "application/stream+json; charset=utf-8"
	with_msg := !query_has_pipe(&q, .STATS)
	for i in 0..<len(r.rows) {
		write_out_row(out, &r.rows[i], with_msg)
	}
	sync_add_query(srv)
	resp.extra = arena_aprintf(arena, "X-Oboe-Scanned-Rows: %d\r\nX-Oboe-Matched-Rows: %d\r\nX-Oboe-Pruned-Blocks: %d\r\nX-Oboe-Elapsed-Ms: %d\r\n", r.scanned_rows, r.matched_rows, r.pruned_blocks, r.elapsed_ns / 1_000_000)
	return 200
}

sync_add_query :: proc(srv: ^Server) {
	sync.atomic_add(&srv.store.queries, 1)
}

Hit :: struct {
	value: string,
	hits:  int,
}

sort_hits :: proc(h: []Hit) {
	for i := 1; i < len(h); i += 1 {
		x := h[i]
		j := i - 1
		for j >= 0 && (h[j].hits < x.hits || (h[j].hits == x.hits && strings.compare(h[j].value, x.value) > 0)) {
			h[j+1] = h[j]
			j -= 1
		}
		h[j+1] = x
	}
}

write_hits :: proc(out: ^Buf, hits: []Hit) {
	buf_string(out, `{"values":[`)
	for i in 0..<len(hits) {
		if i > 0 {
			buf_byte(out, ',')
		}
		buf_string(out, `{"value":`)
		json_escape_into(out, hits[i].value)
		buf_addf(out, `,"hits":%d}}`, hits[i].hits)
	}
	buf_string(out, `]}`)
}

handle_field_names :: proc(srv: ^Server, params: Params, arena: ^Arena, out: ^Buf) -> int {
	now := now_ns()
	q, ok := prepare_query(params, now)
	if !ok {
		buf_string(out, `{"error":"invalid query"}`)
		return 400
	}
	defer query_destroy(&q)
	limit := params_int(params, "limit", 1000)
	if limit <= 0 {
		limit = 1000
	}
	r := execute_query(srv.store, &q, limit)
	defer result_destroy(&r)

	counts: map[string]int
	defer delete_map(counts)
	for i in 0..<len(r.rows) {
		row := &r.rows[i]
		counts["_time"] += 1
		counts["_msg"] += 1
		for f in row.fields {
			counts[f.name] += 1
		}
	}
	hits := make([]Hit, len(counts))
	idx := 0
	for name, n in counts {
		hits[idx] = Hit{value = name, hits = n}
		idx += 1
	}
	sort_hits(hits)
	defer delete(hits)
	write_hits(out, hits)
	return 200
}

handle_field_values :: proc(srv: ^Server, params: Params, arena: ^Arena, out: ^Buf) -> int {
	field, fok := params_get(params, "field")
	if !fok || len(field) == 0 {
		buf_string(out, `{"error":"missing field param"}`)
		return 400
	}
	now := now_ns()
	q, ok := prepare_query(params, now)
	if !ok {
		buf_string(out, `{"error":"invalid query"}`)
		return 400
	}
	defer query_destroy(&q)
	limit := params_int(params, "limit", 1000)
	if limit <= 0 {
		limit = 1000
	}
	r := execute_query(srv.store, &q, limit)
	defer result_destroy(&r)

	counts: map[string]int
	defer delete_map(counts)
	for i in 0..<len(r.rows) {
		row := &r.rows[i]
		v, vok := out_row_get(row, field)
		if vok && len(v) > 0 {
			counts[v] += 1
		}
	}
	hits := make([]Hit, len(counts))
	idx := 0
	for value, n in counts {
		hits[idx] = Hit{value = value, hits = n}
		idx += 1
	}
	sort_hits(hits)
	defer delete(hits)
	write_hits(out, hits)
	return 200
}

// ---------------------------------------------------------------- cache

handle_cache :: proc(srv: ^Server, req: ^Http_Request, key: string, params: Params, arena: ^Arena, resp: ^Http_Response) -> int {
	c := srv.cache
	resp.ctype = "application/json"
	if c == nil {
		buf_string(&resp.body, `{"error":"cache disabled"}`)
		return 503
	}
	cache_key := pct_decode(key, arena)
	if cache_key == "save" {
		if req.method != "POST" {
			buf_string(&resp.body, `{"error":"method not allowed"}`)
			return 405
		}
		if len(srv.cache_file) == 0 {
			buf_string(&resp.body, `{"error":"cache file not configured"}`)
			return 400
		}
		bytes, ok := cache_save_file(c, srv.cache_file)
		if !ok {
			buf_string(&resp.body, `{"error":"cache save failed"}`)
			return 500
		}
		buf_addf(&resp.body, `{{"saved":true,"bytes":%d}}`, bytes)
		return 200
	}
	if len(cache_key) == 0 || cache_key == "stats" {
		if req.method != "GET" {
			buf_string(&resp.body, `{"error":"method not allowed"}`)
			return 405
		}
		st := cache_stats(c)
		buf_addf(&resp.body, `{{"entries":%d,"bytes":%d,"hits":%d,"misses":%d,"evictions":%d,"expirations":%d,"sets":%d,"deletes":%d,"collisions":%d,"buckets":%d,"max_bytes":%d,"max_entries":%d}}`,
			st.entries, st.bytes, st.hits, st.misses, st.evictions, st.expirations,
			st.sets, st.deletes, st.collisions, len(c.buckets), c.cfg.max_bytes, c.cfg.max_entries)
		return 200
	}
	if len(cache_key) > CACHE_MAX_KEY {
		buf_string(&resp.body, `{"error":"key too long"}`)
		return 400
	}
	switch req.method {
	case "GET":
		if cache_get_into(c, cache_key, &resp.body) {
			resp.ctype = "application/octet-stream"
			resp.extra = "X-Cache: hit\r\n"
			return 200
		}
		resp.ctype = "text/plain; charset=utf-8"
		resp.extra = "X-Cache: miss\r\n"
		buf_string(&resp.body, "not found")
		return 404
	case "PUT", "POST":
		ttl := i64(0)
		if t, ok := params_get(params, "ttl"); ok {
			d, dok := parse_duration_ns(t)
			if !dok || d < 0 {
				buf_string(&resp.body, `{"error":"invalid ttl"}`)
				return 400
			}
			ttl = d
		}
		if !cache_set(c, cache_key, req.body, ttl) {
			if c.cfg.max_value > 0 && len(req.body) > c.cfg.max_value {
				buf_string(&resp.body, `{"error":"value too large"}`)
				return 413
			}
			buf_string(&resp.body, `{"error":"cannot store value"}`)
			return 400
		}
		buf_string(&resp.body, `{"ok":true}`)
		resp.extra = "X-Cache: stored\r\n"
		return 200
	case "DELETE":
		if cache_delete(c, cache_key) {
			buf_string(&resp.body, `{"deleted":true}`)
			resp.extra = "X-Cache: deleted\r\n"
		} else {
			buf_string(&resp.body, `{"deleted":false}`)
			resp.extra = "X-Cache: miss\r\n"
		}
		return 200
	}
	buf_string(&resp.body, `{"error":"method not allowed"}`)
	return 405
}

// ---------------------------------------------------------------- metrics

handle_metrics :: proc(srv: ^Server, out: ^Buf) {
	st := store_stats(srv.store)
	up := (now_ns() - srv.start_ns) / 1_000_000_000
	buf_addf(out, "# HELP oboe_ingested_rows_total Rows accepted by the ingest API.\n")
	buf_addf(out, "# TYPE oboe_ingested_rows_total counter\n")
	buf_addf(out, "oboe_ingested_rows_total %d\n", sync.atomic_load(&srv.store.ingested_rows))
	buf_addf(out, "oboe_ingested_bytes_total %d\n", sync.atomic_load(&srv.store.ingested_bytes))
	buf_addf(out, "oboe_queries_total %d\n", sync.atomic_load(&srv.store.queries))
	buf_addf(out, "oboe_flushed_parts_total %d\n", sync.atomic_load(&srv.store.flushed_parts))
	buf_addf(out, "oboe_merged_parts_total %d\n", sync.atomic_load(&srv.store.merged_parts))
	buf_addf(out, "oboe_parts %d\n", st.parts)
	buf_addf(out, "oboe_partitions %d\n", st.partitions)
	buf_addf(out, "oboe_mem_rows %d\n", st.mem_rows)
	buf_addf(out, "oboe_rows %d\n", st.rows)
	buf_addf(out, "oboe_disk_bytes %d\n", st.disk_bytes)
	buf_addf(out, "oboe_http_requests_total %d\n", srv.requests)
	buf_addf(out, "oboe_http_connections_total %d\n", srv.conns)
	if srv.cache != nil {
		cs := cache_stats(srv.cache)
		buf_addf(out, "oboe_cache_entries %d\n", cs.entries)
		buf_addf(out, "oboe_cache_bytes %d\n", cs.bytes)
		buf_addf(out, "oboe_cache_hits_total %d\n", cs.hits)
		buf_addf(out, "oboe_cache_misses_total %d\n", cs.misses)
		buf_addf(out, "oboe_cache_evictions_total %d\n", cs.evictions)
		buf_addf(out, "oboe_cache_expirations_total %d\n", cs.expirations)
		buf_addf(out, "oboe_cache_collisions_total %d\n", cs.collisions)
	}
	if srv.jobq != nil {
		qs := queue_stats(srv.jobq)
		buf_addf(out, "oboe_queue_jobs %d\n", qs.current_jobs)
		buf_addf(out, "oboe_queue_ready %d\n", qs.ready)
		buf_addf(out, "oboe_queue_reserved %d\n", qs.reserved)
		buf_addf(out, "oboe_queue_delayed %d\n", qs.delayed)
		buf_addf(out, "oboe_queue_buried %d\n", qs.buried)
		buf_addf(out, "oboe_queue_connections %d\n", qs.connections)
		buf_addf(out, "oboe_queue_puts_total %d\n", qs.total_jobs)
	}
	buf_addf(out, "oboe_uptime_seconds %d\n", up)
}
