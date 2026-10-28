// Embedding oboe as a library: store + LogsQL query, cache (with optional file
// snapshot), and the job queue. No HTTP server or CLI is involved.
//
// Run with:
//
//	odin run examples/embed
package main

import "core:fmt"
import "core:os"
import "core:time"

import oboe "../../src"

main :: proc() {
	data_dir := "/tmp/oboe_embed"
	os.remove_all(data_dir)
	defer os.remove_all(data_dir)

	run_store(data_dir)
	run_cache(data_dir)
	run_queue()
}

// ---------------------------------------------------------------- store

run_store :: proc(data_dir: string) {
	cfg := oboe.default_store_config()
	cfg.data_path = data_dir
	cfg.stream_fields = []string{"level", "service"}
	cfg.flush_interval = 1 << 62 // this example flushes explicitly
	s := oboe.store_init(cfg, false)
	defer oboe.store_close(s)

	arena: oboe.Arena
	defer oboe.arena_destroy(&arena)

	lines := [2]string{
		`{"_time":"2026-10-26T10:00:00Z","level":"error","service":"api","_msg":"boom"}`,
		`{"_time":"2026-10-26T10:00:01Z","level":"info","service":"api","_msg":"ok"}`,
	}
	rows: [dynamic]oboe.Ingest_Row
	defer {
		for row in rows {
			if len(row.fields) > 0 {
				delete(row.fields)
			}
		}
		delete(rows)
	}

	for line in lines {
		parsed: oboe.Parsed_Line
		if !oboe.parse_json_line(line, &arena, "_msg", "_time", &parsed) {
			continue
		}
		ts, ok := oboe.parse_iso_ns(parsed.time_raw)
		if !ok {
			continue
		}
		fields: []oboe.Field
		if parsed.nfields > 0 {
			fields = make([]oboe.Field, parsed.nfields)
			copy(fields, parsed.fields[:parsed.nfields])
		}
		append(&rows, oboe.Ingest_Row{
			ts     = ts,
			stream = oboe.compute_stream_id(fields, cfg.stream_fields),
			msg    = parsed.msg,
			fields = fields,
		})
	}

	fmt.printfln("store: ingested %d rows", oboe.store_ingest(s, rows[:]))
	oboe.store_flush_all(s)

	q, qok := oboe.parse_query_string("level:error", oboe.now_ns())
	if !qok {
		fmt.eprintln("store: invalid query")
		return
	}
	defer oboe.query_destroy(&q)
	res := oboe.execute_query(s, &q, 100)
	defer oboe.result_destroy(&res)
	fmt.printfln(
		"store: matched=%d scanned=%d pruned_blocks=%d",
		res.matched_rows,
		res.scanned_rows,
		res.pruned_blocks,
	)
	for i in 0..<len(res.rows) {
		row := &res.rows[i]
		service, _ := oboe.out_row_get(row, "service")
		fmt.printfln("store: %s service=%s", row.msg, service)
	}
}

// ---------------------------------------------------------------- cache

run_cache :: proc(data_dir: string) {
	cfg := oboe.default_cache_config()
	cfg.max_bytes = 64 << 20
	cfg.max_entries = 100_000
	c := oboe.cache_init(cfg)
	defer oboe.cache_destroy(c)

	value := transmute([]u8)string("hello from the cache")
	oboe.cache_set(c, "greeting", value, i64(30 * time.Second))

	out := oboe.buf_init(64)
	defer oboe.buf_destroy(&out)
	if oboe.cache_get_into(c, "greeting", &out) {
		fmt.printfln("cache: greeting=%s", out.data[:])
	}
	st := oboe.cache_stats(c)
	fmt.printfln("cache: entries=%d bytes=%d buckets=%d", st.entries, st.bytes, len(c.buckets))

	// Persistence is optional: the cache above is memory-only. Only call these
	// when a snapshot is actually wanted.
	path := fmt.aprintf("%s/cache.bin", data_dir)
	defer delete(path)
	if bytes, ok := oboe.cache_save_file(c, path); ok {
		fmt.printfln("cache: saved snapshot (%d bytes)", bytes)
		if loaded, lok := oboe.cache_load_file(cfg, path); lok {
			defer oboe.cache_destroy(loaded)
			if reloaded, rok := oboe.cache_get(loaded, "greeting"); rok {
				fmt.printfln("cache: reloaded greeting=%s", reloaded)
				delete(reloaded)
			}
		}
	}
	oboe.cache_delete(c, "greeting")
}

// ---------------------------------------------------------------- queue

run_queue :: proc() {
	q := oboe.queue_init(oboe.default_queue_config())
	defer oboe.queue_destroy(q)

	conn := oboe.queue_new_conn(q)
	id, put := oboe.queue_put(
		q,
		"default",
		0,
		0,
		i64(30 * time.Second),
		transmute([]u8)string("job body"),
	)
	fmt.printfln("queue: put id=%d result=%v", id, put)

	body := oboe.buf_init(64)
	defer oboe.buf_destroy(&body)
	if reserved, ok := oboe.queue_reserve_into(q, []string{"default"}, conn, 0, &body); ok {
		fmt.printfln("queue: reserved id=%d body=%s", reserved, body.data[:])
		oboe.queue_delete(q, reserved, conn)
	}
	oboe.queue_conn_closed(q, "default", nil, nil)
}
