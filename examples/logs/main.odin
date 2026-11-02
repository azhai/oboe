// Write JSON lines into logdb and query them with LogsQL. No HTTP port is
// opened; the store runs without its background flush/merge worker.
//
// Run with:
//
//	odin run examples/logs
package main

import "core:fmt"
import "core:os"

import "../../common"
import "../../logdb"

DATA_DIR :: "/tmp/oboe_example_logs"

main :: proc() {
	os.remove_all(DATA_DIR)
	defer os.remove_all(DATA_DIR)

	cfg := logdb.default_store_config()
	cfg.data_path = DATA_DIR
	cfg.stream_fields = []string{"level", "service"}
	cfg.flush_interval = 1 << 62 // this example flushes explicitly
	s := logdb.store_init(cfg, false)
	defer logdb.store_close(s)

	arena: common.Arena
	defer common.arena_destroy(&arena)

	lines := [3]string{
		`{"_time":"2026-10-26T10:00:00Z","level":"error","service":"api","_msg":"boom"}`,
		`{"_time":"2026-10-26T10:00:01Z","level":"info","service":"api","_msg":"ok"}`,
		`{"_time":"2026-10-26T10:00:02Z","level":"error","service":"worker","_msg":"timeout"}`,
	}

	rows: [dynamic]logdb.Ingest_Row
	defer {
		for row in rows {
			if len(row.fields) > 0 {
				delete(row.fields)
			}
		}
		delete(rows)
	}

	now := common.now_ns()
	for line in lines {
		parsed: logdb.Parsed_Line
		if !logdb.parse_json_line(line, &arena, "_msg", "_time", &parsed) {
			fmt.eprintln("logs: bad json line:", line)
			continue
		}
		ts, tok := common.parse_time_ns(parsed.time_raw, now)
		if !tok {
			fmt.eprintln("logs: bad _time:", parsed.time_raw)
			continue
		}
		fields: []logdb.Field
		if parsed.nfields > 0 {
			fields = make([]logdb.Field, parsed.nfields)
			copy(fields, parsed.fields[:parsed.nfields])
		}
		append(&rows, logdb.Ingest_Row{
			ts     = ts,
			stream = logdb.compute_stream_id(fields, cfg.stream_fields),
			msg    = parsed.msg,
			fields = fields,
		})
	}

	fmt.printfln("logs: ingested %d rows", logdb.store_ingest(s, rows[:]))
	logdb.store_flush_all(s)

	// Filter + sort + limit.
	q, ok := logdb.parse_query_string("level:error | sort by (_time) desc | limit 10", now)
	if !ok {
		fmt.eprintln("logs: invalid query")
		return
	}
	defer logdb.query_destroy(&q)

	res := logdb.execute_query(s, &q, 10)
	defer logdb.result_destroy(&res)
	fmt.printfln("logs: matched=%d scanned=%d pruned_blocks=%d", res.matched_rows, res.scanned_rows, res.pruned_blocks)
	for i in 0..<len(res.rows) {
		row := &res.rows[i]
		service, _ := logdb.out_row_get(row, "service")
		fmt.printfln("logs: %s %s", service, row.msg)
	}

	// Grouped aggregation.
	stats_q, sok := logdb.parse_query_string("* | stats by (service) count() as total", now)
	if !sok {
		fmt.eprintln("logs: invalid stats query")
		return
	}
	defer logdb.query_destroy(&stats_q)

	stats := logdb.execute_query(s, &stats_q, 100)
	defer logdb.result_destroy(&stats)
	for i in 0..<len(stats.rows) {
		row := &stats.rows[i]
		service, _ := logdb.out_row_get(row, "service")
		total, _ := logdb.out_row_get(row, "total")
		fmt.printfln("logs: service=%s total=%s", service, total)
	}
}
