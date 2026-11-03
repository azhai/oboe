package main

import "../common"
import "../logdb"
import "../web"
import cachelib "../cache"
import queuelib "../queue"
import "core:c"
import "core:fmt"
import "core:os"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:sys/posix"
import "core:thread"
import "core:time"

VERSION :: "0.2.0"

// ---------------------------------------------------------------- shutdown

// A signal handler cannot receive extra arguments, so the live server is kept
// in a process-global slot. shutdown_requested stays 0 until the first
// SIGINT/SIGTERM; any later signal exits immediately.
@(private)
server_for_signal: ^web.Server

@(private)
shutdown_requested: u32

// handle_shutdown_signal only performs async-signal-safe work: an atomic
// exchange plus server_request_stop (one atomic store and close(2)). It never
// logs, allocates, joins threads or touches the store/cache. The main thread
// runs the full defer chain after server_run returns.
handle_shutdown_signal :: proc "c" (sig: posix.Signal) {
	if sync.atomic_exchange(&shutdown_requested, 1) == 0 {
		web.server_request_stop(server_for_signal)
		return
	}
	// A second signal means the graceful path is stuck (large cache save, disk
	// stall, ...). _exit is async-signal-safe and keeps the 128+N convention.
	posix._exit(c.int(128 + int(sig)))
}

// This Odin toolchain has no core:os/signal package, so the POSIX sigaction
// fallback is the available option on macOS/Linux. SA_RESTART is left off: a
// signal delivered to the thread blocked in accept makes it return EINTR, so
// server_run observes the stopping flag.
install_signal_handlers :: proc() -> bool {
	act: posix.sigaction_t
	posix.sigemptyset(&act.sa_mask)
	act.sa_flags = {}
	act.sa_handler = handle_shutdown_signal
	if posix.sigaction(.SIGINT, &act, nil) != .OK {
		return false
	}
	if posix.sigaction(.SIGTERM, &act, nil) != .OK {
		return false
	}
	return true
}

usage :: proc() {
	fmt.printf(`oboe %s — high-performance log database, cache and job queue (Odin)

Usage:
  oboe [flags]

Flags (logs):
  -data <dir>             data directory (default "./data")
  -listen <host:port>     HTTP listen address (default "0.0.0.0:9428")
  -workers <n>            HTTP worker threads (default 8)
  -max-conns <n>          max simultaneous HTTP connections (default 4096, 0 = unlimited)
  -retention <days>       delete partitions older than N days (default 30, 0 = keep forever)
  -block-rows <n>         rows per storage block (default 8192)
  -merge-threshold <n>    merge a partition once it has N parts (default 8)
  -stream-fields <list>   comma-separated fields that identify a stream (default "level,service")
  -flush-interval <dur>   memtable flush interval, e.g. 1s, 500ms (default 1s)
  -flush-threads <n>      threads encoding a part during flush (default: half the logical cores)
  -sort-threads <n>       threads sorting a memtable during flush (default: half the logical cores, max 4)
  -ingest-shards <n>      striped memtable append buffers per partition (default: logical cores, 4..16)
  -mem-max-rows <n>       hard cap on memtable rows before ingest waits/503 (default 6000000, 0 = no cap)
  -mem-max-bytes <size>   hard cap on memtable bytes, e.g. 512MB, 1GB (default 1GB, 0 = no cap)
  -mem-wait <dur>         how long an insert may block on the cap before 503, e.g. 2s (default 2s)

Flags (cache):
  -cache-size <size>      in-memory capacity, e.g. 256MB, 8GB (default 256MB, 0 = unlimited)
  -cache-max-entries <n>  approximate max cached keys (default 1000000, 0 = unlimited)
  -cache-max-value <size> max value size (default 16MB)
  -cache-file <path>      load the cache from <path> on start and save back to it
  -cache-save-interval <dur> periodic save interval (default 1m, 0 = no periodic saves)
  -no-cache               disable the cache entirely

Flags (job queue):
  -queue-listen <addr>    beanstalk-protocol TCP address (default "127.0.0.1:11300")
  -queue-max-jobs <n>     max jobs held in memory (default 1000000)
  -queue-max-job-bytes <n> max job body size (default 65535)
  -no-queue               disable the job queue

  -h, -help               show this help

HTTP endpoints:
  POST /insert/jsonline                 newline-delimited JSON
  POST /insert/elasticsearch/_bulk      Elasticsearch bulk API
  POST /insert/loki/api/v1/push         Grafana Loki push API
  GET  /select/logsql/query             LogsQL query, NDJSON response
  GET  /select/logsql/field_names       field names for a query
  GET  /select/logsql/field_values      field values for a query
  GET/PUT/DELETE /cache/<key>[?ttl=30s] in-memory cache
  GET  /cache/stats                     cache counters
  POST /cache/save                      persist the cache to -cache-file
  GET  /metrics                         Prometheus metrics
  GET  /health                          health check (200 "OK")
  GET  /                                embedded web UI

queuelib.Queue protocol (beanstalkd-compatible subset):
  put use watch ignore reserve reserve-with-timeout delete release bury touch
  peek peek-ready peek-delayed peek-buried kick kick-job pause-tube
  stats stats-tube stats-job list-tubes list-tube-used list-tubes-watched quit
`, VERSION)
}

next_arg :: proc(args: []string, i: ^int) -> (string, bool) {
	if i^ + 1 >= len(args) {
		return "", false
	}
	i^ += 1
	return args[i^], true
}

parse_duration_arg :: proc(s: string) -> (time.Duration, bool) {
	d, ok := common.parse_duration_ns(s)
	if !ok {
		return 0, false
	}
	return time.Duration(d), true
}

split_comma :: proc(s: string) -> []string {
	out: [dynamic]string
	rest := s
	for len(rest) > 0 {
		i := strings.index_byte(rest, ',')
		item := rest
		if i >= 0 {
			item = rest[:i]
			rest = rest[i+1:]
		} else {
			rest = ""
		}
		item = strings.trim_space(item)
		if len(item) > 0 {
			append(&out, item)
		}
	}
	return out[:]
}

main :: proc() {
	store_cfg := logdb.default_store_config()
	store_cfg.stream_fields = []string{"level", "service"}
	server_cfg := web.default_server_config()
	cache_cfg := cachelib.default_cache_config()
	queue_cfg := queuelib.default_queue_config()
	cache_enabled := true
	queue_enabled := true
	cache_file := ""
	cache_save_interval := time.Minute

	args := os.args[1:]
	i := 0
	for i < len(args) {
		a := args[i]
		switch a {
		case "-h", "-help", "--help":
			usage()
			return
		case "-version", "--version":
			fmt.println("oboe", VERSION)
			return
		case "-data", "--data":
			if v, ok := next_arg(args, &i); ok {
				store_cfg.data_path = v
			}
		case "-listen", "--listen":
			if v, ok := next_arg(args, &i); ok {
				server_cfg.listen = v
			}
		case "-workers", "--workers":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v, 10); nok && n > 0 {
					server_cfg.workers = n
				}
			}
		case "-max-conns", "--max-conns":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v, 10); nok && n >= 0 {
					server_cfg.max_conns = n
				}
			}
		case "-retention", "--retention":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v, 10); nok && n >= 0 {
					store_cfg.retention_days = n
				}
			}
		case "-block-rows", "--block-rows":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v, 10); nok && n >= 64 {
					store_cfg.block_rows = n
				}
			}
		case "-merge-threshold", "--merge-threshold":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v, 10); nok && n >= 2 {
					store_cfg.merge_threshold = n
				}
			}
		case "-flush-interval", "--flush-interval":
			if v, ok := next_arg(args, &i); ok {
				if d, dok := parse_duration_arg(v); dok && d > 0 {
					store_cfg.flush_interval = d
				}
			}
		case "-flush-threads", "--flush-threads":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v, 10); nok && n > 0 {
					store_cfg.flush_threads = n
				}
			}
		case "-sort-threads", "--sort-threads":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v, 10); nok && n > 0 {
					store_cfg.sort_threads = n
				}
			}
		case "-ingest-shards", "--ingest-shards":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v, 10); nok && n > 0 {
					store_cfg.ingest_shards = n
				}
			}
		case "-mem-max-rows", "--mem-max-rows":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v, 10); nok && n >= 0 {
					store_cfg.mem_max_rows = n
				}
			}
		case "-mem-max-bytes", "--mem-max-bytes":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := common.parse_size_bytes(v); nok && n >= 0 {
					store_cfg.mem_max_bytes = n
				}
			}
		case "-mem-wait", "--mem-wait":
			if v, ok := next_arg(args, &i); ok {
				if d, dok := parse_duration_arg(v); dok && d >= 0 {
					store_cfg.mem_wait = d
				}
			}
		case "-stream-fields", "--stream-fields":
			if v, ok := next_arg(args, &i); ok {
				store_cfg.stream_fields = split_comma(v)
			}
		case "-cache-size", "--cache-size":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := common.parse_size_bytes(v); nok {
					cache_cfg.max_bytes = n
				}
			}
		case "-cache-max-entries", "--cache-max-entries":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v, 10); nok && n >= 0 {
					cache_cfg.max_entries = n
				}
			}
		case "-cache-max-value", "--cache-max-value":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := common.parse_size_bytes(v); nok {
					cache_cfg.max_value = int(n)
				}
			}
		case "-cache-file", "--cache-file":
			if v, ok := next_arg(args, &i); ok {
				cache_file = v
			}
		case "-cache-save-interval", "--cache-save-interval":
			if v, ok := next_arg(args, &i); ok {
				if d, dok := parse_duration_arg(v); dok && d >= 0 {
					cache_save_interval = d
				}
			}
		case "-no-cache", "--no-cache":
			cache_enabled = false
		case "-queue-listen", "--queue-listen":
			if v, ok := next_arg(args, &i); ok {
				queue_cfg.listen = v
				if len(v) == 0 || v == "off" {
					queue_enabled = false
				}
			}
		case "-queue-max-jobs", "--queue-max-jobs":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v, 10); nok && n > 0 {
					queue_cfg.max_jobs = n
				}
			}
		case "-queue-max-job-bytes", "--queue-max-job-bytes":
			if v, ok := next_arg(args, &i); ok {
				if n, nok := strconv.parse_int(v, 10); nok && n > 0 {
					queue_cfg.max_job_bytes = n
				}
			}
		case "-no-queue", "--no-queue":
			queue_enabled = false
		case:
			fmt.eprintf("oboe: unknown flag %q\n\n", a)
			usage()
			os.exit(2)
		}
		i += 1
	}

	if !serve(store_cfg, server_cfg, cache_cfg, queue_cfg, cache_enabled, queue_enabled, cache_file, cache_save_interval) {
		// serve already ran its defer chain: store flushed, cache saved,
		// queue destroyed. Only the exit status has to be set here.
		os.exit(1)
	}
}

// serve owns every component allocated for one run and keeps all cleanup in
// main-thread defers, so both the graceful path and a startup failure release
// the store, cache and queue. It returns false only when server_run fails to
// bind/parse the listen address.
serve :: proc(
	store_cfg: logdb.Store_Config,
	server_cfg: web.Server_Config,
	cache_cfg: cachelib.Cache_Config,
	queue_cfg: queuelib.Queue_Config,
	cache_enabled: bool,
	queue_enabled: bool,
	cache_file: string,
	cache_save_interval: time.Duration,
) -> bool {
	store := logdb.store_init(store_cfg)
	defer logdb.store_close(store)

	cache: ^cachelib.Cache
	jobq: ^queuelib.Queue
	qs: ^queuelib.Queue_Server

	if cache_enabled {
		if len(cache_file) > 0 {
			if loaded, lok := cachelib.cache_load_file(cache_cfg, cache_file); lok {
				cache = loaded
				st := cachelib.cache_stats(cache)
				fmt.printf("oboe: cache loaded from %s (entries=%d, bytes=%d)\n", cache_file, st.entries, st.bytes)
			} else {
				fmt.printf("oboe: cache file %s not loaded, starting empty\n", cache_file)
				cache = cachelib.cache_init(cache_cfg)
			}
		} else {
			cache = cachelib.cache_init(cache_cfg)
		}
		fmt.printf("oboe: cache enabled (max_bytes=%d, max_entries=%d, buckets=%d)\n",
			cache_cfg.max_bytes, cache_cfg.max_entries, len(cache.buckets))
	}
	if queue_enabled {
		jobq = queuelib.queue_init(queue_cfg)
		qs = queuelib.queue_server_start(jobq)
		if qs == nil {
			queuelib.queue_destroy(jobq)
			jobq = nil
		}
	}

	saver: cachelib.Cache_Saver
	saver_thread: ^thread.Thread
	if cache != nil && len(cache_file) > 0 {
		// Both SIGINT/SIGTERM and a clean server_run return reach the defer
		// below, so the final save does not depend on the periodic one.
		if cache_save_interval > 0 {
			saver = cachelib.Cache_Saver{
				c =        cache,
				path =     strings.clone(cache_file),
				interval = cache_save_interval,
			}
			saver_thread = thread.create_and_start_with_data(&saver, cachelib.cache_saver_run, self_cleanup = false)
		}
	}

	srv := web.server_init(server_cfg, store, cache, jobq)
	srv.cfg.stream_fields = store_cfg.stream_fields
	srv.cache_file = cache_file
	server_for_signal = srv
	if !install_signal_handlers() {
		fmt.eprintf("oboe: warning: cannot install SIGINT/SIGTERM handlers; shutdown will not flush\n")
	}

	defer {
		// Workers may still read the store/cache, so join them before any
		// component teardown. After server_run returns this is the only place
		// that joins worker threads (the signal handler never does).
		web.server_stop(srv)
		web.server_destroy(srv)
		server_for_signal = nil
		if saver_thread != nil {
			sync.atomic_store(&saver.stop, 1)
			thread.join(saver_thread)
			thread.destroy(saver_thread)
		}
		if qs != nil {
			queuelib.queue_server_stop(qs)
		}
		if jobq != nil {
			queuelib.queue_destroy(jobq)
		}
		if cache != nil {
			if len(cache_file) > 0 {
				if bytes, sok := cachelib.cache_save_file(cache, cache_file); sok {
					fmt.printf("oboe: cache saved to %s (%d bytes)\n", cache_file, bytes)
				} else {
					fmt.eprintf("oboe: cache save to %s failed\n", cache_file)
				}
			}
			cachelib.cache_destroy(cache)
		}
		if saver.path != "" {
			delete(saver.path)
		}
	}

	return web.server_run(srv)
}
