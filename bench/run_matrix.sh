#!/usr/bin/env bash
# Reproducible benchmark matrix for oboe.
#
#   bench/run_matrix.sh [--quick] [--no-soak] [--reps N] [--only name]
#
# The script builds the server, the library benchmark suite and the HTTP load
# generator into $OUT/bin (outside the repository), seeds synthetic data into
# $OUT/data-*, runs every cell REPS times and writes one JSON object per run to
# $OUT/results/raw.jsonl. bench/summarize.py turns that into tables.
#
# Exit status is non-zero if any cell failed to produce a result.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${OUT:-/tmp/oboe_matrix}"
ODIN="${ODIN:-/opt/homebrew/bin/odin}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-19428}"
QPORT="${QPORT:-12399}"
WRITE_PORT="${WRITE_PORT:-19429}"
WRITE_QPORT="${WRITE_QPORT:-12398}"
WORKERS="${WORKERS:-8}"
DUR="${DUR:-5000}"
WARM="${WARM:-2000}"
REPS="${REPS:-3}"
SMALL_ROWS="${SMALL_ROWS:-200000}"
BIG_ROWS="${BIG_ROWS:-5000000}"
SOAK_SECONDS="${SOAK_SECONDS:-180}"
QUICK=0
NO_SOAK=0
ONLY=""

while [ $# -gt 0 ]; do
	case "$1" in
	--quick)
		QUICK=1
		DUR=2000 WARM=1000 REPS=2 SOAK_SECONDS=60 BIG_ROWS=1000000
		shift
		;;
	--no-soak)
		NO_SOAK=1
		shift
		;;
	--reps)
		REPS="$2"
		shift 2
		;;
	--only)
		ONLY="$2"
		shift 2
		;;
	-h | --help)
		sed -n '2,12p' "$0"
		exit 0
		;;
	*)
		echo "unknown argument: $1" >&2
		exit 2
		;;
	esac
done

BIN="$OUT/bin"
RAW="$OUT/results/raw.jsonl"
FAIL="$OUT/results/failures.txt"
mkdir -p "$OUT"/{bin,results,bodies,logs,soak}
mkdir -p "$OUT/data-small" "$OUT/data-big" "$OUT/data-write" "$OUT/data-soak"
: >"$RAW"
: >"$FAIL"

say() { printf '\n== %s ==\n' "$*" >&2; }

# ---------------------------------------------------------------- build

build() {
	say "build"
	"$ODIN" build "$ROOT/cmd" -out:"$BIN/oboe" -o:speed || return 1
	"$ODIN" build "$ROOT/bench" -out:"$BIN/oboe_bench" -o:speed || return 1
	"$ODIN" build "$ROOT/bench/loadgen" -out:"$BIN/oboe_loadgen" -o:speed || return 1
}

meta() {
	{
		echo "# Benchmark run"
		echo
		echo "- date: $(date '+%Y-%m-%d %H:%M:%S %Z')"
		echo "- host: $(hostname)"
		echo "- cpu: $(sysctl -n machdep.cpu.brand_string)"
		echo "- logical cores: $(sysctl -n hw.ncpu)"
		echo "- physical cores: $(sysctl -n hw.physicalcpu)"
		echo "- memory bytes: $(sysctl -n hw.memsize)"
		echo "- os: $(sw_vers -productVersion)"
		echo "- odin: $("$ODIN" version)"
		echo "- build flags: -o:speed for server/bench/loadgen; -microarch: none"
		echo "- server workers: $WORKERS"
		echo "- warmup ms: $WARM, sample ms: $DUR, reps: $REPS"
		echo "- server cmd: -listen $HOST:$PORT -workers $WORKERS -queue-listen $HOST:$QPORT -cache-size 512MB -cache-max-value 16MB -max-body (default) -retention 0 -flush-interval 1s -data <stage dir>"
		echo "- data dirs: $OUT/data-* on /tmp (APFS, internal SSD); recreated per tier, empty at seed time"
		echo "- tmp filesystem:"
		df -h /tmp | tail -1
		echo "- somaxconn: $(sysctl -n kern.ipc.somaxconn 2>/dev/null || echo n/a)"
	} >"$OUT/meta.md"
	cp "$OUT/meta.md" "$OUT/results/meta.md"
}

# ---------------------------------------------------------------- helpers

jqr() { # scenario tier rep
	jq -c --arg s "$1" --arg t "$2" --argjson r "$3" '. + {scenario:$s, tier:$t, rep:$r}'
}

# lg scenario tier rep loadgen-args...
lg() {
	local s="$1" t="$2" r="$3"
	shift 3
	if ! "$BIN/oboe_loadgen" -addr "$HOST:$PORT" "$@" -json 2>>"$OUT/logs/loadgen.err" | jqr "$s" "$t" "$r" >>"$RAW"; then
		echo "FAIL scenario=$s tier=$t rep=$r args=$*" >>"$FAIL"
	fi
}

# bj scenario tier rep bench-args...
bj() {
	local s="$1" t="$2" r="$3"
	shift 3
	if ! "$BIN/oboe_bench" "$@" -json 2>>"$OUT/logs/bench.err" | jqr "$s" "$t" "$r" >>"$RAW"; then
		echo "FAIL scenario=$s tier=$t rep=$r args=$*" >>"$FAIL"
	fi
}

SERVER_PID=""
start_server() { # data port qport tag
	local data="$1" port="$2" qport="$3" tag="$4"
	"$BIN/oboe" \
		-data "$data" \
		-listen "$HOST:$port" \
		-workers "$WORKERS" \
		-queue-listen "$HOST:$qport" \
		-cache-size 512MB \
		-cache-max-value 16MB \
		-retention 0 \
		-flush-interval 1s \
		>"$OUT/logs/server-$tag.log" 2>&1 &
	SERVER_PID=$!
	for _ in $(seq 1 150); do
		if curl -fsS -m 1 "http://$HOST:$port/health" >/dev/null 2>&1; then
			return 0
		fi
		if ! kill -0 "$SERVER_PID" 2>/dev/null; then
			echo "server $tag died; log:" >&2
			tail -20 "$OUT/logs/server-$tag.log" >&2
			SERVER_PID=""
			return 1
		fi
		sleep 0.2
	done
	echo "server $tag did not become healthy" >&2
	return 1
}

stop_server() {
	if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
		kill -TERM "$SERVER_PID" 2>/dev/null
		local waited=0
		while kill -0 "$SERVER_PID" 2>/dev/null && [ "$waited" -lt 8 ]; do
			sleep 1
			waited=$((waited + 1))
		done
		if kill -0 "$SERVER_PID" 2>/dev/null; then
			# Observed after heavy ingest: SIGTERM closes the listener but the
			# process keeps a core busy and never reaches its teardown. The
			# benchmark data is already flushed, so force it down and record it.
			echo "pid=$SERVER_PID stuck ${waited}s after SIGTERM; SIGKILL" \
				>>"$OUT/results/shutdown.log"
			echo "stop_server: pid $SERVER_PID stuck after SIGTERM, SIGKILL" >&2
			kill -KILL "$SERVER_PID" 2>/dev/null
		fi
		wait "$SERVER_PID" 2>/dev/null
	fi
	SERVER_PID=""
}

urlenc() {
	python3 -c 'import sys,urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

seed() { # tier rows dir
	local tier="$1" rows="$2" dir="$3"
	say "seed $tier rows=$rows"
	"$BIN/oboe_bench" -mode ingest -rows "$rows" -threads "$WORKERS" -batch 500 \
		-data "$dir" -keep -json 2>>"$OUT/logs/bench.err" |
		jq -c --arg t "$tier" '. + {tier:$t, rep:0}' | tee -a "$RAW"
}

wanted() {
	return 0
}

# ---------------------------------------------------------------- bodies

make_bodies() {
	say "generate request bodies"
	python3 - "$OUT/bodies" <<'PY'
import json, os, sys, time
out = sys.argv[1]
os.makedirs(out, exist_ok=True)
levels = ["error", "info", "warn", "debug"]
services = ["api", "db", "cache", "auth"]

def row(i):
    return {
        "_msg": f"request id={i} path=/api/v1/items/{i*7} status={200+i%3*100} latency={i%250}ms peer=10.0.{i%16}.{(i*3)%255}",
        "level": levels[i % 4],
        "service": services[i % 4],
        "zone": ["a", "b", "c"][i % 3],
        "host": f"host-{i % 4096:03d}",
        "user": f"user-{i % 8192:04d}",
        "duration": (i * 37) % 5000,
    }

def write(name, text):
    with open(os.path.join(out, name), "w") as f:
        f.write(text)

write("jsonline-1.ndjson", json.dumps(row(0)) + "\n")
write("jsonline-100.ndjson", "".join(json.dumps(row(i)) + "\n" for i in range(100)))
write("elastic-100.ndjson", "".join(
    '{"create":{}}\n' + json.dumps(row(i)) + "\n" for i in range(100)))
now = time.time_ns()
write("loki-100.json", json.dumps({
    "streams": [{
        "stream": {"service": "api", "level": "info"},
        "values": [[str(now + i * 1000), row(i)["_msg"]] for i in range(100)],
    }]
}))
write("small.bin", "0123456789abcdef" * 4)
with open(os.path.join(out, "large-8m.bin"), "wb") as f:
    f.write(b"oboe-large-value-" * (8 * 1024 * 1024 // 17 + 1))
PY
}

# ---------------------------------------------------------------- scenarios

run_health() {
	wanted health || return 0
	local tier="$1"
	say "health keep-alive ($tier)"
	for r in $(seq 1 "$REPS"); do
		for c in 1 8 16 64 128 512; do
			lg health "$tier" "$r" -path /health -label "ka-c$c" -conc "$c" \
				-dur "$DUR" -warm "$WARM" -timeout 1500
		done
	done
}

run_health_new() {
	wanted health-new || return 0
	local tier="$1"
	say "health per-request connection ($tier)"
	for r in $(seq 1 "$REPS"); do
		for c in 1 8 16 64 128 512; do
			lg health_new "$tier" "$r" -path /health -label "new-c$c" -conc "$c" \
				-conns per-request -dur "$DUR" -warm "$WARM" -timeout 1500
		done
	done
}

run_writes() {
	wanted write || return 0
	say "HTTP writes"
	local b="$OUT/bodies"
	local label path body rows expect
	for r in $(seq 1 "$REPS"); do
		for spec in \
			"jsonline-1|/insert/jsonline|$b/jsonline-1.ndjson|1|200" \
			"jsonline-100|/insert/jsonline|$b/jsonline-100.ndjson|100|200" \
			"elastic-100|/insert/elasticsearch/_bulk|$b/elastic-100.ndjson|100|200" \
			"loki-100|/insert/loki/api/v1/push|$b/loki-100.json|100|204"; do
			IFS='|' read -r label path body rows expect <<<"$spec"
			lg write empty "$r" -path "$path" -method POST -body-file "$body" \
				-label "$label" -conc 8 -dur "$DUR" -warm "$WARM" -rows-per-req "$rows" \
				-expect "$expect"
		done
	done
}

run_queries() {
	wanted query || return 0
	local tier="$1" base="$2" end="$3"
	local concs="$4"
	say "LogsQL queries ($tier)"
	local narrow_start=$((base + 8 * 60 * 1000000000))
	local q_filter="level:error | limit 500"
	local q_stats="level:error | stats by (level) count() as total"
	local q_sort="* | sort by (_time) desc | limit 100"
	local q_narrow="* | stats by (host) count() as total"
	for r in $(seq 1 "$REPS"); do
		for c in $concs; do
			lg query "$tier" "$r" -label "filter-c$c" -conc "$c" -dur "$DUR" -warm "$WARM" \
				-path "/select/logsql/query?query=$(urlenc "$q_filter")&limit=500&start=$base&end=$end"
			lg query "$tier" "$r" -label "stats-c$c" -conc "$c" -dur "$DUR" -warm "$WARM" \
				-path "/select/logsql/query?query=$(urlenc "$q_stats")&limit=1000&start=$base&end=$end"
			lg query "$tier" "$r" -label "sort-c$c" -conc "$c" -dur "$DUR" -warm "$WARM" \
				-path "/select/logsql/query?query=$(urlenc "$q_sort")&limit=100&start=$base&end=$end"
			lg query "$tier" "$r" -label "narrow-c$c" -conc "$c" -dur "$DUR" -warm "$WARM" \
				-path "/select/logsql/query?query=$(urlenc "$q_narrow")&limit=1000&start=$narrow_start&end=$end"
		done
	done
}

run_cache() {
	wanted cache || return 0
	say "cache"
	local b="$OUT/bodies"
	for r in $(seq 1 "$REPS"); do
		# Populate the hit rings first (untimed).
		"$BIN/oboe_loadgen" -addr "$HOST:$PORT" -path '/cache/k{i}' -rotate 256 -method PUT \
			-body-file "$b/small.bin" -conc 8 -warm 0 -dur 200 -requests 256 >/dev/null 2>&1
		"$BIN/oboe_loadgen" -addr "$HOST:$PORT" -path '/cache/L{i}' -rotate 8 -method PUT \
			-body-file "$b/large-8m.bin" -conc 8 -warm 0 -dur 300 -requests 1 >/dev/null 2>&1

		lg cache empty "$r" -label put-small -path '/cache/k{i}' -rotate 256 -method PUT \
			-body-file "$b/small.bin" -conc 8 -dur "$DUR" -warm "$WARM"
		lg cache empty "$r" -label get-hit-small -path '/cache/k{i}' -rotate 256 -conc 8 \
			-dur "$DUR" -warm "$WARM"
		lg cache empty "$r" -label get-miss-small -path '/cache/m{i}' -rotate 256 -conc 8 \
			-dur "$DUR" -warm "$WARM" -expect 404
		lg cache empty "$r" -label put-large -path '/cache/L{i}' -rotate 8 -method PUT \
			-body-file "$b/large-8m.bin" -conc 8 -dur "$DUR" -warm "$WARM"
		lg cache empty "$r" -label get-hit-large -path '/cache/L{i}' -rotate 8 -conc 8 \
			-dur "$DUR" -warm "$WARM"
		lg cache empty "$r" -label get-miss-large -path '/cache/M{i}' -rotate 8 -conc 8 \
			-dur "$DUR" -warm "$WARM" -expect 404
	done
}

run_queue() {
	wanted queue || return 0
	say "queue TCP"
	for r in $(seq 1 "$REPS"); do
		for clients in 1 8 32; do
			bj queue_tcp tcp "$r" -mode queue-tcp -addr "$HOST:$QPORT" -qmode put \
				-clients "$clients" -jobs 20000 -json
			bj queue_tcp tcp "$r" -mode queue-tcp -addr "$HOST:$QPORT" -qmode get \
				-clients "$clients" -jobs 20000
			bj queue_tcp tcp "$r" -mode queue-tcp -addr "$HOST:$QPORT" -qmode cycle \
				-clients "$clients" -jobs 20000
			bj queue_tcp tcp "$r" -mode queue-tcp -addr "$HOST:$QPORT" -qmode bury \
				-clients "$clients" -jobs 2000 -rounds 5
		done
	done
}

run_query_lib() { # tier concs
	wanted queries-lib || return 0
	local tier="$1" concs="$2"
	say "library queries ($tier)"
	for r in $(seq 1 "$REPS"); do
		for c in $concs; do
			bj query_lib "$tier" "$r" -mode query -reuse -data "$OUT/data-$tier" \
				-threads "$c" -reps 10 -warm 2
		done
	done
}

# ---------------------------------------------------------------- soak

run_soak() {
	wanted soak || return 0
	[ "$NO_SOAK" = 1 ] && return 0
	say "soak ${SOAK_SECONDS}s"
	start_server "$OUT/data-small" "$PORT" "$QPORT" soak || return 0
	local pid=$SERVER_PID
	local soak="$OUT/soak"
	local q="level:error | limit 500"
	local path="/select/logsql/query?query=$(urlenc "$q")&limit=500&start=now-1h&end=now"
	local soak_ms=$((SOAK_SECONDS * 1000))

	curl -fsS "http://$HOST:$PORT/metrics" >"$soak/metrics-before.txt" 2>/dev/null
	: >"$soak/summary.txt"
	echo "$(date +%s) $(ps -o rss= -p "$pid" | tr -d ' ')" >"$soak/rss.txt"

	touch "$soak/running"
	(
		while [ -f "$soak/running" ]; do
			sleep 5
			local rss
			rss=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')
			[ -n "$rss" ] && echo "$(date +%s) $rss" >>"$soak/rss.txt"
		done
	) &
	local sampler=$!

	"$BIN/oboe_loadgen" -addr "$HOST:$PORT" -path /health -label soak-health -conc 2 \
		-dur "$soak_ms" -warm 0 -timeout 2000 -json 2>/dev/null |
		jq -c '. + {scenario:"soak", tier:"small", rep:0}' >>"$RAW" &
	local health_pid=$!
	"$BIN/oboe_loadgen" -addr "$HOST:$PORT" -path "$path" -label soak-query -conc 1 \
		-dur "$soak_ms" -warm 0 -timeout 20000 -json 2>/dev/null |
		jq -c '. + {scenario:"soak", tier:"small", rep:0}' >>"$RAW" &
	local query_pid=$!
	# Wait only for the load generators: the sampler below loops until the
	# running file is removed, so `wait` with no arguments would deadlock.
	wait "$health_pid" "$query_pid"

	rm -f "$soak/running"
	wait "$sampler" 2>/dev/null
	curl -fsS "http://$HOST:$PORT/metrics" >"$soak/metrics-after.txt" 2>/dev/null
	local files
	files=$(lsof -p "$pid" 2>/dev/null | wc -l | tr -d ' ')
	echo "fd_entries_after=$files" >>"$soak/summary.txt"
	stop_server
}

# ---------------------------------------------------------------- main

build || exit 1
meta
make_bodies
[ -f "$OUT/results/raw.jsonl" ] && : >"$RAW"

if [ -z "$ONLY" ] || [ "$ONLY" = "write" ]; then
	start_server "$OUT/data-write" "$WRITE_PORT" "$WRITE_QPORT" write || exit 1
	PORT_SAVED=$PORT
	PORT="$WRITE_PORT"
	run_writes
	PORT="$PORT_SAVED"
	stop_server
fi

if [ -z "$ONLY" ] || [ "$ONLY" = "small" ]; then
	say "seed small tier"
	SMALL_META=$(seed small "$SMALL_ROWS" "$OUT/data-small")
	SMALL_BASE=$(echo "$SMALL_META" | jq -r 'select(.scenario=="seed") | .base_ts')
	SMALL_END=$(echo "$SMALL_META" | jq -r 'select(.scenario=="seed") | .end_ts')
	# Run the library queries immediately: their LogsQL uses relative time
	# windows (`_time:10m`), which drift if the HTTP stages run for minutes.
	run_query_lib small "1 8 32"

	start_server "$OUT/data-small" "$PORT" "$QPORT" small || exit 1
	run_health small
	run_queries small "$SMALL_BASE" "$SMALL_END" "1 8 32"
	run_cache
	run_queue
	run_health_new small
	stop_server
fi

if [ -z "$ONLY" ] || [ "$ONLY" = "big" ]; then
	say "seed big tier"
	BIG_META=$(seed big "$BIG_ROWS" "$OUT/data-big")
	BIG_BASE=$(echo "$BIG_META" | jq -r 'select(.scenario=="seed") | .base_ts')
	BIG_END=$(echo "$BIG_META" | jq -r 'select(.scenario=="seed") | .end_ts')
	run_query_lib big "1 8"
	start_server "$OUT/data-big" "$PORT" "$QPORT" big || exit 1
	run_queries big "$BIG_BASE" "$BIG_END" "1 8"
	stop_server
fi

run_soak

say "summarize"
python3 "$ROOT/bench/summarize.py" "$RAW" "$OUT/results/summary.md" "$OUT/soak" "$OUT/meta.md" || exit 1
say "done: $OUT/results/summary.md"
[ -s "$FAIL" ] && {
	echo "failures:" >&2
	cat "$FAIL" >&2
	exit 1
}
exit 0
