#!/usr/bin/env python3
"""Turn bench/run_matrix.sh raw output into Markdown tables.

Usage: summarize.py RAW_JSONL OUT_MD SOAK_DIR [META_MD]

Every cell is the median of the repetitions (default 3). A `*` suffix marks a
cell whose throughput spread (max-min)/median exceeds 30%, i.e. a noisy
measurement that should not be quoted as a stable number.
"""
import json
import os
import sys

SPREAD_LIMIT = 0.30

WRITE_ORDER = ["jsonline-1", "jsonline-100", "elastic-100", "loki-100"]
CACHE_ORDER = ["put-small", "get-hit-small", "get-miss-small",
               "put-large", "get-hit-large", "get-miss-large"]
QUERY_ORDER = ["filter", "stats", "sort", "narrow"]


# ---------------------------------------------------------------- helpers

def load(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                print("summarize: skipping malformed line: %s" % line[:120],
                      file=sys.stderr)
    return rows


def med(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return sorted(vals)[len(vals) // 2]


def spread(vals):
    vals = [v for v in vals if v is not None]
    if len(vals) < 2:
        return 0.0
    m = med(vals)
    if m == 0:
        return 0.0 if max(vals) == 0 else float("inf")
    return (max(vals) - min(vals)) / abs(m)


def cell(vals, fmt):
    m = med(vals)
    if m is None:
        return "-"
    s = fmt % m
    if spread(vals) > SPREAD_LIMIT:
        s += "*"
    return s


def num(v, fmt="%d"):
    return "-" if v is None else fmt % v


def group(rows, keyfn):
    out = {}
    for r in rows:
        out.setdefault(keyfn(r), []).append(r)
    return out


def table(headers, lines):
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join(["---"] * len(headers)) + "|"]
    out.extend(lines)
    return "\n".join(out)


def class_and_conc(label, conc):
    name = label
    if "-c" in label:
        name = label.rsplit("-c", 1)[0]
    return name, conc


# ---------------------------------------------------------------- sections

def sec_seed(rows):
    lines = []
    seeds = {r.get("tier"): r for r in rows if r.get("scenario") == "seed"}
    flushes = {r.get("tier"): r for r in rows if r.get("scenario") == "seed_flush"}
    for tier in ("small", "big"):
        s = seeds.get(tier)
        if not s:
            continue
        f = flushes.get(tier, {})
        lines.append("| %s | %s | %s | %s | %s | %s | %s |" % (
            tier, num(s.get("rows")),
            num(s.get("rows_per_s"), "%.0f"),
            num(f.get("rows_per_s"), "%.0f"),
            num(f.get("parts")), num(f.get("partitions")),
            num((f.get("disk_bytes") or 0) / (1 << 20), "%.1f"),
        ))
    if not lines:
        return ""
    return ("## 数据播种\n\n"
            "| tier | rows | ingest rows/s | flush rows/s | parts | partitions | disk MiB |\n"
            "|---|---|---|---|---|---|---|\n" + "\n".join(lines) + "\n")


def sec_health(rows, scenario, title, extra_note=""):
    recs = [r for r in rows if r.get("scenario") == scenario]
    if not recs:
        return ""
    g = group(recs, lambda r: (r.get("tier"), r.get("conc")))
    lines = []
    for (tier, conc) in sorted(g, key=lambda k: (str(k[0]), k[1] or 0)):
        rs = g[(tier, conc)]
        lines.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            tier, num(conc),
            cell([r.get("qps") for r in rs], "%.0f"),
            cell([r.get("p50_ms") for r in rs], "%.2f"),
            cell([r.get("p95_ms") for r in rs], "%.2f"),
            cell([r.get("p99_ms") for r in rs], "%.2f"),
            cell([r.get("served") for r in rs], "%d"),
            cell([r.get("stalled") for r in rs], "%d"),
            cell([r.get("wait_p50_ms") for r in rs], "%.1f"),
            cell([(r.get("http_err") or 0) + (r.get("transport_err") or 0) +
                  (r.get("dial_err") or 0) for r in rs], "%d"),
        ))
    head = "## %s\n\n" % title
    if extra_note:
        head += extra_note + "\n\n"
    return (head +
            "| tier | conc | qps | p50 ms | p95 ms | p99 ms | served | stalled | wait p50 ms | errors |\n"
            "|---|---|---|---|---|---|---|---|---|---|\n" + "\n".join(lines) + "\n")


def sec_write(rows):
    recs = [r for r in rows if r.get("scenario") == "write"]
    if not recs:
        return ""
    g = group(recs, lambda r: r.get("label"))
    lines = []
    order = [l for l in WRITE_ORDER if l in g] + [l for l in g if l not in WRITE_ORDER]
    for label in order:
        rs = g[label]
        body = max((r.get("req_body_bytes") or 0) for r in rs)
        lines.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            label,
            num(next((r.get("rows_per_req") for r in rs
                      if r.get("rows_per_req")), 0)),
            num(body / 1024.0, "%.1f"),
            cell([r.get("qps") for r in rs], "%.0f"),
            cell([r.get("rows_per_s") for r in rs], "%.0f"),
            cell([r.get("p50_ms") for r in rs], "%.2f"),
            cell([r.get("p95_ms") for r in rs], "%.2f"),
            cell([r.get("p99_ms") for r in rs], "%.2f"),
            cell([(r.get("http_err") or 0) + (r.get("transport_err") or 0) +
                  (r.get("dial_err") or 0) for r in rs], "%d"),
        ))
    detail = []
    for label in order:
        for r in sorted(g[label], key=lambda x: x.get("rep") or 0):
            detail.append("| %s | %s | %s | %s | %s | %s | %s |" % (
                label, num(r.get("rep")),
                num(r.get("qps"), "%.0f"), num(r.get("rows_per_s"), "%.0f"),
                num(r.get("p50_ms"), "%.2f"), num(r.get("p99_ms"), "%.2f"),
                num(r.get("requests"), "%d")))
    body_text = ("## HTTP 写入\n\n"
                 "| endpoint | rows/req | body KiB | qps | rows/s | p50 ms | p95 ms | p99 ms | errors |\n"
                 "|---|---|---|---|---|---|---|---|---|\n" + "\n".join(lines) + "\n")
    if len(detail) > len(order):
        body_text += ("\n### 写入重复明细\n\n"
                      "| endpoint | rep | qps | rows/s | p50 ms | p99 ms | requests |\n"
                      "|---|---|---|---|---|---|---|\n" + "\n".join(detail) + "\n")
    return body_text


def sec_query_http(rows):
    recs = [r for r in rows if r.get("scenario") == "query"]
    if not recs:
        return ""
    g = group(recs, lambda r: (r.get("tier"), class_and_conc(r.get("label", ""), r.get("conc"))))
    keys = sorted(g, key=lambda k: (
        str(k[0]), QUERY_ORDER.index(k[1][0]) if k[1][0] in QUERY_ORDER else 99,
        k[1][1] or 0))
    lines = []
    for (tier, (cls, conc)) in keys:
        rs = g[(tier, (cls, conc))]
        lines.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            tier, cls, num(conc),
            cell([r.get("qps") for r in rs], "%.2f"),
            cell([r.get("p50_ms") for r in rs], "%.2f"),
            cell([r.get("p95_ms") for r in rs], "%.2f"),
            cell([r.get("p99_ms") for r in rs], "%.2f"),
            cell([r.get("served") for r in rs], "%d"),
            cell([(r.get("http_err") or 0) + (r.get("transport_err") or 0) for r in rs], "%d"),
        ))
    return ("## LogsQL（HTTP /select/logsql/query）\n\n"
            "| tier | class | conc | qps | p50 ms | p95 ms | p99 ms | served | errors |\n"
            "|---|---|---|---|---|---|---|---|---|\n" + "\n".join(lines) + "\n")


def sec_query_lib(rows):
    recs = [r for r in rows if r.get("scenario") == "query_lib"]
    if not recs:
        return ""
    g = group(recs, lambda r: (r.get("tier"), r.get("class"), r.get("threads")))
    keys = sorted(g, key=lambda k: (
        str(k[0]), QUERY_ORDER.index(k[1]) if k[1] in QUERY_ORDER else 99,
        k[2] or 0))
    lines = []
    for (tier, cls, threads) in keys:
        rs = g[(tier, cls, threads)]
        lines.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            tier, cls, num(threads),
            cell([r.get("qps") for r in rs], "%.2f"),
            cell([r.get("p50_ms") for r in rs], "%.2f"),
            cell([r.get("p95_ms") for r in rs], "%.2f"),
            cell([r.get("p99_ms") for r in rs], "%.2f"),
            cell([r.get("scanned_rows") for r in rs], "%.0f"),
            cell([r.get("matched_rows") for r in rs], "%.0f"),
            cell([r.get("pruned_blocks") for r in rs], "%.0f"),
            cell([r.get("errors") for r in rs], "%d"),
        ))
    return ("## LogsQL（库内直接调用，无 HTTP）\n\n"
            "`scanned/matched/pruned` 为单次查询平均值。\n\n"
            "| tier | class | threads | qps | p50 ms | p95 ms | p99 ms | scanned | matched | pruned | errors |\n"
            "|---|---|---|---|---|---|---|---|---|---|---|\n" + "\n".join(lines) + "\n")


def sec_cache(rows):
    recs = [r for r in rows if r.get("scenario") == "cache"]
    if not recs:
        return ""
    g = group(recs, lambda r: r.get("label"))
    order = [l for l in CACHE_ORDER if l in g] + [l for l in g if l not in CACHE_ORDER]
    payload = {}
    for label in g:
        body = max((r.get("req_body_bytes") or 0) for r in g[label])
        if body:
            payload[label] = body
    small_payload = payload.get("put-small", 0)
    large_payload = payload.get("put-large", 0)

    def value_bytes(label):
        if label in payload:
            return payload[label]
        if "miss" in label:
            return 0
        return large_payload if "large" in label else small_payload

    lines = []
    for label in order:
        rs = g[label]
        mib = [((r.get("qps") or 0) * value_bytes(label) / (1 << 20)) for r in rs]
        lines.append("| %s | %s | %s | %s | %s | %s | %s | %s |" % (
            label,
            "8 MiB" if "large" in label else "64 B",
            num(value_bytes(label), "%d"),
            cell([r.get("qps") for r in rs], "%.0f"),
            cell(mib, "%.1f"),
            cell([r.get("p50_ms") for r in rs], "%.3f"),
            cell([r.get("p99_ms") for r in rs], "%.3f"),
            cell([(r.get("http_err") or 0) + (r.get("transport_err") or 0) +
                  (r.get("dial_err") or 0) for r in rs], "%d"),
        ))
    return ("## 缓存（HTTP，8 并发 keep-alive）\n\n"
            "`MiB/s` 是值本身的吞吐（qps × 每次传输的值大小），不是含 JSON 包装的 HTTP 字节数；"
            "miss 场景没有值传输，记 0。\n\n"
            "| 场景 | payload | value B | qps | MiB/s | p50 ms | p99 ms | errors |\n"
            "|---|---|---|---|---|---|---|---|\n" + "\n".join(lines) + "\n")


def sec_queue(rows):
    recs = [r for r in rows if r.get("scenario") == "queue_tcp"]
    if not recs:
        return ""
    order = ["put", "get", "cycle", "bury"]
    g = group(recs, lambda r: (r.get("qmode"), r.get("clients")))
    keys = sorted(g, key=lambda k: (
        order.index(k[0]) if k[0] in order else 99, k[1] or 0))
    lines = []
    for (mode, clients) in keys:
        rs = g[(mode, clients)]
        lines.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            mode, num(clients),
            cell([r.get("ops_per_s") for r in rs], "%.0f"),
            cell([r.get("p50_ms") for r in rs], "%.3f"),
            cell([r.get("p95_ms") for r in rs], "%.3f"),
            cell([r.get("p99_ms") for r in rs], "%.3f"),
            cell([r.get("kick_p50_ms") for r in rs], "%.3f"),
            cell([r.get("kick_p99_ms") for r in rs], "%.3f"),
            cell([r.get("errors") for r in rs], "%d"),
        ))
    return ("## 队列（TCP beanstalk 子集）\n\n"
            "| qmode | clients | ops/s | p50 ms | p95 ms | p99 ms | kick p50 ms | kick p99 ms | errors |\n"
            "|---|---|---|---|---|---|---|---|---|\n" + "\n".join(lines) + "\n")


def sec_soak(rows, soak_dir):
    lines = []
    parts = []

    rss_path = os.path.join(soak_dir, "rss.txt")
    if os.path.isfile(rss_path):
        samples = []
        with open(rss_path) as f:
            for line in f:
                bits = line.split()
                if len(bits) == 2:
                    try:
                        samples.append((int(bits[0]), int(bits[1])))
                    except ValueError:
                        pass
        if samples:
            first_ts, first = samples[0]
            last_ts, last = samples[-1]
            vals = [kb for _, kb in samples]
            span = last_ts - first_ts
            # Startup RSS is measured before queries touch the on-disk blocks,
            # so it is not a meaningful baseline. Use the first sample at least
            # 10 s in as the steady-state baseline.
            steady = next((kb for ts, kb in samples if ts - first_ts >= 10), first)
            parts.append(
                "RSS 采样 %d 次，跨度 %d s：启动 %.1f MiB，10 s 基线 %.1f MiB，"
                "结束 %.1f MiB，峰值 %.1f MiB，基线到结束 %+.1f%%（%+.1f MiB）。" % (
                    len(samples), span, first / 1024.0, steady / 1024.0,
                    last / 1024.0, max(vals) / 1024.0,
                    0.0 if steady == 0 else (last - steady) * 100.0 / steady,
                    (last - steady) / 1024.0))
            lines.append("| 启动 | 10 s 基线 | 结束 | 峰值 | 基线→结束 | 采样跨度 |")
            lines.append("|---|---|---|---|---|---|")
            lines.append("| %.1f MiB | %.1f MiB | %.1f MiB | %.1f MiB | %+.1f%% | %d s |" % (
                first / 1024.0, steady / 1024.0, last / 1024.0,
                max(vals) / 1024.0,
                0.0 if steady == 0 else (last - steady) * 100.0 / steady, span))
            lines.append("")

    metric_before = read_metrics(os.path.join(soak_dir, "metrics-before.txt"))
    metric_after = read_metrics(os.path.join(soak_dir, "metrics-after.txt"))
    if metric_before and metric_after:
        names = sorted(set(metric_before) | set(metric_after))
        lines.append("| metric | before | after | delta |")
        lines.append("|---|---|---|---|")
        anomalies = []
        for name in names:
            b = metric_before.get(name)
            a = metric_after.get(name)
            if a is None or b is None:
                lines.append("| %s | %s | %s | - |" % (
                    name, num(b, "%.0f"), num(a, "%.0f")))
                continue
            delta = a - b
            flag = ""
            if delta < 0 and (name.endswith("_total") or name == "oboe_uptime_seconds"):
                flag = " **非单调**"
                anomalies.append(name)
            lines.append("| %s | %.0f | %.0f | %+.0f%s |" % (name, b, a, delta, flag))
        lines.append("")
        if anomalies:
            parts.append("counter 回退：%s" % ", ".join(anomalies))
        else:
            parts.append("所有 counter 单调不减。")

    summary_path = os.path.join(soak_dir, "summary.txt")
    if os.path.isfile(summary_path):
        with open(summary_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    parts.append("`%s`" % line)

    recs = [r for r in rows if r.get("scenario") == "soak"]
    if recs:
        lines.append("| label | conc | qps | p50 ms | p95 ms | p99 ms | errors |")
        lines.append("|---|---|---|---|---|---|---|")
        for r in sorted(recs, key=lambda r: r.get("label") or ""):
            lines.append("| %s | %s | %s | %s | %s | %s | %s |" % (
                r.get("label"), num(r.get("conc")),
                num(r.get("qps"), "%.2f"), num(r.get("p50_ms"), "%.2f"),
                num(r.get("p95_ms"), "%.2f"), num(r.get("p99_ms"), "%.2f"),
                (r.get("http_err") or 0) + (r.get("transport_err") or 0) +
                (r.get("dial_err") or 0)))
        lines.append("")

    if not parts and not lines:
        return ""
    body = "## 长稳压测（soak）\n\n" + "\n\n".join(parts) + "\n"
    if lines:
        body += "\n" + "\n".join(lines) + "\n"
    return body


def read_metrics(path):
    out = {}
    if not os.path.isfile(path):
        return out
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            bits = line.split()
            if len(bits) < 2:
                continue
            try:
                out[bits[0]] = float(bits[1])
            except ValueError:
                continue
    return out


# ---------------------------------------------------------------- main

def main():
    if len(sys.argv) < 4:
        print("usage: summarize.py RAW.jsonl OUT.md SOAK_DIR [META.md]",
              file=sys.stderr)
        return 2
    raw, out_md, soak_dir = sys.argv[1], sys.argv[2], sys.argv[3]
    meta = sys.argv[4] if len(sys.argv) > 4 else ""

    rows = load(raw)
    reps = {}
    for r in rows:
        if r.get("scenario") in ("seed", "seed_flush", "soak"):
            continue
        reps.setdefault(r.get("scenario"), set()).add(r.get("rep"))
    nreps = max((len(v) for v in reps.values()), default=0)

    chunks = []
    if meta and os.path.isfile(meta):
        with open(meta, "r", encoding="utf-8") as f:
            chunks.append(f.read().rstrip() + "\n")
    chunks.append(
        "## 说明\n\n"
        "每格为 %d 次重复的中位数；`*` 表示该档吞吐极差/中位数 > %d%%，"
        "属于噪声较大的测量，不应作为稳定值引用。\n"
        "`served` 是测量窗口内至少完成一次请求的虚拟用户数，`stalled` 是其余；"
        "keep-alive 下当 `conc` 超过服务端 `-workers`（8）时，只有 8 个用户会被服务，"
        "其余排队，这正是并发模型的直接读数。\n" % (nreps, int(SPREAD_LIMIT * 100)))

    for fn in (sec_seed, sec_write, sec_query_http, sec_query_lib, sec_cache, sec_queue):
        chunks.append(fn(rows))
    chunks.append(sec_health(rows, "health", "HTTP `/health`（keep-alive）"))
    chunks.append(sec_health(rows, "health_new", "HTTP `/health`（每请求新建连接）"))
    chunks.append(sec_soak(rows, soak_dir))

    text = "\n".join(c for c in chunks if c)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(text)
        if not text.endswith("\n"):
            f.write("\n")
    print("summarize: wrote %s (%d records)" % (out_md, len(rows)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
