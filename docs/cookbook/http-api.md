# HTTP API

默认监听 `0.0.0.0:9428`。所有响应带宽松 CORS 头和 `Server: oboe`。

## 端点总表

| 方法 | 路径 | 成功 | 说明 |
| --- | --- | --- | --- |
| `POST` | `/insert/jsonline` | `200` JSON | NDJSON 写入 |
| `POST` | `/insert/elasticsearch/_bulk` | `200` JSON | ES bulk；`/insert/elasticsearch` 是别名 |
| `POST` | `/insert/loki/api/v1/push` | `204` 空 | Loki push |
| `GET` | `/select/logsql/query` | `200` NDJSON | LogsQL 查询 |
| `GET` | `/select/logsql/field_names` | `200` JSON | 字段名及出现次数 |
| `GET` | `/select/logsql/field_values` | `200` JSON | 字段值及出现次数，需 `field` 参数 |
| `GET` / `PUT` / `POST` / `DELETE` | `/cache/<key>` | 见[缓存](cache.md) | 缓存读写删 |
| `GET` | `/cache/stats`（或 `/cache`） | `200` JSON | 缓存统计 |
| `POST` | `/cache/save` | `200` JSON | 保存快照，需 `-cache-file` |
| `GET` | `/metrics` | `200` 文本 | Prometheus 指标 |
| `GET` | `/health` | `200` `OK` | 健康检查 |
| `GET` | `/`、`/index.html`、`/select` | `200` HTML | 内置 Web UI |
| `OPTIONS` | 任意路径 | `204` 空 | CORS 预检 |

路径末尾多一个 `/` 会被忽略（`/health/` 等同 `/health`）。未知路径返回 `404`
和 `{"error":"unknown path","path":"..."}`。

说明：查询端点的实现不校验 HTTP 方法（非 `GET` 也会执行查询），缓存和写入端点会
校验；上表按推荐用法列出。`/cache/` 前缀路由优先级最高，`/cache/<key>` 的 key 是
URL 解码后的剩余路径。

## 参数

| 端点 | 参数 |
| --- | --- |
| 三种写入 | `_msg_field`、`_time_field`、`_stream_fields` |
| `logsql/query` | `query`、`limit`、`start`、`end` |
| `logsql/field_names` | `query`、`limit` |
| `logsql/field_values` | `query`、`limit`、`field`（必填） |
| 缓存写入 | `ttl`（如 `30s`） |

详细语义见 [日志写入](logs-ingest.md)、[LogsQL](logsql.md)、[缓存](cache.md)。

## 状态码与错误体

| 状态码 | 典型场景 | 响应体 |
| --- | --- | --- |
| `200` | 成功 | 各端点自定义 |
| `204` | Loki 写入成功、`OPTIONS` | 空 |
| `400` | 查询/`start`/`end` 非法、`ttl` 非法、Loki JSON 非法、`/cache/save` 未配置文件、`field_values` 缺 `field`、`Content-Length` 与 `Transfer-Encoding` 并存、非法 chunk size/框架 | `{"error":"..."}` |
| `404` | 未知路径、缓存未命中 | JSON 或纯文本 `not found` |
| `405` | 写入端点或缓存统计/保存方法不对 | `{"error":"method not allowed"}` |
| `413` | 缓存 value 超过 `-cache-max-value`；请求体超过 256 MiB（`Content-Length` 声明或 chunked 累计） | `{"error":"value too large"}` / `{"error":"request body too large"}` |
| `431` | 请求头超过 64 KiB | `{"error":"request header too large"}` |
| `500` | `/cache/save` 写文件失败 | `{"error":"cache save failed"}` |
| `503` | 缓存被关闭时访问缓存端点；同时打开的连接数超过 `-max-conns`（默认 4096，`0` 为不限），此时还会带 `Retry-After: 1` | `{"error":"cache disabled"}` / `{"error":"too many connections"}` |

具体错误串：

- 查询语法：`{"error":"invalid query"}`
- 缓存 TTL：`{"error":"invalid ttl"}`
- 缓存 key 过长：`{"error":"key too long"}`
- `field_values`：`{"error":"missing field param"}`
- Loki：`invalid json` / `expected object` / `missing streams` /
  `streams must be an array`
- 写入方法错误：`{"error":"method not allowed"}`
- 请求框架错误：`{"error":"bad request"}`（`Content-Length` 与 `Transfer-Encoding` 并存、
  非法 chunk size / chunk 框架损坏）
- 请求体超限：`{"error":"request body too large"}`
- 请求头超限：`{"error":"request header too large"}`
- 连接数超限：`{"error":"too many connections"}`（响应带 `Retry-After: 1`）

## 响应类型

| 端点 | `Content-Type` |
| --- | --- |
| 查询 | `application/stream+json; charset=utf-8` |
| 字段名/值、写入摘要、缓存 JSON | `application/json` |
| 缓存命中 | `application/octet-stream` |
| 缓存未命中 | `text/plain; charset=utf-8` |
| `/metrics` | `text/plain; version=0.0.4; charset=utf-8` |
| `/health` | `text/plain; charset=utf-8` |
| UI | `text/html; charset=utf-8` |

查询响应头：

```text
X-Oboe-Scanned-Rows
X-Oboe-Matched-Rows
X-Oboe-Pruned-Blocks
X-Oboe-Elapsed-Ms
```

## CORS 与 OPTIONS

所有经 HTTP 服务发出的响应都带：

```text
Access-Control-Allow-Origin: *
Access-Control-Allow-Headers: *
Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS
```

任意路径的 `OPTIONS` 请求直接返回 `204`（空体），不区分端点和路径：

```bash
curl -sS -D- -o /dev/null -X OPTIONS 'http://127.0.0.1:9428/select/logsql/query'
```

## 健康检查与指标

```bash
curl -sS 'http://127.0.0.1:9428/health'
# OK

curl -sS 'http://127.0.0.1:9428/metrics' | head
```

`/metrics` 是 Prometheus 文本格式，指标分组：

| 组 | 指标 |
| --- | --- |
| 写入/存储 | `oboe_ingested_rows_total`、`oboe_ingested_bytes_total`、`oboe_flushed_parts_total`、`oboe_merged_parts_total`、`oboe_parts`、`oboe_partitions`、`oboe_mem_rows`、`oboe_rows`、`oboe_disk_bytes` |
| 查询/HTTP | `oboe_queries_total`、`oboe_http_requests_total`、`oboe_http_connections_total`、`oboe_uptime_seconds` |
| 缓存（启用时） | `oboe_cache_entries`、`oboe_cache_bytes`、`oboe_cache_hits_total`、`oboe_cache_misses_total`、`oboe_cache_evictions_total`、`oboe_cache_expirations_total`、`oboe_cache_collisions_total` |
| 队列（启用时） | `oboe_queue_jobs`、`oboe_queue_ready`、`oboe_queue_reserved`、`oboe_queue_delayed`、`oboe_queue_buried`、`oboe_queue_connections`、`oboe_queue_puts_total` |

Prometheus 抓取配置：

```yaml
scrape_configs:
  - job_name: oboe
    metrics_path: /metrics
    static_configs:
      - targets: ["127.0.0.1:9428"]
```

注意：抓取 `/metrics` 会调用 `cache_stats`，逐桶扫描 O(entries)；缓存条目很多时
不要用很短的抓取间隔。

## Web UI

浏览器打开 `http://127.0.0.1:9428/`。`/select` 和 `/index.html` 也指向同一份
编译期内嵌页面；页面只读查询，不修改数据。

## 请求限制

| 项 | 上限/默认 |
| --- | --- |
| 请求头 | 64 KiB |
| 请求体 | 256 MiB |
| 空闲读超时 | 60 s |
| 发送超时 | 30 s |
| HTTP worker | `-workers`，默认 8 |
| HTTP 连接 | `-max-conns`，默认 4096（`0` 为不限） |
| 排队连接 | 2^18 |
| 压缩 | 请求体支持 `Content-Encoding: gzip` |
| 分块 | 支持 `Transfer-Encoding: chunked` |
| 100-continue | 支持 `Expect: 100-continue` |
| keep-alive | HTTP/1.1 默认开启 |

超过上限时服务返回 JSON 错误体，并带 `Content-Length` 与 `Connection: close`，发完即
关闭连接，不再读取剩余请求体：

| 触发条件 | 状态码 |
| --- | --- |
| 请求头超过 64 KiB | `431` |
| 声明的 `Content-Length` 超过 256 MiB | `413` |
| chunked 累计 body 超过 256 MiB（在读到超限 chunk 时判定） | `413` |
| `Content-Length` 与 `Transfer-Encoding` 同时出现 | `400` |
| 非法 chunk size 或 chunk 框架损坏 | `400` |
| 同时打开的连接数超过 `-max-conns`（在 accept 后立即判定，不排队） | `503` |

正常请求、keep-alive、chunked、`Expect: 100-continue` 与 gzip 行为不受影响。

## 端点示例

```bash
# 写入两行
curl -sS -X POST 'http://127.0.0.1:9428/insert/jsonline' \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"_time":"2026-10-26T04:00:00Z","level":"error","service":"api","_msg":"boom"}\n{"_time":"2026-10-26T04:00:01Z","level":"info","service":"api","_msg":"ok"}\n'

# 查询
curl -sS -G 'http://127.0.0.1:9428/select/logsql/query' \
  --data-urlencode 'query=level:error | limit 10'

# 字段名
curl -sS -G 'http://127.0.0.1:9428/select/logsql/field_names' \
  --data-urlencode 'query=level:error'

# 字段值
curl -sS -G 'http://127.0.0.1:9428/select/logsql/field_values' \
  --data-urlencode 'query=level:error' \
  --data-urlencode 'field=service'

# 健康与指标
curl -sS 'http://127.0.0.1:9428/health'
curl -sS 'http://127.0.0.1:9428/metrics'
```
