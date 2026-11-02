# 日志写入

oboe 接受三种写入协议，全部写入同一个存储：

| 协议 | 路径 | 请求体 |
| --- | --- | --- |
| JSON line | `POST /insert/jsonline` | 每行一个 JSON 对象（NDJSON） |
| Elasticsearch bulk | `POST /insert/elasticsearch/_bulk`（别名 `/insert/elasticsearch`） | ES bulk 文本 |
| Grafana Loki push | `POST /insert/loki/api/v1/push` | Loki push JSON |

三种入口都支持查询参数 `_msg_field`、`_time_field`、`_stream_fields`（见
[字段映射](#字段映射)）。写入参数只影响本次请求，不改变服务端配置。

## JSON line

每行一个 JSON 对象；空行被跳过。一个对象最多保留 128 个普通字段
（`MAX_JSON_FIELDS`），重复键以最后出现的值为准，超出上限的字段会被忽略。

```bash
curl -X POST 'http://127.0.0.1:9428/insert/jsonline' \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"_time":"2026-10-26T04:00:00Z","level":"error","service":"api","_msg":"boom"}\n{"_time":"2026-10-26T04:00:01Z","level":"info","service":"api","_msg":"ok","latency_ms":"12"}\n'
```

响应：

```json
{"rows":2,"errors":0}
```

- `rows` 是实际写入的行数。
- `errors` 是本请求里解析失败（非法 JSON、`_time` 无法解析）而被丢弃的行数。
  部分行失败时仍返回 `200`，成功行照常写入。
- 缺少 `_time` 时时间戳取写入时刻；缺少 `_msg` 时消息为空字符串。
- 嵌套对象/数组不展开，按原始 JSON 文本存为一个普通字段值；数字、布尔、`null`
  也按原始文本保存。

## Elasticsearch bulk

跳过 action 行，只写入文档行；默认把 `message` 映射为 `_msg`、`@timestamp`
映射为 `_time`，可用 `_msg_field` / `_time_field` 覆盖。

```bash
curl -X POST 'http://127.0.0.1:9428/insert/elasticsearch/_bulk' \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"index":{"_index":"logs"}}\n{"@timestamp":"2026-10-26T04:00:00Z","message":"boom","service":"api","level":"error"}\n'
```

响应是 Elasticsearch 风格的摘要：

```json
{"took":1,"errors":false,"items":[]}
```

`took` 为本次写入耗时（毫秒）。当前实现不返回逐条 `items`；文档行解析失败时
只会计入内部丢弃，响应中的 `errors` 仍为 `false`。需要逐行错误统计时请使用
`/insert/jsonline` 的 `errors` 字段。

## Grafana Loki push

```bash
curl -X POST 'http://127.0.0.1:9428/insert/loki/api/v1/push' \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "streams": [
      {
        "stream": {"service": "api", "level": "error"},
        "values": [
          ["1793000000000000000", "boom"]
        ]
      }
    ]
  }'
```

成功返回 `204`，无响应体；JSON 结构不合法时返回 `400` 和
`{"error":"..."}`。

Loki 细节：

- `streams[].stream` 的标签会作为普通字段写入。
- `values` 的每一项必须是 `[时间戳, 消息]`，且两个元素都必须是 **JSON 字符串**；
  数字形式的时间戳会被跳过。
- 时间戳是十进制字符串：长度不超过 13 时按秒解释，否则按纳秒解释（与实现一致；
  13 位毫秒值也会被当作秒，接入毫秒时间戳时请先补零或改用 RFC3339）。
- 时间戳无法按十进制或 RFC3339 解析时，按写入时刻处理。
- stream id 由 `_stream_fields`（或服务端 `-stream-fields`）决定；两者都为空时
  使用该条 stream 的全部标签。标签本身始终作为普通字段保留，可被 LogsQL 过滤。

## 字段映射

三个查询参数：

| 参数 | 默认 | 作用 |
| --- | --- | --- |
| `_msg_field` | `_msg` | 用哪个 JSON 键作为消息 |
| `_time_field` | `_time` | 用哪个 JSON 键作为时间戳 |
| `_stream_fields` | 服务端 `-stream-fields` | 逗号分隔，决定 stream id |

```bash
curl -X POST \
  'http://127.0.0.1:9428/insert/jsonline?_msg_field=message&_time_field=ts&_stream_fields=service,level' \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"ts":"2026-10-26T04:00:00Z","message":"boom","service":"api","level":"error"}\n'
```

说明：

- `_time` / `_msg` 作为键名始终会被识别；配置 `_time_field` 只是增加一个别名。
  如果同时出现两个键，后出现者覆盖先出现者。
- `_stream_fields` 只影响 stream 分组，不影响字段是否被存储。字段顺序不影响
  stream id；缺失字段按空值参与哈希。
- 未配置 stream 字段时 stream id 为 `0`，所有行落在同一个 stream。

## 批量

一次请求可以包含任意多行（受请求体大小限制）。客户端按批发送即可提高吞吐：

```bash
# 每批 500 行，逐批 POST
split -l 500 events.ndjson chunk_
for f in chunk_*; do
  curl -sS -X POST 'http://127.0.0.1:9428/insert/jsonline' \
    -H 'Content-Type: application/x-ndjson' \
    --data-binary "@$f"
done
```

写入先进入 memtable，再由后台线程按 `-flush-interval`（默认 1s）或行数/字节阈值
落盘。需要立即落盘（例如脚本结束前）时，可通过库 API 调用 `store_flush_all`；
HTTP 没有强制 flush 端点。

## 错误响应与限制

| 情况 | 行为 |
| --- | --- |
| 方法不是 `POST` | `405` + `{"error":"method not allowed"}` |
| 声明或累计的请求体超过 256 MiB | `413` + `{"error":"request body too large"}`，带 `Connection: close` 发送后关闭 |
| 请求头超过 64 KiB | `431` + `{"error":"request header too large"}`，带 `Connection: close` 发送后关闭 |
| `Content-Length` 与 `Transfer-Encoding` 并存、非法 chunk size/框架 | `400` + `{"error":"bad request"}`，带 `Connection: close` 发送后关闭 |
| 单行 JSON 非法 | 该行计入 `errors`，其余行继续写入 |
| 单对象字段超过 128 个 | 多余字段静默忽略 |
| ES bulk 文档行非法 | 丢弃，但响应仍为 `errors:false` |
| Loki JSON 不合法 | `400` + JSON 错误信息 |
| Loki `values` 元素少于两项或前两项不是 JSON 字符串 | 跳过该项（多余元素忽略） |

请求体支持 `Content-Encoding: gzip`（写入前解压，解压后上限同为 256 MiB；超过或
解压失败时直接关闭连接，不返回状态码）：

```bash
gzip -c events.ndjson | curl -X POST 'http://127.0.0.1:9428/insert/jsonline' \
  -H 'Content-Type: application/x-ndjson' \
  -H 'Content-Encoding: gzip' \
  --data-binary @-
```

分块传输（`Transfer-Encoding: chunked`）和 `Expect: 100-continue` 也受支持。
