# LogsQL 查询

查询端点：`GET /select/logsql/query`。响应是 NDJSON（每行一个 JSON 对象），
`Content-Type: application/stream+json; charset=utf-8`。

最小示例：

```bash
curl -sS -G 'http://127.0.0.1:9428/select/logsql/query' \
  --data-urlencode 'query=level:error | limit 10'
```

## 过滤器

| 写法 | 语义 | 示例 |
| --- | --- | --- |
| `*` | 匹配全部 | `*` |
| 裸词 | 在 `_msg` 中按 **token** 匹配，忽略大小写 | `boom` |
| `"短语"` | 在 `_msg` 中按子串匹配，忽略大小写 | `"connection refused"` |
| `field:value` | 在该字段中按 **token** 匹配，忽略大小写 | `level:error` |
| `field:"短语"` | 在该字段中按子串匹配 | `_msg:"timed out"` |
| `field:=value` | 整值精确匹配（忽略大小写） | `service:=api` |
| `field:value*` | 前缀匹配（忽略大小写） | `service:api*` |
| `field:*` | 字段存在且值非空 | `user_id:*` |
| `field:>value` / `>=` / `<` / `<=` | 比较；两边都能解析为数字时按数值，否则按字符串字典序 | `latency_ms:>100` |
| `_time:[start, end]` | 时间范围，闭区间；`*` 表示不限 | `_time:[2026-10-26T00:00:00Z, 2026-10-27T00:00:00Z]` |
| `_time:>时间` / `_time:>=` / `_time:<` / `_time:<=` | 时间点比较 | `_time:>now-1h` |
| `_time:5m` | 过去 5 分钟（等价于 `[now-5m, now]`） | `_time:1h` |
| `NOT expr` / `-expr` | 取反 | `NOT level:error`、`-level:error` |
| `a OR b` | 或；`AND` 可省略，也可显式写 | `level:error OR level:warn` |
| `( ... )` | 分组 | `(level:error OR level:warn) AND service:api` |
| `in(a, b, c)` | `_msg` 上多个 token 的 OR | `in(boom, timeout)` |
| `field:(a, b)` | 该字段上多个 token 的 OR | `level:(error, warn)` |

要点：

- **token 匹配**按字母/数字/下划线切词，因此 `level:error` 不会匹配
  `error_code`；需要子串匹配时用 `field:"..."`。
- 所有字符串比较都不区分 ASCII 大小写（`_msg`、字段名、值、`NOT`/`OR` 关键字）。
- 比较运算先尝试数字；只有一边能转数字时退化为字符串比较，结果可能不符合直觉，
  建议对数值字段两边都写数字。
- `_time` 的区间两端都包含。`_time:>=ts` / `_time:=ts` 是单个纳秒时刻；
  通常用区间或相对时长。
- `field:` 的值里不能直接出现空格；含空格的短语写成 `field:"..."`。
- 只有 `WORD` / `EXACT` 过滤器能利用块级 bloom 裁剪；`PHRASE`、`PREFIX`、
  `RANGE`、`EXISTS` 需要解码候选块。

## 管道

管道用 `|` 分隔，可组合：

| 管道 | 语法 | 说明 |
| --- | --- | --- |
| 限制 | `limit N` | 最多输出 N 行；`0` 会回退到默认限制 |
| 排序 | `sort by (f1, f2 desc) [asc\|desc]` | 最大堆 top-N；`_time` 与数字字段按数值排序，其余按字符串 |
| 字段裁剪 | `fields a, b, c`（别名 `keep`） | 排序/聚合之后裁剪输出字段 |
| 聚合 | `stats [by (f1, f2)] count() as total, sum(x) as s, avg(x), min(x), max(x)` | 分组统计；`count()` 可省略字段 |

示例：

```bash
# 过滤 + 按时间倒序取前 10 条
curl -sS -G 'http://127.0.0.1:9428/select/logsql/query' \
  --data-urlencode 'query=service:api AND level:error | sort by (_time) desc | limit 10'

# 分组计数，并只保留两个字段
curl -sS -G 'http://127.0.0.1:9428/select/logsql/query' \
  --data-urlencode 'query=* | stats by (service) count() as total | fields service, total'

# 数值聚合
curl -sS -G 'http://127.0.0.1:9428/select/logsql/query' \
  --data-urlencode 'query=level:error | stats by (service) count() as total, avg(latency_ms) as avg_ms'
```

管道语义与顺序：

- 实现只取 **第一个** `limit`、**第一个** `sort`、**第一个** `stats` 和
  **最后一个** `fields`；它们在查询串里的先后顺序不影响结果。
- 无 `sort`、无 `stats` 时，扫到 `limit` 行即停止，适合"取最近 N 条"。
- `sort` 需要 `limit` 控制内存；未写 `limit` 时使用默认 1000。
- `stats` 必须扫描完所有匹配行（不会提前停止）；`limit` 限制的是输出分组数。
- `stats` 输出行没有 `_time` 和 `_msg`，只有 `by` 字段与聚合别名。
- 没有 `by` 且没有任何匹配时，仍输出一行零值：`count` 为 `0`，`sum`/`avg`/
  `min`/`max` 为空字符串。
- `sum`/`avg`/`min`/`max` 只累加能解析为数字的值；其他值被跳过。
- `count(field)` 接受参数但计数的是行数，与 `count()` 相同。

## 时间格式

`_time` 过滤器、`start`/`end` 参数都接受：

| 形式 | 示例 |
| --- | --- |
| RFC3339 | `2026-10-26T04:00:00Z` |
| Unix 秒（1–10 位数字） | `1793000000` |
| Unix 毫秒（11–13 位） | `1793000000000` |
| Unix 微秒（14–16 位） | `1793000000000000` |
| Unix 纳秒（17 位及以上） | `1793000000000000000` |
| 相对"现在" | `now`、`now-1h`、`now+5m` |
| 相对偏移 | `-1h`、`+30m` |
| 纯时长 | `5m`、`1h`、`30s`、`1d`、`1w` |

`_time:[start, end]` 里的时长按"现在减该时长"解释，例如
`_time:[1h, *]` 表示最近一小时。`start`/`end` 参数里的纯时长同样相对当前时间。

## HTTP 参数

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `query` | `*` | LogsQL 查询串；为空也按 `*` |
| `limit` | `1000` | 输出行数/分组数；上限 `1,000,000`；非法值回退默认 |
| `start` | - | 起始时间；与 `query` 内 `_time` 条件取 **较大** 值 |
| `end` | - | 结束时间；与 `query` 内 `_time` 条件取 **较小** 值 |

```bash
curl -sS -G 'http://127.0.0.1:9428/select/logsql/query' \
  --data-urlencode 'query=level:error' \
  --data-urlencode 'start=2026-10-26T00:00:00Z' \
  --data-urlencode 'end=2026-10-26T12:00:00Z' \
  --data-urlencode 'limit=50'
```

`start`/`end` 无法解析时返回 `400` 和 `{"error":"invalid query"}`。

## 响应头

```bash
curl -sS -D- -o /dev/null -G 'http://127.0.0.1:9428/select/logsql/query' \
  --data-urlencode 'query=level:error | limit 10'
```

| 响应头 | 含义 |
| --- | --- |
| `X-Oboe-Scanned-Rows` | 解码并检查的行数 |
| `X-Oboe-Matched-Rows` | 通过过滤器的行数 |
| `X-Oboe-Pruned-Blocks` | 因时间范围或 bloom 被跳过的块数 |
| `X-Oboe-Elapsed-Ms` | 本次查询耗时（毫秒） |

这些值每次请求实时计算，不进入 Prometheus 指标，避免高基数。

## 字段名与字段值

```bash
# 查询命中的字段名及出现次数（每行固定计入 _time、_msg）
curl -sS -G 'http://127.0.0.1:9428/select/logsql/field_names' \
  --data-urlencode 'query=level:error' \
  --data-urlencode 'limit=100'

# 某字段的值及出现次数；必须提供 field
curl -sS -G 'http://127.0.0.1:9428/select/logsql/field_values' \
  --data-urlencode 'query=level:error' \
  --data-urlencode 'field=service'
```

两个端点都返回：

```json
{"values":[{"value":"api","hits":2}]}
```

结果按 `hits` 降序、同次数按值升序排列；`limit` 默认 `1000`，
限制的是参与统计的查询行数，不是输出条数。

## 常见错误

| 情况 | 响应 |
| --- | --- |
| 查询语法错误 | `400` + `{"error":"invalid query"}` |
| `start`/`end` 解析失败 | 同上 |
| `field_values` 缺少 `field` | `400` + `{"error":"missing field param"}` |
| 查询成功但没有匹配 | `200`，空响应体（`stats` 除外，见上） |
