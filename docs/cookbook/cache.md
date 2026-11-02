# 缓存

缓存实现参照 fastcache：固定数量的桶、桶内 64 KiB chunk 环形复用、按代淘汰，
可选 TTL 与文件快照。HTTP 服务和 Odin 程序内都可以使用。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `PUT` / `POST` | `/cache/<key>[?ttl=30s]` | 写入原始请求体 |
| `GET` | `/cache/<key>` | 命中返回原始字节，未命中 `404` |
| `DELETE` | `/cache/<key>` | 删除 |
| `GET` | `/cache/stats`（或 `/cache`、`/cache/`） | 统计 |
| `POST` | `/cache/save` | 立即保存到 `-cache-file` |

```bash
# 写入，30 秒后过期；省略 ttl 表示不过期
curl -sS -X PUT --data-binary 'hello' 'http://127.0.0.1:9428/cache/greeting?ttl=30s'

# 读取：命中 200 + 原始字节 + X-Cache: hit
curl -sS -D- 'http://127.0.0.1:9428/cache/greeting'

# 未命中：404 + 纯文本 "not found" + X-Cache: miss
curl -sS -D- 'http://127.0.0.1:9428/cache/missing'

# 删除；删除成功和不存在都返回 200
curl -sS -X DELETE 'http://127.0.0.1:9428/cache/greeting'

# 统计
curl -sS 'http://127.0.0.1:9428/cache/stats'

# 立即保存（需要启动时设置 -cache-file）
curl -sS -X POST 'http://127.0.0.1:9428/cache/save'
```

响应细节：

- 写入成功：`200` + `{"ok":true}` + `X-Cache: stored`。
- 读取命中：`200` + `Content-Type: application/octet-stream` + `X-Cache: hit`。
- 读取未命中：`404` + `Content-Type: text/plain` + `X-Cache: miss`。
- 删除：`200` + `{"deleted":true}` 或 `{"deleted":false}`，`X-Cache: deleted`
  或 `X-Cache: miss`。
- `ttl` 接受 `30s`、`5m`、`1h`、`100ms` 等（`ns/us/ms/s/m/h/d/w`）；
  `ttl=0s` 或省略表示不过期；负数或非法值返回 `400`。
- value 超过 `-cache-max-value` 返回 `413`；其他写入失败返回 `400`。
- key 是 URL 解码后的路径剩余部分，最长 1024 字节；空 key 视为 `/cache/stats`。
- `stats` 和 `save` 是保留 key，不能作为普通缓存键。
- 缓存被 `-no-cache` 关闭时，所有缓存端点返回 `503`
  `{"error":"cache disabled"}`；方法不匹配返回 `405`。

`/cache/stats` 返回：

```json
{
  "entries": 2,
  "bytes": 42,
  "hits": 10,
  "misses": 3,
  "evictions": 0,
  "expirations": 1,
  "sets": 4,
  "deletes": 1,
  "collisions": 0,
  "buckets": 512,
  "max_bytes": 268435456,
  "max_entries": 1000000
}
```

`bytes` 只统计 value 字节数，不含 key、记录头和 chunk 空闲空间。`entries` 是
清理过期项后的存活条目数。

## 库内 API

```odin
import "../../common"
import cachelib "../../cache"
import "core:fmt"
import "core:time"

cfg := cachelib.default_cache_config()
cfg.max_bytes = 64 << 20
cfg.max_entries = 100_000
cfg.max_value = 16 << 20

c := cachelib.cache_init(cfg)
defer cachelib.cache_destroy(c)

// value 会被复制，返回后可以释放入参。
cachelib.cache_set(c, "greeting", transmute([]u8)string("hello"), 0)
cachelib.cache_set(c, "session", transmute([]u8)string("token"), i64(30 * time.Second))

out := common.buf_init(64)
defer common.buf_destroy(&out)
if cachelib.cache_get_into(c, "greeting", &out) {
	// out 不会被 cache_get_into 清空；复用前先 common.buf_reset(&out)。
	fmt.println(string(out.data[:]))
}

// cache_get 返回调用方拥有的副本，用完 delete。
if value, ok := cachelib.cache_get(c, "greeting"); ok {
	defer delete(value)
	fmt.println(string(value))
}

cachelib.cache_delete(c, "greeting")
st := cachelib.cache_stats(c)
fmt.println(st.entries, st.bytes, st.hits, st.misses)
```

常用函数：

| 函数 | 说明 |
| --- | --- |
| `default_cache_config()` | `256MiB` / 100 万 key / 单值 16MiB |
| `cache_init(cfg)` / `cache_destroy(c)` | 创建/销毁；`cache_init` 固定当前 allocator |
| `cache_set(c, key, value, ttl_ns)` | `ttl_ns > 0` 设置过期；`0` 不过期 |
| `cache_get_into(c, key, &buf)` | 写入调用方的 `Buf`，不清空它；返回是否命中 |
| `cache_get(c, key)` | 返回 `[]u8` 副本，调用方 `delete` |
| `cache_delete(c, key)` | 返回是否删除到条目 |
| `cache_stats(c)` | 返回 `Cache_Stats`；会顺带清理过期项 |
| `cache_save_file(c, path)` | 保存快照，返回写入字节数 |
| `cache_load_file(cfg, path)` | 加载快照；配置几何不一致时失败 |

## TTL、大值与容量语义

- **TTL 是绝对纳秒时间戳**，不是滑动过期：读取命中不会续期。过期项在 `cache_get_into`
  命中时惰性删除，`cache_stats` 也会清理并计入 `expirations`。
- 单条记录（含 key 与 12 字节头）放不进 64 KiB chunk 时进入该桶的**大值 FIFO**。
  大值按 `max_bytes / bucket_count` 的字节预算淘汰最旧项；单条 value 即使超过该
  预算也会保留最新写入的那条，后续写入才会把它挤掉。
- 普通小值写入桶内环形 chunk。写满一圈后代数加一，旧代条目整体失效，计入
  `evictions`；这是**整代淘汰，不是 LRU**。
- `max_entries` 按桶均分，超出时优先淘汰大值，否则清空该桶的普通条目并推进代数。
  因此全局条目数是近似值。
- `max_bytes` 按桶均分，并决定每个桶的 chunk 数；`0` 表示不限制。
- key 为空或超过 1024 字节时 `cache_set` 返回 `false`；value 超过 `max_value`
  （且 `max_value > 0`）也返回 `false`。
- 读路径持桶共享锁，写/删持独占锁，可多线程并发调用。`cache_stats` 需要
  O(entries) 时间并短暂独占每个桶，不适合高频调用。
- 值以副本存储、读取返回独立副本，调用方释放入参或返回值不会影响缓存。

## 快照

默认纯内存；只有显式调用保存函数（或启动时设置 `-cache-file`）才会落盘。

```odin
import cachelib "../../cache"
import "core:fmt"
import "core:os"

os.make_directory_all("/tmp/oboe_cache") // 目标目录必须已存在
if bytes, ok := cachelib.cache_save_file(c, "/tmp/oboe_cache/cache.bin"); ok {
	fmt.println(bytes)
}

if loaded, ok := cachelib.cache_load_file(cfg, "/tmp/oboe_cache/cache.bin"); ok {
	defer cachelib.cache_destroy(loaded)
}
```

- 保存写 `<path>.tmp`，`sync` 后 `rename` 覆盖，每桶单独 zlib 压缩；保存期间缓存
  仍可读写，但快照不保证跨桶的同一时刻一致，每个桶内部自洽。
- 文件头记录魔数、版本、桶数、chunk 大小和每桶最大 chunk 数。加载时只校验魔数、
  版本、`chunk_size`，以及 `bucket_count` / `max_chunks` 是否与当前 `cfg` 推导出的
  几何参数一致；`max_entries` 不在校验项里，只有把它调到足以改变桶数（默认布局下
  低于约 2048）时才会影响加载。`-cache-size` 改变桶或每桶 chunk 数时会拒绝旧快照；
  只改 `-cache-max-value` 等非几何参数不会。
- 加载会丢弃已过期条目，重建 map 与 FIFO，因此 `entries/bytes` 可能与保存时略有
  差异；**计数器（hits/misses/sets 等）不写入快照**，加载后从 0 开始。
- 需要周期保存时可复用 CLI 的 `Cache_Saver` + `cache_saver_run`；`path` 字符串必须
  在 saver 存活期间保持有效：

```odin
import cachelib "../../cache"
import "core:thread"
import "core:sync"
import "core:time"

saver := cachelib.Cache_Saver{
	c        = c,
	path     = "/tmp/oboe_cache/cache.bin",
	interval = time.Minute,
}
t := thread.create_and_start_with_data(&saver, cachelib.cache_saver_run, self_cleanup = false)
defer {
	sync.atomic_store(&saver.stop, 1)
	thread.join(t)
	thread.destroy(t)
}
```

- 进程正常退出（含 CLI 的 `SIGINT`/`SIGTERM` 关停）时 CLI 会再保存一次；只有
  `kill -9`、断电或关停期间的第二次信号会跳过退出保存，此时依赖周期保存或外部
  `POST /cache/save`。

## 常见坑

| 现象 | 原因/处理 |
| --- | --- |
| 改了 `-cache-size` 且桶数/每桶 chunk 数变化后快照加载失败 | 几何参数不一致；用相同配置或接受空缓存启动 |
| 只改 `-cache-max-entries` 后快照仍能加载 | `max_entries` 不参与快照校验；只有低到改变桶数（约低于 2048）才会失败 |
| `cache_get_into` 结果里出现旧内容 | 该函数追加写入 `Buf`；复用前 `common.buf_reset` |
| 大 value 写入后很快消失 | 超过桶预算时按 FIFO 淘汰旧项；调大 `-cache-size` 或 `-cache-max-value` |
| `entries` 比设置的上限略多/略少 | 上限按桶均分且整桶清理，是近似值 |
| 高频抓 `/metrics` 变慢 | 每次都会调用 `cache_stats`，逐桶扫描 O(entries) |
| 缓存 key 叫 `stats`/`save` 读写异常 | 这两个是保留路径，不能作为普通 key |
| 重启后 `hits`/`misses` 归零 | 快照只保存条目，不保存计数器 |
| 进程被 `kill -9` 后快照落后 | 只有周期保存或 `POST /cache/save` 的最近一次生效 |
