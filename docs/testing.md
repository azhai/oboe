# 测试指南

oboe 的测试跟随所属包，分别位于 `common/`、`netio/`、`logdb/`、`cache/`、`queue/`、
`web/` 下的 `*_test.odin`，使用 Odin 内置的 `core:testing` 测试运行器。
默认套件共 **87 个用例**：库包 68 个（`common` 10、`logdb` 26、`cache` 13、
`queue` 6、`netio` 1、`web` 12），`tests/` 的 socket 端到端 19 个（HTTP 11、Queue 8）。
并行执行，且在此构建下无内存泄漏或 bad-free 报告。
`tests/stress_test.odin` 里的 4 个并发压力用例默认不参与编译，开启后共 **91 个**（见下文）。

## 1. 运行

需要 Odin `dev-2026-10` 或更新版本。

```bash
# 构建
odin build cmd -out:oboe

# 全部测试（tests 包 blank import 所有库包，-all-packages 会带上各包的测试）
odin test tests -all-packages

# 追加并发压力测试
odin test tests -all-packages -define:ODIN_STRESS=1

# 只跑某个包（开发期排查更快，不涉及 tests/）
odin test logdb
odin test cache
odin test web

# 库用法示例（不启动端口，打印存储/查询、缓存、队列结果）
odin run examples/embed
```

CI 门禁脚本按顺序执行上述构建与测试步骤：

```bash
./scripts/ci.sh
# 可用 ODIN=/path/to/odin 覆盖编译器，OUT_DIR=/path/out 覆盖产物目录
```

脚本依次执行：`odin build cmd`、默认测试、`ODIN_STRESS=1` 测试、`odin build bench`、
`odin build bench/loadgen`、`odin run examples/embed`。仓库当前没有可确认可用的 Odin 镜像或 Gitea action，因此未
添加 `.gitea/workflows/ci.yml`；CI 环境就绪后直接调用该脚本即可。

常用选项（通过 `-define:` 传给测试运行器）：

```bash
# 只跑指定用例（可重复 -define 或用逗号分隔）
odin test tests -all-packages -define:ODIN_TEST_NAMES=common.test_parse_time_ns

# 单线程执行，便于定位并发/分配器问题
odin test tests -all-packages -define:ODIN_TEST_THREADS=1

# 固定随机种子（部分测试使用随机数辅助构造数据）
odin test tests -all-packages -define:ODIN_TEST_RANDOM_SEED=1
```

`odin test` 默认开启内存跟踪。成功时输出只有一行：

```
Finished 87 tests in 1.264304s. All tests were successful.
```

开启压力测试（`-define:ODIN_STRESS=1`）后为 `Finished 91 tests ...`。

若出现 `+++ leak` 或 `bad free`，测试仍可能通过，但说明有分配未释放；这类问题应
视为失败修复。

## 2. 用例分布

| 文件 | 用例 | 覆盖内容 |
| --- | --- | --- |
| `common/util_test.odin` | 7 | 大小/时长/时间解析、UTC 日历与 RFC3339、字符串与 token、百分号解码、缓冲区小端编码/uvarint/JSON 转义、arena 对齐与重置 |
| `logdb/bloom_test.odin` | 3 | 无假阴性（token + 完整值）、假阳性率上界、构建上限与空 builder |
| `common/compress_test.odin` | 3 | zlib 往返与压缩率、空/损坏输入、zlib/gzip 自动识别与解压上限 |
| `logdb/jsonline_test.odin` | 5 | 扁平/嵌套解析、转义与代理对、自定义 `_msg`/`_time` 字段、重复键、字段数上限、格式错误、stream id 稳定性 |
| `logdb/jsonline_more_test.odin` | 3 | 恰好 128 字段与超限丢弃、重复键覆盖、嵌套与 pretty 空白、单字节/BMP/代理对/非法转义、孤立代理项 |
| `logdb/storage_test.odin` | 3 | 块编解码、5000 行落盘/读回/合并、合并后重启的行数与序号恢复 |
| `logdb/storage_more_test.odin` | 6 | retention（过期分区、`retention_days=0` 保留）、超过 `merge_threshold` 的多路合并、`merged_from` 重启去重、不完整 part 清理、损坏 `data.bin`/`index.bin`/`bloom.bin` 拒绝、part 序号不复用 |
| `logdb/query_test.odin` | 2 | LogsQL 解析（过滤器/管道/时间）、查询执行（裁剪、limit、sort、stats、字段裁剪） |
| `logdb/query_more_test.odin` | 4 | 240 行随机数据对比暴力扫描 oracle（filter/stats/sort/limit）、limit 边界与非法值、畸形 query 语料零泄漏、时间表达式表驱动 |
| `web/handlers_test.odin` | 4 | 进程内 `dispatch`：写入→查询→字段名/值→指标/UI/错误码；缓存 HTTP API；ES bulk 与 Loki push；参数与分行工具 |
| `web/jsonline_more_test.odin` | 3 | ES bulk 的 index/delete action、空行与 CRLF、默认 `message`/`@timestamp` 映射与自定义字段、Loki 多 stream、秒与纳秒判定、204 与畸形输入 |
| `web/metrics_test.odin` | 3 | `/metrics` 全部指标名与格式（HELP/TYPE、整数采样）、计数器单调递增（ingested/queries/cache hits/misses）、未配置 jobq 时无队列指标 / 配置后 7 个 `oboe_queue_*` 指标存在且数值取自该队列 |
| `netio/http_test.odin` | 1 | 监听地址解析 |
| `web/http_test.odin` | 2 | 状态码文本、CRLF 行扫描、未启动 server 的销毁与重复 stop |
| `cache/cache_test.odin` | 9 | 基本读写删、TTL 过期、容量/value/key 限制、桶内整代淘汰、条目上限、大值 FIFO、超桶预算的大值、快照保存/加载（几何校验、过期过滤、损坏文件）、4 线程并发读写 |
| `cache/cache_file_more_test.odin` | 4 | 截断/半写快照拒绝、header 与压缩载荷损坏拒绝、几何参数不匹配、并发 save+load、过期项加载过滤 |
| `queue/queue_test.odin` | 6 | 优先级与 reserve/delete、delay、TTR 超时重入、bury/release/kick、kick/pause、上限与断线回收 |
| `tests/http_e2e_test.odin` | 11 | **真实 socket**：keep-alive 三次请求复用连接、chunked 请求体、`Expect: 100-continue`、gzip 请求体、body 上限 413 与恰好等于上限、超长 header 431、Content-Length 与 Transfer-Encoding 冲突 400、非法 chunk 400、OPTIONS/CORS 与 404/405、stop 释放端口 |
| `tests/queue_e2e_test.odin` | 8 | **真实 socket**：put/reserve/delete、优先级、delay、TTR 超时重入、bury/release/kick、pause tube、use/watch/ignore/stats、断线回收 reserved |
| `tests/stress_test.odin` | 4 | **按需**：并发写入 + 并发 flush/merge + 并发查询的最终一致性、缓存 8 线程读写删校验、缓存边写边保存并重载校验、队列 4 生产者/4 消费者恰一次投递 |

`web` 包的 12 个用例（按文件逐个列出，对应上表的 4 + 2 + 3 + 3）：

- `handlers_test.odin`（4）：`test_dispatch_ingest_query_metrics`、`test_dispatch_cache_api`、`test_dispatch_elastic_and_loki`、`test_params_and_line_helpers`
- `http_test.odin`（2）：`test_server_destroy_without_run`、`test_status_and_crlf_helpers`
- `jsonline_more_test.odin`（3）：`test_web_jsonline_more_elastic_bulk`、`test_web_jsonline_more_loki_streams`、`test_web_jsonline_more_loki_errors`
- `metrics_test.odin`（3）：`test_metrics_names_and_format`、`test_metrics_counters_monotonic`、`test_metrics_queue_names`

### 并发压力测试

`stress_test.odin` 整体包在 `when #config(ODIN_STRESS, 0) != 0` 内，未开启时连代码都不
参与编译，因此普通测试数量不受影响。开启后：

- **存储**：8 个线程各写 1 万行（共 8 万行、8 个 stream），同时后台线程反复
  `store_flush_due` + `store_merge_all`，8 个线程持续执行 `limit` 查询；结束后全量落盘，
  用 `stats count()` 校验总数、`level`/`service` 及组合过滤的精确行数。
- **缓存**：8 个线程各持有 64 个互不重叠的 key，4 轮 set/get 校验、删除半数再校验，
  并验证共享 key 与 TTL 过期。
- **缓存快照**：在 8 个写线程持续 set/get 的同时，后台线程每 2 ms 调用一次
  `cache_save_file`；结束后保存最终快照并加载，校验条目数、字节数、所有未删除 key
  的值以及 100 KiB 大值完整保留。
- **队列**：4 生产者各 put 5000 个 job，4 消费者 reserve/delete，按 job id 位图校验
  “恰好消费一次”，最终 `current_jobs/ready/reserved` 归零。

这些用例会把共享结构（Cache/Store/Queue）交给 worker 线程分配和释放，因此测试体
入口把 `context.allocator` 临时切换为进程堆（`runtime.default_allocator()`）。测试运行
器的 per-task allocator 是单线程的 rollback stack，跨线程使用会破坏内存。

### 执行侧重点

- **存储测试**使用进程唯一的临时目录（`/tmp/oboe_<name>_<pid>`），开始前
  `remove_all`，结束前关闭 store 并再次清理。测试直接调用
  `store_ingest`/`store_flush_all`/`store_merge_all`，不依赖后台线程。
- **HTTP 测试**不打开端口：构造 `Http_Request` 后调用 `dispatch`，响应体在
  `arena` 释放前复制成测试持有的字符串。
- **socket 端到端测试**位于 `tests/`，只读调用 `tests/testutil.odin` 的 helper 启动
  server/queue 并绑定 `127.0.0.1:0` 临时端口，目录带进程号；不访问外部网络，也不占用
  固定端口。helper 本身由该目录的维护方负责，其他测试不要修改。
- **队列测试**直接调用队列 API 与 `queue_tick` 等内部函数，避免 50 ms 后台节拍带来
  的不确定性。
- **缓存并发测试**让 4 个线程各做 5000 次 set/get/delete；缓存条目由缓存自身捕获
  的 allocator 分配，因此在测试的跟踪分配器下也不会出现跨线程 bad-free。

## 3. 编写约定

1. 测试过程命名为 `test_*`，并加 `@(test)` 属性；同一文件内保持主题集中。
2. 断言优先使用 `testing.expect(t, cond)` 和 `testing.expect_value(t, got, want)`。
   注意 `expect_value` 没有消息参数（第 4 个参数是源码位置）。
3. 每个用例自行申请、释放内存；Odin 测试运行器会把未释放的分配报告为 leak。
4. 切片字面量不能直接用于 `for ... in []string{...}`：先赋给局部变量再遍历。
5. 时间相关断言留出余量（例如 TTL 10 ms 后 sleep 30 ms），不要依赖精确调度。
6. 文件系统测试使用 `/tmp/oboe_<name>_<pid>`（或 `tests/testutil.odin` 的
   `test_tmpdir`，额外带用例序号），保证可重复执行；多个 `odin test` 进程可以并行，
   不要恢复成固定目录。
7. 需要构造响应/查询结果时，复制到 arena 或测试自己的缓冲，避免持有已销毁的
   `Buf`/`Decoded_Block` 内存。
8. 只在边界处验证输入；内部函数按调用方已校验处理，不额外堆叠防御性检查。

## 4. 常见问题

**Q：测试输出 `+++ leak` 或 `bad free`？**

先单线程复现：

```bash
odin test tests -all-packages -define:ODIN_TEST_THREADS=1 -define:ODIN_TEST_NAMES=common.test_xxx
```

- `leak` 通常是忘了 `delete` 动态数组/字符串/arena，或文件目录条目未用
  `os.file_info_slice_delete` 释放。
- `bad free` 通常是分配和释放使用了不同 allocator。跨线程长期存活的数据应显式
  绑定 owner 的 allocator（参考 `Cache.allocator`）。

**Q：什么时候需要跑 `-define:ODIN_STRESS=1`？**

改动存储 flush/merge、缓存桶/分代或快照格式、队列并发路径，或发布前做回归时。压力
测试会真正启动几十个线程并写 `/tmp/oboe_stress_store_<pid>`，耗时明显长于普通用例；日常
修改工具函数或解析器时只跑默认套件即可。

**Q：如何只验证改名/构建没有破坏代码？**

```bash
odin build cmd -out:oboe
odin test tests -all-packages
grep -rn 'odlog' common netio logdb cache queue web cmd tests README.md docs
```

最后一条应无输出（`ODLI`/`ODLB` 是磁盘格式魔数，允许保留）。

**Q：测试会改到 `./data` 吗？**

不会。所有存储测试都显式指定 `/tmp` 下的目录，并自行清理。

**Q：可以并行跑多个 `odin test` 吗？**

可以。所有文件系统用例都使用带进程号（部分还带用例序号）的唯一目录，不同进程不会
争用同一个 `/tmp/oboe_*`；同一进程内由测试运行器串行调度同一用例。

## 5. 新增测试的检查清单

- [ ] 新用例在对应包内（`odin test tests -all-packages`）通过，且没有 leak/bad-free 报告。
- [ ] 若改动了公开 API 或组件生命周期，`odin run examples/embed` 仍能正常退出。
- [ ] 不依赖外部网络；端到端用例只绑定 `127.0.0.1:0` 临时端口，且不占用固定端口。
- [ ] 临时文件/目录在成功和失败路径上都会清理。
- [ ] 覆盖至少一个正常路径和一个边界/错误路径。
- [ ] 若发现生产代码缺陷，先修生产代码，再保留回归测试。
