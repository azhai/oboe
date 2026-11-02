# 参与开发

## 构建

需要 Odin `dev-2026-10` 或更新版本。

```bash
odin build cmd -out:oboe
./oboe -data ./data
```

`cmd/` 是唯一可执行入口；`common/`、`netio/`、`logdb/`、`cache/`、`queue/`、`web/`
都是库包，没有 `main`，不能单独构建为可执行文件。

## 测试

```bash
odin test tests -all-packages
odin test tests -all-packages -define:ODIN_STRESS=1
```

- 第一条跑常规用例；`ODIN_STRESS=1` 追加并发压力用例。
- 测试使用 `/tmp/oboe_*` 临时目录，结束后自动清理。
- 跨包端到端用例在 `tests/`，包内单元测试在对应包的 `*_test.odin`。
- 测试数量和覆盖范围以 [测试指南](docs/testing.md) 为准；不要在 README 或文档里
  写死未来会变化的用例数。

## 示例

```bash
odin run examples/logs
odin run examples/cache
odin run examples/queue
odin run examples/embed
```

示例必须能独立退出 0，使用唯一的 `/tmp` 目录并在结束时清理。不要新增默认就占用
固定端口的示例；需要验证 HTTP 时优先用 `web.dispatch`，或使用 `127.0.0.1:0` 让
系统分配临时端口。

## 格式与检查

- 使用 **tab** 缩进，保持与现有文件一致的空行和注释风格。
- 当前 Odin 工具链没有内置 `fmt` 子命令（`odin help` 只列出 `build/run/check/...`）。
  如果本地安装了 `odinfmt`，可以在提交前运行；否则至少保证 `odin check` 通过。
- 提交前至少运行：

```bash
odin build cmd -out:/tmp/oboe_ci/oboe
odin check examples/embed
odin test tests -all-packages
```

- 不要提交 `oboe` 二进制、`data/`、`/tmp/oboe_*` 或缓存快照。

## CI

CI 的统一入口是 `scripts/ci.sh`，提交前在仓库根目录运行：

```bash
scripts/ci.sh
```

脚本按顺序执行：

1. `odin version`（可用 `ODIN=/path/to/odin` 指定编译器）
2. `odin build cmd -out:/tmp/oboe_ci/oboe`
3. `odin test tests -all-packages`
4. `odin test tests -all-packages -define:ODIN_STRESS=1`
5. `odin build bench -out:/tmp/oboe_ci/oboe_bench`
6. `odin run examples/embed`

产物写到 `OUT_DIR`（默认 `/tmp/oboe_ci`），可用环境变量覆盖。脚本任一命令失败即
停止。文档链接不在脚本检查范围内，改动 `README.md`/`CONTRIBUTING.md`/`docs/` 后
请自行确认相对链接可达。

## 包边界规则

新增代码时遵守[包布局](docs/modules.md)：

```
common  ←  netio  ←  queue
common  ←  logdb
common  ←  cache
common  +  netio  +  logdb  +  cache  +  queue  ←  web  ←  cmd
```

- `common` 不导入任何业务包；`netio`、`logdb`、`cache` 只依赖 `common`。
- `queue` 依赖 `common` 和 `netio`；`web` 可以依赖所有库包；`cmd` 只做装配。
- 库包不能导入 `cmd`，也不能反向依赖 `web`。
- 需要跨层共享的类型由拥有数据的包导出最小接口，或在装配层适配；不要把业务类型
  塞进 `common` 来绕过依赖方向。
- 不要为了测试在库包里增加只有测试使用的公开函数；测试放 `tests/` 或包内
  `*_test.odin`。

## 文档

- 面向使用者的操作说明放 `docs/cookbook/`，索引见
  [docs/cookbook/README.md](docs/cookbook/README.md)。
- 架构和磁盘格式放 `docs/architecture.md`；包职责放 `docs/modules.md`；
  嵌入用法放 `docs/library.md`。
- 文档里的 API、默认值和限制必须对应当前实现；不要写不存在的函数或未验证的性能
  数字。
