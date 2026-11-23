# training-checkpoint-io-barrier-validator

验证训练框架检查点的 I/O 屏障：异步写入多个分片文件后立即发布清单时，
进程崩溃会导致"清单可见但数据不完整"。本项目在 **Python 3.8.2** 语法
兼容的前提下，用事件回放的方式验证持久化顺序不变量。

> **所有存储均为本地模拟。** `SimulatedStorage` 在内存中模拟本地文件
> 系统的页缓存语义，不触碰任何真实磁盘、分布式文件系统或对象存储。

## 模型

事件日志按序回放，支持的事件：

| 事件 | 含义 |
| --- | --- |
| `write_file` | rank 异步写入分片，仅进入页缓存 |
| `fsync` | 将单个文件内容刷入持久存储 |
| `dir_sync` | fsync 目录，使新建目录项在崩溃后仍可恢复 |
| `manifest_commit` | 发布检查点清单（自身会 fsync + 目录同步） |
| `rank_complete` | 单个 rank 报告完成（**不等于全局 barrier**） |
| `rank_crash` | rank/进程崩溃，页缓存全部丢失 |
| `retry` | 框架重试持久化某个文件 |

恢复时文件可见的条件：内容已 fsync **且** 父目录已同步，两者缺一不可。

## 判定规则

1. 清单发布必须晚于全部依赖文件的持久化，否则记为 barrier 违规。
2. 单 rank 完成不能代表全局 barrier；清单提交时所有期望 rank 必须已完成。
3. 崩溃恢复后，清单与其列出的所有文件均可见，检查点才可发布。

报告输出：可发布检查点（`publishable_checkpoints`）、未持久化文件
（`unpersisted_files`）、重试步骤（`retry_steps`）、违规记录与恢复后
可见文件列表。

## 运行

```bash
# 演示：有缺陷场景（清单可见、分片丢失）与修复场景
python -m checkpoint_barrier

# 单元测试：异步写丢失、目录同步缺失、rank 崩溃、全局 barrier、正常路径
python -m unittest discover -s tests -v
```

## 结构

- `checkpoint_barrier/events.py` — 事件类型定义
- `checkpoint_barrier/storage.py` — 本地模拟存储（页缓存 / fsync / 目录同步 / 崩溃）
- `checkpoint_barrier/validator.py` — 事件回放与屏障校验，生成报告
- `checkpoint_barrier/__main__.py` — 演示 CLI
- `tests/test_checkpoint_barrier.py` — unittest 测试
