# Telemetry Tiered Storage

本地遥测数据的分层存储与自适应降冷系统。接收带时间戳的多维遥测样本，按访问频率与样本年龄在**热层**（内存 + WAL）与**冷层**（压缩归档块）之间自动迁移，按保留策略降采样与删除，并保证迁移、查询与写入并发时的结果一致。

## 构建与测试

```bash
mvn compile     # 编译
mvn test        # 运行全部 27 个自动化测试
mvn spring-boot:run   # 启动服务（默认端口 8080，数据目录 ./data）
```

## 架构

```
                ┌────────────────────────────────────────────┐
                │            TieredStorageEngine             │
                │  （全局公平读写锁；WAL 顺序 == 内存顺序）   │
                └───────┬───────────────────────┬────────────┘
                        │                       │
        ┌───────────────▼──────────┐   ┌────────▼─────────────┐
        │   HotStore（热层）        │   │  ColdStore（冷层）    │
        │  内存索引 (ts,id) 有序    │   │  cold/<stream>/       │
        │  + hot/wal.log（fsync）  │   │  <min>-<max>-<n>.tsb  │
        │  + snapshot.json 检查点   │   │  GZIP 压缩归档块      │
        └──────────────────────────┘   └──────────────────────┘
```

- **WAL 是唯一事实来源**：写入（`PUT`）与所有层间迁移（`ARCHIVE`/`UNARCHIVE`/`DELETE_BLOCK`/`REPLACE`/`DROP`）都先落 WAL（每条 fsync）再改内存状态。
- **恢复 = 加载快照 + 重放 WAL + 清理孤儿块文件**。重放处理器全部幂等。
- 后台 `MigrationService` / `RetentionService` 按 `telemetry.scan-interval` 周期运行；也可通过 REST 手动触发。

## 一致性语义

1. **样本标识唯一**：每个样本携带全局唯一 id（UUID），是跨层去重的键。
2. **读写隔离**：写入与迁移状态变更持写锁（临界区为 O(批次)）；查询持读锁，看到的任一样本**恰好处于一个层**。块文件 IO 在锁外完成，WAL 追加在写锁内完成，因此持久顺序与内存顺序一致。
3. **迁移不丢不重**：
   - 降冷：先写块文件（tmp + fsync + 原子 rename）→ 再追加 `ARCHIVE` → 再从热层移除。崩溃要么留下孤儿文件（恢复时删除），要么完整提交，绝不少样本。
   - 升温：`UNARCHIVE`（记录内携带样本数据，回放不依赖块文件）与 `DELETE_BLOCK` 在同一写锁区间、同一次 fsync 追加，原子生效。
   - 查询合并热层与冷层结果时按 id 去重，即使并发迁移造成瞬态重叠也不会返回重复样本。
4. **崩溃恢复**：未完成的迁移从 WAL 状态继续——已落 WAL 的完整重放，未落 WAL 的块文件作为孤儿清理。恢复后查询结果与崩溃前完全一致（有测试覆盖）。
5. **保留删除**：过期块经 `DELETE_BLOCK` 记录删除；降采样经 `REPLACE` 记录原子替换旧块。删除与并发写入/迁移不会丢失已提交样本，也不留悬空引用（恢复时孤儿文件清理兜底）。
6. **内存有界**：热层样本数硬上限 `max-hot-samples`（写入路径同步触发强制降冷）；查询结果集上限 `max-result-samples`（超限抛 `QueryResultTooLargeException`）；访问热度跟踪器按流数与每流事件数双重有界（LRU 淘汰）。
7. **查询取消与超时**：`QueryOptions` 携带截止时间与合作式取消令牌，引擎在合并循环中周期性检查；取消/超时抛异常后不留下部分迁移状态或临时文件（查询路径本身不做任何迁移 IO）。

## 降冷 / 升温策略

- **降冷（demote）**：流的近期访问量低于 `promote-heat-threshold` 且样本年龄超过 `hot-max-age` 时，按 `migration-batch-size` 分批迁入冷层压缩块。
- **升温（promote）**：流在 `access-window` 内访问次数达到 `promote-heat-threshold` 时，其最新冷块按批迁回热层（受热层上限约束，避免立即回弹）。
- **强制降冷**：热层样本总数超过 `max-hot-samples` 时，无论年龄与热度，全局最老样本被强制降冷，保证内存有界。

## 保留策略

对每个流按样本年龄分三档：

| 年龄 | 处理 |
|---|---|
| `< raw-retention` | 保留原始样本 |
| `raw-retention` ~ `raw-retention + downsampled-retention` | 冷层块按 `downsample-resolution` 聚合为每字段平均值（`downsampled=true`） |
| `≥ raw-retention + downsampled-retention` | 删除（热层与冷层都生效） |

> 注意：降采样只作用于冷层块，配置时应保证 `hot-max-age < raw-retention`，让数据先降冷再降采样。

## 配置项（`telemetry.*`，见 `application.properties`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `data-dir` | `./data` | WAL、快照与冷层块的根目录 |
| `hot-max-age` | `1h` | 超过该年龄的样本成为降冷候选 |
| `access-window` | `10m` | 访问热度滑动窗口 |
| `promote-heat-threshold` | `3` | 窗口内访问达到该次数视为热流（升温并停止降冷） |
| `migration-batch-size` | `500` | 单次迁移批次的样本数（限制锁持有时间与内存） |
| `scan-interval` | `5s` | 迁移/保留后台扫描间隔 |
| `compression` | `GZIP` | 冷层块压缩方式：`NONE` / `GZIP`（块头自描述，可在线改配） |
| `raw-retention` | `1d` | 原始样本保留期 |
| `downsampled-retention` | `7d` | 原始保留期过后降采样数据的额外保留期 |
| `downsample-resolution` | `1m` | 降采样桶宽 |
| `max-hot-samples` | `100000` | 热层样本数硬上限 |
| `max-result-samples` | `100000` | 单次查询结果集硬上限 |
| `default-query-timeout-ms` | `30000` | 默认查询超时（毫秒） |
| `scheduling-enabled` | `true` | 是否启用后台迁移/保留调度 |

## REST API

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/api/telemetry/{stream}` | 写入样本：`{"samples":[{"timestamp":1000,"values":{"cpu":0.5}}]}`，返回分配的样本 id |
| `GET` | `/api/telemetry/{stream}?from=&to=&timeoutMs=` | 跨层查询，按时间有序、按 id 去重；超时返回 408，结果超限返回 413 |
| `GET` | `/api/telemetry/stats` | 热层样本数、冷层块数/样本数、跟踪流数 |
| `POST` | `/api/telemetry/admin/migrate` | 手动触发一次迁移扫描，返回迁移样本数 |
| `POST` | `/api/telemetry/admin/retention` | 手动触发一次保留策略执行，返回影响样本数 |

## 测试覆盖（27 个用例）

- `EngineBasicTest`：写入持久化、唯一标识、有序读取、崩溃后 WAL 恢复、流隔离
- `CrossTierQueryTest`：年龄降冷、热度升温、跨层查询有序去重、块压缩与按时间区间组织
- `ConcurrencyTest`：多线程写入 + 迁移 + 查询 + 保留并发，无重复无丢失
- `RecoveryTest`：迁移后崩溃、孤儿块清理、升温后崩溃、崩溃前后查询结果一致、反复重启稳定
- `RetentionTest`：降采样正确性（桶对齐、平均值）、到期删除、并发写入不丢、无悬空块文件
- `MemoryBoundTest`：持续写入下热层不越界、重启后仍有界、结果集超限拒绝
- `QueryTimeoutTest`：超时与取消、取消后无临时文件与部分状态、引擎继续可用
- `ApiIntegrationTest`：REST 端到端写-迁移-查
