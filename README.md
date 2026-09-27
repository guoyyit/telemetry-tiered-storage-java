# Telemetry Tiered Storage

本地遥测数据的分层存储引擎（Spring Boot, Java 17）。接收带时间戳的多维遥测样本，
按**样本年龄**与**近期访问热度**在热层（内存 + WAL）与冷层（压缩归档文件）之间自动迁移，
支持可配置保留策略、崩溃恢复、并发一致查询与慢查询取消。

## 架构

```
write ──► WAL (fsync) ──► HotStore (内存, 按 stream 的时间有序索引)
                             │  ▲
                降冷 migrate │  │ 升温 warm-up (访问热度)
                             ▼  │
                        ColdStore (gzip 压缩归档, 按时间区间组织文件)
                             │
                        RetentionManager 到期删除
```

- **WriteAheadLog**：所有热层变更先追加 WAL 并 fsync，再应用到内存。记录带
  magic/长度/CRC32 帧，崩溃产生的撕裂尾记录在重放时被安全忽略。
- **HotStore**：每 stream 一条 `(timestamp, id)` 有序索引，并追踪每个样本的最近
  访问时间，供降冷候选选择与升温启发式使用。
- **ColdStore**：目录布局 `<dataDir>/cold/<stream>/<from>-<to>-<seq>.arc[.gz]`。
  每个文件是一个按时间排序的样本批次，文件名即其时间区间。写入先落 `.tmp`
  再原子 rename；启动时清理残留 `.tmp`。
- **MigrationJournal**：迁移批次在开始前落盘（`<dataDir>/journal/migrations.journal`），
  完成后才移除。崩溃后未完成的批次在重启时被幂等地向前回滚（roll-forward）。
- **TieringManager / RetentionManager**：按 `scan-interval` 周期运行，批次大小受
  `migration-batch-size` 限制。

## 一致性语义

1. **写入持久性**：`write` 返回成功即已 WAL fsync；重启后样本可读取。样本 id 为
   UUID，全局唯一。
2. **迁移一致性**：迁移严格按「先写冷层、再删热层」的顺序执行，因此任一样本在任意
   时刻至少存在于一层。查询全程持有迁移读锁，热层删除（写锁临界区）无法与查询
   交错；短暂同时存在于两层的样本在结果中按 id 去重。故迁移期间查询**不重复、
   不缺失**，并发写入也不会被迁移破坏（id 唯一，写与迁移操作不同 key 空间）。
3. **崩溃恢复**：重启时先重放 WAL 重建热层，再对 journal 中每个 pending 批次做
   roll-forward：已归档的样本经 `findExistingIds` 幂等跳过，未归档的重新归档，
   随后从热层移除并 `markDone`。不会重复迁移（查询层按 id 去重兜底），也不会
   丢失样本；恢复后查询结果与崩溃前一致。
4. **保留删除**：热层过期样本在写锁内分批删除（WAL delete + 内存移除），冷层仅
   删除**整个时间区间**都过期的归档文件。删除不会留下悬空引用：重启重放 WAL 后
   已删样本不会复活（delete 记录在 put 之后）。
5. **升温**：冷层样本被查询命中达到阈值后复制回热层（热层作为缓存，冷层归档仍是
   持久副本），查询去重保证逻辑上仍只有一份。
6. **内存有界**：热层样本数由 `max-hot-samples` 限制，写入路径在超限时同步触发
   强制降冷；冷层访问追踪表有上限（10k 条，超限清空重学）；迁移/删除均按批次
   进行，单次内存占用有界。
7. **查询超时与取消**：查询在独立线程池执行，超时即中断并抛
   `QueryCancelledException`。查询只读，不产生迁移状态或文件，因此取消/超时后
   不会留下部分迁移状态或无法回收的中间文件（冷层写入的 `.tmp` 文件仅在迁移
   路径出现，且启动时会被清理）。

## 配置项（`telemetry.storage.*`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `data-dir` | `data` | 所有持久化状态的根目录 |
| `hot-max-age` | `1h` | 样本年龄超过该值才允许降冷 |
| `cool-after-idle` | `30m` | 热样本最近未被访问超过该值才可降冷 |
| `raw-retention` | `7d` | 原始（热层）样本保留期，到期删除 |
| `downsampled-retention` | `30d` | 冷层归档保留期，整个文件区间到期后删除 |
| `migration-batch-size` | `500` | 单次迁移/删除批次的最大样本数 |
| `scan-interval` | `5s` | 降冷/升温/保留扫描间隔 |
| `compression` | `gzip` | 冷层压缩方式：`gzip` 或 `none` |
| `query-timeout` | `30s` | 默认查询超时 |
| `max-hot-samples` | `100000` | 热层内存样本数上限（写入路径强制维持） |

## REST API

```
POST /api/telemetry/{stream}?timestamp=<millis>&value=<double>
     Body(JSON, 可选): {"dim":"value", ...}
     → 200 TelemetrySample（含全局唯一 id）

GET  /api/telemetry/{stream}?from=<millis>&to=<millis>
     → 200 [TelemetrySample, ...]  跨热/冷层合并，按时间升序，无重复
```

## 构建与测试

```bash
mvn test          # 全部测试（含并发迁移、重启中断、过期删除、跨层查询、内存边界）
mvn spring-boot:run
```

测试覆盖：

- `StorageEngineTest` — 写入/唯一 id/持久化重启/跨层合并去重/迁移幂等
- `ConcurrentMigrationTest` — 迁移与读写并发：查询无重复、无缺失已提交样本
- `CrashRecoveryTest` — 迁移各阶段崩溃后的 roll-forward 恢复
- `RetentionTest` — 热层/冷层到期删除、删除与并发写入迁移竞争、重启无悬空引用
- `TieringManagerTest` — 降冷、升温、热层内存边界
- `ColdStoreTest` — 压缩归档、时间区间组织、区间过期删除、tmp 清理
- `QueryTimeoutTest` — 超时取消、取消后无残留中间文件、引擎状态完好
- `ApplicationTests` / `TelemetryControllerTest` — Spring 装配与 REST 端到端
