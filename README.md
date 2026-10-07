# multi-cluster-federation-go

本地可运行的多集群工作负载联邦编排组件。中心集群与多个边缘站点均由
**本地 JSON 文件存储 + 内存状态** 模拟，无需真实集群即可验证：
期望状态在容量、站点可用性与并发协调变化时，都能收敛到可预期的结果。

## 构建与测试

```bash
go build ./...
go test ./...          # 单元测试（日志含期望状态与关键判定依据）
go test -race ./...    # 并发协调的竞态验证
go test -v -run TestFailoverAndRecovery ./...   # 查看故障迁移判定日志
```

## 核心概念

| 概念 | 类型 | 说明 |
|---|---|---|
| 站点 | `Site` | 边缘站点：`Capacity`（容量单位）、`Labels`、`State`（Active/Draining/Failed） |
| 期望状态 | `WorkloadSpec` | 声明式负载：`Replicas`、`Size`（每实例容量）、`Selector`（标签约束）、`MutexKey`（互斥） |
| 实例 | `Instance` | 已放置实例，ID 由 `负载名-序号` 确定性派生 |
| 拒绝 | `Rejection` | 放置失败记录，含可区分原因与逐项判定依据 |
| 门面 | `Federation` | 串行化所有变更与协调，每轮协调后整体落盘 |

## 放置规则

每个实例按以下顺序评估站点（`placement.go`）：

1. **可用性**：仅 `Active` 站点可接收新实例；
2. **标签约束**：站点 Labels 须包含 `Selector` 的全部键值；
3. **互斥**：`MutexKey` 非空时，同一站点不允许两个相同 MutexKey 的实例共存；
4. **容量**：站点 `已用 + Size <= Capacity`，同一轮协调内试放置即计入，绝不超配。

可行站点中按 **(剩余容量降序, 名称升序)** 取首个——完全确定，
因此同一期望状态重复协调的放置结论稳定。

无法放置时给出可区分原因（不静默丢弃、不超量放置）：

| 原因 | 含义 |
|---|---|
| `NoEligibleSite` | 无任何 Active 站点 |
| `LabelMismatch` | 所有 Active 站点标签均不匹配 |
| `MutexConflict` | 标签全过，仅互斥约束挡住 |
| `InsufficientCapacity` | 标签全过，但容量不足 |

`Rejection.Detail` 记录每个站点逐项评估结果（如
`容量不足: edge-a(free=1,need=2)`），作为关键判定依据。

## 故障与恢复语义

- **Failed**：站点上实例视为丢失，下一轮协调在仍满足约束的站点上按原
  实例 ID 重建，容量不被突破；放不下时记录缺口（`Rejection`），
  任一时刻 `实例数 + 缺口数 == 期望数`。
- **Draining**：不再接收新实例，存量实例被迁出至其他可行站点。
- **恢复（Active）**：实例 ID 确定性派生（`负载名-序号`），协调只补缺口、
  不重复创建，因此恢复后不会出现重复实例，总数收敛回期望值。
- **容量调小**：超配站点按实例 ID 降序逐出，直至不超载。

## 声明调整（更新）语义

对已期望的负载再次 `Apply` 同名但字段不同的 `WorkloadSpec` 即为**更新**
（调整 `Size`/`MutexKey`/`Selector`），无需删除重建。下一轮协调时：

1. **原地同步**：存量实例的 `Size`/`MutexKey` 先被改写为最新声明，
   实例 ID（`负载名-序号`）不变——不凭空多实例、不丢实例；
2. **重新评估**：仍满足新声明的实例**留在原站点不动**；
   不再满足的（标签不匹配、互斥冲突、站点超配）被逐出，
   并优先改放到其他可行站点（日志记为 `migrate id: 旧站 -> 新站`），
   无处可去的进入缺口（`Rejection`，原因可区分）；
3. **容量强一致**：调大 `Size` 导致站点超配时，按实例 ID 降序逐出至不超载
   后再补放——落盘状态任何时刻都不超配，不会为放新规格临时超配；
4. **互斥按最新范围生效**：`MutexKey` 调整后同站冲突的实例分开
   （保留 ID 小者，结论确定），包括跨负载撞 key 的场景；
5. **稳定收敛**：同一新声明反复协调结论不变（已收敛后再协调为无操作），
   容量"刚好够"与"差一点放不下"都收敛到明确结论。

### 收敛状态（Status）

`State.Status[负载名]` 每轮协调后重算，可从快照判断调整是否跑完：

| 字段 | 含义 |
|---|---|
| `Desired`/`Placed`/`Pending` | 期望/已放置/缺口副本数 |
| `Converged` | 是否已按最新声明收敛（无缺口） |
| `Migrated` | 本轮被迁走的实例（`id: 旧站->新站`），与 `Rejection`（没放下的）区分 |

### 兼容性

- 旧版本落盘的 JSON 不含 `status` 字段，加载时按空 map 处理，
  `Open` 后的首轮协调即重算出 `Status`，无需迁移；
- 旧状态中的实例会在首轮协调被同步到最新声明（相当于自动执行一次更新），
  实例 ID 与站点归属在仍满足声明时保持不变。

## 协调与并发语义

`Reconcile` 每轮执行：删除不再期望的负载 → 缩容（高序号先删）→
把最新声明同步到存量实例（Size/MutexKey 原地更新）→
逐出失效实例（站点失效/标签不再匹配/互斥冲突）→ 超配站点逐出 →
确定性补放缺口 → 重算每个负载的收敛状态（`Status`）。

- **幂等**：同一 `(State, Desired)` 重复协调结论（实例集合 + 拒绝记录）不变；
  任何操作可安全重试，不放大副作用。
- **并发安全**：`Federation` 用互斥锁串行化所有写操作与周期协调，
  扩容/缩容/更新/删除与站点事件可任意交织。
- **持久化**：每轮协调后状态原子落盘（临时文件 + rename）；
  重新 `Open` 时先加载再协调一轮，重启后结论与落盘前一致。

## 验证方法

`federation/federation_test.go` 覆盖：

| 用例 | 验证点 |
|---|---|
| `TestCapacityInsufficient` | 容量不足按原因拒绝、不超配、重复协调结论稳定 |
| `TestLabelMismatch` / `TestMutexConflict` / `TestNoEligibleSite` | 三类约束失败原因可区分 |
| `TestFailoverAndRecovery` | 故障重建不超容量、恢复后无重复、缺口可解释 |
| `TestDrain` | 排空迁移 |
| `TestConcurrentReconcile` | 期望变更与周期协调并发交织，无重复/丢失，重试为无操作 |
| `TestPersistence` | 落盘重载后结论一致 |
| `TestDeterministicPlacement` | 相同输入两次协调放置结果完全相同 |

`federation/update_test.go` 覆盖声明调整（更新）路径：

| 用例 | 验证点 |
|---|---|
| `TestResizeExactFit` | 调大 Size 且容量刚好够：原地更新不迁移、身份不变、收敛后重试为无操作 |
| `TestResizeMigrateOverflow` | 调大 Size 原站放不下：满足的留站、不满足的迁走，迁移记录可核对 |
| `TestResizeRejectInsufficient` | 调大 Size 全局差一点放不下：`InsufficientCapacity` 缺口、总数守恒、扩容后补放 |
| `TestMutexKeyUpdate` | 新增互斥声明：同站冲突实例被分开，结论稳定 |
| `TestMutexKeyUpdateConflict` | 跨负载撞 key：保留 ID 小者，另一个给出 `MutexConflict` 缺口 |
| `TestUpdatePersistence` | 调整后落盘重载，摆放与收敛状态一致 |
| `TestUpdateConcurrent` | 更新与扩缩容/故障/周期协调并发交织，无重复/丢失，最终收敛 |

本地验证更新行为：

```bash
# 跑全部更新用例，日志含每轮期望状态、resize/rekey/evict/migrate/reject 判定依据
go test -v -run 'TestResize|TestMutexKeyUpdate|TestUpdate' ./federation/

# 并发与竞态验证
go test -race -run TestUpdateConcurrent ./federation/
```

测试日志通过 `t.Logf` 打印期望状态（如 `desired: 3x2`）与每个实例的
放置/逐出/迁移/拒绝判定依据，`go test -v` 可见。
