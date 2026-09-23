# multi-cluster-federation-go

本地可运行的多集群工作负载联邦编排组件。用本地 JSON 存储与内存状态模拟
中心集群（协调器）与多个边缘站点，使期望状态在容量、站点可用性与并发协调
变化时都能收敛到可预期的结果。

## 结构

```
pkg/federation/
  types.go       核心类型：Site / Workload / Instance / Rejection / Report
  scheduler.go   放置规划器（纯函数，确定性）
  reconciler.go  协调器：快照 -> 规划 -> 整体替换 -> 原子落盘 -> 报告
  store.go       JSON 持久化存储（临时文件 + rename 原子写入）
cmd/fedemo/      本地演示：放置、故障迁移、恢复
```

## 放置规则

协调器每次对全量状态做确定性规划，规则如下：

1. **保留优先**：已存在且仍合法的实例（站点 Active、标签满足、容量与互斥
   不冲突、未超副本数）原样保留——同一期望状态重复协调结论稳定。
2. **移除**：负载已删除、超出期望副本数、所在站点失效或约束不再满足的
   实例被移除。
3. **补齐**：缺口按站点名升序 first-fit 放置，实例 ID 为
   `<workload>-<最小可用序号>`，同样输入必然得到同样输出。
4. **约束**：
   - 容量：站点已用容量单元 + 实例 Size 不得超过 Capacity（Size<=0 视为 1）；
   - 标签：站点 Labels 必须包含 Workload.RequiredLabels 的全部键值；
   - 互斥：同一 MutexGroup 内**不同负载**的实例不得共存于同一站点
     （同一负载自身的多个副本可以共存）。
5. **可区分拒绝**：无法放置的实例按主导原因给出 Rejection，绝不静默丢弃
   或超量放置：
   - `NoEligibleSite`：没有 Active 状态的站点；
   - `LabelMismatch`：有 Active 站点但无一满足标签约束；
   - `MutexConflict`：候选站点均被互斥组内其他负载占用；
   - `InsufficientCapacity`：候选站点剩余容量不足。

## 故障与恢复语义

- **故障 / 排空**（`SetSiteState(name, SiteFailed/SiteDraining)`）：该站点
  不再 eligible，其承载实例在下次协调时于仍满足约束的站点重建，且不超出
  容量。容量不足时实例总数出现缺口，缺口以 `Rejection` 形式给出可解释原因。
- **恢复**（`SetSiteState(name, SiteActive)`）：既有合法放置不会被无谓迁移，
  恢复站点只用于补齐未满足的缺口；任一时刻实例总数等于期望值，或期望值
  减去被拒绝数（缺口可解释），绝不出现重复实例。
- **期望状态变更**：扩容、缩容、更新（`UpsertWorkload`）与删除
  （`DeleteWorkload`）可任意交织，变更在下次 `Reconcile` 生效。
- **并发与重试安全**：`Reconcile` 由互斥锁串行化，且基于"快照 -> 纯函数
  规划 -> 整体替换实例集"的模式，可安全重试，不会放大副作用；协调结果
  原子落盘，重新加载后再次协调得到一致结论（`Report.Stable == true`）。

## 验证方法

```sh
go build ./...          # 编译
go test ./... -v        # 单元测试（日志打印期望状态与关键判定依据）
go test ./... -race     # 并发协调竞态检测
go run ./cmd/fedemo     # 本地端到端演示
```

测试覆盖：

| 测试 | 覆盖点 |
| --- | --- |
| `TestPlanInsufficientCapacity` | 容量不足，按原因拒绝 |
| `TestPlanLabelMismatch` | 标签约束冲突 |
| `TestPlanMutexConflict` / `TestPlanMutexSameWorkloadAllowed` | 互斥关系及其边界 |
| `TestPlanDeterministic` / `TestPlanKeepsValidExisting` | 重复协调结论稳定 |
| `TestSiteFailureAndRecovery` | 故障迁移与恢复去重，总数恒为期望值 |
| `TestSiteFailureInsufficientCapacity` | 故障后容量不足的可解释缺口 |
| `TestSiteDraining` | 排空迁移 |
| `TestScaleAndDeleteInterleaved` | 扩缩容与删除交织，无重复无丢失 |
| `TestConcurrentReconcile` | 并发协调收敛且无竞态 |
| `TestPersistenceReloadConsistency` | 落盘重载后结论一致 |
