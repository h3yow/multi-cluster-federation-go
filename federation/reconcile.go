package federation

import (
	"fmt"
	"sort"
)

// instanceID 由负载名与序号确定性派生实例 ID。
// 这是幂等的关键：同一期望状态重复协调、故障重建、声明更新、恢复后都不会
// 产生重复实例；更新声明时实例身份保持不变（迁移而非删除重建）。
func instanceID(workload string, ordinal int) string {
	return fmt.Sprintf("%s-%d", workload, ordinal)
}

// pending 记录本轮被腾出/丢失的实例序号，用于补放后判定它是否属于“迁移”。
type pending struct {
	from       string // 原站点
	reason     string // 被腾出的原因（供日志与 Migration.Reason）
	specDriven bool   // 是否因最新声明（容量/互斥重评）而腾出
}

// survivor 是通过了站点级与标签级校验、待按最新声明重评的存量实例。
type survivor struct {
	wl   string
	ord  int
	inst *Instance
}

// Reconcile 对状态执行一轮协调，使其收敛到期望状态。
//
// 更新语义（运行中调整声明）：每个存量实例都按 Desired 中的最新声明重新判定。
//   - 站点仍可用、标签仍满足、按最新 Size/MutexKey 在全局视角下仍放得下的实例
//     原地保留，其 Size/MutexKey 直接更新为最新值，身份不变、不搬家；
//   - 按最新声明不再满足的实例先腾出（绝不先临时超配），再按确定性规则尝试
//     落到其他可行站点；落不下的记为可解释缺口（Rejection），实例总数不凭空变化；
//   - 同一份声明反复协调，结论稳定（幂等），不会来回挪。
//
// 全局三阶段（跨负载共用一个调度视角，容量/互斥强一致；顺序全部确定性）：
//  1. 删除不再期望的负载；缩容移除高序号实例；
//     移除站点失效/排空/不存在、标签不再匹配的实例；
//  2. 保留重评（按 负载名升序、序号升序）：存活实例用【最新声明】在“只计已确认
//     保留实例”的全局视角上逐个提交，放不下或互斥冲突的腾出；
//  3. 补放缺口（同序）：确定性选站，记录迁移与可区分的拒绝原因；
//  4. 重算每个负载的收敛状态（WorkloadStatus）。
func Reconcile(st *State) []string {
	var log []string

	// 1a. 删除不再期望的负载。
	for id, inst := range st.Instances {
		if _, ok := st.Desired[inst.Workload]; !ok {
			delete(st.Instances, id)
			delete(st.Generation, inst.Workload)
			delete(st.Statuses, inst.Workload)
			log = append(log, fmt.Sprintf("remove %s: workload %q no longer desired", id, inst.Workload))
		}
	}

	// 按负载名排序处理，保证日志与结论确定。
	wlNames := make([]string, 0, len(st.Desired))
	for n := range st.Desired {
		wlNames = append(wlNames, n)
	}
	sort.Strings(wlNames)

	// pen[wl][ordinal] 记录本轮被腾出/丢失的实例。
	pen := map[string]map[int]pending{}
	var survivors []survivor

	// 1b. 缩容 + 站点级/标签级失效清理。
	for _, wl := range wlNames {
		spec := st.Desired[wl]
		pen[wl] = map[int]pending{}

		// 缩容：序号 >= Replicas 的实例移除（身份随序号回收，确定性）。
		for ord := spec.Replicas; ; ord++ {
			id := instanceID(wl, ord)
			if _, ok := st.Instances[id]; !ok {
				break
			}
			delete(st.Instances, id)
			log = append(log, fmt.Sprintf("remove %s: scale down %q to %d replicas", id, wl, spec.Replicas))
		}

		for ord := 0; ord < spec.Replicas; ord++ {
			id := instanceID(wl, ord)
			inst, ok := st.Instances[id]
			if !ok {
				continue // 缺口，阶段三补放
			}
			site, exists := st.Sites[inst.Site]
			if !exists {
				delete(st.Instances, id)
				pen[wl][ord] = pending{from: inst.Site, reason: "site no longer exists"}
				log = append(log, fmt.Sprintf("evict %s from %s: site no longer exists", id, inst.Site))
				continue
			}
			switch site.State {
			case SiteFailed:
				delete(st.Instances, id)
				pen[wl][ord] = pending{from: site.Name, reason: "site failed"}
				log = append(log, fmt.Sprintf("evict %s from %s: site failed", id, site.Name))
				continue
			case SiteDraining:
				delete(st.Instances, id)
				pen[wl][ord] = pending{from: site.Name, reason: "site draining"}
				log = append(log, fmt.Sprintf("evict %s from %s: site draining", id, site.Name))
				continue
			}
			if !matchSelector(spec.Selector, site.Labels) {
				delete(st.Instances, id)
				pen[wl][ord] = pending{from: site.Name, reason: "site labels no longer match selector"}
				log = append(log, fmt.Sprintf("evict %s from %s: site labels no longer match selector", id, site.Name))
				continue
			}
			survivors = append(survivors, survivor{wl: wl, ord: ord, inst: inst})
		}
	}

	// 2. 保留重评（全局、跨负载共用视角，按 负载名、序号 升序）。
	//
	// committed 初始为空、只计入已确认保留的实例：已提交占用构成真实的新规格
	// 基线。这样调大 Size 后放不下的实例腾出，任何时刻都不超配；改变互斥范围后
	// 同站冲突的后序实例腾出；第二轮协调时现存实例即上一轮的提交结果，判定全部
	// 通过 → 不搬家（幂等）。
	committed := newPlacementView(nil)
	for _, sv := range survivors {
		spec := st.Desired[sv.wl]
		site := st.Sites[sv.inst.Site]
		id := sv.inst.ID
		free := committed.free(site)
		if free < spec.Size {
			oldSize := sv.inst.Size
			delete(st.Instances, id)
			pen[sv.wl][sv.ord] = pending{
				from:       site.Name,
				specDriven: true,
				reason:     fmt.Sprintf("size %d->%d: free %d < need %d on %s", oldSize, spec.Size, free, spec.Size, site.Name),
			}
			log = append(log, fmt.Sprintf("vacate %s from %s: updated size %d->%d no longer fits (free=%d)", id, site.Name, oldSize, spec.Size, free))
			continue
		}
		if blocked := committed.mutexBlocked(site.Name, spec.MutexKey, id); blocked {
			owner := committed.mutex[site.Name][spec.MutexKey]
			oldKey := sv.inst.MutexKey
			delete(st.Instances, id)
			pen[sv.wl][sv.ord] = pending{
				from:       site.Name,
				specDriven: true,
				reason:     fmt.Sprintf("mutexKey %q->%q conflicts with %s on %s", oldKey, spec.MutexKey, owner, site.Name),
			}
			log = append(log, fmt.Sprintf("vacate %s from %s: updated mutexKey %q->%q conflicts with %s", id, site.Name, oldKey, spec.MutexKey, owner))
			continue
		}
		// 仍满足最新声明：原地更新为最新规格并计入基线，身份与站点不变。
		sv.inst.Size = spec.Size
		sv.inst.MutexKey = spec.MutexKey
		committed.occupy(sv.inst)
	}

	// 3. 补放缺口（同序，跨负载共用视角）：确定性选站，容量绝不超配。
	rejections := make([]Rejection, 0)
	migrations := map[string][]Migration{}
	for _, wl := range wlNames {
		spec := st.Desired[wl]
		for ord := 0; ord < spec.Replicas; ord++ {
			id := instanceID(wl, ord)
			if _, ok := st.Instances[id]; ok {
				continue
			}
			target, rej := chooseSite(spec, ord, st.Sites, committed)
			if target == "" {
				rejections = append(rejections, rej)
				log = append(log, fmt.Sprintf("reject %s: %s (%s)", id, rej.Reason, rej.Detail))
				continue
			}
			inst := &Instance{
				ID:       id,
				Workload: wl,
				Ordinal:  ord,
				Site:     target,
				Size:     spec.Size,
				MutexKey: spec.MutexKey,
			}
			st.Instances[id] = inst
			committed.occupy(inst)
			if p, had := pen[wl][ord]; had {
				if p.specDriven && p.from != target {
					migrations[wl] = append(migrations[wl], Migration{
						InstanceID: id, Workload: wl, Ordinal: ord,
						From: p.from, To: target, Reason: p.reason,
					})
					log = append(log, fmt.Sprintf("migrate %s: %s -> %s (%s)", id, p.from, target, p.reason))
				} else {
					log = append(log, fmt.Sprintf("rebuild %s on %s (%s)", id, target, p.reason))
				}
			} else {
				log = append(log, fmt.Sprintf("place %s on %s", id, target))
			}
		}
	}

	// 4. 重算每个负载的收敛状态（每轮整体替换，反映本轮最新结论）。
	st.Statuses = map[string]*WorkloadStatus{}
	for _, wl := range wlNames {
		spec := st.Desired[wl]
		placed, rejCount := 0, 0
		for _, inst := range st.Instances {
			if inst.Workload == wl {
				placed++
			}
		}
		for _, r := range rejections {
			if r.Workload == wl {
				rejCount++
			}
		}
		converged := placed == spec.Replicas && rejCount == 0
		desiredGen := st.Generation[wl]
		observed := 0
		switch {
		case converged:
			observed = desiredGen
		case desiredGen > 0:
			observed = desiredGen - 1 // 仍有缺口：最新代次尚未完全生效
		}
		st.Statuses[wl] = &WorkloadStatus{
			Name:               wl,
			DesiredGeneration:  desiredGen,
			ObservedGeneration: observed,
			Replicas:           placed,
			Rejected:           rejCount,
			Converged:          converged,
			Migrations:         migrations[wl],
		}
	}

	// 拒绝记录整体替换：反映本轮协调后的当前缺口，而非历史累积。
	st.Rejections = rejections
	return log
}
