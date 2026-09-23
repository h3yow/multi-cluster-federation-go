// fedemo 本地演示多集群联邦编排：放置、故障迁移、恢复与周期协调。
package main

import (
	"fmt"
	"log"

	fed "github.com/h3yow/multi-cluster-federation-go/pkg/federation"
)

func printReport(tag string, rep *fed.Report) {
	fmt.Printf("== %s == placed=%d removed=%d kept=%d stable=%v\n",
		tag, len(rep.Placed), len(rep.Removed), len(rep.Kept), rep.Stable)
	for _, r := range rep.Rejected {
		fmt.Printf("   拒绝: %s reason=%s (%s)\n", r.WorkloadName, r.Reason, r.Detail)
	}
}

func main() {
	r := fed.NewReconciler(fed.NewStore("/tmp/fedemo-store.json"))
	r.UpsertSite(fed.Site{Name: "edge-a", Capacity: 2, State: fed.SiteActive, Labels: map[string]string{"region": "east"}})
	r.UpsertSite(fed.Site{Name: "edge-b", Capacity: 2, State: fed.SiteActive, Labels: map[string]string{"region": "west"}})
	r.UpsertSite(fed.Site{Name: "edge-c", Capacity: 2, State: fed.SiteActive, Labels: map[string]string{"region": "east"}})

	r.UpsertWorkload(fed.Workload{Name: "web", Replicas: 3})
	r.UpsertWorkload(fed.Workload{Name: "cache", Replicas: 2, RequiredLabels: map[string]string{"region": "east"}})

	rep, err := r.Reconcile()
	if err != nil {
		log.Fatal(err)
	}
	printReport("初始放置", rep)
	fmt.Println("   实例:", r.Instances())

	r.SetSiteState("edge-a", fed.SiteFailed)
	rep, _ = r.Reconcile()
	printReport("edge-a 故障", rep)
	fmt.Println("   实例:", r.Instances())

	r.SetSiteState("edge-a", fed.SiteActive)
	rep, _ = r.Reconcile()
	printReport("edge-a 恢复", rep)
	fmt.Println("   实例:", r.Instances())
}
