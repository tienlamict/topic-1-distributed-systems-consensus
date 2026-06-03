package main

import (
	"fmt"
	"time"

	"raftdemo/raft"
)

// ============================================================================
// Demo: cluster Raft 3 node, minh họa trực quan các cơ chế trong tài liệu
// topic-1-distributed-systems-consensus.md.
//
// Kịch bản (mỗi màn ứng với một phần lý thuyết):
//   Màn 1 — Leader election lần đầu                (§2.2, §4.1)
//   Màn 2 — Log replication + commit qua majority  (§2.2, §3.1.2)
//   Màn 3 — Kill leader → re-election               (§4.1)
//   Màn 4 — Network partition → minority KHÔNG commit được (§1.2 safety, §5.5)
//   Màn 5 — Heal partition → cluster hội tụ lại     (§5.2 Leader Completeness)
// ============================================================================

func banner(title string) {
	fmt.Printf("\n================================================================\n")
	fmt.Printf("  %s\n", title)
	fmt.Printf("================================================================\n")
}

func findLeader(nodes []*raft.Node) *raft.Node {
	for _, n := range nodes {
		if n.IsLeader() {
			return n
		}
	}
	return nil
}

func waitForLeader(nodes []*raft.Node, timeout time.Duration) *raft.Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := findLeader(nodes); l != nil {
			return l
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func printCluster(nodes []*raft.Node) {
	fmt.Println("  --- trạng thái cluster ---")
	for _, n := range nodes {
		st, term, commit, logLen := n.Snapshot()
		fmt.Printf("  node %d: %-9s term=%d commitIndex=%d logLen=%d\n",
			n.ID(), st, term, commit, logLen)
	}
}

func main() {
	net := raft.NewNetwork()
	ids := []int{0, 1, 2}
	nodes := make([]*raft.Node, len(ids))
	for _, id := range ids {
		peers := []int{}
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		nodes[id] = raft.NewNode(id, peers, net)
	}
	for _, n := range nodes {
		n.Start()
	}

	// ----- Màn 1: Leader election -----
	banner("MÀN 1 — Leader election (§2.2, §4.1)")
	fmt.Println("3 node khởi động ở FOLLOWER. Node nào election-timeout trước sẽ")
	fmt.Println("thành CANDIDATE và xin vote. Randomized timeout giúp tránh split vote.")
	leader := waitForLeader(nodes, 5*time.Second)
	if leader == nil {
		fmt.Println("!! Không elect được leader (bất thường)")
		return
	}
	time.Sleep(500 * time.Millisecond)
	printCluster(nodes)

	// ----- Màn 2: Log replication -----
	banner("MÀN 2 — Log replication + commit qua majority (§2.2, §3.1.2)")
	fmt.Println("Client gửi command tới leader. Leader append vào log, replicate qua")
	fmt.Println("AppendEntries, và chỉ COMMIT khi majority (2/3) đã lưu entry.")
	leader.Submit("set x 1")
	leader.Submit("set y 2")
	time.Sleep(1500 * time.Millisecond)
	printCluster(nodes)

	// ----- Màn 3: Kill leader → re-election -----
	banner("MÀN 3 — Kill leader → re-election (§4.1)")
	fmt.Printf("Giết leader hiện tại (node %d). Follower sẽ hết election timeout và\n", leader.ID())
	fmt.Println("bầu leader mới ở term cao hơn.")
	oldLeaderID := leader.ID()
	leader.Kill()
	newLeader := waitForLeader(nodes, 5*time.Second)
	if newLeader == nil {
		fmt.Println("!! Không elect được leader mới")
		return
	}
	fmt.Printf(">> Leader mới: node %d (leader cũ %d đã chết)\n", newLeader.ID(), oldLeaderID)
	newLeader.Submit("set z 3")
	time.Sleep(1500 * time.Millisecond)
	printCluster(nodes)

	// ----- Màn 4: Network partition -----
	banner("MÀN 4 — Network partition: minority KHÔNG commit được (§1.2, §5.5)")
	fmt.Printf("Cô lập leader %d khỏi 2 node còn lại. Leader bị kẹt ở minority (1/3)\n", newLeader.ID())
	fmt.Println("→ KHÔNG đạt majority → command KHÔNG được commit (an toàn, không mất an toàn).")
	fmt.Println("Phía majority (2 node) sẽ bầu leader mới và vẫn hoạt động.")
	fmt.Printf(">> Trước hết hồi sinh node %d (leader cũ đã chết) để phía majority đủ 2 node sống\n", oldLeaderID)
	nodes[oldLeaderID].Revive()
	time.Sleep(1500 * time.Millisecond) // chờ node hồi sinh rejoin + catch-up log

	minority := newLeader.ID()
	majorityIDs := []int{}
	for _, id := range ids {
		if id != minority {
			majorityIDs = append(majorityIDs, id)
		}
	}
	net.Partition([]int{minority}, majorityIDs)

	// Leader cũ (bị cô lập) nhận command nhưng không thể commit
	fmt.Printf(">> Gửi \"set a 99\" tới leader bị cô lập (node %d) — sẽ KHÔNG commit\n", minority)
	newLeader.Submit("set a 99")

	// Chờ majority bầu leader mới
	time.Sleep(3 * time.Second)
	var majorityLeader *raft.Node
	for _, id := range majorityIDs {
		if nodes[id].IsLeader() {
			majorityLeader = nodes[id]
		}
	}
	if majorityLeader != nil {
		fmt.Printf(">> Majority đã bầu leader mới: node %d. Gửi \"set b 7\" — SẼ commit\n", majorityLeader.ID())
		majorityLeader.Submit("set b 7")
		time.Sleep(1500 * time.Millisecond)
	}
	printCluster(nodes)

	// ----- Màn 5: Heal partition -----
	banner("MÀN 5 — Heal partition → cluster hội tụ (§5.2 Leader Completeness)")
	fmt.Println("Khôi phục mạng. Leader cũ ở minority phát hiện term cao hơn → lùi về")
	fmt.Println("FOLLOWER. Entry \"set a 99\" chưa commit bị ghi đè. Mọi log hội tụ.")
	net.Heal()
	time.Sleep(3 * time.Second)
	printCluster(nodes)

	banner("KẾT THÚC")
	fmt.Println("Quan sát: ở mọi thời điểm tối đa 1 leader/term (Election Safety),")
	fmt.Println("entry đã committed không bao giờ mất (Leader Completeness), và")
	fmt.Println("minority không thể commit (đánh đổi availability để giữ safety).")
}
