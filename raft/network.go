package raft

import "sync"

// ============================================================================
// network.go — Lớp mạng in-memory mô phỏng việc gửi RPC giữa các node.
//
// Mục đích: thay cho TCP/gRPC thật, ta dùng một "mạng giả lập" để có thể:
//   - Cắt liên lạc giữa các node (network partition) — minh họa §4 (failure
//     detection qua missing heartbeat) và §5.4 (split-brain).
//   - Quan sát toàn bộ luồng RPC tại một nơi.
//
// Khi hai node bị partition, RPC giữa chúng "rớt" (trả về ok=false), giống
// như packet loss / timeout trong thực tế. Raft phải vẫn SAFE trong điều kiện
// này (§1.2: "không phụ thuộc timing cho correctness").
// ============================================================================

type Network struct {
	mu    sync.RWMutex
	nodes map[int]*Node
	// reachable[a][b] = true nghĩa là node a có thể gửi gói tới node b.
	// Mặc định mọi cặp đều reachable. Partition = set các cặp về false.
	reachable map[int]map[int]bool
}

func NewNetwork() *Network {
	return &Network{
		nodes:     make(map[int]*Node),
		reachable: make(map[int]map[int]bool),
	}
}

func (net *Network) addNode(n *Node) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[n.id] = n
	if net.reachable[n.id] == nil {
		net.reachable[n.id] = make(map[int]bool)
	}
	// Mặc định node mới reachable tới mọi node hiện có và ngược lại.
	for id := range net.nodes {
		net.reachable[n.id][id] = true
		if net.reachable[id] == nil {
			net.reachable[id] = make(map[int]bool)
		}
		net.reachable[id][n.id] = true
	}
}

// canReach: a có gửi được gói tới b không? (đồng thời b phải còn sống).
func (net *Network) canReach(a, b int) bool {
	net.mu.RLock()
	defer net.mu.RUnlock()
	if r, ok := net.reachable[a]; ok {
		if v, ok := r[b]; ok && !v {
			return false
		}
	}
	target := net.nodes[b]
	if target == nil || target.isDead() {
		return false
	}
	return true
}

// Partition: cắt liên lạc HAI CHIỀU giữa hai nhóm node.
// Ví dụ Partition([]int{0}, []int{1,2}) → node 0 bị cô lập khỏi 1 và 2.
func (net *Network) Partition(groupA, groupB []int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	for _, a := range groupA {
		for _, b := range groupB {
			net.reachable[a][b] = false
			net.reachable[b][a] = false
		}
	}
}

// Heal: khôi phục toàn bộ liên lạc (gỡ mọi partition).
func (net *Network) Heal() {
	net.mu.Lock()
	defer net.mu.Unlock()
	for a := range net.reachable {
		for b := range net.reachable[a] {
			net.reachable[a][b] = true
		}
	}
}

// sendRequestVote: chuyển RequestVote RPC từ `from` tới `to`.
// Trả về (reply, ok). ok=false nghĩa là gói bị rớt (partition / node chết).
func (net *Network) sendRequestVote(from, to int, args RequestVoteArgs) (RequestVoteReply, bool) {
	if !net.canReach(from, to) {
		return RequestVoteReply{}, false
	}
	net.mu.RLock()
	target := net.nodes[to]
	net.mu.RUnlock()
	return target.handleRequestVote(args), true
}

// sendAppendEntries: chuyển AppendEntries RPC từ `from` tới `to`.
func (net *Network) sendAppendEntries(from, to int, args AppendEntriesArgs) (AppendEntriesReply, bool) {
	if !net.canReach(from, to) {
		return AppendEntriesReply{}, false
	}
	net.mu.RLock()
	target := net.nodes[to]
	net.mu.RUnlock()
	return target.handleAppendEntries(args), true
}
