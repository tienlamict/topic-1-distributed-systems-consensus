package raft

import (
	"fmt"
	"sort"

	"github.com/tienlamict/topic1-consensus-demo/internal/sim"
)

// Config gom các tham số timing. Quan hệ giữa chúng chính là bất đẳng thức
// broadcastTime ≪ electionTimeout ≪ MTBF ở §4.1 tài liệu lý thuyết.
type Config struct {
	HeartbeatInterval sim.Time `json:"heartbeatInterval"`
	ElectionBase      sim.Time `json:"electionBase"`
	ElectionJitter    sim.Time `json:"electionJitter"`
	MinLatency        sim.Time `json:"minLatency"`
	MaxLatency        sim.Time `json:"maxLatency"`
}

func DefaultConfig() Config {
	// electionTimeout ∈ [150,300)ms — đúng khuyến nghị conservative của paper
	// §9.3. broadcastTime ≈ 2×latency ≈ 20-60ms, nhỏ hơn một bậc.
	return Config{
		HeartbeatInterval: 50,
		ElectionBase:      150,
		ElectionJitter:    150,
		MinLatency:        10,
		MaxLatency:        30,
	}
}

// Metrics là các con số tổng kết, hiển thị ở panel bên phải UI và dùng cho
// phần thực nghiệm định lượng.
type Metrics struct {
	ElectionsStarted int      `json:"electionsStarted"`
	LeaderChanges    int      `json:"leaderChanges"`
	FirstLeaderAt    sim.Time `json:"firstLeaderAt"` // -1 nếu chưa từng bầu được ai
	LeaderlessMs     sim.Time `json:"leaderlessMs"`
	// LeaderlessGaps là độ dài từng quãng cluster không có leader. Gap[0] là
	// lần bầu đầu tiên lúc khởi động; các gap sau là downtime khi phải bầu
	// lại — chính là đại lượng paper §9.3 đo.
	LeaderlessGaps   []sim.Time `json:"leaderlessGaps"`
	MaxTerm          int        `json:"maxTerm"`
	EntriesCommitted int        `json:"entriesCommitted"`
	WritesSubmitted  int        `json:"writesSubmitted"`
	WritesAcked      int        `json:"writesAcked"`
	WritesRejected   int        `json:"writesRejected"`
	WritesLost       int        `json:"writesLost"` // entry bị truncate mà chưa từng commit
	MessagesSent     int        `json:"messagesSent"`
	MessagesLost     int        `json:"messagesLost"`
}

type Cluster struct {
	s   *sim.Sim
	net *sim.Network
	cfg Config

	ids   []string
	nodes map[string]*Node

	metrics Metrics

	nextWID       int
	ackedWID      map[int]bool
	lostWID       map[int]bool
	noLeaderSince sim.Time
	hadLeader     bool
}

func NewCluster(s *sim.Sim, cfg Config, ids []string) *Cluster {
	sort.Strings(ids)
	c := &Cluster{
		s:        s,
		net:      sim.NewNetwork(s, cfg.MinLatency, cfg.MaxLatency),
		cfg:      cfg,
		ids:      ids,
		nodes:    map[string]*Node{},
		ackedWID: map[int]bool{},
		lostWID:  map[int]bool{},
	}
	c.metrics.FirstLeaderAt = -1

	for _, id := range ids {
		peers := []string{}
		for _, o := range ids {
			if o != id {
				peers = append(peers, o)
			}
		}
		n := &Node{
			ID: id, peers: peers, c: c,
			state: Follower,
			log:   []Entry{{Term: 0}}, // sentinel
			alive: true,
		}
		c.nodes[id] = n
		c.net.Register(id, n.recv)
	}
	// Đăng ký xong mới khởi động timer, để mọi node đều có handler sẵn sàng.
	for _, id := range ids {
		n := c.nodes[id]
		n.resetElectionTimer()
		n.emit()
	}
	return c
}

func (c *Cluster) IDs() []string     { return c.ids }
func (c *Cluster) Config() Config    { return c.cfg }
func (c *Cluster) Net() *sim.Network { return c.net }

func (c *Cluster) Metrics() Metrics {
	m := c.metrics
	m.MessagesSent = c.net.Sent
	m.MessagesLost = c.net.Lost
	for _, id := range c.ids {
		if t := c.nodes[id].currentTerm; t > m.MaxTerm {
			m.MaxTerm = t
		}
	}
	// Nếu tới cuối mô phỏng vẫn chưa có leader, cộng nốt quãng leaderless.
	if !c.leaderExists() {
		m.LeaderlessMs += c.s.Now() - c.noLeaderSince
	}
	return m
}

func (c *Cluster) leaderExists() bool {
	for _, id := range c.ids { // duyệt slice, không duyệt map
		n := c.nodes[id]
		if n.alive && n.state == Leader {
			return true
		}
	}
	return false
}

// LeaderID trả về leader hiện tại (chuỗi rỗng nếu chưa có). Dùng cho script
// kịch bản — client thật sẽ phải thử-và-redirect chứ không biết trước.
func (c *Cluster) LeaderID() string { return c.LeaderID2("") }

// LeaderID2 trả về leader hiện tại nhưng bỏ qua node `except`.
//
// Lưu ý: "leader" ở đây là node TỰ CHO RẰNG mình là leader. Khi mạng bị cắt,
// có thể có 2 node cùng ở trạng thái Leader nhưng khác term — phía minority
// chỉ là leader trên danh nghĩa vì nó không commit được gì. Điều này không
// vi phạm Election Safety (property chỉ cấm 2 leader trong CÙNG một term).
func (c *Cluster) LeaderID2(except string) string {
	best, bestTerm := "", -1
	for _, id := range c.ids {
		n := c.nodes[id]
		if id == except || !n.alive || n.state != Leader {
			continue
		}
		if n.currentTerm > bestTerm {
			best, bestTerm = id, n.currentTerm
		}
	}
	return best
}

// onLeadershipMaybeChanged theo dõi tổng thời gian cluster không có leader —
// đây chính là "downtime" mà paper §9.3 đo.
func (c *Cluster) onLeadershipMaybeChanged() {
	now := c.s.Now()
	if c.leaderExists() {
		if !c.hadLeader {
			gap := now - c.noLeaderSince
			c.metrics.LeaderlessMs += gap
			c.metrics.LeaderlessGaps = append(c.metrics.LeaderlessGaps, gap)
			c.hadLeader = true
			if c.metrics.FirstLeaderAt < 0 {
				c.metrics.FirstLeaderAt = now
			}
		}
	} else if c.hadLeader {
		c.hadLeader = false
		c.noLeaderSince = now
	}
}

// ---------------------------------------------------------------- callbacks

func (c *Cluster) onCommit(node string, idx int, e Entry, isLeader bool) {
	c.s.Emit(sim.Record{
		Kind: "commit", Node: node,
		Data: map[string]any{"index": idx, "term": e.Term, "cmd": e.Cmd},
	})
	if !isLeader {
		return
	}
	// Chỉ leader mới trả lời client (Figure 2: leader apply rồi reply).
	c.metrics.EntriesCommitted++
	if e.WID > 0 && !c.ackedWID[e.WID] {
		c.ackedWID[e.WID] = true
		c.metrics.WritesAcked++
		c.s.Emit(sim.Record{
			Kind: "ack", Node: node, Level: "good",
			Text: fmt.Sprintf("COMMITTED index %d: %s → leader %s trả OK cho client", idx, e.Cmd, node),
			Data: map[string]any{"index": idx, "cmd": e.Cmd},
		})
	}
}

func (c *Cluster) onTruncate(node string, from int, removed []Entry) {
	// Đếm theo WRITE chứ không theo node: cùng một entry bị cắt trên nhiều
	// node phía minority vẫn chỉ là một write bị mất.
	for _, e := range removed {
		if e.WID > 0 && !c.ackedWID[e.WID] && !c.lostWID[e.WID] {
			c.lostWID[e.WID] = true
			c.metrics.WritesLost++
		}
	}
	cmds := []string{}
	for _, e := range removed {
		cmds = append(cmds, e.Cmd)
	}
	c.s.Emit(sim.Record{
		Kind: "truncate", Node: node, Level: "bad",
		Text: fmt.Sprintf("%s CẮT BỎ %d entry từ index %d (%v) — log xung đột với leader",
			node, len(removed), from, cmds),
		Data: map[string]any{"from": from, "count": len(removed), "cmds": cmds},
	})
}

// ---------------------------------------------------------------- fault API

func (c *Cluster) Crash(id string) {
	n := c.nodes[id]
	if !n.alive {
		return
	}
	n.alive = false
	n.stopElectionTimer()
	n.hbGen++ // dừng heartbeat loop
	n.state = Follower
	n.votes = nil
	c.net.SetDown(id, true)
	c.s.Emit(sim.Record{Kind: "fault", Node: id, Level: "bad",
		Text: fmt.Sprintf("✕ %s CRASH", id)})
	c.onLeadershipMaybeChanged()
	n.emit()
}

// Recover bật lại node. currentTerm/votedFor/log được giữ nguyên — mô phỏng
// việc khôi phục từ stable storage như §1.2 mô tả.
func (c *Cluster) Recover(id string) {
	n := c.nodes[id]
	if n.alive {
		return
	}
	n.alive = true
	n.state = Follower
	c.net.SetDown(id, false)
	n.resetElectionTimer()
	c.s.Emit(sim.Record{Kind: "fault", Node: id, Level: "info",
		Text: fmt.Sprintf("↺ %s hồi phục (khôi phục term=%d, log=%d entry từ stable storage)",
			id, n.currentTerm, n.lastIndex())})
	n.emit()
}

func (c *Cluster) Partition(groups [][]string) {
	c.net.Partition(groups)
	desc := ""
	for i, g := range groups {
		if i > 0 {
			desc += " | "
		}
		desc += fmt.Sprintf("%v", g)
	}
	c.s.Emit(sim.Record{Kind: "fault", Level: "bad",
		Text: "✂ NETWORK PARTITION: " + desc,
		Data: map[string]any{"groups": c.net.Groups()},
	})
}

func (c *Cluster) Heal() {
	c.net.Heal()
	c.s.Emit(sim.Record{Kind: "fault", Level: "good",
		Text: "✓ Mạng đã liền lại",
		Data: map[string]any{"groups": c.net.Groups()},
	})
}

// ForceElection ép một node ứng cử ngay lập tức. Không có trong Raft thật —
// đây là công cụ dạy học để dựng đúng tình huống muốn minh hoạ (ví dụ S4:
// bắt một node có log cũ đi xin phiếu để thấy nó bị từ chối).
func (c *Cluster) ForceElection(id string) {
	n := c.nodes[id]
	if !n.alive {
		return
	}
	n.becomeCandidate(0)
}

// ---------------------------------------------------------------- client API

// ClientWrite gửi write tới leader hiện tại.
func (c *Cluster) ClientWrite(cmd string) {
	id := c.LeaderID()
	if id == "" {
		c.metrics.WritesSubmitted++
		c.metrics.WritesRejected++
		c.s.Emit(sim.Record{Kind: "log", Level: "warn",
			Text: fmt.Sprintf("client write %q bị từ chối — cluster chưa có leader", cmd)})
		return
	}
	c.ClientWriteTo(id, cmd)
}

// ClientWriteTo gửi write tới đúng một node. Nếu node đó không phải leader,
// write bị từ chối (Raft: follower redirect client sang leader).
func (c *Cluster) ClientWriteTo(id, cmd string) {
	c.metrics.WritesSubmitted++
	n := c.nodes[id]
	if !n.alive || n.state != Leader {
		c.metrics.WritesRejected++
		c.s.Emit(sim.Record{Kind: "log", Level: "warn",
			Text: fmt.Sprintf("client write %q tới %s bị từ chối — node này không phải leader", cmd, id)})
		return
	}
	c.nextWID++
	e := Entry{Term: n.currentTerm, Cmd: cmd, WID: c.nextWID}
	n.log = append(n.log, e)
	c.s.Emit(sim.Record{Kind: "log", Level: "info",
		Text: fmt.Sprintf("client → %s: %s (append vào log tại index %d, CHƯA commit)",
			id, cmd, n.lastIndex())})
	n.emit()
	// Đẩy entry đi ngay, không đợi heartbeat kế tiếp.
	for _, p := range n.peers {
		n.sendAppendTo(p)
	}
}

// ---------------------------------------------------------------- invariants

// CheckInvariants kiểm tra các safety property ở §5.2 tài liệu lý thuyết.
// Dùng trong test chạy hàng nghìn seed để soi bug cài đặt.
func (c *Cluster) CheckInvariants() error {
	// Election Safety: tối đa 1 leader trong một term.
	leaderOfTerm := map[int]string{}
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.alive && n.state == Leader {
			if other, ok := leaderOfTerm[n.currentTerm]; ok {
				return fmt.Errorf("Election Safety vi phạm: %s và %s cùng là leader term %d",
					other, id, n.currentTerm)
			}
			leaderOfTerm[n.currentTerm] = id
		}
	}
	// Log Matching: hai log có cùng (index, term) thì mọi entry trước đó giống nhau.
	for i := 0; i < len(c.ids); i++ {
		for j := i + 1; j < len(c.ids); j++ {
			a, b := c.nodes[c.ids[i]], c.nodes[c.ids[j]]
			n := min(a.lastIndex(), b.lastIndex())
			for k := n; k >= 1; k-- {
				if a.log[k].Term == b.log[k].Term {
					for m := 1; m <= k; m++ {
						if a.log[m] != b.log[m] {
							return fmt.Errorf("Log Matching vi phạm giữa %s và %s tại index %d",
								a.ID, b.ID, m)
						}
					}
					break
				}
			}
		}
	}
	// State Machine Safety: không node nào apply entry KHÁC tại cùng index.
	for i := 0; i < len(c.ids); i++ {
		for j := i + 1; j < len(c.ids); j++ {
			a, b := c.nodes[c.ids[i]], c.nodes[c.ids[j]]
			n := min(a.commitIndex, b.commitIndex)
			for k := 1; k <= n; k++ {
				if a.log[k] != b.log[k] {
					return fmt.Errorf("State Machine Safety vi phạm: %s và %s khác nhau tại index %d đã commit",
						a.ID, b.ID, k)
				}
			}
		}
	}
	return nil
}
