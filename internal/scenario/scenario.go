// Package scenario định nghĩa các kịch bản demo. Mỗi kịch bản là một script
// tác động vào cluster theo mốc thời gian ảo, kèm các Note giải thích để UI
// hiển thị — đây là lớp nối demo với tài liệu lý thuyết.
package scenario

import (
	"fmt"

	"github.com/tienlamict/topic1-consensus-demo/internal/raft"
	"github.com/tienlamict/topic1-consensus-demo/internal/sim"
)

// Param mô tả một tham số chỉnh được từ UI.
type Param struct {
	Key     string `json:"key"`
	Label   string `json:"label"`
	Default int    `json:"default"`
	Min     int    `json:"min"`
	Max     int    `json:"max"`
	Step    int    `json:"step"`
	Help    string `json:"help"`
}

type Scenario struct {
	ID       string  `json:"id"`
	Title    string  `json:"title"`
	Ref      string  `json:"ref"`   // mục tham chiếu trong tài liệu lý thuyết
	Brief    string  `json:"brief"` // hiển thị dưới tiêu đề
	Nodes    int     `json:"nodes"`
	Duration int     `json:"duration"` // ms ảo
	Params   []Param `json:"params,omitempty"`

	run func(c *raft.Cluster, s *sim.Sim, p map[string]int)
}

// Result là thứ trả về cho browser.
type Result struct {
	Scenario string         `json:"scenario"`
	Seed     int64          `json:"seed"`
	Nodes    []string       `json:"nodes"`
	Duration sim.Time       `json:"duration"`
	Config   raft.Config    `json:"config"`
	Metrics  raft.Metrics   `json:"metrics"`
	Trace    []sim.Record   `json:"trace"`
	Params   map[string]int `json:"params"`
}

func nodeIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i+1)
	}
	return ids
}

// Run chạy một kịch bản tới cùng và trả về toàn bộ trace.
func Run(id string, seed int64, params map[string]int) (*Result, error) {
	sc := Get(id)
	if sc == nil {
		return nil, fmt.Errorf("không có kịch bản %q", id)
	}
	if params == nil {
		params = map[string]int{}
	}
	for _, p := range sc.Params {
		if _, ok := params[p.Key]; !ok {
			params[p.Key] = p.Default
		}
	}

	s := sim.New(seed)
	cfg := raft.DefaultConfig()
	if v, ok := params["jitter"]; ok {
		cfg.ElectionJitter = sim.Time(v)
	}
	if v, ok := params["electionBase"]; ok && v > 0 {
		cfg.ElectionBase = sim.Time(v)
	}
	if v, ok := params["maxLatency"]; ok && v > 0 {
		cfg.MaxLatency = sim.Time(v)
		if cfg.MinLatency > cfg.MaxLatency {
			cfg.MinLatency = cfg.MaxLatency
		}
	}

	ids := nodeIDs(sc.Nodes)
	c := raft.NewCluster(s, cfg, ids)
	sc.run(c, s, params)

	dur := sim.Time(sc.Duration)
	s.RunUntil(dur)

	return &Result{
		Scenario: sc.ID, Seed: seed, Nodes: ids, Duration: dur,
		Config: c.Config(), Metrics: c.Metrics(), Trace: s.Trace(), Params: params,
	}, nil
}

func Get(id string) *Scenario {
	for i := range All {
		if All[i].ID == id {
			return &All[i]
		}
	}
	return nil
}

// waitLeader chạy fn ngay khi cluster có leader (poll mỗi 10ms ảo, rẻ vì thời
// gian là ảo). Cần thiết vì thời điểm bầu xong leader phụ thuộc seed.
func waitLeader(c *raft.Cluster, s *sim.Sim, after sim.Time, fn func(leader string)) {
	waitLeaderExcept(c, s, after, "", fn)
}

// waitLeaderExcept chờ một leader KHÁC node `except`.
//
// Cần thiết khi có partition: leader cũ bị cô lập vẫn tự cho mình là leader
// (nó chưa biết mình đã mất quyền), nên nếu chỉ hỏi "có leader chưa?" thì sẽ
// nhận về chính nó. Bản thân điều này cũng là một bài học: trong Raft, một
// node không thể tự biết mình còn là leader hay không nếu không nói chuyện
// được với majority.
func waitLeaderExcept(c *raft.Cluster, s *sim.Sim, after sim.Time, except string, fn func(leader string)) {
	deadline := s.Now() + after + 20000
	var poll func()
	poll = func() {
		if l := c.LeaderID2(except); l != "" {
			fn(l)
			return
		}
		if s.Now() < deadline {
			s.After(10, poll)
		}
	}
	s.After(after, poll)
}

var All = []Scenario{
	// ------------------------------------------------------------------ S1
	{
		ID: "s1", Title: "S1 — Leader election cơ bản",
		Ref:   "§2.2, §4.1",
		Brief: "5 node khởi động cùng lúc nhưng election timeout khác nhau. Xem ai timeout trước, xin phiếu, thắng. Sau đó giết leader để thấy bầu lại.",
		Nodes: 5, Duration: 4000,
		run: func(c *raft.Cluster, s *sim.Sim, p map[string]int) {
			s.Note("info", "Cả 5 node khởi động ở trạng thái Follower. Vòng cung quanh mỗi node là election timeout đang đếm ngược — mỗi node một giá trị ngẫu nhiên khác nhau trong [150,300)ms. Đây chính là randomized timeout ở §4.1.")

			waitLeader(c, s, 0, func(l string) {
				s.Note("good", fmt.Sprintf("%s thắng election: nhận đủ majority (3/5) phiếu trong cùng một term. Từ giờ nó gửi heartbeat (AppendEntries rỗng) mỗi 50ms để giữ quyền lực — đúng §2.2.", l))
			})

			s.At(2000, func() {
				l := c.LeaderID()
				if l == "" {
					return
				}
				s.Note("bad", fmt.Sprintf("Giết leader %s. Các follower sẽ không nhận heartbeat nữa; node nào hết election timeout trước sẽ ứng cử và mở term mới.", l))
				c.Crash(l)
			})

			s.At(2020, func() {
				waitLeader(c, s, 0, func(l string) {
					s.Note("good", fmt.Sprintf("Leader mới: %s. Chú ý term đã tăng lên — mỗi lần bầu là một term mới, và Raft đảm bảo tối đa 1 leader trong một term (Election Safety).", l))
				})
			})
		},
	},

	// ------------------------------------------------------------------ S2
	{
		ID: "s2", Title: "S2 — Split vote & sức mạnh của randomization",
		Ref:   "§2.1, §4.1",
		Brief: "Kéo jitter về 0 để tắt randomized timeout: mọi node timeout cùng lúc, ai cũng tự vote cho mình, không ai đủ majority — term leo thang vô tận. Tăng jitter lên là xong ngay.",
		Nodes: 5, Duration: 12000,
		Params: []Param{
			{
				Key: "jitter", Label: "Độ ngẫu nhiên của election timeout (ms)",
				Default: 0, Min: 0, Max: 300, Step: 5,
				Help: "0 = tắt hoàn toàn randomization.",
			},
			{
				Key: "mode", Label: "Cách gây ra election",
				Default: 0, Min: 0, Max: 1, Step: 1,
				Help: "0 = khởi động lạnh (5 node bật cùng lúc, đồng bộ hoàn hảo — trường hợp xấu nhất). 1 = giết leader của cluster đang chạy (đúng thiết lập paper §9.3, timer các follower đã lệch sẵn do độ trễ heartbeat).",
			},
			{
				Key: "maxLatency", Label: "Độ trễ mạng tối đa một chiều (ms)",
				Default: 30, Min: 2, Max: 120, Step: 2,
				Help: "Quyết định broadcastTime. Bất đẳng thức §4.1 đòi hỏi broadcastTime ≪ electionTimeout; kéo giá trị này lên gần 150ms sẽ thấy cluster rơi vào bầu cử liên miên dù jitter có lớn.",
			},
		},
		run: func(c *raft.Cluster, s *sim.Sim, p map[string]int) {
			if p["jitter"] == 0 {
				s.Note("bad", "Jitter = 0: mọi node dùng CÙNG một election timeout 150ms. Chúng sẽ timeout cùng một thời điểm, cùng tăng term, cùng tự vote cho mình → không ai gom nổi majority. Term sẽ leo thang mãi mà cluster không có leader.")
			} else {
				s.Note("info", fmt.Sprintf("Jitter = %dms: election timeout ∈ [150, %d)ms. Chỉ cần chút ngẫu nhiên này là đủ để một node timeout trước và thắng gọn.", p["jitter"], 150+p["jitter"]))
			}

			if p["mode"] == 0 {
				s.Note("info", "Chế độ KHỞI ĐỘNG LẠNH: cả 5 node bật cùng một thời điểm nên timer của chúng đồng bộ hoàn hảo. Đây là trường hợp xấu nhất có thể — khắc nghiệt hơn thí nghiệm trong paper.")
				waitLeader(c, s, 0, func(l string) {
					s.Note("good", fmt.Sprintf("%s thành leader tại t=%dms.", l, s.Now()))
				})
				return
			}

			// Chế độ giống paper §9.3: để cluster ổn định rồi giết leader.
			// Lúc này timer các follower đã lệch nhau tự nhiên vì heartbeat
			// tới nơi ở các thời điểm khác nhau — đó là nguồn ngẫu nhiên
			// "miễn phí" mà khởi động lạnh không có.
			waitLeader(c, s, 0, func(l string) {
				s.At(3000, func() {
					cur := c.LeaderID()
					if cur == "" {
						return
					}
					s.Note("bad", fmt.Sprintf("Chế độ GIỐNG PAPER: cluster đã chạy ổn định, giờ giết leader %s. Đo xem bao lâu thì bầu được leader mới — đây chính là 'downtime' mà paper §9.3 báo cáo.", cur))
					c.Crash(cur)
					waitLeaderExcept(c, s, 0, cur, func(nl string) {
						s.Note("good", fmt.Sprintf("Leader mới %s sau %dms.", nl, s.Now()-3000))
					})
				})
			})
		},
	},

	// ------------------------------------------------------------------ S3
	{
		ID: "s3", Title: "S3 — Log replication & quorum commit",
		Ref:   "§2.2, §3.1.2",
		Brief: "Client ghi 4 giá trị. Xem entry xuất hiện ở leader (xám = chưa commit), bay đi theo AppendEntries, và chỉ chuyển xanh khi majority đã lưu.",
		Nodes: 5, Duration: 3000,
		run: func(c *raft.Cluster, s *sim.Sim, p map[string]int) {
			s.Note("info", "Chờ bầu leader xong rồi mới có thể ghi — trong Raft mọi client request đều phải qua leader (§2.1).")
			waitLeader(c, s, 0, func(l string) {
				s.Note("info", fmt.Sprintf("Leader là %s. Bắt đầu ghi. Entry màu xám = đã append vào log nhưng CHƯA commit; client vẫn đang chờ.", l))
				cmds := []string{"set x=3", "set y=7", "del z", "set x=9"}
				for i, cmd := range cmds {
					cmd := cmd
					s.After(sim.Time(300*i)+100, func() { c.ClientWrite(cmd) })
				}
				s.After(260, func() {
					s.Note("good", "Entry chuyển xanh khi leader thấy majority (3/5) đã lưu nó — đó là định nghĩa 'committed' ở §2.2. Chỉ tới lúc đó leader mới apply vào state machine và trả OK cho client.")
				})
			})
		},
	},

	// ------------------------------------------------------------------ S4
	{
		ID: "s4", Title: "S4 — Election restriction (log cũ không được làm leader)",
		Ref:   "§2.2, §5.2",
		Brief: "Cô lập n5 rồi ghi vài entry. Khi n5 quay lại và đi xin phiếu, mọi node đều TỪ CHỐI vì log của nó cũ hơn — safety property chặn đứng việc mất dữ liệu đã commit.",
		Nodes: 5, Duration: 5000,
		run: func(c *raft.Cluster, s *sim.Sim, p map[string]int) {
			waitLeader(c, s, 0, func(l string) {
				others := []string{}
				for _, id := range c.IDs() {
					if id != "n5" {
						others = append(others, id)
					}
				}
				s.Note("info", "Cô lập n5 khỏi 4 node còn lại. Phía majority vẫn hoạt động bình thường.")
				c.Partition([][]string{others, {"n5"}})

				for i, cmd := range []string{"set a=1", "set b=2", "set c=3"} {
					cmd := cmd
					s.After(sim.Time(250*i)+150, func() { c.ClientWriteTo(c.LeaderID(), cmd) })
				}

				s.After(1200, func() {
					s.Note("info", "Nối lại mạng. Lúc này log của n5 đã tụt hậu 3 entry so với 4 node kia.")
					c.Heal()
				})

				s.After(1500, func() {
					s.Note("warn", "Giờ ép n5 đi ứng cử. Nó sẽ tăng term lên cao hơn mọi người và gửi RequestVote kèm (lastLogTerm, lastLogIndex) của mình.")
					c.ForceElection("n5")
				})

				s.After(1800, func() {
					s.Note("good", "Mọi node TỪ CHỐI vote cho n5: term nó cao hơn thật, nhưng log không 'at least as up-to-date'. Đây là Election Restriction (§5.4.1 của paper) — nếu không có luật này, n5 lên làm leader sẽ ghi đè mất các entry đã commit, phá vỡ State Machine Safety.")
				})

				s.After(2600, func() {
					s.Note("info", "Ép một node có log đầy đủ ứng cử — lần này thắng ngay, vì nó vừa có term cao vừa có log đủ mới.")
					c.ForceElection("n1")
				})
			})
		},
	},

	// ------------------------------------------------------------------ S5
	{
		ID: "s5", Title: "S5 — Network partition: leader phía minority không commit được",
		Ref:   "§2.2, §5.2",
		Brief: "Cắt cluster 2/3. Leader cũ nằm phía 2 node vẫn nhận write nhưng entry mãi mãi xám. Phía 3 node bầu leader mới và commit bình thường. Khi mạng liền lại, log thừa của leader cũ bị cắt bỏ.",
		Nodes: 5, Duration: 8000,
		run: func(c *raft.Cluster, s *sim.Sim, p map[string]int) {
			waitLeader(c, s, 0, func(oldLeader string) {
				s.After(100, func() { c.ClientWrite("set k=1") })
				s.After(400, func() { c.ClientWrite("set k=2") })

				s.After(700, func() {
					// Nhóm minority: leader cũ + 1 node. Nhóm majority: 3 node còn lại.
					minority := []string{oldLeader}
					majority := []string{}
					for _, id := range c.IDs() {
						if id == oldLeader {
							continue
						}
						if len(minority) < 2 {
							minority = append(minority, id)
						} else {
							majority = append(majority, id)
						}
					}
					s.Note("bad", fmt.Sprintf("PARTITION: %v (có leader cũ %s) tách khỏi %v. Bên trái chỉ có 2/5 node — không đủ majority.", minority, oldLeader, majority))
					c.Partition([][]string{minority, majority})

					// Ghi vào leader cũ ở phía minority — sẽ không bao giờ commit.
					for i, cmd := range []string{"set ghost=1", "set ghost=2"} {
						cmd := cmd
						s.After(sim.Time(200*i)+200, func() {
							c.ClientWriteTo(oldLeader, cmd)
						})
					}
					s.After(600, func() {
						s.Note("warn", fmt.Sprintf("%s vẫn tưởng mình là leader và vẫn append entry, nhưng chỉ có 2/5 node lưu được → không bao giờ đạt majority → entry mãi mãi XÁM. Client không hề nhận OK. Đây là lý do Raft không bao giờ mất dữ liệu đã xác nhận.", oldLeader))
					})

					// Phía majority sẽ tự bầu leader mới.
					s.After(700, func() {
						waitLeaderExcept(c, s, 0, oldLeader, func(newL string) {
							s.Note("good", fmt.Sprintf("Phía majority bầu được leader mới %s với term cao hơn, và commit bình thường vì đủ 3/5. Lúc này cluster có HAI node cùng nghĩ mình là leader — nhưng ở hai term khác nhau, nên Election Safety không bị vi phạm. Chỉ bên có majority mới commit được.", newL))
							for i, cmd := range []string{"set real=1", "set real=2"} {
								cmd := cmd
								s.After(sim.Time(250*i)+100, func() { c.ClientWriteTo(c.LeaderID(), cmd) })
							}
						})
					})
				})

				s.After(4000, func() {
					s.Note("info", "Nối lại mạng. Leader cũ sắp thấy một term cao hơn của mình.")
					c.Heal()
				})

				s.After(4400, func() {
					s.Note("good", "Leader cũ thấy term cao hơn → tự lùi về Follower. Các entry 'ghost' nó đã append nhưng chưa commit bị CẮT BỎ để log khớp với leader mới. Đó là Log Matching Property đang tự sửa chữa. Các write này chưa từng được ack nên client không hề bị lừa.")
				})
			})
		},
	},
}
