package raft_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/tienlamict/topic1-consensus-demo/internal/raft"
	"github.com/tienlamict/topic1-consensus-demo/internal/scenario"
	"github.com/tienlamict/topic1-consensus-demo/internal/sim"
)

// TestDeterministic là bài test nền tảng nhất: cùng seed phải cho ra trace
// giống hệt nhau. Nếu bài này hỏng thì mọi thứ khác đều vô nghĩa — thường
// nguyên nhân là ở đâu đó có duyệt map ảnh hưởng tới thứ tự gọi rng.
func TestDeterministic(t *testing.T) {
	for _, sc := range scenario.All {
		for seed := int64(1); seed <= 5; seed++ {
			a, err := scenario.Run(sc.ID, seed, nil)
			if err != nil {
				t.Fatal(err)
			}
			b, err := scenario.Run(sc.ID, seed, nil)
			if err != nil {
				t.Fatal(err)
			}
			ja, _ := json.Marshal(a)
			jb, _ := json.Marshal(b)
			if string(ja) != string(jb) {
				t.Fatalf("%s seed=%d: hai lần chạy cho kết quả khác nhau", sc.ID, seed)
			}
		}
	}
}

// randomFaults dựng một cluster rồi ném vào đó crash/recover/partition ngẫu
// nhiên, kiểm tra safety property sau mỗi bước. Đây là lưới an toàn chính để
// bắt lỗi cài đặt Raft.
func randomFaults(t *testing.T, seed int64, nodes int) {
	t.Helper()
	s := sim.New(seed)
	cfg := raft.DefaultConfig()
	ids := make([]string, nodes)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i+1)
	}
	c := raft.NewCluster(s, cfg, ids)
	rng := s.Rand()

	var step func()
	step = func() {
		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed=%d t=%d: %v", seed, s.Now(), err)
		}
		switch rng.Intn(6) {
		case 0:
			c.Crash(ids[rng.Intn(nodes)])
		case 1:
			c.Recover(ids[rng.Intn(nodes)])
		case 2:
			// Cắt ngẫu nhiên thành 2 nhóm.
			var a, b []string
			for _, id := range ids {
				if rng.Intn(2) == 0 {
					a = append(a, id)
				} else {
					b = append(b, id)
				}
			}
			if len(a) > 0 && len(b) > 0 {
				c.Partition([][]string{a, b})
			}
		case 3:
			c.Heal()
		default:
			c.ClientWrite(fmt.Sprintf("set k%d=%d", rng.Intn(5), rng.Intn(100)))
		}
		s.After(sim.Time(20+rng.Intn(200)), step)
	}
	s.After(100, step)
	s.RunUntil(20000)

	if err := c.CheckInvariants(); err != nil {
		t.Fatalf("seed=%d cuối mô phỏng: %v", seed, err)
	}
}

// TestSafetyUnderFaults chạy hàng trăm seed với fault injection ngẫu nhiên.
// Kiểm tra 3 property ở §5.2 tài liệu: Election Safety, Log Matching,
// State Machine Safety.
func TestSafetyUnderFaults(t *testing.T) {
	for seed := int64(1); seed <= 300; seed++ {
		randomFaults(t, seed, 5)
	}
	for seed := int64(1); seed <= 100; seed++ {
		randomFaults(t, seed, 3)
	}
}

// TestNoSplitBrain: trong toàn bộ trace, không bao giờ có 2 node cùng tuyên bố
// là leader của cùng một term. Đây là Election Safety kiểm tra theo dòng thời
// gian chứ không chỉ ở các mốc lấy mẫu.
func TestNoSplitBrain(t *testing.T) {
	for _, sc := range scenario.All {
		for seed := int64(1); seed <= 30; seed++ {
			res, err := scenario.Run(sc.ID, seed, nil)
			if err != nil {
				t.Fatal(err)
			}
			leaderOfTerm := map[int]string{}
			cur := map[string]struct {
				state string
				term  int
			}{}
			for _, r := range res.Trace {
				if r.Kind != "node" {
					continue
				}
				b, _ := json.Marshal(r.State)
				var sn struct {
					State string `json:"state"`
					Term  int    `json:"term"`
					Alive bool   `json:"alive"`
				}
				_ = json.Unmarshal(b, &sn)
				cur[r.Node] = struct {
					state string
					term  int
				}{sn.State, sn.Term}
				if sn.State == "leader" && sn.Alive {
					if other, ok := leaderOfTerm[sn.Term]; ok && other != r.Node {
						t.Fatalf("%s seed=%d t=%d: %s và %s cùng là leader term %d",
							sc.ID, seed, r.T, other, r.Node, sn.Term)
					}
					leaderOfTerm[sn.Term] = r.Node
				}
			}
		}
	}
}

// TestAckedWritesSurvive: một entry đã được ack cho client thì phải còn nguyên
// trong log của node có log dài nhất ở cuối mô phỏng — Leader Completeness
// (§5.2). Đây là tính chất "Raft không mất write đã xác nhận", tức đúng thứ mà
// Redis Cluster KHÔNG có (§5.4) — sẽ đối chiếu ở nhánh Redis sau.
func TestAckedWritesSurvive(t *testing.T) {
	for _, sc := range scenario.All {
		for seed := int64(1); seed <= 20; seed++ {
			res, err := scenario.Run(sc.ID, seed, nil)
			if err != nil {
				t.Fatal(err)
			}

			var acked []string
			finalLog := map[string][]raft.Entry{}
			for _, r := range res.Trace {
				switch r.Kind {
				case "ack":
					if cmd, ok := r.Data["cmd"].(string); ok {
						acked = append(acked, cmd)
					}
				case "node":
					b, _ := json.Marshal(r.State)
					var sn struct {
						Log []raft.Entry `json:"log"`
					}
					_ = json.Unmarshal(b, &sn)
					finalLog[r.Node] = sn.Log
				}
			}

			// Gom tập lệnh có trong log dài nhất ở cuối mô phỏng.
			var longest []raft.Entry
			for _, l := range finalLog {
				if len(l) > len(longest) {
					longest = l
				}
			}
			have := map[string]int{}
			for _, e := range longest {
				have[e.Cmd]++
			}
			for _, cmd := range acked {
				if have[cmd] == 0 {
					t.Fatalf("%s seed=%d: write %q đã ack cho client nhưng biến mất khỏi log", sc.ID, seed, cmd)
				}
				have[cmd]--
			}
		}
	}
}
