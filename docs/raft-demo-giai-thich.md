# Giải thích chi tiết: Demo Raft Consensus

> Tài liệu này giải thích **luồng hoạt động** của project demo trong thư mục này và
> **liên hệ trực tiếp** với lý thuyết trong `topic-1-distributed-systems-consensus.md`.
> Mọi tham chiếu dạng **§x.y** trỏ tới mục tương ứng trong file lý thuyết đó.

---

## 0. Mục tiêu & phạm vi

Project này cài đặt một phiên bản **Raft tối giản nhưng đúng** bằng Go, mô phỏng
một cluster 3 node chạy trong **một tiến trình duy nhất** (mỗi node là một goroutine,
liên lạc qua "mạng giả lập" in-memory). Mục tiêu là **nhìn thấy được** các cơ chế
mà file lý thuyết mô tả bằng chữ:

| Cơ chế trong lý thuyết | Minh họa trong demo |
|------------------------|---------------------|
| Server states & terms (§2.1) | Log in ra `[state]` và `[term]` của từng node theo thời gian thực |
| Leader election + randomized timeout (§2.2, §4.1) | Màn 1, Màn 3 |
| Log replication + commit qua majority (§2.2, §3.1.2) | Màn 2 |
| Failure detection qua missing heartbeat (§4.1) | Màn 3 (kill leader) |
| Network partition & safety (§1.2, §5.5) | Màn 4 |
| Leader Completeness — committed entry không mất (§5.2) | Màn 5 |

**Phạm vi CÓ:** leader election, log replication, commit rule an toàn, heartbeat,
network partition, kill/revive node.

**Phạm vi CHƯA (cố ý bỏ để giữ đơn giản):** persistence ra disk thật,
snapshot/InstallSnapshot (§3.1.3), membership change/joint consensus (§1.4),
pre-vote. Đây là các hướng mở rộng — xem mục 7.

---

## 1. Bản đồ source code

```
topic-1-distributed-systems-consensus/
├── raft/
│   ├── types.go     # States, Terms, LogEntry, các RPC message (§2.1, §3.1)
│   ├── network.go   # Mạng in-memory, hỗ trợ cắt liên lạc (partition) (§4, §5.5)
│   └── node.go      # LÕI thuật toán Raft: election, replication, safety
├── cmd/demo/
│   └── main.go      # Kịch bản 5 màn minh họa
└── docs/
    └── raft-demo-giai-thich.md  # (file này)
```

Một node Raft (`raft/node.go`) giữ đúng các biến trạng thái mà Figure 2 của paper
yêu cầu:

```
currentTerm   // term hiện tại — logical clock (§2.1)
votedFor      // đã vote cho ai trong term này (-1 = chưa)
log[]         // replicated log; log[0] là sentinel, entry thật từ index 1
state         // Follower / Candidate / Leader
commitIndex   // index cao nhất đã biết là committed
lastApplied   // index cao nhất đã apply vào state machine
nextIndex[]   // (leader) index kế tiếp sẽ gửi cho từng follower
matchIndex[]  // (leader) index cao nhất đã replicate chắc chắn cho từng follower
```

---

## 2. Ba trạng thái và "term" (§2.1)

Mỗi node luôn ở đúng **một** trong ba trạng thái: `FOLLOWER`, `CANDIDATE`, `LEADER`.
Trong demo, mỗi dòng log có dạng:

```
[+  1.02s][node 1][term  1][CANDIDATE] election timeout → trở thành CANDIDATE...
   ^thời gian   ^node    ^term   ^trạng thái     ^sự kiện
```

**Term là logical clock (§2.1):** mỗi term bắt đầu bằng một election, đánh số tăng dần.
Khi hai node liên lạc, node có term nhỏ hơn sẽ **cập nhật term lên cho bằng** và
**lùi về FOLLOWER**. Đây là cơ chế phát hiện "stale leader" — bạn thấy nó hoạt động
ở Màn 5:

```
[+ 12.25s][node 1][term  3][LEADER ] lùi về FOLLOWER (term 3)
```

Node 1 tưởng mình còn là leader (term 2 cũ) nhưng khi mạng khôi phục, nó nghe thấy
term 3 từ leader mới → tự động thoái vị. Code: `becomeFollowerLocked()` trong `node.go`.

---

## 3. Vòng lặp chính của một node

Toàn bộ điều phối nằm trong `Node.loop()` (`node.go`), tick mỗi 20ms:

```
mỗi 20ms:
  nếu là FOLLOWER hoặc CANDIDATE:
     nếu (now - lastHeard) >= electionTimeout:   → startElection()   // §4.1
  nếu là LEADER:
     nếu (now - lastHbSent) >= heartbeatInterval: → broadcastAppendEntries()  // §2.2
```

- `lastHeard` được **reset** mỗi khi node nhận một RPC hợp lệ (AppendEntries từ leader,
  hoặc khi cấp vote). Đây chính là "nghe thấy nhịp tim của leader".
- Nếu leader chết, follower **không** reset `lastHeard` nữa → đến lúc timeout → tự ứng cử.

### Về thông số timing (§4.1 — rất quan trọng)

Paper khuyến nghị `election timeout = 150–300ms`, `heartbeat ≈ 50ms`, và bất đẳng thức:

```
broadcastTime  ≪  electionTimeout  ≪  MTBF
```

Demo **cố ý phóng to** các con số để mắt người kịp đọc:

| Thông số | Paper (§4.1) | Demo (constant trong `node.go`) |
|----------|--------------|----------------------------------|
| heartbeat | ~50ms | `heartbeatInterval = 300ms` |
| election timeout | 150–300ms | `electionTimeoutMin..Max = 1000–2000ms` |

Quan hệ `heartbeat ≪ electionTimeout` vẫn được giữ (300ms ≪ 1000ms), nên thuật toán
hành xử đúng — chỉ là "chậm lại" để quan sát.

---

## 4. Luồng Màn 1 — Leader Election (§2.2, §4.1)

**Diễn biến quan sát được:**

```
[+1.02s][node 1][term 1][CANDIDATE] election timeout → trở thành CANDIDATE, xin vote
[+1.02s][node 2][term 1][FOLLOWER ] CẤP VOTE cho node 1
[+1.02s][node 1][term 1][CANDIDATE] nhận vote từ node 2 (2/3)
[+1.02s][node 1][term 1][LEADER   ] >>> THẮNG ELECTION — trở thành LEADER <<<
[+1.02s][node 0][term 1][FOLLOWER ] CẤP VOTE cho node 1
```

**Các bước (khớp với §2.2 "Leader Election"):**

1. Cả 3 node khởi động ở FOLLOWER, mỗi node bốc một `electionTimeout` **ngẫu nhiên**
   trong [1000ms, 2000ms]. → `resetElectionTimeout()`.
2. Node nào timeout **trước** (ở đây node 1, ~1.02s) sẽ:
   - tăng `currentTerm` (0 → 1),
   - chuyển sang CANDIDATE,
   - **vote cho chính mình** (`votedFor = id`),
   - gửi `RequestVote` RPC **song song** tới mọi peer. → `startElection()`.
3. Mỗi peer nhận `RequestVote` chạy **Receiver rules §3.1.1** (`handleRequestVote`):
   - term hợp lệ, chưa vote ai, và log candidate "đủ mới" → **cấp vote**.
4. Candidate gom đủ **majority = 2/3** vote → `becomeLeaderLocked()` → LEADER.
5. Ngay khi thành leader, nó gửi heartbeat (AppendEntries rỗng) để **chặn** các node
   khác timeout. Vì vậy node 0/node 2 không bao giờ kịp tự ứng cử.

**Tại sao randomized timeout là "thiết kế thiên tài" (§4.1)?** Nếu mọi node dùng
chung một timeout cố định, chúng sẽ cùng timeout, cùng ứng cử, **chia phiếu (split vote)**,
không ai đạt majority → lặp lại vô hạn. Random hóa khiến **một node hầu như luôn
timeout trước**, thắng gọn trước khi node khác kịp khởi động. Benchmark trong paper
(§4.1): không random → election >10s; chỉ cần 5ms random → ~287ms.

> **Election Safety Property (§5.2):** tối đa 1 leader/term. Được đảm bảo bởi luật
> "mỗi node chỉ vote 1 lần/term" (`votedFor`) → không thể có 2 candidate cùng đạt
> majority trong cùng term (vì majority phải overlap).

---

## 5. Luồng Màn 2 — Log Replication & Commit (§2.2, §3.1.2)

**Diễn biến:**

```
[node 1][LEADER] CLIENT command "set x 1" → append vào log tại index 1
[node 1][LEADER] CLIENT command "set y 2" → append vào log tại index 2
[node 0][FOLLOWER] nhận 2 entry từ leader 1 → log dài 2
[node 1][LEADER] entry index 2 đã COMMITTED (replicate trên 2/3 node)
[node 1][LEADER] APPLY index 1: "set x 1" → state machine = map[x:1]
[node 1][LEADER] APPLY index 2: "set y 2" → state machine = map[x:1 y:2]
```

**Các bước (khớp §2.2 "Log Replication"):**

1. Client gọi `leader.Submit("set x 1")` → leader **append** entry `{Term:1, Cmd:"set x 1"}`
   vào local log. (Follower từ chối Submit — thực tế sẽ redirect, §2.1.)
2. Ở heartbeat kế tiếp, leader gửi `AppendEntries` chứa các entry mới tới mọi follower.
   → `replicateTo()` dùng `nextIndex[peer]` để biết gửi từ đâu.
3. Follower chạy **Receiver rules §3.1.2** (`handleAppendEntries`):
   - kiểm tra `prevLogIndex/prevLogTerm` khớp (Log Matching, §5.2),
   - xoá entry xung đột nếu có, rồi **append** entry mới.
4. Follower trả `Success=true`. Leader cập nhật `matchIndex[peer]`.
5. **Commit rule:** khi một index đã có mặt trên **majority** (`matchIndex >= idx` ở ≥2 node),
   leader nâng `commitIndex` lên index đó → **apply** vào state machine → trả kết quả client.
   → `advanceCommitLocked()` + `applyLocked()`.
6. Lần heartbeat sau, leader đính kèm `leaderCommit` → follower cũng apply (bạn thấy
   node 0, node 2 apply ngay sau đó).

### Điểm an toàn tinh tế (§2.2 cuối mục): chỉ commit entry của term hiện tại

Trong `advanceCommitLocked()` có dòng:

```go
if n.log[idx].Term != n.currentTerm { continue }  // KHÔNG commit entry term cũ bằng cách đếm replica
```

Đây là một **safety restriction quan trọng** của Raft: leader **không** được commit
một entry của term trước chỉ vì nó đã nằm trên majority. Lý do (Figure 8 trong paper):
một entry cũ nằm trên majority vẫn có thể bị ghi đè bởi leader tương lai. Raft chỉ
commit entry **của term hiện tại**, và entry cũ được commit *gián tiếp* nhờ Log Matching.

---

## 6. Luồng Màn 3–5 — Failover, Partition, Hội tụ

### Màn 3 — Kill leader → re-election (§4.1)

```
[node 1][LEADER] đã bị KILL (mô phỏng crash)
... ~1s sau (election timeout) ...
[node 0][term 2][CANDIDATE] election timeout → xin vote
[node 0][term 2][LEADER] >>> THẮNG ELECTION (term 2) <<<
```

`Kill()` đặt cờ `dead` → node ngừng respond mọi RPC (mạng coi như không reachable).
Follower mất heartbeat → timeout → bầu leader mới ở **term cao hơn** (1 → 2). Đây là
**failure detection qua missing heartbeat** (§4.1) — Raft không có "FAIL message"
tường minh như Redis (§4.3), mà *suy ra* từ việc thiếu nhịp tim.

### Màn 4 — Network partition: minority KHÔNG commit (§1.2, §5.5)

Đây là màn quan trọng nhất về **trade-off safety vs availability**.

```
net.Partition([]int{1}, []int{0,2})   // cô lập leader (node 1) khỏi 2 node kia
[node 1][LEADER] CLIENT command "set a 99" → append tại index 4   ← KHÔNG commit được!
[node 2][term 3][LEADER] >>> THẮNG ELECTION (term 3) <<<           ← majority bầu leader mới
[node 2][LEADER] entry index 4 "set b 7" đã COMMITTED              ← majority commit OK
```

**Hai điều xảy ra song song:**

- **Phía minority (node 1, 1/3):** vẫn tưởng mình là leader, nhận `set a 99`, append
  vào log — nhưng **không bao giờ đạt majority** (chỉ nó tự lưu) → `commitIndex` **không**
  tăng → command **không** được apply. Client (nếu có) sẽ không nhận được xác nhận.
- **Phía majority (node 0+2, 2/3):** mất heartbeat từ node 1 → bầu node 2 làm leader
  term 3 → **vẫn hoạt động bình thường**, commit `set b 7`.

> **Đây chính là §1.2 và §5.5:** Raft **ưu tiên safety** — minority **dừng** (không
> trả kết quả sai) thay vì liều lĩnh commit. Hệ thống chỉ tiến triển khi **majority**
> còn liên lạc được. So sánh với Redis (§5.4): Redis cho phép master phía minority
> tiếp tục nhận write một khoảng thời gian → có **window mất dữ liệu** ("last failover
> wins"). Raft thì không — đổi lại availability thấp hơn ở phía minority.

Lưu ý: cùng một **index 4** nhưng hai phía ghi hai entry khác nhau (`set a 99` term 2
ở node 1, `set b 7` term 3 ở node 2). Chỉ entry của majority là *committed*.

### Màn 5 — Heal → hội tụ (Leader Completeness §5.2)

```
net.Heal()
[node 1][term 3][LEADER] lùi về FOLLOWER (term 3)          ← phát hiện term cao hơn
[node 1][FOLLOWER] nhận 1 entry từ leader 2 → log dài 4    ← "set a 99" bị GHI ĐÈ bởi "set b 7"
```

Khi mạng khôi phục:

1. Node 1 (leader cũ term 2) nhận heartbeat term 3 → biết mình stale → **lùi về FOLLOWER**.
2. Leader 2 replicate log của nó cho node 1. Tại index 4, node 1 đang có `set a 99`
   (term 2) nhưng leader gửi `set b 7` (term 3) → **xung đột** → `handleAppendEntries`
   **cắt bỏ** entry cũ và ghi entry mới (Log Matching, §5.2).
3. Kết quả: cả 3 node hội tụ về log y hệt: `{x:1, y:2, z:3, b:7}`.

> **Leader Completeness (§5.2):** entry `set b 7` đã **committed** → nó **chắc chắn**
> có mặt trong log của mọi leader tương lai. Còn `set a 99` **chưa từng committed** →
> được phép biến mất. Đây là ranh giới đảm bảo của Raft: *committed = vĩnh viễn,
> uncommitted = có thể mất.* Không bao giờ có chuyện một entry đã committed bị mất.

---

## 7. Cách chạy

```powershell
# Tại thư mục gốc project
go run ./cmd/demo
```

Demo tự chạy 5 màn, in log theo thời gian thực (~13 giây). Mỗi lần chạy thứ tự
election có thể khác (do randomized timeout) nhưng **các tính chất an toàn luôn đúng**.

Build kiểm tra:

```powershell
go build ./...
```

---

## 8. Đối chiếu nhanh với bảng so sánh Raft vs Redis (§4.3, §5.7)

Demo này cài **Raft** (cột "etcd/Raft, strong" trong bảng §5.7). Những điểm mà demo
giúp bạn *thấy tận mắt* để so sánh với Redis Cluster:

| Tiêu chí (§5.7) | Demo Raft minh họa | Redis Cluster (chỉ lý thuyết §4.2, §5.4) |
|-----------------|--------------------|-------------------------------------------|
| Consensus ở data path | **Có** — mọi `set` phải qua majority trước khi commit (Màn 2) | Không — replicate async, không qua consensus |
| Failover | Leader election qua RequestVote (Màn 3) | Epoch voting `FAILOVER_AUTH_REQUEST/ACK` |
| Mất dữ liệu khi failure | **Không** với committed entry (Màn 5) | **Có window** "last failover wins" (§5.4) |
| Phía minority | **Dừng**, không commit (Màn 4) | Master minority vẫn nhận write 1 lúc → mất write |
| Phát hiện lỗi | Missing heartbeat, *implicit* | PFAIL/FAIL hai pha, *explicit* FAIL message |

---

## 9. Hướng mở rộng (nếu muốn làm tiếp)

Theo thứ tự độ khó tăng dần, bám sát các phần còn lại của file lý thuyết:

1. **Persistence thật** — ghi `currentTerm/votedFor/log` xuống file, đọc lại khi Revive
   (hiện demo giả định state sống sót). Liên hệ §1.2 "recover từ stable storage".
2. **InstallSnapshot / log compaction (§3.1.3)** — khi log quá dài.
3. **Cài song song một cluster "kiểu Redis epoch-voting"** để chạy cạnh nhau và so sánh
   trực tiếp data-loss window (§5.4) vs Raft no-loss — đây là phần *đóng góp học thuật*
   thú vị mà file lý thuyết gợi ý ở §10.
4. **Đo benchmark scaling** — tăng số node (3→5→7), đo độ trễ commit để tái hiện
   "put latency tăng tuyến tính" của etcd (§5.5).
5. **Membership change / joint consensus (§1.4)** — thêm/bớt node lúc đang chạy.
6. **Visualize** — xuất sự kiện ra một dashboard web để xem state/term theo thời gian.

---

*Tài liệu đi kèm project demo Raft, liên hệ với `topic-1-distributed-systems-consensus.md`.*
