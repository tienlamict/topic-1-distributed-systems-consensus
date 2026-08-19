# Đề xuất Demo trực quan — Topic 1: Distributed Systems & Consensus

> **Trạng thái:** Nhánh Raft **đã làm xong** (S1–S5 + thí nghiệm định lượng §4.1) — xem [`../README.md`](../README.md) để chạy. Nhánh Redis Cluster (S7–S11), phần so sánh (S12–S14) và phụ lục Docker (P6) **chưa làm**.
> **Nguồn lý thuyết:** [`topic-1-distributed-systems-consensus_v1.2.md`](topic-1-distributed-systems-consensus_v1.2.md)
> **Ngày:** 15/08/2026
>
> **Thay đổi so với bản đề xuất khi triển khai:**
> - Bỏ WebSocket. Server chạy trọn mô phỏng rồi trả cả trace qua HTTP một lần; browser tự replay → **tua ngược được**, không chỉ tua xuôi.
> - S6 (slider timing requirement) được gộp vào S2 dưới dạng tham số `maxLatency` thay vì làm kịch bản riêng.
> - S4 được nâng từ P1 lên làm luôn, vì logic election restriction vốn đã nằm trong Figure 2.

---

## 1. Mục tiêu demo

Biến phần lý thuyết (Raft + Redis Cluster epoch voting) thành một thứ **nhìn thấy được và chạy lại được**, phục vụ 3 nhu cầu:

| Nhu cầu | Yêu cầu tương ứng |
|---------|-------------------|
| **Giảng/bảo vệ luận văn** | Chiếu lên màn hình, tua chậm, tạm dừng đúng khoảnh khắc "split vote" hay "write bị mất" để giải thích |
| **Chứng minh claim trong tài liệu** | Số liệu sinh ra từ demo phải đối chiếu được với con số trong paper (VD: §4.1 — không randomize thì election > 10s) |
| **Nền cho Topic 2–5** | Cùng codebase mở rộng được sang phần Operator/reconcile sau này |

**Nguyên tắc xuyên suốt:** demo phải **deterministic** — cùng seed → cùng kết quả từng millisecond. Đây là điều kiện bắt buộc để (a) quay video/chụp hình tái hiện được, (b) số liệu trong luận văn có thể reproduce, (c) không bị "lần chạy này ra kết quả khác lần trước" khi đang trình bày.

---

## 2. Lựa chọn phương án

| | **A. Simulator + Web UI** ⭐ đề xuất | **B. Cluster thật (Docker)** | **C. Terminal TUI** |
|---|---|---|---|
| Cách làm | Mô phỏng Raft & Redis Cluster bằng Go, virtual clock, UI web | Chạy Redis Cluster + etcd thật trong Docker, quan sát log | Mô phỏng nhưng render bằng terminal |
| Deterministic | ✅ Hoàn toàn | ❌ Không | ✅ |
| Tua chậm / tua nhanh / step | ✅ | ❌ | ✅ |
| Thấy được message bay giữa node | ✅ | ❌ (chỉ thấy log) | ⚠️ Hạn chế |
| Mô phỏng network partition | ✅ Tức thì | ⚠️ Phải nghịch iptables | ✅ |
| Tính thuyết phục học thuật | ⚠️ "Chỉ là mô phỏng" | ✅ Hệ thống thật | ⚠️ |
| Công sức | Trung bình | Thấp | Thấp |

**Đề xuất: A là chính + B làm phụ lục đối chứng.**

Lý do ghép A+B: điểm yếu duy nhất của simulator là câu hỏi *"mô phỏng có đúng như hệ thống thật không?"*. Ta trả lời bằng một script nhỏ dựng Redis Cluster thật bằng Docker, gây failover thật, rồi **đối chiếu timeline thật với timeline simulator** (thời gian PFAIL→FAIL→promotion). Nếu khớp trong sai số chấp nhận được, simulator được "chứng thực" và mọi scenario khác chạy trên simulator đều đáng tin. Đây cũng chính là điều §9 tài liệu gốc gợi ý ("Redis source code — verify các claim từ spec với code thực tế"), nhưng làm theo hướng dễ kiểm chứng hơn.

---

## 3. Kiến trúc

```
┌─────────────────────────────────────────────────────────┐
│  Browser (không cần build step — vanilla JS + SVG)      │
│  ┌───────────┬──────────────┬───────────────────────┐   │
│  │ Cluster   │ Timeline &   │ Metrics / State       │   │
│  │ topology  │ message flow │ inspector             │   │
│  └───────────┴──────────────┴───────────────────────┘   │
│  Controls: ▶ ⏸ ⏭step  speed×  seed  [inject fault]      │
└────────────────────────┬────────────────────────────────┘
                         │ WebSocket (JSON event stream)
┌────────────────────────┴────────────────────────────────┐
│  Go binary (single file, go:embed toàn bộ UI)           │
│                                                          │
│  ┌────────────────────────────────────────────────┐     │
│  │  Discrete-Event Simulator                      │     │
│  │  • Virtual clock (không dùng time.Now)         │     │
│  │  • Priority queue theo timestamp               │     │
│  │  • Seeded RNG → deterministic                  │     │
│  │  • Network model: latency, drop, partition     │     │
│  └───────────────┬────────────────┬───────────────┘     │
│                  │                │                      │
│         ┌────────┴──────┐  ┌──────┴────────────┐        │
│         │ engine/raft   │  │ engine/redis      │        │
│         │ (§2.1–2.2,    │  │ (§2.3–2.4, §4.2)  │        │
│         │  §3.1, §4.1)  │  │  epoch, PFAIL/FAIL│        │
│         └───────────────┘  └───────────────────┘        │
│                  │                │                      │
│         ┌────────┴────────────────┴───────────┐         │
│         │  Event bus → WebSocket / JSONL file │         │
│         └─────────────────────────────────────┘         │
└──────────────────────────────────────────────────────────┘
```

### Vì sao là discrete-event simulator, không phải goroutine + `time.Sleep`

Đây là quyết định kỹ thuật quan trọng nhất. Nếu dùng goroutine thật + sleep thật:
- Không deterministic (Go scheduler)
- Muốn xem 60 giây failover phải chờ 60 giây thật
- Không "step từng message" được

Với virtual clock: toàn bộ 60 giây mô phỏng chạy xong trong ~10ms, rồi ta **phát lại (replay)** theo tốc độ mong muốn. Vừa nhanh, vừa tua được ngược/xuôi, vừa reproduce chính xác.

### Cấu trúc thư mục dự kiến

```
topic-1-distributed-systems-consensus/
├── docs/                          # tài liệu (đã có)
├── cmd/consensus-demo/main.go     # entrypoint: serve UI + chạy scenario
├── internal/
│   ├── sim/                       # clock, event queue, network, RNG
│   ├── raft/                      # state machine Raft
│   ├── rediscluster/              # epoch voting, gossip, PFAIL/FAIL
│   ├── scenario/                  # các kịch bản S1..S10
│   └── web/                       # HTTP + WebSocket
├── web/                           # index.html, app.js, style.css (go:embed)
├── experiments/                   # script chạy batch → CSV cho biểu đồ luận văn
├── validate/                      # docker-compose Redis Cluster thật (phụ lục)
└── README.md
```

---

## 4. Kịch bản demo — ánh xạ với lý thuyết

Mỗi kịch bản là một nút bấm trên UI. Cột **§** trỏ về mục trong tài liệu lý thuyết.

### Nhóm A — Raft

| ID | Kịch bản | § | Người xem sẽ thấy gì | Ưu tiên |
|----|----------|---|----------------------|---------|
| **S1** | Leader election bình thường | 2.2, 4.1 | 5 node cùng khởi động → mỗi node có election timeout ngẫu nhiên khác nhau (thanh countdown chạy) → node timeout trước tiên thành candidate → RequestVote bay ra 4 hướng → đủ 3 phiếu → leader, heartbeat bắt đầu đập | P0 |
| **S2** | **Split vote & sức mạnh của randomization** | 2.1, 4.1 | Chạy 2 lần cạnh nhau: (a) tắt randomization → term nhảy 1→2→3→4… liên tục không ai thắng, đúng như paper nói "> 10s"; (b) bật randomization → xong trong ~300ms. **Đây là kịch bản đắt giá nhất** vì nó chứng minh trực tiếp §4.1 | P0 |
| **S3** | Log replication & quorum commit | 2.2, 3.1.2 | Client gửi `set x=3` → leader append (entry màu xám = uncommitted) → AppendEntries bay đi → khi phiếu ACK đạt majority, entry chuyển xanh (committed) đồng loạt → leader reply client. Thanh "commitIndex" của từng node nhích lên | P0 |
| **S4** | Election restriction (safety) | 2.2, 5.2 | Cô lập 1 follower cho nó tụt hậu log → nối lại → nó timeout và xin phiếu → **bị từ chối** vì log không "at least as up-to-date" → tooltip hiện phép so sánh `(lastLogTerm, lastLogIndex)` | P1 |
| **S5** | Partition: minority leader không commit được | 2.2, 5.2 | Cắt cluster 5 node thành 2/3. Leader cũ nằm phía 2 node: vẫn nhận write nhưng entry **mãi mãi xám** (không đủ quorum). Phía 3 node bầu leader mới. Heal → leader cũ thấy term cao hơn → tự về follower → **log thừa bị cắt bỏ** (animation xoá entry) — minh hoạ Log Matching Property | P0 |
| **S6** | Timing requirement | 4.1 | Slider chỉnh `broadcastTime` và `electionTimeout`. Kéo cho `broadcastTime` ≈ `electionTimeout` → cluster rơi vào trạng thái bầu cử liên miên. Minh hoạ trực quan bất đẳng thức `broadcastTime ≪ electionTimeout ≪ MTBF` | P1 |

### Nhóm B — Redis Cluster

| ID | Kịch bản | § | Người xem sẽ thấy gì | Ưu tiên |
|----|----------|---|----------------------|---------|
| **S7** | Gossip heartbeat | 2.4 | 6 node (3 master + 3 replica) ping ngẫu nhiên vài node/giây. Bộ đếm hiển thị số ping/s để đối chiếu với ví dụ trong spec (100 nodes, NODE_TIMEOUT=60s → ~330 ping/s) | P1 |
| **S8** | **PFAIL → FAIL hai pha** | 4.2 | Kill 1 master. Node đầu tiên phát hiện tô **vàng (PFAIL)** — chỉ mình nó biết. Gossip lan tin → từng node vàng dần → khi **majority master** cùng báo PFAIL, node đó tô **đỏ (FAIL)** và bắn `FAIL` message ép mọi node đồng loạt đỏ. Thấy rõ ranh giới "local knowledge" vs "majority-confirmed" | P0 |
| **S9** | Replica election với epoch & rank delay | 2.3, 4.2 | Sau FAIL, các replica đợi `500ms + random(0-500ms) + rank×1000ms` (countdown hiện trên từng replica) → replica rank 0 xin `FAILOVER_AUTH_REQUEST` trước → master vote (`lastVoteEpoch` cập nhật, hiện trên node) → thắng → nhận `configEpoch` mới cao nhất → broadcast config | P0 |
| **S10** | **Cửa sổ mất write ("last failover wins")** | 5.4 | Client ghi liên tục. Master **ACK ngay** trước khi replicate (async — mũi tên ACK về client bay trước mũi tên replicate). Kill master đúng lúc đó → replica lên ngôi thiếu mất N write → **bộ đếm đỏ "WRITES LOST: 7"** kèm danh sách key bị mất. Đây là hình ảnh trực quan nhất cho trade-off của Redis | P0 |
| **S11** | Minority side ngừng nhận write | 5.4 | Partition. Phía minority: master vẫn sống nhưng sau `NODE_TIMEOUT` tự từ chối write (chuyển màu xám "read-only"). Trước mốc đó, mọi write đều thuộc diện có thể mất | P1 |

### Nhóm C — So sánh & định lượng

| ID | Kịch bản | § | Đầu ra | Ưu tiên |
|----|----------|---|--------|---------|
| **S12** | **Chế độ split-screen** | 3.3, 4.3 | Chạy Raft và Redis **cạnh nhau, cùng seed, cùng sự cố** (kill leader/master tại cùng thời điểm ảo). Người xem thấy trực tiếp: Raft mất ~300ms nhưng 0 write mất; Redis nhanh hơn trên write path nhưng mất 7 write. **Đây là slide kết luận của Topic 1** | P0 |
| **S13** | Latency/throughput theo cluster size | 5.5 | Chạy batch 3/5/7/9 node → xuất CSV + biểu đồ: quorum write latency tăng tuyến tính (mô hình etcd) vs Redis phẳng. Đối chiếu định tính với bảng §5.5 | P1 |
| **S14** | Availability probability của Redis | 5.4 | Monte Carlo: N master, 1 replica/master, partition ngẫu nhiên 2 node → đo tỉ lệ cluster unavailable, so với công thức `1/(N*2-1)`. Với N=5 kỳ vọng ≈ 11.11% | P2 |

> **Ghi chú trung thực:** S13 mô phỏng *mô hình* latency (quorum = chờ node chậm thứ ⌈n/2⌉), **không phải** benchmark etcd thật. Trong luận văn phải ghi rõ đây là mô hình minh hoạ xu hướng, còn số tuyệt đối lấy từ [10]. S14 tương tự — kiểm chứng công thức, không phải đo hệ thống thật.

---

## 5. Giao diện

```
┌────────────────────────────────────────────────────────────────────────┐
│ [Raft ▾] [Scenario: S5 Partition ▾]  seed:42  ⏮ ⏸ ▶ ⏭  ×0.25 ×1 ×4   │
├──────────────────────────────┬─────────────────────────────────────────┤
│                              │  NODE INSPECTOR — n2                    │
│         (n1)◀──heartbeat──   │  ┌───────────────────────────────────┐  │
│        ╱  ▲  ╲               │  │ state      : Leader               │  │
│    (n5)   │   (n2)★leader    │  │ currentTerm: 3                    │  │
│      ╲    │    ╱             │  │ votedFor   : n2                   │  │
│       (n4)─(n3)              │  │ commitIndex: 7   lastApplied: 7   │  │
│                              │  └───────────────────────────────────┘  │
│   ╌╌╌╌ PARTITION ╌╌╌╌        │  LOG                                    │
│                              │  ┌───┬───┬───┬───┬───┬───┬───┬───┐     │
│  Node: ● leader ○ follower   │  │t1 │t1 │t2 │t2 │t3 │t3 │t3 │t3 │     │
│        ◐ candidate           │  │ ■ │ ■ │ ■ │ ■ │ ■ │ ■ │ ■ │ □ │     │
│  Redis: ▨ PFAIL ▧ FAIL       │  └───┴───┴───┴───┴───┴───┴───┴───┘     │
│                              │      ■ committed   □ uncommitted        │
├──────────────────────────────┴─────────────────────────────────────────┤
│ TIMELINE   t = 4,271 ms                                                │
│ n1 ────────────●RequestVote───────────────────────────────────────      │
│ n2 ──────────────────●becomeLeader────■■■■■ heartbeat ■■■■■─────       │
│ n3 ─────────✕crash──────────────────────────────────────────────       │
│ n4 ──────────────────────●voteGranted───────────────────────────       │
│ n5 ──────────────────────●voteGranted───────────────────────────       │
├────────────────────────────────────────────────────────────────────────┤
│ EVENT LOG                              │ METRICS                       │
│ 4180 n1 election timeout (rand 187ms)  │ Elections     : 2             │
│ 4180 n1 → RequestVote(term=3) → all    │ Leader downtime: 291 ms       │
│ 4223 n4 → voteGranted(term=3)          │ Committed     : 7             │
│ 4225 n5 → voteGranted(term=3)          │ ⚠ Writes lost : 0             │
│ 4225 n1 wins election (3/5 votes)      │ Messages sent : 148           │
└────────────────────────────────────────────────────────────────────────┘
```

**Chi tiết UI đáng chú ý:**
- **Thanh countdown election timeout** vẽ ngay dưới mỗi node — đây là thứ làm cho "randomized timeout" trở nên hiển nhiên, không cần giải thích bằng lời.
- **Message bay có animation** dọc theo cạnh, màu theo loại (RequestVote xanh dương, AppendEntries xanh lá, FAIL đỏ, FAILOVER_AUTH_* tím). Click vào message → xem full payload đúng theo bảng §3.1.
- **Kéo chuột cắt cluster** để tạo partition tuỳ ý, không cần vào code.
- **Nút "Explain"**: mở panel trích đúng đoạn lý thuyết tương ứng với sự kiện đang chọn → nối demo với tài liệu.
- **Xuất GIF/PNG** từng khoảnh khắc để chèn thẳng vào luận văn.

---

## 6. Phạm vi — làm gì và KHÔNG làm gì

**Có làm:**
- Raft: leader election, log replication, commit rule, election restriction, term/persistent state, heartbeat, randomized timeout
- Redis: gossip heartbeat, PFAIL/FAIL, currentEpoch/configEpoch, `lastVoteEpoch`, FAILOVER_AUTH_REQUEST/ACK, replica rank delay, async replication + cửa sổ mất write, minority read-only

**Không làm (cố ý — nêu rõ trong luận văn):**
- Raft: InstallSnapshot / log compaction, joint consensus membership change, pre-vote → *chỉ giải thích bằng sơ đồ tĩnh, không animate. Lý do: chi phí cao, giá trị trực quan thấp.*
- Redis: 16384 hash slot & resharding, hash tag, replica migration, configEpoch conflict resolution → *ngoài trọng tâm consensus của Topic 1.*
- Byzantine fault → tài liệu gốc §10 đã nói không cần.
- Không cài đặt lại Raft "chuẩn production" — đây là **công cụ dạy học**, không phải thư viện. Sẽ ghi rõ trong README để tránh hiểu nhầm.

---

## 7. Kế hoạch triển khai

| Giai đoạn | Nội dung | Kết quả kiểm chứng được |
|-----------|----------|--------------------------|
| **P1. Nền simulator** | virtual clock, event queue, network model (latency/drop/partition), seeded RNG, event bus | Test: chạy 2 lần cùng seed → 2 file event JSONL **giống hệt nhau byte-by-byte** |
| **P2. Engine Raft** | 3 state, term, RequestVote, AppendEntries, commit rule, election restriction | Test bất biến: **không bao giờ có 2 leader cùng term**; committed entry không bao giờ đổi. Chạy 1000 seed ngẫu nhiên với fault injection để soi |
| **P3. UI** | WebSocket, vẽ topology, animation message, timeline, inspector, controls | S1–S3 chạy được, xem mượt |
| **P4. Engine Redis** | gossip, PFAIL/FAIL, epoch, failover voting, async replication + đếm write mất | S8–S10 chạy được; bộ đếm write mất khớp với số write đã ACK nhưng chưa replicate |
| **P5. So sánh & số liệu** | split-screen S12, batch runner xuất CSV, Monte Carlo S14 | Sinh ra được biểu đồ chèn thẳng vào luận văn |
| **P6. Đối chứng thật** *(phụ lục)* | docker-compose Redis Cluster 6 node, gây failover thật, parse log, so timeline với simulator | Bảng so sánh: thời điểm PFAIL/FAIL/promotion thật vs mô phỏng |

Mỗi giai đoạn là một commit chạy được — bạn review được từng bước, không phải chờ đến cuối.

---

## 8. Rủi ro

| Rủi ro | Xử lý |
|--------|-------|
| Cài đặt Raft có bug tinh vi → demo dạy sai | Property test bất biến (P2) chạy hàng nghìn seed; đối chiếu từng dòng với Figure 2 của paper, ghi comment `// Figure 2: rule N` tại mỗi chỗ |
| Redis Cluster spec có chỗ mơ hồ (VD: chi tiết luật vote của master) | Ghi rõ giả định trong README + comment; §7.2 tài liệu gốc đã đánh dấu các điểm "chỉ 1 nguồn" — chính là các chỗ cần ghi chú |
| UI phình to, tốn thời gian hơn phần thuật toán | Vanilla JS + SVG, không framework, không build step. Cắt tính năng theo P0/P1/P2 nếu thiếu thời gian |
| Người phản biện hỏi "mô phỏng có đúng không?" | Đó là lý do có P6 |

---

## 9. Cần bạn quyết trước khi tôi bắt tay

1. **Ngôn ngữ UI**: tiếng Việt / tiếng Anh / song ngữ? (Đề xuất: nhãn kỹ thuật giữ tiếng Anh — `term`, `commitIndex`, `PFAIL` — phần giải thích tiếng Việt.)
2. **Phạm vi đợt đầu**: làm **toàn bộ P0** (S1, S2, S3, S5, S8, S9, S10, S12) hay chỉ **nhánh Raft trước** (S1–S3, S5) rồi review xong mới sang Redis? (Đề xuất: Raft trước — cho bạn thấy sớm chất lượng UI/animation trước khi đầu tư tiếp.)
3. **Có làm P6 (Docker đối chứng) không?** — tăng đáng kể sức nặng học thuật, chi phí thêm khoảng 1 giai đoạn.
4. **Có cần đóng gói xuất bản không** — VD một trang web tĩnh chia sẻ được để chèn link vào luận văn?

Nếu bạn không có ý kiến khác, tôi sẽ mặc định: **song ngữ như đề xuất ở (1), làm nhánh Raft trước (2), có P6 (3), chưa đóng gói xuất bản (4)**.

---

*Bản đề xuất — chưa có dòng code nào được viết. Mọi tham chiếu § đều trỏ về `topic-1-distributed-systems-consensus_v1.2.md`.*
