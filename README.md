# Demo trực quan — Raft Consensus

Công cụ minh hoạ phần lý thuyết trong [`docs/topic-1-distributed-systems-consensus_v1.2.md`](docs/topic-1-distributed-systems-consensus_v1.2.md).
Kế hoạch tổng thể ở [`docs/demo-proposal_v1.md`](docs/demo-proposal_v1.md) — **repo này hiện đã hoàn thành nhánh Raft**; nhánh Redis Cluster chưa làm.

## Chạy

```bash
go run ./cmd/consensus-demo
```

Mở http://localhost:8080.

```bash
go run ./cmd/consensus-demo run -s s5 -seed 42
```

Chạy một kịch bản không cần trình duyệt, in tóm tắt ra terminal. Thêm `-out trace.json` để lấy trace thô.

```bash
go run ./cmd/consensus-demo experiment -runs 200
```

Chạy thí nghiệm định lượng §4.1, xuất CSV vào `experiments/`.

## Cách dùng UI

| Thao tác | Ý nghĩa |
|----------|---------|
| Chọn kịch bản ở thanh trên | Chạy lại mô phỏng ngay |
| Đổi **Seed** | Cùng seed luôn cho kết quả y hệt — dùng để tái hiện chính xác một tình huống khi trình bày |
| Kéo thanh tham số | Mô phỏng chạy lại tức thì với giá trị mới |
| **Space** | Chạy / tạm dừng |
| **→** | Nhảy tới sự kiện đáng chú ý kế tiếp |
| **←** | Lùi 100ms |
| Bấm vào timeline | Nhảy tới thời điểm bất kỳ |
| Bấm vào node | Xem toàn bộ state của node đó ở panel phải |

Vòng cung quanh mỗi node là election timeout đang đếm ngược — nhìn nó là hiểu ngay randomized timeout làm gì. Dải ô vuông dưới node là log: **xanh = đã commit**, **xám = chưa**, số bên trong là term của entry.

## Các kịch bản

| ID | Nội dung | § tài liệu |
|----|----------|------------|
| **S1** | Leader election cơ bản, rồi giết leader để thấy bầu lại | §2.2, §4.1 |
| **S2** | Split vote — kéo jitter về 0 là cluster không bao giờ bầu được leader | §2.1, §4.1 |
| **S3** | Log replication: entry chỉ chuyển xanh khi majority đã lưu | §2.2, §3.1.2 |
| **S4** | Election restriction: node có log cũ bị từ chối vote | §2.2, §5.2 |
| **S5** | Network partition: leader phía minority không commit được, log thừa bị cắt sau khi liền mạng | §2.2, §5.2 |

## Kiến trúc

Mô phỏng **discrete-event với thời gian ảo** — không `time.Now()`, không goroutine, không `time.Sleep`. Mọi nguồn ngẫu nhiên đi qua một `*rand.Rand` có seed duy nhất.

Hệ quả:

- **Deterministic tuyệt đối.** Cùng seed → trace giống nhau từng byte. Có test kiểm chứng điều này.
- **Chạy tức thì.** 12 giây mô phỏng tính xong trong vài ms.
- **Tua ngược được.** Server chạy hết mô phỏng rồi trả cả trace một lần; browser tự dựng lại trạng thái tại thời điểm T bất kỳ. Không có WebSocket.

```
cmd/consensus-demo/     entrypoint: serve UI | run | experiment
internal/sim/           virtual clock, event queue, mô hình mạng
internal/raft/          state machine Raft (bám Figure 2 của paper)
internal/scenario/      kịch bản S1..S5 + lời giải thích hiển thị trên UI
internal/web/           HTTP + asset
```

Lưu ý khi sửa engine: **không bao giờ duyệt map** ở chỗ mà thứ tự duyệt ảnh hưởng tới thứ tự gọi `sim.After` hoặc `sim.Rand` — thứ tự duyệt map trong Go là ngẫu nhiên và sẽ phá vỡ tính deterministic. Luôn duyệt slice đã sắp xếp (`n.peers`, `c.ids`).

Khi chạy `go run` từ thư mục gốc repo, asset được phục vụ thẳng từ đĩa — sửa HTML/CSS/JS chỉ cần F5. Binary đã build thì dùng bản embed.

## Kiểm chứng

```bash
go test ./internal/raft/
```

| Test | Kiểm tra gì |
|------|-------------|
| `TestDeterministic` | Cùng seed cho ra trace giống hệt nhau |
| `TestSafetyUnderFaults` | 400 lượt fault injection ngẫu nhiên (crash/recover/partition/write), kiểm tra Election Safety + Log Matching + State Machine Safety sau mỗi bước |
| `TestNoSplitBrain` | Quét toàn trace: không bao giờ có 2 node cùng là leader của cùng một term |
| `TestAckedWritesSurvive` | Write đã ack cho client không bao giờ biến mất khỏi log — Leader Completeness |

Tất cả đang pass.

## Kết quả thí nghiệm §4.1

5 node, election timeout ∈ [150, 150+jitter)ms, 200 seed mỗi ô. Cột "hội tụ" là số lượt bầu được leader trong thời gian mô phỏng.

**Độ trễ mạng ≤8ms/chiều** (broadcastTime ≈ 16ms, xấp xỉ thiết lập của paper):

| jitter | hội tụ (khởi động lạnh) | trung vị | xấu nhất | hội tụ (giết leader) | trung vị | xấu nhất |
|--------|------------------------|----------|----------|---------------------|----------|----------|
| 0ms | 0/200 | — | — | 0/200 | — | — |
| 5ms | 200/200 | 1981ms | 8815ms | 186/200 | 2571ms | 8648ms |
| 25ms | 200/200 | 170ms | 657ms | 200/200 | 162ms | 961ms |
| 50ms | 200/200 | 173ms | 366ms | 200/200 | 156ms | 362ms |
| 150ms | 200/200 | 185ms | **287ms** | 200/200 | 172ms | **315ms** |
| 300ms | 200/200 | 210ms | 377ms | 200/200 | 199ms | 465ms |

Kết luận định tính khớp hoàn toàn với paper: không randomize thì **không bao giờ** hội tụ; jitter quá nhỏ thì tệ hại; từ 25ms trở lên là tốt; và khoảng khuyến nghị **150–300ms cho worst-case thấp nhất** — đúng như §4.1 nói.

**Cảnh báo khi trích dẫn vào luận văn:** các con số tuyệt đối ở trên **không** trùng với paper và không nên trình bày như thể tái hiện được paper. Cụ thể, paper báo cáo 5ms randomness cho median downtime 287ms, còn mô hình này cho 2571ms. Nguyên nhân: mô hình mạng ở đây chỉ có độ trễ ngẫu nhiên đều, thiếu các nguồn entropy mà máy thật có (clock drift, độ trễ ghi đĩa dao động, scheduler jitter) — chính những thứ đó phá vỡ thế đối xứng giúp phá split vote. Với `-latency 30` (mặc định) khoảng cách còn lớn hơn. Hãy trích dẫn **xu hướng**, không trích dẫn **con số**.

## Phạm vi

Cố ý **không** cài đặt: InstallSnapshot / log compaction, joint consensus membership change, pre-vote, leadership transfer. Xem phần "Phạm vi" trong bản đề xuất.

Đây là **công cụ dạy học**, không phải thư viện Raft dùng được cho production.

## Ghi chú về một quan sát ngoài dự kiến

Ở S4, node bị cô lập (`n5`) liên tục hết election timeout và tăng term của nó lên rất cao. Khi mạng liền lại, term cao đó khiến leader đang hoạt động phải lùi về follower dù nó hoàn toàn khoẻ mạnh — cluster bị gián đoạn vô cớ.

Đây chính là vấn đề mà **pre-vote extension** giải quyết, mục §8.2 của tài liệu lý thuyết có ghi là gap còn lại ("Paper không đề cập"). Demo tình cờ tái hiện được nó, và đây là một điểm đáng viết vào luận văn.
