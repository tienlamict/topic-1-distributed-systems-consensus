# Kịch bản demo Raft — mô tả chi tiết

> **Tài liệu này dành cho ai:** người cần hiểu demo mà **chưa biết gì về Raft**. Mọi thuật ngữ đều được giải thích trước khi dùng.
>
> **Liên quan:**
> - Lý thuyết gốc: [`topic-1-distributed-systems-consensus_v1.2.md`](topic-1-distributed-systems-consensus_v1.2.md) — mọi ký hiệu §x.y trong tài liệu này trỏ về đó
> - Cách chạy demo: [`../README.md`](../README.md)
> - Kế hoạch tổng thể: [`demo-proposal_v1.md`](demo-proposal_v1.md)
>
> Mọi con số thời gian trong tài liệu này là **số thật**, lấy từ demo chạy với `seed = 42`. Bạn mở demo, gõ seed 42, sẽ thấy đúng từng millisecond như mô tả.

---

## 1. Raft trong 5 phút — cho người chưa biết gì

### 1.1. Bài toán cần giải

Bạn có một cơ sở dữ liệu. Nếu chỉ chạy trên **một máy**, máy đó chết là mất tất cả. Nên ta chạy trên **nhiều máy**, mỗi máy giữ một bản sao.

Nhưng ngay lập tức nảy sinh vấn đề: làm sao để các bản sao **giống hệt nhau**, kể cả khi có máy chết giữa chừng, mạng đứt, gói tin đến muộn hoặc đến sai thứ tự?

Đó chính là bài toán **consensus** (đồng thuận). Định nghĩa hình thức ở §1.1: *nhiều server phải đồng ý về một giá trị; một khi đã quyết thì là chung cuộc.*

### 1.2. Hình dung bằng cuốn sổ ghi chép

Cách hình dung dễ nhất — cũng chính là mô hình **replicated state machine** ở §1.3:

> Mỗi máy giữ một **cuốn sổ**. Mỗi dòng trong sổ là một **lệnh** làm thay đổi dữ liệu, ví dụ `set x=3`.
>
> Nếu ta đảm bảo được cả 5 cuốn sổ có **cùng nội dung theo cùng thứ tự**, thì khi mỗi máy đọc sổ và thi hành lần lượt từng lệnh, cả 5 máy sẽ kết thúc ở **cùng một trạng thái dữ liệu**.
>
> Vậy bài toán "giữ 5 cơ sở dữ liệu giống nhau" quy về bài toán đơn giản hơn nhiều: **giữ 5 cuốn sổ giống hệt nhau**.

Cuốn sổ đó, trong thuật ngữ Raft, gọi là **log**. Mỗi dòng gọi là một **log entry**. Số thứ tự dòng gọi là **index**.

Điểm mấu chốt khiến mẹo này hoạt động: máy tính chỉ cần **lặp lại đúng chuỗi lệnh** là ra đúng kết quả — không cần đồng bộ trực tiếp dữ liệu. Đây là lý do toàn bộ Raft xoay quanh việc quản lý cuốn sổ, chứ không quản lý dữ liệu.

### 1.3. Ba vai trong Raft

Tại mỗi thời điểm, mỗi máy đóng đúng **một trong ba vai** (§2.1):

| Vai | Tiếng Việt | Làm gì |
|-----|-----------|--------|
| **Follower** | Thành viên thường | Thụ động. Chỉ ngồi nghe và trả lời. Không tự quyết gì cả. |
| **Candidate** | Ứng cử viên | Trạng thái tạm thời khi đang đi xin phiếu để làm leader. |
| **Leader** | Người lãnh đạo | Nhận mọi yêu cầu từ client, ghi vào sổ của mình, rồi bắt các máy khác chép theo. |

Ở trạng thái bình thường: **đúng 1 leader, còn lại đều là follower**.

Vì sao phải có leader? Vì nó biến bài toán "5 máy cãi nhau xem ghi gì" thành "1 máy quyết, 4 máy chép theo" — đơn giản hơn rất nhiều. Paper gọi đây là đặc trưng **strong leader** của Raft (§1.4): *log entries chỉ chảy một chiều, từ leader sang các máy khác.*

### 1.4. Nhiệm kỳ (term)

Raft chia thời gian thành các **term** — dịch sát nghĩa là **nhiệm kỳ**, giống nhiệm kỳ tổng thống (§2.1).

- Mỗi nhiệm kỳ bắt đầu bằng một cuộc **bầu chọn** (election).
- Ai thắng thì làm leader đến hết nhiệm kỳ đó.
- Nếu bầu mà không ai thắng, nhiệm kỳ kết thúc **không có leader**, và một nhiệm kỳ mới bắt đầu với cuộc bầu mới.
- Term đánh số tăng dần: 1, 2, 3, …

Term có một công dụng thứ hai, tinh tế hơn: nó là **đồng hồ logic**. Khi hai máy nói chuyện, máy nào có term nhỏ hơn thì biết ngay thông tin của mình đã **cũ** (stale) và phải cập nhật theo. Một leader bị cô lập rồi quay lại, thấy term của mình nhỏ hơn của người khác, sẽ **tự động lùi về làm follower** — không cần ai ra lệnh, không cần một tin nhắn "anh bị phế truất" nào cả.

Bạn sẽ thấy cơ chế này hoạt động rất rõ ở **S5**.

### 1.5. Đa số (majority / quorum)

Đây là ý tưởng nền của toàn bộ Raft.

Với cụm 5 máy, **đa số = 3**. Mọi quyết định quan trọng đều cần ít nhất 3 máy đồng ý:
- Muốn làm leader → cần ít nhất 3 phiếu.
- Muốn ghi nhận một lệnh là "chắc chắn" → cần ít nhất 3 máy đã lưu lệnh đó.

Vì sao con số "đa số" lại thần kỳ? Vì **hai nhóm đa số bất kỳ luôn có ít nhất một thành viên chung**. Trong cụm 5 máy, hai nhóm 3 máy bất kỳ chắc chắn giao nhau. Người ở giao điểm đó đóng vai "nhân chứng" — nó biết chuyện gì đã xảy ra ở nhóm trước, nên không thể có hai quyết định mâu thuẫn cùng được thông qua.

Hệ quả trực tiếp: cụm 5 máy **chịu được 2 máy chết**. Nếu chết quá 2, hệ thống **dừng lại** chứ không bao giờ trả kết quả sai (§1.1).

> Đây là đánh đổi cốt lõi của Raft: thà **ngừng phục vụ** còn hơn **trả lời sai**. Nhớ điểm này — khi so sánh với Redis Cluster ở nhánh sau, Redis chọn ngược lại (§5.4).

### 1.6. Hai loại tin nhắn (RPC)

**RPC** = Remote Procedure Call = "gọi hàm trên máy khác qua mạng". Hiểu đơn giản: **một tin nhắn có cấu trúc, gửi đi và chờ trả lời**.

Raft cơ bản chỉ cần **hai loại tin nhắn** (§3.1). Toàn bộ demo này chỉ dùng hai loại đó:

| Tin nhắn | Ai gửi | Nghĩa nôm na |
|----------|--------|--------------|
| **RequestVote** | Candidate | *"Bầu cho tôi làm leader nhé?"* |
| **AppendEntries** | Leader | *"Chép mấy dòng này vào sổ đi."* — và khi không có dòng nào để chép, nó thành *"Tôi còn sống đây."* |

Chi tiết đầy đủ các trường của hai RPC này nằm ở §3.1.1 và §3.1.2. Trong demo, bấm vào một chấm tin nhắn đang bay sẽ thấy đúng các trường đó.

### 1.7. Nhịp tim (heartbeat) và đồng hồ đếm ngược

Leader liên tục gửi **AppendEntries rỗng** (không có dòng nào để chép) cho mọi follower, trong demo là mỗi 50ms. Đó gọi là **heartbeat** — nhịp tim. Ý nghĩa: *"Tôi vẫn sống, đừng đảo chính."*

Mỗi follower chạy một **đồng hồ đếm ngược** gọi là **election timeout**. Cứ nhận được heartbeat là đồng hồ được đặt lại về đầu. Nếu đồng hồ chạy hết mà vẫn im lặng → follower kết luận leader đã chết → tự ứng cử.

Đây chính là toàn bộ cơ chế **phát hiện hỏng hóc** của Raft (§4.1): không có tin nhắn "báo tử" nào cả, chỉ đơn giản là *im lặng quá lâu thì coi như chết*.

> Trong demo, đồng hồ đếm ngược này chính là **vòng cung chạy quanh mỗi node**. Nhìn nó là hiểu ngay, không cần giải thích thêm.

---

## 2. Từ điển thuật ngữ

Tra cứu khi gặp từ lạ. Cột cuối trỏ về mục lý thuyết tương ứng.

### 2.1. Khái niệm nền

| Thuật ngữ | Giải thích | § |
|-----------|-----------|---|
| **node** / **server** | Một máy chủ trong cụm. Demo đặt tên `n1`…`n5`. | — |
| **cluster** | Cụm máy chủ hoạt động như một khối thống nhất. | §1.1 |
| **client** | Bên ngoài gửi yêu cầu vào (ứng dụng của bạn). Trong demo hiện ở event log dạng "client → n1". | — |
| **consensus** | Đồng thuận: nhiều máy cùng thống nhất một giá trị, đã quyết là chung cuộc. | §1.1 |
| **state machine** | "Máy trạng thái" — phần dữ liệu thật cần bảo vệ. Đặc tính bắt buộc: **cùng đầu vào cho cùng đầu ra**, không có yếu tố ngẫu nhiên. Nhờ vậy chép cùng chuỗi lệnh là ra cùng kết quả. | §1.3 |
| **replicated state machine** | Mô hình: nhiều máy cùng chạy một state machine, đồng bộ nhau qua log. | §1.3 |
| **replicate** | Nhân bản — chép một thứ sang máy khác. | §1.3 |
| **majority** / **quorum** | Đa số. Cụm N máy thì đa số là ⌊N/2⌋+1. Cụm 5 thì là 3. | §1.1 |
| **fault tolerance** | Khả năng chịu lỗi. Cụm 5 máy chịu được 2 máy chết. | §1.2 |
| **safety** | Tính an toàn: **không bao giờ trả kết quả sai**, trong mọi điều kiện mạng. | §1.2 |
| **availability** | Tính sẵn sàng: hệ thống còn phục vụ được. Raft ưu tiên safety hơn availability. | §1.2 |

### 2.2. Cuốn sổ (log)

| Thuật ngữ | Giải thích | § |
|-----------|-----------|---|
| **log** | Cuốn sổ ghi lệnh. **Không phải** log ghi lỗi của lập trình viên. | §1.3 |
| **entry** | Một dòng trong sổ. Gồm: số nhiệm kỳ + nội dung lệnh. | §1.3 |
| **index** | Số thứ tự dòng, đếm từ 1. | §3.1.2 |
| **command** | Nội dung lệnh, ví dụ `set x=3`. | §1.3 |
| **append** | Ghi thêm một dòng vào cuối sổ. | §2.2 |
| **committed** | **Từ quan trọng nhất trong cả tài liệu.** Một dòng được coi là "committed" khi leader thấy **đa số** máy đã lưu nó. Đã committed thì **vĩnh viễn không bao giờ mất, không bao giờ đổi**. Trong demo: ô **màu xanh**. | §2.2 |
| **uncommitted** | Đã ghi vào sổ nhưng chưa đủ đa số xác nhận. Có thể bị xoá bất cứ lúc nào. Trong demo: ô **màu xám**. | §2.2 |
| **apply** | Thực sự thi hành lệnh lên dữ liệu. Chỉ làm sau khi đã committed. | §2.2 |
| **commitIndex** | Máy này đã committed tới dòng số mấy. | §3.1.2 |
| **truncate** | **Cắt bỏ** — xoá các dòng cuối sổ vì chúng mâu thuẫn với leader. Chỉ xảy ra với dòng **chưa** committed. | §3.1.2 |

### 2.3. Bầu cử

| Thuật ngữ | Giải thích | § |
|-----------|-----------|---|
| **term** | **Nhiệm kỳ.** Số nguyên tăng dần, mỗi nhiệm kỳ tối đa 1 leader. | §2.1 |
| **election** | **Cuộc bầu chọn** leader mới. | §2.1 |
| **vote** | Lá phiếu. Mỗi máy chỉ được bỏ **đúng 1 phiếu mỗi nhiệm kỳ**. | §3.1.1 |
| **votedFor** | Nhiệm kỳ này tôi đã bầu cho ai. Ghi xuống đĩa để sau khi khởi động lại vẫn không bầu hai lần. | §3.1.1 |
| **election timeout** | Đồng hồ đếm ngược. Hết giờ mà không nghe tin leader thì tự ứng cử. Demo dùng 150–300ms — đúng khoảng paper khuyến nghị. | §4.1 |
| **heartbeat** | Nhịp tim — AppendEntries rỗng, leader gửi mỗi 50ms để giữ ghế. | §4.1 |
| **randomized timeout** | **Mỗi máy có một thời gian đếm ngược NGẪU NHIÊN khác nhau.** Đây là phát minh then chốt của Raft. Xem S2 để hiểu vì sao nó quan trọng đến thế. | §4.1 |
| **jitter** | Biên độ ngẫu nhiên đó. Jitter = 150 nghĩa là timeout rơi vào khoảng [150, 300)ms. | §4.1 |
| **split vote** | **Chia phiếu.** Nhiều máy cùng ứng cử một lúc, phiếu bị xé lẻ, không ai đủ đa số, nhiệm kỳ trôi qua vô ích. | §2.1 |
| **stale** | Cũ / lỗi thời. Một "stale leader" là leader đã bị thay mà chưa biết. | §2.1 |
| **up-to-date** | "Đủ mới". Dùng để so hai cuốn sổ: so nhiệm kỳ của dòng cuối trước; nếu bằng nhau thì sổ nào dài hơn thắng. | §2.2 |
| **Election Restriction** | Luật: chỉ máy có sổ "đủ mới" mới được làm leader. Xem S4. | §2.2 |

### 2.4. Tin nhắn

| Thuật ngữ | Giải thích | § |
|-----------|-----------|---|
| **RPC** | Tin nhắn có cấu trúc gửi qua mạng, có gửi và có trả lời - Remote protocol call. | §3.1 |
| **RequestVote** | *"Bầu cho tôi?"* Kèm nhiệm kỳ và thông tin dòng cuối trong sổ của mình. | §3.1.1 |
| **AppendEntries** | *"Chép mấy dòng này."* Rỗng thì thành heartbeat. | §3.1.2 |
| **prevLogIndex / prevLogTerm** | Leader nói: *"Trước mấy dòng tôi gửi, sổ anh phải có đúng dòng số X thuộc nhiệm kỳ Y."* Nếu follower không khớp thì từ chối — đây là cách Raft phát hiện hai cuốn sổ đã lệch nhau. | §3.1.2 |
| **nextIndex** | Leader ghi nhớ: với mỗi follower, dòng kế tiếp cần gửi là dòng mấy. Bị từ chối thì lùi lại một dòng rồi thử lại. | §3.1.2 |
| **matchIndex** | Leader ghi nhớ: follower này chắc chắn đã chép tới dòng mấy. Dùng để đếm xem đã đủ đa số chưa. | §3.1.2 |

### 2.5. Sự cố

| Thuật ngữ | Giải thích | § |
|-----------|-----------|---|
| **crash** | Máy chết đột ngột. | §1.2 |
| **recover** | Máy sống lại. Trong Raft, nó **giữ nguyên** nhiệm kỳ, lá phiếu và cuốn sổ cũ nhờ đã ghi xuống đĩa (*stable storage*). | §1.2 |
| **network partition** | **Chia cắt mạng.** Mạng vỡ làm hai (hoặc nhiều) mảnh; các máy trong cùng mảnh nói chuyện được với nhau nhưng không nói được sang mảnh kia. Máy nào cũng khoẻ — chỉ có mạng hỏng. Đây là tình huống khó nhất trong hệ phân tán. | §1.2 |
| **split brain** | **Não chia đôi.** Hệ thống vỡ đôi và **cả hai nửa cùng tưởng mình là chính thống**. Ác mộng của mọi hệ phân tán. Raft chống bằng luật đa số — xem S5. | §4.3 |
| **broadcastTime** | Thời gian để một máy gửi tin cho tất cả và nhận đủ hồi đáp. | §4.1 |
| **MTBF** | Thời gian trung bình giữa hai lần hỏng của một máy. | §4.1 |

### 2.6. Riêng của demo

| Thuật ngữ | Giải thích |
|-----------|-----------|
| **seed** | Số khởi tạo bộ sinh ngẫu nhiên. Cùng seed cho kết quả y hệt từng millisecond. **Giải thích đầy đủ ở mục 2.7 ngay dưới đây.** |
| **deterministic** | Tính chất "chạy lại cho ra đúng kết quả cũ". |
| **thời gian ảo** | Demo không dùng đồng hồ thật. 12 giây mô phỏng tính xong trong vài ms rồi phát lại. Vì thế mới tua nhanh / tua chậm / tua ngược được. |
| **trace** | Toàn bộ nhật ký sự kiện của một lượt chạy. Server tính xong gửi cả cục sang trình duyệt. |

### 2.7. Seed và tính tái lập

> **Nói ngay để khỏi hiểu nhầm: seed KHÔNG phải khái niệm của Raft.** Raft thật không có thứ gì tên là seed. Đây thuần tuý là công cụ của **mô phỏng**, sinh ra để ta có thể "tua lại đúng một vũ trụ" phục vụ việc dạy học và kiểm chứng.

#### Seed là gì

Máy tính **không có ngẫu nhiên thật**. Cái ta gọi là "số ngẫu nhiên" thực chất là một dãy số do một công thức toán sinh ra — gọi là *pseudo-random* (giả ngẫu nhiên). Công thức đó cần một giá trị khởi đầu, và **giá trị khởi đầu đó chính là seed** (nghĩa đen: hạt giống).

Đặc tính then chốt: **cùng một seed thì công thức sinh ra đúng cùng một dãy số, theo đúng thứ tự đó.**

```
seed = 42  →  175, 194, 213, 156, 290, 22, 18, 27, ...   (luôn luôn là dãy này)
seed = 7   →  174, 231, 168, 205, 187, 15, 29, 11, ...   (luôn luôn là dãy này)
```

Dãy số nhìn thì "lộn xộn như ngẫu nhiên", nhưng nó hoàn toàn xác định — chạy lại bao nhiêu lần cũng ra y nguyên.

#### Trong demo này, những gì lấy số từ dãy đó

Chỉ có **hai nguồn**:

| Dùng số ngẫu nhiên để làm gì | Khoảng giá trị | Ảnh hưởng tới điều gì |
|------------------------------|----------------|----------------------|
| **Election timeout của mỗi node** — rút một số mới mỗi lần đặt lại đồng hồ đếm ngược | [150, 300)ms | Node nào hết giờ trước, tức node nào được ứng cử trước |
| **Độ trễ của mỗi tin nhắn** — rút một số mỗi lần gửi | 10–30ms | Phiếu bầu của ai về tới trước, heartbeat tới lúc nào |

Lưu ý: demo **không** bật tính năng rớt gói ngẫu nhiên. Những tin nhắn bạn thấy biến mất giữa đường đều có nguyên nhân rõ ràng — mạng bị cắt hoặc node nhận đã chết — chứ không phải xui rủi.

#### Bằng chứng — chạy thật

Chạy kịch bản S1 nhiều lần với các seed khác nhau:

| Seed | Node hết giờ trước | Thời hạn rút được | Ai thắng | Xong lúc |
|------|-------------------|-------------------|----------|----------|
| **42** | `n1` | 175ms | `n1` | **218ms** |
| **7** | `n2` | 174ms | `n2` | **207ms** |
| **99** | `n1` (191ms), rồi `n2` (207ms) cùng ứng cử | 191ms | `n1` | **221ms** |

Chạy lại seed 42 lần thứ hai, thứ ba, thứ một trăm: vẫn **đúng 218ms**. Chạy lại seed 7: vẫn **đúng 207ms**.

Trường hợp seed 99 đáng chú ý — có **hai** node cùng ứng cử vì thời hạn của chúng gần nhau (191ms và 207ms), suýt nữa thành split vote như S2, nhưng `n1` vẫn kịp gom đủ phiếu. Đây là ví dụ tốt cho thấy randomized timeout không phải phép màu tuyệt đối, nó chỉ làm cho split vote trở nên **hiếm**.

#### Trong mã nguồn

Cả mô phỏng dùng **đúng một** bộ sinh số duy nhất, tạo ở `internal/sim/sim.go`:

```go
s := &Sim{rng: rand.New(rand.NewSource(seed))}
```

Mọi chỗ cần ngẫu nhiên đều phải gọi qua `s.Rand()`. Không có nguồn ngẫu nhiên nào khác, không dùng đồng hồ thật, không dùng goroutine. Nhờ vậy **toàn bộ một lượt chạy là một hàm thuần tuý của seed**.

#### Vì sao lại cần thứ này — ba lý do

**1. Để tài liệu nói được số cụ thể.**
Nếu mỗi lần chạy ra một kết quả khác, tôi đã không thể viết *"ở mốc 218ms `n1` thắng cử"* trong tài liệu này — bạn mở lên sẽ thấy số khác và tưởng tôi bịa. Có seed thì mọi mốc thời gian đều kiểm chứng được.

**2. Để trình bày không bị hớ.**
Bạn tập trước với seed 42, đến hôm bảo vệ gõ lại seed 42, diễn biến y hệt từng millisecond. Không có chuyện "hôm qua chạy đẹp mà hôm nay lại rơi vào tình huống khác".

**3. Để test bắt được lỗi.**
Bộ test chạy 400 lượt với 400 seed khác nhau. Khi một lượt phát hiện vi phạm safety, nó in ra số seed — và ta tái hiện lại chính xác lỗi đó bằng cách chạy lại với seed ấy. Không có tính tái lập thì gặp lỗi ngẫu nhiên coi như bó tay.

#### Một hệ quả tinh tế — vì sao code có luật "không được duyệt map"

Vì tất cả chỉ dùng chung một bộ sinh số, **thứ tự gọi phải cố định tuyệt đối**.

Trong Go, thứ tự duyệt một `map` là **ngẫu nhiên theo thiết kế** — cố ý làm vậy để lập trình viên không lỡ phụ thuộc vào thứ tự. Nếu ở đâu đó code duyệt map để gửi tin nhắn, thì thứ tự rút số sẽ khác nhau giữa các lần chạy, và tính tái lập vỡ ngay lập tức.

Đó là lý do có dòng cảnh báo in đậm ở đầu package `sim` và trong README: **luôn duyệt slice đã sắp xếp** (`n.peers`, `c.ids`), không bao giờ duyệt map ở chỗ ảnh hưởng tới thứ tự gọi. Test `TestDeterministic` tồn tại chính là để canh chừng việc này — nó chạy mỗi kịch bản hai lần và so sánh trace từng byte.

#### Cách dùng khi trình bày

- **Cố định seed 42** cho các buổi trình bày chính thức, vì mọi mốc thời gian trong tài liệu này đều theo seed đó.
- **Đổi seed vài lần** khi muốn chứng minh rằng kết quả không phải do dàn xếp: thay đổi node thắng, thay đổi thời gian, nhưng **lần nào cũng có đúng một leader**. Đó là điểm cần thấy — kết quả ngẫu nhiên, nhưng tính đúng đắn thì không.
- Nếu ai hỏi seed là gì, câu trả lời gọn: *"Số để tái lập chính xác một lượt chạy. Hệ thống thật không có nó."*

- Seed database: Khởi tạo data mẫu (do ban đầu bảng chưa có dữ liệu gì)
- Config seed nodes: Khai báo node ban đầu để một node có thể thể tìm và tham gia và Cluster
- Set random seed: Đặt giá trị khởi tạo cho bộ sinh số ngẫu nhiên để kết quả có thể lặp lại.
---

## 3. Cách đọc màn hình demo

### 3.1. Vòng tròn node ở giữa

```
                 term 2 · vote→n2      ← nhiệm kỳ hiện tại + đã bầu cho ai
                ╭───────────────╮
                │  ◜◝           │      ← VÒNG CUNG = đồng hồ đếm ngược
                │ ╭───────────╮ │        election timeout đang chạy.
                │ │    n2     │ │        Chạy hết vòng là node tự ứng cử.
                │ │  LEADER   │ │        Chuyển ĐỎ khi sắp hết giờ.
                │ ╰───────────╯ │        Leader KHÔNG có vòng này —
                ╰───────────────╯        nó không đếm ngược, nó phát heartbeat.
                   ▣ ▣ ▣ ▤ ▤            ← CUỐN SỔ, mỗi ô là một dòng
                   1 1 2 2 2               xanh = committed, xám = chưa
                                           số trong ô = nhiệm kỳ của dòng đó
```

**Màu của node:**

| Màu | Vai |
|-----|-----|
| Xám xanh | Follower |
| Vàng cam | Candidate — đang đi xin phiếu |
| Xanh lá | Leader |
| Đỏ sẫm, có dấu ✕ | Đã chết |

### 3.2. Tin nhắn đang bay

Các chấm tròn di chuyển dọc theo đường nối giữa hai node. Màu cho biết loại:

| Màu | Nhãn | Nghĩa |
|-----|------|-------|
| Xanh dương | `RV` | RequestVote — *"bầu cho tôi?"* |
| Tím | `RV↩` | Trả lời phiếu bầu |
| Xanh lá | `AE` | AppendEntries — *"chép dòng này"* hoặc heartbeat |
| Xanh ngọc | `AE↩` | Trả lời AppendEntries |

Chấm **mờ dần rồi biến mất giữa đường** = tin nhắn bị mất (do mạng đứt hoặc rớt gói).

### 3.3. Khi mạng bị chia cắt

Xuất hiện dòng chữ đỏ **✂ NETWORK PARTITION** ở đỉnh màn hình. Các đường nối giữa hai mảnh chuyển thành **nét đứt đỏ**, và mỗi mảnh được viền một màu riêng.

### 3.4. Các panel khác

| Vùng | Nội dung |
|------|----------|
| **Dải chú thích trên cùng** | Câu giải thích cho đúng khoảnh khắc đang xem. Đây là phần "dạy học" — đọc theo khi trình bày. |
| **Timeline dưới đáy** | Một hàng cho mỗi node, tô màu theo vai qua thời gian. Vạch dọc đỏ/xanh là các mốc sự cố. **Bấm vào để nhảy tới thời điểm bất kỳ.** |
| **Node inspector (phải trên)** | Bấm vào một node để xem toàn bộ trạng thái: nhiệm kỳ, đã bầu cho ai, commitIndex, và từng dòng trong sổ. |
| **Metrics (phải giữa)** | Số liệu tới thời điểm hiện tại + tổng kết cả lượt chạy. |
| **Event log (phải dưới)** | Nhật ký từng sự kiện kèm mốc thời gian. |

### 3.5. Điều khiển

| Phím / thao tác | Tác dụng |
|-----------------|----------|
| `Space` | Chạy / tạm dừng |
| `→` | Nhảy tới **sự kiện đáng chú ý** kế tiếp (không phải nhảy theo bước thời gian cố định) |
| `←` | Lùi 100ms |
| Bấm timeline | Nhảy tới thời điểm bất kỳ |
| Bấm node | Mở inspector của node đó |
| Ô **Seed** | Đổi số để ra một "vũ trụ" khác. Cùng seed luôn cho cùng kết quả. |
| Thanh trượt tham số | Chạy lại ngay với giá trị mới |

> **Mẹo khi thuyết trình:** đặt tốc độ **×0.25** cho đoạn bầu cử — nó diễn ra trong khoảng 40ms, ở tốc độ thường sẽ trôi qua trước khi bạn kịp nói xong một câu. Rồi chuyển ×2 cho các đoạn chờ.

---

# PHẦN II — CÁC KỊCH BẢN

---

## S1 — Leader election cơ bản

**Trả lời câu hỏi:** *Một cụm máy vừa bật lên, chưa ai quen ai. Làm sao chúng chọn được sếp mà không cần con người can thiệp? Và khi sếp chết thì sao?*

**Liên hệ lý thuyết:** §2.2 (Leader Election, 5 bước), §4.1 (Failure Detection qua heartbeat timeout)

### Thiết lập
5 node, chạy 4 giây, tham số mặc định (election timeout 150–300ms, heartbeat 50ms, độ trễ mạng 10–30ms).

### Diễn biến (seed 42)

| Mốc | Chuyện gì xảy ra | Ứng với lý thuyết |
|-----|------------------|-------------------|
| **0ms** | Cả 5 node ở trạng thái Follower. Mỗi node bắt đầu đếm ngược với một thời hạn **ngẫu nhiên khác nhau**. Nhìn 5 vòng cung chạy với tốc độ khác nhau. | §2.2 bước 1 |
| **175ms** | `n1` hết giờ trước tiên (đúng 175ms). Nó **tăng nhiệm kỳ lên 1**, chuyển thành **Candidate** (vàng cam), **tự bỏ phiếu cho mình**, rồi bắn 4 tin `RequestVote` đi 4 hướng. | §2.2 bước 3–4 |
| **190–203ms** | 4 node kia nhận được. Thấy nhiệm kỳ 1 **cao hơn** nhiệm kỳ 0 của mình, chúng cập nhật theo và bỏ phiếu cho `n1`. | §2.1 (term = đồng hồ logic) |
| **218ms** | `n1` gom đủ **3/5 phiếu** (gồm cả phiếu của chính nó) → **thành Leader** (xanh lá). Vòng cung quanh nó biến mất — leader không đếm ngược nữa. | §2.2 bước 5(a) |
| **218ms trở đi** | `n1` phát heartbeat mỗi 50ms. Nhìn các chấm `AE` xanh lá toả ra liên tục. Vòng cung của 4 follower **liên tục bị reset về đầu** mỗi khi heartbeat tới — đó chính là cách leader giữ ghế. | §4.1 |
| **2000ms** | **Giết `n1`.** Nó chuyển đỏ, có dấu ✕. Heartbeat ngừng bặt. | — |
| **2000–2190ms** | Vòng cung của 4 node còn lại **không được reset nữa** nên chạy dần về hết. Đây là khoảnh khắc đáng dừng lại nhất trong S1. | §4.1 |
| **2190ms** | `n5` hết giờ trước — đồng hồ của nó được đặt 194ms từ lần heartbeat cuối — nên chuyển Candidate, **nhiệm kỳ 2**. | §2.2 bước 3 |
| **2220ms** | `n5` thắng với 3/5 phiếu → Leader mới. Tổng thời gian cụm không có sếp: **220ms**. | §2.2 |

### Điểm cần nhấn khi trình bày

1. **Không có ai điều phối cả.** Không có máy chủ trung tâm nào chỉ định leader. Cụm tự tổ chức, chỉ bằng đồng hồ đếm ngược và phiếu bầu.
2. **Nhiệm kỳ tăng từ 1 lên 2.** Đây là bằng chứng trực quan cho §2.1: mỗi lần bầu là một nhiệm kỳ mới. Và Raft đảm bảo **tối đa 1 leader trong một nhiệm kỳ** — tính chất **Election Safety** ở §5.2.
3. **220ms để phục hồi.** Đối chiếu với §4.1: paper khuyến nghị election timeout 150–300ms chính là để đạt con số cỡ này.

### Thí nghiệm tự làm
Đổi seed sang 7, 13, 99. Bạn sẽ thấy **node thắng cử thay đổi**, và **thời gian bầu cũng thay đổi** — nhưng lần nào cũng có đúng một leader. Đó là điểm cần thấy: kết quả ngẫu nhiên, nhưng tính đúng đắn thì không.

---

## S2 — Split vote: vì sao phải ngẫu nhiên hoá

> **Đây là kịch bản có giá trị cao nhất trong cả demo.** Nó chứng minh trực tiếp một claim trong §4.1 mà nếu chỉ đọc chữ thì rất khó tin.

**Trả lời câu hỏi:** *Cái "randomized election timeout" mà paper khen là thiết kế thiên tài — bỏ đi thì sao? Chẳng lẽ chỉ vì thiếu một chút ngẫu nhiên mà cả hệ thống sập?*

**Liên hệ lý thuyết:** §2.1 (split vote), §4.1 (Randomized Election Timeout + số đo của paper §9.3)

### Vấn đề, nói bằng lời

Hãy tưởng tượng 5 người trong phòng họp, tất cả đều được dặn: *"Chờ đúng 150 giây. Nếu chưa có ai làm chủ toạ thì tự ứng cử."*

Vì cả 5 người đếm **cùng một khoảng thời gian**, họ sẽ đứng dậy ứng cử **cùng một lúc**. Mỗi người tự bỏ phiếu cho mình. Kết quả: 5 ứng viên, mỗi người 1 phiếu, **không ai đủ 3 phiếu**.

Thất bại. Cả 5 lại ngồi xuống, lại đếm 150 giây, lại cùng đứng dậy, lại chia phiếu. **Vòng lặp vô tận.**

Đó là **split vote** — chia phiếu.

Cách chữa của Raft đơn giản đến bất ngờ: **cho mỗi người một khoảng chờ ngẫu nhiên khác nhau**. Người nào chờ ngắn nhất sẽ đứng dậy trước, xin phiếu trước, và thắng trước khi người khác kịp đứng lên.

### Ba tham số chỉnh được

| Tham số | Ý nghĩa |
|---------|---------|
| **jitter** | Biên độ ngẫu nhiên (ms). 0 = tắt hoàn toàn, mọi node dùng chung 150ms. |
| **mode** | 0 = khởi động lạnh (5 node bật cùng lúc). 1 = giết leader của cụm đang chạy — **đúng thiết lập của paper §9.3**. |
| **maxLatency** | Độ trễ mạng tối đa. Quyết định `broadcastTime` trong bất đẳng thức §4.1. |

### Diễn biến với jitter = 0 (seed 42)

| Mốc | Nhiệm kỳ | Có leader chưa? |
|-----|----------|-----------------|
| 2000ms | 13 | Chưa |
| 6000ms | 40 | Chưa |
| 11900ms | **79** | **Chưa. Không bao giờ.** |

Đến hết 12 giây mô phỏng, cụm đã mở **395 cuộc bầu cử**, đốt qua **79 nhiệm kỳ**, và **không commit được một dòng nào**. Panel Metrics ghi thẳng: *leader đầu tiên: KHÔNG BAO GIỜ*.

Trên màn hình bạn sẽ thấy cả 5 node **đồng loạt nhấp nháy vàng cam** theo nhịp, đồng loạt reset, đồng loạt lại vàng cam. Một hệ thống hoàn toàn tê liệt trong khi **cả 5 máy đều khoẻ mạnh và mạng hoàn toàn thông suốt**.

### Diễn biến với jitter = 150 (mặc định của Raft thật)

Xong trong **một cuộc bầu duy nhất**, khoảng 220ms.

### Số liệu định lượng

Chạy `go run ./cmd/consensus-demo experiment -runs 200 -latency 8` cho bảng sau (200 seed mỗi ô):

| jitter | Tỉ lệ bầu được leader | Trung vị | Xấu nhất |
|--------|----------------------|----------|----------|
| 0ms | **0/200** | — | — |
| 5ms | 200/200 | 1981ms | 8815ms |
| 25ms | 200/200 | 170ms | 657ms |
| 50ms | 200/200 | 173ms | 366ms |
| **150ms** | 200/200 | 185ms | **287ms** |
| 300ms | 200/200 | 210ms | 377ms |

Đọc bảng này từ trên xuống là thấy trọn vẹn lập luận của §4.1:
- **0ms → không bao giờ hội tụ.**
- **5ms → hội tụ nhưng tệ hại** (trung vị gần 2 giây).
- **Từ 25ms trở lên → tốt.**
- **150ms cho trường hợp xấu nhất thấp nhất** — đúng là lý do paper khuyến nghị khoảng 150–300ms.

### ⚠️ Cảnh báo quan trọng khi trích vào luận văn

Các con số tuyệt đối ở trên **không trùng với paper** và **không được trình bày như thể tái hiện được paper**.

Cụ thể: §4.1 dẫn paper nói *"chỉ 5ms randomness cho median downtime 287ms"*, trong khi mô hình này cho **1981ms** ở cùng mức jitter.

Nguyên nhân đã kiểm chứng: mô hình mạng trong demo chỉ có **độ trễ ngẫu nhiên phân bố đều**, thiếu các nguồn nhiễu mà máy thật có — sai lệch đồng hồ, độ trễ ghi đĩa dao động, độ trễ lập lịch của hệ điều hành. Chính những nhiễu đó phá vỡ thế đối xứng và giúp thoát split vote. Bằng chứng cho giả thuyết này: khi hạ độ trễ mạng từ 30ms xuống 8ms, tỉ lệ hội tụ ở jitter=5ms nhảy từ 42/200 lên 186/200.

**Hãy trích dẫn xu hướng, đừng trích dẫn con số.** Xu hướng thì khớp hoàn toàn; con số thì không.

### Thí nghiệm tự làm
Kéo **maxLatency** lên gần 150ms trong khi vẫn để jitter = 150. Cụm lại rơi vào bầu cử liên miên — lần này không phải vì chia phiếu, mà vì tin nhắn bay chậm hơn thời gian chờ. Đây chính là vế đầu của bất đẳng thức §4.1 bị vi phạm:

```
broadcastTime ≪ electionTimeout ≪ MTBF
```

Nói bằng lời: **tin phải bay nhanh hơn nhiều so với thời gian chờ, và máy phải hỏng thưa hơn nhiều so với thời gian chờ.** Vi phạm vế nào cũng hỏng.

---

## S3 — Log replication: một lệnh ghi đi qua hệ thống như thế nào

**Trả lời câu hỏi:** *Khi ứng dụng gọi `set x=3`, chính xác thì chuyện gì xảy ra? Lúc nào thì được coi là "đã ghi xong"?*

**Liên hệ lý thuyết:** §2.2 (Log Replication, 5 bước), §3.1.2 (AppendEntries RPC)

### Diễn biến (seed 42)

| Mốc | Chuyện gì xảy ra | Bước trong §2.2 |
|-----|------------------|-----------------|
| **218ms** | `n1` thành Leader. Trước lúc này **không thể ghi gì cả** — mọi client request đều phải qua leader. | — |
| **320ms** | Client gửi `set x=3`. `n1` ghi vào sổ của mình ở **dòng 1**. Ô hiện **màu XÁM** — đã ghi nhưng chưa chắc chắn. **Client vẫn đang chờ, chưa nhận được trả lời.** | bước 1 |
| **~325ms** | `n1` bắn `AppendEntries` kèm dòng mới đi 4 hướng. | bước 2 |
| **~340ms** | Follower nhận, ghi vào sổ mình (cũng màu xám), trả lời "đã nhận". | bước 2 |
| **348ms** | `n1` đếm được **3/5** máy đã lưu → **đủ đa số** → tuyên bố dòng này **COMMITTED**. Ô chuyển **XANH**. `n1` thi hành lệnh lên dữ liệu và **giờ mới trả OK cho client**. | bước 3, 5 |
| **~370ms** | Heartbeat kế tiếp mang theo `commitIndex` mới, các follower cũng chuyển ô sang xanh. | bước 3 |

Tổng cộng: **28ms** từ lúc client gửi đến lúc nhận được xác nhận.

Ba lệnh còn lại lặp lại đúng như vậy ở các mốc 620ms, 920ms, 1220ms.

### Điểm cần nhấn

1. **Xám rồi mới xanh — và client chỉ được trả lời khi đã xanh.** Đây là toàn bộ ý nghĩa của từ "committed". Nếu hệ thống trả OK khi ô còn xám thì nó đã nói dối, vì dòng xám vẫn có thể bị xoá.

2. **Đa số, không phải tất cả.** Chỉ cần 3/5 máy lưu là đủ. Hai máy chậm nhất không làm chậm ai. Đây là ý ở §1.2 mục 4: *"minority node chậm không ảnh hưởng performance hệ thống"*. Nếu Raft đòi đủ cả 5 máy, chỉ một máy chậm là kéo tụt cả hệ thống.

3. **Chỉ leader trả lời client.** Follower cũng có đầy đủ dữ liệu nhưng không được phép trả lời. Đây là hệ quả của thiết kế **strong leader** (§1.4).

### Thí nghiệm tự làm
Bấm vào từng node ở các thời điểm khác nhau, xem panel inspector. Bạn sẽ thấy `commitIndex` của các node **lệch nhau vài chục ms** — leader biết trước, follower biết sau. Đây là hình ảnh cụ thể của việc "đồng thuận cần thời gian".

---

## S4 — Election Restriction: không phải ai cũng được làm sếp

**Trả lời câu hỏi:** *Một máy vừa bị cô lập một lúc, sổ của nó đã cũ. Nếu nó quay lại và đòi làm leader thì sao? Nó có xoá mất dữ liệu của mọi người không?*

**Liên hệ lý thuyết:** §2.2 (Safety Restrictions), §5.2 (Leader Completeness, State Machine Safety)

### Vì sao câu hỏi này quan trọng

Trong Raft, **leader là chân lý** — nó bắt mọi người chép theo sổ của nó. Vậy nếu một máy có sổ cũ mà lên làm leader, nó sẽ ép cả cụm xoá đi những dòng đã committed. **Dữ liệu đã hứa chắc chắn với client sẽ biến mất.**

Đây sẽ là một lỗi safety nghiêm trọng. Raft chặn nó bằng một luật bổ sung gọi là **Election Restriction**: *chỉ máy có sổ "đủ mới" (at least as up-to-date) mới được nhận phiếu.*

### Diễn biến (seed 42)

| Mốc | Chuyện gì xảy ra |
|-----|------------------|
| **218ms** | `n1` thành Leader. |
| **220ms** | **Cô lập `n5`** khỏi 4 node còn lại. |
| **370–914ms** | Client ghi 3 lệnh (`set a=1`, `set b=2`, `set c=3`). Cả 3 **committed bình thường** vì phía majority vẫn có 4/5 node. `n5` không hề hay biết. |
| **381→1356ms** | `n5` bị cô lập nên không nhận heartbeat, liên tục hết giờ và tự ứng cử. **Nhiệm kỳ của nó leo từ 2 lên 7** — mà nó vẫn cô đơn một mình, sổ vẫn rỗng. |
| **1420ms** | **Nối lại mạng.** Lúc này: 4 node có sổ 3 dòng ở nhiệm kỳ 1; `n5` có sổ **rỗng** nhưng nhiệm kỳ **7**. |
| **1659–1663ms** | `n5` xin phiếu. **Cả 4 node đều TỪ CHỐI**, với lý do ghi rõ trong event log: <br>`log candidate cũ hơn: (term 0, idx 0) < của tôi (term 1, idx 3)` |
| **1943ms** | `n3` — một node có sổ đầy đủ — ứng cử và **thắng ngay**. |
| **1970ms** | `n5` nhận AppendEntries từ leader mới, lùi về Follower, và **được chép bù** đủ 3 dòng còn thiếu. |

### Điểm cần nhấn — chỗ này tinh tế

Hãy chú ý điều nghịch lý: **`n5` có nhiệm kỳ CAO NHẤT (7 so với 1) nhưng vẫn thua.**

Bốn node kia *có* chấp nhận nhiệm kỳ 7 của nó — chúng cập nhật nhiệm kỳ mình lên 7 và lùi về Follower. Nhưng khi đến phần bỏ phiếu, chúng vẫn **từ chối**.

> **Nhiệm kỳ cao cho bạn quyền được lắng nghe. Sổ đầy đủ mới cho bạn quyền được bầu.**

Cách so hai cuốn sổ (định nghĩa "up-to-date" ở §2.2):
1. So **nhiệm kỳ của dòng cuối** trước — cao hơn thì thắng.
2. Nếu bằng nhau, so **độ dài sổ** — dài hơn thì thắng.

Ở đây `n5` có `(term 0, idx 0)` — sổ rỗng — thua `(term 1, idx 3)` ngay ở bước 1.

Luật này là thứ bảo đảm tính chất **Leader Completeness** ở §5.2: *dòng nào đã committed ở nhiệm kỳ T thì chắc chắn có mặt trong sổ của mọi leader ở nhiệm kỳ cao hơn T.*

### 🔍 Quan sát ngoài dự kiến — nên viết vào luận văn

Nhìn lại mốc **1450ms**: ngay khi mạng vừa liền, `n1` — leader đang khoẻ mạnh, đang phục vụ bình thường — **buộc phải lùi về Follower** chỉ vì `n5` mang về một nhiệm kỳ cao.

Cụm bị gián đoạn **hoàn toàn vô cớ**. Kẻ gây gián đoạn là một node vừa bị cô lập, sổ rỗng, không đóng góp gì, chỉ vì nó ngồi một mình đếm số nhiệm kỳ lên cao.

Đây chính xác là vấn đề mà **pre-vote extension** sinh ra để giải quyết — mục §8.2 của tài liệu lý thuyết ghi đây là gap còn lại (*"Paper không đề cập"*). Ý tưởng của pre-vote: trước khi tăng nhiệm kỳ thật, candidate hỏi thăm trước *"nếu tôi ứng cử thì các anh có bầu không?"*; nếu không ai hưởng ứng thì **không tăng nhiệm kỳ**, nhờ vậy không quấy rối cụm.

Demo tái hiện được vấn đề này một cách tình cờ, và nó là một điểm quan sát tốt để đưa vào phần thảo luận.

---

## S5 — Network partition: hai leader cùng lúc, và vì sao không sao cả

> Đây là kịch bản đắt giá nhất về mặt học thuật, vì nó cho thấy Raft xử lý **split brain** như thế nào.

**Trả lời câu hỏi:** *Mạng vỡ đôi. Cả hai nửa cùng có leader. Liệu dữ liệu có bị hỏng không?*

**Liên hệ lý thuyết:** §2.2, §5.2 (Log Matching, State Machine Safety), §4.3 (split-brain handling)

### Diễn biến (seed 42)

**Giai đoạn 1 — bình thường**

| Mốc | Chuyện gì xảy ra |
|-----|------------------|
| 218ms | `n1` thành Leader (nhiệm kỳ 1). |
| 348ms | `set k=1` committed → dòng 1 XANH trên cả 5 node. |
| 656ms | `set k=2` committed → dòng 2 XANH. |

**Giai đoạn 2 — mạng vỡ**

| Mốc | Chuyện gì xảy ra |
|-----|------------------|
| **920ms** | **PARTITION:** `[n1, n2]` tách khỏi `[n3, n4, n5]`. Leader cũ `n1` nằm ở phía **chỉ có 2/5 node**. |
| 1073ms | Phía 3 node không còn nhận heartbeat, `n5` hết giờ và ứng cử với **nhiệm kỳ 2**. |
| 1118ms | `n5` thắng 3/5 → **Leader mới**. |

**⚠️ Từ 1118ms, cụm có ĐỒNG THỜI hai node tự cho mình là leader:** `n1` (nhiệm kỳ 1) và `n5` (nhiệm kỳ 2).

Đây là **split brain**. Nhưng hãy xem chuyện gì xảy ra tiếp.

**Giai đoạn 3 — hai bên cùng nhận lệnh ghi**

| Mốc | Phía minority (`n1`, `n2`) | Phía majority (`n3`, `n4`, `n5`) |
|-----|---------------------------|----------------------------------|
| 1120ms | Client ghi `set ghost=1` vào dòng 3. Ô **XÁM**. | |
| 1320ms | Client ghi `set ghost=2` vào dòng 4. Ô **XÁM**. | |
| 1763ms | | `set real=1` vào dòng 3 → **COMMITTED, XANH** |
| 2011ms | | `set real=2` vào dòng 4 → **COMMITTED, XANH** |

Hình ảnh trên màn hình lúc này (đọc dấu `#` là xanh/committed, `.` là xám/chưa):

```
n1  # # . .      ← 2 dòng chắc chắn + 2 dòng "ma" mãi mãi xám
n2  # # . .
        ╌╌╌╌ mạng đứt ╌╌╌╌
n3  # # # #      ← 4 dòng đều chắc chắn
n4  # # # #
n5  # # # #
```

**Chìa khoá nằm ở đây:** `n1` vẫn nhận lệnh ghi, vẫn ghi vào sổ, nhưng **chỉ có 2/5 máy lưu được** → không bao giờ đạt đa số → **hai dòng đó vĩnh viễn màu xám**. Và vì chưa xanh nên **client chưa từng nhận được lời xác nhận nào**.

`n1` đã không nói dối. Nó chỉ đơn giản là **không trả lời**.

**Giai đoạn 4 — mạng liền lại**

| Mốc | Chuyện gì xảy ra |
|-----|------------------|
| **4220ms** | Mạng liền. |
| 4229ms | `n2` nhận AppendEntries từ `n5` (nhiệm kỳ 2 > nhiệm kỳ 1 của mình) → lùi về Follower → **CẮT BỎ 2 dòng ghost**, chép 2 dòng real về. |
| 4232ms | `n1` — leader cũ — cũng vậy: thấy nhiệm kỳ cao hơn → **tự lùi về Follower** → **CẮT BỎ 2 dòng ghost**. |

Kết quả cuối: cả 5 node đều có `# # # #` — hai dòng `k=1`, `k=2` từ nhiệm kỳ 1, cộng hai dòng `real=1`, `real=2` từ nhiệm kỳ 2. Hai dòng ghost biến mất không dấu vết.

### Điểm cần nhấn — ba tầng bảo vệ

**1. Split brain có xảy ra, nhưng vô hại.**

Nhiều người tưởng Raft "ngăn không cho có hai leader". Không đúng. Raft **cho phép** hai node cùng tưởng mình là leader — điều đó không tránh được, vì một node bị cô lập không có cách nào biết mình đã bị thay.

Cái Raft đảm bảo là **Election Safety** (§5.2): *tối đa 1 leader trong CÙNG MỘT nhiệm kỳ.* Ở đây `n1` là leader nhiệm kỳ 1, `n5` là leader nhiệm kỳ 2 — **khác nhiệm kỳ, nên không vi phạm gì cả.**

Và leader phía minority là leader **trên danh nghĩa**: nó không commit được gì, nên nó vô hại.

**2. Không có tin nhắn "anh bị phế truất".**

`n1` tự lùi về Follower chỉ vì **nhìn thấy một con số lớn hơn**. Không ai ra lệnh, không có thủ tục bàn giao. Toàn bộ cơ chế nằm ở một câu luật: *thấy term lớn hơn thì cập nhật và lùi về Follower* (§2.1).

Sự đơn giản này chính là điều làm nên tiếng tăm "dễ hiểu" của Raft mà §5.1 nói tới.

**3. Cắt bỏ chỉ đụng vào dòng chưa committed.**

Nhìn kỹ: 2 dòng bị cắt là 2 dòng **xám**. Hai dòng **xanh** ở đầu sổ không hề bị đụng tới, vì chúng đã có mặt ở đa số máy nên leader mới bắt buộc phải có chúng (đây chính là Election Restriction ở S4 đang phát huy tác dụng).

Đây là tính chất **Log Matching** và **State Machine Safety** ở §5.2 đang tự sửa chữa hệ thống mà không cần ai can thiệp.

### Bảng tổng kết ở panel Metrics

```
client write gửi              6
→ được ack                    4      ← client thực sự nhận OK 4 lần
→ bị cắt bỏ (chưa từng ack)   2      ← 2 lệnh ghost, client KHÔNG hề nhận OK
```

Đọc dòng cuối cho đúng: **2 lệnh này bị mất, nhưng chúng chưa bao giờ được hứa hẹn với client.** Không có lời hứa nào bị phá vỡ.

> **Ghi nhớ con số này để so sánh.** Ở nhánh Redis Cluster sắp làm, cùng một tình huống sẽ cho ra kết quả khác hẳn: Redis trả OK cho client **trước khi** nhân bản (§5.4, "last failover wins"), nên khi master chết thì các lệnh **đã được hứa** vẫn biến mất. Bộ đếm ở đó sẽ hiện **write đã ack nhưng bị mất > 0** — điều mà Raft, theo thiết kế, không bao giờ có.

---

# PHẦN III — TỔNG HỢP

---

## 4. Bảng ánh xạ demo ↔ lý thuyết

Dùng bảng này khi viết luận văn: mỗi mục lý thuyết đều có một chỗ trong demo để chỉ vào.

| Mục lý thuyết | Nội dung | Xem ở đâu trong demo |
|---------------|----------|---------------------|
| §1.1 Consensus | Đa số quyết định, đã quyết là chung cuộc | S3 (ô chuyển xanh khi đủ 3/5) |
| §1.2 Fault tolerance | Cụm 5 chịu được 2 máy chết | S5 (phía 3 node vẫn chạy bình thường) |
| §1.3 Replicated state machine | Cuốn sổ + thi hành lệnh | Dải ô vuông dưới mỗi node |
| §1.4 Strong leader | Log chỉ chảy một chiều | S3 (mọi mũi tên AE đều xuất phát từ leader) |
| §2.1 Server states | 3 vai follower/candidate/leader | Màu node ở mọi kịch bản |
| §2.1 Terms | Nhiệm kỳ, đồng hồ logic | S1 (term 1→2), S5 (n1 lùi vì thấy term 2) |
| §2.1 Split vote | Chia phiếu | **S2** với jitter = 0 |
| §2.2 Leader election 5 bước | Toàn bộ quy trình bầu | **S1**, mốc 175→218ms |
| §2.2 Log replication 5 bước | Toàn bộ quy trình ghi | **S3**, mốc 320→348ms |
| §2.2 Safety restriction | Sổ cũ không được làm leader | **S4**, mốc 1659ms |
| §3.1.1 RequestVote RPC | Các trường của tin nhắn | Bấm vào chấm `RV` đang bay |
| §3.1.2 AppendEntries RPC | Các trường + luật nhận | Bấm vào chấm `AE`; luật thứ 3 (cắt bỏ) thấy ở S5 mốc 4229ms |
| §4.1 Heartbeat timeout | Phát hiện hỏng bằng im lặng | **S1**, mốc 2000→2190ms |
| §4.1 Randomized timeout | Vì sao phải ngẫu nhiên | **S2** — toàn bộ kịch bản |
| §4.1 Timing requirement | broadcastTime ≪ electionTimeout | **S2**, kéo thanh maxLatency lên cao |
| §4.1 Số đo của paper | Bảng so sánh theo jitter | `experiment` → CSV |
| §5.2 Election Safety | Tối đa 1 leader/nhiệm kỳ | S5 (2 leader nhưng khác nhiệm kỳ) + test `TestNoSplitBrain` |
| §5.2 Leader Append-Only | Leader chỉ ghi thêm | Sổ của leader không bao giờ bị cắt khi đang là leader |
| §5.2 Log Matching | Hai sổ khớp index+term thì khớp toàn bộ phía trước | **S5**, mốc 4229ms + test `TestSafetyUnderFaults` |
| §5.2 Leader Completeness | Dòng đã committed có mặt ở mọi leader sau | **S4** + test `TestAckedWritesSurvive` |
| §5.2 State Machine Safety | Không hai máy thi hành lệnh khác nhau ở cùng vị trí | test `TestSafetyUnderFaults` |
| §8.2 Pre-vote (gap) | Node cô lập quấy rối cụm | **S4**, mốc 1450ms — quan sát ngoài dự kiến |

## 5. Những gì demo KHÔNG thể hiện

Cần nói rõ khi trình bày, để không bị hỏi hớ:

| Không có | Nằm ở mục nào | Vì sao bỏ |
|----------|---------------|-----------|
| **InstallSnapshot / log compaction** | §3.1.3 | Sổ trong thực tế phải cắt gọn định kỳ, nếu không sẽ phình vô hạn. Cơ chế phức tạp nhưng giá trị trực quan thấp. |
| **Joint consensus (thêm/bớt máy)** | §1.4 | Cách thay đổi số lượng máy trong cụm mà không dừng dịch vụ. Đây là đóng góp riêng của Raft nhưng nằm ngoài trọng tâm demo. |
| **Pre-vote** | §8.2 | Bản thân paper cũng không có. Demo cho thấy **vấn đề** ở S4 nhưng không cài **lời giải**. |
| **Leadership transfer** | §8.2 | Chuyển giao leader có kế hoạch. |
| **Byzantine fault** | §10 | Máy nói dối, cố tình phá hoại. Raft không xử lý loại lỗi này, và §10 đã xác định đề tài không cần. |
| **Ghi đĩa thật** | §1.2 | Demo giả định ghi đĩa là tức thời. Máy thật tốn 0.5–20ms cho mỗi lần ghi, đây là một phần lớn của `broadcastTime`. |

Ngoài ra, cần nói thẳng: **đây là công cụ dạy học, không phải thư viện Raft dùng được cho production.**

## 6. Gợi ý kịch bản trình bày 10 phút

| Phút | Nội dung | Kịch bản | Chỗ dừng lại |
|------|----------|----------|--------------|
| 0–1 | Đặt vấn đề: 5 cuốn sổ phải giống hệt nhau | — | — |
| 1–3 | Cụm tự chọn sếp thế nào | **S1** ×0.25 | Dừng ở **175ms** — chỉ vào vòng cung, giải thích randomized timeout. Rồi dừng ở **2000ms** — chỉ vào 4 vòng cung đang chạy dần về hết. |
| 3–5 | Vì sao phải ngẫu nhiên | **S2** jitter=0 | Để chạy tự do 15 giây. Chỉ vào bộ đếm nhiệm kỳ leo lên 79. Rồi kéo jitter lên 150, chạy lại — xong trong 220ms. **Đây là khoảnh khắc gây ấn tượng nhất.** |
| 5–7 | Một lệnh ghi đi qua hệ thống | **S3** ×0.25 | Dừng ở **330ms** khi ô còn xám — nhấn mạnh *client vẫn đang chờ*. Rồi bước tới **348ms** khi ô chuyển xanh — *giờ mới trả OK*. |
| 7–10 | Mạng vỡ đôi | **S5** ×1 | Dừng ở **1600ms** — hai leader cùng lúc, một bên toàn xám một bên toàn xanh. Rồi nhảy tới **4232ms** — dòng ghost bị cắt. Kết bằng bảng Metrics: *6 gửi, 4 ack, 2 bị cắt, và 2 cái bị cắt chưa bao giờ được hứa.* |

Nếu còn thời gian, S4 là phần thưởng thêm — đặc biệt là quan sát về pre-vote.

## 7. Câu hỏi phản biện có thể gặp

**"Mô phỏng thì chứng minh được gì? Nó có đúng như hệ thống thật không?"**

Ba lớp trả lời. Thứ nhất, phần cài đặt bám sát Figure 2 của paper, mỗi luật đều có chú thích trỏ về đúng ô trong figure để đối chiếu được. Thứ hai, có bộ test chạy 400 lượt gây lỗi ngẫu nhiên (giết máy, hồi sinh, cắt mạng, ghi dữ liệu) và kiểm tra ba tính chất safety ở §5.2 **sau mỗi bước** — nếu cài sai thì test sẽ bắt được. Thứ ba, cần thừa nhận thẳng: đây là mô phỏng, và bản đề xuất có dự kiến một phụ lục dựng Redis Cluster thật bằng Docker để đối chiếu — phần đó chưa làm.

**"Sao con số không khớp với paper?"**

Đã trả lời ở phần S2. Tóm tắt: mô hình mạng thiếu các nguồn nhiễu của máy thật. Xu hướng khớp, con số không khớp, và tài liệu ghi rõ điều đó thay vì che giấu.

**"Vì sao cụm 5 máy mà chỉ chịu được 2 máy chết? Sao không phải 4?"**

Vì cần **đa số** để ra quyết định. Còn 1 máy sống thì nó không thể biết 4 máy kia đã quyết gì trong lúc nó mất liên lạc. Nếu cho phép thiểu số quyết định, hai nửa của một cụm bị chia cắt sẽ ra hai quyết định mâu thuẫn — và không có cách nào hoà giải về sau. Thà dừng còn hơn sai (§1.1).

**"Nếu leader chết ngay sau khi commit nhưng trước khi kịp trả lời client thì sao?"**

Dòng đó vẫn committed (đã có ở đa số máy) nên leader mới chắc chắn có nó. Nhưng client không nhận được trả lời nên sẽ thử lại — và có thể lệnh bị thực hiện **hai lần**. Raft không tự giải quyết chuyện này; hệ thống thật phải gắn mã định danh cho mỗi request để khử trùng lặp. Đây là vấn đề **idempotency**, và §8.4 có ghi nó sẽ quay lại ở Topic 3 (Reconciliation).

**"Cái này liên quan gì tới đề tài Redis Operator?"**

Hai điểm. Thứ nhất, Kubernetes lưu toàn bộ trạng thái trong etcd, mà etcd chạy Raft (§2.5) — nghĩa là **mọi** thao tác của Operator đều đi qua đúng cơ chế vừa xem, và chịu đúng các giới hạn đó (§5.5: độ trễ ghi tăng tuyến tính theo số node). Thứ hai, Redis Cluster cũng có bầu cử riêng cho failover (§2.3, §4.2) nhưng **chỉ dùng cho việc thăng cấp replica, không dùng cho dữ liệu**. Hiểu Raft trước là để thấy rõ Redis đã bỏ đi cái gì và được lại cái gì — đó chính là câu hỏi nghiên cứu số 2 ở §8.3.

---

*Tài liệu mô tả demo trong nhánh Raft. Mọi mốc thời gian lấy từ seed 42, tái hiện được bằng cách nhập đúng seed đó vào demo. Nhánh Redis Cluster chưa làm.*
