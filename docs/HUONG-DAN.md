# DevLogs — hướng dẫn nhanh

Xem log nhiều service cùng lúc, biết service nào đang đỏ và vì sao, xem CI và duyệt PR — trong một
trang. Chạy local, không cần Loki hay docker.

## Cần gì

`python3`, cộng CLI bạn đã có sẵn: `argocd` (log, health), `gh` (tab ci và prs), `claude` (nút
review PR và nút phân tích lỗi). Không cài thêm gì.

## Chạy

```bash
./run.sh          # mở http://localhost:8900
```

Muốn `ask claude` đọc được source thì trỏ tới nơi bạn clone service:

```bash
export DEVLOGS_SRC_ROOT=~/projects/finx
```

Lấy token: DevTools → Application → Cookies → `argocd.token` → dán vào header, bấm **Connect**.

Header hiện thời gian còn lại của token. Hết hạn nó nói thẳng, không để bạn đi tìm bug.
F5 không mất token — token nằm trong RAM của server, không ghi ra file. Restart server cũng không
làm trang chết cứng: stream tự thử lại và chạy tiếp ngay khi bạn dán token mới, không cần F5.

## Tab `logs`

![Log](screenshot-light.png)

Tick service ở cột trái → log chảy vào giữa, trộn nhiều service và **sắp theo thời gian**.

- Cụm prefix dài của cluster (level lặp 2 lần, requestID rỗng, thread, package) được **gỡ bỏ**. Cần
  dòng gốc thì bấm `raw`.
- Lọc theo level, hoặc gõ text / traceId. Bấm vào traceId để chỉ xem đúng luồng đó.
- Stack trace và JSON giữ nguyên một khối. Nháy đúp một dòng để copy.
- `2/2` cạnh tên service là số pod ready/tổng. Đỏ là đang thiếu pod.
- Chấm màu trước tên service trong log = màu của service đó, để phân biệt khi trộn nhiều stream.
- `ask claude` gửi **đúng những dòng đang hiện** (đã qua filter) đi phân tích.

## Tab `health` — vì sao đỏ

![Health](screenshot-health.png)

Danh sách mọi thứ đang đỏ. Chọn một service sẽ thấy:

- **Events** của Kubernetes, warning lên trước — dòng `Back-off restarting failed container` mà
  status không bao giờ nói cho bạn.
- **Log của container đã chết**, không phải container vừa khởi động lại. Crash loop thì log của
  container đang chạy sạch trơn sau vài giây.
- Không chỉ pod: rất nhiều app đỏ chỉ vì `ExternalSecret` không lấy được secret.

Service đã mute thì không hiện ở đây và không tính vào số trên tab.

### Nút `ask claude`

Gửi toàn bộ chẩn đoán trên sang `claude` CLI **trên máy bạn**, trả về văn xuôi: nguyên nhân khả dĩ
nhất, bằng chứng dựa vào đâu, và cần kiểm tra gì tiếp.

Nó đọc thêm hai nguồn mà log không có:

- **Manifest deploy** trong repo GitOps của service (image tag, resource limit, env, probe). Rất
  nhiều nguyên nhân nằm ở đây và không bao giờ xuất hiện trong log — limit vừa bị vượt, probe chết
  vì boot bị throttle, tag vừa roll. Manifest chỉ tham chiếu tên secret, không mang giá trị secret.
- **Source code**, nếu repo đã clone dưới `DEVLOGS_SRC_ROOT`. Lúc đó nó chỉ được ra file và hàm gây
  lỗi. Không có bản clone thì nó chạy trong thư mục rỗng — cố ý, để không đổ tội nhầm codebase.

## Tab `ci`

CI của service đang tick, **chỉ nhánh chính**. Mỗi workflow: run mới nhất, và nếu đỏ thì chỉ ra
**commit làm vỡ** kèm log của job fail. Bên dưới là lịch sử deploy trong repo GitOps của chính
service đó.

Link service với repo một lần. Gợi ý chỉ hiện khi tên khớp chính xác — đoán mò sẽ chỉ CI của repo
team khác.

## Tab `prs`

![PR](screenshot-prs.png)

Toàn bộ PR đang mở, kèm trạng thái review và **kích thước diff**. Diff bốn chữ số in đậm: đừng
approve loại đó trong một lô.

- `claude` gửi diff sang `claude` CLI **trên máy bạn** review. Nó đọc được `CLAUDE.md` của repo, nên
  bắt đúng convention của team. Kết quả về dạng **checklist**, mỗi dòng kèm file và số dòng: bỏ tick
  cái bạn không đồng ý, chọn `comment` hoặc `request changes`, rồi post thành **một** review inline.
  Finding trỏ vào dòng không nằm trong diff vẫn hiện nhưng không post được — GitHub từ chối loại đó.
- Hộp xác nhận có ô nhập comment cho review.
- `approve` / `merge` gọi `tools/approve-prs.py` và `tools/merge-prs.py` đi kèm repo (chạy từ shell
  cũng được). Ai đã có bản riêng trong `~/bin` thì bản đó được ưu tiên.
- Luôn chạy `--dry-run` trước, hộp xác nhận hiện đúng output đó — với merge bạn thấy **PR nào bị từ
  chối và vì sao** trước khi bấm. Trên 3 PR phải gõ chữ để xác nhận. Không có nút approve-tất-cả.
- Ô `all` tick toàn bộ PR mà search hiện tại vừa nạp. **Quá 30 thì hai nút tắt** — tool chỉ nhận 30
  ref đầu rồi bỏ phần còn lại mà không báo, nên phải thu hẹp search. Đổi search thì bỏ hết tick đang
  chọn: cái bạn không còn nhìn thấy thì không kiểm được trước khi approve.

Tab này không cần token ArgoCD, chỉ cần `gh`.

## Phím và thao tác

| | |
|---|---|
| `/` | nhảy vào ô filter service |
| `Escape` | xoá ô filter |
| kéo viền phải sidebar | đổi độ rộng, có nhớ |
| `★` | ghim service |
| `⊘` | ẩn service xuống folder `muted` ở đáy |
| `⊘` trên tên group | mute cả group — và **gỡ mute cả group bằng một cú bấm** |
| chuông | báo khi service đã ghim chuyển đỏ |
| nháy đúp dòng log | copy dòng đó |

Tick service thì hàng `refresh` / `sync` / `restart` mới hiện. `restart` bắt gõ chữ để xác nhận.

Chuông chỉ kêu **một lần mỗi lần chuyển trạng thái**, không kêu lại mỗi 30 giây khi service vẫn
đang đỏ, và không kêu cho service đã mute — mute thắng ghim. Cho phép notification thì có thông báo
desktop. Nó chỉ sống khi tab còn mở: để nhắc, không phải hệ thống trực.
