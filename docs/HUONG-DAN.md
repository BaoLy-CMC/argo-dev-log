# DevLogs — hướng dẫn nhanh

Xem log nhiều service cùng lúc, biết service nào đang đỏ và vì sao, xem CI và duyệt PR — trong một
trang. Chạy local, không cần Loki hay docker.

## Cần gì

`python3`, cộng CLI bạn đã có sẵn: `argocd` (log, health), `gh` (tab ci và prs), `claude` (chỉ cho
nút review). Không cài thêm gì.

## Chạy

```bash
./run.sh          # mở http://localhost:8900
```

Lấy token: DevTools → Application → Cookies → `argocd.token` → dán vào header, bấm **Connect**.

Header hiện thời gian còn lại của token. Hết hạn nó nói thẳng, không để bạn đi tìm bug.
F5 không mất token — token nằm trong RAM của server, không ghi ra file.

## Tab `logs`

![Log](screenshot-light.png)

Tick service ở cột trái → log chảy vào giữa, trộn nhiều service và **sắp theo thời gian**.

- Cụm prefix dài của cluster (level lặp 2 lần, requestID rỗng, thread, package) được **gỡ bỏ**. Cần
  dòng gốc thì bấm `raw`.
- Lọc theo level, hoặc gõ text / traceId. Bấm vào traceId để chỉ xem đúng luồng đó.
- Stack trace và JSON giữ nguyên một khối. Nháy đúp một dòng để copy.
- `2/2` cạnh tên service là số pod ready/tổng. Đỏ là đang thiếu pod.
- Chấm màu trước tên service trong log = màu của service đó, để phân biệt khi trộn nhiều stream.

## Tab `health` — vì sao đỏ

![Health](screenshot-health.png)

Danh sách mọi thứ đang đỏ. Chọn một service sẽ thấy:

- **Events** của Kubernetes, warning lên trước — dòng `Back-off restarting failed container` mà
  status không bao giờ nói cho bạn.
- **Log của container đã chết**, không phải container vừa khởi động lại. Crash loop thì log của
  container đang chạy sạch trơn sau vài giây.
- Không chỉ pod: rất nhiều app đỏ chỉ vì `ExternalSecret` không lấy được secret.

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
  bắt đúng convention của team.
- `approve` / `merge` gọi `tools/approve-prs.py` và `tools/merge-prs.py` đi kèm repo (chạy từ shell
  cũng được). Ai đã có bản riêng trong `~/bin` thì bản đó được ưu tiên.
- Luôn chạy `--dry-run` trước, hộp xác nhận hiện đúng output đó — với merge bạn thấy **PR nào bị từ
  chối và vì sao** trước khi bấm. Trên 3 PR phải gõ chữ để xác nhận. Không có nút approve-tất-cả.

Tab này không cần token ArgoCD, chỉ cần `gh`.

## Phím và thao tác

| | |
|---|---|
| `/` | nhảy vào ô filter service |
| `Escape` | xoá ô filter |
| kéo viền phải sidebar | đổi độ rộng, có nhớ |
| `★` | ghim service |
| `⊘` | ẩn service xuống folder `muted` ở đáy |
| nháy đúp dòng log | copy dòng đó |

Tick service thì hàng `refresh` / `sync` / `restart` mới hiện. `restart` bắt gõ chữ để xác nhận.
