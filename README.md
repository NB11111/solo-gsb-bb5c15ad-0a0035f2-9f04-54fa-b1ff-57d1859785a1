# 断点续传文件 API（Resumable Upload API）

面向弱网客户端的分片上传/断点续传服务：客户端先创建上传会话，再按**任意顺序**提交带起止偏移的二进制分片；服务端拒绝越界或与已收区间重叠的分片，并通过状态接口返回缺失区间。仅当分片**完整覆盖整个文件**且整文件 **SHA-256 校验通过**时才原子完成并开放下载——任何失败都不会产生可下载成品。

创建会话支持**幂等键**：`POST /sessions` 可携带 `Idempotency-Key` 头，首次响应丢失或并发重试时返回同一会话，不会重复建会话（详见下文「创建上传会话」）。

完成后的文件支持两类下载：

- **会话下载**（原有接口，不变）：`GET /sessions/{id}/download`，无次数限制；
- **受控下载票据**（新增）：可签发放发带**有效期（1～1440 分钟）**与**下载次数上限（1～100 次）**的一次性随机票据，凭票 URL 下载；服务端只保存票据的哈希，可随时撤销。

技术栈：Python 3.12 · FastAPI · SQLite（WAL 模式） · Uvicorn，容器化一键启动。

---

## 一键启动

需要 Docker 与 Docker Compose v2（`docker compose`）：

```bash
docker compose up --build -d
```

服务监听宿主机 **8000** 端口：

- 健康检查：<http://localhost:8000/health>
- 交互式文档（Swagger UI）：<http://localhost:8000/docs>
- 停止并清理：`docker compose down`（数据保留在命名卷 `resumable-data` 中；`-v` 连数据一起删除）

> 单容器单 uvicorn worker 部署。会话级互斥锁是进程内对象，多副本水平扩容需要额外的共享锁/存储，本版本不涉及。

---

## 配置项

全部配置通过环境变量覆盖（见 `docker-compose.yml` 的 `environment` 段）：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `/data` | 数据根目录（SQLite 库、分片、成品、临时文件） |
| `DATABASE_URL` | `sqlite:///$DATA_DIR/resumable.db` | SQLite 文件路径（形如 `sqlite:////path/to.db`） |
| `SESSION_TTL_SECONDS` | `86400`（1 天） | 会话存活时长；每次分片活动向后滑动（滑动过期） |
| `CLEANUP_INTERVAL_SECONDS` | `300`（5 分钟） | 后台清理线程的运行间隔 |
| `MAX_CHUNK_BYTES` | `67108864`（64 MiB） | 单个分片最大字节数，`0` 表示不限制 |
| `MAX_UPLOAD_BYTES` | `107374182400`（100 GiB） | 整文件最大字节数，`0` 表示不限制 |
| `MAX_FILENAME_LENGTH` | `255` | 文件名最大长度 |

数据目录布局：

```
$DATA_DIR/
├── resumable.db (+ -wal / -shm)     SQLite 数据库（upload_sessions / chunks / download_tickets / idempotency_keys）
├── files/<session_id>               完成后的成品（原子 rename 落地）
├── parts/<session_id>/<start>-<end> 已接收分片
└── tmp/                             正在接收/拼装的临时文件（启动周期清理）
```

数据库结构通过 `PRAGMA user_version` 管理版本，**服务启动时自动建库/增量迁移**，老库数据保留：v1 为上传会话与分片表，v2 新增 `download_tickets`（受控下载票据表），v3 新增 `idempotency_keys`（创建会话幂等键表，仅存键的 SHA-256 与请求指纹）。

---

## API 约定

- Base URL：`/api/v1`
- 偏移量为字节偏移，采用**半开区间 `[start, end)`**，即 `end - start = 分片字节数`
- 分片可以按**任意顺序**提交，允许重连后只补传缺失部分
- 错误响应体：`{"detail": {"error": "<错误码描述>", ...附加上下文}}`

### 1. 创建上传会话

```http
POST /api/v1/sessions
Content-Type: application/json
Idempotency-Key: order-2026-09-20-0001   ← 可选

{
  "filename": "movie.mp4",
  "total_size": 104857600,
  "sha256": "9f1b...（64 位小写十六进制，允许 sha256: 前缀）",
  "chunk_size": 4194304
}
```

`chunk_size` 仅为客户端建议值，可为 0。返回 `201` 与完整状态（含 `session_id`、`expires_at`、初始 `missing_ranges`）。

**幂等创建（可选）**：携带 `Idempotency-Key` 头时，相同键 + 相同请求的重试返回**首次创建的会话**（同一 `session_id`，状态码仍为 `201`），不会重复建会话——首次响应丢失后的重试与并发重试都安全。

- 键格式：**1～128 个可见 ASCII 字符**（0x21–0x7E，不含空格），不合法返回 `400`。
- 服务端**只保存键的 SHA-256 与规范化请求指纹**（`filename`/`total_size`/`sha256`/`chunk_size` 规范化后的 SHA-256），明文键不落库。
- 相同键搭配**不同**请求（四个字段任一不同）→ `409`，且不新建会话。
- 键记录与会话在**同一事务**中创建；删除会话（`DELETE /sessions/{id}`）时键记录级联删除，之后同键请求按全新请求处理。
- 不带该头的请求行为不变：每次都创建新会话。

### 2. 提交二进制分片

```http
PUT /api/v1/sessions/{session_id}/chunks?start=0&end=4194304
Content-Type: application/octet-stream
Content-Length: 4194304
X-Chunk-SHA256: 3a7b...（可选，本分片字节的 64 位十六进制 SHA-256，允许 sha256: 前缀）

<原始二进制分片>
```

服务端校验（任一不过即拒绝，不写入任何分片数据）：

- 会话存在且处于 `uploading`（未过期/未放弃/未完成）
- `0 <= start < end <= total_size` —— 越界返回 `400`
- `Content-Length == end - start`，且实际请求体长度一致 —— 不一致返回 `400/413`
- 分片不超过 `MAX_CHUNK_BYTES` —— 超限返回 `413`
- `[start, end)` 与任何已收区间**不得重叠**（相邻允许，如已有 `[0,100)` 再传 `[100,200)`）—— 重叠返回 `409`

**`X-Chunk-SHA256`（可选，逐片校验）**：客户端若提供该头，服务端在流式接收分片的同时增量计算 SHA-256，并在**写入分片目录、登记区间之前**完成校验：

- 头格式错误（不是 64 位十六进制、长度不对、含非法字符等）→ `400 {"error":"invalid X-Chunk-SHA256 header"}`，在读取请求体之前拒绝，**不滑动 TTL、不落盘、不登记**；
- 请求体实际长度与区间不符 → 仍按长度错误返回 `400/413`（长度校验优先于摘要）；
- 长度正确但摘要不匹配 → `422 {"error":"chunk sha256 mismatch","expected":...,"actual":...,"start":...,"end":...}`，临时文件立即删除，**分片不落盘、不登记区间**；客户端用正确内容重传同一区间即可正常继续，最终整文件 SHA-256 校验不受影响。
- 不携带该头的请求**行为完全不变**（仅依赖完成时的整文件 SHA-256 兜底）；同一会话的各个分片可以自由选择是否带头。

curl 示例：

```bash
digest=$(sha256sum chunk-0.bin | awk '{print $1}')
curl -X PUT \
  -H "Content-Type: application/octet-stream" \
  -H "X-Chunk-SHA256: $digest" \
  --data-binary @chunk-0.bin \
  "http://localhost:8000/api/v1/sessions/$SID/chunks?start=0&end=4194304"
```

返回当前累计字节数与剩余 `missing_ranges`。并发提交同一区间时只有一个成功，其余得到 `409`。

### 3. 查询状态 / 缺失区间

```http
GET /api/v1/sessions/{session_id}
```

```json
{
  "session_id": "...",
  "filename": "movie.mp4",
  "total_size": 104857600,
  "status": "uploading",
  "bytes_received": 8388608,
  "fully_received": false,
  "complete": false,
  "missing_ranges": [{"start": 4194304, "end": 8388608}, {"start": 12582912, "end": 104857600}],
  "expires_at": "2026-09-21T12:00:00Z",
  "last_access_at": "2026-09-20T12:00:00Z",
  "completed_at": null,
  "download_url": null
}
```

客户端断连恢复时：先查状态，按 `missing_ranges` 补传即可。

### 4. 原子完成

```http
POST /api/v1/sessions/{session_id}/complete
```

- 区间未全覆盖 → `409`，响应里带当前 `missing_ranges`，不产生成品
- 已覆盖但 SHA-256 不匹配 → `422`（返回 `expected`/`actual`），会话保持 `uploading`，客户端可修正后重试或放弃；不产生成品
- 全部通过 → 拼装临时文件 → 校验 → 与状态翻转在同一事务窗口内**原子发布**，状态变为 `completed`，开放下载。完成接口幂等。

### 5. 下载成品（会话下载，不计数）

```http
GET /api/v1/sessions/{session_id}/download
```

仅 `completed` 会话返回 `200`（支持 `Range` 断点下载续传）；其他状态一律 `404`，失败流程中不存在任何可下载产物。该接口保持不变、不校验票据、不限制次数。

### 6. 签发受控下载票据

```http
POST /api/v1/sessions/{session_id}/tickets
Content-Type: application/json

{
  "expires_in_minutes": 60,
  "max_downloads": 5
}
```

- `expires_in_minutes`：有效期分钟数，范围 **1～1440**；`max_downloads`：下载次数上限，范围 **1～100**。越界返回 `422`。
- 仅 `completed` 会话可签发，否则 `409`。
- 服务端生成约 256 bit 熵的随机票据，**数据库只保存其 SHA-256**，明文仅在 `201` 响应中返回这一次，服务端无法再次展示，也无法从库中反推：

```json
{
  "ticket_id": "777aaac7...",
  "session_id": "e3a886dc...",
  "created_at": "2026-09-20T16:00:51Z",
  "expires_at": "2026-09-20T17:00:51Z",
  "max_downloads": 5,
  "remaining_downloads": 5,
  "revoked": false,
  "download_url": "/api/v1/tickets/777aaac7.../download",
  "ticket": "777aaac7....Uw21NON6si8F-Ri4B2HKWFXMCcDkY4wY3u_eyaR8rmQ"
}
```

`ticket` 即下载凭证（`<ticket_id>.<随机串>`，URL-safe），把它拼到下载路径即可分发；请像对待密码一样保管它，票据 URL 可能出现在代理/访问日志中。

### 7. 凭票下载（原子计数，支持 Range）

```http
GET /api/v1/tickets/{ticket}/download
GET /api/v1/tickets/{ticket}/download
Range: bytes=100-199
```

服务端在**单个 SQLite `BEGIN IMMEDIATE` 事务**内完成全部校验与扣减：

1. 票据存在（id 与随机串的哈希同时匹配），否则 `404`；
2. 未撤销，否则 `403`；
3. 未过期（当前时间 ≤ `expires_at`），否则 `410`；
4. `remaining_downloads > 0` 且会话成品仍存在，否则 `429` / `404`。

校验通过后用条件 `UPDATE ... WHERE revoked=0 AND expires_at>? AND remaining_downloads>0` 原子扣减一次再提交。并发请求在事务内串行，后到者看到的剩余次数为 0，**不可能突破上限**；任何校验失败都在扣减前回滚，**绝不计数**。

- 完整下载返回 `200`，`Range` 请求返回 `206`——**Range 同样按一次计数**（下载器的断点续传会消耗票据次数）。
- 次数用尽后返回 `429 {"detail":{"error":"ticket download limit reached"}}`。
- 票据独立计数，互不影响；删除所属会话会级联删除其票据。

### 8. 撤销票据

```http
POST /api/v1/tickets/{ticket}/revoke
```

幂等：已撤销再次调用仍返回 `200` 与票据元数据（`revoked: true`，剩余次数原样保留）。票据不存在/伪造返回 `404`。撤销后任何凭票下载立即得到 `403`，且不扣减。

### 9. 放弃会话

```http
DELETE /api/v1/sessions/{session_id}
```

幂等，返回 `204`：删除会话记录、已收分片与成品。

### 10. 存储汇总（运维只读）

```http
GET /api/v1/storage/summary
```

在**单个 SQLite 只读快照**中按会话状态汇总，供运维容量观测。响应固定包含 `uploading`、`completed`、`aborted`、`expired` 四行（即使计数为零也保留），外加由四行求和得到的 `total` 行：

```json
{
  "statuses": [
    {"status": "uploading", "session_count": 3, "declared_bytes": 210, "stored_bytes": 130},
    {"status": "completed", "session_count": 1, "declared_bytes": 10,  "stored_bytes": 10},
    {"status": "aborted",   "session_count": 0, "declared_bytes": 0,   "stored_bytes": 0},
    {"status": "expired",   "session_count": 0, "declared_bytes": 0,   "stored_bytes": 0}
  ],
  "total": {"status": "total", "session_count": 4, "declared_bytes": 220, "stored_bytes": 140}
}
```

字段口径：

- `declared_bytes`：该状态所有会话 `total_size`（客户端声明大小）之和。
- `stored_bytes`：实际占用的逻辑字节数——`uploading` 取 `bytes_received` 之和，`completed` 取 `total_size` 之和，`aborted` / `expired` 恒为 `0`。

该接口是**纯只读**的：以 `mode=ro` + `PRAGMA query_only` 的独立连接执行单条聚合 SQL，**不扫描文件系统、不更新 `last_access_at`、不滑动会话 TTL，也不触发访问时懒过期**（已过 `expires_at` 但尚未被后台清理的会话仍按 `uploading` 统计）。

### 状态码与生命周期

| 状态 | 含义 |
| --- | --- |
| `uploading` | 收片中；超过 `expires_at` 未活动则过期 |
| `completed` | 校验通过、成品可下载 |
| `expired` | 已被懒过期或后台清理标记，再提交分片得到 `410`，分片已清除 |
| `aborted` | 已放弃（删除接口直接硬删除，正常不可见） |

---

## 过期与清理

- **滑动过期**：创建会话按 `SESSION_TTL_SECONDS` 生成 `expires_at`；每次成功的分片活动自动顺延。
- **后台清理线程**：每 `CLEANUP_INTERVAL_SECONDS` 执行一轮，把过期会话标记为 `expired`、删除其分片目录，并清理已完成会话的分片、孤儿目录和崩溃残留的临时文件。
- **访问时懒过期**：即使后台线程尚未轮到，对已过期会话的任何操作都会立即得到 `410` 并触发清理。
- 已完成的成品**不**随会话过期删除。
- 下载票据的过期与次数在每次凭票下载时于事务内即时校验，过期/撤销的票据不依赖后台清理；删除会话时票据随外键 `ON DELETE CASCADE` 一并删除。

## 弱网客户端建议

- 分片大小取 256 KiB～4 MiB；上传失败时按指数退避重试同一分片（重复提交重叠分片会得到 `409`，可直接忽略并查状态）。
- 恢复后先调 `GET /sessions/{id}`，只补 `missing_ranges`。
- 可选为每个分片附带 `X-Chunk-SHA256` 头：弱网/代理导致的分片片内损坏在提交时即可被 `422` 发现并就地重传，不必等到最后整文件校验失败后放弃整个会话。
- 全部补齐后调 `complete`；`422 sha256 mismatch` 说明有分片内容损坏，应放弃会话后重新创建上传。

---

## 本地开发与测试

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt pytest httpx
DATA_DIR=./data uvicorn app.main:app --reload --port 8000

# 测试（自动使用临时数据目录）
python3 -m pytest -q
```

测试覆盖：乱序完整上传与下载、越界/重叠/长度不符拒绝、缺失区间计算、并发重叠竞态（仅一个成功）、SHA-256 不匹配、空文件、放弃、过期清理与未知会话；受控票据覆盖参数边界、只存哈希、完整/Range 下载计数、耗尽 `429`、过期 `410`、撤销 `403`、伪造票据 `404`、30 并发不突破上限、票据相互独立、删除会话级联失效；幂等键覆盖同键重放返回同一会话、同键不同请求 `409`、键格式边界（1/128 字符、空、空格、非 ASCII）、只存哈希、10 并发同键重试只建一个会话、8 并发同键不同请求恰一个成功、删除会话级联删除键记录，以及 v1/v2 老库自动迁移到最新结构。`X-Chunk-SHA256` 覆盖匹配摘要接受（小写/大写/`sha256:` 前缀）、摘要不符 `422`（不落盘、不登记、可重传后正常完成整文件校验）、头格式错误 `400`（读体前拒绝）、长度错误优先于摘要错误、无头行为不变，以及带头/不带头分片混合上传。
