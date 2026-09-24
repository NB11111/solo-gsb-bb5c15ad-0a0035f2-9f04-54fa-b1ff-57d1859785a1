"""FastAPI 入口：弱网客户端的断点续传文件 API。

接口概览（前缀 /api/v1）：
  POST   /sessions                      创建上传会话（可选 Idempotency-Key 幂等创建）
  GET    /sessions/{id}                 查询状态与缺失区间
  PUT    /sessions/{id}/chunks          提交二进制分片（?start=&end=，半开区间；可选 X-Chunk-SHA256 逐片校验）
  POST   /sessions/{id}/copy-chunk      从已完成来源会话的成品服务端复制一段为目标会话分片
  POST   /sessions/{id}/complete        原子完成（区间全覆盖 + SHA-256 校验）
  DELETE /sessions/{id}                 放弃会话
  GET    /sessions/{id}/download        会话下载（原有接口，不计数）
  POST   /sessions/{id}/tickets         签发受控下载票据（有效期 + 次数上限）
  GET    /tickets/{ticket}/download     凭票下载（原子扣减一次，支持 Range）
  POST   /tickets/{ticket}/revoke       撤销票据
  GET    /storage/summary              运维只读：按四种状态汇总会话与字节数
"""
from __future__ import annotations

import hashlib
import secrets
import shutil
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

from anyio.to_thread import run_sync
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, RedirectResponse

from . import db, idempotency, schemas, settings, storage
from .cleanup import start_janitor
from .locks import session_locks


# ---------------------------------------------------------------------------
# 响应装配
# ---------------------------------------------------------------------------

def _session_payload(row: sqlite3.Row) -> dict:
    conn = db.get_conn()
    chunk_rows = conn.execute(
        "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
        (row["id"],),
    ).fetchall()
    received = [(r["start_offset"], r["end_offset"]) for r in chunk_rows]
    missing = storage.missing_intervals(received, row["total_size"])
    complete = row["status"] == "completed"
    return {
        "session_id": row["id"],
        "filename": row["filename"],
        "total_size": row["total_size"],
        "sha256_expected": row["sha256_expected"],
        "chunk_size": row["chunk_size"],
        "status": row["status"],
        "bytes_received": row["bytes_received"],
        "complete": complete,
        "fully_received": not missing,
        "missing_ranges": [{"start": s, "end": e} for s, e in missing],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "last_access_at": row["last_access_at"],
        "completed_at": row["completed_at"],
        "download_url": f"/api/v1/sessions/{row['id']}/download" if complete else None,
    }


def _http(status: int, error: str, **extra: object) -> HTTPException:
    detail: dict[str, object] = {"error": error}
    detail.update(extra)
    return HTTPException(status_code=status, detail=detail)


def _load_session(session_id: str) -> sqlite3.Row:
    """读取会话；调用方必须持有该会话的锁。

    顺带做懒过期：uploading 且已过 expires_at 时立即标记 expired 并删除分片。
    completed 正常返回；aborted/expired 返回 410；不存在返回 404。
    """
    conn = db.get_conn()
    row = conn.execute(
        "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
    ).fetchone()
    if row is None:
        raise _http(404, "session not found")
    if row["status"] in ("aborted", "expired"):
        raise _http(410, f"session {row['status']}")
    if row["status"] == "completed":
        return row

    if storage.parse_iso(row["expires_at"]) <= storage.utcnow():
        with db.immediate() as tx:
            tx.execute(
                "UPDATE upload_sessions SET status = 'expired' "
                "WHERE id = ? AND status = 'uploading'",
                (session_id,),
            )
        shutil.rmtree(storage.parts_dir(session_id), ignore_errors=True)
        raise _http(410, "session expired")
    return row


def _existing_intervals(tx: sqlite3.Connection, session_id: str) -> list[tuple[int, int]]:
    return [
        (r["start_offset"], r["end_offset"])
        for r in tx.execute(
            "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
            (session_id,),
        ).fetchall()
    ]


# ---------------------------------------------------------------------------
# 生命周期：初始化存储/数据库，启动后台清理
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    storage.ensure_dirs()
    db.init_db()
    _, stop_event = start_janitor()
    try:
        yield
    finally:
        stop_event.set()


app = FastAPI(
    title="Resumable Upload API",
    version="1.0.0",
    description="弱网客户端断点续传文件 API：分片乱序提交、区间校验、原子完成。",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# 会话管理
# ---------------------------------------------------------------------------

@app.post(
    "/api/v1/sessions",
    status_code=201,
    response_model=schemas.SessionResponse,
    tags=["sessions"],
)
def create_session(
    body: schemas.CreateSessionRequest,
    idempotency_key: str | None = Header(
        default=None,
        description="可选幂等键：1～128 个可见 ASCII 字符；相同键 + 相同请求返回首次创建的会话",
    ),
) -> dict:
    storage.ensure_dirs()

    # 幂等键：只保存 SHA-256 与规范化请求指纹，明文键绝不落库。
    key_hash: str | None = None
    fingerprint: str | None = None
    if idempotency_key is not None:
        if not idempotency.is_valid_key(idempotency_key):
            raise _http(
                400,
                "invalid Idempotency-Key header",
                requirement=idempotency.KEY_REQUIREMENT,
            )
        key_hash = idempotency.key_hash(idempotency_key)
        fingerprint = idempotency.request_fingerprint(
            filename=body.filename,
            total_size=body.total_size,
            sha256=body.sha256,
            chunk_size=body.chunk_size,
        )

    session_id = uuid.uuid4().hex
    now_iso, expires_iso = storage.future_expiry()
    # BEGIN IMMEDIATE 把整个“查键 → 建会话 → 登记键”串行化：
    # 并发重试在此排队，后到者读到已提交的键记录后直接重放，不会重复建会话。
    with db.immediate() as conn:
        if key_hash is not None:
            existing = conn.execute(
                "SELECT session_id, request_fingerprint FROM idempotency_keys "
                "WHERE key_hash = ?",
                (key_hash,),
            ).fetchone()
            if existing is not None:
                if existing["request_fingerprint"] != fingerprint:
                    # 同键不同请求：冲突，回滚且不新建任何会话。
                    raise _http(
                        409,
                        "Idempotency-Key already used with a different request",
                    )
                row = conn.execute(
                    "SELECT * FROM upload_sessions WHERE id = ?",
                    (existing["session_id"],),
                ).fetchone()
                if row is not None:
                    # 重放（首次响应丢失/并发重试）：返回首次创建的会话，仍为 201。
                    return _session_payload(row)
                # 键记录残留而会话已不存在（正常会被 ON DELETE CASCADE 清除）：
                # 防御性清理后按首次请求处理。
                conn.execute(
                    "DELETE FROM idempotency_keys WHERE key_hash = ?", (key_hash,)
                )

        conn.execute(
            "INSERT INTO upload_sessions ("
            "id, filename, total_size, sha256_expected, chunk_size, "
            "status, bytes_received, created_at, expires_at, last_access_at"
            ") VALUES (?, ?, ?, ?, ?, 'uploading', 0, ?, ?, ?)",
            (
                session_id,
                body.filename,
                body.total_size,
                body.sha256,
                body.chunk_size,
                now_iso,
                expires_iso,
                now_iso,
            ),
        )
        if key_hash is not None:
            # 键记录与会话同事务落库：要么都成功，要么一起回滚。
            conn.execute(
                "INSERT INTO idempotency_keys "
                "(key_hash, session_id, request_fingerprint, created_at) "
                "VALUES (?, ?, ?, ?)",
                (key_hash, session_id, fingerprint, now_iso),
            )
        row = conn.execute(
            "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
        ).fetchone()
    storage.parts_dir(session_id).mkdir(parents=True, exist_ok=True)
    return _session_payload(row)


@app.get(
    "/api/v1/sessions/{session_id}",
    response_model=schemas.SessionResponse,
    tags=["sessions"],
)
def get_session(session_id: str) -> dict:
    with session_locks.get(session_id):
        row = _load_session(session_id)
        return _session_payload(row)


@app.delete("/api/v1/sessions/{session_id}", status_code=204, tags=["sessions"])
def abort_session(session_id: str) -> Response:
    with session_locks.get(session_id):
        with db.immediate() as tx:
            tx.execute(
                "DELETE FROM upload_sessions WHERE id = ?", (session_id,)
            )  # chunks 由 ON DELETE CASCADE 删除
        storage.remove_session_tree(session_id)
    session_locks.discard(session_id)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# 分片提交
# ---------------------------------------------------------------------------

def _prepare_chunk(session_id: str, start: int, end: int, declared_length: int) -> None:
    """在线程池中执行：会话状态、越界、重叠与大小的首次权威校验，并滑动 TTL。"""
    with session_locks.get(session_id):
        row = _load_session(session_id)
        if row["status"] == "completed":
            raise _http(409, "session already completed; no more chunks accepted")
        total = row["total_size"]
        if end > total:
            raise _http(
                400,
                "chunk range exceeds total_size",
                start=start,
                end=end,
                total_size=total,
            )
        if settings.max_chunk_bytes and end - start > settings.max_chunk_bytes:
            raise _http(
                413,
                "chunk larger than MAX_CHUNK_BYTES",
                max_chunk_bytes=settings.max_chunk_bytes,
            )
        if declared_length != end - start:
            raise _http(
                400,
                "content-length does not match end-start",
                content_length=declared_length,
                expected=end - start,
            )
        conn = db.get_conn()
        existing = [
            (r["start_offset"], r["end_offset"])
            for r in conn.execute(
                "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
                (session_id,),
            ).fetchall()
        ]
        if storage.overlaps_any(start, end, existing):
            raise _http(
                409,
                "chunk overlaps an already received range",
                start=start,
                end=end,
            )
        now = storage.utcnow()
        with db.immediate() as tx:
            tx.execute(
                "UPDATE upload_sessions SET last_access_at = ?, expires_at = ? "
                "WHERE id = ? AND status = 'uploading'",
                (
                    storage.iso(now),
                    storage.iso(now + timedelta(seconds=settings.ttl_seconds)),
                    session_id,
                ),
            )


def _commit_chunk(session_id: str, start: int, end: int, tmp_file: str) -> dict:
    """落盘临时分片并在单个 IMMEDIATE 事务中二次校验 + 登记。"""
    tmp_path = Path(tmp_file)
    with session_locks.get(session_id):
        try:
            with db.immediate() as tx:
                row = tx.execute(
                    "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
                ).fetchone()
                if row is None:
                    raise _http(404, "session not found")
                if row["status"] in ("aborted", "expired") or (
                    row["status"] == "uploading"
                    and storage.parse_iso(row["expires_at"]) <= storage.utcnow()
                ):
                    raise _http(410, "session no longer active")
                if row["status"] == "completed":
                    raise _http(409, "session already completed")
                if end > row["total_size"]:
                    raise _http(400, "chunk range exceeds total_size")

                existing = _existing_intervals(tx, session_id)
                if storage.overlaps_any(start, end, existing):
                    raise _http(
                        409,
                        "chunk overlaps an already received range",
                        start=start,
                        end=end,
                    )

                # 事务内移动文件：若后续 SQL 失败，事务回滚且下面的 except 会删除文件。
                final = storage.part_path(session_id, start, end)
                final.parent.mkdir(parents=True, exist_ok=True)
                tmp_path.replace(final)

                now = storage.utcnow()
                now_iso = storage.iso(now)
                expires_iso = storage.iso(
                    now + timedelta(seconds=settings.ttl_seconds)
                )
                tx.execute(
                    "INSERT INTO chunks "
                    "(session_id, start_offset, end_offset, size, received_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (session_id, start, end, end - start, now_iso),
                )
                tx.execute(
                    "UPDATE upload_sessions "
                    "SET bytes_received = bytes_received + ?, "
                    "last_access_at = ?, expires_at = ? WHERE id = ?",
                    (end - start, now_iso, expires_iso, session_id),
                )
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise

    return _chunk_accepted_payload(session_id, start, end)


def _chunk_accepted_payload(session_id: str, start: int, end: int) -> dict:
    """分片登记成功后的统一响应体（PUT 分片与 copy-chunk 共用同一形态）。"""
    fresh = db.get_conn().execute(
        "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
    ).fetchone()
    payload = _session_payload(fresh)
    return {
        "session_id": session_id,
        "received": {"start": start, "end": end},
        "bytes_received": payload["bytes_received"],
        "missing_ranges": payload["missing_ranges"],
        "fully_received": payload["fully_received"],
        "expires_at": payload["expires_at"],
    }


@app.put(
    "/api/v1/sessions/{session_id}/chunks",
    response_model=schemas.ChunkResponse,
    tags=["chunks"],
)
async def put_chunk(
    session_id: str,
    request: Request,
    start: int = Query(..., ge=0, description="起始偏移（含）"),
    end: int = Query(..., ge=0, description="结束偏移（不含），半开区间 [start, end)"),
    chunk_sha256: str | None = Header(
        default=None,
        alias="X-Chunk-SHA256",
        description=(
            "可选：本分片原始字节的 SHA-256（64 位十六进制，可带 sha256: 前缀）。"
            "提供时服务端在写入分片前校验请求体长度与摘要；格式错误返回 400，"
            "摘要不匹配返回 422，且不落盘、不登记区间，重传正确内容即可恢复。"
        ),
    ),
) -> dict:
    if end <= start:
        raise _http(
            400, "invalid range: require 0 <= start < end", start=start, end=end
        )
    declared_raw = request.headers.get("content-length")
    if declared_raw is None:
        raise _http(411, "content-length header is required")
    try:
        declared_length = int(declared_raw)
    except ValueError:
        raise _http(400, "invalid content-length header")
    if declared_length != end - start:
        raise _http(
            400,
            "content-length does not match end-start",
            content_length=declared_length,
            expected=end - start,
        )
    if settings.max_chunk_bytes and declared_length > settings.max_chunk_bytes:
        raise _http(
            413, "chunk larger than MAX_CHUNK_BYTES",
            max_chunk_bytes=settings.max_chunk_bytes,
        )
    # 头格式错误在读体之前拒绝：不消耗请求体、不滑动 TTL、不接触存储。
    if chunk_sha256 is not None:
        try:
            expected_chunk_digest = schemas.normalize_sha256(chunk_sha256)
        except ValueError:
            raise _http(
                400,
                "invalid X-Chunk-SHA256 header",
                requirement="64 lowercase hex characters, optional sha256: prefix",
            )
    else:
        expected_chunk_digest = None

    # 首次校验 + TTL 滑动（阻塞 DB 操作丢到线程池，避免卡住事件循环）。
    await run_sync(_prepare_chunk, session_id, start, end, declared_length)

    # 流式落临时文件，弱网下也不会把整分片读进内存。
    # 提供 X-Chunk-SHA256 时同步增量计算摘要：校验不过只删除临时文件，
    # 绝不移动到分片目录、绝不登记区间，因此重传正确内容后仍可正常完成。
    tmp = storage.new_temp_file()
    received = 0
    digest = hashlib.sha256() if expected_chunk_digest is not None else None
    try:
        with tmp.open("wb") as out:
            async for block in request.stream():
                received += len(block)
                if received > declared_length:
                    raise _http(
                        413,
                        "request body longer than declared range",
                        declared=declared_length,
                    )
                out.write(block)
                if digest is not None:
                    digest.update(block)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

    if received != end - start:
        tmp.unlink(missing_ok=True)
        raise _http(
            400,
            "body length does not match declared range",
            bytes_received=received,
            expected=end - start,
        )

    if digest is not None:
        actual_chunk_digest = digest.hexdigest()
        if actual_chunk_digest != expected_chunk_digest:
            tmp.unlink(missing_ok=True)
            raise _http(
                422,
                "chunk sha256 mismatch",
                start=start,
                end=end,
                expected=expected_chunk_digest,
                actual=actual_chunk_digest,
            )

    return await run_sync(_commit_chunk, session_id, start, end, str(tmp))


# ---------------------------------------------------------------------------
# 服务端复制分片：从已完成来源会话的成品复制一段到目标会话
#
# 与 PUT 分片的登记规则完全一致（同一校验、同一响应、同一 chunks 表），
# 只是字节来源从客户端请求体换成来源成品文件。流程分两段：
#   1. 同时按 ID 定序持有“来源 + 目标”两把会话锁，完成来源资格、
#      来源区间、目标边界、单片大小、与已收区间重叠的全部预校验，
#      然后流式复制到临时文件；复制期间来源无法被放弃删除、目标无法被放弃。
#   2. 在单个 BEGIN IMMEDIATE 事务中二次校验后移动临时文件并登记，
#      与 _commit_chunk 同样的失败清理：任何异常都回滚事务、删除
#      临时文件（及未登记的落地文件），绝不留下已登记的损坏分片。
# 固定的加锁顺序（ID 字典序）排除了 A→B 与 B→A 反向并发时的循环等待。
# ---------------------------------------------------------------------------


def _copy_chunk(
    target_session_id: str,
    source_session_id: str,
    source_start: int,
    source_end: int,
    target_start: int,
) -> dict:
    length = source_end - source_start
    target_end = target_start + length

    with session_locks.hold(source_session_id, target_session_id):
        # 目标：与 PUT 分片相同的存在性/懒过期加载。
        target = _load_session(target_session_id)
        if target["status"] == "completed":
            raise _http(409, "session already completed; no more chunks accepted")

        # 来源：只允许已完成且成品在盘的会话；未完成/成品缺失一律拒绝。
        conn = db.get_conn()
        source = conn.execute(
            "SELECT * FROM upload_sessions WHERE id = ?", (source_session_id,)
        ).fetchone()
        if source is None:
            raise _http(
                404, "source session not found",
                source_session_id=source_session_id,
            )
        if source["status"] != "completed":
            raise _http(
                409,
                "source session is not completed",
                source_session_id=source_session_id,
                source_status=source["status"],
            )
        if not storage.artifact_path(source_session_id).is_file():
            raise _http(
                409,
                "source completed artifact is missing",
                source_session_id=source_session_id,
            )

        # 来源半开区间必须完整落在来源成品大小内。
        if source_end > source["total_size"]:
            raise _http(
                400,
                "source range exceeds source total_size",
                source_start=source_start,
                source_end=source_end,
                source_total_size=source["total_size"],
            )

        # 目标边界：[target_start, target_end) 不越界。
        if target_end > target["total_size"]:
            raise _http(
                400,
                "copied chunk range exceeds target total_size",
                start=target_start,
                end=target_end,
                total_size=target["total_size"],
            )
        # 单片大小限制与 PUT 分片一致。
        if settings.max_chunk_bytes and length > settings.max_chunk_bytes:
            raise _http(
                413,
                "chunk larger than MAX_CHUNK_BYTES",
                max_chunk_bytes=settings.max_chunk_bytes,
            )
        # 与目标已收区间不得重叠（相邻允许）。
        existing = [
            (r["start_offset"], r["end_offset"])
            for r in conn.execute(
                "SELECT start_offset, end_offset FROM chunks "
                "WHERE session_id = ?",
                (target_session_id,),
            ).fetchall()
        ]
        if storage.overlaps_any(target_start, target_end, existing):
            raise _http(
                409,
                "chunk overlaps an already received range",
                start=target_start,
                end=target_end,
            )

        # 预校验全部通过才复制（仍在双锁内）：流式切片到 tmp，
        # 成品被并发截断/替换会在此抛错，临时文件由辅助函数自行清理。
        try:
            tmp_file = storage.copy_artifact_segment(
                source_session_id, source_start, source_end
            )
        except (ValueError, OSError) as exc:
            raise _http(
                409,
                f"cannot copy source artifact segment: {exc}",
                source_session_id=source_session_id,
            ) from exc
        tmp_path = Path(tmp_file)
        final = storage.part_path(target_session_id, target_start, target_end)

        # 单事务二次校验 + 落盘 + 登记：与 _commit_chunk 同一原子性保证。
        # 失败清理必须仍在双锁内完成：否则刚 replace 落地、事务却回滚的
        # part 文件可能与并发 PUT 同路径竞争，误删别人刚登记的分片。
        try:
            with db.immediate() as tx:
                trow = tx.execute(
                    "SELECT * FROM upload_sessions WHERE id = ?",
                    (target_session_id,),
                ).fetchone()
                if trow is None:
                    raise _http(404, "session not found")
                if trow["status"] in ("aborted", "expired") or (
                    trow["status"] == "uploading"
                    and storage.parse_iso(trow["expires_at"]) <= storage.utcnow()
                ):
                    raise _http(410, "session no longer active")
                if trow["status"] == "completed":
                    raise _http(409, "session already completed")
                if target_end > trow["total_size"]:
                    raise _http(400, "chunk range exceeds total_size")

                if storage.overlaps_any(
                    target_start, target_end, _existing_intervals(tx, target_session_id)
                ):
                    raise _http(
                        409,
                        "chunk overlaps an already received range",
                        start=target_start,
                        end=target_end,
                    )

                # 来源资格在事务内复查：来源若已被并发放弃删除，此处拿到
                # 状态变化/成品缺失，事务回滚，不登记任何分片。
                srow = tx.execute(
                    "SELECT status, total_size FROM upload_sessions WHERE id = ?",
                    (source_session_id,),
                ).fetchone()
                if srow is None or srow["status"] != "completed":
                    raise _http(
                        409,
                        "source session is not completed",
                        source_session_id=source_session_id,
                    )
                if source_end > srow["total_size"]:
                    raise _http(400, "source range exceeds source total_size")
                if not storage.artifact_path(source_session_id).is_file():
                    raise _http(
                        409,
                        "source completed artifact is missing",
                        source_session_id=source_session_id,
                    )

                # 事务内移动文件：后续任一 SQL 失败都会回滚并由 except 删除文件。
                final.parent.mkdir(parents=True, exist_ok=True)
                tmp_path.replace(final)

                now = storage.utcnow()
                now_iso = storage.iso(now)
                expires_iso = storage.iso(
                    now + timedelta(seconds=settings.ttl_seconds)
                )
                tx.execute(
                    "INSERT INTO chunks "
                    "(session_id, start_offset, end_offset, size, received_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (target_session_id, target_start, target_end, length, now_iso),
                )
                tx.execute(
                    "UPDATE upload_sessions "
                    "SET bytes_received = bytes_received + ?, "
                    "last_access_at = ?, expires_at = ? WHERE id = ?",
                    (length, now_iso, expires_iso, target_session_id),
                )
        except BaseException:
            # 仍持有两把会话锁：临时文件与可能已 replace 但未登记的落地
            # 文件一并删除，绝不留下已登记/半登记的损坏分片。
            tmp_path.unlink(missing_ok=True)
            final.unlink(missing_ok=True)
            raise

        return _chunk_accepted_payload(target_session_id, target_start, target_end)


@app.post(
    "/api/v1/sessions/{session_id}/copy-chunk",
    response_model=schemas.ChunkResponse,
    tags=["chunks"],
)
def copy_chunk(session_id: str, body: schemas.CopyChunkRequest) -> dict:
    """从已完成来源会话的成品服务端复制一段，登记为本会话的一个分片。"""
    if body.source_session_id == session_id:
        raise _http(
            400,
            "source and target session must differ",
            source_session_id=body.source_session_id,
        )
    return _copy_chunk(
        target_session_id=session_id,
        source_session_id=body.source_session_id,
        source_start=body.source_start,
        source_end=body.source_end,
        target_start=body.target_start,
    )


# ---------------------------------------------------------------------------
# 完成与下载
# ---------------------------------------------------------------------------

@app.post(
    "/api/v1/sessions/{session_id}/complete",
    response_model=schemas.SessionResponse,
    tags=["sessions"],
)
def complete_session(session_id: str) -> dict:
    with session_locks.get(session_id):
        row = _load_session(session_id)
        if row["status"] == "completed":
            return _session_payload(row)

        conn = db.get_conn()
        intervals = sorted(
            (r["start_offset"], r["end_offset"])
            for r in conn.execute(
                "SELECT start_offset, end_offset FROM chunks WHERE session_id = ?",
                (session_id,),
            ).fetchall()
        )
        missing = storage.missing_intervals(intervals, row["total_size"])
        if missing:
            raise _http(
                409,
                "range incomplete; missing ranges remain",
                missing_ranges=[{"start": s, "end": e} for s, e in missing],
            )

        # 拼装 + 整文件 SHA-256。成品先落在 tmp，绝不提前暴露。
        try:
            tmp_file, digest = storage.assemble_artifact(
                session_id, intervals, row["total_size"]
            )
        except (ValueError, FileNotFoundError, OSError) as exc:
            raise _http(409, f"cannot assemble artifact: {exc}") from exc

        if digest != row["sha256_expected"]:
            Path(tmp_file).unlink(missing_ok=True)
            raise _http(
                422,
                "sha256 mismatch",
                expected=row["sha256_expected"],
                actual=digest,
            )

        storage.files_dir().mkdir(parents=True, exist_ok=True)
        # 与状态翻转放进同一个事务窗口：提交成功前成品不可见；
        # 任一步失败都回滚并删除刚落地的文件，保证“失败没有可下载成品”。
        try:
            with db.immediate() as tx:
                current = tx.execute(
                    "SELECT status FROM upload_sessions WHERE id = ?", (session_id,)
                ).fetchone()
                if current is None or current["status"] != "uploading":
                    raise _http(409, "session is not uploadable")
                storage.atomic_publish(tmp_file, session_id)
                now_iso = storage.iso(storage.utcnow())
                tx.execute(
                    "UPDATE upload_sessions SET "
                    "status = 'completed', bytes_received = ?, "
                    "completed_at = ?, last_access_at = ? WHERE id = ?",
                    (row["total_size"], now_iso, now_iso, session_id),
                )
        except BaseException:
            storage.artifact_path(session_id).unlink(missing_ok=True)
            Path(tmp_file).unlink(missing_ok=True)
            raise

        # 提交成功后才删除分片，节省磁盘。
        shutil.rmtree(storage.parts_dir(session_id), ignore_errors=True)
        fresh = conn.execute(
            "SELECT * FROM upload_sessions WHERE id = ?", (session_id,)
        ).fetchone()
        return _session_payload(fresh)


@app.get("/api/v1/sessions/{session_id}/download", tags=["sessions"])
def download_session(session_id: str) -> FileResponse:
    conn = db.get_conn()
    row = conn.execute(
        "SELECT filename FROM upload_sessions WHERE id = ? AND status = 'completed'",
        (session_id,),
    ).fetchone()
    path = storage.artifact_path(session_id)
    if row is None or not path.is_file():
        raise _http(404, "no completed artifact for this session")
    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename=storage.safe_filename(row["filename"]),
    )


# ---------------------------------------------------------------------------
# 受控下载票据
#
# 明文票据 = 32 字节 URL-safe 随机串（约 256 bit 熵），仅在签发响应中返回一次；
# 库里只存 SHA-256(token)。凭票下载在单个 BEGIN IMMEDIATE 事务内完成
# “存在/未撤销/未过期/剩余次数 > 0”全部校验与原子扣减，
# 条件 UPDATE 配合行级写锁保证并发请求不可能突破次数上限。
# ---------------------------------------------------------------------------


def _ticket_payload(row: sqlite3.Row) -> dict:
    return {
        "ticket_id": row["id"],
        "session_id": row["session_id"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "max_downloads": row["max_downloads"],
        "remaining_downloads": row["remaining_downloads"],
        "revoked": bool(row["revoked"]),
        "download_url": f"/api/v1/tickets/{row['id']}/download",
    }


@app.post(
    "/api/v1/sessions/{session_id}/tickets",
    status_code=201,
    response_model=schemas.TicketResponse,
    tags=["tickets"],
)
def create_ticket(
    session_id: str, body: schemas.CreateTicketRequest
) -> dict:
    now = storage.utcnow()
    now_iso = storage.iso(now)
    expires_iso = storage.iso(
        now + timedelta(minutes=body.expires_in_minutes)
    )

    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode("ascii")).hexdigest()
    ticket_id = uuid.uuid4().hex

    # 会话状态/成品检查与票据登记放在同一个 IMMEDIATE 事务，
    # 票据外键指向 completed 会话；不存在成品则不签发。
    with db.immediate() as tx:
        row = tx.execute(
            "SELECT id FROM upload_sessions WHERE id = ? AND status = 'completed'",
            (session_id,),
        ).fetchone()
        if row is None or not storage.artifact_path(session_id).is_file():
            raise _http(409, "no completed artifact for this session")
        tx.execute(
            "INSERT INTO download_tickets ("
            "id, session_id, token_hash, max_downloads, remaining_downloads, "
            "revoked, created_at, expires_at"
            ") VALUES (?, ?, ?, ?, ?, 0, ?, ?)",
            (
                ticket_id,
                session_id,
                token_hash,
                body.max_downloads,
                body.max_downloads,
                now_iso,
                expires_iso,
            ),
        )
        saved = tx.execute(
            "SELECT * FROM download_tickets WHERE id = ?", (ticket_id,)
        ).fetchone()

    payload = _ticket_payload(saved)
    # 票据随机串本身才是下载凭证；id 只用于拼 URL。
    payload["ticket"] = f"{ticket_id}.{token}"
    return payload


def _claim_ticket_download(credential: str) -> tuple[sqlite3.Row, sqlite3.Row]:
    """在单个 IMMEDIATE 事务中校验票据并原子扣减一次。

    返回 (票据行, 会话行)。任一校验失败都抛 HTTPException 且事务回滚，
    绝不扣减次数。条件 UPDATE 的 rowcount 是并发上限的最终防线。
    """
    ticket_id, _, token = credential.partition(".")
    if not ticket_id or not token:
        raise _http(404, "ticket not found")
    token_hash = hashlib.sha256(token.encode("ascii")).hexdigest()
    now_iso = storage.iso(storage.utcnow())

    with db.immediate() as tx:
        ticket = tx.execute(
            "SELECT * FROM download_tickets WHERE id = ? AND token_hash = ?",
            (ticket_id, token_hash),
        ).fetchone()
        if ticket is None:
            raise _http(404, "ticket not found")
        if ticket["revoked"]:
            raise _http(403, "ticket revoked")
        if ticket["expires_at"] <= now_iso:
            raise _http(410, "ticket expired")
        if ticket["remaining_downloads"] <= 0:
            raise _http(429, "ticket download limit reached")

        session = tx.execute(
            "SELECT id, filename FROM upload_sessions "
            "WHERE id = ? AND status = 'completed'",
            (ticket["session_id"],),
        ).fetchone()
        path = storage.artifact_path(ticket["session_id"])
        if session is None or not path.is_file():
            raise _http(404, "no completed artifact for this session")

        # 原子扣减：WHERE 重放全部前置条件，两个并发请求在此串行，
        # 后到者的 remaining_downloads 已是 0，rowcount=0 → 429，绝不超额。
        cur = tx.execute(
            "UPDATE download_tickets SET remaining_downloads = remaining_downloads - 1 "
            "WHERE id = ? AND token_hash = ? AND revoked = 0 "
            "AND expires_at > ? AND remaining_downloads > 0",
            (ticket["id"], token_hash, now_iso),
        )
        if cur.rowcount != 1:
            raise _http(429, "ticket download limit reached")
        ticket = tx.execute(
            "SELECT * FROM download_tickets WHERE id = ?", (ticket["id"],)
        ).fetchone()
    return ticket, session


@app.get("/api/v1/tickets/{ticket}/download", tags=["tickets"])
def download_with_ticket(ticket: str) -> FileResponse:
    _, session = _claim_ticket_download(ticket)
    session_id = session["id"]
    # 校验与扣减已在事务中完成；Range 请求同样走到这里，按一次计数。
    # FileResponse 原生处理 Range（206）与 If-Modified-Since，凭证不合法时
    # 在到达此处之前就已抛错，因此失败请求不会扣减。
    return FileResponse(
        storage.artifact_path(session_id),
        media_type="application/octet-stream",
        filename=storage.safe_filename(session["filename"]),
    )


@app.post(
    "/api/v1/tickets/{ticket}/revoke",
    response_model=schemas.TicketInfo,
    tags=["tickets"],
)
def revoke_ticket(ticket: str) -> dict:
    ticket_id, _, token = ticket.partition(".")
    if not ticket_id or not token:
        raise _http(404, "ticket not found")
    token_hash = hashlib.sha256(token.encode("ascii")).hexdigest()

    with db.immediate() as tx:
        row = tx.execute(
            "SELECT * FROM download_tickets WHERE id = ? AND token_hash = ?",
            (ticket_id, token_hash),
        ).fetchone()
        if row is None:
            raise _http(404, "ticket not found")
        # 幂等：已撤销不报错。
        tx.execute(
            "UPDATE download_tickets SET revoked = 1 WHERE id = ?", (ticket_id,)
        )
        row = tx.execute(
            "SELECT * FROM download_tickets WHERE id = ?", (ticket_id,)
        ).fetchone()
    return _ticket_payload(row)


# ---------------------------------------------------------------------------
# 运维只读汇总
#
# 仅在单个 SQLite 只读快照中做聚合：不调用 _load_session（不触发懒过期）、
# 不更新 last_access_at、不滑动 TTL，也完全不接触文件系统。
# ---------------------------------------------------------------------------

# 固定四种状态及其输出顺序；缺失状态以零补齐，保证响应恒为四行。
_SUMMARY_STATUSES = ("uploading", "completed", "aborted", "expired")


@app.get(
    "/api/v1/storage/summary",
    response_model=schemas.StorageSummaryResponse,
    tags=["ops"],
)
def storage_summary() -> dict:
    # 单条 GROUP BY 语句在只读连接上一次执行完成，四行计数取自同一快照；
    # SQLite 默认每条语句隐式包裹自己的读事务，读期间看到一致的数据库版本。
    stats: dict[str, dict[str, int]] = {
        status: {"session_count": 0, "declared_bytes": 0, "stored_bytes": 0}
        for status in _SUMMARY_STATUSES
    }
    with db.read_only() as conn:
        rows = conn.execute(
            "SELECT status, "
            "COUNT(*) AS session_count, "
            "COALESCE(SUM(total_size), 0) AS declared_bytes, "
            # stored_bytes 仅按状态语义取字节数：
            # uploading 取已收字节，completed 取成品总大小，其余状态恒为 0。
            "COALESCE(SUM(CASE status "
            "WHEN 'uploading' THEN bytes_received "
            "WHEN 'completed' THEN total_size "
            "ELSE 0 END), 0) AS stored_bytes "
            "FROM upload_sessions GROUP BY status"
        ).fetchall()

    for row in rows:
        stats[row["status"]] = {
            "session_count": row["session_count"],
            "declared_bytes": row["declared_bytes"],
            "stored_bytes": row["stored_bytes"],
        }

    statuses = [
        {
            "status": status,
            "session_count": stats[status]["session_count"],
            "declared_bytes": stats[status]["declared_bytes"],
            "stored_bytes": stats[status]["stored_bytes"],
        }
        for status in _SUMMARY_STATUSES
    ]
    total = {
        "status": "total",
        "session_count": sum(item["session_count"] for item in statuses),
        "declared_bytes": sum(item["declared_bytes"] for item in statuses),
        "stored_bytes": sum(item["stored_bytes"] for item in statuses),
    }
    return {"statuses": statuses, "total": total}


# ---------------------------------------------------------------------------
# 健康检查与文档
# ---------------------------------------------------------------------------

@app.get("/health", tags=["meta"])
def health() -> dict:
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")
