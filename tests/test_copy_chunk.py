"""POST /sessions/{id}/copy-chunk 服务端分片复制测试。"""
from __future__ import annotations

import hashlib
import threading
import time
from pathlib import Path

from fastapi.testclient import TestClient

from app import db, settings, storage
from app.locks import session_locks


def _create(client: TestClient, data: bytes, name: str = "data.bin") -> str:
    resp = client.post(
        "/api/v1/sessions",
        json={
            "filename": name,
            "total_size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["session_id"]


def _completed_session(client: TestClient, data: bytes, name: str = "src.bin") -> str:
    sid = _create(client, data, name)
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end={len(data)}", content=data
    )
    assert r.status_code == 200, r.text
    r = client.post(f"/api/v1/sessions/{sid}/complete")
    assert r.status_code == 200, r.text
    return sid


def _copy(client: TestClient, sid: str, payload: dict):
    return client.post(f"/api/v1/sessions/{sid}/copy-chunk", json=payload)


# ---------------------------------------------------------------------------
# 成功路径
# ---------------------------------------------------------------------------


def test_copy_chunk_then_complete_with_normal_chunks(client: TestClient) -> None:
    data = bytes(i % 251 for i in range(5000))
    src = _completed_session(client, data)

    sid = _create(client, data, name="copy-dst.bin")
    r = _copy(
        client,
        sid,
        {
            "source_session_id": src,
            "source_start": 1000,
            "source_end": 3000,
            "dest_start": 1000,
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["session_id"] == sid
    assert body["received"] == {"start": 1000, "end": 3000}
    assert body["bytes_received"] == 2000
    assert body["fully_received"] is False
    assert body["missing_ranges"] == [{"start": 0, "end": 1000}, {"start": 3000, "end": 5000}]
    assert "expires_at" in body

    # 落盘分片内容必须与来源区间字节一致。
    assert storage.part_path(sid, 1000, 3000).read_bytes() == data[1000:3000]

    # 剩余部分用普通 PUT 补齐——PUT 行为不变，完成仍走全覆盖 + 整文件摘要。
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end=1000", content=data[:1000]
    )
    assert r.status_code == 200, r.text
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=3000&end=5000", content=data[3000:]
    )
    assert r.status_code == 200, r.text
    done = client.post(f"/api/v1/sessions/{sid}/complete")
    assert done.status_code == 200, done.text
    dl = client.get(f"/api/v1/sessions/{sid}/download")
    assert dl.status_code == 200
    assert dl.content == data

    # 来源会话不受影响，仍可下载。
    src_dl = client.get(f"/api/v1/sessions/{src}/download")
    assert src_dl.status_code == 200
    assert src_dl.content == data


def test_copy_partial_range_to_arbitrary_dest_offset(client: TestClient) -> None:
    src_data = bytes((i * 13 + 7) % 256 for i in range(4000))
    src = _completed_session(client, src_data)

    # 目标文件 = 100 字节前缀 + 来源 [500,2500) + 400 字节后缀。
    target_data = b"A" * 100 + src_data[500:2500] + b"B" * 400
    sid = _create(client, target_data, name="patched.bin")

    r = _copy(
        client,
        sid,
        {
            "source_session_id": src,
            "source_start": 500,
            "source_end": 2500,
            "dest_start": 100,
        },
    )
    assert r.status_code == 200, r.text
    assert r.json()["received"] == {"start": 100, "end": 2100}

    client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end=100", content=target_data[:100]
    )
    client.put(
        f"/api/v1/sessions/{sid}/chunks?start=2100&end=2500",
        content=target_data[2100:],
    )
    done = client.post(f"/api/v1/sessions/{sid}/complete")
    assert done.status_code == 200, done.text
    assert client.get(f"/api/v1/sessions/{sid}/download").content == target_data


# ---------------------------------------------------------------------------
# 来源校验
# ---------------------------------------------------------------------------


def test_copy_from_uploading_source_rejected(client: TestClient) -> None:
    source_data = b"source-bytes" * 100
    src = _create(client, source_data)
    # 只传一部分，来源仍是 uploading。
    client.put(
        f"/api/v1/sessions/{src}/chunks?start=0&end=500",
        content=source_data[:500],
    )

    target_data = b"x" * 5000
    sid = _create(client, target_data)
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 0, "source_end": 500, "dest_start": 0},
    )
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "source session is not completed"

    status = client.get(f"/api/v1/sessions/{sid}").json()
    assert status["bytes_received"] == 0
    assert status["missing_ranges"] == [{"start": 0, "end": 5000}]
    assert not list(storage.parts_dir(sid).glob("*")) if storage.parts_dir(sid).exists() else True


def test_copy_from_unknown_source_404(client: TestClient) -> None:
    sid = _create(client, b"y" * 100)
    r = _copy(
        client,
        sid,
        {
            "source_session_id": "deadbeef" * 4,
            "source_start": 0,
            "source_end": 10,
            "dest_start": 0,
        },
    )
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "source session not found"
    assert client.get(f"/api/v1/sessions/{sid}").json()["bytes_received"] == 0


def test_copy_from_expired_source_rejected(client: TestClient, monkeypatch) -> None:
    source_data = b"z" * 200
    src = _create(client, source_data)
    monkeypatch.setattr(settings, "ttl_seconds", 1)
    client.put(
        f"/api/v1/sessions/{src}/chunks?start=0&end=200", content=source_data
    )
    time.sleep(1.1)
    from app.cleanup import run_cleanup

    assert src in run_cleanup()

    sid = _create(client, b"q" * 100)
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 0, "source_end": 100, "dest_start": 0},
    )
    assert r.status_code == 409
    assert "expired" in r.json()["detail"]["error"]


def test_copy_from_hard_deleted_source_404(client: TestClient) -> None:
    src = _completed_session(client, b"abc" * 100)
    assert client.delete(f"/api/v1/sessions/{src}").status_code == 204
    sid = _create(client, b"d" * 300)
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 0, "source_end": 100, "dest_start": 0},
    )
    assert r.status_code == 404
    assert client.get(f"/api/v1/sessions/{sid}").json()["bytes_received"] == 0


def test_copy_when_source_artifact_missing_rejected(client: TestClient) -> None:
    src_data = b"artifact" * 50
    src = _completed_session(client, src_data)
    # 数据库仍标记 completed，但成品被外部删走。
    storage.artifact_path(src).unlink()

    sid = _create(client, src_data)
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 0, "source_end": 100, "dest_start": 0},
    )
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "source completed artifact is missing"
    status = client.get(f"/api/v1/sessions/{sid}").json()
    assert status["bytes_received"] == 0
    assert not list(storage.tmp_dir().glob("recv-*.part"))


# ---------------------------------------------------------------------------
# 区间 / 边界 / 大小 / 重叠
# ---------------------------------------------------------------------------


def test_copy_source_range_out_of_bounds_400(client: TestClient) -> None:
    src = _completed_session(client, b"s" * 100)
    sid = _create(client, b"t" * 500)
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 50, "source_end": 101, "dest_start": 0},
    )
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "source range exceeds source total_size"
    assert client.get(f"/api/v1/sessions/{sid}").json()["bytes_received"] == 0


def test_copy_dest_range_out_of_bounds_400(client: TestClient) -> None:
    src = _completed_session(client, b"s" * 100)
    sid = _create(client, b"t" * 120)
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 0, "source_end": 100, "dest_start": 50},
    )
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "copied chunk range exceeds target total_size"
    assert client.get(f"/api/v1/sessions/{sid}").json()["bytes_received"] == 0


def test_copy_empty_or_invalid_range_422(client: TestClient) -> None:
    src = _completed_session(client, b"s" * 100)
    sid = _create(client, b"t" * 100)
    # 空区间 source_start == source_end
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 10, "source_end": 10, "dest_start": 0},
    )
    assert r.status_code == 422
    # 反向区间
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 20, "source_end": 10, "dest_start": 0},
    )
    assert r.status_code == 422
    # 负偏移 / 缺字段
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": -1, "source_end": 10, "dest_start": 0},
    )
    assert r.status_code == 422
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 0, "source_end": 10},
    )
    assert r.status_code == 422
    assert client.get(f"/api/v1/sessions/{sid}").json()["bytes_received"] == 0


def test_copy_chunk_larger_than_max_413(client: TestClient, monkeypatch) -> None:
    src = _completed_session(client, b"s" * 1000)
    sid = _create(client, b"t" * 1000)
    monkeypatch.setattr(settings, "max_chunk_bytes", 100)
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 0, "source_end": 500, "dest_start": 0},
    )
    assert r.status_code == 413
    assert r.json()["detail"]["max_chunk_bytes"] == 100
    assert client.get(f"/api/v1/sessions/{sid}").json()["bytes_received"] == 0


def test_copy_overlapping_received_range_409(client: TestClient) -> None:
    data = b"o" * 1000
    src = _completed_session(client, data)
    sid = _create(client, data)
    r = client.put(
        f"/api/v1/sessions/{sid}/chunks?start=100&end=200", content=data[100:200]
    )
    assert r.status_code == 200

    # 部分重叠
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 0, "source_end": 150, "dest_start": 50},
    )
    assert r.status_code == 409
    assert "overlaps" in r.json()["detail"]["error"]

    # 完全重叠
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 100, "source_end": 200, "dest_start": 100},
    )
    assert r.status_code == 409

    # 相邻不重叠：[200,300) 合法
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 200, "source_end": 300, "dest_start": 200},
    )
    assert r.status_code == 200, r.text
    status = client.get(f"/api/v1/sessions/{sid}").json()
    assert status["bytes_received"] == 200


def test_copy_into_completed_target_409(client: TestClient) -> None:
    data = b"f" * 100
    src = _completed_session(client, data)
    dst = _completed_session(client, data)
    r = _copy(
        client,
        dst,
        {"source_session_id": src, "source_start": 0, "source_end": 10, "dest_start": 0},
    )
    assert r.status_code == 409
    assert "already completed" in r.json()["detail"]["error"]


def test_copy_same_session_rejected(client: TestClient) -> None:
    # 目标未完成时把自己当来源：来源不是 completed。
    sid = _create(client, b"m" * 100)
    client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end=100", content=b"m" * 100
    )
    r = _copy(
        client,
        sid,
        {"source_session_id": sid, "source_start": 0, "source_end": 10, "dest_start": 0},
    )
    assert r.status_code == 409

    # 自己已完成时：目标 completed 拒绝。
    assert client.post(f"/api/v1/sessions/{sid}/complete").status_code == 200
    r = _copy(
        client,
        sid,
        {"source_session_id": sid, "source_start": 0, "source_end": 10, "dest_start": 0},
    )
    assert r.status_code == 409


def test_copy_into_expired_target_410(client: TestClient, monkeypatch) -> None:
    src = _completed_session(client, b"s" * 100)
    monkeypatch.setattr(settings, "ttl_seconds", 1)
    sid = _create(client, b"t" * 100)
    client.put(
        f"/api/v1/sessions/{sid}/chunks?start=0&end=10", content=b"t" * 10
    )
    time.sleep(1.1)
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 0, "source_end": 10, "dest_start": 10},
    )
    assert r.status_code == 410


def test_copy_unknown_target_404(client: TestClient) -> None:
    src = _completed_session(client, b"s" * 10)
    r = _copy(
        client,
        "deadbeef" * 4,
        {"source_session_id": src, "source_start": 0, "source_end": 10, "dest_start": 0},
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# 失败原子性 & 并发
# ---------------------------------------------------------------------------


def test_copy_tmp_failure_registers_nothing(client: TestClient, monkeypatch) -> None:
    data = b"c" * 500
    src = _completed_session(client, data)
    sid = _create(client, data)

    # 模拟临时文件在事务内落地失败（磁盘错误）：必须回滚、不登记、不留文件。
    orig_replace = Path.replace

    def boom(self: Path, target: Path):
        if str(self).startswith(str(storage.tmp_dir())):
            raise OSError("simulated move failure")
        return orig_replace(self, target)

    monkeypatch.setattr(Path, "replace", boom)
    # TestClient 默认会把服务端异常向上抛；这里需要真实的 500 响应。
    no_raise = TestClient(client.app, raise_server_exceptions=False)
    r = no_raise.post(
        f"/api/v1/sessions/{sid}/copy-chunk",
        json={
            "source_session_id": src,
            "source_start": 0,
            "source_end": 500,
            "dest_start": 0,
        },
    )
    assert r.status_code == 500
    monkeypatch.undo()

    # 数据库：无分片行、已收字节不增加。
    row = db.get_conn().execute(
        "SELECT bytes_received FROM upload_sessions WHERE id = ?", (sid,)
    ).fetchone()
    assert row["bytes_received"] == 0
    assert db.get_conn().execute(
        "SELECT COUNT(*) AS n FROM chunks WHERE session_id = ?", (sid,)
    ).fetchone()["n"] == 0
    # 文件系统：无已落地分片、无残留临时文件。
    assert not any(storage.parts_dir(sid).glob("*")) if storage.parts_dir(sid).exists() else True
    assert not list(storage.tmp_dir().glob("recv-*.part"))

    # 恢复后同一复制可以正常成功并最终完成。
    r = _copy(
        client,
        sid,
        {"source_session_id": src, "source_start": 0, "source_end": 500, "dest_start": 0},
    )
    assert r.status_code == 200, r.text
    assert client.post(f"/api/v1/sessions/{sid}/complete").status_code == 200
    assert client.get(f"/api/v1/sessions/{sid}/download").content == data


def test_copy_blocks_concurrent_source_delete_and_target_abort(
    client: TestClient, monkeypatch
) -> None:
    data = b"locked-range" * 200
    src = _completed_session(client, data)
    sid = _create(client, data)

    entered = threading.Event()
    orig_copy = storage.copy_artifact_range

    def slow_copy(*args, **kwargs):
        entered.set()
        time.sleep(0.4)
        return orig_copy(*args, **kwargs)

    monkeypatch.setattr(storage, "copy_artifact_range", slow_copy)

    result: dict = {}

    def do_copy() -> None:
        result["resp"] = _copy(
            client,
            sid,
            {
                "source_session_id": src,
                "source_start": 0,
                "source_end": 500,
                "dest_start": 0,
            },
        )

    t = threading.Thread(target=do_copy)
    t.start()
    try:
        assert entered.wait(2)
        # 复制进行中：来源删除与目标放弃都必须阻塞在会话锁上。
        assert session_locks.get(src).acquire(blocking=False) is False
        assert session_locks.get(sid).acquire(blocking=False) is False
    finally:
        t.join(5)
    assert not t.is_alive()
    assert result["resp"].status_code == 200, result["resp"].text

    # 复制结束后锁释放，来源可正常删除（目标分片已是自有副本，不受影响）。
    assert client.delete(f"/api/v1/sessions/{src}").status_code == 204
    assert storage.part_path(sid, 0, 500).read_bytes() == data[:500]


def test_concurrent_identical_copy_only_one_wins(client: TestClient) -> None:
    data = b"z" * 400
    src = _completed_session(client, data)
    sid = _create(client, data)
    results: list[int] = []
    barrier = threading.Barrier(6)

    def worker() -> None:
        barrier.wait()
        r = _copy(
            client,
            sid,
            {
                "source_session_id": src,
                "source_start": 0,
                "source_end": 400,
                "dest_start": 0,
            },
        )
        results.append(r.status_code)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)
        assert not t.is_alive()

    assert results.count(200) == 1
    assert all(code == 409 for code in results if code != 200)
    status = client.get(f"/api/v1/sessions/{sid}").json()
    assert status["bytes_received"] == 400
    assert status["fully_received"] is True


def test_concurrent_reverse_copies_do_not_deadlock(client: TestClient) -> None:
    data = bytes(i % 251 for i in range(10_000))
    sources = [_completed_session(client, data, name=f"src{i}.bin") for i in range(2)]
    targets = [_create(client, data, name=f"dst{i}.bin") for i in range(2)]

    n_threads = 8
    barrier = threading.Barrier(n_threads)
    results: list[int] = []

    def worker(index: int) -> None:
        src = sources[index % 2]
        sid = targets[index % 2]
        start = (index // 2) * 100
        end = start + 100  # 各线程区间互不相交
        barrier.wait()
        r = _copy(
            client,
            sid,
            {
                "source_session_id": src,
                "source_start": start,
                "source_end": end,
                "dest_start": start,
            },
        )
        results.append(r.status_code)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
        assert not t.is_alive(), "copy-chunk deadlocked"

    assert len(results) == n_threads
    assert results.count(200) == n_threads

    # 两个目标都能补齐、完成并通过整文件摘要校验。
    for sid in targets:
        r = client.put(
            f"/api/v1/sessions/{sid}/chunks?start=400&end=10000",
            content=data[400:],
        )
        assert r.status_code == 200, r.text
        done = client.post(f"/api/v1/sessions/{sid}/complete")
        assert done.status_code == 200, done.text
        assert client.get(f"/api/v1/sessions/{sid}/download").content == data
