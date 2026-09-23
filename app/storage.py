"""文件系统布局、区间算法与拼装工具。"""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence

from . import settings


# ---------------------------------------------------------------------------
# 时间：统一 UTC，ISO-8601 带 Z 后缀，字符串可直接做字典序比较。
# ---------------------------------------------------------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def future_expiry() -> tuple[str, str]:
    """返回 (当前时间, 过期时间) 的 ISO 字符串。"""
    now = utcnow()
    return iso(now), iso(now + timedelta(seconds=settings.ttl_seconds))


# ---------------------------------------------------------------------------
# 目录布局
#
#   DATA_DIR/
#     db 文件（默认 resumable.db*）
#     files/<session_id>                完成后的成品（原子 rename 落地）
#     parts/<session_id>/<start>-<end>  已收分片
#     tmp/<随机名>                       正在接收/拼装的临时文件
# ---------------------------------------------------------------------------

def files_dir() -> Path:
    return settings.data_dir / "files"


def parts_dir(session_id: str) -> Path:
    return settings.data_dir / "parts" / session_id


def part_path(session_id: str, start: int, end: int) -> Path:
    return parts_dir(session_id) / f"{start}-{end}"


def artifact_path(session_id: str) -> Path:
    return files_dir() / session_id


def tmp_dir() -> Path:
    return settings.data_dir / "tmp"


def ensure_dirs() -> None:
    for d in (files_dir(), settings.data_dir / "parts", tmp_dir()):
        d.mkdir(parents=True, exist_ok=True)


def new_temp_file() -> Path:
    """在 tmp 目录下创建一个新的临时文件（已存在、已关闭）。"""
    fd, name = tempfile.mkstemp(dir=tmp_dir(), prefix="recv-", suffix=".part")
    os.close(fd)
    return Path(name)


def remove_session_tree(session_id: str) -> None:
    """删除会话的分片目录与成品（若存在），用于放弃/过期清理。"""
    shutil.rmtree(parts_dir(session_id), ignore_errors=True)
    artifact = artifact_path(session_id)
    try:
        artifact.unlink()
    except FileNotFoundError:
        pass


def sweep_old_tempfiles(older_than: datetime) -> None:
    """删除早于给定时间的临时文件（崩溃残留）。"""
    for entry in tmp_dir().glob("*"):
        try:
            mtime = datetime.fromtimestamp(entry.stat().st_mtime, tz=timezone.utc)
        except FileNotFoundError:
            continue
        if mtime < older_than and entry.is_file():
            try:
                entry.unlink()
            except FileNotFoundError:
                pass


# ---------------------------------------------------------------------------
# 区间工具：区间一律采用半开 [start, end)。
# ---------------------------------------------------------------------------

def merge_intervals(intervals: Iterable[Sequence[int]]) -> list[tuple[int, int]]:
    """把可能相邻但不重叠的区间合并为连续区间列表。"""
    ordered = sorted(((int(s), int(e)) for s, e in intervals), key=lambda x: x[0])
    merged: list[tuple[int, int]] = []
    for start, end in ordered:
        if merged and start <= merged[-1][1]:
            if end > merged[-1][1]:
                merged[-1] = (merged[-1][0], end)
        else:
            merged.append((start, end))
    return merged


def missing_intervals(
    received: Iterable[Sequence[int]], total_size: int
) -> list[tuple[int, int]]:
    """根据已收区间计算 [0, total_size) 内的缺失区间。"""
    missing: list[tuple[int, int]] = []
    cursor = 0
    for start, end in merge_intervals(received):
        if start > cursor:
            missing.append((cursor, start))
        if end > cursor:
            cursor = end
    if cursor < total_size:
        missing.append((cursor, total_size))
    return missing


def overlaps_any(
    start: int,
    end: int,
    existing: Iterable[Sequence[int]],
) -> bool:
    """半开区间相交判定：start < other.end 且 other.start < end。"""
    return any(start < o_end and o_start < end for o_start, o_end in existing)


# ---------------------------------------------------------------------------
# 文件名与拼装
# ---------------------------------------------------------------------------

_SAFE_NAME = re.compile(r"[^A-Za-z0-9._\-一-鿿]+")


def safe_filename(name: str) -> str:
    """去掉路径分量与危险字符，只保留安全的文件名部分。"""
    name = name.replace("\\", "/").split("/")[-1]
    name = name.lstrip(".")
    cleaned = _SAFE_NAME.sub("_", name).strip("._ ")
    return cleaned or "file"


def assemble_artifact(
    session_id: str, chunks: Sequence[tuple[int, int]], total_size: int
) -> tuple[str, str]:
    """按偏移顺序把分片拼装成成品。

    成品先写到 tmp 下的临时文件，再由调用方做 SHA-256 校验；
    校验通过后由调用方原子 rename 到 files 目录。返回临时文件路径。

    每个分片在读取前再次核对磁盘大小，防止文件被外部破坏。
    """
    tmp_out = new_temp_file()
    seen = 0
    sha = hashlib.sha256()
    try:
        with tmp_out.open("wb") as out:
            for start, end in chunks:
                if start != seen:
                    raise ValueError(f"chunk gap at offset {seen}, chunk starts at {start}")
                src = part_path(session_id, start, end)
                size = src.stat().st_size
                if size != end - start:
                    raise ValueError(f"chunk {start}-{end} size on disk is {size}")
                with src.open("rb") as fh:
                    while True:
                        block = fh.read(1024 * 1024)
                        if not block:
                            break
                        out.write(block)
                        sha.update(block)
                seen = end
        if seen != total_size:
            raise ValueError(f"assembled {seen} bytes, expected {total_size}")
    except Exception:
        tmp_out.unlink(missing_ok=True)
        raise
    return str(tmp_out), sha.hexdigest()


def atomic_publish(tmp_file: str, session_id: str) -> None:
    """把拼装好的临时文件原子替换为成品文件。"""
    os.replace(tmp_file, artifact_path(session_id))
