"""按会话互斥的锁。

同一个会话的分片提交、完成、放弃、懒过期与后台清理互斥，
不同会话之间完全并行。锁本身只是协调者：真正的区间冲突判定
仍然在 BEGIN IMMEDIATE 事务中完成。
"""
from __future__ import annotations

import threading


class SessionLocks:
    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def get(self, session_id: str) -> threading.Lock:
        with self._guard:
            lock = self._locks.get(session_id)
            if lock is None:
                lock = threading.Lock()
                self._locks[session_id] = lock
            return lock

    def discard(self, session_id: str) -> None:
        """会话进入终态后移除锁条目。

        调用方必须已经不持有该锁；若锁正好被其他线程持有则保留条目，
        下一个生命周期的访问会重新拿到同一个锁，不影响正确性。
        """
        with self._guard:
            lock = self._locks.get(session_id)
            if lock is not None and not lock.locked():
                self._locks.pop(session_id, None)


session_locks = SessionLocks()
