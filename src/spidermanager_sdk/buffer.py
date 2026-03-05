"""
异步缓冲区模块

维护内存 buffer，在满足条件（数据量阈值 或 时间窗口）时
触发一次批量 HTTP POST，减少爬虫端网络 IO 开销。
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Callable

logger = logging.getLogger("spidermanager_sdk.buffer")


@dataclass
class BufferEntry:
    """
    单条缓冲记录，对应一次 sdk.insert() 调用。
    """
    table_name: str
    data: dict[str, object]


@dataclass
class FlushBuffer:
    """
    线程安全的内存缓冲区。

    Parameters
    ----------
    max_size : int
        缓冲数据条数阈值，达到后触发 flush。
    flush_interval : float
        时间窗口（秒），即使未达到 max_size 也触发 flush。
    on_flush : Callable[[list[BufferEntry]], None]
        实际的 flush 回调，由 Client 层注入。
    """
    max_size: int = 20
    flush_interval: float = 3.0
    on_flush: Callable[[list[BufferEntry]], None] | None = None

    # ── 内部状态 ──
    _entries: list[BufferEntry] = field(default_factory=list, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _timer: threading.Timer | None = field(default=None, init=False, repr=False)
    _started: bool = field(default=False, init=False, repr=False)

    # ──────────────────────────────────────────────
    # 公开 API
    # ──────────────────────────────────────────────

    def start(self) -> None:
        """启动定时 flush 循环。"""
        if self._started:
            return
        self._started = True
        self._schedule_timer()

    def stop(self) -> None:
        """停止定时器并强制 flush 剩余数据。"""
        self._started = False
        self._cancel_timer()
        self.flush()

    def add(self, entry: BufferEntry) -> None:
        """
        向缓冲区追加一条记录。
        如积累到阈值，立即触发 flush。
        """
        with self._lock:
            self._entries.append(entry)
            current_size = len(self._entries)

        if current_size >= self.max_size:
            self.flush()

    def flush(self) -> None:
        """
        将缓冲区中所有数据取出，交给 on_flush 回调处理。
        本方法线程安全，可被定时器 / 阈值触发 / atexit 多次调用。
        """
        with self._lock:
            if not self._entries:
                return
            batch = self._entries.copy()
            self._entries.clear()

        if not self.on_flush:
            logger.warning("on_flush 回调未设置，%d 条数据被丢弃", len(batch))
            return

        try:
            self.on_flush(batch)
        except Exception:
            logger.exception("flush 回调执行失败，%d 条数据可能丢失", len(batch))

    @property
    def pending_count(self) -> int:
        """当前缓冲区中等待 flush 的数据条数。"""
        with self._lock:
            return len(self._entries)

    # ──────────────────────────────────────────────
    # 内部辅助
    # ──────────────────────────────────────────────

    def _schedule_timer(self) -> None:
        """注册下一次定时 flush。"""
        if not self._started:
            return
        self._timer = threading.Timer(self.flush_interval, self._on_timer_tick)
        self._timer.daemon = True
        self._timer.start()

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _on_timer_tick(self) -> None:
        """定时器触发：flush 后重新调度。"""
        self.flush()
        self._schedule_timer()
