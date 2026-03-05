"""
SpiderManager SDK 客户端

对外暴露三个核心方法::

    sdk.init(api_url=None, task_id=None)
    sdk.insert(table_name, data)
    sdk.flush()

内部协调 Buffer ↔ Transport 之间的数据流转，
并通过 atexit + signal 保证容器销毁前数据完整性。
"""

from __future__ import annotations

import atexit
import logging
import os
import signal
import sys
from collections import defaultdict
from typing import Any

from spidermanager_sdk.buffer import BufferEntry, FlushBuffer
from spidermanager_sdk.transport import HttpTransport

logger = logging.getLogger("spidermanager_sdk.client")

# ── 默认缓冲参数 ──
_DEFAULT_BUFFER_SIZE: int = 20
_DEFAULT_FLUSH_INTERVAL: float = 3.0


class SpiderManagerClient:
    """
    SDK 主客户端。

    职责：
    1. 管理初始化配置（api_url / task_id）
    2. 维护内存 flush buffer
    3. 注册退出钩子确保数据不丢失
    4. 将用户的 ``insert()`` 调用转化为最终的 HTTP POST
    """

    def __init__(self) -> None:
        self._api_url: str = ""
        self._task_id: str = ""
        self._initialized: bool = False
        self._transport: HttpTransport | None = None
        self._buffer: FlushBuffer | None = None
        self._atexit_registered: bool = False

    # ──────────────────────────────────────────────
    # 公开 API
    # ──────────────────────────────────────────────

    def init(
        self,
        api_url: str | None = None,
        task_id: str | None = None,
        *,
        buffer_size: int = _DEFAULT_BUFFER_SIZE,
        flush_interval: float = _DEFAULT_FLUSH_INTERVAL,
    ) -> None:
        """
        初始化 SDK。

        Parameters
        ----------
        api_url : str | None
            SpiderManager 后端地址。若不传，自动读取环境变量 ``SPIDER_API_URL``。
        task_id : str | None
            当前任务 ID。若不传，自动读取环境变量 ``TASK_ID``。
        buffer_size : int
            缓冲数据条数阈值，默认 20。
        flush_interval : float
            时间窗口（秒），默认 3.0。
        """
        # ── 1. 解析配置 ──
        self._api_url = api_url or os.environ.get("SPIDER_API_URL", "")
        self._task_id = task_id or os.environ.get("TASK_ID", "")

        if not self._api_url:
            raise ValueError(
                "api_url 未指定且环境变量 SPIDER_API_URL 未设置，"
                "请通过 sdk.init(api_url='...') 或设置环境变量来配置后端地址。"
            )
        if not self._task_id:
            raise ValueError(
                "task_id 未指定且环境变量 TASK_ID 未设置，"
                "请通过 sdk.init(task_id='...') 或设置环境变量来配置任务 ID。"
            )

        # 去除尾部斜杠
        self._api_url = self._api_url.rstrip("/")

        # ── 2. 初始化传输层 ──
        self._transport = HttpTransport(api_url=self._api_url, task_id=self._task_id)
        self._transport.open()

        # ── 3. 初始化缓冲区 ──
        self._buffer = FlushBuffer(
            max_size=buffer_size,
            flush_interval=flush_interval,
            on_flush=self._handle_flush,
        )
        self._buffer.start()

        # ── 4. 注册退出钩子 ──
        self._register_exit_hooks()

        self._initialized = True
        logger.info(
            "SDK 初始化完成: api_url=%s, task_id=%s, buffer_size=%d, interval=%.1fs",
            self._api_url, self._task_id, buffer_size, flush_interval,
        )

    def insert(self, table_name: str, data: dict[str, Any] | list[dict[str, Any]]) -> None:
        """
        将采集数据提交到缓冲区。

        Parameters
        ----------
        table_name : str
            目标数据表名。
        data : dict | list[dict]
            单条或多条数据记录。
        """
        self._ensure_initialized()

        if isinstance(data, dict):
            data = [data]

        if not data:
            return

        assert self._buffer is not None
        for record in data:
            self._buffer.add(BufferEntry(table_name=table_name, data=record))

    def flush(self) -> None:
        """
        手动强制将缓冲区中所有数据立即上报到后端。
        通常在程序结束前调用，或在需要即时可见性时使用。
        """
        if self._buffer:
            self._buffer.flush()

    def shutdown(self) -> None:
        """
        优雅关闭 SDK：停止定时器 → flush 剩余数据 → 关闭 HTTP 连接。
        """
        logger.info("SDK 正在关闭...")
        if self._buffer:
            self._buffer.stop()
        if self._transport:
            self._transport.close()
        self._initialized = False
        logger.info("SDK 已关闭")

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    @property
    def pending_count(self) -> int:
        """当前缓冲区中等待上报的数据条数。"""
        if self._buffer:
            return self._buffer.pending_count
        return 0

    # ──────────────────────────────────────────────
    # 内部方法
    # ──────────────────────────────────────────────

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError(
                "SDK 尚未初始化，请先调用 sdk.init() 进行配置。"
            )

    def _handle_flush(self, entries: list[BufferEntry]) -> None:
        """
        FlushBuffer 的回调。
        将 entries 按 table_name 分组，逐组上报。
        """
        if not self._transport:
            logger.warning("Transport 未初始化，%d 条数据被丢弃", len(entries))
            return

        # 按表名分组
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for entry in entries:
            grouped[entry.table_name].append(entry.data)

        for table_name, records in grouped.items():
            success = self._transport.send_batch(table_name, records)
            if not success:
                logger.error(
                    "表 '%s' 的 %d 条数据上报失败", table_name, len(records),
                )

    def _register_exit_hooks(self) -> None:
        """
        注册 atexit 和 SIGTERM 信号处理，确保容器销毁前 flush 数据。
        """
        if self._atexit_registered:
            return

        # atexit: 正常退出时 flush
        atexit.register(self.shutdown)

        # SIGTERM: Docker 在 stop 容器时发送此信号
        # 仅在主线程中注册，且仅支持 POSIX 系统
        if _can_register_signal():
            _original_sigterm = signal.getsignal(signal.SIGTERM)

            def _sigterm_handler(signum: int, frame: object) -> None:
                logger.info("收到 SIGTERM 信号，正在 flush 缓冲数据...")
                self.shutdown()
                # 调用原始 handler（如果有）
                if callable(_original_sigterm):
                    _original_sigterm(signum, frame)
                sys.exit(0)

            signal.signal(signal.SIGTERM, _sigterm_handler)

        self._atexit_registered = True
        logger.debug("退出钩子已注册")


def _can_register_signal() -> bool:
    """
    判断当前环境是否允许注册信号处理。
    只有主线程才能注册 signal handler。
    """
    import threading
    return threading.current_thread() is threading.main_thread()
