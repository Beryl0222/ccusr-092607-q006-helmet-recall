"""可控时钟。

停售、通知、消费者响应和逾期升级都依赖时间判断；
测试与联调使用手动时钟，时间只能显式推进，不能回拨。
"""

from __future__ import annotations

from datetime import datetime, timedelta


class ManualClock:
    """手动推进的时钟，保证调度行为可重现。"""

    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None or start.utcoffset() is None:
            raise ValueError("时钟起点必须携带时区")
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> datetime:
        if delta.total_seconds() < 0:
            raise ValueError("时钟不能回拨")
        self._now = self._now + delta
        return self._now
