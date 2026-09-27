"""可控时钟：停售期限、通知响应期与逾期升级都以该时钟为准。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

_CN_TZ = timezone(timedelta(hours=8))


class ControlledClock:
    """可显式推进的时钟，默认从 2026-09-26 09:00+08:00 开始。"""

    def __init__(self, start: Optional[datetime] = None) -> None:
        if start is None:
            start = datetime(2026, 9, 26, 9, 0, tzinfo=_CN_TZ)
        if start.tzinfo is None:
            raise ValueError("时钟初始时间必须携带时区")
        self._now = start.astimezone(_CN_TZ)

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> datetime:
        if delta.total_seconds() < 0:
            raise ValueError("时钟只能向前推进")
        self._now += delta
        return self._now

    def set_now(self, value: datetime) -> None:
        if value.tzinfo is None:
            raise ValueError("时间必须携带时区")
        value = value.astimezone(_CN_TZ)
        if value < self._now:
            raise ValueError("时钟不能回拨")
        self._now = value

    def iso(self) -> str:
        return self._now.isoformat()
