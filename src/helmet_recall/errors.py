"""领域错误类型。"""

from __future__ import annotations

from typing import Any, Mapping


class DomainError(Exception):
    """所有领域规则冲突的基类。"""


class AuthorizationError(DomainError):
    """角色无权执行该操作。"""


class StateError(DomainError):
    """当前聚合状态不允许该操作（如向已冻结批次销售）。"""


class UnknownReferenceError(DomainError):
    """引用的型号、批次、通知等不存在。"""


class ConflictingMessageError(DomainError):
    """同编号消息内容不一致，已进入隔离账本。"""

    def __init__(self, event_id: str, existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> None:
        super().__init__(f"消息 {event_id} 与已存事实冲突，已隔离")
        self.event_id = event_id
        self.existing = existing
        self.incoming = incoming
