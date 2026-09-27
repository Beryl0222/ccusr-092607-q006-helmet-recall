"""事件存储：编号幂等、内容冲突隔离、按聚合顺序追加，支持 JSONL 落盘重放。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .contracts import validate_event
from .errors import ConflictingMessageError, DomainError


@dataclass(frozen=True)
class QuarantinedMessage:
    event_id: str
    existing: dict[str, Any]
    incoming: dict[str, Any]


def _fingerprint(event: Mapping[str, Any]) -> str:
    """业务指纹：类型、聚合与载荷。版本与时间不参与，重发消息这些字段必然过时。"""
    return json.dumps(
        {k: event.get(k) for k in ("event_type", "aggregate_type", "aggregate_id", "payload")},
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    )


class EventStore:
    """内存事件流，可选绑定 JSONL 文件持久化。

    - 同一 ``event_id`` 且内容完全一致：幂等忽略，返回已存事件。
    - 同一 ``event_id`` 但内容不同：记入隔离账本并抛 ``ConflictingMessageError``。
    - 每个聚合的 ``version`` 必须从 1 连续递增（乐观并发）。
    """

    def __init__(self, schema: Mapping[str, Any], path: Optional[str | Path] = None) -> None:
        self._schema = schema
        self._events: list[dict[str, Any]] = []
        self._by_id: dict[str, dict[str, Any]] = {}
        self._versions: dict[str, int] = {}
        self._quarantine: list[QuarantinedMessage] = []
        self._path = Path(path) if path is not None else None
        if self._path is not None and self._path.exists():
            self._load()

    @property
    def events(self) -> Sequence[dict[str, Any]]:
        return self._events

    @property
    def quarantine(self) -> Sequence[QuarantinedMessage]:
        return self._quarantine

    def events_for(self, aggregate_id: str) -> list[dict[str, Any]]:
        return [e for e in self._events if e["aggregate_id"] == aggregate_id]

    def next_version(self, aggregate_id: str) -> int:
        return self._versions.get(aggregate_id, 0) + 1

    def contains(self, event_id: str) -> bool:
        return event_id in self._by_id

    def ingest_duplicate(self, candidate: Mapping[str, Any]) -> Optional[dict[str, Any]]:
        """状态校验前的前置判定。

        返回已存事件表示重复投递（调用方应幂等返回）；
        业务指纹不同则立刻登记隔离并抛出冲突；未见编号返回 None。
        """
        event_id = candidate["event_id"]
        existing = self._by_id.get(event_id)
        if existing is None:
            return None
        if _fingerprint(existing) == _fingerprint(candidate):
            return existing
        record = QuarantinedMessage(event_id, dict(existing), dict(candidate))
        self._quarantine.append(record)
        self._persist_kind("quarantine", {"event_id": event_id, "existing": existing, "incoming": dict(candidate)})
        raise ConflictingMessageError(event_id, existing, candidate)

    def append(self, event: Mapping[str, Any]) -> dict[str, Any]:
        event = dict(event)
        issues = validate_event(event, self._schema)
        if issues:
            raise DomainError(f"事件不符合契约: {issues[0].field} {issues[0].code}")
        event_id = event["event_id"]
        existing = self._by_id.get(event_id)
        if existing is not None:
            if _fingerprint(existing) == _fingerprint(event):
                return existing
            record = QuarantinedMessage(event_id, dict(existing), dict(event))
            self._quarantine.append(record)
            self._persist_kind("quarantine", {"event_id": event_id, "existing": existing, "incoming": event})
            raise ConflictingMessageError(event_id, existing, event)
        aggregate_id = event["aggregate_id"]
        expected = self._versions.get(aggregate_id, 0) + 1
        if event["version"] != expected:
            raise DomainError(
                f"聚合 {aggregate_id} 版本冲突: 期望 {expected}, 收到 {event['version']}"
            )
        stored = dict(event)
        self._events.append(stored)
        self._by_id[event_id] = stored
        self._versions[aggregate_id] = expected
        self._persist_kind("event", stored)
        return stored

    def _persist_kind(self, kind: str, body: Mapping[str, Any]) -> None:
        if self._path is None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"kind": kind, **body}, ensure_ascii=False) + "\n")

    def _load(self) -> None:
        for line in self._path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            kind = record["kind"]
            if kind == "event":
                event = {k: v for k, v in record.items() if k != "kind"}
                self._events.append(event)
                self._by_id[event["event_id"]] = event
                aggregate_id = event["aggregate_id"]
                self._versions[aggregate_id] = max(self._versions.get(aggregate_id, 0), event["version"])
            elif kind == "quarantine":
                self._quarantine.append(
                    QuarantinedMessage(record["event_id"], record["existing"], record["incoming"])
                )
