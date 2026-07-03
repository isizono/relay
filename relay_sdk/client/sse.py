"""SSE frame パーサ（relay-v2-sdk.md §4.2）。

`httpx` の streaming line iterator から SSE frame（`event:` / `id:` / `data:` / comment）を
組み立てる。frame 境界は空行。comment 行（`:` 始まり、relay の `: keepalive`）も
`SSEFrame(kind="comment")` として yield し、呼び出し側（`Subscription.receive`）が
keepalive 契機の保守処理（lease renew / ack flush）を回せるようにする。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator


@dataclass(frozen=True)
class SSEFrame:
    kind: str  # "event" | "comment"
    event: str | None = None
    id: str | None = None
    data: str | None = None
    comment: str | None = None


def parse_sse_lines(lines: Iterable[str]) -> Iterator[SSEFrame]:
    """SSE の行 iterator を frame に組み立てて yield する。

    - 空行で 1 frame 確定。`data:` を持つ frame は `kind="event"`。
    - `:` 始まりの comment 行は即時に `kind="comment"` frame として yield する
      （keepalive 検出を遅延させないため、frame 境界を待たない）。
    """
    event_type: str | None = None
    event_id: str | None = None
    data_parts: list[str] = []

    def flush() -> SSEFrame | None:
        nonlocal event_type, event_id, data_parts
        if data_parts:
            frame = SSEFrame(
                kind="event",
                event=event_type,
                id=event_id,
                data="\n".join(data_parts),
            )
        else:
            frame = None
        event_type = None
        event_id = None
        data_parts = []
        return frame

    for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            frame = flush()
            if frame is not None:
                yield frame
            continue
        if line.startswith(":"):
            yield SSEFrame(kind="comment", comment=line[1:].lstrip())
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "event":
            event_type = value
        elif field == "id":
            event_id = value
        elif field == "data":
            data_parts.append(value)
        # 未知フィールドは無視（SSE 仕様）。
    # stream 終端で未確定 frame が残っていれば flush。
    frame = flush()
    if frame is not None:
        yield frame
