"""Full Jitter バックオフ（outbox dispatcher / SSE 再接続で共用、relay-v2-sdk.md §2.3.1 / §3.4）。

`sleep = random(0, min(cap, base * 2 ** attempt))`（AWS "Exponential Backoff And Jitter"）。
決定的な指数バックオフと異なり、同時に失敗した複数プロセスが同じタイミングで一斉に
retry する thundering herd を避けるため、待ち時間を毎回 [0, ceiling) から一様乱択する。
"""
from __future__ import annotations

import random

# 2 ** _MAX_EXPONENT はどの base / cap 設定でも ceiling を頭打ちさせるのに十分な大きさで、
# かつ float の 2 ** x が overflow しない範囲（attempt が際限なく増え続けても安全に呼べる）。
_MAX_EXPONENT = 62


def full_jitter(
    base_seconds: float,
    cap_seconds: float,
    attempt: int,
    *,
    rng: random.Random | None = None,
) -> float:
    """Full Jitter 方式のバックオフ秒数を返す。

    Args:
        base_seconds: 1 回目の retry（attempt=0）に対応する基準秒数。
        cap_seconds: バックオフの頭打ち秒数。
        attempt: 0 始まりの失敗回数。
        rng: 決定的なテストのために乱数源を差し替えたい場合に渡す（省略時は `random` module）。
    """
    exponent = min(max(attempt, 0), _MAX_EXPONENT)
    ceiling = min(cap_seconds, base_seconds * (2.0**exponent))
    uniform = rng.uniform if rng is not None else random.uniform
    return uniform(0.0, ceiling)
