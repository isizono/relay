"""publisher identity ごとの publish レート制限（両 publish レーン共通）。

subscription レーン（`POST /publish`）と stream レーン（`POST /streams/{id}/messages`）は
`app.state.publish_rate_limiter` に載る同一の `RateLimiter` インスタンスを共有する。token
bucket は publisher identity をキーにするため、1 publisher の publish 流量は両レーン合算で
`publish_rate_limit_per_second` に制限される（relay-v2-wire-api.md §5.4）。
"""
from __future__ import annotations

import threading
import time

from relay.config import Settings


class RateLimiter:
    """publisher identity ごとの token bucket rate limiter。"""

    def __init__(self, rate_per_second: int) -> None:
        self._rate = max(1, rate_per_second)
        self._lock = threading.Lock()
        self._buckets: dict[str, tuple[float, float]] = {}

    def allow(self, identity: str) -> tuple[bool, int]:
        """許可なら `(True, 0)`、拒否なら `(False, retry_after_seconds)`。"""
        now = time.monotonic()
        with self._lock:
            tokens, last = self._buckets.get(identity, (float(self._rate), now))
            tokens = min(float(self._rate), tokens + (now - last) * self._rate)
            if tokens >= 1.0:
                self._buckets[identity] = (tokens - 1.0, now)
                return True, 0
            self._buckets[identity] = (tokens, now)
            retry_after = max(1, int((1.0 - tokens) / self._rate) + 1)
            return False, retry_after


def get_publish_rate_limiter(app_state) -> RateLimiter:
    """`app.state.publish_rate_limiter` を遅延生成して返す（両レーン共有インスタンス）。"""
    limiter = getattr(app_state, "publish_rate_limiter", None)
    if limiter is None:
        settings: Settings = app_state.settings
        limiter = RateLimiter(settings.publish_rate_limit_per_second)
        app_state.publish_rate_limiter = limiter
    return limiter
