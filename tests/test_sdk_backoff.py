"""relay_sdk.backoff（Full Jitter バックオフ）の単体テスト。

outbox dispatcher の retry（base=1s, cap=300s）と SSE 再接続（base=1s, cap=30s）の
両方が共用する `full_jitter()` を、seed 固定の `random.Random` と範囲アサーションで検証する。
ランダム性そのものは崩さず、[0, ceiling] に収まることと ceiling の計算（base * 2 ** attempt
の cap 頭打ち）を確認する。
"""
from __future__ import annotations

import random

from relay_sdk.backoff import full_jitter


class TestFullJitterBounds:
    def test_stays_within_ceiling_across_attempts(self):
        rng = random.Random(42)
        for attempt in range(10):
            ceiling = min(300.0, 1.0 * (2.0**attempt))
            for _ in range(200):
                delay = full_jitter(1.0, 300.0, attempt, rng=rng)
                assert 0.0 <= delay <= ceiling

    def test_cap_enforced_once_exponent_exceeds_cap(self):
        rng = random.Random(1)
        # attempt=10 なら base(1) * 2**10 = 1024 > cap(30) なので cap 頭打ちになるはず。
        for _ in range(200):
            delay = full_jitter(1.0, 30.0, attempt=10, rng=rng)
            assert 0.0 <= delay <= 30.0

    def test_very_large_attempt_does_not_overflow(self):
        """retry が長期間続いても（attempt が際限なく増えても）float overflow しない。"""
        rng = random.Random(2)
        delay = full_jitter(1.0, 300.0, attempt=10_000, rng=rng)
        assert 0.0 <= delay <= 300.0

    def test_negative_attempt_clamped_to_zero(self):
        rng = random.Random(3)
        for _ in range(50):
            delay = full_jitter(1.0, 300.0, attempt=-5, rng=rng)
            assert 0.0 <= delay <= 1.0


class TestFullJitterDeterminism:
    def test_same_seed_yields_same_delay(self):
        delay_a = full_jitter(1.0, 300.0, attempt=3, rng=random.Random(7))
        delay_b = full_jitter(1.0, 300.0, attempt=3, rng=random.Random(7))
        assert delay_a == delay_b

    def test_default_rng_used_when_not_provided(self):
        # rng を渡さなくても [0, ceiling] には必ず収まる（グローバル random を使う経路）。
        for _ in range(200):
            delay = full_jitter(1.0, 300.0, attempt=0)
            assert 0.0 <= delay <= 1.0
