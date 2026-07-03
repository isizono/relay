"""relay_sdk.client（subscriber 側）の単体テスト（FakeRelay 駆動）。

relay-v2-sdk.md §7.1 が「§3.2.1 の title 型分離を回帰から守るため固定すべき」と明記する
項目を必ず含む:

- Event の dataclass fields に title が存在しないこと
- title 付きで publish した event が yield される際、同じ publish_id の EventDisplay が
  on_display に yield 前に 1 回だけ渡ること
- on_display callback が例外を投げても receive() の yield と ack 進行が継続すること
- yield 直前に logger relay_sdk.client.events へ title を含む INFO レコードが出ること、
  および dedup 破棄された再送 frame が DEBUG で記録されること
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

import pytest

from relay_sdk.client import Event, EventDisplay, subscribe
from relay_sdk.testing import FakeRelay


@pytest.fixture()
def fake():
    with FakeRelay() as f:
        yield f


def _subscribe(fake, **kwargs):
    params = dict(
        relay_base_url=fake.base_url,
        subscriber_identity="test-subscriber",
        labels=["entity:decision"],
        agent_card_path=fake.fake_agent_card_path(),
    )
    params.update(kwargs)
    return subscribe(**params)


def _wait(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


# ---------------------------------------------------------------------------
# §7.1 固定項目（title 型分離の回帰防止）
# ---------------------------------------------------------------------------


class TestTitleTypeSeparation:
    def test_event_dataclass_has_no_title_field(self):
        assert "title" not in Event.__dataclass_fields__
        assert "title" in EventDisplay.__dataclass_fields__

    def test_on_display_receives_matching_publish_id_before_yield(self, fake):
        displays: list[EventDisplay] = []

        with _subscribe(fake, on_display=displays.append) as sub:
            fake.publish(ref_type="decision", ref_id=1, labels=["entity:decision"], title="t")
            for event in sub.receive():
                # yield 時点で on_display は既に 1 回呼ばれている（yield 前呼び出し）。
                assert len(displays) == 1
                assert displays[0].publish_id == event.publish_id
                assert displays[0].title == "t"
                # 業務 event 側には title が無い（型で塞がれている）。
                assert not hasattr(event, "title")
                break

    def test_on_display_called_once_per_event(self, fake):
        displays: list[EventDisplay] = []
        with _subscribe(fake, on_display=displays.append) as sub:
            fake.publish(ref_type="decision", ref_id=1, labels=["entity:decision"], title="a")
            fake.publish(ref_type="decision", ref_id=2, labels=["entity:decision"], title="b")
            got = []
            for event in sub.receive():
                got.append(event)
                if len(got) == 2:
                    break
        assert [d.publish_id for d in displays] == [got[0].publish_id, got[1].publish_id]

    def test_on_display_exception_does_not_break_receive_or_ack(self, fake):
        def boom(_display):
            raise ValueError("display failed")

        with _subscribe(fake, on_display=boom, auto_ack=True) as sub:
            sub_id = sub.subscription_id
            fake.publish(ref_type="decision", ref_id=1, labels=["entity:decision"], title="x")
            fake.publish(ref_type="decision", ref_id=2, labels=["entity:decision"], title="y")
            got = []
            for event in sub.receive():
                got.append(event)
                if len(got) == 2:
                    break
            # 例外は伝播せず 2 件 yield された。1 件目は auto_ack で ack 済み（outbox から消える）。
            assert len(got) == 2
            assert _wait(lambda: fake.outbox_size(sub_id) <= 1)

    def test_info_log_with_title_and_debug_log_for_deduped_resend(self, fake, caplog):
        caplog.set_level(logging.DEBUG, logger="relay_sdk.client.events")

        with _subscribe(fake, auto_ack=False) as sub:
            fake.publish(ref_type="decision", ref_id=1, labels=["entity:decision"], title="TITLE-1")
            it = sub.receive()
            e1 = next(it)  # E1 yield（INFO 記録 + title 含む）
            # ack しないまま接続を drop → 再接続で同一 publish_id が再 push される。
            fake.drop_connections()
            fake.publish(ref_type="decision", ref_id=2, labels=["entity:decision"], title="TITLE-2")
            e2 = next(it)  # 再送 E1 は dedup(DEBUG)、E2 が yield される
            assert e1.publish_id != e2.publish_id
            assert e2.ref_id == 2

        info_records = [
            r for r in caplog.records
            if r.name == "relay_sdk.client.events" and r.levelno == logging.INFO
        ]
        debug_records = [
            r for r in caplog.records
            if r.name == "relay_sdk.client.events" and r.levelno == logging.DEBUG
        ]
        # yield された event 分の INFO に title が載っている。
        assert any("TITLE-1" in r.getMessage() for r in info_records)
        # dedup 破棄された再送 frame の DEBUG が記録されている。
        assert any("dedup" in r.getMessage() for r in debug_records)


# ---------------------------------------------------------------------------
# 基本の受信 / ack / close
# ---------------------------------------------------------------------------


class TestReceiveAndAck:
    def test_typical_example_5_2(self, fake):
        # spec §5.2 の典型コード例が動く。
        received = []
        with _subscribe(fake, labels=["topic:474", "event:updated"]) as sub:
            fake.publish(
                ref_type="activity",
                ref_id=9,
                labels=["topic:474", "event:updated", "extra"],
                title="t",
            )
            for event in sub.receive():
                received.append(event)
                break
        assert received[0].ref_type == "activity"
        assert received[0].labels == ["topic:474", "event:updated", "extra"]

    def test_auto_ack_drains_outbox_on_resume(self, fake):
        with _subscribe(fake, auto_ack=True) as sub:
            sub_id = sub.subscription_id
            fake.publish(ref_type="decision", ref_id=1, labels=["entity:decision"])
            fake.publish(ref_type="decision", ref_id=2, labels=["entity:decision"])
            got = []
            for event in sub.receive():
                got.append(event)
                if len(got) == 2:
                    break
            # E1 は resume 時に ack され outbox から消える（E2 は break で未 resume → 残る）。
            assert _wait(lambda: fake.outbox_size(sub_id) == 1)

    def test_batch_ack_example_5_3(self, fake):
        with _subscribe(fake, auto_ack=False) as sub:
            sub_id = sub.subscription_id
            for i in range(3):
                fake.publish(ref_type="decision", ref_id=i, labels=["entity:decision"])
            batch = []
            for event in sub.receive():
                batch.append(event)
                if len(batch) == 3:
                    sub.ack(up_to_publish_id=batch[-1].publish_id)
                    break
            assert _wait(lambda: fake.outbox_size(sub_id) == 0)

    def test_subset_matching_non_match_not_delivered(self, fake):
        # subscribe.labels=[a,b] は publish.labels=[a] にマッチしない（AND / subset）。
        with _subscribe(fake, labels=["a", "b"]) as sub:
            fake.publish(ref_type="d", ref_id=1, labels=["a"])  # non-match
            fake.publish(ref_type="d", ref_id=2, labels=["a", "b", "c"])  # match
            got = []
            for event in sub.receive():
                got.append(event)
                break
            assert got[0].ref_id == 2

    def test_receive_after_close_raises(self, fake):
        sub = _subscribe(fake)
        sub.close()
        with pytest.raises(RuntimeError):
            next(sub.receive())

    def test_empty_labels_raises_before_relay(self, fake):
        with pytest.raises(ValueError):
            subscribe(
                relay_base_url=fake.base_url,
                subscriber_identity="x",
                labels=[],
                agent_card_path=fake.fake_agent_card_path(),
            )


# ---------------------------------------------------------------------------
# 再接続 / resubscribe（fault 注入）
# ---------------------------------------------------------------------------


class TestReconnectAndResubscribe:
    def test_reconnect_replays_unacked(self, fake):
        with _subscribe(fake, auto_ack=False) as sub:
            fake.publish(ref_type="d", ref_id=1, labels=["entity:decision"])
            it = sub.receive()
            e1 = next(it)
            assert e1.ref_id == 1
            # 未 ack のまま drop → 再接続で E1 が再 push される（dedup）→ E2 を返す。
            fake.drop_connections()
            fake.publish(ref_type="d", ref_id=2, labels=["entity:decision"])
            e2 = next(it)
            assert e2.ref_id == 2

    def test_subscription_loss_triggers_resubscribe(self, fake):
        import threading

        with _subscribe(fake, auto_ack=True) as sub:
            old_id = sub.subscription_id
            fake.publish(ref_type="d", ref_id=1, labels=["entity:decision"])
            it = sub.receive()
            e1 = next(it)
            assert e1.ref_id == 1

            # 別スレッドで「新 subscription が採番されたら E2 を publish」する。
            def publish_when_resubscribed():
                for _ in range(250):
                    if sub.subscription_id != old_id:
                        fake.publish(ref_type="d", ref_id=2, labels=["entity:decision"])
                        return
                    time.sleep(0.02)

            fake.simulate_subscription_loss(old_id)  # old sub 失効 → 404 → PermanentError
            threading.Thread(target=publish_when_resubscribed, daemon=True).start()

            e2 = next(it)  # resubscribe 後の新 sub から E2 を受信
            assert e2.ref_id == 2
            assert sub.subscription_id != old_id


# ---------------------------------------------------------------------------
# ブロッカー1: lease renew は event frame でも発火しなければならない
# ---------------------------------------------------------------------------


class TestLeaseRenewOnEventFrames:
    def test_renew_fires_on_event_frame_not_only_keepalive(self, fake, monkeypatch):
        """relay 本体は push が無い間だけ keepalive を送るため、event が keepalive
        間隔より高頻度に届く状況では comment frame が一切来ない。renew の契機を
        comment frame だけに限定すると、この状況で lease が更新されないまま失効し、
        失効中に届いた event は二度と配達されなくなる（relay/subscriptions.py の
        `matching()` が lease 切れ subscription を fan-out 対象から除外するため）。
        """
        import relay_sdk.client.subscription as sub_mod

        calls = {"n": 0}
        original = sub_mod.put_lease

        def spy_put_lease(*args, **kwargs):
            calls["n"] += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(sub_mod, "put_lease", spy_put_lease)

        with _subscribe(fake, lease_ttl_seconds=300) as sub:
            # renew 閾値（残り <= lease_ttl / 3）を、単一の event frame 処理だけで
            # 確実に満たすよう強制的に「期限切れ間近」にする（timing に依存しない
            # 決定的なテストにするため）。
            sub._lease_expires_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            old_expiry = sub.lease_expires_at

            fake.publish(ref_type="d", ref_id=1, labels=["entity:decision"])
            event = next(sub.receive())

        assert event.ref_id == 1
        assert calls["n"] >= 1, (
            "event frame の処理時に lease renew (PUT lease) が呼ばれていない"
            "（comment frame 契機のみだと高頻度 event 下で lease が失効する）"
        )
        assert sub.lease_expires_at != old_expiry

    def test_lease_renewed_purely_by_high_frequency_events(self, fake):
        """実地確認: keepalive comment frame を挟まず event 到着のみで renew される。

        events を publish→即 next() の高頻度ループで回すことで、FakeRelay が
        「pending 無し」判定を挟む隙を与えず（= comment frame を発生させず）、
        event frame の処理だけで lease が更新されることを確認する。
        """
        with _subscribe(fake, lease_ttl_seconds=1) as sub:
            it = sub.receive()
            initial = sub.lease_expires_at
            deadline = time.time() + 3.0
            i = 0
            renewed = False
            while time.time() < deadline:
                fake.publish(ref_type="d", ref_id=i, labels=["entity:decision"])
                next(it)
                i += 1
                if sub.lease_expires_at != initial:
                    renewed = True
                    break
            assert renewed, "高頻度 event 下で lease が一度も renew されなかった"


# ---------------------------------------------------------------------------
# ブロッカー2: SSE 無音検知（read timeout）
# ---------------------------------------------------------------------------


class TestSilentConnectionDetection:
    def test_silent_connection_triggers_timeout_reconnect(self, fake, monkeypatch):
        """半死 TCP 接続（event も keepalive も一切来ない）で receive() が永久ブロック
        しないこと。read timeout（keepalive 間隔の 2 倍）で TransientError に落ち、
        receive() の再接続ループに乗って新規 SSE 接続を試み続けることを確認する。

        単に「いずれ event が届く」だけでは、read timeout が実際に発火して
        再接続したのか、コネクションが（無音のまま）生き残っていて後から
        データが流れてきただけなのかを区別できない（FakeRelay は無音期間中も
        TCP 接続自体は close しないため、read timeout が無効なままでも同じ接続で
        いずれデータを受信できてしまう）。そのため `open_sse` の呼び出し回数
        （= 実際に新規 SSE 接続を試みた回数）で「timeout 検知 → 再接続」が
        実際に起きたことを直接確認する。
        """
        import relay_sdk.client.subscription as sub_mod

        monkeypatch.setenv("RELAY_SSE_KEEPALIVE_S", "0.2")  # read_timeout ≒ 0.4s

        open_calls = {"n": 0}
        original_open_sse = sub_mod.open_sse

        def spy_open_sse(*args, **kwargs):
            open_calls["n"] += 1
            return original_open_sse(*args, **kwargs)

        monkeypatch.setattr(sub_mod, "open_sse", spy_open_sse)

        with _subscribe(fake) as sub:
            fake.simulate_silence(True)
            it = sub.receive()

            def unsilence_and_publish():
                # read_timeout（≒0.4s）の複数周期分、無音を継続させてから解除する。
                time.sleep(1.2)
                fake.simulate_silence(False)
                fake.publish(ref_type="d", ref_id=1, labels=["entity:decision"])

            threading.Thread(target=unsilence_and_publish, daemon=True).start()

            start = time.time()
            event = next(it)  # silence 解除後に配達されるまで待つが、無限ブロックはしない
            elapsed = time.time() - start

        assert event.ref_id == 1
        assert elapsed < 5.0, "無音接続のまま永久ブロックしている（read timeout 未検知）"
        assert open_calls["n"] > 1, (
            "read timeout による再接続が発生していない"
            "（open_sse が 1 回しか呼ばれておらず、無音のまま同一接続でブロックし続けた"
            "可能性がある）"
        )


# ---------------------------------------------------------------------------
# medium3: reconnect_max_attempts 到達後、resubscribe がホットループしないこと
# ---------------------------------------------------------------------------


class TestResubscribeBackoffAfterAttemptsExhausted:
    def test_resubscribe_backs_off_instead_of_hot_looping(self, fake, monkeypatch):
        import relay_sdk.client.subscription as sub_mod

        sleeps: list[float] = []
        monkeypatch.setattr(sub_mod.time, "sleep", lambda s: sleeps.append(s))

        with _subscribe(fake, reconnect_max_attempts=2) as sub:
            fake.simulate_outage(True)  # POST /subscriptions も 503 になる

            original_post_subscription = sub_mod.post_subscription
            calls = {"n": 0}

            def flaky_post_subscription(*args, **kwargs):
                calls["n"] += 1
                if calls["n"] >= 6:
                    fake.simulate_outage(False)
                return original_post_subscription(*args, **kwargs)

            monkeypatch.setattr(sub_mod, "post_subscription", flaky_post_subscription)
            sub._resubscribe()

        # attempt 0,1 は仕様通りの指数バックオフ（即時 1 回 → 1.0s。sleeps[0]==0.0 は
        # 「即時 1 回」分の正常な待機ゼロであり、バグではない）。reconnect_max_attempts(2)
        # 到達後（sleeps[2:]）は毎回 backoff_cap で待つはず（0 delay で連打する
        # ホットループにならないことがこのテストの本題）。
        assert len(sleeps) == 5, f"sleep 回数が想定と異なる（hot loop の疑い）: {sleeps}"
        assert sleeps[0] == pytest.approx(0.0)
        assert sleeps[1] == pytest.approx(1.0)
        assert all(s == pytest.approx(sub._backoff_cap) for s in sleeps[2:]), (
            f"reconnect_max_attempts 到達後に backoff_cap で待っていない: {sleeps}"
        )
        assert all(s > 0 for s in sleeps[2:]), (
            f"reconnect_max_attempts 到達後に sleep 0 のホットループが発生した: {sleeps}"
        )


# ---------------------------------------------------------------------------
# medium4: auto_ack flush retry が keepalive 契機でも働くこと
# ---------------------------------------------------------------------------


class TestAckFlushRetry:
    def test_ack_flush_retried_via_periodic_maintenance_without_new_event(
        self, fake, monkeypatch
    ):
        """resume 直後の ack flush が一度 TransientError で失敗した後、新しい event が
        来なくても（keepalive 契機の保守処理だけで）retry され、outbox が drain される
        ことを確認する。"""
        import relay_sdk.client.subscription as sub_mod

        with _subscribe(fake, auto_ack=True) as sub:
            sub_id = sub.subscription_id
            fake.publish(ref_type="d", ref_id=1, labels=["entity:decision"])

            original_post_ack = sub_mod.post_ack
            state = {"fail_once": True}

            def flaky_post_ack(*args, **kwargs):
                if state["fail_once"]:
                    state["fail_once"] = False
                    from relay_sdk.errors import TransientError

                    raise TransientError("simulated transient ack failure")
                return original_post_ack(*args, **kwargs)

            monkeypatch.setattr(sub_mod, "post_ack", flaky_post_ack)

            it = sub.receive()
            event = next(it)  # E1 yield。resume 直後の flush が 1 回目の TransientError で失敗。
            assert event.ref_id == 1

            got: list = []

            def drive():
                got.append(next(it))  # E2 が来るまでブロックし、その間 keepalive で ack retry する

            t = threading.Thread(target=drive, daemon=True)
            t.start()

            # 新しい event を publish しないまま、keepalive 契機の retry だけで
            # E1 の ack が flush され outbox から消えることを確認する。
            assert _wait(lambda: fake.outbox_size(sub_id) == 0, timeout=3.0), (
                "keepalive 契機の ack retry が働いておらず、E1 が未 ack のまま放置されている"
            )

            # 後片付け: E2 を publish してブロック中のスレッドを解放する。
            fake.publish(ref_type="d", ref_id=2, labels=["entity:decision"])
            t.join(timeout=3)

        assert got and got[0].ref_id == 2
