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
import time

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
