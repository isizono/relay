"""relay_sdk.client.sse（byte 単位 SSE パーサ）の単体テスト。

frame 意味論（境界 = 空行、comment 即時 yield、data 複数行 join、未知 field 無視、
CRLF / chunk 分割耐性）に加え、受信量上限（`max_frame_bytes` / `max_buffer_bytes`）超過時に
壊れた frame を破棄して同期を回復し `kind="overflow"` を 1 度だけ yield すること、および
超過中もバッファが無制限に伸びないことを固定する。
"""
from __future__ import annotations

from typing import Iterator

from relay_sdk.client.sse import SSEFrame, parse_sse_byte_stream

# 上限を十分大きく取り、上限テスト以外では発火させない。
BIG = 1 << 20


def _parse(blob: bytes, *, chunk: int = 8192, max_frame=BIG, max_buffer=BIG) -> list[SSEFrame]:
    def chunks() -> Iterator[bytes]:
        for i in range(0, len(blob), chunk):
            yield blob[i : i + chunk]

    return list(
        parse_sse_byte_stream(chunks(), max_frame_bytes=max_frame, max_buffer_bytes=max_buffer)
    )


# ---------------------------------------------------------------------------
# frame 意味論
# ---------------------------------------------------------------------------


class TestFrameSemantics:
    def test_event_frame_basic(self):
        frames = _parse(b"event: notification\nid: 7\ndata: hello\n\n")
        assert frames == [SSEFrame(kind="event", event="notification", id="7", data="hello")]

    def test_comment_yielded_immediately_without_boundary(self):
        # comment は frame 境界（空行）を待たず即時に yield される（keepalive 検出用）。
        frames = _parse(b": keepalive\n")
        assert frames == [SSEFrame(kind="comment", comment="keepalive")]

    def test_multiline_data_joined_with_newline(self):
        frames = _parse(b"data: a\ndata: b\ndata: c\n\n")
        assert len(frames) == 1
        assert frames[0].data == "a\nb\nc"

    def test_unknown_field_ignored(self):
        frames = _parse(b"event: notification\nfoo: bar\ndata: x\n\n")
        assert frames == [SSEFrame(kind="event", event="notification", id=None, data="x")]

    def test_unknown_event_type_still_yielded(self):
        # event 型の判別（notification か否か）は呼び出し側の責務。パーサは型を落とさない。
        frames = _parse(b"event: bogus\ndata: x\n\n")
        assert frames[0].kind == "event"
        assert frames[0].event == "bogus"

    def test_data_without_leading_space(self):
        # value 先頭スペースは 1 個だけ剥がす。無い場合はそのまま。
        assert _parse(b"data:x\n\n")[0].data == "x"
        assert _parse(b"data:  x\n\n")[0].data == " x"

    def test_crlf_line_endings(self):
        frames = _parse(b"event: notification\r\ndata: y\r\n\r\n")
        assert frames == [SSEFrame(kind="event", event="notification", id=None, data="y")]

    def test_trailing_frame_flushed_at_eof(self):
        # 末尾に空行が無くても、data を持つ未確定 frame は stream 終端で flush される。
        frames = _parse(b"event: notification\ndata: z\n")
        assert frames == [SSEFrame(kind="event", event="notification", id=None, data="z")]

    def test_blank_frame_without_data_is_dropped(self):
        # data の無い frame（event/id だけ）は yield されない。
        assert _parse(b"event: notification\nid: 3\n\n") == []

    def test_split_across_arbitrary_chunk_boundaries(self):
        blob = b"event: notification\nid: 1\ndata: hello world\n\n: keepalive\n"
        # 1 byte ずつ供給しても行/frame 境界を跨いで正しく組み立てる。
        frames = _parse(blob, chunk=1)
        assert frames == [
            SSEFrame(kind="event", event="notification", id="1", data="hello world"),
            SSEFrame(kind="comment", comment="keepalive"),
        ]

    def test_empty_chunks_ignored(self):
        def chunks():
            yield b""
            yield b"data: a\n"
            yield b""
            yield b"\n"

        frames = list(parse_sse_byte_stream(chunks(), max_frame_bytes=BIG, max_buffer_bytes=BIG))
        assert frames == [SSEFrame(kind="event", event=None, id=None, data="a")]


# ---------------------------------------------------------------------------
# 受信量上限: 1 frame の data 累積 byte
# ---------------------------------------------------------------------------


class TestFrameByteLimit:
    def test_oversized_single_data_line_dropped_then_resync(self):
        payload = b"X" * 200
        blob = (
            b"event: notification\nid: 1\ndata: " + payload + b"\n\n"
            b"event: notification\nid: 2\ndata: ok\n\n"
        )
        frames = _parse(blob, max_frame=50)
        # 過大 frame は overflow として通知され、次の frame は正常に parse される。
        assert [f.kind for f in frames] == ["overflow", "event"]
        assert frames[1].data == "ok"
        assert frames[1].id == "2"

    def test_oversized_via_many_small_data_lines(self):
        # 個々の行は小さくても、累積が上限を超えたら frame を破棄する。
        many = b"".join(b"data: chunk\n" for _ in range(100))
        blob = b"event: notification\n" + many + b"\n" + b"data: fine\n\n"
        frames = _parse(blob, max_frame=40)
        assert frames[0].kind == "overflow"
        assert frames[-1].data == "fine"

    def test_overflow_signalled_exactly_once_per_dropped_frame(self):
        blob = b"data: " + b"Y" * 100 + b"\n\n" + b"data: " + b"Z" * 100 + b"\n\n"
        frames = _parse(blob, max_frame=10)
        # 2 つの過大 frame → overflow 2 回、event 0 回。
        assert [f.kind for f in frames] == ["overflow", "overflow"]

    def test_within_limit_not_dropped(self):
        frames = _parse(b"data: hi\n\n", max_frame=100)
        assert frames == [SSEFrame(kind="event", event=None, id=None, data="hi")]


# ---------------------------------------------------------------------------
# 受信量上限: 改行未達の 1 行バッファ
# ---------------------------------------------------------------------------


class TestBufferByteLimit:
    def test_unterminated_line_over_buffer_cap_dropped_and_buffer_bounded(self):
        # 改行を一切送らない巨大バイト列 → バッファ上限で頭打ちにして読み飛ばす。
        # stream 終端で overflow を 1 度通知する。
        blob = b"data: " + b"A" * 10000  # 改行なしで終端
        frames = _parse(blob, chunk=256, max_buffer=100)
        assert frames == [SSEFrame(kind="overflow")]

    def test_unterminated_line_then_resync_to_next_frame(self):
        # 過大な未終端行の後、改行 + 次 frame で同期回復する。
        blob = b"data: " + b"A" * 5000 + b"\n\n" + b"event: notification\ndata: ok\n\n"
        frames = _parse(blob, chunk=256, max_buffer=100)
        assert frames[0].kind == "overflow"
        assert frames[-1].kind == "event"
        assert frames[-1].data == "ok"

    def test_buffer_stays_bounded_during_flood(self, monkeypatch):
        # 改行なしの巨大 flood でも内部バッファが max_buffer + 1 chunk 近傍で頭打ちに
        # なること（＝ buf を clear していること）を、bytearray を差し替えて peak を観測。
        # overflow を最後にだけ yield するが実は全部バッファしていた、という退行を捕える。
        import relay_sdk.client.sse as sse_mod

        observed_max = {"n": 0}

        class _Tracked(bytearray):
            def __iadd__(self, other):
                super().__iadd__(other)
                observed_max["n"] = max(observed_max["n"], len(self))
                return self

        monkeypatch.setattr(sse_mod, "bytearray", _Tracked, raising=False)

        blob = b"C" * 100_000  # 改行なしで送り続ける
        assert _parse(blob, chunk=1000, max_buffer=200) == [SSEFrame(kind="overflow")]
        # 100_000 まで伸びず、max_buffer(200) + 1 chunk(1000) 程度で頭打ち。
        assert observed_max["n"] <= 200 + 1000 + 16
