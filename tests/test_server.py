"""powwow server.py テストスイート。

エッジケース表 #1〜#22 全項目をカバーする。
各テストは独立した一時 DB を使い、実際に条件を突いて期待結果を assert する。
"""
import json
import os
import queue
import socket
import sqlite3
import struct
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from urllib.request import urlopen, Request
from urllib.error import HTTPError

import pytest

import server as srv


# ---------------------------------------------------------------------------
# フィクスチャ
# ---------------------------------------------------------------------------

@pytest.fixture()
def db(tmp_path):
    """各テスト用の一時 SQLite DB パスを返し、スキーマを初期化する。"""
    path = str(tmp_path / "test_powwow.db")
    srv.init_db(path)
    return path


@pytest.fixture()
def http_server(db, tmp_path):
    """実際の ThreadingHTTPServer を起動し、(base_url, db_path) を返す。

    テスト完了後にサーバーを停止する。
    """
    # テスト用にグローバル DB_PATH を一時 path に向けるために server モジュールの
    # デフォルト引数を動的に置き換えるのではなく、ハンドラ内で db パスを共有する方法として
    # server.DB_PATH をモンキーパッチする。
    original_db_path = srv.DB_PATH
    srv.DB_PATH = db

    # _subscribers をクリア（テスト間の汚染防止）
    with srv._sub_lock:
        srv._subscribers.clear()

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), srv.Handler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()

    yield f"http://127.0.0.1:{port}", db

    httpd.shutdown()
    srv.DB_PATH = original_db_path


def _post(url: str, data: dict) -> tuple[int, dict]:
    """JSON POST リクエストを送り (status_code, response_dict) を返す。"""
    body = json.dumps(data).encode("utf-8")
    req = Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except HTTPError as e:
        return e.code, json.loads(e.read())


def _get(url: str) -> tuple[int, dict]:
    """GET リクエストを送り (status_code, response_dict) を返す。"""
    try:
        with urlopen(url) as resp:
            return resp.status, json.loads(resp.read())
    except HTTPError as e:
        return e.code, json.loads(e.read())


def _create_powwow(base_url: str) -> str:
    """POST /create を呼び powwow_code を返す。"""
    status, body = _post(f"{base_url}/create", {})
    assert status == 200, f"create 失敗: {body}"
    return body["powwow_code"]


def _send(base_url: str, powwow: str, handle: str, body_text: str,
          needs_reply: bool = False, in_reply_to=None) -> tuple[int, dict]:
    """POST /send を呼び (status, response) を返す。"""
    return _post(f"{base_url}/send", {
        "powwow": powwow,
        "handle": handle,
        "body": body_text,
        "needs_reply": needs_reply,
        "in_reply_to": in_reply_to,
    })


def _port_of(base_url: str) -> int:
    """base_url（http://127.0.0.1:PORT）からポート番号を取り出す。"""
    return int(base_url.rsplit(":", 1)[1])


def _wait_for(predicate, timeout: float = 2.0, interval: float = 0.02) -> bool:
    """predicate() が True を返すまで最大 timeout 秒ポーリングし、結果を返す。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _open_stream(port: int, code: str, handle: str) -> socket.socket:
    """生 socket で GET /stream に接続し、リクエスト送出済みの socket を返す。

    実際の HTTP サーバーの _handle_stream を通すため、urllib ではなく生 socket を
    使う（接続の途中切断や data 行のインクリメンタル読みを制御するため）。
    """
    s = socket.create_connection(("127.0.0.1", port))
    s.sendall(
        f"GET /stream?powwow={code}&handle={handle} HTTP/1.1\r\n"
        f"Host: 127.0.0.1\r\n\r\n".encode("utf-8")
    )
    s.settimeout(2.0)
    return s


def _read_sse_data(sock: socket.socket) -> dict:
    """SSE ストリームから最初の ``data: {json}`` 行を読み、JSON をパースして返す。

    先行する HTTP ヘッダ行や ``: connected`` コメント行は data 行でないため読み飛ばす。
    """
    buf = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            raise AssertionError(f"data 行を受信する前に接続が切れた: {buf!r}")
        buf += chunk
        for line in buf.decode("utf-8", errors="replace").splitlines():
            if line.startswith("data:"):
                return json.loads(line[len("data:"):].strip())


# ---------------------------------------------------------------------------
# エッジケース #1: history の since は msg_id > since（since 自身は含まない）
# ---------------------------------------------------------------------------

def test_case01_history_since_excludes_since_itself(http_server):
    """GET /history?since=N は msg_id > N のメッセージのみ返し、N 自身は含まない。"""
    base_url, _ = http_server
    code = _create_powwow(base_url)

    status1, r1 = _send(base_url, code, "alice", "msg-1")
    assert status1 == 200
    msg_id_1 = r1["msg_id"]

    status2, r2 = _send(base_url, code, "alice", "msg-2")
    assert status2 == 200
    msg_id_2 = r2["msg_id"]

    # since=msg_id_1 → msg_id > msg_id_1 のみ（msg_id_1 自身は含まない）
    status, resp = _get(f"{base_url}/history?powwow={code}&since={msg_id_1}")
    assert status == 200
    ids = [m["msg_id"] for m in resp["messages"]]
    assert msg_id_1 not in ids, "since 自身が含まれている"
    assert msg_id_2 in ids, "since より後のメッセージが含まれていない"


# ---------------------------------------------------------------------------
# エッジケース #2: history since 未指定は全件返す
# ---------------------------------------------------------------------------

def test_case02_history_no_since_returns_all(http_server):
    """GET /history（since 未指定）は当該 powwow の全メッセージを返す。"""
    base_url, _ = http_server
    code = _create_powwow(base_url)

    for i in range(3):
        _send(base_url, code, "alice", f"msg-{i}")

    status, resp = _get(f"{base_url}/history?powwow={code}")
    assert status == 200
    assert len(resp["messages"]) == 3


# ---------------------------------------------------------------------------
# エッジケース #3: history の各メッセージは6フィールドを全て含む
# ---------------------------------------------------------------------------

def test_case03_history_message_has_all_six_fields(http_server):
    """history の各メッセージは msg_id/handle/body/needs_reply/in_reply_to/created_at の6フィールドを含む。"""
    base_url, _ = http_server
    code = _create_powwow(base_url)
    _send(base_url, code, "alice", "hello")

    status, resp = _get(f"{base_url}/history?powwow={code}")
    assert status == 200
    assert len(resp["messages"]) == 1
    msg = resp["messages"][0]
    for field in ("msg_id", "handle", "body", "needs_reply", "in_reply_to", "created_at"):
        assert field in msg, f"フィールド {field} が存在しない"


# ---------------------------------------------------------------------------
# エッジケース #4: msg_id は単調増加
# ---------------------------------------------------------------------------

def test_case04_msg_id_monotonically_increases(db):
    """連続して保存した2メッセージは msg_id が後者 > 前者（単調増加）。"""
    code = srv.create_powwow(db)
    m1 = srv.save_message(code, "alice", "first", False, None, db)
    m2 = srv.save_message(code, "alice", "second", False, None, db)
    assert m2["msg_id"] > m1["msg_id"], "msg_id が単調増加していない"


# ---------------------------------------------------------------------------
# エッジケース #5: in_reply_to=null のメッセージは受理・保存される
# ---------------------------------------------------------------------------

def test_case05_send_without_in_reply_to_is_accepted(http_server):
    """POST /send で in_reply_to=null（info/request）は常に200で受理・保存される。"""
    base_url, _ = http_server
    code = _create_powwow(base_url)
    status, resp = _send(base_url, code, "alice", "info message", needs_reply=False, in_reply_to=None)
    assert status == 200
    assert "msg_id" in resp


# ---------------------------------------------------------------------------
# エッジケース #6: in_reply_to が有効な msg_id なら受理・保存される
# ---------------------------------------------------------------------------

def test_case06_send_with_valid_in_reply_to_is_accepted(http_server):
    """POST /send で in_reply_to が同一 powwow 内の有効な msg_id なら200で受理し、その値を保存する。"""
    base_url, _ = http_server
    code = _create_powwow(base_url)

    status1, r1 = _send(base_url, code, "alice", "original")
    assert status1 == 200
    parent_id = r1["msg_id"]

    status2, r2 = _send(base_url, code, "bob", "reply", in_reply_to=parent_id)
    assert status2 == 200, f"有効な in_reply_to で失敗: {r2}"

    # history で in_reply_to が保存されていることを確認
    _, hist = _get(f"{base_url}/history?powwow={code}")
    reply_msg = next(m for m in hist["messages"] if m["msg_id"] == r2["msg_id"])
    assert reply_msg["in_reply_to"] == parent_id


# ---------------------------------------------------------------------------
# エッジケース #7: in_reply_to が存在しない msg_id なら400を返し保存しない
# ---------------------------------------------------------------------------

def test_case07_send_with_invalid_in_reply_to_returns_400(http_server):
    """POST /send で in_reply_to が同一 powwow 内に存在しない msg_id なら400を返し、メッセージを保存しない。"""
    base_url, _ = http_server
    code = _create_powwow(base_url)

    status, resp = _send(base_url, code, "alice", "bad reply", in_reply_to=99999)
    assert status == 400, f"無効な in_reply_to で400以外: {status}"

    # メッセージが保存されていないことを確認
    _, hist = _get(f"{base_url}/history?powwow={code}")
    assert len(hist["messages"]) == 0, "無効な in_reply_to でメッセージが保存された"


# ---------------------------------------------------------------------------
# エッジケース #8: POST /send 成功時に last_activity_at が更新される
# ---------------------------------------------------------------------------

def test_case08_send_updates_last_activity_at(db):
    """POST /send 成功時、当該 powwow の last_activity_at が送信時刻に更新される。"""
    code = srv.create_powwow(db)

    conn = sqlite3.connect(db)
    before = conn.execute(
        "SELECT last_activity_at FROM powwows WHERE powwow_code = ?", (code,)
    ).fetchone()[0]
    conn.close()

    # 少し待って時刻差を確保
    time.sleep(0.01)
    srv.save_message(code, "alice", "hello", False, None, db)

    conn = sqlite3.connect(db)
    after = conn.execute(
        "SELECT last_activity_at FROM powwows WHERE powwow_code = ?", (code,)
    ).fetchone()[0]
    conn.close()

    assert after > before, "last_activity_at が更新されていない"


# ---------------------------------------------------------------------------
# エッジケース #9: POST /send は全 SSE 購読者の queue にメッセージを配信する
# ---------------------------------------------------------------------------

def test_case09_send_broadcasts_to_all_subscribers(http_server):
    """POST /send は当該 powwow の全 SSE 購読者にメッセージを配信する（ブロードキャスト）。

    /send（HTTP 経由）で実装の save→broadcast 結線を一括テストする。
    送信内容（body・handle・msg_id）が SSE data 行の JSON に一致することを
    各購読者ごとに突き合わせて検証する（件数だけでなく中身の同一性も検証）。
    """
    base_url, _ = http_server
    code = _create_powwow(base_url)
    port = _port_of(base_url)

    # 別 handle の2購読者を接続（送信者除外に巻き込まれないよう sender とは別 handle）
    sock_bob = _open_stream(port, code, "bob")
    sock_charlie = _open_stream(port, code, "charlie")

    try:
        assert _wait_for(lambda: set(srv.get_presence(code)) >= {"bob", "charlie"}), \
            "/stream 接続後も bob/charlie が presence に揃わない"

        # /send（HTTP）→ save_message → _broadcast の結線が走る
        status, resp = _send(base_url, code, "alice", "broadcast test", needs_reply=True)
        assert status == 200
        sent_msg_id = resp["msg_id"]

        # 各購読者の SSE data 行を読み、送信内容と一致することを確認
        for sock, who in [(sock_bob, "bob"), (sock_charlie, "charlie")]:
            received = _read_sse_data(sock)
            assert received["msg_id"] == sent_msg_id, \
                f"{who} が受信した msg_id が送信値と一致しない: {received}"
            assert received["handle"] == "alice", \
                f"{who} が受信した handle が送信者と一致しない: {received}"
            assert received["body"] == "broadcast test", \
                f"{who} が受信した body が送信内容と一致しない: {received}"
            assert received["needs_reply"] is True, \
                f"{who} が受信した needs_reply が送信値と一致しない: {received}"
    finally:
        sock_bob.close()
        sock_charlie.close()


# ---------------------------------------------------------------------------
# エッジケース #10: GET /stream 接続中の handle は GET /presence に現れる
# ---------------------------------------------------------------------------

def test_case10_stream_handle_appears_in_presence(db):
    """GET /stream 接続中の handle は GET /presence に現れる。"""
    code = srv.create_powwow(db)
    q1: queue.Queue = queue.Queue()
    with srv._sub_lock:
        srv._subscribers[code] = [("alice", q1)]

    handles = srv.get_presence(code)
    assert "alice" in handles, "接続中の alice が presence に現れない"


# ---------------------------------------------------------------------------
# エッジケース #11: SSE write が BrokenPipe で失敗した購読者は presence から除外される
# ---------------------------------------------------------------------------

def test_case11_broken_pipe_removes_subscriber(http_server):
    """SSE write が BrokenPipe で失敗した購読者は presence から除外される。

    実際の `_handle_stream` を HTTP 経由で接続させ、クライアント側 socket を
    強制的に閉じた上で `/send` をトリガに `wfile.write` を発火させる。
    その結果 BrokenPipeError 例外が発生し、`_handle_stream` の finally 節で
    `_subscribers` から該当エントリが除外されることを `get_presence` で検証する。
    テスト内で除外ロジックを再実装せず、本体コードの finally 節（server.py 内）
    のみが除外を行う構造にする。
    """
    base_url, _ = http_server
    code = _create_powwow(base_url)
    port = _port_of(base_url)

    # SSE 接続して presence に "dying_subscriber" を載せる
    sock = _open_stream(port, code, "dying_subscriber")

    # 接続が _subscribers に登録されるまで待つ
    assert _wait_for(lambda: "dying_subscriber" in srv.get_presence(code)), \
        "/stream 接続後も dying_subscriber が presence に現れない"

    # クライアント側を強制 RST で閉じる（次回 wfile.write で BrokenPipe を発火させる）
    # SO_LINGER 0 で送信バッファを破棄してから close すると RST が飛ぶ
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    sock.close()

    # /send をトリガに `_broadcast` → 対象 queue.put → `_handle_stream` の
    # wfile.write が発火し、BrokenPipeError → finally 節で除外される。
    # 送信者は別 handle にして送信者除外(D#2286)に引っかからないようにする。
    status, _ = _send(base_url, code, "other_sender", "trigger")
    assert status == 200

    # finally 節の除外が完了するまで待つ
    assert _wait_for(lambda: "dying_subscriber" not in srv.get_presence(code)), \
        "BrokenPipe 後も presence に dying_subscriber が残っている"

    # _subscribers エントリ自体が物理的に削除されていることも確認
    with srv._sub_lock:
        entries = srv._subscribers.get(code, [])
        handles = [h for h, _q in entries]
    assert "dying_subscriber" not in handles, \
        "BrokenPipe 後も _subscribers に dying_subscriber が残っている"


# ---------------------------------------------------------------------------
# エッジケース #12: POST /create は呼ぶたびに一意な powwow_code を発行する
# ---------------------------------------------------------------------------

def test_case12_create_returns_unique_codes(db):
    """POST /create は呼ぶたびに一意な powwow_code を発行する。"""
    codes = [srv.create_powwow(db) for _ in range(20)]
    assert len(set(codes)) == len(codes), "powwow_code に重複が発生した"


# ---------------------------------------------------------------------------
# エッジケース #13: last_activity_at が1年超過した powwow はアイドル削除される（境界テスト）
# ---------------------------------------------------------------------------

def test_case13_idle_cleanup_deletes_expired_powwow(db):
    """last_activity_at が現在から1年超過した powwow は、アイドル削除でpowwow行とそのmessages全行が消える。"""
    code = srv.create_powwow(db)
    srv.save_message(code, "alice", "old msg", False, None, db)

    # last_activity_at を1年以上前に書き換え
    past = (datetime.now(timezone.utc) - timedelta(days=366)).isoformat()
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE powwows SET last_activity_at = ? WHERE powwow_code = ?",
        (past, code),
    )
    conn.commit()
    conn.close()

    deleted = srv.run_idle_cleanup(db)
    assert deleted == 1, f"削除件数が1ではない: {deleted}"

    # powwow 行が消えていることを確認
    conn = sqlite3.connect(db)
    row = conn.execute(
        "SELECT 1 FROM powwows WHERE powwow_code = ?", (code,)
    ).fetchone()
    msg_row = conn.execute(
        "SELECT 1 FROM messages WHERE powwow_code = ?", (code,)
    ).fetchone()
    conn.close()

    assert row is None, "powwow 行が削除されていない"
    assert msg_row is None, "messages 行が削除されていない"


# ---------------------------------------------------------------------------
# エッジケース #14: last_activity_at が1年未満の powwow は削除されない（境界テスト）
# ---------------------------------------------------------------------------

def test_case14_idle_cleanup_does_not_delete_recent_powwow(db):
    """last_activity_at が現在から1年未満（境界: 1年ちょうど直前）の powwow は削除されない。"""
    code = srv.create_powwow(db)

    # last_activity_at を364日前（1年未満）に書き換え
    recent = (datetime.now(timezone.utc) - timedelta(days=364)).isoformat()
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE powwows SET last_activity_at = ? WHERE powwow_code = ?",
        (recent, code),
    )
    conn.commit()
    conn.close()

    deleted = srv.run_idle_cleanup(db)
    assert deleted == 0, "1年未満の powwow が削除された"

    conn = sqlite3.connect(db)
    row = conn.execute(
        "SELECT 1 FROM powwows WHERE powwow_code = ?", (code,)
    ).fetchone()
    conn.close()
    assert row is not None, "1年未満の powwow 行が消えている"


# ---------------------------------------------------------------------------
# エッジケース #15: server は 127.0.0.1 にbindする
# ---------------------------------------------------------------------------

def test_case15_server_binds_to_localhost(http_server):
    """server は 127.0.0.1 にbindし、127.0.0.1 経由でアクセスできる。"""
    base_url, _ = http_server
    assert base_url.startswith("http://127.0.0.1"), f"バインドアドレスが 127.0.0.1 ではない: {base_url}"
    # 疎通確認: /create が 200 を返すこと
    status, _ = _post(f"{base_url}/create", {})
    assert status == 200


# ---------------------------------------------------------------------------
# エッジケース #16: 存在しない powwow_code への send/history/stream は404を返す
# ---------------------------------------------------------------------------

def test_case16_nonexistent_powwow_returns_404(http_server):
    """存在しない powwow_code への send/history/stream は404を返す。"""
    base_url, _ = http_server
    nonexistent = "no-such-code"

    # /send
    status, _ = _send(base_url, nonexistent, "alice", "hello")
    assert status == 404, f"/send が404以外: {status}"

    # /history
    status, _ = _get(f"{base_url}/history?powwow={nonexistent}")
    assert status == 404, f"/history が404以外: {status}"

    # /presence
    status, _ = _get(f"{base_url}/presence?powwow={nonexistent}")
    assert status == 404, f"/presence が404以外: {status}"

    # /stream（存在しない powwow は SSE を張る前に404を返す）
    status, _ = _get(f"{base_url}/stream?powwow={nonexistent}&handle=alice")
    assert status == 404, f"/stream が404以外: {status}"


# ---------------------------------------------------------------------------
# エッジケース #17: 送信者と同一 handle の購読者はブロードキャスト対象外
# ---------------------------------------------------------------------------

def test_case17_sender_handle_excluded_from_broadcast(db):
    """送信者と同一 handle の SSE 購読者は、その送信者の送信メッセージをブロードキャストで受け取らない（サーバー側でskip）。"""
    code = srv.create_powwow(db)

    sender_q: queue.Queue = queue.Queue()
    other_q: queue.Queue = queue.Queue()
    with srv._sub_lock:
        srv._subscribers[code] = [
            ("alice", sender_q),   # 送信者自身の購読
            ("bob", other_q),      # 別ユーザーの購読
        ]

    msg = {"msg_id": 1, "handle": "alice", "body": "hi",
           "needs_reply": False, "in_reply_to": None, "created_at": "2026-01-01"}
    srv._broadcast(code, "alice", msg)  # alice が送信

    # bob には届く
    assert not other_q.empty(), "bob にメッセージが届いていない"
    # alice 自身には届かない
    assert sender_q.empty(), "送信者 alice 自身にエコーされた"


# ---------------------------------------------------------------------------
# エッジケース #18: needs_reply=true/false が正しく保存・返却される
# ---------------------------------------------------------------------------

def test_case18_needs_reply_stored_and_returned_correctly(http_server):
    """POST /send で needs_reply=true/false がそのまま保存され、history でその値が返る（true/false両方を検証）。"""
    base_url, _ = http_server
    code = _create_powwow(base_url)

    # needs_reply=True
    status, r1 = _send(base_url, code, "alice", "need reply", needs_reply=True)
    assert status == 200
    # needs_reply=False
    status, r2 = _send(base_url, code, "alice", "no reply needed", needs_reply=False)
    assert status == 200

    _, hist = _get(f"{base_url}/history?powwow={code}")
    msgs = {m["msg_id"]: m for m in hist["messages"]}

    assert msgs[r1["msg_id"]]["needs_reply"] is True, "needs_reply=True が正しく保存されていない"
    assert msgs[r2["msg_id"]]["needs_reply"] is False, "needs_reply=False が正しく保存されていない"


# ---------------------------------------------------------------------------
# エッジケース #19: GET /history?limit=N は最大N件に制限して返す
# ---------------------------------------------------------------------------

def test_case19_history_limit_restricts_results(http_server):
    """GET /history?limit=N は最大N件に制限して返す（since と併用可）。"""
    base_url, _ = http_server
    code = _create_powwow(base_url)

    for i in range(5):
        _send(base_url, code, "alice", f"msg-{i}")

    # limit=3 で3件のみ
    status, resp = _get(f"{base_url}/history?powwow={code}&limit=3")
    assert status == 200
    assert len(resp["messages"]) == 3, f"limit=3 なのに {len(resp['messages'])} 件返った"

    # since と併用: since=msg_id[1] かつ limit=2
    all_ids = [m["msg_id"] for m in resp["messages"]]
    since_id = all_ids[0]
    status2, resp2 = _get(f"{base_url}/history?powwow={code}&since={since_id}&limit=2")
    assert status2 == 200
    assert len(resp2["messages"]) <= 2, "since+limit の組合せで件数超過"
    for m in resp2["messages"]:
        assert m["msg_id"] > since_id, "since 以前のメッセージが含まれている"


# ---------------------------------------------------------------------------
# エッジケース #20: 任意の handle をそのまま受理する（真正性を検証しない）
# ---------------------------------------------------------------------------

def test_case20_arbitrary_handle_is_accepted(http_server):
    """server.py はクエリで渡された任意の handle をそのまま受理する（真正性を検証しない）。"""
    base_url, _ = http_server
    code = _create_powwow(base_url)

    # 普通のユーザー名
    status1, _ = _send(base_url, code, "alice", "hi")
    assert status1 == 200

    # 記号を含む任意の文字列
    status2, _ = _send(base_url, code, "handle-with-dashes_and.dots", "hi")
    assert status2 == 200

    # history で handle がそのまま保存されていることを確認
    _, hist = _get(f"{base_url}/history?powwow={code}")
    handles = [m["handle"] for m in hist["messages"]]
    assert "alice" in handles
    assert "handle-with-dashes_and.dots" in handles


# ---------------------------------------------------------------------------
# エッジケース #21: POST /create 時、last_activity_at が created_at と同値で初期化される
# ---------------------------------------------------------------------------

def test_case21_create_initializes_last_activity_at_equal_to_created_at(db):
    """POST /create 時、last_activity_at が created_at と同値で初期化される。"""
    code = srv.create_powwow(db)

    conn = sqlite3.connect(db)
    row = conn.execute(
        "SELECT created_at, last_activity_at FROM powwows WHERE powwow_code = ?",
        (code,),
    ).fetchone()
    conn.close()

    assert row is not None, "powwow が作成されていない"
    created_at, last_activity_at = row
    assert created_at == last_activity_at, (
        f"created_at={created_at} と last_activity_at={last_activity_at} が一致しない"
    )


# ---------------------------------------------------------------------------
# エッジケース #22: powwow_code は UNIQUE 制約があり衝突時リトライで一意発行される
# ---------------------------------------------------------------------------

def test_case22_powwow_code_unique_constraint_and_retry(db, monkeypatch):
    """powwow_code は UNIQUE 制約があり、衝突時リトライで一意発行される。

    `secrets.token_urlsafe` を monkeypatch で「最初の2回は同じ値、3回目は別値」
    に差し替え、create_powwow が:
      1. 1回目で "COLLIDE" を発行（成功）
      2. 2回目で "COLLIDE" を再発行 → INSERT で IntegrityError →
         `except sqlite3.IntegrityError: continue` 経路に入る
      3. リトライで "UNIQUE-2" を発行（成功・別値）
    という分岐を実際に通ることを検証する。「結果的に一意」だけでなく、
    リトライ経路（D#2287 の核心）が実行されることを保証する。
    """
    call_count = {"n": 0}
    values = ["COLLIDE", "COLLIDE", "UNIQUE-2"]

    def fake_token_urlsafe(nbytes: int = 8) -> str:
        v = values[call_count["n"]]
        call_count["n"] += 1
        return v

    monkeypatch.setattr(srv.secrets, "token_urlsafe", fake_token_urlsafe)

    # 1回目: "COLLIDE" が発行される
    code1 = srv.create_powwow(db)
    assert code1 == "COLLIDE", f"1回目の発行値が想定外: {code1}"
    assert call_count["n"] == 1, "1回目は1回呼び出しで成功すべき"

    # 2回目: "COLLIDE" を再試行 → IntegrityError → リトライ → "UNIQUE-2"
    code2 = srv.create_powwow(db)
    assert code2 == "UNIQUE-2", f"リトライ後の発行値が想定外: {code2}"
    assert call_count["n"] == 3, (
        "2回目は token_urlsafe が2回呼ばれる（衝突→リトライ）はず: "
        f"実呼び出し数={call_count['n'] - 1}"
    )

    # DB に両方の code が永続化されている（衝突値は1件のみ）
    conn = sqlite3.connect(db)
    rows = {
        r[0]
        for r in conn.execute("SELECT powwow_code FROM powwows").fetchall()
    }
    conn.close()
    assert rows == {"COLLIDE", "UNIQUE-2"}, f"DB に保存された code が想定外: {rows}"
