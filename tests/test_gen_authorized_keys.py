"""gen_authorized_keys / bridge-connect のテストスイート。

エッジケース表 #1〜#5 をカバーする:
  #1: gen_authorized_keys は各ユーザーに forced command 前置の鍵行を出力する
  #2: members.txt 重複ユーザー名は1回に正規化される
  #3: bridge-connect は $SSH_ORIGINAL_COMMAND="bridge recv --powwow=X" で受信モードに分岐する（D#2302）
  #4: bridge-connect は "bridge send ..." のとき送信モードに分岐する
  #5: bridge-connect は handle を --handle 引数からのみ取り、ORIGINAL_COMMAND 内の handle を無視する
"""
import json
import subprocess
from pathlib import Path

import pytest

# テスト対象モジュールをインポート（pythonpath = ["."] 設定済み）
import gen_authorized_keys as gak
import bridge_connect as bc


# ---------------------------------------------------------------------------
# フィクスチャ
# ---------------------------------------------------------------------------

@pytest.fixture()
def members_file(tmp_path):
    """一時 members.txt ファイルを返す。内容は呼び出し元が書き込む。"""
    return tmp_path / "members.txt"


def _mock_fetch(keys_map: dict):
    """(username -> keys_string) の dict を受け取り、fetch_fn を返す。"""
    def fetch_fn(username: str) -> str:
        return keys_map.get(username, "")
    return fetch_fn


# ---------------------------------------------------------------------------
# エッジケース #1: 各ユーザーに forced command 前置の鍵行が出力される
# ---------------------------------------------------------------------------

def test_case01_forced_command_prepended_to_each_key(tmp_path):
    """gen_authorized_keys は members.txt の各ユーザーにつき、
    `command="bridge-connect --handle=<user>",no-pty...` を前置した鍵行を出力する。

    エッジケース #1（D#2264）に対応。
    """
    members = tmp_path / "members.txt"
    members.write_text("alice\nbob\n")

    fake_keys = {
        "alice": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA alice@github\n",
        "bob": "ssh-rsa AAAAB3NzaC1yc2EAAA bob@github\n",
    }

    bridge_path = "/usr/local/bin/bridge-connect"
    result = gak.generate_authorized_keys(
        members_path=members,
        bridge_connect_path=bridge_path,
        fetch_fn=_mock_fetch(fake_keys),
    )

    lines = [l for l in result.splitlines() if l.strip()]
    assert len(lines) == 2, f"鍵行数が2でない: {lines}"

    # alice の行
    alice_line = lines[0]
    assert alice_line.startswith(
        f'command="{bridge_path} --handle=alice",no-pty,no-port-forwarding,no-X11-forwarding,no-agent-forwarding'
    ), f"alice の forced command が不正: {alice_line}"
    assert "ssh-ed25519" in alice_line, f"alice の鍵が含まれていない: {alice_line}"

    # bob の行
    bob_line = lines[1]
    assert bob_line.startswith(
        f'command="{bridge_path} --handle=bob",no-pty,no-port-forwarding,no-X11-forwarding,no-agent-forwarding'
    ), f"bob の forced command が不正: {bob_line}"
    assert "ssh-rsa" in bob_line, f"bob の鍵が含まれていない: {bob_line}"


def test_case01_multiple_keys_per_user(tmp_path):
    """1ユーザーが複数鍵を持つ場合、各鍵行に forced command が前置される。"""
    members = tmp_path / "members.txt"
    members.write_text("alice\n")

    fake_keys = {
        "alice": (
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA alice@laptop\n"
            "ssh-rsa AAAAB3NzaC1yc2EAAA alice@desktop\n"
        ),
    }

    bridge_path = "/usr/local/bin/bridge-connect"
    result = gak.generate_authorized_keys(
        members_path=members,
        bridge_connect_path=bridge_path,
        fetch_fn=_mock_fetch(fake_keys),
    )

    lines = [l for l in result.splitlines() if l.strip()]
    assert len(lines) == 2, f"alice の鍵2行が出力されるべき: {lines}"
    for line in lines:
        assert '--handle=alice"' in line, f"alice の forced command がない行: {line}"


# ---------------------------------------------------------------------------
# エッジケース #2: 重複ユーザー名は1回に正規化される
# ---------------------------------------------------------------------------

def test_case02_duplicate_usernames_deduplicated(tmp_path):
    """members.txt に同一ユーザー名が重複しても、authorized_keys 出力では1ユーザー1回に正規化される。

    エッジケース #2（M#179 §8）に対応。
    """
    members = tmp_path / "members.txt"
    # alice を2回書く
    members.write_text("alice\nbob\nalice\n")

    fake_keys = {
        "alice": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA alice@github\n",
        "bob": "ssh-rsa AAAAB3NzaC1yc2EAAA bob@github\n",
    }

    bridge_path = "/usr/local/bin/bridge-connect"
    result = gak.generate_authorized_keys(
        members_path=members,
        bridge_connect_path=bridge_path,
        fetch_fn=_mock_fetch(fake_keys),
    )

    lines = [l for l in result.splitlines() if l.strip()]
    # alice の行は1行のみ（重複排除）
    alice_lines = [l for l in lines if "--handle=alice" in l]
    assert len(alice_lines) == 1, f"alice が重複している（{len(alice_lines)}行）: {alice_lines}"
    # bob の行も1行
    bob_lines = [l for l in lines if "--handle=bob" in l]
    assert len(bob_lines) == 1, f"bob が消えた: {bob_lines}"


def test_case02_parse_members_deduplication():
    """parse_members は重複ユーザー名を排除し、出現順で最初のものを残す。"""
    import tempfile, os
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("alice\nbob\nalice\ncharlie\nbob\n")
        fname = f.name
    try:
        users = gak.parse_members(Path(fname))
        assert users == ["alice", "bob", "charlie"], f"重複排除が正しくない: {users}"
    finally:
        os.unlink(fname)


def test_case02_comments_and_blank_lines_ignored(tmp_path):
    """members.txt のコメント行・空行は無視される。"""
    members = tmp_path / "members.txt"
    members.write_text("# コメント\n\nalice\n  \n# 別コメント\nbob\n")

    users = gak.parse_members(members)
    assert users == ["alice", "bob"], f"コメント・空行除去が正しくない: {users}"


# ---------------------------------------------------------------------------
# エッジケース #3: $SSH_ORIGINAL_COMMAND="bridge recv --powwow=X" で受信モード分岐（D#2302）
# ---------------------------------------------------------------------------

def test_case03_bridge_recv_command_triggers_recv_mode():
    """bridge-connect は $SSH_ORIGINAL_COMMAND="bridge recv --powwow=X" で受信モードに分岐する。

    エッジケース #3（D#2272, D#2302）に対応。
    curl_recv_fn に渡された URL が SSE 購読 URL かつ handle が --handle 由来であることを確認。
    """
    received_urls = []

    def mock_recv(url: str) -> int:
        received_urls.append(url)
        return 0

    exit_code = bc.run(
        argv=["--handle=alice", "--server=http://127.0.0.1:8765"],
        original_command="bridge recv --powwow=testcode123",
        curl_recv_fn=mock_recv,
        curl_send_fn=None,
    )

    assert exit_code == 0, f"終了コードが0でない: {exit_code}"
    assert len(received_urls) == 1, f"curl_recv_fn が1回呼ばれていない: {received_urls}"

    url = received_urls[0]
    assert "/stream?" in url, f"受信 URL が /stream を含まない: {url}"
    assert "handle=alice" in url, f"handle が URL に含まれていない: {url}"
    assert "powwow=testcode123" in url, f"powwow_code が URL に含まれていない: {url}"


def test_case03_empty_original_command_returns_error():
    """$SSH_ORIGINAL_COMMAND が空のときはエラー（受信モード分岐しない、D#2302）。"""
    exit_code = bc.run(
        argv=["--handle=alice", "--server=http://127.0.0.1:8765"],
        original_command="",
        curl_recv_fn=lambda url: 0,
    )
    assert exit_code != 0, "空 ORIGINAL_COMMAND はエラーになるべき"


def test_case03_whitespace_only_original_command_returns_error():
    """$SSH_ORIGINAL_COMMAND が空白のみでもエラー（D#2302）。"""
    exit_code = bc.run(
        argv=["--handle=alice", "--server=http://127.0.0.1:8765"],
        original_command="   ",
        curl_recv_fn=lambda url: 0,
    )
    assert exit_code != 0


def test_case03_recv_url_uses_urllib_quote():
    """受信URLの powwow/handle 値は urllib でクエリエンコードされる（D#2302、観点6-3）。

    `&` や `#` を含む powwow_code でも URL が破綻しないこと。
    """
    received_urls = []

    def mock_recv(url: str) -> int:
        received_urls.append(url)
        return 0

    bc.run(
        argv=["--handle=alice", "--server=http://127.0.0.1:8765"],
        original_command="bridge recv --powwow=a&b#c",
        curl_recv_fn=mock_recv,
    )

    assert len(received_urls) == 1
    url = received_urls[0]
    # & と # が URL エンコードされている（'%26', '%23'）こと
    assert "a%26b%23c" in url, f"powwow_code が URL エンコードされていない: {url}"
    # 生の & や # が powwow= の値部分に直接埋まっていないこと
    assert "powwow=a&b" not in url, f"生の & が埋まっている: {url}"


# ---------------------------------------------------------------------------
# エッジケース #4: "bridge send ..." のとき送信モードに分岐する
# ---------------------------------------------------------------------------

def test_case04_bridge_send_command_triggers_send_mode():
    """bridge-connect は $SSH_ORIGINAL_COMMAND が "bridge send ..." のとき送信モードに分岐する。

    エッジケース #4（D#2272）に対応。
    curl_send_fn に渡された URL と payload が正しいことを確認。
    """
    sent_requests = []

    def mock_send(url: str, body: str) -> int:
        sent_requests.append({"url": url, "body": body})
        return 0

    exit_code = bc.run(
        argv=["--handle=bob", "--server=http://127.0.0.1:8765"],
        original_command="bridge send --powwow=abc123 --body=hello",
        curl_recv_fn=None,
        curl_send_fn=mock_send,
    )

    assert exit_code == 0, f"終了コードが0でない: {exit_code}"
    assert len(sent_requests) == 1, f"curl_send_fn が1回呼ばれていない: {sent_requests}"

    req = sent_requests[0]
    assert "/send" in req["url"], f"送信 URL が /send を含まない: {req['url']}"

    payload = json.loads(req["body"])
    assert payload["powwow"] == "abc123", f"powwow_code が不正: {payload}"
    assert payload["body"] == "hello", f"body が不正: {payload}"


def test_case04_bridge_send_with_needs_reply():
    """bridge send --needs-reply フラグが needs_reply=True として POST される。"""
    sent_requests = []

    def mock_send(url: str, body: str) -> int:
        sent_requests.append(json.loads(body))
        return 0

    bc.run(
        argv=["--handle=alice", "--server=http://127.0.0.1:8765"],
        original_command="bridge send --powwow=xyz --body=test --needs-reply",
        curl_send_fn=mock_send,
    )

    assert len(sent_requests) == 1
    assert sent_requests[0]["needs_reply"] is True, "needs_reply が True になっていない"


def test_case04_bridge_send_with_in_reply_to():
    """bridge send --in-reply-to=N が in_reply_to=N（整数）として POST される。"""
    sent_requests = []

    def mock_send(url: str, body: str) -> int:
        sent_requests.append(json.loads(body))
        return 0

    bc.run(
        argv=["--handle=alice", "--server=http://127.0.0.1:8765"],
        original_command="bridge send --powwow=xyz --body=test --in-reply-to=42",
        curl_send_fn=mock_send,
    )

    assert len(sent_requests) == 1
    assert sent_requests[0]["in_reply_to"] == 42, "in_reply_to が整数42になっていない"


# ---------------------------------------------------------------------------
# エッジケース #5: handle は --handle 引数からのみ取り、ORIGINAL_COMMAND 内を無視する
# ---------------------------------------------------------------------------

def test_case05_handle_from_flag_not_from_original_command():
    """bridge-connect は handle を自身の --handle 引数からのみ取り、
    ORIGINAL_COMMAND 内の --handle 指定を無視する（詐称防止、D#2285/D#2264）。

    エッジケース #5 に対応。
    """
    sent_requests = []

    def mock_send(url: str, body: str) -> int:
        sent_requests.append(json.loads(body))
        return 0

    # ORIGINAL_COMMAND に別ユーザー名 "evil-user" を混入
    exit_code = bc.run(
        argv=["--handle=alice", "--server=http://127.0.0.1:8765"],
        original_command="bridge send --powwow=abc --body=hi --handle=evil-user",
        curl_send_fn=mock_send,
    )

    assert exit_code == 0, f"終了コードが0でない: {exit_code}"
    assert len(sent_requests) == 1, f"リクエストが1件でない: {sent_requests}"

    payload = sent_requests[0]
    assert payload["handle"] == "alice", (
        f"handle が forced command 由来の 'alice' ではなく '{payload['handle']}' になっている（詐称防止失敗）"
    )
    assert payload["handle"] != "evil-user", "ORIGINAL_COMMAND 内の evil-user が採用されてしまった"


def test_case05_handle_from_recv_url_not_from_original_command():
    """受信モードでも handle は --handle 引数からのみ URL に埋め込まれる。

    ORIGINAL_COMMAND 内に "--handle=evil-user" を混ぜても無視される（D#2285, D#2302）。
    """
    received_urls = []

    def mock_recv(url: str) -> int:
        received_urls.append(url)
        return 0

    bc.run(
        argv=["--handle=carol", "--server=http://127.0.0.1:8765"],
        original_command="bridge recv --powwow=testcode --handle=evil-user",
        curl_recv_fn=mock_recv,
    )

    assert len(received_urls) == 1
    url = received_urls[0]
    assert "handle=carol" in url, f"受信 URL の handle が carol でない: {url}"
    assert "handle=evil-user" not in url, f"ORIGINAL_COMMAND の handle が採用された: {url}"


# ---------------------------------------------------------------------------
# 補助テスト: parse_send_command
# ---------------------------------------------------------------------------

def test_parse_send_command_valid():
    """"bridge send --powwow=X --body=Y" が正しくパースされる。"""
    params = bc.parse_send_command("bridge send --powwow=abc123 --body=hello")
    assert params is not None
    assert params["powwow"] == "abc123"
    assert params["body"] == "hello"


def test_parse_send_command_not_bridge():
    """"ssh fwd" などの不明コマンドは None を返す。"""
    assert bc.parse_send_command("ssh fwd") is None
    assert bc.parse_send_command("") is None
    assert bc.parse_send_command("bridge") is None


def test_parse_send_command_handle_ignored():
    """ORIGINAL_COMMAND 内の --handle は parse_send_command がパース結果から除外する。"""
    params = bc.parse_send_command("bridge send --powwow=X --body=Y --handle=attacker")
    assert params is not None
    assert "handle" not in params, "parse_send_command が ORIGINAL_COMMAND の handle を受け取った"


# ---------------------------------------------------------------------------
# 補助テスト: build_authorized_keys_lines のフォーマット検証
# ---------------------------------------------------------------------------

def test_build_authorized_keys_lines_format():
    """authorized_keys 行フォーマットが仕様通りか検証する（D#2264）。"""
    lines = gak.build_authorized_keys_lines(
        username="alice",
        raw_keys=["ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA alice@github"],
        bridge_connect_path="/path/to/bridge-connect",
    )
    assert len(lines) == 1
    line = lines[0]
    # フォーマット確認
    expected_prefix = (
        'command="/path/to/bridge-connect --handle=alice"'
        ",no-pty,no-port-forwarding,no-X11-forwarding,no-agent-forwarding"
    )
    assert line.startswith(expected_prefix), f"フォーマット不正: {line}"
    assert "ssh-ed25519" in line, "鍵が含まれていない"


# ---------------------------------------------------------------------------
# 補助テスト: missing --handle のエラー処理
# ---------------------------------------------------------------------------

def test_missing_handle_returns_error():
    """--handle が指定されていない場合、run() は 0 以外を返す。"""
    exit_code = bc.run(argv=[], original_command="")
    assert exit_code != 0, "--handle なしで終了コード0になってしまった"


def test_recv_mode_missing_powwow_returns_error():
    """受信モードで $SSH_ORIGINAL_COMMAND="bridge recv" のみ（--powwow なし）はエラー（D#2302）。"""
    exit_code = bc.run(
        argv=["--handle=alice", "--server=http://127.0.0.1:8765"],
        original_command="bridge recv",
        curl_recv_fn=lambda url: 0,
    )
    assert exit_code != 0, "--powwow なし bridge recv で終了コード0になってしまった"


# ---------------------------------------------------------------------------
# 追加テスト: shlex によるパース（観点5-4）— 本文に空白・引用符を含む送信
# ---------------------------------------------------------------------------

def test_parse_send_command_shlex_quoted_body():
    """--body='hello world' のように引用符付きの本文が shlex で正しくパースされる。"""
    params = bc.parse_send_command("bridge send --powwow=X --body='hello world'")
    assert params is not None
    assert params["body"] == "hello world", f"shlex パース不正: {params}"


def test_parse_send_command_shlex_body_with_special_chars():
    """本文に ; & を含むケース。shlex は引用符内をそのまま保持する。"""
    params = bc.parse_send_command('bridge send --powwow=X --body="a;b&c"')
    assert params is not None
    assert params["body"] == "a;b&c"


def test_parse_send_command_unclosed_quote_returns_none():
    """shlex がパース失敗する不正な引用は None を返す（クラッシュしない）。"""
    assert bc.parse_send_command("bridge send --body='unclosed") is None


def test_parse_recv_command_valid():
    """"bridge recv --powwow=X" が正しくパースされる（D#2302）。"""
    params = bc.parse_recv_command("bridge recv --powwow=abc123")
    assert params is not None
    assert params["powwow"] == "abc123"


def test_parse_recv_command_not_recv():
    """"bridge send ..." や不明コマンドは parse_recv_command が None を返す。"""
    assert bc.parse_recv_command("bridge send --powwow=X") is None
    assert bc.parse_recv_command("") is None
    assert bc.parse_recv_command("ssh fwd") is None


def test_parse_recv_command_handle_ignored():
    """recv コマンド内の --handle も parse_recv_command がパース結果から除外する（D#2285）。"""
    params = bc.parse_recv_command("bridge recv --powwow=X --handle=attacker")
    assert params is not None
    assert "handle" not in params


# ---------------------------------------------------------------------------
# 追加テスト: members.txt ユーザー名バリデーション（観点5-3/6-4）
# ---------------------------------------------------------------------------

def test_parse_members_rejects_shell_injection(tmp_path):
    """members.txt にシェルメタ文字を含む名前があると sys.exit(1) で停止する。"""
    members = tmp_path / "members.txt"
    members.write_text('alice\nbad"; rm -rf / #\nbob\n')
    with pytest.raises(SystemExit) as exc_info:
        gak.parse_members(members)
    assert exc_info.value.code == 1


def test_parse_members_rejects_underscore(tmp_path):
    """アンダースコアを含む名前は GitHub 規則違反で停止する。"""
    members = tmp_path / "members.txt"
    members.write_text("alice\nbad_user\n")
    with pytest.raises(SystemExit):
        gak.parse_members(members)


def test_parse_members_rejects_too_long(tmp_path):
    """40 文字以上のユーザー名は停止する（GitHub規則は最大39文字）。"""
    members = tmp_path / "members.txt"
    members.write_text("a" * 40 + "\n")
    with pytest.raises(SystemExit):
        gak.parse_members(members)


def test_parse_members_rejects_leading_hyphen(tmp_path):
    """先頭ハイフンのユーザー名は停止する（authorized_keys options 誤解釈防止）。"""
    members = tmp_path / "members.txt"
    members.write_text("-evil\n")
    with pytest.raises(SystemExit):
        gak.parse_members(members)


def test_parse_members_accepts_valid_usernames(tmp_path):
    """英数字とハイフンのみで構成された 1-39 文字のユーザー名は許可される。"""
    members = tmp_path / "members.txt"
    members.write_text("alice\nbob-1\nUserName\nA\n" + "a" * 39 + "\n")
    users = gak.parse_members(members)
    assert users == ["alice", "bob-1", "UserName", "A", "a" * 39]


# ---------------------------------------------------------------------------
# 追加テスト: _curl_fetch_keys の失敗時 exit code（観点5-2）
# ---------------------------------------------------------------------------

def test_curl_fetch_keys_raises_on_failure(monkeypatch):
    """curl -sSf が non-zero で終了したら CalledProcessError が伝播する（silent skip しない）。"""
    def fake_run(cmd, **kwargs):
        # check=True 経路を再現
        raise subprocess.CalledProcessError(returncode=22, cmd=cmd)

    monkeypatch.setattr(gak.subprocess, "run", fake_run)
    with pytest.raises(subprocess.CalledProcessError):
        gak._curl_fetch_keys("nonexistent-user-99999")
