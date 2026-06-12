"""mcp_server.py テストスイート。

ssh 呼び出しを subprocess.run のモックで検証する。
各ツールが正しいコマンド・引数を組み立てることをアサートする。

shlex.join 対応後の argv 構造:
  ["ssh", *control_master_opts, HOST, remote_cmd]
  remote_cmd = shlex.join(["bridge", subcmd, "--flag=value", ...])
"""
import json
from unittest.mock import patch, MagicMock

import pytest

import mcp_server as ms


# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------

def _make_mock_run(stdout_data: dict):
    """subprocess.run をモックし、stdout に JSON を返す CompletedProcess を返すヘルパー。"""
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = json.dumps(stdout_data)
    mock_result.stderr = ""
    return mock_result


def _assert_control_master(args: list[str]) -> None:
    """ControlMaster オプション3点が args に含まれることを検証する。"""
    joined = " ".join(args)
    assert "ControlMaster=auto" in joined, f"ControlMaster=auto が args にない: {args}"
    assert "ControlPath=" in joined, f"ControlPath= が args にない: {args}"
    assert "ControlPersist=" in joined, f"ControlPersist= が args にない: {args}"


def _get_remote_cmd(args: list[str]) -> str:
    """ssh コマンドの最後の要素（リモートコマンド文字列）を取り出す。"""
    return args[-1]


# ---------------------------------------------------------------------------
# CreateChannel
# ---------------------------------------------------------------------------

class TestCreateChannel:
    def test_returns_channel_code(self):
        """CreateChannel は {"channel_code": "abc123"} を返す。"""
        mock_result = _make_mock_run({"channel_code": "abc123"})
        with patch("subprocess.run", return_value=mock_result):
            result = ms.CreateChannel()
        assert result == {"channel_code": "abc123"}

    def test_calls_bridge_create(self):
        """CreateChannel は ssh ... 'bridge create' を呼ぶ。"""
        mock_result = _make_mock_run({"channel_code": "abc123"})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.CreateChannel()
        args = mock_run.call_args[0][0]
        assert "ssh" in args
        remote_cmd = _get_remote_cmd(args)
        assert "bridge" in remote_cmd
        assert "create" in remote_cmd

    def test_control_master_options(self):
        """CreateChannel は ControlMaster オプション付きで ssh を呼ぶ。"""
        mock_result = _make_mock_run({"channel_code": "abc123"})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.CreateChannel()
        args = mock_run.call_args[0][0]
        _assert_control_master(args)


# ---------------------------------------------------------------------------
# SendMessage
# ---------------------------------------------------------------------------

class TestSendMessage:
    def test_argv_structure_basic(self):
        """SendMessage はリモートコマンドに --channel=, --body=, --needs-reply=false を含む。"""
        mock_result = _make_mock_run({"msg_id": 1})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.SendMessage(
                channel_code="test-code",
                body="Hello world",
                needs_reply=False,
            )
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "--channel=test-code" in remote_cmd
        assert "--needs-reply=false" in remote_cmd

    def test_body_with_spaces_is_quoted(self):
        """空白を含む body が shlex.join でクォートされてリモートコマンドに含まれる（D#2308）。

        shlex.join は '--body=hello world' のようにフラグ全体をクォートする。
        bridge-connect 側の shlex.split で正しく復元される形式になっていることを確認する。
        """
        mock_result = _make_mock_run({"msg_id": 1})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.SendMessage(
                channel_code="test-code",
                body="hello world",
                needs_reply=False,
            )
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        # shlex.join はフラグ全体をクォートする: '--body=hello world'
        assert "'--body=hello world'" in remote_cmd or '"--body=hello world"' in remote_cmd, \
            f"空白含む body フラグがクォートされていない: {remote_cmd}"
        # shlex.split で復元したとき '--body=hello world' が1トークンになること
        import shlex
        tokens = shlex.split(remote_cmd)
        body_token = next((t for t in tokens if t.startswith("--body=")), None)
        assert body_token == "--body=hello world", \
            f"shlex.split 後の body トークンが期待値と異なる: {body_token}"

    def test_no_handle_in_argv(self):
        """SendMessage は --handle= を含まない（handle は server 側で forced command 固定、D#2285）。"""
        mock_result = _make_mock_run({"msg_id": 1})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.SendMessage(channel_code="code", body="hi")
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "--handle=" not in remote_cmd, f"handle が含まれている: {remote_cmd}"

    def test_in_reply_to_included_when_set(self):
        """in_reply_to 指定時は --in-reply-to=N が含まれる（ハイフン区切り、D#2308）。"""
        mock_result = _make_mock_run({"msg_id": 2})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.SendMessage(channel_code="code", body="reply", in_reply_to=10)
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "--in-reply-to=10" in remote_cmd, f"--in-reply-to=10 がない: {remote_cmd}"

    def test_in_reply_to_omitted_when_none(self):
        """in_reply_to=None のとき --in-reply-to= は含まれない。"""
        mock_result = _make_mock_run({"msg_id": 3})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.SendMessage(channel_code="code", body="hi", in_reply_to=None)
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "--in-reply-to=" not in remote_cmd, f"--in-reply-to= が含まれている: {remote_cmd}"

    def test_needs_reply_true_as_lowercase_string(self):
        """needs_reply=True は --needs-reply=true（小文字文字列）でリモートコマンドに入る（D#2307/2308）。"""
        mock_result = _make_mock_run({"msg_id": 4})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.SendMessage(channel_code="code", body="hi", needs_reply=True)
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "--needs-reply=true" in remote_cmd, f"--needs-reply=true がない: {remote_cmd}"

    def test_needs_reply_false_as_lowercase_string(self):
        """needs_reply=False は --needs-reply=false（小文字文字列）でリモートコマンドに入る（D#2307）。"""
        mock_result = _make_mock_run({"msg_id": 5})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.SendMessage(channel_code="code", body="hi", needs_reply=False)
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "--needs-reply=false" in remote_cmd, f"--needs-reply=false がない: {remote_cmd}"

    def test_control_master_options(self):
        """SendMessage は ControlMaster オプション付きで ssh を呼ぶ。"""
        mock_result = _make_mock_run({"msg_id": 1})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.SendMessage(channel_code="code", body="hi")
        args = mock_run.call_args[0][0]
        _assert_control_master(args)

    def test_bridge_failure_raises_error(self):
        """bridge send が非0終了の場合 RuntimeError が送出される。"""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "auth failed"
        mock_result.stdout = ""
        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError) as exc_info:
                ms.SendMessage(channel_code="code", body="hi")
        assert "1" in str(exc_info.value)
        assert "send" in str(exc_info.value)


# ---------------------------------------------------------------------------
# GetHistory
# ---------------------------------------------------------------------------

class TestGetHistory:
    def _sample_messages(self):
        return [
            {"msg_id": 1, "handle": "alice", "body": "hi",
             "needs_reply": False, "in_reply_to": None, "created_at": "2026-01-01T00:00:00+00:00"},
        ]

    def test_since_included_when_set(self):
        """GetHistory(since=5) は --since=5 をリモートコマンドに含める。"""
        mock_result = _make_mock_run({"messages": self._sample_messages()})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.GetHistory(channel_code="code", since=5)
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "--since=5" in remote_cmd, f"--since=5 がない: {remote_cmd}"

    def test_since_omitted_when_not_set(self):
        """GetHistory(since=None) は --since= を含まない。"""
        mock_result = _make_mock_run({"messages": self._sample_messages()})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.GetHistory(channel_code="code", since=None)
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "--since=" not in remote_cmd, f"--since= が含まれている: {remote_cmd}"

    def test_limit_included_when_set(self):
        """GetHistory(limit=10) は --limit=10 をリモートコマンドに含める。"""
        mock_result = _make_mock_run({"messages": self._sample_messages()})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.GetHistory(channel_code="code", limit=10)
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "--limit=10" in remote_cmd, f"--limit=10 がない: {remote_cmd}"

    def test_limit_omitted_when_not_set(self):
        """GetHistory(limit=None) は --limit= を含まない。"""
        mock_result = _make_mock_run({"messages": self._sample_messages()})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.GetHistory(channel_code="code", limit=None)
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "--limit=" not in remote_cmd, f"--limit= が含まれている: {remote_cmd}"

    def test_returns_messages(self):
        """GetHistory はレスポンス JSON の messages を返す。"""
        messages = self._sample_messages()
        mock_result = _make_mock_run({"messages": messages})
        with patch("subprocess.run", return_value=mock_result):
            result = ms.GetHistory(channel_code="code")
        assert result == {"messages": messages}

    def test_control_master_options(self):
        """GetHistory は ControlMaster オプション付きで ssh を呼ぶ。"""
        mock_result = _make_mock_run({"messages": []})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.GetHistory(channel_code="code")
        args = mock_run.call_args[0][0]
        _assert_control_master(args)

    def test_bridge_failure_raises_error(self):
        """bridge history が非0終了の場合 RuntimeError が送出される。"""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "not found"
        mock_result.stdout = ""
        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError) as exc_info:
                ms.GetHistory(channel_code="code")
        assert "1" in str(exc_info.value)
        assert "history" in str(exc_info.value)


# ---------------------------------------------------------------------------
# GetPresence
# ---------------------------------------------------------------------------

class TestGetPresence:
    def test_returns_handles(self):
        """GetPresence は {"handles": [...]} を返す。"""
        mock_result = _make_mock_run({"handles": ["alice", "bob"]})
        with patch("subprocess.run", return_value=mock_result):
            result = ms.GetPresence(channel_code="code")
        assert result == {"handles": ["alice", "bob"]}

    def test_calls_bridge_presence(self):
        """GetPresence は ssh ... 'bridge presence --channel=...' を呼ぶ。"""
        mock_result = _make_mock_run({"handles": []})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.GetPresence(channel_code="abc")
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "bridge" in remote_cmd
        assert "presence" in remote_cmd
        assert "--channel=abc" in remote_cmd

    def test_control_master_options(self):
        """GetPresence は ControlMaster オプション付きで ssh を呼ぶ。"""
        mock_result = _make_mock_run({"handles": []})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms.GetPresence(channel_code="code")
        args = mock_run.call_args[0][0]
        _assert_control_master(args)

    def test_bridge_failure_raises_error(self):
        """bridge presence が非0終了の場合 RuntimeError が送出される。"""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "error"
        mock_result.stdout = ""
        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError) as exc_info:
                ms.GetPresence(channel_code="code")
        assert "1" in str(exc_info.value)
        assert "presence" in str(exc_info.value)


# ---------------------------------------------------------------------------
# _bridge ヘルパー（単体）
# ---------------------------------------------------------------------------

class TestBridgeHelper:
    def test_argv_structure(self):
        """_bridge は ["ssh", *control_master_opts, HOST, remote_cmd] の構造でサブプロセスを呼ぶ。"""
        mock_result = _make_mock_run({})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms._bridge("create")
        args = mock_run.call_args[0][0]
        assert args[0] == "ssh", f"先頭が ssh でない: {args}"
        assert args[-1] == "bridge create", f"末尾が 'bridge create' でない: {args[-1]}"

    def test_kwargs_become_flags_with_hyphens(self):
        """kwargs のアンダースコアはハイフンに変換されてリモートコマンドに含まれる（D#2308）。"""
        mock_result = _make_mock_run({})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms._bridge("send", channel="code", needs_reply="true", in_reply_to=5)
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "--needs-reply=true" in remote_cmd, f"needs_reply のハイフン変換失敗: {remote_cmd}"
        assert "--in-reply-to=5" in remote_cmd, f"in_reply_to のハイフン変換失敗: {remote_cmd}"

    def test_none_kwargs_are_skipped(self):
        """kwargs の値が None のものは --flag=None として追加されない。"""
        mock_result = _make_mock_run({})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms._bridge("history", channel="code", since=None, limit=None)
        args = mock_run.call_args[0][0]
        remote_cmd = _get_remote_cmd(args)
        assert "--since=" not in remote_cmd
        assert "--limit=" not in remote_cmd

    def test_nonzero_exit_raises_runtime_error(self):
        """bridge が非0終了の場合 RuntimeError を送出する。"""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "err"
        mock_result.stdout = ""
        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError):
                ms._bridge("create")

    def test_capture_output_and_text(self):
        """subprocess.run は capture_output=True, text=True で呼ばれる。"""
        mock_result = _make_mock_run({})
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            ms._bridge("create")
        kwargs = mock_run.call_args[1]
        assert kwargs.get("capture_output") is True
        assert kwargs.get("text") is True
