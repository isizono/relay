"""bridge_connect.py 新サブコマンド（create/history/presence）と _parse_bool の単体テスト。

PR-c/d 結合面修正で追加された機能を検証する。

- parse_create_command / parse_history_command / parse_presence_command のパース挙動
- mode_create / mode_history / mode_presence の URL 組み立て・stdout 出力
- _parse_bool の文字列→bool 変換ロジック（D#2307）
- mode_send / mode_recv の curl -sf 採用と HTTP エラー伝播（Major 1）
"""
import io
import json
import sys
from unittest.mock import patch

import pytest

import bridge_connect as bc


# ---------------------------------------------------------------------------
# parse_create_command
# ---------------------------------------------------------------------------

class TestParseCreateCommand:
    def test_valid(self):
        """'bridge create' は空 dict を返す。"""
        params = bc.parse_create_command("bridge create")
        assert params == {}

    def test_with_handle_in_command_is_ignored(self):
        """'bridge create --handle=evil' でも handle は除外される（D#2285）。"""
        params = bc.parse_create_command("bridge create --handle=evil")
        assert params == {}

    def test_not_create_returns_none(self):
        """'bridge send' や 'bridge recv' は None を返す。"""
        assert bc.parse_create_command("bridge send --powwow=X") is None
        assert bc.parse_create_command("bridge recv --powwow=X") is None
        assert bc.parse_create_command("") is None
        assert bc.parse_create_command("bridge") is None


# ---------------------------------------------------------------------------
# parse_history_command
# ---------------------------------------------------------------------------

class TestParseHistoryCommand:
    def test_valid_with_powwow_only(self):
        """'bridge history --powwow=X' は {"powwow": "X"} を返す。"""
        params = bc.parse_history_command("bridge history --powwow=abc123")
        assert params == {"powwow": "abc123"}

    def test_valid_with_since(self):
        """'bridge history --powwow=X --since=5' は since も含む。"""
        params = bc.parse_history_command("bridge history --powwow=abc --since=5")
        assert params == {"powwow": "abc", "since": "5"}

    def test_valid_with_since_and_limit(self):
        """since と limit を両方含む。"""
        params = bc.parse_history_command(
            "bridge history --powwow=abc --since=5 --limit=10"
        )
        assert params == {"powwow": "abc", "since": "5", "limit": "10"}

    def test_handle_ignored(self):
        """'bridge history --powwow=X --handle=evil' でも handle は除外される。"""
        params = bc.parse_history_command(
            "bridge history --powwow=abc --handle=evil"
        )
        assert "handle" not in params
        assert params["powwow"] == "abc"

    def test_not_history_returns_none(self):
        assert bc.parse_history_command("bridge send --powwow=X") is None
        assert bc.parse_history_command("bridge create") is None


# ---------------------------------------------------------------------------
# parse_presence_command
# ---------------------------------------------------------------------------

class TestParsePresenceCommand:
    def test_valid(self):
        """'bridge presence --powwow=X' は {"powwow": "X"} を返す。"""
        params = bc.parse_presence_command("bridge presence --powwow=abc")
        assert params == {"powwow": "abc"}

    def test_handle_ignored(self):
        params = bc.parse_presence_command(
            "bridge presence --powwow=abc --handle=evil"
        )
        assert "handle" not in params

    def test_not_presence_returns_none(self):
        assert bc.parse_presence_command("bridge send --powwow=X") is None
        assert bc.parse_presence_command("") is None


# ---------------------------------------------------------------------------
# _parse_bool
# ---------------------------------------------------------------------------

class TestParseBool:
    def test_string_true_lowercase(self):
        assert bc._parse_bool("true") is True

    def test_string_false_lowercase(self):
        assert bc._parse_bool("false") is False

    def test_string_true_capitalized(self):
        """'True' は True に変換される（大文字小文字無視）。"""
        assert bc._parse_bool("True") is True

    def test_string_false_uppercase(self):
        """'FALSE' は False に変換される（大文字小文字無視）。"""
        assert bc._parse_bool("FALSE") is False

    def test_bool_true(self):
        assert bc._parse_bool(True) is True

    def test_bool_false(self):
        assert bc._parse_bool(False) is False

    def test_empty_string(self):
        """空文字列は False（"" != "true"）。"""
        assert bc._parse_bool("") is False

    def test_invalid_string(self):
        """'true' 以外の文字列は False。仕様として現実装の挙動をアサート。"""
        assert bc._parse_bool("yes") is False
        assert bc._parse_bool("1") is False
        assert bc._parse_bool("on") is False

    def test_non_string_non_bool_truthy(self):
        """文字列でも bool でもない truthy 値は bool(value) で評価。"""
        # int / list は bool(value) 経由なので 0/[] → False、1/[x] → True
        assert bc._parse_bool(1) is True
        assert bc._parse_bool(0) is False
        assert bc._parse_bool([1, 2]) is True
        assert bc._parse_bool([]) is False

    def test_string_false_via_python_bool_would_be_true(self):
        """回帰テスト: bool('false') == True バグの修正確認（D#2307）。

        Python の bool() は非空文字列を True にするため、bool('false') == True。
        _parse_bool('false') が False を返すことで、バグが修正されていることを示す。
        """
        assert bool("false") is True  # Python のデフォルト挙動（バグの根本原因）
        assert bc._parse_bool("false") is False  # 修正後の正しい挙動


# ---------------------------------------------------------------------------
# mode_create
# ---------------------------------------------------------------------------

class TestModeCreate:
    def test_calls_create_url(self, capsys):
        """mode_create は POST {server}/create を呼ぶ。curl_fn 経由で URL を検証。"""
        received_urls = []

        def mock_create(url: str) -> int:
            received_urls.append(url)
            return 0

        result = bc.mode_create(
            server="http://127.0.0.1:8765",
            curl_fn=mock_create,
        )
        assert result == 0
        assert received_urls == ["http://127.0.0.1:8765/create"]


# ---------------------------------------------------------------------------
# mode_history
# ---------------------------------------------------------------------------

class TestModeHistory:
    def test_powwow_only(self):
        """history_params に powwow のみ → /history?powwow=X を呼ぶ。"""
        received_urls = []

        def mock_curl(url: str) -> int:
            received_urls.append(url)
            return 0

        result = bc.mode_history(
            history_params={"powwow": "abc"},
            server="http://127.0.0.1:8765",
            curl_fn=mock_curl,
        )
        assert result == 0
        assert len(received_urls) == 1
        url = received_urls[0]
        assert url.startswith("http://127.0.0.1:8765/history?")
        assert "powwow=abc" in url
        assert "since=" not in url
        assert "limit=" not in url

    def test_with_since_and_limit(self):
        """since/limit を含む history_params → URL に反映される。"""
        received_urls = []

        def mock_curl(url: str) -> int:
            received_urls.append(url)
            return 0

        bc.mode_history(
            history_params={"powwow": "abc", "since": "5", "limit": "10"},
            server="http://127.0.0.1:8765",
            curl_fn=mock_curl,
        )
        url = received_urls[0]
        assert "powwow=abc" in url
        assert "since=5" in url
        assert "limit=10" in url

    def test_missing_powwow_returns_error(self):
        """powwow なしは 1 を返す。"""
        called = []

        def mock_curl(url: str) -> int:
            called.append(url)
            return 0

        result = bc.mode_history(
            history_params={},
            server="http://127.0.0.1:8765",
            curl_fn=mock_curl,
        )
        assert result == 1
        assert called == [], "powwow なしで curl_fn が呼ばれた"


# ---------------------------------------------------------------------------
# mode_presence
# ---------------------------------------------------------------------------

class TestModePresence:
    def test_calls_presence_url(self):
        received_urls = []

        def mock_curl(url: str) -> int:
            received_urls.append(url)
            return 0

        result = bc.mode_presence(
            presence_params={"powwow": "abc"},
            server="http://127.0.0.1:8765",
            curl_fn=mock_curl,
        )
        assert result == 0
        url = received_urls[0]
        assert url.startswith("http://127.0.0.1:8765/presence?")
        assert "powwow=abc" in url

    def test_missing_powwow_returns_error(self):
        called = []

        def mock_curl(url: str) -> int:
            called.append(url)
            return 0

        result = bc.mode_presence(
            presence_params={},
            server="http://127.0.0.1:8765",
            curl_fn=mock_curl,
        )
        assert result == 1
        assert called == []


# ---------------------------------------------------------------------------
# run() の分岐: create/history/presence
# ---------------------------------------------------------------------------

class TestRunCreateHistoryPresence:
    def test_create_command_dispatches_to_mode_create(self):
        """'bridge create' で curl_create_fn が呼ばれる。"""
        called = []

        def mock_create(url: str) -> int:
            called.append(url)
            return 0

        result = bc.run(
            argv=["--handle=alice"],
            original_command="bridge create",
            curl_create_fn=mock_create,
        )
        assert result == 0
        assert len(called) == 1
        assert called[0].endswith("/create")

    def test_history_command_dispatches_to_mode_history(self):
        """'bridge history --powwow=X' で curl_history_fn が呼ばれる。"""
        called = []

        def mock_history(url: str) -> int:
            called.append(url)
            return 0

        result = bc.run(
            argv=["--handle=alice"],
            original_command="bridge history --powwow=abc --since=3",
            curl_history_fn=mock_history,
        )
        assert result == 0
        assert len(called) == 1
        assert "/history?" in called[0]
        assert "powwow=abc" in called[0]
        assert "since=3" in called[0]

    def test_presence_command_dispatches_to_mode_presence(self):
        """'bridge presence --powwow=X' で curl_presence_fn が呼ばれる。"""
        called = []

        def mock_presence(url: str) -> int:
            called.append(url)
            return 0

        result = bc.run(
            argv=["--handle=alice"],
            original_command="bridge presence --powwow=abc",
            curl_presence_fn=mock_presence,
        )
        assert result == 0
        assert len(called) == 1
        assert "/presence?" in called[0]
        assert "powwow=abc" in called[0]


# ---------------------------------------------------------------------------
# Major 1: HTTP エラー伝播（curl -sf 採用）
# ---------------------------------------------------------------------------

class TestCurlFailFlag:
    """curl は -f / -sf 付きで呼ばれ、HTTP 4xx/5xx で非0終了する（Major 1）。"""

    def _capture_curl_args(self, monkeypatch):
        """subprocess.run の呼び出し引数を捕捉するヘルパー。"""
        captured = []

        class FakeResult:
            returncode = 0
            stdout = "{}"
            stderr = ""

        def fake_run(args, **kwargs):
            captured.append(args)
            return FakeResult()

        monkeypatch.setattr(bc.subprocess, "run", fake_run)
        return captured

    def test_mode_create_uses_curl_fail_flag(self, monkeypatch, capsys):
        """mode_create の実 curl 呼び出しは -f / -sf を含む（HTTP エラーで非0終了させるため）。"""
        captured = self._capture_curl_args(monkeypatch)
        bc.mode_create(server="http://127.0.0.1:8765")
        assert len(captured) == 1
        args = captured[0]
        joined = " ".join(args)
        assert "-sf" in args or "-f" in args, f"curl に -f がない: {args}"

    def test_mode_history_uses_curl_fail_flag(self, monkeypatch):
        captured = self._capture_curl_args(monkeypatch)
        bc.mode_history(
            history_params={"powwow": "abc"},
            server="http://127.0.0.1:8765",
        )
        assert len(captured) == 1
        args = captured[0]
        assert "-sf" in args or "-f" in args, f"curl に -f がない: {args}"

    def test_mode_presence_uses_curl_fail_flag(self, monkeypatch):
        captured = self._capture_curl_args(monkeypatch)
        bc.mode_presence(
            presence_params={"powwow": "abc"},
            server="http://127.0.0.1:8765",
        )
        assert len(captured) == 1
        args = captured[0]
        assert "-sf" in args or "-f" in args, f"curl に -f がない: {args}"

    def test_mode_send_uses_curl_fail_flag(self, monkeypatch):
        captured = self._capture_curl_args(monkeypatch)
        bc.mode_send(
            handle="alice",
            send_params={"powwow": "abc", "body": "hi"},
            server="http://127.0.0.1:8765",
        )
        assert len(captured) == 1
        args = captured[0]
        assert "-sf" in args or "-f" in args, f"curl に -f がない: {args}"


class TestHttpErrorPropagation:
    """HTTP エラー時に curl の出力（stdout/stderr）を stderr に流して終了コードを伝播する。"""

    def _make_failing_run(self, stdout: str = "", stderr: str = ""):
        class FakeResult:
            returncode = 22  # curl の HTTP error 終了コード（参考）

        FakeResult.stdout = stdout
        FakeResult.stderr = stderr
        return FakeResult

    def test_mode_create_propagates_http_error(self, monkeypatch, capsys):
        """curl が非0終了したら mode_create も非0、エラーは stderr に流れる。"""
        result_obj = self._make_failing_run(
            stdout='{"error": "powwow が見つかりません"}',
            stderr="curl: (22) HTTP/1.1 404",
        )
        monkeypatch.setattr(bc.subprocess, "run", lambda *a, **kw: result_obj)

        ret = bc.mode_create(server="http://127.0.0.1:8765")
        assert ret != 0
        captured = capsys.readouterr()
        # stdout か stderr いずれかにエラー情報が含まれる
        combined = captured.err
        assert ("powwow が見つかりません" in combined) or ("404" in combined) or ("curl" in combined), \
            f"エラー情報が stderr に出ていない: stderr={captured.err!r}"

    def test_mode_history_propagates_http_error(self, monkeypatch, capsys):
        result_obj = self._make_failing_run(
            stdout='{"error": "powwow が見つかりません"}',
            stderr="",
        )
        monkeypatch.setattr(bc.subprocess, "run", lambda *a, **kw: result_obj)

        ret = bc.mode_history(
            history_params={"powwow": "missing"},
            server="http://127.0.0.1:8765",
        )
        assert ret != 0
        captured = capsys.readouterr()
        assert "powwow" in captured.err or "error" in captured.err.lower(), \
            f"エラー情報が stderr にない: {captured.err!r}"

    def test_mode_presence_propagates_http_error(self, monkeypatch, capsys):
        result_obj = self._make_failing_run(
            stdout='{"error": "powwow が見つかりません"}',
        )
        monkeypatch.setattr(bc.subprocess, "run", lambda *a, **kw: result_obj)

        ret = bc.mode_presence(
            presence_params={"powwow": "missing"},
            server="http://127.0.0.1:8765",
        )
        assert ret != 0
        captured = capsys.readouterr()
        assert "powwow" in captured.err or "error" in captured.err.lower()

    def test_mode_send_propagates_http_error(self, monkeypatch, capsys):
        result_obj = self._make_failing_run(
            stdout='{"error": "powwow が見つかりません"}',
        )
        monkeypatch.setattr(bc.subprocess, "run", lambda *a, **kw: result_obj)

        ret = bc.mode_send(
            handle="alice",
            send_params={"powwow": "missing", "body": "hi"},
            server="http://127.0.0.1:8765",
        )
        assert ret != 0
        captured = capsys.readouterr()
        assert "powwow" in captured.err or "error" in captured.err.lower()
