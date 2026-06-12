#!/usr/bin/env python3
"""bridge-connect — SSH forced command ラッパー。

authorized_keys で各鍵行に `command="bridge-connect --handle=<user>"` として
forced command が設定される。channel_code は forced command には焼かず、
$SSH_ORIGINAL_COMMAND から取得する（D#2302）。

分岐ロジック（D#2272, D#2302, D#2307）:
  "bridge recv --channel=X"                  → 受信モード（SSE購読を stdout へ中継）
  "bridge send --channel=X --body=Y ..."     → 送信モード（POST /send を実行）
  "bridge create"                            → 作成モード（POST /create を実行）
  "bridge history --channel=X [--since=N] [--limit=N]" → 履歴取得モード（GET /history）
  "bridge presence --channel=X"             → 接続中handle一覧モード（GET /presence）
  それ以外（空含む）                         → エラー

handle は必ず自身の --handle 引数からのみ取得し、$SSH_ORIGINAL_COMMAND 内の
handle 指定は無視する（handle 詐称防止、D#2285）。

使い方（forced command として authorized_keys から自動呼び出し）:
    command="/path/to/bridge-connect --handle=alice",no-pty,... <key>

引数:
    --handle=<user>   handle（authorized_keys で固定される）[必須]
    --server=<url>    サーバーベースURL（デフォルト: http://127.0.0.1:8765）

クライアント側は ssh host "bridge recv --channel=X" / "bridge send --channel=X --body=Y" で呼び出す。
"""
import json
import os
import shlex
import subprocess
import sys
import urllib.parse


SERVER_DEFAULT = "http://127.0.0.1:8765"


def parse_args(argv: list[str]) -> dict:
    """--key=value 形式の引数をパースして dict で返す。

    --handle=alice → {"handle": "alice"}
    """
    result: dict = {}
    for arg in argv:
        if arg.startswith("--"):
            key_value = arg[2:]
            if "=" in key_value:
                key, value = key_value.split("=", 1)
                result[key] = value
            else:
                result[key_value] = True
    return result


def _parse_subcommand(original_cmd: str, expected_subcmd: str) -> dict | None:
    """$SSH_ORIGINAL_COMMAND を shlex.split で安全にパースし、bridge <subcmd> 形式か判定する。

    handle は $SSH_ORIGINAL_COMMAND 内から取得しない（詐称防止、D#2285）。
    expected_subcmd 以外なら None。shlex で空白・引用符を正しく扱う。
    """
    try:
        tokens = shlex.split(original_cmd)
    except ValueError:
        return None
    if len(tokens) < 2 or tokens[0] != "bridge" or tokens[1] != expected_subcmd:
        return None

    params: dict = {}
    for token in tokens[2:]:
        if not token.startswith("--"):
            continue
        kv = token[2:]
        if "=" in kv:
            k, v = kv.split("=", 1)
            if k == "handle":
                continue
            params[k] = v
        else:
            params[kv] = True
    return params


def parse_send_command(original_cmd: str) -> dict | None:
    """"bridge send --channel=X --body=Y [--needs-reply] [--in-reply-to=N]" をパース。"""
    return _parse_subcommand(original_cmd, "send")


def parse_recv_command(original_cmd: str) -> dict | None:
    """"bridge recv --channel=X" をパース。"""
    return _parse_subcommand(original_cmd, "recv")


def parse_create_command(original_cmd: str) -> dict | None:
    """"bridge create" をパース。引数なしで空 dict を返す。"""
    return _parse_subcommand(original_cmd, "create")


def parse_history_command(original_cmd: str) -> dict | None:
    """"bridge history --channel=X [--since=N] [--limit=N]" をパース。"""
    return _parse_subcommand(original_cmd, "history")


def parse_presence_command(original_cmd: str) -> dict | None:
    """"bridge presence --channel=X" をパース。"""
    return _parse_subcommand(original_cmd, "presence")


def _parse_bool(value) -> bool:
    """文字列 "true"/"false" を bool に変換する。

    bridge send では --needs-reply=true/false の文字列が渡される場合がある。
    Python の bool("false") は True になるため、明示的な変換が必要（D#2307）。
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)


def mode_recv(handle: str, channel_code: str, server: str, curl_fn=None) -> int:
    """受信モード: SSE 購読を stdout に中継する。

    curl_fn は (url: str) -> int 形式。None の場合は subprocess.run(curl) を使う。
    戻り値は終了コード。
    """
    qs = urllib.parse.urlencode({"channel": channel_code, "handle": handle})
    url = f"{server}/stream?{qs}"

    if curl_fn is not None:
        return curl_fn(url)

    # 実際の curl で SSE 購読（-N: バッファなし、-s: サイレント）
    result = subprocess.run(
        ["curl", "-N", "-s", url],
        stdin=subprocess.DEVNULL,
    )
    return result.returncode


def _run_curl_and_propagate(args: list[str]) -> int:
    """curl を実行し、成功時は stdout を継承、失敗時はエラー情報を stderr に流す（Major 1 対応）。

    curl は ``-sf --fail-with-body`` で呼び出すことを前提とし、HTTP 4xx/5xx で非0終了する。
    --fail-with-body（curl 7.76+）により、4xx でも body を stdout に出して非0終了するため、
    サーバーの {"error": "..."} を stderr 経由で呼び出し元に伝播できる。

    非0終了時は curl の stdout（サーバーレスポンス本体）と stderr（curl 自体のエラー）を
    まとめて stderr に流して呼び出し元（mcp_server.py 経由で Claude）に伝播させる。
    """
    result = subprocess.run(args, capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if result.returncode != 0:
        if result.stdout:
            print(result.stdout, file=sys.stderr)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        return result.returncode
    if result.stdout:
        sys.stdout.write(result.stdout)
        sys.stdout.flush()
    return 0


def mode_send(
    handle: str,
    send_params: dict,
    server: str,
    curl_fn=None,
) -> int:
    """送信モード: POST /send を実行する。

    handle は必ず自身の --handle 引数から（send_params 内の handle は無視済み）。
    curl_fn は (url: str, body: str) -> int 形式。None の場合は subprocess.run(curl) を使う。
    """
    channel_code = send_params.get("channel", "")
    if not channel_code:
        print("エラー: bridge send には --channel=CODE が必要です", file=sys.stderr)
        return 1

    body_text = send_params.get("body", "")
    needs_reply = _parse_bool(send_params.get("needs-reply", False))
    in_reply_to_raw = send_params.get("in-reply-to")
    in_reply_to: int | None = None
    if in_reply_to_raw is not None:
        try:
            in_reply_to = int(in_reply_to_raw)
        except ValueError:
            print(f"エラー: --in-reply-to は整数で指定してください: {in_reply_to_raw}", file=sys.stderr)
            return 1

    payload = json.dumps(
        {
            "channel": channel_code,
            "handle": handle,  # forced command で固定された値（D#2285）
            "body": body_text,
            "needs_reply": needs_reply,
            "in_reply_to": in_reply_to,
        },
        ensure_ascii=False,
    )

    url = f"{server}/send"

    if curl_fn is not None:
        return curl_fn(url, payload)

    # -sf + --fail-with-body で HTTP エラーを非0終了として伝播（Major 1）
    return _run_curl_and_propagate(
        ["curl", "-sf", "--fail-with-body", "-X", "POST", url,
         "-H", "Content-Type: application/json",
         "-d", payload]
    )


def mode_create(server: str, curl_fn=None) -> int:
    """作成モード: POST /create を実行し、レスポンス JSON を stdout に出力する。

    curl_fn は (url: str) -> int 形式。None の場合は subprocess.run(curl) を使う。
    戻り値は終了コード。
    """
    url = f"{server}/create"

    if curl_fn is not None:
        return curl_fn(url)

    return _run_curl_and_propagate(
        ["curl", "-sf", "--fail-with-body", "-X", "POST", url,
         "-H", "Content-Type: application/json",
         "-d", "{}"]
    )


def mode_history(
    history_params: dict,
    server: str,
    curl_fn=None,
) -> int:
    """履歴取得モード: GET /history を実行し、レスポンス JSON を stdout に出力する。

    curl_fn は (url: str) -> int 形式。None の場合は subprocess.run(curl) を使う。
    戻り値は終了コード。
    """
    channel_code = history_params.get("channel", "")
    if not channel_code:
        print("エラー: bridge history には --channel=CODE が必要です", file=sys.stderr)
        return 1

    qs_dict: dict = {"channel": channel_code}
    since_raw = history_params.get("since")
    if since_raw is not None:
        qs_dict["since"] = since_raw
    limit_raw = history_params.get("limit")
    if limit_raw is not None:
        qs_dict["limit"] = limit_raw

    qs = urllib.parse.urlencode(qs_dict)
    url = f"{server}/history?{qs}"

    if curl_fn is not None:
        return curl_fn(url)

    return _run_curl_and_propagate(
        ["curl", "-sf", "--fail-with-body", url]
    )


def mode_presence(
    presence_params: dict,
    server: str,
    curl_fn=None,
) -> int:
    """接続中handle一覧モード: GET /presence を実行し、レスポンス JSON を stdout に出力する。

    curl_fn は (url: str) -> int 形式。None の場合は subprocess.run(curl) を使う。
    戻り値は終了コード。
    """
    channel_code = presence_params.get("channel", "")
    if not channel_code:
        print("エラー: bridge presence には --channel=CODE が必要です", file=sys.stderr)
        return 1

    qs = urllib.parse.urlencode({"channel": channel_code})
    url = f"{server}/presence?{qs}"

    if curl_fn is not None:
        return curl_fn(url)

    return _run_curl_and_propagate(
        ["curl", "-sf", "--fail-with-body", url]
    )


def run(
    argv: list[str],
    original_command: str | None = None,
    curl_recv_fn=None,
    curl_send_fn=None,
    curl_create_fn=None,
    curl_history_fn=None,
    curl_presence_fn=None,
) -> int:
    """メインロジック。テスト時は curl_*_fn でモック可能。

    original_command: None の場合は環境変数 $SSH_ORIGINAL_COMMAND を使う。
    戻り値は終了コード。
    """
    args = parse_args(argv)

    handle = args.get("handle")
    if not handle:
        print("エラー: --handle=<user> が指定されていません", file=sys.stderr)
        return 1

    server = args.get("server", SERVER_DEFAULT)

    if original_command is None:
        original_command = os.environ.get("SSH_ORIGINAL_COMMAND", "")

    recv_params = parse_recv_command(original_command)
    if recv_params is not None:
        channel_code = recv_params.get("channel", "")
        if not channel_code:
            print("エラー: bridge recv には --channel=CODE が必要です", file=sys.stderr)
            return 1
        return mode_recv(handle, channel_code, server, curl_fn=curl_recv_fn)

    send_params = parse_send_command(original_command)
    if send_params is not None:
        return mode_send(handle, send_params, server, curl_fn=curl_send_fn)

    create_params = parse_create_command(original_command)
    if create_params is not None:
        return mode_create(server, curl_fn=curl_create_fn)

    history_params = parse_history_command(original_command)
    if history_params is not None:
        return mode_history(history_params, server, curl_fn=curl_history_fn)

    presence_params = parse_presence_command(original_command)
    if presence_params is not None:
        return mode_presence(presence_params, server, curl_fn=curl_presence_fn)

    print(
        f"エラー: 不明なコマンド: {original_command!r}"
        "（'bridge recv', 'bridge send', 'bridge create', 'bridge history', 'bridge presence' が必要）",
        file=sys.stderr,
    )
    return 1


def main() -> None:
    """エントリーポイント。"""
    sys.exit(run(sys.argv[1:]))


if __name__ == "__main__":
    main()
