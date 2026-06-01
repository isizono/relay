#!/usr/bin/env python3
"""bridge-connect — SSH forced command ラッパー。

authorized_keys で各鍵行に `command="bridge-connect --handle=<user>"` として
forced command が設定される。powwow_code は forced command には焼かず、
$SSH_ORIGINAL_COMMAND から取得する（D#2302）。

分岐ロジック（D#2272, D#2302）:
  "bridge recv --powwow=X"   → 受信モード（SSE購読を stdout へ中継）
  "bridge send --powwow=X --body=Y ..." → 送信モード（POST /send を実行）
  それ以外（空含む）          → エラー

handle は必ず自身の --handle 引数からのみ取得し、$SSH_ORIGINAL_COMMAND 内の
handle 指定は無視する（handle 詐称防止、D#2285）。

使い方（forced command として authorized_keys から自動呼び出し）:
    command="/path/to/bridge-connect --handle=alice",no-pty,... <key>

引数:
    --handle=<user>   handle（authorized_keys で固定される）[必須]
    --server=<url>    サーバーベースURL（デフォルト: http://127.0.0.1:8765）

クライアント側は ssh host "bridge recv --powwow=X" / "bridge send --powwow=X --body=Y" で呼び出す。
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
    """"bridge send --powwow=X --body=Y [--needs-reply] [--in-reply-to=N]" をパース。"""
    return _parse_subcommand(original_cmd, "send")


def parse_recv_command(original_cmd: str) -> dict | None:
    """"bridge recv --powwow=X" をパース。"""
    return _parse_subcommand(original_cmd, "recv")


def mode_recv(handle: str, powwow_code: str, server: str, curl_fn=None) -> int:
    """受信モード: SSE 購読を stdout に中継する。

    curl_fn は (url: str) -> int 形式。None の場合は subprocess.run(curl) を使う。
    戻り値は終了コード。
    """
    qs = urllib.parse.urlencode({"powwow": powwow_code, "handle": handle})
    url = f"{server}/stream?{qs}"

    if curl_fn is not None:
        return curl_fn(url)

    # 実際の curl で SSE 購読（-N: バッファなし、-s: サイレント）
    result = subprocess.run(
        ["curl", "-N", "-s", url],
        stdin=subprocess.DEVNULL,
    )
    return result.returncode


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
    powwow_code = send_params.get("powwow", "")
    if not powwow_code:
        print("エラー: bridge send には --powwow=CODE が必要です", file=sys.stderr)
        return 1

    body_text = send_params.get("body", "")
    needs_reply = bool(send_params.get("needs-reply", False))
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
            "powwow": powwow_code,
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

    # 実際の curl で POST
    result = subprocess.run(
        ["curl", "-s", "-X", "POST", url,
         "-H", "Content-Type: application/json",
         "-d", payload],
        stdin=subprocess.DEVNULL,
    )
    return result.returncode


def run(
    argv: list[str],
    original_command: str | None = None,
    curl_recv_fn=None,
    curl_send_fn=None,
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
        powwow_code = recv_params.get("powwow", "")
        if not powwow_code:
            print("エラー: bridge recv には --powwow=CODE が必要です", file=sys.stderr)
            return 1
        return mode_recv(handle, powwow_code, server, curl_fn=curl_recv_fn)

    send_params = parse_send_command(original_command)
    if send_params is not None:
        return mode_send(handle, send_params, server, curl_fn=curl_send_fn)

    print(f"エラー: 不明なコマンド: {original_command!r}（'bridge recv' または 'bridge send' が必要）", file=sys.stderr)
    return 1


def main() -> None:
    """エントリーポイント。"""
    sys.exit(run(sys.argv[1:]))


if __name__ == "__main__":
    main()
