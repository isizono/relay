#!/usr/bin/env python3
"""relay MCPサーバー。

Claude Code 向けの MCP (Model Context Protocol) サーバー。
bridge-connect 越しに SSH forced command 経由で relay サーバーと通信する（D#2272）。

RELAY_SSH_HOST 環境変数で接続先ホストを指定する（デフォルト: "relay"）。
実運用では ~/.ssh/config に `Host relay` エイリアスを定義する想定。

ControlMaster オプション (-o ControlMaster=auto -o ControlPath=... -o ControlPersist=600) を
全 SSH 呼び出しに付与し、接続多重化でコストを軽減する（D#2272）。

ツール一覧:
  CreateChannel()                                → {"channel_code": str}
  SendMessage(channel_code, body, needs_reply, in_reply_to) → {"msg_id": int}
  GetHistory(channel_code, since, limit)         → {"messages": [...]}
  GetPresence(channel_code)                      → {"handles": [...]}
"""
import json
import os
import shlex
import subprocess
import tempfile

from mcp.server.fastmcp import FastMCP

HOST = os.environ.get("RELAY_SSH_HOST", "relay")

_ctl_path = os.path.join(tempfile.gettempdir(), "relay_ssh_ctl_%h_%p_%r")
control_master_opts = [
    "-o", "ControlMaster=auto",
    "-o", f"ControlPath={_ctl_path}",
    "-o", "ControlPersist=600",
]

mcp = FastMCP("relay")


def _bridge(subcmd: str, **kwargs) -> str:
    """SSH 越しに bridge <subcmd> を実行し、stdout を返す。

    kwargs のキーはアンダースコアからハイフンに変換して --flag=value 形式にする（D#2308）。
    値に空白・改行が含まれていても shlex.join でシェル安全な1文字列に変換する（D#2308）。
    非0終了の場合は RuntimeError を送出する。
    """
    remote_args = ["bridge", subcmd]
    for k, v in kwargs.items():
        if v is None:
            continue
        flag = k.replace("_", "-")
        remote_args.append(f"--{flag}={v}")
    remote_cmd = shlex.join(remote_args)
    args = ["ssh", *control_master_opts, HOST, remote_cmd]

    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"bridge {subcmd} が非0終了しました (exit={result.returncode}): {result.stderr.strip()}"
        )
    return result.stdout


@mcp.tool()
def CreateChannel() -> dict:
    """新しい channel を作成し、接続コードを返す。

    戻り値: {"channel_code": str}
    """
    output = _bridge("create")
    data = json.loads(output)
    return {"channel_code": data["channel_code"]}


@mcp.tool()
def SendMessage(
    channel_code: str,
    body: str,
    needs_reply: bool = False,
    in_reply_to: int | None = None,
) -> dict:
    """メッセージを送信する。handle は SSH forced command で固定されるため不要（D#2285）。

    戻り値: {"msg_id": int}
    """
    kwargs: dict = {
        "channel": channel_code,
        "body": body,
        "needs_reply": str(needs_reply).lower(),
    }
    if in_reply_to is not None:
        kwargs["in_reply_to"] = in_reply_to
    output = _bridge("send", **kwargs)
    data = json.loads(output)
    return {"msg_id": data["msg_id"]}


@mcp.tool()
def GetHistory(
    channel_code: str,
    since: int | None = None,
    limit: int | None = None,
) -> dict:
    """メッセージ履歴を取得する。

    since 指定時は msg_id > since のメッセージのみ返す。
    戻り値: {"messages": [...]}
    """
    kwargs: dict = {"channel": channel_code}
    if since is not None:
        kwargs["since"] = since
    if limit is not None:
        kwargs["limit"] = limit
    output = _bridge("history", **kwargs)
    data = json.loads(output)
    return {"messages": data["messages"]}


@mcp.tool()
def GetPresence(channel_code: str) -> dict:
    """現在接続中の handle 一覧を取得する。

    戻り値: {"handles": [...]}
    """
    output = _bridge("presence", channel=channel_code)
    data = json.loads(output)
    return {"handles": data["handles"]}


if __name__ == "__main__":
    mcp.run()
