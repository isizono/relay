#!/usr/bin/env python3
"""gen_authorized_keys — members.txt → authorized_keys 生成スクリプト。

使い方:
    ./gen_authorized_keys [members.txt] [bridge-connect のパス]

引数:
    members.txt        GitHubユーザー名リスト（デフォルト: スクリプト同ディレクトリの members.txt）
    bridge-connect パス  forced command に使うパス（デフォルト: スクリプト同ディレクトリの bridge-connect）

標準出力に authorized_keys の内容を出力する。
各鍵行に `command="bridge-connect --handle=<user>",no-pty,no-port-forwarding,no-X11-forwarding,no-agent-forwarding`
を前置する（D#2264）。

重複ユーザー名は1回に正規化する（M#179 §8）。
"""
import re
import subprocess
import sys
from pathlib import Path


# GitHub ユーザー名: 英数字・ハイフン、先頭/末尾ハイフン禁止、連続ハイフン禁止、1〜39文字
# 参考: https://github.com/join → username 規則
GITHUB_USERNAME_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,38}$")


def parse_members(members_path: Path) -> list[str]:
    """members.txt を読み込み、ユーザー名リストを返す（コメント・空行除去・重複排除・バリデーション）。

    重複ユーザー名は出現順で1件目のみ残す（M#179 §8）。
    GitHub ユーザー名規則に合わない行は sys.exit(1) で停止する
    （authorized_keys 経由のコマンドインジェクション防止、D#2285）。
    """
    if not members_path.exists():
        print(f"エラー: {members_path} が見つかりません", file=sys.stderr)
        sys.exit(1)

    seen: set[str] = set()
    users: list[str] = []
    for lineno, raw_line in enumerate(members_path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if not GITHUB_USERNAME_RE.match(line):
            print(
                f"エラー: {members_path}:{lineno} 不正なGitHubユーザー名 {line!r}（英数字とハイフンのみ、1-39文字）",
                file=sys.stderr,
            )
            sys.exit(1)
        if line in seen:
            continue
        seen.add(line)
        users.append(line)
    return users


def fetch_keys(username: str, fetch_fn=None) -> list[str]:
    """GitHub から <username>.keys を取得し、鍵行リストを返す。

    fetch_fn は (username: str) -> str 形式の関数。
    指定しない場合は curl -s で実際に取得する（テスト時はモック可能）。

    鍵が取得できなかった場合は警告を出して空リストを返す。
    """
    if fetch_fn is None:
        fetch_fn = _curl_fetch_keys

    raw = fetch_fn(username)
    if not raw or not raw.strip():
        print(f"警告: {username} の公開鍵が取得できませんでした（スキップ）", file=sys.stderr)
        return []

    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    return lines


def _curl_fetch_keys(username: str) -> str:
    """curl -sSf https://github.com/<username>.keys で鍵文字列を返す。

    -sSf により、HTTPエラー（404等）やネットワーク失敗で exit code 非ゼロになる。
    取得失敗時は CalledProcessError を上位に伝播させ silent skip を防ぐ。
    """
    result = subprocess.run(
        ["curl", "-sSf", f"https://github.com/{username}.keys"],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    return result.stdout


def build_authorized_keys_lines(
    username: str,
    raw_keys: list[str],
    bridge_connect_path: str,
) -> list[str]:
    """ユーザーの鍵リストから authorized_keys の行群を生成する。

    各鍵行に forced command オプションを前置する（D#2264）。
    フォーマット:
        command="<path> --handle=<user>",no-pty,no-port-forwarding,no-X11-forwarding,no-agent-forwarding <key>
    """
    options = (
        f'command="{bridge_connect_path} --handle={username}"'
        ",no-pty,no-port-forwarding,no-X11-forwarding,no-agent-forwarding"
    )
    return [f"{options} {key}" for key in raw_keys]


def generate_authorized_keys(
    members_path: Path,
    bridge_connect_path: str,
    fetch_fn=None,
) -> str:
    """members.txt から authorized_keys の内容文字列を生成して返す。

    fetch_fn: (username: str) -> str。None の場合は curl を使う。
    """
    users = parse_members(members_path)
    all_lines: list[str] = []

    for username in users:
        raw_keys = fetch_keys(username, fetch_fn=fetch_fn)
        lines = build_authorized_keys_lines(username, raw_keys, bridge_connect_path)
        all_lines.extend(lines)

    return "\n".join(all_lines) + ("\n" if all_lines else "")


def main(argv: list[str] | None = None) -> None:
    """エントリーポイント。"""
    if argv is None:
        argv = sys.argv[1:]

    script_dir = Path(__file__).parent

    members_path = Path(argv[0]) if len(argv) >= 1 else script_dir / "members.txt"
    bridge_connect_path = str(argv[1]) if len(argv) >= 2 else str(script_dir / "bridge-connect")

    content = generate_authorized_keys(
        members_path=members_path,
        bridge_connect_path=bridge_connect_path,
        fetch_fn=None,  # 実際の curl を使う
    )
    print(content, end="")


if __name__ == "__main__":
    main()
