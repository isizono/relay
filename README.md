# powwow

別々の人間が使う Claude Code 同士に「認識合わせ」を代行させるための、履歴を持つ軽量メッセージング中継サービス。Claude は自動返信せず、人間が承認してから送る**窓口モデル**を採る。

## アーキテクチャ（概要）

2層ハイブリッド構成。

- **受信**: 各クライアントが SSE（`GET /stream`）を購読し、`Monitor`（persistent）で新着をイベントドリブンに待ち受ける。
- **送信・操作**: MCPサーバーのツール（CreatePowwow / SendMessage / GetHistory / GetPresence）経由。送信は HTTP POST。
- **認証**: SSH 公開鍵に全委譲。`handle` は GitHub ユーザー名で、`github.com/<user>.keys` を流用。`authorized_keys` の forced command で handle を固定するため詐称不可。中継サーバーはホストPCの localhost に bind し、外部到達は SSH forced command 経由のみ。
- **ストレージ**: SQLite。最終メッセージから1年アイドルの powwow は丸ごと自動削除。

設計の確定版は cc-memory M#179、実装計画は task-plan の plan.md（PR分割 a/b/c/d）に集約されている。

## 構成（実装予定）

| ファイル | 役割 | PR |
|---|---|---|
| `server.py` | HTTP中継本体（SQLite・create/send/history/presence/stream・ブロードキャスト・アイドル削除） | a |
| `bridge-connect` | SSH forced command ラッパー（受信/送信分岐・handle注入） | b |
| `gen_authorized_keys` / `members.txt` | 許可ユーザー名リスト → `.keys` → `authorized_keys` 生成 | b |
| `mcp_server.py` | 送信側MCPツール群（内部で ssh 呼び出し・ControlMaster） | c |
| `recv_monitor.sh` | 受信スクリプト（ssh → SSE購読 → stdout、Monitor発火用） | d |

現状の `server.py` は PoC（SSE中継のみ・インメモリ）の移植。PR-a で SQLite 永続化・各エンドポイント・ブロードキャスト・アイドル削除ジョブを足して本実装にする。

## 受信（recv_monitor.sh）

`recv_monitor.sh` は SSH forced command（bridge-connect）経由で SSE を購読し、`data:` 行を stdout に流す。
Claude Code の `Monitor`（persistent）でこのスクリプトの stdout を監視することで、新着メッセージをイベントドリブンに受け取れる。

```bash
# Monitor(persistent) で受信待ち起動
./recv_monitor.sh --powwow=abc123 --host=powwow
```

接続断時は自動再接続する（1 秒インターバル）。取りこぼしは MCP ツール `GetHistory(since=N)` で回収する設計のため、スクリプト側は単純再接続のみで十分（D#2257）。

### オプション

| オプション | 説明 | デフォルト |
|---|---|---|
| `--powwow=CODE` | powwow コード（必須） | — |
| `--host=HOST` | SSH ホスト | `powwow` |
| `--no-reconnect` | 接続断時に再接続しない（デバッグ用） | false |
| `--filter-only` | stdin を読んで `data:` 行のみ流す（テスト用） | false |

### フィルタのテスト

```bash
# data: 行のみ通ることを確認
printf ': connected\n\ndata: {"body":"hi"}\n\n' | ./recv_monitor.sh --filter-only
# → data: {"body":"hi"}

# シェルテスト実行
bash tests/test_recv_monitor.sh
```

## bridge サブコマンド一覧

bridge-connect が受け付けるサブコマンド（SSH forced command 経由）:

| サブコマンド | 用途 | 主な引数 |
|---|---|---|
| `bridge recv --powwow=X` | SSE 購読（受信モード） | `--powwow` |
| `bridge send --powwow=X --body=Y` | メッセージ送信 | `--powwow`, `--body`, `--needs-reply`, `--in-reply-to` |
| `bridge create` | powwow 作成 | なし |
| `bridge history --powwow=X` | メッセージ履歴取得 | `--powwow`, `--since`, `--limit` |
| `bridge presence --powwow=X` | 接続中 handle 一覧取得 | `--powwow` |

MCP サーバー（`mcp_server.py`）は内部でこれらのサブコマンドを SSH 越しに呼び出す。

## メッセージ順序の真実源

メッセージ順序の真実源は **`msg_id`（SQLite `INTEGER PRIMARY KEY AUTOINCREMENT` で単調増加）** とする。SSE ブロードキャストでの**到達順は厳密に保証しない**（複数スレッドが同時に `/send` を叩いた場合、`save_message` の commit 順と各購読者 queue への `put` 順が逆転する可能性がある）。

受信側 Claude は `msg_id` で:
- スレッド構造の復元（`in_reply_to` → 親 `msg_id`）
- 重複・欠落の冪等突合（`GetHistory(since=最後に見た msg_id)` で取りこぼし再取得）

を行うため、broadcast 到達順の前後に依存しない設計になっている。並行 send 時の SQLite 同時書き込みは WAL モード + `busy_timeout=5000ms` で吸収する。

## 開発

```bash
uv run pytest -v
```

`server.py` 本体は標準ライブラリ（`http.server` + `sqlite3`）のみで依存ゼロを維持する。
