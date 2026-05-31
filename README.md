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

## 開発

```bash
uv run pytest -v
```

`server.py` 本体は標準ライブラリ（`http.server` + `sqlite3`）のみで依存ゼロを維持する。
