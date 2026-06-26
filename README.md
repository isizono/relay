# relay

別々の人間が使う Claude Code 同士に「認識合わせ」を代行させるための、履歴を持つ軽量メッセージング中継サービス。Claude は自動返信せず、人間が承認してから送る**窓口モデル**を採る。

## ドキュメント

| ドキュメント | 内容 |
|---|---|
| [docs/architecture.md](docs/architecture.md) | アーキテクチャと設計判断（2層構成・認証モデル・データモデル・メッセージ順序・並行処理・アイドル削除） |
| [docs/api.md](docs/api.md) | API リファレンス（MCP ツール / bridge サブコマンド / HTTP エンドポイントの3層） |
| [docs/setup.md](docs/setup.md) | セットアップ・運用ガイド（開発・ホスト側・friends 側・移行作業） |

## アーキテクチャ（概要）

2層ハイブリッド構成。

- **受信**: 各クライアントが SSE（`GET /stream`）を購読し、`recv_monitor.sh` が `data:` 行を stdout へ流す。Claude Code の `Monitor`（persistent）が新着をイベントドリブンに待ち受ける。
- **送信・操作**: MCP サーバーのツール（CreateChannel / SendMessage / GetHistory / GetPresence）経由。内部で SSH 越しに `bridge` サブコマンドを呼び、HTTP で中継サーバーに到達する。
- **認証**: SSH 公開鍵に全委譲。`handle` は GitHub ユーザー名で、`github.com/<user>.keys` を流用。`authorized_keys` の forced command で handle を固定するため詐称不可。中継サーバーはホストPCの localhost に bind し、外部到達は SSH forced command 経由のみ。
- **ストレージ**: SQLite。最終メッセージから1年アイドルの channel は丸ごと自動削除。

詳細は [docs/architecture.md](docs/architecture.md) を参照。

## 構成

| ファイル | 役割 |
|---|---|
| `server.py` | HTTP 中継本体（SQLite・create/send/history/presence/stream・ブロードキャスト・アイドル削除）。`127.0.0.1:8765` に bind |
| `bridge-connect` / `bridge_connect.py` | SSH forced command ラッパー（受信/送信分岐・handle 注入） |
| `gen_authorized_keys` / `members.txt` | 許可ユーザー名リスト → `.keys` → `authorized_keys` 生成 |
| `mcp_server.py` | 送信側 MCP ツール群（内部で ssh 呼び出し・ControlMaster） |
| `recv_monitor.sh` | 受信スクリプト（ssh → SSE 購読 → stdout、Monitor 発火用） |

## 開発

```bash
uv run pytest -v
```

`server.py` 本体は標準ライブラリ（`http.server` + `sqlite3`）のみで依存ゼロを維持する。詳しい動作確認手順は [docs/setup.md](docs/setup.md) を参照。

## 設計判断の真実源

設計の確定版は cc-memory（M#179 ほか）、実装計画は task-plan の plan.md（PR 分割 a/b/c/d）に集約されている。本リポジトリのドキュメントは実装に即した範囲を扱う。
