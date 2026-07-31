# relay

別々の人間が使う Claude Code 同士に「認識合わせ」を代行させるための、履歴を持つ軽量メッセージング中継サービス。A2A (Agent2Agent) 準拠の HTTP サーバーと Python SDK からなる。

> relay is a lightweight, history-keeping message relay that lets Claude Code agents used by different people stay aligned with each other — an A2A-compliant HTTP server plus a Python SDK.

- **場 (stream) モデル** — 作成者の identity でスコープ化された「場」に member を招待し、メッセージを投函する。配達は member への push（SSE）+ cumulative ack
- **labels 購読** — subscription を作って labels にマッチする publish を受け取る、pub/sub レーン
- **at-least-once 配達** — 永続化するのは outbox のみ。retry / DLQ / 冪等 key を備え、relay 再起動は re-subscribe で自己修復する
- **A2A 準拠** — Bearer token 認証、AgentCard 公開（ES256 署名は任意）、招待ベースの federation peer 登録

## クイックスタート

Python 3.11+ と [uv](https://docs.astral.sh/uv/) が必要。

```bash
git clone https://github.com/isizono/relay.git && cd relay
uv sync

# token → identity の対応表を渡してサーバーを起動(migration は起動時に自動適用)
export RELAY_AUTH_TOKENS='{"tok-a": "agent-a", "tok-b": "agent-b"}'
uv run uvicorn relay.app:app --host 127.0.0.1 --port 8000
```

別ターミナルで 2 つのエージェント役を演じてみる。

```bash
# agent-a が場を作る
curl -sX POST http://127.0.0.1:8000/streams \
  -H 'Authorization: Bearer tok-a' -H 'Content-Type: application/json' \
  -d '{"name": "standup"}'

# agent-b を member に加える（stream_id は上のレスポンスに含まれる値を使う）
curl -sX PUT http://127.0.0.1:8000/streams/<stream_id>/members \
  -H 'Authorization: Bearer tok-a' -H 'Content-Type: application/json' \
  -d '{"identity": "agent-b", "access": "read_write"}'

# agent-b が SSE で受信待ち（identity 単位の多重化接続）
curl -N http://127.0.0.1:8000/events -H 'Authorization: Bearer tok-b'

# agent-a が投函 → agent-b の SSE に届く（body は UTF-8 文字列。JSON を送りたい場合は文字列化してから渡す）
curl -sX POST http://127.0.0.1:8000/streams/<stream_id>/messages \
  -H 'Authorization: Bearer tok-a' -H 'Content-Type: application/json' \
  -d '{"body": "hello from agent-a"}'
```

## 構成

```
relay/          # A2A 準拠 HTTP サーバー本体（Starlette）
migrations/     # SQLite スキーマ（yoyo-migrations、サーバー起動時に自動適用）
relay_sdk/      # Python SDK
  client/       #   subscriber 側（subscribe / SSE 受信 / ack / 再同期）
  http/         #   Bearer 認証付き HTTP / SSE の共通層
  outbox/       #   publisher 側（ローカル outbox への publish + 配送 dispatcher）
docs/
  design/           # プロトコル・SDK の仕様書（一次情報源）
  ARCHITECTURE.md   # 実装のモジュール構成と設計判断の記録
tests/          # サーバー・SDK のテスト（integration/ に E2E roundtrip）
```

## 設定

主な環境変数（すべて省略可、既定値は `relay/config.py` 参照）:

| 環境変数 | 役割 |
|---|---|
| `RELAY_DB_PATH` | SQLite ファイルパス（outbox / dlq / publish_log / agent_cards のみ永続化） |
| `RELAY_AUTH_TOKENS` | Bearer token → identity の対応表（JSON object） |
| `RELAY_SERVER_LOG_PATH` | 構造化ログ + サーバーログ sink（JSON Lines、TTL 90 日） |
| `RELAY_JWS_PRIVATE_KEY_PEM` / `RELAY_JWS_KID` / `RELAY_JWS_JKU` | AgentCard の ES256 署名（未設定なら署名なし最小セット）。federation マシン鍵も兼ねる |
| `RELAY_JWE_PRIVATE_KEY_PEM` | federation envelope body の暗号化鍵（ECDH-ES + A256GCM）。署名鍵とは別鍵。未設定なら envelope は互換のため平文で送る |
| `RELAY_BASE_URL` | 自 relay の公開 base URL（federation 招待 URL 生成・redeem 応答の locator に使う） |
| `RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS` | 既定 `false`。`true` で federation の outbound dial 先に localhost / private IP を許可（同一ホスト検証・開発用） |

## 機能とエンドポイント

| 機能 | endpoint | 状態 |
|---|---|---|
| 場 (stream) CRUD + membership | `POST/GET/DELETE /streams`, `PUT/DELETE/GET /streams/{id}/members` | 実装済み |
| 場 publish + cumulative ack | `POST /streams/{id}/messages`, `POST /streams/{id}/ack` | 実装済み |
| subscription（subscribe / lease / unsubscribe / ack / publish） | `POST /subscriptions` 他 | 実装済み |
| SSE 多重化購読（outbox dispatcher / retry / DLQ） | `GET /events` | 実装済み |
| 運用スナップショット | `GET /status` | 実装済み |
| Prometheus 互換 metrics | `GET /metrics` | 実装済み |
| AgentCard 公開 | `GET /.well-known/agent-card.json` | 実装済み |
| Python SDK（クライアント側） | `relay_sdk/` パッケージ | 実装済み |
| federation peer レジストリ（招待ベース鍵ピン留め） | `POST /federation/peers/redeem`、`python -m relay.invite peer new/redeem/list/revoke` | peer 登録まで実装済み（relay 間のメッセージ配達は未実装） |
| federation envelope 暗号化鍵の追加登録（招待をやり直さない再 pin） | `POST /federation/peers/enc-key`、`python -m relay.invite peer enc-key` | 実装済み |

wire レベルの仕様は [docs/design/relay-v2-wire-api.md](docs/design/relay-v2-wire-api.md)、identity / 認可モデルは [docs/design/relay-v2-identity-authz.md](docs/design/relay-v2-identity-authz.md) を参照。

## Python SDK

`relay_sdk` は publisher 側と subscriber 側で入口が分かれている。

- **publisher 側** (`relay_sdk.outbox`) — アプリはローカル SQLite の outbox に publish で書くだけ。配送は別プロセスの dispatcher（`python -m relay_sdk.outbox`）が担い、retry / backoff を吸収する
- **subscriber 側** (`relay_sdk.client`) — subscribe で subscription を作り、SSE 受信・ack・再接続時の再同期を SDK が面倒を見る

API の詳細仕様は [docs/design/relay-v2-sdk.md](docs/design/relay-v2-sdk.md)、動く実例は `tests/integration/test_sdk_roundtrip.py` を参照。

## ドキュメント

- [docs/design/relay-concept.md](docs/design/relay-concept.md) — なぜ relay が必要か、というコンセプト
- [docs/design/relay-glossary.md](docs/design/relay-glossary.md) — 用語集（場 / subscription / outbox など）
- [docs/design/relay-v2-wire-api.md](docs/design/relay-v2-wire-api.md) — HTTP wire API 仕様
- [docs/design/relay-v2-identity-authz.md](docs/design/relay-v2-identity-authz.md) — identity・認証・認可
- [docs/design/relay-v2-sdk.md](docs/design/relay-v2-sdk.md) — Python SDK 仕様
- [docs/design/relay-sequences.md](docs/design/relay-sequences.md) — 主要シーケンス図
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — 実装のモジュール構成と設計判断の記録。開発中の判断メモを含む歴史的文書のため通読は不要で、まず `docs/design/` の仕様書から読むこと

## 開発

```bash
uv run pytest
```

CI（GitHub Actions）は Python 3.11 / 3.12 / 3.13 でテストを実行し、`uv.lock` の整合検証、パッケージビルド + クリーンな venv へのインストール検証も行う。

## ライセンス

[MIT](LICENSE)
