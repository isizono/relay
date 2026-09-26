# relay

別々の人間が使う Claude Code 同士に「認識合わせ」を代行させるための、at-least-once 配達を保証する軽量メッセージング中継サービス。A2A (Agent2Agent) 準拠の HTTP サーバーと Python SDK からなる。

> relay is a lightweight, at-least-once message relay that lets Claude Code agents used by different people stay aligned with each other — an A2A-compliant HTTP server plus a Python SDK.

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
uv run python -m relay.serve --host 127.0.0.1 --port 8770
```

`python -m relay.serve` は TCP keepalive（`SO_KEEPALIVE` + idle/interval/probe 回数）を
設定した socket で起動する。`uv run uvicorn relay.app:app --host 127.0.0.1 --port 8770`
で直接起動することもできるが、その場合 keepalive は OS 既定のままになる。運用上の注意点は
[docs/ops/running.md](docs/ops/running.md) を参照。

別ターミナルで 2 つのエージェント役を演じてみる。

```bash
# agent-a が場を作る
curl -sX POST http://127.0.0.1:8770/streams \
  -H 'Authorization: Bearer tok-a' -H 'Content-Type: application/json' \
  -d '{"name": "standup"}'

# agent-b を member に加える（stream_id は上のレスポンスに含まれる値を使う）
curl -sX PUT http://127.0.0.1:8770/streams/<stream_id>/members \
  -H 'Authorization: Bearer tok-a' -H 'Content-Type: application/json' \
  -d '{"identity": "agent-b", "access": "read_write"}'

# agent-b が SSE で受信待ち（identity 単位の多重化接続）
curl -N http://127.0.0.1:8770/events -H 'Authorization: Bearer tok-b'

# agent-a が投函 → agent-b の SSE に届く（body は UTF-8 文字列。JSON を送りたい場合は文字列化してから渡す）
curl -sX POST http://127.0.0.1:8770/streams/<stream_id>/messages \
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
tests/          # サーバー・SDK のテスト（integration/ に E2E roundtrip、contract/ に wire 仕様の契約テスト）
```

## 設定

主な環境変数（すべて省略可、既定値は `relay/config.py` 参照）:

| 環境変数 | 役割 |
|---|---|
| `RELAY_DB_PATH` | SQLite ファイルパス（outbox / dlq / publish_log / agent_cards のみ永続化） |
| `RELAY_AUTH_TOKENS` | Bearer token → identity の対応表（JSON object） |
| `RELAY_SERVER_LOG_PATH` | 構造化ログ + サーバーログ sink（JSON Lines、TTL 90 日） |
| `RELAY_JWS_PRIVATE_KEY_PEM` / `RELAY_JWS_KID` / `RELAY_JWS_JKU` | AgentCard の ES256 署名（未設定なら署名なし最小セット）。federation マシン鍵も兼ねる |
| `RELAY_JWE_PRIVATE_KEY_PEM` | relay 間区間の envelope body 暗号化鍵（ECDH-ES + A256GCM）。署名鍵とは別鍵。未設定、または宛先 peer に暗号化鍵が未登録なら envelope は互換のため平文で送る（`RELAY_FEDERATION_REQUIRE_ENCRYPTION` / peer 単位フラグで必須化できる） |
| `RELAY_FEDERATION_REQUIRE_ENCRYPTION` | 既定 `false`。`true` で全 peer 宛の配達に envelope 暗号化を必須化し、鍵が双方揃わない配達は平文で送らず DLQ に回す。peer 単位でも `python -m relay.invite peer require-encryption <handle> on\|off` で切替できる（いずれかが真なら必須） |
| `RELAY_BASE_URL` | 自 relay の公開 base URL（招待 URL 生成・federation redeem 応答の locator に使う）。`python -m relay.invite` 系コマンドの `--base-url` 省略時にも参照する |
| `RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS` | 既定 `false`。`true` で federation の outbound dial 先に localhost / private IP を許可（同一ホスト検証・開発用） |
| `RELAY_TCP_KEEPIDLE` / `RELAY_TCP_KEEPINTVL` / `RELAY_TCP_KEEPCNT` | `python -m relay.serve` の TCP keepalive 設定（既定 60 秒 / 10 秒 / 3 回）。`uvicorn relay.app:app` 直接起動には効かない |

federation envelope の暗号化（`RELAY_JWE_PRIVATE_KEY_PEM`）が守るのは**送信 relay → 受信 relay の区間（relay 間区間）のみ**である。受信側の relay は復号したうえで outbox / publish_log に平文のまま保存し、そこから先（`GET /events` の SSE 配達を含む）は既存の Bearer token 認証済み経路に委ねる。セッション（agent）に届くまでのエンドツーエンドの暗号化ではない。

## 機能とエンドポイント

| 機能 | endpoint | 状態 |
|---|---|---|
| 場 (stream) CRUD + membership | `POST /streams`, `GET /streams`（read 権限を持つ場の一覧）, `GET/DELETE /streams/{id}`, `PUT/DELETE/GET /streams/{id}/members` | 実装済み |
| 場 publish + cumulative ack | `POST /streams/{id}/messages`, `POST /streams/{id}/ack` | 実装済み |
| subscription（subscribe / lease / unsubscribe / ack / publish） | `POST /subscriptions` 他 | 実装済み |
| SSE 多重化購読（outbox dispatcher / retry / DLQ） | `GET /events` | 実装済み |
| 運用スナップショット | `GET /status` | 実装済み |
| Prometheus 互換 metrics | `GET /metrics` | 実装済み |
| AgentCard 公開 | `GET /.well-known/agent-card.json` | 実装済み |
| Python SDK（クライアント側） | `relay_sdk/` パッケージ | 実装済み |
| federation peer レジストリ（招待ベース鍵ピン留め） + relay 間メッセージ配達 | `POST /federation/peers/redeem`、`POST /federation/streams/{id}/messages`（受信側 inbound endpoint）、`python -m relay.invite peer new/redeem/list/revoke` | 実装済み（送信側は既存 outbox dispatcher の egress ステップ、受信側は上記 inbound endpoint） |
| federation envelope 暗号化鍵の追加登録（招待をやり直さない再 pin） | `POST /federation/peers/enc-key`、`python -m relay.invite peer enc-key` | 実装済み |
| peer 単位の envelope 暗号化必須化 | `python -m relay.invite peer require-encryption <handle> on\|off` | 実装済み |

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
- [docs/ops/running.md](docs/ops/running.md) — 運用手順（`--host` の選び方、TCP keepalive、`RELAY_BASE_URL` の役割）
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — 実装のモジュール構成と設計判断の記録。開発中の判断メモを含む歴史的文書のため通読は不要で、まず `docs/design/` の仕様書から読むこと

## 開発

```bash
uv run pytest
```

CI（GitHub Actions）は Python 3.11 / 3.12 / 3.13 でテストを実行し、`uv.lock` の整合検証、パッケージビルド + クリーンな venv へのインストール検証も行う。

## ライセンス

[MIT](LICENSE)
