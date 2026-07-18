# relay

別々の人間が使う Claude Code 同士に「認識合わせ」を代行させるための、履歴を持つ軽量メッセージング中継サービス。

> relay v2 への移行が完了し、SSH forced command 認証ベースの旧実装（クライアント側一式）は
> 撤去済み。実運用の唯一の経路は `relay/` パッケージ配下の relay v2（A2A(Agent2Agent) 準拠、
> Bearer token 認証の Starlette 製 HTTP サーバー）である。撤去の経緯は
> [旧実装の撤去経緯](#旧実装の撤去経緯) を参照。
> 仕様は `docs/design/` 配下（`relay-concept.md` / `relay-glossary.md` /
> `relay-v2-wire-api.md` / `relay-v2-identity-authz.md` / `relay-v2-sdk.md` /
> `relay-sequences.md`）、実装のモジュール構成・設計判断は `docs/ARCHITECTURE.md` を参照。

## relay v2（新実装）

`relay/` パッケージが Starlette 製の HTTP サーバーを実装する。認証は SSH ではなく
`Authorization: Bearer <token>`（`RELAY_AUTH_TOKENS` 環境変数で token → identity の対応表を
指定）。詳しい endpoint 仕様は `docs/design/relay-v2-wire-api.md`、identity / authZ は
`docs/design/relay-v2-identity-authz.md` を参照。

### 起動

```bash
export RELAY_AUTH_TOKENS='{"tok-abc": "agent-a"}'
uv run uvicorn relay.app:app --host 127.0.0.1 --port 8000
```

主な環境変数（すべて省略可、既定値は `relay/config.py` 参照）:

| 環境変数 | 役割 |
|---|---|
| `RELAY_DB_PATH` | SQLite ファイルパス（outbox / dlq / publish_log / agent_cards のみ永続化） |
| `RELAY_AUTH_TOKENS` | Bearer token → identity の対応表（JSON object） |
| `RELAY_SERVER_LOG_PATH` | 構造化ログ + サーバーログ sink（JSON Lines、TTL 90 日） |
| `RELAY_JWS_PRIVATE_KEY_PEM` / `RELAY_JWS_KID` / `RELAY_JWS_JKU` | AgentCard の ES256 署名（未設定なら署名なし最小セット）。federation マシン鍵も兼ねる |
| `RELAY_BASE_URL` | 自 relay の公開 base URL（federation 招待 URL 生成・redeem 応答の locator に使う） |
| `RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS` | 既定 `false`。`true` で federation の outbound dial 先に localhost / private IP を許可（同一ホスト検証・開発用） |

### 実装状況

| 機能 | endpoint | 状態 |
|---|---|---|
| 場 (stream) CRUD + membership | `POST/GET/DELETE /streams`, `PUT/DELETE/GET /streams/{id}/members` | 実装済み |
| 場 publish + cumulative ack | `POST /streams/{id}/messages`, `POST /streams/{id}/ack` | 実装済み |
| subscription（subscribe / lease / unsubscribe / ack / publish） | `POST /subscriptions` 他 | 実装済み |
| SSE 多重化購読（outbox dispatcher / retry / DLQ） | `GET /events` | 実装済み |
| 運用スナップショット | `GET /status` | 実装済み |
| Prometheus 互換 metrics | `GET /metrics` | 実装済み |
| AgentCard 公開 | `GET /.well-known/agent-card.json` | 実装済み |
| Python SDK（クライアント側） | — | 未実装（`docs/design/relay-v2-sdk.md` は仕様のみ） |
| federation peer レジストリ（招待ベース鍵ピン留め） | `POST /federation/peers/redeem`、`python -m relay.invite peer new/redeem/list/revoke` | 実装済み（relay 間メッセージ配達自体は未実装） |

テストは旧 `server.py` 分と合わせて [開発](#開発) のコマンド 1 本で実行できる（`tests/` 配下に
両方のテストファイルが同居している）。

## 旧実装の撤去経緯

relay v2 サーバー側（HTTP wire API）の実装完了後も、以下の 2 点により旧実装
（`server.py` に対する SSH forced command 認証ベースのクライアント一式:
`bridge_connect.py` / `gen_authorized_keys.py` / `mcp_server.py` / `recv_monitor.sh`）を
削除できていなかったが、理由 1 の解消を受けて撤去した。

1. **クライアント側の実装が relay v2 に存在しなかった**。`mcp_server.py`（Claude Code 向け
   MCP ツール）と `recv_monitor.sh`（`Monitor` 向け受信スクリプト）は、どちらも旧 `server.py` の
   HTTP API（`/send` `/stream` `/create` `/history` `/presence`）に対する薄いクライアントで、
   relay v2 の wire API（Bearer token authN、`/streams` `/subscriptions` `/publish`
   `/events`）を話せなかった。relay v2 向けの MCP ツール（`relay_post` / `relay_publish` /
   `relay_subscribe` / `relay_receive`）は別リポジトリ（cc-memory）側に実装済みのため、
   旧クライアント一式は不要になった。
2. **`GetHistory` / `GetPresence` に相当する機能は relay v2 に存在しない**。これは実装漏れではなく
   意図的な仕様変更である。
   - `GetHistory`（`bridge history` / `GET /history`）: `relay-v2-wire-api.md` の設計判断で
     「場 history の永続蓄積」自体が廃止されており、取りこぼしは「未 ack outbox の再送」+
     「retain 切れ時は publisher へ直接 pull」で回収する設計に変わっている。
   - `GetPresence`（`bridge presence` / `GET /presence`）: `GET /streams/{id}/members` は
     構造的な membership（読み書き権限の付与状態）を返すもので、「今 SSE 接続中かどうか」という
     liveness 情報とは別概念。`GET /status` の `active_sse_connections` は総数のみで、
     identity 別の一覧は持たない。relay v2 に liveness の個別一覧取得手段は用意されていない。

**フォローアップ**: 旧実装の本体である `server.py` は、上記クライアント一式の撤去に伴い
呼び出し元を失い孤児化した状態にある（本撤去では削除していない）。`server.py` 自体の削除可否は
別途判断が必要。

## 開発

relay v2（`relay/` パッケージ）と旧 `server.py`（孤児化済み、詳細は
[旧実装の撤去経緯](#旧実装の撤去経緯) を参照）のテストは同じ `tests/` 配下に同居しており、
1 コマンドでまとめて実行できる。

```bash
uv run pytest -v
```

`server.py` 本体は標準ライブラリ（`http.server` + `sqlite3`）のみで依存ゼロを維持する
（relay v2 は authlib / Starlette 等の外部依存を使う。詳細は `pyproject.toml` を参照）。
