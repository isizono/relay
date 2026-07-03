# relay v2 実装アーキテクチャ

> 位置づけ: relay v2 実装（`relay/` パッケージ）のモジュール構成と、設計文書
> （`docs/design/` 配下）と物理 DB schema decision の間で見つかった不整合点の解消方針を
> 記録する。実装着手時の一次情報源は `docs/design/relay-v2-wire-api.md` /
> `relay-v2-identity-authz.md` / `relay-glossary.md` とする。

## パッケージ構成

```
relay/
├── __init__.py
├── config.py         # 実行時設定（環境変数 → Settings dataclass）
├── db.py             # SQLite 接続 + migration 適用（yoyo-migrations）
├── identity.py        # AgentCard 構築 / Bearer token authN / JCS(MUST) / JWS(MAY)
├── streams.py         # stream (場) API（後続タスク実装）
├── subscriptions.py   # subscription API（後続タスク実装）
├── delivery.py         # outbox polling dispatcher / SSE（後続タスク実装）
├── observability.py    # /status /metrics /サーバーログ（後続タスク実装）
└── app.py              # Starlette アプリ組み立て（各モジュールの routes を集約）

migrations/
└── 0001-initial-schema.sql   # yoyo-migrations 形式。以後の schema 変更は追加 migration で行う

tests/
├── test_db.py
├── test_identity.py
├── test_app.py
└── test_config.py
```

各機能モジュール（`streams.py` / `subscriptions.py` / `delivery.py` /
`observability.py`）は `routes: list[starlette.routing.Route]` を公開する。`app.py` は
それらをインポートして集約するだけで、endpoint 実装そのものは持たない。個別 endpoint を
追加する担当は該当モジュールに handler を書き、`routes` に追加すればよい（`app.py` の
変更は不要）。

各 handler は `relay.identity.require_authn` デコレータで authN を通したうえで、
`request.state.identity`（`Identity(id=...)`）を見て structural authZ
（membership 照合 / ownership 照合）を追加で行う。semantic authZ は relay の外
（cc-memory MCP handler）の責務であり、relay 側には実装しない
（`relay-v2-identity-authz.md` §2.5）。

## DB schema — decision 3133 との差分

DB 物理 schema は cc-memory decision「relay v2 DB schema 物理構造を確定」（id 3133,
2026-06-30）で一度確定しているが、その後 2026-06-27 の R1 決着
（`relay-v2-wire-api.md` §0 の前提表、根拠 decision「場 history 完全廃止 / relay
デバッグ用サーバーログ...」id 3082）および 2026-07-03 の凍結整合パッチ（wire-api.md /
identity-authz.md の最新版）で、decision 3133 の一部と矛盾する確定事項が生じていた。
本実装は「一次情報源は `docs/design/` の最新版」というタスク方針に従い、以下の点で
decision 3133 の記述から逸脱している。矛盾を残したまま実装すると後続の担当が古い方の
記述を正としてしまう恐れがあるため、判断根拠を明記する。

### 1. streams / memberships / subscriptions は SQLite に table を作らない

decision 3133 は 7+1 table 構成（`streams` / `memberships` / `subscriptions` を含む）を
定義しているが、`relay-v2-wire-api.md` §0 の前提表は次のように明記している。

> substrate は disk（SQLite）で守るのは outbox のみ。presence / subscription registry /
> lease / stream membership は in-memory（liveness クラス）。relay 再起動は
> re-subscribe + heartbeat で自己修復

この R1 原則の根拠は decision 3082（2026-06-27、decision 3133 より前）で、「relay の
永続性は outbox 1本に畳む」ことを明示的に決定している。`relay-v2-identity-authz.md` §4
（relay 再起動と identity）も「relay 再起動で subscription registry は消失する」
「subscriber は新たに `POST /subscriptions` を呼ぶ」ことを前提に、`404`/`410` の
使い分けなど wire レベルの挙動まで確定させている。

decision 3133 が streams / memberships / subscriptions を SQLite table として記述したのは
R1 原則確定後の詳細設計セッション（`log: 詳細設計補強 軸 1 (DB schema) 議論クローズ`,
id 3168）だが、そのログにも in-memory 化との整合を再検討した形跡はなく、R1 原則との
矛盾が解消されないまま残っていたと判断した。`relay-v2-wire-api.md` /
`relay-v2-identity-authz.md` は 2026-07-03 の凍結整合パッチで更新された最新版であり、
本タスクの指示でも「必ず読むべき一次情報源」と明記されているため、本実装では
**disk 永続化するのは outbox / dlq / publish_log / agent_cards の 4 table のみ**とし、
streams / memberships / subscriptions は in-memory 実装とする。

後続タスク（stream API 実装、subscription API 実装）は、これらを Python の
in-memory データ構造（dict + lock 等）で持つこと。SQLite の `:memory:` 接続を使う場合も、
disk 永続化 DB（outbox 等）とは別の接続にすること（同一 SQLite ファイル内で
「揮発する table」と「永続する table」を混在させることはできないため）。

### 2. outbox は subscription レーン / stream レーンの両方の delivery target を扱う

decision 3133 は outbox の PK を `(subscription_id, publish_id)` のみで定義しているが、
これは 2026-07-03 の凍結整合パッチで新設された stream 用 ack endpoint
（`POST /streams/{stream_id}/ack`）に対応していない。`relay-v2-wire-api.md` §5.6 /
§5.7 は次のように規定する。

> 場レーンの delivery target は「場 × 呼び出し元 identity」に解決される。member は
> 自分宛エントリしか ack できず、他 member 宛エントリは構造上指定できない

つまり stream レーンの outbox エントリは `stream_id` だけでなく `member_identity`
（配達先 identity）まで含めて一意に定まる必要がある。本実装では `outbox` /
`dlq` に `target_type`（`'subscription' | 'stream'`）と `subscription_id` /
`stream_id` / `member_identity`（該当しないレーンでは NULL）を持たせ、

- subscription レーン: `UNIQUE (subscription_id, publish_id) WHERE target_type='subscription'`
- stream レーン: `UNIQUE (stream_id, member_identity, publish_id) WHERE target_type='stream'`

の 2 本の部分インデックスで一意性を分離した（`migrations/0001-initial-schema.sql`）。

### 3. `streams.default_ttl` は仕様上必要（decision 3133 には無いカラム）

`relay-v2-wire-api.md` §3.1 は `POST /streams` の body に `default_ttl?` を持ち、
§6.4 は「場 outbox の retain default = 場の `default_ttl`」と規定している。decision
3133 の streams カラムリストにはこのカラムが無い。streams 自体を in-memory にした
（§1 の変更）ため SQLite schema には影響しないが、後続の stream API 実装で
in-memory streams レコードに `default_ttl` を持たせること。

### 4. カラム名 `agent_did` / `publisher_did` → `identity` 系に変更

decision 3133 は `memberships.agent_did` / `publish_log.publisher_did` という
カラム名を使っているが、`relay-v2-identity-authz.md` §1.4 は DID を明示的にスコープ外
としている（「relay v2 は DID を扱わない」）。`did` を含むカラム名は DID 概念の使用を
暗示してしまうため、`relay-glossary.md` の語彙（identity）に合わせて
`publish_log.publisher_identity` のように改名した。stream membership 自体は
in-memory になったため `memberships.agent_did` 相当のカラムは無くなっている
（§1 参照、後続 stream API 実装側で `identity` という属性名を使うこと）。

### 5. `subscriptions.stream_ids` カラムは採用しない

decision 3133 の `subscriptions` テーブルには `stream_ids JSON` カラムがあるが、
`relay-v2-wire-api.md` の `POST /subscriptions` body（§5.1）にはこのフィールドが無く、
`relay-glossary.md` の membership / subscription エントリも「stream の membership と
subscription は独立（stream のメンバーは自動 subscribe されない。逆も同様）」と
明記している。stream_ids は R1 確定前の（stream 単位で subscribe する）旧設計の
名残と判断し、採用しない。

## Bearer token 検証（authN）の実装方針

`relay-v2-identity-authz.md` §7 未決事項に「Bearer token 発行主体（relay 自前 vs
外部 IdP）は運用判断、本書では relay 設定で選択」とある。本実装では最小セット
（`HTTPAuthSecurityScheme{scheme:"bearer"}`）の実装として、環境変数
`RELAY_AUTH_TOKENS`（JSON: `{"<token>": "<identity>"}`）による静的 token → identity
対応表を採用した（`relay/config.py` の `Settings.auth_tokens`,
`relay/identity.py` の `authenticate_request`）。外部 IdP 連携や JWT 検証への移行が
必要になった場合は `authenticate_request` の実装を差し替える（呼び出し側の
`require_authn` デコレータのインターフェースは変えずに済む設計にしてある）。

## JWS / JCS の実装状態

`relay-v2-identity-authz.md` §1.2.4 の「relay v2 は初期実装で最小セットを満たし、JWS
署名は MAY として将来段階で導入する」という方針に従い、`GET
/.well-known/agent-card.json` は既定では署名なし AgentCard（最小セット）を返す。
`relay/identity.py` に JCS 正規化（`canonicalize_agent_card`, rfc-8785 MUST）と
ES256 JWS 署名/検証（`sign_agent_card` / `verify_agent_card_signature`, MAY）は
実装済みで、`Settings.jws_private_key_pem` / `jws_kid` / `jws_jku` を設定すると
フル準拠セット（署名付き AgentCard）に自動的に切り替わる。

JWS 実装には `authlib` ではなく、同じ Authlib チームが開発する後継ライブラリ
`joserfc` を使った（`authlib.jose` は import 時に non-fatal な deprecation warning
が出て `joserfc` への移行を促す状態だったため）。`pyproject.toml` には `authlib` と
`joserfc` の両方を依存として残している。

AgentCard の `signatures` フィールドの正確な wire format（JWS Compact / Flattened
JSON Serialization のどちらに寄せるか等）は A2A 1.0 spec 本体の記述を本実装では
参照できておらず、`docs/design/relay-v2-identity-authz.md` にも例示が無い。本実装は
`signatures: [{protected, signature}]`（payload は AgentCard 本体から再計算できる
detached 形式）という妥当と考えられる形式を暫定採用した。外部 agent の AgentCard
検証で A2A 準拠クライアントとの相互運用が必要になった時点で、A2A 1.0 spec 本体との
突合を行うこと。

## 未実装 / 後続タスクへの申し送り

- `streams.py` / `subscriptions.py` / `delivery.py` / `observability.py` は
  `routes: list[Route] = []` のみの空モジュール。各担当が endpoint を実装する。
- structural authZ（membership 照合、ownership 照合）のヘルパー関数は未実装。
  `relay.identity.Identity` を受け取って判定する関数を各モジュール側に置くか、
  共通化するかは後続の判断に委ねる。
- `agent_cards` テーブル（外部 agent の AgentCard キャッシュ）の読み書きロジックは
  未実装。schema のみ用意した。
- `server_log`（`Settings.server_log_path`、既定 `relay-server.jsonl`）の
  append-only 書き込みロジックは未実装（`observability.py` 担当）。
- polling dispatcher（outbox polling → SSE push → retry → DLQ 化）は未実装
  （`delivery.py` 担当）。
