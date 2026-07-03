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
├── errors.py           # error envelope（{code, message, details}）+ relay 固有 error_code 定数
├── idempotency.py       # idempotency_key の 15 分 dedup（stream / subscription レーン共通）
├── identity.py        # AgentCard 構築 / Bearer token authN / JCS(MUST) / JWS(MAY)
├── streams.py         # stream (場) API + membership + structural authZ（実装済み）
├── subscriptions.py   # subscription API（実装済み: subscribe / lease / unsubscribe / ack / publish）
├── delivery.py         # outbox polling dispatcher / SSE / retry / DLQ（実装済み）
├── observability.py    # 構造化ログ + サーバーログ sink（実装済み）。/status /metrics は未実装
└── app.py              # Starlette アプリ組み立て（各モジュールの routes を集約 + dispatcher 起動）

migrations/
├── 0001-initial-schema.sql        # yoyo-migrations 形式。以後の schema 変更は追加 migration で行う
└── 0002-outbox-expires-at.sql     # outbox.expires_at 列追加（DLQ sweep の retain 判定用）

tests/
├── test_db.py
├── test_identity.py
├── test_app.py
├── test_config.py
├── test_streams.py
├── test_subscriptions.py
├── test_delivery.py
└── test_observability.py
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

- `subscriptions.py`（subscribe / lease renew / unsubscribe / ack / publish）、
  `delivery.py`（outbox dispatcher / SSE / retry / DLQ）、`observability.py` の
  構造化ログ + サーバーログ sink は Delivery タスクで実装済み（詳細は後続の節を参照）。
  `streams.py` は Resources タスク（stream CRUD + membership + structural authZ）で
  実装済み。
- `agent_cards` テーブル（外部 agent の AgentCard キャッシュ）の読み書きロジックは
  未実装。schema のみ用意した。
- `GET /status` / `GET /metrics`（observability.md §7.1, §7.2）は未実装
  （後続タスクの担当分）。`observability.py` は構造化ログ + サーバーログ sink のみ
  実装済み。
- `PUT /streams/{stream_id}/members` で write member が 0 人になる操作へのガードは無い
  （Resources タスクからの申し送り、未解消のまま）。

## `streams.py` 実装（stream CRUD + membership + structural authZ）

`POST /streams` / `GET /streams/{stream_id}` / `DELETE /streams/{stream_id}` /
`POST /streams/{stream_id}/messages` / `PUT`・`DELETE`・`GET /streams/{stream_id}/members` /
`POST /streams/{stream_id}/ack` を実装した（`relay-v2-wire-api.md` §3, §5.6,
`relay-v2-identity-authz.md` §2.2）。

### stream / membership の in-memory registry

`StreamRegistry`（`relay/streams.py`）が `dict[str, StreamRecord]` + `threading.Lock` で
保持する。app インスタンスごとに `request.app.state.stream_registry` へ遅延生成され、
同一 app を共有する他モジュール（`delivery.py` の `GET /events` が「認証 identity の
member 場を自動含む」判定をする際など）からも同じ属性名で参照できる。

### stream レーン publish は `streams.py` 側で outbox に直接 INSERT する

`POST /streams/{stream_id}/messages` の `202 Accepted` は「outbox 永続化完了」が条件
（wire-api.md §3.2, §6.1 transactional outbox）であるため、read 権限を持つ member 宛の
outbox エントリ作成は `delivery.py`（polling dispatcher）を待たず `streams.py` が
`relay.db` 経由で直接行う。`delivery.py` の責務は outbox からの読み出し（polling →
SSE push → retry → DLQ 化）に限定される。

### 追加した `relay/errors.py`

A2A 1.0 spec Section 3.3.2 相当の共通 error envelope（`{code, message, details}`）と、
relay 固有 error_code 定数を集約する共通モジュールを新設した。`streams.py` だけでなく
`subscriptions.py` / `delivery.py` 側の endpoint 実装でも同じ envelope 形式を使うことを
想定している。

### status code の判断（wire-api.md の記述を優先）

`POST /streams/{stream_id}/messages` の write 権限不足は、wire-api.md §3.2 / §8 が
明示的に `403 Forbidden` と記載しているためその通りに実装した（`DELETE
/streams/{stream_id}` や membership 変更の write 権限不足も同様に `403`）。一方
`POST /streams/{stream_id}/ack` は wire-api.md §5.6 が「場が不在、または呼び出し元が
read 権限を持つ member でない」を同一の `404 Not Found` と明記しているため、そちらは
存在と権限不足を区別しない実装にした。

### 既知のギャップ（後続タスクへの申し送り）

1. **`GET /streams`（一覧）は wire-api.md に存在しない**。本タスクの依頼文には
   「`GET /streams`」という記載があったが、`relay-v2-wire-api.md` §2 / §3 の
   endpoint 一覧には `GET /streams/{stream_id}`（単一 stream のメタ取得）しか
   定義されていない。一覧 endpoint を新設するかどうかは仕様上未確定のため、本タスクでは
   `GET /streams/{stream_id}` のみを実装し、一覧 endpoint は実装していない。必要であれば
   別途仕様を確定してから追加すべきである。
2. **（解消済み、Delivery タスクで対応）** `idempotency_key` の 15 分 dedup
   （wire-api.md §6.3）は `relay/idempotency.py` の共通ヘルパーで実装した。
   stream レーン（本モジュール）と subscription レーンの `POST /publish` の両方が
   `app.state.idempotency_store` を共有する。詳細は後続の Delivery 実装セクションを
   参照。
3. **（解消済み、Delivery タスクで対応）** `ttl` / `default_ttl` は
   `migrations/0002-outbox-expires-at.sql` で追加した `outbox.expires_at` 列に
   enqueue 時点で計算した期限を書き込み、`relay/delivery.py` の DLQ sweep
   （`_sweep_retain_exceeded`）がこれを見て retain 超過を検出するようになった。
4. **error_code の一部は cc-memory 側の既存 decision（`error_code = A2A 8 種 + relay
   固有最小集合`）の列挙にない**。`StreamAlreadyExistsError`（`POST /streams` の
   stream_id 重複、409）と `InvalidRequestError`（汎用 400 バリデーション）を追加した。
   前者は既存 decision の列挙に conflict 用の code が無いための追加、後者は
   `LabelValidationError` が labels 専用の名前であり stream_id / body / access 等の
   汎用バリデーションに転用するのは意味的に不適切と判断したための追加である。
   error_code 一覧を「上限 7 種程度」で運用する方針との整合は、確定 decision 側の
   見直しが必要か検討すべきである。
5. **同一 stream の write member が 0 人になる操作（自分自身の write 権限を削除する
   membership 変更等）へのガードは無い**。wire-api.md / identity-authz.md にこの
   edge case の規定がないため、意図的に制約を追加していない。

## `relay/delivery.py` + `relay/subscriptions.py` 実装（outbox dispatcher / SSE / retry / DLQ / cumulative ack / server log）

`GET /events`（SSE 多重化購読）、outbox polling dispatcher、push retry、DLQ sweep、
`POST /subscriptions/{id}/ack`、および subscription レーンの残り endpoint
（`POST /subscriptions` / `PUT /subscriptions/{id}/lease` / `DELETE /subscriptions/{id}` /
`POST /publish`）を実装した。

### スコープ判断: subscription レーンの CRUD も含めて実装した

Foundation が書いた `subscriptions.py` の元 docstring は「subscribe / lease renew /
unsubscribe / ack / publish はすべて後続タスクの担当分」としていたが、本タスクの依頼文が
明示的に要求する項目（`POST /subscriptions/{id}/ack`、`GET /events` の ownership /
lease 検証、dispatcher の DLQ permanent error 判定）はいずれも `SubscriptionRegistry`
（subscriber identity・lease・labels を保持する in-memory registry）の存在を前提とする。
この registry を作らずに ack や `GET /events` だけを実装することはできない。加えて
`POST /subscriptions`（subscribe）が無いと registry に何も登録できず、統合テストで
実際の HTTP 経路を検証できない。そのため本タスクでは `subscriptions.py` を
`StreamRegistry`（Resources 実装）と対称な設計で全面的に実装した
（`SubscriptionRegistry` + 5 endpoint 全部）。`POST /publish` の subset マッチング fan-out
も同様の理由で実装している。

### `SubscriptionRegistry`

`relay/subscriptions.py` の `SubscriptionRegistry` は `dict[str, SubscriptionRecord]` +
`threading.Lock` で保持する（`StreamRegistry` と同じパターン）。`app.state.subscription_registry`
に app インスタンスごとに遅延生成され、`relay/delivery.py`（`GET /events` の ownership 検証、
dispatcher の DLQ permanent error 判定）からも `get_registry_from_state(app_state)` で
同じインスタンスを参照する。`is_lease_expired()` は「不存在」を `False` として返す
（「不存在」と「lease 切れ」の区別は呼び出し側が `is_owner()` / `get()` と組み合わせて
行う設計。wire-api.md §5.7 の 404 / 410 使い分けに対応）。

### outbox schema 拡張: `outbox.expires_at`（`migrations/0002-outbox-expires-at.sql`）

Resources 実装時点の outbox schema（`migrations/0001-initial-schema.sql`）には、
enqueue 時点で retain 期限を保持する列が無かった。DLQ sweep が retain 超過
（wire-api.md §6.6）を判定するにはこの情報が必須なため、`outbox.expires_at`（TEXT、
ISO8601）を追加した。stream レーン（`streams.py` の `post_stream_message`）・
subscription レーン（`subscriptions.py` の `publish`）双方の INSERT 時に
`enqueued_at + retain_seconds` を計算して書き込む。`dlq` table 側には追加していない
（dead 化した時点で「なぜ dead になったか」は `error_code` に記録され、`expires_at` の
情報は不要になるため）。

### push retry: relay 自身の SSE push とは別に、SDK 側 dispatcher の Full Jitter backoff が存在する点に注意

cc-memory の decision 記録には retry backoff に関する 2 系統の決定が存在し、混同しやすい。

- **relay 自身の push retry**（本実装の対象）: `GET /events` で push した SSE イベントが
  接続先の `asyncio.Queue`（`Connection.queue`、slow consumer 検出用のバッファ）に
  入らない場合、初回 100ms・係数 2・最大 5 回・累積約 3.1 秒の指数バックオフで retry する
  （wire-api.md §6.4、`relay-glossary.md`「polling dispatcher」）。Full Jitter ではない
  単純な指数バックオフ。5 回すべて失敗したら接続を強制切断する（wire-api.md §6.4 / SSE
  slow consumer 強制切断の決定）。`relay/delivery.py` の `PUSH_RETRY_DELAYS_SECONDS` /
  `_push_with_retry` がこれに対応する。
- **SDK 側（cc-memory 等）のローカル outbox → relay への POST dispatcher backoff**
  （本実装のスコープ外）: Full Jitter、base=1 秒、cap=300 秒。これは cc-memory 側の
  ローカル outbox から relay へ `POST /publish` する際の retry であり、relay 本体
  （このリポジトリ）には実装しない。`relay-v2-sdk.md` 側の実装対象。

「dispatcher」という語が両方の文脈で使われるため、本実装のコード内コメントでは
「push retry」（relay 内、本実装）と表記し、SDK 側の同名の仕組みとは明示的に区別した。

### SSE keepalive の実装: sse-starlette 組み込み `ping` は使わず自前生成

`sse_starlette.sse.EventSourceResponse` の `ping` パラメータは「0 で無効化」と docstring に
書かれているが、実際の実装は `while self.active: await anyio.sleep(self._ping_interval)` の
ループを `ping_interval=0` のまま無条件で起動するため、`anyio.sleep(0)` によるビジーループ
になる（sse-starlette 側の既知の挙動、`ping=0` 起因で CPU 100% 近くまで張り付く事象を
実装中に実機で確認した）。本実装では `ping` に実用上到達しない大きな値
（`_DISABLE_BUILTIN_PING_INTERVAL_SECONDS = 10_000_000`）を渡して組み込み ping を事実上
無効化し、代わりに `event_stream()` ジェネレータ自身が
`asyncio.wait_for(queue.get(), timeout=settings.sse_keepalive_seconds)` のタイムアウトで
`: keepalive` コメント行（30 秒間隔、wire-api.md §5.5）を生成する。`send_timeout` は
transport 層の write 詰まり（TCP レベルの zombie 接続）を検出する保険として残している。

### slow consumer 検出の実装: `asyncio.Queue` の backpressure をシグナルとして使う

relay と subscriber の間の実際の TCP write 詰まりを ASGI 層で直接検出するのは
（sse-starlette / Starlette の抽象化越しでは）現実的ではないため、本実装は
「dispatcher → SSE 送信 generator」間に有界 `asyncio.Queue`
（`CONNECTION_QUEUE_MAXSIZE = 256`）を挟み、dispatcher がここへの `put_nowait()` に
失敗する（= generator 側が十分な速さで消費できていない）ことを slow consumer の
シグナルとして扱う。retry を使い切ってもキューに空きが出ない場合、接続を強制切断する
（`_force_disconnect`）。`send_timeout`（既定 5 秒）は、真に TCP write がハングする
ケースへの保険として併用している。

### `GET /events` の実装: HTTP 層とジェネレータ / 検証ロジックを分離

`get_events`（HTTP handler）は薄く保ち、以下を独立した関数に切り出した。

- `validate_subscription_ids(sub_registry, identity_id, subscription_ids)`:
  ownership（404）→ lease 状態（410）の検証順序（wire-api.md §5.7）。
- `event_stream(conn, manager, settings, app_state)`: `conn.queue` を読んで SSE event に
  変換する非同期ジェネレータ。

この分離は主にテスト容易性のためである。**実機で確認した事実として、この環境の
Starlette `TestClient`（httpx ラップ）も `httpx.ASGITransport` も、内部で
`await app(scope, receive, send)` の完了を待ってからレスポンスを返す実装になっており、
終端しない SSE stream を「ストリーミングで読む」ことができず、そのままではテストが
無限に hang する**（`TestClient.stream()` を使っても同様。両者のソースを直接確認して
特定した）。そのため:

- ロジックの大半（ownership/lease 検証、keepalive のタイムアウト生成、force-disconnect
  センチネルでの停止）は `validate_subscription_ids` / `event_stream` を直接呼ぶ形の単体
  テストで検証する（`tests/test_delivery.py`）。
- dispatcher が実際に `Connection.queue` へ push しカーソルを進める挙動は
  `dispatch_once(app)` を `asyncio.run()` でラップした非同期テストから直接呼び出して検証する
  （`asyncio.Queue` / `asyncio.Event` はループに紐づくため、同一イベントループ内で完結
  させる必要がある。pytest-asyncio は導入せず、各テストを同期関数から `asyncio.run(...)`
  する方式にした）。
- `GET /events` の実際の wire（SSE ヘッダ・event framing・実際のプッシュ〜受信）だけは、
  uvicorn を実 TCP port で起動した上で `httpx.Client`（非 ASGI transport、実ソケット）
  から読む統合テストで検証する（`tests/test_delivery.py` の `LiveServer` fixture）。

### dispatcher 単一プロセス enforcement（file lock）

`relay/app.py` の lifespan で `delivery.try_acquire_dispatcher_lock(settings.dispatcher_lock_path)`
を呼び、non-blocking `fcntl.flock` で排他制御する（wire-api.md §6.2）。lock を取得できた
プロセスだけが `run_dispatcher_loop` を起動する。取得できなければ（他プロセスが既に
dispatcher を担っている）そのプロセスは HTTP handler だけ動かし、dispatcher は起動しない。
テストでは `Settings.dispatcher_lock_path` を `tmp_path` 配下に向けることで、テスト間の
lock 競合を避けている（デフォルト値のまま複数 app を並行起動すると同一 lock file を取り合う
点に注意。既存モジュールの `db_path` と同じ注意が必要）。

### DLQ sweep の実装

`_sweep_retain_exceeded`（`outbox.expires_at` 経過）と `_sweep_permanent_errors`
（subscription lane で `subscription_id` が registry に存在しない、または lease 切れ）の
2 経路で `outbox` → `dlq` へ行を移す（`_move_to_dlq`、INSERT + DELETE を同一 transaction
内で実行）。dead 化のたびに `observability.record_event(..., "outbox_dead", ...)` で
構造化ログを 1 件出す（`publish_id` で trace 可能、wire-api.md §7.3 の要求）。
`_sweep_dlq_physical_delete` が `dead_at` から `Settings.dlq_retention_days`（既定 7 日）
経過した行を物理 DELETE する。stream レーンの permanent error（member 削除等）は
明示的には検出していない（wire-api.md / identity-authz.md にこの edge case の規定が
無いため、意図的に対象外とした。stream は close されても消滅しないため「target の消滅」
に相当する事象が subscription レーンほど明確でない）。

### idempotency dedup（`relay/idempotency.py`）

stream レーン・subscription レーン共通の in-memory dedup store
（`app.state.idempotency_store`）。`idempotency_key` 指定時は
`(lane, publisher_identity, idempotency_key)` で 15 分 window の dedup、省略時は
wire-api.md §6.3 の擬似キー補完式（`publisher_identity` / scope（stream_id か ref の
正規化 JSON）/ labels 正規化 / body・title の hash / 受信秒精度 ts）で擬似キーを計算する。

### rate limiting（`POST /publish`、`RateLimiter`）

`subscriptions.py` に token bucket 方式の `RateLimiter`（`app.state.publish_rate_limiter`、
publisher identity ごと）を実装し、`POST /publish` に適用した（wire-api.md §5.4、既定
100 req/sec、超過時 `429` + `Retry-After` ヘッダ）。`POST /streams/{id}/messages`
（場投函）への適用は wire-api.md §10 が「残置（実装段階で詰める）」と明記しているため、
本タスクでは見送った（Resources 実装のまま）。

### structured log / server log（`relay/observability.py`）

Foundation の元 docstring は「構造化ログ」（`publish_id` で trace する短期ログ）と
「サーバーログ」（payload 込み・TTL 90 日の長期デバッグ sink）を 2 つの独立した sink として
想定していたが、本実装ではこれを単一の JSON Lines append-only sink
（`Settings.server_log_path`、既定 `relay-server.jsonl`）に統合した（`event` フィールドで
種別を判別できるため、実用上 2 sink に分離する必然性が薄いと判断した）。
`record_event(app_state, event_type, **fields)` を publish 受領（stream / subscription
両レーン）・ack 受領（両レーン）・subscribe・unsubscribe・SSE 接続 / 切断・DLQ 移動・
dispatcher 内部エラーで呼んでいる。**購読者向け読み取り endpoint は一切持たない**
（wire-api.md §7.3 が明示的に禁止する「`since=N` 型 pull の裏口化」を避けるため）。
TTL（既定 90 日）を過ぎた行は `purge_expired_server_log` で間引く。書き込みのたびに
毎回スキャンすると I/O コストが無視できなくなるため、`_maybe_gc` で最短実行間隔
（既定 1 時間に 1 回）を設けている。

### 既知のギャップ / 後続タスクへの申し送り

1. **ack 未着タイムアウト（60 秒で SSE 接続を強制 close）は未実装**。cc-memory 側の
   関連 decision には存在するが、本タスクの依頼文が明示する 9 項目には含まれておらず、
   実装コストとのバランスから見送った。「push はできているが subscriber 側の受信ループが
   スタックしている」ケースの検知に相当し、slow consumer 強制切断（本実装済み、queue
   backpressure ベース）とは異なる障害モードをカバーする。
2. **stream レーンの permanent error 検出（DLQ sweep）は未実装**。上記 DLQ sweep の節
   参照。
3. **TCP keepalive（`TCP_KEEPIDLE` / `TCP_KEEPINTVL` / `TCP_KEEPCNT`）は未設定**。
   cc-memory 側の関連 decision は具体値（60 秒 / 10 秒 / 3 回）を確定しているが、これは
   ASGI アプリケーションコードの層ではなく uvicorn の起動オプション
   （`--limit-max-requests` 等とは別の socket オプション）で設定するものであり、
   本実装（`relay/` パッケージ内のコード）のスコープ外と判断した。本番運用時の uvicorn
   起動コマンド側で設定する必要がある。
4. **`GET /status` / `GET /metrics` は未実装**（後続タスクの担当分、observability.md
   §7.1, §7.2）。`relay/observability.py` は構造化ログ + サーバーログ sink のみ実装した。
5. **1 SSE 接続で複数 target を多重化する際の ack バッチ境界・部分 ack の最適化は
   未着手**。現状の実装は正しく動作する（各 target は個別の delivery target として
   cumulative ack される）が、性能最適化（大量 target 保持時の dispatcher 1 cycle の
   処理時間）は検証していない。
6. **subscription レーンの `POST /publish` に対する subset マッチング性能
   （wire-api.md §10「10,000 subscriptions × 100 labels で p99 200ms」）は未検証**。
   現状の実装は `SubscriptionRegistry.matching()` で全 subscription を線形走査する素朴な
   実装であり、性能 SLO 検証・最適化（inverted index 等）は T7（observability /
   性能）相当のタスクに委ねる。

## Subscriptions タスク: `SubscriptionRegistry` の無制限メモリ増加を解消

Delivery タスクの実装レビュー中に見つけた点。`SubscriptionRegistry` は unsubscribe
（`DELETE /subscriptions/{id}`）でのみレコードを除去しており、lease が切れて renew
されないまま放置された subscription（subscriber がクラッシュして re-subscribe も
unsubscribe もしないケース）は `_subs` dict に無期限に残り続ける実装になっていた。
wire-api.md §5.7 は「所有者本人の lease 切れ subscription への操作は registry 残存時に
限り `410`」と規定しており、この 410 ヒントを提供する目的で registry 残存自体は必要だが、
無期限に残す必然性はない（`404` / `410` はどちらも subscriber 側で「re-subscribe せよ」
の同一シグナルとして扱われるため、いつまで `410` を返せるかは機能的な互換性に影響しない）。

対応として `SubscriptionRegistry.evict_expired(older_than_seconds)` を追加し、
`relay/delivery.py` の `dispatch_once`（既存の DLQ sweep サイクル）から毎 polling cycle
呼び出すようにした。lease 切れから `Settings.subscription_registry_retention_seconds`
（既定 1 時間、`RELAY_SUBSCRIPTION_REGISTRY_RETENTION_SECONDS` で override 可）を過ぎた
subscription を registry から物理的に除去する。除去後は非所有者と同じ `404` になる。
