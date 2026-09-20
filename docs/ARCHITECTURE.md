# relay v2 実装アーキテクチャ

> 位置づけ: relay v2 実装（`relay/` パッケージ）のモジュール構成と、設計文書
> （`docs/design/` 配下）と物理 DB schema decision の間で見つかった不整合点の解消方針を
> 記録する。実装着手時の一次情報源は `docs/design/relay-v2-wire-api.md` /
> `relay-v2-identity-authz.md` / `relay-glossary.md` とする。
> 本書は実装時の判断記録を含む歴史的文書であり、モジュール構成・実装状況の現在の正はリポジトリ実体と README を参照のこと。

## パッケージ構成

```
relay/
├── __init__.py
├── config.py         # 実行時設定（環境変数 → Settings dataclass）
├── db.py             # SQLite 接続 + migration 適用（yoyo-migrations）
├── errors.py           # error envelope（{code, message, details}）+ relay 固有 error_code 定数
├── idempotency.py       # idempotency_key の 15 分 dedup（stream / subscription レーン共通）
├── identity.py        # AgentCard 構築 / Bearer token authN / JCS(MUST) / JWS(MAY)
├── agent_cards.py       # 外部 agent の AgentCard 取得 + agent_cards table キャッシュ（実装済み）
├── streams.py         # stream (場) API + membership + structural authZ（実装済み）
├── subscriptions.py   # subscription API（実装済み: subscribe / lease / unsubscribe / ack / publish）
├── delivery.py         # outbox polling dispatcher / SSE / retry / DLQ（実装済み）
├── observability.py    # GET /status・GET /metrics（Prometheus 互換）・構造化ログ + サーバーログ sink（実装済み）
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

## DB schema — 初期設計との差分

DB 物理 schema は一度確定しているが、その後の R1 決着
（`relay-v2-wire-api.md` §0 の前提表、根拠は「場 history 完全廃止 / relay
デバッグ用サーバーログ...」という決定）および凍結整合パッチ（wire-api.md /
identity-authz.md の最新版）で、初期設計の一部と矛盾する確定事項が生じていた。
本実装は「一次情報源は `docs/design/` の最新版」というタスク方針に従い、以下の点で
初期設計の記述から逸脱している。矛盾を残したまま実装すると後続の担当が古い方の
記述を正としてしまう恐れがあるため、判断根拠を明記する。

### 1. streams / memberships / subscriptions は SQLite に table を作らない

初期設計は 7+1 table 構成（`streams` / `memberships` / `subscriptions` を含む）を
定義しているが、`relay-v2-wire-api.md` §0 の前提表は次のように明記している。

> substrate は disk（SQLite）で守るのは outbox のみ。presence / subscription registry /
> lease / stream membership は in-memory（liveness クラス）。relay 再起動は
> re-subscribe + heartbeat で自己修復

この R1 原則は「relay の永続性は outbox 1本に畳む」ことを明示的に定めたものである。
`relay-v2-identity-authz.md` §4（relay 再起動と identity）も「relay 再起動で
subscription registry は消失する」「subscriber は新たに `POST /subscriptions` を呼ぶ」
ことを前提に、`404`/`410` の使い分けなど wire レベルの挙動まで確定させている。

初期設計が streams / memberships / subscriptions を SQLite table として記述したのは
R1 原則確定後の詳細設計セッションだが、そのログにも in-memory 化との整合を再検討した
形跡はなく、R1 原則との矛盾が解消されないまま残っていたと判断した。`relay-v2-wire-api.md`
/ `relay-v2-identity-authz.md` は凍結整合パッチで更新された最新版であり、本タスクの
指示でも「必ず読むべき一次情報源」と明記されているため、本実装では
**disk 永続化するのは outbox / dlq / publish_log / agent_cards の 4 table のみ**とし、
streams / memberships / subscriptions は in-memory 実装とする。

後続タスク（stream API 実装、subscription API 実装）は、これらを Python の
in-memory データ構造（dict + lock 等）で持つこと。SQLite の `:memory:` 接続を使う場合も、
disk 永続化 DB（outbox 等）とは別の接続にすること（同一 SQLite ファイル内で
「揮発する table」と「永続する table」を混在させることはできないため）。

### 2. outbox は subscription レーン / stream レーンの両方の delivery target を扱う

初期設計は outbox の PK を `(subscription_id, publish_id)` のみで定義していたが、
これは凍結整合パッチで新設された stream 用 ack endpoint
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

### 3. `streams.default_ttl` は仕様上必要（初期設計には無いカラム）

`relay-v2-wire-api.md` §3.1 は `POST /streams` の body に `default_ttl?` を持ち、
§6.4 は「場 outbox の retain default = 場の `default_ttl`」と規定している。初期設計の
streams カラムリストにはこのカラムが無い。streams 自体を in-memory にした
（§1 の変更）ため SQLite schema には影響しないが、後続の stream API 実装で
in-memory streams レコードに `default_ttl` を持たせること。

### 4. カラム名 `agent_did` / `publisher_did` → `identity` 系に変更

初期設計は `memberships.agent_did` / `publish_log.publisher_did` という
カラム名を使っていたが、`relay-v2-identity-authz.md` §1.4 は DID を明示的にスコープ外
としている（「relay v2 は DID を扱わない」）。`did` を含むカラム名は DID 概念の使用を
暗示してしまうため、`relay-glossary.md` の語彙（identity）に合わせて
`publish_log.publisher_identity` のように改名した。stream membership 自体は
in-memory になったため `memberships.agent_did` 相当のカラムは無くなっている
（§1 参照、後続 stream API 実装側で `identity` という属性名を使うこと）。

### 5. `subscriptions.stream_ids` カラムは採用しない

初期設計の `subscriptions` テーブルには `stream_ids JSON` カラムがあるが、
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
- （解消済み）`agent_cards` テーブル（外部 agent の AgentCard キャッシュ）の読み書き
  ロジックを `relay/agent_cards.py` に実装した。詳細は後続の「`relay/agent_cards.py` 実装」
  節を参照。
- （解消済み）`PUT /streams/{stream_id}/members` で write member が 0 人になる操作への
  ガードを追加した。詳細は後続の「write member 0 人ガード」節を参照。
- `GET /status` / `GET /metrics`（wire-api.md §7.1, §7.2）、構造化ログの `level` 統一、
  Prometheus カウンタの各 endpoint への配線は Observability タスクで実装済み
  （詳細は後続の節を参照）。

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
5. **（解消済み）同一 stream の write member が 0 人になる operation へのガードを追加した**。
   詳細は下記「write member 0 人ガード（`PUT /streams/{stream_id}/members`）」節を参照。

## `relay/agent_cards.py` 実装（外部 agent の AgentCard 取得 + キャッシュ）

`agent_cards` table（`migrations/0001-initial-schema.sql`）の読み書きロジックと、外部 agent の
公開 AgentCard（`<base_url>/.well-known/agent-card.json`）の取得・JWS 署名検証を実装した
（identity-authz.md §1.2.3, §1.3, §4.2）。

### なぜ SQLite table なのか（他 registry と非対称）

subscription registry / stream membership / SSE 接続は R1 原則で in-memory（relay 再起動で消える）
だが、`agent_cards` は SQLite table（`db.py` の 4 disk table の 1 つ）である。これは
identity-authz.md §4.2「identity 自体（AgentCard / 公開鍵）は relay の in-memory state とは独立に
disk 永続化され、relay 再起動を跨いで保持される」に対応する。identity は liveness（生死）ではなく
credential（真正性の根拠）であり、揮発させる対象ではない。

### 関数構成

- `fetch_agent_card(base_url, http_get=, timeout=)` / `fetch_jwks(jku, ...)`: HTTP GET。実 HTTP を
  スタブできるよう `http_get: (url, timeout) -> (status, bytes)` を注入点にしてある。既定は stdlib
  `urllib`（production 依存を増やさないため。httpx は dev 依存のまま）。
- `store_agent_card` / `get_cached_agent_card` / `get_cached_jwks`: `agent_cards`（`identity` が
  PRIMARY KEY）への upsert と読み出し。`expires_at`（`fetched_at + ttl_seconds`、既定 TTL は
  `Settings.agent_card_cache_ttl_seconds` = 1h）超過は cache miss として扱う。`ttl_seconds=None` は
  無期限キャッシュ（`expires_at` NULL）。
- `verify_card_signature(card, public_key_pem= | jwks=)`: identity-authz.md §1.2.3 の検証手順。
  署名対象は `signatures` を除外した AgentCard の JCS 正規化（`relay.identity.canonicalize_agent_card`
  と同一）。PEM 直接指定と、`jku` から取得した JWKS（`kid` で KeySet から鍵解決）の両方をサポート。
  検証鍵が無い / 署名不一致はすべて `False`（fail-closed）。
- `get_or_fetch_agent_card(...)`: cache hit ならキャッシュを返し、miss / TTL 超過なら fetch → 任意で
  署名検証（`verify_public_key_pem` / `verify_jwks` を渡したとき。失敗なら `AgentCardFetchError` で
  キャッシュせず fail-closed）→ store する。検証鍵を渡さない場合は署名検証をスキップする（最小
  セット AgentCard は署名なしで公開される。§1.2.4）。

### スコープ

本タスクは「読み書きロジック + 取得・キャッシュ」までを実装対象とした。この cache を利用する
具体的な endpoint / 呼び出し経路（例: incoming request の JWS 署名検証への配線、federation）は
wire-api.md / identity-authz.md に endpoint として定義がなく、relay 内部の利用側実装が具体化した
時点で配線する。JWKS 鍵ローテーション運用手順（identity-authz.md §7 未決事項）も同様に本タスクの
スコープ外。

## write member 0 人ガード（`PUT /streams/{stream_id}/members`）

`StreamRegistry.put_member_checked` を追加し、`PUT /streams/{stream_id}/members` の適用後に
write 権限（`write` / `read_write`）を持つ member が 0 人になる membership 変更を拒否する
（`400 InvalidRequestError`）。write member が 0 人の stream は、以後 write を要求する全操作
（`POST .../messages` 投函 / `DELETE /streams/{id}` close / `PUT`・`DELETE .../members`
membership 変更）を実行できる identity が存在しなくなり、恒久的に操作不能になる（membership は
in-memory であり relay 再起動でしか解消しない）。

**このガードは `PUT` 経由の事故的 demote（唯一の write member が自分の access を `read` に
落とす等）を防ぐものであり、lockout を構造的に防ぎ切るものではない**。次節の通り
`DELETE .../members`（自己離脱）は identity-authz.md §2.2 により本人なら常に許可されるため
ガード対象外であり、(a) 唯一の write member が自己離脱する、(b) 複数 write member が相互に
demote し合った後に残った 1 人が自己離脱する、のいずれの経路でも write member 0 人の stream に
今なお到達できる。自己離脱経由の lockout は仕様上残る。

### 仕様上の位置づけ

`relay-v2-wire-api.md` §3.3 / `relay-v2-identity-authz.md` §2.2 にこの edge case の明示規定は
無い。したがって本ガードは確定仕様の実装ではなく、未規定の edge case に対して relay 側で妥当な
既定を与えたものである。status code は `400`（`InvalidRequestError`）を採用した。呼び出し元は
membership 変更の write 権限自体は持つ（`403` 判定は通過済み）ため、権限不足を表す `403` ではなく、
「要求された終端状態が不正（write member 0 人）」を表す `400` が意味的に妥当と判断した
（error_code の増殖を避けるため既存の `InvalidRequestError` を再利用し、専用 code は新設しない）。

### `DELETE .../members`（自己離脱）にガードを掛けない理由

ガードは `PUT`（access 変更）にのみ掛け、`DELETE .../members`（member 削除）には掛けない。

- `DELETE` で write member が 0 人に到達する経路は、唯一の write member が自分自身を削除する
  「自己離脱」の場合に限られる（他 member を削除する呼び出しは write 権限を持つ呼び出し元自身が
  残るため 0 人にならない）。自己離脱は identity-authz.md §2.2 が「本人であれば常に許可する」と
  明示規定しており、ガードで覆さない。
- `PUT` の自己 demote（唯一の write member が自分を `read` に落とす）は「離脱」ではなく access 変更で
  あり、§2.2 の自己離脱許可の対象外。member として残りつつ stream を操作不能にする事故的な footgun
  であるため、ガード対象とした。この非対称性（自己離脱は許可 / 自己 demote は拒否）は上記の仕様
  規定に沿ったものである。

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

`_sweep_retain_exceeded`（`outbox.expires_at` 経過）・`_sweep_permanent_errors`
（subscription lane で `subscription_id` が registry に存在しない、または lease 切れ）・
`_sweep_stream_permanent_errors`（stream lane の permanent error、下記）の 3 経路で
`outbox` → `dlq` へ行を移す（`_move_to_dlq`、INSERT + DELETE を同一 transaction
内で実行）。dead 化のたびに `observability.record_event(..., "outbox_dead", ...)` で
構造化ログを 1 件出す（`publish_id` で trace 可能、wire-api.md §7.3 の要求）。
`_sweep_dlq_physical_delete` が `dead_at` から `Settings.dlq_retention_days`（既定 7 日）
経過した行を物理 DELETE する。

#### stream lane の permanent error 検出（`_sweep_stream_permanent_errors`）

stream lane の permanent error は subscription lane と非対称な条件で判定する
（wire-api.md §6.6）。subscription lane は `subscription_id` が再接続ごとの使い捨て UUID で
「registry から消えた」ことが target 消滅を意味するが、stream lane の `member_identity` は
relay 再起動を跨いで安定するため「membership 不在」だけでは配達不能を定義できない。判定条件は
**「場が registry に生存 AND `NOT has_read_access(stream_id, member_identity)`」**とし、read 権限を
失った（他 member による除去 / `read_write` → `write` 降格）member 宛の未 ack エントリを
`StreamReadAccessRevoked` で dead 化する。

- **restart-safe 性**: 「場が registry に生存」を AND 条件に含むことが核心。relay 再起動直後は
  membership registry が空なので `stream_registry.get(stream_id)` が常に None を返し、この sweep は
  1 件も dead 化しない（§6.1「再起動でも未配達エントリは保持される」を破らない）。判定を「member が
  registry に居ない」だけにすると再起動直後に stream outbox 全件を dead 化する退化があり採らない。
- **自己離脱は別経路**: 本人による membership 解除（`DELETE /streams/{id}/members?identity=<自分>`）は
  この sweep の対象外。`relay.streams.delete_member` が subscription lane の unsubscribe と同型に、
  当該 member 宛の未 ack outbox を同一 transaction で即時削除する（DLQ を経由しない、§5.3 / §6.6）。
  ここで dead 化するのは他 member による除去・降格という involuntary な read 権限喪失だけである。

### idempotency dedup（`relay/idempotency.py`）

stream レーン・subscription レーン共通の in-memory dedup store
（`app.state.idempotency_store`）。`idempotency_key` 指定時は
`(lane, publisher_identity, idempotency_key)` で 15 分 window の dedup、省略時は
wire-api.md §6.3 の擬似キー補完式（`publisher_identity` / scope（stream_id か ref の
正規化 JSON）/ labels 正規化 / body・title の hash / 受信秒精度 ts）で擬似キーを計算する。

### rate limiting（`POST /publish` / `POST /streams/{id}/messages`、`RateLimiter`）

`ratelimit.py` に token bucket 方式の `RateLimiter`（`app.state.publish_rate_limiter`、
publisher identity ごと）を実装し、両 publish レーン（subscription レーン `POST /publish` と
stream レーン `POST /streams/{id}/messages`）に適用する（wire-api.md §5.4 / §3.2、既定
100 req/sec、超過時 `429` + `Retry-After` ヘッダ）。両レーンは同一の limiter インスタンスを
共有するため、1 publisher の publish 流量は両レーン合算で制限される。

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

### ack 未着タイムアウト（push 済みだが ack が進まない接続の強制切断）

`_enforce_ack_timeouts`（`relay/delivery.py`）を `dispatch_once` の polling cycle に追加した。
「push は成功している（SSE queue に積めている = SSE 送信は進んでいる）が、subscriber 側の
受信 / ack ループがスタックして ack が返ってこない」接続を検知して強制切断する
（既定 60 秒、`Settings.ack_timeout_seconds` / `RELAY_ACK_TIMEOUT_SECONDS`）。

- **既存の slow consumer 切断（`_push_with_retry`）との違い**: slow consumer 切断は
  `asyncio.Queue` の backpressure（`put_nowait` が `QueueFull` を返す）をシグナルにする。
  これは「SSE queue に積めない」ケースを見る。一方 ack 未着タイムアウトは「queue には積めて
  いる（SSE 送信は進む）が subscriber が ack を返さない」ケースを見る。両者は別の障害モードで
  あり、独立して発火する。
- **検知方法**: 接続ごとに「push 済み（`publish_id <= cursor`）だが未 ack のまま outbox に
  残る最古 publish_id（floor）」を毎 cycle 観測する（ack は outbox からエントリを削除するため、
  push 済みなのに outbox に残る = 未 ack）。floor が `ack_timeout_seconds` の間 1 度も進まない
  （= その間 1 件も ack されていない）接続を stuck とみなして切断する。cumulative ack で floor が
  少しでも上がる / 全部 ack されて floor が消える限り、進捗ありとして timer を張り直すため、
  遅いが進捗のある subscriber は切断しない。
- **切断後の回復**: 強制切断してもエントリは outbox に残るため、subscriber は再接続時に
  resume（wire-api.md §6.5、未 ack エントリの再 push）で回収する。at-least-once は ack に置く
  という既存 invariant を壊さない。
- **観測**: 強制切断は warning 構造化ログ（`sse_ack_timeout_disconnect`、`recent_warnings` /
  サーバーログに載る）で観測する。Prometheus metric は増設していない（wire-api.md §7.2 が列挙する
  固定 9 種に ack 未着タイムアウト用の counter は無く、仕様の metric 集合を拡張しないため）。

### 既知のギャップ / 後続タスクへの申し送り
2. **（解消済み）stream レーンの permanent error 検出（DLQ sweep）を実装した**。
   上記「stream lane の permanent error 検出」の節と `_sweep_stream_permanent_errors` を参照。
   自己離脱の即時削除（`relay.streams.delete_member`）と合わせて対応した。実装にあたり
   解釈の余地があった点・残るトレードオフを下記「stream lane DLQ 実装で判断した点」に記録する。
3. **（解消済み）TCP keepalive は `python -m relay.serve`（`relay/serve.py`）で設定した**。
   listen socket を自前で作って `SO_KEEPALIVE` と `TCP_KEEPIDLE`（Linux）/ `TCP_KEEPALIVE`
   （macOS の同義の別名定数）、`TCP_KEEPINTVL` / `TCP_KEEPCNT` を設定してから uvicorn の
   `Server.run(sockets=...)` に渡す。既定値は idle 60 秒 / interval 10 秒 / count 3 回で、
   環境変数 `RELAY_TCP_KEEPIDLE` / `RELAY_TCP_KEEPINTVL` / `RELAY_TCP_KEEPCNT` で上書き
   できる。プラットフォームに該当定数が無い場合はその項目だけ設定をスキップし警告ログを
   出す（起動は落とさない）。`uvicorn relay.app:app` を直接起動した場合はこの keepalive
   設定は適用されず OS 既定のままになる。運用上の注意点は `docs/ops/running.md` を参照。
4. **（解消済み、Observability タスクで対応）** `GET /status` / `GET /metrics` は
   wire-api.md §7.1, §7.2 に従い実装した。詳細は後続の Observability 実装セクションを参照。
5. **1 SSE 接続で複数 target を多重化する際の ack バッチ境界・部分 ack の最適化は
   未着手**。現状の実装は正しく動作する（各 target は個別の delivery target として
   cumulative ack される）が、性能最適化（大量 target 保持時の dispatcher 1 cycle の
   処理時間）は検証していない。
6. **（検証済み）subscription レーンの subset マッチング性能を実測した**。
   `SubscriptionRegistry.matching()` は全 subscription を線形走査する素朴な実装だが、
   10,000 subscriptions × publish labels 100 の条件で per-call p99 が sub-millisecond
   （開発機実測で約 0.3〜0.4ms、SLO の 200ms に対し 2〜3 桁の余裕）であり、全 subscription が
   マッチする最悪ケースでも同程度だった。したがって inverted index 等の最適化は現時点で過剰
   実装であり、実装しない。ベンチマークは `tests/test_subscriptions.py` の
   `TestMatchingPerformance` に残した（実測値の記録 + O(n^2) 化のような致命的性能退化を SLO
   200ms を上限として検知する回帰ガード）。実測環境・条件は当該 test を参照。

### stream lane DLQ 実装で判断した点（解釈の余地・トレードオフ）

stream lane permanent error 検出・自己離脱即時削除の実装にあたり、確定仕様の解釈で余地が
あった点と、設計上残るトレードオフを記録する。実装は下記の判断で確定させたが、後続で見直す
余地がある。

1. **DLQ error_code は `errors.py` ではなく `delivery.py` に置いた**。依頼は「新しい error_code が
   必要なら `relay/errors.py` に追加する」だったが、DLQ の `error_code`（`RetainExceeded` /
   `SubscriptionUnavailable`）は既存実装で `delivery.py` のモジュール定数として `dlq.error_code`
   列に書かれており、`errors.py` の HTTP error envelope 用 error_code（A2A 8 種 + relay 固有最小集合、
   §3.3.2）とは別 namespace である。新設した `StreamReadAccessRevoked` も既存 DLQ error_code に
   倣って `delivery.py` に置いた。これにより、error_code を「上限 7 種程度で運用」する方針が対象と
   する HTTP error envelope の error_code 数は増えていない（DLQ error_code はその 7 種の外）。

2. **「read 権限を失った状態が一定期間続いたエントリを DLQ 化」の "一定期間" は、多 cycle 継続を
   測る猶予タイマーではなく「次の dispatcher sweep cycle で判定」と解釈した**。確定仕様の文言は
   猶予帯（grace period）とも読めるが、restart-safe 性は「場が registry に生存」という構造的 AND
   条件から導かれる（時間経過ではない）ため、per-member の「read 権限喪失を最初に観測した時刻」を
   追跡する状態は持たせていない。判定は毎 sweep cycle の registry 現在値を参照する即時方式で、
   subscription lane の `_sweep_permanent_errors`（lease 切れを即 dead 化、猶予タイマー無し）と対称。
   帰結として、降格を挟んで 1 sweep cycle が走ると（直後に再昇格しても）その cycle 内で dead 化
   しうる（access 変更 flapping の誤判定）。これは lease のような時間的猶予帯を場レーンが持たない
   ことによる既知の代償であり、`tests/test_delivery.py::TestStreamDlqSweep` の flapping 2 ケースで
   挙動を固定した。多 cycle 猶予が必要と判断されたら、`unacked_since` 方式（`_enforce_ack_timeouts`
   参照）に倣って per-target の初観測時刻を追跡する拡張余地がある。

3. **自己離脱の即時削除は `DELETE self`（DELETE handler）に限定し、`PUT` による自己降格
   （`read_write` → `write`）は sweep 経路（DLQ 化）のままにした**。確定仕様は即時削除の対象を
   「自己離脱（DELETE self、本人による membership 解除）」と明示しており、自分で PUT して read 権限を
   落とすケースは言及が無い。文言どおり DELETE self のみを即時削除とし、自己降格を含むその他の
   read 権限喪失は involuntary 扱いで sweep に委ねた。自己降格も「明示的な関心放棄」とみなして即時
   削除に含めるべきかは仕様の追補待ちの論点として残る。

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

## Observability タスク: `GET /status` / `GET /metrics` / 構造化ログの `level` 統一

`relay/observability.py` に `GET /status`（wire-api.md §7.1）と `GET /metrics`（§7.2、
Prometheus text exposition format）を実装した。両 endpoint とも他の GET 系 endpoint と同様
`require_authn` のみを通す（authN のみで authZ なし、identity-authz.md §2.1）。

### 構造化ログへの `level` フィールド追加と `recent_warnings`

`record_event(app_state, event_type, level="info"|"warning", **fields)` に `level` 引数を
追加した（既定 `"info"`、後方互換）。`level="warning"` の event は、サーバーログへの追記に加えて
`app_state` 上の in-memory リングバッファ（`collections.deque(maxlen=50)`、
`RECENT_WARNINGS_MAXLEN`）にも積まれ、`GET /status` の `recent_warnings` はこのバッファを
そのまま返す。

`level="warning"` を付けた既存 event: `outbox_dead`（DLQ 移動、`delivery.py`）、
`subscription_registry_evicted`（lease 切れ registry 除去、`delivery.py`）、
`dispatcher_error`（dispatcher 内部エラー、`delivery.py`）、`sse_slow_consumer_disconnect`
（新設、下記）。加えて `relay/identity.py` の `require_authn` に認証失敗時の
`record_event(..., "authn_failed", level="warning", reason=...)` を追加した
（wire-api.md §7.3 が構造化ログの対象に「認証失敗」を明記しているが、従来は未実装だった）。
`identity.py` から `observability` への import は関数内 lazy import にしている
（`observability.py` は自身の `GET /status` / `GET /metrics` 実装のために
`relay.identity.require_authn` を import 済みであり、モジュールトップレベルで逆方向の
import を足すと循環 import になるため）。

### `GET /status` の実装

`uptime_seconds`（`app.state.started_at` を `relay/app.py` の `create_app` で
`time.monotonic()` により記録、壁時計のずれの影響を受けない）、`subscriptions_count` /
`streams_count`（`SubscriptionRegistry.count()` / `StreamRegistry.count()`、本タスクで追加）、
`active_sse_connections`（`ConnectionManager.count()`、本タスクで追加）、
`outbox_pending_count` / `outbox_dead_count`（`outbox` / `dlq` table の `COUNT(*)`）、
`publish_rate_5min`、`recent_warnings` を返す。

**`publish_rate_5min` の解釈**: wire-api.md §7.1 はフィールド名のみで単位を明記していない。
「5 分間の publish 件数」（カウント）と「秒あたりレート」（Prometheus `rate()` 相当）のどちらとも
読めるため、本実装ではフィールド名の `_rate_` を字義通りに取り、直近 5 分間の `publish_log`
件数を 300 秒で割った秒間レートとして実装した（推測に基づく判断であり、確定仕様ではない）。
カウントそのものが必要な場合は仕様側で明確化した上で実装を見直すべきである。

### `GET /metrics`（Prometheus 互換）の実装

`MetricsRegistry`（`app_state.metrics_registry`、`dict[str, dict[label_tuple, float]]` +
`threading.Lock`）が counter 系 7 metric を in-memory で保持する。呼び出し側は
`observability.inc_metric(app_state, name, **labels)` を該当箇所で呼ぶだけでよく、registry の
生成・保持は `observability.py` に閉じる（`streams.py` / `subscriptions.py` / `delivery.py` は
いずれも `observability` を import 済みだが、`observability.py` はそれらを import しない —
この非対称性で循環 import を避けている）。

gauge 系 2 metric（`relay_outbox_depth` / `relay_sse_connections`）は積算せず、スクレイプ時点で
DB / `ConnectionManager` から実測して都度計算する（カウンタの drift を防ぐため）。

wire-api.md §7.2 が列挙する 9 metric すべてを実装した。カウンタは未 increment のラベル組み合わせ
を出力しない（典型的な Prometheus client library の挙動に合わせた）。

| metric | 種別 | 呼び出し箇所 |
|---|---|---|
| `relay_publish_received_total{publisher_identity}` | counter | `streams.post_stream_message` / `subscriptions.publish` の成功パス |
| `relay_publish_failed_total{failure_reason}` | counter | 上記 2 endpoint の各バリデーション失敗パス（`failure_reason` は `stream_not_found` / `membership_required` / `stream_gone` / `invalid_request` / `rate_limited` の snake_case 文字列。error envelope の `code`（`StreamNotFoundError` 等）とは別の namespace として定義した） |
| `relay_push_delivered_total{lane}` | counter | `delivery._dispatch_to_connections` の push 成功時（`lane` は `target_type` の値 `stream`/`subscription` をそのまま使う） |
| `relay_ack_received_total` | counter | `streams.ack_stream` / `subscriptions.ack_subscription` の成功パス（label なし、両レーン合算。wire-api.md の記法が `relay_push_delivered_total{lane}` 等と異なり `{}` を伴わないため） |
| `relay_outbox_dead_total` | counter | `delivery._move_to_dlq`（DLQ 移動のたび） |
| `relay_subscription_lease_expirations_total` | counter | `delivery._sweep_expired_subscription_registry`（registry から実際に evict された時点。`is_lease_expired()` の個々の観測点では increment しない — 同一 subscription への繰り返しチェックで水増しされるのを避けるため、「evict という 1 回限りの事象」に対応づけた） |
| `relay_sse_slow_consumer_disconnects_total` | counter | `delivery._push_with_retry`（retry 枯渇 → 強制切断時。呼び出し元は `_force_disconnect` の唯一の呼び出し元でもあるため二重計上の心配はない） |
| `relay_outbox_depth` | gauge | `GET /metrics` スクレイプ時点で `SELECT COUNT(*) FROM outbox` |
| `relay_sse_connections` | gauge | `GET /metrics` スクレイプ時点で `ConnectionManager.count()` |

label には `subscription_id` / `delivery_target` を使わない（wire-api.md §7.2 の禁止事項）。

### `_push_with_retry` のシグネチャ変更

`delivery._push_with_retry(conn, event_dict)` に `app_state` 引数を追加した
（`_push_with_retry(conn, event_dict, app_state=None)`）。強制切断時の構造化ログ + metric
記録に必要なため。呼び出し元 `_dispatch_to_connections` からは実 `app_state` を渡す。
既存テスト（`tests/test_delivery.py`）の直接呼び出し箇所は `None` または
`SimpleNamespace()` を明示的に渡すよう更新した。

### 既知のギャップ / 判断が必要な点

1. **`publish_rate_5min` の単位解釈は推測**（上記）。仕様側での明確化が望ましい。
2. **`relay_publish_failed_total` の `failure_reason` は error envelope の `code` と別 namespace**
   （上記表参照）。両者の対応関係はコード内コメントのみで、仕様書には未記載。
3. **`GET /status` の DB 集計（`outbox_pending_count` 等）はリクエストのたびに `COUNT(*)`
   を実行する**。outbox 件数が非常に多くなった場合の性能は未検証（性能 SLO 検証自体は
   本タスクのスコープ外として明示的に見送られている）。
4. **`recent_warnings` の保持件数（50 件）とバッファのスコープ（app インスタンス単位、
   relay 再起動で消える）は本タスクでの判断**。wire-api.md は件数・保持期間を規定していない。

## Verify タスク: outbox 障害時に `503` を返す共通 exception handler を追加

T6 退化モード検証（受け入れ基準「outbox 障害 (disk full / DB corrupt 擬似)
で `POST /publish` が `503`」）を実機で検証したところ、DB ファイルの権限を落として
SQLite の read/write を失敗させると、各 endpoint 実装（`streams.py` / `subscriptions.py`）
は `sqlite3.Error` を未処理のまま送出し、Starlette デフォルトの `500 Internal Server Error`
（非構造化・error envelope 無し）になっていた。wire-api.md §8 が明示的に `503` と規定して
いるため、これは受け入れ基準を満たさない実装欠陥と判断し、その場で修正した。

個々の endpoint 実装が SQLite 操作のたびに try/except するのではなく、`relay/app.py`
の `create_app` で Starlette の `exception_handlers={sqlite3.Error: handle_outbox_unavailable}`
を登録し、一箇所に集約した（`relay/errors.py` に `OUTBOX_UNAVAILABLE =
"OutboxUnavailableError"` を追加）。`sqlite3.Error` は `sqlite3.OperationalError` /
`sqlite3.DatabaseError` 等の基底クラスであり、DB ファイルの権限エラー・disk full・
corruption のいずれもこの経路で捕捉される。`tests/test_app.py` の
`TestOutboxUnavailable` で `db.get_connection` を monkeypatch して `sqlite3.OperationalError`
を送出させ、`503` + `{"code": "OutboxUnavailableError"}` を返すことを検証した。実機
（DB ファイルを `chmod 000` して SQLite の open を失敗させる方法）でも `503` を確認した。

## `relay_sdk`（Python SDK、クライアント側）実装

`docs/design/relay-v2-sdk.md` の仕様に従い、クライアント側 Python SDK を新規パッケージ
`relay_sdk/` として実装した。relay 本体（`relay/` パッケージ）には一切変更を加えていない。

### パッケージ構成（実装済み）

```
relay_sdk/
├── __init__.py           # errors の re-export
├── errors.py             # RelayProtocolError / TransientError / PermanentError（§4.4）
├── config.py             # 環境変数解決（§6）
├── testing.py            # FakeRelay（§7.1、in-process HTTP server stub）
├── outbox/               # publisher 側（§2）
│   ├── __init__.py       # publish / poll / mark_delivered / run_dispatcher の re-export
│   ├── schema.py         # relay_outbox DDL + create_outbox_table
│   ├── publisher.py      # publish(conn, ...) 本体 + debug 用 poll / mark_delivered
│   ├── dispatcher.py     # run_dispatcher 常駐ループ + file lock singleton
│   └── __main__.py       # python -m relay_sdk.outbox（CLI entrypoint、§2.3.2）
├── client/               # subscriber 側（§3）
│   ├── __init__.py       # subscribe / Subscription / Event / EventDisplay / reconcile
│   ├── subscription.py   # Subscription / Event / EventDisplay / subscribe()
│   ├── sse.py            # SSE frame パーサ
│   └── reconcile.py      # retain 切れ fallback ヘルパ（§3.5）
└── http/                 # protocol 層（§4）
    ├── __init__.py
    ├── request.py        # post_publish / post_subscription / put_lease / delete_subscription / post_ack / open_sse + status→例外翻訳
    └── auth.py           # Bearer / JWS 署名・検証 + make_client
```

### 実装カバレッジ（依頼の優先順位に対応）

| 優先度 | 項目 | 状態 |
|---|---|---|
| 1 | publisher `publish()` + `run_dispatcher()`、subscriber `subscribe()` → `receive()` → `ack()` / `close()`、§5.1/§5.2 の典型コード例 | 実装済み・テスト済み |
| 2 | `relay_sdk.http`（6 request 関数）+ `relay_sdk.errors`（3 例外分類） | 実装済み・テスト済み |
| 3 | `FakeRelay` + §7.1 の title 型分離「固定すべき 4 項目」の回帰テスト | 実装済み・テスト済み |
| 4 | 実 `relay/app.py` に対する integration test（往復 1 本 + dispatcher 再起動） | 実装済み・テスト済み |
| 5 | JWS 署名/検証（`http.auth`）、`reconcile()`（§3.5）、CLI entrypoint（`python -m relay_sdk.outbox`） | 実装済み・テスト済み |

新規テスト（`tests/test_sdk_*.py` + `tests/integration/test_sdk_roundtrip.py`）を追加した。
全体で 407 passed（既存 354 + SDK 分 53）。relay 本体テストへの破壊はない。

### 判断した点（一次情報源に曖昧さがあり、独自解釈で進めた箇所）

1. **SSE 接続は最初の `receive()` で張る（`subscribe()` では張らない）**。§3.1 の文面は
   「`subscribe()` が…`GET /events` で SSE 接続を張る」だが、SSE stream は generator の
   lifecycle であり、`subscribe()` で開いて `receive()` まで保持するのは httpx streaming の
   扱いが煩雑になる。`subscribe()`→`receive()` 間の publish は relay が未 ack outbox として
   保持し接続時に再 push する（§6.5）ため、遅延して張っても取りこぼさない。配達確証への
   影響はないと判断し、SSE は `receive()` 初回で張る実装にした。

2. **`auto_ack` の flush タイミングは「caller が event を resume した直後に即 flush」**。
   §3.2 は「次の relay 通信（lease renew / 次の ack flush / close）で送る」と batch を示唆
   するが、v1 は正確性優先で、resume 直後に cumulative ack を 1 回 POST する実装にした。
   これは §3.3 の「handle が成功した直後の event だけが ack される」を厳密に満たし、
   「resume 直後の flush」自体が spec の言う「次の relay 通信」に該当する。event ごとに
   1 POST になるが cumulative なのでコストは O(1)（wire §5.6）。batch 効率化が要る場合は
   caller が `auto_ack=False` + `ack()` を使う（§5.3）。この判断は推測に基づく実装選択で
   あり、性能要求が出た段階で deferred flush へ差し替えられる。

3. **dispatcher の per-row retry backoff は in-memory 管理**。§2.1 の `relay_outbox` schema
   には「次回リトライ時刻」列が無い。§2.3.1 手順4 の「同一ループ内で待つのではなく次回
   polling まで待つ」指数バックオフを、schema を拡張せず dispatcher プロセスの in-memory
   dict（`_backoff_until`）で刻む実装にした。dispatcher は単一プロセス（file lock で enforce）
   のため in-memory で十分で、プロセス再起動時は backoff state を失って即リトライになる
   （at-least-once を壊さない。過剰再送は relay 側 idempotency 15 分 dedup が吸収する）。

4. **`FakeRelay` は httpx.MockTransport ではなく実 in-process HTTP server（`http.server`）**。
   §7.1 は「Python オブジェクトで模した stub」とするが、SDK が使う httpx streaming / SSE
   frame parse / 再接続の実経路をそのまま通すには MockTransport では不足（streaming の
   incremental read を模しにくい）と判断し、stdlib の `ThreadingHTTPServer` で実 socket を
   張る stub にした。「real relay なしに駆動」という §7.1 の要件は満たす（`relay/app.py` に
   依存しない）。SSE body は `Connection: close`（body-until-close）で httpx に incremental
   に読ませる。fault 注入（`simulate_outage` / `simulate_subscription_loss` /
   `drop_connections`）を実装した。**auth は FakeRelay 側で強制しない**（`Authorization`
   ヘッダを無視する）。認証経路の検証は integration test（実 relay）側で行う。

5. **Bearer token 解決は `RELAY_BEARER_TOKEN` 環境変数を主経路にした**。現行 relay の authN
   （`relay/identity.py`）は `Authorization: Bearer <token>` の静的照合（`RELAY_AUTH_TOKENS`
   の token→identity 表）であり、relay は **JWS Bearer を consume しない**。したがって
   §4.3 の JWS 署名（`sign_jws`、pyjwt[crypto] + ES256）は A2A 準拠を見据えた MAY 機能と
   して実装したが、現行 relay に対する実認証は plain Bearer token で行う。integration test は
   relay の `Settings.auth_tokens` と `RELAY_BEARER_TOKEN` を揃える（publisher / subscriber を
   同一 identity で回す。publish は authN のみ、subscribe は subscriber==認証 identity を要求
   するが、同一 identity なら両立する）。`subscribe()` の signature は spec 通り（`bearer_token`
   引数を足していない）。`run_dispatcher()` には利便のため任意の `bearer_token` 引数を追加した
   （spec signature の superset。省略時は env にフォールバック）。

6. **subscriber は場（stream）レーンの event を skip する**。§3.2 の `Event` 型は
   `ref_type` / `ref_id` / `labels` を持つ subscription レーン形状で、場レーンの `body` のみの
   event は表現できない。`subscribe()` は subscription を張るだけで場 membership を張らない
   ため、`GET /events` に場 event が混ざる状況は通常発生しない。防御的に
   `delivery_target` が `sub:` で始まらない frame は skip する実装にした。

7. **`reconnect_max_attempts` 枯渇時は例外ではなく resubscribe**。§3.4 は「max_attempts に
   達するか 404/410 を受け取ったら新規 subscribe に切り替える」と規定するため、再接続上限に
   達しても caller へ例外を投げず、新規 `POST /subscriptions` で自己修復する（self-healing）。
   `RELAY_SSE_RECONNECT_MAX_ATTEMPTS=0` は無限（§6）として扱う。

8. **`title` の 200 文字上限は文字数（`len()`）で判定**。§2.2 / wire §5.4 の「200 UTF-8 chars」
   を byte 数ではなく文字数と解釈した（曖昧さあり）。超過時は truncate せず `ValueError`
   （SDK は truncate しない、§2.2）。

9. **`pyproject.toml` は変更していない**。SDK が使う `httpx`（dev 依存）と `pyjwt[crypto]`
   （`mcp` の推移的依存として uv.lock に pin 済み）はいずれも既存の依存解決で利用可能なため、
   依存追加は不要と判断した（並行して `relay/` を触る別担当との衝突回避も兼ねる）。`joserfc`
   / `rfc8785` も relay 本体が既に依存として持つ（AgentCard 検証の相互運用で流用）。

### 未実装 / 後続タスクへの申し送り

1. **AsyncClient / asyncio 版 API は未実装**（§8 で v1 スコープ外と明記。同期版のみ）。
2. **`tests/contract/`（§7.3 の独立 contract test）は未作成**。wire フォーマットの整合は
   integration test（実 relay に対する往復）で間接的に検証している。ワイヤ API ドキュメント
   更新時に SDK 側追随漏れを機械検知する専用スイートは後続で追加すべきである。
3. **§7.2 の integration 観点のうち未自動化のもの**:
   - subscriber プロセス再起動 → 新規 subscribe → 古い outbox の 7 日後 GC（time-shift
     fixture）: dispatcher 側の DLQ 7 日 GC は `_gc_dlq` の単体テストで検証済みだが、
     daemon loop を time-shift で 7 日進める統合検証は未実施。
   - `429` の `Retry-After` 尊重: dispatcher cycle の単体テスト（`test_429_respects_retry_after`）
     で検証済み。実 relay の rate limit（既定 100 req/sec）を実際に超過させる統合検証は、
     テスト時間とのバランスから見送った。
   - JWS 署名・検証（鍵不一致で接続拒否）: `sign_jws` / `verify_relay_agent_card` の単体
     テストで検証済み。ただし**現行 relay は JWS Bearer を consume しない**（静的 Bearer 照合、
     判断5）ため、「鍵不一致で実 relay が接続拒否する」統合検証は現行 relay では意味を持た
     ない。relay 側が JWS 検証を実装した段階で追加すべきである。
   - SSE 30 秒 keepalive で長期接続が落ちないこと: keepalive comment frame の parse と
     それを契機とした lease renew は実装済み（`sse.py` / `Subscription._maybe_renew_lease`）
     だが、30 秒級の長時間統合テストはスイート実行時間を圧迫するため自動化していない。
4. **（第三者レビュー指摘により訂正済み、後述「第三者レビュー対応」参照）** 当初
   lease renew の発火契機を keepalive comment frame に限定していたが、これは
   event が keepalive 間隔より高頻度に届く状況で renew が一切発火しない欠陥
   だった。event / keepalive いずれの frame でも発火するよう修正済み。

## 第三者レビュー対応（ブロッカー2件・medium4件の修正）

初版実装に対する第三者（Fable）レビューで、致命的な欠陥 2 件（ブロッカー）と修正推奨
4 件（medium）が指摘された。すべて `relay_sdk/` 側の修正で、`relay/` パッケージ（サーバー
実装）には触れていない。

### ブロッカー1: lease renew が高頻度 event 下で一生発火しない

`Subscription._stream_once`（`relay_sdk/client/subscription.py`）は当初、`_maybe_renew_lease()`
の呼び出しを SSE の comment（keepalive）frame 受信時のみに限定していた。しかし relay 本体
（`relay/delivery.py`）は「push が無い間だけ」keepalive を送るため、event が keepalive
間隔（既定 30 秒）より高頻度に届く状況では keepalive 自体が一切来ない。結果として lease
（既定 300 秒）が更新されないまま失効し、失効後は relay 側の `SubscriptionRegistry.matching()`
（`relay/subscriptions.py`）が当該 subscription を fan-out 対象から除外するため、失効中に
publish された event は二度と配達されなくなる欠陥だった。

修正: `_maybe_renew_lease()` を独立の `_run_periodic_maintenance()` に統合し、event frame /
comment frame いずれの処理時にも呼ぶようにした（`_stream_once` 内、frame の種別判定より前）。
回帰テストは `tests/test_sdk_client.py::TestLeaseRenewOnEventFrames` に 2 本追加した。
1 本目は `_lease_expires_at` を直接過去日時に書き換えて renew 閾値を即座に満たす決定的な
テスト（`put_lease` の呼び出し回数をスパイして検証）、2 本目は実際に高頻度 publish を
続けて `lease_expires_at` が実地で更新されることを確認するタイミングベースのテスト。

### ブロッカー2: SSE 無音検知（read timeout）が未実装 + read timeout が全リクエストで無効化されていた

`relay_sdk.http.auth.make_client` は当初 `httpx.Timeout(timeout, read=None)` で client
全体の read timeout を無効化していた。コメントには「keepalive で明示検出する」とあったが
その検出ロジック自体がどこにも実装されておらず、`Subscription._keepalive_seconds` は
保持されるだけの未使用フィールドだった。この結果、半死 TCP 接続（ソケットは生存したまま
一切データが来ない状態）で `receive()` が永久ブロックしうるだけでなく、read timeout が
client 全体で無効化されていたことにより **`POST /publish` 等の通常リクエストまで**
無応答時に永久ブロックしうる状態だった（dispatcher の配達ループが停止しうる）。

修正は 2 段階:
1. `make_client` は `timeout` を connect/read/write/pool の全軸に適用する単純な形に戻し、
   通常リクエストの read timeout を有効化した。
2. `relay_sdk.http.request.open_sse` に `read_timeout` 引数を追加し、SSE request にだけ
   個別に長い read timeout（`Subscription._stream_once` が `keepalive_seconds * 2`、既定
   60 秒を渡す）を上書きできるようにした。`_stream_once` は `open_sse` を囲む try/except で
   `httpx.TimeoutException` / `httpx.TransportError` を捕捉し `TransientError` に翻訳する
   （`receive()` の再接続ループに乗せるため）。

read timeout の閾値を「keepalive 間隔そのもの」ではなく「2 周期分」にしたのは、keepalive
送出の揺らぎ（dispatcher の polling 間隔・GIL 競合等）で誤検知しないための安全マージンで
あり、確定した数値根拠があるわけではない（判断の性質としては推測に基づく安全側の選択）。

回帰テストは 2 レイヤーで担保した。
- protocol 層（`tests/test_sdk_http.py::TestReadTimeoutScoping`）: `make_client` が通常
  request に read timeout を適用すること、`open_sse(read_timeout=...)` が対象 request
  だけ read timeout を上書きし connect/write/pool は client 既定のまま保つことを、
  `httpx.Client.stream` を monkeypatch して直接検証。
- 振る舞いレベル（`tests/test_sdk_client.py::TestSilentConnectionDetection`）: `FakeRelay`
  に `simulate_silence()`（SSE 接続を張ったまま event も keepalive も一切書き込まない、
  半死接続を模す fault 注入）を追加し、`open_sse` の呼び出し回数をスパイして「read timeout
  検知 → 再接続」が実際に起きたことを直接確認する。**単に「いずれ event が届く」だけの
  アサーションでは、`FakeRelay` が無音期間中も TCP 接続自体を close しないため、read
  timeout が無効なままでも同じ接続でいずれデータを受信できてしまい、fix の有無を区別
  できない**（実際にこの誤りに一度陥り、当初のテストは fix を revert しても pass して
  しまっていた。`open_sse` 呼び出し回数という直接的なシグナルに変更して修正した）。

### medium3: reconnect_max_attempts 到達後、resubscribe がホットループする

`Subscription._resubscribe` の retry ループは `delay = self._next_reconnect_delay(); if delay:
time.sleep(delay)` という形で、`_next_reconnect_delay()` が `reconnect_max_attempts` 到達後に
返す `None` を「sleep 無し」と解釈していた。`_resubscribe` 自身は「新規 subscribe に切り替えた
後の retry ループ」であり、そこで `_next_reconnect_delay()` が `None` を返すのは「もう
sleep しなくてよい」という意味ではなく「上限に達した」という意味でしかない。結果、
`_resubscribe` は上限到達後 `POST /subscriptions` を delay ゼロで連打するホットループに
陥っていた。

修正: `time.sleep(delay if delay is not None else self._backoff_cap)` とし、`None` の場合は
`backoff_cap`（既定 30 秒）で待つようにした。回帰テストは
`tests/test_sdk_client.py::TestResubscribeBackoffAfterAttemptsExhausted` に追加した。
`time.sleep` を monkeypatch して呼び出し引数を記録し、`reconnect_max_attempts=2` 到達後の
全 sleep が `backoff_cap` になっていること（ゼロ delay の連打が起きていないこと）を検証する。

### medium4: auto_ack の flush retry が「次の event」を待つしかなく、event が来ないと永久に未 ack のまま残る

`_buffer_and_flush_ack` は resume 直後に一度だけ ack flush を試み、`TransientError` で
失敗した場合は warning を出すのみで、次に flush が retry される契機は「次の event が
yield されたとき」に限定されていた。event が来ない期間が続くと、この未 flush の ack が
無期限に放置される欠陥だった（ブロッカー1と同じ frame 処理箇所の問題であるため、
まとめて `_run_periodic_maintenance()` に統合して解消した）。

修正: `_run_periodic_maintenance()` が lease renew に加えて、`self._ack_buffer is not None`
なら `_flush_ack()` を retry するようにした。event frame だけでなく comment（keepalive）
frame でも呼ばれるため、event が来ない間も keepalive 契機で retry される。回帰テストは
`tests/test_sdk_client.py::TestAckFlushRetry` に追加した。`post_ack` を monkeypatch して
最初の 1 回だけ `TransientError` を注入し、新しい event を publish しないまま
（別スレッドで `receive()` をブロックさせた状態で）keepalive 契機のみで outbox が drain
されることを確認する。

### medium5: httpx が dev group のみで runtime dependency として宣言されていなかった

`relay_sdk` は import 時に `httpx` を必須とするが、`pyproject.toml` では `httpx` が
`[dependency-groups].dev` にのみ含まれていた（旧来 `relay/delivery.py` の統合テスト
専用だったため）。`relay_sdk` を dev 依存なしで install したアプリでは `ImportError` に
なる状態だった。

修正: `httpx` を `[project.dependencies]` に移動した（`dev` グループからは削除。`uv lock`
で再解決済み）。回帰テストは `tests/test_sdk_packaging.py` に追加し、`pyproject.toml` を
`tomllib` でパースして `httpx` が `[project.dependencies]` に含まれることを検証する。

### medium6: outbox の labels 列破損が dispatcher 全体をクラッシュさせる

`dispatcher._deliver_row`（`relay_sdk/outbox/dispatcher.py`）は `json.loads(row["labels"])`
を try/except の**外**で呼んでいた。一方 daemon ループ（`run_dispatcher`）は
`except sqlite3.Error` しか捕捉しないため、outbox の `labels` 列に不正な JSON が入った行が
1 つでもあると `json.JSONDecodeError` が daemon ループ全体を突き抜けて daemon スレッドを
落とし、同一 cycle 内の後続行（正常行も含む）は一切処理されず、以後 daemon が再起動
されない限り全配達が止まるクラッシュループになっていた。

修正: labels のデコードを `_deliver_row` 内の try/except に含め、`json.JSONDecodeError` /
`TypeError` を捕捉した場合は該当行を（retry せず）即 dead 化するようにした（`_RowResult.DEAD`。
壊れたデータはリトライしても直らないため）。回帰テストは
`tests/test_sdk_dispatcher.py::TestMalformedRow` に 2 本追加した。1 本目は `_dispatch_once`
単体で、labels 破損行の**前**に正常行を、**後**に破損行を置いた場合に正常行が配達され
続けること・破損行が dead 化されることを確認する（破損行を先に置いて `ORDER BY id` で
先に処理させ、クラッシュがそこで起きた場合に後続の正常行に到達できないことを検出できる
配置にした）。2 本目は `run_dispatcher` の daemon スレッドレベルで、破損行の後に daemon が
生存し続け、新たに enqueue した正常行を配達し続けることを確認する。

### レビュー対応中に見つけた副次的な欠陥（`relay_sdk.testing.FakeRelay`）

medium3 の回帰テスト作成中に、`FakeRelay`（テスト用 stub、`relay_sdk/testing.py`）自体の
バグを発見した。`do_POST` / `do_PUT` が outage（`simulate_outage(True)`）応答を返す際、
受信した request body を読み切らずに応答を返していた。HTTP/1.1 keep-alive 接続では、
未読の body バイトが残ったまま次の応答を書くと、その未読バイトが後続 request の
先頭に混入してパース位置がずれ、`http.server.BaseHTTPRequestHandler` が次の request line
を誤読して `501 Unsupported method` 等の破損応答を返す（実際に、outage 中に同一接続で
複数回 POST を送るテストで 1 回おきに 501 が返る形で顕在化した）。

修正: `do_POST` / `do_PUT` / `do_DELETE` の routing 冒頭で必ず body を drain
（`_drain_body()`）してからバッファに保持し、`_read_json()` はそのバッファから読むように
変更した（`self.rfile` からの再読み込みをやめた）。これは `relay_sdk` 本体のバグではなく
test double 側の実装欠陥だが、修正しない限り「同一 keep-alive 接続上で outage 中に
複数回 request する」パターンのテストが不安定になるため、あわせて修正した。

## federation envelope 暗号化のスコープ（relay 間区間のみ、E2E ではない）

`relay/federation_peers.py`（`encrypt_envelope_body` / `decrypt_envelope_body`）が提供する
JWE 暗号化（ECDH-ES + A256GCM）が守るのは、**送信側 relay が宛先 peer relay へ HTTP `POST`
する区間（relay 間区間）だけ**である。これは federation という機能が「別々の relay インスタンス
の間」でメッセージを中継する層であり、各 relay インスタンス自身とそこに繋がる agent
（publisher / subscriber）との間は、federation を経由しない local な stream / subscription
と同じく、既存の Bearer token 認証済み HTTP / SSE 経路がそのまま使われることによる。

- 送信側（`relay/federation_egress.py`）: envelope の `body` を暗号化し `body_jwe` として送る
  かどうかは自分と宛先 peer の鍵の有無で決まる。`origin_stream_id` / `origin_publish_id` /
  `from_sub` / `to_members` は配達ルーティングに必要なメタデータのため、暗号化の有無に
  関わらず常に平文のまま送る（envelope 全体が暗号化されるわけではない）
- 受信側（`relay/federation_inbound.py`）: `body_jwe` を自分の秘密鍵で復号したあと、
  他の受信メッセージと同様 `publish_log` / `outbox` に**平文で** INSERT する。以後の
  local member への配達（`GET /events` の SSE）は暗号化されない、既存の認証済み経路である

したがって、この暗号化は「publish した agent から購読側 agent までのエンドツーエンドの
暗号化」ではない。中間の relay インスタンス自身（disk 上の DB、プロセスメモリ）は平文の
body を見ることができる。関与するのは「relay インスタンス間のネットワーク区間の盗聴・
改竄からの保護」のみである。

暗号化鍵が双方揃わない場合は互換のため平文 `body` にフォールバックする（`relay-server.jsonl`
への `federation_plaintext_fallback` ログと `relay_federation_plaintext_fallback_total`
カウンタで可視化される）。全体設定 `Settings.federation_require_encryption` または peer 単位の
`peers.require_encryption`（`python -m relay.invite peer require-encryption`）を真にすると、
このフォールバックをやめて `PeerEncryptionRequired` で DLQ に回す（README「設定」節参照）。

## `tests/integration/federation_harness.py`（2 relay federation 統合テストの基盤）

`relay/federation_egress.py`（送信側）・`relay/federation_inbound.py`（受信側）は
それぞれ単体テスト済みだが、2 relay 間で実際に署名付き HTTP 越しの配達が通ることそのもの
を検証する integration test はこれまで無かった（`tests/integration/test_federation_cli_roundtrip.py`
は招待 / redeem / enc-key の CLI 面を実 2 relay で検証済みだが、stream publish → egress →
inbound → SSE 受信までは通していない）。`tests/integration/federation_harness.py` は
この経路を埋める test harness で、`tests/integration/test_federation_roundtrip.py` から使う。

### 構成

`LiveRelay` が 1 relay インスタンスを実 TCP port（uvicorn + daemon thread、
`tests/test_delivery.py` の `LiveServer` と同型）で起動する。`FederationPair` が A/B
2 つの `LiveRelay` を束ね、`start()` 内で `python -m relay.invite peer new/redeem/enc-key`
を実際に呼んで招待発行 → redeem → enc-key 交換までを完了させる。以降は
`create_stream` / `add_federation_member` / `publish` / `open_sse` を組み合わせて
往復を書ける。`federation_pair` という pytest fixture（`federation_harness.py` 定義）が
この完了済み `FederationPair` を渡す。

### 再利用時の注意

- dispatcher（egress / local push とも）は各 `LiveRelay` の in-process asyncio task
  （`relay/app.py` の lifespan、`Settings.dispatcher_poll_interval_seconds` で待ち時間を
  縮めている）が担う。テスト側で dispatcher を別途起動する配線は要らない
- `GET /events` は終端しないストリームのため ASGI transport / Starlette `TestClient` では
  読めない（`tests/test_delivery.py` 冒頭 docstring 参照）。`FederationPair.open_sse` は
  実ソケット越しの `httpx.Client.stream()` を使う
- `tests/integration/` に `__init__.py` は無い（pytest の rootless import 前提）ため、
  harness の import は `from federation_harness import ...`（相対 import ではない）
- peer registry に `enc_key_jwk` が pin 済みであることは、実際に配達された envelope が
  暗号化（`body_jwe`）されたことの証明にはならない（平文フォールバックへの regression でも
  registry 側の値は変わらない）。wire 上で実際にどちらが送られたかを確認したい場合は
  `capture_egress_envelope(monkeypatch)` を使う。`relay.federation_net.build_async_client`
  を実ソケットのまま `event_hooks` で差し替え、egress dispatcher が POST する envelope
  JSON を横取りする（`tests/test_federation_egress.py` の `_patch_transport` と異なり
  `httpx.MockTransport` には差し替えないため、実際の配送は妨げない）
