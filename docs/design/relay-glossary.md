# relay v2 グロッサリー

> **位置づけ**: relay v2 で使う用語の確定版定義集。relay 概論、ワイヤ / API 仕様、
> identity / authZ 仕様、SDK 仕様、シーケンス図集の各文書が共通の語彙で書けるよう、
> ユビキタス言語をここに集約する。
>
> **想定読者**: relay v2 の各設計文書を読み書きする実装者・設計者。本書は他の文書を読む
> 前後で随時参照する辞書として使う。
>
> **表記方針**: 各エントリは「英語名 / 一行定義 / 詳細 / 関連用語 / 出典」の 5 段構成で
> 並べる。原則として用語名は英語で書き、日本語名は併記が定着している場合のみ括弧で
> 添える。

---

## §0 一行要約

relay v2 は、エージェント間で非同期に流れるメッセージを at-least-once で配達する
汎用 message bus である。本書はその relay v2 を語るときに使う用語を、
中核モデル / ack / identity / 内部機構の 4 カテゴリで定義する。

旧 v1 系の文書や、第三者レビューの議論経過で散見される表記揺れ（「場」「publish_seq」
「history?since=N」など）は、本書 §5 で v2 用語に整理した。

---

## §1 中核モデル

### stream

**一行定義**: publisher と subscriber を仲介する pure pass-through な配信トピック。

**詳細**:

- 名前付きの共有空間で、`stream_id`（文字列）で識別する。`stream_id` は作成者 identity で
  スコープ化された canonical id（`{作成者 identity}:{name}`）であり、作成時に relay が構築して返す。
  作成者は名前空間内の `name` のみを選べるため、identity をまたいだ stream_id の衝突・横取り
  （squatting）が構造的に起きない（ワイヤ / API 仕様 §3.1）。
- メッセージは投函された瞬間にメンバーへの配達経路（outbox）に転写されるだけで、
  stream という場所には永続蓄積されない（pass-through）。
- stream は membership（identity ごとの read / write access の集合）を持ち、これが場固有の
  structural authZ の判定材料として機能する。
- stream の close は「新規投函を止める」ことだけを意味し、過去メッセージのアーカイブを
  作らない。close 後も outbox に残った未配達分は retain 期間まで配達を継続する。
- 機能要件文書の本文では日本語の「場」と表記される箇所が残るが、relay v2 のドキュメント
  群（本書を含む）では英語の **stream** に統一する。「場」は §5 で旧表記として整理する。

**関連用語**: publisher / subscriber / membership / outbox / labels

**出典**: 機能要件文書、概論、ワイヤ / API 仕様

---

### publisher

**一行定義**: stream または subscription レーンにメッセージを流す主体。

**詳細**:

- stream に投函する場合は `POST /streams/{stream_id}/messages` を、subscription レーンに
  流す場合は `POST /publish` を呼ぶ。
- 認証済み identity であれば publisher になれる（publish は authN のみで通る）。
- 種別（cc-memory のようなエンティティ管理側、agent、外部 bot、CLI など）を relay は
  限定しない。汎用バス原則の帰結である。
- publisher は idempotency_key を生成して指定することが推奨される（同一 publisher
  からの再送を 15 分窓で dedup できる）。省略時は relay 側で擬似キーを補完する。

**関連用語**: subscriber / publish / publish_id / idempotency_key

**出典**: 機能要件文書、概論、ワイヤ / API 仕様

---

### subscriber

**一行定義**: stream または subscription を経由してメッセージを受け取る主体。

**詳細**:

- subscribe API を呼んで subscription を作り、その後 SSE 接続を張って push を受ける。
- stream のメンバーとして配達を受ける場合は、subscribe を呼ばなくても read 権限を持つ
  membership があれば配達対象になる（stream のメンバーは自動 subscribe されないが、
  メンバー宛 push は届く）。
- 種別（ow エージェント、一般 claude セッション、外部 UI、外部 bot など）を relay は
  区別しない。subscriber 種別中立性と呼ぶ。
- subscriber identity の経時的同一性は relay の責務外である。再接続した subscriber は
  relay にとって新規 subscriber として扱われる（subscriber 履歴中立性、後述）。

**関連用語**: subscription / subscription_id / SSE / 履歴中立性

**出典**: 機能要件文書、概論、ワイヤ / API 仕様

---

### subscription

**一行定義**: subscriber が宣言する「どの labels マッチ条件でメッセージを受け取るか」
の購読定義。

**詳細**:

- 各 subscription は `(subscriber identity, labels セット, relay 採番の subscription_id)`
  で識別する。
- labels マッチング条件は「subscribe.labels が publish.labels の subset であればマッチ」
  という AND セマンティクスで定義される（subset 判定）。
- 同一の `(subscriber, labels)` でも複数の subscription を独立に持てる（独立 lease）。
- labels セットを変更したい場合は、新しい subscription を作って古い subscription を
  unsubscribe する（途中 mutate する経路はない）。
- subscribe API は authZ 対象外で、認証済み identity であれば任意の labels セットで
  subscribe できる。漏れてはならない情報がある場合は publisher 側 publish gate で
  担保する。

**関連用語**: subscription_id / labels / lease / SSE / outbox

**出典**: 機能要件文書、ワイヤ / API 仕様、identity / authZ 仕様

---

### subscription_id

**一行定義**: subscription に発行される一意な ID（UUID）。relay 側で採番する。

**詳細**:

- subscriber が持参して継続する経路を持たない。再接続時は新規 subscribe を呼んで
  新しい subscription_id を得る。
- relay 再起動で in-memory な subscription registry が消失するため、再起動後に旧
  subscription_id を持参しても `404 Not Found` が返る（relay は「かつて存在した」事実を
  持たないため `410` を返せない。`410 Gone` は lease 切れ済み subscription が registry に
  残っている間だけ返る best-effort のヒントで、subscriber はどちらも re-subscribe の
  シグナルとして扱う）。
- 旧 subscription_id 宛に残った outbox エントリは「subscription_id 不存在」を理由に
  permanent error 経路で DLQ に倒され、時間経過で消滅する。
- subscription_id の経時同一性管理（再起動前後で「同じ subscriber」とみなすか）は
  relay の責務外で、ow / publisher 側ポリシーで担保する。subscriber 履歴中立性の中核
  である。

**関連用語**: subscription / 履歴中立性 / DLQ / lease

**出典**: 機能要件文書、ワイヤ / API 仕様

---

### membership

**一行定義**: stream ごとに保持される「誰が write 権限（投函）/ read 権限（受信）を
持つか」の集合。

**詳細**:

- relay は stream ごとに `(identity, access)` の集合を持ち、`access` は
  `read` / `write` / `read_write` のいずれかを取る。write = 投函権、read = 受信権で、
  場レーンの配達先は read 権限を持つ member 集合で決まる。
- field 名は `role` ではなく `access` とする（role 概念を relay の状態モデルに乗せない）。
- これは structural authZ の判定材料で、relay が直接照合する。
- 「誰がこの stream を close してよいか」「誰が cancel を投げてよいか」のような操作
  権限は relay は判定しない（semantic authZ は relay 外、後述の cc-memory MCP handler
  同期ゲートに寄せる）。
- stream の membership と subscription は独立で、メンバーは自動 subscribe されないし、
  subscription だけで stream のメンバーになることもない。
- stream 作成者は作成時に write 権限を持つ member として自動登録される（bootstrap）。

**関連用語**: stream / authZ 境界 / structural authZ

**出典**: 機能要件文書、ワイヤ / API 仕様

---

### outbox

**一行定義**: subscription ごと（および stream メンバーごと）に保持される、未 ack
メッセージの永続キュー。

**詳細**:

- relay が disk に永続化する唯一のデータが outbox である。subscription registry、
  lease、SSE 接続状態、stream membership などはすべて in-memory に置き、relay 再起動
  時には消える設計を取る。
- outbox エントリのキーは `(subscription_id, publish_id)`（または stream メンバー宛の
  場合は配達ターゲットごと）で、relay 全体で global に単調な publish_id によって順序が
  決まる。
- publish 受領時に subscription マッチングと outbox INSERT を単一 transaction で行う
  ことで、202 Accepted が返った時点で「publish は永続化済み」が保証される
  （transactional outbox パターン）。
- ack 受領時に対応エントリを削除する。
- 配達できない状態が一定期間続くと DLQ 経路に倒し、polling 対象から外す。

**関連用語**: at-least-once / publish_id / ack / DLQ / transactional outbox

**出典**: 機能要件文書、ワイヤ / API 仕様、シーケンス図集

---

### DLQ

**一行定義**: dead letter queue。permanent error 状態の outbox エントリを退避する場所。

**詳細**:

- outbox エントリが以下のいずれかに該当すると `dead_at` をセットされ、polling 対象から
  外れる。
  - push retry が累積上限（3.1 秒、初回 100ms × 係数 2 × 5 回）に達し、retain 期間内に
    再接続なし
  - retain 期間（subscription レーンは default 24 時間、stream レーンは stream の
    `default_ttl`）超過
  - permanent error（subscriber identity 削除済み、subscription_id 不存在、lease 切れ、
    relay 再起動による in-memory registry 消失など、意図しない delivery target の消滅）
- 明示 unsubscribe は DLQ を通らない。未 ack エントリは unsubscribe と同一 transaction で
  即時削除される（明示的な関心放棄は事故ではないため、DLQ と warn ログは意図しない消滅の
  観測専用に保つ）。
- dead 化時には warn 構造化ログを 1 件出力する。
- dead エントリは `dead_at` から 7 日後に物理 DELETE される（運用観察期間）。
- `/status` で件数が、`/metrics` で `relay_outbox_dead_total` カウンタが exposure される。

**関連用語**: outbox / retain / 履歴中立性 / publisher 直接 pull

**出典**: 機能要件文書、ワイヤ / API 仕様、シーケンス図集

---

### labels

**一行定義**: publisher がメッセージに付与し、subscriber が subscription 作成時に絞り
条件として宣言する key-value セット。relay にとっては不透明である。

**詳細**:

- publish 時は publisher が任意の文字列集合を付与し、subscribe 時は subscriber が
  任意の文字列集合を関心条件として宣言する。
- relay はラベルの意味を解釈しない。subset 判定だけを行い、subscribe.labels が
  publish.labels の部分集合であればマッチとみなす（AND セマンティクス）。
- ラベルの命名規約（`<ns>:<value>` 形式、`entity:` / `topic:` / `activity:` / `event:`
  などの予約 namespace）や意味づけは、relay を使う側のエージェントが決める。
- 空配列 `[]` は subscribe 時に `400 Bad Request` で拒否される（firehose 防止）。
- AND / OR / NOT の複雑な組み合わせは relay 側で持たず、複数 subscription に分解して
  表現する。
- 「K8s 由来の labels」と同質の語彙で、key-value 集合に対する subset 判定で配達先を
  絞る発想を継承している。

**関連用語**: subscription / publish / マッチング / namespace 規約

**出典**: 機能要件文書、概論、ワイヤ / API 仕様

---

### publish_id

**一行定義**: publish ごとに付与される一意通番。relay 全体で global に単調増加する。

**詳細**:

- 1 つの publish_id は次の 3 役を兼ねる。
  - **ack の cumulative カーソル**: `POST /subscriptions/{id}/ack { up_to_publish_id: N }`
    （場レーンは `POST /streams/{id}/ack`）の N がそのまま publish_id である。
  - **outbox エントリのキーの一部**: `(subscription_id, publish_id)` の組で outbox
    エントリを一意に識別する。
  - **SSE event の `id:` 行**: subscriber 側の重複検知や、subscription 間での発生順
    比較に使う。
- relay 全体で global に単調なため、subscription をまたいだ発生順の比較が可能である。
- stream ごと・subscription ごとの個別 seq（旧 `stream_seq` / `subscription_seq`）は
  v2 では持たない（§5 で旧表記として整理）。
- 表記は **publish_id で統一**する。設計議論の中間版に現れる `publish_seq` / `seq` /
  `publish_sequence` などの表記は v2 ドキュメント群では使わない。

**関連用語**: ack / cumulative ack / outbox / SSE

**出典**: 機能要件文書、ワイヤ / API 仕様、シーケンス図集

---

### lease

**一行定義**: subscription の有効期間。subscriber が renew しない間に失効する。

**詳細**:

- subscribe 時に `lease_ttl` を秒で指定する（default 300 秒、min 30 秒、max 86400 秒）。
- subscriber は `PUT /subscriptions/{id}/lease` で renew する。期限切れの subscription
  に対する renew は `410 Gone`（registry 消失後は `404 Not Found`）が返り、subscriber は
  新規 subscribe を呼ぶ。
- lease と retain は独立した軸で、大小制約は置かない（`retain_seconds > lease_ttl` は正当。
  短い lease を renew し続ける長寿命 subscriber が retain=24h の再送猶予を持つのが標準の姿）。
  lease が renew されず切れると、retain の残りに関係なく当該 subscription の未 ack エントリ
  は DLQ 経路に倒される。実効 replay 窓は min(retain, subscription が生存した期間)。
- lease / subscription registry は in-memory に保持され、relay 再起動で消失する。
  再起動後は re-subscribe + heartbeat による自己修復に依存する設計である。
- lease TTL の min / max は subscriber 種別に依存させず固定する（subscriber 種別中立性）。

**関連用語**: subscription_id / retain / 履歴中立性

**出典**: 機能要件文書、ワイヤ / API 仕様

---

## §2 ack / 配達セマンティクス

### ack

**一行定義**: subscriber が relay に「ここまで受け取り済」を伝える application-level の
確認応答。

**詳細**:

- subscription レーンは `POST /subscriptions/{id}/ack { up_to_publish_id: N }` で送る。
  stream レーンは `POST /streams/{id}/ack { up_to_publish_id: N }` で送り、対象は
  「場 × 呼び出し identity」宛のエントリに解決される。
- relay は当該 delivery target の outbox から `publish_id <= N` のエントリを一括削除
  する（cumulative ack、後述）。
- **TCP write 完了は ack ではない**。relay が SSE で push し終わっても outbox エントリは
  消えず、subscriber が application-level ack を返したときに初めて消える。これは
  subscriber プロセスが SSE 受信後に crash する構造的穴を防ぐためである。
- 同じ `(subscription_id, up_to_publish_id)` を 2 回送っても `200 OK` で冪等。
- 非所有・不存在の subscription_id への ack は `404 Not Found` が返る（存在露呈回避。
  所有者本人の lease 切れ subscription が registry に残っている間のみ `410 Gone`）。

**関連用語**: cumulative ack / at-least-once / outbox / 暗黙再 push

**出典**: 機能要件文書、ワイヤ / API 仕様、シーケンス図集

---

### cumulative ack

**一行定義**: `up_to_publish_id` までの全 publish を 1 つの ack でまとめて確認する方式。

**詳細**:

- 個別の publish_id に対する per-message ack ではなく、累積カーソルで一気に消す。
- subscriber 側の ack コストが O(1) になり、push 頻度が高くても ack 往復が線形に
  増えない。
- subscriber は受信順に都度 ack してもよいし、batch 処理してから最大の publish_id を
  1 回だけ ack してもよい。処理単位の取り方は subscriber 側のポリシーである。
- 同じ `up_to_publish_id` の 2 回送出は冪等で、relay は何も削除せずに `200 OK` を返す。

**関連用語**: ack / publish_id / outbox

**出典**: 機能要件文書、ワイヤ / API 仕様、シーケンス図集

---

### at-least-once

**一行定義**: relay の配達保証。同じメッセージが同じ subscriber に複数回届く可能性が
あることを subscriber が受け入れる代わりに、消失しないことを保証する。

**詳細**:

- relay は subscriber が ack するまで再 push し続ける（outbox + retry + 暗黙再 push）。
- subscriber は重複受信を冪等性で吸収する責務を持つ。重複排除キーは
  `(subscription_id, publish_id)` の組を使う。
- 同一 publish が同一 subscription に複数回送られる可能性は、SSE 切断とその後の再 push、
  push retry、ack 前 crash などで発生する。
- at-most-once や exactly-once の保証は relay は提供しない。
- transactional outbox（publish 受領と outbox INSERT を 1 transaction で実行）+
  polling dispatcher + idempotent consumer の 3 点セットが配達保証の中核である。

**関連用語**: outbox / ack / 暗黙再 push / transactional outbox

**出典**: 機能要件文書、概論、シーケンス図集

---

### 暗黙再 push

**一行定義**: SSE 再接続時の resume 動作。relay が subscriber の outbox にある未 ack
分を黙って再 push する。

**詳細**:

- subscriber は再接続時に何のカーソルも relay に申告しない。relay 側の per-subscription
  ack 状態がそのままカーソルになる。
- 旧設計で議論された `Last-Event-ID` ヘッダや `GET /history?since=N` のような申告型
  resume は v2 では使わない（後述の旧表記）。`Last-Event-ID` を送っても relay は無視する。
- 設計上の到達点は、subscriber 側のギャップ検知や seq 申告ロジックを構造的に不要に
  したことにある。relay が「outbox に残っているものを順に流し直す」だけで取りこぼしが
  解消する。
- retain 期間（default 24 時間）を超えた取りこぼしは relay の責務外で、subscriber は
  publisher 側の永続真実ストアに直接 pull して補完する。

**関連用語**: ack / outbox / retain / publisher 直接 pull

**出典**: 機能要件文書、概論、ワイヤ / API 仕様、シーケンス図集

---

### retain

**一行定義**: subscriber が SSE を切断している間、outbox エントリを保持しておく期間。

**詳細**:

- subscription レーンは default 24 時間。subscribe 時に `delivery_options.retain_seconds`
  で override 可能（min 60 / max 86400）。lease_ttl とは独立した軸で、大小制約はない。
- stream レーンは stream の `default_ttl` を使う。
- retain 期間を超えた未 ack エントリは DLQ 経路に倒され、7 日後に物理 DELETE される。
- 「relay は短期の便利な再送装置、長期の真実源は publisher」という責務境界を明示する
  パラメータである。

**関連用語**: outbox / DLQ / publisher 直接 pull

**出典**: 機能要件文書、ワイヤ / API 仕様

---

## §3 identity / authZ

### identity

**一行定義**: エージェントを一意に決める抽象概念。relay v2 では AgentCard + 鍵ペアで
表現する。

**詳細**:

- 「誰が publish したか」「誰が subscribe したか」を判定するための基本単位である。
- relay v2 では A2A 1.0 spec の AgentCard を identity の公開記述子として採用し、認証
  方式は SecurityScheme で宣言される。
- identity の真正性検証は relay が責任を持つ（authN）。「特定 identity が特定操作を
  してよいか」の意味判定は relay 外（cc-memory MCP handler）に寄せる（semantic authZ）。
- subscriber identity の経時的同一性は relay の責務外で、ow / publisher 側ポリシーで
  担保する。

**関連用語**: AgentCard / authZ 境界 / 履歴中立性

**出典**: identity / authZ 仕様、機能要件文書

---

### AgentCard

**一行定義**: A2A 1.0 spec で定義された、エージェントの記述子 JSON 文書。

**詳細**:

- `/.well-known/agent-card.json` で `application/a2a+json` として公開する。
- 内容は `name` / `version` / `supportedInterfaces` / `capabilities` /
  `securitySchemes` / `security` / `provider` / `documentationUrl` / `signatures` などを
  含む。
- 公開版 AgentCard には機密でない最小情報を載せ、認証済み client 向けの追加情報は
  extended AgentCard で返す（A2A 1.0 spec の `GetExtendedAgentCard`）。
- relay は「自分自身が 1 つの A2A agent」として AgentCard を持つ。relay 経由で接続する
  worker / orch / cc-memory ごとの skill 詳細は各 agent 側の AgentCard で表明され、
  relay 自身は中継であって skill 提供主体ではない。
- AgentCard 取得自体には認証を要求しない。

**関連用語**: identity / SecurityScheme / JWS / JCS

**出典**: identity / authZ 仕様

---

### JWS

**一行定義**: JSON Web Signature（RFC 7515）。AgentCard 署名検証のフォーマット。

**詳細**:

- A2A 1.0 spec §8.4 は AgentCard の JWS 署名を **MAY**（推奨だが必須でない）と規定する。
- relay v2 は初期実装で「署名なし AgentCard を公開する最小セット」を満たし、JWS 署名は
  将来段階で導入する MAY 機能と位置づける。
- 採用する場合は ES256 を基本とし、公開鍵は `/.well-known/jwks.json` に JWKS として配置
  する。`kid` と `jku` を JWS protected header に含める。
- 署名対象は AgentCard 全体から `signatures` フィールドを除外したもの。署名検証手順は
  A2A 1.0 spec §8.4.3 の MUST 手順に従う。
- 鍵ローテーション時は旧 `kid` を JWKS に一定期間残し、ローテ前後で AgentCard の検証を
  継続できる状態を保つ。

**関連用語**: AgentCard / JCS / JWKS / SecurityScheme

**出典**: identity / authZ 仕様

---

### JCS

**一行定義**: JSON Canonicalization Scheme（RFC 8785）。JWS 署名前の JSON 正規化手順。

**詳細**:

- A2A 1.0 spec §8.4.1 は「AgentCard を署名する場合、署名前に JCS で正規化する」ことを
  **MUST** と規定する。JWS を採用するなら JCS は必須である。
- 正規化対象は `signatures` フィールドを除外した AgentCard で、protobuf 由来の default
  値プロパティ（空文字列、false、空配列など）も正規化前に削る。
- 検証側（AgentCard を受け取る側）も同じ JCS で正規化してから署名照合する。
- 実装は `rfc8785` 系の Python パッケージを使うのが基本で、署名 / 検証は `pyjwt` 等と
  組み合わせる。

**関連用語**: JWS / AgentCard / SecurityScheme

**出典**: identity / authZ 仕様

---

### SecurityScheme

**一行定義**: A2A 1.0 spec で定義された認証方式の宣言。AgentCard 内で表明する。

**詳細**:

- A2A 1.0 spec §4.5 は SecurityScheme を OpenAPI 3.2.0 基盤の 5 type（`apiKey` /
  `http` / `oauth2` / `openIdConnect` / `mtls`）で規定する。
- relay v2 の最小セットは `HTTPAuthSecurityScheme { scheme: "bearer" }` を 1 個宣言する
  構成で、Bearer token をすべての incoming request で検証する。
- フル準拠セットでは上記に加えて `OpenIdConnectSecurityScheme` または
  `OAuth2SecurityScheme` を追加する。
- AgentCard の `securitySchemes` の各エントリは discriminated-union の wrapper-key 形を
  取り、OpenAPI の flat 形（`{"type": "http", "scheme": "bearer"}`）ではない点に注意する。

**関連用語**: AgentCard / authZ 境界 / Bearer

**出典**: identity / authZ 仕様

---

### authZ 境界

**一行定義**: relay 側 authZ は structural authZ（構造判定）に限定し、semantic authZ
（意味判定）は relay の外（cc-memory MCP handler 同期ゲート）に置く方針。

**詳細**:

- read 系 endpoint は 2 分類。instance-global（`GET /status` / `GET /metrics` / AgentCard 等）
  は authN のみで通す。特定 resource を名指しする参照（`GET /streams/{id}` のメタ取得 /
  `GET /streams/{id}/members` の member 一覧、`GET /events` の `subscription_ids=` 参照）には
  membership / ownership の構造判定（structural authZ の一部）が掛かり、非当事者には不在 id と
  同一の `404` を返して存在を露呈しない。「特定 entity の閲覧禁止」のような message body の
  内容に基づく細粒度 read filter は relay は持たない。必要なら publisher 側で公開 / 非公開を
  分けて publish する。
- publish は authN のみで通す。「この labels への publish は禁止」のような細粒度 filter は
  relay は持たない。ただし stream への投函は write 権限の membership を要求する
  （structural authZ の一部）。
- subscribe は authZ 対象外で、認証済み identity であれば任意の labels セットで subscribe
  できる。漏れてはならない情報は subscribe filter ではなく publish gate で担保する。
- relay は command（close / cancel / spawn 等の ow 命令語彙）を認識しない。
  「identity → 許可 command 集合」の authZ table も持たない（それは事実上の role 定義で
  あり、role 概念を relay に持ち込まないという責務境界に反する）。`DELETE /streams/{id}`
  などの状態変更系は write 権限の membership / subscriber 当事者性という構造的事実の照合
  だけで通す。拒否は 2 段階で、完全非メンバー / 非所有者には不在 id と同一の `404`（存在
  秘匿）、権限不足の member には `403` を返す。
- 意味判定（「この identity がこの場の close を呼んでよいか」「この spawn を許可すべき
  状況か」など）は relay の外で、かつ操作の実行と同じプロセス内で同期に行う
  （cc-memory MCP handler 同期ゲート）。これにより「relay が close を受理したが ow 側で
  禁止判定が後追いで出る」のような race を構造的に消す。

**関連用語**: authN / structural authZ / semantic authZ / membership

**出典**: identity / authZ 仕様、機能要件文書

---

### authN

**一行定義**: authentication。identity の真正性検証。

**詳細**:

- relay v2 では Bearer token 検証や JWS 検証によって行う。
- 全 HTTP request は AgentCard で宣言済みの SecurityScheme で認証される
  （A2A 1.0 spec §7.4 の MUST）。
- 認可エラー時は「リソース存在を露呈しない」原則に従い、`404` / `403` を使い分ける
  （A2A 1.0 spec §7.5）。

**関連用語**: authZ 境界 / AgentCard / SecurityScheme

**出典**: identity / authZ 仕様

---

### structural authZ（構造判定）

**一行定義**: relay 自身が resource 管理の過程で機械的に記録した構造的事実（stream の
membership、subscription の subscriber identity）との照合のみで決まる可否判定。relay が
担当する。

**詳細**:

- 具体的には、stream への投函 / close / membership 変更に対する write 権限の membership
  照合と、subscription を名指しする操作（unsubscribe / lease renew / SSE 受信 / ack）に
  対する subscriber 当事者性（ownership）照合を指す。
- message body の解釈、ow の状態、操作の意味（それが task の cancel なのか worker の
  spawn なのか）を判定材料にした時点で semantic authZ であり、relay の外に置く。
- 「identity → 許可操作集合」のような authZ table は持たない。
- 機能要件文書の「coarse-grained authZ」に対応する（旧称。§5 参照）。

**関連用語**: authZ 境界 / membership / semantic authZ

**出典**: identity / authZ 仕様

---

### semantic authZ（意味判定）

**一行定義**: 「この identity がこの特定 stream / 特定 subscription / 特定 entity に
対する操作をしてよいか」を ow の状態・ポリシーに照らして行う判定。relay の外で行う。

**詳細**:

- 判定は cc-memory MCP handler 内で **同期判定 → 状態変更 → relay への publish** を 1
  つの transaction として扱う（cc-memory MCP handler 同期ゲート）。
- 非同期構造（「relay 経由で操作を投げてから後で別経路で authZ 結果を反映する」）
  は race condition を呼び込むため採らない。
- 結果として relay 側は構造判定に留まり、relay が close / cancel / spawn の
  意味判定をしないという責務境界が成立する。
- 機能要件文書の「fine-grained authZ」に対応する（旧称。§5 参照）。

**関連用語**: authZ 境界 / structural authZ / cc-memory MCP handler 同期ゲート

**出典**: identity / authZ 仕様

---

### DID

**一行定義**: Decentralized Identifier（W3C DID Core 仕様）。relay v2 では **スコープ
外**として扱う。

**詳細**:

- A2A 1.0 spec 本体（spec docs / proto）に DID に関する記述は一切ない。1.0 announcing
  post も identity 基盤として JWS + JWKS（Web PKI / 既存鍵管理）を前提に書かれている。
- relay v2 は DID を扱わず、AgentCard + JWS + JWKS（jku）で identity を閉じる。
- 将来 `did:web` を採用する場合でも、AgentCard 側を変えずに `jku` を DID document の
  `verificationMethod` URL に向けるだけで接続できる拡張余地は残る。
- `did:key` / `did:peer` などの静的鍵経路は JWKS と用途が直接競合するため、relay 用途では
  現時点では採用しない。
- A2A spec が将来 DID を採用したとき、または多テナント間の cross-org identity が要件化
  したときに別文書で議論する。

**関連用語**: AgentCard / JWS / JWKS

**出典**: identity / authZ 仕様

---

## §4 transport / 内部機構

### transport buffer

**一行定義**: relay の自己定義。配達中継に専念し、永続真実は持たないという思想の表明。

**詳細**:

- relay は「メッセージを運ぶ場所」であって、「メッセージを溜めておく場所」ではない。
- 過去のメッセージや entity の最新状態を後から読み返したい場合は、relay ではなく
  publisher 側の永続真実ストア（cc-memory など）に直接 pull する。
- relay 自身が永続化するのは未配達の outbox だけで、配達済み・retain 期間切れの
  メッセージは relay 内に残らない。これは設計の制約ではなく **思想の明示** である。
- relay にメッセージ履歴 API を生やすと、関心領域（誰が真実源か、誰がスキーマを進化
  させるか）が relay と publisher の両方に発生し、責務境界がぼやける。v2 はこれを構造的
  に防ぐ。

**関連用語**: mechanism-policy 分離 / outbox / publisher 直接 pull

**出典**: 概論、責務境界 decision

---

### mechanism-policy 分離

**一行定義**: relay の中核思想。relay は配達メカニズムだけを提供し、ポリシーは使用側
エージェントの責務として完全に分離する。

**詳細**:

- relay が持つもの（メカニズム）: 配達経路（outbox / push / retry / retain）、配達の
  at-least-once 保証、送信者の真正性検証（authN）、structural authZ。
- relay が持たないもの（ポリシー）: メッセージ本文の意味解釈、ロール概念
  （orch / dispatcher / worker などの認識）、semantic authZ、関心領域の述語評価
  （複雑な AND / OR / NOT の合成）、subscriber identity の経時的同一性管理。
- ポリシーは時とともに揺れる（誰がどの操作をしてよいか、述語の意味、ロールの粒度）が、
  メカニズムは比較的安定する（配達は配達であって、再送と ack で閉じる）。
- これにより relay は「ow 専用 bus」ではなく、別のエージェント体系でも再利用できる
  汎用バスとして成立する。

**関連用語**: transport buffer / authZ 境界 / 履歴中立性

**出典**: 概論、責務境界 decision

---

### 履歴中立性

**一行定義**: relay は subscriber を「現在の subscription_id」としてのみ識別し、
それ以前 / 以外の同一性を扱わないという原則。

**詳細**:

- subscriber 履歴中立性とも呼ぶ。
- 再接続 / lease 切れ / relay 再起動などで subscription_id が無効化された subscriber が
  再 subscribe した場合、relay にとっては完全な新規 subscriber として処理される。
- subscription_id を持参して「同じ subscription を継続したい」と要求する経路は
  存在しない（subscribe API は subscription_id 引数を受け付けない）。
- 旧 subscription_id 宛 outbox エントリは「subscription_id 不存在」を理由に DLQ 行きと
  なり時間で消滅する。
- 再 subscribe 後の labels セット復元、取りこぼし回収、再構築起動契機などはすべて ow 側
  / publisher 側の責務である。
- 結果として relay 実装は subscribe / unsubscribe / renew / ack の素直な CRUD 的
  lifecycle で閉じ、subscriber identity ledger を持たない。

**関連用語**: subscription_id / DLQ / publisher 直接 pull

**出典**: 機能要件文書、概論、シーケンス図集

---

### transactional outbox

**一行定義**: publish 受領と outbox INSERT を単一 transaction で実行し、at-least-once の
起点を構造的に保証するパターン。

**詳細**:

- publisher の `POST /publish` を受領した relay は、subscription マッチングと各
  subscription への outbox エントリ作成を単一 transaction で済ませてから `202 Accepted`
  を返す。
- 202 が返った時点で「publish は relay の outbox に永続化済み」が保証されるため、以降
  relay が再起動しても当該 publish は失われない。
- relay が disk で守るのは outbox のみで、subscription registry / lease / presence /
  stream membership は in-memory に置く設計と整合する。
- publisher 側 SDK（`relay_sdk.outbox`）も同パターンを採用し、業務 write と outbox INSERT
  を同一 SQLite transaction に乗せる。dispatcher が outbox を polling して relay の
  `POST /publish` を呼ぶ。

**関連用語**: outbox / at-least-once / polling dispatcher

**出典**: 機能要件文書、ワイヤ / API 仕様、SDK 仕様、シーケンス図集

---

### polling dispatcher

**一行定義**: outbox を polling して relay へ配達する常駐 daemon。

**詳細**:

- relay 内部の dispatcher は outbox を 100ms 〜 1 秒間隔で polling し、未送エントリを
  SELECT → push → `processed_at` UPDATE の単一ループで処理する。
- 単一プロセス内シングルトン（二重 push 防止、ファイル lock で enforce）。
- push 失敗時は指数バックオフ retry（初回 100ms、係数 2、最大 5 回、累積上限 3.1 秒）。
  超過後は outbox に戻して再接続を待つ。
- publisher 側 SDK にも同名の dispatcher 概念があり、`python -m relay_sdk.outbox` で
  常駐させて business app の outbox を relay へ配達する。

**関連用語**: outbox / transactional outbox / DLQ

**出典**: 機能要件文書、ワイヤ / API 仕様、SDK 仕様

---

### publisher 直接 pull

**一行定義**: retain 期間を超えた取りこぼしを、relay ではなく publisher 側の永続真実
ストアに直接 pull で取りに行く fallback 経路。

**詳細**:

- relay は ≤ retain（default 24h）の便利再送までを保証し、それ以前は publisher
  （cc-memory 等）が source of truth であるという責務分担を明示する。
- subscriber は relay outbox から再 push される範囲を超えて offline だった場合、
  publisher の light pull endpoint（cc-memory なら `search` / `get_map` 拡張）に
  直接当たって取りこぼしを補完する。
- 「relay が `ここから先は届かない` を明示的に通知する経路は持たない」設計のため、
  subscriber 側は定期 full reconciliation で publisher に当たる責務を負う
  （SDK に薄い `reconcile()` ヘルパを用意する）。

**関連用語**: retain / 履歴中立性 / DLQ / transport buffer

**出典**: 機能要件文書、概論、SDK 仕様

---

### SSE

**一行定義**: Server-Sent Events。relay が subscriber に push を流すための HTTP-based
single-direction streaming protocol。

**詳細**:

- subscriber は `GET /events?subscription_ids=<id1>,<id2>,...` で SSE 接続を開く。
- 1 つの SSE 接続に複数の subscription_id を多重化できる。同時に、接続した identity が
  read 権限を持つ member である stream のメッセージも同じ接続に流れる（payload 内の
  `delivery_target` で判別する）。
- SSE event の `id:` 行には `publish_id` が乗る。relay は subscriber 側の
  `Last-Event-ID` ヘッダを resume には使わない（ack カーソルが真実源）。
- 30 秒ごとに `: keepalive` コメント行を送って、proxy / load balancer の close を防ぐ。
- 多重化時の merge 順序は subscription バースト（1 subscription_id ぶんを全部 → 次の
  subscription_id ぶんを全部）で、cross-subscription 順序保証は relay 要件外。

**関連用語**: subscription / publish_id / keepalive

**出典**: 機能要件文書、ワイヤ / API 仕様、シーケンス図集

---

### dispatcher（publisher 側）

**一行定義**: publisher プロセスで常駐し、業務アプリの outbox を relay へ配達する daemon。

**詳細**:

- `relay_sdk.outbox.run_dispatcher(...)` または `python -m relay_sdk.outbox` で起動する。
- 業務アプリは `publish(conn, ref_type=..., ref_id=..., labels=..., title=...)` を業務
  write と同じ SQLite transaction で呼ぶだけでよく、relay への配達は dispatcher が
  担当する。
- ループ内挙動は relay 内部 dispatcher と同様で、未送エントリの SELECT → `POST /publish`
  → `processed_at` UPDATE の繰り返し。指数バックオフ retry と DLQ GC（7 日後物理 DELETE）
  を持つ。
- 同一 DB に対して複数の dispatcher が走ると二重 publish を起こすため、SQLite ファイル
  lock と lockfile で enforce する。

**関連用語**: transactional outbox / polling dispatcher / outbox

**出典**: SDK 仕様

---

### relay 再起動と自己修復

**一行定義**: relay が disk で守るのは outbox のみで、subscription registry / lease /
presence / stream membership は in-memory に置く設計の帰結。

**詳細**:

- relay 再起動で in-memory state は消失する。subscriber は再接続時に新規 subscribe を
  呼び、relay は新しい subscription_id を発行する（旧 ID を持参して継続する経路はない）。
- 旧 subscription_id 宛 outbox エントリは「subscription_id 不存在」を理由に DLQ 行きと
  なり、7 日後に物理 DELETE される。
- AgentCard / JWKS / Bearer token の検証情報は外部由来（外部 IdP または relay の
  disk-persisted 設定）で、in-memory state とは独立に保持される。再起動後も同じ
  AgentCard を返し、同じ JWKS で署名検証が成立する。
- 再起動を跨ぐ取りこぼしの回収は publisher 直接 pull が担当する。

**関連用語**: 履歴中立性 / outbox / DLQ / publisher 直接 pull

**出典**: 機能要件文書、identity / authZ 仕様、シーケンス図集

---

## §5 廃止語 / 旧表記

設計議論の経緯や v1 / v2 中間版で使われていたが、v2 ドキュメント群では使わない表記の
対応表を以下に示す。本書を含む v2 ドキュメント群はすべて右側の用語に統一する。

| 旧表記 | v2 統一表記 | 備考 |
|---|---|---|
| 場 | stream | 機能要件文書本文には日本語の「場」が残るが、ドキュメント群では英語 stream を使う |
| publish_seq / seq / publish_sequence | publish_id | 名称改称。意味は同じ（relay 全体で global に単調な ID） |
| stream_seq | （廃止） | stream 内 seq は v2 では存在しない。stream 内順序は publish_id 昇順で代替 |
| subscription_seq | （廃止） | subscription 内 seq は v2 では存在しない。gap 検知は relay 側 per-購読 outbox 管理で構造的に保証 |
| `GET /history?since=N` | （廃止） | stream の永続蓄積廃止に伴い endpoint そのものを削除 |
| `GET /streams/{id}/history` | （廃止） | 同上 |
| Last-Event-ID による resume | 暗黙再 push | relay は `Last-Event-ID` を受け取っても無視する。subscriber は再接続するだけでよい |
| `initial_replay` parameter | （廃止） | subscribe API から削除。取りこぼしは未 ack outbox 再 push と publisher 直接 pull で回収 |
| `replay_unavailable` event | （廃止） | replay 不可状態の通知経路は v2 では存在しない |
| `gc_marker` 通知 | （廃止） | 同上 |
| archive / archive_ttl（stream 単位） | （廃止） | stream の永続蓄積廃止に伴い archive 概念が消滅 |
| fire-and-forget | at-least-once + cumulative ack | v1 の配達保証の表記。v2 では outbox + retry + ack で確定的に保証する |
| TCP write 完了 = 配達完了 | application-level ack | TCP write 完了は ack ではない |
| `role`（membership の field / writer / reader / both） | `access`（read / write / read_write） | role 概念を relay の状態モデルに乗せない。write = 投函権、read = 受信権 |
| coarse-grained authZ | structural authZ | 構造判定。relay が担当（membership / ownership の照合） |
| fine-grained authZ | semantic authZ | 意味判定。relay 外（cc-memory MCP handler 同期ゲート） |
| command（relay の語彙として） | （廃止） | relay は close / cancel / spawn 等の命令語彙を認識しない。relay 上では不透明 body の stream 投函として流れるだけ |
| `POST /events/ack`（per-message バッチ ack） | `POST /subscriptions/{id}/ack` / `POST /streams/{id}/ack` | cumulative ack（`up_to_publish_id`）にレーン別 endpoint で一本化 |

---

## §6 関連ドキュメント

本書を辞書として参照する側のドキュメント群を以下に示す。

| ドキュメント | 役割 | ファイル |
|---|---|---|
| 概論 | relay v2 の最上位コンセプト。初めて relay v2 に触れる人が「relay とは何か」を理解する起点 | `relay-concept.md` |
| ワイヤ / API 仕様 | HTTP endpoint、payload、status code、配達セマンティクスの詳細 | `relay-v2-wire-api.md` |
| identity / authZ 仕様 | A2A 1.0 準拠の認証・認可の詳細 | `relay-v2-identity-authz.md` |
| SDK 仕様 | publisher / subscriber 側 Python SDK の API | `relay-v2-sdk.md` |
| シーケンス図集 | publish / subscribe / ack / retry / DLQ の動作を mermaid で可視化 | `relay-sequences.md` |

上位の設計成果物としては、relay 機能要件文書と、relay v2 と cc-memory の責務境界を
まとめた recompose ドキュメントが存在する。本書はこれらの成果物に登場する用語を集約
したものである。
