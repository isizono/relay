# relay v2 ワイヤ / API 仕様

> **位置づけ**: relay v2 を「実装者が semantics の食い違いなく 1 つの relay を書ける」粒度まで落とした
> インターフェース仕様。上位要件は機能要件 v2（cc-memory M#507）+ R1 改訂（論点#3 決着、
> D#3081-3083）。本書は **R1 を本文に優先**して書かれている。
>
> **スコープ**: HTTP ワイヤプロトコル（endpoint / payload / status code / seq / 配達セマンティクス）。
> 以下は本書のスコープ外:
> - **identity / authZ**（AgentCard・JWS・scope・認証ミドルウェア）→ `relay-v2-identity-authz.md`（A2A 1.0 準拠で重いため独立）
> - **永続層の物理 schema**（table 定義・engine）→ substrate engine 確定（A#1193）待ち。本書は論理モデルまで
> - **Python SDK の API**（`relay_outbox` / `relay-client`）→ `relay-v2-sdk.md`
> - **cc-memory 連携プロトコル** → cc-memory 側にのみ存在（協調プロトコル v1 / M#522）

---

## 0. R1 前提（本書が立脚する確定事項）

論点#3 決着（2026-06-27, D#3081-3083）により、要件 v2 本文から以下が変わっている。本書はこの
改訂後の姿で書かれている。

| 項目 | R1 での扱い |
|---|---|
| **場 (stream) の history 永続蓄積** | **廃止**。場は pure pass-through。relay が永続化するのは未配達 in-flight の **outbox だけ** |
| **`GET /history?since=N`** | **削除**。再生台帳が無いので since=N の引く先が無い |
| **場 close 後の archive + TTL 90日** | **廃止**。場 close は「新規投函を止める」だけ。archive 概念は消える |
| **resume（取りこぼし回収）** | 購読者が Last-Event-ID で replay 申告する経路を廃し、**relay が当該購読の未 ack outbox を再 push** する動作に一本化 |
| **substrate** | disk（SQLite）で守るのは **outbox のみ**。presence / subscription registry / lease / stream membership は in-memory（liveness クラス）。relay 再起動は re-subscribe + heartbeat で自己修復 |
| **サーバーログ** | relay 自身のデバッグ用 append-only sink を追加（§7）。**購読者向け endpoint は持たない**（since=N を生やすと裏口から場 history が復活するため禁止） |

### 0.1 本書で確定した未決項目（R1 が実装計画 T0 送りにした論点）

R1-5 ほかで「未決」とされた IF レベルの論点を、本書作成にあたり以下で確定した（出典: 本リポジトリ
設計セッション 2026-06-27、ユーザー裁定）。

| 論点 | 決定 | 理由 |
|---|---|---|
| **seq 体系** | **`publish_id` 1 系統に畳む**（`stream_seq` / `subscription_seq` は廃止） | 主用途だった history pull(since=N) が消滅。永続台帳が無い以上「歯抜けなし per-stream」を保証できず、中途半端な seq は嘘になる。場内順序は `publish_id` の相対順序で足り、per-target の gap 検知も relay 側 outbox の未 ack 再 push（§6.5）で構造的に代替される |
| **ack 方式** | **cumulative ack API**（subscriber が `up_to_publish_id` を送り、relay が当該 delivery target の `publish_id <= N` を outbox から一括削除） | R1 の「per-購読 ack 状態がカーソルそのもの」を素直に実装。push 成功＝削除の暗黙 ack より配達確証が強い。cumulative なので ack コストは push 頻度に対して O(1) |
| **SSE `id:`** | publish_id を **`id:` 行と payload の両方に載せる** | 重複検知・subscriber 間の発生順比較に使える。実装コストはほぼゼロ。ただし relay の resume は `Last-Event-ID` を見ず、**ack カーソルを真実源**とする |

---

## 1. 核心モデル（IF から見た要約）

relay は 2 つの **publish 源**を、共通の **outbox 配達メカ**で at-least-once 配送する。

| publish 源 | 配達先の決まり方 | endpoint |
|---|---|---|
| **場 (stream)** | 場の **membership**（read 権限を持つ member 集合） | `POST /streams/{id}/messages` |
| **subscription** | **labels の subset マッチ** | `POST /publish` |

- 両系統とも内部 outbox を経由 → SSE で push → subscriber が cumulative ack（§5.6）→ outbox から削除。
- 場 membership と subscription は**独立**（場のメンバーは自動 subscribe されない。逆も同様）。
- subscriber 種別（ow agent / 一般 session / 外部 UI / 外部 bot）を relay は**区別しない**。

### 1.1 配達ターゲット（delivery target）

outbox の 1 エントリは 1 つの **delivery target** 宛。target は次のいずれか:

- `sub:<subscription_id>` — subscription レーンのマッチ結果
- `stream:<stream_id>` × member identity — 場メンバーへの配達

SSE 接続（`GET /events`）は **認証済み identity の単一多重化接続**で、その identity 宛の両系統の
配達を 1 本に流す（§5）。

---

## 2. endpoint 一覧

| メソッド & パス | 役割 | 主返却 |
|---|---|---|
| **場 (stream)** | | |
| `POST /streams` | 場の作成 | `201 { stream_id }` |
| `DELETE /streams/{stream_id}` | 場の close（新規投函停止のみ） | `204` |
| `GET /streams/{stream_id}` | 場のメタ取得 | `200 { stream_id, state, created_at }` |
| `POST /streams/{stream_id}/messages` | 場への投函（場 publish） | `202 { publish_id, matched_members }` |
| `PUT /streams/{stream_id}/members` | membership 付与/更新 | `200` |
| `DELETE /streams/{stream_id}/members?identity=` | membership 削除 | `204` |
| `GET /streams/{stream_id}/members` | membership 一覧 | `200 { members: [...] }` |
| `POST /streams/{stream_id}/ack` | 場レーンの cumulative ack（呼び出し identity 宛） | `200` |
| **subscription** | | |
| `POST /subscriptions` | subscribe（関心宣言） | `201 { subscription_id, lease_expires_at }` |
| `PUT /subscriptions/{subscription_id}/lease` | lease renew | `200 { lease_expires_at }` |
| `DELETE /subscriptions/{subscription_id}` | unsubscribe | `204` |
| `POST /subscriptions/{subscription_id}/ack` | subscription レーンの cumulative ack | `200` |
| `POST /publish` | subscription レーン publish | `202 { publish_id, matched_subscriptions }` |
| **配達 (delivery)** | | |
| `GET /events?subscription_ids=` | SSE 多重化購読 | `text/event-stream` |
| **observability** | | |
| `GET /status` | 運用スナップショット | `200 {...}`（§7.1） |
| `GET /metrics` | Prometheus 互換 | `200`（§7.2） |
| **identity**（詳細は別書） | | |
| `GET /.well-known/agent-card.json` | AgentCard 公開 | `200 application/a2a+json` |

> **削除された旧 endpoint**: `GET /streams/{id}/history`（R1: 場 history 廃止）。
> 旧 relay の `POST /send` → `POST /streams/{id}/messages`、`GET /stream`(SSE) → `GET /events` + 場 membership push、
> `GET /history` → **継承せず**（取りこぼしは未 ack outbox の再 push と、retain 切れ時の publisher 直接 pull で回収）。

---

## 3. 場 (stream) API

### 3.1 `POST /streams` — 場の作成

```
POST /streams
Body: { name: <string>, default_ttl?: <seconds> }
→ 201 Created { stream_id, created_at }   (stream_id = 作成者 identity でスコープ化した canonical id)
→ 400 Bad Request  (name が空 / ':' または '/' を含む / 長さ上限超過)
→ 409 Conflict  (同一作成者の名前空間内で同名 stream 既存)
→ 429 Too Many Requests  (registry 資源上限: 総数 / 作成者 identity あたり。§6.8)
```

- **stream_id は作成者 identity でスコープ化する**。呼び出し側は名前空間内の `name` のみを指定し、
  relay は canonical stream_id = `{作成者 identity}:{name}` を構築して `201` で返す。以後の全操作
  （`GET` / `close` / `messages` / `members` / `ack`）はこの canonical stream_id でアドレスする。
  - 目的: stream_id を global 名前空間にすると、攻撃者が正規利用者の使いそうな stream_id を予測して
    先取り（squatting）し、正規 create を `409` で締め出したり squat した stream の write member として
    居座ったりできる。creator identity を構造的に前置することで、ある identity が別 identity の名前空間で
    stream を作成することが構造上不可能になり、名前空間の横取りが成立しなくなる。
  - `name` は非空文字列で、区切り文字 `:` と URL パス区切り `/` を含んではならない（`400`）。これにより
    canonical 文字列の区切り構造が一意に保たれる。長さには上限を課す（既定 128 文字、設定可能、超過は
    `400`）。canonical stream_id は registry・outbox・delivery target key に埋め込まれるため、他の
    入力フィールド上限（§6.9 の title / labels）と同じ DoS 防御の一部。
  - member は creator から canonical stream_id を out-of-band に知らされる（招待制）ため、member 側で
    canonical id を再構築する機構は要らない。
- `default_ttl` は場メッセージの outbox retain default（省略時は relay 既定、§6.4）。
- registry 資源上限に達している場合は `429`（`ResourceLimitExceededError`）で作成を拒否する（§6.8）。
  同一作成者の名前空間内での既存 stream の再作成（`409`）は新規スロットを消費しないため上限判定より前に
  評価する。異なる identity は同名 `name` でも別 canonical になるため衝突しない。
- 作成者 identity は当該場の write 権限を持つ member（`access: "write"`）として自動登録される
  （bootstrap。これがないと最初の member を追加できる identity が存在しない。identity 別書 §2.2。
  受信も必要なら作成後に自分の access を `read_write` に更新する）。
- **archive_ttl は廃止**（R1: archive 概念なし）。

### 3.2 `POST /streams/{stream_id}/messages` — 場への投函

```
POST /streams/{stream_id}/messages
Body: { body: <bytes | UTF-8 text>, ttl?: <seconds>, idempotency_key?: <string> }
→ 202 Accepted { publish_id, matched_members: <int> }
→ 403 Forbidden   (write 権限のない member による投函)
→ 404 Not Found   (場が不在、または投函者が member でない。露呈回避)
→ 410 Gone        (場が close 済み、新規投函拒否)
→ 429 Too Many Requests { Retry-After }   (publisher ごと rate limit 超過, default 100 req/sec。§5.4 と共通)
```

- 投函者 identity は HTTP 認証で確定（§identity 別書 FR-5）。
- `202 Accepted` は outbox 永続化完了が条件（§6.1）。subscription publish（§5.4）と受理 semantics は
  同一（受理 = relay が忘れない、配達は非同期）。
- `body` は relay にとって不透明（bytes / UTF-8）。
- `idempotency_key` 指定時は同一 stream 内で 15 分 dedup。省略時は relay が擬似キー補完（§6.3）。
- relay は場の **read 権限を持つ member**（`access: "read" | "read_write"`）に対し outbox エントリを
  作成し、`matched_members` を返す。write 権限は投函権、read 権限は受信権であり、配達先は受信権で
  決まる。投函者自身も read 権限を持てば配達対象に含む（自己メッセージの無視は subscriber 側の判断）。
- **stream_seq は付与しない**（廃止）。場メッセージも subscription レーンと同じ `publish_id`
  （グローバル単調）で識別・順序づけされる（§4）。
- 投函には §5.4 の publisher ごと rate limit を掛ける（subscription レーン `POST /publish` と
  同一の token bucket を publisher identity 単位で共有する。超過は `429` + `Retry-After`）。

### 3.3 membership API

```
PUT /streams/{stream_id}/members
Body: { identity: <string>, access: "read" | "write" | "read_write" }
→ 200 OK
→ 403 Forbidden   (write 権限のない member による変更)
→ 404 Not Found   (場が不在、または呼び出し元が member でない。露呈回避)

DELETE /streams/{stream_id}/members?identity=<id>
→ 204 No Content
→ 403 Forbidden   (write 権限のない member による他 member の削除)
→ 404 Not Found   (場が不在、または呼び出し元が member でない。非メンバーの自己離脱試行を含む。露呈回避)

GET /streams/{stream_id}/members
→ 200 OK { members: [ { identity, access }, ... ] }
→ 404 Not Found   (場が不在、または呼び出し元が member でない。露呈回避)
```

- membership = structural authZ の判定材料（誰が read / write 権限を持つか）のみ。write = 投函権、
  read = 受信権。field 名は `role` ではなく `access` とする（role 概念を relay の状態モデルに
  乗せない、identity 別書 §2.2）。
- 参照系（`GET /streams/{stream_id}` のメタ取得 / `GET /streams/{stream_id}/members` の member
  一覧）は当該場の **member 限定**（`access` 種別は問わない）。非メンバーには不在の stream_id と
  同一の `404` を返し、場の存在・member 構成を露呈しない（identity 別書 §2.1）。
- 書き込み系（投函 §3.2 / membership 変更 §3.3 / close §3.4）の拒否は 2 段階: **完全非メンバー**には
  不在の stream_id と同一の `404`（参照系の存在秘匿を書き込み endpoint への probe でバイパスさせ
  ない）、**write 権限のない member** には `403`（member は場の存在を正当に知っているため露呈に
  ならない。identity 別書 §2.2）。
- 自分自身の membership 削除（離脱）は member 本人であれば `access` 種別によらず常に許可する
  （identity 別書 §2.2）。
- 「誰が close してよいか」「誰が cancel してよいか」等の semantic な判定は relay は持たない
  （ow 側。identity 別書 §2.5）。

### 3.4 `DELETE /streams/{stream_id}` — 場の close

```
DELETE /streams/{stream_id}
→ 204 No Content
→ 403 Forbidden   (write 権限のない member による close)
→ 404 Not Found   (場が不在、または呼び出し元が member でない。露呈回避)
```

- close は **新規投函を止めるだけ**。archive は作らない。
- close 後の `POST .../messages` は `410 Gone`。
- close 時点で outbox に残っている未配達エントリは **retain 期間まで配達を継続**（close は投函口を閉じるだけで配達は止めない）。
- close 済み場の in-memory record は idle-GC の対象になる（§6.8）。close から猶予期間を過ぎ、かつ
  その場の未配達 outbox エントリが drain し切ったものを registry から除去する。除去後の同名 `stream_id`
  は不在（`GET` は `404`）となり、再作成が可能になる。

---

## 4. seq 体系（`publish_id` 1 系統）

R1 + 本書 §0.1 により **`publish_id` 1 系統**に確定。

- **`publish_id`** は relay 全体で global に単調増加する**整数**通番。publish（場 / subscription
  両レーン）ごとに relay が採番する。
- 1 つの `publish_id` が 3 役を兼ねる:
  1. **ack の cumulative カーソル**（`up_to_publish_id`、§5.6）
  2. **outbox エントリのキーの一部**（delivery target × `publish_id`、§6）
  3. **SSE `id:` 行**（重複検知、subscriber 間の発生順比較、§5.5）
- **`stream_seq` / `subscription_seq` は存在しない**。場内順序が必要な subscriber は、同一
  `stream_id` 宛イベントの `publish_id` 昇順で並べる（同一場宛の投函は relay 採番順 = `publish_id`
  単調）。per-target の gap 検知用 seq も持たない — 取りこぼしは relay 側 per-target outbox の
  未 ack 再 push（§6.5）で構造的に回収されるため、subscriber 側の gap 検知を前提にしない。

---

## 5. subscription / 配達 API

### 5.1 `POST /subscriptions` — subscribe

```
POST /subscriptions
Body: {
  subscriber: <identity>,            // 認証済みハンドル
  labels: [<string>, ...],           // 順序無視・重複削除して set 扱い。空配列は 400。個数/文字列長上限あり（§6.9）
  lease_ttl?: <seconds>,             // default 300, min 30, max 86400
  delivery_options?: {
    retain_seconds?: <int>           // outbox エントリの保持上限秒数。default 86400(24h), min 60, max 86400（§6.4）
  }
}
→ 201 Created { subscription_id, lease_expires_at }
→ 400 Bad Request   (labels == [] : firehose 防止)
→ 429 Too Many Requests  (registry 資源上限: 総数 / subscriber あたり。§6.8)
```

- relay が `subscription_id`（UUID）を採番して返す。labels 変更は「新 subscribe + 旧 unsubscribe」で表現。
- registry 資源上限に達している場合は `429`（`ResourceLimitExceededError`）で subscribe を拒否する（§6.8）。
- `subscriber` は呼び出し元の認証済み identity と一致しなければならない（不一致は `403`。代理 subscribe
  は認めない）。以後この subscription への操作はこの identity に限定される（§5.7）。
- 同一 `(subscriber, labels)` でも複数 subscription を持てる（独立 lease）。
- `initial_replay` 系のオプションは **持たない**（R1: 取りこぼしは未 ack outbox 再 push で回収）。
- `lease_ttl` と `retain_seconds` は**独立した軸**であり、大小制約を置かない。lease は subscription の
  生存（renew で延命し続ける liveness）、retain は outbox エントリ 1 件の保持上限（durability）。
  短い lease を renew し続ける長寿命 subscriber が retain=24h の再送猶予を持つのが標準の姿で、
  `retain_seconds > lease_ttl` は正当。lease が renew されず切れると、retain の残りに関係なく当該
  subscription の未 ack エントリは配達対象から外れる（§6.4 の動的規則）。実効 replay 窓は
  min(retain, subscription が生存した期間)。

### 5.2 マッチング規則（subset / AND）

- マッチ条件: **subscribe.labels が publish.labels の subset** であればマッチ。
- `subscribe.labels=[X,Y]` は publish.labels に X と Y の両方が含まれるときだけマッチ（AND）。
- `subscribe.labels=[X]` は `publish.labels=[X,Y,Z]` にもマッチ（labels が多い方が「より特定」）。
- AND/OR/NOT の組み合わせは subscriber が複数 subscription に分解して表現。

### 5.3 lease renew / unsubscribe

```
PUT /subscriptions/{subscription_id}/lease
Body: { lease_ttl?: <seconds> }     // 省略時は subscribe 時の値を再適用
→ 200 OK { lease_expires_at }
→ 404 Not Found   (subscription 不在、または呼び出し元が subscriber でない。§5.7)
→ 410 Gone   (lease 切れ済み subscription が registry に残存。§5.7)

DELETE /subscriptions/{subscription_id}
→ 204 No Content
→ 404 Not Found   (subscription 不在、または呼び出し元が subscriber でない。§5.7)
```

- unsubscribe は当該 subscription の未 ack outbox エントリを同一 transaction で削除する（明示的な
  関心放棄であり DLQ には送らない。lease 切れとの扱いの差は §6.6）。

> lease / subscription registry は in-memory（R1: liveness クラス）。relay 再起動で消えるため、
> subscriber は再接続時に **re-subscribe（idempotent な使い方）+ heartbeat** で自己修復する。

### 5.4 `POST /publish` — subscription レーン publish

```
POST /publish
Body: {
  ref: { type: <string>, id: <int | string> },
  labels: [<string>, ...],           // 個数上限・1 個あたり文字列長上限あり（§6.9）
  title?: <string>,                  // 文字列長上限あり。超過は 400（relay は truncate せず拒否、§6.9）
  idempotency_key?: <string>         // 15 分内同一キーは dedup
}
→ 202 Accepted { publish_id, matched_subscriptions: <int> }
→ 400 Bad Request   (labels 上限超過 = LabelValidationError / title 上限超過 = InvalidRequestError。§6.9)
→ 429 Too Many Requests { Retry-After }   (publisher ごと rate limit 超過, default 100 req/sec)
```

- `202 Accepted` = 「outbox 永続化完了」。relay が publish を忘れることはない（§6.1）。
- fan-out（マッチ算出 → 各 subscription の outbox エントリ作成）は単一 transaction（atomicity）。
- publisher は cc-memory に限定しない（汎用 bus 原則）。

### 5.5 `GET /events` — SSE 多重化購読

```
GET /events?subscription_ids=<id1>,<id2>,...
Accept: text/event-stream
→ 200 text/event-stream

event: notification
id: <publish_id>
data: {
  delivery_target,        // "sub:<subscription_id>" | "stream:<stream_id>"（stream_id は
                          // ":" を含む canonical id のため、パースは先頭 ":" 1 個のみで分割する）
  publish_id,             // int、グローバル単調（id: 行と同値）
  ref?,                   // subscription レーンのとき
  labels?,                // subscription レーンのとき
  body?,                  // 場レーンのとき（不透明 body）
  title?,
  delivered_at
}
```

- 1 つの SSE 接続が複数 `subscription_id` を多重化。**加えて**、接続した identity が read 権限を持つ
  member である場のメッセージも同じ接続に流れる（delivery_target で判別）。
- SSE `id:` に `publish_id` を載せる（重複検知用）。**relay は `Last-Event-ID` を resume に使わない**
  — 再接続時は per-購読 ack カーソルに基づき未 ack outbox を黙って再 push する（§6.5）。
- keepalive: push が無い間も 30 秒ごとに `: keepalive` コメント行（proxy 切断防止）。keepalive の
  write 失敗は当該接続の強制切断トリガーになる（§6.4）。
- `subscription_ids=` の各 id には ownership 検証が掛かる（§5.7）。呼び出し元 identity の所有でない /
  存在しない id が 1 つでも含まれる場合、接続確立前に `404 Not Found`。relay 再起動で registry が
  消えた後も同じ応答になる（§5.7）。所有する subscription の lease が切れて registry に残っている
  場合は `410 Gone`。subscriber は `404` / `410` のいずれも「re-subscribe せよ」のシグナルとして扱う。

### 5.6 cumulative ack（レーン別 endpoint）

```
POST /subscriptions/{subscription_id}/ack        // subscription レーン
Body: { up_to_publish_id: <int> }
→ 200 OK
→ 404 Not Found   (subscription 不在、または呼び出し元が subscriber でない。§5.7)
→ 410 Gone        (lease 切れ済み subscription が registry に残存。§5.7)

POST /streams/{stream_id}/ack                    // 場レーン
Body: { up_to_publish_id: <int> }
→ 200 OK
→ 404 Not Found   (場が不在、または呼び出し元が read 権限を持つ member でない。露呈回避)
```

- ack は per-message ではなく **cumulative**。relay は当該 delivery target の outbox から
  `publish_id <= up_to_publish_id` のエントリを一括削除する。subscriber は都度 ack しても、
  batch 処理後に最大の `publish_id` を 1 回だけ ack してもよい（ack コストは O(1)）。
- 場レーンの delivery target は「場 × 呼び出し元 identity」に解決される。member は自分宛エントリ
  しか ack できず、他 member 宛エントリは構造上指定できない（§5.7）。場レーンは subscription を
  介さないため lease を持たず、membership の存在自体が配達継続の条件になる。
- **ack されるまで outbox エントリは残る**（push 成功だけでは削除しない）。これにより subscriber が
  受信後・処理前にクラッシュしても、再接続時に再 push される（at-least-once の確証を ack に置く）。
- 冪等: 同じ `up_to_publish_id` を 2 回送っても削除対象ゼロ件の no-op で `200 OK`。
- 重複配達（再 push）の排除は subscriber が `(subscription_id, publish_id)` の組で行う（SDK 別書
  §4.2 と整合）。場レーンは subscription_id を持たないため `(stream_id, publish_id)` の組で同様に行う。

### 5.7 subscription 操作の ownership 検証

`subscription_id` を参照するすべての operation — lease renew / unsubscribe（§5.3）、`GET /events` の
`subscription_ids=`（§5.5）、cumulative ack（§5.6 の `POST /subscriptions/{id}/ack`）— で、relay は
**呼び出し元の認証済み identity が当該 subscription の `subscriber` と一致すること**を検証する。
この検証は当事者性（ownership）という relay 内部の構造的事実のみに基づく判定であり、
**structural authZ の一部**である（identity 別書 §2.2）。

- 不一致は `404 Not Found`。存在しない subscription_id と**同一応答**とし、subscription の存在自体を
  第三者に露呈しない（§8 の露呈回避原則）。検証順序は ownership（404）→ lease 状態（410）。
  非所有者に `410` を返して存在を推測させることもしない。
- `410 Gone` は「所有者本人の subscription が lease 切れ済みで registry にまだ残っている」場合にのみ
  返る。relay 再起動で registry が消えた後は「かつて存在した」事実を relay が持たないため `404` に
  なる（`410` は返せるときだけ返す best-effort のヒント）。subscriber は `404` / `410` のどちらも
  「re-subscribe せよ」のシグナルとして同一に扱う（SDK 別書 §3.4）。
- subscription_id（UUID）は secret ではなく単なる識別子として扱う。ログ・観測系への露出を前提とし、
  「id を知っていること」を authZ にしない（capability URL 方式を採らない）。この検証が無い場合、
  露出した id 1 つで第三者が unsubscribe / ack を撃ち、正当な subscriber には「何も届かない」だけで
  異常の手がかりが残らない**無音の配達妨害**が成立してしまう。
- 場レーンの ack（`POST /streams/{id}/ack`、§5.6）は、呼び出し元 identity 宛のエントリ
  （target = 場 × 当該 identity）だけに解決される。他 member 宛のエントリは構造上指定できないため
  場レーンに ownership 違反は存在せず、該当エントリが無ければ冪等 no-op（§5.6）。

---

## 6. 配達基盤（outbox）

> 物理 schema（table 定義 / engine）は substrate 確定（A#1193）待ち。本節は論理的振る舞いを規定する。
> R1: relay が disk で守るのは **outbox のみ**。

### 6.1 transactional outbox

- publish 受領（場 / subscription 双方）→ マッチング → 各 delivery target の outbox エントリ作成を
  **単一 transaction**で実行。`202 Accepted` は outbox 永続化完了が条件。
- relay 再起動でも未配達エントリは保持される。

### 6.2 polling dispatcher

- 内蔵 dispatcher が outbox を polling（間隔 100ms〜1s, 設定可）。
- 単一プロセス内シングルトン（二重 push 防止、ファイル lock で enforce）。
- `未 ack かつ未 dead のエントリを target 単位で順に SELECT → SSE push` ループ。

### 6.3 冪等キー

- `idempotency_key` は publisher が生成・指定（推奨）。`(idempotency_key, publisher_identity)` で 15 分 dedup。
- 省略時 relay は `(publisher_identity, ref/stream, labels 正規化 hash, body 正規化 hash, 受信秒精度 ts)` で擬似キー補完。

### 6.4 retain / push retry

- subscription outbox の retain default = 24h（subscribe 時 `retain_seconds` で override 可、§5.1）。
- 場 outbox の retain default = 場の `default_ttl`（§3.1、未指定なら relay 既定 24h）。
- retain の許容域は両レーン共通で min 60 / max 86400（24h）。上限 24h は R1 の「relay は history を
  持たない」原則による — それを超える範囲の回収は publisher pull の責務（§6.7）。
- **`retain_seconds <= lease_ttl` のような静的制約は置かない**。outbox エントリが配達対象から外れる
  条件は、次のうち**最初に起きた事象**という動的規則に一本化する:
  - (a) subscriber の明示 ack（§5.6）→ エントリ削除
  - (b) retain 超過 → dead 化（§6.6）
  - (c) delivery target の消滅 — lease 切れ / relay 再起動による registry 消失（permanent error）→
    dead 化（§6.6）。unsubscribe は dead 化ではなく即時削除（§5.3）
- push 失敗時は指数バックオフ retry（初回 100ms, 係数 2, 最大 5 回 ≒ 累積 3.1s）。
- **retry 超過時、relay は当該 SSE 接続を強制切断する**（slow consumer 切断）。TCP 上は生存したまま
  write だけ失敗し続ける接続（受信側 buffer 詰まり等）を「切断」に正規化し、配達再開のトリガーを
  「再接続時 resume（§6.5）」の 1 系統に保つ。これにより「切断イベントが発生せず再 push が無期限に
  始まらない」状態を構造的に排除する。エントリは outbox に残り、再接続時に再 push される。
- push が発生しない間の zombie 接続は keepalive（§5.5、30 秒毎）の write 失敗で検出し、同じく強制
  切断する。強制切断は構造化ログ + `relay_sse_slow_consumer_disconnects_total`（§7.2）で観測する。

### 6.5 resume（再接続時の再 push）— R1 一本化

- subscriber 再接続（`GET /events`）時、relay は当該購読の **未 ack outbox エントリを古い順に再 push** する。
- subscriber は `Last-Event-ID` を送ってよいが、relay はそれを無視する（ack カーソルが真実源）。
- subscriber は重複を delivery target ごとの `publish_id`（subscription レーンは
  `(subscription_id, publish_id)`、場レーンは `(stream_id, publish_id)` の組）で吸収し、処理後に
  cumulative ack を返す（§5.6）。

### 6.6 DLQ（dead letter）

- outbox エントリは以下で `dead` 化し polling 対象外に:
  - retain 期間（default 24h / 場は default_ttl）超過、未 ack のまま
  - permanent error = delivery target の消滅（subscriber identity 削除済み / subscription の lease 切れ /
    relay 再起動による registry 消失 等）
- unsubscribe（§5.3）は dead 化ではなく未 ack エントリの**即時削除**。明示的な関心放棄は事故ではない
  ため、DLQ と warn ログは意図しない消滅（lease 切れ / registry 消失）の観測専用に保つ。
- dead 化時に warn 構造化ログ（§7.3）。`dead` から 7 日後に物理削除。
- `/status` に件数、`/metrics` に `relay_outbox_dead_total`。

### 6.7 retain 切れ時の fallback

- retain 超過、または subscription 消滅（lease 切れ / registry 消失）で dead 化したエントリは
  relay outbox から replay 不可。
- subscriber は publisher（cc-memory 等）へ直接 pull して取りこぼしを回収する
  （責務境界: relay は **subscription 生存中かつ retain 内**の便利再送、それを超える範囲は
  publisher が source of truth）。
- 再接続時、relay 側に該当 outbox が無ければ単に再 push 対象ゼロ（無音）。subscriber は別途
  定期 full reconciliation で publisher に当たる（SDK 側 3 段階 reconciliation、別書）。

### 6.8 registry 資源上限 + idle-GC（DoS 防御）

stream / subscription registry は in-memory（§0 R1）で、無制限に作成できると単一 peer が
relay のメモリを枯渇させられる。両 registry に以下を課す。

- **総数上限 + per-identity 上限**: 作成時に registry 全体の登録数と、その identity（stream は
  作成者、subscription は subscriber）の登録数を検査する。いずれか超過なら作成を拒否し
  `429 Too Many Requests`（`ResourceLimitExceededError`）を返す。上限値は設定可能で、既定は
  「想定同時 peer 数 × 1 peer あたり想定リソース数」を目安に置く。判定と登録は atomic に行い、
  並行作成による上限すり抜けを防ぐ。同一作成者の名前空間内での既存 stream の再作成（canonical
  stream_id が既存、`409`）は新規スロットを消費しないため上限判定より前に評価する。
- **idle-GC**: subscription は lease 切れから猶予期間を過ぎたものを registry から除去する（§5.7）。
  stream は close から猶予期間を過ぎ、かつ未配達 outbox エントリが drain し切ったものを除去する
  （close 済み場は新規 outbox を増やせない〈§3.4〉ため、未配達が無ければ以後も無く、除去は
  未配達メッセージの配達経路を絶たない）。除去は dispatcher の polling cycle（§6.2）で駆動する。
  除去は warn 構造化ログ（§7.3）で観測する。

### 6.9 入力フィールド上限（DoS 防御）

無制限の `title` 文字列や大量の `label` は registry / outbox のメモリを膨らませられる。個々の
入力フィールドに以下の上限を課す（`payload` 全体のサイズ上限は別途、§6.10 で扱う）。

- **title**: 文字列長の上限（`POST /publish`）。超過は `400`（`InvalidRequestError`）。relay は
  truncate せず拒否する（暗黙の切り詰めで publisher の意図を書き換えない）。
- **labels**: 配列の要素数上限と、各 label の文字列長上限（`POST /publish` / `POST /subscriptions`）。
  超過は `400`（`LabelValidationError`）。
- **stream の name**: 文字列長の上限（`POST /streams`、§3.1）。超過は `400`（`InvalidRequestError`）。
  canonical stream_id として registry・outbox・delivery target key に埋め込まれるため識別子系の
  上限に揃える。
- 上限値は設定可能で、既定は routing key（label）と表示用見出し（title）の実運用サイズを目安に
  置く。

### 6.10 request body サイズ上限

- body を受け取る全 endpoint（`POST /streams`, `POST /streams/{id}/messages`,
  `PUT /streams/{id}/members`, `POST /streams/{id}/ack`, `POST /subscriptions`,
  `PUT /subscriptions/{id}/lease`, `POST /subscriptions/{id}/ack`, `POST /publish`）で共通の
  request body サイズ上限（既定 256 KiB）を設ける。超過時は `413 Payload Too Large`
  （`PayloadTooLargeError`）。
- 上限値は本書や機能要件文書に明記された数値ではなく、一般的なメッセージング API の慣行
  （例: Amazon SQS のメッセージサイズ上限 256KiB）を参考にした実装既定値。運用側は
  `RELAY_MAX_PAYLOAD_BYTES` 環境変数で上書きできる。
- 個別フィールド（`body` / `title` / `labels` 等）ごとの長さ上限は §6.9 で別途課すが、本節の
  request body サイズ上限は request body 全体のバイト数のみを見る。理由: セキュリティ監査で
  指摘された脅威は「特定フィールドが大きすぎる」ことではなく「`await request.json()` が任意
  サイズの body を無条件に全部メモリへ読み込む」こと自体であるため、body 全体を対象にする方が
  発生源に近い。

---

## 7. observability

### 7.1 `GET /status`

```
200 OK {
  uptime_seconds,
  subscriptions_count,
  active_sse_connections,
  streams_count,
  outbox_pending_count,
  outbox_dead_count,
  publish_rate_5min,
  recent_warnings: [...]
}
```

- `recent_warnings` の各要素は warning の**種別・発生時刻・構造的な resource 識別子**
  （`event` / `ts` / `lane` / `target_type` / `publish_id` / `stream_id` / `subscription_id` /
  `error_code` / `oldest_unacked_publish_id`）のみを載せる。`GET /status` は authN のみ（authZ なし）で
  任意の認証済み client が読めるため、payload・title 本文や free-form な reason、peer identity は
  載せない（cross-tenant のユーザーデータ漏洩を避ける）。full な warning entry は relay 内部の
  サーバーログ sink（§7.3）にのみ残す。

### 7.2 `GET /metrics`（Prometheus 互換）

`relay_publish_received_total` / `relay_push_delivered_total{lane}` /
`relay_outbox_depth` / `relay_outbox_dead_total` / `relay_sse_connections` /
`relay_subscription_lease_expirations_total` / `relay_publish_failed_total{failure_reason}` /
`relay_ack_received_total` / `relay_sse_slow_consumer_disconnects_total`。

- label に peer identity（`publisher_identity`）・`subscription_id`・`delivery_target` を
  **使わない**（許可する label は `lane` = `stream` | `subscription` と
  `failure_reason` の低カーディナリティ enum のみ）。`GET /metrics` は authN のみ（authZ なし）で
  任意の認証済み client が読めるため、label 経由で他 peer の identity 列挙や subscription_id 露出
  （§5.7 が防ぐ攻撃の前提になる）を許すと cross-tenant の情報漏洩になる。label cardinality 爆発も
  避ける。per-publisher / per-target の追跡は構造化ログ（§7.3、`publish_id` / `publisher_identity`
  trace）で行う。

### 7.3 構造化ログ + サーバーログ

- **構造化ログ**: publish / push / subscribe / unsubscribe / ack / 認証失敗 / outbox エラー / DLQ 移動を
  JSON で出力。1 publish の trace は `publish_id` で関連付け。
- **サーバーログ（R1 新設）**: relay 自身のデバッグ用 append-only sink。payload 込み。TTL 90 日でローテ GC。
  **購読者向け endpoint は持たない**（since=N を生やすと裏口から場 history が復活するため禁止）。
  outbox の永続ストアとは**物理分離**し、配達経路には一切関与しない観測専用 sink とする。

---

## 8. status code 規約

| code | 意味 | 主な発生箇所 |
|---|---|---|
| `200` | 取得 / 更新成功 | GET 系, lease renew, membership, cumulative ack |
| `201` | 生成成功 | 場作成, subscribe |
| `202` | 受理（配達は非同期） | 場投函, publish |
| `204` | 成功・本文なし | unsubscribe, member 削除, 場 close |
| `400` | 不正リクエスト | labels==[], 必須欠落 |
| `403` | 認可なし | member の write 権限不足（投函 / close / membership 変更）, subscribe の `subscriber` ≠ 認証 identity |
| `404` | 不存在（露呈回避含む） | 場 / subscription 不在, 非所有 subscription への操作（§5.7）, 非メンバーによる場への操作（参照 / 投函 / close / membership 変更。§3.2–3.4） |
| `409` | 競合 | 同一作成者の名前空間内で同名 stream 既存（canonical stream_id 既存） |
| `410` | 消滅 / 期限切れ | close 済み場への投函, 所有者本人による lease 切れ subscription への操作（registry 残存時のみ。§5.7） |
| `413` | request body サイズ超過 | request body が上限（既定 256 KiB、§6.10）を超過 |
| `429` | rate limit / 資源上限 | publisher ごと publish 上限, registry 資源上限（stream / subscription 作成の総数 / per-identity。§6.8） |
| `503` | 一時不能 | outbox 障害（disk full / DB corrupt） |

- 認可エラーは「リソース存在を露呈しない」（A2A §7.5）。完全非メンバー / 非所有者には不在 id と
  同一の `404`、資格を持つ member の権限不足には `403` を使い分ける（詳細は identity 別書 §2.1 / §2.2）。

---

## 9. データフロー（要約）

### 9.1 場への投函

```
member A → POST /streams/{X}/messages
  → relay: 場 X の write 権限（membership）検証
  → relay: publish_id 採番、場 X の read 権限を持つ各 member の outbox にエントリ作成（単一 tx）
  → relay: 202 { publish_id, matched_members }
  → dispatcher: outbox polling → 接続中 member の SSE へ push（delivery_target="stream:X"）
  → member: 受信 → 処理 → POST /streams/{X}/ack { up_to_publish_id }
  → 切断中 member: retain まで outbox 保持 → 再接続で未 ack 再 push
```

### 9.2 subscription publish

```
publisher → POST /publish { ref, labels, title?, idempotency_key? }
  → relay: idempotency dedup（15分）→ subset マッチ算出 → 各 subscription の outbox にエントリ作成（単一 tx）
  → relay: 202 { publish_id, matched_subscriptions }
  → dispatcher: outbox polling → 接続中 subscriber の SSE へ push（delivery_target="sub:<id>"）
  → subscriber: 受信 → 処理 → POST /subscriptions/{id}/ack { up_to_publish_id }
  → 切断中 / retain 切れ: 未 ack 再 push、または publisher 直接 pull で回収（§6.7）
```

---

## 10. 残置（実装段階で詰める）

- **マッチング性能**: subset 判定を 10,000 subscriptions × 100 labels で p99 200ms に収める（inverted index 等）。
- **場メンバーの SSE 受信開始**: `GET /events` に member の場を自動含めるか、明示 `stream_ids=` も受けるか（本書は「認証 identity の member 場を自動含む」前提。明示指定オプションは実装段階で要否判断）。
- **物理 schema / engine**: outbox table 定義・SQLite vs LMDB（A#1193 確定待ち）。

---

## 11. 関連

- 機能要件 v2: cc-memory M#507（+ R1 改訂）
- 論点#3 決着メモ: `docs/design/topic474-論点3-場history-substrate-決着.md`（別 PR）
- identity / authZ 仕様: `relay-v2-identity-authz.md`（A#1201）
- シーケンス図集: `relay-v2-sequences.md`（A#1199）
- Python SDK 仕様: `relay-v2-sdk.md`（A#1203）
- substrate engine 確定: A#1193（relay 本体実装のゲート）
