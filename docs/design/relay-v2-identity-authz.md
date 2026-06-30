# relay v2 identity / authZ 仕様

> **位置づけ**: relay v2 の identity（認証 = authN）と authZ（認可）の仕様を、A2A 1.0 spec
> 準拠ラインで独立に書き下した文書。上位要件は機能要件文書（FR-5 系）。ワイヤプロトコル本体
> （endpoint / payload / 配達セマンティクス）は `relay-v2-wire-api.md` 側にあり、本書はそこから
> 切り出されている。
>
> **スコープ**: relay 自身が relay HTTP 境界で行う authN / authZ の規定。具体的には:
>
> - AgentCard / JWS / JCS / SecurityScheme（A2A 1.0 spec 準拠）
> - relay の認証フロー（publisher / subscriber の identity 確立）
> - relay 側 authZ 境界（**command 認可に限定**、subscribe は authZ 対象外）
> - relay 再起動と identity の関係
>
> **本書のスコープ外**:
>
> - ow（cc-memory / orch / worker）側の fine-grained authZ ポリシー本体。本書は relay 側の認可ゲート
>   と ow 側 MCP handler 同期ゲートの**境界**だけを書く。ow 側の判定ロジックは ow 側設計に閉じる
> - ワイヤプロトコルの endpoint / payload / status code 全集
>   → `relay-v2-wire-api.md`
> - cc-memory 連携プロトコル → cc-memory 側にのみ存在（協調プロトコル v1）
> - 用語の最終確定: relay v2 のユビキタス言語は議論中。本書は暫定的に機能要件文書 v3 の用語に揃え、
>   「場」と「stream」が併存する場合は原則「stream」を使う。A2A spec 由来の用語（AgentCard / JWS /
>   JCS / DID / SecurityScheme）はそのまま英語で書く

---

## 0. 一行で言うと

**relay の identity は A2A 1.0 spec の AgentCard + SecurityScheme + （任意で）JWS 署名で閉じる。
relay 側 authZ は coarse-grained に限定する: read は全許可、subscribe は authZ 対象外、command 系
（close / cancel / spawn 等）だけに認可ゲートをかける。fine-grained な「この identity がこの操作を
してよいか」の最終判定は relay 内ではなく cc-memory MCP handler 内で同期判定する。**

---

## 1. identity（A2A 1.0 spec 準拠）

A2A 1.0 spec は identity 基盤として AgentCard + SecurityScheme（OpenAPI 3.2.0 基盤）+ 任意 JWS 署名
を規定する。relay v2 はこの spec をそのまま採用する（独自スキームは作らない）。DID には A2A 1.0
spec 本体が一切触れていないため、本書では DID をスコープ外とする（§1.4）。

### 1.1 AgentCard

#### 1.1.1 配置

```
GET /.well-known/agent-card.json
→ 200 OK
Content-Type: application/a2a+json
Body: <AgentCard JSON>
```

- A2A 1.0 spec の well-known URI 規約に従い `/.well-known/agent-card.json` に配置する。
- media type は `application/a2a+json` を用いる（A2A 1.0 spec IANA 登録に従う）。
- AgentCard 取得自体に認証は要求しない（公開 AgentCard）。詳細を絞った公開版を返し、認証済み
  client 向けの追加情報は **extended AgentCard**（§1.1.4）で返す。

#### 1.1.2 公開 AgentCard の構造

relay が公開する AgentCard には少なくとも以下を含める。

| field | 内容 |
|---|---|
| `name` | relay インスタンス名 |
| `version` | relay 実装バージョン |
| `supportedInterfaces` | relay が話す A2A interface 一覧。`protocolBinding` は `HTTP+JSON` を基本とする |
| `capabilities` | relay が宣言する機能フラグ。`extendedAgentCard: true` を立てる |
| `securitySchemes` | relay が受理する認証スキーム集合（§1.5） |
| `security` | requirement object。どの scheme を必須とするか |
| `provider`（任意） | 運用主体 |
| `documentationUrl`（任意） | 本書 / ワイヤ仕様への参照 |
| `signatures`（MAY） | JWS 署名（§1.2） |

> **注**: relay は「自分自身が 1 つの A2A agent として」AgentCard を持つ。relay 経由で接続する
> worker / orch / cc-memory ごとの skill 詳細は relay 自身の AgentCard ではなく、各 agent 側の
> AgentCard 側で表明する。relay は中継であって skill 提供主体ではない。

#### 1.1.3 capabilities 列挙

公開 AgentCard の `capabilities` には relay が提供する機能を列挙する。最低限、以下を真として
宣言する。

- `streaming: true` — SSE 配達（`GET /events`）を行う
- `pushNotifications: false` — webhook push 通知は v2 では提供しない（SSE 配達のみ）
- `extendedAgentCard: true` — 認証済み client に追加情報を返す（§1.1.4）

#### 1.1.4 extended AgentCard

A2A 1.0 spec の `GetExtendedAgentCard` を実装する。

- 認証済み client が JSON-RPC で `GetExtendedAgentCard` を呼ぶと、公開 AgentCard より詳細な
  AgentCard を返す。
- 公開版に乗せたくない情報（運用上の連絡先・内部 endpoint・追加 scope 一覧の細部）はこちらに
  寄せる。
- A2A 1.0 spec の MUST に従い、client は公開 AgentCard の `securitySchemes` で宣言済みのいずれかで
  認証してから呼び出す。

### 1.2 JWS（JSON Web Signature）

A2A 1.0 spec §8.4 は AgentCard を JWS Compact Serialization で署名することを **MAY** と規定する
（MUST ではない）。relay v2 では JWS 署名を **MAY（推奨）** として実装する。

#### 1.2.1 署名対象

- AgentCard 全体から `signatures` フィールドを除外したものを正規化対象とする
  （§1.3 JCS のステップ 4 と整合、A2A 1.0 spec §8.4.3）。
- protobuf 由来の default 値プロパティ（空文字列・false・空配列等）は正規化前に明示的に削る
  （A2A 1.0 spec §8.4.3 step 3）。

#### 1.2.2 鍵管理

- 署名鍵は **ES256** を基本とする（A2A 1.0 spec の例示で用いられているアルゴリズム）。
- 公開鍵は `/.well-known/jwks.json` に JWKS として配置する（rfc-7517）。
- JWS protected header に `kid`（鍵 ID）と `jku`（JWKS の URL）を含める。
- 鍵ローテーション時は旧 `kid` を JWKS に一定期間残し、ローテ前後で AgentCard の検証を継続できる
  状態を保つ。

#### 1.2.3 verification 手順

A2A 1.0 spec §8.4.3 の MUST 手順に従う。

1. AgentCard の `signatures` 配列から検証対象の署名要素を取り出す。
2. JWS protected header の `kid` と `jku` から（または別途信頼済み key store から）公開鍵を取得
   する。
3. 受領した AgentCard から default 値プロパティを除去する。
4. **`signatures` フィールド自体を canonicalization 対象から除外**する（循環依存回避）。
5. 残りを JCS（rfc-8785）で正規化する（§1.3）。
6. 公開鍵で署名検証する。

#### 1.2.4 relay の段階定義

| 段階 | 内容 |
|---|---|
| **最小セット**（spec 違反にならない最低限） | 署名なし AgentCard を公開し、`signatures` フィールドを省略する |
| **フル準拠セット** | ES256 で JWS 署名し、`/.well-known/jwks.json` を公開する |

relay v2 は初期実装で**最小セット**を満たし、JWS 署名は MAY として将来段階で導入する。
段階移行時に AgentCard の wire 形は破壊的に変わらない（`signatures` 追加だけ）。

### 1.3 JCS（JSON Canonicalization Scheme、rfc-8785）

A2A 1.0 spec §8.4.1 は「AgentCard を署名する場合、署名前に JCS（rfc-8785）で正規化する」ことを
**MUST** と規定する。relay v2 は JWS 署名を採用する場合、JCS を MUST として実装する。

- 正規化の対象は §1.2.1 の通り `signatures` 除外後の AgentCard。
- 実装ライブラリは `rfc8785` 系の Python パッケージを利用する。
- 検証側（AgentCard を受け取る側）も同じ JCS で正規化してから署名照合する。relay 側で AgentCard
  を発行するだけでなく、外部 agent の AgentCard を検証する経路でも JCS を使う。

### 1.4 DID（Decentralized Identifier）はスコープ外

#### 1.4.1 事実

A2A 1.0 spec 本体（spec docs / proto）には DID / `did:` / decentralized identifier に関する記述が
**一切ない**。1.0 announcing post も identity 基盤として JWS + JWKS（Web PKI / 既存鍵管理）を前提に
書かれている。DID は研究コミュニティから A2A の上位層に被せる提案（AIP / Trust Fabric 等）が複数
出ているが、いずれも spec の外側の提案レイヤである。

#### 1.4.2 relay v2 の立場

- relay v2 は **DID を扱わない**。AgentCard + JWS + JWKS（jku）で identity を閉じる。
- 将来 did:web を採用する場合でも、AgentCard 側を変えずに `jku` を did document の
  `verificationMethod` URL に向けるだけで接続できる拡張余地は残る（did:web は
  `https://{host}/.well-known/did.json` ベース）。
- did:key / did:peer 等の静的鍵経路は JWKS と用途が直接競合するため、relay 用途では現時点で採用
  しない。

#### 1.4.3 仕様改訂時の扱い

A2A spec が将来 DID を採用したとき、または relay の運用で多テナント間の cross-org identity が
要件化したときに、別文書（または本書の次版）で DID 採用を議論する。本版では「**DID はスコープ
外**」と明示するに留める。

### 1.5 SecurityScheme

A2A 1.0 spec §4.5 は SecurityScheme を OpenAPI 3.2.0 基盤の 5 type で規定する。

| type | A2A での object 名 | 主用途 |
|---|---|---|
| `apiKey` | `APIKeySecurityScheme` | header / query / cookie 経由の固定キー |
| `http` | `HTTPAuthSecurityScheme` | Bearer / Basic / Digest |
| `oauth2` | `OAuth2SecurityScheme` | 各 OAuth2 flow（authorizationCode / clientCredentials / 等） |
| `openIdConnect` | `OpenIdConnectSecurityScheme` | OIDC discovery URL ベース |
| `mtls` | `MutualTlsSecurityScheme` | mTLS（クライアント証明書） |

#### 1.5.1 relay 採用方針

| 段階 | 採用 SecurityScheme |
|---|---|
| **最小セット** | `HTTPAuthSecurityScheme { scheme: "bearer" }` 1 個 |
| **フル準拠セット** | 上記 + `OpenIdConnectSecurityScheme` または `OAuth2SecurityScheme` を追加 |

最小セットでも spec 違反にはならない。relay は Bearer token を A2A 1.0 spec §7.4 の MUST に従い
「すべての incoming request」に対して検証する。

#### 1.5.2 宣言形式

A2A 1.0 spec の canonical JSON は OpenAPI flat 形（`{"type": "http", "scheme": "bearer"}`）ではなく、
**discriminated-union の wrapper-key 形**を採る。AgentCard の `securitySchemes` フィールドは次のように
書く。

```json
{
  "securitySchemes": {
    "bearer": {
      "httpAuthSecurityScheme": {
        "scheme": "bearer"
      }
    }
  },
  "security": [
    { "bearer": [] }
  ]
}
```

- `securitySchemes` の各エントリは `{ <schemeWrapperKey>: { <fields> } }` 形を取る。`type` という
  フィールドは持たない。
- A2A 1.0 spec docs / proto に flat `"type": "http"` 形は出現しない。relay 実装の AgentCard
  シリアライザはこの canonical JSON 形で出力する。
- `security` requirement object の各エントリの「array に列挙される文字列」は scope または role 名を
  指す（OpenAPI 3.2.0 では bearer の場合 in-band 非交換の advisory role 名扱い）。relay は scope を
  AgentCard.security で広告するだけでなく、JWS token 内の `scope` / `scp` claim を自前で検証する
  必要がある（§3.4）。

---

## 2. authZ（認可）境界

relay v2 の authZ は **3 軸分離**で定義する（機能要件文書 FR-5.5）。

| 軸 | 担当 | 内容 |
|---|---|---|
| **authN（identity 真正性）** | **relay** | 全 API はこれを最低限通る。Bearer token 検証 / JWS 検証等 |
| **coarse-grained authZ** | **relay** | 「この identity が relay の特定 API を呼んでよいか」の粗粒度判定。具体的には **command 系 endpoint への access**（§2.2） |
| **fine-grained authZ** | **relay 外**（cc-memory MCP handler） | 「この identity がこの **特定 stream / 特定 subscription / 特定 entity** に対する操作をしてよいか」の細粒度判定 |

### 2.1 read は全許可

GET 系 endpoint（`GET /streams/{id}`、`GET /streams/{id}/members`、`GET /events`、`GET /status`、
`GET /metrics`、`GET /.well-known/agent-card.json` 等）は **authN（identity 確認）のみで authZ なし**
で通す。

- A2A 1.0 spec §7.4 の MUST に従い、認証は全 request に対して行う。
- 認証済み identity であれば、relay が公開する read 系 endpoint には誰でも access できる。
- 「特定 entity の閲覧禁止」のような細粒度 read filter は relay は持たない。必要なら publisher 側
  （cc-memory 側）で公開 vs 非公開を分けて publish する。

### 2.2 command 認可に限定

relay 側 authZ は **command 系 endpoint のみ**にかける。

- relay が「command」として扱うのは、relay 自身の状態または外部状態を破壊的に変更する操作で、
  かつ relay が直接実行するもの:
  - **場 (stream) の close**（`DELETE /streams/{id}`）
  - **場のメンバーシップ削除**（`DELETE /streams/{id}/members?identity=`）
  - **subscription の unsubscribe**（他者の subscription を切ろうとするケース。自分の subscription を
    切るのは authZ 対象外）
- これらは relay 設定で「identity → 許可 command 集合」のマッピング（coarse-grained authZ table）に
  従って許可 / 拒否する。
- 「特定の close 操作対象が cancel すべきかどうか」「spawn を許してよい状況か」のような
  状況依存判定は relay の外（cc-memory MCP handler）で行う（§2.5）。

> **note**: 機能要件文書 FR-5.5 は「coarse-grained authZ は relay の責務」と定めるが、本書はその
> coarse-grained authZ の適用範囲を **read を除く command 系のみ**に絞ることを明示する。
> publish / subscribe / read は coarse-grained authZ の対象から外す（§2.3 / §2.4）。

### 2.3 publish は authN のみ

`POST /publish`（subscription レーン publish）および `POST /streams/{id}/messages`（場 publish）は
authN のみで通す。

- 認証済み identity であれば publish できる。
- 「この labels への publish は禁止」のような細粒度 filter は relay は持たない。
- ただし `POST /streams/{id}/messages` は「writer membership」を要求する（場固有のアクセス権、
  機能要件文書 FR-2 の coarse-grained authZ）。membership は relay 側で coarse-grained authZ として
  保持する（§2.2 と同じ層）。

### 2.4 subscribe は authZ 対象外

`POST /subscriptions`（subscribe）および `GET /events?subscription_ids=`（SSE 受信）は authZ 対象外
とする。

- 認証済み identity であれば、誰でも任意の labels セットで subscribe できる。
- 「特定 labels の subscribe を禁止する」「特定 entity の通知を受け取れる identity を限定する」の
  ような細粒度 authZ は relay は持たない（機能要件文書 FR-5.5 と整合）。
- これは「subscribe は labels セットの意図的宣言であって、relay は配達経路だけを持つ」という
  責務境界 decision からの帰結。誰が何を購読すべきかは publisher 側（cc-memory 側）が宣言ツール +
  配線代行で扱う。
- 結果として、subscribe 経路で漏れてはならない情報がある場合は、**publisher 側で publish しない**
  ことで担保する（subscribe filter ではなく publish gate で担保する設計）。

### 2.5 cc-memory MCP handler 同期ゲート

fine-grained authZ（「この identity がこの **特定 stream / 特定 subscription / 特定 entity** に対する
操作をしてよいか」の判定）は **relay の外で、かつ操作の実行と同じプロセス内で同期に行う**。

具体的には:

- close / cancel / spawn 等の意味的に重要な操作は、cc-memory MCP handler 内で **同期判定 → 状態
  変更 → relay への publish** を一つの transaction として扱う。
- 「relay 経由で command を投げてから後で別経路で authZ 結果を反映する」ような非同期構造は採らない
  （race condition が発生する）。
- relay は cc-memory MCP handler が**判定を済ませた command** を受け取って配達するだけ。
  relay 自身が close / cancel / spawn の意味判定を行うことはない。

設計帰結:

- relay 側 authZ table は粒度を「command 系 endpoint に対する全体的 access 可否」までに留める。
- 細粒度判定（「この identity がこの場の close を呼んでよいか」）は cc-memory プロセス内 handler に
  寄せ、judgment lookup と state mutation を atomic に行う。
- これにより「relay が close を受理したが ow 側で禁止判定が後追いで出る」「逆に ow 側で禁止判定が
  出ているのに relay が close を実行してしまう」という race を構造的に消す。

---

## 3. 認証フロー

### 3.1 publisher → relay

```
1. publisher が relay の /.well-known/agent-card.json を取得
   → AgentCard を読み、securitySchemes / security から「どの認証方式を使うべきか」を決定
2. （JWS 採用時）publisher は AgentCard の signatures を JCS + 公開鍵で検証
   → AgentCard の改竄を排除し、relay の identity を確立
3. publisher は AgentCard.securitySchemes に基づき認証 credential を取得
   （Bearer token は別経路で取得済みを前提、A2A 1.0 spec §7.3 の out-of-band 想定）
4. publisher は POST /publish または POST /streams/{id}/messages を呼ぶ
   Authorization: Bearer <token>
5. relay は token を検証 → identity 確立 → publish を受理
   （publish レーンは authZ 対象外、§2.3）
```

### 3.2 subscriber → relay

```
1. subscriber は §3.1 と同じ手順で AgentCard 取得 + 認証 credential 準備
2. subscriber は POST /subscriptions を呼ぶ
   Authorization: Bearer <token>
   Body: { subscriber: <identity>, labels: [...], lease_ttl? }
3. relay は token を検証 → identity 確立 → subscription を発行（subscription_id を返す）
   subscribe 自体は authZ 対象外（§2.4）、誰でも認証が通れば subscribe できる
4. subscriber は GET /events?subscription_ids=<id> で SSE 接続を張る
   Authorization: Bearer <token>
5. relay は配達対象の outbox エントリを SSE 経由で push
6. subscriber は POST /subscriptions/{id}/ack で cumulative ack を返す
```

### 3.3 command 系（authZ 経路）

```
1. command を出す側（例えば orch）は cc-memory MCP handler を呼ぶ
   （relay 直接ではなく cc-memory プロセス内 handler 経由）
2. cc-memory MCP handler は同期で:
   a. 呼び出し identity の authN 結果を確認
   b. 操作対象（場 / subscription / entity）に対する fine-grained authZ を判定
   c. 許可なら状態変更を実行
   d. 必要に応じて relay に publish して通知を流す
3. relay は cc-memory から received した publish を identity 真正性のみ確認して配達
   （relay 側 fine-grained 判定なし）
```

`DELETE /streams/{id}` のような relay 直接 endpoint を経由する command も同様で、relay 側
coarse-grained authZ は「この identity が delete API を呼んでよいか」までを判定し、
「この場を delete してよい状況か」は cc-memory 側の同期 handler が事前判定したうえで relay を呼ぶ。

### 3.4 JWS token 内の scope 検証

A2A 1.0 spec の bearer scheme で AgentCard.security に列挙された scope は in-band 非交換の advisory
role 名扱いとなる（§1.5.2）。relay は JWS bearer token 内の `scope` / `scp` claim を自前抽出して
照合する。

- token 形式の細部（claim 名 / 形式 / rfc-9068 採用可否）は relay 実装で確定する。本書では
  「scope claim を JWS token に乗せ、relay 側 coarse-grained authZ table 引きと突き合わせる」までを
  規定する。
- scope の具体命名（`relay:command.close` 等）は議論中（§7）。

---

## 4. relay 再起動と identity

relay v2 の永続層は **outbox のみ**（機能要件文書 FR-4.1）であり、subscription registry / lease /
identity → role 束縛は **in-memory** に置く。これにより identity の経時挙動に以下の特徴が出る。

### 4.1 in-memory state 喪失後の subscriber 再接続

- relay 再起動で subscription registry は消失する。
- subscriber が再接続するときは **新たに `POST /subscriptions` を呼ぶ**。
- relay は新しい `subscription_id` を発行する（旧 ID を持参して継続する経路はない、
  機能要件文書 FR-3.0 subscriber 履歴中立性）。
- relay にとっては「同じ identity の同じ labels セット」であっても **完全な新規 subscriber** と
  して扱う（subscription_id が違うため別人扱い）。
- 旧 subscription_id 宛 outbox エントリは「subscription_id 不存在」を理由に DLQ 行きとなり、
  時間経過で消滅する（機能要件文書 FR-4.7）。

### 4.2 identity 自体（AgentCard / DID）は relay 再起動で喪失しない

- AgentCard 公開鍵 / JWKS / 認証発行情報は外部由来（外部 IdP または relay の disk-persisted 設定）。
- relay プロセスの in-memory state とは独立して保持される。
- relay 再起動後も同じ AgentCard を返し、同じ JWKS で署名検証が成立する。
- 認証発行された Bearer token は relay 再起動前後で同じく有効（relay は token 状態を in-memory に
  持たない、外部 IdP に検証を委ねる方針を採る場合）。

### 4.3 subscriber identity の経時同一性は ow 側責務

- relay は「現在の subscription_id」でのみ subscriber を識別する。
- 「同じ subscriber が再接続前後で同一エージェントである」ことの担保は **relay の責務ではない**。
- ow 側で「subscriber identity の経時 ledger」「再構築起動契機」「取りこぼし回収」を扱う。
- relay は subscription 単位の authN（identity 真正性）だけを保証する。

### 4.4 設計帰結

| 状況 | relay の振る舞い |
|---|---|
| 同じ identity が新しい subscription_id で再接続 | relay は完全な別人として扱う。labels セットも新規宣言を要求する |
| 旧 subscription_id 持参で再接続要求 | relay は受理しない。subscriber は新規 subscribe を呼び直す |
| relay 再起動を跨いだ未配達 outbox エントリ | subscription_id 不存在 → DLQ 行き → 時間経過で物理削除（機能要件文書 FR-4.7） |
| 取りこぼし回収 | 機能要件文書 FR-4.8 に従い、publisher（cc-memory 等）直接 pull で subscriber 側が補完する |

---

## 5. 将来拡張

本書では採用しないが、将来要件化したときに別文書（または本書の次版）で議論する候補。

### 5.1 認可の granularity 拡張（labels ベース authZ）

現状の relay v2 は subscribe を authZ 対象外としている（§2.4）が、将来多テナント運用で「組織 A の
identity は組織 B のみが publish した labels を subscribe できない」のような要件が出たとき、relay
内に **labels ベースの fine-grained authZ** を導入する余地がある。

検討すべき点:

- subscribe authZ を入れるか、publish gate のみで担保するか
- labels の namespace 規約（`<ns>:<value>` 形式）に authZ namespace を予約するか
- cc-memory MCP handler 同期ゲート（§2.5）と整合する形で relay 側にも判定を入れるか

### 5.2 revocation list（DID / 鍵失効）

- 現状の relay v2 は JWS 鍵ローテーション（§1.2.2）を扱うが、**revocation**（漏洩鍵の即時無効化）の
  仕組みは持たない。
- 漏洩時の運用は「JWKS から該当 `kid` を即削除 → 全 client が AgentCard を引き直す」で対応する想定。
- 将来 DID を採用する場合（§1.4.3）は DID method 側の revocation 経路を別途検討する。

### 5.3 in-task auth（A2A 1.0 spec §7.6）

- 通常 token と「危険操作用の追加 credential」を分けて持つ A2A 1.0 spec の in-task auth フローは、
  現状 relay v2 では採用しない。
- command 系の fine-grained 判定は cc-memory MCP handler 同期ゲート（§2.5）で済ませる方針なので、
  in-task auth を入れる動機は薄い。
- 将来 cross-org delegation が要件化したとき検討する。

---

## 6. 参考一次資料

- A2A v1.0 specification（HTML / raw markdown）— AgentCard / SecurityScheme / 認証 MUST 要件の根拠
- A2A 1.0 announcing post — JWS による AgentCard 署名の位置づけ
- rfc-7515（JWS）— JSON Web Signature
- rfc-7517（JWK）— JSON Web Key（JWKS の形）
- rfc-8785（JCS）— JSON Canonicalization Scheme
- OpenAPI 3.2.0 Security Scheme Object — A2A SecurityScheme の上流仕様
- 機能要件文書（FR-5 系）— relay v2 の identity / authZ 要件
- 責務境界 decision 群（先行設計議論）— relay = 配達 + 真正性 + シグナル配達 / ow = 全ポリシー
  （authZ 含む）、subscriber 履歴中立性、cc-memory MCP handler 同期ゲート

---

## 7. 本書の未決事項

| 項目 | 状態 | 備考 |
|---|---|---|
| AgentCard の `provider` / `documentationUrl` の最終 URL | 未定 | relay リポ確定後に埋める |
| 公開 vs extended AgentCard の field 切り分け | 推測 | 運用要件が出てきたとき確定する |
| Bearer token 発行主体（relay 自前 vs 外部 IdP） | 推測 | 運用判断、本書では「relay 設定で選択」と書くに留める |
| coarse-grained authZ table の具体スキーマ | 未定 | 実装計画 IF 凍結時に確定する |
| scope 命名規則（`relay:command.close` 等） | 議論中 | 先行設計議論で 5 scope 案が出ているが、本書では「command 認可に限定」の境界だけを書き、scope 命名の最終 freeze は別議論 |
| JWS 鍵ローテーション運用手順 | 未定 | 運用ドキュメント側 |
