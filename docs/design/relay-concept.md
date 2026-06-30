# relay 概論 / コンセプト

> **位置づけ**: 初めて relay v2 に触れる人が「relay とは何か」を理解するための最上位ドキュメント。
> ワイヤ / API 仕様、identity / authZ 仕様、SDK 仕様、シーケンス図集、グロッサリーの前にまず読む。
>
> **想定読者**: relay を使う側のエージェント実装者、relay 自体の実装者、relay を含むアーキテクチャを
> 評価したい設計者。
>
> **ユビキタス言語**: relay v2 の用語は現在議論中である。本書は暫定的に機能要件文書の用語に揃える。
> 用語確定後に一括置換される可能性がある。

---

## 1. 一行で言うと

**relay は、エージェント同士が非同期にメッセージをやり取りするための汎用 message bus である。**

agent-to-agent（A2A）の transport buffer として動き、配達のメカニズムだけを提供する。
「誰が誰に送ってよいか」「どの順序で読むか」「いつ購読をやめるか」といったポリシーは、
relay を使う側のエージェント（例: ow）が決める。

---

## 2. relay は何を解こうとしているか

### 2.1 人間承認制チャットからの脱却

relay の v1 は、人間同士のペアチャットを SSH 経路で中継する仕組みだった。
チャンネルに人間が SSE で繋ぎっぱなしになり、ログは relay 内に永続蓄積され、
相手は人間が読むことを前提として書かれていた。

エージェント同士がそれなりの頻度で自律的に話し始めるようになると、この前提は崩れる。
人間がいない時間軸で発火するメッセージ、purpose-built なラベルで絞った購読、
プロセスが死んでも届く保証、複数エージェントへの fan-out — これらを「人間が画面を見ている」
モデルの上に積むのは無理がある。

v2 は **agent-to-agent message bus** として最初から書き直されている。
人間のチャットは v2 のユースケースから外れる（必要なら別 UI を v2 の上に建てればよい、
というスタンス）。

### 2.2 transport buffer であって真実の保管庫ではない

relay は「メッセージを運ぶ場所」であって、「メッセージを溜めておく場所」ではない。
過去のメッセージや、エンティティの最新状態を後から読み返したい場合は、
relay ではなく **publisher 側の永続真実ストア**（cc-memory など）に直接 pull する。

relay 自身が永続化するのは未配達の outbox だけで、配達済み・retain 期間切れの
メッセージは relay 内に残らない。これは設計の制約ではなく **思想の明示**である。

> 補足: relay にメッセージ履歴 API を生やすと、関心領域（誰が真実源か、誰がスキーマを
> 進化させるか）が relay と publisher の両方に発生し、責務境界がぼやける。
> v2 はこれを構造的に防ぐため、relay 側に履歴を持たないことを選んでいる。

---

## 3. 設計思想 — メカニズムとポリシーの分離

relay v2 の中核思想は、メカニズム（mechanism）とポリシー（policy）を厳密に分離することである。

### 3.1 relay が持つもの（メカニズム）

- メッセージを配達する経路（outbox、push、retry、retain）
- 配達の at-least-once 保証
- 送信者の真正性検証（identity の authN）
- 「subscribe API を呼んでよいか」レベルの粗粒度な authZ

### 3.2 relay が持たないもの（ポリシー）

- メッセージ本文の意味解釈（done / heartbeat / blocked などの判定）
- ロール概念（orch / dispatcher / worker といった役割の認識）
- 細粒度の authZ（特定 labels を subscribe してよいか、特定操作を実行してよいか）
- 関心領域の述語評価（AND / OR / NOT の複雑な組み合わせ）
- subscriber の identity の経時的同一性管理

ポリシーは relay を使う側のエージェント（典型的には ow）に委ねられる。
これによって relay は「ow 専用 bus」ではなく、別のエージェント体系でも再利用できる
**汎用バス**として成立する。

### 3.3 なぜこれが効くか

ポリシーは時とともに揺れる（誰がどの操作をしてよいか、述語の意味、ロールの粒度）。
メカニズムは比較的安定である（配達は配達であって、再送と ack で閉じる）。

ポリシーを relay に埋め込むと、ポリシーの変更が relay 本体の改修を要求するようになり、
他のエージェント体系への再利用も難しくなる。逆に、メカニズムだけ持つ relay の上に
ポリシーをエージェント側で組み立てれば、relay は安定土台として下で固定でき、
ポリシーは上で自由に進化できる。

---

## 4. transport buffer モデル

relay の挙動は、次の 4 点を抑えれば把握できる。

### 4.1 stream は pure pass-through

**stream**（機能要件文書内で「場」とも表現される）は、名前付きの共有空間である。
publisher は stream にメッセージを投函し、stream のメンバーにそれが配達される。

stream 自体には永続蓄積がない。メッセージは投函された瞬間に、配達経路（outbox）に
転写されるだけで、stream という場所には残らない。stream の close は「新規投函を止める」
ことを意味し、過去メッセージのアーカイブを作ることは意味しない。

### 4.2 outbox だけが永続化される

relay が disk に永続化する唯一のデータは **outbox** である。outbox は、
「ある購読者宛にまだ配達できていないメッセージ」のキューであり、購読者ごとに独立して持たれる。

subscription registry、lease 情報、SSE 接続状態などはすべて in-memory で、
relay 再起動時には消える。これは意図的な設計で、再起動後は購読者の再 subscribe と
heartbeat によって自己修復させる。

### 4.3 at-least-once + 明示 ack

relay は **at-least-once** で配達する。同じメッセージが同じ購読者に複数回届く可能性が
あるため、購読者側は冪等に処理することが期待される。

配達完了の判定は、TCP write が完了したことではなく、**購読者が application-level の
ack を返したこと**で行う。購読者がメッセージを受け取って処理し、ack エンドポイントに
「ここまで受領した」を明示することで、relay はその範囲を outbox から削除する
（cumulative ack）。

これにより、配達中に購読者プロセスが crash しても、ack していない範囲は outbox に
残り、再接続時に再配達される。

### 4.4 resume は outbox の暗黙再 push で解決

SSE 接続が切れた場合、購読者は単に再接続するだけでよい。
購読者は「どこから再開したい」をカーソルとして申告しない（Last-Event-ID のような
仕組みを使わない）。relay 側の per-購読 ack 状態がカーソルそのものになっているため、
再接続時に未 ack の outbox をそのまま再 push すれば取りこぼしが解消する。

retain 期間（既定 24 時間）を超えた取りこぼしは、relay の責務外となる。
その場合は publisher 側の永続真実ストアに直接 pull することで補完する。

> この設計は、「relay は短期の便利な再送装置、長期の真実源は publisher」という
> 責務境界を明示している。

---

## 5. 登場人物

relay を語るときに最低限押さえておきたい用語を、以下に列挙する。

### publisher（パブリッシャ）

メッセージを送り出す主体。stream に投函する場合と、subscription レーンに publish する
場合の 2 系統がある。cc-memory のようなエンティティ管理側がよく publisher になるが、
relay は publisher 種別を限定しない。

### subscriber（サブスクライバ）

メッセージを受け取る主体。subscribe API を呼んで subscription を作り、その後 SSE で
接続して push を受ける。subscriber 種別（エージェント、UI、外部 bot など）を
relay は区別しない。

### stream（ストリーム、機能要件文書内で「場」とも表現される）

名前付きの共有空間。membership（誰が投函できるか、誰が読めるか）を持つ。
stream への投函はそのメンバーに配達される。stream 自体は pass-through で、
relay 内に永続蓄積を持たない。

### subscription（サブスクリプション）

「このラベル集合に興味がある」という関心宣言。subscribe API を呼ぶと relay が
subscription_id を発行し、購読者はそれを保持する。
publisher が publish したメッセージのラベル集合に対して、subscription のラベル集合が
subset としてマッチすれば、その subscription に配達される（AND セマンティクス）。

### outbox（アウトボックス）

購読者ごとに保持される、未 ack メッセージの永続キュー。relay が disk で
永続化するのは outbox のみ。配達済み（ack 受領済み）のエントリは削除される。

### ack（アック）

購読者発の application-level cumulative ack。「`up_to_publish_id` までを受領した」を
relay に明示し、relay はその範囲を outbox から削除する。TCP write 完了は ack ではない。

### DLQ（dead letter queue）

permanent error 状態の outbox エントリを退避する場所。
代表例は、subscription_id が不存在になった（lease 切れ、明示 unsubscribe、
relay 再起動による in-memory registry 消失など）ケース。
DLQ 行きしたエントリは時間で物理 GC され、自然消滅する。

### labels（ラベル）

publish 時と subscribe 時に付与される、relay にとって**不透明な文字列の集合**。
relay はラベルの意味を解釈しない。集合の subset 判定だけを行う。
ラベルの命名規約や意味づけは、relay を使う側のエージェントが決める。

### publish_id（パブリッシュ ID）

relay 全体で単調増加する global な ID。SSE event の `id:` 行に乗り、ack の
cumulative カーソルとして使われ、配達順の比較にも使える。
stream ごと・subscription ごとの個別 seq は持たない。

---

## 6. v1 との断絶

relay v2 は v1 の延長線上にあるのではなく、別物として再設計されている。
主な違いを以下に示す。

| 観点 | v1 | v2 |
|---|---|---|
| 想定通信 | 人間ペアのチャット中継 | エージェント同士の A2A メッセージング |
| 場（チャンネル / stream）の永続蓄積 | あり（history pull 可能） | なし（pass-through） |
| 購読者の同定 | SSE 接続中の人間 | A2A spec 準拠の identity + subscription_id |
| 配達保証 | fire-and-forget 寄り | at-least-once（outbox + retry + retain + ack） |
| identity | SSH + forced command | A2A 1.0 準拠（AgentCard / Bearer / JWS など） |
| 細粒度 authZ | チャネル単位 ACL | relay は持たない（使用側で運用 filter） |
| 移行方針 | — | big-bang cut-over、データは全捨て |

v2 のリリースは、v1 からの段階的マイグレーションを取らない。
データは全捨てされ、API も別物になる。並存運用や dual-write は要件外である。

---

## 7. A2A 1.0 spec への接続

relay v2 の identity / authentication 周りは、A2A 1.0 spec に準拠する。

主な接地点は以下の通りである。

- AgentCard を `/.well-known/agent-card.json` で `application/a2a+json` として公開する
- 認証スキームは securitySchemes で宣言する（最小セットは Bearer token）
- AgentCard の JWS 署名検証（ES256）を任意で行える
- 認可エラー時はリソース存在を露呈しない

DID（Decentralized Identifiers）の統合は A2A 1.0 spec 本体に含まれていないため、
v2 ではスコープ外とする。

詳細は別ドキュメント（identity / authZ 仕様書）を参照する。

---

## 8. relay が「やらないこと」

relay の責務境界を明確にするため、relay がやらないことを以下に明示する。

- メッセージ本文の意味解釈（done 判定、heartbeat 判定など）
- ロール概念の認識（orch / dispatcher / worker など）
- 細粒度 authZ（特定 labels への subscribe 可否、close / cancel の権限判定）
- 関心領域の述語評価（複雑な AND / OR / NOT の合成）
- subscriber identity の経時的同一性管理（再接続 = 新規 subscriber として扱う）
- メッセージ履歴の長期保管（短期 outbox retain と監査用サーバーログのみ）
- multi-tenant の分離（複数組織で共有したい場合は別 relay インスタンスを立てる）
- 並存運用 / dual-write proxy（運用判断で別途検討する）

これらが必要な場合は、relay の上に乗るエージェント（ow など）または publisher 側で
解決する。

---

## 9. 関連ドキュメント

このドキュメントの先に読むものを以下に示す。

- **relay v2 ワイヤ / API 仕様**: HTTP endpoint、payload、status code、配達セマンティクスの詳細
- **relay v2 identity / authZ 仕様**: A2A 1.0 準拠の認証・認可の詳細
- **relay v2 SDK 仕様**: publisher / subscriber 側のクライアント SDK
- **relay v2 シーケンス図集**: publish / subscribe / ack / retry / DLQ の動作を図で追える資料
- **relay v2 グロッサリー**: 用語集の最終版（ユビキタス言語確定後）

上位の設計成果物としては、relay 機能要件文書、relay v2 と cc-memory の責務境界を
まとめた recompose ドキュメントが存在する。

---

## 10. 用語に関する注記

本書執筆時点で、relay v2 のユビキタス言語は確定していない。以下の暫定方針を取る。

- 機能要件文書の用語に揃える
- 「場」と「stream」が併存する場合、本書では原則として **stream** に統一する
- subscription / subscriber / publisher / outbox / ack / DLQ / labels はそのまま英語で表記する

ユビキタス言語確定後、関連ドキュメントを一括置換する予定である。
