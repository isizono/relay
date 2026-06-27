# 論点#3 決着 — 場 history retention + substrate engine 確定

> **このファイルの用途**: 本セッション（2026-06-27）の cc-memory MCP が切断していて記録できなかったため、
> 決着内容と議論ログを単一 MD に退避したもの。**別セッションでこの MD をコピペ or pull して cc-memory に取り込む**こと。
> 取り込み手順は末尾「§7 別セッションでの取り込み手順」を参照。
>
> - topic: 474（ow と relay の責務境界整理）
> - domain: `domain:relay`, `domain:ow`
> - 前提ハンドオフ: M#525 / 監査 M#524 / 基盤決定 L#3135・L#3137(core-1)

---

## §0. TL;DR（3行）

1. **substrate = B 確定**: relay は disk(SQLite) を持つ。守るのは **outbox（未配達 in-flight）だけ**。他は in-memory で自己修復。
2. **場 history = 完全廃止**: 永続性を outbox 1本に畳む。代わりに relay デバッグ用 **サーバーログ**（append-only / payload 込み / 90日 / 購読者IFなし）を持つ。
3. **SSE resume = outbox 暗黙再 push**: `history?since=N` は削除。relay が未 ack outbox を黙って再 push する。

---

## §1. 出発点（前セッション終端の現在地）

- **L#3135**: relay は永続真実を所有しない＝bounded transport buffer。真実は全部 cc-memory にエージェントが意図的に書く。
- **L#3137 (core-1)**: 場 history の最大消費者だった「ow 状態再構成」が消えた。耐久的事実＝cc-memory pull、liveness＝relay 直近窓だけ。
- **監査 M#524 の lean**: substrate は B（relay が disk）優位、A（in-memory）は構造的に不可。engine（LMDB vs SQLite）は SQLite 継続・詰まったら LMDB。
- **未決だったのが論点#3**: 場 history の実 retention。これが「disk 必須か in-memory 許容か」の最終決め手という整理だった。

---

## §2. 議論で踏んだ再フレーム（経緯ログ・退場記録）

### 2-1. 因果の向きを正した
ハンドオフは「retention の数字 → A vs B が決まる」と書いていたが、読み直すと**因果が逆**。
retention 数字は表面で、その下に substrate を実際に決める問い＝**「relay は自分の再起動を無損失で生き延びる必要があるか」**が埋まっていた。

### 2-2. 場 history の消費者を core-1 後に棚卸し
| 場 history の用途 | core-1 後 |
|---|---|
| ow 状態再構成 | ❌ 消滅（cc-memory pull に移行） |
| サルベージ/post-mortem | ❌ git WIP commit + cc-memory salvage log を使う、場 history は読まない |
| outbox retain 内の取りこぼし回収 | → これは **outbox(24h)** の仕事。場 history ではない |
| SSE reconnect resume（`since=N`） | △ 残るが、必要窓は秒〜分、かつ後述で outbox に吸収 |

→ 長期 archive(90日) には実消費者が居ない。

### 2-3. A' という変種を検討して棄却
- **案A（監査の正規案）**: relay disk ゼロ、保証は送信側 e2e ack。→ 監査で「構造的に不可」（fan-out を知るのは relay だけ、publisher は件数しか受け取らない FR-3.6 → per-subscriber e2e ack を追えない）。
- **A'（本セッションで持ち出した造語の変種）**: relay disk ゼロだが保証を**購読者側 pull 照合 + cc-memory**に置く。案A の死因（送信側 ack）を回避でき、L#3135/D#3018 に純化して最もシンプル。
- **A' 棄却理由**: at-least-once の保証を「各購読者の pull ループの信頼性」に全賭けする。が、その pull ループは ow で**実際に壊れている**（domain:ow tag_note の観測症状: Monitor reactive 死=症状5 / truncated body で done 見落とし=症状6 / heartbeat 途絶=症状1）。**保証を最も壊れやすいリンクに移すのが致命**。
  - 補足: 監査が挙げた B 根拠（#2 e2e ack / #5 lease 消失）は A' では溶けるが、この「観測事実ベースで A' を殺す」より強い根拠が立った。

### 2-4. ユーザー裁定で B 確定
ユーザー「アウトボックスは持つとして、B で」。→ substrate = B 確定。

### 2-5. 「身分データは持つ必要があるか」を詰めた
relay が抱える非メッセージ群（presence / subscription registry / lease / identity→role 束縛）は全部 **liveness クラス = 自己修復**。
relay 再起動で飛んでも、購読者の re-subscribe（idempotent）+ heartbeat で数秒で再構築される。
→ **disk が要るのは outbox だけ。残りは in-memory（運用中 RAM には持つが永続化不要）。**

### 2-6. 場 history を完全廃止に決定
ユーザー「場ヒストリーはなしにしようか」。
- at-least-once は outbox だけで閉じる（publish→fan-out マッチ→購読ごと outbox→配達→ack→破棄）。場 history は保証に非関与。
- 場 history が足していたのは backscroll と ack 済み再読のみ＝L#3135 で非ケース。
- → 監査 発見1 の「永続性2機構（場 history と outbox）の混同」が **outbox 1本**に畳まれて解消。

### 2-7. サーバーログは別物として残す
ユーザー「サーバーログはとるでいいかも？リレーのデバッグ用に。90日で消える」。
- **場 history（データ機構, source, 配達保証に関与）** と **サーバーログ（observability sink, 配達非関与）** は完全別物。
- サーバーログ＝append-only sink、payload 込み、TTL 90日、**購読者向けエンドポイントなし**（`since=N` を生やすと裏口から場 history が復活するので禁止）。FR-8 observability の領分。outbox の SQLite とは物理分離。
- さまよっていた「90日」がここで本当の住処を見つけた（場 archive 90日からの移設）。

### 2-8. resume の since=N を再検討して完全削除に倒れた
ユーザー「その心は？」をきっかけに自己訂正:
- `since=N` は「N より後ろを再生できる台帳」があって初めて意味を持つ。その台帳＝場 history を消した以上、引く先が無い。
- resume は「購読者が seq を申告」ではなく「**relay が当該購読の未 ack outbox を黙って再 push**」に。relay 側 per-購読 ack 状態がカーソルそのもの。
- since は将来の重複抑制ヒントとして optional 余地のみ（今は作らない、at-least-once + 冪等購読者で吸収）。

---

## §3. 確定事項（cc-memory に decision として記録するもの）

### D-1: relay substrate = B（disk=SQLite、守るのは outbox だけ）
**decision**:
relay substrate は B（relay が disk=SQLite を持つ）に確定。disk で守る対象は **outbox（未配達 in-flight メッセージ）だけ**。
presence / subscription registry / lease / identity→role 束縛 は liveness クラスで in-memory に置き、relay 再起動は購読者の re-subscribe(idempotent) + heartbeat で自己修復するため disk 不要。
engine は SQLite 継続。LMDB は「SQLite が実際に詰まる証拠が出たら」検討する保留オプション（過剰最適化回避、監査 M#524）。
**reason**:
at-least-once は outbox だけで閉じる。relay は真実を持たない transport(L#3135)、liveness は直近窓のみ(L#3137 core-1)。案A は監査で構造的に不可（送信側 e2e ack が fan-out を追えない）。A'(in-memory + 購読者 pull 保証) は保証を flaky な購読者 pull ループ（観測症状 1/5/6）に移すため不採用。ユーザー裁定「アウトボックスは持つとして、B で」。
**tags**: `domain:relay`, `domain:ow`, `intent:design`, `substrate`, `transport-buffer`, `responsibility-boundary`, `at-least-once`, `state-restoration`

### D-2: 場 history 完全廃止 / relay デバッグ用サーバーログ（90日）を別途持つ
**decision**:
場 history（チャンネル生ログ、購読者が API で読むデータ機構）は**完全廃止**。relay の永続性は outbox 1本に畳む（監査 発見1 の「2機構混同」を解消）。
別途、**relay 自身のデバッグ用サーバーログ**を持つ: append-only sink、payload 込み、TTL 90日でローテ GC、**購読者向けエンドポイントなし**（`since=N` 不可）。FR-8 observability の領分。outbox の SQLite とは物理分離。
**reason**:
配達保証(at-least-once)は outbox のみで成立し、場 history は保証に非関与だった。場 history が足していたのは backscroll と ack 済み再読のみで、どちらも L#3135（真実は cc-memory、エージェントは揮発、後継は遡らず再構成）で非ケース。デバッグ便宜は observability の sink で代替でき、それは配達経路に依存しないため「永続性2機構」問題を再発させない。さまよっていた 90日 はこのサーバーログに移設。ユーザー裁定「場ヒストリーはなし」「サーバーログはとる、90日で消える」。
**tags**: `domain:relay`, `domain:ow`, `intent:design`, `substrate`, `observability`, `responsibility-boundary`, `transport-buffer`

### D-3: SSE 再接続 resume = outbox 暗黙再 push（`history?since=N` 削除）
**decision**:
`history?since=N`（FR-1.4）は**完全削除**。場 history を消した世界では since=N の引く先（再生台帳）が存在しない。
resume は「relay が当該購読の未 ack outbox を黙って再 push」する動作に置換。購読者はカーソルを申告しない。relay 側の per-購読 ack 状態がカーソルそのもの。
since は将来の重複抑制ヒントとして optional 余地のみ残す（今は作らない）。
**reason**:
台帳（場 history）を消したので since=N は引く先を失う。outbox は未 ack 分を per-購読で保持しており、relay は何が未配達かを自分で知っているため、購読者の seq 申告なしに再 push できる。重複は at-least-once + 冪等購読者で吸収。
**tags**: `domain:relay`, `intent:design`, `substrate`, `at-least-once`, `transport-buffer`

---

## §4. relay の最終形（B の実像）

| データ | disk（永続・再起動を生存） | in-memory（揮発・名乗り直しで再構築） |
|---|---|---|
| **outbox**（未配達メッセージ） | ✅ ここだけ | |
| presence / subscription / lease | | ✅ |
| identity→role 束縛 | | ✅ |
| 場 history | ❌ 廃止 | ❌ 廃止 |
| **サーバーログ**（デバッグ用 sink, 90日） | ✅（observability, outbox とは物理分離） | |

**relay = SQLite に outbox だけ守り、残りは揮発して自己修復する薄い transport + 観測用サーバーログ。**

「車輪の再発明では？」への整理: 汎用ブローカー（NATS 等＝案C）は規模に対し過剰で監査棄却済み。relay 固有のグルー（label-subset fan-out マッチング FR-3.3 / 3系統 seq / ow role 紐づき lease / cc-memory 協調）は broker を入れても自分で書く。relay は「SQLite(永続)+SSE(push) という標準部品の上に被せた薄く読める自前アダプタ(~300行 Python)」であって、フルスクラッチ broker ではない。自己メンテ性（Claude が一読して直せる）がこの規模では broker 機能より価値が高い。

---

## §5. M#507 改訂リスト（後続作業 / 実装計画 T0 の前段）

- **FR-1.5**（場 archive 90日）→ 削除。90日はサーバーログ(FR-8)へ移設
- **FR-1.1**（場の時系列永続蓄積）→ 削除。場は pure pass-through、in-flight のみ outbox
- **FR-1.4**（`history?since=N`）→ 削除。resume は outbox 暗黙再 push に置換
- **FR-8**（observability）→ サーバーログ追記（append-only / payload 込み / 90日 TTL / 購読者IFなし / outbox と物理分離）
- **3系統 seq**（場ごと / subscription ごと / publish グローバル）→ 場ごと seq の resume 用途消失で畳める可能性。実装計画で精査（本セッションでは未決）
- **C-2(D#2654-2657)** と **domain:ow tag_note「relay 履歴=ow が持つ唯一の永続データ」** → 改訂（既存の宿題、M#524/M#525 既出）

---

## §6. 残る直交論点（substrate と独立、持ち越し）

- **#4 SSE 多重化 resume の穴**（FR-3.8 publish_seq vs subscription_seq の Last-Event-ID、M#507 §6.3 残置）。全 substrate 案で消えない。
- **#7 第2層（生きたコンテキスト記録）**＝orch Layer 2、A#1189(動的文脈 curation)で別途進行中。substrate と分離可能。
- **lease(D#3025)** と確定 substrate の相互作用の精査。

---

## §7. 別セッションでの取り込み手順（cc-memory 正常なセッションで実施）

1. **decision 3本を登録**: §3 の D-1 / D-2 / D-3 を `add_decisions` で topic 474 に追加（各 decision/reason/tags は §3 の通り）。
2. **M#525 ハンドオフを更新**: `update_material(525, ...)` で「次は論点#3」→「**論点#3 決着済**（D-1/D-2/D-3、本 MD §3）。次は M#507 改訂 + C-2/tag_note 後始末 → 実装計画 T0-T7」に書き換え。
3. **退場ログを 1 本残す**: 本セッションの経緯（§2）を log として topic 474 に記録（タイトル例: 「論点#3 決着 — 場 history 廃止 / substrate=B / resume=outbox暗黙再push（cc-memory切断によりMD退避経由で取り込み）」）。
4. （任意）本 MD への参照を残したい場合、relay リポの `docs/design/topic474-論点3-場history-substrate-決着.md`（ブランチ `claude/ba-history-retention-substrate-wshdew`）を source に明記。

> 注: 本セッションは cc-memory MCP 切断のため上記を実行できず、この MD をリモートに push して引き継ぐ。
