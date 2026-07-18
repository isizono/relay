# relay federation 設計 — 2026-07-04 探索の記録

> **位置づけ**: 2026-07-04 のセキュリティ監査への対応から派生した「異なる peer 間の Claude
> 通信」の設計探索。**確定仕様ではなく、次セッションで中身を詰めるための叩き台**である。今日は
> 調査・案出しまでを行い、方針の地図を残す。詳細裁定と実装は次セッションに送る。
>
> **一次資料**: 監査対応提案 v2 / net-thinker による設計案出し全文 /
> federation×登録制の確定 / 議論経緯。本書は
> それらを 1 枚に統合した読み物である。

---

## 0. 今日決まったこと（確定）

- **topology = federation 型**: 各 peer が自分の relay を持ち、relay 同士が連合する。中央の共有
  relay に全員が繋ぐ集権型は採らない。動機は認証上の必然ではなく「主権分散・単一障害点を作らない」
  という設計美学（登録制なら共有 relay 1 個でも認証は成立するため、federation は認証のためではない）。
- **peer = 登録制の知り合い**: 事前に相手を登録した者のみ接続可能。完全な他人（オープンな
  cross-org）は対象外。
- **招待は URL 方式**: 招待 URL に relay endpoint + 一回性トークン + 招待側の鍵 fingerprint を載せる。

この確定により認証設計が大幅に簡素化される。登録制なので「登録＝相手の裏書き」が成立し、事前登録
した相手の公開鍵で真正性を確認すれば足りる。完全な他人向けの did:web / 公的 PKI は不要で、外部
AgentCard を非信頼な jku から取得・検証する経路（監査 High の H-1/H-2、SSRF の温床）も回避できる。

---

## 1. 出発点：セキュリティ監査への対応提案

セキュリティ監査で出た全 finding を「異なる peer 通信」の観点で再配置した提案が
ある。核心は次の一点に集約される。

**「異なる peer 通信」の本質は、relay の信頼の公理が反転すること。** 現状は「認証が通った相手は
信頼できる仲間」（単一オペレータが token を配る身内モデル）。peer 世界では「認証は通ったが、信頼は
できない相手」になる。

この反転で、監査 finding は 3 系統に割れる。

| 系統 | 意味 | 代表 finding |
|---|---|---|
| **(A) identity 確立の必要条件** | 相手の identity を確立する仕組みを入れた瞬間に発生 / v1 無認証を露出した瞬間に致命化 | 旧 v1 server.py の無認証（退役で消滅）、H-1/H-2（外部カード検証を配線する場合のみ） |
| **(A') 相転移で新規に生じる詐称・妨害** | 身内世界に存在しない能動攻撃。認証は通るが悪意ある相手が可能にする | stream 名前空間の横取り、idempotency 抑止（他 peer の publish を握り潰す）、認証チャネルの詐称 |
| **(B) 悪化する既存穴** | 穴の性質は身内でも同じ。攻撃者が現実化して実害が顕在化するだけ | DoS 群、TLS 未明示、observability・列挙の偵察面 |

実際に *相* を変えるのは (A) と (A') であり、(B) は連続的に悪化するだけである。

---

## 2. 設計の全体像（net-thinker による案出し）

上記の提案を土台に、federation の具体設計を思考特化の案出しで詰めた。5 テーマの
推奨は以下（すべて設計判断・推測であり、次セッションで裁定する）。

### 2.1 identity の階層 — 信頼はマシン鍵 1 つ、宛先は `sub@peer`

1 台の PC で複数の Claude（orch / worker / 作業セッション）が動く。federation の identity を
どう構造化するかで、2 つの案が対立していた。

- 案X（各 Claude が AgentCard を持ち、対外通信で peer 情報を付加する）
- 案Y（マシン = 1 AgentCard、内部の Claude は relay 内部の sub-identity）

**推奨は案 Y。** 決定的な論拠は **fate-sharing** である。同一マシン上の全 Claude は同じ OS・同じ
ユーザー・同じディスクを共有し、侵害境界が同一である（1 つの鍵を読める攻撃者は全部読める）。
マシン内で鍵を分けても隔離はほぼゼロなので、暗号の境界は侵害の境界に一致させるべきであり、信頼の
単位はマシン鍵 1 つになる。業界前例も揃っている（Matrix は homeserver 単位の鍵で `@user:server` を
署名、メールの DKIM はドメイン鍵、XMPP の s2s もサーバー単位）。

ただし「対外で付加情報を付ける」というユーザーの直感は正しく、それは案 X ではなく **命名の階層化**
として実現される。対外 identity を `<sub>@<peer>`（例 `orch@peer-a`）の 2 階層にし、`sub` は relay が
認証済みローカル token から機械的に刻印する（body の自己申告は無視・上書き）。inbound は namespace を
強制する（peer B から来たメッセージの from は必ず `*@peer-b` に強制。メールの SPF alignment と同型）。
これで「B が別 peer を名乗る」cross-peer 詐称が構造的に不可能になる。暗号鍵は peer あたり 1 本のまま。

対外に見せる面は role 名（`orch` など安定した名前）を既定とし、短命の worker は外から隠す。将来
「どの Claude が言ったか」を跨 relay で暗号学的に帰属する要件が出たら、マシン鍵が短命 sub-credential を
署名発行する階層証明を後付けできる（Matrix が実際に辿った進化順序と同じ）。

### 2.2 ネットワークメタファ — アドレスは借用、配達と信頼は email/Matrix が正確

「IP の内と外の仕組みを借りる」という発想は有効だが、**借りてよい所とダメな所が明確に分かれる**。

| ネットワーク概念 | relay federation での対応 | 判定 |
|---|---|---|
| プライベート IP | マシン内 Claude の内部 identity | ✅ ハマる |
| パブリック IP | relay の対外 endpoint + peer identity | ✅ ハマる（identity/locator 分離の注意付き） |
| ポートフォワーディング | role 公開表（外部 `orch` → 内部の現セッション） | ✅ ハマる |
| エフェメラルポート | 短命 worker への一時セッションアドレス | ✅ ハマる |
| NAT（本体） | relay が内部 identity ↔ 対外表現を変換 | ⚠️ 部分的（relay は透過的書換器でなく認証終端する門番。SMTP MTA が正確） |
| DNS | peer の discovery | ⚠️ 歪む（招待 URL が代替。グローバル名前解決は不要） |
| STUN / hole punching | — | ❌ 破綻（HTTP/TCP で成功率が低く CGNAT でほぼ不成立） |
| オーバーレイ（Tailscale 等） | 到達性の外注 | ✅ デプロイ選択肢として（設計に取り込まず下に敷く） |
| **SMTP/email（MTA・store-and-forward・DKIM/SPF）** | **配達・信頼構造** | ✅✅ 最もハマる |
| **Matrix/XMPP federation** | homeserver = relay | ✅✅ 最もハマる |

**借りるべき最大の教訓は逆方向にある**: IP の最大の失敗は「アドレスに identity と locator を重畳
させた」ことで、その解消に数十年かかった。relay では最初から **peer identity = pin 済みの鍵、
locator = endpoint URL** を別物として持つ。こうすると ngrok で URL が変わっても鍵（identity）は不変で
吸収できる。そして offline 時の store-and-forward は IP の語彙では書けず、メール（MTA 間 store-and-
forward、DKIM ≒ pin 鍵、`user@domain` ≒ `sub@peer`）の語彙で自然に書ける。

### 2.3 reachability — 「方向非依存の peer link」

federation の直接接続には、少なくとも片方の relay が公開到達可能である必要がある。全員が NAT 内だと
誰も繋げない。ここを次のように解く。

**プロトコルを「方向非依存の peer link」（A–B の論理チャネル）として定義する。** 物理的には
(i) 相手 endpoint への直接 dial（相手が到達可能なとき）、(ii) 到達可能な側が accept し NAT 内の側が
outbound で張る逆方向チャネル、のどちらでも成立する。この逆方向チャネルは **既存の SSE / outbox /
ack 機構とほぼ同型**なので再利用度が高い。

「誰がホストか」をプロトコルに焼き付けないので、主権分散の美学がプロトコル層で保たれる。トンネル
（ngrok 等）やオーバーレイ（Tailscale 等）は「その下に敷けるデプロイ選択肢」に降格する（トンネルは
到達性の主権を事業者に渡す隠れた集権なので既定にはしない）。store-and-forward は送信側の relay が
自分の outbox に持つ（送信者主権）。実装上は既存 outbox の `target_type` に `peer` レーンを足す形が
素直に乗る。ただし peer link は liveness ではなく credential クラスなので disk 永続にする。

### 2.4 招待フロー — pull → push の 1 往復で双方向 pin

```
[発行]   A が invite {token(128bit), expiry, memo} を生成し、
         人間チャネル(Signal/メール等)で B に招待 URL を渡す
         URL = endpoint(locator) + A の鍵 fingerprint + token（token は URL fragment に置く）

[B: pull] B が A の公開 AgentCard を GET → カードの鍵と URL 内 fingerprint を照合
          （招待 URL を運んだ人間チャネルの真正性が、そのまま信頼の根になる）

[B: push] B が自己署名 AgentCard を作り、redemption を POST
          署名対象に {token, timestamp, A の fingerprint} を含める（リプレイ防止）

[A: 検証] A が token を原子的に check-and-mark（一回性）→ カード自己署名を検証（鍵所持証明のみ）
          →（推奨）人間承認「Bob と名乗るマシン(fp:XXXX)を登録する?」→ B の鍵を pin

[応答]   A が自分の署名付きカード・peer handle・逆方向チャネル接続先を返す
         → B が A の鍵を pin（照合済み）→ 双方向 pin 成立
```

1 往復で双方向の pin が成立する（相互招待は不要）。漏洩対策は 3 つのノブで持つ: 一回性（漏洩 URL の
後追い使用は失敗し、それ自体が検知シグナルになる）/ 短命（有効窓を限定）/ 人間承認（漏洩を「即侵害」
から「見知らぬ fingerprint の承認要求」に降格。fingerprint を帯域外で照合すれば実質無効化）。token を
URL fragment に置き redemption を POST 限定にするのは、チャットのリンクプレビュー bot が token を
消費する実世界の事故を防ぐため。

鍵ローテーションは 2 経路を峻別する。無侵害時は旧鍵で署名した継続（新旧併用の猶予付き）、**侵害時は
必ず再招待にフォールバック**する（旧鍵署名は攻撃者にもできるため、混ぜると自動 rotation が乗っ取り
経路になる）。登録解除はローカルの一方的操作で完結させる（相手が offline でも解除できる）。

### 2.5 セキュリティ — 「鍵来歴不変条件」で H-1 を構造的に封じる

監査で最も深刻だった H-1（非信頼な jku から鍵を取ってカード自身を検証し、署名検証を無意味化する）
は、次の一文を仕様に固定すれば構造的に消える。

> **pin される検証鍵がシステムに入る経路は (1) 招待 redemption と (2) pin 済み旧鍵で署名された
> rotation の 2 つのみ。カード内の URL（jku）を辿って鍵を取得する経路は federation に存在しない。
> fetch したカードによって pin 済み鍵を更新しない。**

要は SSH の known_hosts と同じ意味論で、**鍵が変わったら「更新」ではなく「警報」**として扱う。招待
フローで push されたカードの自己署名検証は H-1 ではない（信頼の由来は署名ではなく token の一回性と
人間承認であり、署名は鍵所持証明の役割しか負わない）。ただし再来経路が 3 つあるので明示的に禁止
する: ①カードの TTL refresh で鍵を静かに上書きする（現行 `agent_cards.py` のキャッシュ発想を鍵に
流用すると即再来）、②未使用の `fetch_jwks` を federation 経路から呼べるようにする、③将来の階層証明
導入時にチェーンの根を pin 外から取る。

H-2（SSRF）は「面が 1 本に縮む」がゼロにはならない。招待 URL の pull と federation の dial（署名付き
locator 更新後の dial 先を含む）という 1 系統に、監査提案の SSRF ガード一式（https 限定・内部 IP
レンジ拒否〔IPv6 含む〕・redirect 無効・解決 IP pin・応答サイズ上限）を適用する。

---

## 3. 監査 finding との接続（この設計が何を塞ぐか）

- **H-1**: 「鍵来歴不変条件」で構造的に封じる（§2.5）。登録制 + 招待 pin なので、そもそも非信頼な
  外部カードを検証する状況が発生しない。
- **H-2**: SSRF ガードの適用面が「任意カードの jku」から「招待 pull + federation dial」に限定される。
- **内部/相互の詐称**（複数 Claude、cross-peer）: relay が from を刻印し、`sub@peer` の namespace
  強制で構造的に排除する（§2.1）。
- **新しい pre-auth 面**: redemption endpoint が唯一の無認証入口になるので、サイズ上限・rate limit・
  不正 token へ 404・fail-closed パースで最小・堅牢にする。
- **偵察面（observability・列挙）**: federation identity には observability の read を出さない。
  resource 名指し read は member 限定 + `404` 秘匿に改訂済み（identity 別書 §2.1）のため、残る
  改訂対象は instance-global read（`GET /status` / `GET /metrics` 等）を「ローカル identity のみ」に
  絞ること（判定材料は identity がローカルか peer かの構造的事実なので、relay=メカ/ow=ポリシーの
  責務境界は壊さない）。
- **cross-peer での既存 finding 悪化**（名前空間横取り・idempotency 抑止）: stream_id を peer 含む
  identity スコープ化し、dedup スコープに `sub@peer` を含める。
- **v1 の open relay 化を最初から禁止**: transitive forwarding（A が受けた C の発言を B に転送）は
  v1 の非目標として明記する（SMTP の open relay の轍を踏まない）。

---

## 4. 未決（次セッションで詰める）

1. **extended AgentCard の内部 roster 範囲**: 登録済み peer に、自分の内部の顔ぶれ（role 一覧）を
   どこまで見せるか。プライバシー勾配のノブ設計。
2. **一時セッションアドレスのプロトコル**: 特定 worker と長く対話を継続するための、エフェメラル
   アドレスの払い出し・回収。
3. **federation レーンの署名形式**: peer 間リクエストを JWS detached で署名するか、都度 mint する
   短命トークンにするか。ローカルレーン（静的 token）と federation レーン（鍵ベース）で強度を分ける
   方針は決まっている。
4. **read endpoint の peer への公開範囲**: 本体仕様の read は「instance-global は authN のみ /
   resource 名指しは member 限定 + `404` 秘匿」に改訂済み（identity 別書 §2.1）。peer identity に
   対して instance-global read（`GET /status` 等）をどこまで開けるかの定義。
5. **全員 NAT 時の既定案内**: 全ペアが NAT 内のとき、トンネル（R1）を手順書で推すか、オーバーレイ
   （Tailscale 等、R4）を推すか。

---

