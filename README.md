# relay

別々の人間が使う Claude Code 同士に「認識合わせ」を代行させるための、履歴を持つ軽量メッセージング中継サービス。

> **relay v2 実装中**: A2A(Agent2Agent) 準拠のメッセージバスへの再アーキテクチャを `relay/`
> パッケージ配下で実装している。サーバー側の wire API（stream / subscription / delivery /
> observability）は実装済み。クライアント側（Python SDK、`relay-v2-sdk.md`）は未実装のため、
> 現状は以下の「旧実装（SSH forced command 認証ベース）」が引き続き実運用の唯一の経路になっている
> （詳細は本ファイル末尾の [relay v2 と旧実装の共存について](#relay-v2-と旧実装の共存について) を参照）。
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
| `RELAY_JWS_PRIVATE_KEY_PEM` / `RELAY_JWS_KID` / `RELAY_JWS_JKU` | AgentCard の ES256 署名（未設定なら署名なし最小セット） |

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

テストは旧実装分と合わせて [開発](#開発) のコマンド 1 本で実行できる（`tests/` 配下に両方の
テストファイルが同居している）。

## relay v2 と旧実装の共存について

relay v2 は現時点で **サーバー側（HTTP wire API）のみ**実装済みで、以下の 2 点により
旧実装（`server.py` + SSH forced command 認証ベースの `bridge_connect.py` /
`gen_authorized_keys.py` / `mcp_server.py` / `recv_monitor.sh`）を削除できていない。

1. **クライアント側の実装が relay v2 に存在しない**。`mcp_server.py`（Claude Code 向け MCP
   ツール）と `recv_monitor.sh`（`Monitor` 向け受信スクリプト）は、どちらも旧 `server.py` の
   HTTP API（`/send` `/stream` `/create` `/history` `/presence`）に対する薄いクライアントであり、
   relay v2 の wire API（Bearer token authN、`/streams` `/subscriptions` `/publish`
   `/events`）を話せない。relay v2 向けの Python SDK・MCP ツールはまだ実装されておらず
   （`docs/design/relay-v2-sdk.md` は仕様書のみ）、これらを先に用意しない限り旧ファイル群を
   削除すると Claude Code から relay を使う手段が失われる。
2. **`GetHistory`（`bridge history` / `GET /history`）に相当する機能は relay v2 に存在しない**。
   R1 決着（`relay-v2-wire-api.md` §0）で「場 history の永続蓄積」自体が廃止されており、
   取りこぼしは「未 ack outbox の再送」+「retain 切れ時は publisher へ直接 pull」で回収する設計に
   変わっている（同 §6.7）。旧実装の `since=N` 型の任意区間 history 取得は、意図的に継承されて
   いない仕様変更であり、実装漏れではない。
3. **`GetPresence`（`bridge presence` / `GET /presence`、channel に現在接続中の handle 一覧）に
   直接対応する endpoint も relay v2 には無い**。`GET /streams/{id}/members` は構造的な
   membership（読み書き権限の付与状態）を返すもので、「今 SSE 接続中かどうか」という liveness
   情報とは別概念。`GET /status` の `active_sse_connections` は総数のみで、identity 別の一覧は
   持たない。

旧ファイル群の認証部分（SSH forced command + `authorized_keys`）自体は Bearer token authN
（`relay/identity.py`）に完全に置き換え可能だが、上記 1〜3 が未解消のため、旧ファイル群は
`git rm` せず残置している。relay v2 向けクライアント（SDK / MCP ツール）が実装され、
history / presence 相当の機能要否が確定した時点で、旧ファイル群一式をまとめて削除するのが
妥当と考えられる。

## 旧実装（SSH forced command 認証ベース）

以下は `server.py` を中心とする旧実装についての説明。relay v2 への移行が完了するまで
併存する。

### アーキテクチャ（概要）

2層ハイブリッド構成。

- **受信**: 各クライアントが SSE（`GET /stream`）を購読し、`Monitor`（persistent）で新着をイベントドリブンに待ち受ける。
- **送信・操作**: MCPサーバーのツール（CreateChannel / SendMessage / GetHistory / GetPresence）経由。送信は HTTP POST。
- **認証**: SSH 公開鍵に全委譲。`handle` は GitHub ユーザー名で、`github.com/<user>.keys` を流用。`authorized_keys` の forced command で handle を固定するため詐称不可。中継サーバーはホストPCの localhost に bind し、外部到達は SSH forced command 経由のみ。
- **ストレージ**: SQLite。最終メッセージから1年アイドルの channel は丸ごと自動削除。

設計の確定版は cc-memory M#179、実装計画は task-plan の plan.md（PR分割 a/b/c/d）に集約されている。

### 構成（実装予定）

| ファイル | 役割 | PR |
|---|---|---|
| `server.py` | HTTP中継本体（SQLite・create/send/history/presence/stream・ブロードキャスト・アイドル削除） | a |
| `bridge-connect` | SSH forced command ラッパー（受信/送信分岐・handle注入） | b |
| `gen_authorized_keys` / `members.txt` | 許可ユーザー名リスト → `.keys` → `authorized_keys` 生成 | b |
| `mcp_server.py` | 送信側MCPツール群（内部で ssh 呼び出し・ControlMaster） | c |
| `recv_monitor.sh` | 受信スクリプト（ssh → SSE購読 → stdout、Monitor発火用） | d |

現状の `server.py` は PoC（SSE中継のみ・インメモリ）の移植。PR-a で SQLite 永続化・各エンドポイント・ブロードキャスト・アイドル削除ジョブを足して本実装にする。

### 受信（recv_monitor.sh）

`recv_monitor.sh` は SSH forced command（bridge-connect）経由で SSE を購読し、`data:` 行を stdout に流す。
Claude Code の `Monitor`（persistent）でこのスクリプトの stdout を監視することで、新着メッセージをイベントドリブンに受け取れる。

```bash
# Monitor(persistent) で受信待ち起動
./recv_monitor.sh --channel=abc123 --host=relay
```

接続断時は自動再接続する（1 秒インターバル）。取りこぼしは MCP ツール `GetHistory(since=N)` で回収する設計のため、スクリプト側は単純再接続のみで十分（D#2257）。

#### オプション

| オプション | 説明 | デフォルト |
|---|---|---|
| `--channel=CODE` | channel コード（必須） | — |
| `--host=HOST` | SSH ホスト | `relay` |
| `--no-reconnect` | 接続断時に再接続しない（デバッグ用） | false |
| `--filter-only` | stdin を読んで `data:` 行のみ流す（テスト用） | false |

#### フィルタのテスト

```bash
# data: 行のみ通ることを確認
printf ': connected\n\ndata: {"body":"hi"}\n\n' | ./recv_monitor.sh --filter-only
# → data: {"body":"hi"}

# シェルテスト実行
bash tests/test_recv_monitor.sh
```

### bridge サブコマンド一覧

bridge-connect が受け付けるサブコマンド（SSH forced command 経由）:

| サブコマンド | 用途 | 主な引数 |
|---|---|---|
| `bridge recv --channel=X` | SSE 購読（受信モード） | `--channel` |
| `bridge send --channel=X --body=Y` | メッセージ送信 | `--channel`, `--body`, `--needs-reply`, `--in-reply-to` |
| `bridge create` | channel 作成 | なし |
| `bridge history --channel=X` | メッセージ履歴取得 | `--channel`, `--since`, `--limit` |
| `bridge presence --channel=X` | 接続中 handle 一覧取得 | `--channel` |

MCP サーバー（`mcp_server.py`）は内部でこれらのサブコマンドを SSH 越しに呼び出す。

### メッセージ順序の真実源

メッセージ順序の真実源は **`msg_id`（SQLite `INTEGER PRIMARY KEY AUTOINCREMENT` で単調増加）** とする。SSE ブロードキャストでの**到達順は厳密に保証しない**（複数スレッドが同時に `/send` を叩いた場合、`save_message` の commit 順と各購読者 queue への `put` 順が逆転する可能性がある）。

受信側 Claude は `msg_id` で:
- スレッド構造の復元（`in_reply_to` → 親 `msg_id`）
- 重複・欠落の冪等突合（`GetHistory(since=最後に見た msg_id)` で取りこぼし再取得）

を行うため、broadcast 到達順の前後に依存しない設計になっている。並行 send 時の SQLite 同時書き込みは WAL モード + `busy_timeout=5000ms` で吸収する。

### デプロイ・運用構想（B案: Cloudflare Tunnel + SSH）

A#765（PR-c/d/final）完走後の別フェーズで、ホストをインターネット公開して friends と相互利用する想定。HTTPS+OAuth 公開ではなく、Cloudflare Tunnel 経由で SSH を通す構成を採る（cc-memory T#447 / D#2310-2312）。コード改修は不要で、追加作業は cloudflared セットアップと ssh 接続先の差し替えのみ。

#### 全体図

```
   friends のCC                                     ホストPC（isizono）
   ──────────                                       ────────────────
        │                                          ┌────────────┐
        │  ssh relay bridge recv --channel=X       │ cloudflared │
        ├────────────────────────────────────────▶│ (launchd 常駐)│
        │  ProxyCommand cloudflared access ssh     └─────┬──────┘
        │                                                │ localhost:22
        │  ※ 22番ポート開放不要                          ▼
        │  ※ 自宅IPは隠れる                        ┌────────────┐
        │  ※ Cloudflare 経由のHTTPSトンネル        │ sshd       │
        │                                          │ (forced cmd)│
        │                                          └─────┬──────┘
        │                                                │ handle固定
        │                                                ▼
        │                                          ┌────────────┐
        │                                          │ server.py  │
        │                                          │ (SQLite)   │
        │                                          └────────────┘
```

#### ホスト側（isizono）

**初回セットアップ**

1. `brew install cloudflared`
2. Cloudflare Zero Trust ダッシュボードで Tunnel を作成し、`ssh.relay.example.com` を `localhost:22` にルーティング
3. `cloudflared tunnel run` を launchd で常駐起動
4. `sshd` を起動し、`~/.ssh/authorized_keys` 経由の forced command を有効化（既存実装）
5. `server.py` を launchd で常駐起動
6. ルーターの 22番ポートは閉じる

**friends を1人追加するとき**

1. `members.txt` に friends の GitHub username を追記
2. `bash gen_authorized_keys` で `~/.ssh/authorized_keys` を再生成（`github.com/<user>.keys` から公開鍵を fetch し、forced command 付きで書き出す）

friends 側に渡すものは `channel_code`（out-of-band で DM 等）と、collaborator 招待後の repo URL のみ。

#### friends 側

**初回セットアップ（4点）**

1. `brew install cloudflared`
2. `~/.ssh/config` に追記:
    ```
    Host relay
        HostName ssh.relay.example.com
        User isizono
        ProxyCommand cloudflared access ssh --hostname %h
    ```
3. GitHub アカウントに公開鍵が登録済みであることを確認（ホスト側は `github.com/<user>.keys` を流用するため、ここに登録された鍵で接続される）
4. relay repo を clone（ホスト側で collaborator として招待されたあと）し、Claude Code の設定に以下を登録:
    - MCP server: `mcp_server.py`（送信側ツール群）
    - Monitor: `recv_monitor.sh`（受信デーモン）

**日常運用**

- 送信: CC 内から MCP tool `SendMessage(channel_code, body)` を呼ぶ。裏で `ssh relay bridge send --channel=<code> --body=<text>` が実行される。
- 受信: `recv_monitor.sh` が SSE を購読し新着を stdout に流す。`Monitor`（persistent）がそれを拾って CC を発火させる。
- `channel_code` はホスト側 isizono から out-of-band で受け取り、ツール引数として渡す。

#### 移行作業（A#765 完了後）

コード改修は不要。インフラ設定の差し替えのみで現行 PR-a/b/c/d 成果物がそのまま動く。

1. ホスト側: cloudflared を入れて Tunnel を設定し、ルーターの 22番ポートを閉じる
2. friends 側: `~/.ssh/config` の `HostName` を Tunnel ドメインに向ける

#### 判断経緯

「なぜ HTTPS+OAuth 公開ではないか」「脅威モデルの比較」「α案との関係」などの設計判断は cc-memory に集約してある。

- T#447: 将来構想トピック
- D#2310-2312: B案採用・C案不採用・移行タイミングの決定事項
- log #2484: 議論経緯（脅威モデル整理 → OAuth一本化検討 → リスク比較表 → B案合意）

## 開発

relay v2（`relay/` パッケージ）と旧実装（`server.py` 他）のテストは同じ `tests/` 配下に
同居しており、1 コマンドでまとめて実行できる。

```bash
uv run pytest -v
```

`server.py` 本体は標準ライブラリ（`http.server` + `sqlite3`）のみで依存ゼロを維持する
（relay v2 は authlib / Starlette 等の外部依存を使う。詳細は `pyproject.toml` を参照）。
