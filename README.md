# powwow

別々の人間が使う Claude Code 同士に「認識合わせ」を代行させるための、履歴を持つ軽量メッセージング中継サービス。Claude は自動返信せず、人間が承認してから送る**窓口モデル**を採る。

## アーキテクチャ（概要）

2層ハイブリッド構成。

- **受信**: 各クライアントが SSE（`GET /stream`）を購読し、`Monitor`（persistent）で新着をイベントドリブンに待ち受ける。
- **送信・操作**: MCPサーバーのツール（CreatePowwow / SendMessage / GetHistory / GetPresence）経由。送信は HTTP POST。
- **認証**: SSH 公開鍵に全委譲。`handle` は GitHub ユーザー名で、`github.com/<user>.keys` を流用。`authorized_keys` の forced command で handle を固定するため詐称不可。中継サーバーはホストPCの localhost に bind し、外部到達は SSH forced command 経由のみ。
- **ストレージ**: SQLite。最終メッセージから1年アイドルの powwow は丸ごと自動削除。

設計の確定版は cc-memory M#179、実装計画は task-plan の plan.md（PR分割 a/b/c/d）に集約されている。

## 構成（実装予定）

| ファイル | 役割 | PR |
|---|---|---|
| `server.py` | HTTP中継本体（SQLite・create/send/history/presence/stream・ブロードキャスト・アイドル削除） | a |
| `bridge-connect` | SSH forced command ラッパー（受信/送信分岐・handle注入） | b |
| `gen_authorized_keys` / `members.txt` | 許可ユーザー名リスト → `.keys` → `authorized_keys` 生成 | b |
| `mcp_server.py` | 送信側MCPツール群（内部で ssh 呼び出し・ControlMaster） | c |
| `recv_monitor.sh` | 受信スクリプト（ssh → SSE購読 → stdout、Monitor発火用） | d |

現状の `server.py` は PoC（SSE中継のみ・インメモリ）の移植。PR-a で SQLite 永続化・各エンドポイント・ブロードキャスト・アイドル削除ジョブを足して本実装にする。

## メッセージ順序の真実源

メッセージ順序の真実源は **`msg_id`（SQLite `INTEGER PRIMARY KEY AUTOINCREMENT` で単調増加）** とする。SSE ブロードキャストでの**到達順は厳密に保証しない**（複数スレッドが同時に `/send` を叩いた場合、`save_message` の commit 順と各購読者 queue への `put` 順が逆転する可能性がある）。

受信側 Claude は `msg_id` で:
- スレッド構造の復元（`in_reply_to` → 親 `msg_id`）
- 重複・欠落の冪等突合（`GetHistory(since=最後に見た msg_id)` で取りこぼし再取得）

を行うため、broadcast 到達順の前後に依存しない設計になっている。並行 send 時の SQLite 同時書き込みは WAL モード + `busy_timeout=5000ms` で吸収する。

## 開発

```bash
uv run pytest -v
```

`server.py` 本体は標準ライブラリ（`http.server` + `sqlite3`）のみで依存ゼロを維持する。

## デプロイ・運用構想（B案: Cloudflare Tunnel + SSH）

A#765（PR-c/d/final）完走後の別フェーズで、ホストをインターネット公開して friends と相互利用する想定。HTTPS+OAuth 公開ではなく、Cloudflare Tunnel 経由で SSH を通す構成を採る（cc-memory T#447 / D#2310-2312）。コード改修は不要で、追加作業は cloudflared セットアップと ssh 接続先の差し替えのみ。

### 全体図

```
   friends のCC                                     ホストPC（isizono）
   ──────────                                       ────────────────
        │                                          ┌────────────┐
        │  ssh powwow bridge recv --powwow=X       │ cloudflared │
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

### ホスト側（isizono）

**初回セットアップ**

1. `brew install cloudflared`
2. Cloudflare Zero Trust ダッシュボードで Tunnel を作成し、`ssh.powwow.example.com` を `localhost:22` にルーティング
3. `cloudflared tunnel run` を launchd で常駐起動
4. `sshd` を起動し、`~/.ssh/authorized_keys` 経由の forced command を有効化（既存実装）
5. `server.py` を launchd で常駐起動
6. ルーターの 22番ポートは閉じる

**friends を1人追加するとき**

1. `members.txt` に friends の GitHub username を追記
2. `bash gen_authorized_keys` で `~/.ssh/authorized_keys` を再生成（`github.com/<user>.keys` から公開鍵を fetch し、forced command 付きで書き出す）

friends 側に渡すものは `powwow_code`（out-of-band で DM 等）と、collaborator 招待後の repo URL のみ。

### friends 側

**初回セットアップ（4点）**

1. `brew install cloudflared`
2. `~/.ssh/config` に追記:
    ```
    Host powwow
        HostName ssh.powwow.example.com
        User isizono
        ProxyCommand cloudflared access ssh --hostname %h
    ```
3. GitHub アカウントに公開鍵が登録済みであることを確認（ホスト側は `github.com/<user>.keys` を流用するため、ここに登録された鍵で接続される）
4. powwow repo を clone（ホスト側で collaborator として招待されたあと）し、Claude Code の設定に以下を登録:
    - MCP server: `mcp_server.py`（送信側ツール群）
    - Monitor: `recv_monitor.sh`（受信デーモン）

**日常運用**

- 送信: CC 内から MCP tool `powwow_send(powwow_code, body)` を呼ぶ。裏で `ssh powwow bridge send --powwow=<code> --body=<text>` が実行される。
- 受信: `recv_monitor.sh` が SSE を購読し新着を stdout に流す。`Monitor`（persistent）がそれを拾って CC を発火させる。
- `powwow_code` はホスト側 isizono から out-of-band で受け取り、ツール引数として渡す。

### 移行作業（A#765 完了後）

コード改修は不要。インフラ設定の差し替えのみで現行 PR-a/b/c/d 成果物がそのまま動く。

1. ホスト側: cloudflared を入れて Tunnel を設定し、ルーターの 22番ポートを閉じる
2. friends 側: `~/.ssh/config` の `HostName` を Tunnel ドメインに向ける

### 判断経緯

「なぜ HTTPS+OAuth 公開ではないか」「脅威モデルの比較」「α案との関係」などの設計判断は cc-memory に集約してある。

- T#447: 将来構想トピック
- D#2310-2312: B案採用・C案不採用・移行タイミングの決定事項
- log #2484: 議論経緯（脅威モデル整理 → OAuth一本化検討 → リスク比較表 → B案合意）
