# セットアップ・運用ガイド

relay をホスト（中継サーバーを動かす人）と friends（接続する人）で使うための手順。

- アーキテクチャ全体像は [architecture.md](architecture.md)
- API 仕様は [api.md](api.md)

## 開発・ローカル動作確認

依存は最小限。`server.py` 本体は標準ライブラリのみ（依存ゼロ）、MCP サーバーのみ `mcp` を使う。

```bash
# テスト一式
uv run pytest -v

# 中継サーバー単体起動（127.0.0.1:8765）
uv run python server.py
```

`recv_monitor.sh` のフィルタ単体テスト:

```bash
# data: 行のみ通ることを確認
printf ': connected\n\ndata: {"body":"hi"}\n\n' | ./recv_monitor.sh --filter-only
# → data: {"body":"hi"}

# シェルテスト実行
bash tests/test_recv_monitor.sh
```

---

## ホスト側セットアップ（isizono）

中継サーバーをインターネット公開して friends と相互利用する構成。HTTPS+OAuth 公開ではなく、Cloudflare Tunnel 経由で SSH を通す（B案）。コード改修は不要で、追加作業は cloudflared セットアップと ssh 接続先の差し替えのみ。

### 全体図

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

### 初回セットアップ

1. `brew install cloudflared`
2. Cloudflare Zero Trust ダッシュボードで Tunnel を作成し、`ssh.relay.example.com` を `localhost:22` にルーティング
3. `cloudflared tunnel run` を launchd で常駐起動
4. `sshd` を起動し、`~/.ssh/authorized_keys` 経由の forced command を有効化（次節で生成）
5. `server.py` を launchd で常駐起動（`127.0.0.1:8765` に bind）
6. ルーターの 22番ポートは閉じる

### friends を1人追加するとき

1. `members.txt` に friends の GitHub username を1行追記

   ```
   # relay 許可メンバーリスト
   alice
   bob
   ```

2. `authorized_keys` を再生成する。`gen_authorized_keys` は `github.com/<user>.keys` から公開鍵を fetch し、forced command 付きで標準出力に書き出す:

   ```bash
   ./gen_authorized_keys > ~/.ssh/authorized_keys
   # 引数省略時は同ディレクトリの members.txt と bridge-connect を使う
   # 明示する場合: ./gen_authorized_keys members.txt /abs/path/to/bridge-connect
   ```

   生成される各行の形:

   ```
   command="<path>/bridge-connect --handle=alice",no-pty,no-port-forwarding,no-X11-forwarding,no-agent-forwarding ssh-ed25519 AAAA...
   ```

   > `members.txt` のユーザー名は GitHub ユーザー名規則で厳格に検証され、不正な行が1つでもあると生成を中断する（コマンドインジェクション防止）。公開鍵が取得できないユーザーは警告を出してスキップされる。

3. friends 側に渡すもの: `channel_code`（out-of-band で DM 等）と、collaborator 招待後の repo URL。

---

## friends 側セットアップ

### 初回セットアップ（4点）

1. `brew install cloudflared`
2. `~/.ssh/config` に追記:

   ```
   Host relay
       HostName ssh.relay.example.com
       User isizono
       ProxyCommand cloudflared access ssh --hostname %h
   ```

   > MCP サーバーは `RELAY_SSH_HOST`（デフォルト `relay`）で接続先を引く。上の `Host relay` エイリアスがその実体になる。

3. GitHub アカウントに公開鍵が登録済みであることを確認（ホスト側は `github.com/<user>.keys` を流用するため、ここに登録された鍵で接続される）
4. relay repo を clone（collaborator 招待後）し、Claude Code の設定に登録:
   - MCP server: `mcp_server.py`（送信側ツール群）
   - Monitor: `recv_monitor.sh`（受信デーモン）

### 日常運用

- **送信**: Claude Code 内から MCP tool `SendMessage(channel_code, body)` を呼ぶ。裏で `ssh relay bridge send --channel=<code> --body=<text>` が実行される。
- **受信**: `recv_monitor.sh` を `Monitor`（persistent）で起動しておく。SSE を購読し新着を stdout に流し、`Monitor` がそれを拾って Claude Code を発火させる。

  ```bash
  ./recv_monitor.sh --channel=abc123 --host=relay
  ```

  接続断時は自動再接続する（1秒インターバル）。取りこぼしは `GetHistory(since=N)` で回収する設計のため、スクリプト側は単純再接続のみ。

- `channel_code` はホスト側 isizono から out-of-band で受け取り、ツール引数として渡す。

---

## 移行作業（既存 PoC からの切り替え）

コード改修は不要。インフラ設定の差し替えのみで現行の成果物がそのまま動く。

1. ホスト側: cloudflared を入れて Tunnel を設定し、ルーターの 22番ポートを閉じる
2. friends 側: `~/.ssh/config` の `HostName` を Tunnel ドメインに向ける

### なぜこの構成か

「HTTPS+OAuth 公開ではなく SSH + Cloudflare Tunnel を採る」「脅威モデルの比較」「移行タイミング」などの設計判断は cc-memory に集約してある（T#447 / D#2310-2312、議論経緯は log #2484）。
