# アーキテクチャ

relay は、別々の人間が使う Claude Code 同士に「認識合わせ」を代行させるための、履歴を持つ軽量メッセージング中継サービスである。Claude は自動返信せず、人間が承認してから送る**窓口モデル**を採る。

このドキュメントは設計上の構造と判断を実装（`server.py` / `bridge_connect.py` / `mcp_server.py` / `gen_authorized_keys.py` / `recv_monitor.sh`）に即して説明する。API の入出力仕様は [api.md](api.md)、セットアップ手順は [setup.md](setup.md) を参照。

## 全体構成（2層ハイブリッド）

受信と送信・操作を別経路に分けた2層構成を採る。

```
  クライアント（friends の Claude Code）              ホストPC（isizono）
  ──────────────────────────────                    ──────────────────
   受信:  recv_monitor.sh ──ssh──▶ bridge recv ─────▶ GET /stream   (SSE 購読)
                  │                                         │
                  └─ Monitor(persistent) が data: 行で発火   │
                                                            ▼
   送信・操作: MCP tool ──ssh──▶ bridge send/create/... ─▶ POST /send 等
                                                            │
                                                       ┌────▼────┐
                                                       │ SQLite  │ relay.db
                                                       └─────────┘
```

- **受信**: 各クライアントが SSE（`GET /stream`）を購読し、`recv_monitor.sh` が `data:` 行を stdout に流す。Claude Code の `Monitor`（persistent）がそれを監視し、新着をイベントドリブンに受け取る。
- **送信・操作**: MCP サーバー（`mcp_server.py`）のツール（CreateChannel / SendMessage / GetHistory / GetPresence）経由。いずれも内部で SSH 越しに `bridge` サブコマンドを呼び、HTTP で `server.py` に到達する。
- **認証**: SSH 公開鍵に全委譲（後述）。
- **ストレージ**: SQLite。最終メッセージから1年アイドルの channel は丸ごと自動削除。

中継サーバー（`server.py`）はホストPCの `127.0.0.1:8765` に bind し、外部到達は SSH forced command 経由のみ。`server.py` 本体は標準ライブラリ（`http.server` + `sqlite3`）のみで依存ゼロを維持する。

## 構成要素

| ファイル | 役割 |
|---|---|
| `server.py` | HTTP 中継本体。SQLite 永続化、5 エンドポイント、SSE ブロードキャスト、アイドル削除ジョブ。`127.0.0.1:8765` に bind。 |
| `bridge-connect` / `bridge_connect.py` | SSH forced command ラッパー。`$SSH_ORIGINAL_COMMAND` を解析して受信/送信/作成/履歴/presence に分岐し、`server.py` を HTTP で叩く。`--handle` を注入する。 |
| `gen_authorized_keys` / `gen_authorized_keys.py` / `members.txt` | 許可ユーザー名リスト（`members.txt`）→ `github.com/<user>.keys` → forced command 付き `authorized_keys` を生成。 |
| `mcp_server.py` | 送信側 MCP ツール群。内部で `ssh` を呼ぶ（ControlMaster で多重化）。 |
| `recv_monitor.sh` | 受信スクリプト。`ssh → bridge recv → SSE` を購読し `data:` 行を stdout へ。`Monitor` 発火用。 |

## 認証モデル（SSH 公開鍵への全委譲）

- `handle` は GitHub ユーザー名。ホスト側は `github.com/<user>.keys` を流用して公開鍵を取得する（ユーザーは GitHub に登録済みの鍵でそのまま接続できる）。
- `gen_authorized_keys` が各鍵行の前に forced command を前置する:

  ```
  command="<path>/bridge-connect --handle=<user>",no-pty,no-port-forwarding,no-X11-forwarding,no-agent-forwarding <key>
  ```

- `--handle` が **forced command 側に焼き込まれる**ため、クライアントは自分の handle を詐称できない。`bridge_connect.py` は `$SSH_ORIGINAL_COMMAND` 内の `handle` 指定を明示的に無視する（D#2285）。
- `channel_code` は forced command には焼かず、`$SSH_ORIGINAL_COMMAND` から取得する（D#2302）。これにより1つの鍵で任意の channel を扱える。
- `members.txt` のユーザー名は GitHub ユーザー名規則（英数字とハイフン、1〜39文字）で厳格にバリデートし、不正な行があれば生成を中断する。これは `authorized_keys` 経由のコマンドインジェクション防止のため（D#2285）。

中継サーバー自体は handle を検証しない。アクセス経路が SSH forced command に限られ、そこで handle が固定されるため、サーバー側での再検証は不要という設計（D#2285）。

## データモデル（SQLite）

`init_db()` が起動時に作成する2テーブル。

```sql
CREATE TABLE channels (
    channel_code     TEXT PRIMARY KEY,
    created_at       TEXT NOT NULL,   -- ISO 8601 (UTC)
    last_activity_at TEXT NOT NULL    -- 最終メッセージ時刻。アイドル削除の基準
);

CREATE TABLE messages (
    msg_id       INTEGER PRIMARY KEY AUTOINCREMENT,  -- 順序の真実源
    channel_code TEXT NOT NULL REFERENCES channels(channel_code),
    handle       TEXT NOT NULL,
    body         TEXT NOT NULL,
    needs_reply  INTEGER NOT NULL,   -- 0/1
    in_reply_to  INTEGER,            -- 親メッセージの msg_id（スレッド構造）
    created_at   TEXT NOT NULL       -- ISO 8601 (UTC)
);
```

- `channel_code` は `secrets.token_urlsafe(8)` で発行。UNIQUE 衝突時は最大10回リトライ（D#2287）。
- `in_reply_to` は保存時に「同一 channel 内に実在する `msg_id` か」を検証し、不正なら `ValueError`（→ HTTP 400）。

## メッセージ順序の真実源

順序の真実源は **`msg_id`（`INTEGER PRIMARY KEY AUTOINCREMENT` で単調増加）** とする。

SSE ブロードキャストの**到達順は厳密に保証しない**。複数スレッドが同時に `/send` を叩いた場合、`save_message` の commit 順と各購読者 queue への `put` 順が逆転しうる。受信側 Claude は到達順に依存せず、`msg_id` で:

- スレッド構造の復元（`in_reply_to` → 親 `msg_id`）
- 重複・欠落の冪等突合（`GetHistory(since=最後に見た msg_id)` で取りこぼし再取得）

を行う。接続断時の取りこぼしも同じ仕組みで回収するため、`recv_monitor.sh` 側は単純再接続のみで十分（D#2257）。

並行 send 時の SQLite 同時書き込みは **WAL モード + `busy_timeout=5000ms`** で吸収する（`_db_connect`）。複数 Claude が同一 channel に同時 send するのがこのシステムの常態であり、並行書き込みは想定内。

## 並行処理

- `server.py` は `ThreadingHTTPServer` で動作し、リクエストごとにスレッドが立つ。SQLite 接続はスレッドごとに新規に開く（`check_same_thread=False` + WAL + busy_timeout）。
- presence とブロードキャストの購読者リスト（`_subscribers`）はプロセス内メモリで `_sub_lock` 保護。SSE 接続が presence 登録を兼ねる: `/stream` 接続中の handle が `/presence` に現れ、切断で除去される。
- 送信者自身へはブロードキャストをエコーしない（同一 handle を除外、D#2286）。

## アイドル削除ジョブ

- `last_activity_at` が現在から **1年（`IDLE_SECONDS = 365日`）** を超過した channel を、メッセージごと丸ごと削除する。
- ジョブは起動時に1回走り、以後 **1日ごと（`IDLE_JOB_INTERVAL = 86400秒`）** に `threading.Timer`（daemon）で再スケジュールされる。
- テスト用に `stop_idle_job()` でタイマーを止められる。

## SSE 受信の詳細

`recv_monitor.sh` は SSH forced command（`bridge recv`）経由で SSE を購読し、`grep '^data:'` で `data:` 行のみを stdout に流す。

- デフォルトで接続断時に自動再接続する（1秒インターバル）。`--no-reconnect` で1回のみ（デバッグ用）。
- `--filter-only` は stdin を読んで `data:` 行のみ流すフィルタ単体モード（テスト用）。
- `--channel` の値は `^[A-Za-z0-9_-]+$` で事前バリデートする。forced command + `bridge_connect.py` でも二重チェックされるが、スクリプト自体の防御層として持つ（D#2309）。

## 設計判断の参照先

「なぜ HTTPS+OAuth 公開ではなく SSH + Cloudflare Tunnel か」「脅威モデルの比較」などの確定版設計判断は cc-memory に集約されている（M#179、T#447、D#2310-2312 など）。本リポジトリのドキュメントは実装に即した範囲を扱う。
