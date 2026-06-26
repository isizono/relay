# API リファレンス

relay には3つの API 層がある。通常クライアントが直接触れるのは **MCP ツール**で、その下に **bridge サブコマンド**（SSH forced command）、さらに下に **HTTP エンドポイント**（`server.py`）がある。

```
MCP ツール (mcp_server.py)  ──ssh──▶  bridge サブコマンド (bridge_connect.py)  ──HTTP──▶  server.py
```

このドキュメントは3層すべてを記載する。アーキテクチャ上の背景は [architecture.md](architecture.md) を参照。

## 共通: メッセージオブジェクト

`/history` と SSE で返るメッセージの形:

| フィールド | 型 | 説明 |
|---|---|---|
| `msg_id` | int | 単調増加 ID。順序の真実源 |
| `handle` | string | 送信者の GitHub ユーザー名 |
| `body` | string | 本文 |
| `needs_reply` | bool | 返信要求フラグ |
| `in_reply_to` | int \| null | 親メッセージの `msg_id`（スレッド構造） |
| `created_at` | string | ISO 8601（UTC） |

> SSE ブロードキャストの payload にはこれに加えて `channel_code` が含まれる（`save_message` の戻り値をそのまま配信するため）。`/history` の各要素には `channel_code` は含まれない。

---

## MCP ツール（`mcp_server.py`）

Claude Code が直接呼ぶ層。接続先ホストは環境変数 `RELAY_SSH_HOST`（デフォルト `relay`）。全 SSH 呼び出しに ControlMaster オプション（`ControlPersist=600`）を付与して接続を多重化する。`handle` は SSH forced command で固定されるため引数に取らない。

### `CreateChannel()`

新しい channel を作成する。

- 戻り値: `{"channel_code": str}`

### `SendMessage(channel_code, body, needs_reply=False, in_reply_to=None)`

メッセージを送信する。

| 引数 | 型 | 必須 | 説明 |
|---|---|---|---|
| `channel_code` | str | ✓ | 送信先 channel |
| `body` | str | ✓ | 本文 |
| `needs_reply` | bool | | 返信要求フラグ |
| `in_reply_to` | int \| None | | 親メッセージの `msg_id` |

- 戻り値: `{"msg_id": int}`

### `GetHistory(channel_code, since=None, limit=None)`

メッセージ履歴を取得する。`since` 指定時は `msg_id > since`（since 自身は含まない）。取りこぼし回収にはここを使う。

- 戻り値: `{"messages": [メッセージオブジェクト, ...]}`

### `GetPresence(channel_code)`

現在 SSE 接続中の handle 一覧を取得する。

- 戻り値: `{"handles": [str, ...]}`（重複除去・順序不定）

---

## bridge サブコマンド（`bridge_connect.py`）

SSH forced command 経由で呼ばれる層。クライアントは `ssh relay "bridge <subcmd> ..."` の形で叩く（MCP ツールが内部でこれを行う）。`--handle` は authorized_keys の forced command で固定され、`$SSH_ORIGINAL_COMMAND` 内の handle 指定は無視される（詐称防止、D#2285）。

| サブコマンド | 用途 | 引数 | 出力 |
|---|---|---|---|
| `bridge create` | channel 作成 | なし | `{"channel_code": ...}` |
| `bridge recv --channel=X` | SSE 購読（受信） | `--channel` | SSE ストリーム |
| `bridge send --channel=X --body=Y` | メッセージ送信 | `--channel`, `--body`, `--needs-reply=true/false`, `--in-reply-to=N` | `{"msg_id": ...}` |
| `bridge history --channel=X` | 履歴取得 | `--channel`, `--since=N`, `--limit=N` | `{"messages": [...]}` |
| `bridge presence --channel=X` | 接続中 handle 一覧 | `--channel` | `{"handles": [...]}` |

- 値に空白・改行を含んでも `shlex` で安全に1コマンドへ変換される（送信側 `mcp_server.py` は `shlex.join`、受信側 `bridge_connect.py` は `shlex.split`）。
- HTTP エラー（4xx/5xx）は curl の `-sf --fail-with-body` で非0終了として伝播し、サーバーの `{"error": ...}` を stderr 経由で呼び出し元へ返す。

---

## HTTP エンドポイント（`server.py`）

最下層。`127.0.0.1:8765` に bind し、外部到達は SSH forced command 経由のみ。直接叩くのはホスト内のデバッグ時のみ。

### `POST /create`

channel を発行する。

- リクエストボディ: なし（または `{}`）
- レスポンス: `200 {"channel_code": str}`

### `GET /stream?channel=CODE&handle=NAME`

SSE を購読する。接続が presence 登録を兼ねる。

| クエリ | 必須 | 説明 |
|---|---|---|
| `channel` | ✓ | channel_code |
| `handle` | ✓ | 購読者の handle |

- レスポンス: `Content-Type: text/event-stream`。接続直後に `: connected` コメント、以降メッセージごとに `data: {json}\n\n`。
- 自分（同一 handle）が送ったメッセージはエコーされない（D#2286）。
- エラー: `400`（channel/handle 欠落）、`404`（channel 不在）。

### `POST /send`

メッセージを保存し、同一 channel の購読者へブロードキャストする。

リクエストボディ（JSON）:

```json
{
  "channel": "abc123",
  "handle": "alice",
  "body": "本文",
  "needs_reply": false,
  "in_reply_to": null
}
```

| フィールド | 必須 | 説明 |
|---|---|---|
| `channel` | ✓ | channel_code |
| `handle` | ✓ | 送信者 handle（bridge 層で固定された値） |
| `body` | ✓ | 本文（空文字は可、`null` は不可） |
| `needs_reply` | | 既定 `false` |
| `in_reply_to` | | 親メッセージの `msg_id`。同一 channel 内に実在しないと `400` |

- レスポンス: `200 {"msg_id": int}`
- エラー: `400`（JSON パース失敗 / 必須欠落 / 不正な `in_reply_to`）、`404`（channel 不在）。

### `GET /history?channel=CODE[&since=N][&limit=N]`

履歴を `msg_id` 昇順で返す。

| クエリ | 必須 | 説明 |
|---|---|---|
| `channel` | ✓ | channel_code |
| `since` | | `msg_id > since` のみ返す（exclusive） |
| `limit` | | 最大件数（正の整数） |

- レスポンス: `200 {"messages": [メッセージオブジェクト, ...]}`
- エラー: `400`（`since` が非整数 / `limit` が正整数でない）、`404`（channel 不在）。

### `GET /presence?channel=CODE`

現在 SSE 接続中の handle 一覧を返す。

- レスポンス: `200 {"handles": [str, ...]}`（重複除去・順序不定）
- エラー: `400`（channel 欠落）、`404`（channel 不在）。

---

## エラー形式

HTTP 層のエラーレスポンスは一律:

```json
{"error": "理由（日本語）"}
```

bridge / MCP 層は curl の非0終了を捕捉し、このエラー本文を stderr 経由で呼び出し元（最終的に Claude）へ伝播する。
