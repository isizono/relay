# relay の運用手順

relay サーバーの起動・bind 先の選び方・TCP keepalive・招待 URL の base URL について、実装から確認できる事実と運用上の推奨をまとめる。

## 起動

```bash
uv run python -m relay.serve --host 127.0.0.1 --port 8000
```

`relay.serve`（`relay/serve.py`）は listen socket を自前で作って TCP keepalive を設定したうえで uvicorn に渡し、`relay.app:app` を起動する。詳細は後述の「TCP keepalive」を参照。

`uv run uvicorn relay.app:app --host 127.0.0.1 --port 8000` で直接起動することもできる。ASGI アプリケーションとしての振る舞いは同一だが、この起動方法では TCP keepalive は OS 既定のままになる。

## `--host` の選び方

`--host` に何を渡すかで、どこからの接続を受け付けるかが変わる。

| 指定値 | 受け付ける接続元 | 備考 |
|---|---|---|
| `127.0.0.1`（既定） | 同一マシン上のプロセスのみ | 別マシンからは到達不可 |
| `0.0.0.0` | 全ネットワークインタフェース（ループバック + LAN 等） | `127.0.0.1` 経由の既存クライアントも引き続き繋がる |
| マシン固有の IP（例 `192.168.1.5`） | そのインタフェース宛の接続のみ | `127.0.0.1` 経由の接続は繋がらなくなる |

事実として、特定の IP アドレスを `--host` に指定すると listen socket はそのインタフェースにのみ bind され、ループバックインタフェース（`127.0.0.1`）は含まれない。そのため、同一マシン上で `127.0.0.1` 宛に接続していた既存のクライアント（`python -m relay.invite` の既定 `--base-url` や、動作確認用の `curl http://127.0.0.1:...` 等）は、`--host` をマシン固有の IP に変更した途端に接続できなくなる。同一マシンからの接続と LAN からの接続を両方とも受け付けたい場合は、特定 IP ではなく `0.0.0.0` を指定する必要がある。

LAN 上の他マシンから relay を共有したい場合は `--host 0.0.0.0` が必要になる。ただしこれは relay の TCP ポートを LAN 上の任意のホストから到達可能にすることを意味する。relay は Bearer token（`RELAY_AUTH_TOKENS` で静的に設定するか、招待 `python -m relay.invite client new` で発行した credential）による認証を要求する設計であり、`GET /` と `GET /.well-known/agent-card.json` を除く endpoint は無効な token を弾く。`0.0.0.0` で bind する場合、この認証設定（有効な token を関係者以外に渡さないこと、不要になった credential は `python -m relay.invite client revoke` で失効させること）が到達可能性の唯一の防御線になる点に留意する。

## TCP keepalive

TCP keepalive は、データのやり取りが無いまま一定時間が経過した接続に対して、OS が定期的に「生きているか」を確認するプローブパケットを送る仕組みである。応答が一定回数途絶えると OS はその接続を切断する。これが無いと、NAT やロードバランサ、ネットワーク機器がアイドル接続をタイムアウトで内部的に破棄していても、relay 側のプロセスは「接続はまだ張られている」と思い込んだまま半開き（half-open）状態の接続を保持し続けることがある。

`python -m relay.serve` は listen socket に対して以下を設定する（`relay/serve.py` の `configure_keepalive`）。

| 設定項目 | 意味 | 既定値 | 上書き用環境変数 |
|---|---|---|---|
| `SO_KEEPALIVE` | keepalive 自体を有効化する | 常に有効 | なし（無効化オプションは提供しない） |
| idle（`TCP_KEEPIDLE` / macOS では `TCP_KEEPALIVE`） | 最後の送受信から最初の keepalive probe を送るまでの秒数 | 60 秒 | `RELAY_TCP_KEEPIDLE` |
| interval（`TCP_KEEPINTVL`） | probe が無応答だった場合の再送間隔（秒） | 10 秒 | `RELAY_TCP_KEEPINTVL` |
| count（`TCP_KEEPCNT`） | 無応答を何回連続で確認したら接続を切断するか | 3 回 | `RELAY_TCP_KEEPCNT` |

listen socket に設定した keepalive オプションは `accept()` で生成される個々の接続 socket にも継承される。実行環境に該当する定数が存在しない場合（例: 上記以外のプラットフォーム）はその項目の設定のみをスキップし、警告ログを出力したうえで起動は継続する。

`uvicorn relay.app:app` を直接起動した場合、この keepalive 設定は適用されず OS 既定の値（Linux では idle 2 時間程度が一般的）のままになる。

## `RELAY_BASE_URL` の役割

`RELAY_BASE_URL` は、招待 URL（`python -m relay.invite client new` / `peer new` が出力する URL）の base として使う値であり、federation peer の redeem 応答に載せる自分自身の locator（相手が以後この relay 宛にメッセージを送る際の宛先）にも使われる。

`python -m relay.invite` 系コマンドの base URL 解決順序は次のとおりである。

1. コマンドラインの `--base-url`
2. 環境変数 `RELAY_BASE_URL`
3. 既定値 `http://127.0.0.1:8770`（同一マシンからしか redeem できない値）

`--base-url` も `RELAY_BASE_URL` も指定しなかった場合、既定値へフォールバックしたことを知らせる警告が標準エラー出力に1行出る。別マシンから redeem させたい場合は、`RELAY_BASE_URL` を relay の公開 URL に設定するか、コマンド実行のたびに `--base-url` を渡す必要がある。
