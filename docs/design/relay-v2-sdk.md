# relay v2 Python SDK 仕様

> **位置づけ**: relay v2 が公式に提供する Python SDK の API と振る舞いの仕様。relay のワイヤプロトコル（`relay-v2-wire-api.md`）と、relay 自身の機能要件（外部資材で凍結済）の上に立つアプリケーション層インターフェースである。
>
> **スコープ**: publisher 側 SDK（`relay_outbox`）と subscriber 側 SDK（`relay_client`）の API・振る舞い・設定。ワイヤプロトコル本体・identity / authZ の詳細・cc-memory 側 endpoint 仕様は本書の対象外。
>
> **本書の前提**: relay v2 機能要件で凍結された次の事項に立脚する。
>
> | 前提 | 出典 | 本 SDK での具現化 |
> |---|---|---|
> | publish 保証 = transactional outbox + at-least-once + retain 24h default | 機能要件 v3 FR-4 | `relay_outbox.publish()` で同一 SQLite tx に INSERT、dispatcher が relay へ POST |
> | subscription_id は relay が採番、subscriber 持参復旧経路なし（履歴中立） | 機能要件 v3 FR-3.0 / FR-3.2 | `Subscription` は再接続のたびに新規 subscribe を行う |
> | SSE resume = relay が未 ack outbox を黙って再 push（`Last-Event-ID` 不使用） | 機能要件 v3 FR-4.8 / FR-3.9 | SDK は `Last-Event-ID` ヘッダを送らず、再接続後の cumulative ack カーソルを真実源とする |
> | ack = subscriber 発の app-level cumulative ack（`POST /subscriptions/{id}/ack { up_to_publish_id }`） | 機能要件 v3 FR-3.10 | `Subscription.ack(up_to_publish_id)` をラップ |
> | seq 体系 = `publish_id` 1 系統（relay 全体 global 単調） | 機能要件 v3 §4.4 | event payload の `publish_id` がそのまま ack カーソル |
> | identity = A2A authN（AgentCard + Bearer / JWS）、subscriber 種別中立 | 機能要件 v3 FR-5 | SDK は AgentCard を読み込んで securityScheme を選択、JWS は `pyjwt` 等で署名 |

---

## 0. 用語

| 用語 | 意味 |
|---|---|
| **publisher** | relay にイベントを投函する側のアプリケーション。本 SDK では `relay_outbox` を import して同一 SQLite tx に outbox 行を書く |
| **subscriber** | relay から push を受け取る側のアプリケーション。本 SDK では `relay_client.subscribe()` で `Subscription` を取得して受信ループを回す |
| **outbox** | publisher 側 DB 内に置かれた pending イベント行の集合。dispatcher が relay へ配達する前に必ず通る永続地点 |
| **dispatcher** | publisher プロセス（または別プロセス）で常駐する relay 配達 daemon。outbox を polling して relay の `POST /publish` を呼ぶ |
| **subscription_id** | relay が `POST /subscriptions` で採番する UUID。subscriber が `Subscription` を保持している間だけ有効。subscriber プロセス再起動 / lease 切れで失効し、再 subscribe で新 ID を発行する |
| **publish_id** | relay 全体で global 単調な整数 ID。subscription 内順序・ack カーソル・SSE `id:` 行の三役を兼ねる |
| **stream** | 機能要件 v3 で言う「場」の SDK 内呼称。membership ベースで配達先が決まる publish 源 |

> 機能要件 v3 のユビキタス言語は議論中。本書では「stream」を原則使い、「場」とは併記しない。

---

## 1. パッケージ構成

SDK は単一 wheel として配布する。1 つのパッケージ内に publisher 用と subscriber 用の入口を持つ。

```
relay_sdk/                     # 配布パッケージ名（暫定）
├── __init__.py
├── outbox/                    # publisher 側
│   ├── __init__.py            # publish(), poll(), mark_delivered() の re-export
│   ├── schema.py              # CREATE TABLE / migration helper
│   ├── publisher.py           # publish(conn, ...) 本体
│   └── dispatcher.py          # daemon entrypoint
├── client/                    # subscriber 側
│   ├── __init__.py            # subscribe() の re-export
│   ├── subscription.py        # Subscription / Event / EventDisplay クラス
│   ├── sse.py                 # SSE 接続管理（再接続・heartbeat）
│   └── reconcile.py           # retain 切れ時の publisher 直接 pull
├── http/                      # protocol 層
│   ├── __init__.py
│   ├── request.py             # POST /publish, /subscriptions, /ack の組み立て
│   └── auth.py                # AgentCard + JWS 署名 / 検証
└── errors.py                  # PermanentError / TransientError / RelayProtocolError
```

publisher だけ使うアプリは `from relay_sdk.outbox import publish, run_dispatcher` を、subscriber だけ使うアプリは `from relay_sdk.client import subscribe` を呼べばよい。protocol 層は両者共通で内部から利用される。

---

## 2. publisher 側 SDK（`relay_sdk.outbox`）

### 2.1 outbox スキーマ

SDK は SQLite 用の以下の table 定義を提供する。利用アプリの migration chain に組み込んで使う。

```sql
CREATE TABLE IF NOT EXISTS relay_outbox (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  ref_type        TEXT    NOT NULL,
  ref_id          TEXT    NOT NULL,
  labels          TEXT    NOT NULL,             -- JSON array
  title           TEXT,
  idempotency_key TEXT    NOT NULL,             -- SDK が auto-generate（id を流用）
  created_at      TEXT    NOT NULL,             -- ISO8601 UTC
  processed_at    TEXT,                         -- NULL = pending
  retry_count     INTEGER NOT NULL DEFAULT 0,
  last_error      TEXT,
  dead_at         TEXT                          -- NOT NULL = DLQ 行き
);

CREATE INDEX IF NOT EXISTS idx_relay_outbox_pending
  ON relay_outbox(id)
  WHERE processed_at IS NULL AND dead_at IS NULL;
```

- `idempotency_key` は SDK が `str(id)` を自動付与する（呼び出し側で指定する経路は持たない）。relay 側 dedup（15 分 window）と publisher 側 outbox の同一性を一致させる。
- `dead_at` セット後の行は dispatcher polling 対象外となり、`dead_at` から 7 日後に物理 DELETE される（`run_dispatcher` 内に GC ループを持つ）。

### 2.2 `publish(conn, *, ref_type, ref_id, labels, title=None)` — 同一 tx で outbox に INSERT

```python
from sqlite3 import Connection
from typing import Sequence

def publish(
    conn: Connection,
    *,
    ref_type: str,
    ref_id: str | int,
    labels: Sequence[str],
    title: str | None = None,
) -> int:
    """
    呼び出し元から渡された conn の transaction に乗って outbox 行を INSERT する。

    Args:
        conn: 業務 write が乗っている SQLite Connection。SDK は commit / rollback を呼ばない。
        ref_type: relay 機能要件 v3 FR-3.6 の ref.type（"decision" / "log" / "material" / ...）。
        ref_id: 業務 entity の PK。
        labels: AND set として扱われる opaque string のリスト。空配列は ValueError。
        title: 200 UTF-8 chars 以内。超過時は publisher 責任で truncate（SDK は truncate しない）。

    Returns:
        INSERT された outbox 行の id（後の poll() / mark_delivered() で参照可能）。

    Raises:
        ValueError: labels が空 / title が 200 chars 超 / ref_id が空。

    呼び出し側の責務:
        - conn.commit() を呼ぶこと。SDK は呼ばない。
        - 業務 write と本関数の INSERT が同一 transaction に乗っていること。
    """
```

**振る舞い**:

- `conn` は呼び出し側が管理する SQLite Connection。SDK は `BEGIN` も `COMMIT` も呼ばない。「業務 write と outbox INSERT の atomicity」は呼び出し側が同一 tx で両方走らせることで成立する。
- `idempotency_key` は `INSERT` 直後の `lastrowid` を `str()` 化して同じ tx で UPDATE する（autoincrement の値を再利用するため）。
- `created_at` は UTC ISO8601。`processed_at` / `retry_count` / `last_error` / `dead_at` は default のまま。

**典型利用**:

```python
import sqlite3
from relay_sdk.outbox import publish

conn = sqlite3.connect("app.db")
try:
    # 業務 write
    conn.execute("INSERT INTO decisions (title, body) VALUES (?, ?)", ("draft", "..."))
    decision_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # 同一 tx で outbox にも書く
    publish(
        conn,
        ref_type="decision",
        ref_id=decision_id,
        labels=["domain:cc-memory", "entity:decision", "event:created", f"topic:{topic_id}"],
        title="draft",
    )

    conn.commit()
except Exception:
    conn.rollback()
    raise
```

### 2.3 dispatcher daemon

dispatcher は outbox を polling して relay の `POST /publish` を呼ぶ常駐プロセス。SDK は entrypoint を 2 種類提供する。

#### 2.3.1 `run_dispatcher(...)` — Python から起動

```python
import threading
from pathlib import Path

def run_dispatcher(
    *,
    db_path: Path | str,
    relay_base_url: str,
    agent_card_path: Path | str,
    jws_key_path: Path | str | None = None,
    poll_interval_seconds: float = 0.5,
    retry_backoff_base_seconds: float = 1.0,
    retry_backoff_cap_seconds: float = 300.0,
    transient_retry_deadline_seconds: float = 86400.0,
    dlq_gc_interval_seconds: float = 3600.0,
    stop_event: threading.Event | None = None,
) -> None:
    """
    outbox を polling して relay へ配達する常駐ループ。終了は stop_event.set() で行う。

    プロセス内シングルトン: 同一 db_path 上で複数 run_dispatcher が走ると二重 publish を起こす。
    SDK は SQLite ファイル lock + lockfile（"<db_path>.dispatcher.lock"）で enforce する。
    """
```

ループ内挙動:

1. `SELECT * FROM relay_outbox WHERE processed_at IS NULL AND dead_at IS NULL ORDER BY id LIMIT 100`
2. 各行について `POST /publish` を呼ぶ
3. 成功 → `UPDATE ... SET processed_at = ? WHERE id = ?`
4. transient error → `UPDATE ... SET retry_count = retry_count + 1, last_error = ? WHERE id = ?`
   - retry は Full Jitter（`random(0, min(retry_backoff_cap_seconds, retry_backoff_base_seconds * 2 ** retry_count))`、既定 base=1 秒・cap=300 秒）で同一ループ内で待つのではなく次回 polling まで待つ。決定的な指数バックオフと異なり、複数 publisher プロセスが同時に relay 障害から復帰する際の一斉 retry（thundering herd）を避けるため、待ち時間は毎回 `[0, ceiling)` から一様乱択する
   - dead 化は retry 回数ではなく `created_at` からの経過時間で判定する。`transient_retry_deadline_seconds`（既定 24h）を過ぎても配達できなければその場で `dead_at` をセットする。24h 以内は retry 回数に上限を設けず待ち続ける
5. permanent error（HTTP 4xx で `400` / `403` / `404` 系、認証エラーや payload 不正など）→ retry せず即 `dead_at` セット
6. dispatcher は `dlq_gc_interval_seconds` ごとに `DELETE FROM relay_outbox WHERE dead_at IS NOT NULL AND dead_at < ?`（`?` は `dead_at` から 7 日前）を実行

#### 2.3.2 `python -m relay_sdk.outbox` — systemd 等から起動

CLI entrypoint。引数は環境変数（§6）で渡す。

```sh
$ RELAY_BASE_URL=https://relay.example.com \
  RELAY_AGENT_CARD_PATH=/etc/relay/agent-card.json \
  RELAY_JWS_KEY_PATH=/etc/relay/jws.pem \
  RELAY_OUTBOX_DB=/var/lib/myapp/app.db \
  python -m relay_sdk.outbox
```

`SIGTERM` / `SIGINT` 受領で `stop_event.set()` 相当の終了処理に入る。in-flight な `POST /publish` を `Retry-After` 通知なしに中断しないため、現行のリクエストが返るのを最大 30 秒待ってから exit する。

### 2.4 debug 用 API（`poll` / `mark_delivered`）

通常は dispatcher だけが触る outbox を、開発時に直接観察 / 操作するための小さな関数を debug 用に公開する。

```python
from sqlite3 import Connection
from typing import Sequence, TypedDict

class OutboxRow(TypedDict):
    id: int
    ref_type: str
    ref_id: str
    labels: list[str]
    title: str | None
    idempotency_key: str
    created_at: str
    processed_at: str | None
    retry_count: int
    last_error: str | None
    dead_at: str | None

def poll(conn: Connection, limit: int = 100) -> list[OutboxRow]:
    """pending な outbox 行を id 昇順で limit 件返す。dispatcher の polling と同じ条件。

    用途は debug / 検査のみ。dispatcher と同時に呼ぶと取得した行を dispatcher が並行 publish する可能性があるが、
    poll() は publish せず読み出すだけなので二重配達は起きない（mark_delivered() を別途呼んだ場合のみ問題）。
    """

def mark_delivered(conn: Connection, ids: Sequence[int]) -> None:
    """指定 id の outbox 行に processed_at をセットする。

    用途は「relay に手動で配達済みにしてしまったので outbox から消したい」等の運用救済操作のみ。
    通常パスでは dispatcher が UPDATE するため呼ばない。
    """
```

`poll` / `mark_delivered` を一般アプリのコードから呼ぶことは想定しない。ドキュメントとしては「dispatcher 内部用」として公開し、外部呼び出しは debug 用途に限定する。

---

## 3. subscriber 側 SDK（`relay_sdk.client`）

### 3.1 `subscribe(...) -> Subscription` — subscription 作成 + SSE 接続確立

```python
from pathlib import Path
from typing import Callable, Sequence

def subscribe(
    *,
    relay_base_url: str,
    subscriber_identity: str,
    labels: Sequence[str],
    agent_card_path: Path | str,
    jws_key_path: Path | str | None = None,
    lease_ttl_seconds: int = 300,
    retain_seconds: int | None = None,
    auto_ack: bool = True,
    on_display: Callable[["EventDisplay"], None] | None = None,
) -> "Subscription":
    """
    relay に POST /subscriptions を投げて subscription_id を採番し、GET /events で SSE 接続を張る。

    Args:
        subscriber_identity: 認証済みハンドル文字列。AgentCard と整合する必要がある。
        labels: AND set として扱われる。空配列は relay 側で 400 になるので呼び出し前に ValueError。
        lease_ttl_seconds: 機能要件 v3 FR-3.2 で min 30, max 86400。
        retain_seconds: SSE 切断中の outbox 保持秒数。省略時は relay 既定（24h）。
                        lease_ttl とは独立した軸で、大小制約はない（retain > lease は正当）。
        auto_ack: True なら receive() のイテレーションが次に進んだ時点（= 直前に yield した
                  event の処理が完了したとみなせる時点）で、その publish_id を cumulative ack
                  のバッファに積み、次の relay 通信（ack flush / lease renew / close）で送る
                  （§3.3）。False なら呼び出し側が Subscription.ack(publish_id) を明示的に呼ぶ。
        on_display: 表示・通知用 callback。各 event の yield 直前に EventDisplay を渡して
                    呼ばれる（§3.2.1）。callback 内の例外は SDK が捕捉してログに記録し、
                    配達・ack 進行には影響しない。

    Returns:
        Subscription オブジェクト（context manager としても使える）。

    Raises:
        RelayProtocolError: relay からの 4xx 応答。
        TransientError: relay 到達不能 / 5xx。
    """
```

`Subscription` 自身は context manager。`with subscribe(...) as sub:` で使うと終了時に `unsubscribe` まで自動で呼ぶ。

### 3.2 `Subscription.receive() -> Iterator[Event]` — 受信ループ

```python
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Iterator

@dataclass(frozen=True)
class Event:
    """receive() が yield する業務判定用イベント。

    分岐・判定に使ってよいのは ref_type / ref_id / labels のみ。
    publish_id は ack カーソル、delivered_at は reconcile の since_ts（§3.5）に使う。
    title は意図的に持たない（§3.2.1）。
    """
    publish_id: int
    subscription_id: str
    ref_type: str
    ref_id: str | int
    labels: list[str]
    delivered_at: str

@dataclass(frozen=True)
class EventDisplay:
    """ロギング・通知表示専用のイベントメタデータ。業務判定に使ってはならない。

    subscribe(on_display=...) の callback と SDK 内部ログにのみ流れる。
    """
    publish_id: int
    ref_type: str
    ref_id: str | int
    title: str | None

class Subscription(AbstractContextManager["Subscription"]):
    @property
    def subscription_id(self) -> str: ...

    @property
    def lease_expires_at(self) -> str: ...

    def receive(self) -> Iterator[Event]:
        """
        SSE から event を 1 件ずつ yield する。

        振る舞い:
            - 1 イベント受信 → EventDisplay を SDK 内部 logger に記録し、
              on_display が指定されていれば呼ぶ → caller に yield（§3.2.1）。
            - caller がループを次に進めた瞬間、auto_ack=True なら直前 yield 分の publish_id を
              cumulative ack の up_to_publish_id 候補としてバッファし、次の relay 通信 (lease renew /
              次の ack flush / close) で送る。
            - SSE 切断検知 → 内部で再接続。subscription_id が relay 側でまだ生きていれば
              GET /events に再接続し直し、relay が未 ack outbox を黙って再 push する。
            - 404 / 410（subscription_id 失効・不明。relay 再起動による registry 消失を含む）
              → 新規 POST /subscriptions で再 subscribe し、
              subscription_id を更新、SSE を張り直す。受信再開後は relay 再起動跨ぎになっているため、
              retain 切れ分は publisher 直接 pull で別途回収する責務が caller 側にある（§3.5）。
            - lease 切れ間近（lease_ttl_seconds の 1/3 以下）になると裏で PUT lease を打って renew する。

        Yield 順序保証:
            - subscription 内では publish_id 昇順。
            - 同一 publish_id を複数回 yield しない（subscription 内の重複は SDK 側で
              (subscription_id, publish_id) で dedup する）。
            - subscription をまたぐ場合は relay 側で subscription バースト順（FR-3.8）。
              cross-subscription 順序保証は relay 要件外。
        """

    def ack(self, up_to_publish_id: int) -> None:
        """
        auto_ack=False のとき呼ぶ cumulative ack。

        POST /subscriptions/{subscription_id}/ack
        Body: { up_to_publish_id }

        relay は outbox 内の publish_id <= up_to_publish_id を削除する。
        同じ up_to_publish_id を 2 回送っても 200 OK 冪等。
        """

    def close(self) -> None:
        """
        DELETE /subscriptions/{subscription_id} を呼んで SSE を切断する。
        auto_ack=True で未 flush の ack が残っていれば close 前に flush する。
        以後 receive() を呼ぶと RuntimeError。
        """
```

#### 3.2.1 Event から title を分離する型設計

`Event` は wire payload 上の `title` を保持しない。title は `EventDisplay` に分離し、後述の 2 経路（SDK 内部ログ / `on_display` callback）にのみ流す。subscriber の業務判定コード（`receive()` ループの body）から title に触れる経路を型レベルで塞ぐためである。

**title を業務判定に使えない根拠**:

- title は publisher 責任のベストエフォートな表示ヒントである。relay は truncate も正規化もしない（§2.2）ため、publisher 側の生成ロジック変更だけで文字列内容が変わる。
- at-least-once 再送では publish 時点の title がそのまま再配達される。title の文字列内容を entity の現在状態の判定に使うと、再送で届いた古い title に基づく鮮度バグになる。
- 実際に、title の文字列内容（"done" を含むか等）で完了判定を行った subscriber が、publisher 側の truncation によって event を見落とす障害が観察されている。

以上から、「業務判定は ref + labels のみで行う」は運用規約ではなく型で強制すべきである。`Event` に title 属性が存在しないため、title 分岐は静的型チェックまたは AttributeError として即座に顕在化する。event 起因の判定キーが必要な場合は、publisher が labels に載せる（publisher 側設計の責務）か、subscriber が ref で publisher から entity を読み直す。

**title の流通経路（次の 2 つのみ）**:

1. **SDK 内部ログ**: dedup を通過した event を yield する直前に、logger `relay_sdk.client.events` へ INFO で 1 レコード記録する（`publish_id` / `ref_type` / `ref_id` / `labels` / `title` を含む）。dedup で破棄した再送 frame は同 logger に DEBUG で記録する。caller の処理（yield 後の handler 実行）より前に記録するため、handler が例外で落ちても title を含む受信文脈がログに残る。
2. **`on_display` callback**: 通知バナー等の人間向け表示に title を使うアプリは、`subscribe(on_display=...)` で `EventDisplay` を受け取る。callback は対応する `Event` の yield 直前に受信スレッド上で 1 回だけ呼ばれる。callback 内の例外は SDK が捕捉してログに記録し、配達・ack 進行に影響させない。戻り値も無視する。表示専用であり、処理結果を受信ループや ack に反映する経路を持たない。

### 3.3 auto_ack の意味と注意

`auto_ack=True` は「caller が `for event in sub.receive(): handle(event)` を素直に書くだけで at-least-once が成立する」糖衣である。**handle が成功した直後の event だけが ack されるよう、yield 後 next() が呼ばれた時点を「処理完了」とみなす**。途中で例外が出た場合、最後に成功した event までしか cumulative ack に進めないため、未処理 event は次回再接続で再 push される。

caller 側で「複数 event をバッチして処理してから 1 度に ack したい」「event 受信と処理が非同期で進む」等の要件があるときは `auto_ack=False` にして自前で `ack()` を呼ぶ。

### 3.4 reconnect 戦略

- SSE 切断（TCP close / heartbeat 30s 不在 / EOF）を検知。
- 即時で 1 回 retry、失敗したら Full Jitter（`random(0, min(cap, base * 2 ** attempt))`、既定 base=1 秒・cap=30 秒）で待って同一 subscription_id に再接続し続ける。決定的な指数バックオフと異なり、relay 再起動直後に多数の subscriber が同時に再接続する一斉 retry（thundering herd）を、待ち時間の乱択で分散させる。
- 回数ベースの諦め（旧 `reconnect_max_attempts`）は持たない。死活判定は lease renew に一本化しており、subscription 操作への `404` / `410`（subscription_id 失効・不明）を受け取った場合だけ新規 subscribe に切り替える（relay 再起動で registry が消えた場合は `404` が返るため、`410` だけを分岐キーにしない）。
- 再接続後は relay が自動的に未 ack 分を再 push するため、SDK 側で resume を申告する経路は持たない（`Last-Event-ID` ヘッダは送らない）。

### 3.5 retain 切れ時の fallback は publisher 直接 pull

retain 期間（default 24h）を超えた取りこぼしは relay の outbox から消えている。subscriber は publisher（cc-memory 等）に直接 read を投げて補完する。SDK 側ではこの経路を強制しないが、reconciliation を簡単にするための薄いヘルパだけ用意する。

```python
from typing import Any, Callable, Iterator, Sequence

def reconcile(
    *,
    fetcher: Callable[[str | None], Iterator[Any]],
    labels: Sequence[str],
    since_ts: str | None = None,
) -> Iterator[Any]:
    """
    publisher 直接 pull の薄いラッパ。

    fetcher は subscriber アプリ側が用意する関数で、引数に since_ts 文字列を受け取り
    更新済み entity を順次 yield する callable。cc-memory なら get_map / search の light モードを叩く。

    SDK は labels namespace の解釈は行わない（cc-memory ↔ relay 協調プロトコルの責務）。
    SDK の役割は「(a) 直近 ack 済みの delivered_at を since_ts として渡す」「(b) fetcher の出力を
    Iterator として返す」だけ。
    """
```

reconciliation の本筋ロジック（labels → 内部 tool 呼び出しの翻訳・差分検出・full pull）は publisher 側プロトコルに固有なので SDK には含めず、別パッケージ（cc-memory 配下の SDK 拡張）で実装する。

---

## 4. protocol 層の翻訳

`relay_sdk.http` は publisher / subscriber 両側から内部利用される薄い HTTP クライアントである。

### 4.1 HTTP request 構築

| メソッド | 呼び出し元 | 役割 |
|---|---|---|
| `post_publish(client, *, ref, labels, title, idempotency_key)` | dispatcher | `POST /publish` |
| `post_subscription(client, *, subscriber, labels, lease_ttl, retain_seconds)` | `subscribe()` | `POST /subscriptions` |
| `put_lease(client, *, subscription_id, lease_ttl)` | `Subscription` 内裏側 | `PUT /subscriptions/{id}/lease` |
| `delete_subscription(client, *, subscription_id)` | `Subscription.close()` | `DELETE /subscriptions/{id}` |
| `post_ack(client, *, subscription_id, up_to_publish_id)` | `Subscription.ack()` | `POST /subscriptions/{id}/ack` |
| `open_sse(client, *, subscription_ids)` | `Subscription.receive()` | `GET /events?subscription_ids=...` を SSE で開く |

`client` は `httpx.Client`（同期） / `httpx.AsyncClient`（非同期）どちらか。SDK の v1 は同期 API のみ提供する。AsyncClient 対応は後段でアプリ需要が出てから追加する。

### 4.2 SSE 接続管理

- `httpx` の `client.stream("GET", url, headers=...)` を使う。
- 30 秒以内に何も読まれなければ TCP close と扱う（relay 側 keepalive 間隔は 30 秒、機能要件 v3 FR-4.6）。
- event 単位の dedup は `(subscription_id, publish_id)` で行う。SDK 内に LRU set（直近 10000 件）を持つ。
- 再接続の Full Jitter バックオフは §3.4 参照。

### 4.3 JWS 署名生成 / 検証

- AgentCard の `securitySchemes` を読み込み、Bearer 用 token を `RELAY_BEARER_TOKEN` 環境変数または `jws_key_path` 経由の私鍵で生成する。
- relay 側 AgentCard を起動時に取得し、JWS で署名されていれば `/.well-known/jwks.json` の公開鍵で検証する（MAY 要件、機能要件 v3 FR-5.3）。検証失敗時は接続を拒否する。
- 署名 / 検証は `pyjwt`（ES256）と `rfc8785`（JCS）を使う。SDK 自体は wrapper として `relay_sdk.http.auth` に閉じ込める。

### 4.4 エラーハンドリング

SDK は HTTP / SSE 由来のエラーを次の 3 種類に分類して例外で表現する。

```python
class RelayProtocolError(Exception):
    """relay からの 4xx / 仕様外応答。caller 側で原因を直す必要がある（permanent）。"""

class TransientError(Exception):
    """5xx / 接続不能 / timeout。dispatcher は Full Jitter backoff で retry、subscriber は SSE 再接続で復帰。"""

class PermanentError(Exception):
    """subscription が失効・不明になった状態（subscription 操作への 404 / 410）。caller 側で再 subscribe が必要。"""
```

dispatcher 側のリトライ判定:

| HTTP 応答 | 分類 | 振る舞い |
|---|---|---|
| `2xx` | success | `processed_at` を更新 |
| `400 / 403 / 404`（`POST /publish` への応答、認証エラー・payload 不正等） | `RelayProtocolError` | 即 `dead_at` セット、retry しない |
| `429` | `TransientError` | `Retry-After` ヘッダ尊重、retry_count を進める |
| `5xx` / timeout / TCP RST | `TransientError` | Full Jitter backoff で retry（`created_at` から 24h 過ぎても解消しなければ `dead_at` セット） |
| `404 / 410`（subscription 操作: lease renew / `GET /events` / ack への応答）| `PermanentError` | dispatcher 側では発生しない。subscriber 側で受領したら新規 subscribe に切り替え |

---

## 5. 典型コード例

### 5.1 publisher: 同一 SQLite tx で書く

```python
import sqlite3
from pathlib import Path
from relay_sdk.outbox import publish

def record_decision(conn: sqlite3.Connection, title: str, body: str, topic_id: int) -> int:
    """cc-memory のような publisher アプリでの典型例。"""
    try:
        cur = conn.execute(
            "INSERT INTO decisions (title, body) VALUES (?, ?)",
            (title, body),
        )
        decision_id = cur.lastrowid

        publish(
            conn,
            ref_type="decision",
            ref_id=decision_id,
            labels=[
                "domain:cc-memory",
                "entity:decision",
                "event:created",
                f"topic:{topic_id}",
            ],
            title=title,
        )

        conn.commit()
        return decision_id
    except Exception:
        conn.rollback()
        raise


def main() -> None:
    conn = sqlite3.connect(Path("/var/lib/myapp/app.db"))
    record_decision(conn, "ack カーソル一本化", "...", topic_id=474)
```

dispatcher は別プロセスで `python -m relay_sdk.outbox` を上げておく。アプリ本体は `publish()` を呼んで `conn.commit()` するだけでよい。

### 5.2 subscriber: auto_ack で受け取る

```python
from pathlib import Path
from relay_sdk.client import subscribe, Event, EventDisplay

def handle(event: Event) -> None:
    print(f"received: ref={event.ref_type}:{event.ref_id} labels={event.labels}")

def show_banner(meta: EventDisplay) -> None:
    print(f"[relay] {meta.ref_type}:{meta.ref_id} {meta.title or '(no title)'}")

def main() -> None:
    with subscribe(
        relay_base_url="https://relay.example.com",
        subscriber_identity="ow-orch-alpha",
        labels=["topic:474", "event:updated"],
        agent_card_path=Path("/etc/relay/agent-card.json"),
        jws_key_path=Path("/etc/relay/jws.pem"),
        auto_ack=True,
        on_display=show_banner,
    ) as sub:
        for event in sub.receive():
            handle(event)
```

### 5.3 subscriber: バッチ ack で受け取る

```python
from pathlib import Path
from relay_sdk.client import subscribe, Event

def main() -> None:
    with subscribe(
        relay_base_url="https://relay.example.com",
        subscriber_identity="ow-orch-alpha",
        labels=["domain:cc-memory", "entity:activity", "event:updated"],
        agent_card_path=Path("/etc/relay/agent-card.json"),
        auto_ack=False,
    ) as sub:
        batch: list[Event] = []
        for event in sub.receive():
            batch.append(event)
            if len(batch) >= 50:
                process_batch(batch)
                sub.ack(up_to_publish_id=batch[-1].publish_id)
                batch.clear()

def process_batch(events: list[Event]) -> None:
    ...
```

---

## 6. 設定 / 環境変数

| 変数 | 役割 | default |
|---|---|---|
| `RELAY_BASE_URL` | relay の base URL（`https://...`） | なし（必須） |
| `RELAY_AGENT_CARD_PATH` | publisher / subscriber 自身の AgentCard JSON | なし（必須） |
| `RELAY_JWS_KEY_PATH` | JWS 署名用私鍵 PEM。指定なしなら Bearer のみで認証 | なし |
| `RELAY_BEARER_TOKEN` | Bearer token 文字列。`RELAY_JWS_KEY_PATH` と排他 | なし |
| `RELAY_OUTBOX_DB` | dispatcher が見る SQLite ファイル | なし（dispatcher 起動時必須） |
| `RELAY_OUTBOX_POLL_INTERVAL_MS` | dispatcher の polling 間隔（ミリ秒） | `500` |
| `RELAY_OUTBOX_RETRY_BACKOFF_BASE_MS` | retry の Full Jitter base（ミリ秒） | `1000` |
| `RELAY_OUTBOX_RETRY_BACKOFF_CAP_S` | retry の Full Jitter cap（秒） | `300` |
| `RELAY_OUTBOX_TRANSIENT_RETRY_DEADLINE_S` | 一時的失敗を retry し続ける期限（`created_at` からの経過秒）。超過で dead 化 | `86400` |
| `RELAY_OUTBOX_DLQ_GC_INTERVAL_S` | DLQ GC ループ間隔（秒） | `3600` |
| `RELAY_SSE_KEEPALIVE_S` | SSE 無通信判定の閾値（秒） | `30` |
| `RELAY_SSE_RECONNECT_BACKOFF_BASE_S` | 再接続の Full Jitter base（秒） | `1` |
| `RELAY_SSE_RECONNECT_BACKOFF_CAP_S` | 再接続の Full Jitter cap（秒） | `30` |
| `RELAY_HTTP_TIMEOUT_S` | HTTP request の timeout（秒） | `10` |

環境変数は `subscribe()` / `run_dispatcher()` の引数で override 可能。引数で渡された値が優先される。

---

## 7. テスト戦略

### 7.1 in-memory fake relay（unit test 用）

SDK は同パッケージ内に `relay_sdk.testing.FakeRelay` を提供する。relay の HTTP / SSE 振る舞いを Python オブジェクトで模した stub で、SDK の publisher / subscriber コードを実 relay なしに駆動できる。

```python
from relay_sdk.testing import FakeRelay
from relay_sdk.client import subscribe

def test_subscriber_receives_published_event() -> None:
    with FakeRelay() as fake:
        # fake は relay_base_url を返す
        with subscribe(
            relay_base_url=fake.base_url,
            subscriber_identity="test-subscriber",
            labels=["entity:decision"],
            agent_card_path=fake.fake_agent_card_path(),
        ) as sub:
            fake.publish(ref_type="decision", ref_id=1, labels=["entity:decision"], title="t")

            events = []
            for event in sub.receive():
                events.append(event)
                if len(events) == 1:
                    break

            assert events[0].ref_id == 1
            assert events[0].publish_id > 0
```

`FakeRelay` が提供する振る舞い:

- `POST /subscriptions` / `DELETE /subscriptions/{id}` / `PUT /subscriptions/{id}/lease` / `POST /publish` / `POST /subscriptions/{id}/ack` / `GET /events` の最小実装
- in-memory な outbox（subscription_id -> [Event]）
- subset マッチング（機能要件 v3 FR-3.3 と同じ）
- cumulative ack（FR-3.10 と同じ）
- ack 前切断 → 再接続で再 push（FR-4.8）
- `fake.simulate_outage()` / `fake.simulate_subscription_loss(subscription_id)`（subscription 操作への 404 / 410 応答の注入）等のフォールト注入 API

ただし relay の永続性 / DLQ / 7 日 GC / SSE keepalive 30 秒は模さない。これらは integration test 側で見る。

FakeRelay を使う unit test では、§3.2.1 の型分離を回帰から守るため少なくとも次を固定する:

- `Event` の dataclass fields に `title` が存在しないこと
- title 付きで publish した event が yield される際、同じ `publish_id` の `EventDisplay` が `on_display` に yield 前に 1 回だけ渡ること
- `on_display` callback が例外を投げても `receive()` の yield と ack 進行が継続すること
- yield 直前に logger `relay_sdk.client.events` へ title を含む INFO レコードが出ること、および dedup 破棄された再送 frame が DEBUG で記録されること

### 7.2 integration test（real relay 起動）

`tests/integration/` 配下に real relay バイナリ（または relay リポジトリで提供される `python -m relay.server`）を `pytest fixture` で起動するスイートを置く。SDK は relay の同じリポジトリ内に同居しているので、CI 上では `pyproject.toml` の dev dependency として relay 本体を入れて in-process で起動する経路を取る。

カバーする観点:

- publisher の `publish()` → dispatcher → relay → subscriber `receive()` の往復
- dispatcher 停止 → outbox 蓄積 → 再起動で配達再開
- subscriber プロセス再起動 → 新規 subscribe → 古い outbox は DLQ 経由で 7 日後 GC（test では time-shift fixture で 7 日後を擬似）
- `429` 受領時の `Retry-After` 尊重
- JWS 署名・検証（鍵不一致で接続拒否）
- SSE 30 秒 keepalive で長期接続が落ちないこと

### 7.3 contract test

SDK が relay へ送る HTTP request / SSE consume の形は relay 機能要件の仕様と整合している必要がある。`tests/contract/` 配下に relay 機能要件 v3 と本書の対応表に基づく契約テストを置き、ワイヤ API ドキュメントを更新したときに SDK 側も追随漏れなく検知できるようにする。

---

## 8. 本書がやっていないこと

- AsyncClient / asyncio 版 SDK の API（v1 は同期のみ。需要が出てから別書）
- cc-memory 側 entity → labels 変換ロジック（cc-memory 側 publisher 実装側の責務）
- subscriber アプリの reconciliation 設計（`reconcile()` ヘルパ以上は publisher 固有プロトコル側）
- relay の disk schema / migration tool（relay 本体側）
- AgentCard の発行 / 配布手順（identity / authZ 別書）
- 多言語版 SDK（v1 は Python のみ）

---

## 9. 関連

- relay 機能要件 v3（凍結）— relay が満たす振る舞いの一次ソース
- ワイヤ / API 仕様（`relay-v2-wire-api.md`）— 本書が依存する HTTP / SSE プロトコル
- cc-memory ↔ relay 協調プロトコル v1（cc-memory 側で凍結）— `relay_outbox.publish()` の利用側ガイド
- identity / authZ 仕様（`relay-v2-identity-authz.md`、別書）— AgentCard / JWS / Bearer の詳細
