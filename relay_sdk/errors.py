"""relay v2 SDK の例外分類（relay-v2-sdk.md §4.4）。

SDK は HTTP / SSE 由来のエラーを 3 種類に分類する。呼び出し側（dispatcher /
subscriber）の復帰戦略がこの分類で決まる。

- ``RelayProtocolError``: relay からの 4xx / 仕様外応答。原因は caller 側にあり、
  リトライしても直らない（permanent）。dispatcher は当該 outbox 行を即 dead 化する。
- ``TransientError``: 5xx / 接続不能 / timeout / 429。時間を置けば復帰しうる。dispatcher は
  指数バックオフで retry、subscriber は SSE 再接続で復帰する。
- ``PermanentError``: subscription が失効・不明になった（subscription 操作への 404 / 410）。
  caller 側で再 subscribe が必要。dispatcher 側では発生しない（publish は subscription_id を
  持たないため）。subscriber 側で受領したら新規 subscribe に切り替える。

``StreamNotFoundError`` / ``StreamAlreadyExistsError`` は streams 系 endpoint 固有の
404 / 409 を表す ``RelayProtocolError`` のサブクラス。既存 3 分類の互換性（広く
``except RelayProtocolError`` で捕捉できること）を保ちつつ、streams を扱う呼び出し側
だけが狭く捕捉できるようにする。
"""
from __future__ import annotations


class RelayProtocolError(Exception):
    """relay からの 4xx / 仕様外応答（permanent、caller が原因を直す必要がある）。

    ``status_code`` / ``code``（error envelope の ``code``）を保持する。
    """

    def __init__(
        self, message: str, *, status_code: int | None = None, code: str | None = None
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code


class TransientError(Exception):
    """5xx / 接続不能 / timeout / 429（時間を置けば復帰しうる）。

    ``429`` の場合は ``retry_after``（秒）に ``Retry-After`` ヘッダの値を保持する。
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class PermanentError(Exception):
    """subscription が失効・不明になった状態（subscription 操作への 404 / 410）。

    caller 側で再 subscribe が必要。
    """

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class StreamNotFoundError(RelayProtocolError):
    """stream が未作成（`POST /streams/{id}/messages` への 404）。

    caller は `post_stream` で作成してから再試行できる。
    """


class StreamAlreadyExistsError(RelayProtocolError):
    """同名 stream が既に存在する（`POST /streams` への 409）。

    caller は作成済みとして扱ってよい（同時作成競合の正常系）。
    """
