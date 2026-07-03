"""stream（場）関連 endpoint（relay-v2-wire-api.md §3）。

`POST /streams` / `DELETE /streams/{stream_id}` / `GET /streams/{stream_id}` /
`POST /streams/{stream_id}/messages` / `PUT`・`DELETE`・`GET /streams/{stream_id}/members` /
`POST /streams/{stream_id}/ack` はここに実装する（後続タスクの担当分）。

stream の状態（membership 含む）は relay-v2-wire-api.md §0 の R1 原則により in-memory
実装とする。SQLite には streams / memberships table を持たない
（設計判断の詳細は docs/ARCHITECTURE.md §DB schema を参照）。structural authZ
（write 権限 membership の照合、identity-authz.md §2.2）もここで行う。

`relay.identity.require_authn` を各 handler に適用し、`request.state.identity` から
呼び出し元 identity を得ること。
"""
from __future__ import annotations

from starlette.routing import Route

routes: list[Route] = []
