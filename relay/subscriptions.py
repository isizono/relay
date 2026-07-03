"""subscription 関連 endpoint（relay-v2-wire-api.md §5.1〜§5.4, §5.6, §5.7）。

`POST /subscriptions` / `PUT /subscriptions/{id}/lease` / `DELETE /subscriptions/{id}` /
`POST /subscriptions/{id}/ack` / `POST /publish` はここに実装する（後続タスクの担当分）。

subscription registry（lease 含む）は relay-v2-wire-api.md §0 の R1 原則により in-memory
実装とする。SQLite には subscriptions table を持たない
（設計判断の詳細は docs/ARCHITECTURE.md §DB schema を参照）。

subscription を名指しする操作は ownership 検証（呼び出し元 identity == subscriber 本人）を
structural authZ として行う（identity-authz.md §2.2, wire-api.md §5.7）。非所有・不明な
subscription_id は 404（存在露呈回避）、所有者本人の lease 切れ済みは 410。

`relay.identity.require_authn` を各 handler に適用し、`request.state.identity` から
呼び出し元 identity を得ること。labels の subset マッチングは Python in-memory で行う。
"""
from __future__ import annotations

from starlette.routing import Route

routes: list[Route] = []
