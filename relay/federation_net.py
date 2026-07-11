"""federation の outbound dial（egress / CLI redeem）に共通する SSRF ガード。

federation の outbound fetch は「egress dial（locator 宛）」と「CLI redeem の POST」に
集約する（jku fetch は federation の信頼判定に使わない）。いずれにも以下のガード一式を
適用する:

- scheme は https 限定（`allow_private=True` のときのみ http + private レンジを許可。
  同一ホスト E2E・開発用）
- 内部 IP レンジ拒否（loopback / private / link-local / reserved / multicast、
  IPv4-mapped IPv6・NAT64 well-known prefix を含む）
- redirect 無効
- 応答サイズ上限
- timeout
"""
from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlparse

import httpx

DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_RESPONSE_BYTES = 1_000_000  # 1MB

# NAT64 well-known prefix（RFC 6052）。ipaddress モジュールに判定属性が無いため個別チェックする。
_NAT64_WELL_KNOWN_PREFIX = ipaddress.ip_network("64:ff9b::/96")


class LocatorRejected(Exception):
    """locator が outbound ガードで拒否された（scheme 不正 / 内部 IP 宛 等）。"""


def _is_internal_address(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if ip.is_loopback or ip.is_link_local or ip.is_private or ip.is_reserved or ip.is_multicast:
        return True
    if isinstance(ip, ipaddress.IPv6Address):
        mapped = ip.ipv4_mapped
        if mapped is not None:
            return _is_internal_address(mapped)
        if ip in _NAT64_WELL_KNOWN_PREFIX:
            return True
    return False


def validate_locator(url: str, *, allow_private: bool) -> None:
    """`url` が federation の outbound dial 先として許可されるかを検証する。

    許可されない場合は `LocatorRejected` を送出する（fail-closed）。DNS 解決した先の
    IP アドレスを検査するため、ホスト名の内部 IP への割り当て（DNS rebinding 含む）を
    ここで弾く。呼び出し側は検証直後に同じホスト名へ dial すること（TOCTOU の窓は
    fate-sharing 前提の federation peer 間では受容する）。
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise LocatorRejected(f"locator の scheme は http/https のみ許可されます: {url!r}")
    if parsed.scheme == "http" and not allow_private:
        raise LocatorRejected(
            "http scheme の locator は RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS=true のときのみ許可されます"
        )
    host = parsed.hostname
    if not host:
        raise LocatorRejected(f"locator にホスト名がありません: {url!r}")

    try:
        addrinfo = socket.getaddrinfo(host, None)
    except socket.gaierror as exc:
        raise LocatorRejected(f"locator のホスト名を解決できません: {host!r}") from exc

    for family, _type, _proto, _canonname, sockaddr in addrinfo:
        raw_ip = sockaddr[0]
        ip = ipaddress.ip_address(raw_ip)
        if _is_internal_address(ip) and not allow_private:
            raise LocatorRejected(
                f"locator が内部 IP レンジを指しています（{host!r} -> {raw_ip!r}）"
            )


def build_client(*, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> httpx.Client:
    """outbound dial 用の `httpx.Client` を構築する（redirect 無効固定）。"""
    return httpx.Client(follow_redirects=False, timeout=timeout)


def build_async_client(*, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> httpx.AsyncClient:
    """outbound dial 用の `httpx.AsyncClient` を構築する（redirect 無効固定）。

    egress（`relay.federation_egress`）は asyncio dispatcher ループ上で動くため、
    `build_client`（同期・CLI 用）と異なりノンブロッキングな `AsyncClient` を使う。
    """
    return httpx.AsyncClient(follow_redirects=False, timeout=timeout)


def read_body_capped(
    response: httpx.Response, *, max_bytes: int = DEFAULT_MAX_RESPONSE_BYTES
) -> bytes:
    """応答 body を `max_bytes` 上限で読み取る。超過時は `LocatorRejected` を送出する。"""
    body = response.content
    if len(body) > max_bytes:
        raise LocatorRejected(f"応答 body が上限（{max_bytes} bytes）を超過しています")
    return body
