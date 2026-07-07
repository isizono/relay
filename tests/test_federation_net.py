"""relay.federation_net テストスイート。

federation の outbound dial 共通ヘルパー（SSRF ガード・応答サイズ上限）を検証する。
DNS 解決を伴わずに完結させるため、ホスト部は IP リテラルのみ使う（実在しないホスト名の
解決失敗ケースのみ `socket.getaddrinfo` を monkeypatch する）。
"""
from __future__ import annotations

import socket
from unittest.mock import Mock

import pytest

from relay import federation_net


class TestValidateLocatorScheme:
    def test_rejects_non_http_scheme(self):
        with pytest.raises(federation_net.LocatorRejected):
            federation_net.validate_locator("ftp://8.8.8.8", allow_private=False)

    def test_rejects_http_without_allow_private(self):
        with pytest.raises(federation_net.LocatorRejected):
            federation_net.validate_locator("http://8.8.8.8", allow_private=False)

    def test_allows_http_with_allow_private(self):
        federation_net.validate_locator("http://8.8.8.8", allow_private=True)

    def test_allows_https_public_ip_by_default(self):
        federation_net.validate_locator("https://8.8.8.8", allow_private=False)

    def test_rejects_missing_hostname(self):
        with pytest.raises(federation_net.LocatorRejected):
            federation_net.validate_locator("https:///no-host-path", allow_private=False)


class TestValidateLocatorInternalRanges:
    """内部 IP レンジは既定 (allow_private=False) で拒否、opt-in で許可される。"""

    _INTERNAL_URLS = [
        "https://127.0.0.1",
        "https://[::1]",
        "https://10.0.0.5",
        "https://192.168.1.1",
        "https://172.16.0.1",
        "https://[fc00::1]",  # ULA
        "https://[fe80::1]",  # link-local
        "https://[::ffff:127.0.0.1]",  # IPv4-mapped IPv6
        "https://[64:ff9b::1]",  # NAT64 well-known prefix
    ]

    @pytest.mark.parametrize("url", _INTERNAL_URLS)
    def test_rejected_by_default(self, url):
        with pytest.raises(federation_net.LocatorRejected):
            federation_net.validate_locator(url, allow_private=False)

    @pytest.mark.parametrize("url", _INTERNAL_URLS)
    def test_allowed_when_opted_in(self, url):
        federation_net.validate_locator(url, allow_private=True)


class TestValidateLocatorDnsFailure:
    def test_unresolvable_hostname_is_rejected(self, monkeypatch):
        def fake_getaddrinfo(host, port):
            raise socket.gaierror("no such host")

        monkeypatch.setattr(federation_net.socket, "getaddrinfo", fake_getaddrinfo)
        with pytest.raises(federation_net.LocatorRejected):
            federation_net.validate_locator(
                "https://nonexistent.invalid.example", allow_private=False
            )


class TestReadBodyCapped:
    def test_returns_body_within_limit(self):
        response = Mock(content=b'{"ok": true}')
        assert federation_net.read_body_capped(response, max_bytes=100) == b'{"ok": true}'

    def test_rejects_body_over_limit(self):
        response = Mock(content=b"x" * 101)
        with pytest.raises(federation_net.LocatorRejected):
            federation_net.read_body_capped(response, max_bytes=100)
