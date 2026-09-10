"""`relay.serve`（keepalive 付きサーバー起動エントリポイント）のテスト。

`configure_keepalive` は実 socket を伴わないモックで setsockopt の呼び出し内容を確認する
（Linux 風 `TCP_KEEPIDLE` を使う環境と macOS 風 `TCP_KEEPALIVE` を使う環境の両方を
`monkeypatch` で再現し、実行プラットフォームに依存せず両分岐を検証する）。
`create_listen_socket` のみ実際に 127.0.0.1 の空きポートへ 1 回 bind する。
"""
from __future__ import annotations

import logging
import socket
from unittest.mock import MagicMock

from relay import serve


class TestResolveKeepaliveSettings:
    def test_defaults_when_env_unset(self, monkeypatch):
        monkeypatch.delenv("RELAY_TCP_KEEPIDLE", raising=False)
        monkeypatch.delenv("RELAY_TCP_KEEPINTVL", raising=False)
        monkeypatch.delenv("RELAY_TCP_KEEPCNT", raising=False)
        assert serve.resolve_keepalive_settings() == (60, 10, 3)

    def test_env_vars_override_defaults(self, monkeypatch):
        monkeypatch.setenv("RELAY_TCP_KEEPIDLE", "30")
        monkeypatch.setenv("RELAY_TCP_KEEPINTVL", "5")
        monkeypatch.setenv("RELAY_TCP_KEEPCNT", "2")
        assert serve.resolve_keepalive_settings() == (30, 5, 2)


class TestConfigureKeepalive:
    def test_so_keepalive_is_always_set(self):
        sock = MagicMock()
        serve.configure_keepalive(sock, idle=60, interval=10, count=3)
        sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    def test_linux_style_platform_uses_tcp_keepidle(self, monkeypatch):
        monkeypatch.setattr(socket, "TCP_KEEPIDLE", 4, raising=False)
        monkeypatch.setattr(socket, "TCP_KEEPINTVL", 5, raising=False)
        monkeypatch.setattr(socket, "TCP_KEEPCNT", 6, raising=False)
        sock = MagicMock()
        serve.configure_keepalive(sock, idle=60, interval=10, count=3)
        sock.setsockopt.assert_any_call(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
        sock.setsockopt.assert_any_call(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
        sock.setsockopt.assert_any_call(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)

    def test_macos_style_platform_uses_tcp_keepalive_when_keepidle_absent(self, monkeypatch):
        monkeypatch.delattr(socket, "TCP_KEEPIDLE", raising=False)
        monkeypatch.setattr(socket, "TCP_KEEPALIVE", 16, raising=False)
        sock = MagicMock()
        serve.configure_keepalive(sock, idle=60, interval=10, count=3)
        sock.setsockopt.assert_any_call(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, 60)

    def test_missing_idle_constants_logs_warning_and_does_not_raise(self, monkeypatch, caplog):
        monkeypatch.delattr(socket, "TCP_KEEPIDLE", raising=False)
        monkeypatch.delattr(socket, "TCP_KEEPALIVE", raising=False)
        sock = MagicMock()
        with caplog.at_level(logging.WARNING, logger="relay.serve"):
            serve.configure_keepalive(sock, idle=60, interval=10, count=3)
        assert any(
            "TCP_KEEPIDLE" in record.message and "TCP_KEEPALIVE" in record.message
            for record in caplog.records
        )
        # idle 用の定数が無くても SO_KEEPALIVE 自体の設定は行われる。
        sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    def test_missing_keepintvl_logs_warning_and_does_not_raise(self, monkeypatch, caplog):
        monkeypatch.delattr(socket, "TCP_KEEPINTVL", raising=False)
        sock = MagicMock()
        with caplog.at_level(logging.WARNING, logger="relay.serve"):
            serve.configure_keepalive(sock, idle=60, interval=10, count=3)
        assert any("TCP_KEEPINTVL" in record.message for record in caplog.records)

    def test_missing_keepcnt_logs_warning_and_does_not_raise(self, monkeypatch, caplog):
        monkeypatch.delattr(socket, "TCP_KEEPCNT", raising=False)
        sock = MagicMock()
        with caplog.at_level(logging.WARNING, logger="relay.serve"):
            serve.configure_keepalive(sock, idle=60, interval=10, count=3)
        assert any("TCP_KEEPCNT" in record.message for record in caplog.records)


class TestCreateListenSocket:
    def test_binds_free_port_and_enables_so_keepalive(self):
        sock = serve.create_listen_socket("127.0.0.1", 0, idle=60, interval=10, count=3)
        try:
            host, port = sock.getsockname()
            assert host == "127.0.0.1"
            assert port > 0
            assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
        finally:
            sock.close()


class TestBuildParser:
    def test_defaults(self):
        args = serve.build_parser().parse_args([])
        assert args.host == serve.DEFAULT_HOST
        assert args.port == serve.DEFAULT_PORT

    def test_custom_host_and_port(self):
        args = serve.build_parser().parse_args(["--host", "0.0.0.0", "--port", "9000"])
        assert args.host == "0.0.0.0"
        assert args.port == 9000
