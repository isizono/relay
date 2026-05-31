#!/usr/bin/env python3
"""最小SSE中継サーバー（PoC用・使い捨て）。

- POST /send  : メッセージ(JSON想定)を受け取り、接続中の全購読者にSSEでプッシュ
- GET  /stream: SSEストリーム（接続を保持して新着を即プッシュ）

イベントドリブン待ち受け（Monitor + curl -N /stream）の実証だけが目的。
ルーム・認証・フラグ・送信などは一切持たない。
"""
import json
import queue
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 8765
subscribers: list[queue.Queue] = []
lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # アクセスログ抑制
        pass

    def do_GET(self):
        if self.path != "/stream":
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        q: queue.Queue = queue.Queue()
        with lock:
            subscribers.append(q)
        try:
            self.wfile.write(b": connected\n\n")  # 接続確立コメント
            self.wfile.flush()
            while True:
                msg = q.get()
                self.wfile.write(f"data: {msg}\n\n".encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with lock:
                if q in subscribers:
                    subscribers.remove(q)

    def do_POST(self):
        if self.path != "/send":
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode("utf-8")
        # SSEのdata行は1行に正規化（改行は潰す）
        try:
            line = json.dumps(json.loads(raw), ensure_ascii=False)
        except json.JSONDecodeError:
            line = raw.replace("\n", " ").replace("\r", " ")
        with lock:
            n = len(subscribers)
            for q in subscribers:
                q.put(line)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(f"delivered to {n} subscriber(s)".encode("utf-8"))


def main():
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(
        f"SSE relay on http://127.0.0.1:{PORT}  (POST /send, GET /stream)",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
