# relay CLAUDE.md

このファイルは `/code-review` プラグインがPRレビュー時に自動で読み込む。relay固有のレビュー観点を記載する。一般的な良し悪しの指摘に加えて、以下の観点を重点的に確認すること。

## アーキテクチャ上の制約

- DB永続化は `outbox` / `dlq` / `publish_log` / `agent_cards` の4テーブルに限定する設計決定がある（`docs/ARCHITECTURE.md`参照）。`streams` / `memberships` / `subscriptions` はin-memory実装が前提であり、これらをSQLiteテーブル化する変更は要注意。
- 機能モジュール（`streams.py` / `subscriptions.py` / `delivery.py` / `observability.py` 等）は `routes: list[Route]` を公開し、`app.py` はそれを集約するだけという構成になっている。`app.py` にendpoint実装を直書きする変更は構成からの逸脱。
- semantic authZ（業務的な認可判断）はrelayの責務外で、relayはstructural authZ（membership照合・ownership照合）のみを行う設計になっている。relay内にビジネスルール的な認可条件が増えていないか確認する。

## observability

- `observability.record_event` の `level="warning"` は `GET /status` が返す `recent_warnings` リングバッファ（容量50件）に積まれる。デフォルト動作・非強制的な条件分岐（未設定時のフォールバック等）を `warning` にすると、通常運用でバッファが埋まり本当の異常が隠れる。actionable/exceptionalな事象（DLQ移動・lease失効・dispatcherエラー・認証失敗等）に限定されているか確認する。
- Prometheusメトリクスの `help` テキストが表す意味（例: 「送信成功数」）と実装の計上対象（例: 「送信試行回数」でリトライ毎に再計上）が一致しているか確認する。
- retry処理内でのログ・メトリクス発火条件が、状態（初回か再試行か等）を正しく反映しているか確認する。`attempt_count` のような汎用カウンタを別の意味（特定条件の初回判定等）に転用していないか疑う。

## リソース管理・タイムアウト

- テストフィクスチャ・ヘルパーの起動処理は `try/finally` でクリーンアップを保証すること。起動が部分的に成功した状態（一部プロセス/接続が立ち上がった後の例外）でリークしないか確認する。
- タイムアウト・デッドラインの保証は、ブロッキング呼び出し（`iter_lines()` 等のストリーム読み取り）の前後どちらで評価されるかに注意する。ブロッキング呼び出しの戻り値を待った後にしかデッドラインを見ていない実装は、無応答時に指定タイムアウトで打ち切れない。

## at-least-once配達

- outbox / DLQ / 冪等キー（15分dedup）の意味論を変更する場合、`docs/design/relay-v2-wire-api.md` の該当箇所との整合を確認する。
- retryのバックオフはFull Jitter方式（`relay_sdk/backoff.py`）を踏襲する。
