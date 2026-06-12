#!/usr/bin/env bash
# recv_monitor.sh — SSH 経由で bridge recv を実行し、SSE の data: 行を stdout に流す。
#
# relay サーバーからメッセージを受信するためのスクリプト。
# SSH forced command（bridge-connect）経由で SSE を購読し、
# Monitor(persistent) で各 data: 行をイベントとして検知できる（D#2212）。
#
# 使い方:
#   ./recv_monitor.sh --channel=CODE [--host=HOST] [--no-reconnect] [--filter-only]
#
# オプション:
#   --channel=CODE     channel コード（必須）
#   --host=HOST        SSH ホスト（デフォルト: relay）
#   --no-reconnect     接続断時に再接続しない（デバッグ・テスト用）
#   --filter-only      stdin を読んで data: 行のみ stdout に流す（テスト用フィルタ単体モード）
#
# 使用例（Monitor(persistent) で受信待ち）:
#   ./recv_monitor.sh --channel=abc123 --host=relay
#
set -euo pipefail

SSH_HOST="relay"
CHANNEL_CODE=""
NO_RECONNECT=false
FILTER_ONLY=false

# SSE data: 行フィルタ関数（PoC M#171 の grep と同一挙動）
filter_sse_data() {
    # grep がマッチなしのとき exit 1 を返すが、ループ継続のため || true で握りつぶす
    grep --line-buffered '^data:' || true
}

# 引数パース
for arg in "$@"; do
    case "$arg" in
        --channel=*)
            CHANNEL_CODE="${arg#--channel=}"
            ;;
        --host=*)
            SSH_HOST="${arg#--host=}"
            ;;
        --no-reconnect)
            NO_RECONNECT=true
            ;;
        --filter-only)
            FILTER_ONLY=true
            ;;
        *)
            echo "エラー: 未知のオプション: $arg" >&2
            exit 1
            ;;
    esac
done

# --channel=CODE の事前バリデーション（D#2309）
# forced command + bridge_connect.py のパーサーで二重チェックされているが、
# スクリプト自体の防御層として [A-Za-z0-9_-]+ のみ許可する。
# バリデーションは --filter-only 分岐より前で行う（--filter-only でも --channel が
# 指定されていれば同じバリデーションを通す）。
# --filter-only モード単体で --channel を要求しないケース（stdinのみ流す）を許容するため、
# CHANNEL_CODE が空のときはバリデーションをスキップする（filter-only 専用）。
if [ -n "$CHANNEL_CODE" ]; then
    if ! echo "$CHANNEL_CODE" | grep -qE '^[A-Za-z0-9_-]+$'; then
        echo "エラー: --channel の値に不正な文字が含まれています: $CHANNEL_CODE" >&2
        exit 2
    fi
fi

# --filter-only モード: stdin → data: 行のみ stdout へ流して終了（テスト用）
if $FILTER_ONLY; then
    filter_sse_data
    exit 0
fi

# 通常モードでは --channel=CODE は必須
if [ -z "$CHANNEL_CODE" ]; then
    echo "エラー: --channel=CODE が指定されていません" >&2
    exit 1
fi

SSH_CMD="bridge recv --channel=${CHANNEL_CODE}"

if $NO_RECONNECT; then
    # 再接続なし（1回のみ）
    ssh -T -o BatchMode=yes "$SSH_HOST" "$SSH_CMD" | filter_sse_data || true
else
    # 接続断時に再接続（取りこぼしは GetHistory で回収する設計、D#2257）
    while true; do
        ssh -T -o BatchMode=yes "$SSH_HOST" "$SSH_CMD" | filter_sse_data || true
        sleep 1
    done
fi
