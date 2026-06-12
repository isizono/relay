#!/usr/bin/env bash
# recv_monitor.sh のテストスイート。
#
# テスト対象: recv_monitor.sh の --filter-only モードと --channel バリデーション。
# SSE フィルタ挙動（#1a〜#1f）と入力バリデーション（#2a〜#2b）を検証する。
#
# 実行方法:
#   bash tests/test_recv_monitor.sh
#
set -euo pipefail

SCRIPT="$(cd "$(dirname "$0")/.." && pwd)/recv_monitor.sh"

PASS=0
FAIL=0

run_test() {
    local name="$1"
    local actual="$2"
    local expected="$3"
    if [ "$actual" = "$expected" ]; then
        echo "PASS: $name"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $name"
        echo "  expected: $(echo "$expected" | cat -A)"
        echo "  actual:   $(echo "$actual" | cat -A)"
        FAIL=$((FAIL + 1))
    fi
}

run_exit_test() {
    local name="$1"
    local expected_exit="$2"
    local actual_exit="$3"
    if [ "$actual_exit" = "$expected_exit" ]; then
        echo "PASS: $name"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $name (expected exit=$expected_exit, got exit=$actual_exit)"
        FAIL=$((FAIL + 1))
    fi
}

# ---------------------------------------------------------------------------
# #1a: data: 行のみ通す
# ---------------------------------------------------------------------------
actual=$(printf ': connected\n\ndata: {"body":"hi"}\n\n' | "$SCRIPT" --filter-only || true)
run_test "#1a data行のみ通す" "$actual" 'data: {"body":"hi"}'

# ---------------------------------------------------------------------------
# #1b: 複数の data: 行をすべて通す
# ---------------------------------------------------------------------------
actual=$(printf 'data: {"body":"hello"}\ndata: {"body":"world"}\n' | "$SCRIPT" --filter-only || true)
run_test "#1b 複数のdata行すべて通す" "$actual" 'data: {"body":"hello"}
data: {"body":"world"}'

# ---------------------------------------------------------------------------
# #1c: data: 行のみの入力
# ---------------------------------------------------------------------------
actual=$(printf 'data: only-data\n' | "$SCRIPT" --filter-only || true)
run_test "#1c data行のみの入力" "$actual" 'data: only-data'

# ---------------------------------------------------------------------------
# #1d: data: 行なし → 出力なし
# ---------------------------------------------------------------------------
actual=$(printf ': connected\n\nevent: ping\nid: 1\n\n' | "$SCRIPT" --filter-only || true)
run_test "#1d data行なし → 出力なし" "$actual" ''

# ---------------------------------------------------------------------------
# #1e: event:/id: 行を除外
# ---------------------------------------------------------------------------
actual=$(printf 'event: message\nid: 5\ndata: {"body":"ok"}\n' | "$SCRIPT" --filter-only || true)
run_test "#1e event:/id: 行を除外" "$actual" 'data: {"body":"ok"}'

# ---------------------------------------------------------------------------
# #1f: 空入力 → 出力なし
# ---------------------------------------------------------------------------
actual=$(printf '' | "$SCRIPT" --filter-only || true)
run_test "#1f 空入力 → 出力なし" "$actual" ''

# ---------------------------------------------------------------------------
# #2a: 不正な channel_code（空白含む）は exit 2
# ---------------------------------------------------------------------------
set +e
"$SCRIPT" --channel="hello world" 2>/dev/null
actual_exit=$?
set -e
run_exit_test "#2a 空白含むchannel_codeはexit2" "2" "$actual_exit"

# ---------------------------------------------------------------------------
# #2b: 不正な channel_code（記号含む）は exit 2
# ---------------------------------------------------------------------------
set +e
"$SCRIPT" --channel="code;evil" 2>/dev/null
actual_exit=$?
set -e
run_exit_test "#2b 記号含むchannel_codeはexit2" "2" "$actual_exit"

# ---------------------------------------------------------------------------
# #2c: 正常な channel_code はバリデーションを通過する（filter-only モードで動作確認）
#   ※ SSH呼び出しは行わず、--channel のバリデーション通過のみ確認
#   valid_code を渡して --filter-only モードで起動（--channel はバリデーション通過後は無視）
# ---------------------------------------------------------------------------
# バリデーション通過後の --filter-only 動作で正常コードの受容を確認
actual=$(printf 'data: ok\n' | "$SCRIPT" --channel=abc-123_XY --filter-only || true)
run_test "#2c 正常なchannel_codeはバリデーション通過" "$actual" 'data: ok'

# ---------------------------------------------------------------------------
# #2d: --filter-only でも不正な channel_code は exit 2（Major 2: バリデーション位置修正）
#   バリデーションが --filter-only 分岐より前で実行されることを確認
# ---------------------------------------------------------------------------
set +e
printf 'data: ok\n' | "$SCRIPT" --channel="bad;evil" --filter-only 2>/dev/null
actual_exit=$?
set -e
run_exit_test "#2d --filter-onlyでも不正なchannel_codeはexit2" "2" "$actual_exit"

# ---------------------------------------------------------------------------
# #2e: --filter-only 単体（--channel なし）はバリデーションをスキップして動作
#   stdin → data: 行のみ stdout に流す
# ---------------------------------------------------------------------------
actual=$(printf 'data: standalone\n' | "$SCRIPT" --filter-only || true)
run_test "#2e --filter-only単体（--channelなし）は動作" "$actual" 'data: standalone'

# ---------------------------------------------------------------------------
# 結果サマリ
# ---------------------------------------------------------------------------
echo ""
echo "結果: PASS=${PASS}, FAIL=${FAIL}"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
