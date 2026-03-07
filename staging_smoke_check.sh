#!/usr/bin/env bash
set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8080}"
SERVICE_NAME="${SERVICE_NAME:-ssp}"
OUT_DIR="${OUT_DIR:-$(mktemp -d /tmp/ssp-staging-smoke.XXXXXX)}"
START_TS="$(date '+%Y-%m-%d %H:%M:%S')"

TAG_25="${TAG_25:-}"
TAG_26="${TAG_26:-}"
TAG_WRAPPER="${TAG_WRAPPER:-}"
TAG_INLINE="${TAG_INLINE:-}"
TAG_TIMEOUT="${TAG_TIMEOUT:-}"
SID_NUMERIC="${SID_NUMERIC:-}"

require_cmd() {
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "[!] Missing required command: $1" >&2
		exit 1
	fi
}

require_var() {
	local name="$1"
	if [ -z "${!name:-}" ]; then
		echo "[!] Required environment variable is not set: $name" >&2
		exit 1
	fi
}

fail() {
	echo "[!] $*" >&2
	exit 1
}

step() {
	echo
	echo "[$1/8] $2"
}

assert_file_contains() {
	local file="$1"
	local needle="$2"
	if ! grep -Fq "$needle" "$file"; then
		fail "Expected '$needle' in $file"
	fi
}

assert_xml_like() {
	local file="$1"
	if ! grep -Eq '<\?xml|<VAST' "$file"; then
		fail "Expected XML/VAST output in $file"
	fi
}

fetch_vast() {
	local name="$1"
	local url="$2"
	local file="$OUT_DIR/$name.xml"
	echo "GET $url"
	curl -fsS "$url" -o "$file"
	assert_xml_like "$file"
	printf '%s\n' "$file"
}

metric_value() {
	local key="$1"
	curl -fsS "$BASE/metrics" | awk -v wanted="$key" '$1 == wanted { print $2 }'
}

check_logs_clean() {
	local logs
	logs="$(journalctl -u "$SERVICE_NAME" --since "$START_TS" --no-pager || true)"
	printf '%s\n' "$logs" > "$OUT_DIR/journal.log"
	if printf '%s\n' "$logs" | grep -Eqi 'http 400|invalid xml|non-vast payload|panic|fatal|unknown supply source'; then
		fail "Detected critical log patterns since $START_TS; see $OUT_DIR/journal.log"
	fi
}

main() {
	require_cmd git
	require_cmd go
	require_cmd curl
	require_cmd grep
	require_cmd awk
	require_cmd systemctl
	require_cmd journalctl

	require_var TAG_25
	require_var TAG_26
	require_var TAG_WRAPPER
	require_var TAG_INLINE
	require_var TAG_TIMEOUT

	echo "=== SSP Staging Smoke Check ==="
	echo "Base URL: $BASE"
	echo "Service:  $SERVICE_NAME"
	echo "Artifacts: $OUT_DIR"

	step 1 "Sync, test, build, and restart"
	git pull --ff-only origin main
	git rev-parse --short HEAD
	go test ./...
	go build -o ssp ./cmd/ssp
	systemctl restart "$SERVICE_NAME"
	systemctl is-active --quiet "$SERVICE_NAME" || fail "Service $SERVICE_NAME is not active"
	curl -fsS "$BASE/health" >/dev/null

	step 2 "Validate ORTB 2.5 compatibility path"
	go test ./internal/adapter -run 'TestORTBAdapterDowngradesRequestForORTB25|TestORTBAdapterPreservesTransactionIDWhileRemovingOnlyConfiguredSupplyChainFields' -v
	ortb25_file="$(fetch_vast "ortb25" "$BASE/vast/$TAG_25?app_bundle=com.stage.ortb25&ifa=stage-ortb25&ip=1.1.1.1")"
	head -n 5 "$ortb25_file"

	step 3 "Validate timeout classification and remaining-budget tmax clamping"
	go test ./internal/adapter -run 'TestORTBAdapterClampsOutboundTMaxToClientBudget|TestORTBAdapterClampsOutboundTMaxToRemainingContextBudget' -v
	go test ./internal/pipeline -run 'TestExecuteTracksAdapterErrorsSeparatelyFromInternalErrors' -v
	time curl -fsS "$BASE/vast/$TAG_TIMEOUT?app_bundle=com.stage.timeout&ifa=stage-timeout&ip=1.1.1.1&tmax=120" -o "$OUT_DIR/timeout.xml"
	assert_xml_like "$OUT_DIR/timeout.xml"

	step 4 "Validate VAST version negotiation"
	go test ./internal/vast -run 'TestBuildNoAdForRequestUsesRequestedVASTVersion|TestBuildNoAdForRequestUsesRequestedVAST40Version|TestBuildWrapperUsesRequestedVAST41WithFallbackAndErrorHandling|TestBuildNoAdForRequestUsesRequestedVAST42Version' -v

	step 5 "Validate inline and wrapper VAST features"
	go test ./internal/vast -run 'TestBuildWrapperUsesRequestedVAST41WithFallbackAndErrorHandling|TestBuildInlineAddsViewableMeasurementForVAST41' -v
	inline_file="$(fetch_vast "inline" "$BASE/vast/$TAG_INLINE?app_bundle=com.stage.inline&ifa=stage-inline&ip=1.1.1.1")"
	wrapper_file="$(fetch_vast "wrapper" "$BASE/vast/$TAG_WRAPPER?app_bundle=com.stage.wrapper&ifa=stage-wrapper&ip=1.1.1.1")"
	assert_file_contains "$inline_file" "ViewableImpression"
	assert_file_contains "$inline_file" "UniversalAdID"
	assert_file_contains "$inline_file" "<Error>"
	assert_file_contains "$wrapper_file" "fallbackOnNoAd=\"true\""
	assert_file_contains "$wrapper_file" "followAdditionalWrappers=\"true\""
	assert_file_contains "$wrapper_file" "<VASTAdTagURI"
	assert_file_contains "$wrapper_file" "<Error>"

	step 6 "Validate measurement and delivery-health reporting"
	before_viewables="$(metric_value ssp_viewable_impressions_total)"
	before_viewables="${before_viewables:-0}"
	curl -fsS "$BASE/api/v1/event/viewable?rid=stage-viewable-check&bndl=com.stage.viewable" >/dev/null
	after_viewables="$(metric_value ssp_viewable_impressions_total)"
	after_viewables="${after_viewables:-0}"
	if [ "$after_viewables" -le "$before_viewables" ]; then
		fail "Expected ssp_viewable_impressions_total to increase"
	fi
	delivery_health="$(curl -fsS "$BASE/api/v1/analytics/reports/delivery-health")"
	printf '%s\n' "$delivery_health" > "$OUT_DIR/delivery-health.json"
	printf '%s\n' "$delivery_health" | grep -Fq '"viewability_rate"' || fail "Delivery health response did not include viewability_rate"

	step 7 "Run end-to-end smoke requests"
	fetch_vast "e2e-ortb25" "$BASE/vast/$TAG_25?app_bundle=com.stage.ortb25&ifa=e2e-ortb25&ip=1.1.1.1" >/dev/null
	fetch_vast "e2e-ortb26" "$BASE/vast/$TAG_26?app_bundle=com.stage.ortb26&ifa=e2e-ortb26&ip=1.1.1.1" >/dev/null
	fetch_vast "e2e-wrapper" "$BASE/vast/$TAG_WRAPPER?app_bundle=com.stage.wrapper&ifa=e2e-wrapper&ip=1.1.1.1" >/dev/null
	fetch_vast "e2e-inline" "$BASE/vast/$TAG_INLINE?app_bundle=com.stage.inline&ifa=e2e-inline&ip=1.1.1.1" >/dev/null
	time curl -fsS "$BASE/vast/$TAG_TIMEOUT?app_bundle=com.stage.timeout&ifa=e2e-timeout&ip=1.1.1.1&tmax=120" -o "$OUT_DIR/e2e-timeout.xml"
	assert_xml_like "$OUT_DIR/e2e-timeout.xml"
	if [ -n "$SID_NUMERIC" ]; then
		fetch_vast "e2e-sid" "$BASE/api/vast?sid=$SID_NUMERIC&app_bundle=com.stage.sid&ifa=e2e-sid&ip=1.1.1.1" >/dev/null
	fi
	for file in "$OUT_DIR"/e2e-*.xml; do
		echo "=== $(basename "$file") ==="
		grep -m1 -E '<VAST version=' "$file" || head -n 2 "$file"
	done

	step 8 "Apply release-gate checks"
	check_logs_clean
	curl -fsS "$BASE/health" >/dev/null
	echo "All staging smoke checks passed. Artifacts saved to $OUT_DIR"
}

main "$@"