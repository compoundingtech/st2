#!/usr/bin/env bash
# st2 Claude StopFailure hook: append one private machine-local JSONL record, then surface
# infrastructure wedges through presence and, when declared, the agent's supervisor inbox.
# Fail-open; lifecycle reporting must never wedge the harness.

set -u

identity="${ST_AGENT:-}"
root="${ST_ROOT:-${CATALOG:-}}"
supervisor="${ST_SUPERVISOR:-}"
input="$(cat 2>/dev/null || true)"
error_type="unknown"

# One file per identity avoids cross-agent append contention. The complete provider payload is
# useful for diagnosis, but sensitive keys and token-shaped strings are redacted before the line
# reaches disk. Oversized sanitized payloads keep a bounded preview. Invalid JSON is never copied.
if command -v jq >/dev/null 2>&1; then
  timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || printf unknown)"
  record_identity="${identity:-unknown}"
  record="$(
    printf '%s' "$input" | jq -cs \
      --arg timestamp "$timestamp" \
      --arg identity "$record_identity" '
      def sensitive_key:
        ascii_downcase
        | gsub("[^a-z0-9]"; "")
        | test("authorization|cookie|password|passwd|secret|credential|apikey|privatekey|token");
      def redact_string:
        gsub("(?i)bearer[[:space:]]+[A-Za-z0-9._~+/=-]{8,}"; "Bearer [REDACTED]")
        | gsub("(?i)(access[_-]?token|refresh[_-]?token|api[_-]?key|client[_-]?secret|password|passwd|secret)[[:space:]]*[:=][[:space:]]*[^&[:space:],;]+"; "[REDACTED]")
        | gsub("(?i)(sk-[A-Za-z0-9_-]{12,}|github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9_]{20,}|xox[baprs]-[A-Za-z0-9-]{10,}|AKIA[A-Z0-9]{16})"; "[REDACTED]")
        | gsub("[A-Za-z0-9_-]{20,}\\.[A-Za-z0-9_-]{20,}\\.[A-Za-z0-9_-]{10,}"; "[REDACTED]")
        | gsub("[A-Za-z0-9_+/=~-]{32,}"; "[REDACTED]");
      def scrub:
        walk(
          if type == "object" then
            with_entries(
              if (.key | sensitive_key) then .value = "[REDACTED]" else . end
            )
          elif type == "string" then
            redact_string
          else
            .
          end
        );
      if length == 1 and (.[0] | type) == "object" then
        .[0]
      else
        error("expected one JSON object")
      end
      | . as $original
      | (($original.error_type // $original.error // "unknown") | tostring | redact_string) as $error_type
      | (scrub) as $payload
      | ($payload | tojson) as $encoded
      | {
          schema: 1,
          timestamp: $timestamp,
          event: "StopFailure",
          identity: $identity,
          error_type: $error_type,
          payload: (
            if ($encoded | length) <= 16384 then
              $payload
            else
              {truncated: true, preview: $encoded[0:16384]}
            end
          )
        }
    ' 2>/dev/null
  )"
  if [[ -z "$record" ]]; then
    record="$(jq -cn \
      --arg timestamp "$timestamp" \
      --arg identity "$record_identity" '
      {
        schema: 1,
        timestamp: $timestamp,
        event: "StopFailure",
        identity: $identity,
        error_type: "unknown",
        payload: null,
        payload_error: "invalid_json"
      }
    ' 2>/dev/null || true)"
  fi
  error_type="$(printf '%s' "$record" | jq -r '.error_type // "unknown"' 2>/dev/null || printf unknown)"

  state_base="${XDG_STATE_HOME:-}"
  if [[ -z "$state_base" && -n "${HOME:-}" ]]; then
    state_base="${HOME}/.local/state"
  fi
  if [[ -n "$state_base" && -n "$record" ]]; then
    safe_identity="$(printf '%s' "$record_identity" | tr -c 'A-Za-z0-9._-' '_' 2>/dev/null || printf unknown)"
    record_dir="${state_base}/st2/hook-events/stop-failure"
    record_file="${record_dir}/${safe_identity}.jsonl"
    (
      umask 077
      mkdir -p "$record_dir" || exit 0
      chmod 700 "$record_dir" || true
      printf '%s\n' "$record" >> "$record_file" || exit 0
      chmod 600 "$record_file" || true
    ) 2>/dev/null || true
  fi
fi

if [[ -z "$identity" || -z "$root" ]] || ! command -v st2 >/dev/null 2>&1 || ! command -v jq >/dev/null 2>&1; then
  exit 0
fi

status="away"
notify="yes"
case "$error_type" in
  rate_limit)
    notify=""
    ;;
  authentication_failed | oauth_org_not_allowed | billing_error)
    status="offline"
    ;;
  max_output_tokens | invalid_request | model_not_found)
    exit 0
    ;;
esac

st2 status "$identity" --root "$root" --set "$status" >/dev/null 2>&1 || true
if [[ -n "$notify" && -n "$supervisor" ]]; then
  st2 message send "$supervisor" \
    --root "$root" \
    --as "$identity" \
    --subject "agent ${identity} stopped: ${error_type}" \
    -m "Agent ${identity} ended a Claude turn with error_type=${error_type}. Status set to ${status}; inspect or nudge it when appropriate." \
    >/dev/null 2>&1 || true
fi
exit 0
