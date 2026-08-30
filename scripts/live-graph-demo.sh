#!/usr/bin/env bash
set -Eeuo pipefail

readonly REQUIRED_BRANCH="design/st3-engineering"
readonly REQUIRED_COMMIT="95ad704"
readonly DEFAULT_EVAL="weird-git-setup"
readonly SOCKET_WAIT_SECONDS=15

demo_root=""
daemon_pid=""
eval_pid=""

say() {
  printf '%s\n' "$*"
}

fail() {
  printf 'Cannot start the demo: %s\n' "$*" >&2
  exit 1
}

show_daemon_log() {
  if [[ -n "$demo_root" && -s "$demo_root/daemon.log" ]]; then
    printf 'The daemon log follows:\n' >&2
    while IFS= read -r line; do
      printf '  %s\n' "$line" >&2
    done <"$demo_root/daemon.log"
  fi
}

valid_demo_root() {
  local target_root=$1
  local target_base=${XDG_RUNTIME_DIR:-/tmp}
  local target_name=${target_root##*/}

  if [[ -d "$target_base" ]]; then
    target_base=$(cd -- "$target_base" && pwd -P)
  fi
  target_base=${target_base%/}
  [[ -n "$target_base" ]] || target_base=/
  [[ "${target_root%/*}" == "$target_base" ]]
  [[ "$target_name" =~ ^st3-live-demo\.[A-Za-z0-9]{6}$ ]]
}

process_start_ticks() {
  local process_pid=$1
  local process_stat=""
  local process_fields=""

  IFS= read -r process_stat <"/proc/$process_pid/stat" 2>/dev/null || return 1
  process_fields=${process_stat##*) }
  set -- $process_fields
  printf '%s\n' "${20:-}"
}

demo_daemon_matches() {
  local target_root=$1
  local target_pid=$2
  local -a daemon_arguments=()

  [[ -r "/proc/$target_pid/cmdline" ]] || return 1
  mapfile -d '' -t daemon_arguments <"/proc/$target_pid/cmdline"
  [[ "${daemon_arguments[0]:-}" == "$target_root/st3" ]]
  [[ "${daemon_arguments[1]:-}" == up ]]
}

demo_eval_matches() {
  local target_root=$1
  local target_pid=$2
  local -a eval_arguments=()

  [[ -r "/proc/$target_pid/cmdline" ]] || return 1
  mapfile -d '' -t eval_arguments <"/proc/$target_pid/cmdline"
  [[ "${eval_arguments[0]:-}" == "$target_root/st3" ]]
  [[ "${eval_arguments[1]:-}" == --endpoint ]]
  [[ "${eval_arguments[2]:-}" == "$target_root/st3.sock" ]]
  [[ "${eval_arguments[3]:-}" == eval ]]
}

stop_demo_resources() {
  local target_root=$1
  local target_daemon_pid=$2
  local can_wait_for_daemon=$3
  local attempt=0
  local cleanup_failed=false
  local target_eval_pid=""
  local pty_root=""
  local session_json=""
  local session_name=""
  local -a session_names=()

  if [[ -z "$target_root" ]]; then
    return 0
  fi
  if ! valid_demo_root "$target_root"; then
    printf 'The cleanup refused an invalid demo root: %s\n' "$target_root" >&2
    return 1
  fi

  if [[ -r "$target_root/eval.pid" ]]; then
    IFS= read -r target_eval_pid <"$target_root/eval.pid" || true
  fi
  if [[ "$target_eval_pid" =~ ^[0-9]+$ ]] \
    && demo_eval_matches "$target_root" "$target_eval_pid"; then
    say "Stopping the demo graph."
    kill -TERM "$target_eval_pid" 2>/dev/null
    for ((attempt = 0; attempt < 20; attempt++)); do
      demo_eval_matches "$target_root" "$target_eval_pid" || break
      sleep 0.05
    done
    if demo_eval_matches "$target_root" "$target_eval_pid"; then
      kill -KILL "$target_eval_pid" 2>/dev/null
    fi
  fi

  if [[ -n "$target_daemon_pid" ]] && demo_daemon_matches "$target_root" "$target_daemon_pid"; then
    say "Stopping the demo daemon."
    kill -TERM "$target_daemon_pid" 2>/dev/null
    for ((attempt = 0; attempt < 50; attempt++)); do
      demo_daemon_matches "$target_root" "$target_daemon_pid" || break
      sleep 0.1
    done
    if demo_daemon_matches "$target_root" "$target_daemon_pid"; then
      kill -KILL "$target_daemon_pid" 2>/dev/null
    fi
  fi
  if [[ "$can_wait_for_daemon" == true && -n "$target_daemon_pid" ]]; then
    wait "$target_daemon_pid" 2>/dev/null
  fi

  if [[ -d "$target_root" ]]; then
    pty_root="$target_root/state/pty"
    for _ in 1 2 3 4 5; do
      if ! session_json=$(pty --root "$pty_root" list --json 2>/dev/null); then
        printf 'The demo could not list its PTY sessions. The demo root remains at %s\n' \
          "$target_root" >&2
        cleanup_failed=true
        break
      fi
      mapfile -t session_names < <(jq -r '.[].name' <<<"$session_json")
      if ((${#session_names[@]} == 0)); then
        break
      fi
      say "Stopping the demo sessions."
      for session_name in "${session_names[@]}"; do
        pty --root "$pty_root" kill "$session_name" >/dev/null 2>&1
        pty --root "$pty_root" rm "$session_name" >/dev/null 2>&1
      done
      sleep 0.1
    done

    if session_json=$(pty --root "$pty_root" list --json 2>/dev/null) \
      && [[ $(jq 'length' <<<"$session_json") == 0 ]]; then
      say "Removing the fresh demo root."
      rm -rf -- "$target_root"
    else
      printf 'The demo root remains because a PTY session did not stop: %s\n' \
        "$target_root" >&2
      cleanup_failed=true
    fi
  fi

  [[ "$cleanup_failed" == false ]]
}

cleanup() {
  local exit_status=$?

  trap - EXIT ERR HUP INT TERM
  set +e

  if ! stop_demo_resources "$demo_root" "$daemon_pid" true; then
    exit_status=1
  fi
  if [[ -n "$eval_pid" ]]; then
    wait "$eval_pid" 2>/dev/null
  fi

  exit "$exit_status"
}

run_cleanup_watchdog() {
  local controller_pid=$1
  local controller_start=$2
  local target_daemon_pid=$3
  local target_root=$4

  trap '' HUP INT TERM
  if [[ ! "$controller_pid" =~ ^[0-9]+$ || ! "$target_daemon_pid" =~ ^[0-9]+$ ]]; then
    return 2
  fi
  valid_demo_root "$target_root" || return 2
  printf '%s\n' "$$" >"$target_root/cleanup-watchdog.ready"

  while [[ $(process_start_ticks "$controller_pid" 2>/dev/null || true) == "$controller_start" ]]; do
    sleep 0.2
  done

  set +e
  stop_demo_resources "$target_root" "$target_daemon_pid" false
}

on_error() {
  local exit_status=$?
  trap - ERR
  printf 'The demo stopped because a command failed.\n' >&2
  exit "$exit_status"
}

if [[ "${1:-}" == --cleanup-watchdog ]]; then
  if (($# != 5)); then
    exit 2
  fi
  run_cleanup_watchdog "$2" "$3" "$4" "$5"
  exit $?
fi

trap cleanup EXIT
trap on_error ERR
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

script_path=${BASH_SOURCE[0]}
if [[ "$script_path" == */* ]]; then
  script_parent=${script_path%/*}
else
  script_parent=.
fi
readonly repo_root=$(cd -- "$script_parent/.." && pwd -P)
cd "$repo_root"

list_available_evals() {
  printf 'Available evals under %s/evals/st3:\n' "$repo_root" >&2
  while IFS= read -r eval_file; do
    eval_dir=${eval_file%/eval.kdl}
    printf '  %s\n' "${eval_dir##*/}" >&2
  done < <(find "$repo_root/evals/st3" -mindepth 2 -maxdepth 2 -name eval.kdl -print | sort)
}

resolve_eval() {
  local requested=$1
  local candidate=$requested

  if [[ "$requested" != */* ]]; then
    candidate="$repo_root/evals/st3/$requested"
  fi
  if [[ ! -d "$candidate" ]]; then
    printf 'The eval directory does not exist: %s\n' "$candidate" >&2
    list_available_evals
    exit 1
  fi
  if [[ ! -f "$candidate/eval.kdl" ]]; then
    printf 'The eval directory has no eval.kdl file: %s\n' "$candidate" >&2
    list_available_evals
    exit 1
  fi

  (cd -- "$candidate" && pwd -P)
}

say "Checking the checkout and demo tools."

if [[ ! -t 0 || ! -t 1 ]]; then
  fail "Run this script in an interactive terminal so the graph can attach."
fi

for required_command in cargo rustc codex pty git find sort install jq mktemp sha256sum setsid timeout sleep rm; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    fail "The $required_command command is missing. Install it and put it on PATH."
  fi
done

current_branch=$(git branch --show-current) || fail "Git cannot read the current branch."
if [[ "$current_branch" != "$REQUIRED_BRANCH" ]]; then
  fail "Switch to the $REQUIRED_BRANCH branch. The current branch is ${current_branch:-detached}."
fi

if ! git merge-base --is-ancestor "$REQUIRED_COMMIT" HEAD; then
  fail "Update this branch so it includes commit $REQUIRED_COMMIT."
fi

worktree_changes=$(git status --short) || fail "Git cannot check the worktree."
if [[ -n "$worktree_changes" ]]; then
  printf '%s\n' "$worktree_changes" >&2
  fail "The worktree has changes. Commit or move them before the demonstration."
fi

requested_eval=${1:-$DEFAULT_EVAL}
eval_dir=$(resolve_eval "$requested_eval")

command -v cargo
command -v rustc
command -v codex
command -v pty
command -v git
cargo --version
rustc --version
codex --version
pty --version

runtime_base=${XDG_RUNTIME_DIR:-/tmp}
if [[ ! -d "$runtime_base" || ! -w "$runtime_base" ]]; then
  fail "The runtime directory is not writable: $runtime_base"
fi
runtime_base=$(cd -- "$runtime_base" && pwd -P)

say "Building st3 before the demonstration starts."
cargo build -p st3 --locked

demo_root=$(mktemp -d "$runtime_base/st3-live-demo.XXXXXX")
chmod 700 "$demo_root"

cargo_target_dir=${CARGO_TARGET_DIR:-$repo_root/target}
if [[ "$cargo_target_dir" != /* ]]; then
  cargo_target_dir="$repo_root/$cargo_target_dir"
fi
source_binary="$cargo_target_dir/debug/st3"
if [[ ! -x "$source_binary" ]]; then
  fail "Cargo did not create the expected st3 binary at $source_binary."
fi

demo_binary="$demo_root/st3"
install -m 0755 "$source_binary" "$demo_binary"
binary_sha=$(sha256sum "$demo_binary")
say "Copied one immutable st3 binary: $binary_sha"

socket_path="$demo_root/st3.sock"
daemon_log="$demo_root/daemon.log"
say "Starting the demo daemon in the background. Its log is $daemon_log"
setsid "$demo_binary" up \
  --node live-demo \
  --state-dir "$demo_root/state" \
  --socket "$socket_path" \
  >"$daemon_log" 2>&1 &
daemon_pid=$!

controller_start=$(process_start_ticks "$$") \
  || fail "The cleanup watchdog cannot identify the demo controller."
cleanup_watchdog_log="$demo_root/cleanup-watchdog.log"
setsid "$repo_root/scripts/live-graph-demo.sh" \
  --cleanup-watchdog "$$" "$controller_start" "$daemon_pid" "$demo_root" \
  >"$cleanup_watchdog_log" 2>&1 &
cleanup_watchdog_pid=$!
cleanup_watchdog_ready=false
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  if [[ -s "$demo_root/cleanup-watchdog.ready" ]]; then
    cleanup_watchdog_ready=true
    break
  fi
  if ! kill -0 "$cleanup_watchdog_pid" 2>/dev/null; then
    break
  fi
  sleep 0.05
done
if [[ "$cleanup_watchdog_ready" != true ]]; then
  if [[ -s "$cleanup_watchdog_log" ]]; then
    while IFS= read -r line; do
      printf '  %s\n' "$line" >&2
    done <"$cleanup_watchdog_log"
  fi
  fail "The cleanup watchdog did not start."
fi

say "Waiting for the daemon socket to accept a connection."
socket_ready=false
socket_deadline=$((SECONDS + SOCKET_WAIT_SECONDS))
while ((SECONDS < socket_deadline)); do
  if timeout 1 "$demo_binary" \
    --endpoint "$socket_path" \
    status --scope daemon/live-demo \
    >/dev/null 2>&1; then
    socket_ready=true
    break
  fi
  if ! kill -0 "$daemon_pid" 2>/dev/null; then
    show_daemon_log
    fail "The demo daemon exited before its socket became ready."
  fi
  sleep 0.1
done

if [[ "$socket_ready" != true ]]; then
  show_daemon_log
  fail "The daemon socket did not accept a connection within $SOCKET_WAIT_SECONDS seconds."
fi

say "Running the strict daemon checks. Every check must pass."
if ! "$demo_binary" --endpoint "$socket_path" doctor --strict; then
  show_daemon_log
  fail "A strict daemon check failed. Fix the named check before the demonstration."
fi

say "Starting the $requested_eval graph. This demonstration usually takes three to six minutes."
say "Press Control-C once to stop the eval and clean up the demo."
"$demo_binary" \
  --endpoint "$socket_path" \
  eval "$eval_dir" --graph &
eval_pid=$!
printf '%s\n' "$eval_pid" >"$demo_root/eval.pid"
wait "$eval_pid"
