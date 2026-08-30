#!/usr/bin/env bash
set -Eeuo pipefail

readonly REQUIRED_BRANCH="design/st3-engineering"
readonly REQUIRED_COMMIT="95ad704"
readonly DEFAULT_EVAL="weird-git-setup"
readonly SOCKET_WAIT_SECONDS=15

demo_root=""
daemon_pid=""

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

cleanup() {
  local exit_status=$?
  local pty_root=""
  local session_json=""
  local session_name=""
  local watchdog_pid=""
  local -a session_names=()

  trap - EXIT ERR INT TERM
  set +e

  if [[ -n "$daemon_pid" ]] && kill -0 "$daemon_pid" 2>/dev/null; then
    say "Stopping the demo daemon."
    kill -TERM "$daemon_pid" 2>/dev/null
    (
      sleep 5
      kill -KILL "$daemon_pid" 2>/dev/null
    ) &
    watchdog_pid=$!
    wait "$daemon_pid" 2>/dev/null
    kill "$watchdog_pid" 2>/dev/null
    wait "$watchdog_pid" 2>/dev/null
  elif [[ -n "$daemon_pid" ]]; then
    wait "$daemon_pid" 2>/dev/null
  fi

  if [[ -n "$demo_root" && -d "$demo_root" ]]; then
    pty_root="$demo_root/state/pty"
    for _ in 1 2 3 4 5; do
      if ! session_json=$(pty --root "$pty_root" list --json 2>/dev/null); then
        printf 'The demo could not list its PTY sessions. The demo root remains at %s\n' \
          "$demo_root" >&2
        exit_status=1
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
      rm -rf -- "$demo_root"
    else
      printf 'The demo root remains because a PTY session did not stop: %s\n' \
        "$demo_root" >&2
      exit_status=1
    fi
  fi

  exit "$exit_status"
}

on_error() {
  local exit_status=$?
  trap - ERR
  printf 'The demo stopped because a command failed.\n' >&2
  exit "$exit_status"
}

trap cleanup EXIT
trap on_error ERR
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
  eval "$eval_dir" --graph
