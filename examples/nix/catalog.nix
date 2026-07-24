# Illustrative: a NIX derivation acting as a RENDERER that projects agent-spec files into an st2
# CATALOG. This is the render-side companion to ../ir (convoy-style IR) — a *different* renderer
# targeting the *same* generic spec.
#
# Per the VRS spec's Layers: RENDER (a compiler — knows harnesses, personas, hooks) and RUN (st2 —
# a dumb reconciler over a catalog) are separate layers. nix is *one* renderer; st2 runs whatever
# catalog it finds and never needs to know what "claude" is. This file lowers high-level, harness-
# agnostic agent data → <catalog>/<host>/<identity>/agent.kdl (the runner-normative spec, §2), and
# stages the workspace persona overlay for activation.
#
#   Build:   nix-build catalog.nix --argstr workspace /abs/path/to/a/repo
#   Check:   st2 validate ./result     # the contract-check a renderer's CI gates on (fails → fix)
#   Run:     st2 up ./result
#
# `st2 validate` is the key handshake: nix emits a catalog, validate confirms it hit st2's contract
# *before* anything runs — so a renderer change that breaks the spec is caught at build time.

{ pkgs ? import <nixpkgs> { }
, # The repo each agent runs in. Like ../ir, override this to a real path.
  workspace ? "/replace/with/a/real/repo/path"
}:

let
  inherit (pkgs) lib;

  host = "demo";

  # ---- The agents, as generic data -----------------------------------------------------------
  # This is the harness-agnostic shape any renderer targets: role/harness/model/persona + wiring.
  # A CoS/root agent (no supervisor) and a worker it supervises.
  agents = {
    cos = {
      identity = "demo-cos-claude";
      role = "cos";
      harness = "claude";
      model = "opus";
      persona = "cos";
    };
    worker = {
      identity = "demo-worker-claude";
      role = "worker";
      harness = "claude";
      model = "opus";
      persona = "worker";
      supervisor = "demo-cos-claude";
    };
  };

  busId = a: "${host}.${a.identity}";

  # ---- Render helpers (the "compiler" a real renderer expands) --------------------------------
  # The harness command st2 runs verbatim. A real renderer bakes the persona/model/boot prompt in
  # here; st2 executes it opaquely under `sh -c` and stays harness-agnostic.
  agentCommand = a:
    "exec ${a.harness} --permission-mode bypassPermissions --model '${a.model}' "
    + "'Boot: set your st status to available, drain your inbox, then stand by for work via ding.'";

  # The ding sidecar — watches the agent's inbox and pokes its pty (kick-driven, not polling).
  dingCommand = a:
    "st ding ${busId a} --identity ${busId a} --root $CATALOG/smalltalk";

  # Lower one agent → the agent.kdl st2 consumes (spec §2 runner-normative fields only; render-only
  # fields like harness/model/persona are baked into the command above, never emitted as spec keys).
  # Note `$CATALOG` stays literal — st2 expands it at spawn against the catalog root.
  mkAgentKdl = a: ''
    // Rendered by the nix example renderer — one renderer targeting the generic st2 spec.
    agent "${a.identity}" {
      identity "${a.identity}"
      host "${host}"
      type "service"
      workspace "${workspace}"
    ${lib.optionalString (a ? supervisor) "  supervisor \"${a.supervisor}\"\n"}  pty "agent" {
        command #"${agentCommand a}"#
        tags role="agent"
        env {
          ST_AGENT "${busId a}"
          ST_ROOT "$CATALOG/smalltalk"
          PTY_ROOT "$CATALOG/pty"
        }
      }
      exec "ding" {
        command #"${dingCommand a}"#
        env {
          ST_AGENT "${busId a}"
          ST_ROOT "$CATALOG/smalltalk"
          PTY_ROOT "$CATALOG/pty"
        }
      }
    }
  '';

  # The persona overlay Claude Code auto-loads from the workspace cwd (.claude/rules/*.md, whose
  # @-import points at the persona). A real renderer copies the SHA-pinned persona; here it's a stub
  # so the example is self-contained. It is *staged* in the store (a pure derivation can't write into
  # a workspace outside $out) and materialized by activate.sh — the impure step, like HM activation.
  mkPersona = a: ''
    # Persona: ${a.role}
    (Illustrative persona for ${busId a}. A real renderer substitutes the SHA-pinned role persona.)
  '';

  agentKdlFile = a: pkgs.writeText "${a.identity}.agent.kdl" (mkAgentKdl a);
  personaFile = a: pkgs.writeText "${a.identity}.PERSONA.md" (mkPersona a);

  # Assemble one agent into the catalog tree ($out) — pure.
  placeAgent = a: ''
    mkdir -p "$out/${host}/${a.identity}"
    cp ${agentKdlFile a} "$out/${host}/${a.identity}/agent.kdl"

    # Bus member dirs (native-bus presence): resources/inbox + archive.
    mkdir -p "$out/${host}/${a.identity}/resources/inbox" "$out/${host}/${a.identity}/resources/archive"

    # Stage the overlay for activation (see activate.sh).
    mkdir -p "$out/overlay/${a.identity}"
    cp ${personaFile a} "$out/overlay/${a.identity}/PERSONA.md"
  '';

in
pkgs.runCommand "st2-catalog-demo" { } ''
  ${lib.concatMapStringsSep "\n" placeAgent (lib.attrValues agents)}

  # activate.sh — the impure step: link each agent's staged persona overlay into its workspace cwd
  # (Claude Code loads .claude/rules from the workspace). Run it after build, once, per host.
  cat > "$out/activate.sh" <<'SH'
  #!/bin/sh
  set -eu
  WORKSPACE="${workspace}"
  for id in ${lib.concatStringsSep " " (map (a: a.identity) (lib.attrValues agents))}; do
    mkdir -p "$WORKSPACE/.convoy" "$WORKSPACE/.claude/rules"
    cp "$(dirname "$0")/overlay/$id/PERSONA.md" "$WORKSPACE/.convoy/PERSONA.md"
    printf '@../../.convoy/PERSONA.md\n' > "$WORKSPACE/.claude/rules/convoy.md"
  done
  echo "overlay activated into $WORKSPACE"
  SH
  chmod +x "$out/activate.sh"

  echo "nix renderer → st2 catalog at $out (validate with: st2 validate $out)"
''
