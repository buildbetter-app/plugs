# Prepared caller adapters

This directory is an unapplied I8 candidate for the five census hook commands,
the catch-me-up skill, and project/global caller guidance. It is prepared from
the pinned Plug base and never edits live .claude, .codex, Beads stores, or
host settings.

Every adapter takes an absolute --binding path whose schema is
plug-host-binding/1. The binding names one endpoint/incarnation/principal and
exactly cody-ops and signals-v2, each with an absolute workspace and a lossless
original-ID map. Project selection is always an explicit argument; cwd,
BEADS_DIR, labels, and ambient server variables are ignored.

binding.py performs the metadata-free server.hello handshake, then puts
_meta.project and _meta.agent on project RPCs. Principal validation uses the
mounted agent.brief.result.whoami.principal; the catalog-only auth.whoami
method is never called.
HTTP requests carry the required Plug-RPC: 1 admission header and JSON media
headers, as the pinned server transport requires.

hook_adapter.py preserves the hook JSON envelope and event name while rendering
claims, ready records, the real durable/control recentChanges event union
(type, resource, position, and details), ontologySummary, conventions, and
memories. A close reason is rendered only from the shared typed
i8.host.close_reason metadata slot while a row is terminal by its installed
project category; reopened rows must
omit the old resolution. The adapter performs no lifecycle writes; the
separate script lane owns the atomic CloseOp plus SetMeta write. Canonical
Claude/Codex payload members beyond the event name are intentionally ignored
after validating the configured event, and the fixture proves that policy for
all five commands.

catch_me_up_adapter.py pages one unfiltered view with supported fields and
revision ordering, follows the signed cursor chain on one content asOf snapshot,
and reads bead.get at that snapshot. Assignment precedence is current writable
fields.assignee (including explicit null), separate live lease.holder,
workflow assignment, then retained source assignment. Both nikhil and its
canonical user:nikhil principal select rows; output preserves the exact identity.
It applies the nikhil OR source decision selection, flattens the result to the legacy
bd-list JSON record shape while preserving omitted versus null fields and
deterministic order, and returns the original top-level JSON array envelope.
Current status names remain authoritative; historical source status and old
closure data cannot undo a transition or reopen. Source issue_type and otherwise
unavailable source fields remain preserved, without reserved restore metadata.

binding.py reads the installed project-effective ontology before rendering and
again before output. Status names are never renamed or inferred from categories.
Known categories are native active/wip/done/frozen and the pinned Plug
triage/backlog/unstarted/started/completed/canceled vocabulary; unknown status
or category produces a typed refusal. done/completed/canceled are terminal.
ontology.get has no asOf: the second read detects observed drift only and does
not establish an atomic ontology/content snapshot. The native category/import
successor remains separately owned by B21.

prepare.py writes only public hook stanzas, hashes, the complete public
catch-me-up skill preimage/replacement, and unapplied templates into a
caller-supplied disposable output directory. The catch-up patch replaces the
one old BEADS_DIR/bd list instruction in the full body with an explicit
binding/project adapter command; application_check.py applies that patch with
zero fuzz to a disposable full-skill copy.

render.py consumes that captured public preparation packet, verifies every
allowlisted file hash, and writes concrete commands and regenerated patches to
a fresh /tmp/plug-swarm tree. It never reads binding contents or live settings.
Pass absolute --snapshot, --output, --caller-root and --binding paths. Paths
are quoted for the shell, including spaces, quotes, dollar substitutions and
backticks; skill code spans use matching Markdown delimiters. Matchers, status
messages, feature/plugin choices and the rest of the full skill are retained.
application_check.py accepts both captured templates and rendered packets.

Run the synthetic gate from this directory:

    export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH
    python3 -B tools/agentcallers/fixture_test.py
    python3 -B tools/agentcallers/render_fixture.py

The actual composition boundary gate is
TestI8ActualTwoProjectAdapterBoundary in //internal/server/composition:composition_test,
run through remote-bazel. It restores a public synthetic archive into one SQLite
store containing both projects, installs a declared synthetic project-overlay
ontology, uses a real authenticated canonical principal, and invokes the actual
Python hook/catch-up processes through HTTP RPC. This proves the caller boundary
against that installed fixture ontology, not native-category import or real-store
parity. No private input enters the remote gate.

Both that real boundary and TestI8RenderedPublicCallerCommands execute the
commands extracted from rendered settings and the full skill through /bin/sh,
using metacharacters in literal caller/binding paths. The latter also checks
zero-fuzz application, preserved scheduling/settings, failure exit envelopes
and rejection of corrupt or escaping snapshots. These tests do not install
hooks or plugins. On the delivered compatible G1 source, the real boundary also
selects declared noncanonical active/done targets through create, typed close
plus the shared typed reason, and reopen. It runs the emitted commands in both
the terminal and reopened states and checks that current exact status names
and terminal-only resolution survive projection. These read/context adapters
perform no lifecycle writes; the public fixture retains the ordinary guards.
