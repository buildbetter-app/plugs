---
name: plug
description: Read a Plug agent brief, atomically claim work, update it and retain project memories through the configured Plug service.
---

# Plug agent caller

Use the configured Plug service and its authenticated identity. The host supplies
`PLUG_SERVER` and `PLUG_TOKEN`; keep the token out of prompts, arguments, output,
and commits. A configured server failure is a failure, never a reason to open a
different store. Use `plug --project PROJECT --json` before each command.

1. Read `brief --budget-tokens 2048` for project context and visible memories.
2. Inspect `ready --type issue`. Claim work atomically with `claim-next --type
   issue`, rather than picking a ready row and assuming it is yours. A null result
   means no claim was made.
3. Use the returned ID with `get`, `update`, `comment` and `close`. Preserve
   revision expectations, warnings and typed errors. A committed write with a
   warning must not be retried as a new write.
4. Save reusable project context with `memory save --key KEY --content TEXT`;
   inspect it with `memory list`, `memory get ID`, or remove it with
   `memory delete ID`. Memory continuations remain bound to the current store.

Every write accepts an explicit `--idempotency-key`. After a lost response, retry
only the identical operation with the key printed by the caller. Never mint a
new key for an uncertain write, retry a refusal blindly, or rewrite a typed
permission error as an empty success.

For an MCP host, use the existing discovered tools (`agent_brief`,
`bead_claim_next`, `bead_update`, `bead_comment`, and `bead_close`)
with the same semantics and project metadata. For non-core methods such as
`bead.get` or `memory.list`, use discovered `plug_call` with the canonical
method name and named params; there are no `memory_*` tools. The host owns bearer injection.
Do not invent compatibility commands, shell flags, or bypass tools.

Rehearsal mode uses synthetic records and disposable stores only. GitHub writes,
push, deployment, gate side effects, staging and production access are outside
the rehearsal. Local receipts do not certify factory-wide caller migration or
authenticated staging acceptance.
