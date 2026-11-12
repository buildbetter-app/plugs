# B20 closed native mutation vocabulary

Done = every native EventOp at the authenticated 9d6fff760 pin has an exact
named mapping to independently observed native and public Plug behavior on
memory, SQLite and PostgreSQL, or a named adopted difference with its actual
observations. Completion requires independent exact-source and original focus
plus zero-extra canonical whole review. Source tables alone earn no execution
credit. This new unit is separate from official run 43206939409 on landed98fd.

The pin's `internal/storage/issueops/journal.go:55-99` and
`journal_vocabulary_test.go:19-46` declare **seven** engine kinds and **six**
public kinds. `RunJournalEveryMutationKindLandsARow` drives the same seven.
`b20CheckPinnedVocabulary` authenticates the complete archive SHA256, parses
the actual declaration and both classification sets, and refuses drift in
either direction. The other Plug catalog types remain distinct catalog types;
this enumeration does not invent additional native EventOps for them.

| Native kind | Actual public Plug operation / event | Disposition and proof |
| --- | --- | --- |
| `create` | `bead.create` / `bead.created` | Two real endpoint creates; exact current and committed after images. |
| `update` | `bead.update` / `bead.updated` | Exact title update; same-value no-op and typed refusal preserve rows/head. |
| `close` | `bead.close` / `bead.closed` | Native open/closed and adopted ontology todo/done mapping, exact committed state. |
| `delete` | public `tx.apply` delete / `bead.deleted` | Native null issue image; Plug retained before, absent after/current row, public not_found. |
| `dep_add` | `bead.link` / `bead.linked` | Exact relates-to endpoint/kind, journal payload and physical dependency readback. |
| `dep_remove` | `bead.unlink` / `bead.unlinked` | Exact removed edge, retained readable before and no remaining edge. |
| `comment` | `bead.comment` / `bead.commented` | **NATIVE_ENGINE_ONLY_PLUG_DURABLE_COMMENT**: native journals the exact structured text/author but excludes it from its six-kind public feed; Plug's adopted catalog emits it durably. Both actual observations are required. |

The comment difference is already adopted: the pinned source explicitly
defines its engine-only split; frozen `contracts/events.md:3-6,27-34` combines
the durable log and names `bead.commented`, and B15 in
`spec/behaviors/content.md:225-256` keeps durable comment history/event meaning.
No absence of evidence is classified as a deliberate difference. No new
product policy, hook, prune, retention-floor or compaction API is introduced.

`TestB20EveryNativeMutationKindHasPublicCommittedDisposition` is ordinary
coverage through real `Client.Call`, service/Writer and byte RPC on all three
engines. It checks every committed prefix through all four actual CLI forms,
complete ordered event values, authoritative store readback, no-op/refusal
conservation, original signed-cursor reuse through physical SQL close/reopen
in the same process/key, and the next gapless commit. Memory stays on its
original store. No protected interleaving or private input is used.

The manual `:record_vocabulary_test` selects
`TestB20NativeMutationVocabularyTwins`, builds the same pinned production
binary through the unchanged canonical helper, initializes two independent
public stores, and executes all seven kinds through native CLI operations.
Every prefix retains original export and all since-exclusive limit0 reads.
The actual second-operation UOW abort is separate from Plug admission refusal.
All original SQL numeric tokens remain lossless. Completed individual and
aggregate saved files are read back against raw SQL/command digests for the
four current families and new dependency/comment auxiliary images.

All prior caller/cursor/CLI/native-prune/precision assertions and immutable
originals remain retained. Native prune remains the adopted difference in
B20 history.md:160-169. Operator retention-floor absence is not a B20 failure.
History, admission, official delivery, hosted/current-main and FullS1 remain
separate qualifications. Until the new named executions finish, this unit is
AUTHORED_NOT_RUN.
