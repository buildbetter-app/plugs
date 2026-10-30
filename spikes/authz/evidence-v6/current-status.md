# T2 evidence archive status

This archive preserves the original T2 evidence without reopening the executable
spike. Provenance is deliberately split as follows:

- **Measured source:** `69f28caaf71a673296abe3120c60be5cb3af5aec` produced the
  repaired v6 measurements and is not an archive or product-policy commit.
- **Published evidence:** `93a3cd0cca2579d307fd9a33bd70e2113bf1f6d8` published
  the four v6 artifacts retained here.
- **Repaired/current legacy head:** `bc6074ae2e2241217e2cd6568ddd89ec8454f399`
  is the last published legacy PR head whose history and attribution remain
  intact.
- **New archive merge SHA:**
  `dc8c1273fa0eb964e057c458ef34c89e58b04b39` is the normal forward merge that
  pins Plug main at `f38cde061e262e47224e593da6d39a2e9d17b323` while resolving
  obsolete legacy-tree paths to that main. The final evidence-only archive
  commit is recorded separately in the archive handoff report.

The four structured v6 files remain byte-for-byte and mode-for-mode unchanged
from their publication/current-legacy copies. Their retained result states keep
the page p95 target **<50 ms** and grouped-count p95 target **<250 ms** as
**FAILED**; no latency target is weakened or waived. The 2026-10-28 initial
baseline remains explicitly **invalid historical evidence** because it used a
sparse corpus that exposed all rows, fabricated rows-touched labels, rolled back
Q5 claims, omitted the transaction prefix from closure timing, and lacked an
exact reviewable source binding. Those historical numbers are preserved rather
than rewritten.

Current main separately adopted the fixed-permission decisions. That adoption
is not evidence from this archive and does not grant new runtime, policy,
performance, resource, SQL-engine, or hosted acceptance. This archive performs
no source repair, benchmark rerun, database operation, fixture replacement,
BUILD/MODULE port, or lexicon exemption; `tools/lexicon/source_manifest.txt` is
the pinned main inventory and remains complete and sorted.
