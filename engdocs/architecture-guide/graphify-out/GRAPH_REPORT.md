# Scope and limitations

AST-only graph of three public backend interface files (`backend/backend.go`, `backend/types.go`, `backend/errors.go`) at Beads `9d6fff760b7a314b476539198504d77ce0959656`. Source paths omit the `backend/` prefix. This is a focused contract map, not a repository call graph. Alias-heavy Go API extraction does not expose all type relationships. No semantic extraction or LLM tokens were used (0 input, 0 output).

# Graph Report - /tmp/plugs-beads-backend-contract  (2026-10-22)

## Corpus Check
- Corpus is ~2,015 words - fits in a single context window. You may not need a graph.

## Summary
- 16 nodes · 19 edges · 5 communities
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Backend contract 0
- Backend contract 1
- Backend contract 2

## God Nodes (most connected - your core abstractions)
1. `ForEach()` - 4 edges
2. `Collect()` - 4 edges
3. `NewSliceIter()` - 3 edges
4. `Register()` - 2 edges
5. `Lookup()` - 2 edges

## Surprising Connections (you probably didn't know these)
- `ForEach()` --references--> `T`  [EXTRACTED]
  backend.go →   _Bridges community 1 → community 2_

## Import Cycles
- None detected.

## Communities (5 total, 0 thin omitted)

### Community 0 - "Backend contract 0"
Cohesion: 0.38
Nodes (3): Backend, Lookup(), Register()

### Community 1 - "Backend contract 1"
Cohesion: 0.67
Nodes (4): Collect(), ForEach(), Context, Iter

### Community 2 - "Backend contract 2"
Cohesion: 0.67
Nodes (3): NewSliceIter(), SliceIter, T

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `ForEach()` connect `Backend contract 1` to `Backend contract 2`?**
  _High betweenness centrality (0.019) - this node is a cross-community bridge._
- **Why does `Collect()` connect `Backend contract 1` to `Backend contract 2`?**
  _High betweenness centrality (0.019) - this node is a cross-community bridge._