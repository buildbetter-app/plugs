// Authorization fact integration requirements
//
// Slice is an internal, snapshot-bound port, not a public plugin or token API.
// The host must validate stored fixed facts, retain exact source assignments,
// and check projection integrity. Only core/pipeline and drivers own WriteTx.
// No adapter in this package claims to implement those transactions.
//
// ACLDependency (A) covers every tuple, team, registry, project-active and
// authorization-projection change, including insertion into an empty predicate.
// ContextDependency (C) covers authoritative credential/session/provider and
// sponsor generations/activity, expiry checks, defaults and proof lifecycle.
// Capture C atomically with verification; a new C cannot relabel old context.
// GrantDependency covers every requested scope, even an absent tuple/member.
// Point hits additionally record source record revisions. ProjectForBead reads
// the source record, exact name registry and consistent derived binding;
// missing registry entries still record A and the bead/closure dependencies.
// Groups is complete or errors; an empty enumeration still records A. There
// is no cached or flattened effective-permission set or implicit global scope.
//
// Every recorded key needs a persistent lock/version identity before insertion
// and after deletion. Source/projection mutations and A/C/scope version bumps
// must commit atomically. Guard reuse validates StoreID/incarnation, acquires
// the complete strongest-mode lock set in record < read_fact < sentinel <
// derived order, and acquires head last. The host also records plugins and
// ontology on writes and all actual guard/closure/blocker dependencies. No
// lock key may be discovered after locking. An error is never a miss or an
// empty result; revocation cannot wait for a TTL or asynchronous fan-out.
//
// FilterExpr is a closed reference representation. An engine adapter must bind
// the request's current principal/context, intersect caveats and restrictions,
// apply forbid precedence and the predicate before pagination/counting, and
// record the whole indexed predicate including misses. Unsupported nodes and
// check-only links fail explicitly. Match provides executable finite semantics,
// not measured SQL, claim exhaustion, a protected-management implementation,
// interactive human proof, or transaction/runtime/performance acceptance.
package authz
