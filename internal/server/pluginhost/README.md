# Plugin host callback runtimes

Each subscriber binding owns a pool of up to four runtime instances. Guest
callbacks may execute concurrently on different pool members. Every member has
its own guest module, linear memory, and runtime state; guest in-memory state is
never shared between members.

`Host.State()` is the shared host-side compare-and-swap store for a binding.
Use it for state that must be observed consistently across callback instances.

Acquiring a runtime has a separate bounded queue wait. If all four members are
busy when that bound expires, the host returns a retryable pool-busy error.
Callback execution receives its normal phase timeout only after a runtime has
been acquired. A callback that fails or exceeds that execution timeout keeps
the existing subscriber-unavailable mapping.
