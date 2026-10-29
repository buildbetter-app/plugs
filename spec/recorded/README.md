# Recorded behavior references

This package contains the first independently recorded reference vectors for
BUILD-09. The vectors were produced from the pinned `beads-base` source commit
`9d6fff760b7a314b476539198504d77ce0959656` in disposable workspaces, then
recorded a second time from a fresh workspace. The committed test checks the
source pin, recorder inputs and hashes, the exact raw-vector digest, and
byte-identical repeatability.

The reference engine is embedded Dolt in the pinned source. These files are
reference expectations only; they do not claim that Plug has passed B17 or B24,
and they do not replace the existing connected acceptance targets. Future
SQLite/Postgres replay work must consume these vectors and record any
projection as an explicit deviation.

Run the focused check with:

```sh
bazel --ignore_all_rc_files test //spec/recorded:goldens_test \
  --test_output=all --lockfile_mode=error
```

The recorder commands and source-file hashes are retained in `goldens.json`.
The B17 recorder is the pinned metadata conformance runner. The B24 recorder
runs the pinned `bd` binary through fixed past/future defer inputs and explicit
reopen/claim probes. The B24 source uses the runtime clock for the date
comparison; timestamp-bearing fields are intentionally absent from its
projection, so this artifact is an outcome golden rather than a controlled-time
certificate.
