B03 public native r5 input provenance

This is a byte-preserving gzip/TAR transport of the existing public rich native
fixture. It contains no new capture, source rewrite or synthetic history.

Producer source: 046f972c64ec0875664cc1a907a4d85b67eb427f,
tree 00b12028d65392b76a59aae26fe1b47360e9a354,
parent 99021f543e57d57b27d7a8d0d6263eafa4d2b4cf.

Committed payload: 26027831bada7a85c764f066d39712e26fa81eee,
tree 825736783c65ca5d6573d09afd2b1747699932fc,
path internal/plugdump/testdata/native-rich-r5/.

Every regular file in that exact Git subtree is retained byte-for-byte under
the same relative name. The additional handoff members retain the corrected
delivery, correction, evidence manifest, distinct review, original command,
raw Bazel log, exit code and evidence status. All members have a separate full
SHA-256 in b03-rich-r5-catalog.inputs.json. TAR headers use sorted names,
mode 0444 and mtime zero; gzip mtime is zero. Compression changes no inner bytes.

Transport: b03-rich-r5-inputs.tar.gz, SHA-256
112d9beaf34078be8fdfe0ba745c85a35f2d527944afba0d834f88b831f66a2e,
1,914,362 bytes, 1,148 members. The existing BinarySHA256 exact-path mechanism
admits this public fixture under the standing 19:39Z rule; no checker pattern,
directory exemption or validation change is involved.

The two native source identities remain separate. review-project has 122
authenticated commits and root commitment
a63f4a39ab3579301be116a8ba13a0b4af76b728f6a440da484d662fb374cc06.
delivery-project has 123 authenticated commits and root commitment
b46369e90a37731ee80a9f0d2c1f1109f8ed40ff0d3c8f02f1ddccd12897ef96.
Each has its own unchanged capture-set.json, head.tar and one chained part.
The test never treats these as one merged delta set.

The corrected delivery SHA-256 is
ec5aa36fe9fe33c680c725dab76bcf613d27f05d9633544a667d8d5caac7235f;
the evidence manifest SHA-256 is
5500d4e72947c5321879f0bc1316f3be24662e01d16c048ab5b860ac215db1b2.
The original producer focus is 43100856893. Its three XML cases passed with
zero failures, errors or skips. Producer whole is NOT_RUN. The distinct review
is R5-CORRECTED-FINITE-HANDOFF-046f972c64.md by retained reviewer 8234.
None of these inputs certifies consumer admission, reopen, hosted, landing,
private/official behavior, production or FullS1.

B03 expectations come from the producer's pre-capture raw SQL snapshots,
source milestones, literal catalogs, physical YAML and public CLI outcomes.
No Plug output supplies the expected values. Native close/reclose/reopen
instants are actual observations, never assumed capture-clock equalities.
Machine-local metadata values are opaque original provenance and are never
decoded for B03 semantic comparison or analyzed. The unchanged r4 transport
and its legacy TAR bytes remain in place as separate refusal controls.
