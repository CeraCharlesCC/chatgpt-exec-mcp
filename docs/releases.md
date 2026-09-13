# Release provenance

The canonical repository is `CeraCharlesCC/chatgpt-exec-mcp`. The `Release`
workflow tests and builds Linux x86_64 binaries on GitHub-hosted Ubuntu 24.04
with Rust 1.93.1 and the committed Cargo lockfile. It remaps builder paths using
the same standalone recipe as local builds.

Pushes to `main`, version tags, and manual runs generate GitHub artifact
attestations using the standard SLSA provenance predicate. Pull requests only
build and test. Version tags also publish a GitHub Release asset. The binary
requires a compatible Linux system (including glibc); it is not a static or
cross-platform binary.

Select and review the full Git commit you intend to use, then verify the actual
downloaded bytes with a current GitHub CLI before executing them:

```bash
gh release download v0.1.0 --repo CeraCharlesCC/chatgpt-exec-mcp \
  --pattern chatgpt-exec-mcp-linux-x86_64
# Set CORE_COMMIT to the reviewed full Git commit (for ops, the core gitlink).
gh attestation verify ./chatgpt-exec-mcp-linux-x86_64 \
  --repo CeraCharlesCC/chatgpt-exec-mcp \
  --signer-workflow CeraCharlesCC/chatgpt-exec-mcp/.github/workflows/release.yml \
  --source-digest "$CORE_COMMIT" \
  --signer-digest "$CORE_COMMIT" \
  --deny-self-hosted-runners
chmod +x ./chatgpt-exec-mcp-linux-x86_64
```

The tag is a download locator; the reviewed Git commit is the source pin. A
locally calculated checksum, commit-shaped label, or private manifest does not
prove publication or build provenance. No separate public SHA registry or
custom attestation predicate is used. Attestation failure stops adoption; a
local build is not substituted for the attested binary.

Attestations establish origin and integrity, not that the source is bug-free
or independently reproducible. Review the source and workflow at the selected
commit. The standalone build command remains useful for development, but local
builds do not acquire GitHub provenance.

References: [GitHub artifact attestations](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations)
and [GitHub CLI verification options](https://cli.github.com/manual/gh_attestation_verify).
