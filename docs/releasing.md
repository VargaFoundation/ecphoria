# Releasing

A release is one version, one tag, one set of artifacts. The workspace moves as a unit: every crate
carries `version.workspace = true`, because `ecphoria-gateway 0.4` against `ecphoria-core 0.3` is a
combination nobody tests.

## What a release produces

| Artifact | Where | Signed |
| :-- | :-- | :-- |
| `ghcr.io/vargafoundation/ecphoria:{version}` | GHCR, linux/amd64 + linux/arm64 | cosign (keyless, GitHub OIDC) |
| `…:{version}-memory` | same, memory-only edition ([editions.md](./editions.md)) | cosign |
| SBOM (SPDX JSON), one per image | cosign attestation + GitHub Release asset | cosign attest |
| SLSA build provenance, one per image | attached to the image in GHCR | `attest-build-provenance` |
| `ghcr.io/vargafoundation/ecphoria-operator:{version}` | GHCR | cosign |
| Python SDK, npm SDK | PyPI, npm | — |
| GitHub Release + changelog | GitHub | — |

Images are referenced **by digest** in anything that matters: a tag can be moved, a digest cannot.
The signature and the provenance are over the digest for the same reason.

## Versioning

Semver, and two groups:

**Move with the server**, enforced by `scripts/check-versions.sh` in CI:

- the workspace (every crate, the `ecphoria-server` binary, the image tag)
- `deploy/helm/ecphoria` `appVersion` — the image the chart deploys
- `ops/operator`

**Version independently**, because they are separate packages with their own compatibility story:

- the Helm chart's own `version` (a chart revision is not a server release)
- `sdk/python`, `sdk/typescript`, `bindings/python`

The failure the check catches is a quiet one: a release where the chart still names the previous
`appVersion` deploys the old image with the new chart, and nothing looks wrong until someone asks
why a new endpoint is missing.

## Cutting one

```bash
cargo install cargo-release git-cliff   # once

make check && make test                 # fmt, clippy, tests
cargo test --workspace --no-default-features   # the memory-only edition too
make versions

cargo release 0.2.0                     # bumps, regenerates CHANGELOG.md, commits, tags — locally
git show                                # read what it did
git push && git push --tags             # this is what starts the release workflow
```

`cargo release` is configured in [`release.toml`](../release.toml) not to push: pushing the tag is
what triggers the build, so it stays a deliberate step taken once the working tree is what you meant
it to be.

The changelog is generated from the commit history by `git-cliff` (`cliff.toml`), not written by
hand, so it cannot drift from what actually shipped. Which means the commit messages **are** the
changelog — a release is only as legible as the conventional-commit subjects that went into it.

## Verifying a release

```bash
# Signature (keyless — the identity is the release workflow itself, not a key someone holds)
cosign verify ghcr.io/vargafoundation/ecphoria:0.2.0 \
  --certificate-identity-regexp 'https://github.com/VargaFoundation/ecphoria/.github/workflows/release.yml@.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

# SBOM and build provenance
cosign verify-attestation --type spdxjson ghcr.io/vargafoundation/ecphoria:0.2.0 …
gh attestation verify oci://ghcr.io/vargafoundation/ecphoria:0.2.0 --repo VargaFoundation/ecphoria
```

## If a release goes wrong

Do not move a tag. A moved tag means the signature and the provenance describe something other than
what a user now pulls, and anyone who verified the old digest has no way to notice. Cut the next
patch version instead, and if the bad one is dangerous, delete its tag from GHCR so it cannot be
pulled at all.
