# Releasing

MoG uses a tag-driven GitHub Actions workflow to build and publish release artifacts.

## Prepare the release PR

1. Update `CHANGELOG.md` with the new version and date.
2. Ensure version references are correct (README badge, `internal/version/version.go`, etc.).
3. Run tests:

   ```bash
   go test ./...
   ```

4. Open a PR titled like: `Release vX.Y.Z`.

## Cut the release

1. Merge the release PR to `main`.
2. Create and push an annotated tag:

   ```bash
   git tag -a v0.1.0 -m "v0.1.0"
   git push origin v0.1.0
   ```

3. The `Release` workflow will:
   - cross-compile `mog` for:
     - Linux: `amd64`, `arm64`, `386`, `armv6`, `armv7`
     - macOS: `amd64`, `arm64`
     - Windows: `amd64`, `arm64`, `386`
     - FreeBSD: `amd64`, `arm64`
   - package artifacts into `dist/` (`.tar.gz` / `.zip`)
   - embed build metadata in `mog --version` and include `BUILD_INFO.txt` inside every archive
   - publish a GitHub Release with assets + `SHA256SUMS`
