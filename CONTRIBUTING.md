# Contributing to go-ssekit

Thanks for taking the time to help! Bug reports, docs fixes and pull requests are all welcome.

## Reporting a bug

Open an issue using the **Bug report** template. A minimal program that reproduces the problem is the fastest way to a fix.

Security problems go through [private vulnerability reporting](https://github.com/shani8615/go-ssekit/security/advisories/new), not public issues. See [SECURITY.md](SECURITY.md).

## Development

You need Go 1.24 or newer.

```bash
go test -race ./...        # unit tests
go vet ./...
golangci-lint run ./...    # https://golangci-lint.run
```

## Pull requests

- **One change per PR.** Small, focused PRs get reviewed quickly.
- **Tests come with the change.** Every bug fix needs a regression test that fails without the fix.
- **No new dependencies** without opening an issue first. This library depends only on the standard library, and that's a feature.
- **Public API changes** need a short note in the PR description explaining why. Until v1.0.0, breaking changes are possible but should be rare and justified.
- Keep `go test -race ./...` and `golangci-lint run` green. CI runs both.

## Be kind

Assume good intent, keep discussions technical, and help newcomers.
