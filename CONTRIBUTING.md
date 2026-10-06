# Contributing

Thank you for helping improve the CollieAi SDKs. Bug reports, questions and
pull requests are welcome.

## How changes land

This repository is published from CollieAi's internal source, where every
change also runs against the CollieAi server's own test suites. We review pull
requests here; an accepted change is then applied in the internal source and
comes back to this repository with the next export. Your pull request is
closed with a link to that commit, and you are credited as a co-author
(`Co-authored-by:`).

Direct pushes to `main` are not possible for anyone: every change arrives
through a reviewed pull request with green CI.

## Before you start

- For a bug, open an issue with the SDK, version, a minimal reproduction and
  what you expected.
- For a larger change, open an issue first so we can agree on the approach.
- Security issues go to security@collieai.io, not to issues
  (see [SECURITY.md](SECURITY.md)).

## Setting up and running the tests

The commands below are the ones CI runs ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)).
No CollieAi account or network access to the service is needed: the tests use
local fakes.

### Python (3.9 or newer)

```bash
cd python
python -m pip install -e ".[dev,openai]" fastapi
python -m pytest tests
```

### Node.js (18 or newer)

```bash
cd node
npm ci
npx tsc --noEmit -p tsconfig.json
npx tsc --noEmit -p examples/tsconfig.json
npx vitest run
npm run build
```

### .NET (8)

```bash
cd dotnet
dotnet test CollieAi.sln
dotnet build examples/AspNetCoreChat/AspNetCoreChat.csproj
```

A run with skipped tests does not count as passing.

## Behavior changes

The three SDKs implement the same contract. A change in behavior is accepted
only with tests, and:

- if it changes shared behavior (poll pacing, verdict resolution, the MCP
  canonical form), it updates or adds a case in [`conformance/`](conformance/)
  and passes in all three SDKs;
- if it changes behavior in one SDK, it says why the others differ or changes
  them in the same pull request.

## Commit messages

One logical change per commit, summary in the imperative, with the area in
front:

```
fix(python): retry a chunk submit after a connection reset
feat(node): accept an AbortSignal in protectStream
docs: clarify the poll budget
```

## Code of conduct

Participation in this project is governed by the
[Code of Conduct](CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions are licensed under the
[MIT License](LICENSE) of this repository.
