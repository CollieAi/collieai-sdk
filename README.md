# CollieAi SDKs

[![CI](https://github.com/CollieAi/collieai-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/CollieAi/collieai-sdk/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Client SDKs for [CollieAi](https://collieai.io), the AI guardrails service for
LLM applications: prompt injection detection, PII masking, input and output
moderation. You keep calling your own model; the SDK checks the prompt before
the call and streams back **only text CollieAi has released**, never raw model
output.

| Language | Package | Install | Documentation |
|---|---|---|---|
| Python 3.9+ | [`collieai`](https://pypi.org/project/collieai/) | `pip install collieai` | [Python SDK](https://docs.collieai.io/sdks/python-sdk) |
| Node.js 18+ / TypeScript | [`@collieai/sdk`](https://www.npmjs.com/package/@collieai/sdk) | `npm install @collieai/sdk` | [Node SDK](https://docs.collieai.io/sdks/node-sdk) |
| .NET 8 | [`CollieAi.Client`](https://www.nuget.org/packages/CollieAi.Client) | `dotnet add package CollieAi.Client` | [.NET SDK](https://docs.collieai.io/sdks/dotnet-sdk) |

The guardrails run on the CollieAi server: you need a CollieAi API key and a
project whose policy defines the rules. The SDKs are the client side.

## Where the SDKs fit

CollieAi can sit in front of your model in two ways:

- **Proxy.** Point an OpenAI- or Anthropic-compatible client at CollieAi, which
  calls the model for you. No SDK needed.
- **SDK.** Keep calling the model yourself. The SDK sends the prompt to
  CollieAi first, then streams the model's output through CollieAi's
  moderation jobs (the asynchronous API) and gives your application only the
  released text.

## Repository layout

| Directory | Contents |
|---|---|
| [`python/`](python) | Python SDK, tests, examples |
| [`node/`](node) | Node.js / TypeScript SDK, tests, examples |
| [`dotnet/`](dotnet) | .NET SDK, tests, examples |
| [`conformance/`](conformance) | Shared offline vectors: every SDK's tests check the same expected values |

All three SDKs share one version number and are released together. Release
notes: [CHANGELOG.md](CHANGELOG.md).

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md). Report vulnerabilities privately as
described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE), starting with 2.2.0. Earlier releases remain under Apache-2.0.
