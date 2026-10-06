# Security policy

## Reporting a vulnerability

Please report security vulnerabilities privately, not in public issues, pull
requests or discussions:

- email **security@collieai.io**, or
- use GitHub's private vulnerability reporting:
  [Report a vulnerability](https://github.com/CollieAi/collieai-sdk/security/advisories/new).

Include the affected package (`collieai`, `@collieai/sdk` or
`CollieAi.Client`) and version, the steps to reproduce, and the impact you
observed. Do not include real API keys or customer data; synthetic values are
enough.

We confirm receipt within **3 business days**, keep you informed while we
investigate and fix, agree the disclosure date with you, and credit you in
the advisory if you wish.

## Supported versions

| Version | Supported |
|---|---|
| 2.x (latest release) | Yes |
| 1.x and older | No |

The three SDKs share one version number. A security fix ships as a new patch
release of all three, even when only one of them is affected.

## Safe harbor

We will not pursue legal action against research done in good faith that
follows this policy:

- test only against your own CollieAi account and projects;
- do not access, change or delete data that is not yours, beyond the minimum
  needed to demonstrate the issue;
- do not degrade the service (no denial-of-service or load testing) and do
  not use social engineering or physical attacks;
- report the issue promptly and give us reasonable time to fix it before any
  public disclosure.

## Scope

This repository contains the client SDKs. Vulnerabilities in the CollieAi
service itself can be reported to the same address.
