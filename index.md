---
title: SOP Data & Compute Platform
---

# SOP: one engine for data and compute

SOP is an ACID-compliant B-Tree storage engine with a distributed coordination model built in. It runs embedded in your process on a laptop, and the same code scales out to a cluster that shares storage and coordination state. No separate database server to operate.

![SOP quickstart demo](docs/assets/quickstart.gif)

```
go run ./examples/quickstart
```

## Why it matters

- **ACID transactions on a B-Tree engine.** Ordered keys, ranged scans, and transactional writes in one library.
- **Erasure coding instead of replication.** Storage-efficient fault tolerance built into the file system layer.
- **Swarm computing.** Applications coordinate as one system: work distribution and failover come from the platform, not glue code.
- **Polyglot by design.** One Go core, bindings for Python (`pip install sop4py`), C# (`dotnet add package Sop`), Java and Rust in progress.
- **AI-ready.** Ships with a Data Manager, script runtime, and an embedded copilot for conversational data ops.

## Get it

| Channel | Command |
| :--- | :--- |
| Go | `go get github.com/sharedcode/sop` |
| Python | `pip install sop4py` |
| C# | `dotnet add package Sop` |
| Container demo | `docker run ghcr.io/gerardrecinto/sop-quickstart:stable` |

## Learn more

- [What is SOP, in plain words](WHAT_IS_SOP.md)
- [Getting started](GETTING_STARTED.md)
- [Architecture whitepaper](SOP_ARCHITECTURE_WHITEPAPER.md)
- [Cookbook](COOKBOOK.md)
- [Scalability](SCALABILITY.md)
- [Changelog](CHANGELOG.md)
- [Source and releases](https://github.com/gerardrecinto/sop)

Every commit to master runs the full delivery pipeline: build, tests, container packaging, a staging smoke test, then a human-approved promotion to production and this site.
