# Contributing to SOP

Thank you for your interest in contributing to Scalable Objects Persistence (SOP)! We welcome contributions from the community.

## Getting Started

1.  **Read the Documentation**:
    *   **[README.md](README.md)**: High-level overview and quick start.
    *   **[ARCHITECTURE.md](ARCHITECTURE.md)**: Understanding the codebase structure, specifically the Public vs. Internal package split.

2.  **Explore the Code**:
    *   SOP V2 is written in Go. We prioritize simplicity and readability.
    *   Check out the unit tests to understand the interfaces.

## Development Workflow

1.  **Communication**:
    *   Before starting a major feature, please open an issue or discussion to coordinate with the authors. This helps avoid duplication of effort.

2.  **Branching Strategy**:
    *   Fork the repository.
    *   Create a feature branch for your changes.
    *   Submit a Pull Request (PR) to the `master` branch.

3.  **Testing**:
    *   **Unit Tests**: Run `go test ./...` for fast feedback.
    *   **Integration Tests**: Critical for verifying backend interactions.
        *   For `inredcfs` (Hybrid backend), you must run:
            ```bash
            export SOP_RUN_INREDCFS_IT=1
            go test -v -tags=integration -count=1 ./inredcfs/integrationtests/...
            ```
    *   Ensure all tests pass before submitting your PR.

## Code Structure & Guidelines

*   **Public vs. Internal**:
    *   Public packages (e.g., `inredcfs`, `streamingdata`) are the user-facing API.
    *   Internal packages (e.g., `internal/inredck`) contain implementation details that should not be exposed.
    *   See [ARCHITECTURE.md](ARCHITECTURE.md) for more details on why `inredck` is internal.

*   **Style**: Follow standard Go idioms and formatting (`gofmt`).

## Questions?

Don't be shy to ask questions in the [Discussions](https://github.com/SharedCode/sop/discussions) tab. We are happy to help!
