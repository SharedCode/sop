# SOP: An AI-First, Enterprise-Grade SDLC

At the heart of Scalable Objects Persistence (SOP) lies a commitment not just to high-performance code, but to **high-performance engineering**. We believe that a robust, reliable database engine requires a rigorous, automated, and modern Software Development Life Cycle (SDLC).

We have embraced an **AI-First** approach, leveraging the synergy between human architectural oversight and AI-driven implementation to achieve velocity and quality that would be impossible otherwise.

## 🤖 The AI-First Advantage

SOP is a pioneer in **AI-Assisted Engineering**. We don't just use AI to write snippets; we use it to architect and implement entire language bindings.

*   **Polyglot by Design, AI by Implementation**: The core engine is written in Go by human experts. However, our **Rust** and **Java** bindings were developed almost entirely using **GitHub Copilot** and **Gemini**, under strict human supervision.
*   **Synergy**: This approach allows us to maintain a small, highly efficient core team while supporting a vast ecosystem of languages. The AI handles the boilerplate and idiomatic translation, while human engineers ensure architectural integrity, security, and correctness.
*   **Automated Refactoring**: We utilize AI agents to perform complex refactoring tasks across thousands of lines of code, ensuring our codebase remains clean and modern without stalling feature development.

## 🛠️ Professional DevOps & Automation

We treat our infrastructure with the same care as our code. Our release process is fully automated, ensuring that every build is reproducible, secure, and standard-compliant.

### 1. Source Control & Workflow (GitHub)
*   **Branching Strategy**: We follow a feature-branch workflow.
*   **Pull Requests**: Currently, the core team commits directly to `master` to maintain high velocity. However, we are fully prepared to switch to a strict Pull Request workflow immediately upon onboarding new contributors.
*   **CI/CD**: Automated pipelines run unit tests, integration tests, and linters on every push.

### 2. The "One-Stop Hub" Build System
We have engineered a centralized build system that orchestrates releases across all languages simultaneously.
*   **Single Source of Truth**: A master build script coordinates the compilation of the Go core into shared libraries (`.so`, `.dll`, `.dylib`) for every target platform (Linux, Windows, macOS, ARM64/x64).
*   **Consistent Versioning**: When we cut a release (e.g., `v2.0.34`), the system ensures that the Python Wheel, NuGet Package, and Java JAR all ship with the exact same underlying engine version.

### 3. Advanced Packaging
We adhere to the highest packaging standards for each ecosystem:
*   **Python (PyPI)**: We publish **Platform-Specific Wheels**. Instead of a bloated "fat" package, `pip install sop4py` downloads a highly optimized binary tailored exactly to your OS and Architecture.
*   **C# (.NET)**: Our NuGet packages utilize the standard `runtimes/` folder structure for seamless cross-platform compatibility.
*   **Java (Maven)**: We embed native libraries as resources that are automatically extracted by JNA at runtime.

## 🤝 Join the Future of Engineering

SOP is more than a database; it is a testament to what a modern, AI-empowered engineering team can achieve.

*   **Low Friction**: Our automated tooling means you spend time coding, not fighting build scripts.
*   **High Impact**: Your contributions are instantly amplified by our multi-language distribution system.
*   **AI Playground**: Experience firsthand how to integrate AI agents into a real-world, complex software project.

We invite developers who are passionate about **Systems Programming**, **AI Engineering**, and **DevOps** to join us. Help us build the storage engine of the future, using the tools of the future.
