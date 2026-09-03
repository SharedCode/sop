# SOP Arena — Distributed Systems Survival Game & Architecture Demo

[![Deploy to GitHub Pages](https://github.com/sharedcode/sop-arena/actions/workflows/deploy.yml/badge.svg)](https://sharedcode.github.io/sop-arena/)
[![Go Version](https://img.shields.io/badge/Engine-Go_/_WASM-00ADD8?logo=go)](https://github.com/sharedcode/sop)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

> **"Break the system. Watch SOP recover. Experience one engine for data and compute."**

[**Live Interactive Experience: sharedcode.github.io/sop-arena**](https://sharedcode.github.io/sop-arena/)

---

## 🎯 The Vision: "Show Me Why I Should Care"

The traditional approach to explaining distributed storage and compute is writing lengthy technical documentation. **SOP Arena** turns abstract technical features (ACID transactions, B-Tree storage, erasure coding, swarm computing, and failover coordination) into an **interactive 2–3 minute survival simulation**.

Instead of telling investors and CTOs that SOP simplifies infrastructure, **SOP Arena lets them break the system and watch it heal in real-time**.

```
WITHOUT SOP (Current Multi-Component Tax):
Database → Queue → Workers → Distributed Locks → Retries → ZooKeeper → Failover Glue
(4–6 separate failure domains | 15–50ms latency | 60% engineering time on glue code)

WITH SOP (Unified Engine):
Application → SOP
(1 Single Engine | Sub-millisecond execution | 100% ACID consistency | Automatic failover)
```

---

## 🎮 Core Interactive Features

### 1. ⚡ "The 60-Second Disaster" (Scripted 'Aha!' Scenario)
A fully automated, high-intensity simulation demonstrating resilience:
1. **Baseline Load**: 10,000 TPS steady state.
2. **Traffic Surge**: Ingestion spikes to 45,000 TPS.
3. **Hardware Fault**: Storage Shard #02 crashes offline mid-transaction.
4. **Swarm Crash**: Agent Worker 03 terminates abruptly.
5. **Transaction Storm**: Concurrent OCC lock contention resolved in `<0.5ms`.
6. **Erasure Rebuild**: Reed-Solomon parity fragments reconstruct missing blocks.
7. **System Stabilized**: 100.00% consistency maintained with **0 dropped writes**.

### 2. 🤖 Flagship Scenario: "Real-Time AI Agent Workforce"
- Demonstrates **AI + Persistent State + Distributed Compute + Coordination**.
- 5,000 autonomous agent tasks processed in parallel.
- Agents crash mid-thought without corrupting memory context or losing uncommitted write frames.
- Context is handed off to surviving swarm workers in `<15ms`.

### 3. ⚖️ "With SOP vs. Without SOP" Interactive Split
- Visual comparison contrasting traditional fragmented architectures (Postgres + Kafka + Redis + ZooKeeper) with SOP's unified architecture.

### 4. 💼 Investor Mode (Interactive Pitch Deck)
- **The Problem**: The multi-component tax and glue code burden.
- **The Thesis**: "One engine for data and compute."
- **Commercial Use Cases**: Interactive cards for *AI Agent Workforces*, *Real-Time Multiplayer Games*, *Sub-Millisecond Fintech Ledgers*, *Edge/IoT Swarms*, and *Serverless Metadata*.

### 5. 🧠 SOP Copilot (Explainability Q&A)
- Contextual explainer answering:
  - *"Why did the node failure not corrupt the data?"*
  - *"How is this different from Postgres + Kafka + Redis?"*
  - *"Where does the ACID transaction actually happen?"*
  - *"What is the business ROI for a CTO?"*

### 6. 🔊 Procedural Web Audio Synthesizer
- Built using the native Web Audio API with zero external audio files. Synthesizes subtle clicks, commit chimes, fault alarms, and victory fanfares (with instant mute control).

---

## 🛠️ Tech Stack & Architecture

- **Frontend**: React 18 + TypeScript + Vite
- **Styling**: Tailwind CSS (Dark Obsidian & Emerald Telemetry theme)
- **Icons**: Lucide React + Custom SVG
- **Graphics**: Hardware-accelerated HTML5 2D Canvas rendering live bezier connections and particle physics at 60 FPS
- **Deployment**: Static single-page application optimized for GitHub Pages (`base: './'`)

---

## 🚀 Local Development

```bash
# 1. Clone the repository
git clone https://github.com/sharedcode/sop.git
cd sop/sop-arena

# 2. Install dependencies
npm install

# 3. Start development server
npm run dev

# 4. Build for production
npm run build
```

---

## 🌐 GitHub Pages Deployment

### Option A: Standalone Repository (`sharedcode/sop-arena`)
1. Create a new repository: `https://github.com/sharedcode/sop-arena`
2. Push the contents of the `sop-arena/` directory to `main`
3. In **Settings > Pages > Source**, select **`GitHub Actions`**
4. The workflow in `.github/workflows/deploy.yml` will automatically build and publish to:
   `https://sharedcode.github.io/sop-arena/`

### Option B: Monorepo Deployment (`sharedcode/sop`)
The root repository includes `.github/workflows/deploy-arena.yml` which deploys from `./sop-arena/dist` automatically on pushes.

---

## 📄 License & Attribution

SOP (Scalable Objects Persistence) is an open-source project by [SharedCode](https://github.com/sharedcode/sop).  
Licensed under the Apache-2.0 License.
