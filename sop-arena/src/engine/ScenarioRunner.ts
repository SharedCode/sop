import { SopBackendInterface } from './SopBackendInterface';
import { sounds } from './SoundEffects';

export interface ScenarioEvent {
  title: string;
  subtitle: string;
  stepNumber: number;
  totalSteps: number;
  progressPercent: number;
  sopTakeaway: string;
}

export type ScenarioCallback = (event: ScenarioEvent | null, isFinished: boolean) => void;

export class ScenarioRunner {
  private backend: SopBackendInterface;
  private intervalId: number | null = null;
  private isRunning: boolean = false;

  constructor(backend: SopBackendInterface) {
    this.backend = backend;
  }

  public runDisaster60s(onUpdate: ScenarioCallback) {
    if (this.isRunning) return;
    this.isRunning = true;
    sounds.playClick();

    // Reset system first to clean state
    this.backend.resetSystem();

    const timeline = [
      {
        atSec: 0,
        title: 'Phase 1: Baseline Workload',
        subtitle: '10,000 transactions/sec steady state across compute workers.',
        sopTakeaway: 'SOP coordinates storage & compute with zero separate database servers.',
        action: () => {
          this.backend.setTargetTps(10000);
        }
      },
      {
        atSec: 6,
        title: 'Phase 2: Traffic Surge (45,000 TPS)',
        subtitle: 'High-volume batch ingest arrives at the gateway.',
        sopTakeaway: 'SOP dynamically balances write pipelines across B-Tree node segments.',
        action: () => {
          this.backend.setTargetTps(45000);
        }
      },
      {
        atSec: 14,
        title: 'Phase 3: Hardware Fault — Storage Shard #02 Offline',
        subtitle: 'Disk array fails during active transaction writing.',
        sopTakeaway: 'Reed-Solomon Erasure Coding reconstructs missing blocks without 3x storage overhead.',
        action: () => {
          this.backend.failStorageNode('store-2');
        }
      },
      {
        atSec: 22,
        title: 'Phase 4: Swarm Worker Crash (Agent Worker 03)',
        subtitle: 'Compute node crashes abruptly mid-transaction batch.',
        sopTakeaway: 'SOP detects heartbeat timeout and redistributes uncommitted tasks to active swarm.',
        action: () => {
          this.backend.killWorker('worker-3');
        }
      },
      {
        atSec: 30,
        title: 'Phase 5: High-Contention Transaction Storm',
        subtitle: 'Concurrent workers attempt simultaneous updates on the same B-Tree keys.',
        sopTakeaway: 'Optimistic Concurrency Control (OCC) guarantees strict serializability with zero corruption.',
        action: () => {
          this.backend.createTransactionStorm(2);
        }
      },
      {
        atSec: 38,
        title: 'Phase 6: Automatic Erasure Reconstruction',
        subtitle: 'SOP parity rebuild succeeds; re-integrating recovered shard.',
        sopTakeaway: 'Automated failover restores 100% capacity without operator intervention.',
        action: () => {
          this.backend.recoverStorageNode('store-2');
        }
      },
      {
        atSec: 46,
        title: 'Phase 7: Swarm Scaling & Self-Healing',
        subtitle: 'New worker spawned to absorb residual queue backlog.',
        sopTakeaway: 'Compute and data scale symmetrically as one unified engine.',
        action: () => {
          this.backend.addWorker();
          this.backend.setTargetTps(20000);
        }
      },
      {
        atSec: 54,
        title: 'Phase 8: System Stabilization',
        subtitle: 'All in-flight transactions finalized with 100.00% consistency.',
        sopTakeaway: 'Zero data lost. Zero dropped transactions. Zero manual glue code.',
        action: () => {
          this.backend.triggerSelfHealing();
        }
      }
    ];

    const totalDurationSec = 60;
    let elapsedSec = 0;
    let currentStepIdx = 0;

    // Trigger initial step
    timeline[0].action();
    onUpdate({
      title: timeline[0].title,
      subtitle: timeline[0].subtitle,
      stepNumber: 1,
      totalSteps: timeline.length,
      progressPercent: 0,
      sopTakeaway: timeline[0].sopTakeaway
    }, false);

    this.intervalId = window.setInterval(() => {
      elapsedSec += 1;
      const progress = Math.min(100, Math.round((elapsedSec / totalDurationSec) * 100));

      // Check if next timeline event triggered
      const nextEvent = timeline.find(t => t.atSec === elapsedSec);
      if (nextEvent) {
        currentStepIdx = timeline.indexOf(nextEvent);
        nextEvent.action();
      }

      const activeEvent = timeline[currentStepIdx];
      onUpdate({
        title: activeEvent.title,
        subtitle: activeEvent.subtitle,
        stepNumber: currentStepIdx + 1,
        totalSteps: timeline.length,
        progressPercent: progress,
        sopTakeaway: activeEvent.sopTakeaway
      }, false);

      if (elapsedSec >= totalDurationSec) {
        this.stop();
        sounds.playVictory();
        onUpdate(null, true);
      }
    }, 1000);
  }

  public runAiAgentWorkforce(onUpdate: ScenarioCallback) {
    if (this.isRunning) return;
    this.isRunning = true;
    sounds.playClick();

    this.backend.resetSystem();
    this.backend.setTargetTps(30000);

    const timeline = [
      {
        atSec: 0,
        title: 'AI Swarm Ingestion: 5,000 Autonomous Agent Tasks',
        subtitle: 'Agents independently reading context, embedding vectors, and persisting memory.',
        sopTakeaway: 'SOP provides unified vector & structured state for LLM memory tiers.',
        action: () => {
          this.backend.setTargetTps(35000);
        }
      },
      {
        atSec: 8,
        title: 'Simulating LLM Agent Crash Mid-Reasoning Loop',
        subtitle: 'Agent Worker 02 terminated while holding uncommitted context buffer.',
        sopTakeaway: 'ACID atomicity rolls back incomplete memory frame, leaving agent state pristine.',
        action: () => {
          this.backend.killWorker('worker-2');
        }
      },
      {
        atSec: 16,
        title: 'Swarm Auto-Rebalancing: Context Handed Off to Worker 05',
        subtitle: 'Zero lost tokens. Task resumes on surviving node in <15ms.',
        sopTakeaway: 'No separate Redis or RabbitMQ needed: coordination is native.',
        action: () => {
          this.backend.addWorker();
        }
      },
      {
        atSec: 25,
        title: 'High-Density Vector Range Scan Across B-Tree Shards',
        subtitle: '128-dimensional similarity searches evaluated with 0ms network hops.',
        sopTakeaway: 'SOP embedded vectors eliminate dedicated vector database clusters.',
        action: () => {
          this.backend.createTransactionStorm(1.5);
        }
      },
      {
        atSec: 35,
        title: 'Swarm Mission Complete',
        subtitle: 'All agent memory contexts synchronized and durable.',
        sopTakeaway: 'One engine for data and compute.',
        action: () => {
          this.backend.triggerSelfHealing();
        }
      }
    ];

    const totalDurationSec = 40;
    let elapsedSec = 0;
    let currentStepIdx = 0;

    timeline[0].action();
    onUpdate({
      title: timeline[0].title,
      subtitle: timeline[0].subtitle,
      stepNumber: 1,
      totalSteps: timeline.length,
      progressPercent: 0,
      sopTakeaway: timeline[0].sopTakeaway
    }, false);

    this.intervalId = window.setInterval(() => {
      elapsedSec += 1;
      const progress = Math.min(100, Math.round((elapsedSec / totalDurationSec) * 100));

      const nextEvent = timeline.find(t => t.atSec === elapsedSec);
      if (nextEvent) {
        currentStepIdx = timeline.indexOf(nextEvent);
        nextEvent.action();
      }

      const activeEvent = timeline[currentStepIdx];
      onUpdate({
        title: activeEvent.title,
        subtitle: activeEvent.subtitle,
        stepNumber: currentStepIdx + 1,
        totalSteps: timeline.length,
        progressPercent: progress,
        sopTakeaway: activeEvent.sopTakeaway
      }, false);

      if (elapsedSec >= totalDurationSec) {
        this.stop();
        sounds.playVictory();
        onUpdate(null, true);
      }
    }, 1000);
  }

  public stop() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    this.isRunning = false;
  }

  public getIsRunning(): boolean {
    return this.isRunning;
  }
}
