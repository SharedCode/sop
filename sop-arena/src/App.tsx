import React, { useState, useEffect, useRef } from 'react';
import { 
  TopologyNode, 
  JobParticle, 
  SystemMetrics, 
  LogEntry, 
  DataRecord, 
  ViewMode 
} from './types';
import { SimulationBackend } from './engine/SimulationBackend';
import { ScenarioRunner, ScenarioEvent } from './engine/ScenarioRunner';
import { Header } from './components/Header';
import { TopologyGraph } from './components/TopologyGraph';
import { MetricsGrid } from './components/MetricsGrid';
import { ControlPanel } from './components/ControlPanel';
import { EventLogStream } from './components/EventLogStream';
import { CompareModal } from './components/CompareModal';
import { InvestorModeView } from './components/InvestorModeView';
import { CopilotDrawer } from './components/CopilotDrawer';
import { MissionSuccessModal } from './components/MissionSuccessModal';
import { DataInspectorModal } from './components/DataInspectorModal';
import { 
  Database, 
  GitCompare, 
  Sparkles, 
  Layers, 
  Flame, 
  Play, 
  Briefcase,
  ShieldCheck,
  Bot
} from 'lucide-react';

export const App: React.FC = () => {
  const backendRef = useRef<SimulationBackend | null>(null);
  const scenarioRef = useRef<ScenarioRunner | null>(null);

  // Application State
  const [viewMode, setViewMode] = useState<ViewMode>('arena');
  const [nodes, setNodes] = useState<TopologyNode[]>([]);
  const [particles, setParticles] = useState<JobParticle[]>([]);
  const [metrics, setMetrics] = useState<SystemMetrics>({
    tps: 15000,
    totalTransactions: 150000,
    consistencyRate: 100.00,
    activeWorkers: 6,
    totalWorkers: 6,
    activeStorageNodes: 4,
    totalStorageNodes: 4,
    avgLatencyMs: 0.28,
    p99LatencyMs: 0.84,
    reliabilityScore: 99.2,
    redistributedJobs: 0,
    erasureChunksRecovered: 0,
    conflictsResolved: 0,
    glueComponentsEliminated: 6,
  });
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [records, setRecords] = useState<DataRecord[]>([]);

  // Modals & Drawers
  const [isCopilotOpen, setIsCopilotOpen] = useState(false);
  const [isCompareOpen, setIsCompareOpen] = useState(false);
  const [isSuccessOpen, setIsSuccessOpen] = useState(false);
  const [isDataInspectorOpen, setIsDataInspectorOpen] = useState(false);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);

  // Scenario Progress
  const [activeScenario, setActiveScenario] = useState<ScenarioEvent | null>(null);
  const [isScenarioRunning, setIsScenarioRunning] = useState(false);

  // Initialize Backend & Listeners
  useEffect(() => {
    const backend = new SimulationBackend();
    backendRef.current = backend;
    scenarioRef.current = new ScenarioRunner(backend);

    backend.init({
      onNodesUpdate: (updatedNodes) => setNodes(updatedNodes),
      onMetricsUpdate: (updatedMetrics) => setMetrics(updatedMetrics),
      onParticlesUpdate: (updatedParticles) => setParticles(updatedParticles),
      onRecordsUpdate: (updatedRecords) => setRecords(updatedRecords),
      onLogEntry: (newLog) => {
        setLogs((prev) => {
          const next = [...prev, newLog];
          if (next.length > 250) return next.slice(next.length - 250);
          return next;
        });
      },
    });

    return () => {
      backend.destroy();
      if (scenarioRef.current) {
        scenarioRef.current.stop();
      }
    };
  }, []);

  // Scenario Handlers
  const handleStartDisaster = () => {
    if (!scenarioRef.current) return;
    setIsScenarioRunning(true);
    scenarioRef.current.runDisaster60s((event, isFinished) => {
      setActiveScenario(event);
      if (isFinished) {
        setIsScenarioRunning(false);
        setActiveScenario(null);
        setIsSuccessOpen(true);
      }
    });
  };

  const handleStartAiSwarm = () => {
    if (!scenarioRef.current) return;
    setIsScenarioRunning(true);
    scenarioRef.current.runAiAgentWorkforce((event, isFinished) => {
      setActiveScenario(event);
      if (isFinished) {
        setIsScenarioRunning(false);
        setActiveScenario(null);
        setIsSuccessOpen(true);
      }
    });
  };

  return (
    <div className="min-h-screen flex flex-col bg-dark-950 text-slate-100">
      
      {/* Top Navbar */}
      <Header
        currentMode={viewMode}
        onSelectMode={(mode) => setViewMode(mode)}
        onOpenCopilot={() => setIsCopilotOpen(true)}
        onOpenCompare={() => setIsCompareOpen(true)}
        reliabilityScore={metrics.reliabilityScore}
      />

      {/* Main Content Area */}
      <main className="flex-grow max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-6 space-y-6">
        
        {viewMode === 'investor' ? (
          <InvestorModeView onBackToArena={() => setViewMode('arena')} />
        ) : (
          <div className="space-y-6">
            
            {/* Arena Hero Banner */}
            <section className="bg-gradient-to-r from-dark-900 via-dark-850 to-dark-900 border border-dark-800 rounded-3xl p-6 sm:p-8 relative overflow-hidden shadow-xl">
              <div className="relative z-10 flex flex-col lg:flex-row items-start lg:items-center justify-between gap-6">
                <div className="max-w-2xl space-y-2">
                  <div className="inline-flex items-center space-x-2 px-2.5 py-0.5 rounded-full bg-brand-500/10 border border-brand-500/30 text-brand-400 text-xs font-mono font-semibold">
                    <Flame className="w-3.5 h-3.5" />
                    <span>ZELTRIN ARENA: Keep the System Alive</span>
                  </div>
                  <h1 className="text-2xl sm:text-3xl lg:text-4xl font-extrabold text-white tracking-tight">
                    Can Your Infrastructure Survive 100k TPS & Node Crashes?
                  </h1>
                  <p className="text-slate-300 text-sm leading-relaxed">
                    Experience what happens when persistence, transactions, and swarm compute live in <strong>one unified engine</strong>. Break nodes, spawn transaction storms, and watch Zeltrin recover automatically with zero glue code.
                  </p>
                </div>

                <div className="flex flex-wrap items-center gap-3">
                  <button
                    onClick={handleStartDisaster}
                    disabled={isScenarioRunning}
                    className="px-5 py-3 rounded-xl bg-gradient-to-r from-brand-600 to-brand-500 hover:from-brand-500 hover:to-brand-400 text-black font-bold text-xs shadow-lg shadow-brand-500/25 transition transform active:scale-95 flex items-center space-x-2"
                  >
                    <Play className="w-4 h-4 fill-current" />
                    <span>SURVIVE THE 60-SEC DISASTER</span>
                  </button>

                  <button
                    onClick={() => setViewMode('investor')}
                    className="px-4 py-3 rounded-xl bg-dark-800 hover:bg-dark-750 border border-dark-700 text-slate-200 font-semibold text-xs flex items-center space-x-2 transition"
                  >
                    <Briefcase className="w-4 h-4 text-accent-cyan" />
                    <span>Investor Deck</span>
                  </button>
                </div>
              </div>
            </section>

            {/* Live Metrics Grid */}
            <MetricsGrid metrics={metrics} />

            {/* Live Distributed Topology Canvas */}
            <div className="space-y-2">
              <div className="flex items-center justify-between px-1">
                <div className="flex items-center space-x-2 text-xs font-bold text-white uppercase tracking-wider font-mono">
                  <Layers className="w-4 h-4 text-brand-400" />
                  <span>Live Distributed Systems Topology</span>
                </div>
                <div className="flex items-center space-x-2">
                  <button
                    onClick={() => setIsDataInspectorOpen(true)}
                    className="text-xs px-3 py-1 rounded-lg bg-dark-900 hover:bg-dark-850 text-accent-violet border border-accent-violet/30 font-mono flex items-center space-x-1.5 transition"
                  >
                    <Database className="w-3.5 h-3.5" />
                    <span>Inspect B-Tree Data</span>
                  </button>
                  <button
                    onClick={() => setIsCompareOpen(true)}
                    className="text-xs px-3 py-1 rounded-lg bg-dark-900 hover:bg-dark-850 text-brand-400 border border-brand-500/30 font-mono flex items-center space-x-1.5 transition"
                  >
                    <GitCompare className="w-3.5 h-3.5" />
                    <span>Without Zeltrin vs With Zeltrin</span>
                  </button>
                </div>
              </div>

              <TopologyGraph
                nodes={nodes}
                particles={particles}
                selectedNodeId={selectedNodeId}
                onSelectNode={(node) => setSelectedNodeId(node.id)}
              />
            </div>

            {/* Control Panel & Real-time Event Stream (2 Columns) */}
            <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
              <div className="lg:col-span-6">
                <ControlPanel
                  onStartDisaster={handleStartDisaster}
                  onStartAiSwarm={handleStartAiSwarm}
                  onIncreaseLoad={(tps) => backendRef.current?.setTargetTps(tps)}
                  onAddWorker={() => backendRef.current?.addWorker()}
                  onRemoveWorker={() => backendRef.current?.removeWorker()}
                  onKillWorker={() => backendRef.current?.killWorker()}
                  onFailStorageNode={() => backendRef.current?.failStorageNode()}
                  onCreateTxStorm={() => backendRef.current?.createTransactionStorm()}
                  onSelfHealing={() => backendRef.current?.triggerSelfHealing()}
                  onReset={() => backendRef.current?.resetSystem()}
                  activeScenario={activeScenario}
                  isScenarioRunning={isScenarioRunning}
                  currentTps={metrics.tps}
                />
              </div>

              <div className="lg:col-span-6">
                <EventLogStream
                  logs={logs}
                  onClearLogs={() => setLogs([])}
                />
              </div>
            </div>

          </div>
        )}

      </main>

      {/* Footer */}
      <footer className="border-t border-dark-800 bg-dark-950 py-6 text-center text-xs text-slate-500 font-mono">
        <div className="max-w-7xl mx-auto px-4 flex flex-col sm:flex-row items-center justify-between gap-3">
          <div className="flex items-center space-x-2">
            <span className="font-semibold text-slate-400">Zeltrin Arena</span>
            <span>•</span>
            <span>Durable Memory and Verification Infrastructure</span>
            <span>•</span>
            <span className="text-brand-400 font-semibold">One engine for data and compute</span>
          </div>
          <div className="flex items-center space-x-4">
            <a href="https://github.com/sharedcode/zeltrin" target="_blank" rel="noopener noreferrer" className="text-brand-400 hover:underline">
              GitHub Repository
            </a>
            <span>•</span>
            <a href="https://sharedcode.github.io/zeltrin/" target="_blank" rel="noopener noreferrer" className="text-slate-400 hover:text-white">
              Zeltrin Technical Demo
            </a>
          </div>
        </div>
      </footer>

      {/* Modals & Drawers */}
      <CompareModal
        isOpen={isCompareOpen}
        onClose={() => setIsCompareOpen(false)}
      />

      <CopilotDrawer
        isOpen={isCopilotOpen}
        onClose={() => setIsCopilotOpen(false)}
      />

      <MissionSuccessModal
        isOpen={isSuccessOpen}
        onClose={() => setIsSuccessOpen(false)}
        metrics={metrics}
        onTryAgain={handleStartDisaster}
      />

      <DataInspectorModal
        isOpen={isDataInspectorOpen}
        onClose={() => setIsDataInspectorOpen(false)}
        records={records}
      />

    </div>
  );
};

export default App;
