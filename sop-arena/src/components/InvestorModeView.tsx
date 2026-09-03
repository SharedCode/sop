import React, { useState } from 'react';
import { USE_CASES } from '../data/useCases';
import { 
  Briefcase, 
  Sparkles, 
  TrendingUp, 
  CheckCircle2, 
  Bot, 
  Gamepad2, 
  Landmark, 
  Radio, 
  Server 
} from 'lucide-react';
import { GithubIcon } from './GithubIcon';
import { UseCaseCard } from '../types';

interface InvestorModeViewProps {
  onBackToArena: () => void;
}

export const InvestorModeView: React.FC<InvestorModeViewProps> = ({ onBackToArena }) => {
  const [selectedCase, setSelectedCase] = useState<UseCaseCard>(USE_CASES[0]);

  const renderIcon = (name: string) => {
    switch (name) {
      case 'Bot': return <Bot className="w-5 h-5 text-accent-cyan" />;
      case 'Gamepad2': return <Gamepad2 className="w-5 h-5 text-purple-400" />;
      case 'Landmark': return <Landmark className="w-5 h-5 text-emerald-400" />;
      case 'Radio': return <Radio className="w-5 h-5 text-amber-400" />;
      case 'Server': return <Server className="w-5 h-5 text-blue-400" />;
      default: return <Sparkles className="w-5 h-5 text-brand-400" />;
    }
  };

  return (
    <div className="space-y-10 py-4 max-w-6xl mx-auto">
      
      {/* 1. Hero Pitch & Executive Thesis */}
      <section className="bg-gradient-to-b from-dark-850 to-dark-900 border border-dark-750 rounded-3xl p-8 sm:p-12 relative overflow-hidden glow-emerald">
        <div className="max-w-3xl space-y-4">
          <div className="inline-flex items-center space-x-2 px-3 py-1 rounded-full bg-accent-cyan/10 border border-accent-cyan/30 text-accent-cyan text-xs font-mono font-semibold uppercase">
            <Briefcase className="w-3.5 h-3.5" />
            <span>Executive & Technical Investment Thesis</span>
          </div>

          <h1 className="text-3xl sm:text-5xl font-extrabold text-white tracking-tight leading-tight">
            One Engine for <br />
            <span className="bg-gradient-to-r from-brand-400 via-accent-cyan to-accent-violet bg-clip-text text-transparent">
              Data and Compute.
            </span>
          </h1>

          <p className="text-slate-300 text-base sm:text-lg leading-relaxed">
            Modern enterprise applications spend 60% of engineering bandwidth gluing together databases, message queues, distributed locks, and retry handlers. SOP replaces this fragmented multi-component tax with a single, embedded ACID B-Tree and swarm compute engine.
          </p>

          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 pt-4">
            <div className="bg-dark-950 p-4 rounded-xl border border-dark-800">
              <span className="text-xs text-slate-400 font-mono block">Infrastructure Tax</span>
              <span className="text-2xl font-bold text-rose-400 font-mono">-60%</span>
              <span className="text-[10px] text-slate-500 block">Less Glue Code</span>
            </div>
            <div className="bg-dark-950 p-4 rounded-xl border border-dark-800">
              <span className="text-xs text-slate-400 font-mono block">Local Execution</span>
              <span className="text-2xl font-bold text-brand-400 font-mono">&lt; 0.3ms</span>
              <span className="text-[10px] text-slate-500 block">Zero DB Hops</span>
            </div>
            <div className="bg-dark-950 p-4 rounded-xl border border-dark-800">
              <span className="text-xs text-slate-400 font-mono block">Fault Recovery</span>
              <span className="text-2xl font-bold text-accent-cyan font-mono">100%</span>
              <span className="text-[10px] text-slate-500 block">Zero State Rollback</span>
            </div>
            <div className="bg-dark-950 p-4 rounded-xl border border-dark-800">
              <span className="text-xs text-slate-400 font-mono block">Deployment</span>
              <span className="text-2xl font-bold text-accent-violet font-mono">Embedded</span>
              <span className="text-[10px] text-slate-500 block">Go / Python / C# / WASM</span>
            </div>
          </div>
        </div>
      </section>

      {/* 2. Interactive Enterprise Use Cases */}
      <section className="space-y-6">
        <div>
          <div className="inline-flex items-center space-x-2 text-xs font-mono font-semibold uppercase text-brand-400 mb-2">
            <TrendingUp className="w-3.5 h-3.5" />
            <span>Target Markets & Commercial Applications</span>
          </div>
          <h2 className="text-2xl sm:text-3xl font-bold text-white tracking-tight">Who Benefits from SOP?</h2>
          <p className="text-slate-400 text-sm">Select an industry vertical to explore realistic deployment scenarios and technical advantages.</p>
        </div>

        {/* Horizontal Card Selectors */}
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
          {USE_CASES.map((uc) => {
            const isSelected = selectedCase.id === uc.id;
            return (
              <button
                key={uc.id}
                onClick={() => setSelectedCase(uc)}
                className={`p-4 rounded-2xl border text-left transition flex flex-col justify-between space-y-3 ${
                  isSelected
                    ? 'bg-dark-850 border-brand-500 shadow-lg shadow-brand-500/10 ring-1 ring-brand-500'
                    : 'bg-dark-900 border-dark-800 hover:border-dark-700 text-slate-400 hover:text-white'
                }`}
              >
                <div className="flex items-center justify-between">
                  <div className="p-2 rounded-xl bg-dark-950 border border-dark-800">
                    {renderIcon(uc.iconName)}
                  </div>
                  {uc.id === 'ai-workforce' && (
                    <span className="text-[9px] px-1.5 py-0.5 rounded bg-brand-500/20 text-brand-400 font-mono font-bold">
                      FLAGSHIP
                    </span>
                  )}
                </div>
                <div>
                  <h3 className={`text-xs font-bold ${isSelected ? 'text-white' : 'text-slate-300'}`}>{uc.title}</h3>
                  <p className="text-[10px] text-slate-500 line-clamp-2 mt-1">{uc.tagline}</p>
                </div>
              </button>
            );
          })}
        </div>

        {/* Selected Use Case Deep Dive Card */}
        <div className="bg-dark-900 border border-dark-800 rounded-3xl p-6 sm:p-8 space-y-6">
          <div className="flex flex-wrap items-center justify-between gap-4 pb-4 border-b border-dark-800">
            <div className="flex items-center space-x-3">
              <div className="p-3 rounded-2xl bg-dark-950 border border-dark-700">
                {renderIcon(selectedCase.iconName)}
              </div>
              <div>
                <span className="text-[10px] font-mono font-bold uppercase tracking-wider text-brand-400 px-2 py-0.5 rounded bg-brand-500/10 border border-brand-500/30">
                  {selectedCase.badge}
                </span>
                <h3 className="text-xl font-bold text-white mt-1">{selectedCase.title}</h3>
              </div>
            </div>

            {/* Metrics pills */}
            <div className="flex flex-wrap items-center gap-3">
              {selectedCase.metrics.map((m, idx) => (
                <div key={idx} className="bg-dark-950 px-3 py-2 rounded-xl border border-dark-800 text-center">
                  <span className="text-[10px] text-slate-500 font-mono block">{m.label}</span>
                  <span className="text-sm font-bold font-mono text-brand-400">{m.value}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-3 bg-dark-950 p-5 rounded-2xl border border-dark-800">
              <h4 className="text-xs font-mono font-bold uppercase tracking-wider text-rose-400">Current Industry Problem</h4>
              <p className="text-xs text-slate-300 leading-relaxed">{selectedCase.problem}</p>
            </div>

            <div className="space-y-3 bg-dark-950 p-5 rounded-2xl border border-brand-500/30 glow-emerald">
              <h4 className="text-xs font-mono font-bold uppercase tracking-wider text-brand-400">The SOP Solution</h4>
              <p className="text-xs text-slate-300 leading-relaxed">{selectedCase.sopAdvantage}</p>
            </div>
          </div>

          <div className="space-y-2">
            <span className="text-xs font-mono font-semibold uppercase tracking-wider text-slate-400">Technical Features Utilized:</span>
            <div className="flex flex-wrap gap-2">
              {selectedCase.technicalFeatures.map((feat, idx) => (
                <span key={idx} className="text-xs px-3 py-1.5 rounded-lg bg-dark-950 border border-dark-800 text-slate-300 flex items-center space-x-1.5 font-mono">
                  <CheckCircle2 className="w-3.5 h-3.5 text-brand-400" />
                  <span>{feat}</span>
                </span>
              ))}
            </div>
          </div>
        </div>
      </section>

      {/* 3. Call to Action / Open Source Links */}
      <section className="bg-gradient-to-r from-dark-900 via-dark-850 to-dark-900 border border-dark-800 rounded-3xl p-8 flex flex-col sm:flex-row items-center justify-between gap-6">
        <div>
          <h3 className="text-xl font-bold text-white mb-1">Ready to explore the open-source code?</h3>
          <p className="text-xs text-slate-400">Inspect the Go implementation, benchmarks, and architectural documentation on GitHub.</p>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <button
            onClick={onBackToArena}
            className="px-5 py-2.5 rounded-xl bg-dark-800 hover:bg-dark-700 text-white font-semibold text-xs border border-dark-700 transition"
          >
            Back to Interactive Arena
          </button>
          <a
            href="https://github.com/sharedcode/sop"
            target="_blank"
            rel="noopener noreferrer"
            className="px-5 py-2.5 rounded-xl bg-brand-500 hover:bg-brand-400 text-black font-semibold text-xs transition shadow-lg shadow-brand-500/20 flex items-center space-x-2"
          >
            <GithubIcon className="w-4 h-4 text-black" />
            <span>GitHub Repository</span>
          </a>
        </div>
      </section>

    </div>
  );
};
