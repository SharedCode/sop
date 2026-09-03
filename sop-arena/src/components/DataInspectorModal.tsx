import React from 'react';
import { X, Database, Layers, CheckCircle2 } from 'lucide-react';
import { DataRecord } from '../types';

interface DataInspectorModalProps {
  isOpen: boolean;
  onClose: () => void;
  records: DataRecord[];
}

export const DataInspectorModal: React.FC<DataInspectorModalProps> = ({
  isOpen,
  onClose,
  records,
}) => {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-dark-950/80 backdrop-blur-md animate-fade-in">
      <div className="bg-dark-900 border border-dark-750 rounded-2xl max-w-3xl w-full max-h-[85vh] overflow-y-auto p-6 sm:p-8 shadow-2xl space-y-5">
        
        {/* Header */}
        <div className="flex items-center justify-between pb-3 border-b border-dark-800">
          <div className="flex items-center space-x-3">
            <div className="w-9 h-9 rounded-xl bg-accent-violet/10 border border-accent-violet/30 flex items-center justify-center text-accent-violet">
              <Database className="w-5 h-5" />
            </div>
            <div>
              <h2 className="text-lg font-bold text-white tracking-tight">Embedded B-Tree State Inspector</h2>
              <p className="text-xs text-slate-400">Live inspected key-value slots and partition allocations.</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 rounded-lg bg-dark-850 hover:bg-dark-800 text-slate-400 hover:text-white border border-dark-700 transition"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Data Table */}
        <div className="bg-dark-950 rounded-xl border border-dark-800 overflow-hidden font-mono text-xs">
          <div className="grid grid-cols-12 bg-dark-850 p-3 border-b border-dark-800 text-[11px] font-bold text-slate-400">
            <span className="col-span-4">OBJECT KEY</span>
            <span className="col-span-2">VERSION</span>
            <span className="col-span-3">SHARD ASSIGNMENT</span>
            <span className="col-span-3">STATUS</span>
          </div>

          <div className="divide-y divide-dark-850">
            {records.map((r, idx) => (
              <div key={idx} className="grid grid-cols-12 p-3 hover:bg-dark-900 transition items-center">
                <span className="col-span-4 font-bold text-brand-400 truncate">{r.key}</span>
                <span className="col-span-2 text-slate-300">v{r.version}</span>
                <span className="col-span-3 text-accent-cyan truncate">{r.shard}</span>
                <div className="col-span-3 flex items-center space-x-1.5 text-emerald-400">
                  <CheckCircle2 className="w-3.5 h-3.5" />
                  <span>COMMITTED</span>
                </div>
                <div className="col-span-12 mt-2 p-2 rounded bg-dark-900 border border-dark-800/80 text-[10px] text-slate-400 overflow-x-auto">
                  <code>{JSON.stringify(r.value)}</code>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="flex justify-end pt-2">
          <button
            onClick={onClose}
            className="px-4 py-2 rounded-xl bg-dark-800 hover:bg-dark-700 text-white font-semibold text-xs border border-dark-700 transition"
          >
            Close Inspector
          </button>
        </div>

      </div>
    </div>
  );
};
