import { useState, useRef } from 'react'
import type { FormEvent } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import './App.css'

type Message = {
  role: 'user' | 'ai' | 'system';
  content: string;
};

function App() {
  const [messages, setMessages] = useState<Message[]>([
    { role: 'ai', content: 'Hello! Ask me about your SOP databases or general knowledge.' }
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [copilotOpen, setCopilotOpen] = useState(false);
  const [copilotMinimized, setCopilotMinimized] = useState(false);
  const [explorerOpen, setExplorerOpen] = useState(false);
  const [selectedItem, setSelectedItem] = useState<{title: string, desc: string} | null>(null);

  const abortControllerRef = useRef<AbortController | null>(null);
  const chatHistoryRef = useRef<HTMLDivElement>(null);

  const stopGeneration = () => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
      setLoading(false);
      setMessages(prev => [...prev, { role: 'system', content: 'Stopped.' }]);
    }
  }

  const sendMessage = async (e?: FormEvent) => {
    e?.preventDefault();
    if (!input.trim() || loading) return;

    if (abortControllerRef.current) {
      stopGeneration();
      return;
    }

    const userMessage = input.trim();
    setMessages(prev => [...prev, { role: 'user', content: userMessage }]);
    setInput('');
    setLoading(true);

    const abortController = new AbortController();
    abortControllerRef.current = abortController;

    setMessages(prev => [...prev, { role: 'ai', content: '' }]);

    setTimeout(() => {
      if (chatHistoryRef.current) {
        chatHistoryRef.current.scrollTop = chatHistoryRef.current.scrollHeight;
      }
    }, 50);

    try {
      const response = await fetch('/api/ai/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: userMessage }),
        signal: abortController.signal
      });

      if (!response.ok) throw new Error('Network response was not ok');

      const reader = response.body?.getReader();
      const decoder = new TextDecoder();

      if (reader) {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          const chunk = decoder.decode(value, { stream: true });
          setMessages(prev => {
            const newMessages = [...prev];
            const lastMsg = newMessages[newMessages.length - 1];
            if (lastMsg && lastMsg.role === 'ai') {
              lastMsg.content += chunk;
            }
            return newMessages;
          });

          if (chatHistoryRef.current) {
            chatHistoryRef.current.scrollTop = chatHistoryRef.current.scrollHeight;
          }
        }
      }
    } catch (error: any) {
      if (error.name !== 'AbortError') {
        setMessages(prev => [...prev, { role: 'system', content: `Error: ${error.message}` }]);
      }
    } finally {
      setLoading(false);
      abortControllerRef.current = null;
    }
  }

  return (
    <div className="app-layout">
      <header className="app-header">
        <div className="logo">SOP Platform</div>
        <div className="tools">
          <button className="nav-btn" onClick={() => { setCopilotOpen(!copilotOpen); setCopilotMinimized(false); }}>
            SOP Copilot
          </button>
          <button className="nav-btn mobile-only" onClick={() => setExplorerOpen(!explorerOpen)}>
            Explorer
          </button>
        </div>
      </header>

      <div className="main-workspace">
        <aside className={`pane explorer-pane ${explorerOpen ? 'open' : ''}`}>
          <div className="pane-header">Explorer</div>
          <div className="pane-content">
            <ul className="tree-view">
              <li className="tree-node">Clusters</li>
              <li className="tree-node">Databases
                <ul>
                  <li onClick={() => { setSelectedItem({title: 'UsersDB', desc: 'Main user database node.'}); setExplorerOpen(false); }}>UsersDB</li>
                  <li onClick={() => { setSelectedItem({title: 'LogsDB', desc: 'System and application logs.'}); setExplorerOpen(false); }}>LogsDB</li>
                </ul>
              </li>
              <li className="tree-node">Integrations</li>
              <li className="tree-node">Settings</li>
            </ul>
          </div>
        </aside>

        <main className="pane center-pane">
          <div className="pane-header">Databases & Tables</div>
          <div className="pane-content tables-grid">
            <div className="data-card" onClick={() => setSelectedItem({title: 'Users Table', desc: 'Contains user profiles and role data. Over 1M+ records.'})}>
              <h3>Users Table</h3>
              <p>Records: 1M+</p>
            </div>
            <div className="data-card" onClick={() => setSelectedItem({title: 'Sessions Table', desc: 'Active user sessions.'})}>
              <h3>Sessions Table</h3>
              <p>Records: 45k</p>
            </div>
            <div className="data-card" onClick={() => setSelectedItem({title: 'Metrics', desc: 'System event logs, App latency and health metrics.'})}>
              <h3>Metrics Table</h3>
              <p>Records: 23M</p>
            </div>
          </div>
        </main>

        <aside className="pane details-pane">
          <div className="pane-header">Details</div>
          <div className="pane-content">
            {!selectedItem ? (
              <p className="empty-state">Select an item to view details</p>
            ) : (
              <div className="selected-details">
                <h3>{selectedItem.title}</h3>
                <p>{selectedItem.desc}</p>
                <hr className="details-divider" />
                <p className="details-status">Status: Active</p>
                <button className="action-btn">View Data</button>
              </div>
            )}
          </div>
        </aside>
      </div>

      {/* Floating Copilot Window */}
      <div className={`copilot-window ${!copilotOpen ? 'hidden' : ''} ${copilotMinimized ? 'collapsed' : ''}`}>
        <div className="copilot-header" onDoubleClick={() => setCopilotMinimized(!copilotMinimized)}>
          <span>SOP Copilot</span>
          <div className="copilot-actions">
            <button className="icon-btn" onClick={(e) => { e.stopPropagation(); setCopilotMinimized(!copilotMinimized); }}>
              -
            </button>
            <button className="icon-btn" onClick={(e) => { e.stopPropagation(); setCopilotOpen(false); }}>
              x
            </button>
          </div>
        </div>
        <div className="copilot-body" ref={chatHistoryRef}>
          {messages.map((msg, i) => (
            <div key={i} className={`chat-message ${msg.role}`}>
              <strong>{msg.role === 'user' ? 'You' : msg.role === 'ai' ? 'Copilot' : 'System'}</strong> 
              <div className="msg-text">
                {msg.role === 'user' ? (
                  <pre>{msg.content}</pre>
                ) : (
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>
                    {msg.content}
                  </ReactMarkdown>
                )}
              </div>
            </div>
          ))}
          {loading && <div className="chat-message ai typing">Thinking...</div>}
        </div>
        <form className="copilot-footer" onSubmit={sendMessage}>
          <input 
            type="text" 
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask Copilot..." 
            disabled={loading}
          />
          <button type="submit" disabled={loading || !input.trim()}>
            {loading ? 'Stop' : 'Send'}
          </button>
        </form>
      </div>
    </div>
  )
}

export default App
