import { useEffect, useState, useCallback } from 'react'
import { api } from '../services/api'
import { useToast } from '../hooks/useToast'

const ICONS = {
  gmail: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <rect x="2" y="5" width="20" height="14" rx="2"/>
      <path d="M2 7l10 6 10-6"/>
    </svg>
  ),
  sheets: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <rect x="4" y="3" width="16" height="18" rx="2"/>
      <line x1="4" y1="9" x2="20" y2="9"/>
      <line x1="4" y1="15" x2="20" y2="15"/>
      <line x1="12" y1="3" x2="12" y2="21"/>
    </svg>
  ),
  calendar: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="4" width="18" height="17" rx="2"/>
      <line x1="16" y1="2" x2="16" y2="6"/>
      <line x1="8" y1="2" x2="8" y2="6"/>
      <line x1="3" y1="10" x2="21" y2="10"/>
    </svg>
  ),
  slack: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="10" width="4" height="11" rx="2"/>
      <rect x="10" y="3" width="4" height="11" rx="2"/>
      <rect x="17" y="10" width="4" height="11" rx="2"/>
      <rect x="10" y="17" width="11" height="4" rx="2"/>
    </svg>
  ),
  openai: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="9"/>
      <path d="M12 3c-2 3-3 6-3 9s1 6 3 9"/>
      <path d="M12 3c2 3 3 6 3 9s-1 6-3 9"/>
      <line x1="3" y1="12" x2="21" y2="12"/>
    </svg>
  ),
  grok: (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/>
    </svg>
  ),
}

export default function ConnectorsPage() {
  const toast = useToast()
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState(null)

  const load = useCallback(async () => {
    setLoading(true)
    try { setItems(await api.listConnectors()) }
    catch (e) { toast.error(e.message) }
    finally { setLoading(false) }
  }, [toast])

  useEffect(() => { load() }, [load])

  const handleConnect = async (c) => {
    if (!c.configured) {
      toast.error(`${c.name} not configured on the server`)
      return
    }
    if (c.id === 'gmail') {
      window.location.href = api.gmailAuthUrl()
    } else if (c.id === 'openai' || c.id === 'grok') {
      setBusy(c.id)
      try {
        await api.testAiConnector(c.id)
        toast.success(`${c.name} connection verified`)
      } catch (e) {
        toast.error(e.message)
      } finally {
        setBusy(null)
      }
    } else {
      toast.info(`${c.name} OAuth flow coming soon`)
    }
  }

  const handleDisconnect = async (c) => {
    if (!confirm(`Disconnect ${c.name}?`)) return
    setBusy(c.id)
    try {
      await api.disconnectConnector(c.id)
      toast.success(`Disconnected ${c.name}`)
      load()
    } catch (e) {
      toast.error(e.message)
    } finally {
      setBusy(null)
    }
  }

  return (
    <div className="connectors-page">
      <div className="connectors-header">
        <div>
          <h2 className="connectors-title">Connectors</h2>
          <div className="connectors-subtitle">Link third-party services — AI providers, email, spreadsheets, and more.</div>
        </div>
      </div>

      {loading ? (
        <div className="connectors-empty">Loading…</div>
      ) : items.length === 0 ? (
        <div className="connectors-empty">No connectors available.</div>
      ) : (
        <div className="connectors-grid">
          {items.map(c => (
            <div key={c.id} className={`connector-card ${c.connected ? 'connected' : ''}`}>
              <div className="connector-icon">{ICONS[c.id] || ICONS.gmail}</div>
              <div className="connector-info">
                <div className="connector-name-row">
                  <span className="connector-name">{c.name}</span>
                  {c.connected && <span className="connector-dot" title="Connected"/>}
                </div>
                <div className="connector-desc">{c.description}</div>
                <div className="connector-status">
                  {!c.configured && <span className="status-muted">Not configured on server</span>}
                  {c.configured && c.connected && (
                    <span className="status-connected">
                      Connected{c.account ? ` · ${c.account}` : ''}
                    </span>
                  )}
                  {c.configured && !c.connected && <span className="status-idle">Not connected</span>}
                </div>
              </div>
              <div className="connector-actions">
                {(c.id === 'openai' || c.id === 'grok') ? (
                  c.connected ? (
                    <button
                      className="btn-small"
                      disabled={busy === c.id}
                      onClick={() => handleConnect(c)}
                    >
                      {busy === c.id ? 'Testing...' : 'Test'}
                    </button>
                  ) : (
                    <span className="status-muted" style={{ fontSize: 11 }}>Add API key in .env</span>
                  )
                ) : c.connected ? (
                  <button
                    className="btn-small btn-danger"
                    disabled={busy === c.id}
                    onClick={() => handleDisconnect(c)}
                  >
                    {busy === c.id ? '…' : 'Disconnect'}
                  </button>
                ) : (
                  <button
                    className="btn-primary"
                    disabled={!c.configured}
                    onClick={() => handleConnect(c)}
                  >
                    Connect
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
