import { useEffect, useState, useCallback } from 'react'

const CATEGORIES = { daily: 'Daily Reports', weekly: 'Weekly Reports' }

function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

function timeAgo(dateStr) {
  if (!dateStr) return 'Never'
  const diff = Date.now() - new Date(dateStr).getTime()
  const mins = Math.floor(diff / 60000)
  if (mins < 1) return 'Just now'
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ${mins % 60}m ago`
  const days = Math.floor(hrs / 24)
  return `${days}d ago`
}

export default function WhatsAppReports() {
  const [reports, setReports] = useState([])
  const [loading, setLoading] = useState(true)
  const [downloading, setDownloading] = useState({})
  const [downloadingAll, setDownloadingAll] = useState(false)
  const [error, setError] = useState(null)
  const [success, setSuccess] = useState(null)
  const [syncInfo, setSyncInfo] = useState(null)
  const [completed, setCompleted] = useState({})
  const [selectedDate, setSelectedDate] = useState('')
  const [dateMode, setDateMode] = useState('today')

  useEffect(() => {
    const token = localStorage.getItem('token')
    const headers = { Authorization: `Bearer ${token}` }

    fetch('/api/wa-reports', { headers })
      .then(r => r.json())
      .then(d => { setReports(d.reports || []); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })

    fetch('/api/sync/status', { headers })
      .then(r => r.json())
      .then(d => setSyncInfo(d))
      .catch(() => {})
  }, [])

  const generateReport = useCallback(async (reportId, label, silent = false) => {
    setDownloading(prev => ({ ...prev, [reportId]: true }))
    if (!silent) { setError(null); setSuccess(null) }
    try {
      const token = localStorage.getItem('token')
      const params = selectedDate ? `?report_date=${selectedDate}` : ''
      const res = await fetch(`/api/wa-reports/${reportId}${params}`, {
        headers: { Authorization: `Bearer ${token}` },
      })
      if (!res.ok) {
        const j = await res.json().catch(() => ({}))
        throw new Error(j.detail || `Error ${res.status}`)
      }
      const blob = await res.blob()
      const cd = res.headers.get('content-disposition') || ''
      const match = cd.match(/filename=(.+)/)
      const filename = match ? match[1] : `${reportId}.xlsx`
      downloadBlob(blob, filename)
      setCompleted(prev => ({ ...prev, [reportId]: true }))
      setTimeout(() => setCompleted(prev => ({ ...prev, [reportId]: false })), 3000)
      if (!silent) {
        setSuccess(`${label} generated & downloaded!`)
        setTimeout(() => setSuccess(null), 3000)
      }
      return true
    } catch (e) {
      if (!silent) setError(e.message)
      return false
    } finally {
      setDownloading(prev => ({ ...prev, [reportId]: false }))
    }
  }, [selectedDate])

  const generateAll = useCallback(async (category) => {
    const items = reports.filter(r => (r.category || 'other') === category)
    if (!items.length) return
    setDownloadingAll(true)
    setError(null)
    setSuccess(null)
    let ok = 0
    let fail = 0
    for (const r of items) {
      const result = await generateReport(r.id, r.label, true)
      if (result) ok++; else fail++
    }
    setDownloadingAll(false)
    if (fail === 0) {
      setSuccess(`All ${ok} ${category} reports generated & downloaded!`)
    } else {
      setError(`${ok} downloaded, ${fail} failed`)
    }
    setTimeout(() => { setSuccess(null); setError(null) }, 4000)
  }, [reports, generateReport])

  const grouped = {}
  for (const r of reports) {
    const cat = r.category || 'other'
    if (!grouped[cat]) grouped[cat] = []
    grouped[cat].push(r)
  }

  const anyDownloading = Object.values(downloading).some(Boolean) || downloadingAll

  return (
    <div className="wa-page">
      {/* Top bar */}
      <div className="wa-topbar">
        <div className="wa-topbar-left">
          <h2 className="wa-topbar-title">WhatsApp Reports</h2>
          <span className="wa-topbar-sub">
            Generate BU-wise / Manager-wise WhatsApp-ready Excel reports
            {selectedDate && (
              <span className="wa-date-badge">
                {dateMode === 'month'
                  ? new Date(selectedDate).toLocaleDateString('en-US', { year: 'numeric', month: 'long' })
                  : new Date(selectedDate + 'T00:00').toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' })}
              </span>
            )}
          </span>
        </div>
        <div className="wa-topbar-right">
          <div className="wa-date-picker">
            <div className="wa-date-tabs">
              <button
                className={`wa-date-tab ${dateMode === 'today' ? 'active' : ''}`}
                onClick={() => { setDateMode('today'); setSelectedDate('') }}
              >Today</button>
              <button
                className={`wa-date-tab ${dateMode === 'date' ? 'active' : ''}`}
                onClick={() => setDateMode('date')}
              >Date</button>
              <button
                className={`wa-date-tab ${dateMode === 'month' ? 'active' : ''}`}
                onClick={() => setDateMode('month')}
              >Month</button>
            </div>
            {dateMode === 'date' && (
              <input
                type="date"
                className="wa-date-input"
                value={selectedDate}
                onChange={e => setSelectedDate(e.target.value)}
              />
            )}
            {dateMode === 'month' && (
              <input
                type="month"
                className="wa-date-input"
                value={selectedDate ? selectedDate.slice(0, 7) : ''}
                onChange={e => setSelectedDate(e.target.value ? `${e.target.value}-01` : '')}
              />
            )}
          </div>
          <div className="wa-sync-pill">
            <span className={`wa-sync-dot ${syncInfo?.running ? 'syncing' : ''}`} />
            {syncInfo?.running
              ? 'Syncing...'
              : syncInfo?.last_sync
                ? `Synced ${timeAgo(syncInfo.last_sync)}`
                : 'Not synced yet'}
          </div>
        </div>
      </div>

      {/* Alerts */}
      {error && (
        <div className="wa-toast wa-toast-error">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>
          <span>{error}</span>
          <button className="wa-toast-x" onClick={() => setError(null)}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
          </button>
        </div>
      )}
      {success && (
        <div className="wa-toast wa-toast-success">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>
          <span>{success}</span>
        </div>
      )}

      {/* Body */}
      {loading ? (
        <div className="wa-empty">
          <div className="wa-spinner-lg" />
          <span>Loading reports...</span>
        </div>
      ) : (
        <div className="wa-body">
          {Object.entries(CATEGORIES).map(([catKey, catLabel]) => {
            const items = grouped[catKey] || []
            if (!items.length) return null
            const catDownloading = items.some(r => downloading[r.id])
            return (
              <section key={catKey} className="wa-section">
                <div className="wa-section-head">
                  <div className="wa-section-left">
                    <div className="wa-section-icon">
                      {catKey === 'daily' ? (
                        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>
                      ) : (
                        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 3v18h18"/><path d="M7 16l4-8 4 4 6-8"/></svg>
                      )}
                    </div>
                    <h3 className="wa-section-title">{catLabel}</h3>
                    <span className="wa-section-count">{items.length}</span>
                  </div>
                  <button
                    className="wa-gen-all-btn"
                    disabled={anyDownloading}
                    onClick={() => generateAll(catKey)}
                  >
                    {catDownloading ? (
                      <><span className="wa-btn-spin" /> Generating...</>
                    ) : (
                      <>
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                        Generate All
                      </>
                    )}
                  </button>
                </div>

                <div className="wa-cards">
                  {items.map(r => {
                    const busy = downloading[r.id]
                    const done = completed[r.id]
                    const groupType = r.id.includes('offshore')
                      ? 'Offshore Manager'
                      : r.id.includes('bu')
                        ? 'Business Unit'
                        : r.id.includes('recruiter')
                          ? 'Recruiter'
                          : ''
                    return (
                      <div key={r.id} className={`wa-card ${busy ? 'wa-card-busy' : ''} ${done ? 'wa-card-done' : ''}`}>
                        <div className="wa-card-left">
                          <div className="wa-card-title">{r.label}</div>
                          {groupType && (
                            <span className="wa-card-tag">
                              <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/></svg>
                              {groupType}
                            </span>
                          )}
                        </div>
                        <button
                          className={`wa-card-btn ${busy ? 'busy' : ''} ${done ? 'done' : ''}`}
                          disabled={busy || anyDownloading}
                          onClick={() => generateReport(r.id, r.label)}
                        >
                          {busy ? (
                            <><span className="wa-btn-spin" /> Generating...</>
                          ) : done ? (
                            <>
                              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polyline points="20 6 9 17 4 12"/></svg>
                              Done
                            </>
                          ) : (
                            <>
                              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                              Generate
                            </>
                          )}
                        </button>
                      </div>
                    )
                  })}
                </div>
              </section>
            )
          })}
        </div>
      )}

      <style>{`
        .wa-page {
          flex: 1;
          display: flex;
          flex-direction: column;
          overflow-y: auto;
        }

        /* ─── Top bar ─── */
        .wa-topbar {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 16px;
          padding: 18px 28px;
          border-bottom: 1px solid var(--border);
          background: var(--bg-secondary);
          flex-shrink: 0;
          flex-wrap: wrap;
        }
        .wa-topbar-left {
          display: flex;
          flex-direction: column;
          gap: 2px;
        }
        .wa-topbar-title {
          margin: 0;
          font-size: 1.1rem;
          font-weight: 700;
          color: var(--text-primary);
        }
        .wa-topbar-sub {
          font-size: 0.78rem;
          color: var(--text-muted);
          display: flex;
          align-items: center;
          gap: 8px;
          flex-wrap: wrap;
        }
        .wa-date-badge {
          display: inline-flex;
          align-items: center;
          padding: 1px 8px;
          border-radius: 6px;
          background: var(--accent-muted);
          color: var(--accent);
          font-size: 0.72rem;
          font-weight: 600;
        }
        .wa-topbar-right {
          display: flex;
          align-items: center;
          gap: 10px;
        }

        /* ─── Date picker ─── */
        .wa-date-picker {
          display: flex;
          align-items: center;
          gap: 8px;
        }
        .wa-date-tabs {
          display: flex;
          border-radius: 8px;
          overflow: hidden;
          border: 1px solid var(--border);
          background: var(--bg-surface);
        }
        .wa-date-tab {
          padding: 5px 12px;
          border: none;
          background: none;
          font-size: 0.75rem;
          font-weight: 600;
          font-family: var(--font-sans);
          color: var(--text-muted);
          cursor: pointer;
          transition: all 0.15s;
        }
        .wa-date-tab:not(:last-child) {
          border-right: 1px solid var(--border);
        }
        .wa-date-tab.active {
          background: var(--accent);
          color: #fff;
        }
        .wa-date-tab:hover:not(.active) {
          background: var(--bg-elevated);
          color: var(--text-primary);
        }
        .wa-date-input {
          padding: 5px 10px;
          border-radius: 7px;
          border: 1px solid var(--border);
          background: var(--bg-surface);
          color: var(--text-primary);
          font-size: 0.78rem;
          font-family: var(--font-sans);
          outline: none;
          transition: border-color 0.15s;
        }
        .wa-date-input:focus {
          border-color: var(--accent);
        }

        /* ─── Sync pill ─── */
        .wa-sync-pill {
          display: flex;
          align-items: center;
          gap: 7px;
          padding: 5px 12px;
          border-radius: 16px;
          background: var(--bg-surface);
          border: 1px solid var(--border);
          font-size: 0.75rem;
          font-weight: 500;
          color: var(--text-secondary);
        }
        .wa-sync-dot {
          width: 7px;
          height: 7px;
          border-radius: 50%;
          background: var(--success);
        }
        .wa-sync-dot.syncing {
          animation: wa-pulse 1.2s ease-in-out infinite;
          background: var(--info);
        }

        /* ─── Toasts ─── */
        .wa-toast {
          display: flex;
          align-items: center;
          gap: 8px;
          margin: 16px 28px 0;
          padding: 10px 14px;
          border-radius: var(--radius-md);
          font-size: 0.82rem;
          font-weight: 500;
          animation: wa-slideDown 0.2s ease;
        }
        .wa-toast-error {
          background: rgba(232,74,90,0.08);
          border: 1px solid rgba(232,74,90,0.2);
          color: var(--danger);
        }
        .wa-toast-success {
          background: rgba(74,232,122,0.08);
          border: 1px solid rgba(74,232,122,0.2);
          color: var(--success);
        }
        .wa-toast-x {
          margin-left: auto;
          background: none;
          border: none;
          color: inherit;
          cursor: pointer;
          opacity: 0.6;
          padding: 2px;
        }
        .wa-toast-x:hover { opacity: 1; }

        /* ─── Empty / loading ─── */
        .wa-empty {
          flex: 1;
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          gap: 14px;
          color: var(--text-muted);
          font-size: 0.88rem;
        }
        .wa-spinner-lg {
          width: 28px;
          height: 28px;
          border: 3px solid var(--border);
          border-top-color: var(--accent);
          border-radius: 50%;
          animation: wa-spin 0.7s linear infinite;
        }

        /* ─── Body ─── */
        .wa-body {
          flex: 1;
          padding: 24px 28px 32px;
          display: flex;
          flex-direction: column;
          gap: 32px;
          max-width: 1000px;
          width: 100%;
          margin: 0 auto;
        }

        /* ─── Section ─── */
        .wa-section-head {
          display: flex;
          align-items: center;
          justify-content: space-between;
          margin-bottom: 12px;
        }
        .wa-section-left {
          display: flex;
          align-items: center;
          gap: 8px;
        }
        .wa-section-icon {
          width: 28px;
          height: 28px;
          border-radius: 7px;
          background: var(--accent-muted);
          color: var(--accent);
          display: flex;
          align-items: center;
          justify-content: center;
        }
        .wa-section-title {
          margin: 0;
          font-size: 0.92rem;
          font-weight: 600;
          color: var(--text-primary);
        }
        .wa-section-count {
          font-size: 0.68rem;
          font-weight: 600;
          padding: 1px 7px;
          border-radius: 8px;
          background: var(--accent-muted);
          color: var(--accent);
        }

        /* ─── Generate All ─── */
        .wa-gen-all-btn {
          display: flex;
          align-items: center;
          gap: 6px;
          padding: 6px 14px;
          border-radius: 7px;
          border: 1px solid var(--border);
          background: var(--bg-surface);
          color: var(--text-secondary);
          font-size: 0.78rem;
          font-weight: 600;
          font-family: var(--font-sans);
          cursor: pointer;
          transition: all 0.15s;
        }
        .wa-gen-all-btn:hover:not(:disabled) {
          border-color: var(--accent);
          color: var(--accent);
          background: var(--accent-muted);
        }
        .wa-gen-all-btn:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        /* ─── Cards ─── */
        .wa-cards {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
          gap: 10px;
        }
        .wa-card {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 12px;
          padding: 14px 16px;
          border-radius: var(--radius-md);
          background: var(--bg-surface);
          border: 1px solid var(--border);
          transition: border-color 0.15s, box-shadow 0.15s;
        }
        .wa-card:hover {
          border-color: var(--accent);
        }
        .wa-card-busy {
          border-color: var(--info);
          background: rgba(74,158,232,0.04);
        }
        .wa-card-done {
          border-color: var(--success);
          background: rgba(74,232,122,0.04);
        }
        .wa-card-left {
          display: flex;
          flex-direction: column;
          gap: 4px;
          min-width: 0;
        }
        .wa-card-title {
          font-size: 0.85rem;
          font-weight: 500;
          color: var(--text-primary);
          line-height: 1.3;
        }
        .wa-card-tag {
          display: inline-flex;
          align-items: center;
          gap: 4px;
          font-size: 0.68rem;
          color: var(--text-muted);
          background: var(--bg-elevated);
          padding: 1px 7px;
          border-radius: 5px;
          width: fit-content;
        }

        /* ─── Card button ─── */
        .wa-card-btn {
          display: flex;
          align-items: center;
          gap: 6px;
          padding: 7px 14px;
          border-radius: 7px;
          border: none;
          background: var(--accent);
          color: #fff;
          font-size: 0.78rem;
          font-weight: 600;
          font-family: var(--font-sans);
          cursor: pointer;
          white-space: nowrap;
          flex-shrink: 0;
          transition: background 0.15s, transform 0.1s;
        }
        .wa-card-btn:hover:not(:disabled) {
          background: var(--accent-hover);
          transform: translateY(-1px);
        }
        .wa-card-btn:active:not(:disabled) {
          transform: translateY(0);
        }
        .wa-card-btn:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }
        .wa-card-btn.busy {
          background: var(--bg-elevated);
          color: var(--text-secondary);
          cursor: wait;
          opacity: 1;
        }
        .wa-card-btn.done {
          background: var(--success);
          color: #fff;
          opacity: 1;
        }

        /* ─── Spinners ─── */
        .wa-btn-spin {
          display: inline-block;
          width: 12px;
          height: 12px;
          border: 2px solid rgba(255,255,255,0.2);
          border-top-color: currentColor;
          border-radius: 50%;
          animation: wa-spin 0.6s linear infinite;
        }

        /* ─── Animations ─── */
        @keyframes wa-spin { to { transform: rotate(360deg); } }
        @keyframes wa-pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }
        @keyframes wa-slideDown { from { opacity: 0; transform: translateY(-6px); } to { opacity: 1; transform: translateY(0); } }

        /* ─── Responsive ─── */
        @media (max-width: 640px) {
          .wa-topbar { padding: 14px 16px; }
          .wa-body { padding: 16px; }
          .wa-cards { grid-template-columns: 1fr; }
          .wa-card { flex-direction: column; align-items: stretch; }
          .wa-card-btn { justify-content: center; }
          .wa-section-head { flex-direction: column; align-items: flex-start; gap: 10px; }
        }
      `}</style>
    </div>
  )
}
