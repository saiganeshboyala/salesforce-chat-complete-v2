import { useEffect, useState } from 'react'

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
  const [downloading, setDownloading] = useState(null)
  const [error, setError] = useState(null)
  const [success, setSuccess] = useState(null)
  const [syncInfo, setSyncInfo] = useState(null)

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

  const handleDownload = async (reportId, label) => {
    setDownloading(reportId)
    setError(null)
    setSuccess(null)
    try {
      const token = localStorage.getItem('token')
      const res = await fetch(`/api/wa-reports/${reportId}`, {
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
      setSuccess(`${label} downloaded successfully!`)
      setTimeout(() => setSuccess(null), 3000)
    } catch (e) {
      setError(e.message)
    } finally {
      setDownloading(null)
    }
  }

  const grouped = {}
  for (const r of reports) {
    const cat = r.category || 'other'
    if (!grouped[cat]) grouped[cat] = []
    grouped[cat].push(r)
  }

  return (
    <div className="wa-reports-page">
      {/* Header */}
      <div className="wa-reports-header">
        <div className="wa-reports-header-left">
          <div className="wa-reports-icon">
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
              <path d="M21 11.5a8.38 8.38 0 01-.9 3.8 8.5 8.5 0 01-7.6 4.7 8.38 8.38 0 01-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 01-.9-3.8 8.5 8.5 0 014.7-7.6 8.38 8.38 0 013.8-.9h.5a8.48 8.48 0 018 8v.5z"/>
            </svg>
          </div>
          <div>
            <h2 className="wa-reports-title">WhatsApp Reports</h2>
            <p className="wa-reports-subtitle">
              Generate BU-wise / Manager-wise WhatsApp-ready Excel files
            </p>
          </div>
        </div>
        <div className="wa-sync-badge">
          <span className={`wa-sync-dot ${syncInfo?.running ? 'syncing' : ''}`} />
          <span className="wa-sync-text">
            {syncInfo?.running
              ? 'Syncing...'
              : syncInfo?.last_sync
                ? `Synced ${timeAgo(syncInfo.last_sync)}`
                : 'Not synced yet'}
          </span>
        </div>
      </div>

      {/* Alerts */}
      {error && (
        <div className="wa-alert wa-alert-error">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>
          {error}
          <button className="wa-alert-close" onClick={() => setError(null)}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
          </button>
        </div>
      )}

      {success && (
        <div className="wa-alert wa-alert-success">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>
          {success}
        </div>
      )}

      {/* Content */}
      {loading ? (
        <div className="wa-loading">
          <div className="wa-loading-spinner" />
          <span>Loading reports...</span>
        </div>
      ) : (
        <div className="wa-reports-grid">
          {Object.entries(CATEGORIES).map(([catKey, catLabel]) => {
            const items = grouped[catKey] || []
            if (!items.length) return null
            return (
              <div key={catKey} className="wa-category">
                <div className="wa-category-header">
                  <div className="wa-category-icon">
                    {catKey === 'daily' ? (
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>
                    ) : (
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 3v18h18"/><path d="M7 16l4-8 4 4 6-8"/></svg>
                    )}
                  </div>
                  <h3 className="wa-category-title">{catLabel}</h3>
                  <span className="wa-category-count">{items.length}</span>
                </div>
                <div className="wa-report-list">
                  {items.map(r => {
                    const isDownloading = downloading === r.id
                    const groupType = r.id.includes('offshore')
                      ? 'Offshore Manager'
                      : r.id.includes('bu')
                        ? 'Business Unit'
                        : r.id.includes('recruiter')
                          ? 'Recruiter'
                          : ''
                    return (
                      <div key={r.id} className={`wa-report-card ${isDownloading ? 'downloading' : ''}`}>
                        <div className="wa-report-info">
                          <div className="wa-report-name">{r.label}</div>
                          {groupType && (
                            <div className="wa-report-tag">
                              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/></svg>
                              {groupType}
                            </div>
                          )}
                        </div>
                        <button
                          className={`wa-download-btn ${isDownloading ? 'loading' : ''}`}
                          onClick={() => handleDownload(r.id, r.label)}
                          disabled={isDownloading}
                        >
                          {isDownloading ? (
                            <>
                              <span className="wa-btn-spinner" />
                              Generating...
                            </>
                          ) : (
                            <>
                              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
                                <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/>
                                <polyline points="7 10 12 15 17 10"/>
                                <line x1="12" y1="15" x2="12" y2="3"/>
                              </svg>
                              Download
                            </>
                          )}
                        </button>
                      </div>
                    )
                  })}
                </div>
              </div>
            )
          })}
        </div>
      )}

      <style>{`
        .wa-reports-page {
          height: 100%;
          overflow-y: auto;
          padding: 28px 32px;
          max-width: 960px;
          margin: 0 auto;
        }

        /* Header */
        .wa-reports-header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          margin-bottom: 24px;
          gap: 16px;
          flex-wrap: wrap;
        }
        .wa-reports-header-left {
          display: flex;
          align-items: center;
          gap: 14px;
        }
        .wa-reports-icon {
          width: 44px;
          height: 44px;
          border-radius: 12px;
          background: var(--accent-muted);
          color: var(--accent);
          display: flex;
          align-items: center;
          justify-content: center;
          flex-shrink: 0;
        }
        .wa-reports-title {
          margin: 0;
          font-size: 1.3rem;
          font-weight: 700;
          color: var(--text-primary);
          line-height: 1.2;
        }
        .wa-reports-subtitle {
          margin: 3px 0 0;
          font-size: 0.82rem;
          color: var(--text-secondary);
          line-height: 1.3;
        }

        /* Sync badge */
        .wa-sync-badge {
          display: flex;
          align-items: center;
          gap: 8px;
          padding: 7px 14px;
          border-radius: 20px;
          background: var(--bg-surface);
          border: 1px solid var(--border);
          font-size: 0.78rem;
          color: var(--text-secondary);
          flex-shrink: 0;
        }
        .wa-sync-dot {
          width: 8px;
          height: 8px;
          border-radius: 50%;
          background: var(--success);
          flex-shrink: 0;
        }
        .wa-sync-dot.syncing {
          animation: wa-pulse 1.2s ease-in-out infinite;
          background: var(--info);
        }

        /* Alerts */
        .wa-alert {
          display: flex;
          align-items: center;
          gap: 10px;
          padding: 10px 14px;
          border-radius: var(--radius-md);
          font-size: 0.85rem;
          margin-bottom: 16px;
          animation: wa-slideDown 0.25s ease;
        }
        .wa-alert-error {
          background: rgba(232, 74, 90, 0.1);
          border: 1px solid rgba(232, 74, 90, 0.25);
          color: var(--danger);
        }
        .wa-alert-success {
          background: rgba(74, 232, 122, 0.1);
          border: 1px solid rgba(74, 232, 122, 0.25);
          color: var(--success);
        }
        .wa-alert-close {
          margin-left: auto;
          background: none;
          border: none;
          color: inherit;
          cursor: pointer;
          padding: 2px;
          opacity: 0.7;
          transition: opacity 0.15s;
        }
        .wa-alert-close:hover { opacity: 1; }

        /* Loading */
        .wa-loading {
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          gap: 14px;
          padding: 60px 0;
          color: var(--text-secondary);
          font-size: 0.9rem;
        }
        .wa-loading-spinner {
          width: 32px;
          height: 32px;
          border: 3px solid var(--border);
          border-top-color: var(--accent);
          border-radius: 50%;
          animation: wa-spin 0.8s linear infinite;
        }

        /* Categories */
        .wa-reports-grid {
          display: flex;
          flex-direction: column;
          gap: 28px;
        }
        .wa-category-header {
          display: flex;
          align-items: center;
          gap: 10px;
          margin-bottom: 14px;
          padding-bottom: 10px;
          border-bottom: 1px solid var(--border);
        }
        .wa-category-icon {
          width: 30px;
          height: 30px;
          border-radius: 8px;
          background: var(--accent-muted);
          color: var(--accent);
          display: flex;
          align-items: center;
          justify-content: center;
          flex-shrink: 0;
        }
        .wa-category-title {
          margin: 0;
          font-size: 0.95rem;
          font-weight: 600;
          color: var(--text-primary);
        }
        .wa-category-count {
          margin-left: auto;
          font-size: 0.72rem;
          font-weight: 600;
          padding: 2px 8px;
          border-radius: 10px;
          background: var(--accent-muted);
          color: var(--accent);
        }

        /* Report cards */
        .wa-report-list {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }
        .wa-report-card {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 16px;
          padding: 14px 16px;
          border-radius: var(--radius-md);
          background: var(--bg-secondary);
          border: 1px solid var(--border);
          transition: border-color 0.2s, box-shadow 0.2s;
        }
        .wa-report-card:hover {
          border-color: var(--accent);
          box-shadow: 0 0 0 1px var(--accent-muted);
        }
        .wa-report-card.downloading {
          opacity: 0.85;
        }
        .wa-report-info {
          display: flex;
          flex-direction: column;
          gap: 6px;
          min-width: 0;
        }
        .wa-report-name {
          font-size: 0.9rem;
          font-weight: 500;
          color: var(--text-primary);
          line-height: 1.3;
        }
        .wa-report-tag {
          display: inline-flex;
          align-items: center;
          gap: 5px;
          font-size: 0.72rem;
          color: var(--text-muted);
          padding: 2px 8px;
          background: var(--bg-surface);
          border-radius: 6px;
          width: fit-content;
        }

        /* Download button */
        .wa-download-btn {
          display: flex;
          align-items: center;
          gap: 7px;
          padding: 8px 16px;
          border-radius: 8px;
          border: none;
          background: var(--accent);
          color: #fff;
          font-size: 0.82rem;
          font-weight: 600;
          font-family: var(--font-sans);
          cursor: pointer;
          white-space: nowrap;
          transition: background 0.2s, transform 0.1s;
          flex-shrink: 0;
        }
        .wa-download-btn:hover:not(:disabled) {
          background: var(--accent-hover);
          transform: translateY(-1px);
        }
        .wa-download-btn:active:not(:disabled) {
          transform: translateY(0);
        }
        .wa-download-btn.loading {
          background: var(--bg-elevated);
          color: var(--text-secondary);
          cursor: wait;
        }

        /* Spinner in button */
        .wa-btn-spinner {
          display: inline-block;
          width: 14px;
          height: 14px;
          border: 2px solid rgba(255,255,255,0.2);
          border-top-color: currentColor;
          border-radius: 50%;
          animation: wa-spin 0.7s linear infinite;
        }

        /* Animations */
        @keyframes wa-spin {
          to { transform: rotate(360deg); }
        }
        @keyframes wa-pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.4; }
        }
        @keyframes wa-slideDown {
          from { opacity: 0; transform: translateY(-8px); }
          to { opacity: 1; transform: translateY(0); }
        }

        /* Responsive */
        @media (max-width: 640px) {
          .wa-reports-page { padding: 20px 16px; }
          .wa-reports-header { flex-direction: column; align-items: flex-start; }
          .wa-report-card { flex-direction: column; align-items: flex-start; gap: 12px; }
          .wa-download-btn { width: 100%; justify-content: center; }
        }
      `}</style>
    </div>
  )
}
