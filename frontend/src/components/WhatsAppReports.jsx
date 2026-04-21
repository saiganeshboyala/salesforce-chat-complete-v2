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

export default function WhatsAppReports() {
  const [reports, setReports] = useState([])
  const [loading, setLoading] = useState(true)
  const [downloading, setDownloading] = useState(null)
  const [error, setError] = useState(null)
  const [success, setSuccess] = useState(null)

  useEffect(() => {
    const token = localStorage.getItem('token')
    fetch('/api/wa-reports', { headers: { Authorization: `Bearer ${token}` } })
      .then(r => r.json())
      .then(d => { setReports(d.reports || []); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
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
    <div style={{ padding: '2rem', maxWidth: 900, margin: '0 auto', height: '100%', overflowY: 'auto' }}>
      <div style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 700, display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '1.6rem' }}>📱</span> WhatsApp Reports
        </h2>
        <p style={{ margin: '0.5rem 0 0', color: 'var(--text-secondary, #888)', fontSize: '0.9rem' }}>
          Generate BU-wise / Manager-wise WhatsApp-ready messages as Excel files. Each row = one BU or Manager message ready to copy-paste.
        </p>
      </div>

      {error && (
        <div style={{
          padding: '0.75rem 1rem', marginBottom: '1rem', borderRadius: 8,
          background: 'rgba(234,90,117,0.12)', color: '#ea5a75', fontSize: '0.9rem',
        }}>
          {error}
        </div>
      )}

      {success && (
        <div style={{
          padding: '0.75rem 1rem', marginBottom: '1rem', borderRadius: 8,
          background: 'rgba(138,194,74,0.12)', color: '#6a9a3a', fontSize: '0.9rem',
        }}>
          {success}
        </div>
      )}

      {loading ? (
        <div style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-secondary, #888)' }}>
          Loading reports...
        </div>
      ) : (
        Object.entries(CATEGORIES).map(([catKey, catLabel]) => {
          const items = grouped[catKey] || []
          if (!items.length) return null
          return (
            <div key={catKey} style={{ marginBottom: '2rem' }}>
              <h3 style={{
                fontSize: '1.05rem', fontWeight: 600, marginBottom: '0.75rem',
                paddingBottom: '0.5rem', borderBottom: '1px solid var(--border, #333)',
                color: 'var(--text-primary, #eee)',
              }}>
                {catKey === 'daily' ? '📅' : '📊'} {catLabel}
              </h3>
              <div style={{ display: 'grid', gap: '0.6rem' }}>
                {items.map(r => (
                  <div
                    key={r.id}
                    style={{
                      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                      padding: '0.85rem 1rem', borderRadius: 8,
                      background: 'var(--card-bg, #1e1e1e)',
                      border: '1px solid var(--border, #333)',
                      transition: 'border-color 0.2s',
                    }}
                    onMouseEnter={e => e.currentTarget.style.borderColor = 'var(--accent, #4a9ee8)'}
                    onMouseLeave={e => e.currentTarget.style.borderColor = 'var(--border, #333)'}
                  >
                    <div>
                      <div style={{ fontWeight: 500, fontSize: '0.95rem', color: 'var(--text-primary, #eee)' }}>
                        {r.label}
                      </div>
                      <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary, #888)', marginTop: 2 }}>
                        {r.id.includes('offshore') ? 'Grouped by Offshore Manager' : r.id.includes('bu') ? 'Grouped by BU' : ''}
                      </div>
                    </div>
                    <button
                      onClick={() => handleDownload(r.id, r.label)}
                      disabled={downloading === r.id}
                      style={{
                        padding: '0.5rem 1.1rem', borderRadius: 6, border: 'none',
                        background: downloading === r.id ? '#555' : 'var(--accent, #4a9ee8)',
                        color: '#fff', cursor: downloading === r.id ? 'wait' : 'pointer',
                        fontWeight: 500, fontSize: '0.85rem', whiteSpace: 'nowrap',
                        display: 'flex', alignItems: 'center', gap: 6,
                        transition: 'background 0.2s',
                      }}
                    >
                      {downloading === r.id ? (
                        <>
                          <span style={{ display: 'inline-block', width: 14, height: 14, border: '2px solid rgba(255,255,255,0.3)', borderTopColor: '#fff', borderRadius: '50%', animation: 'spin 1s linear infinite' }} />
                          Generating...
                        </>
                      ) : (
                        <>
                          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                            <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M7 10l5 5 5-5M12 15V3" />
                          </svg>
                          Download Excel
                        </>
                      )}
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )
        })
      )}

      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
      `}</style>
    </div>
  )
}
