import { useState } from 'react'
import { copyText } from '../utils/export'

function confidenceColor(score) {
  if (score >= 85) return '#4ae87a'
  if (score >= 65) return '#f5a623'
  return '#e84a5f'
}

export default function SOQLBlock({ soql, route, confidence }) {
  const [open, setOpen] = useState(false)
  const [copied, setCopied] = useState(false)

  if (!soql) return null

  const handleCopy = async (e) => {
    e.stopPropagation()
    if (await copyText(soql)) { setCopied(true); setTimeout(() => setCopied(false), 2000) }
  }

  return (
    <div style={{ marginTop: 6 }}>
      <button className="soql-toggle" onClick={() => setOpen(!open)}>
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M16 18l6-6-6-6M8 6l-6 6 6 6"/></svg>
        SQL Query
        {route && <span className="soql-badge">{route}</span>}
        {confidence != null && (
          <span className="soql-badge" style={{ color: confidenceColor(confidence), borderColor: confidenceColor(confidence), border: '1px solid', background: 'transparent' }}>
            {confidence}% confident
          </span>
        )}
        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ transform: open ? 'rotate(180deg)' : 'none', transition: 'transform 150ms' }}>
          <path d="M6 9l6 6 6-6"/>
        </svg>
      </button>
      {open && (
        <div className="soql-block">
          <pre className="soql-code">{soql}</pre>
          <button className="action-btn soql-copy" onClick={handleCopy} title="Copy">
            {copied
              ? <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#4ae87a" strokeWidth="2"><path d="M20 6L9 17l-5-5"/></svg>
              : <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>
            }
          </button>
        </div>
      )}
    </div>
  )
}
