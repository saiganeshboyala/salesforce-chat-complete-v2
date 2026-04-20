import { useEffect, useState } from 'react'
import { api } from '../services/api'
import { useToast } from '../hooks/useToast'

function formatRecordsAsText(records, max = 20) {
  if (!Array.isArray(records) || records.length === 0) return ''
  const headers = Object.keys(records[0]).filter(k => k !== 'attributes')
  const lines = [headers.join(' | ')]
  records.slice(0, max).forEach(r => {
    lines.push(headers.map(h => String(r[h] ?? '')).join(' | '))
  })
  if (records.length > max) lines.push(`… (${records.length - max} more rows)`)
  return lines.join('\n')
}

function stripHtml(s) {
  if (!s) return ''
  return s.replace(/<[^>]+>/g, '').replace(/\n{3,}/g, '\n\n').trim()
}

export default function GmailComposeModal({ message, onClose }) {
  const toast = useToast()
  const [to, setTo] = useState('')
  const [cc, setCc] = useState('')
  const [subject, setSubject] = useState('Fyxo Report')
  const [body, setBody] = useState('')
  const [sending, setSending] = useState(false)
  const [status, setStatus] = useState(null) // {connected, account, configured}

  useEffect(() => {
    // Build the initial body from the AI answer + data preview
    const textAnswer = stripHtml(message?.content || '')
    const recordPreview = formatRecordsAsText(message?.data?.records || [])
    const parts = [textAnswer]
    if (recordPreview) parts.push('\n---\n', recordPreview)
    setBody(parts.join('\n').trim())
  }, [message])

  useEffect(() => {
    // Check Gmail connection status on open
    let cancelled = false
    ;(async () => {
      try {
        const list = await api.listConnectors()
        const gmail = (list || []).find(c => c.id === 'gmail')
        if (!cancelled) setStatus(gmail || { configured: false, connected: false })
      } catch {
        if (!cancelled) setStatus({ configured: false, connected: false })
      }
    })()
    return () => { cancelled = true }
  }, [])

  const handleConnect = () => {
    window.location.href = api.gmailAuthUrl()
  }

  const handleSend = async () => {
    if (!to.trim()) { toast.error('Recipient is required'); return }
    setSending(true)
    try {
      await api.gmailSend({
        to: to.trim(),
        cc: cc.trim() || null,
        subject: subject.trim(),
        body,
      })
      toast.success('Email sent')
      onClose()
    } catch (e) {
      toast.error(e.message)
    } finally {
      setSending(false)
    }
  }

  const notConnected = status && (!status.configured || !status.connected)

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()} style={{ maxWidth: 580 }}>
        <div className="modal-header">
          <h3 className="modal-title">Send as Email</h3>
          <button className="action-btn" onClick={onClose} aria-label="Close">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
            </svg>
          </button>
        </div>

        {status === null ? (
          <div className="modal-empty">Checking Gmail connection…</div>
        ) : notConnected ? (
          <div>
            <div className="modal-hint" style={{ fontSize: 13, color: 'var(--text-secondary)', marginBottom: 16 }}>
              {status.configured
                ? 'Connect your Google account to send emails directly from Gmail.'
                : 'Gmail is not configured on the server. Ask an admin to set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET.'}
            </div>
            <div className="modal-actions">
              <button className="btn-secondary" onClick={onClose}>Cancel</button>
              {status.configured && (
                <button className="btn-primary" onClick={handleConnect}>Connect Gmail</button>
              )}
            </div>
          </div>
        ) : (
          <div className="modal-form">
            {status.account && (
              <div className="modal-hint" style={{ fontSize: 12, color: 'var(--text-muted)' }}>
                Sending as <strong style={{ color: 'var(--text-secondary)' }}>{status.account}</strong>
              </div>
            )}
            <label>
              <span>To</span>
              <input
                className="input-field"
                type="email"
                value={to}
                onChange={e => setTo(e.target.value)}
                placeholder="recipient@example.com"
                autoFocus
              />
            </label>
            <label>
              <span>CC (optional)</span>
              <input
                className="input-field"
                type="text"
                value={cc}
                onChange={e => setCc(e.target.value)}
                placeholder="Comma-separated emails"
              />
            </label>
            <label>
              <span>Subject</span>
              <input
                className="input-field"
                type="text"
                value={subject}
                onChange={e => setSubject(e.target.value)}
              />
            </label>
            <label>
              <span>Body</span>
              <textarea
                className="input-field"
                value={body}
                onChange={e => setBody(e.target.value)}
                rows={10}
                style={{ fontFamily: 'var(--font-mono)', fontSize: 12, lineHeight: 1.5 }}
              />
            </label>
            <div className="modal-actions">
              <button className="btn-secondary" onClick={onClose} disabled={sending}>Cancel</button>
              <button className="btn-primary" onClick={handleSend} disabled={sending || !to.trim()}>
                {sending ? 'Sending…' : 'Send Email'}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
