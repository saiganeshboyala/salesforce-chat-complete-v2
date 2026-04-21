import { useMemo, useState } from 'react'

const XIcon = (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
  </svg>
)

const MAX_BODY_LEN = 1800       // keep mailto: URL under most client limits
const MAX_PREVIEW_ROWS = 20

function formatRecordsAsText(records, maxRows = MAX_PREVIEW_ROWS) {
  if (!records?.length) return ''
  const cols = Object.keys(records[0])
  const lines = []
  lines.push(cols.join(' | '))
  lines.push(cols.map(() => '---').join(' | '))
  for (const r of records.slice(0, maxRows)) {
    lines.push(cols.map(c => String(r[c] ?? '')).join(' | '))
  }
  if (records.length > maxRows) {
    lines.push(`…and ${records.length - maxRows} more rows`)
  }
  return lines.join('\n')
}

function stripHtml(text = '') {
  return text.replace(/<[^>]+>/g, '')
}

export default function EmailModal({ message, onClose }) {
  const defaultSubject = useMemo(() => {
    const q = (message.question || 'Data answer').trim().replace(/\s+/g, ' ')
    return q.length > 70 ? q.slice(0, 67) + '…' : q
  }, [message.question])

  const defaultBody = useMemo(() => {
    const parts = []
    if (message.question) parts.push(`Question:\n${message.question}\n`)
    parts.push(`Answer:\n${stripHtml(message.content || '')}`)
    if (message.data?.records?.length) {
      parts.push('\nData preview:')
      parts.push(formatRecordsAsText(message.data.records))
    }
    if (message.soql) parts.push(`\nSQL:\n${message.soql}`)
    let body = parts.join('\n')
    if (body.length > MAX_BODY_LEN) body = body.slice(0, MAX_BODY_LEN) + '\n…(truncated)'
    return body
  }, [message])

  const [to, setTo] = useState('')
  const [subject, setSubject] = useState(defaultSubject)
  const [body, setBody] = useState(defaultBody)
  const [copied, setCopied] = useState(false)

  const buildMailto = () => {
    const params = new URLSearchParams()
    if (subject) params.set('subject', subject)
    if (body) params.set('body', body)
    const qs = params.toString().replace(/\+/g, '%20')
    return `mailto:${encodeURIComponent(to)}?${qs}`
  }

  const handleSend = () => {
    window.location.href = buildMailto()
    // Give the mail client a moment, then close
    setTimeout(onClose, 400)
  }

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(`To: ${to}\nSubject: ${subject}\n\n${body}`)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {}
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()} style={{ maxWidth: 640 }}>
        <div className="modal-header">
          <h3 className="modal-title">Send as Email</h3>
          <button type="button" className="action-btn" onClick={onClose}>{XIcon}</button>
        </div>

        <form className="modal-form" onSubmit={e => { e.preventDefault(); handleSend() }}>
          <label>To
            <input
              className="input-field"
              type="email"
              value={to}
              onChange={e => setTo(e.target.value)}
              placeholder="recipient@example.com"
              autoFocus
            />
          </label>
          <label>Subject
            <input
              className="input-field"
              value={subject}
              onChange={e => setSubject(e.target.value)}
            />
          </label>
          <label>Body
            <textarea
              className="input-field"
              rows={12}
              value={body}
              onChange={e => setBody(e.target.value)}
              style={{ fontFamily: 'var(--font-mono)', fontSize: 12, resize: 'vertical' }}
            />
            <span className="modal-hint">
              Opens in your default mail client. {body.length}/{MAX_BODY_LEN} chars
            </span>
          </label>

          <div className="modal-actions">
            <button type="button" className="btn-secondary" onClick={handleCopy}>
              {copied ? 'Copied!' : 'Copy to clipboard'}
            </button>
            <button type="submit" className="btn-primary" disabled={!to}>
              Open in Mail Client
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
