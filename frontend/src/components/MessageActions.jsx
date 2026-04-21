import { useEffect, useRef, useState } from 'react'
import { copyText, downloadCSV } from '../utils/export'
import { api } from '../services/api'
import GmailComposeModal from './GmailComposeModal'
import ScheduleModal from './ScheduleModal'
import { useToast } from '../hooks/useToast'

const Icon = ({ d, size = 14, color = 'currentColor' }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={color} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d={d}/></svg>
)

export default function MessageActions({ message }) {
  const toast = useToast()
  const [copied, setCopied] = useState(false)
  const [fb, setFb] = useState(null)
  const [showEmail, setShowEmail] = useState(false)
  const [showSchedule, setShowSchedule] = useState(false)
  const [showDownloadMenu, setShowDownloadMenu] = useState(false)
  const [exportingPdf, setExportingPdf] = useState(false)
  const downloadRef = useRef(null)
  const canSchedule = Boolean(message.soql || message.data?.query)

  useEffect(() => {
    if (!showDownloadMenu) return
    const onDocClick = (e) => {
      if (!downloadRef.current?.contains(e.target)) setShowDownloadMenu(false)
    }
    document.addEventListener('mousedown', onDocClick)
    return () => document.removeEventListener('mousedown', onDocClick)
  }, [showDownloadMenu])

  const onCopy = async () => {
    if (await copyText(message.content)) {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
      toast.success('Copied to clipboard')
    } else {
      toast.error('Copy failed')
    }
  }

  const downloadQuery = message.data?.query || message.soql

  const handleDownload = (format) => {
    setShowDownloadMenu(false)
    const recs = message.data?.records
    if (format === 'csv' && recs?.length) {
      downloadCSV(recs)
      toast.success(`Downloaded ${recs.length} rows as CSV`)
      return
    }
    if (downloadQuery) {
      window.open(api.exportUrl(downloadQuery, format), '_blank')
      toast.info(`Preparing ${format.toUpperCase()} download…`)
    } else {
      toast.error('Nothing to download')
    }
  }

  const onExportPdf = async () => {
    if (exportingPdf) return
    setExportingPdf(true)
    try {
      const blob = await api.exportPdf({
        title: (message.question || 'Fyxo Report').slice(0, 80),
        question: message.question || '',
        answer: message.content || '',
        soql: message.soql || message.data?.query || null,
        records: message.data?.records || [],
      })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `fyxo_report_${Date.now()}.pdf`
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
      toast.success('PDF downloaded')
    } catch (e) {
      toast.error(`PDF export failed: ${e.message}`)
    } finally {
      setExportingPdf(false)
    }
  }

  const onFeedback = async (type) => {
    setFb(type)
    try {
      await api.feedback(message.question || '', type)
      toast.success('Feedback saved')
    } catch (e) {
      toast.error(`Feedback failed: ${e.message}`)
    }
  }

  const hasData = message.data?.records?.length > 0 || message.data?.query

  return (
    <div className="message-actions">
      <button className={`action-btn ${copied ? 'active-good' : ''}`} onClick={onCopy} title="Copy">
        {copied
          ? <Icon d="M20 6L9 17l-5-5" color="#4ae87a" />
          : <Icon d="M8 4H6a2 2 0 00-2 2v12a2 2 0 002 2h12a2 2 0 002-2v-2 M16 4h2a2 2 0 012 2v2 M8 4h8v4H8z" />
        }
      </button>
      {hasData && (
        <div className="download-wrapper" ref={downloadRef}>
          <button className="action-btn" onClick={() => setShowDownloadMenu(v => !v)} title="Download">
            <Icon d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4 M7 10l5 5 5-5 M12 15V3" />
          </button>
          {showDownloadMenu && (
            <div className="download-menu">
              <button type="button" onClick={() => handleDownload('csv')}>
                <span className="download-menu-title">CSV</span>
                <span className="download-menu-sub">Comma-separated</span>
              </button>
              <button type="button" onClick={() => handleDownload('xlsx')} disabled={!downloadQuery}>
                <span className="download-menu-title">Excel (.xlsx)</span>
                <span className="download-menu-sub">{downloadQuery ? 'Formatted workbook' : 'Requires a SQL query'}</span>
              </button>
            </div>
          )}
        </div>
      )}
      <button className="action-btn" onClick={onExportPdf} disabled={exportingPdf} title="Export as PDF">
        {exportingPdf
          ? <Icon d="M12 2v4 M12 18v4 M4.93 4.93l2.83 2.83 M16.24 16.24l2.83 2.83 M2 12h4 M18 12h4 M4.93 19.07l2.83-2.83 M16.24 7.76l2.83-2.83" />
          : <Icon d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z M14 2v6h6 M9 13h6 M9 17h6 M9 9h1" />
        }
      </button>
      <button className="action-btn" onClick={() => setShowEmail(true)} title="Send as email">
        <Icon d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z M22 6l-10 7L2 6" />
      </button>
      {canSchedule && (
        <button className="action-btn" onClick={() => setShowSchedule(true)} title="Schedule this report">
          <Icon d="M12 6v6l4 2 M12 22a10 10 0 100-20 10 10 0 000 20z" />
        </button>
      )}
      <div className="action-divider" />
      {showEmail && <GmailComposeModal message={message} onClose={() => setShowEmail(false)} />}
      {showSchedule && <ScheduleModal message={message} onClose={() => setShowSchedule(false)} />}
      <button className={`action-btn ${fb === 'good' ? 'active-good' : ''}`} onClick={() => onFeedback('good')} title="Good answer">
        <Icon d="M14 9V5a3 3 0 00-6 0v0a3 3 0 003 3h0 M2 20h1.5a2 2 0 002-2v-5a2 2 0 00-2-2H2 M8.5 13h6.1a2 2 0 011.94 1.52l.75 3A2 2 0 0115.35 20H8.5z" />
      </button>
      <button className={`action-btn ${fb === 'bad' ? 'active-bad' : ''}`} onClick={() => onFeedback('bad')} title="Bad answer">
        <Icon d="M10 15V19a3 3 0 006 0v0a3 3 0 00-3-3h0 M22 4h-1.5a2 2 0 00-2 2v5a2 2 0 002 2H22 M15.5 11H9.4a2 2 0 01-1.94-1.52l-.75-3A2 2 0 018.65 4h6.85z" />
      </button>
    </div>
  )
}
