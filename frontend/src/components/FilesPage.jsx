import { useEffect, useRef, useState } from 'react'
import { api } from '../services/api'
import { useToast } from '../hooks/useToast'

const UploadIcon = (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4 M17 8l-5-5-5 5 M12 3v12"/>
  </svg>
)

function fmtSize(n) {
  if (!n) return '0 B'
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  return `${(n / (1024 * 1024)).toFixed(1)} MB`
}
function fmtDate(iso) {
  if (!iso) return '—'
  try { return new Date(iso).toLocaleString() } catch { return iso }
}

export default function FilesPage({ onUseInChat }) {
  const toast = useToast()
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(true)
  const [uploading, setUploading] = useState(false)
  const [expanded, setExpanded] = useState(null)
  const [details, setDetails] = useState({})
  const inputRef = useRef(null)

  const load = async () => {
    setLoading(true)
    try { setItems(await api.listUploads()) }
    catch (e) { toast.error(e.message) }
    finally { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  const handleFiles = async (files) => {
    if (!files?.length) return
    setUploading(true)
    for (const f of files) {
      try {
        await api.uploadFile(f)
        toast.success(`Uploaded ${f.name}`)
      } catch (e) {
        toast.error(`${f.name}: ${e.message}`)
      }
    }
    setUploading(false)
    load()
  }

  const handleDelete = async (id, name) => {
    if (!confirm(`Delete "${name}"?`)) return
    try {
      await api.deleteUpload(id)
      toast.success('File deleted')
      load()
    } catch (e) { toast.error(e.message) }
  }

  const toggleExpand = async (id) => {
    if (expanded === id) { setExpanded(null); return }
    setExpanded(id)
    if (!details[id]) {
      try {
        const meta = await api.getUpload(id)
        setDetails(prev => ({ ...prev, [id]: meta }))
      } catch (e) { toast.error(e.message) }
    }
  }

  return (
    <div className="files-page">
      <div className="files-header">
        <div>
          <h2 className="files-title">Uploaded Files</h2>
          <div className="files-subtitle">
            {items.length} file{items.length === 1 ? '' : 's'} · CSV, XLSX up to 15 MB
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <input
            ref={inputRef}
            type="file"
            accept=".csv,.xlsx,.xls"
            multiple
            style={{ display: 'none' }}
            onChange={e => { handleFiles(e.target.files); e.target.value = '' }}
          />
          <button className="btn-primary" disabled={uploading} onClick={() => inputRef.current?.click()}>
            {UploadIcon} {uploading ? 'Uploading…' : 'Upload File'}
          </button>
        </div>
      </div>

      {loading ? (
        <div className="files-empty">Loading…</div>
      ) : items.length === 0 ? (
        <div className="files-empty">
          No files yet. Upload a CSV or Excel file, then attach it to a chat question.
        </div>
      ) : (
        <div className="files-list">
          {items.map(f => {
            const isExpanded = expanded === f.id
            const d = details[f.id]
            return (
              <div key={f.id} className="file-card">
                <div className="file-card-head" onClick={() => toggleExpand(f.id)}>
                  <div className="file-card-info">
                    <div className="file-card-name">{f.filename}</div>
                    <div className="file-card-meta">
                      {f.row_count.toLocaleString()} rows · {f.headers.length} cols · {fmtSize(f.size_bytes)} · {fmtDate(f.uploaded_at)}
                    </div>
                  </div>
                  <div className="file-card-actions">
                    <button className="btn-small" onClick={e => { e.stopPropagation(); onUseInChat?.(f) }}>
                      Use in chat
                    </button>
                    <button className="btn-small btn-danger" onClick={e => { e.stopPropagation(); handleDelete(f.id, f.filename) }}>
                      Delete
                    </button>
                  </div>
                </div>
                {isExpanded && d && (
                  <div className="file-card-body">
                    <div className="file-detail-label">Preview (first {d.preview?.length || 0} rows)</div>
                    <div className="file-preview-scroll">
                      <table className="file-preview-table">
                        <thead>
                          <tr>{d.headers.map(h => <th key={h}>{h}</th>)}</tr>
                        </thead>
                        <tbody>
                          {(d.preview || []).map((row, i) => (
                            <tr key={i}>
                              {d.headers.map(h => <td key={h}>{String(row[h] ?? '')}</td>)}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
