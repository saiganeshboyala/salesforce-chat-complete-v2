import { useState, useMemo, useEffect } from 'react'
import { api } from '../services/api'

function isAggregate(records) {
  if (!records?.length) return false
  const keys = Object.keys(records[0]).filter(k => k !== 'attributes')
  return keys.some(k => k.startsWith('expr') || /^(cnt|count|sum|avg|min|max)$/i.test(k))
}

function formatCell(v) {
  if (v == null) return '—'
  if (typeof v === 'object') {
    if (v.Name) return v.Name
    return JSON.stringify(v)
  }
  if (typeof v === 'boolean') return v ? 'Yes' : 'No'
  return String(v)
}

function cmp(a, b, dir) {
  if (a == null && b == null) return 0
  if (a == null) return 1
  if (b == null) return -1
  if (typeof a === 'number' && typeof b === 'number') {
    return dir === 'asc' ? a - b : b - a
  }
  const as = String(a), bs = String(b)
  return dir === 'asc' ? as.localeCompare(bs, undefined, { numeric: true }) : bs.localeCompare(as, undefined, { numeric: true })
}

function pickRecordId(row) {
  if (!row) return null
  if (row.Id) return row.Id
  if (row.id) return row.id
  const attr = row.attributes
  if (attr?.url) {
    const m = attr.url.match(/\/([a-zA-Z0-9]{15,18})$/)
    if (m) return m[1]
  }
  return null
}

function pickRecordName(row) {
  return row?.Name || row?.name || null
}

function pickObjectType(row) {
  return row?.attributes?.type || null
}

function NotePopover({ row, existingNotes, onClose, onSaved }) {
  const recordId = pickRecordId(row)
  const recordName = pickRecordName(row)
  const objectType = pickObjectType(row)
  const [text, setText] = useState('')
  const [tags, setTags] = useState('')
  const [saving, setSaving] = useState(false)
  const [err, setErr] = useState('')

  const submit = async (e) => {
    e.preventDefault()
    if (!text.trim()) { setErr('Note text required'); return }
    setSaving(true); setErr('')
    try {
      await api.createAnnotation({
        record_id: recordId,
        record_name: recordName,
        object_type: objectType,
        text: text.trim(),
        tags: tags.split(',').map(t => t.trim()).filter(Boolean),
      })
      setText(''); setTags('')
      onSaved?.()
    } catch (ex) {
      setErr(ex.message)
    } finally {
      setSaving(false)
    }
  }

  const remove = async (id) => {
    if (!confirm('Delete this note?')) return
    try {
      await api.deleteAnnotation(id)
      onSaved?.()
    } catch (ex) {
      setErr(ex.message)
    }
  }

  return (
    <div className="note-popover-backdrop" onClick={onClose}>
      <div className="note-popover" onClick={e => e.stopPropagation()}>
        <div className="note-popover-head">
          <div>
            <div className="note-popover-title">Notes</div>
            <div className="note-popover-record">{recordName || recordId}</div>
          </div>
          <button className="note-popover-close" onClick={onClose}>×</button>
        </div>

        {existingNotes?.length > 0 && (
          <div className="note-popover-existing">
            {existingNotes.map(n => (
              <div key={n.id} className="note-popover-item">
                <div className="note-popover-text">{n.text}</div>
                {(n.tags || []).length > 0 && (
                  <div className="note-popover-tags">
                    {n.tags.map(t => <span key={t} className="note-tag-mini">{t}</span>)}
                  </div>
                )}
                <div className="note-popover-meta">
                  {new Date(n.updated_at).toLocaleString()}
                  <button type="button" className="note-popover-del" onClick={() => remove(n.id)}>Delete</button>
                </div>
              </div>
            ))}
          </div>
        )}

        <form onSubmit={submit} className="note-popover-form">
          <textarea
            value={text}
            onChange={e => setText(e.target.value)}
            rows={3}
            placeholder="Add a note…"
            autoFocus
          />
          <input
            value={tags}
            onChange={e => setTags(e.target.value)}
            placeholder="Tags (comma-separated)"
          />
          {err && <div className="note-error">{err}</div>}
          <div className="note-popover-actions">
            <button type="button" className="btn-secondary" onClick={onClose}>Close</button>
            <button type="submit" className="btn-primary" disabled={saving}>
              {saving ? 'Saving…' : 'Add note'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

const PAGE_SIZE = 25

export default function DataTable({ records, totalSize }) {
  const [sortKey, setSortKey] = useState(null)
  const [sortDir, setSortDir] = useState('asc')
  const [notesMap, setNotesMap] = useState({})
  const [openRow, setOpenRow] = useState(null)
  const [page, setPage] = useState(0)

  useEffect(() => { setPage(0) }, [records])

  const cols = useMemo(
    () => (records?.length ? Object.keys(records[0]).filter(k => k !== 'attributes') : []),
    [records]
  )

  const recordIds = useMemo(
    () => (records || []).map(pickRecordId).filter(Boolean),
    [records]
  )

  const annotatable = recordIds.length > 0

  const loadNotes = async () => {
    if (!recordIds.length) return
    try {
      const res = await api.annotationLookup(recordIds)
      setNotesMap(res.map || {})
    } catch {}
  }

  useEffect(() => { loadNotes() /* eslint-disable-next-line */ }, [recordIds.join(',')])

  const sorted = useMemo(() => {
    if (!sortKey || !records?.length) return records
    return [...records].sort((a, b) => cmp(a[sortKey], b[sortKey], sortDir))
  }, [records, sortKey, sortDir])

  if (!records?.length || records.length < 3) return null
  if (isAggregate(records)) return null

  const totalPages = Math.ceil(sorted.length / PAGE_SIZE)
  const pageRows = sorted.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)
  const start = page * PAGE_SIZE + 1
  const end = Math.min((page + 1) * PAGE_SIZE, sorted.length)

  const toggle = (col) => {
    if (sortKey === col) setSortDir(sortDir === 'asc' ? 'desc' : 'asc')
    else { setSortKey(col); setSortDir('asc') }
    setPage(0)
  }

  return (
    <div className="data-table-container">
      <div className="data-table-header">
        <span className="data-table-count">
          {sorted.length > PAGE_SIZE
            ? `${start}–${end} of ${sorted.length.toLocaleString()}`
            : `${sorted.length.toLocaleString()}`}
          {totalSize && totalSize > sorted.length ? ` (${totalSize.toLocaleString()} total in Salesforce)` : ''}
          {' '}records
        </span>
      </div>
      <div className="data-table-scroll">
        <table className="data-table">
          <thead>
            <tr>
              {cols.map(c => {
                const active = sortKey === c
                return (
                  <th key={c} onClick={() => toggle(c)} className={active ? `sort-${sortDir}` : ''}>
                    <span className="th-label">{c.replace(/__c$/, '').replace(/_/g, ' ')}</span>
                    <span className="th-arrow">{active ? (sortDir === 'asc' ? '↑' : '↓') : '↕'}</span>
                  </th>
                )
              })}
              {annotatable && <th className="note-col-head">Notes</th>}
            </tr>
          </thead>
          <tbody>
            {pageRows.map((r, i) => {
              const rid = pickRecordId(r)
              const rowNotes = rid ? (notesMap[rid] || []) : []
              return (
                <tr key={i}>
                  {cols.map(c => {
                    const val = formatCell(r[c])
                    return <td key={c} title={val}>{val}</td>
                  })}
                  {annotatable && (
                    <td className="note-col">
                      {rid ? (
                        <button
                          type="button"
                          className={`note-btn ${rowNotes.length ? 'has-notes' : ''}`}
                          title={rowNotes.length ? `${rowNotes.length} note(s)` : 'Add note'}
                          onClick={() => setOpenRow(r)}
                        >
                          {rowNotes.length ? `📝 ${rowNotes.length}` : '📝'}
                        </button>
                      ) : '—'}
                    </td>
                  )}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      {totalPages > 1 && (
        <div className="data-table-pagination">
          <button
            className="pagination-btn"
            disabled={page === 0}
            onClick={() => setPage(0)}
            title="First page"
          >««</button>
          <button
            className="pagination-btn"
            disabled={page === 0}
            onClick={() => setPage(p => p - 1)}
          >‹ Prev</button>
          <span className="pagination-info">
            Page {page + 1} of {totalPages}
          </span>
          <button
            className="pagination-btn"
            disabled={page >= totalPages - 1}
            onClick={() => setPage(p => p + 1)}
          >Next ›</button>
          <button
            className="pagination-btn"
            disabled={page >= totalPages - 1}
            onClick={() => setPage(totalPages - 1)}
            title="Last page"
          >»»</button>
        </div>
      )}
      {openRow && (
        <NotePopover
          row={openRow}
          existingNotes={notesMap[pickRecordId(openRow)] || []}
          onClose={() => setOpenRow(null)}
          onSaved={loadNotes}
        />
      )}
    </div>
  )
}
