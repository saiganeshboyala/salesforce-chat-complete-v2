import { useEffect, useMemo, useState } from 'react'
import { api } from '../services/api'

const TAG_PALETTE = ['#4a9ee8', '#e8734a', '#8ac24a', '#b762d9', '#e8c94a', '#ea5a75', '#4ad9c4', '#a59584']
function tagColor(tag) {
  let h = 0
  for (let i = 0; i < tag.length; i++) h = (h * 31 + tag.charCodeAt(i)) & 0xffff
  return TAG_PALETTE[h % TAG_PALETTE.length]
}

function TagChip({ tag, onClick, active }) {
  return (
    <span
      className={`note-tag ${active ? 'active' : ''}`}
      onClick={onClick}
      style={{ borderColor: tagColor(tag), color: tagColor(tag) }}
    >
      {tag}
    </span>
  )
}

function NoteCard({ note, onEdit, onDelete }) {
  return (
    <div className="note-card">
      <div className="note-card-head">
        <div className="note-record">
          <span className="note-record-name">{note.record_name || note.record_id}</span>
          {note.object_type && <span className="note-object">{note.object_type}</span>}
        </div>
        <div className="note-card-actions">
          <button className="btn-secondary" onClick={() => onEdit(note)}>Edit</button>
          <button className="btn-secondary danger" onClick={() => onDelete(note.id)}>Delete</button>
        </div>
      </div>
      <div className="note-text">{note.text}</div>
      {(note.tags || []).length > 0 && (
        <div className="note-tags">
          {note.tags.map(t => <TagChip key={t} tag={t} />)}
        </div>
      )}
      <div className="note-meta">
        Updated {new Date(note.updated_at).toLocaleString()}
      </div>
    </div>
  )
}

function NoteForm({ initial, onSave, onCancel }) {
  const [text, setText] = useState(initial?.text || '')
  const [tags, setTags] = useState((initial?.tags || []).join(', '))
  const [saving, setSaving] = useState(false)
  const [err, setErr] = useState('')

  const submit = async (e) => {
    e.preventDefault()
    if (!text.trim()) { setErr('Note text is required'); return }
    setSaving(true); setErr('')
    try {
      const tagArr = tags.split(',').map(t => t.trim()).filter(Boolean)
      await onSave({ text: text.trim(), tags: tagArr })
    } catch (ex) {
      setErr(ex.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <form className="note-form" onSubmit={submit}>
      <label>Note
        <textarea value={text} onChange={e => setText(e.target.value)} rows={4} autoFocus placeholder="Add a note about this record…" />
      </label>
      <label>Tags <span className="muted-inline">(comma-separated)</span>
        <input value={tags} onChange={e => setTags(e.target.value)} placeholder="priority, follow-up" />
      </label>
      {err && <div className="note-error">{err}</div>}
      <div className="note-form-actions">
        <button type="button" className="btn-secondary" onClick={onCancel}>Cancel</button>
        <button type="submit" className="btn-primary" disabled={saving}>{saving ? 'Saving…' : 'Save note'}</button>
      </div>
    </form>
  )
}

export default function NotesPage() {
  const [notes, setNotes] = useState([])
  const [tags, setTags] = useState([])
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState('')
  const [q, setQ] = useState('')
  const [selectedTag, setSelectedTag] = useState('')
  const [editing, setEditing] = useState(null)

  const load = async () => {
    setLoading(true); setErr('')
    try {
      const params = {}
      if (q) params.q = q
      if (selectedTag) params.tag = selectedTag
      const [n, t] = await Promise.all([api.listAnnotations(params), api.annotationTags()])
      setNotes(n.notes || [])
      setTags(t.tags || [])
    } catch (ex) {
      setErr(ex.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() /* eslint-disable-next-line */ }, [selectedTag])

  const submitSearch = (e) => { e.preventDefault(); load() }

  const handleUpdate = async (patch) => {
    if (!editing) return
    await api.updateAnnotation(editing.id, patch)
    setEditing(null)
    await load()
  }

  const handleDelete = async (id) => {
    if (!confirm('Delete this note?')) return
    await api.deleteAnnotation(id)
    await load()
  }

  const grouped = useMemo(() => {
    const m = {}
    notes.forEach(n => { (m[n.record_id] = m[n.record_id] || []).push(n) })
    return m
  }, [notes])

  return (
    <div className="notes-page">
      <div className="notes-header">
        <div>
          <h2 className="notes-title">Data Annotations</h2>
          <p className="notes-subtitle">Personal notes and tags attached to Salesforce records</p>
        </div>
      </div>

      <form className="notes-search" onSubmit={submitSearch}>
        <input value={q} onChange={e => setQ(e.target.value)} placeholder="Search notes or record names…" />
        <button type="submit" className="btn-secondary">Search</button>
        {(q || selectedTag) && (
          <button type="button" className="btn-secondary" onClick={() => { setQ(''); setSelectedTag(''); load() }}>Clear</button>
        )}
      </form>

      {tags.length > 0 && (
        <div className="notes-tag-filter">
          {tags.map(t => (
            <TagChip
              key={t.tag}
              tag={`${t.tag} · ${t.count}`}
              active={selectedTag === t.tag}
              onClick={() => setSelectedTag(selectedTag === t.tag ? '' : t.tag)}
            />
          ))}
        </div>
      )}

      {err && <div className="note-error">{err}</div>}

      {editing && (
        <div className="note-form-wrap">
          <div className="note-form-label">Editing note on <b>{editing.record_name || editing.record_id}</b></div>
          <NoteForm initial={editing} onSave={handleUpdate} onCancel={() => setEditing(null)} />
        </div>
      )}

      {loading && !notes.length ? <div className="muted">Loading…</div> :
       notes.length === 0 ? <div className="muted">No notes yet. Add one from any chat result row.</div> : (
        <div className="notes-list">
          {Object.entries(grouped).map(([rid, list]) => (
            <div key={rid} className="notes-group">
              <div className="notes-group-head">{list[0].record_name || rid} <span className="muted-inline">({list.length})</span></div>
              {list.map(n => <NoteCard key={n.id} note={n} onEdit={setEditing} onDelete={handleDelete} />)}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
