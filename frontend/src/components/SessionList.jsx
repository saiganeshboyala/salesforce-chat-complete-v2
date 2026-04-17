import { useState } from 'react'

const XIcon = (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
    <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
  </svg>
)
const SearchIcon = (
  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
  </svg>
)
const StarOutline = (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
  </svg>
)
const StarFilled = (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" strokeWidth="1" strokeLinejoin="round">
    <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
  </svg>
)

function relTime(iso) {
  if (!iso) return ''
  const diff = (Date.now() - new Date(iso).getTime()) / 1000
  if (diff < 60) return 'just now'
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  if (diff < 604800) return `${Math.floor(diff / 86400)}d ago`
  return new Date(iso).toLocaleDateString()
}

export default function SessionList({ sessions, activeId, onPick, onDelete, onPin, search, onSearchChange, labels = {} }) {
  const [confirmId, setConfirmId] = useState(null)

  const handleDelete = (e, id) => {
    e.stopPropagation()
    if (confirmId === id) {
      onDelete(id)
      setConfirmId(null)
    } else {
      setConfirmId(id)
      setTimeout(() => setConfirmId(c => c === id ? null : c), 2500)
    }
  }

  const handlePin = (e, id) => {
    e.stopPropagation()
    if (onPin) onPin(id)
  }

  const pinned = sessions.filter(s => s.pinned)
  const recent = sessions.filter(s => !s.pinned)

  const renderItem = (s) => (
    <div
      key={s.id}
      className={`session-item ${activeId === s.id ? 'active' : ''} ${s.pinned ? 'pinned' : ''}`}
      onClick={() => onPick(s.id)}
      title={s.title}
    >
      <button
        type="button"
        className={`session-pin ${s.pinned ? 'is-pinned' : ''}`}
        onClick={e => handlePin(e, s.id)}
        title={s.pinned ? 'Unpin' : 'Pin chat'}
      >
        {s.pinned ? StarFilled : StarOutline}
      </button>
      <div className="session-item-body">
        <div className="session-item-title">{s.title || 'Untitled'}</div>
        <div className="session-item-meta">{relTime(s.updated_at)} · {s.message_count || 0} msgs</div>
      </div>
      <button
        type="button"
        className={`session-delete ${confirmId === s.id ? 'confirm' : ''}`}
        onClick={e => handleDelete(e, s.id)}
        title={confirmId === s.id ? 'Click again to confirm' : 'Delete'}
      >
        {XIcon}
      </button>
    </div>
  )

  return (
    <div className="session-list">
      <div className="session-search">
        <span className="session-search-icon">{SearchIcon}</span>
        <input
          type="text"
          value={search}
          onChange={e => onSearchChange(e.target.value)}
          placeholder={labels.searchChats || 'Search chats...'}
          className="session-search-input"
        />
      </div>

      {sessions.length === 0 && (
        <div className="session-empty">
          {search ? 'No matching chats' : 'No saved chats yet'}
        </div>
      )}

      {pinned.length > 0 && (
        <>
          <div className="session-list-header">{labels.pinned || 'Pinned'}</div>
          {pinned.map(renderItem)}
        </>
      )}

      {recent.length > 0 && (
        <>
          <div className="session-list-header">{labels.recent || 'Recent'}</div>
          {recent.map(renderItem)}
        </>
      )}
    </div>
  )
}
