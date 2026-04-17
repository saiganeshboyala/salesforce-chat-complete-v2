import { useEffect, useState } from 'react'
import { api } from '../services/api'
import { useToast } from '../hooks/useToast'

const XIcon = (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
  </svg>
)

export default function UsersModal({ currentUser, onClose }) {
  const toast = useToast()
  const [users, setUsers] = useState([])
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState('')
  const [mode, setMode] = useState('list') // list | add | reset
  const [resetFor, setResetFor] = useState(null)

  // Add form state
  const [form, setForm] = useState({ username: '', password: '', name: '', role: 'user' })
  const [newPwd, setNewPwd] = useState('')

  const load = async () => {
    setLoading(true)
    setErr('')
    try {
      const data = await api.listUsers()
      setUsers(data)
    } catch (e) { setErr(e.message) }
    finally { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  const handleAdd = async (e) => {
    e.preventDefault()
    setErr('')
    try {
      await api.createUser(form)
      toast.success(`User "${form.username}" created`)
      setForm({ username: '', password: '', name: '', role: 'user' })
      setMode('list')
      load()
    } catch (e) { setErr(e.message); toast.error(e.message) }
  }

  const handleDelete = async (username) => {
    if (!confirm(`Delete user "${username}"? This cannot be undone.`)) return
    setErr('')
    try {
      await api.deleteUser(username)
      toast.success(`User "${username}" deleted`)
      load()
    } catch (e) { setErr(e.message); toast.error(e.message) }
  }

  const handleReset = async (e) => {
    e.preventDefault()
    setErr('')
    try {
      await api.resetPassword(resetFor, newPwd)
      toast.success(`Password reset for ${resetFor}`)
      setMode('list')
      setResetFor(null)
      setNewPwd('')
    } catch (e) { setErr(e.message); toast.error(e.message) }
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h3 className="modal-title">
            {mode === 'add' ? 'Add User'
              : mode === 'reset' ? `Reset Password — ${resetFor}`
              : 'User Management'}
          </h3>
          <button type="button" className="action-btn" onClick={onClose}>{XIcon}</button>
        </div>

        {err && <div className="modal-error">{err}</div>}

        {mode === 'list' && (
          <>
            <div className="modal-toolbar">
              <button className="btn-primary" onClick={() => setMode('add')}>+ Add User</button>
            </div>
            {loading ? (
              <div className="modal-empty">Loading…</div>
            ) : (
              <div className="users-table">
                <div className="users-row users-row-head">
                  <span>Username</span><span>Name</span><span>Role</span><span>Created</span><span></span>
                </div>
                {users.map(u => {
                  const isSelf = u.username === currentUser.username
                  const isAdminAcct = u.username === 'admin'
                  const canDelete = !isSelf && !isAdminAcct
                  return (
                    <div key={u.username} className="users-row">
                      <span className="users-cell-username">{u.username}{isSelf && <em> (you)</em>}</span>
                      <span>{u.name}</span>
                      <span><span className={`role-badge role-${u.role}`}>{u.role}</span></span>
                      <span className="users-cell-muted">{u.created ? new Date(u.created).toLocaleDateString() : '—'}</span>
                      <span className="users-cell-actions">
                        <button className="btn-small" onClick={() => { setResetFor(u.username); setMode('reset') }}>Reset PW</button>
                        <button className="btn-small btn-danger" disabled={!canDelete} onClick={() => handleDelete(u.username)}>Delete</button>
                      </span>
                    </div>
                  )
                })}
              </div>
            )}
          </>
        )}

        {mode === 'add' && (
          <form onSubmit={handleAdd} className="modal-form">
            <label>Username
              <input className="input-field" value={form.username} onChange={e => setForm({...form, username: e.target.value})} required autoFocus />
            </label>
            <label>Name
              <input className="input-field" value={form.name} onChange={e => setForm({...form, name: e.target.value})} required />
            </label>
            <label>Password
              <input className="input-field" type="password" value={form.password} onChange={e => setForm({...form, password: e.target.value})} required minLength={6} />
            </label>
            <label>Role
              <select className="input-field" value={form.role} onChange={e => setForm({...form, role: e.target.value})}>
                <option value="user">User</option>
                <option value="admin">Admin</option>
              </select>
            </label>
            <div className="modal-actions">
              <button type="button" className="btn-secondary" onClick={() => setMode('list')}>Cancel</button>
              <button type="submit" className="btn-primary">Create</button>
            </div>
          </form>
        )}

        {mode === 'reset' && (
          <form onSubmit={handleReset} className="modal-form">
            <label>New password
              <input className="input-field" type="password" value={newPwd} onChange={e => setNewPwd(e.target.value)} required minLength={6} autoFocus />
            </label>
            <div className="modal-actions">
              <button type="button" className="btn-secondary" onClick={() => { setMode('list'); setResetFor(null); setNewPwd('') }}>Cancel</button>
              <button type="submit" className="btn-primary">Reset Password</button>
            </div>
          </form>
        )}
      </div>
    </div>
  )
}
