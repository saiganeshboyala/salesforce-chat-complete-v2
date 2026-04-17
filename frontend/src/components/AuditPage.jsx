import { useEffect, useState, useCallback } from 'react'
import { api } from '../services/api'

const ACTION_CLASS = {
  login: 'audit-action-login',
  login_failed: 'audit-action-error',
  logout: 'audit-action-login',
  chat_question: 'audit-action-chat',
  feedback: 'audit-action-chat',
  export: 'audit-action-export',
  user_created: 'audit-action-login',
  user_deleted: 'audit-action-error',
  schema_refresh: 'audit-action-export',
}

function fmt(iso) {
  if (!iso) return ''
  try { return new Date(iso).toLocaleString() } catch { return iso }
}

function fmtDetails(d) {
  if (!d) return ''
  if (typeof d === 'string') return d
  const keys = Object.keys(d)
  if (!keys.length) return ''
  return keys.map(k => `${k}: ${String(d[k]).slice(0, 80)}`).join(' · ')
}

export default function AuditPage() {
  const [data, setData] = useState({ entries: [], users: [], actions: [], total: 0 })
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState('')
  const [filters, setFilters] = useState({ user: '', action: '', start: '', end: '' })
  const [page, setPage] = useState(1)
  const pageSize = 50

  const load = useCallback(async () => {
    setLoading(true); setErr('')
    try {
      const res = await api.audit({ ...filters, page, page_size: pageSize })
      setData(res)
    } catch (e) {
      setErr(e.message)
    } finally {
      setLoading(false)
    }
  }, [filters, page])

  useEffect(() => { load() }, [load])

  const onFilter = (patch) => { setFilters(f => ({ ...f, ...patch })); setPage(1) }

  const totalPages = Math.max(1, Math.ceil((data.total || 0) / pageSize))

  return (
    <div className="audit-page">
      <div className="audit-header">
        <div>
          <h2 className="audit-title">Audit Log</h2>
          <p className="audit-subtitle">{data.total?.toLocaleString() || 0} total entries</p>
        </div>
        <button className="btn-secondary" onClick={load} disabled={loading}>
          {loading ? 'Loading…' : 'Refresh'}
        </button>
      </div>

      <div className="audit-filters">
        <select value={filters.user} onChange={e => onFilter({ user: e.target.value })}>
          <option value="">All users</option>
          {(data.users || []).map(u => <option key={u} value={u}>{u}</option>)}
        </select>
        <select value={filters.action} onChange={e => onFilter({ action: e.target.value })}>
          <option value="">All actions</option>
          {(data.actions || []).map(a => <option key={a} value={a}>{a}</option>)}
        </select>
        <input type="date" value={filters.start} onChange={e => onFilter({ start: e.target.value })} />
        <input type="date" value={filters.end} onChange={e => onFilter({ end: e.target.value ? e.target.value + 'T23:59:59' : '' })} />
      </div>

      {err && <div className="audit-error">{err}</div>}

      <div className="audit-table-wrap">
        <table className="audit-table">
          <thead>
            <tr>
              <th style={{width: 170}}>Timestamp</th>
              <th style={{width: 140}}>User</th>
              <th style={{width: 140}}>Action</th>
              <th>Details</th>
              <th style={{width: 110}}>IP</th>
            </tr>
          </thead>
          <tbody>
            {(data.entries || []).length === 0 ? (
              <tr><td colSpan={5} className="audit-empty">{loading ? 'Loading…' : 'No audit entries'}</td></tr>
            ) : (
              data.entries.map((e, i) => (
                <tr key={i}>
                  <td>{fmt(e.timestamp)}</td>
                  <td>{e.username || '—'}</td>
                  <td>
                    <span className={`audit-badge ${ACTION_CLASS[e.action] || 'audit-action-default'}`}>
                      {e.action}
                    </span>
                  </td>
                  <td className="audit-details">{fmtDetails(e.details)}</td>
                  <td className="audit-ip">{e.ip_address || '—'}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <div className="audit-pagination">
        <button className="btn-secondary" onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page <= 1}>← Prev</button>
        <span className="audit-page-info">Page {page} of {totalPages}</span>
        <button className="btn-secondary" onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages}>Next →</button>
      </div>
    </div>
  )
}
