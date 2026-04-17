import { useEffect, useState } from 'react'
import { api } from '../services/api'
import { useToast } from '../hooks/useToast'

const WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

function scheduleSummary(s) {
  const parts = [s.frequency]
  if (s.frequency === 'weekly' && s.weekday != null) parts.push(WEEKDAYS[s.weekday])
  if (s.frequency === 'monthly' && s.day_of_month) parts.push(`day ${s.day_of_month}`)
  parts.push(`at ${s.time}`)
  return parts.join(' · ')
}

function fmtDate(iso) {
  if (!iso) return '—'
  try { return new Date(iso).toLocaleString() } catch { return iso }
}

export default function SchedulesPage() {
  const toast = useToast()
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState('')
  const [runs, setRuns] = useState({}) // { scheduleId: [runs] }
  const [expanded, setExpanded] = useState(null)

  const load = async () => {
    setLoading(true); setErr('')
    try {
      const list = await api.listSchedules()
      setItems(list || [])
    } catch (e) { setErr(e.message) }
    finally { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  const handleDelete = async (id) => {
    if (!confirm('Delete this schedule?')) return
    try {
      await api.deleteSchedule(id)
      toast.success('Schedule deleted')
      load()
    } catch (e) { setErr(e.message); toast.error(e.message) }
  }

  const handleRunNow = async (id) => {
    try {
      const meta = await api.runScheduleNow(id)
      if (meta?.status === 'ok') toast.success(`Ran: ${meta.row_count ?? 0} rows`)
      else toast.error(`Run failed: ${meta?.error || 'unknown error'}`)
      load()
      if (expanded === id) loadRuns(id)
    } catch (e) { setErr(e.message); toast.error(e.message) }
  }

  const handleToggle = async (s) => {
    try {
      await api.updateSchedule(s.id, { enabled: !s.enabled })
      toast.info(s.enabled ? 'Schedule disabled' : 'Schedule enabled')
      load()
    } catch (e) { setErr(e.message); toast.error(e.message) }
  }

  const loadRuns = async (id) => {
    try {
      const r = await api.listScheduleRuns(id)
      setRuns(prev => ({ ...prev, [id]: r }))
    } catch (e) { setErr(e.message) }
  }

  const toggleExpand = (id) => {
    if (expanded === id) { setExpanded(null); return }
    setExpanded(id)
    if (!runs[id]) loadRuns(id)
  }

  return (
    <div className="schedules-page">
      <div className="schedules-header">
        <div>
          <h2 className="schedules-title">Scheduled Reports</h2>
          <div className="schedules-subtitle">
            {items.length} schedule{items.length === 1 ? '' : 's'} · runner ticks every minute
          </div>
        </div>
        <button className="btn-secondary" onClick={load}>Refresh</button>
      </div>

      {err && <div className="modal-error" style={{ margin: '0 24px 16px' }}>{err}</div>}

      {loading ? (
        <div className="schedules-empty">Loading…</div>
      ) : items.length === 0 ? (
        <div className="schedules-empty">
          No schedules yet. Ask a question in Chat, then click the clock icon on the answer to schedule it.
        </div>
      ) : (
        <div className="schedules-list">
          {items.map(s => {
            const isExpanded = expanded === s.id
            const statusClass = s.last_status === 'ok' ? 'ok'
              : s.last_status === 'error' ? 'error' : 'idle'
            return (
              <div key={s.id} className={`schedule-card ${s.enabled ? '' : 'disabled'}`}>
                <div className="schedule-card-head" onClick={() => toggleExpand(s.id)}>
                  <div className="schedule-card-info">
                    <div className="schedule-card-name">{s.name}</div>
                    <div className="schedule-card-meta">{scheduleSummary(s)}</div>
                  </div>
                  <div className="schedule-card-side">
                    <span className={`schedule-status schedule-status-${statusClass}`}>
                      {s.last_status || 'pending'}
                    </span>
                    <div className="schedule-next">next: {fmtDate(s.next_run)}</div>
                  </div>
                </div>

                {isExpanded && (
                  <div className="schedule-card-body">
                    <div className="schedule-detail-row"><strong>SOQL:</strong> <code>{s.soql}</code></div>
                    {s.question && <div className="schedule-detail-row"><strong>Question:</strong> {s.question}</div>}
                    <div className="schedule-detail-row"><strong>Recipients:</strong> {s.recipients?.length ? s.recipients.join(', ') : '(none)'}</div>
                    <div className="schedule-detail-row"><strong>Last run:</strong> {fmtDate(s.last_run)} {s.last_row_count != null && `(${s.last_row_count} rows)`}</div>
                    {s.last_error && <div className="schedule-detail-row schedule-error"><strong>Error:</strong> {s.last_error}</div>}

                    <div className="schedule-actions">
                      <button className="btn-small" onClick={() => handleRunNow(s.id)}>Run now</button>
                      <button className="btn-small" onClick={() => handleToggle(s)}>
                        {s.enabled ? 'Disable' : 'Enable'}
                      </button>
                      <button className="btn-small btn-danger" onClick={() => handleDelete(s.id)}>Delete</button>
                    </div>

                    {runs[s.id]?.length > 0 && (
                      <div className="schedule-runs">
                        <div className="schedule-runs-header">Recent runs</div>
                        {runs[s.id].map((r, i) => (
                          <div key={i} className="schedule-run-row">
                            <span className={`schedule-status schedule-status-${r.status === 'ok' ? 'ok' : 'error'}`}>{r.status}</span>
                            <span>{fmtDate(r.ran_at)}</span>
                            <span className="users-cell-muted">{r.row_count ?? 0} rows{r.error ? ` — ${r.error}` : ''}</span>
                          </div>
                        ))}
                      </div>
                    )}
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
