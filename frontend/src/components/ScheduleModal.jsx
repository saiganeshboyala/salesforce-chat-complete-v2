import { useState } from 'react'
import { api } from '../services/api'
import { useToast } from '../hooks/useToast'

const XIcon = (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
  </svg>
)

const WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

export default function ScheduleModal({ message, onClose, onCreated }) {
  const toast = useToast()
  const [name, setName] = useState(() => (message.question || 'Scheduled report').slice(0, 60))
  const [frequency, setFrequency] = useState('daily')
  const [time, setTime] = useState('09:00')
  const [weekday, setWeekday] = useState(0)
  const [dayOfMonth, setDayOfMonth] = useState(1)
  const [recipientsText, setRecipientsText] = useState('')
  const [saving, setSaving] = useState(false)
  const [err, setErr] = useState('')

  const soql = message.soql || message.data?.query || ''

  const handleSubmit = async (e) => {
    e.preventDefault()
    if (!soql) { setErr('This answer has no SOQL to schedule.'); return }
    setSaving(true); setErr('')
    try {
      const payload = {
        name,
        question: message.question || '',
        soql,
        frequency,
        time,
        weekday: frequency === 'weekly' ? Number(weekday) : null,
        day_of_month: frequency === 'monthly' ? Number(dayOfMonth) : null,
        recipients: recipientsText.split(/[,\s]+/).map(s => s.trim()).filter(Boolean),
      }
      await api.createSchedule(payload)
      toast.success('Schedule created')
      onCreated?.()
      onClose()
    } catch (ex) {
      setErr(ex.message)
      toast.error(ex.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()} style={{ maxWidth: 560 }}>
        <div className="modal-header">
          <h3 className="modal-title">Schedule This Report</h3>
          <button type="button" className="action-btn" onClick={onClose}>{XIcon}</button>
        </div>

        {err && <div className="modal-error">{err}</div>}

        {!soql && (
          <div className="modal-error">
            Only answers with a SOQL query can be scheduled.
          </div>
        )}

        <form className="modal-form" onSubmit={handleSubmit}>
          <label>Name
            <input className="input-field" value={name} onChange={e => setName(e.target.value)} required />
          </label>

          <label>SOQL (read-only)
            <textarea className="input-field" value={soql} readOnly rows={3}
              style={{ fontFamily: 'var(--font-mono)', fontSize: 11, resize: 'vertical' }} />
          </label>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14 }}>
            <label>Frequency
              <select className="input-field" value={frequency} onChange={e => setFrequency(e.target.value)}>
                <option value="daily">Daily</option>
                <option value="weekly">Weekly</option>
                <option value="monthly">Monthly</option>
              </select>
            </label>
            <label>Time
              <input className="input-field" type="time" value={time} onChange={e => setTime(e.target.value)} required />
            </label>
          </div>

          {frequency === 'weekly' && (
            <label>Day of week
              <select className="input-field" value={weekday} onChange={e => setWeekday(e.target.value)}>
                {WEEKDAYS.map((d, i) => <option key={d} value={i}>{d}</option>)}
              </select>
            </label>
          )}

          {frequency === 'monthly' && (
            <label>Day of month (1–28)
              <input className="input-field" type="number" min="1" max="28" value={dayOfMonth}
                onChange={e => setDayOfMonth(e.target.value)} />
            </label>
          )}

          <label>Email recipients (optional, comma-separated)
            <input className="input-field" value={recipientsText}
              onChange={e => setRecipientsText(e.target.value)} placeholder="alice@corp.com, bob@corp.com" />
            <span className="modal-hint">Results are saved to disk. Email delivery is not yet wired — recipients are recorded.</span>
          </label>

          <div className="modal-actions">
            <button type="button" className="btn-secondary" onClick={onClose}>Cancel</button>
            <button type="submit" className="btn-primary" disabled={saving || !soql}>
              {saving ? 'Saving…' : 'Create Schedule'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
