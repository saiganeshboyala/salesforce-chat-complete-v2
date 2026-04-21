import { useEffect, useState } from 'react'
import { api } from '../services/api'

const CONDITIONS = [
  { value: 'greater_than', label: 'greater than' },
  { value: 'less_than',    label: 'less than' },
  { value: 'equals',       label: 'equals' },
  { value: 'changed',      label: 'changed since last check' },
]

const FREQUENCIES = [
  { value: 'hourly', label: 'Hourly' },
  { value: 'daily',  label: 'Daily' },
  { value: 'weekly', label: 'Weekly' },
]

const EMPTY_FORM = {
  name: '', soql: '', condition: 'greater_than', threshold: 0, frequency: 'daily', enabled: true,
}

function RuleForm({ initial, onSave, onCancel }) {
  const [f, setF] = useState(initial || EMPTY_FORM)
  const [err, setErr] = useState('')
  const [saving, setSaving] = useState(false)
  const update = (k, v) => setF(prev => ({ ...prev, [k]: v }))

  const submit = async (e) => {
    e.preventDefault()
    if (!f.name.trim() || !f.soql.trim()) { setErr('Name and SQL query are required'); return }
    setSaving(true); setErr('')
    try {
      await onSave({ ...f, threshold: Number(f.threshold) || 0 })
    } catch (ex) {
      setErr(ex.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <form className="alert-form" onSubmit={submit}>
      <label>Name
        <input value={f.name} onChange={e => update('name', e.target.value)} placeholder="Low inventory" />
      </label>
      <label>SQL Query
        <textarea rows={3} value={f.soql} onChange={e => update('soql', e.target.value)}
          placeholder='SELECT COUNT(*) FROM "Student__c" WHERE "Student_Marketing_Status__c" = $$In Market$$' />
      </label>
      <div className="alert-form-row">
        <label>Condition
          <select value={f.condition} onChange={e => update('condition', e.target.value)}>
            {CONDITIONS.map(c => <option key={c.value} value={c.value}>{c.label}</option>)}
          </select>
        </label>
        <label>Threshold
          <input type="number" value={f.threshold} onChange={e => update('threshold', e.target.value)}
            disabled={f.condition === 'changed'} />
        </label>
        <label>Frequency
          <select value={f.frequency} onChange={e => update('frequency', e.target.value)}>
            {FREQUENCIES.map(c => <option key={c.value} value={c.value}>{c.label}</option>)}
          </select>
        </label>
      </div>
      <label className="alert-form-checkbox">
        <input type="checkbox" checked={f.enabled} onChange={e => update('enabled', e.target.checked)} />
        Enabled
      </label>
      {err && <div className="alert-error">{err}</div>}
      <div className="alert-form-actions">
        <button type="button" className="btn-secondary" onClick={onCancel}>Cancel</button>
        <button type="submit" className="btn-primary" disabled={saving}>{saving ? 'Saving…' : 'Save'}</button>
      </div>
    </form>
  )
}

function RuleCard({ rule, onEdit, onDelete, onToggle, onCheck }) {
  const [checking, setChecking] = useState(false)
  const cond = CONDITIONS.find(c => c.value === rule.condition)?.label || rule.condition

  const runCheck = async () => {
    setChecking(true)
    try { await onCheck(rule.id) } finally { setChecking(false) }
  }

  return (
    <div className={`alert-card ${rule.triggered ? 'triggered' : ''} ${!rule.enabled ? 'disabled' : ''}`}>
      <div className="alert-card-head">
        <div className="alert-name">{rule.name}</div>
        {rule.triggered && <span className="alert-badge triggered">Triggered</span>}
        {!rule.enabled && <span className="alert-badge disabled">Disabled</span>}
      </div>
      <div className="alert-desc">
        When result is <b>{cond}</b>
        {rule.condition !== 'changed' && <> <b>{rule.threshold}</b></>}
        {' · '}<i>{FREQUENCIES.find(f => f.value === rule.frequency)?.label}</i>
      </div>
      <div className="alert-soql"><code>{rule.soql}</code></div>
      <div className="alert-meta">
        Last value: <b>{rule.last_value ?? '—'}</b>
        {rule.last_checked && <> · Checked: {new Date(rule.last_checked).toLocaleString()}</>}
        {rule.last_triggered && <> · Last triggered: {new Date(rule.last_triggered).toLocaleString()}</>}
      </div>
      <div className="alert-actions">
        <button className="btn-secondary" onClick={runCheck} disabled={checking}>
          {checking ? 'Checking…' : 'Check now'}
        </button>
        <button className="btn-secondary" onClick={() => onToggle(rule)}>
          {rule.enabled ? 'Disable' : 'Enable'}
        </button>
        <button className="btn-secondary" onClick={() => onEdit(rule)}>Edit</button>
        <button className="btn-secondary danger" onClick={() => onDelete(rule.id)}>Delete</button>
      </div>
    </div>
  )
}

export default function AlertsPage() {
  const [tab, setTab] = useState('rules')
  const [rules, setRules] = useState([])
  const [history, setHistory] = useState([])
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState('')
  const [showForm, setShowForm] = useState(false)
  const [editing, setEditing] = useState(null)

  const load = async () => {
    setLoading(true); setErr('')
    try {
      const [a, h] = await Promise.all([api.listAlerts(), api.alertHistory()])
      setRules(a.rules || [])
      setHistory(h.entries || [])
    } catch (ex) {
      setErr(ex.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const handleSave = async (payload) => {
    if (editing) await api.updateAlert(editing.id, payload)
    else         await api.createAlert(payload)
    setShowForm(false); setEditing(null)
    await load()
  }

  const handleDelete = async (id) => {
    if (!confirm('Delete this alert rule?')) return
    await api.deleteAlert(id); await load()
  }

  const handleToggle = async (rule) => {
    await api.updateAlert(rule.id, { enabled: !rule.enabled }); await load()
  }

  const handleCheck = async (id) => {
    try { await api.checkAlert(id); await load() }
    catch (ex) { setErr(ex.message) }
  }

  const handleCheckAll = async () => {
    setLoading(true)
    try { await api.checkAllAlerts(); await load() }
    catch (ex) { setErr(ex.message) }
    finally   { setLoading(false) }
  }

  return (
    <div className="alerts-page">
      <div className="alerts-header">
        <div>
          <h2 className="alerts-title">Alert Rules</h2>
          <p className="alerts-subtitle">Watch SQL queries and get notified when thresholds are crossed</p>
        </div>
        <div className="alerts-header-actions">
          <button className="btn-secondary" onClick={handleCheckAll} disabled={loading}>Check all</button>
          <button className="btn-primary" onClick={() => { setEditing(null); setShowForm(true) }}>
            + New Alert
          </button>
        </div>
      </div>

      <div className="alerts-tabs">
        <button className={tab === 'rules' ? 'active' : ''} onClick={() => setTab('rules')}>
          Rules ({rules.length})
        </button>
        <button className={tab === 'history' ? 'active' : ''} onClick={() => setTab('history')}>
          History ({history.length})
        </button>
      </div>

      {err && <div className="alert-error">{err}</div>}

      {showForm && (
        <div className="alert-form-wrap">
          <RuleForm
            initial={editing || EMPTY_FORM}
            onSave={handleSave}
            onCancel={() => { setShowForm(false); setEditing(null) }}
          />
        </div>
      )}

      {tab === 'rules' && (
        <div className="alerts-list">
          {loading && !rules.length ? <div className="muted">Loading…</div> :
           rules.length === 0 ? <div className="muted">No alert rules yet. Click "New Alert" to create one.</div> :
           rules.map(r => (
             <RuleCard
               key={r.id}
               rule={r}
               onEdit={(rule) => { setEditing(rule); setShowForm(true) }}
               onDelete={handleDelete}
               onToggle={handleToggle}
               onCheck={handleCheck}
             />
           ))}
        </div>
      )}

      {tab === 'history' && (
        <div className="alert-history">
          {history.length === 0 ? <div className="muted">No alerts triggered yet.</div> : (
            <table className="alert-history-table">
              <thead>
                <tr><th>Time</th><th>Rule</th><th>Value</th><th>Condition</th><th>Threshold</th></tr>
              </thead>
              <tbody>
                {history.map((h, i) => (
                  <tr key={i}>
                    <td>{new Date(h.timestamp).toLocaleString()}</td>
                    <td>{h.rule_name}</td>
                    <td><b>{h.value}</b></td>
                    <td>{CONDITIONS.find(c => c.value === h.condition)?.label || h.condition}</td>
                    <td>{h.threshold}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  )
}
