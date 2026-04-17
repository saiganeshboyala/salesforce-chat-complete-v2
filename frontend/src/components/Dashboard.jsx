import { useState, useEffect, useCallback } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid, PieChart, Pie } from 'recharts'
import { api } from '../services/api'
import { useToast } from '../hooks/useToast'

const COLORS = ['#e8734a', '#4a9ee8', '#4ae87a', '#e8d44a', '#a74ae8', '#e84a5a', '#4ae8d4', '#e8a44a']
const tooltipStyle = {
  contentStyle: { background: '#19191f', border: '1px solid #2a2a36', borderRadius: 10, color: '#e8e8ec', fontSize: 12 },
}

const AUTO_REFRESH_MS = 5 * 60 * 1000

const PRESETS = [
  { id: 'p_total',  type: 'metric', title: 'Total Students',       soql: 'SELECT COUNT() FROM Student__c', chartType: 'auto' },
  { id: 'p_market', type: 'metric', title: 'Students In Market',   soql: "SELECT COUNT() FROM Student__c WHERE Student_Marketing_Status__c='In Market'", chartType: 'auto' },
  { id: 'p_jobs',   type: 'metric', title: 'Active Jobs',          soql: 'SELECT COUNT() FROM Job__c', chartType: 'auto' },
  { id: 'p_iv',     type: 'metric', title: 'Interviews This Month',soql: 'SELECT COUNT() FROM Interview__c WHERE CreatedDate = THIS_MONTH', chartType: 'auto' },
  { id: 'p_status', type: 'chart',  title: 'Students by Status',   soql: 'SELECT Student_Marketing_Status__c, COUNT(Id) cnt FROM Student__c GROUP BY Student_Marketing_Status__c ORDER BY COUNT(Id) DESC', chartType: 'pie' },
  { id: 'p_tech',   type: 'chart',  title: 'Students by Technology', soql: 'SELECT Student_Technology__c, COUNT(Id) cnt FROM Student__c GROUP BY Student_Technology__c ORDER BY COUNT(Id) DESC LIMIT 10', chartType: 'bar' },
  { id: 'p_subs',   type: 'chart',  title: 'Monthly Submissions',  soql: 'SELECT CALENDAR_MONTH(CreatedDate) mon, COUNT(Id) cnt FROM Submission__c WHERE CreatedDate = THIS_YEAR GROUP BY CALENDAR_MONTH(CreatedDate) ORDER BY CALENDAR_MONTH(CreatedDate)', chartType: 'bar' },
  { id: 'p_exp',    type: 'chart',  title: 'Expenses by BU',       soql: 'SELECT Business_Unit__c, SUM(Amount__c) total FROM Expense__c GROUP BY Business_Unit__c ORDER BY SUM(Amount__c) DESC', chartType: 'bar' },
]

const CHART_TYPES = [
  { value: 'auto', label: 'Auto' },
  { value: 'bar',  label: 'Bar' },
  { value: 'pie',  label: 'Pie' },
]
const WIDGET_TYPES = [
  { value: 'metric', label: 'Metric (single number)' },
  { value: 'chart',  label: 'Chart (bar/pie)' },
  { value: 'table',  label: 'Table (list)' },
]

function genId() { return `w_${Date.now()}_${Math.random().toString(36).slice(2, 8)}` }

function deriveChartData(records) {
  if (!records?.length) return []
  const keys = Object.keys(records[0]).filter(k => k !== 'attributes')
  const valKey = keys.find(k => k.startsWith('expr') || /^(cnt|count|sum|avg|total)$/i.test(k)) || keys[keys.length - 1]
  const labelKey = keys.find(k => k !== valKey) || keys[0]
  return records
    .map(r => ({ name: String(r[labelKey] ?? 'N/A').replace(/_/g,' ').replace(/__c$/,''), value: Number(r[valKey]) || 0 }))
    .filter(d => d.value > 0)
}

function WidgetBody({ widget }) {
  const [state, setState] = useState({ loading: true, err: null, data: null })

  const load = useCallback(async () => {
    setState(s => ({ ...s, loading: true }))
    try {
      const res = await api.runWidget(widget.soql)
      setState({ loading: false, err: null, data: res })
    } catch (e) {
      setState({ loading: false, err: e.message, data: null })
    }
  }, [widget.soql])

  useEffect(() => {
    load()
    const t = setInterval(load, AUTO_REFRESH_MS)
    return () => clearInterval(t)
  }, [load])

  if (state.loading) return <div className="widget-empty">Loading…</div>
  if (state.err) return <div className="widget-empty widget-error">{state.err}</div>
  if (!state.data) return <div className="widget-empty">No data</div>

  const records = state.data.records || []
  const total = state.data.totalSize ?? records.length

  if (widget.type === 'metric') {
    let value = total
    if (records.length === 1) {
      const r = records[0]
      const numKey = Object.keys(r).find(k => typeof r[k] === 'number' && k !== 'attributes')
      if (numKey) value = r[numKey]
    }
    return <div className="widget-metric">{Number(value).toLocaleString()}</div>
  }

  if (widget.type === 'table') {
    if (!records.length) return <div className="widget-empty">No records</div>
    const cols = Object.keys(records[0]).filter(k => k !== 'attributes').slice(0, 4)
    return (
      <div className="widget-table-wrap">
        <table className="widget-table">
          <thead><tr>{cols.map(c => <th key={c}>{c.replace(/__c$/,'').replace(/_/g,' ')}</th>)}</tr></thead>
          <tbody>
            {records.slice(0, 8).map((r, i) => (
              <tr key={i}>{cols.map(c => <td key={c} title={String(r[c] ?? '')}>{String(r[c] ?? '—')}</td>)}</tr>
            ))}
          </tbody>
        </table>
      </div>
    )
  }

  // chart
  const data = deriveChartData(records)
  if (!data.length) return <div className="widget-empty">No chart data</div>
  const chartType = widget.chartType === 'auto' ? (data.length <= 6 ? 'pie' : 'bar') : widget.chartType
  if (chartType === 'pie') {
    return (
      <ResponsiveContainer width="100%" height={200}>
        <PieChart>
          <Pie data={data} cx="50%" cy="50%" outerRadius={75} innerRadius={38} dataKey="value"
               label={({ name, percent }) => `${name} ${(percent*100).toFixed(0)}%`}>
            {data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
          </Pie>
          <Tooltip {...tooltipStyle} />
        </PieChart>
      </ResponsiveContainer>
    )
  }
  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart data={data} layout="vertical" margin={{ left: 10, right: 10 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#2a2a36" horizontal={false} />
        <XAxis type="number" tick={{ fill: '#9898a8', fontSize: 10 }} />
        <YAxis type="category" dataKey="name" width={110} tick={{ fill: '#e8e8ec', fontSize: 11 }} axisLine={false} tickLine={false} />
        <Tooltip {...tooltipStyle} />
        <Bar dataKey="value" radius={[0, 4, 4, 0]} barSize={18}>
          {data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

function AddWidgetModal({ onAdd, onClose }) {
  const [tab, setTab] = useState('preset')
  const [form, setForm] = useState({ type: 'metric', title: '', soql: '', chartType: 'auto' })

  const submit = () => {
    if (!form.title.trim() || !form.soql.trim()) return
    onAdd({ id: genId(), ...form, position: Date.now() })
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal dashboard-modal" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Add Widget</h3>
          <button className="modal-close" onClick={onClose}>×</button>
        </div>
        <div className="modal-body">
          <div className="tab-row">
            <button className={`tab ${tab === 'preset' ? 'active' : ''}`} onClick={() => setTab('preset')}>Presets</button>
            <button className={`tab ${tab === 'custom' ? 'active' : ''}`} onClick={() => setTab('custom')}>Custom</button>
          </div>

          {tab === 'preset' ? (
            <div className="preset-grid">
              {PRESETS.map(p => (
                <button key={p.id} className="preset-card" onClick={() => onAdd({ ...p, id: genId(), position: Date.now() })}>
                  <div className="preset-type">{p.type === 'metric' ? 'METRIC' : 'CHART'}</div>
                  <div className="preset-title">{p.title}</div>
                </button>
              ))}
            </div>
          ) : (
            <div className="custom-form">
              <label>
                <span>Title</span>
                <input type="text" value={form.title} onChange={e => setForm(f => ({ ...f, title: e.target.value }))} placeholder="e.g., Students by Status" />
              </label>
              <label>
                <span>Type</span>
                <select value={form.type} onChange={e => setForm(f => ({ ...f, type: e.target.value }))}>
                  {WIDGET_TYPES.map(w => <option key={w.value} value={w.value}>{w.label}</option>)}
                </select>
              </label>
              {form.type === 'chart' && (
                <label>
                  <span>Chart type</span>
                  <select value={form.chartType} onChange={e => setForm(f => ({ ...f, chartType: e.target.value }))}>
                    {CHART_TYPES.map(c => <option key={c.value} value={c.value}>{c.label}</option>)}
                  </select>
                </label>
              )}
              <label>
                <span>SOQL</span>
                <textarea rows={4} value={form.soql} onChange={e => setForm(f => ({ ...f, soql: e.target.value }))} placeholder="SELECT COUNT() FROM Student__c" />
              </label>
              <div className="modal-footer">
                <button className="btn-secondary" onClick={onClose}>Cancel</button>
                <button className="btn-primary" onClick={submit} disabled={!form.title.trim() || !form.soql.trim()}>Add Widget</button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export default function Dashboard({ onAsk }) {
  const toast = useToast()
  const [builtIn, setBuiltIn] = useState(null)
  const [loadingBuiltIn, setLoadingBuiltIn] = useState(true)
  const [config, setConfig] = useState({ widgets: [] })
  const [editMode, setEditMode] = useState(false)
  const [showAdd, setShowAdd] = useState(false)

  useEffect(() => {
    api.dashboard().then(setBuiltIn).catch(console.error).finally(() => setLoadingBuiltIn(false))
    api.getDashboardConfig().then(setConfig).catch(err => {
      console.warn('Dashboard config load failed', err)
    })
  }, [])

  const persist = async (widgets) => {
    try {
      await api.saveDashboardConfig(widgets)
      setConfig({ widgets })
    } catch (e) {
      toast.error(`Save failed: ${e.message}`)
    }
  }

  const addWidget = async (w) => {
    const widgets = [...config.widgets, w]
    setShowAdd(false)
    await persist(widgets)
    toast.success('Widget added')
  }

  const removeWidget = async (id) => {
    await persist(config.widgets.filter(w => w.id !== id))
  }

  const moveWidget = async (id, dir) => {
    const idx = config.widgets.findIndex(w => w.id === id)
    if (idx === -1) return
    const next = [...config.widgets]
    const target = idx + dir
    if (target < 0 || target >= next.length) return
    ;[next[idx], next[target]] = [next[target], next[idx]]
    await persist(next)
  }

  return (
    <div className="dashboard">
      <div className="dashboard-toolbar">
        <div className="dashboard-title">Dashboard</div>
        <div className="dashboard-actions">
          {editMode && (
            <button className="btn-primary" onClick={() => setShowAdd(true)}>+ Add Widget</button>
          )}
          <button className="btn-secondary" onClick={() => setEditMode(m => !m)}>
            {editMode ? 'Done' : 'Edit Dashboard'}
          </button>
        </div>
      </div>

      {/* Built-in summary cards */}
      {loadingBuiltIn ? (
        <div className="widget-empty">Loading built-in stats…</div>
      ) : builtIn && (
        <div className="dashboard-grid">
          {[
            { label: 'Total Students', value: builtIn.total_students, query: 'How many students do I have?' },
            { label: 'In Market', value: builtIn.students_in_market, query: 'List students in market', color: '#4a9ee8' },
            { label: 'Verbal Confirmations', value: builtIn.verbal_confirmations, query: 'List students with verbal confirmation', color: '#4ae87a' },
            { label: 'Project Started', value: builtIn.project_started, query: 'List students with project started', color: '#e8d44a' },
            { label: 'Exits', value: builtIn.exits, query: 'How many students have exited?', color: '#e84a5a' },
            { label: 'Accounts', value: builtIn.total_accounts, query: 'How many accounts are there?' },
            { label: 'Contacts', value: builtIn.total_contacts, query: 'How many contacts are there?' },
          ].map(c => (
            <div key={c.label} className="stat-card" onClick={() => onAsk?.(c.query)}>
              <div className="stat-value" style={c.color ? { color: c.color } : {}}>
                {typeof c.value === 'number' ? c.value.toLocaleString() : c.value ?? '—'}
              </div>
              <div className="stat-label">{c.label}</div>
            </div>
          ))}
        </div>
      )}

      {/* Custom widgets */}
      {config.widgets.length > 0 && (
        <div className="custom-widgets">
          <div className="custom-widgets-label">My Widgets</div>
          <div className="custom-widget-grid">
            {config.widgets.map((w, i) => (
              <div key={w.id} className={`custom-widget ${editMode ? 'edit-mode' : ''}`}>
                <div className="custom-widget-header">
                  <span className="custom-widget-title">{w.title}</span>
                  {editMode && (
                    <div className="custom-widget-controls">
                      <button onClick={() => moveWidget(w.id, -1)} disabled={i === 0} title="Move up">↑</button>
                      <button onClick={() => moveWidget(w.id, 1)} disabled={i === config.widgets.length - 1} title="Move down">↓</button>
                      <button onClick={() => removeWidget(w.id)} title="Delete" className="del">×</button>
                    </div>
                  )}
                </div>
                <WidgetBody widget={w} />
              </div>
            ))}
          </div>
        </div>
      )}

      {editMode && config.widgets.length === 0 && (
        <div className="custom-widgets-empty">
          <p>No custom widgets yet. Click <b>+ Add Widget</b> to get started.</p>
        </div>
      )}

      {showAdd && <AddWidgetModal onAdd={addWidget} onClose={() => setShowAdd(false)} />}
    </div>
  )
}
