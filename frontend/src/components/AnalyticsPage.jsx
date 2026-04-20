import { useState, useEffect } from 'react'
import { api } from '../services/api'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, CartesianGrid, LineChart, Line, Legend,
} from 'recharts'

const COLORS = ['#2F5486', '#4a9ee8', '#4ae87a', '#e8d44a', '#a74ae8', '#e84a8a', '#4ae8d4', '#e8a44a', '#7ae84a', '#4a74e8']
const FUNNEL_COLORS = ['#4a9ee8', '#4ae87a', '#e8d44a', '#2F5486', '#a74ae8', '#e84a8a']

const tooltipStyle = {
  contentStyle: { background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: 8, color: 'var(--text-primary)', fontSize: 12 },
  itemStyle: { color: 'var(--text-primary)' },
}

function MetricCard({ card, onDrill }) {
  const main = card.metric || {}
  return (
    <div className="analytics-metric-card">
      <div className="analytics-metric-value">{main.value}</div>
      <div className="analytics-metric-label">{main.label}</div>
      <div className="analytics-metric-details">
        {card.data.map((d, i) => (
          <div key={i} className="analytics-metric-detail analytics-clickable" onClick={() => d.drilldown && onDrill(d.drilldown)}>
            <span className="analytics-dot" style={{ background: d.color || COLORS[i] }} />
            <span>{d.label}</span>
            <strong>{d.value?.toLocaleString()}</strong>
          </div>
        ))}
      </div>
    </div>
  )
}

function FunnelChart({ data, onDrill }) {
  const max = Math.max(...data.map(d => d.count))
  return (
    <div className="analytics-funnel">
      {data.map((d, i) => {
        const pct = max > 0 ? (d.count / max * 100) : 0
        return (
          <div key={i} className="analytics-funnel-row analytics-clickable" onClick={() => d.drilldown && onDrill(d.drilldown)}>
            <div className="analytics-funnel-label">{d.stage}</div>
            <div className="analytics-funnel-bar-wrap">
              <div
                className="analytics-funnel-bar"
                style={{ width: `${Math.max(pct, 3)}%`, background: FUNNEL_COLORS[i % FUNNEL_COLORS.length] }}
              />
              <span className="analytics-funnel-count">{d.count.toLocaleString()}</span>
            </div>
          </div>
        )
      })}
    </div>
  )
}

function BarCard({ data, onDrill }) {
  const handleClick = (entry) => {
    if (entry?.drilldown) onDrill(entry.drilldown)
  }
  return (
    <ResponsiveContainer width="100%" height={Math.max(220, data.length * 36)}>
      <BarChart data={data} layout="vertical" margin={{ left: 10, right: 20, top: 5, bottom: 5 }}
        onClick={(e) => e?.activePayload?.[0]?.payload && handleClick(e.activePayload[0].payload)}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" horizontal={false} />
        <XAxis type="number" tick={{ fill: 'var(--text-muted)', fontSize: 11 }} axisLine={{ stroke: 'var(--border)' }} />
        <YAxis type="category" dataKey="name" width={130} tick={{ fill: 'var(--text-primary)', fontSize: 11 }} axisLine={false} tickLine={false} />
        <Tooltip {...tooltipStyle} formatter={v => [v.toLocaleString(), 'Count']} />
        <Bar dataKey="value" radius={[0, 5, 5, 0]} barSize={20} style={{ cursor: 'pointer' }}>
          {data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

function PieCard({ data, onDrill }) {
  const handleClick = (_, idx) => {
    if (data[idx]?.drilldown) onDrill(data[idx].drilldown)
  }
  return (
    <ResponsiveContainer width="100%" height={300}>
      <PieChart>
        <Pie data={data} cx="50%" cy="50%" outerRadius={100} innerRadius={50} dataKey="value"
          label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
          labelLine={{ stroke: 'var(--text-muted)', strokeWidth: 0.5 }} stroke="none"
          onClick={handleClick} style={{ cursor: 'pointer' }}>
          {data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
        </Pie>
        <Tooltip {...tooltipStyle} formatter={v => [v.toLocaleString(), 'Count']} />
      </PieChart>
    </ResponsiveContainer>
  )
}

function LineCard({ card, onDrill }) {
  const data = card.data || []
  const keys = Object.keys(data[0] || {}).filter(k => k !== 'month' && k !== 'drilldown' && k !== 'predicted')
  const mainKey = keys[0]

  const handleClick = (e) => {
    if (e?.activePayload?.[0]?.payload?.drilldown) {
      onDrill(e.activePayload[0].payload.drilldown)
    }
  }

  return (
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={data} margin={{ left: 10, right: 20, top: 10, bottom: 5 }} onClick={handleClick}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
        <XAxis dataKey="month" tick={{ fill: 'var(--text-muted)', fontSize: 11 }} axisLine={{ stroke: 'var(--border)' }} />
        <YAxis tick={{ fill: 'var(--text-muted)', fontSize: 11 }} axisLine={{ stroke: 'var(--border)' }} />
        <Tooltip {...tooltipStyle} />
        <Legend wrapperStyle={{ fontSize: 12 }} />
        {mainKey && (
          <Line type="monotone" dataKey={mainKey} stroke="#4a9ee8" strokeWidth={2}
            dot={{ r: 4, fill: '#4a9ee8', cursor: 'pointer' }} connectNulls={false}
            name={mainKey.charAt(0).toUpperCase() + mainKey.slice(1)} />
        )}
        <Line type="monotone" dataKey="predicted" stroke="#2F5486" strokeWidth={2} strokeDasharray="6 3"
          dot={{ r: 5, fill: '#2F5486', strokeWidth: 2, stroke: '#fff' }} name="Predicted (Next Month)" />
      </LineChart>
    </ResponsiveContainer>
  )
}

function TableCard({ data, onDrill }) {
  if (!data?.length) return null
  const cols = Object.keys(data[0]).filter(k => k !== 'drilldown')
  return (
    <div className="analytics-table-scroll">
      <table className="data-table">
        <thead>
          <tr>
            {cols.map(c => (
              <th key={c}>{c.replace(/([A-Z])/g, ' $1').replace(/^./, s => s.toUpperCase())}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((r, i) => (
            <tr key={i} className="analytics-clickable" onClick={() => r.drilldown && onDrill(r.drilldown)}>
              {cols.map(c => (
                <td key={c}>{typeof r[c] === 'number' ? r[c].toLocaleString() : r[c] ?? '\u2014'}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function AnalyticsCard({ card, onDrill }) {
  return (
    <div className={`analytics-card ${card.chartType === 'line' ? 'analytics-card-wide' : ''}`}>
      <div className="analytics-card-header">
        <h3 className="analytics-card-title">{card.title}</h3>
        {card.metric && card.chartType !== 'metric' && (
          <span className="analytics-card-badge">{card.metric.label}: {card.metric.value}</span>
        )}
      </div>
      <p className="analytics-card-desc">{card.description}</p>
      <div className="analytics-card-chart">
        {card.chartType === 'funnel' && <FunnelChart data={card.data} onDrill={onDrill} />}
        {card.chartType === 'bar' && <BarCard data={card.data} onDrill={onDrill} />}
        {card.chartType === 'pie' && <PieCard data={card.data} onDrill={onDrill} />}
        {card.chartType === 'line' && <LineCard card={card} onDrill={onDrill} />}
        {card.chartType === 'metric' && <MetricCard card={card} onDrill={onDrill} />}
        {card.chartType === 'table' && <TableCard data={card.data} onDrill={onDrill} />}
      </div>
      <div className="analytics-card-hint">Click any item to drill down</div>
    </div>
  )
}

export default function AnalyticsPage({ onDrillDown }) {
  const [cards, setCards] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [generatedAt, setGeneratedAt] = useState('')

  const load = async () => {
    setLoading(true)
    setError('')
    try {
      const res = await api.predictiveAnalytics()
      setCards(res.cards || [])
      setGeneratedAt(res.generated_at || '')
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const handleDrill = (question) => {
    if (onDrillDown) onDrillDown(question)
  }

  return (
    <div className="analytics-page">
      <div className="analytics-header">
        <div>
          <h2 className="analytics-title">AI Analytics</h2>
          <p className="analytics-subtitle">
            Predictive insights from your Salesforce data
            {generatedAt && <span className="analytics-time"> — {new Date(generatedAt).toLocaleString()}</span>}
          </p>
        </div>
        <button className="btn-primary" onClick={load} disabled={loading}>
          {loading ? 'Loading...' : 'Refresh'}
        </button>
      </div>

      {error && <div className="analytics-error">{error}</div>}

      {loading ? (
        <div className="analytics-loading">
          <span className="thinking-asterisk">*</span>
          <span>Analyzing your Salesforce data...</span>
        </div>
      ) : (
        <div className="analytics-grid">
          {cards.map(card => (
            <AnalyticsCard key={card.id} card={card} onDrill={handleDrill} />
          ))}
        </div>
      )}
    </div>
  )
}
