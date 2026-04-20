import { useState } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell } from 'recharts'
import { api } from '../services/api'

const tooltipStyle = {
  contentStyle: { background: '#19191f', border: '1px solid #2a2a36', borderRadius: 10, color: '#e8e8ec', fontSize: 12 },
}
const COLOR_A = '#4a9ee8'
const COLOR_B = '#2F5486'

function asChartData(result) {
  const records = result?.records || []
  if (!records.length) return []
  const r = records[0]
  const keys = Object.keys(r).filter(k => k !== 'attributes')
  const valKey = keys.find(k => k.startsWith('expr') || /^(cnt|count|sum|avg|total)$/i.test(k)) || keys[keys.length - 1]
  if (records.length === 1) {
    return [{ name: 'Total', value: Number(r[valKey]) || result.totalSize || 0 }]
  }
  const labelKey = keys.find(k => k !== valKey) || keys[0]
  return records.map(row => ({
    name: String(row[labelKey] ?? 'N/A').replace(/_/g,' ').replace(/__c$/,''),
    value: Number(row[valKey]) || 0,
  }))
}

function pickSingleValue(result) {
  const records = result?.records || []
  if (!records.length) return result?.totalSize || 0
  const r = records[0]
  for (const [k, v] of Object.entries(r)) {
    if (k === 'attributes') continue
    if (typeof v === 'number') return v
  }
  return result?.totalSize || records.length
}

function ChartBlock({ label, result, color }) {
  const data = asChartData(result)
  return (
    <div className="compare-panel">
      <div className="compare-panel-label">{label}</div>
      <div className="compare-panel-value">{pickSingleValue(result).toLocaleString()}</div>
      {data.length > 1 && (
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={data} margin={{ left: 10, right: 10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2a36" vertical={false} />
            <XAxis dataKey="name" tick={{ fill: '#9898a8', fontSize: 10 }} />
            <YAxis tick={{ fill: '#9898a8', fontSize: 10 }} />
            <Tooltip {...tooltipStyle} />
            <Bar dataKey="value" radius={[4, 4, 0, 0]}>
              {data.map((_, i) => <Cell key={i} fill={color} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}

export default function ComparisonPage() {
  const [question, setQuestion] = useState('')
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState('')
  const [data, setData] = useState(null)

  const submit = async (e) => {
    e?.preventDefault?.()
    if (!question.trim() || loading) return
    setLoading(true); setErr(''); setData(null)
    try {
      const res = await api.compare({ question: question.trim() })
      setData(res)
    } catch (ex) {
      setErr(ex.message)
    } finally {
      setLoading(false)
    }
  }

  const diff = data ? data.diff : 0
  const pct = data?.pct_change
  const positive = diff > 0
  const negative = diff < 0

  return (
    <div className="compare-page">
      <div className="compare-header">
        <h2 className="compare-title">Comparison Mode</h2>
        <p className="compare-subtitle">Compare metrics across two periods or groups</p>
      </div>

      <form className="compare-form" onSubmit={submit}>
        <input
          type="text" value={question}
          onChange={e => setQuestion(e.target.value)}
          placeholder='e.g., "Compare submissions this month vs last month"'
          disabled={loading}
        />
        <button type="submit" className="btn-primary" disabled={loading || !question.trim()}>
          {loading ? 'Comparing…' : 'Compare'}
        </button>
      </form>

      {err && <div className="compare-error">{err}</div>}

      {data && (
        <>
          <div className="compare-summary">
            <span className={`compare-diff ${positive ? 'pos' : ''} ${negative ? 'neg' : ''}`}>
              {positive ? '↑' : negative ? '↓' : '='}
              {' '}{Math.abs(diff).toLocaleString()}
              {pct != null && ` (${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%)`}
            </span>
            <span className="compare-summary-label">{data.label2} vs {data.label1}</span>
          </div>

          <div className="compare-grid">
            <ChartBlock label={data.label1} result={data.result1} color={COLOR_A} />
            <ChartBlock label={data.label2} result={data.result2} color={COLOR_B} />
          </div>

          <div className="compare-table-wrap">
            <table className="compare-table">
              <thead>
                <tr><th>Metric</th><th>{data.label1}</th><th>{data.label2}</th><th>Change</th><th>%</th></tr>
              </thead>
              <tbody>
                <tr>
                  <td>Total</td>
                  <td>{Number(data.value1).toLocaleString()}</td>
                  <td>{Number(data.value2).toLocaleString()}</td>
                  <td className={positive ? 'pos' : negative ? 'neg' : ''}>
                    {positive ? '+' : ''}{Number(data.diff).toLocaleString()}
                  </td>
                  <td className={positive ? 'pos' : negative ? 'neg' : ''}>
                    {pct != null ? `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%` : '—'}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <details className="compare-queries">
            <summary>Generated SOQL</summary>
            <div><b>{data.label1}:</b> <code>{data.query1}</code></div>
            <div><b>{data.label2}:</b> <code>{data.query2}</code></div>
          </details>
        </>
      )}
    </div>
  )
}
