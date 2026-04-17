import { useState, useEffect, useCallback } from 'react'
import { api } from '../services/api'
import { useToast } from '../hooks/useToast'
import DataChart from './DataChart'
import DataTable from './DataTable'

const SUGGESTIONS = [
  'Overview of all students by marketing status',
  'Account distribution and contact summary',
  'Top 10 accounts by number of contacts',
  'Monthly student trends and pipeline analysis',
  'Compare students in market vs verbal confirmations',
]

function MetricCard({ card }) {
  const val = card.totalSize ?? card.records?.length ?? 0
  return (
    <div className="analytics-metric-card">
      <div className="analytics-metric-value">{val.toLocaleString()}</div>
      <div className="analytics-metric-title">{card.title}</div>
      <div className="analytics-metric-desc">{card.description}</div>
    </div>
  )
}

function ChartCard({ card }) {
  const records = card.records || []
  if (records.length === 0) {
    return (
      <div className="analytics-chart-card">
        <h4 className="analytics-card-title">{card.title}</h4>
        <p className="analytics-card-desc">{card.description}</p>
        {card.error ? (
          <div className="analytics-card-error">{card.error}</div>
        ) : (
          <div className="analytics-card-empty">No data</div>
        )}
      </div>
    )
  }

  return (
    <div className="analytics-chart-card">
      <h4 className="analytics-card-title">{card.title}</h4>
      <p className="analytics-card-desc">{card.description}</p>
      {card.chartType === 'table' ? (
        <DataTable records={records} />
      ) : (
        <DataChart records={records} />
      )}
      {card.soql && (
        <div className="analytics-card-soql">{card.soql}</div>
      )}
    </div>
  )
}

function InsightPanel({ insight }) {
  if (!insight) return null
  const html = insight
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/^- (.+)$/gm, '<li>$1</li>')
    .replace(/(<li>.*<\/li>)/s, '<ul>$1</ul>')
    .replace(/\n/g, '<br/>')
  return (
    <div className="analytics-insight-panel">
      <h4 className="analytics-insight-title">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>
        AI Insights
      </h4>
      <div className="analytics-insight-body" dangerouslySetInnerHTML={{ __html: html }} />
    </div>
  )
}

export default function AnalyticsPage() {
  const toast = useToast()
  const [prompt, setPrompt] = useState('')
  const [cards, setCards] = useState([])
  const [insight, setInsight] = useState('')
  const [loading, setLoading] = useState(false)
  const [insightLoading, setInsightLoading] = useState(false)
  const [lastPrompt, setLastPrompt] = useState('')
  const [providers, setProviders] = useState([])
  const [provider, setProvider] = useState('')

  // Load available AI providers
  useEffect(() => {
    api.aiProviders()
      .then(d => {
        setProviders(d.providers || [])
        if (!provider && d.providers?.length) setProvider(d.providers[0].id)
      })
      .catch(() => {})
  }, [])

  const generate = useCallback(async (text) => {
    const q = (text || prompt).trim()
    if (!q) return
    setLoading(true)
    setCards([])
    setInsight('')
    setLastPrompt(q)
    try {
      const res = await api.analyticsGenerate(q, provider || undefined)
      setCards(res.cards || [])

      // Auto-generate insight
      setInsightLoading(true)
      try {
        const ins = await api.analyticsInsight(q, res.cards || [], provider || undefined)
        setInsight(ins.insight || '')
      } catch (e) {
        console.error('Insight error:', e)
      } finally {
        setInsightLoading(false)
      }
    } catch (e) {
      toast.error(e.message)
    } finally {
      setLoading(false)
    }
  }, [prompt, provider, toast])

  const handleSubmit = (e) => {
    e.preventDefault()
    generate()
  }

  const handleSuggestion = (s) => {
    setPrompt(s)
    generate(s)
  }

  return (
    <div className="analytics-page">
      <div className="analytics-header">
        <div>
          <h2 className="analytics-title">AI Analytics</h2>
          <div className="analytics-subtitle">
            Ask anything about your Salesforce data — Claude generates live charts and insights.
          </div>
        </div>
      </div>

      {/* Search bar */}
      <form className="analytics-search" onSubmit={handleSubmit}>
        <div className="analytics-search-row">
          <input
            className="input-field analytics-search-input"
            value={prompt}
            onChange={e => setPrompt(e.target.value)}
            placeholder="e.g. Show me student distribution by marketing status with trends..."
            disabled={loading}
          />
          {providers.length > 0 && (
            <select
              className="input-field analytics-provider-select"
              value={provider}
              onChange={e => setProvider(e.target.value)}
              disabled={loading}
              title="AI Provider"
            >
              {providers.map(p => (
                <option key={p.id} value={p.id}>{p.name} ({p.model})</option>
              ))}
            </select>
          )}
          <button className="btn-primary" type="submit" disabled={loading || !prompt.trim()}>
            {loading ? 'Analyzing...' : 'Generate'}
          </button>
        </div>
      </form>

      {/* Suggestions (show when no cards) */}
      {cards.length === 0 && !loading && (
        <div className="analytics-suggestions">
          <div className="analytics-suggestions-label">Try these:</div>
          <div className="analytics-suggestions-list">
            {SUGGESTIONS.map((s, i) => (
              <button
                key={i}
                className="analytics-suggestion-chip"
                onClick={() => handleSuggestion(s)}
              >
                {s}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Loading */}
      {loading && (
        <div className="analytics-loading">
          <div className="analytics-spinner" />
          <div>Analyzing your Salesforce data...</div>
        </div>
      )}

      {/* Results */}
      {cards.length > 0 && (
        <>
          {/* Metric cards row */}
          {cards.filter(c => c.chartType === 'metric').length > 0 && (
            <div className="analytics-metrics-row">
              {cards.filter(c => c.chartType === 'metric').map((c, i) => (
                <MetricCard key={i} card={c} />
              ))}
            </div>
          )}

          {/* Chart cards grid */}
          <div className="analytics-cards-grid">
            {cards.filter(c => c.chartType !== 'metric').map((c, i) => (
              <ChartCard key={i} card={c} />
            ))}
          </div>

          {/* AI Insight */}
          {insightLoading ? (
            <div className="analytics-loading" style={{ padding: '20px 24px' }}>
              <div className="analytics-spinner" />
              <div>Generating insights...</div>
            </div>
          ) : (
            <InsightPanel insight={insight} />
          )}

          {/* Regenerate */}
          <div className="analytics-actions">
            <button className="btn-small" onClick={() => generate(lastPrompt)} disabled={loading}>
              Regenerate
            </button>
          </div>
        </>
      )}
    </div>
  )
}
