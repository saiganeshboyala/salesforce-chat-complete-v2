import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, CartesianGrid } from 'recharts'

const COLORS = ['#2F5486', '#4a9ee8', '#4ae87a', '#e8d44a', '#a74ae8', '#e84a8a', '#4ae8d4', '#e8a44a', '#7ae84a', '#4a74e8']

const tooltipStyle = {
  contentStyle: { background: '#19191f', border: '1px solid #2a2a36', borderRadius: 10, color: '#e8e8ec', fontSize: 12 },
  itemStyle: { color: '#e8e8ec' },
}

function prepare(records) {
  if (!records?.length) return null
  const keys = Object.keys(records[0]).filter(k => k !== 'attributes')
  const valKey = keys.find(k => k.startsWith('expr') || /cnt|count|sum|avg/i.test(k))
  if (!valKey) return null
  if (records.length === 1) return null
  const labelKey = keys.find(k => k !== valKey) || keys[0]
  const data = records
    .map(r => {
      let label = r[labelKey] ?? 'N/A'
      if (label && typeof label === 'object') label = label.Name || label.name || JSON.stringify(label)
      return { name: String(label).replace(/__c$/,'').replace(/_/g,' '), value: Number(r[valKey]) || 0 }
    })
    .filter(d => d.value > 0)
  if (!data.length) return null
  return { data, type: data.length <= 8 ? 'pie' : 'bar' }
}

export default function DataChart({ records, totalSize }) {
  const result = prepare(records)
  if (!result) return null
  const { data, type } = result

  return (
    <div className="chart-container">
      <div className="chart-header">
        <span className="chart-label">{totalSize ? `${totalSize.toLocaleString()} results` : `${data.length} categories`}</span>
      </div>
      {type === 'bar' ? (
        <ResponsiveContainer width="100%" height={Math.max(220, data.length * 38)}>
          <BarChart data={data} layout="vertical" margin={{ left: 10, right: 20, top: 5, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2a36" horizontal={false} />
            <XAxis type="number" tick={{ fill: '#9898a8', fontSize: 11 }} axisLine={{ stroke: '#2a2a36' }} />
            <YAxis type="category" dataKey="name" width={150} tick={{ fill: '#e8e8ec', fontSize: 12 }} axisLine={false} tickLine={false} />
            <Tooltip {...tooltipStyle} formatter={v => [v.toLocaleString(), 'Count']} />
            <Bar dataKey="value" radius={[0, 5, 5, 0]} barSize={22}>
              {data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      ) : (
        <ResponsiveContainer width="100%" height={300}>
          <PieChart>
            <Pie data={data} cx="50%" cy="50%" outerRadius={110} innerRadius={55} dataKey="value"
              label={({ name, percent }) => `${name} ${(percent*100).toFixed(0)}%`}
              labelLine={{ stroke: '#5a5a6e', strokeWidth: 0.5 }} stroke="none">
              {data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
            </Pie>
            <Tooltip {...tooltipStyle} formatter={v => [v.toLocaleString(), 'Count']} />
          </PieChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
