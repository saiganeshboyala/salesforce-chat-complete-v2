import { useEffect, useMemo, useRef, useState, useCallback } from 'react'
import { api } from '../services/api'

const VIEW_W = 1200
const VIEW_H = 820

function nodeRadius(recordCount, maxCount, base = 28) {
  if (!recordCount || !maxCount) return base
  const ratio = Math.log10(recordCount + 1) / Math.log10(maxCount + 1)
  return base + ratio * 22
}

function nodeFill(recordCount, maxCount) {
  const ratio = maxCount ? Math.log10(recordCount + 1) / Math.log10(maxCount + 1) : 0
  const alpha = 0.35 + ratio * 0.55
  return `rgba(232, 115, 74, ${alpha.toFixed(3)})`
}

function shortLabel(label, max = 18) {
  if (!label) return ''
  return label.length > max ? label.slice(0, max - 1) + '…' : label
}

// Layout: focused node at center, neighbors in a ring around it.
function computeFocusedLayout(focusId, data) {
  if (!data || !focusId) return null
  const center = data.nodes.find(n => n.id === focusId)
  if (!center) return null

  const neighborMap = new Map()
  for (const e of data.edges) {
    if (e.from === focusId && e.to !== focusId) {
      if (!neighborMap.has(e.to)) neighborMap.set(e.to, { dir: 'out', edges: [] })
      neighborMap.get(e.to).edges.push({ ...e, outgoing: true })
    } else if (e.to === focusId && e.from !== focusId) {
      if (!neighborMap.has(e.from)) neighborMap.set(e.from, { dir: 'in', edges: [] })
      neighborMap.get(e.from).edges.push({ ...e, outgoing: false })
    }
  }

  const neighborIds = Array.from(neighborMap.keys())
  const ringCount = neighborIds.length
  const cx = VIEW_W / 2
  const cy = VIEW_H / 2

  const positions = { [focusId]: { x: cx, y: cy, r: 0 } }

  // Distribute neighbors across up to 2 concentric rings when there are many
  const ring1Cap = 16
  const innerR = Math.min(VIEW_W, VIEW_H) * 0.26
  const outerR = innerR + 150

  neighborIds.forEach((id, i) => {
    const onOuter = i >= ring1Cap
    const ringSize = onOuter ? (ringCount - ring1Cap) : Math.min(ringCount, ring1Cap)
    const idx = onOuter ? (i - ring1Cap) : i
    const r = onOuter ? outerR : innerR
    const angleOffset = onOuter ? Math.PI / ringSize : 0
    const angle = (idx / ringSize) * 2 * Math.PI + angleOffset - Math.PI / 2
    positions[id] = {
      x: cx + r * Math.cos(angle),
      y: cy + r * Math.sin(angle),
      r,
    }
  })

  const neighborList = neighborIds.map(id => {
    const node = data.nodes.find(n => n.id === id)
    return { node, meta: neighborMap.get(id) }
  }).filter(x => x.node)

  return { center, neighborList, positions, neighborMap }
}

export default function SchemaMap() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState('')
  const [filter, setFilter] = useState('')
  const [focusId, setFocusId] = useState(null)
  const [selected, setSelected] = useState(null)
  const [overrides, setOverrides] = useState({}) // manual node positions
  const [pan, setPan] = useState({ x: 0, y: 0 })
  const [zoom, setZoom] = useState(1)
  const svgRef = useRef(null)
  const dragState = useRef(null)

  useEffect(() => {
    setLoading(true)
    api.schemaRelationships()
      .then(d => {
        setData(d)
        // Default focus = biggest object by record count
        const nodes = d?.nodes || []
        if (nodes.length) {
          const biggest = nodes.reduce((a, b) => ((b.record_count || 0) > (a.record_count || 0) ? b : a))
          setFocusId(biggest.id)
        }
      })
      .catch(e => setErr(e.message))
      .finally(() => setLoading(false))
  }, [])

  const maxCount = useMemo(
    () => (data?.nodes.reduce((m, n) => Math.max(m, n.record_count || 0), 0) || 1),
    [data]
  )

  const sidebarList = useMemo(() => {
    if (!data) return []
    const q = filter.trim().toLowerCase()
    let list = data.nodes
    if (q) list = list.filter(n => n.id.toLowerCase().includes(q) || (n.label || '').toLowerCase().includes(q))
    return [...list].sort((a, b) => (b.record_count || 0) - (a.record_count || 0))
  }, [data, filter])

  const layout = useMemo(() => computeFocusedLayout(focusId, data), [focusId, data])

  // Effective positions = layout + any user drag overrides, reset when focus changes
  const positions = useMemo(() => {
    if (!layout) return {}
    const out = {}
    for (const [id, p] of Object.entries(layout.positions)) {
      out[id] = overrides[id] || { x: p.x, y: p.y }
    }
    return out
  }, [layout, overrides])

  // Reset overrides and viewport when focus changes
  useEffect(() => {
    setOverrides({})
    setPan({ x: 0, y: 0 })
    setZoom(1)
    setSelected(null)
  }, [focusId])

  // Screen → SVG coordinates
  const screenToSvg = useCallback((clientX, clientY) => {
    const svg = svgRef.current
    if (!svg) return { x: 0, y: 0 }
    const rect = svg.getBoundingClientRect()
    // Account for current viewBox
    const vbW = VIEW_W / zoom
    const vbH = VIEW_H / zoom
    const vbX = (VIEW_W - vbW) / 2 - pan.x
    const vbY = (VIEW_H - vbH) / 2 - pan.y
    const x = vbX + ((clientX - rect.left) / rect.width) * vbW
    const y = vbY + ((clientY - rect.top) / rect.height) * vbH
    return { x, y }
  }, [zoom, pan])

  const onNodeMouseDown = (e, id) => {
    e.stopPropagation()
    const { x, y } = screenToSvg(e.clientX, e.clientY)
    const p = positions[id]
    dragState.current = { type: 'node', id, ox: x - p.x, oy: y - p.y, moved: false }
  }

  const onCanvasMouseDown = (e) => {
    dragState.current = { type: 'pan', startX: e.clientX, startY: e.clientY, pan: { ...pan }, moved: false }
  }

  const onMouseMove = useCallback((e) => {
    const ds = dragState.current
    if (!ds) return
    ds.moved = true
    if (ds.type === 'node') {
      const { x, y } = screenToSvg(e.clientX, e.clientY)
      setOverrides(prev => ({ ...prev, [ds.id]: { x: x - ds.ox, y: y - ds.oy } }))
    } else if (ds.type === 'pan') {
      const svg = svgRef.current
      if (!svg) return
      const rect = svg.getBoundingClientRect()
      const scaleX = (VIEW_W / zoom) / rect.width
      const scaleY = (VIEW_H / zoom) / rect.height
      const dx = (e.clientX - ds.startX) * scaleX
      const dy = (e.clientY - ds.startY) * scaleY
      setPan({ x: ds.pan.x + dx, y: ds.pan.y + dy })
    }
  }, [screenToSvg, zoom])

  const onMouseUp = useCallback((e) => {
    const ds = dragState.current
    if (ds?.type === 'pan' && !ds.moved) {
      setSelected(null)
    }
    dragState.current = null
  }, [])

  useEffect(() => {
    window.addEventListener('mousemove', onMouseMove)
    window.addEventListener('mouseup', onMouseUp)
    return () => {
      window.removeEventListener('mousemove', onMouseMove)
      window.removeEventListener('mouseup', onMouseUp)
    }
  }, [onMouseMove, onMouseUp])

  const onWheel = (e) => {
    e.preventDefault()
    const factor = e.deltaY < 0 ? 1.15 : 1 / 1.15
    setZoom(z => Math.max(0.4, Math.min(3, z * factor)))
  }

  const viewBox = useMemo(() => {
    const vbW = VIEW_W / zoom
    const vbH = VIEW_H / zoom
    const vbX = (VIEW_W - vbW) / 2 - pan.x
    const vbY = (VIEW_H - vbH) / 2 - pan.y
    return `${vbX} ${vbY} ${vbW} ${vbH}`
  }, [zoom, pan])

  const resetView = () => { setOverrides({}); setPan({ x: 0, y: 0 }); setZoom(1) }

  if (loading) return <div className="schema-map-empty">Loading schema graph…</div>
  if (err) return <div className="schema-map-empty">Error: {err}</div>
  if (!data || data.nodes.length === 0) {
    return <div className="schema-map-empty">No schema loaded. Run <code>python -m scripts.refresh_schema</code>.</div>
  }

  const focusNode = layout?.center
  const selectedNode = selected && data.nodes.find(n => n.id === selected)
  const detailNode = selectedNode || focusNode
  const detailEdges = detailNode
    ? data.edges.filter(e => e.from === detailNode.id || e.to === detailNode.id)
    : []

  return (
    <div className="schema-map">
      <div className="schema-map-toolbar">
        <div>
          <h2 className="schema-map-title">Schema Explorer</h2>
          <div className="schema-map-subtitle">
            {data.nodes.length} objects · {data.edges.length} relationships
            {focusNode && ` · focused on ${focusNode.label}`}
          </div>
        </div>
        <div className="schema-map-controls">
          <button className="btn-secondary" onClick={() => setZoom(z => Math.min(3, z * 1.2))} title="Zoom in">+</button>
          <button className="btn-secondary" onClick={() => setZoom(z => Math.max(0.4, z / 1.2))} title="Zoom out">−</button>
          <button className="btn-secondary" onClick={resetView} title="Reset view">Reset</button>
        </div>
      </div>

      <div className="schema-map-body">
        {/* Left: object list */}
        <aside className="schema-map-list">
          <input
            type="text"
            className="input-field schema-map-filter"
            placeholder="Search objects…"
            value={filter}
            onChange={e => setFilter(e.target.value)}
          />
          <div className="schema-map-list-scroll">
            {sidebarList.map(n => (
              <div
                key={n.id}
                className={`schema-map-list-item ${focusId === n.id ? 'focused' : ''}`}
                onClick={() => setFocusId(n.id)}
              >
                <div className="list-item-name">{n.label}</div>
                <div className="list-item-meta">
                  {(n.record_count || 0).toLocaleString()} rec · {n.field_count} fields
                </div>
              </div>
            ))}
            {sidebarList.length === 0 && <div className="schema-map-empty-small">No matches.</div>}
          </div>
        </aside>

        {/* Canvas */}
        <div className="schema-map-canvas">
          {!focusNode && <div className="schema-map-empty-small">Select an object from the list to explore.</div>}
          {focusNode && layout && (
            <svg
              ref={svgRef}
              viewBox={viewBox}
              onMouseDown={onCanvasMouseDown}
              onWheel={onWheel}
              style={{ cursor: dragState.current?.type === 'pan' ? 'grabbing' : 'grab' }}
            >
              <defs>
                <marker id="arrow-out" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                  <path d="M 0 0 L 10 5 L 0 10 z" fill="#2F5486" />
                </marker>
                <marker id="arrow-in" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                  <path d="M 0 0 L 10 5 L 0 10 z" fill="#4a9ee8" />
                </marker>
                <radialGradient id="focus-glow" cx="50%" cy="50%" r="50%">
                  <stop offset="0%" stopColor="rgba(232,115,74,0.35)" />
                  <stop offset="100%" stopColor="rgba(232,115,74,0)" />
                </radialGradient>
              </defs>

              {/* Glow behind focus */}
              <circle
                cx={positions[focusNode.id]?.x}
                cy={positions[focusNode.id]?.y}
                r={180}
                fill="url(#focus-glow)"
                pointerEvents="none"
              />

              {/* Edges from focus to each neighbor */}
              {layout.neighborList.map(({ node, meta }) => {
                const a = positions[focusNode.id]
                const b = positions[node.id]
                if (!a || !b) return null
                const outgoing = meta.dir === 'out'
                const isSel = selected === node.id
                return (
                  <g key={`edge-${node.id}`}>
                    <line
                      x1={a.x} y1={a.y} x2={b.x} y2={b.y}
                      stroke={outgoing ? '#2F5486' : '#4a9ee8'}
                      strokeWidth={isSel ? 2.4 : 1.2}
                      strokeOpacity={isSel ? 1 : 0.55}
                      markerEnd={outgoing ? 'url(#arrow-out)' : 'url(#arrow-in)'}
                    />
                    {meta.edges.length > 1 && (
                      <text
                        x={(a.x + b.x) / 2}
                        y={(a.y + b.y) / 2 - 4}
                        fontSize="10"
                        fill="#9898a8"
                        textAnchor="middle"
                        pointerEvents="none"
                      >
                        {meta.edges.length}×
                      </text>
                    )}
                  </g>
                )
              })}

              {/* Neighbor nodes */}
              {layout.neighborList.map(({ node }) => {
                const p = positions[node.id]
                if (!p) return null
                const r = nodeRadius(node.record_count, maxCount, 22)
                const isSel = selected === node.id
                return (
                  <g
                    key={node.id}
                    transform={`translate(${p.x},${p.y})`}
                    onMouseDown={e => onNodeMouseDown(e, node.id)}
                    onClick={e => {
                      e.stopPropagation()
                      if (!dragState.current?.moved) setSelected(node.id === selected ? null : node.id)
                    }}
                    onDoubleClick={e => { e.stopPropagation(); setFocusId(node.id) }}
                    style={{ cursor: 'pointer' }}
                  >
                    <circle
                      r={r}
                      fill={nodeFill(node.record_count, maxCount)}
                      stroke={isSel ? '#2F5486' : '#3a3a4a'}
                      strokeWidth={isSel ? 2.6 : 1.2}
                    />
                    <text
                      textAnchor="middle"
                      y={r + 14}
                      fontSize="12"
                      fill="#e8e8ec"
                      style={{ fontFamily: 'DM Sans, sans-serif', pointerEvents: 'none' }}
                    >
                      {shortLabel(node.label)}
                    </text>
                  </g>
                )
              })}

              {/* Focus node (rendered last so it's on top) */}
              {(() => {
                const p = positions[focusNode.id]
                if (!p) return null
                const r = nodeRadius(focusNode.record_count, maxCount, 40)
                return (
                  <g
                    transform={`translate(${p.x},${p.y})`}
                    onMouseDown={e => onNodeMouseDown(e, focusNode.id)}
                    style={{ cursor: 'pointer' }}
                  >
                    <circle r={r + 4} fill="none" stroke="#2F5486" strokeWidth={2} opacity={0.55} />
                    <circle r={r} fill={nodeFill(focusNode.record_count, maxCount)} stroke="#2F5486" strokeWidth={3} />
                    <text
                      textAnchor="middle"
                      y={4}
                      fontSize="13"
                      fontWeight="600"
                      fill="#fff"
                      style={{ fontFamily: 'DM Sans, sans-serif', pointerEvents: 'none' }}
                    >
                      {shortLabel(focusNode.label, 14)}
                    </text>
                  </g>
                )
              })()}

              {/* Legend */}
              <g transform={`translate(20, ${VIEW_H - 70})`} pointerEvents="none">
                <rect width="200" height="58" rx="8" fill="rgba(25,25,31,0.85)" stroke="#2a2a36" />
                <line x1="12" y1="20" x2="40" y2="20" stroke="#2F5486" strokeWidth="2" markerEnd="url(#arrow-out)" />
                <text x="48" y="24" fontSize="11" fill="#e8e8ec">outgoing (lookup)</text>
                <line x1="12" y1="42" x2="40" y2="42" stroke="#4a9ee8" strokeWidth="2" markerEnd="url(#arrow-in)" />
                <text x="48" y="46" fontSize="11" fill="#e8e8ec">incoming</text>
              </g>
            </svg>
          )}
        </div>

        {/* Right: details */}
        <aside className="schema-map-details">
          {detailNode && (
            <>
              <h3 className="schema-map-node-title">{detailNode.label}</h3>
              <div className="schema-map-node-id">{detailNode.id}</div>
              <div className="schema-map-stats">
                <div><strong>{(detailNode.record_count || 0).toLocaleString()}</strong> records</div>
                <div><strong>{detailNode.field_count}</strong> fields</div>
                <div><strong>{detailEdges.length}</strong> relationships</div>
              </div>
              {selectedNode && selectedNode.id !== focusId && (
                <button className="btn-secondary schema-focus-btn" onClick={() => setFocusId(selectedNode.id)}>
                  Focus on this object
                </button>
              )}
              <div className="schema-map-rel-header">Relationships</div>
              <div className="schema-map-rel-list">
                {detailEdges.length === 0 && <div className="schema-map-empty-small">No relationships.</div>}
                {detailEdges.map((e, i) => {
                  const outgoing = e.from === detailNode.id
                  const other = outgoing ? e.to : e.from
                  return (
                    <div key={i} className="schema-map-rel-item" onClick={() => setFocusId(other)} style={{ cursor: 'pointer' }}>
                      <span className="rel-arrow">{outgoing ? '→' : '←'}</span>
                      <div>
                        <div className="rel-target">{other}</div>
                        <div className="rel-field">via {e.field}</div>
                      </div>
                    </div>
                  )
                })}
              </div>
              <div className="schema-map-hint">
                Drag nodes · scroll to zoom · drag background to pan · double-click a neighbor to refocus
              </div>
            </>
          )}
        </aside>
      </div>
    </div>
  )
}
