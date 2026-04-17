export function downloadCSV(records, filename = 'salesforce_export.csv') {
  if (!records?.length) return
  const keys = Object.keys(records[0]).filter(k => k !== 'attributes')
  const escape = (v) => {
    if (v == null) return ''
    const s = String(v)
    return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s.replace(/"/g, '""')}"` : s
  }
  const csv = [keys.join(','), ...records.map(r => keys.map(k => escape(r[k])).join(','))].join('\n')
  const blob = new Blob([csv], { type: 'text/csv' })
  const a = document.createElement('a')
  a.href = URL.createObjectURL(blob)
  a.download = filename
  a.click()
  URL.revokeObjectURL(a.href)
}

export async function copyText(text) {
  try { await navigator.clipboard.writeText(text); return true } catch { return false }
}
