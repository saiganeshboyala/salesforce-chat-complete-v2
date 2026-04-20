const BASE = '/api'

function authHeaders() {
  const token = localStorage.getItem('token')
  const h = { 'Content-Type': 'application/json' }
  if (token) h['Authorization'] = `Bearer ${token}`
  return h
}

async function request(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: authHeaders(),
    ...options,
    // preserve caller's headers while still sending auth
    ...(options.headers ? { headers: { ...authHeaders(), ...options.headers } } : {}),
  })
  if (!res.ok) {
    let detail = `${res.status}: ${res.statusText}`
    try { const j = await res.json(); if (j.detail) detail = j.detail } catch {}
    throw new Error(detail)
  }
  if (res.status === 204) return null
  return res.json()
}

async function uploadFile(path, file) {
  const token = localStorage.getItem('token')
  const fd = new FormData()
  fd.append('file', file)
  const headers = {}
  if (token) headers['Authorization'] = `Bearer ${token}`
  const res = await fetch(`${BASE}${path}`, { method: 'POST', headers, body: fd })
  if (!res.ok) {
    let detail = `${res.status}: ${res.statusText}`
    try { const j = await res.json(); if (j.detail) detail = j.detail } catch {}
    throw new Error(detail)
  }
  return res.json()
}

export const api = {
  chat:       (sessionId, question, attachmentId = null) =>
    request('/chat', { method: 'POST', body: JSON.stringify({
      session_id: sessionId, question, attachment_id: attachmentId,
    }) }),
  welcome:    ()                    => request('/welcome'),
  health:     ()                    => request('/health'),
  overview:   ()                    => request('/overview'),
  dashboard:  ()                    => request('/dashboard'),
  getDashboardConfig: ()            => request('/dashboard/config'),
  saveDashboardConfig: (widgets)    => request('/dashboard/config', { method: 'POST', body: JSON.stringify({ widgets }) }),
  runWidget: (soql)                 => request(`/dashboard/widget?q=${encodeURIComponent(soql)}`),
  compare:    (payload)             => request('/compare', { method: 'POST', body: JSON.stringify(payload) }),

  // Alerts
  listAlerts:     ()                => request('/alerts'),
  createAlert:    (payload)         => request('/alerts', { method: 'POST', body: JSON.stringify(payload) }),
  updateAlert:    (id, patch)       => request(`/alerts/${encodeURIComponent(id)}`, { method: 'PATCH', body: JSON.stringify(patch) }),
  deleteAlert:    (id)              => request(`/alerts/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  checkAlert:     (id)              => request(`/alerts/${encodeURIComponent(id)}/check`, { method: 'POST' }),
  checkAllAlerts: ()                => request('/alerts/check', { method: 'POST' }),
  alertHistory:   ()                => request('/alerts/history'),

  // Annotations
  listAnnotations: (params = {}) => {
    const qs = new URLSearchParams()
    Object.entries(params).forEach(([k, v]) => { if (v != null && v !== '') qs.set(k, v) })
    return request(`/annotations${qs.toString() ? `?${qs}` : ''}`)
  },
  createAnnotation: (payload)         => request('/annotations', { method: 'POST', body: JSON.stringify(payload) }),
  updateAnnotation: (id, patch)       => request(`/annotations/${encodeURIComponent(id)}`, { method: 'PATCH', body: JSON.stringify(patch) }),
  deleteAnnotation: (id)              => request(`/annotations/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  annotationTags:   ()                => request('/annotations/tags'),
  annotationLookup: (ids)             => request('/annotations/lookup', { method: 'POST', body: JSON.stringify({ record_ids: ids }) }),
  feedback:   (question, fb)        => request('/feedback', { method: 'POST', body: JSON.stringify({ question, feedback: fb }) }),
  learning:   ()                    => request('/learning-stats'),
  exportUrl:  (soql, format = 'csv') => `${BASE}/export?q=${encodeURIComponent(soql)}&format=${format}`,
  exportPdf:  async (payload) => {
    const res = await fetch(`${BASE}/export/pdf`, {
      method: 'POST', headers: authHeaders(), body: JSON.stringify(payload),
    })
    if (!res.ok) {
      let detail = `${res.status}: ${res.statusText}`
      try { const j = await res.json(); if (j.detail) detail = j.detail } catch {}
      throw new Error(detail)
    }
    return res.blob()
  },

  // Reports
  listReports:    ()                => request('/reports'),
  getReport:      (id)              => request(`/reports/${encodeURIComponent(id)}`),
  createReport:   (payload)         => request('/reports', { method: 'POST', body: JSON.stringify(payload) }),
  updateReport:   (id, patch)       => request(`/reports/${encodeURIComponent(id)}`, { method: 'PATCH', body: JSON.stringify(patch) }),
  deleteReport:   (id)              => request(`/reports/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  runReport:      (id)              => request(`/reports/${encodeURIComponent(id)}/run`, { method: 'POST' }),
  previewReport:  (payload)         => request('/reports/preview', { method: 'POST', body: JSON.stringify(payload) }),
  suggestReport:  (prompt)          => request('/reports/suggest', { method: 'POST', body: JSON.stringify({ prompt }) }),

  // Schema
  schemaRelationships: ()           => request('/schema/relationships'),
  schemaObjects:       ()           => request('/schema/objects'),

  // Uploads
  uploadFile:     (file)            => uploadFile('/uploads', file),
  listUploads:    ()                => request('/uploads'),
  getUpload:      (id)              => request(`/uploads/${encodeURIComponent(id)}`),
  deleteUpload:   (id)              => request(`/uploads/${encodeURIComponent(id)}`, { method: 'DELETE' }),

  // Schedules
  listSchedules:     ()             => request('/schedules'),
  createSchedule:    (payload)      => request('/schedules', { method: 'POST', body: JSON.stringify(payload) }),
  updateSchedule:    (id, patch)    => request(`/schedules/${encodeURIComponent(id)}`, { method: 'PATCH', body: JSON.stringify(patch) }),
  deleteSchedule:    (id)           => request(`/schedules/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  runScheduleNow:    (id)           => request(`/schedules/${encodeURIComponent(id)}/run`, { method: 'POST' }),
  listScheduleRuns:  (id)           => request(`/schedules/${encodeURIComponent(id)}/runs`),

  // Sessions
  listSessions:   (q)               => request(`/sessions${q ? `?q=${encodeURIComponent(q)}` : ''}`),
  getSession:     (id)              => request(`/sessions/${encodeURIComponent(id)}`),
  deleteSession:  (id)              => request(`/sessions/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  pinSession:     (id)              => request(`/sessions/${encodeURIComponent(id)}/pin`, { method: 'POST' }),

  // Audit
  audit: (params = {}) => {
    const qs = new URLSearchParams()
    Object.entries(params).forEach(([k, v]) => { if (v != null && v !== '') qs.set(k, v) })
    return request(`/audit${qs.toString() ? `?${qs}` : ''}`)
  },

  // Connectors
  listConnectors:      ()            => request('/connectors'),
  disconnectConnector: (id)          => request(`/connectors/${encodeURIComponent(id)}/disconnect`, { method: 'POST' }),
  gmailAuthUrl:        ()            => {
    const token = localStorage.getItem('token') || ''
    return `${BASE}/connectors/gmail/auth?token=${encodeURIComponent(token)}`
  },
  gmailSend:           (payload)     => request('/connectors/gmail/send', { method: 'POST', body: JSON.stringify(payload) }),

  // AI providers
  testAiConnector:     (id)          => request(`/connectors/${encodeURIComponent(id)}/test`, { method: 'POST' }),
  aiProviders:         ()            => request('/ai/providers'),

  // Predictive Analytics
  predictiveAnalytics: ()            => request('/analytics/predictive'),

  // Sync
  syncStatus:          ()            => request('/sync/status'),
  syncRun:             ()            => request('/sync/run', { method: 'POST' }),

  // AI Analytics
  analyticsGenerate:   (prompt, provider) => request('/analytics/generate', { method: 'POST', body: JSON.stringify({ prompt, provider }) }),
  analyticsInsight:    (prompt, cards, provider) => request('/analytics/insight', { method: 'POST', body: JSON.stringify({ prompt, cards, provider }) }),

  // Users (admin)
  listUsers:      ()                => request('/auth/users'),
  createUser:     (payload)         => request('/auth/register', { method: 'POST', body: JSON.stringify(payload) }),
  deleteUser:     (username)        => request(`/auth/users/${encodeURIComponent(username)}`, { method: 'DELETE' }),
  resetPassword:  (username, pwd)   => request('/auth/admin-reset-password', { method: 'POST', body: JSON.stringify({ username, new_password: pwd }) }),
  me:             ()                => request('/auth/me'),
  changePassword: (oldPwd, newPwd)  => request('/auth/change-password', { method: 'POST', body: JSON.stringify({ old_password: oldPwd, new_password: newPwd }) }),
}
