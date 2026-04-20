import { useState, useEffect, useRef, useCallback } from 'react'
import { api } from './services/api'
import DataChart from './components/DataChart'
import DataTable from './components/DataTable'
import SOQLBlock from './components/SOQLBlock'
import MessageActions from './components/MessageActions'
import Dashboard from './components/Dashboard'
import SessionList from './components/SessionList'
import UsersModal from './components/UsersModal'
import SchemaMap from './components/SchemaMap'
import SchedulesPage from './components/SchedulesPage'
import FilesPage from './components/FilesPage'
import ConnectorsPage from './components/ConnectorsPage'
import AuditPage from './components/AuditPage'
import ComparisonPage from './components/ComparisonPage'
import AlertsPage from './components/AlertsPage'
import NotesPage from './components/NotesPage'
import ReportBuilder from './components/ReportBuilder'
import AnalyticsPage from './components/AnalyticsPage'
import { useChat } from './hooks/useChat'
import { useToast } from './hooks/useToast'
import { useTranslation, LANGUAGES } from './utils/i18n'

// ── Icons ─────────────────────────────────────────────
const I = {
  chat: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/></svg>,
  dash: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>,
  plus: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>,
  send: <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>,
  bot: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/><circle cx="8" cy="16" r="1" fill="currentColor"/><circle cx="16" cy="16" r="1" fill="currentColor"/></svg>,
  logout: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4M16 17l5-5-5-5M21 12H9"/></svg>,
  sf: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/></svg>,
  user: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M20 21v-2a4 4 0 00-4-4H8a4 4 0 00-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>,
  history: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>,
  graph: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="6" cy="6" r="3"/><circle cx="18" cy="6" r="3"/><circle cx="12" cy="18" r="3"/><line x1="8.5" y1="7.5" x2="15.5" y2="7.5"/><line x1="7.5" y1="8.5" x2="10.5" y2="15.5"/><line x1="16.5" y1="8.5" x2="13.5" y2="15.5"/></svg>,
  clock: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>,
  file: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>,
  paperclip: <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21.44 11.05l-9.19 9.19a6 6 0 01-8.49-8.49l9.19-9.19a4 4 0 015.66 5.66l-9.2 9.19a2 2 0 01-2.83-2.83l8.49-8.48"/></svg>,
  xmark: <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>,
  menu: <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/></svg>,
  plug: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M9 2v6 M15 2v6 M6 8h12v4a6 6 0 01-12 0V8z M12 18v4"/></svg>,
  sun: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>,
  moon: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z"/></svg>,
  audit: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"/></svg>,
  compare: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M8 3v18 M16 3v18 M3 8h5 M16 16h5 M3 16h5 M16 8h5"/></svg>,
  bell: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M18 8A6 6 0 006 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 01-3.46 0"/></svg>,
  note: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>,
  report: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M3 3v18h18"/><path d="M7 16V9"/><path d="M12 16V5"/><path d="M17 16v-5"/></svg>,
  analytics: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg>,
}

const timeStr = (d) => d ? new Date(d).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : ''

function renderMd(text) {
  if (!text) return ''

  // Split into lines for block-level processing
  const lines = text.split('\n')
  const out = []
  let inTable = false
  let inList = false
  let tableRows = []

  const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  const inline = (s) => esc(s)
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/`([^`]+)`/g, '<code>$1</code>')

  const flushTable = () => {
    if (tableRows.length < 2) { inTable = false; tableRows = []; return }
    let html = '<div class="md-table-wrap"><table class="md-table"><thead><tr>'
    const headers = tableRows[0]
    headers.forEach(h => { html += `<th>${inline(h.trim())}</th>` })
    html += '</tr></thead><tbody>'
    for (let i = 2; i < tableRows.length; i++) {
      html += '<tr>'
      tableRows[i].forEach(c => { html += `<td>${inline(c.trim())}</td>` })
      html += '</tr>'
    }
    html += '</tbody></table></div>'
    out.push(html)
    tableRows = []
    inTable = false
  }

  let listType = 'ul'
  const flushList = () => { if (inList) { out.push(`</${listType}>`); inList = false } }

  for (const line of lines) {
    // Table row
    if (line.trim().startsWith('|') && line.trim().endsWith('|')) {
      if (!inTable) { flushList(); inTable = true }
      const cells = line.trim().slice(1, -1).split('|')
      if (cells.every(c => /^[\s-:]+$/.test(c))) { tableRows.push(null); continue }
      tableRows.push(cells)
      continue
    }
    if (inTable) flushTable()

    // Headers
    if (/^#### (.+)$/.test(line)) {
      flushList()
      out.push(`<h5 style="margin:10px 0 4px;font-size:13px;font-weight:600;color:var(--text-secondary)">${inline(line.slice(5))}</h5>`)
      continue
    }
    if (/^### (.+)$/.test(line)) {
      flushList()
      out.push(`<h4 style="margin:12px 0 4px;font-size:14px;font-weight:600">${inline(line.slice(4))}</h4>`)
      continue
    }
    if (/^## (.+)$/.test(line)) {
      flushList()
      out.push(`<h3 style="margin:14px 0 4px;font-size:15px;font-weight:600">${inline(line.slice(3))}</h3>`)
      continue
    }
    if (/^# (.+)$/.test(line)) {
      flushList()
      out.push(`<h3 style="margin:16px 0 6px;font-size:16px;font-weight:700">${inline(line.slice(2))}</h3>`)
      continue
    }

    // Horizontal rule
    if (/^---+$/.test(line.trim())) {
      flushList()
      out.push('<hr style="border:none;border-top:1px solid var(--border);margin:12px 0"/>')
      continue
    }

    // Bullet list
    if (/^[-*] (.+)$/.test(line)) {
      if (!inList) { listType = 'ul'; out.push('<ul class="md-list">'); inList = true }
      out.push(`<li>${inline(line.replace(/^[-*] /, ''))}</li>`)
      continue
    }
    // Numbered list
    if (/^\d+\.\s+(.+)$/.test(line)) {
      if (!inList) { listType = 'ol'; out.push('<ol class="md-list" style="padding-left:20px">'); inList = true }
      out.push(`<li>${inline(line.replace(/^\d+\.\s+/, ''))}</li>`)
      continue
    }
    if (inList) flushList()

    // Empty line
    if (!line.trim()) { out.push('<br/>'); continue }

    // Normal paragraph
    out.push(`<p style="margin:4px 0">${inline(line)}</p>`)
  }
  if (inTable) flushTable()
  if (inList) flushList()

  return out.join('')
}

// ── Login Page ────────────────────────────────────────
function LoginPage({ onLogin }) {
  const { t } = useTranslation()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  const handleSubmit = async (e) => {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || 'Login failed')
      localStorage.setItem('token', data.token)
      localStorage.setItem('user', JSON.stringify(data.user))
      onLogin(data.user, data.token)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'var(--bg-primary)' }}>
      <form onSubmit={handleSubmit} style={{
        width: 360, background: 'var(--bg-surface)', border: '1px solid var(--border)',
        borderRadius: 'var(--radius-xl)', padding: '36px 32px',
      }}>
        <div style={{ textAlign: 'center', marginBottom: 28 }}>
          <div style={{
            width: 48, height: 48, borderRadius: 14, background: 'var(--accent)',
            display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
            fontWeight: 700, fontSize: 18, color: 'white', marginBottom: 12,
          }}>SF</div>
          <h1 style={{ fontSize: 20, fontWeight: 600, margin: '0 0 4px' }}>{t('login.title')}</h1>
          <p style={{ fontSize: 13, color: 'var(--text-muted)', margin: 0 }}>{t('login.subtitle')}</p>
        </div>

        {error && (
          <div style={{
            padding: '10px 14px', background: 'rgba(232,74,90,0.1)', border: '1px solid rgba(232,74,90,0.3)',
            borderRadius: 'var(--radius-md)', color: '#e84a5a', fontSize: 13, marginBottom: 16,
          }}>{error}</div>
        )}

        <div style={{ marginBottom: 14 }}>
          <label style={{ fontSize: 12, color: 'var(--text-secondary)', display: 'block', marginBottom: 6 }}>{t('login.username')}</label>
          <input
            type="text" value={username} onChange={e => setUsername(e.target.value)}
            className="input-field" placeholder="admin" autoFocus required
          />
        </div>

        <div style={{ marginBottom: 20 }}>
          <label style={{ fontSize: 12, color: 'var(--text-secondary)', display: 'block', marginBottom: 6 }}>{t('login.password')}</label>
          <input
            type="password" value={password} onChange={e => setPassword(e.target.value)}
            className="input-field" placeholder="••••••••" required
          />
        </div>

        <button type="submit" disabled={loading} style={{
          width: '100%', padding: 12, background: 'var(--accent)', color: 'white', border: 'none',
          borderRadius: 'var(--radius-md)', fontSize: 14, fontWeight: 600, cursor: 'pointer',
          fontFamily: 'var(--font-sans)', opacity: loading ? 0.6 : 1,
        }}>
          {loading ? t('login.signingIn') : t('login.signIn')}
        </button>

      </form>
    </div>
  )
}


// ── Main App ──────────────────────────────────────────
export default function App() {
  const toast = useToast()
  const { t, lang, setLang } = useTranslation()
  const [user, setUser] = useState(null)
  const [token, setToken] = useState(null)
  const [view, setView] = useState('chat')
  const [welcome, setWelcome] = useState(null)
  const [input, setInput] = useState('')
  const [sessions, setSessions] = useState([])
  const [sessionSearch, setSessionSearch] = useState('')
  const [showUsers, setShowUsers] = useState(false)
  const [attachment, setAttachment] = useState(null)
  const [uploading, setUploading] = useState(false)
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [theme, setTheme] = useState(() => localStorage.getItem('theme') || 'dark')
  const inputRef = useRef(null)
  const fileInputRef = useRef(null)

  // Apply theme to root
  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
    localStorage.setItem('theme', theme)
  }, [theme])

  const toggleTheme = () => setTheme(t => t === 'dark' ? 'light' : 'dark')

  // Restore session from localStorage
  useEffect(() => {
    const t = localStorage.getItem('token')
    const u = localStorage.getItem('user')
    if (t && u) { setToken(t); setUser(JSON.parse(u)) }
  }, [])

  // OAuth return handler — shows toast + navigates to Connectors
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const flag = params.get('connector')
    if (!flag) return
    const id = params.get('id') || 'account'
    if (flag === 'connected') {
      toast.success(`Connected ${id}`)
      setView('connectors')
    } else if (flag === 'error') {
      toast.error(`Connection failed: ${params.get('reason') || 'unknown'}`)
    }
    window.history.replaceState({}, '', window.location.pathname)
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const refreshSessions = useCallback(async (q = sessionSearch) => {
    if (!localStorage.getItem('token')) return
    try {
      const list = await api.listSessions(q)
      setSessions(list || [])
    } catch (err) {
      console.error('Failed to list sessions', err)
    }
  }, [sessionSearch])

  const handleSessionChanged = useCallback(() => {
    refreshSessions()
  }, [refreshSessions])

  const { sessionId, messages, loading, send, newChat, loadSession, bottomRef } = useChat(handleSessionChanged)

  useEffect(() => {
    api.welcome().then(setWelcome).catch(() => {})
  }, [])

  // Load session list after login
  useEffect(() => {
    if (user && token) refreshSessions('')
  }, [user, token]) // eslint-disable-line react-hooks/exhaustive-deps

  // Debounced search
  useEffect(() => {
    if (!user) return
    const t = setTimeout(() => refreshSessions(sessionSearch), 250)
    return () => clearTimeout(t)
  }, [sessionSearch]) // eslint-disable-line react-hooks/exhaustive-deps

  const handleLogin = (u, t) => {
    setUser(u)
    setToken(t)
    toast.success(`Welcome, ${u.name || u.username}`)
  }

  const handleLogout = () => {
    localStorage.removeItem('token')
    localStorage.removeItem('user')
    setUser(null)
    setToken(null)
    setSessions([])
    toast.info('Signed out')
  }

  const handleSend = () => {
    if (!input.trim()) return
    send(input, attachment?.id || null)
    setInput('')
    // Keep the attachment pinned so follow-up questions reuse it; user removes manually.
    inputRef.current?.focus()
  }

  const handlePickFile = async (e) => {
    const file = e.target.files?.[0]
    e.target.value = ''
    if (!file) return
    setUploading(true)
    try {
      const meta = await api.uploadFile(file)
      setAttachment(meta)
      toast.success(`Attached ${meta.filename}`)
    } catch (err) {
      toast.error(`Upload failed: ${err.message}`)
    } finally {
      setUploading(false)
    }
  }

  const handleUseFileInChat = (meta) => {
    setAttachment(meta)
    setView('chat')
    toast.info(`Attached ${meta.filename} — ask a question`)
  }

  const handleKey = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSend() }
  }

  const handleDashAsk = (q) => { setView('chat'); setTimeout(() => send(q), 100) }
  const handleAnalyticsDrill = (q) => { setView('chat'); setTimeout(() => send(q), 100) }

  const closeSidebar = () => setSidebarOpen(false)

  const handleNavClick = (v) => { setView(v); closeSidebar() }

  const handleNewChat = () => {
    newChat()
    setView('chat')
    closeSidebar()
  }

  const handlePickSession = async (id) => {
    await loadSession(id)
    setView('chat')
    closeSidebar()
  }

  const handleDeleteSession = async (id) => {
    try {
      await api.deleteSession(id)
      if (id === sessionId) newChat()
      refreshSessions()
      toast.success('Chat deleted')
    } catch (err) {
      toast.error(`Delete failed: ${err.message}`)
    }
  }

  const handlePinSession = async (id) => {
    try {
      const res = await api.pinSession(id)
      refreshSessions()
      toast.info(res.pinned ? 'Pinned' : 'Unpinned')
    } catch (err) {
      toast.error(`Pin failed: ${err.message}`)
    }
  }

  if (!user) return <LoginPage onLogin={handleLogin} />

  const quickActions = [
    { label: "Today's Submissions", icon: '📋', q: "Today's submissions by BU" },
    { label: 'Weekly Confirmations', icon: '🎉', q: 'Last week confirmations with congratulations' },
    { label: 'Students In Market', icon: '📊', q: 'How many students are in market?' },
    { label: 'Monthly Report', icon: '📈', q: 'This month submissions, interviews and confirmations' },
    { label: 'No Interviews (14d)', icon: '⚠️', q: '2 weeks no interviews by BU' },
    { label: 'BU Expenses', icon: '💰', q: 'Expenses and placement cost by BU' },
  ]

  return (
    <div className={`app ${sidebarOpen ? 'sidebar-open' : ''}`}>
      {sidebarOpen && <div className="sidebar-backdrop" onClick={closeSidebar} />}
      {/* Sidebar */}
      <aside className={`sidebar ${sidebarOpen ? 'open' : ''}`}>
        <div className="sidebar-header">
          <div className="sidebar-logo">SF</div>
          <span className="sidebar-title">Data Chat</span>
        </div>

        <div className="sidebar-top">
          <button className="new-chat-btn" onClick={handleNewChat}>
            {I.plus} {t('sidebar.newChat')}
          </button>

          <nav className="sidebar-nav">
            <div className={`nav-item ${view === 'chat' ? 'active' : ''}`} onClick={() => handleNavClick('chat')}>
              {I.chat} <span>{t('sidebar.chat')}</span>
            </div>
            <div className={`nav-item ${view === 'dashboard' ? 'active' : ''}`} onClick={() => handleNavClick('dashboard')}>
              {I.dash} <span>{t('sidebar.dashboard')}</span>
            </div>
            <div className={`nav-item ${view === 'reports' ? 'active' : ''}`} onClick={() => handleNavClick('reports')}>
              {I.report} <span>Reports</span>
            </div>
            <div className={`nav-item ${view === 'analytics' ? 'active' : ''}`} onClick={() => handleNavClick('analytics')}>
              {I.analytics} <span>AI Analytics</span>
            </div>
            <div className={`nav-item ${view === 'schema' ? 'active' : ''}`} onClick={() => handleNavClick('schema')}>
              {I.graph} <span>{t('sidebar.schema')}</span>
            </div>
            <div className={`nav-item ${view === 'schedules' ? 'active' : ''}`} onClick={() => handleNavClick('schedules')}>
              {I.clock} <span>{t('sidebar.schedules')}</span>
            </div>
            <div className={`nav-item ${view === 'files' ? 'active' : ''}`} onClick={() => handleNavClick('files')}>
              {I.file} <span>{t('sidebar.files')}</span>
            </div>
            <div className={`nav-item ${view === 'connectors' ? 'active' : ''}`} onClick={() => handleNavClick('connectors')}>
              {I.plug} <span>{t('sidebar.connectors')}</span>
            </div>
            {user.role === 'admin' && (
              <>
                <div className={`nav-item ${view === 'audit' ? 'active' : ''}`} onClick={() => handleNavClick('audit')}>
                  {I.audit} <span>{t('sidebar.audit')}</span>
                </div>
                <div className="nav-item" onClick={() => { setShowUsers(true); closeSidebar() }}>
                  {I.user} <span>{t('sidebar.users')}</span>
                </div>
              </>
            )}
          </nav>
        </div>

        <SessionList
          sessions={sessions}
          activeId={sessionId}
          onPick={handlePickSession}
          onDelete={handleDeleteSession}
          onPin={handlePinSession}
          search={sessionSearch}
          onSearchChange={setSessionSearch}
          labels={{ pinned: t('sidebar.pinned'), recent: t('sidebar.recent'), searchChats: t('sidebar.searchChats') }}
        />

        <div className="sidebar-footer">
          <div className="language-picker">
            <select value={lang} onChange={e => setLang(e.target.value)} title={t('language.label')}>
              {LANGUAGES.map(l => <option key={l.code} value={l.code}>{l.label}</option>)}
            </select>
          </div>
          <div style={{
            display: 'flex', alignItems: 'center', gap: 8,
            padding: '8px 10px', borderRadius: 'var(--radius-md)',
            background: 'var(--bg-elevated)', fontSize: 12,
          }}>
            <div style={{
              width: 26, height: 26, borderRadius: 'var(--radius-sm)',
              background: 'var(--accent-muted)', display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}>{I.user}</div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontWeight: 500, color: 'var(--text-primary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{user.name || user.username}</div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>{user.role}</div>
            </div>
            <button onClick={toggleTheme} className="action-btn" title={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'} style={{ color: 'var(--text-muted)' }}>
              {theme === 'dark' ? I.sun : I.moon}
            </button>
            <button onClick={handleLogout} className="action-btn" title="Logout" style={{ color: 'var(--text-muted)' }}>
              {I.logout}
            </button>
          </div>
        </div>
      </aside>

      {showUsers && <UsersModal currentUser={user} onClose={() => setShowUsers(false)} />}

      {/* Main */}
      <main className="main">
        <button className="mobile-menu-btn" onClick={() => setSidebarOpen(true)} aria-label="Open menu">
          {I.menu}
        </button>
        {view === 'dashboard' ? (
          <Dashboard onAsk={handleDashAsk} />
        ) : view === 'schema' ? (
          <SchemaMap />
        ) : view === 'schedules' ? (
          <SchedulesPage />
        ) : view === 'files' ? (
          <FilesPage onUseInChat={handleUseFileInChat} />
        ) : view === 'connectors' ? (
          <ConnectorsPage />
        ) : view === 'audit' ? (
          <AuditPage />
        ) : view === 'compare' ? (
          <ComparisonPage />
        ) : view === 'alerts' ? (
          <AlertsPage />
        ) : view === 'notes' ? (
          <NotesPage />
        ) : view === 'reports' ? (
          <ReportBuilder />
        ) : view === 'analytics' ? (
          <AnalyticsPage onDrillDown={handleAnalyticsDrill} />
        ) : (
          <>
            <div className="chat-header">
              <span className="chat-header-title">Salesforce Data Chat</span>
              <span className="chat-header-badge">
                {welcome?.data?.total_objects || '—'} objects · {(welcome?.data?.total_records || 0).toLocaleString()} records
              </span>
            </div>

            <div className="messages">
              {messages.length === 0 && (
                <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 24 }}>
                  <div style={{ width: 56, height: 56, borderRadius: 16, background: 'var(--accent-muted)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>{I.sf}</div>
                  <div style={{ textAlign: 'center' }}>
                    <h2 style={{ fontSize: 20, fontWeight: 600, marginBottom: 6 }}>{t('chat.welcome', { name: user.name || user.username })}</h2>
                    <p style={{ color: 'var(--text-muted)', fontSize: 13, maxWidth: 420 }}>
                      {t('chat.welcomeSub')}
                    </p>
                  </div>
                  <div className="quick-actions-grid">
                    {quickActions.map(a => (
                      <button key={a.q} className="quick-action-card" onClick={() => send(a.q)}>
                        <span className="quick-action-icon">{a.icon}</span>
                        <span className="quick-action-label">{a.label}</span>
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {(() => {
                const lastAsstId = [...messages].reverse().find(m => m.role === 'assistant' && !m.streaming && !m.isError)?.id
                return messages.map(msg => {
                const isStreamingEmpty = msg.streaming && !msg.content
                const isStreaming = msg.streaming && !!msg.content
                const showSuggestions = msg.id === lastAsstId && msg.suggestions?.length > 0
                return (
                  <div key={msg.id} className={`message ${msg.role}`}>
                    <div className="message-avatar">
                      {msg.role === 'user' ? (user.name?.[0] || 'U') : I.bot}
                    </div>
                    <div className="message-body">
                      <div className="message-content">
                        {isStreamingEmpty ? (
                          <div className="thinking-indicator">
                            <span className="thinking-asterisk">*</span>
                            <span className="thinking-text">Thinking...</span>
                          </div>
                        ) : (
                          <>
                            <span
                              dangerouslySetInnerHTML={{
                                __html: msg.role === 'assistant' ? renderMd(msg.content) : msg.content,
                              }}
                            />
                            {isStreaming && <span className="streaming-cursor" />}
                          </>
                        )}
                      </div>
                      {msg.role === 'assistant' && !msg.isError && (
                        <>
                          {msg.data?.records?.length > 0 && <DataChart records={msg.data.records} totalSize={msg.data.totalSize} />}
                          {msg.data?.records?.length > 0 && <DataTable records={msg.data.records} totalSize={msg.data.totalSize} />}
                          <SOQLBlock soql={msg.soql} route={msg.data?.route} />
                          {!msg.streaming && <MessageActions message={msg} />}
                          {showSuggestions && (
                            <div className="suggestion-chips">
                              {msg.suggestions.map(s => (
                                <button key={s} className="suggestion-chip" onClick={() => send(s)} disabled={loading}>
                                  {s}
                                </button>
                              ))}
                            </div>
                          )}
                        </>
                      )}
                      <div className="message-time">{timeStr(msg.ts)}</div>
                    </div>
                  </div>
                )
              })
              })()}
              <div ref={bottomRef} />
            </div>

            <div className="input-area">
              {attachment && (
                <div className="attachment-chip">
                  {I.file}
                  <span className="attachment-chip-name">{attachment.filename}</span>
                  <span className="attachment-chip-meta">
                    {attachment.row_count?.toLocaleString()} rows · {attachment.headers?.length} cols
                  </span>
                  <button
                    type="button"
                    className="attachment-chip-remove"
                    onClick={() => setAttachment(null)}
                    title="Remove attachment"
                  >
                    {I.xmark}
                  </button>
                </div>
              )}
              <div className="input-wrapper">
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv,.xlsx,.xls"
                  style={{ display: 'none' }}
                  onChange={handlePickFile}
                />
                <button
                  className="input-upload-btn"
                  onClick={() => fileInputRef.current?.click()}
                  disabled={uploading || loading}
                  title="Attach CSV or Excel file"
                >
                  {I.paperclip}
                </button>
                <textarea ref={inputRef} className="input-field" value={input}
                  onChange={e => setInput(e.target.value)} onKeyDown={handleKey}
                  placeholder={attachment ? t('chat.placeholderFile', { file: attachment.filename }) : t('chat.placeholder')}
                  rows={1} disabled={loading} />
                <button className="send-btn" onClick={handleSend} disabled={loading || !input.trim()}>
                  {I.send}
                </button>
              </div>
              <div className="input-footer">
                <span className="input-hint">
                  {uploading ? t('chat.uploading') : t('chat.shiftEnter')}
                </span>
                <span className="input-hint">{t('chat.loggedInAs', { name: user.username })}</span>
              </div>
            </div>
          </>
        )}
      </main>
    </div>
  )
}
