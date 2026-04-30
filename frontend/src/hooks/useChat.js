import { useState, useCallback, useRef, useEffect } from 'react'
import { api } from '../services/api'

let idCounter = 0
const uid = () => `msg_${++idCounter}_${Date.now()}`
const newSessionId = () => `s_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`

function normalizeMessage(m) {
  return {
    id: m.id || uid(),
    role: m.role,
    content: m.content,
    soql: m.soql,
    data: m.data,
    question: m.question,
    suggestions: m.suggestions || [],
    ts: m.ts ? new Date(m.ts) : new Date(),
    isError: m.isError,
    fileDownload: m.fileDownload || null,
  }
}

export function useChat(onSessionChanged) {
  const [messages, setMessages] = useState([])
  const [loading, setLoading] = useState(false)
  const [sessionId, setSessionId] = useState(() => newSessionId())
  const bottomRef = useRef(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  const send = useCallback(async (text, attachmentId = null) => {
    const q = text.trim()
    if (!q || loading) return
    const isFirstMessage = messages.length === 0

    setMessages(prev => [...prev, {
      id: uid(), role: 'user', content: q, ts: new Date(),
      attachmentId: attachmentId || undefined,
    }])

    // Pre-insert a placeholder assistant message that will be filled in
    // progressively as tokens arrive. `streaming: true` tells the renderer
    // to show the blinking cursor until the stream completes.
    const asstId = uid()
    setMessages(prev => [...prev, {
      id: asstId,
      role: 'assistant',
      content: '',
      question: q,
      ts: new Date(),
      streaming: true,
    }])
    setLoading(true)

    const token = localStorage.getItem('token')
    const headers = { 'Content-Type': 'application/json' }
    if (token) headers['Authorization'] = `Bearer ${token}`

    let streamedText = ''
    let finalSoql = null
    let finalData = null
    let finalRoute = null
    let finalSuggestions = null
    let finalConfidence = null

    const applyPatch = (patch) => {
      setMessages(prev => prev.map(m => m.id === asstId ? { ...m, ...patch } : m))
    }

    try {
      const res = await fetch('/api/chat/stream', {
        method: 'POST',
        headers,
        body: JSON.stringify({
          session_id: sessionId,
          question: q,
          attachment_id: attachmentId,
        }),
      })
      if (!res.ok) {
        let detail = `${res.status}: ${res.statusText}`
        try { const j = await res.json(); if (j.detail) detail = j.detail } catch {}
        throw new Error(detail)
      }

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })

        // SSE events are separated by a blank line
        let idx
        while ((idx = buffer.indexOf('\n\n')) !== -1) {
          const rawEvent = buffer.slice(0, idx)
          buffer = buffer.slice(idx + 2)
          // Each event may have multiple `data: ` lines; join them
          const dataLines = rawEvent.split('\n')
            .filter(l => l.startsWith('data: '))
            .map(l => l.slice(6))
          if (!dataLines.length) continue
          let evt
          try { evt = JSON.parse(dataLines.join('\n')) } catch { continue }

          switch (evt.type) {
            case 'thinking':
            case 'thinking_done':
              break
            case 'route':
              finalRoute = evt.data
              break
            case 'soql':
              finalSoql = evt.data
              applyPatch({ soql: evt.data })
              break
            case 'data':
              finalData = evt.data
              applyPatch({ data: evt.data })
              break
            case 'token':
              streamedText += evt.data
              applyPatch({ content: streamedText })
              break
            case 'suggestions':
              finalSuggestions = evt.data
              applyPatch({ suggestions: evt.data })
              break
            case 'error':
              throw new Error(evt.data || 'Stream error')
            case 'done':
              if (evt.data) {
                if (evt.data.answer) streamedText = evt.data.answer
                if (evt.data.soql) finalSoql = evt.data.soql
                if (evt.data.data) finalData = evt.data.data
                if (evt.data.suggestions) finalSuggestions = evt.data.suggestions
                if (evt.data.confidence != null) finalConfidence = evt.data.confidence
                if (evt.data.file_download) applyPatch({ fileDownload: evt.data.file_download })
              }
              break
            default:
              break
          }
        }
      }

      applyPatch({
        content: streamedText,
        soql: finalSoql,
        data: finalData,
        route: finalRoute,
        confidence: finalConfidence,
        suggestions: finalSuggestions || [],
        streaming: false,
      })

      if (isFirstMessage && onSessionChanged) onSessionChanged(sessionId)
      else if (onSessionChanged) onSessionChanged(sessionId, /*silent*/ true)
    } catch (err) {
      applyPatch({
        content: streamedText
          ? `${streamedText}\n\n[Stream interrupted: ${err.message}]`
          : `Error: ${err.message}`,
        isError: !streamedText,
        streaming: false,
      })
    } finally {
      setLoading(false)
    }
  }, [loading, sessionId, messages.length, onSessionChanged])

  const editMessage = useCallback((msgId, newText) => {
    if (loading) return
    const idx = messages.findIndex(m => m.id === msgId)
    if (idx === -1) return
    setMessages(prev => prev.slice(0, idx))
    setTimeout(() => send(newText), 50)
  }, [loading, messages, send])

  const regenerate = useCallback((msgId) => {
    if (loading) return
    const idx = messages.findIndex(m => m.id === msgId)
    if (idx === -1) return
    const userMsg = messages.slice(0, idx).reverse().find(m => m.role === 'user')
    if (!userMsg) return
    setMessages(prev => prev.slice(0, idx))
    setTimeout(() => send(userMsg.content), 50)
  }, [loading, messages, send])

  const newChat = useCallback(() => {
    setSessionId(newSessionId())
    setMessages([])
  }, [])

  const loadSession = useCallback(async (id) => {
    try {
      const s = await api.getSession(id)
      setSessionId(s.id)
      setMessages((s.messages || []).map(normalizeMessage))
    } catch (err) {
      console.error('Failed to load session', err)
    }
  }, [])

  return { sessionId, messages, loading, send, editMessage, regenerate, newChat, loadSession, bottomRef }
}
