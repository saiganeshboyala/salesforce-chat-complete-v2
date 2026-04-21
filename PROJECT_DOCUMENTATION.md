# Fyxo Chat — Complete Project Documentation

**Project:** Fyxo Chat (Salesforce AI Chat Platform)
**Version:** 2.0
**Date:** April 22, 2026
**Team:** Sai Ganesh Boyala

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Technology Stack](#3-technology-stack)
4. [System Architecture Diagram](#4-system-architecture-diagram)
5. [Backend — API & Modules](#5-backend--api--modules)
6. [AI Intelligence Pipeline](#6-ai-intelligence-pipeline)
7. [Semantic Query Layer](#7-semantic-query-layer)
8. [Database Architecture](#8-database-architecture)
9. [Frontend Application](#9-frontend-application)
10. [API Reference](#10-api-reference)
11. [Third-Party Integrations](#11-third-party-integrations)
12. [Security & Authentication](#12-security--authentication)
13. [Testing & Quality](#13-testing--quality)
14. [Deployment](#14-deployment)
15. [Configuration Reference](#15-configuration-reference)
16. [Directory Structure](#16-directory-structure)

---

## 1. Executive Summary

Fyxo Chat is an AI-powered conversational interface that allows non-technical users to query Salesforce CRM data using natural language. Instead of writing SQL or navigating Salesforce reports, users simply type questions like "how many students are in market?" or "show me this month's submissions by BU" and receive instant, accurate answers with charts, tables, and downloadable reports.

### Key Capabilities

- **Natural Language Querying** — Ask questions in plain English, get answers backed by real database data
- **Multi-AI Provider Support** — Claude (Anthropic), GPT-4o (OpenAI), and Grok (xAI) with automatic failover
- **Real-Time Streaming** — Server-Sent Events for word-by-word response streaming
- **Self-Learning System** — Every Q&A interaction is saved; the AI uses past successful queries as few-shot examples
- **97.7% Accuracy** — Semantic query layer handles most questions without AI, ensuring deterministic correctness
- **4-Layer Intelligence System** — Synonym expansion → Fuzzy cache → SQL validation → Answer verification
- **Multi-Format Export** — CSV, XLSX, PDF export with branded formatting
- **Scheduled Reports** — Automated daily/weekly/monthly report generation
- **Dashboard & Analytics** — Live KPI metrics, AI-generated analytics cards, period-over-period comparisons
- **Gmail Integration** — Send reports and messages directly via Gmail OAuth
- **Audit Trail** — Complete audit log of all user actions with IP tracking

### Business Impact

| Metric | Before | After |
|--------|--------|-------|
| Time to get a data answer | 5-15 min (navigate Salesforce, build report) | 3-5 seconds |
| Report generation | Manual, hours per report | Automated, instant |
| Data access | Limited to trained Salesforce users | Any team member via chat |
| Follow-up questions | Start from scratch | Conversational context maintained |

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        FRONTEND (React 18)                       │
│  Chat UI │ Dashboard │ Analytics │ Reports │ Schema │ Connectors │
│                    ↕ REST API + SSE                               │
├─────────────────────────────────────────────────────────────────┤
│                      BACKEND (FastAPI / Python)                  │
│                                                                  │
│  ┌──────────┐    ┌──────────────┐    ┌─────────────┐            │
│  │  Auth &   │    │  Chat Engine  │    │  Connectors │            │
│  │  Users    │    │  + Streaming  │    │  Gmail/Slack│            │
│  └──────────┘    └──────┬───────┘    └─────────────┘            │
│                         │                                        │
│            ┌────────────┼────────────┐                           │
│            ▼            ▼            ▼                           │
│  ┌──────────────┐ ┌──────────┐ ┌──────────┐                    │
│  │  Semantic     │ │  Direct   │ │    AI     │                    │
│  │  Query Layer  │ │  Report   │ │  Engine   │                    │
│  │  (No AI)      │ │  Pattern  │ │ Claude/   │                    │
│  │  97.7% of Qs  │ │  Matcher  │ │ GPT/Grok  │                    │
│  └──────┬───────┘ └─────┬────┘ └─────┬────┘                    │
│         │               │            │                           │
│         └───────────┬───┘────────────┘                           │
│                     ▼                                            │
│  ┌──────────────────────────────────────────────┐               │
│  │           PostgreSQL Database                  │               │
│  │  Salesforce Mirror │ Learning Memory │ Sessions│               │
│  └──────────────────────────────────────────────┘               │
│                     ▲                                            │
│                     │ Incremental Sync (15 min)                  │
│  ┌──────────────────┴───────────────────────────┐               │
│  │           Salesforce REST API                  │               │
│  │  OAuth2 │ SOQL │ Schema Discovery              │               │
│  └──────────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────────┘
```

### Request Flow

1. **User types a question** in the chat UI
2. **Frontend** sends POST to `/api/chat` or `/api/chat/stream` (SSE)
3. **Layer 1 — Synonym Expansion**: Normalizes slang, abbreviations, misspellings
4. **Layer 2 — Semantic Query Layer**: Tries to map the question to exact SQL using pattern matching (handles 97.7% of questions)
5. **Layer 3 — Direct Report Pattern Matcher**: Checks against predefined report templates (multi-query reports)
6. **Layer 4 — AI Engine** (if semantic/direct fails):
   a. Fuzzy cache lookup (reuse verified past SQL)
   b. AI routes question → SQL / RAG / BOTH
   c. AI generates PostgreSQL SQL with schema context
   d. Auto-fix common SQL mistakes
   e. Validate fields against schema + validate picklist values
   f. Execute SQL, retry up to 2x on error
7. **Answer Generation**: AI formats the raw data into a user-friendly response
8. **Answer Verification**: Cross-checks counts and names against actual DB data
9. **Learning**: Saves the Q&A pair to `learning_memory` for future reference
10. **Response**: Returns answer + SQL used + raw data + suggestions

---

## 3. Technology Stack

### Backend
| Component | Technology | Purpose |
|-----------|-----------|---------|
| API Framework | FastAPI 0.115 | REST API with async support, auto-generated OpenAPI docs |
| Runtime | Python 3.11+ | Async/await throughout |
| ASGI Server | Uvicorn 0.30 | Production-grade ASGI server |
| Database | PostgreSQL 15+ | Primary data store (Salesforce mirror + app data) |
| ORM | SQLAlchemy 2.0 (async) | Database models and queries |
| DB Driver | asyncpg 0.30 | Async PostgreSQL driver |
| AI - Primary | Anthropic Claude (claude-sonnet-4) | SQL generation, answer formatting, analytics |
| AI - Fallback 1 | OpenAI GPT-4o | Fallback SQL/answer generation |
| AI - Fallback 2 | xAI Grok-3 | Second fallback |
| Vector DB | Qdrant 1.12 (local) | Semantic similarity search (RAG) |
| Embeddings | OpenAI text-embedding-3-small | 1536-dimension text embeddings |
| Auth | python-jose (JWT) + passlib (bcrypt) | Token auth + password hashing |
| Rate Limiting | slowapi 0.1.9 | Per-endpoint rate limits |
| PDF Export | ReportLab 4.2 | Branded PDF report generation |
| Excel Export | openpyxl 3.1 | XLSX file generation |
| HTTP Client | httpx 0.27 | Async HTTP for Salesforce API |

### Frontend
| Component | Technology | Purpose |
|-----------|-----------|---------|
| Framework | React 18.3 | UI framework |
| Build Tool | Vite 5.4 | Dev server + production bundler |
| Charts | Recharts 2.12 | Bar, pie, line charts |
| Styling | CSS Variables + Custom Design System | Light/dark themes |
| API Layer | Custom fetch wrapper | JWT-authenticated API calls |
| Streaming | EventSource (SSE) | Real-time word-by-word streaming |

### Infrastructure
| Component | Technology | Purpose |
|-----------|-----------|---------|
| Salesforce | REST API + OAuth2 | CRM data source |
| Database | PostgreSQL (local or AWS RDS) | Data persistence |
| Deployment | Single server (deploy.sh) | FastAPI serves API + built React static files |

---

## 4. System Architecture Diagram

### Data Flow

```
Salesforce CRM
      │
      │  OAuth2 + REST API
      │  (incremental sync every 15 min)
      ▼
┌─────────────────┐
│   PostgreSQL     │
│                  │
│  Student__c      │  18 Salesforce tables
│  Submissions__c  │  mirrored locally
│  Interviews__c   │
│  Manager__c      │
│  Job__c          │
│  Employee__c     │
│  ... (12 more)   │
│                  │
│  learning_memory │  Self-learning Q&A store
│  chat_sessions   │  User chat history
│  chat_messages   │  Individual messages
│  users           │  App user accounts
│  audit_log       │  Action audit trail
│  sync_log        │  Sync tracking
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│        Query Intelligence Pipeline    │
│                                       │
│  1. Synonym Expansion (60+ mappings)  │
│  2. Semantic Layer (pattern → SQL)    │
│  3. Fuzzy Cache (verified Q&A reuse)  │
│  4. AI SQL Generation (Claude/GPT)    │
│  5. SQL Auto-Fix (field/value fixes)  │
│  6. Schema Validation (field check)   │
│  7. Picklist Validation (value check) │
│  8. Execution + Retry (up to 3x)     │
│  9. Answer Formatting (AI)            │
│  10. Answer Verification (count/name) │
└─────────────────────────────────────┘
```

### AI Provider Failover Chain

```
User Question
      │
      ▼
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Claude   │────▶│  OpenAI  │────▶│   Grok   │
│ (Primary) │fail │(Fallback)│fail │(Fallback)│
└──────────┘     └──────────┘     └──────────┘
```

---

## 5. Backend — API & Modules

### Module Breakdown

| Module | File | Lines | Description |
|--------|------|-------|-------------|
| Main API | `app/main.py` | ~1,320 | All FastAPI route definitions |
| AI Engine | `app/chat/ai_engine.py` | ~2,200 | Hybrid SQL+RAG engine, synonym map, routing, validation |
| Semantic Layer | `app/chat/semantic.py` | ~1,700 | Pattern-based SQL generation (no AI needed) |
| Chat Engine | `app/chat/engine.py` | ~200 | Session management, answer/stream orchestration |
| Learning Memory | `app/chat/memory.py` | ~360 | Q&A storage, feedback tracking, similar query lookup |
| RAG Engine | `app/chat/rag.py` | ~200 | Vector search via Qdrant + OpenAI embeddings |
| DB Sync | `app/database/sync.py` | ~400 | Salesforce → PostgreSQL incremental sync |
| DB Query | `app/database/query.py` | ~100 | PostgreSQL query execution layer |
| DB Models | `app/database/models.py` | ~500 | SQLAlchemy ORM models for all tables |
| Auth | `app/auth_users.py` | ~300 | JWT auth, user CRUD, password hashing |
| Reports | `app/reports.py` | ~350 | Report builder with AI-suggest |
| Schedules | `app/schedules.py` | ~300 | Automated report scheduling |
| Alerts | `app/alerts.py` | ~250 | Threshold-based data alerts |
| PDF Export | `app/pdf_export.py` | ~200 | Branded PDF generation |
| Analytics | `app/analytics.py` | ~200 | Predictive analytics queries |
| Comparisons | `app/compare.py` | ~150 | Period-over-period comparisons |
| Connectors | `app/connectors/` | ~500 | Gmail, Google, Slack, OpenAI, Grok integrations |

---

## 6. AI Intelligence Pipeline

### 4-Layer Intelligence System

The AI engine uses a sophisticated 4-layer approach to ensure accurate answers:

#### Layer 1: Synonym & Slang Expansion
Preprocesses every question before any handler sees it. Handles:

| Category | Examples |
|----------|---------|
| Status synonyms | "bench" → "in market", "placed" → "project started", "vc" → "verbal confirmation" |
| Entity abbreviations | "subs" → "submissions", "ints" → "interviews", "stds" → "students" |
| Time shortcuts | "ytd" → "yesterday", "lw" → "last week", "tm" → "this month" |
| Tech normalization | "dotnet" → ".NET", "powerbi" → "PowerBI", "sf" → "salesforce" |
| Visa mapping | "h1b" → "H1", "h4ead" → "H4 EAD", "green card" → "GC" |
| Common misspellings | "submisions" → "submissions", "interveiw" → "interview", "studnet" → "student" |
| Conversational slang | "gimme" → "give me", "wanna" → "want to", "gonna" → "going to" |

**60+ synonym mappings** + **11 regex abbreviation patterns** (e.g., `hw many` → `how many`, `#  of` → `number of`)

#### Layer 2: SQL Validation & Auto-Fix
After the AI generates SQL, multiple validation steps catch common mistakes:

| Fix Type | Example |
|----------|---------|
| Wrong field names | `Email__c` → `Marketing_Email__c`, `Interview_Date__c` → `Interview_Date1__c` |
| Missing JOINs | Adds Student→Manager JOIN when Interviews needs BU name |
| Wrong date fields | `CreatedDate` → `Submission_Date__c` on Submissions table |
| Unquoted identifiers | Adds double-quotes to Salesforce table/column names |
| Lowercase tables | `students` → `"Student__c"` |
| Picklist case errors | `'in market'` → `'In Market'`, `'bench'` → `'In Market'` |
| Picklist value validation | Validates WHERE clause values against live DB picklist values |

#### Layer 3: Fuzzy Cache (Self-Learning)
Before calling the AI, checks `learning_memory` for similar verified questions:

- Uses **SequenceMatcher** (character-level similarity) + **Jaccard index** (word overlap)
- Combined score: `60% sequence + 40% Jaccard`
- Thresholds:
  - Verified (`feedback='good'`): reuse at 75%+ similarity
  - Unverified: reuse at 92%+ similarity
- Skips AI entirely when a match is found — instant response

#### Layer 4: Answer Verification
After the AI formats the answer, cross-checks against actual DB data:

| Check | What It Does |
|-------|-------------|
| Count verification | If AI says "**500 students**" but DB returned 487, corrects to 487 |
| Breakdown totals | Verifies GROUP BY totals add up correctly |
| LIMIT correction | If LIMIT 2000 returned 2000 but true count is 5,000, corrects the headline |
| Name verification | Checks that names in the answer actually appear in the query results |
| Case correction | Fixes name casing to match actual DB values |

### SOQL Prompt Engineering

The AI receives a comprehensive system prompt (~700 lines) including:
- Exact table/column names with double-quoting rules
- All picklist values with exact spelling
- User term → status mappings ("bench" → 'In Market')
- PostgreSQL date function reference
- JOIN patterns for cross-table queries
- 12+ worked SQL examples
- Field name warnings (common mistakes to avoid)
- Dynamic live picklist values from DB
- Past successful queries as few-shot examples (from learning memory)

---

## 7. Semantic Query Layer

The semantic layer handles **97.7%** of questions without any AI call, guaranteeing deterministic, fast, and correct responses.

### Query Types Handled

| Level | Category | Examples |
|-------|----------|---------|
| L1 | Counts & Lists | "how many students", "show all submissions" |
| L2 | Status/Tech/Visa Filters | "in market java students", "H1 visa students" |
| L3 | Date Ranges | "today submissions", "last week interviews", "last 30 days" |
| L4 | BU-Specific | "students under Divya", "Divya BU submissions" |
| L5 | Group By & Top N | "submissions BU wise", "top 5 BUs", "average days in market" |
| L6 | Person Lookups | "details of Sai Ganesh", "who is Chinnamsetty" |
| L7 | Multi-Filter | "H1 java students under Divya this month" |
| L8 | Cross-Table | "students with no interviews in 2 weeks", "no submissions 3 days" |
| L9 | Reports & Analytics | "monthly BU report", "leaderboard", "conversion rate", "month comparison" |
| L10 | Financial & Edge Cases | "expenses", "placement cost", "payroll", "interview amounts" |
| MSG | Message Generation | "draft message for students", "write follow-up email" |

### Pattern Coverage

- **16 status mappings** (In Market, Exit, Verbal Confirmation, etc.)
- **18 technology keywords** (Java, .NET, DevOps, Data Engineering, etc.)
- **8 visa types** (H1, OPT, STEM, CPT, GC, H4 EAD, L2, USC)
- **12 time range patterns** (today, yesterday, this/last week/month, last N days/weeks/months)
- **8 group-by fields** (BU, technology, visa, status, type, offshore manager, recruiter)
- **13 report pattern templates** (monthly sub+int+conf, weekly reports, confirmations, pre-marketing, expenses, payroll, etc.)

### AI-Powered Message Generation

When the semantic layer detects a message/email request:
1. Identifies audience (students, BU members, team)
2. Detects tone (professional, motivational, urgent)
3. Fetches relevant real data from DB
4. Passes data to AI with strict "only use provided data" prompt
5. Falls back to pre-built templates if AI fails

---

## 8. Database Architecture

### Salesforce-Mirrored Tables (18 tables)

| Table | Description | Key Fields |
|-------|-------------|------------|
| `Student__c` | Candidates/students | Name, Status, Technology, Visa, Days in Market, Manager, Phone, Email |
| `Submissions__c` | Job submissions | Student Name, BU Name, Client, Submission Date, Rate |
| `Interviews__c` | Interview records | Student, Type, Status, Amount, Bill Rate, Interview Date |
| `Manager__c` | BU managers | Name, Type (Lead/Manager), Active, Expenses, Students Count |
| `Job__c` | Active placements | Student, Pay Rate, Bill Rate, Profit, Project Type, Start/End Date |
| `Employee__c` | Internal staff | Name, Department, Role |
| `BU_Performance__c` | BU metrics | Manager, Date, Performance scores |
| `Organization__c` | Companies | Name, Industry, Size |
| `Account` | Salesforce accounts | Standard Salesforce fields |
| `Contact` | Salesforce contacts | Standard Salesforce fields |
| + 8 more | Various Salesforce objects | BS__c, Tech_Support__c, New_Student__c, etc. |

### Application Tables

| Table | Description | Key Fields |
|-------|-------------|------------|
| `users` | App user accounts | username (PK), password_hash, role (admin/user), created_at |
| `learning_memory` | Self-learning Q&A store | question, sql_query, answer, route, feedback, used_count |
| `chat_sessions` | Chat sessions | user, title, pinned, created_at |
| `chat_messages` | Messages in sessions | session_id, role, content, soql, data (JSON) |
| `chat_history` | Per-user query log | username, question, answer, sql_query, route |
| `audit_log` | Security audit trail | action, username, ip_address, details, timestamp |
| `sync_log` | Sync tracking | object_name, last_sync, records_synced, status |

### Sync Engine

- **Incremental sync** every 15 minutes (configurable)
- Uses `LastModifiedDate` to fetch only changed records
- PostgreSQL upsert (`INSERT ... ON CONFLICT DO UPDATE`)
- Tracks sync status per object in `sync_log`
- Manual full sync available via admin API

### Vector Database (Qdrant)

- Local Qdrant instance at `backend/data/qdrant/`
- Collection: `salesforce_rag`
- Embedding model: OpenAI `text-embedding-3-small` (1536 dimensions)
- Used for semantic similarity search (RAG path)

---

## 9. Frontend Application

### Pages

| Page | Component | Description |
|------|-----------|-------------|
| Chat | `App.jsx` (inline) | Main conversational interface with streaming, markdown rendering, suggestion chips |
| Dashboard | `Dashboard.jsx` | Live KPI cards (students, submissions, interviews) + status bar chart |
| Analytics | `AnalyticsPage.jsx` | AI-generated analytics cards with bar/pie/line charts |
| Schema Map | `SchemaMap.jsx` | Visual Salesforce object relationship explorer |
| Reports | `ReportBuilder.jsx` | Drag-and-drop report builder with AI-suggest |
| Schedules | `SchedulesPage.jsx` | Automated report scheduling (daily/weekly/monthly) |
| Connectors | `ConnectorsPage.jsx` | Third-party integration management (Gmail, Slack) |
| Files | `FilesPage.jsx` | File upload management (PDF, CSV, XLSX) |
| Audit Log | `AuditPage.jsx` | Admin-only action history viewer |
| Comparisons | `ComparisonPage.jsx` | Period-over-period data comparison |
| Alerts | `AlertsPage.jsx` | Threshold-based data alert rules |
| Notes | `NotesPage.jsx` | Per-record data annotations |

### Key UI Features

- **Streaming Responses** — Word-by-word SSE streaming with typing indicator
- **Inline Data Tables** — Sortable, paginated tables for query results
- **Auto Charts** — Automatic bar/pie/line chart selection based on data shape
- **Message Actions** — Thumbs up/down feedback, copy, CSV/XLSX/PDF export per message
- **Session Management** — Chat sessions with search, pin, rename, delete
- **Dark/Light Theme** — Toggle with CSS variable system, persisted to localStorage
- **Multi-Language** — Internationalization support via `i18n.jsx`
- **Responsive Design** — Mobile-friendly sidebar + chat layout

---

## 10. API Reference

### Authentication Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| POST | `/api/auth/login` | None | Login, returns JWT (rate: 10/min) |
| POST | `/api/auth/register` | Admin | Create user |
| GET | `/api/auth/me` | JWT | Current user profile |
| POST | `/api/auth/change-password` | JWT | Change own password |
| GET | `/api/auth/users` | Admin | List all users |
| DELETE | `/api/auth/users/{username}` | Admin | Delete user |

### Chat Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| POST | `/api/chat` | Optional | Ask a question (rate: 30/min) |
| POST | `/api/chat/stream` | Optional | SSE streaming response |
| GET | `/api/welcome` | None | Welcome message + suggestions |
| POST | `/api/feedback` | Optional | Thumbs up/down feedback |
| GET | `/api/learning-stats` | None | Learning memory statistics |
| GET | `/api/history` | JWT | User's question history |

### Session Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/sessions` | JWT | List sessions (supports `?q=` search) |
| GET | `/api/sessions/{id}` | JWT | Load session with messages |
| DELETE | `/api/sessions/{id}` | JWT | Delete session |
| POST | `/api/sessions/{id}/pin` | JWT | Toggle pin |

### Dashboard & Analytics

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/dashboard` | None | Live KPI metrics |
| GET/POST | `/api/dashboard/config` | JWT | Widget layout |
| GET | `/api/dashboard/widget` | JWT | Run single widget query |
| GET | `/api/analytics/predictive` | JWT | Predictive analytics |
| POST | `/api/analytics/generate` | JWT | AI-generate analytics cards |
| POST | `/api/analytics/insight` | JWT | AI executive summary |

### Export Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/export?q=SQL&format=csv` | JWT | Export as CSV or XLSX |
| POST | `/api/export/pdf` | Optional | Generate branded PDF |

### Report & Schedule Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET/POST | `/api/reports` | JWT | List / create reports |
| GET/PATCH/DELETE | `/api/reports/{id}` | JWT | CRUD on reports |
| POST | `/api/reports/{id}/run` | JWT | Execute report |
| POST | `/api/reports/suggest` | JWT | AI-suggest report config |
| GET/POST | `/api/schedules` | JWT | List / create schedules |
| POST | `/api/schedules/{id}/run` | JWT | Run schedule now |

### Data & Schema Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/overview` | None | Object summary |
| GET | `/api/schema/objects` | None | Full schema with fields |
| GET | `/api/schema/relationships` | None | Relationship map |
| POST | `/api/refresh-schema` | None | Re-discover schema |
| GET | `/api/sync/status` | JWT | Sync status |
| POST | `/api/sync/run` | Admin | Trigger manual sync |

### Alert, Annotation, Upload Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET/POST | `/api/alerts` | JWT | CRUD alert rules |
| POST | `/api/alerts/check` | JWT | Check all rules now |
| GET/POST | `/api/annotations` | JWT | CRUD data notes |
| POST/GET | `/api/uploads` | JWT | Upload/list files |

### Connector Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/connectors` | JWT | List connector statuses |
| GET | `/api/connectors/gmail/auth` | Token | Initiate Gmail OAuth |
| POST | `/api/connectors/gmail/send` | JWT | Send email via Gmail |
| GET | `/api/ai/providers` | JWT | List configured AI providers |

### Health

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/health` | None | System health + AI providers + learning stats |

---

## 11. Third-Party Integrations

| Service | Status | Purpose |
|---------|--------|---------|
| **Salesforce** | Production | Primary CRM data source (OAuth2 client_credentials) |
| **Claude (Anthropic)** | Production | Primary AI for SQL generation + answer formatting |
| **OpenAI GPT-4o** | Production | Fallback AI + text embeddings for RAG |
| **Grok (xAI)** | Production | Secondary fallback AI |
| **PostgreSQL** | Production | Local data mirror + app persistence |
| **Qdrant** | Production | Local vector database for semantic search |
| **Gmail** | Production | Send emails/reports via Google OAuth2 |
| **Google Sheets** | Planned | Export data to spreadsheets |
| **Google Calendar** | Planned | Schedule reminders |
| **Slack** | Planned | Team notifications |

---

## 12. Security & Authentication

### Authentication Flow

1. User logs in via `/api/auth/login` with username/password
2. Server validates against bcrypt-hashed password in PostgreSQL
3. Returns JWT token (HS256, 24-hour expiry)
4. Frontend stores JWT in `localStorage`
5. All subsequent API calls include `Authorization: Bearer <token>`

### Security Measures

| Measure | Implementation |
|---------|---------------|
| Password Hashing | bcrypt via passlib |
| Token Auth | JWT (HS256) with 24h expiry |
| Password Policy | Min 8 chars, 1 uppercase, 1 digit |
| Rate Limiting | Login: 10/min, Chat: 30/min (slowapi) |
| SQL Injection Prevention | Read-only SQL enforcement (blocks INSERT/UPDATE/DELETE/DROP) |
| Audit Logging | All actions logged with username, IP, timestamp |
| CORS | Configurable allowed origins |
| Write Protection | All generated SQL is read-only (SELECT only) |

### Role-Based Access

| Role | Permissions |
|------|------------|
| `admin` | Full access: user management, sync control, audit log, all features |
| `user` | Chat, dashboard, reports, exports, sessions, connectors |

---

## 13. Testing & Quality

### Test Harness

- **11,754 unique test questions** across 30+ categories
- **97.7% pass rate** on the semantic query layer
- Categories tested:
  - Basic counts and lists
  - Status/technology/visa filters
  - Date ranges (today, yesterday, this/last week/month, last N days)
  - BU-specific queries
  - Group-by and top-N
  - Person name lookups
  - Multi-filter combinations
  - Cross-table queries
  - Reports and analytics
  - Financial queries
  - Message generation
  - Conversational/slang
  - Typos and abbreviations
  - Complex natural language

### Test Features

- **Checkpoint/Resume** — Test progress saved; can resume from where it left off
- **Retry Failed** — `--retry-failed` flag to only re-test previously failed questions
- **Rate Limit Handling** — Automatic backoff when API rate limits are hit
- **HTML Report** — Generates detailed HTML report with pass/fail per question
- **Self-Learning** — Saves successful Q&A pairs to `learning_memory` for future AI improvement

### Validation Strategy

| Layer | What's Validated |
|-------|-----------------|
| Semantic Layer | Returns correct SQL + non-empty results for every test question |
| AI SQL Generation | Fields validated against schema before execution |
| Picklist Values | WHERE clause values checked against live DB values |
| Answer Counts | AI-generated counts cross-checked against actual DB results |
| Name Accuracy | Names in AI answers verified against query result data |

---

## 14. Deployment

### Prerequisites

- Python 3.11+
- Node.js 18+
- PostgreSQL 15+
- Salesforce org with API access (client_credentials OAuth)
- At least one AI API key (Anthropic, OpenAI, or xAI)

### Setup Steps

```bash
# 1. Clone repository
git clone <repo-url>
cd salesforce-chat-complete-v2

# 2. Backend setup
cd backend
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials

# 3. Initialize database
python -m scripts.refresh_schema    # Discover Salesforce schema
# Database tables are auto-created on first startup

# 4. Frontend setup
cd ../frontend
npm install
npm run build                       # Build production bundle

# 5. Start server
cd ../backend
uvicorn app.main:app --host 0.0.0.0 --port 8000

# The app serves both API and frontend static files on port 8000
```

### Environment Variables (Required)

```env
# Salesforce
SALESFORCE_INSTANCE_URL=https://your-instance.salesforce.com
SALESFORCE_CLIENT_ID=your_client_id
SALESFORCE_CLIENT_SECRET=your_client_secret

# AI (at least one required)
ANTHROPIC_API_KEY=sk-ant-...
OPENAI_API_KEY=sk-...
GROK_API_KEY=xai-...

# Database
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/dbname

# Auth
JWT_SECRET=your_64_char_hex_secret
```

### Production Deployment

The `deploy.sh` script handles:
1. Git pull latest code
2. Install Python dependencies
3. Build frontend
4. Restart the application

---

## 15. Configuration Reference

### `/backend/app/config.py` — Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `SALESFORCE_INSTANCE_URL` | — | Salesforce org URL |
| `SALESFORCE_CLIENT_ID` | — | OAuth2 client ID |
| `SALESFORCE_CLIENT_SECRET` | — | OAuth2 client secret |
| `DATABASE_URL` | `postgresql+asyncpg://...localhost/fyxo` | PostgreSQL connection string |
| `JWT_SECRET` | — | Secret for JWT token signing |
| `ANTHROPIC_API_KEY` | — | Claude API key |
| `OPENAI_API_KEY` | — | OpenAI API key |
| `GROK_API_KEY` | — | xAI Grok API key |
| `CLAUDE_MODEL` | `claude-sonnet-4-20250514` | Claude model ID |
| `OPENAI_MODEL` | `gpt-4o` | OpenAI model ID |
| `GROK_MODEL` | `grok-3-mini-fast` | Grok model ID |
| `EMBEDDING_MODEL` | `text-embedding-3-small` | Embedding model for RAG |
| `EMBEDDING_DIMENSIONS` | `1536` | Embedding vector size |
| `SYNC_INTERVAL_MINUTES` | `15` | Salesforce sync interval |
| `CORS_ORIGINS` | `["http://localhost:5173"]` | Allowed CORS origins |

---

## 16. Directory Structure

```
salesforce-chat-complete-v2/
├── README.md
├── deploy.sh
├── PROJECT_DOCUMENTATION.md        ← This file
│
├── backend/
│   ├── .env                        # Live credentials
│   ├── .env.example                # Template
│   ├── requirements.txt            # Python dependencies
│   ├── app/
│   │   ├── main.py                 # FastAPI app + all route definitions
│   │   ├── config.py               # Settings (reads .env)
│   │   ├── auth_users.py           # JWT auth + user management
│   │   ├── alerts.py               # Alert rule engine
│   │   ├── analytics.py            # Predictive analytics
│   │   ├── annotations.py          # Data notes/tags
│   │   ├── audit.py                # Audit logging
│   │   ├── compare.py              # Period comparisons
│   │   ├── dashboard_config.py     # Widget layouts
│   │   ├── pdf_export.py           # PDF generation
│   │   ├── reports.py              # Report builder
│   │   ├── schedules.py            # Scheduled reports
│   │   ├── uploads.py              # File uploads
│   │   │
│   │   ├── chat/
│   │   │   ├── ai_engine.py        # 4-layer AI intelligence pipeline
│   │   │   ├── engine.py           # Chat session manager
│   │   │   ├── memory.py           # Self-learning memory (PostgreSQL)
│   │   │   ├── rag.py              # Vector search (Qdrant)
│   │   │   ├── semantic.py         # Semantic query layer (97.7% coverage)
│   │   │   └── sessions.py         # Chat session persistence
│   │   │
│   │   ├── connectors/
│   │   │   ├── gmail.py            # Gmail OAuth + send
│   │   │   ├── google_oauth.py     # Shared Google OAuth flow
│   │   │   ├── sheets.py           # Google Sheets
│   │   │   ├── calendar.py         # Google Calendar
│   │   │   ├── slack.py            # Slack
│   │   │   ├── openai_conn.py      # OpenAI connection test
│   │   │   └── grok.py             # Grok connection test
│   │   │
│   │   ├── database/
│   │   │   ├── engine.py           # SQLAlchemy async engine
│   │   │   ├── models.py           # ORM models (18 SF tables + app tables)
│   │   │   ├── sync.py             # Salesforce → PostgreSQL sync
│   │   │   ├── query.py            # SQL query executor
│   │   │   └── analytics_sql.py    # Analytics via PostgreSQL
│   │   │
│   │   ├── salesforce/
│   │   │   ├── auth.py             # OAuth2 client_credentials
│   │   │   ├── schema.py           # Schema discovery + cache
│   │   │   └── soql_executor.py    # SOQL via REST API
│   │   │
│   │   └── models/
│   │       └── schemas.py          # Pydantic request/response models
│   │
│   ├── scripts/
│   │   ├── refresh_schema.py       # Discover Salesforce schema
│   │   └── sync_rag.py             # Build Qdrant embeddings index
│   │
│   ├── tests/
│   │   └── test_semantic_5000.py   # 11,754-question test harness
│   │
│   └── data/
│       ├── schema_cache.json       # Cached Salesforce schema
│       ├── learning_memory.json    # Fallback JSON learning store
│       ├── qdrant/                 # Qdrant vector DB files
│       └── users/                  # Per-user data files
│
└── frontend/
    ├── package.json                # React 18 + Recharts + Vite
    ├── vite.config.js              # Dev server config
    ├── index.html
    ├── dist/                       # Built production bundle
    └── src/
        ├── main.jsx                # React entry point
        ├── App.jsx                 # Main shell (sidebar + chat + routing)
        ├── index.css               # Design system (CSS variables, themes)
        ├── services/
        │   └── api.js              # API client with JWT auth
        ├── hooks/
        │   ├── useChat.js          # Chat state management
        │   └── useToast.jsx        # Toast notifications
        ├── utils/
        │   ├── export.js           # CSV download + clipboard
        │   └── i18n.jsx            # Internationalization
        └── components/
            ├── Dashboard.jsx       # KPI cards + charts
            ├── DataChart.jsx       # Auto chart selection
            ├── DataTable.jsx       # Sortable/paginated tables
            ├── AnalyticsPage.jsx   # AI analytics cards
            ├── ReportBuilder.jsx   # Report builder
            ├── SchedulesPage.jsx   # Schedule management
            ├── ConnectorsPage.jsx  # Integration management
            ├── AuditPage.jsx       # Audit log viewer
            ├── SchemaMap.jsx       # Schema explorer
            ├── AlertsPage.jsx      # Alert rules
            ├── ComparisonPage.jsx  # Period comparisons
            ├── NotesPage.jsx       # Data annotations
            ├── FilesPage.jsx       # File uploads
            ├── SessionList.jsx     # Chat session sidebar
            ├── MessageActions.jsx  # Per-message actions
            ├── SOQLBlock.jsx       # SQL viewer
            ├── UsersModal.jsx      # User management
            ├── EmailModal.jsx      # Gmail compose
            └── ScheduleModal.jsx   # Schedule editor
```

---

## Summary of Recent Enhancements (Current Sprint)

| Enhancement | Impact |
|------------|--------|
| AI-powered message generation with real data grounding | Composes emails/notifications using actual DB data instead of generic templates |
| 4-layer intelligence system (synonym → cache → validation → verification) | Dramatically improved accuracy for AI-handled questions |
| Expanded test harness to 11,754 questions | Comprehensive coverage validation across all query types |
| Fixed ambiguous column errors in JOIN queries | Eliminated runtime SQL errors for BU-filtered queries |
| Fixed false positive BU name / technology detection | Prevented "provide" from matching "Data Engineering", "Full Stack" from matching as BU name |
| Picklist value validation against live DB | Catches AI spelling mistakes before SQL execution |
| Fuzzy cache with SequenceMatcher + Jaccard scoring | Instant responses for similar previously-answered questions |
| Synonym expansion for 60+ slang/abbreviation patterns | Handles "hw many stds on bench" → correct SQL |
| Answer verification with breakdown total validation | Ensures AI-reported numbers match actual DB data |
| Interview amounts per student handler | New semantic pattern for cross-table amount queries |
| Skip conceptual questions for AI handling | "What are the interview criteria?" no longer dumps all 35K records |

---

*Document generated: April 22, 2026*
*Fyxo Chat v2.0 — Built by Sai Ganesh Boyala*
