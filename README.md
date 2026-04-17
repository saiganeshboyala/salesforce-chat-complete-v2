# Salesforce Data Chat

AI-powered conversational interface for your Salesforce CRM data. Ask questions in natural language, get real-time answers with charts, tables, and downloadable reports.

## Architecture

```
┌──────────────┐     ┌───────────────┐     ┌─────────────────┐
│   React UI   │────▶│  FastAPI       │────▶│  Salesforce     │
│   (Browser)  │◀────│  Backend      │◀────│  REST API       │
└──────────────┘     │               │     │  (SOQL queries) │
                     │  ┌───────────┐│     └─────────────────┘
                     │  │ AI Engine ││
                     │  │           ││     ┌─────────────────┐
                     │  │ Router    │├────▶│  Claude / Grok  │
                     │  │ SOQL Gen  ││◀────│  / OpenAI API   │
                     │  │ RAG       ││     └─────────────────┘
                     │  │ Learning  ││
                     │  └───────────┘│
                     └───────────────┘
```

### How It Works

1. **User asks a question** in natural language
2. **Router** classifies it: SOQL (exact data), RAG (analysis), or BOTH
3. **SOQL path**: AI writes a Salesforce query → Salesforce executes it → exact results
4. **RAG path**: Semantic search finds similar records → AI analyzes patterns
5. **Self-learning**: Every Q&A pair is saved. Past successful queries improve future ones
6. **Answer**: AI formats the results as a clean, natural language response

### Routes

| Route | When Used | Example |
|-------|-----------|---------|
| **SOQL** | Counts, lists, filters, aggregations | "How many students in market?" |
| **RAG** | Pattern analysis, risk assessment, fuzzy search | "Which students are at risk?" |
| **BOTH** | Complex questions needing data + analysis | "Analyze the conversion funnel" |

## Features

### Chat
- Real-time SOQL queries against Salesforce (no sync delay)
- Auto-generated charts (bar/pie) for GROUP BY results
- Collapsible SOQL query viewer with copy button
- CSV download button on every answer with data
- Thumbs up/down feedback (teaches the system)
- Copy answer to clipboard
- Markdown rendering (tables, bold, code blocks)
- Conversation memory (follows up on previous questions)
- Suggestion chips for empty chat

### Dashboard
- Live metrics cards (Total Students, In Market, Verbal Confirmations, etc.)
- Status breakdown bar chart
- Clickable cards → runs the query in chat

### Self-Learning
- Every question + SOQL + answer is saved
- Similar past queries are shown to AI as examples
- Thumbs up/down prioritizes good patterns
- System gets smarter with every interaction

### AI Providers (fallback chain)
```
Claude → Grok → OpenAI
```
If one fails (no credits, rate limit), the next one handles it automatically.

## Project Structure

```
h/
├── backend/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── config.py                  # Settings from .env
│   │   ├── main.py                    # FastAPI app + all endpoints
│   │   ├── salesforce/
│   │   │   ├── __init__.py
│   │   │   ├── auth.py                # OAuth2 client_credentials
│   │   │   ├── schema.py              # Auto-discover objects/fields
│   │   │   └── soql_executor.py       # Execute SOQL (READ ONLY)
│   │   ├── chat/
│   │   │   ├── __init__.py
│   │   │   ├── ai_engine.py           # Hybrid SOQL + RAG engine
│   │   │   ├── engine.py              # Chat session manager
│   │   │   ├── rag.py                 # OpenAI embeddings + Qdrant
│   │   │   └── memory.py              # Self-learning memory
│   │   └── models/
│   │       ├── __init__.py
│   │       └── schemas.py             # Pydantic request/response models
│   ├── scripts/
│   │   ├── refresh_schema.py          # Discover Salesforce schema
│   │   └── sync_rag.py                # Build RAG index (optional)
│   ├── data/                          # Cached schema + learning memory
│   ├── requirements.txt
│   ├── .env.example
│   └── .env                           # Your credentials (not committed)
├── frontend/
│   ├── index.html
│   ├── package.json
│   ├── vite.config.js
│   └── src/
│       ├── main.jsx                   # Entry point
│       ├── index.css                  # Design system (CSS variables)
│       ├── App.jsx                    # Main app shell + sidebar + chat
│       ├── services/
│       │   └── api.js                 # Centralized API calls
│       ├── hooks/
│       │   └── useChat.js             # Chat state management
│       ├── utils/
│       │   └── export.js              # CSV download + clipboard
│       └── components/
│           ├── DataChart.jsx          # Auto bar/pie charts
│           ├── SOQLBlock.jsx          # SOQL query viewer
│           ├── MessageActions.jsx     # Copy, download, feedback
│           └── Dashboard.jsx          # Metrics cards + chart
└── README.md
```

## Setup

### Prerequisites
- Python 3.11 or 3.12 (NOT 3.14 — pydantic doesn't support it yet)
- Node.js 18+
- Salesforce Connected App with client_credentials grant
- At least one AI API key (OpenAI, Grok, or Claude)

### 1. Backend Setup

```bash
cd h/backend

# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (Mac/Linux)
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure credentials
copy .env.example .env     # Windows
cp .env.example .env        # Mac/Linux

# Edit .env with your credentials
notepad .env                # Windows
nano .env                   # Mac/Linux
```

### 2. Configure .env

```env
# Salesforce (required)
SALESFORCE_INSTANCE_URL=https://your-instance.salesforce.com
SALESFORCE_LOGIN_URL=https://your-instance.salesforce.com
SALESFORCE_CLIENT_ID=your_connected_app_client_id
SALESFORCE_CLIENT_SECRET=your_connected_app_client_secret

# AI API Keys (at least one required)
ANTHROPIC_API_KEY=sk-ant-...          # Claude
GROK_API_KEY=xai-...                  # Grok (x.ai)
OPENAI_API_KEY=sk-...                 # OpenAI

# AI Models (optional — defaults shown)
CLAUDE_MODEL=claude-sonnet-4-20250514
GROK_MODEL=grok-3-mini-fast
OPENAI_MODEL=gpt-4o

# Embedding for RAG (optional)
EMBEDDING_MODEL=text-embedding-3-large
EMBEDDING_DIMENSIONS=3072

# Auto schema refresh (optional, 24hr format)
AUTO_SYNC_TIME=02:00
```

### 3. Discover Salesforce Schema

```bash
python -m scripts.refresh_schema
```

This takes ~30 seconds. It discovers all objects and fields in your Salesforce org and caches them locally.

### 4. Build RAG Index (Optional)

```bash
python -m scripts.sync_rag
```

This fetches sample records and creates embeddings for semantic search. Takes ~2 minutes. Costs ~$0.01 for OpenAI embeddings. The app works without this — SOQL handles most questions.

### 5. Build Frontend

```bash
cd ../frontend
npm install
npm run build
```

### 6. Start the App

```bash
cd ../backend
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000

## Development Mode

For hot-reload during development:

```bash
# Terminal 1: Backend (auto-restart on changes)
cd h/backend
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2: Frontend (hot module replacement)
cd h/frontend
npm run dev
```

Frontend dev server runs on http://localhost:5173 and proxies /api to the backend.

## API Reference

### Chat

```
POST /api/chat
Body: { "session_id": "abc123", "question": "How many students?" }
Response: {
  "answer": "You have 2,786 students...",
  "soql": "SELECT COUNT() FROM Student__c",
  "data": { "totalSize": 2786, "records": [...], "route": "SOQL" }
}
```

### Feedback (Self-Learning)

```
POST /api/feedback
Body: { "question": "How many students?", "feedback": "good" }
Response: { "status": "saved" }
```

### Dashboard

```
GET /api/dashboard
Response: {
  "total_students": 2786,
  "students_in_market": 942,
  "verbal_confirmations": 69,
  "project_started": 541,
  "exits": 1167,
  "status_breakdown": [...]
}
```

### Export CSV

```
GET /api/export?q=SELECT Name FROM Student__c LIMIT 10
Response: CSV file download
```

### Schema Overview

```
GET /api/overview
Response: { "total_objects": 162, "total_records": 32939, "objects": {...} }
```

### Health Check

```
GET /api/health
Response: {
  "status": "healthy",
  "mode": "hybrid_soql_rag_learning",
  "objects": 162,
  "ai_providers": ["Grok (grok-3-mini-fast)", "OpenAI (gpt-4o)"],
  "learning": { "total_interactions": 47, "good_feedback": 12 }
}
```

### Refresh Schema

```
POST /api/refresh-schema
Response: { "status": "refreshed", "objects": 162 }
```

## Deployment (EC2)

### Minimum Requirements
- **Instance**: t3.small (2 vCPU, 2GB RAM) — enough for this architecture
- **OS**: Ubuntu 24.04
- **Storage**: 10GB
- **Ports**: 8000 (or 80 with nginx)

### Deploy Steps

```bash
# SSH into EC2
ssh -i key.pem ubuntu@your-ec2-ip

# Install dependencies
sudo apt update && sudo apt install -y python3.11 python3.11-venv nodejs npm

# Clone
git clone https://github.com/saiganeshboyala/salesforce-chat.git
cd salesforce-chat/h

# Backend
cd backend
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
nano .env  # add credentials

# Schema
python -m scripts.refresh_schema

# Frontend
cd ../frontend
npm install && npm run build

# Start with systemd (auto-restart)
cd ../backend
sudo tee /etc/systemd/system/sfchat.service << EOF
[Unit]
Description=Salesforce Data Chat
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/salesforce-chat/h/backend
ExecStart=/home/ubuntu/salesforce-chat/h/backend/.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable sfchat
sudo systemctl start sfchat
```

### With Nginx (port 80)

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## Cost

| Component | Monthly Cost |
|-----------|-------------|
| EC2 t3.small | ~$15 (₹1,200) |
| AI API (100 questions/day) | ~$15 (₹1,200) |
| OpenAI Embeddings (RAG sync) | ~$0.01 per sync |
| Salesforce API | Free (included in license) |
| **Total** | **~$30/month (₹2,500)** |

### Cost per Question
| Provider | Cost per Question |
|----------|------------------|
| Grok 3 Mini Fast | ~₹0.05 |
| GPT-4o Mini | ~₹0.05 |
| GPT-4o | ~₹0.10 |
| Claude Sonnet 4 | ~₹0.40 |
| Claude Opus 4.6 | ~₹2.00 |

## Switching to Production Salesforce

Change 3 values in .env:

```env
SALESFORCE_INSTANCE_URL=https://your-company.my.salesforce.com
SALESFORCE_LOGIN_URL=https://your-company.my.salesforce.com
SALESFORCE_CLIENT_ID=your_production_client_id
SALESFORCE_CLIENT_SECRET=your_production_client_secret
```

Then refresh the schema:
```bash
python -m scripts.refresh_schema
```

Everything else works automatically — the schema discovery finds all objects and fields.

## Security

- **Read-only**: Only SELECT queries are allowed. INSERT/UPDATE/DELETE are blocked at code level.
- **No data stored permanently**: Only schema (field names) is cached. Record data is queried live and never saved.
- **API keys**: Stored in .env, never committed to git.
- **Learning memory**: Saves questions and SOQL queries only, not actual record data.

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Python 3.14 build errors | Use Python 3.11 or 3.12 |
| Claude "credit balance too low" | Add credits at console.anthropic.com or remove ANTHROPIC_API_KEY from .env |
| "Schema not loaded" | Run `python -m scripts.refresh_schema` |
| SOQL errors in answers | Delete `data/learning_memory.json` to clear bad learned patterns |
| Frontend not loading | Run `npm run build` in frontend folder |
| Port 8000 already in use | Kill old process: `netstat -ano | findstr 8000` then `taskkill /PID <pid> /F` |

## License

Proprietary — Incresol Technologies
