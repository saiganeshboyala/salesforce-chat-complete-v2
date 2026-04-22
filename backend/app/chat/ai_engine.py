"""
Hybrid AI Engine with Self-Learning

Every question + answer is saved. The AI uses past successful
queries as examples to write better SQL over time.
Users can thumbs-up/down answers to train it.
"""
import json, logging, re, time
from difflib import SequenceMatcher
from app.config import settings
from app.salesforce.schema import schema_to_prompt, get_schema
from app.database.query import execute_query
from app.chat.rag import search as rag_search, is_indexed
from app.chat.memory import save_interaction, get_learning_examples_prompt, find_similar_past_queries
from app.chat.semantic import handle_semantic_query
from app.chat.query_cache import find_cached_query, cache_query, init_cache as init_query_cache

logger = logging.getLogger(__name__)


def _sanitize_sql_input(value):
    """Strip SQL injection characters from values interpolated into SQL strings."""
    if not value:
        return value
    return re.sub(r"[;'\"\-\-\\/*]", "", str(value)).strip()


# ── Layer 1: Synonym / Slang Expansion ─────────────────────────────
_SYNONYM_MAP = {
    # Status synonyms (only multi-word or unambiguous single words)
    "on bench": "in market", "benched": "in market",
    "on market": "in market",
    "got placed": "project started",
    "started project": "project started",
    "got confirmed": "verbal confirmation",
    "vc": "verbal confirmation",
    "exited": "exit", "quit": "exit",
    "premarketing": "pre marketing",
    "pre-marketing": "pre marketing",
    # Entity synonyms (only abbreviations that won't collide)
    "subs": "submissions",
    "ints": "interviews",
    "confs": "confirmations", "conf": "confirmations",
    "conformation": "confirmation", "conformations": "confirmations",
    "stds": "students", "std": "students",
    # Time synonyms
    "ytd": "yesterday", "yday": "yesterday", "yest": "yesterday",
    "tmrw": "tomorrow", "2day": "today", "2morrow": "tomorrow",
    "lw": "last week", "tw": "this week", "lm": "last month", "tm": "this month",
    # Tech synonyms (only unambiguous)
    "dotnet": ".NET", "dot net": ".NET",
    "sfdc": "SFDC",
    "powerbi": "PowerBI", "power bi": "PowerBI",
    "devops": "DevOps", "dev ops": "DevOps",
    "servicenow": "Service Now", "service now": "Service Now",
    "data science": "DS/AI", "ai/ml": "DS/AI",
    "business analyst": "Business Analyst",
    "rpa": "RPA", "sap btp": "SAP BTP",
    # Visa synonyms
    "h1b": "H1", "h-1b": "H1", "h1 visa": "H1",
    "h4ead": "H4 EAD", "h4 ead": "H4 EAD",
    "opt visa": "OPT", "stem opt": "STEM",
    "green card": "GC", "gc ead": "GC",
    "us citizen": "USC",
    # Action synonyms
    "gimme": "give me", "lemme": "let me", "wanna": "want to",
    "gonna": "going to", "gotta": "got to",
    "pls": "please", "plz": "please", "thx": "thanks",
    # BU / role synonyms
    "mgr": "manager", "mgrs": "managers",
    # Common misspellings
    "submisions": "submissions", "submision": "submission",
    "interveiw": "interview", "interveiws": "interviews",
    "studnet": "student", "studnets": "students",
    "manger": "manager", "mangers": "managers",
    "tecnology": "technology", "technolgy": "technology",
    "perfomance": "performance", "preformance": "performance",
}
# REMOVED (high collision risk):
# "verbal" → corrupts "verbal interview"
# "training" → corrupts "training session"
# "joined" → corrupts "joined interview"
# "left"/"gone" → corrupts "left join", "gone through"
# "marketing" → corrupts "marketing email"
# "lead"/"leads" → corrupts "lead time"
# "sub"/"int" → corrupts "sub query", "interval"
# "bench" → handled by _STATUS_MAP in semantic.py
# "placed"/"confirmed" → handled by _STATUS_MAP in semantic.py
# "ds"/"ba"/"sf"/"sap"/"bu"/"citizen" → too short, ambiguous

_ABBREVIATION_PATTERNS = [
    (re.compile(r'\bhw\s+many\b', re.I), "how many"),
    (re.compile(r'\bwht\b', re.I), "what"),
    (re.compile(r'\bshw\b', re.I), "show"),
    (re.compile(r'\blst\b', re.I), "list"),
    (re.compile(r'\bno\.\s*of\b', re.I), "number of"),
    (re.compile(r'\b#\s*of\b', re.I), "number of"),
    (re.compile(r'\bw/\b', re.I), "with"),
    (re.compile(r'\bw/o\b', re.I), "without"),
    (re.compile(r'\bb/w\b', re.I), "between"),
    (re.compile(r'\bbu\s*wise\b', re.I), "BU wise"),
    (re.compile(r'\btech\s*wise\b', re.I), "technology wise"),
]


_SYNONYM_PATTERNS = None

def _get_synonym_patterns():
    global _SYNONYM_PATTERNS
    if _SYNONYM_PATTERNS is None:
        _SYNONYM_PATTERNS = []
        for slang, expanded in sorted(_SYNONYM_MAP.items(), key=lambda x: -len(x[0])):
            pattern = re.compile(r'\b' + re.escape(slang) + r'\b', re.IGNORECASE)
            _SYNONYM_PATTERNS.append((pattern, expanded))
    return _SYNONYM_PATTERNS


def _normalize_question(question):
    """Expand synonyms, fix slang, normalize abbreviations before any handler sees the question."""
    q = question.strip()
    if not q:
        return q

    for pattern, replacement in _ABBREVIATION_PATTERNS:
        q = pattern.sub(replacement, q)

    for pattern, expanded in _get_synonym_patterns():
        q = pattern.sub(expanded, q)

    if q != question.strip():
        logger.info(f"Normalized: '{question.strip()[:80]}' → '{q[:80]}'")
    return q


# ── Layer 1b: Vague / Ambiguous Question Detection ──────────────────

_VAGUE_PATTERNS = [
    (re.compile(r'\b(?:good|best|top|worst|poor|strong|weak)\s+(?:performing|performer|student|bu|manager|team)', re.I),
     "Could you clarify what metric you'd like? For example: most submissions, most interviews, most placements, highest confirmation rate?"),
    (re.compile(r'\b(?:how is|how are|how\'s)\s+\w+\s+(?:doing|performing|going)\b', re.I),
     "Could you specify what metric you'd like to see? For example: submission count, interview count, placement count, or confirmation rate?"),
    (re.compile(r'\b(?:rank|ranking|compare|comparison)\b.*\b(?:bu|manager|team|student)\b', re.I),
     "Could you specify what to rank by? For example: by submissions count, interview count, placements, or revenue?"),
    (re.compile(r'\b(?:overall|general|complete)\s+(?:status|summary|overview|picture)\b', re.I),
     "Could you specify what area? For example: students in market, submissions this week, interviews this month, or placements?"),
]

_VAGUE_ADJECTIVES = re.compile(r'\b(?:good|best|worst|poor|strong|weak|top performing|bottom performing|impressive|great)\b', re.I)
_STATUS_CONTEXT = re.compile(r'\b(?:status|final.status|type|category|result)\b', re.I)

def _detect_vague_question(question):
    q = question.lower()
    is_quantitative = any(w in q for w in ["how many", "how much", "count", "total", "list", "show", "give me", "get me"])

    if is_quantitative and _VAGUE_ADJECTIVES.search(question) and not _STATUS_CONTEXT.search(question):
        return ("Your question includes a subjective term that the database can't filter on. "
                "Could you replace it with a measurable criteria? For example: "
                "'students with more than 5 submissions' or 'students with 0 interviews in 14 days'.")

    if is_quantitative:
        return None
    for pattern, clarification in _VAGUE_PATTERNS:
        if pattern.search(question):
            return clarification
    return None


# ── Layer 1c: Unanswerable Question Detection ───────────────────────

_UNANSWERABLE_PATTERNS = [
    (re.compile(r'\b(?:why\s+(?:is|are|do|does|did|has|have|was|were))\b', re.I),
     "I can show you the data, but I cannot determine reasons or causes from the database. Would you like to see the related numbers instead?"),
    (re.compile(r'\b(?:should\s+(?:i|we)|what\s+should|recommend|suggest|advice)\b', re.I),
     "I can show you the data to help you decide, but I can't make recommendations. What specific numbers would help?"),
    (re.compile(r'\b(?:predict|forecast|will\s+(?:it|they|we|there)|going\s+to\s+(?:be|happen|increase|decrease))\b', re.I),
     "I can show current and historical data, but I cannot predict future outcomes. Would you like to see the trend data instead?"),
    (re.compile(r'\b(?:what\s+(?:if|would\s+happen))\b', re.I),
     "I can only query existing data — I can't run hypothetical scenarios. Would you like to see the current numbers?"),
]

def _detect_unanswerable(question):
    q = question.lower()
    if any(w in q for w in ["how many", "how much", "count", "total", "list", "show", "give me", "get me"]):
        return None
    for pattern, response in _UNANSWERABLE_PATTERNS:
        if pattern.search(question):
            return response
    return None


# ── Layer 1d: Follow-up Resolution ────────────────────────────────

_FOLLOWUP_PATTERNS = [
    re.compile(r'^(?:and |but |also |what about |how about |ok |okay )', re.I),
    re.compile(r'\b(?:instead|rather|compared|versus|vs)\b', re.I),
    re.compile(r'\b(?:more details|break it down|drill down|expand|elaborate)\b', re.I),
]

_STRONG_FOLLOWUP_PATTERNS = [
    re.compile(r'^(?:same |same\b)', re.I),
    re.compile(r'^(?:what about |how about )', re.I),
    re.compile(r'^\b(?:and|but)\s+(?:what|how|show|list|for)\b', re.I),
]

_PRONOUN_FOLLOWUP = re.compile(r'\b(?:those|these|them|that one|the same)\b', re.I)

_STANDALONE_INDICATORS = [
    re.compile(r'\b(?:how many|how much|count|total|list|show|give me|get me)\b', re.I),
    re.compile(r'\b(?:who|which|find|search)\b', re.I),
    re.compile(r'\b(?:details?|info)\s+(?:of|on|about|for)\b', re.I),
    re.compile(r'\b(?:students?|submissions?|interviews?|managers?|employees?|recruiters?|bus?|jobs?|placements?)\b', re.I),
    re.compile(r'\b(?:performance|report|expenses?|salary|payroll)\b', re.I),
    re.compile(r'\b(?:yesterday|today|this week|this month|last week|last month)\b', re.I),
    re.compile(r'(?:__c|BU-|BU\s)', re.I),
]


def _is_followup(question, conversation_history):
    if not conversation_history or len(conversation_history) < 2:
        return False
    q = question.strip()

    # Check standalone indicators first — if ANY matches, it's standalone
    standalone_score = sum(1 for pat in _STANDALONE_INDICATORS if pat.search(q))
    if standalone_score >= 1:
        return False

    # Very short questions (1-2 words) with no entity are likely follow-ups
    if len(q.split()) <= 2:
        return True

    # Strong follow-up signals (sentence starts with "same", "what about", etc.)
    for pat in _STRONG_FOLLOWUP_PATTERNS:
        if pat.search(q):
            return True

    # Pronoun references to previous context
    if _PRONOUN_FOLLOWUP.search(q):
        return True

    # General follow-up patterns need at least 2 matches to trigger
    followup_score = sum(1 for pat in _FOLLOWUP_PATTERNS if pat.search(q))
    return followup_score >= 2


FOLLOWUP_RESOLVE_PROMPT = """You resolve follow-up questions into standalone queries.

Given the conversation history and a follow-up question, rewrite it as a complete, self-contained question
that includes all necessary context (entity names, time ranges, filters) from the conversation.

RULES:
- Output ONLY the rewritten question, nothing else
- PRESERVE ALL filters from the previous query unless the user explicitly changes them
- If previous query had a BU filter, time range, status, or technology — keep them ALL
- Only change the specific thing the user asked to change
- If the follow-up asks about "them"/"those"/"it", replace with the actual entity from context
- If the follow-up changes a time range ("what about last month"), keep everything else the same
- If the follow-up says "same for Java", change ONLY the technology — keep BU, time range, status filters
- If the follow-up asks for "more details" or "break it down", add "show details" or "BU wise" as appropriate
- Never output explanations, just the rewritten question"""


async def _resolve_followup(question, conversation_history):
    if not _is_followup(question, conversation_history):
        return question

    ctx_parts = []
    for msg in conversation_history[-6:]:
        role = msg.get("role", "")
        content = msg.get("content", "")
        if role == "user":
            ctx_parts.append(f"User: {content[:200]}")
        elif role == "assistant":
            ctx_parts.append(f"Assistant: {content[:300]}")

    prompt = f"Conversation:\n{chr(10).join(ctx_parts)}\n\nFollow-up question: {question}\n\nRewritten standalone question:"
    resolved = await _call_ai(FOLLOWUP_RESOLVE_PROMPT, prompt, max_tokens=150, temperature=0)

    if resolved and len(resolved.strip()) > 5:
        resolved = resolved.strip().strip('"').strip("'")
        logger.info(f"Follow-up resolved: '{question[:60]}' → '{resolved[:60]}'")
        return resolved
    return question


# ── Domain Knowledge ──────────────────────────────────────────────

DOMAIN_KNOWLEDGE = """
BUSINESS CONTEXT — Staffing/Consulting Company:
- "In Market" = student is actively being marketed to clients for placement
- "Verbal Confirmation" (VC) = client verbally confirmed they want the student
- "Project Started" = student started working on a client project (successful placement)
- "Exit" = student left the program
- "Pre Marketing" = student is in training, not yet ready for market
- BU = Business Unit, managed by a BU Manager who oversees a team of students
- Submissions = resumes sent to clients for job opportunities
- Interviews = client interviews scheduled/completed for students
- Days in Market = how long a student has been actively marketed (lower is better for placements)
- Conversion funnel: In Market → Submissions → Interviews → Verbal Confirmation → Project Started
- Key metrics: submission rate, interview-to-confirmation ratio, average days to placement
- A "good" BU has high submission counts, quick placements, and low days-in-market averages
"""


# ── Layer 3: Fuzzy Cache Short-Circuit ─────────────────────────────

async def _fuzzy_cache_lookup(question):
    """Check learning_memory for a very similar verified question. Returns (sql, True) or (None, False)."""
    try:
        examples = await find_similar_past_queries(question, top_k=10)
        if not examples:
            return None, False

        q_lower = question.lower().strip()
        q_words = set(q_lower.split())

        for ex in examples:
            past_q = ex["past_question"].lower().strip()
            past_sql = ex.get("past_soql", "")
            feedback = ex.get("feedback", "none")

            if not past_sql or not past_sql.strip().upper().startswith("SELECT"):
                continue

            if feedback == "bad":
                logger.debug(f"Fuzzy cache SKIP (negative feedback): '{past_q[:60]}'")
                continue

            ratio = SequenceMatcher(None, q_lower, past_q).ratio()

            past_words = set(past_q.split())
            overlap = len(q_words & past_words)
            union = len(q_words | past_words)
            jaccard = overlap / union if union else 0

            combined = (ratio * 0.6) + (jaccard * 0.4)

            if feedback == "good" and combined >= 0.85:
                logger.info(f"Fuzzy cache HIT (verified, score={combined:.2f}): '{past_q[:60]}'")
                return past_sql, True
            elif feedback == "good" and combined >= 0.82:
                logger.info(f"Fuzzy cache SOFT HIT (verified, score={combined:.2f}): '{past_q[:60]}'")
                return past_sql, True
            elif combined >= 0.92:
                logger.info(f"Fuzzy cache HIT (unverified, score={combined:.2f}): '{past_q[:60]}'")
                return past_sql, True

    except Exception as e:
        logger.warning(f"Fuzzy cache lookup failed: {str(e)[:80]}")
    return None, False

# ── Dynamic Picklist Values (loaded from DB, refreshed every 5 min) ─────────
_picklist_cache = None
_picklist_cache_ts = 0
_PICKLIST_TTL = 300

async def _load_picklist_values():
    """Load actual picklist values from PostgreSQL for accurate SQL generation."""
    global _picklist_cache, _picklist_cache_ts
    if _picklist_cache is not None and (time.time() - _picklist_cache_ts) < _PICKLIST_TTL:
        return _picklist_cache

    queries = {
        "Student_Marketing_Status__c": 'SELECT DISTINCT "Student_Marketing_Status__c" FROM "Student__c" WHERE "Student_Marketing_Status__c" IS NOT NULL ORDER BY 1',
        "Marketing_Visa_Status__c": 'SELECT DISTINCT "Marketing_Visa_Status__c" FROM "Student__c" WHERE "Marketing_Visa_Status__c" IS NOT NULL ORDER BY 1',
        "Technology__c": 'SELECT DISTINCT "Technology__c" FROM "Student__c" WHERE "Technology__c" IS NOT NULL ORDER BY 1',
        "Submission_Status__c": 'SELECT DISTINCT "Submission_Status__c" FROM "Submissions__c" WHERE "Submission_Status__c" IS NOT NULL ORDER BY 1',
        "Interview_Type__c": 'SELECT DISTINCT "Type__c" FROM "Interviews__c" WHERE "Type__c" IS NOT NULL ORDER BY 1',
        "Interview_Final_Status__c": 'SELECT DISTINCT "Final_Status__c" FROM "Interviews__c" WHERE "Final_Status__c" IS NOT NULL ORDER BY 1',
        "Job_Project_Type__c": 'SELECT DISTINCT "Project_Type__c" FROM "Job__c" WHERE "Project_Type__c" IS NOT NULL ORDER BY 1',
        "Employee_Deptment__c": 'SELECT DISTINCT "Deptment__c" FROM "Employee__c" WHERE "Deptment__c" IS NOT NULL ORDER BY 1',
        "BU_Names": 'SELECT DISTINCT "BU_Name__c" FROM "Submissions__c" WHERE "BU_Name__c" IS NOT NULL ORDER BY 1',
    }
    result = {}
    for key, sql in queries.items():
        try:
            r = await execute_query(sql)
            if "error" not in r and r.get("records"):
                col = list(r["records"][0].keys())[0]
                result[key] = [rec[col] for rec in r["records"] if rec.get(col)]
        except Exception:
            pass
    _picklist_cache = result
    _picklist_cache_ts = time.time()
    logger.info(f"Loaded picklist values: {', '.join(f'{k}({len(v)})' for k, v in result.items())}")
    return result


def _build_picklist_prompt(picklists):
    """Build the picklist section for the SQL prompt using live DB values."""
    if not picklists:
        return ""
    lines = ["\nACTUAL PICKLIST VALUES FROM DATABASE (use EXACT spelling):"]
    mapping = {
        "Student_Marketing_Status__c": "Student__c.Student_Marketing_Status__c",
        "Marketing_Visa_Status__c": "Student__c.Marketing_Visa_Status__c",
        "Technology__c": "Student__c.Technology__c",
        "Submission_Status__c": "Submissions__c.Submission_Status__c",
        "Interview_Type__c": "Interviews__c.Type__c",
        "Interview_Final_Status__c": "Interviews__c.Final_Status__c",
        "Job_Project_Type__c": "Job__c.Project_Type__c",
        "Employee_Deptment__c": "Employee__c.Deptment__c",
    }
    for key, label in mapping.items():
        vals = picklists.get(key, [])
        if vals:
            lines.append(f"  {label}: {', '.join(repr(v) for v in vals)}")
    bu_names = picklists.get("BU_Names", [])
    if bu_names:
        lines.append(f"  BU Manager Names ({len(bu_names)} total): {', '.join(repr(n) for n in bu_names[:30])}{'...' if len(bu_names) > 30 else ''}")
    return "\n".join(lines)

# ── Query Result Cache (avoid re-querying for same question within 5 min)
_soql_cache = {}
_CACHE_TTL = 300  # 5 minutes

def _cache_key(question):
    return question.strip().lower()

def _cache_get(question):
    key = _cache_key(question)
    entry = _soql_cache.get(key)
    if entry and (time.time() - entry["ts"]) < _CACHE_TTL:
        logger.info(f"Cache hit: {key[:60]}")
        return entry["soql"], entry["result"], entry["recs"]
    if entry:
        del _soql_cache[key]
    return None

def _cache_set(question, soql, result, recs):
    if not recs:
        return
    key = _cache_key(question)
    _soql_cache[key] = {"soql": soql, "result": result, "recs": recs, "ts": time.time()}
    if len(_soql_cache) > 100:
        oldest = min(_soql_cache, key=lambda k: _soql_cache[k]["ts"])
        del _soql_cache[oldest]


# ── Report Pattern Matcher (skip AI for known reports) ──────────
# ORDER MATTERS: more specific patterns (multi-keyword) must come before generic ones.
REPORT_PATTERNS = [
    {
        "keywords": ["monthly sub", "monthly int", "monthly submission", "monthly interview", "monthly confirmation", "monthly conformation", "month sub & int", "monthly sub & int"],
        "time_keywords": {"last month": "LAST_MONTH", "this month": "THIS_MONTH"},
        "default_time": "THIS_MONTH",
        "queries": [
            """SELECT "Student_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} ORDER BY "BU_Name__c" LIMIT 2000""",
            """SELECT s."Name" AS "Student_Name", m."Name" AS "BU_Name", i."Type__c", i."Final_Status__c", i."Amount__c", i."Interview_Date1__c" FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE i."Interview_Date1__c" >= {time_start} AND i."Interview_Date1__c" < {time_end} ORDER BY m."Name" LIMIT 2000""",
            """SELECT s."Name", m."Name" AS "BU_Name", s."Technology__c", s."Verbal_Confirmation_Date__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'Verbal Confirmation' AND s."Verbal_Confirmation_Date__c" >= {time_start} AND s."Verbal_Confirmation_Date__c" < {time_end} ORDER BY m."Name" LIMIT 2000""",
        ],
        "labels": ["Monthly Submissions", "Monthly Interviews", "Monthly Confirmations"],
        "summary_queries": [
            ("""SELECT "BU_Name__c" AS "BU_Name", COUNT(*) AS cnt FROM "Submissions__c" WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} AND "BU_Name__c" IS NOT NULL GROUP BY "BU_Name__c" ORDER BY cnt DESC""", "_summary_Monthly Submissions"),
            ("""SELECT m."Name" AS "BU_Name", COUNT(*) AS cnt, SUM(CASE WHEN i."Final_Status__c" IN ('Confirmation', 'Expecting Confirmation', 'Verbal Confirmation') THEN 1 ELSE 0 END) AS conf_cnt, COALESCE(SUM(i."Amount__c"), 0) AS total_amount FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE i."Interview_Date1__c" >= {time_start} AND i."Interview_Date1__c" < {time_end} AND m."Name" IS NOT NULL GROUP BY m."Name" ORDER BY cnt DESC""", "_summary_Monthly Interviews"),
            ("""SELECT m."Name" AS "BU_Name", COUNT(*) AS cnt FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'Verbal Confirmation' AND s."Verbal_Confirmation_Date__c" >= {time_start} AND s."Verbal_Confirmation_Date__c" < {time_end} AND m."Name" IS NOT NULL GROUP BY m."Name" ORDER BY cnt DESC""", "_summary_Monthly Confirmations"),
        ],
    },
    {
        "keywords": ["last week sub", "last week int", "weekly sub", "weekly int", "last week submission", "last week interview"],
        "time_keywords": {},
        "default_time": "LAST_WEEK",
        "by_lead": True,
        "queries_bu": [
            """SELECT "Student_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} ORDER BY "BU_Name__c" LIMIT 2000""",
            """SELECT s."Name" AS "Student_Name", m."Name" AS "BU_Name", i."Type__c", i."Final_Status__c", i."Interview_Date1__c", i."Amount__c" FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE i."Interview_Date1__c" >= {time_start} AND i."Interview_Date1__c" < {time_end} ORDER BY m."Name" LIMIT 2000""",
        ],
        "queries_lead": [
            """SELECT "Student_Name__c", "Offshore_Manager_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} ORDER BY "Offshore_Manager_Name__c" LIMIT 2000""",
            """SELECT s."Name" AS "Student_Name", s."Offshore_Manager_Name__c", m."Name" AS "BU_Name", i."Type__c", i."Final_Status__c", i."Interview_Date1__c" FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE i."Interview_Date1__c" >= {time_start} AND i."Interview_Date1__c" < {time_end} ORDER BY s."Offshore_Manager_Name__c" LIMIT 2000""",
        ],
        "labels": ["Last Week Submissions", "Last Week Interviews"],
    },
    {
        "keywords": ["confirmation", "conformation", "congratulation", "verbal confirmation", "verbal conformation", "confirmed"],
        "time_keywords": {"last week": "LAST_WEEK", "this week": "THIS_WEEK", "this month": "THIS_MONTH", "last month": "LAST_MONTH", "yesterday": "YESTERDAY", "today": "TODAY"},
        "default_time": "LAST_WEEK",
        "queries": [
            """SELECT s."Name", m."Name" AS "BU_Name", s."Technology__c", s."Verbal_Confirmation_Date__c", s."Marketing_Visa_Status__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'Verbal Confirmation' AND s."Verbal_Confirmation_Date__c" >= {time_start} AND s."Verbal_Confirmation_Date__c" < {time_end} ORDER BY m."Name" LIMIT 2000"""
        ],
        "labels": ["Confirmations"],
    },
    {
        "keywords": ["pre marketing", "premarketing", "pre-marketing"],
        "time_keywords": {},
        "default_time": None,
        "queries": [
            """SELECT s."Name", m."Name" AS "BU_Name", s."PreMarketingStatus__c", s."Resume_Preparation__c", s."Resume_Verified_By_Lead__c", s."Resume_Verified_By_Manager__c", s."Resume_Verification__c", s."Resume_Review__c", s."Otter_Screening__c", s."Otter_Final_Screening__c", s."Otter_Real_Time_Screeing_1__c", s."Otter_Real_Time_Screeing_2__c", s."Has_Linkedin_Created__c", s."Student_LinkedIn_Account_Review__c", s."MQ_Screening_By_Lead__c", s."MQ_Screening_By_Manager__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'Pre Marketing' ORDER BY m."Name" LIMIT 2000"""
        ],
        "labels": ["PreMarketing Students"],
    },
    {
        "keywords": ["yesterday submission"],
        "time_keywords": {},
        "default_time": "YESTERDAY",
        "by_lead": True,
        "queries_bu": [
            """SELECT "Student_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c", "Offshore_Manager_Name__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} ORDER BY "BU_Name__c" LIMIT 2000"""
        ],
        "queries_lead": [
            """SELECT "Student_Name__c", "Offshore_Manager_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} ORDER BY "Offshore_Manager_Name__c" LIMIT 2000"""
        ],
        "labels": ["Yesterday Submissions"],
    },
    {
        "keywords": ["3 day", "three day", "no submission", "last 3 days no"],
        "time_keywords": {},
        "default_time": None,
        "by_lead": True,
        "queries_bu": [
            """SELECT s."Name", m."Name" AS "BU_Name", s."Technology__c", s."Last_Submission_Date__c", s."Days_in_Market_Business__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'In Market' AND (s."Last_Submission_Date__c" < CURRENT_DATE - INTERVAL '3 days' OR s."Last_Submission_Date__c" IS NULL) ORDER BY m."Name" LIMIT 2000"""
        ],
        "queries_lead": [
            """SELECT s."Name", s."Offshore_Manager_Name__c", m."Name" AS "BU_Name", s."Technology__c", s."Last_Submission_Date__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'In Market' AND (s."Last_Submission_Date__c" < CURRENT_DATE - INTERVAL '3 days' OR s."Last_Submission_Date__c" IS NULL) ORDER BY s."Offshore_Manager_Name__c" LIMIT 2000"""
        ],
        "labels": ["Students with No Recent Submissions"],
    },
    {
        "keywords": ["mandatory field", "missing field", "interview mandatory"],
        "time_keywords": {"last week": "LAST_WEEK", "this week": "THIS_WEEK", "this month": "THIS_MONTH"},
        "default_time": "THIS_WEEK",
        "queries": [
            """SELECT s."Name" AS "Student_Name", m."Name" AS "BU_Name", i."Type__c", i."Interview_Date1__c", i."Amount__c", i."Bill_Rate__c", i."Final_Status__c" FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE (i."Amount__c" IS NULL OR i."Bill_Rate__c" IS NULL OR i."Final_Status__c" IS NULL) AND i."Interview_Date1__c" >= {time_start} AND i."Interview_Date1__c" < {time_end} ORDER BY m."Name" LIMIT 2000"""
        ],
        "labels": ["Interviews with Missing Fields"],
    },
    {
        "keywords": ["no interview", "2 week no interview", "two week no interview", "no int"],
        "time_keywords": {},
        "default_time": None,
        "by_lead": True,
        "queries_bu": [
            """SELECT s."Name", m."Name" AS "BU_Name", s."Technology__c", s."Days_in_Market_Business__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'In Market' AND s."Id" NOT IN (SELECT "Student__c" FROM "Interviews__c" WHERE "Interview_Date1__c" >= CURRENT_DATE - INTERVAL '14 days') ORDER BY m."Name" LIMIT 2000"""
        ],
        "queries_lead": [
            """SELECT s."Name", m."Name" AS "BU_Name", s."Offshore_Manager_Name__c", s."Technology__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'In Market' AND s."Id" NOT IN (SELECT "Student__c" FROM "Interviews__c" WHERE "Interview_Date1__c" >= CURRENT_DATE - INTERVAL '14 days') ORDER BY s."Offshore_Manager_Name__c" LIMIT 2000"""
        ],
        "labels": ["In-Market Students with No Interviews (14 days)"],
    },
    {
        "keywords": ["expense", "placement cost", "per placement"],
        "time_keywords": {},
        "default_time": None,
        "queries": [
            """SELECT "Name", "Total_Expenses_MIS__c", "Each_Placement_Cost__c", "BU_Student_With_Job_Count__c", "Students_Count__c", "In_Market_Students_Count__c", "Verbal_Count__c", "IN_JOB_Students_Count__c" FROM "Manager__c" WHERE "Active__c" = true ORDER BY "Name" LIMIT 2000"""
        ],
        "labels": ["BU Expenses & Placement Costs"],
    },
    {
        "keywords": ["payroll", "bench payroll", "job payroll"],
        "time_keywords": {},
        "default_time": None,
        "queries": [
            """SELECT s."Name" AS "Student_Name", m."Name" AS "BU_Name", j."PayRate__c", j."Caluculated_Pay_Rate__c", j."Pay_Roll_Tax__c", j."Profit__c", j."Bill_Rate__c", j."Payroll_Month__c", j."Project_Type__c", j."Technology__c" FROM "Job__c" j LEFT JOIN "Student__c" s ON j."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON j."Share_With__c" = m."Id" WHERE j."Active__c" = true ORDER BY m."Name" LIMIT 2000""",
            """SELECT s."Name", m."Name" AS "BU_Name", s."Technology__c", s."Days_in_Market_Business__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'In Market' ORDER BY m."Name" LIMIT 2000""",
        ],
        "labels": ["Active Job Payroll", "Bench (In-Market Students)"],
    },
    {
        "keywords": ["total interview", "interview amount", "total amount"],
        "time_keywords": {"last month": "LAST_MONTH", "this month": "THIS_MONTH", "last week": "LAST_WEEK", "this week": "THIS_WEEK"},
        "default_time": "THIS_MONTH",
        "queries": [
            """SELECT s."Name" AS "Student_Name", m."Name" AS "BU_Name", i."Type__c", i."Amount__c", i."Amount_INR__c", i."Bill_Rate__c", i."Final_Status__c", i."Interview_Date1__c" FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE i."Interview_Date1__c" >= {time_start} AND i."Interview_Date1__c" < {time_end} ORDER BY m."Name" LIMIT 2000"""
        ],
        "labels": ["Interviews with Amounts"],
    },
    {
        "keywords": ["weekly report for", "weekly report of", "send weekly report", "weekly performance report"],
        "time_keywords": {"last week": "LAST_WEEK", "this week": "THIS_WEEK", "this month": "THIS_MONTH", "last month": "LAST_MONTH"},
        "default_time": "LAST_WEEK",
        "name_filter": True,
        "whatsapp_format": True,
        "queries": [
            """SELECT "Student_Name__c", "BU_Name__c", "Offshore_Manager_Name__c", "Recruiter_Name__c", "Client_Name__c", "Submission_Date__c" FROM "Submissions__c" WHERE "BU_Name__c" ILIKE '%{name}%' AND "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} ORDER BY "Offshore_Manager_Name__c", "Student_Name__c" LIMIT 2000""",
            """SELECT s."Name" AS "Student_Name", m."Name" AS "BU_Name", i."Type__c", i."Final_Status__c", i."Amount__c", i."Interview_Date1__c" FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE m."Name" ILIKE '%{name}%' AND i."Interview_Date1__c" >= {time_start} AND i."Interview_Date1__c" < {time_end} ORDER BY m."Name" LIMIT 2000""",
            """SELECT s."Name", m."Name" AS "BU_Name", s."Technology__c", s."Verbal_Confirmation_Date__c", s."Marketing_Visa_Status__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'Verbal Confirmation' AND m."Name" ILIKE '%{name}%' AND s."Verbal_Confirmation_Date__c" >= {time_start} AND s."Verbal_Confirmation_Date__c" < {time_end} ORDER BY m."Name" LIMIT 2000""",
            """SELECT s."Name", m."Name" AS "BU_Name", s."Technology__c", s."Days_in_Market_Business__c", s."Last_Submission_Date__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'In Market' AND m."Name" ILIKE '%{name}%' AND (s."Last_Submission_Date__c" < CURRENT_DATE - INTERVAL '3 days' OR s."Last_Submission_Date__c" IS NULL) ORDER BY m."Name" LIMIT 2000""",
        ],
        "labels": ["Weekly Submissions", "Weekly Interviews", "Weekly Confirmations", "Students Needing Attention"],
    },
    {
        "keywords": ["performance of", "performance for", "performance report"],
        "time_keywords": {"last week": "LAST_WEEK", "this week": "THIS_WEEK", "this month": "THIS_MONTH", "last month": "LAST_MONTH"},
        "default_time": "LAST_WEEK",
        "name_filter": True,
        "queries": [
            """SELECT "Student_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c" FROM "Submissions__c" WHERE "BU_Name__c" ILIKE '%{name}%' AND "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} ORDER BY "Submission_Date__c" LIMIT 2000""",
            """SELECT s."Name" AS "Student_Name", m."Name" AS "BU_Name", i."Type__c", i."Final_Status__c", i."Amount__c", i."Interview_Date1__c" FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE m."Name" ILIKE '%{name}%' AND i."Interview_Date1__c" >= {time_start} AND i."Interview_Date1__c" < {time_end} ORDER BY i."Interview_Date1__c" LIMIT 2000""",
        ],
        "labels": ["Submissions", "Interviews"],
    },
    {
        "keywords": ["student performance"],
        "time_keywords": {"last week": "LAST_WEEK", "this week": "THIS_WEEK", "this month": "THIS_MONTH", "last month": "LAST_MONTH"},
        "default_time": "LAST_WEEK",
        "by_lead": True,
        "queries_bu": [
            """SELECT "Student_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} ORDER BY "BU_Name__c", "Student_Name__c" LIMIT 2000"""
        ],
        "queries_lead": [
            """SELECT "Student_Name__c", "Offshore_Manager_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} ORDER BY "Offshore_Manager_Name__c", "Student_Name__c" LIMIT 2000"""
        ],
        "labels": ["Student Performance (Submissions)"],
    },
    {
        "keywords": ["recruiter performance"],
        "time_keywords": {"last week": "LAST_WEEK", "this week": "THIS_WEEK", "this month": "THIS_MONTH", "last month": "LAST_MONTH"},
        "default_time": "LAST_WEEK",
        "by_lead": True,
        "queries_bu": [
            """SELECT "Recruiter_Name__c", "Student_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} ORDER BY "BU_Name__c", "Recruiter_Name__c" LIMIT 2000"""
        ],
        "queries_lead": [
            """SELECT "Recruiter_Name__c", "Student_Name__c", "Offshore_Manager_Name__c", "BU_Name__c", "Submission_Date__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} ORDER BY "Offshore_Manager_Name__c", "Recruiter_Name__c" LIMIT 2000"""
        ],
        "labels": ["Recruiter Performance (Submissions)"],
    },
]


_PG_TIME_RANGES = {
    "THIS_MONTH":  ("DATE_TRUNC('month', CURRENT_DATE)", "DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'"),
    "LAST_MONTH":  ("DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'", "DATE_TRUNC('month', CURRENT_DATE)"),
    "THIS_WEEK":   ("DATE_TRUNC('week', CURRENT_DATE)", "DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '1 week'"),
    "LAST_WEEK":   ("DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week'", "DATE_TRUNC('week', CURRENT_DATE)"),
    "TODAY":       ("CURRENT_DATE", "CURRENT_DATE + INTERVAL '1 day'"),
    "YESTERDAY":   ("CURRENT_DATE - INTERVAL '1 day'", "CURRENT_DATE"),
}


async def _handle_direct_report(question):
    """Handle report-type questions by running aggregate SQL and building the answer server-side.
    Returns (answer_text, sql_used, data_payload) or None if not a report question."""
    q_lower = question.lower().replace("conformation", "confirmation").replace("submision", "submission")

    # Detect report type
    is_monthly = any(kw in q_lower for kw in ["monthly sub", "monthly int", "monthly submission", "monthly interview",
                                                "monthly confirmation", "month sub & int", "monthly sub & int",
                                                "monthly report"])
    is_weekly = any(kw in q_lower for kw in ["last week sub", "last week int", "weekly sub", "weekly int",
                                               "last week submission", "last week interview"])
    is_bu_wise = any(kw in q_lower for kw in ["bu wise", "bu-wise", "bu report"])

    if not (is_monthly or is_weekly or is_bu_wise):
        return None

    # If a specific BU name is mentioned (e.g. "for BU Divya"), skip aggregate
    # and let the AI/template handle BU-specific queries
    bu_name_match = re.search(r'\bfor\s+bu\s+(.+)', q_lower)
    if bu_name_match:
        return None

    # Determine time range
    if "last month" in q_lower:
        time_val, time_label = "LAST_MONTH", "Last Month"
    elif "this week" in q_lower:
        time_val, time_label = "THIS_WEEK", "This Week"
    elif "last week" in q_lower or is_weekly:
        time_val, time_label = "LAST_WEEK", "Last Week"
    elif "yesterday" in q_lower:
        time_val, time_label = "YESTERDAY", "Yesterday"
    elif "today" in q_lower:
        time_val, time_label = "TODAY", "Today"
    else:
        time_val, time_label = "THIS_MONTH", "This Month"
    time_start, time_end = _PG_TIME_RANGES[time_val]

    # Run 3 aggregate queries in parallel-style
    sub_sql = (
        f'SELECT "BU_Name__c" AS "BU_Name", COUNT(*) AS sub_cnt '
        f'FROM "Submissions__c" '
        f'WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} '
        f'AND "BU_Name__c" IS NOT NULL '
        f'GROUP BY "BU_Name__c"'
    )
    int_sql = (
        f'SELECT m."Name" AS "BU_Name", COUNT(*) AS int_cnt, '
        f'SUM(CASE WHEN i."Final_Status__c" IN (\'Confirmation\', \'Expecting Confirmation\', \'Verbal Confirmation\') THEN 1 ELSE 0 END) AS conf_cnt, '
        f'COALESCE(SUM(i."Amount__c"), 0) AS total_amount '
        f'FROM "Interviews__c" i '
        f'LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" '
        f'LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" '
        f'WHERE i."Interview_Date1__c" >= {time_start} AND i."Interview_Date1__c" < {time_end} '
        f'AND m."Name" IS NOT NULL '
        f'GROUP BY m."Name"'
    )
    conf_sql = (
        f'SELECT m."Name" AS "BU_Name", COUNT(*) AS vc_cnt '
        f'FROM "Student__c" s '
        f'LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" '
        f'WHERE s."Student_Marketing_Status__c" = \'Verbal Confirmation\' '
        f'AND s."Verbal_Confirmation_Date__c" >= {time_start} AND s."Verbal_Confirmation_Date__c" < {time_end} '
        f'AND m."Name" IS NOT NULL '
        f'GROUP BY m."Name"'
    )

    sub_result = await execute_query(sub_sql)
    int_result = await execute_query(int_sql)
    conf_result = await execute_query(conf_sql)

    if "error" in sub_result and "error" in int_result:
        return None

    # Build BU lookup maps
    sub_map = {}
    for r in sub_result.get("records", []):
        r.pop("attributes", None)
        sub_map[r["BU_Name"]] = r.get("sub_cnt", 0)

    int_map = {}
    for r in int_result.get("records", []):
        r.pop("attributes", None)
        int_map[r["BU_Name"]] = {
            "interviews": r.get("int_cnt", 0),
            "confirmations": r.get("conf_cnt", 0),
            "amount": float(r.get("total_amount", 0) or 0),
        }

    conf_map = {}
    for r in conf_result.get("records", []):
        r.pop("attributes", None)
        conf_map[r["BU_Name"]] = r.get("vc_cnt", 0)

    # Merge all BU names
    all_bus = sorted(set(list(sub_map.keys()) + list(int_map.keys()) + list(conf_map.keys())))

    # Build summary table records
    summary_records = []
    total_s, total_i, total_c, total_a = 0, 0, 0, 0.0
    for bu in all_bus:
        subs = sub_map.get(bu, 0)
        int_data = int_map.get(bu, {})
        ints = int_data.get("interviews", 0)
        confs = int_data.get("confirmations", 0) + conf_map.get(bu, 0)
        amt = int_data.get("amount", 0.0)
        total_s += subs
        total_i += ints
        total_c += confs
        total_a += amt
        summary_records.append({
            "BU_Name": bu,
            "Submissions": subs,
            "Interviews": ints,
            "Confirmations": confs,
            "Interview_Amount": round(amt, 2),
        })

    # Sort by total activity descending
    summary_records.sort(key=lambda x: x["Submissions"] + x["Interviews"], reverse=True)

    # Build answer text server-side (no AI formatting needed)
    answer_lines = [f"**{time_label} — Submissions, Interviews, Confirmations & Interview Amount BU wise**\n"]
    answer_lines.append("| BU Name | Submissions | Interviews | Confirmations | Interview Amount |")
    answer_lines.append("|---|---:|---:|---:|---:|")
    for r in summary_records:
        answer_lines.append(
            f"| {r['BU_Name']} | {r['Submissions']:,} | {r['Interviews']:,} | {r['Confirmations']:,} | {r['Interview_Amount']:,.2f} |"
        )
    answer_lines.append(
        f"| **Total** | **{total_s:,}** | **{total_i:,}** | **{total_c:,}** | **{total_a:,.2f}** |"
    )
    answer_lines.append(f"\n{len(all_bus)} BUs | {total_s:,} submissions | {total_i:,} interviews | {total_c:,} confirmations | ${total_a:,.2f} total interview amount")

    answer_text = "\n".join(answer_lines)
    queries_used = f"-- Submissions by BU\n{sub_sql}\n\n-- Interviews by BU\n{int_sql}\n\n-- Confirmations by BU\n{conf_sql}"

    # Also fetch detail records for drill-down (paginated display)
    detail_sql = (
        f'SELECT "Student_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c" '
        f'FROM "Submissions__c" '
        f'WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end} '
        f'ORDER BY "BU_Name__c" LIMIT 2000'
    )
    detail_result = await execute_query(detail_sql)
    detail_recs = detail_result.get("records", []) if "error" not in detail_result else []
    for r in detail_recs:
        r.pop("attributes", None)
        r["_query_label"] = "Monthly Submissions"

    data_payload = {
        "totalSize": len(summary_records),
        "records": summary_records + detail_recs[:200],
        "query": queries_used,
        "route": "SQL",
        "rag_results": 0,
    }

    return {
        "answer": answer_text,
        "soql": queries_used,
        "data": data_payload,
        "summary_records": summary_records,
    }

def _match_report_pattern(question):
    """Match question to a known report pattern. Returns list of (query, label) or None."""
    q_lower = question.lower()
    q_lower = q_lower.replace("conformation", "confirmation").replace("submision", "submission")

    for pattern in REPORT_PATTERNS:
        if not any(kw in q_lower for kw in pattern["keywords"]):
            continue

        time_val = pattern.get("default_time")
        for time_kw, time_literal in pattern.get("time_keywords", {}).items():
            if time_kw in q_lower:
                time_val = time_literal
                break

        is_lead = any(w in q_lower for w in ["lead", "offshore", "offshore manager"])
        has_by_lead = pattern.get("by_lead", False)

        if has_by_lead and is_lead:
            queries = pattern.get("queries_lead", pattern.get("queries", []))
        elif has_by_lead:
            queries = pattern.get("queries_bu", pattern.get("queries", []))
        else:
            queries = pattern.get("queries", [])

        resolved = []
        labels = pattern.get("labels", [])

        name_val = ""
        if pattern.get("name_filter"):
            import re as _re
            name_match = _re.search(r'(?:weekly\s+(?:performance\s+)?report\s+(?:for|of)\s+(?:bu\s+)?)(.+?)(?:\s+(?:last|this|yesterday|today|of\s+last|of\s+this)|\s*$)', q_lower, _re.IGNORECASE)
            if not name_match:
                name_match = _re.search(r'(?:send\s+weekly\s+report\s+(?:for\s+)?(?:bu\s+)?)(.+?)(?:\s+(?:last|this|yesterday|today|of\s+last|of\s+this)|\s*$)', q_lower, _re.IGNORECASE)
            if not name_match:
                name_match = _re.search(r'(?:performance\s+(?:of|for|report\s+(?:of|for))\s+)(.+?)(?:\s+(?:last|this|yesterday|today|of\s+last|of\s+this)|\s*$)', q_lower, _re.IGNORECASE)
            if name_match:
                name_val = name_match.group(1).strip().rstrip('.')
            if not name_val:
                continue

        time_start, time_end = "", ""
        if time_val and time_val in _PG_TIME_RANGES:
            time_start, time_end = _PG_TIME_RANGES[time_val]

        for i, q_template in enumerate(queries):
            sql = q_template
            if time_start:
                sql = sql.replace("{time_start}", time_start).replace("{time_end}", time_end)
            if name_val:
                sql = sql.replace("{name}", name_val)
            label = labels[i] if i < len(labels) else f"Query {i+1}"
            resolved.append((sql, label))

        resolved_summaries = []
        for sq_template, sq_label in pattern.get("summary_queries", []):
            sq = sq_template
            if time_start:
                sq = sq.replace("{time_start}", time_start).replace("{time_end}", time_end)
            if name_val:
                sq = sq.replace("{name}", name_val)
            resolved_summaries.append((sq, sq_label))

        if resolved:
            logger.info(f"Report pattern matched: {labels[0] if labels else 'unknown'} ({len(resolved)} queries)")
            return {"queries": resolved, "summary_queries": resolved_summaries, "whatsapp": pattern.get("whatsapp_format", False), "name": name_val}

    return None

# ── Step 1: Pick the right object(s) ─────────────────────────────
OBJECT_PICKER_PROMPT = """You are a PostgreSQL database expert for a staffing/consulting company.
Given a user question, decide which database table(s) to query.

Return ONLY a JSON object like: {"objects": ["Student__c"], "reason": "student data with BU via Manager__r"}
No other text.

OBJECT RELATIONSHIPS (all interconnected):
  Student__c.Manager__c -> Manager__c (BU). Use Manager__r.Name for BU name.
  Submissions__c.Student__c -> Student__c. Has BU_Name__c text field.
  Interviews__c.Student__c -> Student__c. Interviews__c.Submissions__c -> Submissions__c.
  IMPORTANT: Interviews__c has NO BU_Name__c. For BU-wise interview data, must JOIN: Interviews__c -> Student__c -> Manager__c.
  Job__c.Student__c -> Student__c. Job__c.Share_With__c -> Manager__c (BU).
  Employee__c.Onshore_Manager__c -> Manager__c (BU). Employee__c.Cluster__c -> Cluster__c.
  BU_Performance__c.BU__c -> Manager__c (monthly BU metrics).
  Manager__c.Cluster__c -> Cluster__c. Manager__c.Organization__c -> Organization__c.

KEY RULES:
- "details of [person name]" / "who is [name]" / "find [name]" -> Student__c FIRST (most people are students), also try Employee__c and Contact
- "details on [BU name]" / "info on [BU]" -> Manager__c (Name, Active__c, Students_Count__c, In_Market_Students_Count__c, Total_Expenses__c, Verbal_Count__c, etc.)
- "students under BU X" -> Student__c (use Manager__r.Name LIKE '%X%' for cross-object lookup)
- "student status" / "in market" / "exit" -> Student__c
- "submissions for BU X" -> Submissions__c (has BU_Name__c text field)
- "recruiters" / "recruiter submissions" / "recruiter performance" -> Submissions__c (has Recruiter_Name__c text field)
- "interviews" / "interview count" -> Interviews__c (add Student__c + Manager__c if BU-wise needed)
- "employees" / "employee" -> Employee__c
- "jobs" / "placements" / "W2" -> Job__c
- "BU performance" / "metrics" (aggregate counts only) -> BU_Performance__c
- "performance of [person name]" -> Submissions__c + Interviews__c (actual activity data, NOT BU_Performance__c)
- "tech support" -> Tech_Support__c
- "organizations" / "company" -> Organization__c
- "pre marketing" / "premarketing" -> Student__c (PreMarketingStatus__c, Resume_Preparation__c, Otter fields)
- "yesterday submissions" -> Submissions__c (Submission_Date__c = YESTERDAY)
- "no submissions" / "3 days no submissions" -> Student__c (Last_Submission_Date__c)
- "mandatory fields" / "missing fields" -> Interviews__c (check null Amount__c, Bill_Rate__c, Final_Status__c)
- "expenses" / "placement cost" -> Manager__c (Total_Expenses_MIS__c, Each_Placement_Cost__c)
- "payroll" / "bench payroll" -> Job__c + Student__c (payroll fields + in-market students)
- "monthly sub" / "monthly int" / "monthly confirmation" -> Submissions__c + Interviews__c + Student__c
- For cross-object questions, pick the PRIMARY object and use __r lookups for related data."""

# ── Step 2: Generate SQL with focused schema ────────────────────
SOQL_PROMPT = """You are a PostgreSQL SQL expert for a staffing/consulting company database.
Return ONLY the SQL query. No explanation, no markdown, no backticks.

CRITICAL: All table and column names MUST be double-quoted (case-sensitive PostgreSQL).
Example: SELECT "Name", "Technology__c" FROM "Student__c" WHERE "Student_Marketing_Status__c" = 'In Market'

ACTUAL PICKLIST VALUES (use EXACT spelling — never guess):
  Student_Marketing_Status__c: 'Exit', 'In Market', 'Payroll Purpose', 'Pre Marketing', 'Project Completed', 'Project Completed-In Market', 'Project Started', 'Verbal Confirmation'
  Marketing_Visa_Status__c: 'CPT', 'GC', 'H1', 'H4 EAD', 'L2', 'OPT', 'STEM', 'USC'
  Technology__c: 'AEM', 'AIGEE', 'Business Analyst', 'CS', 'DE', 'DevOps', 'DS/AI', 'JAVA', '.NET', 'Network Engineer', 'Oracle CPQ', 'Oracle EBS Developer', 'Oracle WebCenter Content Developer', 'PowerBI', 'RPA', 'SAP BTP', 'Service Now', 'SFDC', 'SQL Developer'
  Submission_Status__c: 'Interview Scheduled', 'Submitted'
  Interviews__c.Type__c: 'Assessment', 'Client', 'Final Round', 'First Round', 'Fourth Round', 'HR', 'Implementation', 'NA', 'Second Round', 'Third Round', 'Vendor'
  Interviews__c.Final_Status__c: 'Average', 'Cancelled', 'Confirmation', 'Expecting Confirmation', 'Good', 'Re-Scheduled', 'Very Bad', 'Very Good'
  Job__c.Project_Type__c: 'C2C', 'PD', 'W2', 'W2-2'
  Job__c.Visa_Status__c: 'CPT', 'H1', 'H1B-Selected', 'H4 EAD', 'L2', 'OPT', 'STEM', 'STEM-P', 'Stem-Progress', 'USC'
  Manager__c.Type__c: 'Lead', 'Manager'
  Employee__c.Deptment__c: 'Accounts', 'BDM', 'Central Office', 'Graphic Designers', 'HR', 'INDIA HR', 'Networking', 'Offshore Floor Manager', 'Offshore Manger', 'OPT Recruiter', 'Otter', 'Proxy Allocation', 'Proxy Coordination', 'Recruiter', 'Resume Writer', 'Supervisors'

USER TERM → CORRECT STATUS MAPPING:
  "bench" / "on bench" / "in market" → 'In Market'
  "exit" / "left" / "exited" → 'Exit'
  "confirmed" / "confirmation" / "verbal" → 'Verbal Confirmation'
  "project started" / "placed" / "started" → 'Project Started'
  "project completed" → 'Project Completed' or 'Project Completed-In Market'
  "pre marketing" / "premarketing" / "training" → 'Pre Marketing'
  "active" (for jobs) → WHERE "Active__c" = true
  "active" (for students) → 'In Market' (students in market are active)
  "not in market" / "not on bench" → != 'In Market' (NEGATION — use != or NOT IN)
  "not exit" → != 'Exit' (NEGATION)

RULES:
- ALWAYS double-quote table names: "Student__c", "Submissions__c", "Interviews__c", "Job__c", "Employee__c"
- ALWAYS double-quote column names: "Name", "BU_Name__c", "Technology__c", etc.
- Use EXACT field names from the schema. NEVER guess or invent field names.
- Use EXACT picklist values from the list above. NEVER guess or abbreviate.
- For person names, use ILIKE '%LastName%' (case-insensitive search by LAST NAME).
  Example: "Sai Ganesh Chinnamsetty" -> WHERE "Name" ILIKE '%Chinnamsetty%'
  If full name doesn't match, try last name only. Never use exact match (=) for names.
- Always include "Name" in SELECT + as many useful fields as possible.
- For "details of [person]": SELECT ALL important fields (Name, status, technology, phone, email, visa, days in market, etc.)
- For follow-ups like "by lead" or "this month": look at PREVIOUS SQL and modify.
- Max LIMIT 2000. Only SELECT. If impossible, return: NO_SQL
- Use PostgreSQL date functions (see below). NEVER use SOQL date literals like TODAY, THIS_MONTH, LAST_N_DAYS.

CORRECT DATE FIELDS PER TABLE (use these, NOT CreatedDate):
- Submissions__c: use "Submission_Date__c" (Date) for time filtering
- Interviews__c: use "Interview_Date1__c" (Date) for time filtering. NOT "Interview_Date__c" (DateTime) or "CreatedDate"
- Student__c confirmations: use "Verbal_Confirmation_Date__c" (Date)
- Student__c project start: use "Job_Start_Date__c" (Date) or "Paid_Offer_Start_Date__c" (Date)
- Job__c: use "Project_Start_Date__c" / "Project_End_Date__c" (Date)
- BU_Performance__c: use "Date__c" (Date)
- Only use "CreatedDate" when no business date field exists or when checking record creation

DATE FUNCTIONS (PostgreSQL - ALWAYS use these):
- Today: CURRENT_DATE
- Yesterday: CURRENT_DATE - INTERVAL '1 day'
- This week: "col" >= DATE_TRUNC('week', CURRENT_DATE)
- Last week: "col" >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week' AND "col" < DATE_TRUNC('week', CURRENT_DATE)
- This month: "col" >= DATE_TRUNC('month', CURRENT_DATE) AND "col" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
- Last month: "col" >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND "col" < DATE_TRUNC('month', CURRENT_DATE)
- Last N days: "col" >= CURRENT_DATE - INTERVAL 'N days'
- This quarter: "col" >= DATE_TRUNC('quarter', CURRENT_DATE)
- This year: "col" >= DATE_TRUNC('year', CURRENT_DATE)

CROSS-OBJECT QUERIES (use LEFT JOIN):
- Student -> BU Manager: LEFT JOIN "Manager__c" ON "Student__c"."Manager__c" = "Manager__c"."Id"
- Submission -> Student: LEFT JOIN "Student__c" ON "Submissions__c"."Student__c" = "Student__c"."Id"
- Interview -> Student: LEFT JOIN "Student__c" ON "Interviews__c"."Student__c" = "Student__c"."Id"
- For BU queries on Submissions: use "BU_Name__c" directly (no JOIN needed)
- IMPORTANT: For BU queries on Interviews: "Interviews__c" does NOT have "BU_Name__c". You MUST JOIN through Student to Manager:
  LEFT JOIN "Student__c" s ON "Interviews__c"."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id"
  Then GROUP BY m."Name" to get BU-wise breakdown. Do NOT use "Onsite_Manager__c" as BU name — it is a different role.
- "Onsite_Manager__c" in Interviews__c = the onsite manager (NOT the BU manager). Never confuse them.
- Subqueries: WHERE "Id" NOT IN (SELECT "Student__c" FROM "Interviews__c" WHERE ...)

WHEN USER ASKS "how many" or "count" (simple count question):
- Use SELECT COUNT(*) AS cnt FROM "table" WHERE ... to get the exact number.
- Example: "how many students in market?" -> SELECT COUNT(*) AS cnt FROM "Student__c" WHERE "Student_Marketing_Status__c" = 'In Market'
- Example: "how many submissions today?" -> SELECT COUNT(*) AS cnt FROM "Submissions__c" WHERE "Submission_Date__c" = CURRENT_DATE

WHEN USER ASKS for breakdown ("BU wise", "tech wise", "by technology", "by BU"):
- Use GROUP BY with COUNT(*) to get the breakdown.
- Example: "submissions BU wise" -> SELECT "BU_Name__c", COUNT(*) AS cnt FROM "Submissions__c" WHERE ... GROUP BY "BU_Name__c" ORDER BY cnt DESC

WHEN USER ASKS "show", "list", "details" (wants to see records):
- Return actual records with "Name" + key fields. LIMIT 2000.

WHEN USER ASKS ABOUT A BU (business unit):
- BU = a manager name like 'Divya Panguluri'.
- For submissions: SELECT "Student_Name__c", "BU_Name__c" FROM "Submissions__c" WHERE "BU_Name__c" ILIKE '%Divya%'
- For students with JOIN: SELECT s."Name", m."Name" AS "BU_Manager" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE m."Name" ILIKE '%Divya%'

EXAMPLES:
Q: "how many students in market under Divya?"
A: SELECT s."Name", m."Name" AS "BU_Manager", s."Technology__c", s."Days_in_Market_Business__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'In Market' AND m."Name" ILIKE '%Divya%' ORDER BY s."Days_in_Market_Business__c" DESC LIMIT 2000

Q: "last week submissions by BU"
A: SELECT "Student_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c", "Offshore_Manager_Name__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week' AND "Submission_Date__c" < DATE_TRUNC('week', CURRENT_DATE) ORDER BY "BU_Name__c" LIMIT 2000

Q: "this month submissions BU wise"
A: SELECT "Student_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= DATE_TRUNC('month', CURRENT_DATE) AND "Submission_Date__c" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' ORDER BY "BU_Name__c" LIMIT 2000

Q: "today interviews"
A: SELECT i."Name", s."Name" AS "Student_Name", i."Type__c", i."Final_Status__c", i."Interview_Date1__c", i."Amount__c" FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" WHERE i."Interview_Date1__c" = CURRENT_DATE LIMIT 2000

Q: "top BUs by submission count this month"
A: SELECT "BU_Name__c", COUNT(*) AS cnt FROM "Submissions__c" WHERE "Submission_Date__c" >= DATE_TRUNC('month', CURRENT_DATE) AND "Submission_Date__c" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' GROUP BY "BU_Name__c" ORDER BY cnt DESC LIMIT 30

Q: "monthly interviews BU wise"
A: SELECT m."Name" AS "BU_Name", COUNT(*) AS cnt, SUM(i."Amount__c") AS total_amount FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE i."Interview_Date1__c" >= DATE_TRUNC('month', CURRENT_DATE) AND i."Interview_Date1__c" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' GROUP BY m."Name" ORDER BY cnt DESC LIMIT 2000

Q: "monthly confirmations BU wise"
A: SELECT m."Name" AS "BU_Name", COUNT(*) AS cnt FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'Verbal Confirmation' AND s."Verbal_Confirmation_Date__c" >= DATE_TRUNC('month', CURRENT_DATE) AND s."Verbal_Confirmation_Date__c" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' GROUP BY m."Name" ORDER BY cnt DESC LIMIT 2000

STATUS NEGATION ("not in market", "not exit", "not placed"):
- "not in market" / "students who are not in market" → WHERE "Student_Marketing_Status__c" != 'In Market'
- "not exit" → WHERE "Student_Marketing_Status__c" != 'Exit'
- CAREFULLY read the user's intent: "not in market" means EXCLUDE 'In Market', "in market" means INCLUDE 'In Market'
- For multiple exclusions: WHERE "Student_Marketing_Status__c" NOT IN ('Exit', 'Project Completed')

RECRUITER QUERIES:
- "recruiters" = Submissions__c."Recruiter_Name__c" (text field on Submissions__c)
- "recruiters with zero/no submissions" = find recruiter names who did NOT submit in the period
- "recruiter performance" = GROUP BY "Recruiter_Name__c" with COUNT
- Do NOT treat "recruiters" as a person name or BU name — it refers to the role/field

NEGATION QUERIES ("no", "zero", "without", "not having"):
- "no interviews" / "zero interviews" / "without interviews" = students NOT IN the Interviews table for that period
- ALWAYS use NOT IN subquery: WHERE "Id" NOT IN (SELECT "Student__c" FROM "Interviews__c" WHERE ...)
- NEVER confuse "no interviews" with "interview count" — "no interviews" means ZERO interviews, not a summary of interviews
- When combined with "BU wise": GROUP BY the BU manager name from the Manager__c JOIN
- "recruiters with zero submissions" = recruiters NOT IN submissions for that period

Q: "students with no interviews in 2 weeks"
A: SELECT "Name", "Technology__c", "Days_in_Market_Business__c" FROM "Student__c" WHERE "Student_Marketing_Status__c" = 'In Market' AND "Id" NOT IN (SELECT "Student__c" FROM "Interviews__c" WHERE "Interview_Date1__c" >= CURRENT_DATE - INTERVAL '14 days') LIMIT 2000

Q: "last 2 weeks no interviews for students by BU wise"
A: SELECT m."Name" AS "BU_Name", COUNT(*) AS student_count FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'In Market' AND s."Id" NOT IN (SELECT "Student__c" FROM "Interviews__c" WHERE "Interview_Date1__c" >= CURRENT_DATE - INTERVAL '14 days') GROUP BY m."Name" ORDER BY student_count DESC LIMIT 2000

Q: "no submissions this week by BU"
A: SELECT "BU_Name__c", COUNT(*) AS student_count FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'In Market' AND s."Id" NOT IN (SELECT "Student__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= DATE_TRUNC('week', CURRENT_DATE)) GROUP BY "BU_Name__c" ORDER BY student_count DESC LIMIT 2000

Q: "students who are not in market"
A: SELECT "Name", "Student_Marketing_Status__c", "Technology__c" FROM "Student__c" WHERE "Student_Marketing_Status__c" != 'In Market' ORDER BY "Student_Marketing_Status__c" LIMIT 2000

Q: "list of all recruiters who did zero submissions last week"
A: SELECT DISTINCT "Recruiter_Name__c" FROM "Submissions__c" WHERE "Recruiter_Name__c" IS NOT NULL AND "Recruiter_Name__c" NOT IN (SELECT DISTINCT "Recruiter_Name__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week' AND "Submission_Date__c" < DATE_TRUNC('week', CURRENT_DATE) AND "Recruiter_Name__c" IS NOT NULL) LIMIT 2000

Q: "recruiter wise submissions this month"
A: SELECT "Recruiter_Name__c", COUNT(*) AS cnt FROM "Submissions__c" WHERE "Submission_Date__c" >= DATE_TRUNC('month', CURRENT_DATE) AND "Submission_Date__c" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' AND "Recruiter_Name__c" IS NOT NULL GROUP BY "Recruiter_Name__c" ORDER BY cnt DESC LIMIT 2000

Q: "details of Sai Ganesh Chinnamsetty"
A: SELECT "Name", "Student_Marketing_Status__c", "Technology__c", "Phone__c", "Marketing_Email__c", "Personal_Email__c", "Marketing_Visa_Status__c", "Days_in_Market_Business__c", "Last_Submission_Date__c", "Verbal_Confirmation_Date__c", "Project_Start_Date__c" FROM "Student__c" WHERE "Name" ILIKE '%Chinnamsetty%' LIMIT 2000

Q: "Details on NG-BU"
A: SELECT "Name", "Active__c", "Students_Count__c", "In_Market_Students_Count__c", "Verbal_Count__c", "Total_Expenses__c", "Each_Placement_Cost__c" FROM "Manager__c" WHERE "Name" ILIKE '%NG%' LIMIT 10

Q: "List all students under BU Divya Panguluri"
A: SELECT s."Name", s."Student_Marketing_Status__c", s."Technology__c", s."Days_in_Market_Business__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE m."Name" ILIKE '%Divya Panguluri%' ORDER BY s."Name" LIMIT 2000

Q: "List all in-market students with technology DS/AI"
A: SELECT "Name", "Technology__c", "Days_in_Market_Business__c", "Last_Submission_Date__c" FROM "Student__c" WHERE "Student_Marketing_Status__c" = 'In Market' AND "Technology__c" = 'DS/AI' LIMIT 2000

Q: "List all in-market students with 61 to 90 days in market"
A: SELECT "Name", "Technology__c", "Days_in_Market_Business__c", "Last_Submission_Date__c" FROM "Student__c" WHERE "Student_Marketing_Status__c" = 'In Market' AND "Days_in_Market_Business__c" >= 61 AND "Days_in_Market_Business__c" <= 90 ORDER BY "Days_in_Market_Business__c" DESC LIMIT 2000

Q: "last week interviews by BU"
A: SELECT m."Name" AS "BU_Name", s."Name" AS "Student_Name", i."Type__c", i."Final_Status__c", i."Amount__c", i."Interview_Date1__c" FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE i."Interview_Date1__c" >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week' AND i."Interview_Date1__c" < DATE_TRUNC('week', CURRENT_DATE) ORDER BY m."Name" LIMIT 2000

Q: "monthly submissions interviews confirmations BU wise"
A: SELECT "BU_Name__c", COUNT(*) AS cnt FROM "Submissions__c" WHERE "Submission_Date__c" >= DATE_TRUNC('month', CURRENT_DATE) AND "Submission_Date__c" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' GROUP BY "BU_Name__c" ORDER BY cnt DESC LIMIT 2000

COMPLEX QUERIES (multiple conditions, subqueries, JOINs):

Q: "students who had interviews last week but no submissions this week"
A: SELECT "Name", "Technology__c", "Days_in_Market_Business__c" FROM "Student__c" WHERE "Student_Marketing_Status__c" = 'In Market' AND "Id" IN (SELECT "Student__c" FROM "Interviews__c" WHERE "Interview_Date1__c" >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week' AND "Interview_Date1__c" < DATE_TRUNC('week', CURRENT_DATE)) AND "Id" NOT IN (SELECT "Student__c" FROM "Submissions__c" WHERE "Submission_Date__c" >= DATE_TRUNC('week', CURRENT_DATE)) LIMIT 2000

Q: "in-market students under BU Divya with more than 90 days and no interviews in 2 weeks"
A: SELECT s."Name", s."Technology__c", s."Days_in_Market_Business__c", m."Name" AS "BU_Name" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'In Market' AND m."Name" ILIKE '%Divya%' AND s."Days_in_Market_Business__c" > 90 AND s."Id" NOT IN (SELECT "Student__c" FROM "Interviews__c" WHERE "Interview_Date1__c" >= CURRENT_DATE - INTERVAL '14 days') ORDER BY s."Days_in_Market_Business__c" DESC LIMIT 2000

Q: "BUs with submissions this month but zero interviews"
A: SELECT DISTINCT sub."BU_Name__c" FROM "Submissions__c" sub WHERE sub."Submission_Date__c" >= DATE_TRUNC('month', CURRENT_DATE) AND sub."Submission_Date__c" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' AND sub."BU_Name__c" NOT IN (SELECT DISTINCT m."Name" FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE i."Interview_Date1__c" >= DATE_TRUNC('month', CURRENT_DATE) AND i."Interview_Date1__c" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month') LIMIT 2000

Q: "top 5 BUs by submissions this month with their interview count"
A: SELECT sub_data."BU_Name__c", sub_data.sub_count, COALESCE(int_data.int_count, 0) AS int_count FROM (SELECT "BU_Name__c", COUNT(*) AS sub_count FROM "Submissions__c" WHERE "Submission_Date__c" >= DATE_TRUNC('month', CURRENT_DATE) AND "Submission_Date__c" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' GROUP BY "BU_Name__c" ORDER BY sub_count DESC LIMIT 5) sub_data LEFT JOIN (SELECT m."Name" AS bu_name, COUNT(*) AS int_count FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE i."Interview_Date1__c" >= DATE_TRUNC('month', CURRENT_DATE) AND i."Interview_Date1__c" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' GROUP BY m."Name") int_data ON sub_data."BU_Name__c" = int_data.bu_name

Q: "students with verbal confirmation this month and their BU"
A: SELECT s."Name", s."Technology__c", s."Verbal_Confirmation_Date__c", m."Name" AS "BU_Name" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Student_Marketing_Status__c" = 'Verbal Confirmation' AND s."Verbal_Confirmation_Date__c" >= DATE_TRUNC('month', CURRENT_DATE) AND s."Verbal_Confirmation_Date__c" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' ORDER BY s."Verbal_Confirmation_Date__c" DESC LIMIT 2000

FIELD NAME WARNINGS (common mistakes to avoid):
- Student__c does NOT have "Email__c" — use "Marketing_Email__c" or "Personal_Email__c"
- Interviews__c does NOT have "BU_Name__c" — JOIN through Student__c -> Manager__c
- Interviews__c."Onsite_Manager__c" is NOT the BU manager — it is a different role
- Use "Interview_Date1__c" (Date type) for date comparisons, NOT "Interview_Date__c" (DateTime)
- Student__c."Offshore_Manager_Name__c" = offshore manager name (text field, no JOIN needed)
- Submissions__c."BU_Name__c" = BU manager name (text field, no JOIN needed)
- Submissions__c."Offshore_Manager_Name__c" = offshore manager name (text field, no JOIN needed)"""

ANSWER_PROMPT = """You are a helpful data assistant for a staffing/consulting company. You answer questions conversationally, like a knowledgeable colleague.

""" + DOMAIN_KNOWLEDGE + """

CORE RULES:
- Use ONLY the data in QUERY RESULTS. NEVER fabricate or guess numbers.
- NEVER use subjective words: "impressive", "concerning", "notable", "interesting", "surprisingly", "unfortunately", "alarming".
- NEVER predict future trends unless explicitly asked.
- If the data cannot answer the question, say: "The data shows [what it shows], but I cannot determine [what was asked] from this."

CONVERSATIONAL RULES:
- Be direct and natural. Don't sound like a report generator.
- For grouped/breakdown data: after the table, add 1-2 lines highlighting the highest and lowest values. Example: "Divya's BU leads with 32 submissions, while Ravi's BU has the fewest at 8."
- For comparison questions: highlight the difference. Example: "Submissions are up 15% from last week (127 vs 110)."
- For trend data: describe the direction factually. Example: "Submissions have increased each of the last 3 weeks."
- For simple counts: just answer naturally. "There are **2,000 students** currently in market."
- Don't over-explain. One insight line after grouped data is enough.
- If conversation history is provided, reference it naturally. Example: "Following up on the BU-wise data..."

RESPONSE LENGTH — match to question complexity:
  * Simple count question ("how many X?") -> ONE sentence with the number. Nothing else.
  * Breakdown question ("BU wise", "tech wise") -> A summary table. No extra detail unless asked.
  * Detail question ("show", "list", "details of") -> Table of records.
  * Report question ("weekly report", "performance") -> Full structured report.
- If 0 records or error: say so clearly, suggest a rephrased question.

RESPONSE RULES BY QUESTION TYPE:

TYPE 1: SIMPLE COUNT ("how many students in market?", "total submissions today?")
- Answer in ONE sentence: "**2,000 students** are currently in market."
- Do NOT show tables, breakdowns, or details unless explicitly asked.
- Do NOT add insights, warnings, or suggestions.

TYPE 2: BREAKDOWN ("BU wise", "by technology", "count by status")
- Show a clean summary table with the grouping and counts:
  | BU Name | Count |
  |:--------|:-----:|
  | Divya Panguluri | 18 |
  | Abhijith Reddy | 15 |
  | **Total** | **33** |
- Add a one-line headline before the table.
- No detail records unless asked.

TYPE 3: LIST/SHOW ("show students", "list submissions", "details of X")
- Headline with count: "**45 students** under BU Divya Panguluri."
- Then a table of records (max 25 rows, note if more exist).
- Group by BU/category if data spans multiple groups.

TYPE 4: REPORT ("weekly report", "performance report", "send report for BU X")
- Full structured report with headline, summary table, details per group, and insights.

FORMATTING:
- **Bold** key numbers. Format dates as "Apr 15, 2026", numbers with commas.
- Tables: left-align names, center-align numbers. Max 6 columns.
- Never show database IDs.
- No filler phrases like "Based on the data..." or "According to the results...".
- If data has a PRE-COMPUTED SUMMARY section, use those exact numbers.

EXAMPLES:

Q: "how many students in market?" (data: [{cnt: 2000}])
A: **2,000 students** are currently in market.

Q: "submissions BU wise this month" (data: [{BU_Name__c: "Divya", cnt: 32}, {BU_Name__c: "Abhijith", cnt: 28}...])
A: **127 submissions** this month across **8 BUs**.

| BU Name | Submissions |
|:--------|:-----------:|
| **Divya Panguluri** | **32** |
| Abhijith Reddy | 28 |
| Prabhakar Kunreddy | 22 |
| **Total** | **127** |

Q: "show students under Divya" (data: records with Name, Technology, etc.)
A: **18 students** under BU Divya Panguluri.

| Student Name | Technology | Days in Market | Status |
|:-------------|:-----------|:--------------:|:------:|
| Ravi Kumar | JAVA | 45 | In Market |
| Priya Sharma | DE | 30 | In Market |
(showing 18 records)

WHATSAPP / MESSAGE DRAFTING:
When user asks to "draft a message", "send to whatsapp", "write a message for":
- Plain text, concise, with numbers from data. Under 200 words.
- Use *bold* (WhatsApp style), not markdown **bold**."""

WEEKLY_REPORT_PROMPT = """You are a performance report generator for a staffing/consulting company. Generate a WhatsApp-style weekly performance report from the data below.

FORMAT RULES:
- Use PLAIN TEXT only (no markdown bold/italics — WhatsApp uses *bold* and _italic_).
- Use *text* for bold (WhatsApp style).
- Use emojis for visual structure.
- Keep it concise, data-driven, and action-oriented.
- Group by Offshore Manager → Recruiter → Student hierarchy when data is available.
- Include totals per manager and grand total.
- Highlight top performers and students needing attention.
- End with a motivational line or action item.

REPORT STRUCTURE:
1. Header with report title, BU name, and date range
2. Summary numbers (total submissions, interviews, confirmations)
3. Per-manager breakdown with student details
4. Students needing attention (0 submissions, no interviews)
5. Closing line

EXAMPLE OUTPUT:
📊 *Weekly Performance Report*
*BU: Gulam Siddiqui*
📅 Week: Apr 14 - Apr 20, 2026

━━━━━━━━━━━━━━━━━━━━
📈 *Summary*
• Submissions: 45
• Interviews: 12
• Confirmations: 2
━━━━━━━━━━━━━━━━━━━━

👤 *Manager: Ravi Kumar* (18 subs, 5 ints)

┌ Student Name | Subs | Ints
├ Amit Patel | 5 | 2
├ Priya Singh | 4 | 1
├ Rahul Sharma | 3 | 1
└ Others (4 students) | 6 | 1

👤 *Manager: Lakshmi Reddy* (12 subs, 3 ints)

┌ Student Name | Subs | Ints
├ Kiran Rao | 4 | 1
├ Neha Gupta | 3 | 1
└ Others (3 students) | 5 | 1

━━━━━━━━━━━━━━━━━━━━
⚠️ *Needs Attention*
• Suresh Kumar - 0 submissions (5 days)
• Deepak Verma - 0 interviews (14 days)
━━━━━━━━━━━━━━━━━━━━

✅ *Total: 45 subs | 12 ints | 2 confirmations*
🎯 Keep pushing! Target: 60 subs next week.

RULES:
- Use ONLY data from QUERY RESULTS. Never invent names or numbers.
- If a field is missing, skip that section.
- Show ALL students with data, grouped by manager.
- For students with 0 activity, list them in the "Needs Attention" section.
- Include Days in Market if available.
"""

ROUTER_PROMPT = """Decide how to answer this database question. Return ONLY one word.
Default to SQL unless the question is clearly about finding similar/related records.
SQL — counts, lists, filters, sums, dates, specific records, status, reports, performance, any data question
RAG — ONLY for: "find similar", "records like", "recommend", vague pattern matching
BOTH — need exact data AND similarity search (very rare)
Return ONLY: SQL, RAG, or BOTH"""

RAG_PROMPT = """You are an elite data analyst for a staffing/consulting company. You have:
1. QUERY RESULTS — exact numbers from the database (authoritative)
2. SIMILAR RECORDS — found by semantic search (supplementary context)

RULES:
- QUERY RESULTS are the primary source of truth. Never contradict them.
- Use SIMILAR RECORDS only to add context or confirm patterns.
- Never fabricate data. Every number must come from the results.
- Follow the same formatting rules as the main answer prompt (bold numbers, tables, insights).
- Start with a headline, then summary table, then insights."""

SUGGESTIONS_PROMPT = """Given a user's question and the answer they just received, suggest 3
short follow-up questions they might naturally ask next. Each must be under 50 characters
and be a natural refinement, drill-down, or related angle — not a repetition.
Return ONLY a JSON array of strings, no prose, no markdown. Example: ["...", "...", "..."]"""


# ── AI Provider Calls ────────────────────────────────────────────

async def _call_ai(system, message, max_tokens=2000, provider=None, temperature=0.1):
    """
    Call an AI provider. If provider is specified, use only that one.
    Otherwise try Claude → Grok → OpenAI in order.
    """
    msgs = [{"role": "user", "content": message}]

    def _claude():
        from anthropic import Anthropic
        import httpx
        r = Anthropic(api_key=settings.anthropic_api_key, timeout=httpx.Timeout(45.0, connect=10.0)).messages.create(
            model=settings.claude_model, max_tokens=max_tokens, system=system,
            temperature=temperature, messages=msgs)
        return r.content[0].text

    def _grok():
        from openai import OpenAI
        r = OpenAI(api_key=settings.grok_api_key, base_url="https://api.x.ai/v1", timeout=45.0).chat.completions.create(
            model=settings.grok_model, max_tokens=max_tokens, temperature=temperature,
            messages=[{"role": "system", "content": system}] + msgs)
        return r.choices[0].message.content

    def _openai():
        from openai import OpenAI
        r = OpenAI(api_key=settings.openai_api_key, timeout=45.0).chat.completions.create(
            model=settings.openai_model, max_tokens=max_tokens, temperature=temperature,
            messages=[{"role": "system", "content": system}] + msgs)
        return r.choices[0].message.content

    if provider == "claude" and settings.anthropic_api_key:
        try: return _claude()
        except Exception as e: logger.warning(f"Claude: {str(e)[:80]}")
        return None
    if provider == "grok" and settings.grok_api_key:
        try: return _grok()
        except Exception as e: logger.warning(f"Grok: {str(e)[:80]}")
        return None
    if provider == "openai" and settings.openai_api_key:
        try: return _openai()
        except Exception as e: logger.warning(f"OpenAI: {str(e)[:80]}")
        return None

    # Default fallback order: Claude → OpenAI (GPT-4o) → Grok
    # OpenAI before Grok because GPT-4o follows formatting instructions much better
    if settings.anthropic_api_key:
        try: return _claude()
        except Exception as e: logger.warning(f"Claude: {str(e)[:80]}")
    if settings.openai_api_key:
        try: return _openai()
        except Exception as e: logger.warning(f"OpenAI: {str(e)[:80]}")
    if settings.grok_api_key:
        try: return _grok()
        except Exception as e: logger.warning(f"Grok: {str(e)[:80]}")

    return None


async def _call_ai_stream(system, message, max_tokens=2000):
    """Async generator yielding text chunks. Tries Claude → OpenAI → Grok."""
    msgs = [{"role": "user", "content": message}]

    if settings.anthropic_api_key:
        try:
            from anthropic import Anthropic
            import httpx
            client = Anthropic(api_key=settings.anthropic_api_key, timeout=httpx.Timeout(60.0, connect=10.0))
            with client.messages.stream(
                model=settings.claude_model, max_tokens=max_tokens,
                system=system, messages=msgs,
            ) as stream:
                for text in stream.text_stream:
                    if text:
                        yield text
            return
        except Exception as e:
            logger.warning(f"Claude stream: {str(e)[:80]}")

    if settings.openai_api_key:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=settings.openai_api_key)
            response = client.chat.completions.create(
                model=settings.openai_model, max_tokens=max_tokens, temperature=0.1,
                messages=[{"role": "system", "content": system}] + msgs, stream=True,
            )
            for chunk in response:
                delta = chunk.choices[0].delta.content if chunk.choices else None
                if delta:
                    yield delta
            return
        except Exception as e:
            logger.warning(f"OpenAI stream: {str(e)[:80]}")

    if settings.grok_api_key:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=settings.grok_api_key, base_url="https://api.x.ai/v1")
            response = client.chat.completions.create(
                model=settings.grok_model, max_tokens=max_tokens, temperature=0.1,
                messages=[{"role": "system", "content": system}] + msgs, stream=True,
            )
            for chunk in response:
                delta = chunk.choices[0].delta.content if chunk.choices else None
                if delta:
                    yield delta
            return
        except Exception as e:
            logger.warning(f"Grok stream: {str(e)[:80]}")

    return


# ── Data Summary (pre-compute for AI accuracy) ──────────────────

async def _build_data_summary(records, true_total=None, soql_query=None):
    """Pre-compute counts and groupings from records so the AI doesn't have to count manually."""
    if not records or len(records) < 3:
        return ""

    total = true_total or len(records)
    is_limited = true_total and true_total > len(records)

    summary_lines = []
    summary_lines.append(f"\n{'='*60}")
    summary_lines.append(f"PRE-COMPUTED DATA SUMMARY — COPY THESE NUMBERS INTO YOUR RESPONSE")
    summary_lines.append(f"{'='*60}")
    summary_lines.append(f"TOTAL RECORDS = {total}")
    if is_limited:
        summary_lines.append(f"NOTE: {true_total} total records. Use {true_total} in your headline.")

    # If result was limited, run GROUP BY queries for accurate breakdowns
    group_by_results = {}
    if is_limited and soql_query:
        from_m = re.search(r'FROM\s+("[\w]+"|\w+)', soql_query, re.IGNORECASE)
        where_m = re.search(r'(WHERE\s+.+?)(?:\s+ORDER|\s+GROUP|\s+LIMIT|\s*$)', soql_query, re.IGNORECASE | re.DOTALL)
        if from_m:
            obj_name = from_m.group(1)
            where_clause = where_m.group(1) if where_m else ""
            # Determine which groupable fields exist in the records
            groupable_fields = []
            sample = records[0] if records else {}
            for field in ["Technology__c", "BU_Name__c", "Onsite_Manager__c",
                          "Student_Marketing_Status__c", "Final_Status__c",
                          "Type__c", "Offshore_Manager_Name__c", "Recruiter_Name__c",
                          "Project_Type__c", "Marketing_Visa_Status__c"]:
                if field in sample:
                    groupable_fields.append(field)

            for field in groupable_fields[:4]:
                try:
                    quoted_field = f'"{field}"'
                    # obj_name might already be quoted from the AI SQL
                    table_ref = obj_name if obj_name.startswith('"') else f'"{obj_name}"'
                    gq = f"SELECT {quoted_field}, COUNT(*) AS cnt FROM {table_ref} {where_clause} GROUP BY {quoted_field} ORDER BY cnt DESC LIMIT 30"
                    logger.info(f"GROUP BY query: {gq[:200]}")
                    gr = await execute_query(gq)
                    logger.info(f"GROUP BY {field} result: {len(gr.get('records', []))} rows")
                    if "error" not in gr and gr.get("records"):
                        counts = {}
                        for rec in gr["records"]:
                            val = rec.get(field)
                            cnt = rec.get("cnt") or rec.get("count", 0)
                            if val and val != "None":
                                counts[val] = cnt
                        if counts and len(counts) > 1:
                            group_by_results[field] = counts
                            logger.info(f"GROUP BY {field} counts: {dict(list(counts.items())[:5])}")
                except Exception as e:
                    logger.warning(f"GROUP BY {field} failed: {e}")

    group_fields = ["BU_Name__c", "Onsite_Manager__c", "Offshore_Manager_Name__c",
                     "Manager__r", "Recruiter_Name__c", "Technology__c",
                     "Student_Marketing_Status__c", "Type__c", "Final_Status__c",
                     "Project_Type__c", "_query_label"]

    tables_built = []

    for field in group_fields:
        # Use GROUP BY results if available (accurate for full dataset)
        if field in group_by_results:
            counts = group_by_results[field]
            label = field.replace("__c", "").replace("__r", "").replace("_", " ")
            sorted_counts = sorted(counts.items(), key=lambda x: -x[1])
            group_total = sum(c for _, c in sorted_counts)
            summary_lines.append(f"\nBREAKDOWN BY {label.upper()} (copy these exact numbers):")
            summary_lines.append(f"| {label} | Count |")
            summary_lines.append(f"|---|---|")
            for name, count in sorted_counts[:20]:
                summary_lines.append(f"| {name} | {count} |")
            summary_lines.append(f"| **Total** | **{group_total}** |")
            if len(sorted_counts) > 20:
                summary_lines.append(f"(+ {len(sorted_counts) - 20} more groups)")
            tables_built.append(label)
            continue

        # Fallback: count from fetched records
        counts = {}
        for r in records:
            val = None
            if field == "Manager__r" and isinstance(r.get("Manager__r"), dict):
                val = r["Manager__r"].get("Name")
            elif field == "Manager__r":
                continue
            else:
                val = r.get(field)
            if val and val != "None":
                counts[val] = counts.get(val, 0) + 1

        if counts and 1 < len(counts) <= 50:
            label = field.replace("__c", "").replace("__r", "").replace("_", " ")
            sorted_counts = sorted(counts.items(), key=lambda x: -x[1])
            group_total = sum(c for _, c in sorted_counts)
            summary_lines.append(f"\nBREAKDOWN BY {label.upper()} (copy these exact numbers):")
            summary_lines.append(f"| {label} | Count |")
            summary_lines.append(f"|---|---|")
            for name, count in sorted_counts[:20]:
                summary_lines.append(f"| {name} | {count} |")
            summary_lines.append(f"| **Total** | **{group_total}** |")
            if len(sorted_counts) > 20:
                summary_lines.append(f"(+ {len(sorted_counts) - 20} more groups)")
            tables_built.append(label)

    # Sum numeric fields
    num_fields = ["Amount__c", "Amount_INR__c", "Bill_Rate__c", "Pay_Roll_Tax__c",
                  "Profit__c", "Caluculated_Pay_Rate__c", "Payroll_Month__c",
                  "Total_Expenses_MIS__c", "Each_Placement_Cost__c",
                  "Days_in_Market_Business__c"]
    for field in num_fields:
        vals = [r.get(field) for r in records if r.get(field) is not None]
        if vals:
            try:
                nums = [float(v) for v in vals if v is not None]
                if nums:
                    label = field.replace("__c", "").replace("_", " ")
                    summary_lines.append(f"  {label}: sum={sum(nums):,.2f}, avg={sum(nums)/len(nums):,.2f}, count={len(nums)}")
            except (ValueError, TypeError):
                pass

    summary_lines.append(f"{'='*60}")

    return "\n".join(summary_lines) if tables_built else ""


# ── Helper Functions ─────────────────────────────────────────────

async def _generate_suggestions(question, answer, max_n=3):
    if not answer:
        return []
    try:
        snippet = (answer or "")[:1200]
        raw = await _call_ai(
            SUGGESTIONS_PROMPT, f"Question: {question}\n\nAnswer: {snippet}",
            max_tokens=200,
        )
        if not raw:
            return []
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            if raw.lower().startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        start = raw.find("[")
        end = raw.rfind("]")
        if start == -1 or end == -1 or end <= start:
            return []
        arr = json.loads(raw[start:end + 1])
        if not isinstance(arr, list):
            return []
        out = []
        for item in arr:
            if isinstance(item, str):
                s = item.strip()
                if s and len(s) <= 80:
                    out.append(s[:80])
            if len(out) >= max_n:
                break
        return out
    except Exception as e:
        logger.warning(f"Suggestions failed: {str(e)[:80]}")
        return []


def _get_focused_schema(obj_names):
    """Build a detailed schema prompt for specific objects only."""
    schema = get_schema()
    if not schema:
        return ""
    lines = []
    for obj_name in obj_names:
        if obj_name not in schema:
            continue
        obj_data = schema[obj_name]
        count = obj_data.get("record_count", "?")
        label = obj_data.get("label", obj_name)
        lines.append(f"\n{obj_name} ({label}, {count:,} records):")
        lines.append("  ALL FIELDS (use exact names):")
        for f in obj_data.get("fields", []):
            refs = f.get("referenceTo", [])
            ref_str = f" → {', '.join(refs)}" if refs else ""
            groupable = " [NOT groupable]" if not f.get("groupable", True) else ""
            lines.append(f"    {f['name']} ({f['type']}, \"{f['label']}\"){ref_str}{groupable}")
    return "\n".join(lines)


def _extract_object_fields_hint(soql, _schema_text=None):
    """Extract object names from a SQL query (including JOINs) and list their exact fields."""
    all_tables = re.findall(r'(?:FROM|JOIN)\s+"?(\w+)"?', soql, re.IGNORECASE)
    if not all_tables:
        return ""
    schema = get_schema()
    lines = []
    seen = set()
    for obj_name in all_tables:
        if obj_name in seen or obj_name not in schema:
            continue
        seen.add(obj_name)
        fields = schema[obj_name].get("fields", [])
        field_names = [f"{f['name']} ({f['label']}, {f['type']})" for f in fields[:80]]
        lines.append(f"\nAVAILABLE FIELDS on {obj_name}:")
        lines.extend(field_names)
    return "\n".join(lines) if lines else ""


# ── Common AI mistakes → auto-fix rules ──────────────────────────
_FIELD_FIXES = {
    "Email__c": "Marketing_Email__c",
    "email__c": "Marketing_Email__c",
    "Interview_Date__c": "Interview_Date1__c",
    "interview_date__c": "Interview_Date1__c",
    "BU_Name": "BU_Name__c",
    "Status__c": "Student_Marketing_Status__c",
    "Marketing_Status__c": "Student_Marketing_Status__c",
    "Visa_Status__c": "Marketing_Visa_Status__c",
    "Days_In_Market__c": "Days_in_Market_Business__c",
    "days_in_market": "Days_in_Market_Business__c",
    "Last_Submission__c": "Last_Submission_Date__c",
    "Submission_Date": "Submission_Date__c",
    "Interview_Date": "Interview_Date1__c",
    "Full_Name__c": "Student_Full_Name__c",
    "Student_Name": "Student_Full_Name__c",
}

_INTERVIEW_BU_JOIN = (
    ' LEFT JOIN "Student__c" _s ON "Interviews__c"."Student__c" = _s."Id"'
    ' LEFT JOIN "Manager__c" _m ON _s."Manager__c" = _m."Id"'
)


def _auto_fix_sql(soql):
    """Auto-fix common AI SQL mistakes before execution. Returns (fixed_sql, fixes_applied)."""
    fixes = []
    fixed = soql

    # 1. Fix known wrong field names
    for wrong, correct in _FIELD_FIXES.items():
        if f'"{wrong}"' in fixed:
            fixed = fixed.replace(f'"{wrong}"', f'"{correct}"')
            fixes.append(f'{wrong} → {correct}')

    # 2. Fix Interview BU queries: AI uses BU_Name__c on Interviews but it doesn't exist
    all_tables = re.findall(r'(?:FROM|JOIN)\s+"?(\w+)"?', fixed, re.IGNORECASE)
    if 'Interviews__c' in all_tables and 'Manager__c' not in all_tables:
        if '"BU_Name__c"' in fixed or 'BU_Name' in fixed:
            # Need to add JOIN through Student→Manager
            from_m = re.search(r'(FROM\s+"Interviews__c"(?:\s+\w+)?)', fixed, re.IGNORECASE)
            if from_m:
                fixed = fixed.replace(from_m.group(1), from_m.group(1) + _INTERVIEW_BU_JOIN)
                fixed = fixed.replace('"BU_Name__c"', '_m."Name"')
                fixes.append('Added Student→Manager JOIN for BU name on Interviews')

    # 3. Fix wrong date fields: CreatedDate instead of proper date fields
    if 'Submissions__c' in all_tables and '"CreatedDate"' in fixed:
        if 'Submission_Date__c' not in fixed:
            fixed = fixed.replace('"CreatedDate"', '"Submission_Date__c"')
            fixes.append('CreatedDate → Submission_Date__c for Submissions')

    if 'Interviews__c' in all_tables and '"CreatedDate"' in fixed:
        if 'Interview_Date1__c' not in fixed:
            fixed = fixed.replace('"CreatedDate"', '"Interview_Date1__c"')
            fixes.append('CreatedDate → Interview_Date1__c for Interviews')

    # 4. Fix Onsite_Manager__c used as BU name (it's not the BU manager)
    if '"Onsite_Manager__c"' in fixed and any(kw in soql.lower() for kw in ['bu', 'manager', 'group']):
        if 'Manager__c' not in all_tables and 'Interviews__c' in all_tables:
            from_m = re.search(r'(FROM\s+"Interviews__c"(?:\s+\w+)?)', fixed, re.IGNORECASE)
            if from_m and _INTERVIEW_BU_JOIN not in fixed:
                fixed = fixed.replace(from_m.group(1), from_m.group(1) + _INTERVIEW_BU_JOIN)
            fixed = fixed.replace('"Onsite_Manager__c"', '_m."Name"')
            fixes.append('Onsite_Manager__c → Manager JOIN for BU name')

    # 5. Fix missing double-quotes on Salesforce table names
    for tbl in ['Student__c', 'Submissions__c', 'Interviews__c', 'Manager__c', 'Job__c',
                'Employee__c', 'Organization__c', 'Contact', 'Account']:
        # Match unquoted table name after FROM/JOIN (not already quoted)
        pattern = rf'(FROM|JOIN)\s+(?!")({re.escape(tbl)})(\s)'
        if re.search(pattern, fixed, re.IGNORECASE):
            fixed = re.sub(pattern, rf'\1 "{tbl}"\3', fixed, flags=re.IGNORECASE)
            fixes.append(f'Added quotes to {tbl}')

    # 6. Fix lowercase table names (stale copies)
    stale_map = {
        'students': '"Student__c"', 'submissions': '"Submissions__c"',
        'interviews': '"Interviews__c"', 'managers': '"Manager__c"',
        'jobs': '"Job__c"', 'employees': '"Employee__c"',
    }
    for stale, correct in stale_map.items():
        pattern = rf'(FROM|JOIN)\s+{stale}(\s|$)'
        if re.search(pattern, fixed, re.IGNORECASE):
            fixed = re.sub(pattern, rf'\1 {correct}\2', fixed, flags=re.IGNORECASE)
            fixes.append(f'{stale} → {correct}')

    # 7. Fix technology value normalization (AI strips special chars)
    tech_fixes = {
        "'DSAI'": "'DS/AI'", "'dsai'": "'DS/AI'", "'Ds/Ai'": "'DS/AI'", "'ds/ai'": "'DS/AI'",
        "'JAVA'": "'JAVA'", "'java'": "'JAVA'", "'Java'": "'JAVA'",
        "'dotnet'": "'.NET'", "'DOTNET'": "'.NET'", "'DotNet'": "'.NET'", "'Dotnet'": "'.NET'",
        "'dot net'": "'.NET'", "'DOT NET'": "'.NET'",
        "'net'": "'.NET'", "'NET'": "'.NET'",
        "'servicenow'": "'Service Now'", "'ServiceNow'": "'Service Now'", "'SERVICENOW'": "'Service Now'",
        "'powerbi'": "'PowerBI'", "'POWERBI'": "'PowerBI'", "'Power BI'": "'PowerBI'",
        "'rpa'": "'RPA'",
        "'devops'": "'DevOps'", "'DEVOPS'": "'DevOps'",
        "'sfdc'": "'SFDC'",
    }
    if '"Technology__c"' in fixed:
        for wrong_tech, correct_tech in tech_fixes.items():
            if wrong_tech in fixed:
                fixed = fixed.replace(wrong_tech, correct_tech)
                fixes.append(f'Tech value {wrong_tech} → {correct_tech}')

    # 8. Fix picklist value typos
    value_fixes = {
        "'in market'": "'In Market'", "'IN MARKET'": "'In Market'",
        "'pre marketing'": "'Pre Marketing'", "'PRE MARKETING'": "'Pre Marketing'",
        "'verbal confirmation'": "'Verbal Confirmation'", "'VERBAL CONFIRMATION'": "'Verbal Confirmation'",
        "'project started'": "'Project Started'", "'PROJECT STARTED'": "'Project Started'",
        "'project completed'": "'Project Completed'", "'PROJECT COMPLETED'": "'Project Completed'",
        "'exit'": "'Exit'", "'EXIT'": "'Exit'",
        "'Bench'": "'In Market'", "'bench'": "'In Market'", "'on bench'": "'In Market'",
    }
    for wrong_val, correct_val in value_fixes.items():
        if wrong_val in fixed:
            fixed = fixed.replace(wrong_val, correct_val)
            fixes.append(f'Value {wrong_val} → {correct_val}')

    if fixes:
        logger.info(f"Auto-fixed SQL: {', '.join(fixes)}")
    return fixed, fixes


def _validate_soql_fields(soql):
    """Check if the SQL query uses valid field/object names. Returns error string or None."""
    schema = get_schema()
    if not schema:
        return None

    # For JOINed queries, collect valid fields from ALL tables in the query
    all_tables = re.findall(r'(?:FROM|JOIN)\s+"?(\w+)"?', soql, re.IGNORECASE)
    if not all_tables:
        return None

    obj_name = all_tables[0]
    if obj_name not in schema:
        return f"Object '{obj_name}' not found. Available: {', '.join(sorted(schema.keys())[:20])}"

    valid_fields = {'count', 'id', 'cnt', 'total', 'total_amount', 'range_label', 'avg_days', 'sub_cnt', 'int_cnt', 'conf_cnt'}
    for tbl in all_tables:
        if tbl in schema:
            valid_fields.update(f['name'].lower() for f in schema[tbl].get('fields', []))

    bad_fields = []

    # Check SELECT clause
    select_m = re.search(r'SELECT\s+(.+?)\s+FROM', soql, re.IGNORECASE | re.DOTALL)
    if select_m:
        for part in select_m.group(1).split(','):
            part = part.strip()
            if not part or '(' in part:
                continue
            if ' AS ' in part.upper():
                part = part[:part.upper().index(' AS ')].strip()
            if '__r.' in part or '.' in part:
                continue
            if '*' in part:
                continue
            field = part.strip().strip('"')
            if field.lower() not in valid_fields:
                bad_fields.append(field)

    # Check WHERE clause fields
    where_m = re.search(r'WHERE\s+(.+?)(?:\s+ORDER|\s+GROUP|\s+LIMIT|\s*$)', soql, re.IGNORECASE | re.DOTALL)
    if where_m:
        where_clause = where_m.group(1)
        for field_m in re.finditer(r'"?([\w.]+)"?\s*(?:=|!=|<|>|LIKE|ILIKE|IN\s*\(|IS\s)', where_clause, re.IGNORECASE):
            field = field_m.group(1).strip()
            if field.upper() in ('AND', 'OR', 'NOT', 'NULL', 'TRUE', 'FALSE', 'TODAY',
                                  'YESTERDAY', 'THIS_MONTH', 'LAST_MONTH', 'THIS_YEAR',
                                  'LAST_N_DAYS', 'NEXT_N_DAYS', 'CURRENT_DATE', 'DATE_TRUNC',
                                  'INTERVAL'):
                continue
            if '__r.' in field:
                continue
            field_name = field.split('.')[-1].strip('"')
            if field_name.lower() not in valid_fields:
                bad_fields.append(field_name)

    # Check GROUP BY / ORDER BY
    for clause in ('GROUP BY', 'ORDER BY'):
        clause_m = re.search(rf'{clause}\s+(.+?)(?:\s+(?:ORDER|LIMIT|HAVING)|\s*$)', soql, re.IGNORECASE)
        if clause_m:
            for part in clause_m.group(1).split(','):
                field = part.strip().split('.')[-1].strip().strip('"')
                if field and field.upper() not in ('ASC', 'DESC', 'NULLS', 'FIRST', 'LAST', 'CNT', 'TOTAL', 'TOTAL_AMOUNT'):
                    if field.lower() not in valid_fields:
                        bad_fields.append(field)

    if bad_fields:
        unique_bad = list(dict.fromkeys(bad_fields))
        return f"Invalid fields on {obj_name}: {', '.join(unique_bad)}. Use only fields listed in the schema."
    return None


async def _validate_picklist_values(soql):
    """Check if WHERE clause uses valid picklist values. Returns warning string or None."""
    picklists = await _load_picklist_values()
    if not picklists:
        return None

    field_to_picklist = {
        "Student_Marketing_Status__c": "Student_Marketing_Status__c",
        "Marketing_Visa_Status__c": "Marketing_Visa_Status__c",
        "Technology__c": "Technology__c",
        "Submission_Status__c": "Submission_Status__c",
        "Type__c": "Interview_Type__c",
        "Final_Status__c": "Interview_Final_Status__c",
        "Project_Type__c": "Job_Project_Type__c",
        "Deptment__c": "Employee_Deptment__c",
    }

    warnings = []
    for field_name, picklist_key in field_to_picklist.items():
        valid_vals = picklists.get(picklist_key, [])
        if not valid_vals:
            continue
        pattern = rf'"{re.escape(field_name)}"\s*=\s*\'([^\']+)\''
        for m in re.finditer(pattern, soql):
            used_val = m.group(1)
            if used_val not in valid_vals:
                close = [v for v in valid_vals if v.lower() == used_val.lower()]
                if close:
                    warnings.append(f"{field_name}: '{used_val}' should be '{close[0]}'")
                else:
                    best = None
                    best_ratio = 0
                    for v in valid_vals:
                        r = SequenceMatcher(None, used_val.lower(), v.lower()).ratio()
                        if r > best_ratio:
                            best_ratio = r
                            best = v
                    if best and best_ratio >= 0.6:
                        warnings.append(f"{field_name}: '{used_val}' → did you mean '{best}'?")
                    else:
                        warnings.append(f"{field_name}: '{used_val}' not in valid values: {', '.join(valid_vals[:10])}")

    if warnings:
        logger.warning(f"Picklist validation: {'; '.join(warnings)}")
        return "; ".join(warnings)
    return None


async def _pick_objects(question, schema_text):
    """Step 1: AI picks the right object(s) for the question."""
    raw = await _call_ai(
        OBJECT_PICKER_PROMPT,
        f"Schema objects:\n{schema_text[:4000]}\n\nQuestion: {question}",
        max_tokens=200, temperature=0,
    )
    if not raw:
        return None
    try:
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.strip("`").strip()
            if raw.lower().startswith("json"):
                raw = raw[4:].strip()
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            data = json.loads(raw[start:end + 1])
            objects = data.get("objects", [])
            reason = data.get("reason", "")
            if objects:
                logger.info(f"Object picker: {objects} — {reason}")
                return objects
    except Exception as e:
        logger.warning(f"Object picker parse failed: {str(e)[:60]}")
    return None


# ── Multi-query execution ────────────────────────────────────────

async def _execute_multi_query(query_pairs):
    """Execute multiple SQL queries and combine results."""
    all_recs = []
    all_queries = []
    total_size = 0
    has_error = False

    for soql, label in query_pairs:
        logger.info(f"Multi-query [{label}]: {soql[:150]}")
        result = await execute_query(soql)

        if "error" in result:
            logger.warning(f"Multi-query [{label}] error: {result['error'][:100]}")
            has_error = True
            continue

        recs = result.get("records", [])
        for r in recs:
            r.pop("attributes", None)
            r["_query_label"] = label

        all_recs.extend(recs)
        total_size += result.get("totalSize", len(recs))
        all_queries.append(f"-- {label}\n{soql}")

    if not all_recs and has_error:
        return " ; ".join(q for q, _ in query_pairs), {"error": "All queries failed"}, None

    queries_str = "\n".join(all_queries)
    combined_result = {"totalSize": total_size, "records": all_recs}
    return queries_str, combined_result, all_recs


# ── Complex Query Planning ────────────────────────────────────

_COMPLEX_INDICATORS = [
    re.compile(r'\b(?:but|however|except|excluding)\b.*\b(?:and|with|who|that)\b', re.I),
    re.compile(r'\b(?:who|that|which)\s+(?:have|has|had|did|do)\b.*\b(?:but|and)\b.*\b(?:no|not|without|zero)\b', re.I),
    re.compile(r'\b(?:with|having)\b.*\b(?:but|and)\b.*\b(?:without|no|not|zero)\b', re.I),
    re.compile(r'\b(?:compare|comparison|versus|vs)\b', re.I),
    re.compile(r'\b(?:between|from)\b.*\b(?:and|to)\b.*\b(?:between|from)\b', re.I),
]

_MULTI_CONDITION = re.compile(
    r'\b(?:and|but|who|that|with|under|where|having)\b',
    re.I
)

QUERY_PLAN_PROMPT = """You are a SQL query planner. Given a complex question about a staffing database,
break it down into a structured query plan.

Return ONLY a JSON object (no markdown, no explanation):
{
  "primary_table": "Student__c",
  "joins": ["Manager__c via Student__c.Manager__c"],
  "filters": [
    {"field": "Student_Marketing_Status__c", "op": "=", "value": "In Market"},
    {"type": "subquery", "logic": "NOT IN", "table": "Submissions__c", "date_field": "Submission_Date__c", "date_range": "this week"}
  ],
  "output": "list of student names with technology and days in market",
  "group_by": null,
  "order_by": "Days_in_Market_Business__c DESC"
}

IMPORTANT TABLE RELATIONSHIPS:
- Student__c.Manager__c -> Manager__c.Id (BU manager)
- Submissions__c.Student__c -> Student__c.Id (has BU_Name__c text field)
- Interviews__c.Student__c -> Student__c.Id (NO BU_Name__c — must JOIN through Student to Manager)
- Job__c.Student__c -> Student__c.Id
- Employee__c.Onshore_Manager__c -> Manager__c.Id

STATUS VALUES: 'In Market', 'Exit', 'Pre Marketing', 'Project Started', 'Project Completed', 'Verbal Confirmation'
NEGATION: "not in market" = != 'In Market', "no submissions" = NOT IN subquery, "without interviews" = NOT IN subquery
RECRUITERS: Submissions__c."Recruiter_Name__c" (text field)
BU: Manager__c."Name" or Submissions__c."BU_Name__c" (text field)"""


def _is_complex_query(question):
    """Detect if a question needs query planning."""
    q = question.lower()
    conditions = len(_MULTI_CONDITION.findall(q))
    if conditions >= 3:
        return True
    for pat in _COMPLEX_INDICATORS:
        if pat.search(q):
            return True
    return False


async def _plan_complex_query(question, schema_text):
    """Generate a query plan for complex questions, then use it to guide SQL generation."""
    try:
        plan_json = await _call_ai(QUERY_PLAN_PROMPT,
            f"Schema context:\n{schema_text[:4000]}\n\nQuestion: {question}",
            500, temperature=0)
        if not plan_json:
            return None
        plan_json = plan_json.strip()
        if plan_json.startswith("```"):
            plan_json = plan_json.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        plan = json.loads(plan_json)
        logger.info(f"Query plan: table={plan.get('primary_table')}, filters={len(plan.get('filters', []))}")
        return plan
    except (json.JSONDecodeError, Exception) as e:
        logger.debug(f"Query planning failed: {e}")
        return None


def _plan_to_hint(plan):
    """Convert query plan to a hint string for the SQL generator."""
    if not plan:
        return ""
    parts = ["\nQUERY PLAN (follow this structure):"]
    parts.append(f"  Primary table: {plan.get('primary_table', '?')}")
    if plan.get("joins"):
        parts.append(f"  JOINs needed: {', '.join(plan['joins'])}")
    for i, f in enumerate(plan.get("filters", []), 1):
        if f.get("type") == "subquery":
            parts.append(f"  Filter {i}: {f.get('logic', 'NOT IN')} subquery on {f.get('table')} "
                        f"(date: {f.get('date_field')} {f.get('date_range', '')})")
        else:
            parts.append(f"  Filter {i}: {f.get('field')} {f.get('op', '=')} '{f.get('value')}'")
    if plan.get("group_by"):
        parts.append(f"  GROUP BY: {plan['group_by']}")
    if plan.get("order_by"):
        parts.append(f"  ORDER BY: {plan['order_by']}")
    parts.append(f"  Output: {plan.get('output', 'records')}")
    return "\n".join(parts)


# ── Answer Validation ─────────────────────────────────────────

_NEGATION_WORDS_Q = re.compile(r'\b(?:not|no|non|without|zero|never|exclude|except|other than|don.t|doesn.t|aren.t|isn.t|wasn.t|weren.t)\b', re.I)
_NEGATION_SQL = re.compile(r'(?:!=|<>|\bNOT\s+IN\b|\bNOT\s+LIKE\b|\bNOT\s+ILIKE\b|\bIS\s+NOT\b|\bNOT\s+EXISTS\b)', re.I)

_TIME_PATTERNS = {
    "today": re.compile(r'\btoday\b', re.I),
    "yesterday": re.compile(r'\byesterday\b', re.I),
    "this week": re.compile(r'\bthis\s+week\b', re.I),
    "last week": re.compile(r'\blast\s+week\b', re.I),
    "this month": re.compile(r'\bthis\s+month\b', re.I),
    "last month": re.compile(r'\blast\s+month\b', re.I),
}

_TIME_SQL = {
    "today": re.compile(r'CURRENT_DATE(?!\s*-)', re.I),
    "yesterday": re.compile(r"CURRENT_DATE\s*-\s*INTERVAL\s*'1\s*day'", re.I),
    "this week": re.compile(r"DATE_TRUNC\s*\(\s*'week'", re.I),
    "last week": re.compile(r"DATE_TRUNC\s*\(\s*'week'.*INTERVAL\s*'1\s*week'", re.I),
    "this month": re.compile(r"DATE_TRUNC\s*\(\s*'month'", re.I),
    "last month": re.compile(r"DATE_TRUNC\s*\(\s*'month'.*INTERVAL\s*'1\s*month'", re.I),
}


async def _validate_answer_logic(question, sql):
    """
    Check if the SQL logic matches the user's intent.
    Returns a mismatch description string, or None if valid.
    Fast rule-based checks first, then AI validation for complex cases.
    """
    q_lower = question.lower()
    sql_upper = sql.upper()
    issues = []

    # 1. Negation mismatch: user says "not X" but SQL has "= X" without negation
    q_has_negation = bool(_NEGATION_WORDS_Q.search(q_lower))
    sql_has_negation = bool(_NEGATION_SQL.search(sql))
    if q_has_negation and not sql_has_negation:
        status_match = re.search(r'"Student_Marketing_Status__c"\s*=\s*\'([^\']+)\'', sql)
        if status_match:
            status_val = status_match.group(1)
            neg_context = re.search(rf'\b(?:not|no|non|without|exclude|except)\b.*\b{re.escape(status_val.lower().split()[0])}\b', q_lower)
            if neg_context:
                issues.append(f"User asked for NOT '{status_val}' but SQL uses = '{status_val}' (missing negation)")

    # 2. Wrong table: user asks about interviews but SQL queries submissions (or vice versa)
    if "interview" in q_lower and '"Interviews__c"' not in sql and '"Submissions__c"' in sql:
        if "submission" not in q_lower:
            issues.append("User asked about interviews but SQL queries Submissions__c instead of Interviews__c")
    if "submission" in q_lower and '"Submissions__c"' not in sql and '"Interviews__c"' in sql:
        if "interview" not in q_lower:
            issues.append("User asked about submissions but SQL queries Interviews__c instead of Submissions__c")

    # 3. Time range mismatch: user says "last week" but SQL has "this month"
    for time_label, q_pat in _TIME_PATTERNS.items():
        if q_pat.search(q_lower):
            sql_pat = _TIME_SQL.get(time_label)
            if sql_pat and not sql_pat.search(sql):
                other_times = [t for t, p in _TIME_SQL.items() if t != time_label and p.search(sql)]
                if other_times:
                    issues.append(f"User asked for '{time_label}' but SQL uses '{other_times[0]}' date range")
            break

    # 4. COUNT vs LIST mismatch: user asks "list/show" but SQL returns COUNT only
    wants_list = bool(re.search(r'\b(?:list|show|give me|get me|details|which|who)\b', q_lower))
    is_count_only = bool(re.match(r'\s*SELECT\s+COUNT\s*\(', sql, re.I)) and "GROUP BY" not in sql_upper
    if wants_list and is_count_only:
        issues.append("User wants to see records (list/show) but SQL only returns a COUNT")

    # 5. User asks "how many/count" but SQL returns full records (less critical, skip)

    # 6. GROUP BY mismatch: user asks "BU wise" but no GROUP BY
    if re.search(r'\b(?:bu\s*wise|by\s+bu|tech\s*wise|by\s+technology|recruiter\s*wise|by\s+recruiter)\b', q_lower):
        if "GROUP BY" not in sql_upper and "ORDER BY" not in sql_upper:
            issues.append("User asks for a breakdown (wise/by) but SQL has no GROUP BY or ORDER BY")

    if issues:
        return "; ".join(issues)

    # 7. AI validation for complex/ambiguous cases (only if no rule-based issues found)
    # Skip AI validation for simple queries to save cost/time
    is_complex = (
        q_has_negation
        or "compare" in q_lower
        or "between" in q_lower
        or re.search(r'\b(?:highest|lowest|top|bottom|best|worst|most|least)\b', q_lower)
    )
    if not is_complex:
        return None

    try:
        verdict = await _call_ai(
            "You verify if a SQL query correctly answers a user's question. "
            "Check: negation logic, correct table, correct filters, correct date range, correct aggregation. "
            "If the SQL correctly answers the question, respond with exactly: VALID\n"
            "If there is a mismatch, respond with: MISMATCH: <one sentence explaining the issue>",
            f"User question: {question}\nGenerated SQL: {sql}",
            100, temperature=0)
        if verdict and "MISMATCH" in verdict.upper():
            mismatch_reason = verdict.split(":", 1)[-1].strip() if ":" in verdict else verdict
            return mismatch_reason
    except Exception as e:
        logger.debug(f"AI validation skipped: {e}")

    return None


# ── SQL Path ────────────────────────────────────────────────────

async def _soql_path(question, schema_text, history=None, last_soql=None):
    # Check cache first (skip for follow-ups that modify previous SQL)
    if not last_soql:
        cached = _cache_get(question)
        if cached:
            return cached

    # Step 0a: Smart query cache (embedding-based semantic match)
    if not last_soql:
        cached_sql, score, cached_q = find_cached_query(question)
        if cached_sql:
            cached_sql, _ = _auto_fix_sql(cached_sql)
            result = await execute_query(cached_sql)
            if "error" not in result and result.get("records"):
                recs = result.get("records", [])
                for r in recs:
                    r.pop("attributes", None)
                logger.info(f"Smart cache served {len(recs)} records (score={score:.3f}, from='{cached_q[:40]}')")
                _cache_set(question, cached_sql, result, recs)
                return cached_sql, result, recs
            logger.info("Smart cache SQL failed, falling through to AI")

    # Step 0a-fallback: Fuzzy word-overlap cache (backup if embedding cache misses)
    if not last_soql:
        cached_sql, found = await _fuzzy_cache_lookup(question)
        if found and cached_sql:
            cached_sql, _ = _auto_fix_sql(cached_sql)
            result = await execute_query(cached_sql)
            if "error" not in result and result.get("records"):
                recs = result.get("records", [])
                for r in recs:
                    r.pop("attributes", None)
                logger.info(f"Fuzzy cache served {len(recs)} records")
                _cache_set(question, cached_sql, result, recs)
                return cached_sql, result, recs
            logger.info("Fuzzy cache SQL failed, falling through to AI")

    # Step 0b: Try direct pattern match (instant, no AI call needed)
    if not last_soql:
        pattern_match = _match_report_pattern(question)
        if pattern_match:
            pattern_queries = pattern_match["queries"]
            if len(pattern_queries) == 1:
                soql, label = pattern_queries[0]
                result = await execute_query(soql)
                if "error" not in result:
                    recs = result.get("records", [])
                    for r in recs:
                        r.pop("attributes", None)
                    return soql, result, recs
                logger.warning(f"Pattern query failed [{label}]: {result.get('error', '')[:100]}")
            else:
                queries_str, combined_result, combined_recs = await _execute_multi_query(pattern_queries)
                if combined_recs is not None:
                    summary_queries = pattern_match.get("summary_queries", [])
                    if summary_queries:
                        _, _, summary_recs = await _execute_multi_query(summary_queries)
                        if summary_recs:
                            combined_recs.extend(summary_recs)
                    return queries_str, combined_result, combined_recs

    learning = await get_learning_examples_prompt(question)

    # Load live picklist values from DB
    picklists = await _load_picklist_values()
    picklist_prompt = _build_picklist_prompt(picklists)

    # Step 1: Pick the right object(s)
    picked_objects = await _pick_objects(question, schema_text)

    # Step 2: Build focused schema for picked objects
    if picked_objects:
        focused = _get_focused_schema(picked_objects)
        prompt = f"TARGET OBJECTS (query these):\n{focused}\n\nFULL SCHEMA CONTEXT:\n{schema_text}\n{picklist_prompt}\n{learning}\nQuestion: {question}"
    else:
        prompt = f"Schema:\n{schema_text}\n{picklist_prompt}\n{learning}\nQuestion: {question}"

    # For complex queries, generate a plan first to guide SQL generation
    plan_hint = ""
    if not last_soql and _is_complex_query(question):
        logger.info(f"Complex query detected, planning: {question[:60]}")
        plan = await _plan_complex_query(question, schema_text)
        plan_hint = _plan_to_hint(plan)

    if plan_hint:
        prompt = f"{prompt}\n{plan_hint}"

    if history:
        conv_ctx = _build_conversation_context(history)
        prompt = f"{conv_ctx}{prompt}"
    if last_soql:
        prompt = (
            "If the user is refining a previous query, modify the PREVIOUS SQL below "
            "instead of writing a new one. Only modify it - don't rewrite from scratch "
            "unless the topic changed completely.\n"
            f"PREVIOUS SQL: {last_soql}\n\n"
            + prompt
        )

    # Generate SQL with temperature=0 for deterministic output
    q = await _call_ai(SOQL_PROMPT, prompt, 500, temperature=0)
    if not q:
        return None, None, None
    q = q.strip().replace("```soql", "").replace("```sql", "").replace("```", "").strip()
    if q in ("NO_SOQL", "NO_SQL") or not q.upper().startswith("SELECT"):
        return None, None, None

    logger.info(f"SQL (raw): {q[:200]}")

    # Auto-fix common AI mistakes BEFORE validation
    q, auto_fixes = _auto_fix_sql(q)
    if auto_fixes:
        logger.info(f"SQL (auto-fixed): {q[:200]}")

    # Pre-validate fields
    validation_error = _validate_soql_fields(q)
    if validation_error:
        logger.warning(f"SQL validation: {validation_error}")
        obj_hint = _extract_object_fields_hint(q, schema_text)
        fix = await _call_ai(SOQL_PROMPT,
            f"Validation error: {validation_error}\nQuery: {q}\n{obj_hint}\n{learning}\nRewrite using ONLY valid fields listed above. Remember to double-quote all table and column names.",
            500, temperature=0)
        if fix:
            fix = fix.strip().replace("```soql", "").replace("```sql", "").replace("```", "").strip()
            if fix.upper().startswith("SELECT"):
                logger.info(f"SQL fixed (validation): {fix[:200]}")
                q = fix

    # Pre-validate picklist values
    picklist_warning = await _validate_picklist_values(q)
    if picklist_warning:
        obj_hint = _extract_object_fields_hint(q, schema_text)
        fix = await _call_ai(SOQL_PROMPT,
            f"Picklist value errors: {picklist_warning}\nQuery: {q}\n{picklist_prompt}\n{obj_hint}\nFix the picklist values to use EXACT values from the ACTUAL PICKLIST VALUES list.",
            500, temperature=0)
        if fix:
            fix = fix.strip().replace("```soql", "").replace("```sql", "").replace("```", "").strip()
            if fix.upper().startswith("SELECT"):
                fix, _ = _auto_fix_sql(fix)
                logger.info(f"SQL fixed (picklist): {fix[:200]}")
                q = fix

    result = await execute_query(q)

    # Retry 1: Fix based on PostgreSQL error message
    if "error" in result:
        obj_hint = _extract_object_fields_hint(q, schema_text)
        fix = await _call_ai(SOQL_PROMPT,
            f"SQL FAILED with PostgreSQL error:\n{result['error'][:400]}\n\nFailed query:\n{q}\n\n{obj_hint}\n\n{learning}\n\nWrite a CORRECTED PostgreSQL query. Double-quote all table/column names. Use ONLY fields from AVAILABLE FIELDS above.",
            500, temperature=0)
        if fix:
            fix = fix.strip().replace("```soql", "").replace("```sql", "").replace("```", "").strip()
            if fix.upper().startswith("SELECT"):
                fix, _ = _auto_fix_sql(fix)
                logger.info(f"SQL retry 1: {fix[:200]}")
                q = fix
                result = await execute_query(q)

    # Retry 2: Completely different approach if still failing
    if "error" in result:
        fix2 = await _call_ai(SOQL_PROMPT,
            f"Two queries failed. Try a COMPLETELY DIFFERENT approach.\nQuestion: {question}\nLast error: {result['error'][:300]}\n\nSchema:\n{schema_text[:8000]}\n{learning}\n\nWrite a simpler PostgreSQL query. Double-quote all identifiers. Try a different table if needed.",
            500, temperature=0)
        if fix2:
            fix2 = fix2.strip().replace("```soql", "").replace("```sql", "").replace("```", "").strip()
            if fix2.upper().startswith("SELECT"):
                fix2, _ = _auto_fix_sql(fix2)
                logger.info(f"SQL retry 2 (different approach): {fix2[:200]}")
                q = fix2
                result = await execute_query(q)

        if "error" in result:
            return q, result, None

    recs = result.get("records", [])
    for r in recs:
        r.pop("attributes", None)

    # Answer validation: check SQL logic matches the user's question
    if recs and not last_soql:
        mismatch = await _validate_answer_logic(question, q)
        if mismatch:
            logger.warning(f"Answer validation MISMATCH: {mismatch}")
            obj_hint = _extract_object_fields_hint(q, schema_text)
            fix = await _call_ai(SOQL_PROMPT,
                f"ANSWER VALIDATION FAILED.\n"
                f"User asked: {question}\n"
                f"Generated SQL: {q}\n"
                f"Problem: {mismatch}\n\n"
                f"{obj_hint}\n\n{learning}\n\n"
                f"Write a CORRECTED query that accurately answers the user's question. "
                f"Pay close attention to: negation (NOT/!=), correct table, correct filters, "
                f"correct GROUP BY, and correct date range.",
                500, temperature=0)
            if fix:
                fix = fix.strip().replace("```soql", "").replace("```sql", "").replace("```", "").strip()
                if fix.upper().startswith("SELECT"):
                    fix, _ = _auto_fix_sql(fix)
                    logger.info(f"SQL fixed (answer validation): {fix[:200]}")
                    fix_result = await execute_query(fix)
                    if "error" not in fix_result and fix_result.get("records"):
                        q = fix
                        result = fix_result
                        recs = result.get("records", [])
                        for r in recs:
                            r.pop("attributes", None)
                        logger.info(f"Answer validation fix succeeded: {len(recs)} records")

    # Retry 3: Empty result recovery — query succeeded but returned 0 rows
    # Check if the question implies data should exist and try to fix filters
    if not recs and "error" not in result and not last_soql:
        expects_data = any(w in question.lower() for w in [
            "list", "show", "get", "all", "details", "students under",
            "performance of", "submissions for", "interviews for",
        ])
        if expects_data:
            logger.info(f"Empty result recovery: trying to broaden query")
            # Check for exact-match WHERE clauses that could use ILIKE instead
            has_exact = re.search(r'"(\w+)"\s*=\s*\'([^\']+)\'', q)
            picklist_fields = {'Student_Marketing_Status__c', 'Marketing_Visa_Status__c',
                              'Final_Status__c', 'Submission_Status__c', 'Type__c'}
            if has_exact and has_exact.group(1) not in picklist_fields:
                broadened = re.sub(
                    r'"(\w+)"\s*=\s*\'([^\']+)\'',
                    lambda m: f'"{m.group(1)}" ILIKE \'%{m.group(2)}%\''
                        if m.group(1) not in picklist_fields else m.group(0),
                    q
                )
                if broadened != q:
                    logger.info(f"Empty result recovery (ILIKE): {broadened[:200]}")
                    result2 = await execute_query(broadened)
                    if "error" not in result2 and result2.get("records"):
                        q = broadened
                        result = result2
                        recs = result.get("records", [])
                        for r in recs:
                            r.pop("attributes", None)

            # If still empty, ask AI to fix with context about expected data
            if not recs:
                obj_hint = _extract_object_fields_hint(q, schema_text)
                fix = await _call_ai(SOQL_PROMPT,
                    f"This query returned 0 rows but the user expects results:\n{q}\n\n"
                    f"Question: {question}\n\n{obj_hint}\n\n{learning}\n\n"
                    "The query likely has a filter that's too strict or uses wrong values. "
                    "Common issues: wrong field name for filtering, exact match instead of ILIKE, "
                    "wrong picklist value, missing JOIN. "
                    "Write a corrected query that would return the expected data.",
                    500, temperature=0)
                if fix:
                    fix = fix.strip().replace("```soql", "").replace("```sql", "").replace("```", "").strip()
                    if fix.upper().startswith("SELECT"):
                        fix, _ = _auto_fix_sql(fix)
                        logger.info(f"Empty result recovery (AI fix): {fix[:200]}")
                        result3 = await execute_query(fix)
                        if "error" not in result3 and result3.get("records"):
                            q = fix
                            result = result3
                            recs = result.get("records", [])
                            for r in recs:
                                r.pop("attributes", None)

    # If LIMIT was hit, get true total count via COUNT(*) query
    total_size = result.get("totalSize", len(recs))
    limit_m = re.search(r'LIMIT\s+(\d+)', q, re.IGNORECASE)
    if limit_m and total_size >= int(limit_m.group(1)):
        from_m = re.search(r'FROM\s+("[\w]+"|\w+)', q, re.IGNORECASE)
        where_m = re.search(r'(WHERE\s+.+?)(?:\s+ORDER|\s+GROUP|\s+LIMIT|\s*$)', q, re.IGNORECASE | re.DOTALL)
        if from_m:
            table_name = from_m.group(1)
            count_q = f"SELECT COUNT(*) FROM {table_name}"
            if where_m:
                count_q += f" {where_m.group(1)}"
            try:
                count_result = await execute_query(count_q)
                logger.info(f"Count query: {count_q[:150]}")
                if "error" not in count_result:
                    recs_count = count_result.get("records", [])
                    if recs_count and len(recs_count) == 1:
                        first_val = next(iter(recs_count[0].values()), None)
                        if isinstance(first_val, int):
                            true_total = first_val
                        else:
                            true_total = count_result.get("totalSize", total_size)
                    else:
                        true_total = count_result.get("totalSize", total_size)
                    result["totalSize"] = true_total
                    result["_limited"] = True
                    logger.info(f"True count: {true_total} (LIMIT returned {total_size})")
            except Exception as e:
                logger.warning(f"Count query failed: {e}")

    if recs and not last_soql:
        _cache_set(question, q, result, recs)
        try:
            cache_query(question, q)
        except Exception as e:
            logger.debug(f"Smart cache save skipped: {e}")

    return q, result, recs


def _rag_path(question):
    if not is_indexed():
        return None
    logger.info(f"RAG search: {question[:60]}")
    results = rag_search(question, top_k=15)
    return results if results else None


async def _route(question):
    r = await _call_ai(ROUTER_PROMPT, question, 10, temperature=0)
    if r:
        r = r.strip().upper()
        if "BOTH" in r:
            return "BOTH"
        if "RAG" in r:
            return "RAG"
    return "SQL"


def _verify_answer_counts(answer, soql_result, soql_recs, question):
    """Post-verify: if AI answer has a count number, check it matches actual DB result."""
    if not answer or not soql_recs:
        return answer
    try:
        total = soql_result.get("totalSize", len(soql_recs))

        # For count queries (single row, single numeric column)
        if len(soql_recs) == 1 and len(soql_recs[0]) <= 2:
            first_val = None
            for k, v in soql_recs[0].items():
                if k == "attributes":
                    continue
                if isinstance(v, (int, float)) and v is not None:
                    first_val = int(v)
                    break
            if first_val is not None:
                nums_in_answer = re.findall(r'[\d,]+', answer)
                nums_in_answer = [int(n.replace(",", "")) for n in nums_in_answer if n.replace(",", "").isdigit() and int(n.replace(",", "")) > 0]
                if nums_in_answer and nums_in_answer[0] != first_val:
                    wrong_num = nums_in_answer[0]
                    correct = f"{first_val:,}"
                    answer = answer.replace(str(wrong_num), correct, 1)
                    answer = answer.replace(f"{wrong_num:,}", correct, 1)
                    logger.info(f"Answer count corrected: {wrong_num} -> {first_val}")

        # For GROUP BY queries, verify breakdown totals add up
        if len(soql_recs) > 1 and all("cnt" in r or "count" in r for r in soql_recs[:3]):
            db_total = sum(int(r.get("cnt") or r.get("count", 0)) for r in soql_recs)
            if db_total > 0:
                nums_in_answer = re.findall(r'\*\*(\d[\d,]*)\*\*', answer)
                if nums_in_answer:
                    for num_str in nums_in_answer:
                        num_val = int(num_str.replace(",", ""))
                        if num_val != db_total and abs(num_val - db_total) > 2:
                            if num_val == len(soql_recs):
                                answer = answer.replace(f"**{num_str}**", f"**{db_total:,}**", 1)
                                logger.info(f"Breakdown total corrected: {num_val} -> {db_total}")
                                break

        # For list queries where LIMIT was hit, ensure answer uses true total
        if soql_result.get("_limited") and total > len(soql_recs):
            shown = len(soql_recs)
            shown_str = f"{shown:,}"
            total_str = f"{total:,}"
            if f"**{shown_str}" in answer and shown_str != total_str:
                answer = answer.replace(f"**{shown_str}", f"**{total_str}", 1)
                logger.info(f"Answer total corrected: {shown} -> {total}")

        # Verify no fabricated names: check that names in the answer appear in the data
        if len(soql_recs) <= 30:
            data_names = set()
            for r in soql_recs:
                for k, v in r.items():
                    if isinstance(v, str) and k in ("Name", "Student_Name__c", "Student_Name", "BU_Name__c", "BU_Name", "BU_Manager"):
                        data_names.add(v.strip())
            if data_names and len(data_names) <= 50:
                name_pattern = re.compile(r'\|\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*\|')
                for m in name_pattern.finditer(answer):
                    found_name = m.group(1).strip()
                    if found_name not in data_names and found_name != "Total":
                        close = [n for n in data_names if n.lower() == found_name.lower()]
                        if close:
                            answer = answer.replace(found_name, close[0])
                            logger.info(f"Name case corrected: '{found_name}' -> '{close[0]}'")

    except Exception as e:
        logger.warning(f"Answer verification failed: {e}")
    return answer


def _build_conversation_context(conversation_history):
    if not conversation_history:
        return ""
    pairs = []
    for msg in conversation_history[-8:]:
        role = msg.get("role", "")
        content = msg.get("content", "")
        if role == "user":
            pairs.append(f"User asked: {content[:250]}")
        elif role == "assistant":
            lines = content.split("\n")
            summary = "\n".join(lines[:8]) if len(lines) > 8 else content
            pairs.append(f"You answered: {summary[:400]}")
    if not pairs:
        return ""
    return "CONVERSATION HISTORY (use for context, not as data source):\n" + "\n".join(pairs) + "\n\n"


def _is_whatsapp_report(question):
    """Detect if the user wants a WhatsApp-style formatted report."""
    q = question.lower()
    triggers = [
        "send weekly report", "weekly report for", "weekly report of",
        "weekly performance report", "send report for", "whatsapp report",
        "send performance report",
    ]
    return any(t in q for t in triggers)


# ── Domain Question Handler ────────────────────────────────────────

_DOMAIN_QA = {
    r'\bwhat\s+(?:is|are)\s+(?:a\s+)?bu\b': "**BU (Business Unit)** is a team of students managed by a BU Manager. Each BU Manager oversees marketing, submissions, and placements for their team of students.",
    r'\bwhat\s+(?:is|does)\s+in\s+market\s+mean': "**In Market** means a student is actively being marketed to clients for job placement. Their resume is being submitted and they're available for interviews.",
    r'\bwhat\s+(?:is|does)\s+(?:vc|verbal\s+confirmation)\s+mean': "**Verbal Confirmation (VC)** means a client has verbally confirmed they want to hire the student. It's the stage before the official project start.",
    r'\bwhat\s+(?:is|does)\s+project\s+started\s+mean': "**Project Started** means the student has been successfully placed and started working on a client project. This is a successful placement.",
    r'\bwhat\s+(?:is|does)\s+pre\s*marketing\s+mean': "**Pre Marketing** means the student is still in training and not yet ready to be marketed to clients.",
    r'\bwhat\s+(?:is|are)\s+submissions?\b': "**Submissions** are resumes sent to clients for job opportunities. Each submission represents a student's resume being forwarded to a potential employer.",
    r'\bwhat\s+(?:is|does)\s+days?\s+in\s+market\s+mean': "**Days in Market** tracks how long a student has been actively marketed. Lower is better — it means faster placement.",
    r'\bhow\s+(?:does|do)\s+(?:the\s+)?placement\s+(?:process\s+)?work': "The **placement funnel** works like this:\n\n1. **Pre Marketing** → Student trains\n2. **In Market** → Student is actively marketed\n3. **Submissions** → Resumes sent to clients\n4. **Interviews** → Client interviews the student\n5. **Verbal Confirmation** → Client says yes\n6. **Project Started** → Student starts working\n\nKey metrics: submission rate, interview-to-confirmation ratio, and average days to placement.",
    r'\bwhat\s+(?:is|does)\s+exit\s+mean': "**Exit** means the student has left the program — either voluntarily or was removed. They are no longer being marketed.",
    r'\bwhat\s+(?:statuses|status)\s+(?:are\s+there|exist|do\s+you\s+have)': "The main **student statuses** are:\n\n- **Pre Marketing** — In training\n- **In Market** — Actively being marketed\n- **Verbal Confirmation** — Client confirmed hire\n- **Project Started** — Successfully placed\n- **Exit** — Left the program",
}
_DOMAIN_QA_COMPILED = [(re.compile(k, re.I), v) for k, v in _DOMAIN_QA.items()]


def _handle_domain_question(question):
    for pattern, answer in _DOMAIN_QA_COMPILED:
        if pattern.search(question):
            return answer
    return None


# ── WhatsApp Report Detection ────────────────────────────────────

_REPORT_PATTERNS = [
    (re.compile(r'(?:generate|create|make|give|send|prepare|get)\s+(?:the\s+)?(?:whatsapp|wa|watsapp)?\s*(?:premarketing|pre\s*marketing)\s+(?:report)?\s*(?:bu|business\s*unit)', re.I), "premarketing_bu"),
    (re.compile(r'(?:premarketing|pre\s*marketing)\s+(?:report\s+)?(?:bu|business\s*unit)\s*(?:wise|report)?', re.I), "premarketing_bu"),

    (re.compile(r'(?:yesterday|yday)\s+(?:submission|sub)\s+(?:report\s+)?(?:bu|business\s*unit)', re.I), "yesterday_submissions_bu"),
    (re.compile(r'(?:yesterday|yday)\s+(?:submission|sub)\s+(?:report\s+)?(?:offshore|off\s*shore)\s*(?:manager)?', re.I), "yesterday_submissions_offshore"),

    (re.compile(r'(?:last\s+)?3\s*(?:days?)?\s+no\s+(?:submission|sub).*(?:bu|business\s*unit)', re.I), "no_submissions_3days_bu"),
    (re.compile(r'(?:no\s+submission|without\s+submission).*(?:3\s*day|last\s*3).*(?:bu|business\s*unit)', re.I), "no_submissions_3days_bu"),
    (re.compile(r'(?:last\s+)?3\s*(?:days?)?\s+no\s+(?:submission|sub).*(?:offshore|off\s*shore)', re.I), "no_submissions_3days_offshore"),
    (re.compile(r'(?:no\s+submission|without\s+submission).*(?:3\s*day|last\s*3).*(?:offshore|off\s*shore)', re.I), "no_submissions_3days_offshore"),

    (re.compile(r'(?:interview)\s+(?:mandatory|missing|empty)\s+(?:field|data).*(?:bu|business\s*unit)', re.I), "interview_mandatory_fields_bu"),

    (re.compile(r'(?:last\s+)?2\s*(?:weeks?)?\s+no\s+(?:interview).*(?:bu|business\s*unit)', re.I), "no_interviews_2weeks_bu"),
    (re.compile(r'(?:no\s+interview|without\s+interview).*(?:2\s*week|last\s*2).*(?:bu|business\s*unit)', re.I), "no_interviews_2weeks_bu"),
    (re.compile(r'(?:last\s+)?2\s*(?:weeks?)?\s+no\s+(?:interview).*(?:offshore|off\s*shore)', re.I), "no_interviews_2weeks_offshore"),
    (re.compile(r'(?:no\s+interview|without\s+interview).*(?:2\s*week|last\s*2).*(?:offshore|off\s*shore)', re.I), "no_interviews_2weeks_offshore"),

    (re.compile(r'(?:last\s+week)\s+(?:submission|sub|performance).*(?:interview).*(?:bu|business\s*unit)', re.I), "last_week_performance_bu"),
    (re.compile(r'(?:last\s+week)\s+(?:report|performance).*(?:bu|business\s*unit)', re.I), "last_week_performance_bu"),
    (re.compile(r'(?:last\s+week)\s+(?:submission|sub|performance).*(?:interview).*(?:offshore|off\s*shore)', re.I), "last_week_performance_offshore"),
    (re.compile(r'(?:last\s+week)\s+(?:report|performance).*(?:offshore|off\s*shore)', re.I), "last_week_performance_offshore"),

    (re.compile(r'(?:recruiter)\s+(?:last\s+week|weekly)\s+(?:performance|report).*(?:bu|business\s*unit)', re.I), "recruiter_performance_bu"),
    (re.compile(r'(?:recruiter)\s+(?:last\s+week|weekly)\s+(?:performance|report).*(?:offshore|off\s*shore)', re.I), "recruiter_performance_offshore"),
]

_REPORT_GENERIC = re.compile(
    r'(?:generate|create|make|prepare|get|give)\s+(?:the\s+)?(?:whatsapp|wa|watsapp)\s+'
    r'(?:report|message|msg)', re.I
)


async def _handle_report_request(question):
    from app.whatsapp_reports import REPORT_REGISTRY
    q = question.lower()

    for pattern, report_type in _REPORT_PATTERNS:
        if pattern.search(q):
            entry = REPORT_REGISTRY.get(report_type)
            if entry:
                logger.info(f"[ROUTE=WA_REPORT] Detected report: {report_type}")
                return report_type, entry["label"]

    if _REPORT_GENERIC.search(q):
        report_list = "\n".join(
            f"- **{v['label']}** → _\"{k}\"_"
            for k, v in REPORT_REGISTRY.items()
        )
        return None, (
            "I can generate these WhatsApp reports for you:\n\n"
            f"{report_list}\n\n"
            "Just ask for any specific one, e.g. _\"generate premarketing report BU wise\"_ "
            "or _\"last 3 days no submissions BU wise report\"_.\n\n"
            "You can also download them as Excel from the **WA Reports** tab in the sidebar."
        )

    return None, None


# ── Main Answer Functions ────────────────────────────────────────

async def answer_question(question, conversation_history=None, username=None, last_soql=None):
    # Layer 1: Normalize question (expand synonyms, fix slang/typos)
    start_time = time.time()
    original_question = question
    question = _normalize_question(question)

    # Layer 1a: Resolve follow-up questions using conversation context
    question = await _resolve_followup(question, conversation_history)

    # Layer 1a2: Domain knowledge questions (no DB needed)
    domain_answer = _handle_domain_question(question)
    if domain_answer:
        logger.info(f"[ROUTE=DOMAIN] Q='{question[:60]}'")
        return {"answer": domain_answer, "soql": None, "data": None, "route": "DOMAIN",
                "suggestions": ["How many students are in market?", "Show submissions this week BU wise", "List interviews this month"]}

    # Layer 1a3: WhatsApp report generation
    report_type, report_result = await _handle_report_request(question)
    if report_type:
        from app.whatsapp_reports import REPORT_REGISTRY
        entry = REPORT_REGISTRY[report_type]
        try:
            xlsx_bytes = await entry["handler"]()
            import base64
            b64 = base64.b64encode(xlsx_bytes).decode()
            return {
                "answer": f"**{entry['label']}** report generated successfully!\n\n"
                          f"The Excel file is ready for download with WhatsApp-formatted messages.\n\n"
                          f"You can also generate this anytime from the **WA Reports** tab in the sidebar.",
                "soql": None, "data": None, "route": "WA_REPORT",
                "file_download": {"filename": f"{report_type}.xlsx", "data": b64, "mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
                "suggestions": ["Generate premarketing report BU wise", "Last 3 days no submissions BU wise", "Last 2 weeks no interviews report"],
            }
        except Exception as e:
            logger.error(f"Report generation failed: {e}")
            return {"answer": f"Report generation failed: {str(e)}\n\nYou can try downloading it from the **WA Reports** tab.",
                    "soql": None, "data": None, "route": "ERROR"}
    elif report_result:
        return {"answer": report_result, "soql": None, "data": None, "route": "WA_REPORT",
                "suggestions": ["Generate premarketing report BU wise", "Last 3 days no submissions BU wise", "Last 2 weeks no interviews report"]}

    # Layer 1b: Detect unanswerable questions first (why/predict/should)
    unanswerable_msg = _detect_unanswerable(question)
    if unanswerable_msg:
        logger.info(f"Unanswerable question detected: {question[:60]}")
        return {"answer": unanswerable_msg, "soql": None, "data": None, "route": "CLARIFY",
                "suggestions": ["How many students are in market?", "Show submissions this week BU wise", "List interviews this month"]}

    # Layer 1c: Detect vague/ambiguous questions → ask for clarification
    vague_msg = _detect_vague_question(question)
    if vague_msg:
        logger.info(f"Vague question detected: {question[:60]}")
        return {"answer": vague_msg, "soql": None, "data": None, "route": "CLARIFY",
                "suggestions": ["How many students are in market?", "Show submissions this week BU wise", "List interviews this month"]}

    schema_text = schema_to_prompt()
    if not schema_text or "No schema" in schema_text:
        return {"answer": "Schema not loaded. Run: python -m scripts.refresh_schema", "soql": None, "data": None}

    # Fast path 1: Semantic layer (always runs — no AI, guaranteed correct)
    semantic = await handle_semantic_query(question)
    if semantic:
        elapsed = round(time.time() - start_time, 2)
        logger.info(f"[ROUTE=SEMANTIC] Q='{question[:60]}' rows={len(semantic.get('data', {}).get('records', []))} time={elapsed}s")
        await save_interaction(question, semantic["soql"], semantic["answer"], "SQL", username=username)
        suggestions = await _generate_suggestions(question, semantic["answer"])
        return {
            "answer": semantic["answer"],
            "soql": semantic["soql"],
            "route": "SQL",
            "rag_used": False,
            "suggestions": suggestions,
            "data": semantic["data"],
        }

    # Fast path 2: Direct report handler (BU-wise aggregate reports)
    direct = await _handle_direct_report(question)
    if direct:
        elapsed = round(time.time() - start_time, 2)
        logger.info(f"[ROUTE=DIRECT_REPORT] Q='{question[:60]}' time={elapsed}s")
        await save_interaction(question, direct["soql"], direct["answer"], "SQL", username=username)
        suggestions = await _generate_suggestions(question, direct["answer"])
        return {
            "answer": direct["answer"],
            "soql": direct["soql"],
            "route": "SQL",
            "rag_used": False,
            "suggestions": suggestions,
            "data": direct["data"],
        }

    route = await _route(question)
    logger.info(f"Route: {route} | Q: {question[:60]}")

    soql_query, soql_result, soql_recs = None, None, None
    rag_results = None

    if route in ("SQL", "BOTH"):
        soql_query, soql_result, soql_recs = await _soql_path(question, schema_text, conversation_history, last_soql=last_soql)

        if soql_recs is None or (soql_recs is not None and len(soql_recs) == 0):
            name_words = [w for w in question.split() if len(w) > 2 and w[0].isupper()]
            if len(name_words) >= 2:
                last_word = _sanitize_sql_input(name_words[-1])
                fallback_q = f"""SELECT s."Name", s."Student_Marketing_Status__c", s."Technology__c", m."Name" AS "BU_Name", s."Phone__c", s."Marketing_Email__c", s."Marketing_Visa_Status__c", s."Days_in_Market_Business__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Name" ILIKE '%{last_word}%' LIMIT 50"""
                try:
                    fb_result = await execute_query(fallback_q)
                    if "error" not in fb_result and fb_result.get("records"):
                        recs = fb_result["records"]
                        for r in recs:
                            r.pop("attributes", None)
                        soql_query = fallback_q
                        soql_result = fb_result
                        soql_recs = recs
                        logger.info(f"Name fallback found {len(recs)} records for '{last_word}'")
                except Exception as e:
                    logger.warning(f"Name fallback failed: {e}")

    if route in ("RAG", "BOTH"):
        rag_results = _rag_path(question)
        if not rag_results and route == "RAG":
            detect = await _call_ai(ROUTER_PROMPT,
                f"Which object for '{question}'?\n{schema_text[:3000]}\nReturn ONLY API name.", 50)
            if detect:
                obj = detect.strip().split("\n")[0].strip()
                schema = get_schema()
                if obj not in schema:
                    for n in schema:
                        if obj.lower() in n.lower():
                            obj = n
                            break
                if obj in schema:
                    fields = [f["name"] for f in schema[obj].get("fields", [])
                             if f["type"] not in ("boolean",) and not f["name"].startswith("UserPref")][:25]
                    try:
                        r = await execute_query(f"SELECT {','.join(fields)} FROM {obj} ORDER BY LastModifiedDate DESC LIMIT 100")
                        if "error" not in r:
                            rag_results = [{"text": json.dumps({k: v for k, v in rec.items() if k != "attributes"}, default=str)[:500], "score": 1.0} for rec in r.get("records", [])[:30]]
                    except Exception:
                        pass

    # Build context for the answer AI
    parts = []
    if soql_recs is not None:
        total = soql_result.get("totalSize", len(soql_recs))
        is_limited = soql_result.get("_limited", False)
        shown = len(soql_recs)

        # Check if multi-query results (records have _query_label)
        labels = set(r.get("_query_label") for r in soql_recs if "_query_label" in r)
        if labels:
            parts.append(f"COMBINED QUERY RESULTS ({total} total records from {len(labels)} queries):\nSQL used:\n{soql_query}")

            # Separate summary records from detail records
            summary_labels = {l for l in labels if l.startswith("_summary_")}
            detail_labels = labels - summary_labels

            # Build BU summary from aggregate queries if available
            bu_summary = {}
            if summary_labels:
                for slabel in sorted(summary_labels):
                    display_label = slabel.replace("_summary_", "")
                    group = [r for r in soql_recs if r.get("_query_label") == slabel]
                    for rec in group:
                        bu = rec.get("BU_Name", "Unknown")
                        if bu not in bu_summary:
                            bu_summary[bu] = {}
                        bu_summary[bu][display_label] = rec.get("cnt", 0)
                        if "conf_cnt" in rec:
                            bu_summary[bu][display_label + "_confirmations"] = rec.get("conf_cnt", 0)
                        if "total_amount" in rec:
                            bu_summary[bu][display_label + "_amount"] = float(rec.get("total_amount", 0) or 0)
            else:
                # Fallback: count from raw records (less accurate if LIMIT hit)
                for label in sorted(detail_labels):
                    group = [r for r in soql_recs if r.get("_query_label") == label]
                    clean = [{k: v for k, v in r.items() if k != "_query_label"} for r in group]
                    bu_field = None
                    for f in ["BU_Name__c", "BU_Name", "BU_Manager"]:
                        if clean and f in clean[0]:
                            bu_field = f
                            break
                    if bu_field:
                        for rec in clean:
                            bu = rec.get(bu_field, "Unknown")
                            if bu not in bu_summary:
                                bu_summary[bu] = {}
                            bu_summary[bu][label] = bu_summary[bu].get(label, 0) + 1
                            for amt_field in ["Amount__c", "total_amount"]:
                                if amt_field in rec and rec[amt_field]:
                                    amt_key = f"{label}_amount"
                                    bu_summary[bu][amt_key] = bu_summary[bu].get(amt_key, 0) + float(rec[amt_field] or 0)

            # Append detail record samples (not summaries)
            for label in sorted(detail_labels):
                group = [r for r in soql_recs if r.get("_query_label") == label]
                clean = [{k: v for k, v in r.items() if k != "_query_label"} for r in group]
                parts.append(f"\n--- {label} ({len(group)} records) ---")
                parts.append(json.dumps(clean[:100], indent=2, default=str)[:20000])

            # Add pre-computed summary table
            if bu_summary:
                # Determine columns for the summary table
                has_confirmations = any(k.endswith("_confirmations") for bu_data in bu_summary.values() for k in bu_data)
                has_amount = any(k.endswith("_amount") for bu_data in bu_summary.values() for k in bu_data)
                display_cols = sorted(detail_labels) if detail_labels else sorted({l.replace("_summary_", "") for l in summary_labels})

                parts.append("\n\nPRE-COMPUTED BU SUMMARY (use these EXACT numbers in your answer):")
                header = "| BU Name | " + " | ".join(display_cols)
                if has_confirmations:
                    header += " | Confirmations"
                if has_amount:
                    header += " | Interview Amount"
                parts.append(header + " |")
                sep = "|---|" + "|".join(["---"] * len(display_cols))
                if has_confirmations:
                    sep += "|---"
                if has_amount:
                    sep += "|---"
                parts.append(sep + "|")

                totals = {l: 0 for l in display_cols}
                total_conf = 0
                total_amt = 0.0
                for bu in sorted(bu_summary.keys()):
                    row = f"| {bu}"
                    for l in display_cols:
                        cnt = bu_summary[bu].get(l, 0)
                        totals[l] += cnt
                        row += f" | {cnt}"
                    if has_confirmations:
                        conf = sum(v for k, v in bu_summary[bu].items() if k.endswith("_confirmations"))
                        total_conf += conf
                        row += f" | {conf}"
                    if has_amount:
                        amt = sum(v for k, v in bu_summary[bu].items() if k.endswith("_amount"))
                        total_amt += amt
                        row += f" | {amt:,.2f}"
                    parts.append(row + " |")
                total_row = "| **Total**"
                for l in display_cols:
                    total_row += f" | **{totals[l]}**"
                if has_confirmations:
                    total_row += f" | **{total_conf}**"
                if has_amount:
                    total_row += f" | **{total_amt:,.2f}**"
                parts.append(total_row + " |")
        else:
            if is_limited:
                parts.append(f"QUERY RESULTS: **{total} TOTAL records** in database (showing {shown} below, but the TRUE TOTAL is {total}).\nIMPORTANT: Use {total} as the total count, NOT {shown}.\nSQL used: {soql_query}")
            else:
                parts.append(f"QUERY RESULTS ({total} total records from database):\nSQL used: {soql_query}")
            parts.append(json.dumps(soql_recs[:200], indent=2, default=str)[:50000])

    if rag_results:
        parts.append(f"\nSEMANTIC SEARCH RESULTS ({len(rag_results)} similar records):")
        for r in rag_results[:15]:
            parts.append(f"  [{r.get('sf_object', '')}] (sim: {r.get('score', 0):.2f}) {r['text'][:400]}")

    if not parts:
        error_detail = ""
        if soql_result and "error" in soql_result:
            error_detail = f"\n\n**Query error:** {soql_result['error'][:200]}"
        elif soql_query:
            error_detail = "\n\nThe query ran but returned 0 records."
        no_data_msg = (
            "I couldn't find data for that question."
            f"{error_detail}"
            "\n\n**Try:**"
            "\n- Check the spelling of names or statuses"
            "\n- Use broader terms (e.g. 'students' instead of a specific name)"
            "\n- Ask about a specific object: Students, Submissions, Interviews, Jobs, Employees"
        )
        await save_interaction(question, soql_query, no_data_msg, route, username=username)
        return {"answer": no_data_msg, "soql": soql_query, "data": None, "suggestions": ["How many students are in market?", "Show today's submissions by BU", "List all BU managers"]}

    # Pre-compute data summary for accurate counts
    if soql_recs:
        tt = soql_result.get("totalSize", len(soql_recs)) if soql_result else None
        data_summary = await _build_data_summary(soql_recs, true_total=tt, soql_query=soql_query)
        if data_summary:
            parts.append(data_summary)

    # Change 4: Template answers for simple COUNT/GROUP queries — skip AI entirely
    q_lower = question.lower()
    is_group_question = any(w in q_lower for w in ["bu wise", "by bu", "tech wise", "by technology",
                                                     "visa wise", "by visa", "status wise", "by status",
                                                     "manager wise", "by manager"])
    if is_group_question and soql_recs and len(soql_recs) > 1 and all("cnt" in r for r in soql_recs):
        group_key = next((k for k in soql_recs[0] if k != "cnt"), None)
        if group_key:
            total = sum(r.get("cnt", 0) for r in soql_recs)
            lines = [f"**{total:,} total** across **{len(soql_recs)} groups**\n"]
            lines.append(f"| {group_key} | Count |")
            lines.append("|---|---:|")
            for r in soql_recs:
                lines.append(f"| {r.get(group_key, 'N/A')} | {r.get('cnt', 0):,} |")
            lines.append(f"| **Total** | **{total:,}** |")
            template_answer = "\n".join(lines)
            elapsed = round(time.time() - start_time, 2)
            logger.info(f"[ROUTE=GROUP_TEMPLATE] Q='{question[:60]}' groups={len(soql_recs)} total={total} time={elapsed}s")
            await save_interaction(question, soql_query, template_answer, route, username=username)
            suggestions = await _generate_suggestions(question, template_answer)
            return {
                "answer": template_answer, "soql": soql_query, "route": route,
                "rag_used": False, "suggestions": suggestions,
                "data": {"totalSize": total, "records": soql_recs[:200], "query": soql_query, "route": route},
            }

    is_count_question = any(w in q_lower for w in ["how many", "how much", "total", "count of"])
    if is_count_question and soql_recs and len(soql_recs) == 1:
        rec = soql_recs[0]
        count_val = rec.get("cnt") or rec.get("count") or rec.get("total")
        if count_val is not None:
            count_val = int(count_val)
            entity = "records"
            for word in ["student", "submission", "interview", "job", "employee", "manager", "placement"]:
                if word in q_lower:
                    entity = word + "s"
                    break
            template_answer = f"**{count_val:,} {entity}** match your query."
            elapsed = round(time.time() - start_time, 2)
            logger.info(f"[ROUTE=COUNT_TEMPLATE] Q='{question[:60]}' count={count_val} time={elapsed}s")
            await save_interaction(question, soql_query, template_answer, route, username=username)
            suggestions = await _generate_suggestions(question, template_answer)
            return {
                "answer": template_answer, "soql": soql_query, "route": route,
                "rag_used": False, "suggestions": suggestions,
                "data": {"totalSize": count_val, "records": soql_recs[:200], "query": soql_query, "route": route},
            }

    # Detect if this is a WhatsApp-style report request
    use_whatsapp = _is_whatsapp_report(question)
    if use_whatsapp:
        system = WEEKLY_REPORT_PROMPT
    elif route in ("RAG", "BOTH"):
        system = RAG_PROMPT
    else:
        system = ANSWER_PROMPT

    conv_ctx = _build_conversation_context(conversation_history)
    prompt = f"{conv_ctx}Question: {question}\n\nData:\n" + "\n".join(parts)

    answer = await _call_ai(system, prompt, max_tokens=6000)

    # Post-verify: check if count in answer matches actual DB data
    if answer and soql_recs is not None:
        answer = _verify_answer_counts(answer, soql_result, soql_recs, question)

    # Result-sanity: warn if query hit the row limit
    if soql_recs and len(soql_recs) >= 2000 and answer:
        answer += "\n\n> **Note:** Results are capped at 2,000 rows. The actual count may be higher — please refine your query for exact numbers."

    elapsed = round(time.time() - start_time, 2)
    logger.info(f"[ROUTE={route}] Q='{question[:60]}' rows={len(soql_recs) if soql_recs else 0} rag={bool(rag_results)} time={elapsed}s")
    await save_interaction(question, soql_query, answer or "", route, username=username)
    suggestions = await _generate_suggestions(question, answer or "")

    return {
        "answer": answer or "Found data but couldn't summarize.",
        "soql": soql_query,
        "route": route,
        "rag_used": rag_results is not None and len(rag_results) > 0,
        "suggestions": suggestions,
        "data": {
            "totalSize": soql_result.get("totalSize", 0) if soql_result and "error" not in soql_result else 0,
            "records": [r for r in (soql_recs or []) if not str(r.get("_query_label", "")).startswith("_summary_")][:200],
            "query": soql_query,
            "route": route,
            "rag_results": len(rag_results) if rag_results else 0,
        } if soql_recs or rag_results else None,
    }


async def answer_question_stream(question, conversation_history=None, username=None, last_soql=None):
    """
    Async generator yielding structured events for Server-Sent Events.
    """
    # Layer 1: Normalize question
    start_time = time.time()
    original_question = question
    question = _normalize_question(question)

    # Layer 1a: Resolve follow-up questions using conversation context
    question = await _resolve_followup(question, conversation_history)

    # Layer 1a2: Domain knowledge questions (no DB needed)
    domain_answer = _handle_domain_question(question)
    if domain_answer:
        logger.info(f"[ROUTE=DOMAIN] Q='{question[:60]}'")
        yield {"type": "token", "data": domain_answer}
        yield {"type": "done", "data": {"answer": domain_answer, "soql": None, "data": None, "route": "DOMAIN",
               "suggestions": ["How many students are in market?", "Show submissions this week BU wise", "List interviews this month"]}}
        return

    # Layer 1a3: WhatsApp report generation
    report_type, report_result = await _handle_report_request(question)
    if report_type:
        from app.whatsapp_reports import REPORT_REGISTRY
        entry = REPORT_REGISTRY[report_type]
        yield {"type": "thinking", "data": f"Generating {entry['label']}..."}
        try:
            xlsx_bytes = await entry["handler"]()
            import base64
            b64 = base64.b64encode(xlsx_bytes).decode()
            answer = (f"**{entry['label']}** report generated successfully!\n\n"
                      f"The Excel file is ready for download with WhatsApp-formatted messages.\n\n"
                      f"You can also generate this anytime from the **WA Reports** tab in the sidebar.")
            yield {"type": "thinking_done", "data": None}
            yield {"type": "token", "data": answer}
            yield {"type": "done", "data": {
                "answer": answer, "soql": None, "data": None, "route": "WA_REPORT",
                "file_download": {"filename": f"{report_type}.xlsx", "data": b64, "mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
                "suggestions": ["Generate premarketing report BU wise", "Last 3 days no submissions BU wise", "Last 2 weeks no interviews report"],
            }}
        except Exception as e:
            logger.error(f"Report generation failed (stream): {e}")
            msg = f"Report generation failed: {str(e)}\n\nYou can try downloading it from the **WA Reports** tab."
            yield {"type": "thinking_done", "data": None}
            yield {"type": "token", "data": msg}
            yield {"type": "done", "data": {"answer": msg, "soql": None, "data": None, "route": "ERROR"}}
        return
    elif report_result:
        yield {"type": "token", "data": report_result}
        yield {"type": "done", "data": {"answer": report_result, "soql": None, "data": None, "route": "WA_REPORT",
               "suggestions": ["Generate premarketing report BU wise", "Last 3 days no submissions BU wise", "Last 2 weeks no interviews report"]}}
        return

    # Layer 1b: Detect unanswerable questions first (why/predict/should)
    unanswerable_msg = _detect_unanswerable(question)
    if unanswerable_msg:
        logger.info(f"Unanswerable question (stream): {question[:60]}")
        yield {"type": "token", "data": unanswerable_msg}
        yield {"type": "done", "data": {"answer": unanswerable_msg, "soql": None, "data": None, "route": "CLARIFY",
               "suggestions": ["How many students are in market?", "Show submissions this week BU wise", "List interviews this month"]}}
        return

    # Layer 1c: Detect vague/ambiguous questions
    vague_msg = _detect_vague_question(question)
    if vague_msg:
        logger.info(f"Vague question (stream): {question[:60]}")
        yield {"type": "token", "data": vague_msg}
        yield {"type": "done", "data": {"answer": vague_msg, "soql": None, "data": None, "route": "CLARIFY",
               "suggestions": ["How many students are in market?", "Show submissions this week BU wise", "List interviews this month"]}}
        return

    schema_text = schema_to_prompt()
    if not schema_text or "No schema" in schema_text:
        msg = "Schema not loaded. Run: python -m scripts.refresh_schema"
        yield {"type": "token", "data": msg}
        yield {"type": "done", "data": {"answer": msg, "soql": None, "data": None}}
        return

    yield {"type": "thinking", "data": "Analyzing question"}

    # Fast path 1: Semantic layer (always runs — no AI, guaranteed correct)
    semantic = await handle_semantic_query(question)
    if semantic:
        logger.info(f"Semantic handler (stream): {question[:60]}")
        yield {"type": "route", "data": "SQL"}
        yield {"type": "thinking", "data": "Matched semantic pattern"}
        yield {"type": "soql", "data": semantic["soql"]}
        yield {"type": "data", "data": semantic["data"]}
        yield {"type": "thinking_done", "data": None}
        yield {"type": "token", "data": semantic["answer"]}
        await save_interaction(question, semantic["soql"], semantic["answer"], "SQL", username=username)
        suggestions = await _generate_suggestions(question, semantic["answer"])
        yield {"type": "done", "data": {
            "answer": semantic["answer"],
            "soql": semantic["soql"],
            "route": "SQL",
            "rag_used": False,
            "suggestions": suggestions,
            "data": semantic["data"],
        }}
        return

    # Fast path 2: Direct report handler (BU-wise aggregate reports)
    direct = await _handle_direct_report(question)
    if direct:
            logger.info(f"Direct report handler (stream): {question[:60]}")
            yield {"type": "route", "data": "SQL"}
            yield {"type": "thinking", "data": "Building BU report from database"}
            yield {"type": "soql", "data": direct["soql"]}
            yield {"type": "data", "data": direct["data"]}
            yield {"type": "thinking_done", "data": None}
            yield {"type": "token", "data": direct["answer"]}
            await save_interaction(question, direct["soql"], direct["answer"], "SQL", username=username)
            suggestions = await _generate_suggestions(question, direct["answer"])
            yield {"type": "done", "data": {
                "answer": direct["answer"],
                "soql": direct["soql"],
                "route": "SQL",
                "rag_used": False,
                "suggestions": suggestions,
                "data": direct["data"],
            }}
            return

    route = await _route(question)
    logger.info(f"Route (stream): {route} | Q: {question[:60]}")
    yield {"type": "route", "data": route}
    yield {"type": "thinking", "data": f"Route → {route}"}

    soql_query, soql_result, soql_recs = None, None, None
    rag_results = None

    if route in ("SQL", "BOTH"):
        # Check cache
        if not last_soql:
            cached = _cache_get(question)
            if cached:
                yield {"type": "thinking", "data": "Found cached result (< 5 min old)"}
                soql_query, soql_result, soql_recs = cached
                yield {"type": "soql", "data": soql_query}
            else:
                pattern_match = _match_report_pattern(question)
                if pattern_match:
                    yield {"type": "thinking", "data": f"Matched report pattern ({len(pattern_match['queries'])} queries)"}

        if soql_recs is None:
            yield {"type": "thinking", "data": "Picking database tables"}
            soql_query, soql_result, soql_recs = await _soql_path(question, schema_text, conversation_history, last_soql=last_soql)

        if soql_query:
            yield {"type": "soql", "data": soql_query}

        if soql_recs is not None and len(soql_recs) > 0:
            yield {"type": "thinking", "data": f"Fetched {len(soql_recs)} records from database"}
        elif soql_result and "error" in soql_result:
            yield {"type": "thinking", "data": "Query error — trying fallback"}

        # Fallback: if SOQL path failed and question looks like a person name search
        if soql_recs is None or (soql_recs is not None and len(soql_recs) == 0):
            name_words = [w for w in question.split() if len(w) > 2 and w[0].isupper()]
            if len(name_words) >= 2:
                yield {"type": "thinking", "data": "Searching by name"}
                last_word = _sanitize_sql_input(name_words[-1])
                fallback_q = f"""SELECT s."Name", s."Student_Marketing_Status__c", s."Technology__c", m."Name" AS "BU_Name", s."Phone__c", s."Marketing_Email__c", s."Marketing_Visa_Status__c", s."Days_in_Market_Business__c" FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE s."Name" ILIKE '%{last_word}%' LIMIT 50"""
                try:
                    fb_result = await execute_query(fallback_q)
                    if "error" not in fb_result and fb_result.get("records"):
                        recs = fb_result["records"]
                        for r in recs:
                            r.pop("attributes", None)
                        soql_query = fallback_q
                        soql_result = fb_result
                        soql_recs = recs
                        yield {"type": "soql", "data": fallback_q}
                        yield {"type": "thinking", "data": f"Name search found {len(recs)} records"}
                        logger.info(f"Name fallback found {len(recs)} records for '{last_word}'")
                except Exception as e:
                    logger.warning(f"Name fallback failed: {e}")

    if route in ("RAG", "BOTH"):
        rag_results = _rag_path(question)
        if not rag_results and route == "RAG":
            detect = await _call_ai(ROUTER_PROMPT,
                f"Which object for '{question}'?\n{schema_text[:3000]}\nReturn ONLY API name.", 50)
            if detect:
                obj = detect.strip().split("\n")[0].strip()
                schema = get_schema()
                if obj not in schema:
                    for n in schema:
                        if obj.lower() in n.lower():
                            obj = n
                            break
                if obj in schema:
                    fields = [f["name"] for f in schema[obj].get("fields", [])
                             if f["type"] not in ("boolean",) and not f["name"].startswith("UserPref")][:25]
                    try:
                        r = await execute_query(f"SELECT {','.join(fields)} FROM {obj} ORDER BY LastModifiedDate DESC LIMIT 100")
                        if "error" not in r:
                            rag_results = [{"text": json.dumps({k: v for k, v in rec.items() if k != "attributes"}, default=str)[:500], "score": 1.0} for rec in r.get("records", [])[:30]]
                    except Exception:
                        pass

    data_payload = None
    if soql_recs or rag_results:
        data_payload = {
            "totalSize": soql_result.get("totalSize", 0) if soql_result and "error" not in soql_result else 0,
            "records": [r for r in (soql_recs or []) if not str(r.get("_query_label", "")).startswith("_summary_")][:200],
            "query": soql_query,
            "route": route,
            "rag_results": len(rag_results) if rag_results else 0,
        }
        yield {"type": "data", "data": data_payload}

    # Build answer prompt
    parts = []
    if soql_recs is not None:
        total = soql_result.get("totalSize", len(soql_recs))
        is_limited = soql_result.get("_limited", False)
        shown = len(soql_recs)

        labels = set(r.get("_query_label") for r in soql_recs if "_query_label" in r)
        if labels:
            parts.append(f"COMBINED QUERY RESULTS ({total} total records from {len(labels)} queries):\nSQL used:\n{soql_query}")

            summary_labels = {l for l in labels if l.startswith("_summary_")}
            detail_labels = labels - summary_labels

            bu_summary = {}
            if summary_labels:
                for slabel in sorted(summary_labels):
                    display_label = slabel.replace("_summary_", "")
                    group = [r for r in soql_recs if r.get("_query_label") == slabel]
                    for rec in group:
                        bu = rec.get("BU_Name", "Unknown")
                        if bu not in bu_summary:
                            bu_summary[bu] = {}
                        bu_summary[bu][display_label] = rec.get("cnt", 0)
                        if "conf_cnt" in rec:
                            bu_summary[bu][display_label + "_confirmations"] = rec.get("conf_cnt", 0)
                        if "total_amount" in rec:
                            bu_summary[bu][display_label + "_amount"] = float(rec.get("total_amount", 0) or 0)
            else:
                for label in sorted(detail_labels):
                    group = [r for r in soql_recs if r.get("_query_label") == label]
                    clean = [{k: v for k, v in r.items() if k != "_query_label"} for r in group]
                    bu_field = None
                    for f in ["BU_Name__c", "BU_Name", "BU_Manager"]:
                        if clean and f in clean[0]:
                            bu_field = f
                            break
                    if bu_field:
                        for rec in clean:
                            bu = rec.get(bu_field, "Unknown")
                            if bu not in bu_summary:
                                bu_summary[bu] = {}
                            bu_summary[bu][label] = bu_summary[bu].get(label, 0) + 1
                            for amt_field in ["Amount__c", "total_amount"]:
                                if amt_field in rec and rec[amt_field]:
                                    amt_key = f"{label}_amount"
                                    bu_summary[bu][amt_key] = bu_summary[bu].get(amt_key, 0) + float(rec[amt_field] or 0)

            for label in sorted(detail_labels):
                group = [r for r in soql_recs if r.get("_query_label") == label]
                clean = [{k: v for k, v in r.items() if k != "_query_label"} for r in group]
                parts.append(f"\n--- {label} ({len(group)} records) ---")
                parts.append(json.dumps(clean[:100], indent=2, default=str)[:20000])

            if bu_summary:
                has_confirmations = any(k.endswith("_confirmations") for bu_data in bu_summary.values() for k in bu_data)
                has_amount = any(k.endswith("_amount") for bu_data in bu_summary.values() for k in bu_data)
                display_cols = sorted(detail_labels) if detail_labels else sorted({l.replace("_summary_", "") for l in summary_labels})

                parts.append("\n\nPRE-COMPUTED BU SUMMARY (use these EXACT numbers in your answer):")
                header = "| BU Name | " + " | ".join(display_cols)
                if has_confirmations:
                    header += " | Confirmations"
                if has_amount:
                    header += " | Interview Amount"
                parts.append(header + " |")
                sep = "|---|" + "|".join(["---"] * len(display_cols))
                if has_confirmations:
                    sep += "|---"
                if has_amount:
                    sep += "|---"
                parts.append(sep + "|")
                totals = {l: 0 for l in display_cols}
                total_conf = 0
                total_amt = 0.0
                for bu in sorted(bu_summary.keys()):
                    row = f"| {bu}"
                    for l in display_cols:
                        cnt = bu_summary[bu].get(l, 0)
                        totals[l] += cnt
                        row += f" | {cnt}"
                    if has_confirmations:
                        conf = sum(v for k, v in bu_summary[bu].items() if k.endswith("_confirmations"))
                        total_conf += conf
                        row += f" | {conf}"
                    if has_amount:
                        amt = sum(v for k, v in bu_summary[bu].items() if k.endswith("_amount"))
                        total_amt += amt
                        row += f" | {amt:,.2f}"
                    parts.append(row + " |")
                total_row = "| **Total**"
                for l in display_cols:
                    total_row += f" | **{totals[l]}**"
                if has_confirmations:
                    total_row += f" | **{total_conf}**"
                if has_amount:
                    total_row += f" | **{total_amt:,.2f}**"
                parts.append(total_row + " |")
        else:
            if is_limited:
                parts.append(f"QUERY RESULTS: **{total} TOTAL records** in database (showing {shown} below, but the TRUE TOTAL is {total}).\nIMPORTANT: Use {total} as the total count, NOT {shown}.\nSQL used: {soql_query}")
            else:
                parts.append(f"QUERY RESULTS ({total} total records from database):\nSQL used: {soql_query}")
            parts.append(json.dumps(soql_recs[:200], indent=2, default=str)[:50000])

    if rag_results:
        parts.append(f"\nSEMANTIC SEARCH RESULTS ({len(rag_results)} similar records):")
        for r in rag_results[:15]:
            parts.append(f"  [{r.get('sf_object', '')}] (sim: {r.get('score', 0):.2f}) {r['text'][:400]}")

    if not parts:
        error_detail = ""
        if soql_result and "error" in soql_result:
            error_detail = f"\n\n**Query error:** {soql_result['error'][:200]}"
        elif soql_query:
            error_detail = "\n\nThe query ran but returned 0 records."

        no_data_msg = (
            "I couldn't find data for that question."
            f"{error_detail}"
            "\n\n**Try:**"
            "\n- Check the spelling of names or statuses"
            "\n- Use broader terms (e.g. 'students' instead of a specific name)"
            "\n- Ask about a specific object: Students, Submissions, Interviews, Jobs, Employees"
        )
        yield {"type": "thinking_done", "data": None}
        yield {"type": "token", "data": no_data_msg}
        try:
            await save_interaction(question, soql_query, no_data_msg, route, username=username)
        except Exception:
            pass
        error_suggestions = [
            "How many students are in market?",
            "Show today's submissions by BU",
            "List all BU managers",
        ]
        yield {"type": "suggestions", "data": error_suggestions}
        yield {"type": "done", "data": {"answer": no_data_msg, "soql": soql_query, "route": route, "data": None, "suggestions": error_suggestions}}
        return

    # Pre-compute data summary for accurate counts
    if soql_recs:
        tt = soql_result.get("totalSize", len(soql_recs)) if soql_result else None
        data_summary = await _build_data_summary(soql_recs, true_total=tt, soql_query=soql_query)
        if data_summary:
            parts.append(data_summary)

    # Detect if this is a WhatsApp-style report request
    use_whatsapp = _is_whatsapp_report(question)
    if use_whatsapp:
        yield {"type": "thinking", "data": "Generating WhatsApp-style report"}
    else:
        yield {"type": "thinking", "data": "Generating formatted answer"}
    yield {"type": "thinking_done", "data": None}

    if use_whatsapp:
        system = WEEKLY_REPORT_PROMPT
    elif route in ("RAG", "BOTH"):
        system = RAG_PROMPT
    else:
        system = ANSWER_PROMPT

    conv_ctx = _build_conversation_context(conversation_history)
    prompt = f"{conv_ctx}Question: {question}\n\nData:\n" + "\n".join(parts)

    collected = []
    try:
        async for chunk in _call_ai_stream(system, prompt, max_tokens=6000):
            if not chunk:
                continue
            collected.append(chunk)
            yield {"type": "token", "data": chunk}
    except Exception as e:
        logger.error(f"Stream error: {e}")
        yield {"type": "error", "data": str(e)}

    answer = "".join(collected) or "Found data but couldn't summarize."

    # Post-verify counts in streamed answer
    if soql_recs is not None:
        answer = _verify_answer_counts(answer, soql_result, soql_recs, question)

    try:
        await save_interaction(question, soql_query, answer, route, username=username)
    except Exception as e:
        logger.warning(f"save_interaction failed: {e}")

    suggestions = await _generate_suggestions(question, answer)
    if suggestions:
        yield {"type": "suggestions", "data": suggestions}

    yield {
        "type": "done",
        "data": {
            "answer": answer,
            "soql": soql_query,
            "route": route,
            "data": data_payload,
            "suggestions": suggestions,
        },
    }
