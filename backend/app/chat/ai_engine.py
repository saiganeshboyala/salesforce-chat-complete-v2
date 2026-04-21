"""
Hybrid AI Engine with Self-Learning

Every question + answer is saved. The AI uses past successful
queries as examples to write better SQL over time.
Users can thumbs-up/down answers to train it.
"""
import json, logging, re, time
from app.config import settings
from app.salesforce.schema import schema_to_prompt, get_schema
from app.database.query import execute_query
from app.chat.rag import search as rag_search, is_indexed
from app.chat.memory import save_interaction, get_learning_examples_prompt

logger = logging.getLogger(__name__)

# ── Dynamic Picklist Values (loaded from DB on first use) ─────────
_picklist_cache = None

async def _load_picklist_values():
    """Load actual picklist values from PostgreSQL for accurate SQL generation."""
    global _picklist_cache
    if _picklist_cache is not None:
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
- "students under BU X" -> Student__c (use Manager__r.Name LIKE '%X%' for cross-object lookup)
- "student status" / "in market" / "exit" -> Student__c
- "submissions for BU X" -> Submissions__c (has BU_Name__c text field)
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

Q: "students with no interviews in 2 weeks"
A: SELECT "Name", "Technology__c", "Days_in_Market_Business__c" FROM "Student__c" WHERE "Student_Marketing_Status__c" = 'In Market' AND "Id" NOT IN (SELECT "Student__c" FROM "Interviews__c" WHERE "Interview_Date1__c" >= CURRENT_DATE - INTERVAL '14 days') LIMIT 2000

Q: "details of Sai Ganesh Chinnamsetty"
A: SELECT "Name", "Student_Marketing_Status__c", "Technology__c", "Phone__c", "Marketing_Email__c", "Personal_Email__c", "Marketing_Visa_Status__c", "Days_in_Market_Business__c", "Last_Submission_Date__c", "Verbal_Confirmation_Date__c", "Project_Start_Date__c" FROM "Student__c" WHERE "Name" ILIKE '%Chinnamsetty%' LIMIT 2000

Q: "last week interviews by BU"
A: SELECT m."Name" AS "BU_Name", s."Name" AS "Student_Name", i."Type__c", i."Final_Status__c", i."Amount__c", i."Interview_Date1__c" FROM "Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE i."Interview_Date1__c" >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week' AND i."Interview_Date1__c" < DATE_TRUNC('week', CURRENT_DATE) ORDER BY m."Name" LIMIT 2000

Q: "monthly submissions interviews confirmations BU wise"
A: SELECT "BU_Name__c", COUNT(*) AS cnt FROM "Submissions__c" WHERE "Submission_Date__c" >= DATE_TRUNC('month', CURRENT_DATE) AND "Submission_Date__c" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' GROUP BY "BU_Name__c" ORDER BY cnt DESC LIMIT 2000

FIELD NAME WARNINGS (common mistakes to avoid):
- Student__c does NOT have "Email__c" — use "Marketing_Email__c" or "Personal_Email__c"
- Interviews__c does NOT have "BU_Name__c" — JOIN through Student__c -> Manager__c
- Interviews__c."Onsite_Manager__c" is NOT the BU manager — it is a different role
- Use "Interview_Date1__c" (Date type) for date comparisons, NOT "Interview_Date__c" (DateTime)
- Student__c."Offshore_Manager_Name__c" = offshore manager name (text field, no JOIN needed)
- Submissions__c."BU_Name__c" = BU manager name (text field, no JOIN needed)
- Submissions__c."Offshore_Manager_Name__c" = offshore manager name (text field, no JOIN needed)"""

ANSWER_PROMPT = """You are a data analyst for a staffing/consulting company. Give PRECISE, CONCISE answers.

IRON RULES:
- Use ONLY the data in QUERY RESULTS. NEVER fabricate or guess.
- Match your response length to the question complexity:
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
        r = Anthropic(api_key=settings.anthropic_api_key).messages.create(
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
            client = Anthropic(api_key=settings.anthropic_api_key)
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

    valid_fields = {'count', 'id', 'cnt', 'total', 'total_amount'}
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
            # Skip aliases (AS ...), relationship traversals, and table-prefixed fields
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


# ── SQL Path ────────────────────────────────────────────────────

async def _soql_path(question, schema_text, history=None, last_soql=None):
    # Check cache first (skip for follow-ups that modify previous SQL)
    if not last_soql:
        cached = _cache_get(question)
        if cached:
            return cached

    # Step 0: Try direct pattern match (instant, no AI call needed)
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

    if history:
        ctx = "\n".join(f"{m['role']}: {m['content'][:200]}" for m in history[-4:])
        prompt = f"Conversation:\n{ctx}\n\n{prompt}"
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

    logger.info(f"SQL: {q[:200]}")

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
                logger.info(f"SQL retry 2 (different approach): {fix2[:200]}")
                q = fix2
                result = await execute_query(q)

        if "error" in result:
            return q, result, None

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
                # Check if answer has a different number
                nums_in_answer = re.findall(r'[\d,]+', answer)
                nums_in_answer = [int(n.replace(",", "")) for n in nums_in_answer if n.replace(",", "").isdigit() and int(n.replace(",", "")) > 0]
                if nums_in_answer and nums_in_answer[0] != first_val:
                    wrong_num = nums_in_answer[0]
                    correct = f"{first_val:,}"
                    answer = answer.replace(str(wrong_num), correct, 1)
                    answer = answer.replace(f"{wrong_num:,}", correct, 1)
                    logger.info(f"Answer count corrected: {wrong_num} -> {first_val}")

        # For list queries where LIMIT was hit, ensure answer uses true total
        if soql_result.get("_limited") and total > len(soql_recs):
            shown = len(soql_recs)
            # If answer says the shown count instead of true total
            shown_str = f"{shown:,}"
            total_str = f"{total:,}"
            if f"**{shown_str}" in answer and shown_str != total_str:
                answer = answer.replace(f"**{shown_str}", f"**{total_str}", 1)
                logger.info(f"Answer total corrected: {shown} -> {total}")

    except Exception as e:
        logger.warning(f"Answer verification failed: {e}")
    return answer


def _is_whatsapp_report(question):
    """Detect if the user wants a WhatsApp-style formatted report."""
    q = question.lower()
    triggers = [
        "send weekly report", "weekly report for", "weekly report of",
        "weekly performance report", "send report for", "whatsapp report",
        "send performance report",
    ]
    return any(t in q for t in triggers)


# ── Main Answer Functions ────────────────────────────────────────

async def answer_question(question, conversation_history=None, username=None, last_soql=None):
    schema_text = schema_to_prompt()
    if not schema_text or "No schema" in schema_text:
        return {"answer": "Schema not loaded. Run: python -m scripts.refresh_schema", "soql": None, "data": None}

    route = await _route(question)
    logger.info(f"Route: {route} | Q: {question[:60]}")

    soql_query, soql_result, soql_recs = None, None, None
    rag_results = None

    if route in ("SQL", "BOTH"):
        soql_query, soql_result, soql_recs = await _soql_path(question, schema_text, conversation_history, last_soql=last_soql)

        if soql_recs is None or (soql_recs is not None and len(soql_recs) == 0):
            name_words = [w for w in question.split() if len(w) > 2 and w[0].isupper()]
            if len(name_words) >= 2:
                last_word = name_words[-1]
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

    # Detect if this is a WhatsApp-style report request
    use_whatsapp = _is_whatsapp_report(question)
    if use_whatsapp:
        system = WEEKLY_REPORT_PROMPT
    elif route in ("RAG", "BOTH"):
        system = RAG_PROMPT
    else:
        system = ANSWER_PROMPT

    prompt = f"Question: {question}\n\nData:\n" + "\n".join(parts)
    if conversation_history:
        ctx = "\n".join(f"{m['role']}: {m['content'][:150]}" for m in conversation_history[-4:])
        prompt = f"Conversation:\n{ctx}\n\n{prompt}"

    answer = await _call_ai(system, prompt, max_tokens=6000)

    # Post-verify: check if count in answer matches actual DB data
    if answer and soql_recs is not None:
        answer = _verify_answer_counts(answer, soql_result, soql_recs, question)

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
    schema_text = schema_to_prompt()
    if not schema_text or "No schema" in schema_text:
        msg = "Schema not loaded. Run: python -m scripts.refresh_schema"
        yield {"type": "token", "data": msg}
        yield {"type": "done", "data": {"answer": msg, "soql": None, "data": None}}
        return

    yield {"type": "thinking", "data": "Analyzing question"}

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
                last_word = name_words[-1]
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

    prompt = f"Question: {question}\n\nData:\n" + "\n".join(parts)
    if conversation_history:
        ctx = "\n".join(f"{m['role']}: {m['content'][:150]}" for m in conversation_history[-4:])
        prompt = f"Conversation:\n{ctx}\n\n{prompt}"

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
