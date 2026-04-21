"""
Semantic Query Layer — maps natural language to exact SQL without AI.

Covers L1-L10 question types:
  L1: Counts, lists, general stats
  L2: Status/technology/visa/active filters
  L3: Date ranges (today, yesterday, this/last week/month, last N days)
  L4: BU-specific queries
  L5: Group-by, top-N, averages
  L6: Name lookups, student profiles, BU manager details
  L7: Multi-filter combinations
  L8: Cross-table, performance, no-activity
  L9: Reports, analytics, comparisons
  L10: Financial, edge cases, abbreviations
"""
import re
import logging
from app.database.query import execute_query

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# ENTITY DEFINITIONS
# ═══════════════════════════════════════════════════════════════

ENTITIES = {
    "students": {
        "table": '"Student__c"',
        "name_field": '"Name"',
        "count_sql": 'SELECT COUNT(*) AS cnt FROM "Student__c"',
        "list_fields": '"Name", "Student_Marketing_Status__c", "Technology__c", "Marketing_Visa_Status__c", "Days_in_Market_Business__c"',
        "detail_fields": '"Name", "Student_Marketing_Status__c", "Technology__c", "Marketing_Visa_Status__c", "Days_in_Market_Business__c", "Marketing_Email__c", "Phone__c", "University__c", "Batch__c", "Marketing_Start_Date__c", "Last_Submission_Date__c", "Submission_Count__c", "Interviews_Count__c", "Verbal_Confirmation_Date__c"',
        "date_field": '"CreatedDate"',
        "bu_join": 'LEFT JOIN "Manager__c" m ON "Student__c"."Manager__c" = m."Id"',
        "bu_field_alias": 'm."Name" AS "BU_Name"',
        "label": "students",
    },
    "submissions": {
        "table": '"Submissions__c"',
        "name_field": '"Student_Name__c"',
        "count_sql": 'SELECT COUNT(*) AS cnt FROM "Submissions__c"',
        "list_fields": '"Student_Name__c", "BU_Name__c", "Client_Name__c", "Submission_Date__c", "Rate__c"',
        "date_field": '"Submission_Date__c"',
        "bu_field": '"BU_Name__c"',
        "label": "submissions",
    },
    "interviews": {
        "table": '"Interviews__c"',
        "name_field": '"Name"',
        "count_sql": 'SELECT COUNT(*) AS cnt FROM "Interviews__c"',
        "list_fields": 's."Name" AS "Student_Name", m."Name" AS "BU_Name", i."Type__c", i."Final_Status__c", i."Amount__c", i."Interview_Date1__c"',
        "date_field": '"Interview_Date1__c"',
        "needs_join": True,
        "from_clause": '"Interviews__c" i LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id"',
        "bu_field_alias": 'm."Name"',
        "label": "interviews",
    },
    "jobs": {
        "table": '"Job__c"',
        "name_field": '"Name"',
        "count_sql": 'SELECT COUNT(*) AS cnt FROM "Job__c"',
        "list_fields": '"Name", "Active__c", "Technology__c", "Client_Name__c", "Job_Location__c", "Bill_Rate__c", "Project_Start_Date__c"',
        "date_field": '"CreatedDate"',
        "label": "jobs",
    },
    "employees": {
        "table": '"Employee__c"',
        "name_field": '"Name"',
        "count_sql": 'SELECT COUNT(*) AS cnt FROM "Employee__c"',
        "list_fields": '"Name", "Deptment__c", "Email__c", "BU_Name__c"',
        "date_field": '"CreatedDate"',
        "label": "employees",
    },
    "managers": {
        "table": '"Manager__c"',
        "name_field": '"Name"',
        "count_sql": 'SELECT COUNT(*) AS cnt FROM "Manager__c"',
        "list_fields": '"Name", "Active__c", "Students_Count__c", "In_Market_Students_Count__c", "Verbal_Count__c", "Total_Expenses__c", "Each_Placement_Cost__c"',
        "date_field": '"CreatedDate"',
        "label": "BU managers",
    },
    "contacts": {
        "table": '"Contact"',
        "name_field": '"Name"',
        "count_sql": 'SELECT COUNT(*) AS cnt FROM "Contact"',
        "list_fields": '"Name", "Email", "Phone", "Title"',
        "date_field": '"CreatedDate"',
        "label": "contacts",
    },
    "accounts": {
        "table": '"Organization__c"',
        "name_field": '"Name"',
        "count_sql": 'SELECT COUNT(*) AS cnt FROM "Organization__c"',
        "list_fields": '"Name", "Website__c", "Domain__c"',
        "date_field": '"CreatedDate"',
        "label": "accounts",
    },
}

# ═══════════════════════════════════════════════════════════════
# KEYWORD MAPS
# ═══════════════════════════════════════════════════════════════

_ENTITY_KEYWORDS = [
    ("students", ["student", "students", "bench", "in market", "headcount", "team size",
                   "pre marketing", "premarketing", "verbal", "confirmation", "exit",
                   "project started", "project completed",
                   "candidates", "candidate", "consultant", "consultants",
                   "resources", "resource", "trainees", "trainee", "people",
                   "days in market", "dim ", "marketing status", "batch"]),
    ("submissions", ["submission", "submissions", "sub count", "subs ",
                      "submitted", "subs today", "subs this", "subs last",
                      "daily subs", "weekly subs", "resume sent", "resumes sent",
                      "profiles sent", "profiles submitted"]),
    ("interviews", ["interview", "interviews", "int count", "ints ",
                     "scheduled interview", "upcoming interview",
                     "int today", "ints today", "ints this", "ints last",
                     "daily ints", "weekly ints"]),
    ("jobs", ["job ", "jobs", "active job", "open position", "open role",
              "job opening", "job posting", "requirement", "requirements"]),
    ("employees", ["employee", "employees", "staff", "internal team",
                    "team member", "team members", "workforce"]),
    ("managers", ["bu manager", "bu names", "manager list", "manager leaderboard",
                   "bus ", "placement cost", "bu expense",
                   "all bus", "all bu", "manager", "managers",
                   "business unit", "business units"]),
    ("contacts", ["contact", "contacts"]),
    ("accounts", ["account", "accounts", "organization", "company", "companies",
                   "client list", "vendor list"]),
]

_STATUS_MAP = {
    "in market": "In Market", "bench": "In Market", "on bench": "In Market",
    "active in market": "In Market", "currently in market": "In Market",
    "currently in marketing": "In Market", "marketing": "In Market",
    "in the market": "In Market", "in-market": "In Market",
    "actively marketing": "In Market", "being marketed": "In Market",
    "available": "In Market", "ready for market": "In Market",
    "verbal confirmation": "Verbal Confirmation", "verbal": "Verbal Confirmation",
    "confirmed": "Verbal Confirmation", "confirmation": "Verbal Confirmation",
    "got confirmed": "Verbal Confirmation", "who got confirmed": "Verbal Confirmation",
    "vc ": "Verbal Confirmation", "verbals": "Verbal Confirmation",
    "got placed": "Verbal Confirmation", "placed": "Verbal Confirmation",
    "got offer": "Verbal Confirmation", "received offer": "Verbal Confirmation",
    "pre marketing": "Pre Marketing", "premarketing": "Pre Marketing",
    "pre-marketing": "Pre Marketing", "not ready": "Pre Marketing",
    "pre market": "Pre Marketing", "training": "Pre Marketing",
    "in training": "Pre Marketing", "not in market": "Pre Marketing",
    "exit": "Exit", "exited": "Exit", "left": "Exit", "pulled out": "Exit",
    "dropped": "Exit", "quit": "Exit", "terminated": "Exit",
    "removed": "Exit", "left the program": "Exit",
    "project started": "Project Started", "started project": "Project Started",
    "on project": "Project Started", "working on project": "Project Started",
    "in project": "Project Started", "currently working": "Project Started",
    "project completed": "Project Completed", "completed project": "Project Completed",
    "finished project": "Project Completed",
    "project completed-in market": "Project Completed-In Market",
    "payroll": "Payroll Purpose", "payroll purpose": "Payroll Purpose",
}

_TECH_KEYWORDS = [
    ("Java", ["java"]),
    ("Python", ["python"]),
    ("DevOps", ["devops"]),
    (".NET", [".net", "dotnet"]),
    ("Data Engineering", ["data engineering", "de "]),
    ("SFDC", ["sfdc", "salesforce developer"]),
    ("DS/AI", ["ds/ai", "ds ai", "data science", "aigee"]),
    ("Business Analyst", ["business analyst", "ba "]),
    ("ServiceNow", ["servicenow"]),
    ("SAP BTP", ["sap btp", "sap"]),
    ("RPA", ["rpa"]),
    ("PowerBI", ["powerbi", "power bi"]),
    ("Tableau", ["tableau"]),
    ("AEM", ["aem"]),
    ("SQL Developer", ["sql developer"]),
    ("Cyber Security", ["cyber security", "cybersecurity", "cs "]),
    ("Scrum Master", ["scrum master"]),
    ("Full Stack", ["full stack", "fullstack"]),
    ("React JS", ["react"]),
    ("Angular", ["angular"]),
    ("AWS", ["aws"]),
    ("Azure", ["azure"]),
    ("QA", ["qa ", "testing", "selenium"]),
    ("Pega", ["pega"]),
    ("MuleSoft", ["mulesoft"]),
    ("Snowflake", ["snowflake"]),
    ("Databricks", ["databricks"]),
    ("UI Developer", ["ui developer", "ui dev"]),
]

_VISA_MAP = {
    "h1": "H1", "h1b": "H1", "h1 visa": "H1", "h1b visa": "H1",
    "h-1b": "H1", "h-1": "H1",
    "opt": "OPT", "opt visa": "OPT", "opt ead": "OPT",
    "stem": "STEM", "stem opt": "STEM", "stem ead": "STEM",
    "gc": "GC", "green card": "GC", "gc ead": "GC",
    "permanent resident": "GC",
    "h4 ead": "H4 EAD", "h4ead": "H4 EAD", "h4": "H4 EAD",
    "h-4 ead": "H4 EAD", "h-4": "H4 EAD",
    "usc": "USC", "citizen": "USC", "us citizen": "USC",
    "cpt": "CPT", "cpt visa": "CPT",
    "l2": "L2", "l2 ead": "L2", "l-2": "L2",
    "l1": "L1", "l-1": "L1",
    "tn": "TN", "tn visa": "TN",
}

_TIME_RANGES = {
    "today": ("CURRENT_DATE", "CURRENT_DATE + INTERVAL '1 day'", "today"),
    "today's": ("CURRENT_DATE", "CURRENT_DATE + INTERVAL '1 day'", "today"),
    "yesterday": ("CURRENT_DATE - INTERVAL '1 day'", "CURRENT_DATE", "yesterday"),
    "yesterday's": ("CURRENT_DATE - INTERVAL '1 day'", "CURRENT_DATE", "yesterday"),
    "this week": ("DATE_TRUNC('week', CURRENT_DATE)", "DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '1 week'", "this week"),
    "current week": ("DATE_TRUNC('week', CURRENT_DATE)", "DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '1 week'", "this week"),
    "last week": ("DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week'", "DATE_TRUNC('week', CURRENT_DATE)", "last week"),
    "previous week": ("DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week'", "DATE_TRUNC('week', CURRENT_DATE)", "last week"),
    "past week": ("CURRENT_DATE - INTERVAL '7 days'", "CURRENT_DATE + INTERVAL '1 day'", "past 7 days"),
    "this month": ("DATE_TRUNC('month', CURRENT_DATE)", "DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'", "this month"),
    "current month": ("DATE_TRUNC('month', CURRENT_DATE)", "DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'", "this month"),
    "last month": ("DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'", "DATE_TRUNC('month', CURRENT_DATE)", "last month"),
    "previous month": ("DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'", "DATE_TRUNC('month', CURRENT_DATE)", "last month"),
    "past month": ("CURRENT_DATE - INTERVAL '30 days'", "CURRENT_DATE + INTERVAL '1 day'", "past 30 days"),
    "this year": ("DATE_TRUNC('year', CURRENT_DATE)", "DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year'", "this year"),
    "current year": ("DATE_TRUNC('year', CURRENT_DATE)", "DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year'", "this year"),
    "last year": ("DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'", "DATE_TRUNC('year', CURRENT_DATE)", "last year"),
    "previous year": ("DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'", "DATE_TRUNC('year', CURRENT_DATE)", "last year"),
    "last 3 days": ("CURRENT_DATE - INTERVAL '3 days'", "CURRENT_DATE + INTERVAL '1 day'", "last 3 days"),
    "last 7 days": ("CURRENT_DATE - INTERVAL '7 days'", "CURRENT_DATE + INTERVAL '1 day'", "last 7 days"),
    "last 14 days": ("CURRENT_DATE - INTERVAL '14 days'", "CURRENT_DATE + INTERVAL '1 day'", "last 14 days"),
    "last 30 days": ("CURRENT_DATE - INTERVAL '30 days'", "CURRENT_DATE + INTERVAL '1 day'", "last 30 days"),
    "last 90 days": ("CURRENT_DATE - INTERVAL '90 days'", "CURRENT_DATE + INTERVAL '1 day'", "last 90 days"),
}

# ═══════════════════════════════════════════════════════════════
# DETECTION HELPERS
# ═══════════════════════════════════════════════════════════════

def _detect_entity(q):
    for entity, keywords in _ENTITY_KEYWORDS:
        for kw in keywords:
            if kw in q:
                if entity == "students" and kw in ("in market", "verbal", "confirmation", "exit",
                                                    "bench", "pre marketing", "premarketing",
                                                    "project started", "project completed",
                                                    "available", "placed", "training"):
                    if any(w in q for w in ["submission", "interview", "subs ", "ints ",
                                             "sub count", "int count"]):
                        continue
                return entity
    return None

def _detect_status(q):
    for kw, val in _STATUS_MAP.items():
        if kw in q:
            return val
    return None

def _detect_tech(q):
    for val, keywords in _TECH_KEYWORDS:
        for kw in keywords:
            if kw in q:
                return val
    return None

def _detect_visa(q):
    for kw, val in _VISA_MAP.items():
        if kw in q:
            return val
    return None

def _detect_time(q):
    for label, (start, end, display) in _TIME_RANGES.items():
        if label in q:
            return start, end, display
    m = re.search(r'last\s+(\d+)\s+days?', q)
    if m:
        n = m.group(1)
        return f"CURRENT_DATE - INTERVAL '{n} days'", "CURRENT_DATE + INTERVAL '1 day'", f"last {n} days"
    m = re.search(r'last\s+(\d+)\s+weeks?', q)
    if m:
        n = int(m.group(1)) * 7
        return f"CURRENT_DATE - INTERVAL '{n} days'", "CURRENT_DATE + INTERVAL '1 day'", f"last {m.group(1)} weeks"
    m = re.search(r'last\s+(\d+)\s+months?', q)
    if m:
        n = m.group(1)
        return f"CURRENT_DATE - INTERVAL '{n} months'", "CURRENT_DATE + INTERVAL '1 day'", f"last {n} months"
    m = re.search(r'past\s+(\d+)\s+days?', q)
    if m:
        n = m.group(1)
        return f"CURRENT_DATE - INTERVAL '{n} days'", "CURRENT_DATE + INTERVAL '1 day'", f"past {n} days"
    return None, None, None

def _detect_bu_name(q, question):
    patterns = [
        r'(?:under|for|of|bu)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,4})',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,4})(?:\'?s?\s+(?:student|submission|interview|bu|team|performance|subs|ints))',
    ]
    for pat in patterns:
        m = re.search(pat, question)
        if m:
            name = m.group(1).strip()
            stop = {'How', 'Show', 'List', 'Give', 'What', 'The', 'All', 'Total', 'Count',
                    'Number', 'Many', 'This', 'Last', 'Month', 'Week', 'Today', 'Send',
                    'Weekly', 'Monthly', 'Daily', 'Java', 'Python', 'DevOps'}
            if name.split()[0] not in stop:
                return name
    # Short BU name patterns like "abhijith subs", "divya students"
    m = re.search(r'^([a-z]+)\s+(?:subs|ints|students|team|performance|submissions|interviews)', q)
    if m:
        name = m.group(1).title()
        stop_short = {'Idle', 'Active', 'Total', 'All', 'Top', 'Bottom', 'Best', 'Worst',
                      'No', 'Zero', 'How', 'Show', 'Give', 'List', 'Get', 'My'}
        if name not in stop_short:
            return name
    return None

def _detect_person_name(q, question):
    patterns = [
        r'(?:details?\s+(?:of|for)\s+)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
        r'(?:status\s+of\s+)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
        r'(?:who\s+is\s+)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
        r'(?:find\s+(?:student\s+)?(?:named\s+)?)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
        r'(?:search\s+(?:for\s+)?(?:student\s+)?)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
        r'(?:look\s+up\s+)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
        r'(?:information\s+(?:of|for|about)\s+)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
        r'(?:profile\s+(?:of|for)\s+)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
        r'(?:everything\s+about\s+)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
        r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(?:details|information|status|profile|submissions?|interviews?)',
        r'(?:submissions?\s+(?:for|of)\s+)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
        r'(?:interviews?\s+(?:for|of)\s+)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
    ]
    stop = {'Show', 'List', 'Give', 'What', 'How', 'All', 'Total', 'Find', 'Search', 'The',
            'Students', 'Which', 'Java', 'Python', 'Send', 'Weekly', 'Monthly', 'BU'}
    for pat in patterns:
        m = re.search(pat, question)
        if m:
            name = m.group(1).strip()
            if name.split()[0] not in stop and len(name) > 2:
                return name
    # "All students named X"
    m = re.search(r'(?:named|name)\s+([A-Z][a-z]+)', question)
    if m:
        return m.group(1)
    return None

def _detect_days_threshold(q):
    m = re.search(r'(?:more\s+than|over|above|greater\s+than|>|exceeding|beyond)\s*(\d+)\s*days', q)
    if m:
        return int(m.group(1))
    m = re.search(r'(\d+)\+?\s*days\s+(?:in\s+market|no\s+|or\s+more|plus)', q)
    if m:
        return int(m.group(1))
    m = re.search(r'(\d+)\+\s*(?:dim|days)', q)
    if m:
        return int(m.group(1))
    if "long time" in q or "too long" in q:
        return 90
    return None

def _detect_top_n(q):
    m = re.search(r'top\s+(\d+)', q)
    if m:
        return int(m.group(1))
    m = re.search(r'(?:bottom|lowest|worst|least)\s+(\d+)', q)
    if m:
        return -int(m.group(1))
    m = re.search(r'(?:best|highest)\s+(\d+)', q)
    if m:
        return int(m.group(1))
    if "top " in q:
        return 10
    return None

def _detect_no_activity(q):
    if any(w in q for w in ["no submission", "zero submission", "no sub", "without submission",
                             "no recent submission", "dormant", "inactive", "no activity",
                             "not submitted", "haven't submitted", "havent submitted",
                             "0 submission", "0 subs", "no subs",
                             "not getting submission", "idle student", "idle"]):
        days_m = re.search(r'(\d+)\s*(?:day|week)', q)
        if days_m:
            n = int(days_m.group(1))
            if "week" in q:
                n *= 7
            return "submissions", n
        return "submissions", 7
    if any(w in q for w in ["no interview", "zero interview", "no int", "without interview",
                             "not interviewed", "haven't interviewed", "havent interviewed",
                             "0 interview", "0 ints", "no ints",
                             "not getting interview"]):
        days_m = re.search(r'(\d+)\s*(?:day|week)', q)
        if days_m:
            n = int(days_m.group(1))
            if "week" in q:
                n *= 7
            return "interviews", n
        return "interviews", 14
    return None, None

def _detect_group_by(q, entity):
    if any(w in q for w in ["technology wise", "by technology", "tech wise", "technology breakdown",
                             "per technology", "each technology", "technology-wise", "techwise",
                             "grouped by technology", "group by tech", "tech breakdown"]):
        return '"Technology__c"', "Technology"
    if any(w in q for w in ["visa wise", "by visa", "visa type", "visa status", "visa category",
                             "per visa", "each visa", "visa-wise", "grouped by visa",
                             "visa breakdown"]):
        return '"Marketing_Visa_Status__c"', "Visa Status"
    if any(w in q for w in ["status wise", "by status", "status breakdown", "per status",
                             "each status", "status-wise", "grouped by status",
                             "statuswise"]):
        return '"Student_Marketing_Status__c"', "Status"
    if any(w in q for w in ["bu wise", "by bu", "manager wise", "by manager", "bu report",
                             "per bu", "each bu", "bu-wise", "buwise", "grouped by bu",
                             "per manager", "manager-wise", "managerwise",
                             "business unit wise", "by business unit"]):
        return "_BU_", "BU Name"
    if any(w in q for w in ["client wise", "by client", "per client", "each client",
                             "client-wise", "clientwise", "grouped by client",
                             "client breakdown"]):
        if entity == "submissions":
            return '"Client_Name__c"', "Client"
        if entity == "interviews":
            return 'i."Client_Name__c"', "Client"
    if any(w in q for w in ["type wise", "by type", "interview type", "per type",
                             "type-wise", "typewise", "grouped by type"]):
        return '"Type__c"', "Interview Type"
    if any(w in q for w in ["batch wise", "by batch", "per batch", "each batch",
                             "batch-wise", "batchwise"]) and entity == "students":
        return '"Batch__c"', "Batch"
    if any(w in q for w in ["university wise", "by university", "per university",
                             "college wise", "by college"]) and entity == "students":
        return '"University__c"', "University"
    return None, None


# ═══════════════════════════════════════════════════════════════
# SQL BUILDERS
# ═══════════════════════════════════════════════════════════════

def _build_where(wheres):
    return " WHERE " + " AND ".join(wheres) if wheres else ""

async def _run(sql):
    result = await execute_query(sql)
    if "error" in result:
        logger.warning(f"Semantic query failed: {result['error'][:120]}")
        return None, result
    recs = result.get("records", [])
    for r in recs:
        r.pop("attributes", None)
    return recs, result

def _table_answer(title, headers, rows, totals=None, footer=""):
    lines = [f"**{title}**\n"]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join(["---:" if i > 0 else "---" for i in range(len(headers))]) + "|")
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    if totals:
        lines.append("| " + " | ".join(f"**{c}**" for c in totals) + " |")
    if footer:
        lines.append(f"\n{footer}")
    return "\n".join(lines)

def _make_result(answer, sql, recs, total=None):
    return {
        "answer": answer,
        "soql": sql,
        "data": {
            "totalSize": total or len(recs),
            "records": recs[:200],
            "query": sql,
            "route": "SQL",
            "rag_results": 0,
        },
    }


# ═══════════════════════════════════════════════════════════════
# MAIN HANDLER
# ═══════════════════════════════════════════════════════════════

async def handle_semantic_query(question):
    q = question.lower().strip()
    q = (q.replace("conformation", "confirmation")
          .replace("submision", "submission").replace("submisions", "submissions")
          .replace("intervew", "interview").replace("interveiw", "interview")
          .replace("studens", "students").replace("studnets", "students")
          .replace("employe", "employee").replace("mangaer", "manager")
          .replace("confrimation", "confirmation").replace("confermation", "confirmation")
          .replace("submsission", "submission").replace("submssion", "submission")
          .replace("intrvw", "interview").replace("interivew", "interview")
          .replace("  ", " "))

    # ── L6: Person name lookup ──────────────────────────────
    person = _detect_person_name(q, question)
    if person and not any(w in q for w in ["bu wise", "by bu", "manager list", "all bu"]):
        return await _handle_person_lookup(person, q, question)

    # ── L1 General: stats/summary/overview ──────────────────
    if any(w in q for w in ["quick summary", "brief overview", "daily update", "status update",
                             "what is happening", "give me stats", "show numbers", "give me kpis",
                             "dashboard data", "show me everything", "show me all data",
                             "data overview", "system summary", "how are we doing",
                             "are we on track", "what's happening",
                             "overall summary", "overall status", "overall numbers",
                             "give me summary", "show summary", "today's update",
                             "todays update", "morning update", "end of day",
                             "eod update", "eod report", "eod summary",
                             "daily report", "daily status", "quick update",
                             "what are the numbers", "current numbers",
                             "tell me everything", "full summary",
                             "what do we have", "show everything",
                             "how is everything", "how's everything"]):
        return await _handle_summary()

    # ── L9: Month-over-month comparison ────────────────────
    if any(w in q for w in ["vs last month", "compared to last month", "month over month",
                             "this month vs", "mom ", "month comparison", "compare this month",
                             "versus last month", "comparison with last month",
                             "this month compared", "month on month", "m-o-m",
                             "growth from last month", "change from last month",
                             "trend this month", "how much more than last month",
                             "increase from last month", "decrease from last month"]):
        return await _handle_month_comparison(q)

    # ── L9: Conversion rate / funnel ───────────────────────
    if any(w in q for w in ["conversion rate", "conversion ratio", "funnel", "pipeline",
                             "submission to interview", "interview to confirmation",
                             "sub to int", "hit rate",
                             "success rate", "placement rate", "conversion",
                             "sub to interview", "interview to placement",
                             "how many convert", "what percentage",
                             "strike rate", "win rate"]):
        bu_name = _detect_bu_name(q, question)
        return await _handle_conversion_rate(q, bu_name)

    # ── L9: BU leaderboard / scorecard ─────────────────────
    if any(w in q for w in ["leaderboard", "scorecard", "ranking", "best bu", "worst bu",
                             "top performing", "bottom performing", "bu performance",
                             "compare bu", "bu comparison", "which bu",
                             "bu ranking", "rank all bu", "rank bus",
                             "best performing", "worst performing",
                             "highest submissions", "lowest submissions",
                             "most productive", "least productive",
                             "who is leading", "who is behind",
                             "performance comparison", "bu standings"]):
        return await _handle_bu_leaderboard(q)

    # ── L9: Multi-metric BU report ─────────────────────────
    if any(w in q for w in ["monthly report", "bu wise report", "bu report",
                             "monthly bu", "bu monthly", "full report",
                             "subs and int", "submissions and interview",
                             "complete report", "detailed report",
                             "comprehensive report", "weekly report",
                             "send me report", "generate report",
                             "give me report", "show report",
                             "submissions interviews confirmation",
                             "all metrics", "all numbers bu",
                             "weekly bu", "bu weekly"]):
        bu_name = _detect_bu_name(q, question)
        time_start, time_end, time_label = _detect_time(q)
        return await _handle_bu_full_report(q, bu_name, time_start, time_end, time_label)

    # ── L8: No-activity queries (always about students, check before entity detection)
    no_act_type, no_act_days = _detect_no_activity(q)
    if no_act_type:
        bu_name = _detect_bu_name(q, question)
        return await _handle_no_activity(no_act_type, no_act_days, bu_name, q)

    entity = _detect_entity(q)
    if not entity:
        return None

    ent = ENTITIES[entity]
    status = _detect_status(q)
    tech = _detect_tech(q)
    visa = _detect_visa(q)
    time_start, time_end, time_label = _detect_time(q)
    bu_name = _detect_bu_name(q, question)
    days_thresh = _detect_days_threshold(q)
    group_field, group_label = _detect_group_by(q, entity)
    top_n = _detect_top_n(q)

    # ── L10: Financial / expense queries ────────────────────
    if entity == "managers" and any(w in q for w in ["expense", "placement cost", "cost",
                                                      "profitability", "efficiency",
                                                      "spending", "budget", "money",
                                                      "investment", "roi", "cost per",
                                                      "how much spent", "total cost",
                                                      "expenditure", "expenses"]):
        return await _handle_financial(q, bu_name)

    # Build WHERE clauses
    wheres = []
    needs_bu_join = False

    if status and entity == "students":
        wheres.append(f'"Student_Marketing_Status__c" = \'{status}\'')
    if tech and entity == "students":
        wheres.append(f'"Technology__c" ILIKE \'%{tech}%\'')
    if visa and entity == "students":
        wheres.append(f'"Marketing_Visa_Status__c" = \'{visa}\'')
    if "active" in q and entity in ("jobs", "managers"):
        wheres.append('"Active__c" = true')
    if days_thresh and entity == "students":
        wheres.append(f'"Days_in_Market_Business__c" > {days_thresh}')
        if not status:
            wheres.append('"Student_Marketing_Status__c" = \'In Market\'')

    if time_start:
        if entity == "interviews" and ent.get("needs_join"):
            wheres.append(f'i.{ent["date_field"]} >= {time_start} AND i.{ent["date_field"]} < {time_end}')
        else:
            wheres.append(f'{ent["date_field"]} >= {time_start} AND {ent["date_field"]} < {time_end}')

    # Numeric filter: rate/amount above/below X
    rate_m = re.search(r'(?:rate|amount|bill\s*rate)\s+(?:is\s+)?(?:above|over|greater than|>|more than|exceeds?|higher than)\s*(\d+)', q)
    if rate_m:
        val = rate_m.group(1)
        if entity == "submissions":
            wheres.append(f'"Rate__c" > {val}')
        elif entity == "interviews":
            wheres.append(f'i."Amount__c" > {val}')
        elif entity == "jobs":
            wheres.append(f'"Bill_Rate__c" > {val}')
    rate_m2 = re.search(r'(?:rate|amount|bill\s*rate)\s+(?:is\s+)?(?:below|under|less than|<|lower than)\s*(\d+)', q)
    if rate_m2:
        val = rate_m2.group(1)
        if entity == "submissions":
            wheres.append(f'"Rate__c" < {val}')
        elif entity == "interviews":
            wheres.append(f'i."Amount__c" < {val}')
        elif entity == "jobs":
            wheres.append(f'"Bill_Rate__c" < {val}')

    if bu_name:
        if entity == "students":
            needs_bu_join = True
            wheres.append(f'm."Name" ILIKE \'%{bu_name}%\'')
        elif entity == "submissions":
            wheres.append(f'"BU_Name__c" ILIKE \'%{bu_name}%\'')
        elif entity == "interviews":
            wheres.append(f'm."Name" ILIKE \'%{bu_name}%\'')

    where_sql = _build_where(wheres)

    # ── Desc builder ────────────────────────────────────────
    desc_parts = []
    if status:
        desc_parts.append(status.lower())
    if tech:
        desc_parts.append(tech)
    if visa:
        desc_parts.append(f"{visa} visa")
    if time_label:
        desc_parts.append(time_label)
    if bu_name:
        desc_parts.append(f"under {bu_name}")
    if days_thresh:
        desc_parts.append(f">{days_thresh} days in market")
    desc = f" ({', '.join(desc_parts)})" if desc_parts else ""

    # ── AVERAGE queries (L5) — check before group-by since "average X by Y" has both
    if any(w in q for w in ["average", "avg", "mean"]):
        result = await _handle_average(entity, ent, q, wheres, where_sql, needs_bu_join)
        if result:
            return result

    # ── GROUP BY queries (L5, L7, L9) ──────────────────────
    if group_field:
        return await _handle_group_by(entity, ent, group_field, group_label, wheres, where_sql, needs_bu_join, desc, time_label)

    # ── TOP N queries (L5) ─────────────────────────────────
    if top_n:
        return await _handle_top_n(entity, ent, top_n, q, wheres, where_sql)

    # ── COUNT queries (L1-L4, L7) ──────────────────────────
    is_count = any(w in q for w in ["how many", "count", "total", "number of", "cnt", "how much",
                                     "give me count", "show count", "total number",
                                     "what is the count", "what's the count",
                                     "how much", "headcount", "strength",
                                     "kitne", "kितने"])
    if is_count:
        return await _handle_count(entity, ent, wheres, needs_bu_join, desc)

    # ── LIST queries (default) ─────────────────────────────
    return await _handle_list(entity, ent, wheres, where_sql, needs_bu_join, desc)


# ═══════════════════════════════════════════════════════════════
# HANDLER FUNCTIONS
# ═══════════════════════════════════════════════════════════════

async def _handle_count(entity, ent, wheres, needs_bu_join, desc):
    if entity == "interviews" and ent.get("needs_join"):
        sql = f'SELECT COUNT(*) AS cnt FROM {ent.get("from_clause", ent["table"])}'
    else:
        sql = ent["count_sql"]
        if needs_bu_join:
            sql = f'SELECT COUNT(*) AS cnt FROM {ent["table"]} {ent.get("bu_join", "")}'
    sql += _build_where(wheres)
    recs, result = await _run(sql)
    if recs is None:
        return None
    count = recs[0].get("cnt", 0) if recs else 0
    answer = f"**{count:,} {ent['label']}**{desc}."
    return _make_result(answer, sql, recs)


async def _handle_list(entity, ent, wheres, where_sql, needs_bu_join, desc):
    if entity == "interviews" and ent.get("needs_join"):
        sql = f'SELECT {ent["list_fields"]} FROM {ent["from_clause"]}'
    elif needs_bu_join and entity == "students":
        sql = (f'SELECT s."Name", s."Student_Marketing_Status__c", s."Technology__c", '
               f's."Marketing_Visa_Status__c", s."Days_in_Market_Business__c", m."Name" AS "BU_Name" '
               f'FROM "Student__c" s {ent["bu_join"]}')
    else:
        sql = f'SELECT {ent["list_fields"]} FROM {ent["table"]}'
    order_field = f'i.{ent["name_field"]}' if entity == "interviews" and ent.get("needs_join") else ent["name_field"]
    sql += where_sql + f' ORDER BY {order_field} LIMIT 2000'

    count_sql = f'SELECT COUNT(*) AS cnt FROM {ent.get("from_clause", ent["table"])}'
    if needs_bu_join and entity == "students":
        count_sql = f'SELECT COUNT(*) AS cnt FROM "Student__c" s {ent["bu_join"]}'
    count_sql += where_sql

    recs, _ = await _run(sql)
    cnt_recs, _ = await _run(count_sql)
    if recs is None:
        return None
    total = cnt_recs[0].get("cnt", len(recs)) if cnt_recs else len(recs)
    answer = f"**{total:,} {ent['label']}**{desc}."
    return _make_result(answer, sql, recs, total)


async def _handle_group_by(entity, ent, group_field, group_label, wheres, where_sql, needs_bu_join, desc, time_label):
    if group_field == "_BU_":
        return await _handle_bu_group(entity, ent, wheres, time_label)

    if entity == "interviews" and ent.get("needs_join"):
        gf = group_field if '.' in group_field else f'i.{group_field}'
        sql = f'SELECT {gf}, COUNT(*) AS cnt FROM {ent["from_clause"]}'
    else:
        sql = f'SELECT {group_field}, COUNT(*) AS cnt FROM {ent["table"]}'
        if needs_bu_join:
            sql += f' {ent.get("bu_join", "")}'
    extra_where = f' AND {group_field} IS NOT NULL' if where_sql else f' WHERE {group_field} IS NOT NULL'
    sql += where_sql + extra_where + f' GROUP BY {group_field} ORDER BY cnt DESC'

    recs, _ = await _run(sql)
    if recs is None:
        return None
    total = sum(r.get("cnt", 0) for r in recs)
    fk = next((k for k in recs[0] if k != "cnt"), "Name") if recs else "Name"
    rows = [[r.get(fk, "N/A"), f"{r.get('cnt', 0):,}"] for r in recs]
    totals = ["**Total**", f"**{total:,}**"]
    footer = f"{len(recs)} {group_label.lower()}s, {total:,} total {ent['label']}"
    if time_label:
        footer += f" ({time_label})"
    answer = _table_answer(f"{ent['label'].title()} by {group_label}", [group_label, "Count"], rows, totals, footer)
    return _make_result(answer, sql, recs)


async def _handle_bu_group(entity, ent, wheres, time_label):
    where_sql = _build_where(wheres)
    if entity == "students":
        sql = (f'SELECT m."Name" AS "BU_Name", COUNT(*) AS cnt FROM "Student__c" '
               f'LEFT JOIN "Manager__c" m ON "Student__c"."Manager__c" = m."Id"'
               f'{where_sql}')
        sql += (' AND' if wheres else ' WHERE') + ' m."Name" IS NOT NULL GROUP BY m."Name" ORDER BY cnt DESC'
    elif entity == "submissions":
        sql = f'SELECT "BU_Name__c" AS "BU_Name", COUNT(*) AS cnt FROM "Submissions__c"{where_sql}'
        sql += (' AND' if wheres else ' WHERE') + ' "BU_Name__c" IS NOT NULL GROUP BY "BU_Name__c" ORDER BY cnt DESC'
    elif entity == "interviews":
        sql = (f'SELECT m."Name" AS "BU_Name", COUNT(*) AS cnt FROM "Interviews__c" i '
               f'LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" '
               f'LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id"'
               f'{where_sql}')
        sql += (' AND' if wheres else ' WHERE') + ' m."Name" IS NOT NULL GROUP BY m."Name" ORDER BY cnt DESC'
    else:
        return None

    recs, _ = await _run(sql)
    if recs is None:
        return None
    total = sum(r.get("cnt", 0) for r in recs)
    rows = [[r.get("BU_Name", "N/A"), f"{r.get('cnt', 0):,}"] for r in recs]
    totals = ["**Total**", f"**{total:,}**"]
    footer = f"{len(recs)} BUs, {total:,} total {ent['label']}"
    if time_label:
        footer += f" ({time_label})"
    answer = _table_answer(f"{ent['label'].title()} by BU", ["BU Name", "Count"], rows, totals, footer)
    return _make_result(answer, sql, recs)


async def _handle_top_n(entity, ent, n, q, wheres, where_sql):
    is_bottom = n < 0
    abs_n = abs(n)
    order_dir = "ASC" if is_bottom else "DESC"
    label_prefix = "Bottom" if is_bottom else "Top"

    if entity == "managers":
        order_field = '"Students_Count__c"'
        if "expense" in q:
            order_field = '"Total_Expenses__c"'
        elif "verbal" in q or "confirmation" in q:
            order_field = '"Verbal_Count__c"'
        elif "in market" in q:
            order_field = '"In_Market_Students_Count__c"'
        elif "submission" in q or "sub" in q:
            order_field = '"Students_Count__c"'
        sql = f'SELECT {ent["list_fields"]} FROM {ent["table"]} WHERE "Active__c" = true ORDER BY {order_field} {order_dir} NULLS LAST LIMIT {abs_n}'
    elif entity == "students":
        order_field = '"Submission_Count__c"'
        if "interview" in q:
            order_field = '"Interviews_Count__c"'
        elif "days" in q or "longest" in q:
            order_field = '"Days_in_Market_Business__c"'
        sql = f'SELECT "Name", "Student_Marketing_Status__c", "Technology__c", {order_field} FROM {ent["table"]}'
        if wheres:
            sql += where_sql
        sql += f' ORDER BY {order_field} {order_dir} NULLS LAST LIMIT {abs_n}'
    elif entity == "submissions":
        sql = (f'SELECT "BU_Name__c", COUNT(*) AS cnt FROM "Submissions__c"'
               f'{where_sql}' + (' AND' if wheres else ' WHERE') +
               f' "BU_Name__c" IS NOT NULL GROUP BY "BU_Name__c" ORDER BY cnt {order_dir} LIMIT {abs_n}')
    elif entity == "interviews":
        sql = (f'SELECT m."Name" AS "BU_Name", COUNT(*) AS cnt FROM {ent["from_clause"]}'
               f'{where_sql}' + (' AND' if wheres else ' WHERE') +
               f' m."Name" IS NOT NULL GROUP BY m."Name" ORDER BY cnt {order_dir} LIMIT {abs_n}')
    else:
        return None

    recs, _ = await _run(sql)
    if recs is None:
        return None
    answer = f"**{label_prefix} {abs_n} {ent['label']}:**"
    return _make_result(answer, sql, recs)


async def _handle_average(entity, ent, q, wheres, where_sql, needs_bu_join):
    if entity == "students" and "days" in q:
        if "by technology" in q or "technology wise" in q:
            sql = ('SELECT "Technology__c", ROUND(AVG("Days_in_Market_Business__c")) AS avg_days, COUNT(*) AS cnt '
                   'FROM "Student__c" WHERE "Student_Marketing_Status__c" = \'In Market\' AND "Technology__c" IS NOT NULL '
                   'GROUP BY "Technology__c" ORDER BY avg_days DESC')
        elif "by bu" in q or "bu wise" in q:
            sql = ('SELECT m."Name" AS "BU_Name", ROUND(AVG(s."Days_in_Market_Business__c")) AS avg_days, COUNT(*) AS cnt '
                   'FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" '
                   'WHERE s."Student_Marketing_Status__c" = \'In Market\' AND m."Name" IS NOT NULL '
                   'GROUP BY m."Name" ORDER BY avg_days DESC')
        else:
            sql = ('SELECT ROUND(AVG("Days_in_Market_Business__c")) AS avg_days FROM "Student__c" '
                   'WHERE "Student_Marketing_Status__c" = \'In Market\'')
        recs, _ = await _run(sql)
        if recs is None:
            return None
        if len(recs) == 1 and "avg_days" in recs[0]:
            answer = f"**Average days in market: {recs[0]['avg_days'] or 0}** (for In Market students)"
        else:
            answer = f"**Average days in market by {'Technology' if 'technology' in q else 'BU'}:**"
        return _make_result(answer, sql, recs)

    if entity in ("submissions", "interviews") and any(w in q for w in ["rate", "amount"]):
        if entity == "submissions":
            sql = f'SELECT ROUND(AVG("Rate__c"), 2) AS avg_rate FROM "Submissions__c"{where_sql}'
        else:
            sql = f'SELECT ROUND(AVG(i."Amount__c"), 2) AS avg_amount FROM {ENTITIES["interviews"]["from_clause"]}{where_sql}'
        recs, _ = await _run(sql)
        if recs is None:
            return None
        val = recs[0].get("avg_rate") or recs[0].get("avg_amount") or 0
        answer = f"**Average {'rate' if entity == 'submissions' else 'amount'}: {val}**"
        return _make_result(answer, sql, recs)

    return None


async def _handle_no_activity(no_type, days, bu_name, q):
    wheres = ['"Student_Marketing_Status__c" = \'In Market\'']
    needs_bu_join = False

    if no_type == "submissions":
        wheres.append(f'("Last_Submission_Date__c" < CURRENT_DATE - INTERVAL \'{days} days\' OR "Last_Submission_Date__c" IS NULL)')
    elif no_type == "interviews":
        wheres.append(f'"Id" NOT IN (SELECT "Student__c" FROM "Interviews__c" WHERE "Interview_Date1__c" >= CURRENT_DATE - INTERVAL \'{days} days\')')

    if bu_name:
        needs_bu_join = True
        wheres.append(f'm."Name" ILIKE \'%{bu_name}%\'')

    if needs_bu_join:
        sql = (f'SELECT s."Name", m."Name" AS "BU_Name", s."Technology__c", s."Days_in_Market_Business__c", s."Last_Submission_Date__c" '
               f'FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id"')
    else:
        sql = ('SELECT "Name", "Technology__c", "Days_in_Market_Business__c", "Last_Submission_Date__c" '
               'FROM "Student__c"')
    sql += _build_where(wheres) + ' ORDER BY "Name" LIMIT 2000'

    count_sql = f'SELECT COUNT(*) AS cnt FROM "Student__c"'
    if needs_bu_join:
        count_sql = f'SELECT COUNT(*) AS cnt FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id"'
    count_sql += _build_where(wheres)

    recs, _ = await _run(sql)
    cnt_recs, _ = await _run(count_sql)
    if recs is None:
        return None
    total = cnt_recs[0].get("cnt", len(recs)) if cnt_recs else len(recs)
    bu_desc = f" under {bu_name}" if bu_name else ""
    answer = f"**{total:,} in-market students** with no {no_type} in {days} days{bu_desc}."
    return _make_result(answer, sql, recs, total)


async def _handle_person_lookup(person, q, question):
    last_name = person.split()[-1]
    sql = (f'SELECT s."Name", s."Student_Marketing_Status__c", s."Technology__c", '
           f's."Marketing_Visa_Status__c", s."Days_in_Market_Business__c", s."Marketing_Email__c", '
           f's."Phone__c", s."University__c", s."Last_Submission_Date__c", s."Submission_Count__c", '
           f's."Interviews_Count__c", s."Verbal_Confirmation_Date__c", s."Marketing_Start_Date__c", '
           f'm."Name" AS "BU_Name" '
           f'FROM "Student__c" s LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" '
           f'WHERE s."Name" ILIKE \'%{last_name}%\' ORDER BY s."Name" LIMIT 50')
    recs, _ = await _run(sql)
    if recs is None or len(recs) == 0:
        # Try Employee table
        sql2 = f'SELECT "Name", "Email__c", "Deptment__c", "BU_Name__c" FROM "Employee__c" WHERE "Name" ILIKE \'%{last_name}%\' LIMIT 20'
        recs2, _ = await _run(sql2)
        if recs2 and len(recs2) > 0:
            answer = f"**{len(recs2)} employee(s)** found matching '{person}'."
            return _make_result(answer, sql2, recs2)
        return None

    if len(recs) == 1:
        r = recs[0]
        answer = (f"**{r.get('Name', 'N/A')}**\n"
                  f"- Status: {r.get('Student_Marketing_Status__c', 'N/A')}\n"
                  f"- Technology: {r.get('Technology__c', 'N/A')}\n"
                  f"- Visa: {r.get('Marketing_Visa_Status__c', 'N/A')}\n"
                  f"- BU: {r.get('BU_Name', 'N/A')}\n"
                  f"- Days in Market: {r.get('Days_in_Market_Business__c', 'N/A')}\n"
                  f"- Submissions: {r.get('Submission_Count__c', 0)}\n"
                  f"- Interviews: {r.get('Interviews_Count__c', 0)}\n"
                  f"- Email: {r.get('Marketing_Email__c', 'N/A')}\n"
                  f"- Phone: {r.get('Phone__c', 'N/A')}")
    else:
        answer = f"**{len(recs)} students** found matching '{person}'."
    return _make_result(answer, sql, recs)


async def _handle_financial(q, bu_name):
    wheres = ['"Active__c" = true']
    if bu_name:
        wheres.append(f'"Name" ILIKE \'%{bu_name}%\'')

    sql = (f'SELECT "Name", "Total_Expenses__c", "Total_Expenses_MIS__c", "Each_Placement_Cost__c", '
           f'"Students_Count__c", "In_Market_Students_Count__c", "Verbal_Count__c", '
           f'"BU_Student_With_Job_Count__c", "IN_JOB_Students_Count__c" '
           f'FROM "Manager__c"' + _build_where(wheres) +
           f' ORDER BY "Total_Expenses__c" DESC NULLS LAST LIMIT 200')
    recs, _ = await _run(sql)
    if recs is None:
        return None

    rows = []
    t_exp, t_cost = 0, 0
    for r in recs:
        exp = round(r.get("Total_Expenses__c") or 0)
        cost = round(r.get("Each_Placement_Cost__c") or 0)
        stud = r.get("Students_Count__c") or 0
        im = r.get("In_Market_Students_Count__c") or 0
        vc = r.get("Verbal_Count__c") or 0
        t_exp += exp
        t_cost += cost
        rows.append([r["Name"], f"${exp:,}", f"${cost:,}", f"{stud:,.0f}", f"{im:,.0f}", f"{vc:,.0f}"])

    totals = ["**Total**", f"**${t_exp:,}**", f"**${t_cost:,}**", "", "", ""]
    answer = _table_answer(
        "BU Expenses & Efficiency",
        ["BU Name", "Total Expenses", "Per Placement Cost", "Students", "In Market", "Verbals"],
        rows, totals,
        f"{len(recs)} active BUs, ${t_exp:,} total expenses"
    )
    return _make_result(answer, sql, recs)


async def _handle_summary():
    queries = {
        "students": 'SELECT COUNT(*) AS cnt FROM "Student__c"',
        "in_market": 'SELECT COUNT(*) AS cnt FROM "Student__c" WHERE "Student_Marketing_Status__c" = \'In Market\'',
        "pre_marketing": 'SELECT COUNT(*) AS cnt FROM "Student__c" WHERE "Student_Marketing_Status__c" = \'Pre Marketing\'',
        "verbal": 'SELECT COUNT(*) AS cnt FROM "Student__c" WHERE "Student_Marketing_Status__c" = \'Verbal Confirmation\'',
        "exit": 'SELECT COUNT(*) AS cnt FROM "Student__c" WHERE "Student_Marketing_Status__c" = \'Exit\'',
        "subs_month": 'SELECT COUNT(*) AS cnt FROM "Submissions__c" WHERE "Submission_Date__c" >= DATE_TRUNC(\'month\', CURRENT_DATE)',
        "ints_month": 'SELECT COUNT(*) AS cnt FROM "Interviews__c" WHERE "Interview_Date1__c" >= DATE_TRUNC(\'month\', CURRENT_DATE)',
        "subs_today": 'SELECT COUNT(*) AS cnt FROM "Submissions__c" WHERE "Submission_Date__c" = CURRENT_DATE',
        "ints_today": 'SELECT COUNT(*) AS cnt FROM "Interviews__c" WHERE "Interview_Date1__c" = CURRENT_DATE',
        "jobs": 'SELECT COUNT(*) AS cnt FROM "Job__c" WHERE "Active__c" = true',
        "managers": 'SELECT COUNT(*) AS cnt FROM "Manager__c" WHERE "Active__c" = true',
    }
    counts = {}
    all_sql = []
    for key, sql in queries.items():
        recs, _ = await _run(sql)
        counts[key] = recs[0].get("cnt", 0) if recs else 0
        all_sql.append(f"-- {key}\n{sql}")

    answer = (
        f"**Dashboard Summary**\n\n"
        f"**Students:** {counts['students']:,} total | {counts['in_market']:,} In Market | "
        f"{counts['pre_marketing']:,} Pre Marketing | {counts['verbal']:,} Verbal Confirmation | {counts['exit']:,} Exit\n\n"
        f"**This Month:** {counts['subs_month']:,} submissions | {counts['ints_month']:,} interviews\n\n"
        f"**Today:** {counts['subs_today']:,} submissions | {counts['ints_today']:,} interviews\n\n"
        f"**Active:** {counts['jobs']:,} jobs | {counts['managers']:,} BU managers"
    )
    return {
        "answer": answer,
        "soql": "\n".join(all_sql),
        "data": {"totalSize": 0, "records": [], "query": "\n".join(all_sql), "route": "SQL", "rag_results": 0},
    }


async def _handle_month_comparison(q):
    is_subs = any(w in q for w in ["submission", "sub"])
    is_ints = any(w in q for w in ["interview", "int"])
    if not is_subs and not is_ints:
        is_subs = True

    queries = {}
    all_sql = []
    if is_subs:
        for label, where in [("subs_this", "\"Submission_Date__c\" >= DATE_TRUNC('month', CURRENT_DATE)"),
                              ("subs_last", "\"Submission_Date__c\" >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND \"Submission_Date__c\" < DATE_TRUNC('month', CURRENT_DATE)")]:
            sql = f'SELECT COUNT(*) AS cnt FROM "Submissions__c" WHERE {where}'
            recs, _ = await _run(sql)
            queries[label] = recs[0].get("cnt", 0) if recs else 0
            all_sql.append(sql)
    if is_ints:
        for label, where in [("ints_this", "\"Interview_Date1__c\" >= DATE_TRUNC('month', CURRENT_DATE)"),
                              ("ints_last", "\"Interview_Date1__c\" >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND \"Interview_Date1__c\" < DATE_TRUNC('month', CURRENT_DATE)")]:
            sql = f'SELECT COUNT(*) AS cnt FROM "Interviews__c" WHERE {where}'
            recs, _ = await _run(sql)
            queries[label] = recs[0].get("cnt", 0) if recs else 0
            all_sql.append(sql)

    parts = []
    if is_subs:
        this_s, last_s = queries.get("subs_this", 0), queries.get("subs_last", 0)
        diff_s = this_s - last_s
        pct_s = round(diff_s / last_s * 100, 1) if last_s else 0
        sign_s = "+" if diff_s >= 0 else ""
        parts.append(f"**Submissions:** {this_s:,} this month vs {last_s:,} last month ({sign_s}{diff_s:,}, {sign_s}{pct_s}%)")
    if is_ints:
        this_i, last_i = queries.get("ints_this", 0), queries.get("ints_last", 0)
        diff_i = this_i - last_i
        pct_i = round(diff_i / last_i * 100, 1) if last_i else 0
        sign_i = "+" if diff_i >= 0 else ""
        parts.append(f"**Interviews:** {this_i:,} this month vs {last_i:,} last month ({sign_i}{diff_i:,}, {sign_i}{pct_i}%)")

    answer = "**Month-over-Month Comparison**\n\n" + "\n\n".join(parts)
    combined_sql = ";\n".join(all_sql)
    return {"answer": answer, "soql": combined_sql,
            "data": {"totalSize": 0, "records": [], "query": combined_sql, "route": "SQL", "rag_results": 0}}


async def _handle_conversion_rate(q, bu_name):
    bu_where_sub = ""
    bu_where_int = ""
    bu_where_stu = ""
    bu_desc = ""
    if bu_name:
        bu_where_sub = f' AND "BU_Name__c" ILIKE \'%{bu_name}%\''
        bu_where_int = f' AND m."Name" ILIKE \'%{bu_name}%\''
        bu_where_stu = f' AND m."Name" ILIKE \'%{bu_name}%\''
        bu_desc = f" for {bu_name}"

    time_where_sub = ""
    time_where_int = ""
    if any(w in q for w in ["this month", "current month"]):
        time_where_sub = " AND \"Submission_Date__c\" >= DATE_TRUNC('month', CURRENT_DATE)"
        time_where_int = " AND i.\"Interview_Date1__c\" >= DATE_TRUNC('month', CURRENT_DATE)"
    elif any(w in q for w in ["last month"]):
        time_where_sub = " AND \"Submission_Date__c\" >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND \"Submission_Date__c\" < DATE_TRUNC('month', CURRENT_DATE)"
        time_where_int = " AND i.\"Interview_Date1__c\" >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND i.\"Interview_Date1__c\" < DATE_TRUNC('month', CURRENT_DATE)"

    sql_subs = f'SELECT COUNT(*) AS cnt FROM "Submissions__c" WHERE 1=1{bu_where_sub}{time_where_sub}'
    sql_ints = (f'SELECT COUNT(*) AS cnt FROM "Interviews__c" i '
                f'LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" '
                f'LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" WHERE 1=1{bu_where_int}{time_where_int}')
    sql_conf = (f'SELECT COUNT(*) AS cnt FROM "Student__c" s '
                f'LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" '
                f'WHERE s."Student_Marketing_Status__c" = \'Verbal Confirmation\'{bu_where_stu}')

    subs_r, _ = await _run(sql_subs)
    ints_r, _ = await _run(sql_ints)
    conf_r, _ = await _run(sql_conf)

    subs = subs_r[0].get("cnt", 0) if subs_r else 0
    ints = ints_r[0].get("cnt", 0) if ints_r else 0
    conf = conf_r[0].get("cnt", 0) if conf_r else 0

    s2i = round(ints / subs * 100, 1) if subs else 0
    i2c = round(conf / ints * 100, 1) if ints else 0
    s2c = round(conf / subs * 100, 1) if subs else 0

    answer = (f"**Conversion Rates{bu_desc}**\n\n"
              f"- Submissions: **{subs:,}**\n"
              f"- Interviews: **{ints:,}** (Sub→Int: **{s2i}%**)\n"
              f"- Confirmations: **{conf:,}** (Int→Conf: **{i2c}%**)\n"
              f"- Overall (Sub→Conf): **{s2c}%**")
    combined_sql = f"{sql_subs};\n{sql_ints};\n{sql_conf}"
    return {"answer": answer, "soql": combined_sql,
            "data": {"totalSize": 0, "records": [], "query": combined_sql, "route": "SQL", "rag_results": 0}}


async def _handle_bu_leaderboard(q):
    sql = ('SELECT m."Name" AS "BU_Name", '
           'm."Students_Count__c", m."In_Market_Students_Count__c", '
           'm."Verbal_Count__c", m."Total_Expenses__c", m."Each_Placement_Cost__c", '
           'COALESCE(sub.cnt, 0) AS "Submissions", '
           'COALESCE(intv.cnt, 0) AS "Interviews" '
           'FROM "Manager__c" m '
           'LEFT JOIN (SELECT "BU_Name__c", COUNT(*) AS cnt FROM "Submissions__c" '
           "WHERE \"Submission_Date__c\" >= DATE_TRUNC('month', CURRENT_DATE) "
           'GROUP BY "BU_Name__c") sub ON sub."BU_Name__c" = m."Name" '
           'LEFT JOIN (SELECT m2."Name" AS bu, COUNT(*) AS cnt FROM "Interviews__c" i2 '
           'LEFT JOIN "Student__c" s2 ON i2."Student__c" = s2."Id" '
           'LEFT JOIN "Manager__c" m2 ON s2."Manager__c" = m2."Id" '
           "WHERE i2.\"Interview_Date1__c\" >= DATE_TRUNC('month', CURRENT_DATE) "
           'GROUP BY m2."Name") intv ON intv.bu = m."Name" '
           'WHERE m."Active__c" = true ORDER BY "Submissions" DESC NULLS LAST')

    recs, _ = await _run(sql)
    if recs is None:
        return None

    rows = []
    for r in recs:
        rows.append([
            r.get("BU_Name", "N/A"),
            f"{r.get('Students_Count__c') or 0:.0f}",
            f"{r.get('In_Market_Students_Count__c') or 0:.0f}",
            f"{r.get('Submissions', 0):,}",
            f"{r.get('Interviews', 0):,}",
            f"{r.get('Verbal_Count__c') or 0:.0f}",
            f"${r.get('Total_Expenses__c') or 0:,.0f}",
        ])

    answer = _table_answer(
        "BU Leaderboard (This Month)",
        ["BU Name", "Students", "In Market", "Subs", "Ints", "Verbals", "Expenses"],
        rows,
        footer=f"{len(recs)} active BUs"
    )
    return _make_result(answer, sql, recs)


async def _handle_bu_full_report(q, bu_name, time_start, time_end, time_label):
    if not time_start:
        time_start = "DATE_TRUNC('month', CURRENT_DATE)"
        time_end = "DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'"
        time_label = "this month"

    bu_filter_sub = f' AND "BU_Name__c" ILIKE \'%{bu_name}%\'' if bu_name else ""
    bu_filter_mgr = f' AND m."Name" ILIKE \'%{bu_name}%\'' if bu_name else ""

    sql_subs = (f'SELECT "BU_Name__c" AS "BU_Name", COUNT(*) AS "Submissions" '
                f'FROM "Submissions__c" WHERE "Submission_Date__c" >= {time_start} AND "Submission_Date__c" < {time_end}{bu_filter_sub} '
                f'GROUP BY "BU_Name__c" ORDER BY "Submissions" DESC')

    sql_ints = (f'SELECT m."Name" AS "BU_Name", COUNT(*) AS "Interviews", '
                f'SUM(CASE WHEN i."Final_Status__c" = \'Confirmed\' THEN 1 ELSE 0 END) AS "Confirmations", '
                f'COALESCE(SUM(i."Amount__c"), 0) AS "Interview_Amount" '
                f'FROM "Interviews__c" i '
                f'LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" '
                f'LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" '
                f'WHERE i."Interview_Date1__c" >= {time_start} AND i."Interview_Date1__c" < {time_end}{bu_filter_mgr} '
                f'AND m."Name" IS NOT NULL GROUP BY m."Name" ORDER BY "Interviews" DESC')

    subs_recs, _ = await _run(sql_subs)
    ints_recs, _ = await _run(sql_ints)
    if subs_recs is None and ints_recs is None:
        return None

    bu_data = {}
    for r in (subs_recs or []):
        bn = r.get("BU_Name", "Unknown")
        bu_data.setdefault(bn, {"Submissions": 0, "Interviews": 0, "Confirmations": 0, "Interview_Amount": 0})
        bu_data[bn]["Submissions"] = r.get("Submissions", 0)
    for r in (ints_recs or []):
        bn = r.get("BU_Name", "Unknown")
        bu_data.setdefault(bn, {"Submissions": 0, "Interviews": 0, "Confirmations": 0, "Interview_Amount": 0})
        bu_data[bn]["Interviews"] = r.get("Interviews", 0)
        bu_data[bn]["Confirmations"] = r.get("Confirmations", 0)
        bu_data[bn]["Interview_Amount"] = round(r.get("Interview_Amount") or 0)

    sorted_bus = sorted(bu_data.items(), key=lambda x: x[1]["Submissions"], reverse=True)
    rows = []
    t_sub, t_int, t_conf, t_amt = 0, 0, 0, 0
    table_recs = []
    for bn, d in sorted_bus:
        rows.append([bn, f"{d['Submissions']:,}", f"{d['Interviews']:,}",
                     f"{d['Confirmations']:,}", f"${d['Interview_Amount']:,}"])
        t_sub += d["Submissions"]
        t_int += d["Interviews"]
        t_conf += d["Confirmations"]
        t_amt += d["Interview_Amount"]
        table_recs.append({"BU_Name": bn, "Submissions": d["Submissions"], "Interviews": d["Interviews"],
                           "Confirmations": d["Confirmations"], "Interview_Amount": d["Interview_Amount"]})

    totals = ["**Total**", f"**{t_sub:,}**", f"**{t_int:,}**", f"**{t_conf:,}**", f"**${t_amt:,}**"]
    bu_desc = f" for {bu_name}" if bu_name else ""
    answer = _table_answer(
        f"Monthly BU Report ({time_label}){bu_desc}",
        ["BU Name", "Submissions", "Interviews", "Confirmations", "Int. Amount"],
        rows, totals,
        f"{len(sorted_bus)} BUs | {t_sub:,} subs | {t_int:,} ints | {t_conf:,} confirmations | ${t_amt:,} amount"
    )
    combined_sql = f"{sql_subs};\n{sql_ints}"
    return {"answer": answer, "soql": combined_sql,
            "data": {"totalSize": len(table_recs), "records": table_recs, "query": combined_sql, "route": "SQL", "rag_results": 0}}
