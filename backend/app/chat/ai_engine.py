"""
Hybrid AI Engine with Self-Learning

Every question + answer is saved. The AI uses past successful
queries as examples to write better SOQL over time.
Users can thumbs-up/down answers to train it.
"""
import json, logging, re, time
from app.config import settings
from app.salesforce.schema import schema_to_prompt, get_schema
from app.database.query import execute_query
from app.chat.rag import search as rag_search, is_indexed
from app.chat.memory import save_interaction, get_learning_examples_prompt

logger = logging.getLogger(__name__)

# ── SOQL Result Cache (avoid re-querying Salesforce for same question within 5 min)
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
            "SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE Submission_Date__c = {time} ORDER BY BU_Name__c LIMIT 2000",
            "SELECT Student__r.Name, Onsite_Manager__c, Type__c, Final_Status__c, Amount__c, Interview_Date__c FROM Interviews__c WHERE CreatedDate = {time} ORDER BY Onsite_Manager__c LIMIT 2000",
            "SELECT Name, Manager__r.Name, Technology__c, Verbal_Confirmation_Date__c FROM Student__c WHERE Student_Marketing_Status__c = 'Verbal Confirmation' AND Verbal_Confirmation_Date__c = {time} ORDER BY Manager__r.Name LIMIT 2000",
        ],
        "labels": ["Monthly Submissions", "Monthly Interviews", "Monthly Confirmations"],
    },
    {
        "keywords": ["last week sub", "last week int", "weekly sub", "weekly int", "last week submission", "last week interview"],
        "time_keywords": {},
        "default_time": "LAST_WEEK",
        "by_lead": True,
        "queries_bu": [
            "SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE CreatedDate = LAST_WEEK ORDER BY BU_Name__c LIMIT 2000",
            "SELECT Student__r.Name, Onsite_Manager__c, Type__c, Final_Status__c, Interview_Date__c FROM Interviews__c WHERE CreatedDate = LAST_WEEK ORDER BY Onsite_Manager__c LIMIT 2000",
        ],
        "queries_lead": [
            "SELECT Student_Name__c, Offshore_Manager_Name__c, BU_Name__c, Client_Name__c FROM Submissions__c WHERE CreatedDate = LAST_WEEK ORDER BY Offshore_Manager_Name__c LIMIT 2000",
            "SELECT Student__r.Name, Offshore_Manager__c, Type__c, Final_Status__c FROM Interviews__c WHERE CreatedDate = LAST_WEEK ORDER BY Offshore_Manager__c LIMIT 2000",
        ],
        "labels": ["Last Week Submissions", "Last Week Interviews"],
    },
    {
        "keywords": ["confirmation", "conformation", "congratulation", "verbal confirmation", "verbal conformation", "confirmed"],
        "time_keywords": {"last week": "LAST_WEEK", "this week": "THIS_WEEK", "this month": "THIS_MONTH", "last month": "LAST_MONTH", "yesterday": "YESTERDAY", "today": "TODAY"},
        "default_time": "LAST_WEEK",
        "queries": [
            "SELECT Name, Manager__r.Name, Technology__c, Verbal_Confirmation_Date__c, Marketing_Visa_Status__c FROM Student__c WHERE Student_Marketing_Status__c = 'Verbal Confirmation' AND Verbal_Confirmation_Date__c = {time} ORDER BY Manager__r.Name LIMIT 2000"
        ],
        "labels": ["Confirmations"],
    },
    {
        "keywords": ["pre marketing", "premarketing", "pre-marketing"],
        "time_keywords": {},
        "default_time": None,
        "queries": [
            "SELECT Name, Manager__r.Name, PreMarketingStatus__c, Resume_Preparation__c, Resume_Verified_By_Lead__c, Resume_Verified_By_Manager__c, Resume_Verification__c, Resume_Review__c, Otter_Screening__c, Otter_Final_Screening__c, Otter_Real_Time_Screeing_1__c, Otter_Real_Time_Screeing_2__c, Has_Linkedin_Created__c, Student_LinkedIn_Account_Review__c, MQ_Screening_By_Lead__c, MQ_Screening_By_Manager__c FROM Student__c WHERE Student_Marketing_Status__c = 'Pre Marketing' ORDER BY Manager__r.Name LIMIT 2000"
        ],
        "labels": ["PreMarketing Students"],
    },
    {
        "keywords": ["yesterday submission"],
        "time_keywords": {},
        "default_time": None,
        "by_lead": True,
        "queries_bu": [
            "SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c, Offshore_Manager_Name__c FROM Submissions__c WHERE Submission_Date__c = YESTERDAY ORDER BY BU_Name__c LIMIT 2000"
        ],
        "queries_lead": [
            "SELECT Student_Name__c, Offshore_Manager_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE Submission_Date__c = YESTERDAY ORDER BY Offshore_Manager_Name__c LIMIT 2000"
        ],
        "labels": ["Yesterday Submissions"],
    },
    {
        "keywords": ["3 day", "three day", "no submission", "last 3 days no"],
        "time_keywords": {},
        "default_time": None,
        "by_lead": True,
        "queries_bu": [
            "SELECT Name, Manager__r.Name, Technology__c, Last_Submission_Date__c, Days_in_Market_Business__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' AND (Last_Submission_Date__c < LAST_N_DAYS:3 OR Last_Submission_Date__c = null) ORDER BY Manager__r.Name LIMIT 2000"
        ],
        "queries_lead": [
            "SELECT Name, Offshore_Manager_Name__c, Manager__r.Name, Technology__c, Last_Submission_Date__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' AND (Last_Submission_Date__c < LAST_N_DAYS:3 OR Last_Submission_Date__c = null) ORDER BY Offshore_Manager_Name__c LIMIT 2000"
        ],
        "labels": ["Students with No Recent Submissions"],
    },
    {
        "keywords": ["mandatory field", "missing field", "interview mandatory"],
        "time_keywords": {"last week": "LAST_WEEK", "this week": "THIS_WEEK", "this month": "THIS_MONTH"},
        "default_time": "THIS_WEEK",
        "queries": [
            "SELECT Student__r.Name, Onsite_Manager__c, Type__c, Interview_Date__c, Amount__c, Bill_Rate__c, Final_Status__c FROM Interviews__c WHERE (Amount__c = null OR Bill_Rate__c = null OR Final_Status__c = null) AND CreatedDate = {time} ORDER BY Onsite_Manager__c LIMIT 2000"
        ],
        "labels": ["Interviews with Missing Fields"],
    },
    {
        "keywords": ["no interview", "2 week no interview", "two week no interview", "no int"],
        "time_keywords": {},
        "default_time": None,
        "by_lead": True,
        "queries_bu": [
            "SELECT Name, Manager__r.Name, Technology__c, Days_in_Market_Business__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' AND Id NOT IN (SELECT Student__c FROM Interviews__c WHERE CreatedDate >= LAST_N_DAYS:14) ORDER BY Manager__r.Name LIMIT 2000"
        ],
        "queries_lead": [
            "SELECT Name, Manager__r.Name, Offshore_Manager_Name__c, Technology__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' AND Id NOT IN (SELECT Student__c FROM Interviews__c WHERE CreatedDate >= LAST_N_DAYS:14) ORDER BY Offshore_Manager_Name__c LIMIT 2000"
        ],
        "labels": ["In-Market Students with No Interviews (14 days)"],
    },
    {
        "keywords": ["expense", "placement cost", "per placement"],
        "time_keywords": {},
        "default_time": None,
        "queries": [
            "SELECT Name, Total_Expenses_MIS__c, Each_Placement_Cost__c, BU_Student_With_Job_Count__c, Students_Count__c, In_Market_Students_Count__c, Verbal_Count__c, IN_JOB_Students_Count__c FROM Manager__c WHERE Active__c = true ORDER BY Name LIMIT 2000"
        ],
        "labels": ["BU Expenses & Placement Costs"],
    },
    {
        "keywords": ["payroll", "bench payroll", "job payroll"],
        "time_keywords": {},
        "default_time": None,
        "queries": [
            "SELECT Student__r.Name, Share_With__r.Name, PayRate__c, Caluculated_Pay_Rate__c, Pay_Roll_Tax__c, Profit__c, Bill_Rate__c, Payroll_Month__c, Project_Type__c, Technology__c FROM Job__c WHERE Active__c = true ORDER BY Share_With__r.Name LIMIT 2000",
            "SELECT Name, Manager__r.Name, Technology__c, Days_in_Market_Business__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' ORDER BY Manager__r.Name LIMIT 2000",
        ],
        "labels": ["Active Job Payroll", "Bench (In-Market Students)"],
    },
    {
        "keywords": ["monthly sub", "monthly int", "monthly submission", "monthly interview", "monthly confirmation", "monthly conformation", "month sub & int", "monthly sub & int"],
        "time_keywords": {"last month": "LAST_MONTH", "this month": "THIS_MONTH"},
        "default_time": "THIS_MONTH",
        "queries": [
            "SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE Submission_Date__c = {time} ORDER BY BU_Name__c LIMIT 2000",
            "SELECT Student__r.Name, Onsite_Manager__c, Type__c, Final_Status__c, Amount__c, Interview_Date__c FROM Interviews__c WHERE CreatedDate = {time} ORDER BY Onsite_Manager__c LIMIT 2000",
            "SELECT Name, Manager__r.Name, Technology__c, Verbal_Confirmation_Date__c FROM Student__c WHERE Student_Marketing_Status__c = 'Verbal Confirmation' AND Verbal_Confirmation_Date__c = {time} ORDER BY Manager__r.Name LIMIT 2000",
        ],
        "labels": ["Monthly Submissions", "Monthly Interviews", "Monthly Confirmations"],
    },
    {
        "keywords": ["total interview", "interview amount", "total amount"],
        "time_keywords": {"last month": "LAST_MONTH", "this month": "THIS_MONTH", "last week": "LAST_WEEK", "this week": "THIS_WEEK"},
        "default_time": "THIS_MONTH",
        "queries": [
            "SELECT Student__r.Name, Onsite_Manager__c, Type__c, Amount__c, Amount_INR__c, Bill_Rate__c, Final_Status__c, Interview_Date__c FROM Interviews__c WHERE CreatedDate = {time} ORDER BY Onsite_Manager__c LIMIT 2000"
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
            "SELECT Student_Name__c, BU_Name__c, Offshore_Manager_Name__c, Recruiter_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE BU_Name__c LIKE '%{name}%' AND CreatedDate = {time} ORDER BY Offshore_Manager_Name__c, Student_Name__c LIMIT 2000",
            "SELECT Student__r.Name, Onsite_Manager__c, Offshore_Manager__c, Type__c, Final_Status__c, Amount__c, Interview_Date__c FROM Interviews__c WHERE Onsite_Manager__c LIKE '%{name}%' AND CreatedDate = {time} ORDER BY Offshore_Manager__c LIMIT 2000",
            "SELECT Name, Manager__r.Name, Technology__c, Verbal_Confirmation_Date__c, Marketing_Visa_Status__c FROM Student__c WHERE Student_Marketing_Status__c = 'Verbal Confirmation' AND Manager__r.Name LIKE '%{name}%' AND Verbal_Confirmation_Date__c = {time} ORDER BY Manager__r.Name LIMIT 2000",
            "SELECT Name, Manager__r.Name, Technology__c, Days_in_Market_Business__c, Last_Submission_Date__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' AND Manager__r.Name LIKE '%{name}%' AND (Last_Submission_Date__c < LAST_N_DAYS:3 OR Last_Submission_Date__c = null) ORDER BY Manager__r.Name LIMIT 2000",
        ],
        "labels": ["Weekly Submissions", "Weekly Interviews", "Weekly Confirmations", "Students Needing Attention"],
    },
    {
        "keywords": ["performance of", "performance for", "performance report"],
        "time_keywords": {"last week": "LAST_WEEK", "this week": "THIS_WEEK", "this month": "THIS_MONTH", "last month": "LAST_MONTH"},
        "default_time": "LAST_WEEK",
        "name_filter": True,
        "queries": [
            "SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE BU_Name__c LIKE '%{name}%' AND CreatedDate = {time} ORDER BY Submission_Date__c LIMIT 2000",
            "SELECT Student__r.Name, Onsite_Manager__c, Type__c, Final_Status__c, Amount__c, Interview_Date__c FROM Interviews__c WHERE Onsite_Manager__c LIKE '%{name}%' AND CreatedDate = {time} ORDER BY Interview_Date__c LIMIT 2000",
        ],
        "labels": ["Submissions", "Interviews"],
    },
    {
        "keywords": ["student performance"],
        "time_keywords": {"last week": "LAST_WEEK", "this week": "THIS_WEEK", "this month": "THIS_MONTH", "last month": "LAST_MONTH"},
        "default_time": "LAST_WEEK",
        "by_lead": True,
        "queries_bu": [
            "SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE CreatedDate = {time} ORDER BY BU_Name__c, Student_Name__c LIMIT 2000"
        ],
        "queries_lead": [
            "SELECT Student_Name__c, Offshore_Manager_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE CreatedDate = {time} ORDER BY Offshore_Manager_Name__c, Student_Name__c LIMIT 2000"
        ],
        "labels": ["Student Performance (Submissions)"],
    },
    {
        "keywords": ["recruiter performance"],
        "time_keywords": {"last week": "LAST_WEEK", "this week": "THIS_WEEK", "this month": "THIS_MONTH", "last month": "LAST_MONTH"},
        "default_time": "LAST_WEEK",
        "by_lead": True,
        "queries_bu": [
            "SELECT Recruiter_Name__c, Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE CreatedDate = {time} ORDER BY BU_Name__c, Recruiter_Name__c LIMIT 2000"
        ],
        "queries_lead": [
            "SELECT Recruiter_Name__c, Student_Name__c, Offshore_Manager_Name__c, BU_Name__c, Submission_Date__c FROM Submissions__c WHERE CreatedDate = {time} ORDER BY Offshore_Manager_Name__c, Recruiter_Name__c LIMIT 2000"
        ],
        "labels": ["Recruiter Performance (Submissions)"],
    },
]


def _match_report_pattern(question):
    """Match question to a known report pattern. Returns list of (query, label) or None."""
    q_lower = question.lower()
    # Normalize common typos
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

        # Extract person name for name_filter patterns
        name_val = ""
        if pattern.get("name_filter"):
            import re as _re
            # Try "weekly report for BU [name]" pattern first
            name_match = _re.search(r'(?:weekly\s+(?:performance\s+)?report\s+(?:for|of)\s+(?:bu\s+)?)(.+?)(?:\s+(?:last|this|yesterday|today|of\s+last|of\s+this)|\s*$)', q_lower, _re.IGNORECASE)
            if not name_match:
                # Try "send weekly report [name]" pattern
                name_match = _re.search(r'(?:send\s+weekly\s+report\s+(?:for\s+)?(?:bu\s+)?)(.+?)(?:\s+(?:last|this|yesterday|today|of\s+last|of\s+this)|\s*$)', q_lower, _re.IGNORECASE)
            if not name_match:
                # Fall back to "performance of/for [name]" pattern
                name_match = _re.search(r'(?:performance\s+(?:of|for|report\s+(?:of|for))\s+)(.+?)(?:\s+(?:last|this|yesterday|today|of\s+last|of\s+this)|\s*$)', q_lower, _re.IGNORECASE)
            if name_match:
                name_val = name_match.group(1).strip().rstrip('.')
            if not name_val:
                continue

        for i, q_template in enumerate(queries):
            soql = q_template
            if time_val:
                soql = soql.replace("{time}", time_val)
            if name_val:
                soql = soql.replace("{name}", name_val)
            label = labels[i] if i < len(labels) else f"Query {i+1}"
            resolved.append((soql, label))

        if resolved:
            logger.info(f"Report pattern matched: {labels[0] if labels else 'unknown'} ({len(resolved)} queries)")
            return {"queries": resolved, "whatsapp": pattern.get("whatsapp_format", False), "name": name_val}

    return None

# ── Step 1: Pick the right object(s) ─────────────────────────────
OBJECT_PICKER_PROMPT = """You are a Salesforce schema expert for a staffing/consulting company.
Given a user question, decide which Salesforce object(s) to query.

Return ONLY a JSON object like: {"objects": ["Student__c"], "reason": "student data with BU via Manager__r"}
No other text.

OBJECT RELATIONSHIPS (all interconnected):
  Student__c.Manager__c -> Manager__c (BU). Use Manager__r.Name for BU name.
  Submissions__c.Student__c -> Student__c. Has BU_Name__c text field.
  Interviews__c.Student__c -> Student__c. Interviews__c.Submissions__c -> Submissions__c.
  Job__c.Student__c -> Student__c. Job__c.Share_With__c -> Manager__c (BU).
  Employee__c.Onshore_Manager__c -> Manager__c (BU). Employee__c.Cluster__c -> Cluster__c.
  BU_Performance__c.BU__c -> Manager__c (monthly BU metrics).
  Manager__c.Cluster__c -> Cluster__c. Manager__c.Organization__c -> Organization__c.

KEY RULES:
- "details of [person name]" / "who is [name]" / "find [name]" -> Student__c FIRST (most people are students), also try Employee__c and Contact
- "students under BU X" -> Student__c (use Manager__r.Name LIKE '%X%' for cross-object lookup)
- "student status" / "in market" / "exit" -> Student__c
- "submissions for BU X" -> Submissions__c (has BU_Name__c text field)
- "interviews" / "interview count" -> Interviews__c
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

# ── Step 2: Generate SOQL with focused schema ────────────────────
SOQL_PROMPT = """You are a SOQL expert for a staffing/consulting company's Salesforce CRM.
Return ONLY the SOQL query. No explanation, no markdown, no backticks.

RULES:
- Use EXACT field names from the schema. NEVER guess or invent field names.
- Check BUSINESS REPORT PATTERNS in the schema for pre-built queries. Use them when they match.
- Check FIELD-TO-QUESTION MAPPING and COMMON QUERY PATTERNS in the schema.
- Check ACTUAL PICKLIST VALUES for correct spelling in WHERE clauses.
- For person names, use LIKE '%LastName%' (search by LAST NAME for best results).
  Example: "Sai Ganesh Chinnamsetty" → WHERE Name LIKE '%Chinnamsetty%'
  If full name doesn't match, try last name only. Never use exact match (=) for names.
- Always include Name in SELECT + as many useful fields as possible for "details" queries.
- For "details of [person]" or "personal details": SELECT ALL important fields (Name, status, technology, manager, dates, phone, email, visa, etc.) — not just 3-5 fields.
- For follow-ups like "by lead" or "this month": look at PREVIOUS SOQL and modify the GROUP/ORDER/WHERE.
- Max LIMIT 2000. Only SELECT. If impossible, return: NO_SOQL
- SOQL has NO date arithmetic. Date filters: TODAY, THIS_MONTH, LAST_N_DAYS:30, etc.
- textarea fields CANNOT be in GROUP BY or WHERE =.

CROSS-OBJECT QUERIES (use __r for parent lookups):
- Student -> BU: Manager__r.Name (Student__c.Manager__c references Manager__c)
- Submission -> Student: Student__r.Name (Submissions__c.Student__c references Student__c)
- Interview -> Student: Student__r.Name. Interview -> Submission: Submissions__r.Name
- Job -> Student: Student__r.Name. Job -> BU: Share_With__r.Name
- Employee -> BU: Onshore_Manager__r.Name. Employee -> Cluster: Cluster__r.Name
- BU Performance -> BU: BU__r.Name
- Use subqueries for complex filters: WHERE Id IN (SELECT Student__c FROM Interviews__c WHERE ...)

WHEN USER ASKS "how many" or "count":
- Return actual records with Name + key fields (not just COUNT).
- The system auto-shows the total count. Only use COUNT()/GROUP BY for breakdowns.

WHEN USER ASKS ABOUT A BU (business unit):
- BU = a manager name like 'Divya Panguluri'.
- BEST: SELECT Name, Manager__r.Name, Technology__c FROM Student__c WHERE Manager__r.Name LIKE '%Divya%'
- For submission details: SELECT Student_Name__c, BU_Name__c FROM Submissions__c WHERE BU_Name__c LIKE '%Divya%'

EXAMPLES (learn from these):
Q: "how many students in market under Divya?"
A: SELECT Name, Manager__r.Name, Technology__c, Days_in_Market_Business__c, Student_Marketing_Status__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' AND Manager__r.Name LIKE '%Divya%' ORDER BY Days_in_Market_Business__c DESC LIMIT 2000

Q: "last week submissions by BU"
A: SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c, Offshore_Manager_Name__c FROM Submissions__c WHERE CreatedDate = LAST_WEEK ORDER BY BU_Name__c LIMIT 2000

Q: "students with no interviews in 2 weeks"
A: SELECT Name, Manager__r.Name, Technology__c, Days_in_Market_Business__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' AND Id NOT IN (SELECT Student__c FROM Interviews__c WHERE CreatedDate >= LAST_N_DAYS:14) ORDER BY Manager__r.Name LIMIT 2000

Q: "top BUs by submission count this month"
A: SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE Submission_Date__c = THIS_MONTH ORDER BY BU_Name__c LIMIT 2000

Q: "details of Sai Ganesh Chinnamsetty"
A: SELECT Name, Student_Marketing_Status__c, Technology__c, Manager__r.Name, Phone__c, Email__c, Marketing_Visa_Status__c, Days_in_Market_Business__c, Last_Submission_Date__c, PreMarketingStatus__c, Verbal_Confirmation_Date__c, Project_Start_Date__c FROM Student__c WHERE Name LIKE '%Chinnamsetty%' LIMIT 2000"""

ANSWER_PROMPT = """You are an elite data analyst for a staffing/consulting company. You produce executive-quality reports from Salesforce data.

IRON RULES:
- Use ONLY the data in QUERY RESULTS. NEVER fabricate, guess, or assume any data point.
- NEVER show fake names like "John Doe". Every name, number, date must come from the results.
- PRE-COMPUTED SUMMARY IS YOUR SOURCE OF TRUTH: At the end of the data you will find a "PRE-COMPUTED DATA SUMMARY" with BREAKDOWN tables. These tables contain the EXACT counts you must use. Copy the numbers directly — do NOT re-count the JSON records yourself. If the summary says JAVA | 20, your table MUST show JAVA | 20. The TOTAL RECORDS number goes in your headline.
- If 0 records or error: say so clearly, suggest a rephrased question.
- If data partially answers: give what you have, note the gap.

RESPONSE STRUCTURE (follow this exact pattern):

1. **HEADLINE** — One bold sentence directly answering the question with the key number.
   The total count MUST equal "TOTAL RECORDS" from DATA SUMMARY. NOT the number of unique groups.
   Example: if 64 records across 7 technologies → "**64 confirmations** across **7 technologies**" (NOT "7 confirmations").
   "**45 students** are currently under BU Divya Panguluri."
   "**12 submissions** were made yesterday across **4 BUs**."

2. **SUMMARY TABLE** — Always show a summary/rollup table first when data spans multiple groups:
   | BU Manager | Students | Submissions | Interviews | Confirmations |
   |:-----------|:--------:|:-----------:|:----------:|:-------------:|
   | **Divya Panguluri** | **18** | 12 | 5 | 2 |
   | Adithya Reddy | 15 | 8 | 3 | 1 |
   | **Total** | **33** | **20** | **8** | **3** |

3. **DETAIL SECTION** — If helpful, show per-group details under collapsible headers:
   ### BU: Divya Panguluri (18 students)
   | Student Name | Technology | Status | Days in Market |
   |:-------------|:-----------|:------:|:--------------:|
   | Ravi Kumar | JAVA | In Market | 45 |

4. **INSIGHTS** — 1-2 actionable observations:
   - 📊 Top performer: Divya Panguluri leads with 12 submissions
   - ⚠️ 5 students have 0 submissions in 3+ days — need attention

FORMATTING RULES:
- **Bold** all key numbers and totals. Bold the top performer in each table.
- Always add a **Total** row at the bottom of summary tables.
- Format: dates as "Jan 15, 2024", money as "$1,234", numbers with commas "1,234".
- Tables: left-align names, center-align numbers. Max 6 columns.
- Show up to 25 detail rows per group. If more, note "(showing 25 of 89)".
- Never show Salesforce IDs unless specifically asked.
- No filler phrases. No "Based on the data..." or "According to the results...".
- Use markdown headers (###) to separate BU/Lead groups in detail sections.

REPORT-SPECIFIC FORMATS:

CONFIRMATIONS:
- Celebratory tone: "🎉 **8 students** received verbal confirmations last week!"
- Table: Student | BU | Technology | Visa | Confirmation Date
- End with: "Congratulations to all confirmed students!"

PERFORMANCE BY BU/LEAD:
- Summary table: Manager | Total Subs | Total Ints | Top Student | Sub Count
- Show the #1 student per group. Bold the top overall.
- Rank groups by total count descending.

NO INTERVIEWS / NO SUBMISSIONS (attention reports):
- ⚠️ Warning tone: "**23 in-market students** have had no interviews in 14 days."
- Summary: BU | Count of Students Needing Attention
- Then per-BU detail: Student | Technology | Days in Market
- End with: "These students need immediate pipeline attention."

PRE-MARKETING:
- Checklist table per BU:
  | Student | Resume | Otter | LinkedIn | MQ Screening |
  |:--------|:------:|:-----:|:--------:|:------------:|
  | Name | ✅ Done | ⏳ Pending | ✅ Done | ❌ Not Started |
- Show completion percentage per BU.

COMBINED MONTHLY REPORTS (Subs + Ints + Confirmations):
- Executive summary table first:
  | BU | Submissions | Interviews | Confirmations | Total Amount |
- Then separate sections for each data type if detail is needed.
- Show month-over-month comparison hint if last month data exists.

EXPENSES / PLACEMENT:
- Table: BU | Expenses | Placements | Cost/Placement | Students | In Market
- Bold the most cost-efficient BU.
- Calculate and show Cost/Placement = Expenses ÷ Placements.

PAYROLL:
- Active Jobs table: BU | Active Jobs | Avg Pay Rate | Total Payroll | Profit
- Bench table: BU | Bench Students | Technologies
- Summary: Total Active Payroll vs Bench Count.

EXAMPLE — "Last week submissions by BU":
**127 submissions** were made last week across **8 BUs**.

| BU Manager | Submissions | Top Student | Their Count |
|:-----------|:-----------:|:------------|:-----------:|
| **Divya Panguluri** | **32** | Ravi Kumar | 8 |
| Adithya Reddy | 28 | Priya Sharma | 6 |
| Prabhakar Kunreddy | 22 | Amit Patel | 5 |
| **Total** | **127** | | |

### Top 5 Students Overall
| Student | BU | Submissions | Clients |
|:--------|:---|:-----------:|:--------|
| **Ravi Kumar** | Divya Panguluri | **8** | Google, Meta, Amazon |

📊 Divya Panguluri's BU leads with 25% of all submissions.

EXAMPLE — "2 weeks no interviews by BU":
⚠️ **34 in-market students** have had no interviews in the last 14 days.

| BU Manager | Students Needing Attention |
|:-----------|:-------------------------:|
| **Kiran Reddy** | **8** |
| Ravi Mandala | 6 |
| Satish Reddy | 5 |
| **Total** | **34** |

### BU: Kiran Reddy (8 students)
| Student | Technology | Days in Market |
|:--------|:-----------|:--------------:|
| Arun Sharma | JAVA | 67 |
| Priya Reddy | DE | 45 |

⚠️ 8 students have been in market 45+ days with zero interviews — escalation recommended.

INDIVIDUAL BU/MANAGER PERFORMANCE:
When asked "performance of [name]" or "performance for [name]":
- Show actual submissions and interviews from the time period, NOT BU_Performance__c aggregate metrics.
- Structure:
  **[Name]'s Performance — Last Week**

  **Submissions: [X]**
  | Student | Client | Date |

  **Interviews: [Y]**
  | Student | Type | Status | Date |

  Summary: X submissions, Y interviews

WHATSAPP / MESSAGE DRAFTING:
When user asks to "draft a message", "send to whatsapp", "write a message for":
- Write a clear, professional, concise message with SPECIFIC numbers from the data.
- Include: total submissions, total interviews, any confirmations, top-performing students by name.
- Format as a ready-to-copy message (no markdown bold — use plain text for WhatsApp).
- Keep it under 200 words. Be direct and data-driven.
- Example format:
  ---
  Hi [Name],

  Here's your team's performance summary for last week:

  Submissions: 25 (across 12 students)
  Interviews: 8
  Top students: Ravi Kumar (5 subs), Priya Sharma (4 subs)

  Students needing attention: 3 with zero submissions

  Great work on the submission volume! Let's focus on converting more interviews this week.
  ---"""

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

ROUTER_PROMPT = """Decide how to answer this Salesforce question. Return ONLY one word.
Default to SOQL unless the question is clearly about finding similar/related records.
SOQL — counts, lists, filters, sums, dates, specific records, status, reports, performance, any data question
RAG — ONLY for: "find similar", "records like", "recommend", vague pattern matching
BOTH — need exact data AND similarity search (very rare)
Return ONLY: SOQL, RAG, or BOTH"""

RAG_PROMPT = """You are an elite data analyst for a staffing/consulting company. You have:
1. QUERY RESULTS — exact numbers from Salesforce database (authoritative)
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
        r = OpenAI(api_key=settings.grok_api_key, base_url="https://api.x.ai/v1").chat.completions.create(
            model=settings.grok_model, max_tokens=max_tokens, temperature=temperature,
            messages=[{"role": "system", "content": system}] + msgs)
        return r.choices[0].message.content

    def _openai():
        from openai import OpenAI
        r = OpenAI(api_key=settings.openai_api_key).chat.completions.create(
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
        from_m = re.search(r'FROM\s+(\w+)', soql_query, re.IGNORECASE)
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
                    gq = f"SELECT {field}, COUNT(Id) cnt FROM {obj_name} {where_clause} GROUP BY {field} ORDER BY COUNT(Id) DESC LIMIT 30"
                    gr = await execute_query(gq)
                    if "error" not in gr and gr.get("records"):
                        counts = {}
                        for rec in gr["records"]:
                            val = rec.get(field)
                            cnt = rec.get("cnt", 0)
                            if val and val != "None":
                                counts[val] = cnt
                        if counts and len(counts) > 1:
                            group_by_results[field] = counts
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
    """Extract the object name from a SOQL query and list its exact fields."""
    m = re.search(r'FROM\s+(\w+)', soql, re.IGNORECASE)
    if not m:
        return ""
    obj_name = m.group(1)
    schema = get_schema()
    if obj_name not in schema:
        return ""
    fields = schema[obj_name].get("fields", [])
    field_names = [f"{f['name']} ({f['label']}, {f['type']})" for f in fields[:80]]
    return f"\nAVAILABLE FIELDS on {obj_name}:\n" + "\n".join(field_names)


def _validate_soql_fields(soql):
    """Check if the SOQL query uses valid field/object names. Returns error string or None."""
    schema = get_schema()
    if not schema:
        return None

    m = re.search(r'FROM\s+(\w+)', soql, re.IGNORECASE)
    if not m:
        return None
    obj_name = m.group(1)
    if obj_name not in schema:
        return f"Object '{obj_name}' not found. Available: {', '.join(sorted(schema.keys())[:20])}"

    valid_fields = {f['name'].lower() for f in schema[obj_name].get('fields', [])}
    valid_fields.update({'count', 'id'})

    bad_fields = []

    # Check SELECT clause
    select_m = re.search(r'SELECT\s+(.+?)\s+FROM', soql, re.IGNORECASE | re.DOTALL)
    if select_m:
        for part in select_m.group(1).split(','):
            part = part.strip()
            if not part or '(' in part:
                continue
            # Skip __r relationship traversals (e.g. Manager__r.Name, Student__r.Name)
            if '__r.' in part or '.' in part:
                continue
            field = part.strip()
            if field.lower() not in valid_fields:
                bad_fields.append(field)

    # Check WHERE clause fields
    where_m = re.search(r'WHERE\s+(.+?)(?:\s+ORDER|\s+GROUP|\s+LIMIT|\s*$)', soql, re.IGNORECASE | re.DOTALL)
    if where_m:
        where_clause = where_m.group(1)
        for field_m in re.finditer(r'([\w.]+)\s*(?:=|!=|<|>|LIKE|IN\s*\()', where_clause, re.IGNORECASE):
            field = field_m.group(1).strip()
            if field.upper() in ('AND', 'OR', 'NOT', 'NULL', 'TRUE', 'FALSE', 'TODAY',
                                  'YESTERDAY', 'THIS_MONTH', 'LAST_MONTH', 'THIS_YEAR',
                                  'LAST_N_DAYS', 'NEXT_N_DAYS'):
                continue
            # Skip __r relationship traversals
            if '__r.' in field:
                continue
            if field.split('.')[-1].lower() not in valid_fields:
                bad_fields.append(field)

    # Check GROUP BY / ORDER BY
    for clause in ('GROUP BY', 'ORDER BY'):
        clause_m = re.search(rf'{clause}\s+(.+?)(?:\s+(?:ORDER|LIMIT|HAVING|ASC|DESC)|\s*$)', soql, re.IGNORECASE)
        if clause_m:
            for part in clause_m.group(1).split(','):
                field = part.strip().split('.')[-1].strip()
                if field and field.upper() not in ('ASC', 'DESC', 'NULLS', 'FIRST', 'LAST'):
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
    """Execute multiple SOQL queries and combine results."""
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


# ── SOQL Path ────────────────────────────────────────────────────

async def _soql_path(question, schema_text, history=None, last_soql=None):
    # Check cache first (skip for follow-ups that modify previous SOQL)
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
                    return queries_str, combined_result, combined_recs

    learning = get_learning_examples_prompt(question)

    # Step 1: Pick the right object(s)
    picked_objects = await _pick_objects(question, schema_text)

    # Step 2: Build focused schema for picked objects
    if picked_objects:
        focused = _get_focused_schema(picked_objects)
        prompt = f"TARGET OBJECTS (query these):\n{focused}\n\nFULL SCHEMA CONTEXT:\n{schema_text}\n{learning}\nQuestion: {question}"
    else:
        prompt = f"Schema:\n{schema_text}\n{learning}\nQuestion: {question}"

    if history:
        ctx = "\n".join(f"{m['role']}: {m['content'][:200]}" for m in history[-4:])
        prompt = f"Conversation:\n{ctx}\n\n{prompt}"
    if last_soql:
        prompt = (
            "If the user is refining a previous query, modify the PREVIOUS SOQL below "
            "instead of writing a new one. Only modify it — don't rewrite from scratch "
            "unless the topic changed completely.\n"
            f"PREVIOUS SOQL: {last_soql}\n\n"
            + prompt
        )

    # Generate SOQL with temperature=0 for deterministic output
    q = await _call_ai(SOQL_PROMPT, prompt, 500, temperature=0)
    if not q:
        return None, None, None
    q = q.strip().replace("```soql", "").replace("```sql", "").replace("```", "").strip()
    if q == "NO_SOQL" or not q.upper().startswith("SELECT"):
        return None, None, None

    logger.info(f"SOQL: {q[:200]}")

    # Pre-validate fields before hitting Salesforce
    validation_error = _validate_soql_fields(q)
    if validation_error:
        logger.warning(f"SOQL validation: {validation_error}")
        obj_hint = _extract_object_fields_hint(q, schema_text)
        fix = await _call_ai(SOQL_PROMPT,
            f"Validation error: {validation_error}\nQuery: {q}\n{obj_hint}\n{learning}\nRewrite using ONLY valid fields listed above.",
            500, temperature=0)
        if fix:
            fix = fix.strip().replace("```soql", "").replace("```sql", "").replace("```", "").strip()
            if fix.upper().startswith("SELECT"):
                logger.info(f"SOQL fixed (validation): {fix[:200]}")
                q = fix

    result = await execute_query(q)

    # Retry 1: Fix based on Salesforce error message
    if "error" in result:
        obj_hint = _extract_object_fields_hint(q, schema_text)
        fix = await _call_ai(SOQL_PROMPT,
            f"SOQL FAILED with error:\n{result['error'][:400]}\n\nFailed query:\n{q}\n\n{obj_hint}\n\n{learning}\n\nWrite a CORRECTED query. Use ONLY fields from AVAILABLE FIELDS above. If a field doesn't exist on this object, try a different object or approach.",
            500, temperature=0)
        if fix:
            fix = fix.strip().replace("```soql", "").replace("```sql", "").replace("```", "").strip()
            if fix.upper().startswith("SELECT"):
                logger.info(f"SOQL retry 1: {fix[:200]}")
                q = fix
                result = await execute_query(q)

    # Retry 2: Completely different approach if still failing
    if "error" in result:
        fix2 = await _call_ai(SOQL_PROMPT,
            f"Two queries failed. Try a COMPLETELY DIFFERENT approach.\nQuestion: {question}\nLast error: {result['error'][:300]}\n\nSchema:\n{schema_text[:8000]}\n{learning}\n\nUse a simpler query on a different object if needed. Fetch raw records and let the answer AI handle grouping.",
            500, temperature=0)
        if fix2:
            fix2 = fix2.strip().replace("```soql", "").replace("```sql", "").replace("```", "").strip()
            if fix2.upper().startswith("SELECT"):
                logger.info(f"SOQL retry 2 (different approach): {fix2[:200]}")
                q = fix2
                result = await execute_query(q)

        if "error" in result:
            return q, result, None

    recs = result.get("records", [])
    for r in recs:
        r.pop("attributes", None)

    # If LIMIT was hit, get true total count via COUNT() query
    total_size = result.get("totalSize", len(recs))
    limit_m = re.search(r'LIMIT\s+(\d+)', q, re.IGNORECASE)
    if limit_m and total_size >= int(limit_m.group(1)):
        from_m = re.search(r'FROM\s+(\w+)', q, re.IGNORECASE)
        where_m = re.search(r'(WHERE\s+.+?)(?:\s+ORDER|\s+GROUP|\s+LIMIT|\s*$)', q, re.IGNORECASE | re.DOTALL)
        if from_m:
            count_q = f"SELECT COUNT() FROM {from_m.group(1)}"
            if where_m:
                # Remove __r traversals from WHERE for COUNT (they work but let's keep it simple)
                count_q += f" {where_m.group(1)}"
            try:
                count_result = await execute_query(count_q)
                if "error" not in count_result:
                    true_total = count_result.get("totalSize", total_size)
                    result["totalSize"] = true_total
                    result["_limited"] = True
                    logger.info(f"True count: {true_total} (LIMIT returned {total_size})")
            except Exception:
                pass

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
    return "SOQL"


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

    if route in ("SOQL", "BOTH"):
        soql_query, soql_result, soql_recs = await _soql_path(question, schema_text, conversation_history, last_soql=last_soql)

        if soql_recs is None or (soql_recs is not None and len(soql_recs) == 0):
            name_words = [w for w in question.split() if len(w) > 2 and w[0].isupper()]
            if len(name_words) >= 2:
                last_word = name_words[-1]
                fallback_q = f"SELECT Name, Student_Marketing_Status__c, Technology__c, Manager__r.Name, Phone__c, Email__c, Marketing_Visa_Status__c, Days_in_Market_Business__c FROM Student__c WHERE Name LIKE '%{last_word}%' LIMIT 50"
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
            parts.append(f"COMBINED QUERY RESULTS ({total} total records from {len(labels)} queries):\nSOQL used:\n{soql_query}")
            for label in sorted(labels):
                group = [r for r in soql_recs if r.get("_query_label") == label]
                clean = [{k: v for k, v in r.items() if k != "_query_label"} for r in group]
                parts.append(f"\n--- {label} ({len(group)} records) ---")
                parts.append(json.dumps(clean[:100], indent=2, default=str)[:20000])
        else:
            if is_limited:
                parts.append(f"QUERY RESULTS: **{total} TOTAL records** in Salesforce (showing {shown} below, but the TRUE TOTAL is {total}).\nIMPORTANT: Use {total} as the total count, NOT {shown}.\nSOQL used: {soql_query}")
            else:
                parts.append(f"QUERY RESULTS ({total} total records from Salesforce):\nSOQL used: {soql_query}")
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
        save_interaction(question, soql_query, no_data_msg, route, username=username)
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

    save_interaction(question, soql_query, answer or "", route, username=username)
    suggestions = await _generate_suggestions(question, answer or "")

    return {
        "answer": answer or "Found data but couldn't summarize.",
        "soql": soql_query,
        "route": route,
        "rag_used": rag_results is not None and len(rag_results) > 0,
        "suggestions": suggestions,
        "data": {
            "totalSize": soql_result.get("totalSize", 0) if soql_result and "error" not in soql_result else 0,
            "records": (soql_recs or [])[:200],
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

    if route in ("SOQL", "BOTH"):
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
            yield {"type": "thinking", "data": "Picking Salesforce objects"}
            soql_query, soql_result, soql_recs = await _soql_path(question, schema_text, conversation_history, last_soql=last_soql)

        if soql_query:
            yield {"type": "soql", "data": soql_query}

        if soql_recs is not None and len(soql_recs) > 0:
            yield {"type": "thinking", "data": f"Fetched {len(soql_recs)} records from Salesforce"}
        elif soql_result and "error" in soql_result:
            yield {"type": "thinking", "data": "Query error — trying fallback"}

        # Fallback: if SOQL path failed and question looks like a person name search
        if soql_recs is None or (soql_recs is not None and len(soql_recs) == 0):
            name_words = [w for w in question.split() if len(w) > 2 and w[0].isupper()]
            if len(name_words) >= 2:
                yield {"type": "thinking", "data": "Searching by name"}
                last_word = name_words[-1]
                fallback_q = f"SELECT Name, Student_Marketing_Status__c, Technology__c, Manager__r.Name, Phone__c, Email__c, Marketing_Visa_Status__c, Days_in_Market_Business__c FROM Student__c WHERE Name LIKE '%{last_word}%' LIMIT 50"
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
            "records": (soql_recs or [])[:200],
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
            parts.append(f"COMBINED QUERY RESULTS ({total} total records from {len(labels)} queries):\nSOQL used:\n{soql_query}")
            for label in sorted(labels):
                group = [r for r in soql_recs if r.get("_query_label") == label]
                clean = [{k: v for k, v in r.items() if k != "_query_label"} for r in group]
                parts.append(f"\n--- {label} ({len(group)} records) ---")
                parts.append(json.dumps(clean[:100], indent=2, default=str)[:20000])
        else:
            if is_limited:
                parts.append(f"QUERY RESULTS: **{total} TOTAL records** in Salesforce (showing {shown} below, but the TRUE TOTAL is {total}).\nIMPORTANT: Use {total} as the total count, NOT {shown}.\nSOQL used: {soql_query}")
            else:
                parts.append(f"QUERY RESULTS ({total} total records from Salesforce):\nSOQL used: {soql_query}")
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
            save_interaction(question, soql_query, no_data_msg, route, username=username)
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

    try:
        save_interaction(question, soql_query, answer, route, username=username)
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
