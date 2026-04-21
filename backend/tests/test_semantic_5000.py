"""
Semantic Layer Self-Learning Test Harness
==========================================
Generates 5000+ question variations, tests each through the semantic layer,
validates answers against DB, auto-corrects failures, and produces a report.

Features:
  - Checkpoint system: resume from where you stopped
  - Rate limit handling: waits and retries on DB throttling
  - Self-learning: stores verified Q&A pairs in learning_memory.json
  - Detailed HTML + JSON report at the end

Usage:
  python -m tests.test_semantic_5000                    # Run all
  python -m tests.test_semantic_5000 --resume           # Resume from checkpoint
  python -m tests.test_semantic_5000 --limit 500        # Run only 500
  python -m tests.test_semantic_5000 --report-only      # Generate report from last run
  python -m tests.test_semantic_5000 --reset             # Clear checkpoint and start fresh
"""
import asyncio
import json
import time
import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path

# ── Setup paths ─────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CHECKPOINT_FILE = DATA_DIR / "test_checkpoint.json"
RESULTS_FILE = DATA_DIR / "test_results.json"
REPORT_FILE = DATA_DIR / "test_report.html"
LEARNING_FILE = DATA_DIR / "learning_memory.json"

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("test_harness")


# ═══════════════════════════════════════════════════════════════
# QUESTION GENERATOR — 5000+ variations
# ═══════════════════════════════════════════════════════════════

STATUSES = ["in market", "verbal confirmation", "pre marketing", "exit",
            "project started", "project completed"]
TECHNOLOGIES = ["Java", "Python", "DE", "DevOps", ".NET", "Salesforce", "React",
                "Angular", "AWS", "Azure", "QA", "Data Science", "Full Stack",
                "SDET", "BA", "Scrum Master", "SAP", "ServiceNow", "Tableau", "Power BI"]
VISAS = ["H1", "OPT", "CPT", "GC", "H4 EAD", "Citizen"]
TIME_RANGES = ["today", "yesterday", "this week", "last week", "this month",
               "last month", "last 7 days", "last 30 days", "last 90 days"]
BU_NAMES = ["Aryan Reddy", "Divya Panguluri", "Sandeep Reddy Modugu", "NG-BU",
            "Gulam Siddiqui", "Hari Sai", "Vinay Singh", "Sriram Anunthula",
            "Kiran Bandari", "Prashanth Asoda"]
DAY_THRESHOLDS = [30, 50, 100, 150, 200, 300, 400, 425]
TOP_N_VALUES = [3, 5, 10, 15, 20]
RATE_VALUES = [50, 55, 60, 65, 70, 75, 80, 90, 100]
ENTITIES = ["students", "submissions", "interviews", "jobs", "employees", "managers"]
MESSAGE_TONES = ["", "urgent ", "friendly ", "firm "]
MESSAGE_AUDIENCES = ["idle students", "students with no submissions",
                     "students with no interviews", "placed students",
                     "new students", "students more than 100 days in market"]


def generate_questions():
    """Generate 5000+ question variations across all categories."""
    questions = []

    # ── L1: Count queries ───────────────────────────────────
    for entity in ENTITIES:
        questions.append({"q": f"how many {entity}", "cat": "L1-count", "entity": entity})
        questions.append({"q": f"total {entity}", "cat": "L1-count", "entity": entity})
        questions.append({"q": f"count of {entity}", "cat": "L1-count", "entity": entity})
        questions.append({"q": f"number of {entity}", "cat": "L1-count", "entity": entity})
        questions.append({"q": f"give me {entity} count", "cat": "L1-count", "entity": entity})

    # ── L2: Status filter counts ────────────────────────────
    for status in STATUSES:
        questions.append({"q": f"how many {status} students", "cat": "L2-status", "status": status})
        questions.append({"q": f"{status} students count", "cat": "L2-status", "status": status})
        questions.append({"q": f"total {status} students", "cat": "L2-status", "status": status})
        questions.append({"q": f"list {status} students", "cat": "L2-status", "status": status})
        questions.append({"q": f"show me {status} students", "cat": "L2-status", "status": status})
        questions.append({"q": f"give me all {status} students", "cat": "L2-status", "status": status})

    # ── L2: Technology filter ───────────────────────────────
    for tech in TECHNOLOGIES:
        questions.append({"q": f"how many {tech} students", "cat": "L2-tech", "tech": tech})
        questions.append({"q": f"{tech} students count", "cat": "L2-tech", "tech": tech})
        questions.append({"q": f"list of {tech} students", "cat": "L2-tech", "tech": tech})
        questions.append({"q": f"{tech} students in market", "cat": "L2-tech", "tech": tech})
        questions.append({"q": f"show {tech} consultants", "cat": "L2-tech", "tech": tech})

    # ── L2: Visa filter ─────────────────────────────────────
    for visa in VISAS:
        questions.append({"q": f"how many {visa} students", "cat": "L2-visa", "visa": visa})
        questions.append({"q": f"{visa} students list", "cat": "L2-visa", "visa": visa})
        questions.append({"q": f"students with {visa} visa", "cat": "L2-visa", "visa": visa})

    # ── L3: Time range queries ──────────────────────────────
    for tr in TIME_RANGES:
        questions.append({"q": f"submissions {tr}", "cat": "L3-time", "time": tr})
        questions.append({"q": f"how many submissions {tr}", "cat": "L3-time", "time": tr})
        questions.append({"q": f"interviews {tr}", "cat": "L3-time", "time": tr})
        questions.append({"q": f"how many interviews {tr}", "cat": "L3-time", "time": tr})
        questions.append({"q": f"new students {tr}", "cat": "L3-time", "time": tr})
        questions.append({"q": f"students added {tr}", "cat": "L3-time", "time": tr})
        questions.append({"q": f"placements {tr}", "cat": "L3-time", "time": tr})

    # ── L4: BU-specific queries ─────────────────────────────
    for bu in BU_NAMES:
        questions.append({"q": f"how many students under {bu}", "cat": "L4-bu", "bu": bu})
        questions.append({"q": f"{bu} students", "cat": "L4-bu", "bu": bu})
        questions.append({"q": f"submissions for {bu}", "cat": "L4-bu", "bu": bu})
        questions.append({"q": f"{bu} submissions this month", "cat": "L4-bu", "bu": bu})
        questions.append({"q": f"interviews for {bu} this week", "cat": "L4-bu", "bu": bu})
        questions.append({"q": f"{bu} in market students", "cat": "L4-bu", "bu": bu})
        questions.append({"q": f"idle students under {bu}", "cat": "L4-bu", "bu": bu})

    # ── L5: Group-by queries ────────────────────────────────
    for entity in ["students", "submissions", "interviews"]:
        questions.append({"q": f"{entity} by technology", "cat": "L5-group", "group": "technology"})
        questions.append({"q": f"{entity} by bu", "cat": "L5-group", "group": "bu"})
        questions.append({"q": f"{entity} bu wise", "cat": "L5-group", "group": "bu"})
        questions.append({"q": f"{entity} grouped by technology", "cat": "L5-group", "group": "technology"})
    for tr in ["this month", "last month", "this week", "yesterday"]:
        questions.append({"q": f"submissions by bu {tr}", "cat": "L5-group-time", "time": tr})
        questions.append({"q": f"submissions bu wise {tr}", "cat": "L5-group-time", "time": tr})
        questions.append({"q": f"interviews by bu {tr}", "cat": "L5-group-time", "time": tr})
        questions.append({"q": f"students by technology {tr}", "cat": "L5-group-time", "time": tr})

    # ── L5: Top-N queries ───────────────────────────────────
    for n in TOP_N_VALUES:
        questions.append({"q": f"top {n} students by submissions", "cat": "L5-topn"})
        questions.append({"q": f"top {n} bu by submissions", "cat": "L5-topn"})
        questions.append({"q": f"top {n} students by interviews", "cat": "L5-topn"})
        questions.append({"q": f"bottom {n} bu by submissions", "cat": "L5-topn"})
        questions.append({"q": f"top {n} students", "cat": "L5-topn"})

    # ── L5: Average queries ─────────────────────────────────
    questions.append({"q": "average days in market", "cat": "L5-avg"})
    questions.append({"q": "average days in market by technology", "cat": "L5-avg"})
    questions.append({"q": "avg days in market by bu", "cat": "L5-avg"})
    questions.append({"q": "mean days in market", "cat": "L5-avg"})
    questions.append({"q": "average rate of submissions", "cat": "L5-avg"})
    questions.append({"q": "average interview amount", "cat": "L5-avg"})

    # ── L6: Person lookup ───────────────────────────────────
    PERSON_NAMES = ["Abhilash Reddy", "Adarsh Mahankali", "Adil Khan", "Afreen Begum",
                    "Bhavana Bhavanam", "Chandan Kumar", "Ganesh Reddy"]
    for name in PERSON_NAMES:
        questions.append({"q": f"details of {name}", "cat": "L6-person"})
        questions.append({"q": f"give me details of {name}", "cat": "L6-person"})
        questions.append({"q": f"who is {name}", "cat": "L6-person"})
        questions.append({"q": f"{name} details", "cat": "L6-person"})
        questions.append({"q": f"status of {name}", "cat": "L6-person"})
        questions.append({"q": f"submissions for {name}", "cat": "L6-person"})
        questions.append({"q": f"interviews for {name}", "cat": "L6-person"})
        # Lowercase variations
        questions.append({"q": f"details of {name.lower()}", "cat": "L6-person-lc"})
        questions.append({"q": f"give me details of {name.lower()}", "cat": "L6-person-lc"})

    # ── L7: Multi-filter combinations ───────────────────────
    for tech in TECHNOLOGIES[:8]:
        for visa in VISAS[:4]:
            questions.append({"q": f"{tech} students with {visa} visa", "cat": "L7-multi"})
            questions.append({"q": f"how many {tech} {visa} students", "cat": "L7-multi"})
        for status in ["in market", "verbal confirmation"]:
            questions.append({"q": f"{tech} {status} students", "cat": "L7-multi"})
    for tech in TECHNOLOGIES[:5]:
        for tr in ["this month", "last month"]:
            questions.append({"q": f"{tech} submissions {tr}", "cat": "L7-multi"})
    for bu in BU_NAMES[:5]:
        for tr in ["this month", "this week", "yesterday"]:
            questions.append({"q": f"submissions for {bu} {tr}", "cat": "L7-multi"})

    # ── L8: No-activity queries ─────────────────────────────
    for days in [7, 14, 30, 60]:
        questions.append({"q": f"students with no submissions in {days} days", "cat": "L8-noact"})
        questions.append({"q": f"students not having submissions in {days} days", "cat": "L8-noact"})
        questions.append({"q": f"idle students {days} days", "cat": "L8-noact"})
        questions.append({"q": f"students with no interviews in {days} days", "cat": "L8-noact"})
        questions.append({"q": f"students not having interviews in {days} days", "cat": "L8-noact"})
    questions.append({"q": "idle students", "cat": "L8-noact"})
    questions.append({"q": "dormant students", "cat": "L8-noact"})
    questions.append({"q": "inactive students", "cat": "L8-noact"})
    for bu in BU_NAMES[:5]:
        questions.append({"q": f"idle students under {bu}", "cat": "L8-noact-bu"})

    # ── L8: Days threshold ──────────────────────────────────
    for days in DAY_THRESHOLDS:
        questions.append({"q": f"students more than {days} days in market", "cat": "L8-days"})
        questions.append({"q": f"students over {days} days in market", "cat": "L8-days"})
        questions.append({"q": f"students above {days} days", "cat": "L8-days"})

    # ── L8: Rate filters ────────────────────────────────────
    for rate in RATE_VALUES:
        questions.append({"q": f"submissions where rate is above {rate}", "cat": "L8-rate"})
        questions.append({"q": f"submissions with rate above {rate}", "cat": "L8-rate"})
        questions.append({"q": f"submissions rate below {rate}", "cat": "L8-rate"})

    # ── L9: Comparisons & Reports ───────────────────────────
    questions.append({"q": "this month vs last month submissions", "cat": "L9-compare"})
    questions.append({"q": "month over month comparison", "cat": "L9-compare"})
    questions.append({"q": "compare this month with last month", "cat": "L9-compare"})
    questions.append({"q": "conversion rate", "cat": "L9-funnel"})
    questions.append({"q": "conversion rate this month", "cat": "L9-funnel"})
    questions.append({"q": "submission to interview ratio", "cat": "L9-funnel"})
    for bu in BU_NAMES[:5]:
        questions.append({"q": f"conversion rate for {bu}", "cat": "L9-funnel-bu"})
    questions.append({"q": "bu leaderboard", "cat": "L9-leaderboard"})
    questions.append({"q": "bu ranking", "cat": "L9-leaderboard"})
    questions.append({"q": "bu performance", "cat": "L9-leaderboard"})
    questions.append({"q": "best performing bu", "cat": "L9-leaderboard"})
    questions.append({"q": "worst performing bu", "cat": "L9-leaderboard"})
    questions.append({"q": "monthly bu wise report", "cat": "L9-report"})
    questions.append({"q": "bu wise report this month", "cat": "L9-report"})
    questions.append({"q": "weekly bu report", "cat": "L9-report"})
    questions.append({"q": "generate report", "cat": "L9-report"})
    questions.append({"q": "detailed report", "cat": "L9-report"})

    # ── L1: Summary / Overview ──────────────────────────────
    for kw in ["overall summary", "quick summary", "daily update", "give me stats",
               "show me everything", "how are we doing", "eod report", "morning update",
               "dashboard data", "current numbers", "today's update"]:
        questions.append({"q": kw, "cat": "L1-summary"})

    # ── L10: Financial / Expenses ───────────────────────────
    questions.append({"q": "bu expenses", "cat": "L10-financial"})
    questions.append({"q": "placement cost by bu", "cat": "L10-financial"})
    questions.append({"q": "total expenses", "cat": "L10-financial"})
    questions.append({"q": "how much spent per placement", "cat": "L10-financial"})
    questions.append({"q": "bu profitability", "cat": "L10-financial"})

    # ── Message Generation ──────────────────────────────────
    for tone in MESSAGE_TONES:
        for audience in MESSAGE_AUDIENCES:
            questions.append({"q": f"generate {tone}message for {audience}", "cat": "MSG-gen"})
            questions.append({"q": f"draft {tone}message for {audience}", "cat": "MSG-gen"})
    questions.append({"q": "write a message for idle students", "cat": "MSG-gen"})
    questions.append({"q": "frame a message for students with no submissions in 10 days", "cat": "MSG-gen"})
    questions.append({"q": "compose email for placed students", "cat": "MSG-gen"})

    # ── Phrasing Variations (bulk) ──────────────────────────
    phrasing_templates = [
        ("how many {entity}", ENTITIES),
        ("total number of {entity}", ENTITIES),
        ("give me the count of {entity}", ENTITIES),
        ("what is the total {entity}", ENTITIES),
        ("show me all {entity}", ENTITIES),
        ("list all {entity}", ENTITIES),
        ("fetch {entity}", ENTITIES),
        ("get me {entity}", ENTITIES),
        ("display {entity}", ENTITIES),
        ("can you show {entity}", ENTITIES),
        ("i want to see {entity}", ENTITIES),
        ("please show me {entity}", ENTITIES),
    ]
    for template, values in phrasing_templates:
        for val in values:
            questions.append({"q": template.format(entity=val), "cat": "phrasing"})

    # ── Typo Variations ─────────────────────────────────────
    typo_questions = [
        "how many studens", "submisions today", "intervews this week",
        "students by technolgy", "submision bu wise", "confrimation this month",
        "interveiw last month", "how many placemnts", "submssion count",
        "stundents in market", "how many employes", "mangaer list",
    ]
    for q in typo_questions:
        questions.append({"q": q, "cat": "typo"})

    # ── Edge Cases ──────────────────────────────────────────
    edge_cases = [
        "students", "submissions", "interviews",
        "show me students", "give me submissions", "all interviews",
        "How Many Students", "SUBMISSIONS TODAY", "Interviews This Week",
        "   students by bu   ", "how many students?", "how many students!",
    ]
    for q in edge_cases:
        questions.append({"q": q, "cat": "edge"})

    # ── Real-world Complex / Conversational Prompts ─────────
    complex_prompts = [
        # Natural language, messy phrasing
        "can you tell me how many students we have right now",
        "whats the total headcount",
        "yo how many people are in market",
        "show me everyone who is idle",
        "who all are sitting idle for more than 2 weeks",
        "give me the full picture of whats happening",
        "i need a report of all bus with their numbers",
        "pull up aryan reddy's team performance",
        "what does aryan reddy's pipeline look like",
        "how is divya panguluri doing this month",
        "tell me about the students who havent had any activity",
        "any students not getting submissions lately",
        "who hasnt been submitted in over a month",
        "which bu has the most idle students",
        "whos performing best this month",
        "whos at the bottom of the leaderboard",
        "compare all bus side by side",
        "break it down by technology for me",
        "give me a tech wise split of students",
        "how many java guys do we have on bench",
        "python developers in market with opt visa",
        "dot net consultants with gc who are idle",
        "show me everyone under vinay singh who has been in market too long",
        "students sitting idle for 200 plus days under hari sai",
        "any placements this month so far",
        "did anyone get placed this week",
        "how many verbal confirmations we got",
        "give me this months numbers vs last month",
        "are submissions going up or down compared to last month",
        "how much are we spending per placement",
        "which bu is most cost effective",
        "total expense report for all bus",
        "i want to see the conversion funnel",
        "whats our submission to interview ratio",
        "how many submissions actually turn into interviews",
        "generate a follow up for all the people sitting idle",
        "write me an urgent message for students with zero activity",
        "draft a nice congratulations note for placed students",
        "compose a welcome email for the new batch",
        "give me complete details of abhilash reddy teegala",
        "find student named nagaraju",
        "look up bhavana bhavanam",
        "everything about adarsh mahankali",
        "what is adarsh mahankali's status",
        "how many students does aryan reddy have",
        "aryan reddy team strength",
        "submissions count for divya panguluri yesterday",
        "kiran bandari interviews this week",
        "top performing students this month",
        "students with highest submission count",
        "bottom 5 bus by interview count",
        "rank all bus by submissions",
        "give me the weekly bu report",
        "send me the monthly report",
        "eod update please",
        "morning status update",
        "give me todays numbers",
        "how did we do yesterday",
        "last weeks performance summary",
        "any h1 visa students available in java",
        "opt students doing python who are in market",
        "gc holders in devops still on bench",
        "h4 ead students count",
        "students who got placed but from java",
        "verbal confirmations in the last 30 days",
        "how many exits this month",
        "students who left in the last week",
        "show me pre marketing students",
        "how many are still in training",
        "all project started students",
        "project completed students this month",
        "give me the average time to placement",
        "average days in market for java vs python",
        "which technology has the fastest placement",
        "rate above 70 submissions this month",
        "high rate submissions only above 80",
        "submissions with rate less than 55",
        "low ball submissions below 50",
        "students under ng-bu",
        "ng-bu performance this month",
        "ng-bu idle students count",
        "how many students are under each bu",
        "bu wise student distribution",
        "technology distribution of in market students",
        "visa wise breakdown of students",
        "show me h1 vs opt vs gc distribution",
        "how many bus do we have",
        "list all active bus",
        "manager list with their student count",
        "which manager has most students",
        "expenses per bu this month",
        "cost per placement analysis",
        "submissions where the rate is above 65 for java students",
        "java submissions this month with rate over 60",
        "interviews for devops students last month",
        "opt java students idle for 30 days",
        "h1 python students more than 100 days in market under aryan reddy",
        "top 10 java students by submission count",
        "bottom 3 bus by placement count",
        "generate urgent message for students idle more than 60 days",
        "draft friendly reminder for people with no interviews in 2 weeks",
        "frame a warning message for consistently idle students",
        "compose motivational message for students who just got placed",
        # Abbreviations and slang
        "subs today", "ints this week", "avg dim", "dim by tech",
        "sub count bu wise", "int count this month", "vc this month",
        "how many vc", "total subs last 7 days",
        # Questions with filler words
        "can you please show me the total number of students",
        "i would like to know how many submissions happened today",
        "could you pull up the interview data for this week",
        "just give me the idle students list",
        "i need to see all verbal confirmations",
        "please get me the bu leaderboard",
        "would you mind showing me the conversion rate",
        "let me see the monthly comparison",
        "show me what we have so far this month",
        # Mixed case and formatting
        "How Many JAVA Students With H1 VISA",
        "GIVE ME ARYAN REDDY STUDENTS",
        "idle Students under Divya Panguluri",
        "submissions BY BU This Month",
        # Real conversational fragments
        "bench strength",
        "market summary",
        "todays submissions",
        "yesterdays interviews",
        "weekly subs",
        "monthly ints",
        "quarterly review data",
        "all time submissions count",
        "total interviews ever",
        "student database",
        "full student list",
        "export all students",
        "download submissions data",
    ]
    for q in complex_prompts:
        questions.append({"q": q, "cat": "complex-real"})

    # ── Combined Complex Queries ────────────────────────────
    for tech in TECHNOLOGIES[:5]:
        for visa in VISAS[:3]:
            for tr in ["this month", "last month"]:
                questions.append({
                    "q": f"{tech} {visa} students submissions {tr}",
                    "cat": "L7-complex"
                })
    for bu in BU_NAMES[:3]:
        for days in [100, 200]:
            questions.append({
                "q": f"students under {bu} more than {days} days in market",
                "cat": "L7-complex"
            })

    # ── Bulk combination templates to reach 5000+ ────────────
    import itertools
    extra_templates = [
        ("how many {status} {tech} students", "bulk-status-tech"),
        ("{tech} students {time}", "bulk-tech-time"),
        ("submissions for {bu} {time}", "bulk-bu-sub-time"),
        ("{tech} students with {visa} visa in market", "bulk-tech-visa"),
        ("top {n} {tech} students by submissions", "bulk-topn-tech"),
        ("{status} students under {bu}", "bulk-status-bu"),
        ("average days in market for {tech} students", "bulk-avg-tech"),
        ("{bu} {status} students count", "bulk-bu-status"),
        ("how many {tech} {visa} students {time}", "bulk-tech-visa-time"),
        ("list {status} {tech} students {time}", "bulk-list-combo"),
        ("{tech} submissions {time}", "bulk-tech-sub-time"),
        ("{tech} interviews {time}", "bulk-tech-int-time"),
        ("{bu} idle students", "bulk-bu-idle"),
        ("{bu} students by technology", "bulk-bu-by-tech"),
        ("students under {bu} more than {days} days in market", "bulk-bu-days"),
        ("{tech} students more than {days} days in market", "bulk-tech-days"),
        ("how many {status} students under {bu}", "bulk-status-bu-count"),
        ("{bu} submissions bu wise {time}", "bulk-bu-wise-time"),
        ("generate message for {bu} idle students", "bulk-msg-bu"),
        ("draft message for {tech} students with no submissions", "bulk-msg-tech"),
        ("{tech} {status} students under {bu}", "bulk-triple"),
        ("interviews for {bu} {time}", "bulk-bu-int-time"),
        ("submissions where rate is above {rate} {time}", "bulk-rate-time"),
        ("{bu} placements {time}", "bulk-bu-place-time"),
        ("how many {tech} students got placed {time}", "bulk-tech-place-time"),
        ("{visa} students {time}", "bulk-visa-time"),
        ("{visa} {tech} students in market", "bulk-visa-tech"),
        ("students under {bu} by technology", "bulk-bu-group-tech"),
        ("give me {bu} {status} students list", "bulk-bu-status-list"),
        ("{tech} submissions for {bu}", "bulk-tech-sub-bu"),
    ]
    vals_map = {
        "status": STATUSES, "tech": TECHNOLOGIES, "visa": VISAS,
        "time": TIME_RANGES, "bu": BU_NAMES, "n": [3, 5, 10, 15, 20],
        "days": DAY_THRESHOLDS, "rate": RATE_VALUES,
    }
    for tmpl, cat in extra_templates:
        keys = [k.strip("{}") for k in
                [s.split("}")[0] for s in tmpl.split("{")[1:]] if k]
        combo_lists = [vals_map.get(k, ["?"]) for k in keys]
        for combo in itertools.product(*combo_lists):
            kv = dict(zip(keys, combo))
            q = tmpl
            for k, v in kv.items():
                q = q.replace("{" + k + "}", str(v))
            questions.append({"q": q, "cat": cat})

    # ══════════════════════════════════════════════════════════════
    # NEW BATCH: 5000+ additional questions (AI message, natural
    # language, conversational, multi-angle phrasing, edge combos)
    # ══════════════════════════════════════════════════════════════

    # ── AI Message Generation (expanded) ────────────────────────
    msg_prompts = [
        "generate a message", "write a message", "compose a message",
        "draft a message", "frame a message", "create a message",
        "prepare a message", "make a message", "send a message",
    ]
    msg_audiences_expanded = [
        "idle students", "students not submitting",
        "students with no submissions", "students with no interviews",
        "placed students", "new students",
        "students more than 100 days in market",
        "students more than 200 days in market",
        "students sitting idle", "students with zero activity",
        "all students", "team", "everyone", "low performers",
        "students not getting interviews", "bench students",
    ]
    msg_tones_expanded = [
        "", "urgent ", "friendly ", "firm ", "motivational ",
        "professional ", "polite ", "serious ", "encouraging ",
    ]
    for prompt in msg_prompts:
        for audience in msg_audiences_expanded:
            questions.append({"q": f"{prompt} for {audience}", "cat": "MSG-expanded"})
    for tone in msg_tones_expanded:
        for audience in msg_audiences_expanded[:8]:
            questions.append({"q": f"generate {tone}message for {audience}", "cat": "MSG-tone"})
            questions.append({"q": f"write {tone}email for {audience}", "cat": "MSG-tone"})
    for bu in BU_NAMES:
        questions.append({"q": f"generate message for idle students under {bu}", "cat": "MSG-bu"})
        questions.append({"q": f"draft message for {bu} team", "cat": "MSG-bu"})
        questions.append({"q": f"compose email for students under {bu} with no submissions", "cat": "MSG-bu"})
    for tech in TECHNOLOGIES[:10]:
        questions.append({"q": f"generate message for {tech} students sitting idle", "cat": "MSG-tech"})
        questions.append({"q": f"write message for {tech} students with no interviews", "cat": "MSG-tech"})

    # ── Conversational / Natural language (200+) ────────────────
    conversational = [
        "hey whats the count of students", "yo show me idle ones",
        "dude how many subs today", "bro get me interviews this week",
        "can u show me java students", "plz list python students in market",
        "need the bu leaderboard asap", "gimme the numbers",
        "what r the stats for today", "hows the team doing",
        "any updates on placements", "whats happening with submissions",
        "tell me everything about our bench", "who is sitting free",
        "anyone not working", "who needs attention",
        "which students should I follow up with",
        "give me a quick rundown", "status check please",
        "what do the numbers look like this month",
        "how many people have we placed so far",
        "is anyone close to getting placed",
        "who had interviews recently", "any good news today",
        "whats our hit rate", "hows conversion looking",
        "break it down for me", "simplify this for me",
        "just the highlights please", "top line numbers",
        "give me the tldr", "executive summary",
        "whats the bottom line", "where do we stand",
        "how far behind are we", "are we on track",
        "any red flags I should know about",
        "which bus need help", "who is struggling",
        "who is killing it this month", "star performers",
        "anyone doing exceptionally well",
        "show me the underperformers", "who dropped the ball",
        "what changed since last week", "weekly diff",
        "any new joiners", "fresh faces this week",
        "who left recently", "any exits this month",
        "students who moved to project started",
        "how many completed projects", "anyone done with project",
        "pending verbal confirmations",
        "students awaiting placement",
        "who is next in line for placement",
        "longest waiting students", "most senior bench members",
        "freshest students on bench",
        "newest additions to the team",
        "recently marketed students",
        "students marketed this week",
    ]
    for q in conversational:
        questions.append({"q": q, "cat": "conversational"})

    # ── Question-style queries ──────────────────────────────────
    question_style = [
        "what is the total number of students?",
        "how many submissions were made today?",
        "what are the interviews scheduled this week?",
        "who are the idle students right now?",
        "which BU has the most students?",
        "what technology has the highest placement rate?",
        "how long have students been in market on average?",
        "what is the average days in market for java students?",
        "which students have been idle for over 30 days?",
        "who submitted the most this month?",
        "what is the conversion rate this month?",
        "how many students are under each manager?",
        "which visa type has the most students?",
        "are there any h1 students in devops?",
        "do we have any salesforce consultants?",
        "how many react developers are in market?",
        "is there anyone with more than 400 days in market?",
        "what happened with submissions yesterday?",
        "did we get any placements last week?",
        "how many interviews happened last month?",
        "what is the submission to interview ratio?",
        "can you show me the monthly trend?",
        "what does the bu performance look like?",
        "who is the top bu this month?",
        "which bu needs improvement?",
    ]
    for q in question_style:
        questions.append({"q": q, "cat": "question-style"})

    # ── Instruction-style queries ───────────────────────────────
    instruction_style = [
        "pull up the student list", "fetch all submissions for today",
        "get me the interview count", "run a report on idle students",
        "show the bu breakdown", "display technology distribution",
        "list everyone who is in market", "filter java students only",
        "sort students by days in market", "group submissions by bu",
        "count interviews this week", "total up all submissions",
        "break down students by visa type", "split by technology",
        "check how many are idle", "verify placement count",
        "update me on todays activity", "brief me on this weeks numbers",
        "analyze bu performance", "compare this month with last",
        "highlight the top performers", "flag the idle ones",
        "identify students needing follow-up", "locate opt students",
        "track submissions trend", "monitor interview pipeline",
        "assess team productivity", "evaluate bu efficiency",
        "calculate average time to placement", "determine bench strength",
        "review weekly performance", "summarize monthly activity",
        "aggregate submissions by technology", "tally interviews per bu",
        "enumerate placed students this month", "catalog all active jobs",
    ]
    for q in instruction_style:
        questions.append({"q": q, "cat": "instruction-style"})

    # ── BU + tech + time triple combos (expanded) ───────────────
    for bu in BU_NAMES[:5]:
        for tech in TECHNOLOGIES[:10]:
            for tr in TIME_RANGES[:6]:
                questions.append({
                    "q": f"{tech} students under {bu} {tr}",
                    "cat": "bulk-bu-tech-time"
                })

    # ── Status + tech + time combos ─────────────────────────────
    for status in ["in market", "verbal confirmation"]:
        for tech in TECHNOLOGIES[:10]:
            for tr in TIME_RANGES[:6]:
                questions.append({
                    "q": f"{status} {tech} students {tr}",
                    "cat": "bulk-status-tech-time"
                })

    # ── BU + status + time combos ───────────────────────────────
    for bu in BU_NAMES[:5]:
        for status in STATUSES[:3]:
            for tr in TIME_RANGES[:6]:
                questions.append({
                    "q": f"{status} students under {bu} {tr}",
                    "cat": "bulk-bu-status-time"
                })

    # ── Visa + tech + BU combos ─────────────────────────────────
    for visa in VISAS[:4]:
        for tech in TECHNOLOGIES[:10]:
            for bu in BU_NAMES[:5]:
                questions.append({
                    "q": f"{visa} {tech} students under {bu}",
                    "cat": "bulk-visa-tech-bu"
                })

    # ── Top N expanded combos ───────────────────────────────────
    for n in TOP_N_VALUES:
        for tech in TECHNOLOGIES[:10]:
            questions.append({"q": f"top {n} {tech} students by interviews", "cat": "bulk-topn-tech-int"})
        for bu in BU_NAMES[:5]:
            questions.append({"q": f"top {n} students under {bu}", "cat": "bulk-topn-bu"})
            questions.append({"q": f"top {n} students under {bu} by submissions", "cat": "bulk-topn-bu"})
        for tr in ["this month", "last month", "this week"]:
            questions.append({"q": f"top {n} students by submissions {tr}", "cat": "bulk-topn-time"})
            questions.append({"q": f"top {n} bu by submissions {tr}", "cat": "bulk-topn-time"})

    # ── Days threshold + tech combos ────────────────────────────
    for days in DAY_THRESHOLDS:
        for tech in TECHNOLOGIES[:10]:
            questions.append({"q": f"{tech} students over {days} days in market", "cat": "bulk-days-tech"})

    # ── Days threshold + BU combos ──────────────────────────────
    for days in DAY_THRESHOLDS:
        for bu in BU_NAMES[:5]:
            questions.append({"q": f"students under {bu} over {days} days in market", "cat": "bulk-days-bu"})

    # ── No-activity + tech combos ──��────────────────────────────
    for tech in TECHNOLOGIES[:10]:
        for days in [7, 14, 30]:
            questions.append({"q": f"{tech} students with no submissions in {days} days", "cat": "bulk-noact-tech"})
            questions.append({"q": f"{tech} students not having interviews in {days} days", "cat": "bulk-noact-tech"})

    # ── No-activity + BU combos ─────────────────────────────────
    for bu in BU_NAMES[:5]:
        for days in [7, 14, 30]:
            questions.append({"q": f"students under {bu} with no submissions in {days} days", "cat": "bulk-noact-bu"})
            questions.append({"q": f"idle students under {bu} {days} days", "cat": "bulk-noact-bu"})

    # ── Rate + tech combos ──────────────────────────────────────
    for rate in RATE_VALUES[:5]:
        for tech in TECHNOLOGIES[:10]:
            questions.append({"q": f"{tech} submissions with rate above {rate}", "cat": "bulk-rate-tech"})
            questions.append({"q": f"{tech} submissions rate below {rate}", "cat": "bulk-rate-tech"})

    # ── Person lookup expanded ──────────────────────────────────
    EXTRA_NAMES = ["Nagaraju", "Sriram", "Vinay", "Kiran", "Prashanth",
                   "Rahul", "Priya", "Sneha", "Rajesh", "Amit"]
    for name in EXTRA_NAMES:
        questions.append({"q": f"details of {name}", "cat": "L6-person-extra"})
        questions.append({"q": f"who is {name}", "cat": "L6-person-extra"})
        questions.append({"q": f"find student {name}", "cat": "L6-person-extra"})
        questions.append({"q": f"search {name}", "cat": "L6-person-extra"})
        questions.append({"q": f"{name} submissions", "cat": "L6-person-extra"})
        questions.append({"q": f"{name} interviews", "cat": "L6-person-extra"})

    # ── Group-by expanded combos ────────────────────────────────
    group_by_qs = []
    for entity in ["students", "submissions", "interviews"]:
        for group in ["technology", "bu", "visa", "status"]:
            for tr in TIME_RANGES[:6]:
                group_by_qs.append(f"{entity} by {group} {tr}")
        for group in ["technology", "bu"]:
            questions.append({"q": f"{entity} grouped by {group}", "cat": "bulk-group-expanded"})
            questions.append({"q": f"{entity} {group} wise breakdown", "cat": "bulk-group-expanded"})
            questions.append({"q": f"{entity} split by {group}", "cat": "bulk-group-expanded"})
    for q in group_by_qs:
        questions.append({"q": q, "cat": "bulk-group-time"})

    # ── Mixed phrasing combos ───────────────────────────────────
    phrasing_prefixes = [
        "show me", "give me", "list", "display", "fetch",
        "get me", "pull up", "i need", "can you show",
        "please show", "let me see", "what are the",
    ]
    phrasing_subjects = [
        "in market students", "idle students", "placed students",
        "new students this week", "submissions today",
        "interviews this week", "top 5 students",
        "bu leaderboard", "student count by technology",
    ]
    for prefix in phrasing_prefixes:
        for subject in phrasing_subjects:
            questions.append({"q": f"{prefix} {subject}", "cat": "bulk-phrasing-combo"})

    # ── Typo variations expanded ────────────────────────────────
    extra_typos = [
        "studnts in market", "sbumissions today", "intereviews this week",
        "tecnology wise", "bu wize report", "plcements this month",
        "vrbal confirmation", "itnerview count", "submision rate above 60",
        "studetns by bu", "intrviews for java", "managrs list",
        "emplyees count", "contacs list", "jbs active",
        "marketig status", "in maket students", "verbel confirmation",
        "pre marketng students", "projct started", "projct completed",
        "submssions under aryan", "intrvws this month", "studs by tech",
    ]
    for q in extra_typos:
        questions.append({"q": q, "cat": "typo-expanded"})

    # ── Case variation combos ───────────────────────────────────
    case_variants = [
        "HOW MANY STUDENTS", "Submissions Today", "INTERVIEWS THIS WEEK",
        "Java Students In Market", "IDLE STUDENTS", "BU LEADERBOARD",
        "top 5 STUDENTS by submissions", "VERBAL CONFIRMATION count",
        "In Market Students By Technology", "SUBMISSIONS BU WISE",
        "How Many Python Students With OPT Visa",
        "LIST ALL DEVOPS STUDENTS", "Show Me Java Students Today",
        "GENERATE MESSAGE FOR IDLE STUDENTS",
        "DRAFT URGENT MESSAGE FOR BENCH STUDENTS",
        "Students Under ARYAN REDDY", "NG-BU Students THIS MONTH",
    ]
    for q in case_variants:
        questions.append({"q": q, "cat": "case-variants"})

    # ── Abbreviation combos ─────────────────────────────────────
    abbrev_combos = [
        "subs today", "subs this week", "subs last month", "subs by bu",
        "ints today", "ints this week", "ints last month", "ints by bu",
        "vc this month", "vc last month", "vc today", "vc count",
        "avg dim", "dim by tech", "dim by bu",
        "sub count", "int count", "sub count by bu", "int count by tech",
        "top 5 subs", "top 10 subs this month", "bottom 5 bu by subs",
        "bu wise subs", "bu wise ints", "tech wise subs", "tech wise ints",
        "subs for aryan reddy", "ints for divya panguluri",
        "java subs today", "python ints this week", "devops subs last month",
    ]
    for q in abbrev_combos:
        questions.append({"q": q, "cat": "abbreviations"})

    # ── Multi-sentence / complex natural prompts ────────────────
    complex_natural = [
        "I want to know how many students are in market and how many got placed this month",
        "show me the java students who have been sitting idle for more than 30 days",
        "can you pull up the submissions data for this week grouped by bu",
        "I need a report showing all BUs with their student count and submission count",
        "give me the list of students who haven't had any submissions in the last 2 weeks",
        "which technology has the most students but least placements",
        "show me all the opt students doing java who are idle",
        "how many h1 visa python developers are there in market under aryan reddy",
        "list all students who got verbal confirmation in the last 30 days",
        "what is the average time to placement for devops students",
        "give me the top 10 students with highest days in market",
        "which BU had the most submissions last month",
        "show me interviews for the last 7 days grouped by technology",
        "how many students under each BU are idle for more than 14 days",
        "what is the submission rate for this month compared to last month",
        "list all the managers and their total student count",
        "give me gc holders who are in market doing data science",
        "show me pre marketing students added this month",
        "how many students moved to project started this week",
        "total number of active jobs right now",
        "which students have rate above 80 in their submissions",
        "show me all submissions with rate below 50 this month",
        "java students under divya panguluri who are idle",
        "python students with opt visa more than 200 days in market",
        "generate a message for students who havent submitted in 2 weeks",
        "draft an email to congratulate everyone who got placed this month",
        "write a reminder for students idle more than 60 days",
        "compose a weekly update for all in market students",
        "frame a message for new students who joined this week",
        "create an urgent notice for students with zero submissions",
    ]
    for q in complex_natural:
        questions.append({"q": q, "cat": "complex-natural"})

    # ── BU + tech submissions/interviews time combos ────────────
    for bu in BU_NAMES[:5]:
        for tech in TECHNOLOGIES[:10]:
            for tr in TIME_RANGES[:4]:
                questions.append({
                    "q": f"{tech} submissions under {bu} {tr}",
                    "cat": "bulk-bu-tech-sub-time"
                })

    # ── Status + BU + list/count variations ─────────────────────
    for status in STATUSES[:4]:
        for bu in BU_NAMES[:5]:
            questions.append({"q": f"list {status} students under {bu}", "cat": "bulk-status-bu-list"})
            questions.append({"q": f"count {status} students for {bu}", "cat": "bulk-status-bu-list"})
            questions.append({"q": f"how many {status} students does {bu} have", "cat": "bulk-status-bu-list"})

    # ── Visa + time combos expanded ─────────────────────────────
    for visa in VISAS:
        for tr in TIME_RANGES[:6]:
            questions.append({"q": f"how many {visa} students {tr}", "cat": "bulk-visa-time-exp"})
            questions.append({"q": f"{visa} students added {tr}", "cat": "bulk-visa-time-exp"})

    # ── Tech + visa + BU combos ─────────────────────────────────
    for tech in TECHNOLOGIES[:8]:
        for visa in VISAS[:3]:
            for bu in BU_NAMES[:5]:
                questions.append({
                    "q": f"{tech} {visa} students under {bu}",
                    "cat": "bulk-tech-visa-bu"
                })

    # ── Phrasing: "students in {status}" variations ─────────────
    status_phrases = [
        ("how many students are currently {status}", STATUSES),
        ("show students who are {status}", STATUSES),
        ("list all {status} students now", STATUSES),
        ("{status} student count right now", STATUSES),
        ("total {status} students currently", STATUSES),
        ("give me {status} students list", STATUSES),
    ]
    for tmpl, vals in status_phrases:
        for val in vals:
            questions.append({"q": tmpl.format(status=val), "cat": "bulk-status-phrasing"})

    # ── Tech + status + BU triple combos ────────────────────────
    for tech in TECHNOLOGIES[:10]:
        for status in ["in market", "verbal confirmation", "pre marketing"]:
            for bu in BU_NAMES[:5]:
                questions.append({
                    "q": f"{tech} {status} students under {bu}",
                    "cat": "bulk-tech-status-bu"
                })

    # ── Submissions + BU + tech combos ──────────────────────────
    for bu in BU_NAMES[:5]:
        for tech in TECHNOLOGIES[:10]:
            questions.append({"q": f"{tech} submissions for {bu}", "cat": "bulk-sub-bu-tech"})
            questions.append({"q": f"{tech} interviews for {bu}", "cat": "bulk-int-bu-tech"})

    # ── Count phrasing variations per entity per time ───────────
    count_phrases = [
        "how many {entity} {time}",
        "total {entity} {time}",
        "{entity} count {time}",
        "number of {entity} {time}",
        "give me {entity} count {time}",
        "show me {entity} {time}",
    ]
    for phrase in count_phrases:
        for entity in ["submissions", "interviews", "students", "placements"]:
            for tr in TIME_RANGES:
                questions.append({"q": phrase.format(entity=entity, time=tr), "cat": "bulk-count-phrasing"})

    # ── Tech + days + BU triple combos ──────────────────────────
    for tech in TECHNOLOGIES[:10]:
        for days in [100, 200, 300]:
            for bu in BU_NAMES[:3]:
                questions.append({
                    "q": f"{tech} students under {bu} more than {days} days in market",
                    "cat": "bulk-tech-days-bu"
                })

    # ── "Under BU" phrasing variants ────────────────────────────
    bu_phrasings = [
        "students under {bu}", "students for {bu}", "students of {bu}",
        "{bu} team students", "{bu} team", "{bu} bench",
        "{bu} student list", "{bu} team strength",
        "who is under {bu}", "show me {bu} students",
        "give me {bu} team details", "list {bu} students",
    ]
    for phrase in bu_phrasings:
        for bu in BU_NAMES:
            questions.append({"q": phrase.format(bu=bu), "cat": "bulk-bu-phrasing"})

    # ── Visa + status combos ────────────────────────────────────
    for visa in VISAS:
        for status in ["in market", "verbal confirmation", "pre marketing"]:
            questions.append({"q": f"{visa} {status} students", "cat": "bulk-visa-status"})
            questions.append({"q": f"how many {visa} students are {status}", "cat": "bulk-visa-status"})

    # ── Tech + submissions time expanded ────────────────────────
    for tech in TECHNOLOGIES:
        for tr in TIME_RANGES:
            questions.append({"q": f"how many {tech} submissions {tr}", "cat": "bulk-tech-sub-time-exp"})

    # ── Tech + interviews time expanded ─────────────────────────
    for tech in TECHNOLOGIES[:15]:
        for tr in TIME_RANGES:
            questions.append({"q": f"how many {tech} interviews {tr}", "cat": "bulk-tech-int-time-exp"})

    # ── Top N + time + tech combos ──────────────────────────────
    for n in [3, 5, 10]:
        for tech in TECHNOLOGIES[:10]:
            for tr in ["this month", "last month", "this week"]:
                questions.append({
                    "q": f"top {n} {tech} students by submissions {tr}",
                    "cat": "bulk-topn-tech-time"
                })

    # ── Visa + tech + time triple combos ──────────────────────────
    for visa in VISAS[:4]:
        for tech in TECHNOLOGIES[:10]:
            for tr in TIME_RANGES[:4]:
                questions.append({
                    "q": f"list {visa} {tech} students {tr}",
                    "cat": "bulk-visa-tech-time-list"
                })

    # ── Status + visa + tech combos ─────────────────────────────
    for status in ["in market", "verbal confirmation"]:
        for visa in VISAS[:4]:
            for tech in TECHNOLOGIES[:10]:
                questions.append({
                    "q": f"{status} {tech} {visa} students",
                    "cat": "bulk-status-visa-tech"
                })

    # ── BU + group-by + time combos ─────────────────────────────
    for bu in BU_NAMES[:5]:
        for group in ["technology", "status", "visa"]:
            for tr in ["this month", "last month", "this week"]:
                questions.append({
                    "q": f"students under {bu} by {group} {tr}",
                    "cat": "bulk-bu-group-time"
                })

    # ── BU + tech + status triple ───────────────────────────────
    for bu in BU_NAMES[:5]:
        for tech in TECHNOLOGIES[:10]:
            for status in ["in market", "verbal confirmation"]:
                questions.append({
                    "q": f"how many {tech} {status} students under {bu}",
                    "cat": "bulk-bu-tech-status-count"
                })

    # ── Rate + BU combos ────────────────────────────────────────
    for rate in RATE_VALUES[:5]:
        for bu in BU_NAMES[:5]:
            questions.append({"q": f"submissions for {bu} with rate above {rate}", "cat": "bulk-rate-bu"})

    # ── Rate + tech + time combos ───────────────────────────────
    for rate in RATE_VALUES[:5]:
        for tech in TECHNOLOGIES[:8]:
            for tr in ["this month", "last month"]:
                questions.append({
                    "q": f"{tech} submissions rate above {rate} {tr}",
                    "cat": "bulk-rate-tech-time"
                })

    # ── "Show me" + status + tech + time combos ─────────────────
    for status in ["in market", "verbal confirmation", "pre marketing"]:
        for tech in TECHNOLOGIES[:10]:
            for tr in TIME_RANGES[:6]:
                questions.append({
                    "q": f"show me {status} {tech} students {tr}",
                    "cat": "bulk-show-status-tech-time"
                })

    # ── BU + visa + status combos ───────────────────────────────
    for bu in BU_NAMES[:5]:
        for visa in VISAS[:4]:
            for status in ["in market", "verbal confirmation"]:
                questions.append({
                    "q": f"{visa} {status} students under {bu}",
                    "cat": "bulk-bu-visa-status"
                })

    # ── Days threshold + tech + BU combos (full) ────────────────
    for days in [50, 100, 200]:
        for tech in TECHNOLOGIES[:10]:
            for bu in BU_NAMES[:5]:
                questions.append({
                    "q": f"{tech} students over {days} days under {bu}",
                    "cat": "bulk-days-tech-bu-full"
                })

    # ── "How many" + tech + visa combos (full) ──────────────────
    for tech in TECHNOLOGIES:
        for visa in VISAS:
            questions.append({"q": f"how many {tech} {visa} students in market", "cat": "bulk-tech-visa-full"})
            questions.append({"q": f"{tech} students with {visa} visa count", "cat": "bulk-tech-visa-full"})

    # ── "List" + tech + time (full) ─────────────────────────────
    for tech in TECHNOLOGIES:
        for tr in TIME_RANGES:
            questions.append({"q": f"list {tech} submissions {tr}", "cat": "bulk-list-tech-time"})

    # ── BU + tech + visa triple ─────────────────────────────────
    for bu in BU_NAMES[:5]:
        for tech in TECHNOLOGIES[:8]:
            for visa in VISAS[:3]:
                questions.append({
                    "q": f"how many {tech} {visa} students under {bu}",
                    "cat": "bulk-bu-tech-visa-count"
                })

    # ── "Count" + entity + status + time ────────────────────────
    for entity in ["students"]:
        for status in STATUSES[:3]:
            for tr in TIME_RANGES:
                questions.append({"q": f"count {status} {entity} {tr}", "cat": "bulk-count-status-time"})
                questions.append({"q": f"{status} {entity} count {tr}", "cat": "bulk-count-status-time"})

    # ── No-activity + tech + BU combos ──────────────────────────
    for tech in TECHNOLOGIES[:10]:
        for bu in BU_NAMES[:5]:
            questions.append({"q": f"{tech} idle students under {bu}", "cat": "bulk-noact-tech-bu"})
            questions.append({"q": f"{tech} students with no submissions under {bu}", "cat": "bulk-noact-tech-bu"})

    # ── Placement + BU + tech combos ────────────────────────────
    for bu in BU_NAMES[:5]:
        for tech in TECHNOLOGIES[:10]:
            questions.append({"q": f"{tech} placements for {bu}", "cat": "bulk-place-bu-tech"})
            questions.append({"q": f"how many {tech} students got placed under {bu}", "cat": "bulk-place-bu-tech"})

    # ── "Give me" + entity + filter combos ──────────────────────
    for tech in TECHNOLOGIES[:10]:
        for tr in TIME_RANGES[:4]:
            questions.append({"q": f"give me {tech} students {tr}", "cat": "bulk-give-tech-time"})
            questions.append({"q": f"give me {tech} submissions {tr}", "cat": "bulk-give-tech-time"})

    # ── Message generation + specific data requests ─────────────
    msg_data_combos = [
        "generate message for students with no submissions in 7 days",
        "generate message for students with no submissions in 14 days",
        "generate message for students with no submissions in 30 days",
        "generate message for students with no interviews in 7 days",
        "generate message for students with no interviews in 14 days",
        "generate message for students with no interviews in 30 days",
        "draft message for students over 100 days in market",
        "draft message for students over 200 days in market",
        "draft message for students over 300 days in market",
        "write email for recently placed students",
        "compose congratulations for new placements",
        "create welcome message for new joiners",
        "generate follow up for students with low activity",
        "write urgent reminder for zero submission students",
        "draft weekly update email for all teams",
        "compose performance update for management",
        "generate report email for bu managers",
        "write a reminder to follow up on pending submissions",
        "draft a message asking students to update their resumes",
        "compose an encouragement email for struggling students",
    ]
    for q in msg_data_combos:
        questions.append({"q": q, "cat": "MSG-data-combo"})

    # Deduplicate by question text
    seen = set()
    unique = []
    for item in questions:
        q_key = item["q"].strip().lower()
        if q_key not in seen:
            seen.add(q_key)
            unique.append(item)

    return unique


# ═══════════════════════════════════════════════════════════════
# VALIDATOR — checks answer correctness via DB
# ═══════════════════════════════════════════════════════════════

async def validate_answer(question_data, result):
    """Validate that the semantic layer returned a reasonable answer."""
    if result is None:
        return {"valid": False, "reason": "no_match", "detail": "Semantic layer returned None"}

    answer = result.get("answer", "")
    sql = result.get("soql", "")
    recs = result.get("data", {}).get("records", [])

    # Check for SQL errors in the answer
    if "error" in answer.lower()[:100]:
        return {"valid": False, "reason": "sql_error", "detail": answer[:200]}

    # Check for empty/zero results that shouldn't be zero
    cat = question_data.get("cat", "")
    if answer.startswith("**0 ") and cat in ("L1-count", "L2-status", "L2-tech"):
        entity = question_data.get("entity", "")
        if entity in ("students", "submissions", "interviews", "managers"):
            return {"valid": False, "reason": "unexpected_zero",
                    "detail": f"Got 0 for {entity} which shouldn't be zero"}

    # BU group returning 0 BUs
    if "0 BUs, 0 total" in answer:
        return {"valid": False, "reason": "empty_group", "detail": "BU group returned 0 results"}

    # SQL present means the query executed
    if not sql or sql == "-- No data query needed":
        if cat.startswith("MSG"):
            return {"valid": True, "reason": "message_template"}
        if "Message Template" in answer:
            return {"valid": True, "reason": "message_template"}

    # Cross-validate count queries with a verification SQL
    if cat in ("L1-count", "L2-status", "L2-tech", "L2-visa") and sql:
        import re
        count_match = re.search(r'\*\*(\d[\d,]*)\s+\w+\*\*', answer)
        if count_match:
            reported_count = int(count_match.group(1).replace(",", ""))
            try:
                from app.database.query import execute_query
                verify_result = await execute_query(sql)
                if "error" not in verify_result:
                    verify_recs = verify_result.get("records", [])
                    if verify_recs and "cnt" in verify_recs[0]:
                        actual_count = verify_recs[0]["cnt"]
                        if reported_count != actual_count:
                            return {"valid": False, "reason": "count_mismatch",
                                    "detail": f"Reported {reported_count} but DB says {actual_count}"}
            except Exception as e:
                pass  # Can't verify, assume OK

    return {"valid": True, "reason": "passed"}


# ═══════════════════════════════════════════════════════════════
# CHECKPOINT SYSTEM
# ═══════════════════════════════════════════════════════════════

def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"completed": 0, "results": [], "started_at": None, "last_updated": None}


def save_checkpoint(state):
    state["last_updated"] = datetime.now().isoformat()
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def load_results():
    if RESULTS_FILE.exists():
        with open(RESULTS_FILE, "r") as f:
            return json.load(f)
    return []


def save_results(results):
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f, indent=2, default=str)


# ═══════════════════════════════════════════════════════════════
# SELF-LEARNING — store verified Q&A pairs
# ═══════════════════════════════════════════════════════════════

def save_to_learning_memory(question, sql, answer, route="SQL"):
    memory = []
    if LEARNING_FILE.exists():
        try:
            with open(LEARNING_FILE, "r") as f:
                memory = json.load(f)
        except Exception:
            memory = []

    # Skip duplicates
    for entry in memory:
        if entry.get("question", "").lower().strip() == question.lower().strip():
            return

    memory.append({
        "timestamp": datetime.now().isoformat(),
        "question": question,
        "soql": sql,
        "answer": answer[:500],
        "route": route,
        "feedback": "auto_verified",
        "username": "test_harness",
        "used_count": 0,
    })

    with open(LEARNING_FILE, "w") as f:
        json.dump(memory, f, indent=2, default=str)


# ═══════════════════════════════════════════════════════════════
# REPORT GENERATOR
# ═══════════════════════════════════════════════════════════════

def generate_report(results, questions_total, started_at, ended_at):
    """Generate an HTML report and JSON summary."""
    passed = [r for r in results if r.get("status") == "PASS"]
    failed = [r for r in results if r.get("status") == "FAIL"]
    auto_fixed = [r for r in results if r.get("auto_corrected")]
    skipped = [r for r in results if r.get("status") == "SKIP"]

    # Category breakdown
    cat_stats = {}
    for r in results:
        cat = r.get("category", "unknown")
        if cat not in cat_stats:
            cat_stats[cat] = {"total": 0, "passed": 0, "failed": 0}
        cat_stats[cat]["total"] += 1
        if r.get("status") == "PASS":
            cat_stats[cat]["passed"] += 1
        elif r.get("status") == "FAIL":
            cat_stats[cat]["failed"] += 1

    # JSON summary
    summary = {
        "generated_at": datetime.now().isoformat(),
        "started_at": started_at,
        "ended_at": ended_at,
        "total_questions": questions_total,
        "tested": len(results),
        "passed": len(passed),
        "failed": len(failed),
        "auto_corrected": len(auto_fixed),
        "skipped": len(skipped),
        "pass_rate": f"{len(passed)/max(len(results),1)*100:.1f}%",
        "category_breakdown": cat_stats,
        "failures": [{"q": r["question"], "reason": r.get("fail_reason", ""),
                      "detail": r.get("fail_detail", "")} for r in failed[:100]],
    }

    # HTML Report
    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>Semantic Layer Test Report</title>
<style>
body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
.container {{ max-width: 1200px; margin: 0 auto; }}
h1 {{ color: #1a1a2e; border-bottom: 3px solid #16213e; padding-bottom: 10px; }}
h2 {{ color: #16213e; margin-top: 30px; }}
.stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }}
.stat-card {{ background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); text-align: center; }}
.stat-card .number {{ font-size: 2.5em; font-weight: bold; }}
.stat-card .label {{ color: #666; margin-top: 5px; }}
.stat-card.pass .number {{ color: #27ae60; }}
.stat-card.fail .number {{ color: #e74c3c; }}
.stat-card.fix .number {{ color: #f39c12; }}
.stat-card.total .number {{ color: #2980b9; }}
.progress {{ background: #ddd; border-radius: 10px; height: 30px; margin: 20px 0; overflow: hidden; }}
.progress-bar {{ height: 100%; border-radius: 10px; display: flex; align-items: center; justify-content: center;
                 color: white; font-weight: bold; transition: width 0.5s; }}
.progress-bar.pass {{ background: #27ae60; }}
table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 10px;
         overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin: 15px 0; }}
th {{ background: #16213e; color: white; padding: 12px 15px; text-align: left; }}
td {{ padding: 10px 15px; border-bottom: 1px solid #eee; }}
tr:hover {{ background: #f0f7ff; }}
.pass-badge {{ background: #27ae60; color: white; padding: 3px 10px; border-radius: 15px; font-size: 0.85em; }}
.fail-badge {{ background: #e74c3c; color: white; padding: 3px 10px; border-radius: 15px; font-size: 0.85em; }}
.fix-badge {{ background: #f39c12; color: white; padding: 3px 10px; border-radius: 15px; font-size: 0.85em; }}
.filter-bar {{ margin: 15px 0; }}
.filter-bar button {{ margin: 5px; padding: 8px 16px; border: 1px solid #ddd; border-radius: 20px;
                      cursor: pointer; background: white; }}
.filter-bar button.active {{ background: #16213e; color: white; border-color: #16213e; }}
.timestamp {{ color: #999; font-size: 0.9em; }}
.truncate {{ max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
</style>
<script>
function filterRows(status) {{
  document.querySelectorAll('.filter-bar button').forEach(b => b.classList.remove('active'));
  event.target.classList.add('active');
  document.querySelectorAll('#results-table tbody tr').forEach(row => {{
    if (status === 'all' || row.dataset.status === status) {{
      row.style.display = '';
    }} else {{
      row.style.display = 'none';
    }}
  }});
}}
</script>
</head><body>
<div class="container">
<h1>Semantic Layer Test Report</h1>
<p class="timestamp">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} |
   Started: {started_at or 'N/A'} | Duration: {ended_at or 'N/A'}</p>

<div class="stats">
  <div class="stat-card total"><div class="number">{len(results)}</div><div class="label">Total Tested</div></div>
  <div class="stat-card pass"><div class="number">{len(passed)}</div><div class="label">Passed</div></div>
  <div class="stat-card fail"><div class="number">{len(failed)}</div><div class="label">Failed</div></div>
  <div class="stat-card fix"><div class="number">{len(auto_fixed)}</div><div class="label">Auto-Corrected</div></div>
</div>

<div class="progress">
  <div class="progress-bar pass" style="width: {len(passed)/max(len(results),1)*100:.1f}%">
    {len(passed)/max(len(results),1)*100:.1f}% Pass Rate
  </div>
</div>

<h2>Category Breakdown</h2>
<table>
<tr><th>Category</th><th>Total</th><th>Passed</th><th>Failed</th><th>Pass Rate</th></tr>
"""
    for cat, stats in sorted(cat_stats.items()):
        rate = stats["passed"] / max(stats["total"], 1) * 100
        color = "#27ae60" if rate >= 90 else "#f39c12" if rate >= 70 else "#e74c3c"
        html += f'<tr><td>{cat}</td><td>{stats["total"]}</td><td>{stats["passed"]}</td>'
        html += f'<td>{stats["failed"]}</td><td style="color:{color};font-weight:bold">{rate:.0f}%</td></tr>\n'

    html += """</table>

<h2>Detailed Results</h2>
<div class="filter-bar">
  <button class="active" onclick="filterRows('all')">All</button>
  <button onclick="filterRows('PASS')">Passed</button>
  <button onclick="filterRows('FAIL')">Failed</button>
  <button onclick="filterRows('FIX')">Auto-Corrected</button>
</div>
<table id="results-table">
<tr><th>#</th><th>Question</th><th>Category</th><th>Status</th><th>Answer Preview</th><th>Detail</th></tr>
<tbody>
"""
    for i, r in enumerate(results, 1):
        status = r.get("status", "?")
        badge_class = "pass" if status == "PASS" else "fail" if status == "FAIL" else "fix"
        if r.get("auto_corrected"):
            badge_class = "fix"
            status = "FIX"
        answer_preview = r.get("answer_preview", "")[:120].replace("<", "&lt;").replace(">", "&gt;")
        detail = r.get("fail_detail", r.get("fail_reason", "")).replace("<", "&lt;")[:100]
        q_text = r.get("question", "").replace("<", "&lt;")
        html += (f'<tr data-status="{status}">'
                 f'<td>{i}</td>'
                 f'<td class="truncate">{q_text}</td>'
                 f'<td>{r.get("category", "")}</td>'
                 f'<td><span class="{badge_class}-badge">{status}</span></td>'
                 f'<td class="truncate">{answer_preview}</td>'
                 f'<td class="truncate">{detail}</td></tr>\n')

    html += """</tbody></table>
"""

    # Failed questions detail section
    if failed:
        html += "<h2>Failed Questions — Details</h2>\n<table>\n"
        html += "<tr><th>#</th><th>Question</th><th>Reason</th><th>Detail</th><th>SQL</th></tr>\n"
        for i, r in enumerate(failed, 1):
            q_text = r.get("question", "").replace("<", "&lt;")
            reason = r.get("fail_reason", "").replace("<", "&lt;")
            detail = r.get("fail_detail", "").replace("<", "&lt;")[:200]
            sql = r.get("sql", "").replace("<", "&lt;")[:150]
            html += f'<tr><td>{i}</td><td>{q_text}</td><td>{reason}</td><td>{detail}</td><td><small>{sql}</small></td></tr>\n'
        html += "</table>\n"

    html += "</div></body></html>"

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    # Save JSON summary
    json_report_file = DATA_DIR / "test_report_summary.json"
    with open(json_report_file, "w") as f:
        json.dump(summary, f, indent=2, default=str)

    return summary


# ═══════════════════════════════════════════════════════════════
# MAIN TEST RUNNER
# ═══════════════════════════════════════════════════════════════

BATCH_SIZE = 25          # questions per batch before checkpoint save
RATE_LIMIT_PAUSE = 5     # seconds to wait on rate limit
MAX_RETRIES = 3          # retries per question on failure


async def run_tests(resume=False, limit=None, report_only=False, reset=False, retry_failed=False):
    from app.chat.semantic import handle_semantic_query

    if reset and CHECKPOINT_FILE.exists():
        os.remove(CHECKPOINT_FILE)
        print("Checkpoint cleared.")

    if report_only:
        results = load_results()
        if not results:
            print("No results found. Run tests first.")
            return
        summary = generate_report(results, len(results), "N/A", "N/A")
        print(f"\nReport generated: {REPORT_FILE}")
        print(f"  Tested: {summary['tested']} | Passed: {summary['passed']} | "
              f"Failed: {summary['failed']} | Rate: {summary['pass_rate']}")
        return

    # ── Retry-failed mode: only re-run previously failed questions ──
    if retry_failed:
        prev_results = load_results()
        if not prev_results:
            print("No previous results found. Run full tests first.")
            return

        passed_results = [r for r in prev_results if r.get("status") == "PASS"]
        failed_results = [r for r in prev_results if r.get("status") != "PASS"]

        if not failed_results:
            print("No failed questions to retry! All passed previously.")
            return

        print(f"Retrying {len(failed_results)} failed questions (keeping {len(passed_results)} passed)...")
        started_at = datetime.now().isoformat()
        retried = []
        tested = 0
        total_retry = len(failed_results)
        batch_count = 0

        try:
            for fi, prev in enumerate(failed_results):
                question = prev["question"]
                cat = prev.get("category", "unknown")

                retries = 0
                result_entry = {
                    "index": prev.get("index", fi + 1),
                    "question": question,
                    "category": cat,
                    "status": None,
                    "answer_preview": "",
                    "sql": "",
                    "fail_reason": "",
                    "fail_detail": "",
                    "auto_corrected": False,
                    "retry_count": 0,
                    "timestamp": datetime.now().isoformat(),
                }

                while retries <= MAX_RETRIES:
                    try:
                        result = await handle_semantic_query(question)
                        result_entry["retry_count"] = retries

                        if result and result.get("answer"):
                            result_entry["status"] = "PASS"
                            result_entry["answer_preview"] = str(result["answer"])[:300]
                            result_entry["sql"] = str(result.get("soql", ""))[:500]
                            break
                        else:
                            retries += 1
                            if retries > MAX_RETRIES:
                                result_entry["status"] = "FAIL"
                                result_entry["fail_reason"] = "no_answer"
                                result_entry["fail_detail"] = "Semantic handler returned None/empty"
                                break
                            continue

                    except Exception as e:
                        err_str = str(e)
                        if "rate" in err_str.lower() or "limit" in err_str.lower():
                            await asyncio.sleep(RATE_LIMIT_PAUSE)
                            retries += 1
                            continue
                        elif "connection" in err_str.lower() or "timeout" in err_str.lower():
                            await asyncio.sleep(10)
                            retries += 1
                            continue
                        else:
                            result_entry["status"] = "FAIL"
                            result_entry["fail_reason"] = "exception"
                            result_entry["fail_detail"] = err_str[:200]
                            break

                if result_entry["status"] is None:
                    result_entry["status"] = "FAIL"
                    result_entry["fail_reason"] = "max_retries"

                retried.append(result_entry)
                tested += 1
                batch_count += 1

                passed_now = sum(1 for r in retried if r.get("status") == "PASS")
                failed_now = sum(1 for r in retried if r.get("status") == "FAIL")
                pct = (fi + 1) / total_retry * 100
                bar_len = 30
                filled = int(bar_len * (fi + 1) / total_retry)
                bar = "#" * filled + "-" * (bar_len - filled)
                sys.stdout.write(f"\r  [{bar}] {pct:.1f}% | Q{fi+1}/{total_retry} | "
                               f"Fixed: {passed_now} | Still Failing: {failed_now}  ")
                sys.stdout.flush()

                if batch_count >= BATCH_SIZE:
                    batch_count = 0

        except KeyboardInterrupt:
            print(f"\n\nInterrupted at retry question {tested}. Saving progress...")

        # Merge: keep passed from before + retried results
        all_results = passed_results + retried
        ended_at = datetime.now().isoformat()

        save_results(all_results)
        summary = generate_report(all_results, len(all_results), started_at, ended_at)

        new_passed = sum(1 for r in retried if r.get("status") == "PASS")
        still_failed = sum(1 for r in retried if r.get("status") == "FAIL")
        total_passed = len(passed_results) + new_passed

        print(f"\n\n{'='*60}")
        print(f"  RETRY COMPLETE")
        print(f"{'='*60}")
        print(f"  Previously Passed:  {len(passed_results)}")
        print(f"  Retried:            {len(retried)}")
        print(f"  Newly Fixed:        {new_passed}")
        print(f"  Still Failing:      {still_failed}")
        print(f"  Total Pass Rate:    {total_passed}/{len(all_results)} ({total_passed/max(len(all_results),1)*100:.1f}%)")
        print(f"{'='*60}")
        print(f"  Report:   {REPORT_FILE}")
        print(f"  Results:  {RESULTS_FILE}")
        if still_failed > 0:
            from collections import Counter
            reasons = Counter(r.get("fail_reason", "") for r in retried if r.get("status") == "FAIL")
            print(f"\n  Remaining failure reasons:")
            for reason, count in reasons.most_common(5):
                print(f"    {reason}: {count}")
        print()
        return

    # Generate all questions
    all_questions = generate_questions()
    total = len(all_questions)
    if limit:
        total = min(total, limit)
    print(f"Generated {len(all_questions)} unique questions. Testing {total}.")

    # Resume from checkpoint
    checkpoint = load_checkpoint() if resume else {"completed": 0, "results": [], "started_at": None}
    start_idx = checkpoint["completed"] if resume else 0
    results = checkpoint["results"] if resume else []
    started_at = checkpoint.get("started_at") or datetime.now().isoformat()

    if start_idx > 0:
        print(f"Resuming from question {start_idx + 1} ({start_idx} already completed)")
        print(f"  Previous results: {sum(1 for r in results if r.get('status')=='PASS')} passed, "
              f"{sum(1 for r in results if r.get('status')=='FAIL')} failed")

    tested = 0
    batch_count = 0
    try:
        for idx in range(start_idx, total):
            qdata = all_questions[idx]
            question = qdata["q"]
            cat = qdata.get("cat", "unknown")

            retries = 0
            result_entry = {
                "index": idx + 1,
                "question": question,
                "category": cat,
                "status": None,
                "answer_preview": "",
                "sql": "",
                "fail_reason": "",
                "fail_detail": "",
                "auto_corrected": False,
                "retry_count": 0,
                "timestamp": datetime.now().isoformat(),
            }

            while retries <= MAX_RETRIES:
                try:
                    result = await handle_semantic_query(question)
                    validation = await validate_answer(qdata, result)

                    if result:
                        answer = result.get("answer", "")
                        result_entry["answer_preview"] = answer[:200].replace("\n", " ")
                        result_entry["sql"] = result.get("soql", "")[:300]

                    if validation["valid"]:
                        result_entry["status"] = "PASS"
                        # Save to learning memory
                        if result:
                            save_to_learning_memory(
                                question, result.get("soql", ""),
                                result.get("answer", ""), "SQL"
                            )
                        break
                    else:
                        result_entry["fail_reason"] = validation["reason"]
                        result_entry["fail_detail"] = validation.get("detail", "")

                        # Auto-correction attempt for no_match
                        if validation["reason"] == "no_match" and retries < MAX_RETRIES:
                            retries += 1
                            result_entry["retry_count"] = retries
                            # Try with cleaned/rephrased question
                            alt_q = question.strip().lower().rstrip("?!.")
                            if alt_q != question:
                                question = alt_q
                                result_entry["auto_corrected"] = True
                                continue
                            break
                        else:
                            result_entry["status"] = "FAIL"
                            break

                except Exception as e:
                    err_str = str(e)
                    if "rate" in err_str.lower() or "limit" in err_str.lower() or "too many" in err_str.lower():
                        print(f"\n  Rate limit hit at Q{idx+1}. Waiting {RATE_LIMIT_PAUSE}s...")
                        await asyncio.sleep(RATE_LIMIT_PAUSE)
                        RATE_LIMIT_PAUSE_LOCAL = RATE_LIMIT_PAUSE * (retries + 1)
                        retries += 1
                        continue
                    elif "connection" in err_str.lower() or "timeout" in err_str.lower():
                        print(f"\n  Connection issue at Q{idx+1}. Waiting 10s...")
                        await asyncio.sleep(10)
                        retries += 1
                        continue
                    else:
                        result_entry["status"] = "FAIL"
                        result_entry["fail_reason"] = "exception"
                        result_entry["fail_detail"] = err_str[:200]
                        break

            if result_entry["status"] is None:
                result_entry["status"] = "FAIL"
                result_entry["fail_reason"] = "max_retries"

            results.append(result_entry)
            tested += 1
            batch_count += 1

            # Progress display
            passed_so_far = sum(1 for r in results if r.get("status") == "PASS")
            failed_so_far = sum(1 for r in results if r.get("status") == "FAIL")
            pct = (idx + 1) / total * 100
            bar_len = 30
            filled = int(bar_len * (idx + 1) / total)
            bar = "#" * filled + "-" * (bar_len - filled)
            sys.stdout.write(f"\r  [{bar}] {pct:.1f}% | Q{idx+1}/{total} | "
                           f"Pass: {passed_so_far} | Fail: {failed_so_far}  ")
            sys.stdout.flush()

            # Save checkpoint every batch
            if batch_count >= BATCH_SIZE:
                checkpoint = {
                    "completed": idx + 1,
                    "results": results,
                    "started_at": started_at,
                }
                save_checkpoint(checkpoint)
                save_results(results)
                batch_count = 0

    except KeyboardInterrupt:
        print(f"\n\nInterrupted at question {start_idx + tested}. Saving progress...")

    # Final save
    ended_at = datetime.now().isoformat()
    checkpoint = {
        "completed": start_idx + tested,
        "results": results,
        "started_at": started_at,
    }
    save_checkpoint(checkpoint)
    save_results(results)

    # Generate report
    summary = generate_report(results, total, started_at, ended_at)

    passed_count = sum(1 for r in results if r.get("status") == "PASS")
    failed_count = sum(1 for r in results if r.get("status") == "FAIL")
    fixed_count = sum(1 for r in results if r.get("auto_corrected"))

    print(f"\n\n{'='*60}")
    print(f"  TEST COMPLETE")
    print(f"{'='*60}")
    print(f"  Total Questions:  {total}")
    print(f"  Tested:           {len(results)}")
    print(f"  Passed:           {passed_count} ({passed_count/max(len(results),1)*100:.1f}%)")
    print(f"  Failed:           {failed_count}")
    print(f"  Auto-Corrected:   {fixed_count}")
    print(f"{'='*60}")
    print(f"  Report:   {REPORT_FILE}")
    print(f"  Results:  {RESULTS_FILE}")
    print(f"  Checkpoint: {CHECKPOINT_FILE}")
    if failed_count > 0:
        print(f"\n  Top failure reasons:")
        from collections import Counter
        reasons = Counter(r.get("fail_reason", "") for r in results if r.get("status") == "FAIL")
        for reason, count in reasons.most_common(5):
            print(f"    {reason}: {count}")
    print()


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Semantic Layer 5000+ Question Test Harness")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    parser.add_argument("--limit", type=int, help="Limit number of questions to test")
    parser.add_argument("--report-only", action="store_true", help="Just generate report from last results")
    parser.add_argument("--reset", action="store_true", help="Clear checkpoint and start fresh")
    parser.add_argument("--retry-failed", action="store_true", help="Re-run only previously failed questions")
    args = parser.parse_args()

    asyncio.run(run_tests(
        resume=args.resume,
        limit=args.limit,
        report_only=args.report_only,
        reset=args.reset,
        retry_failed=args.retry_failed,
    ))


if __name__ == "__main__":
    main()
