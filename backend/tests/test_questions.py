"""
Comprehensive Test Suite for Chat API (PostgreSQL backend)
1200+ questions across difficulty levels 1-10 with self-correcting retry logic.

Run: python tests/test_questions.py
  --max N         Limit to first N questions
  --category X    Filter by category name
  --level N       Filter by difficulty level (1-10)
  --retries N     Max retries for failed questions (default 2)
  --url URL       API base URL (default http://localhost:8000)
  --parallel N    Concurrent requests (default 1)

Self-correcting: on failure, the runner re-sends the question with a hint
to the AI, then retries. If still failing after max retries, it logs the error.

Requires: pip install httpx
"""
import os
import sys
import json
import time
import uuid
import io
import httpx
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

BASE_URL = os.getenv("TEST_API_URL", "http://localhost:8000")
AUTH_TOKEN = os.getenv("TEST_AUTH_TOKEN", "")

# ═══════════════════════════════════════════════════════════════════════
# DIFFICULTY LEVEL 1 - Simple counts and lists (single table, no filters)
# ═══════════════════════════════════════════════════════════════════════

LEVEL_1_BASIC_COUNTS = [
    "How many students do we have?",
    "Total student count",
    "How many submissions total?",
    "How many interviews total?",
    "How many jobs are there?",
    "How many employees?",
    "How many BU managers?",
    "Total accounts count",
    "Total contacts count",
    "How many active jobs?",
    "Count of students",
    "Count of submissions",
    "Count of interviews",
    "Number of students",
    "Number of submissions",
    "Number of interviews",
    "Number of jobs",
    "Number of employees",
    "Give me total students",
    "What's the student count?",
]

LEVEL_1_SIMPLE_LISTS = [
    "Show all students",
    "List all students",
    "Show student names",
    "List all BU managers",
    "Show all BU names",
    "List all technologies",
    "Show all jobs",
    "List active jobs",
    "Show all employees",
    "List employee names",
    "Show submissions",
    "List interviews",
    "Show student technologies",
    "Show student visa statuses",
    "List all student statuses",
    "Show marketing statuses",
    "List all students sorted by name",
    "Show latest students added",
    "List recently added students",
    "Show all student names and technology",
]

# ═══════════════════════════════════════════════════════════════════════
# DIFFICULTY LEVEL 2 - Single filter queries (one WHERE condition)
# ═══════════════════════════════════════════════════════════════════════

LEVEL_2_STATUS_FILTER = [
    "Show students in market",
    "List students with verbal confirmation",
    "Show students with exit status",
    "List pre marketing students",
    "Show project started students",
    "Students with project completed status",
    "How many students in market?",
    "How many students with verbal confirmation?",
    "Count of students in exit",
    "How many pre marketing students?",
    "How many project started?",
    "Students with project completed in market status",
    "How many students are active in market?",
    "Show confirmed students",
    "List students on bench",
    "Students not ready for market",
    "Who got pulled out?",
    "Show students on hold",
    "Students currently in marketing",
    "Active in market students",
]

LEVEL_2_TECHNOLOGY_FILTER = [
    "Show Java students",
    "List Python students",
    "Show DevOps students",
    "List .NET students",
    "Show Data Engineering students",
    "List SFDC students",
    "Show DS/AI students",
    "List Business Analyst students",
    "Show ServiceNow students",
    "List SAP BTP students",
    "Show RPA students",
    "List PowerBI students",
    "How many Java students?",
    "How many DevOps students?",
    "How many Python students?",
    "How many .NET students?",
    "How many Data Engineering students?",
    "Java student count",
    "DevOps student count",
    "DE students count",
    "CS students list",
    "AEM students",
]

LEVEL_2_VISA_FILTER = [
    "Show H1 visa students",
    "List OPT students",
    "Show GC holders",
    "List H4 EAD students",
    "Show STEM students",
    "List USC students",
    "Show CPT students",
    "List L2 students",
    "How many H1 students?",
    "How many OPT students?",
    "How many GC students?",
    "How many H4 EAD students?",
    "Count of STEM OPT students",
    "Count of USC students",
    "H1B student list",
    "OPT student count",
    "GC student count",
    "Visa wise student count",
    "Students with citizen status",
    "H1 transfer students",
]

LEVEL_2_ACTIVE_FILTER = [
    "Show active jobs",
    "List active employees",
    "Show active BU managers",
    "How many active jobs?",
    "How many active employees?",
    "Active student count",
    "Active employees list",
    "Show active BU manager names",
    "Active jobs count",
    "List all active managers",
]

# ═══════════════════════════════════════════════════════════════════════
# DIFFICULTY LEVEL 3 - Date-filtered queries (time ranges)
# ═══════════════════════════════════════════════════════════════════════

LEVEL_3_TODAY_YESTERDAY = [
    "Show today's submissions",
    "List today's interviews",
    "How many submissions today?",
    "How many interviews today?",
    "Today submission count",
    "Today interview count",
    "Yesterday's submissions",
    "Yesterday submissions count",
    "How many submissions yesterday?",
    "Yesterday's interviews",
    "How many interviews yesterday?",
    "Today's new students",
    "Students added today",
    "Submissions for today",
    "Interviews scheduled today",
    "Any submissions today?",
    "Any interviews today?",
    "Anything new today?",
    "Today's update",
    "What happened today?",
]

LEVEL_3_THIS_LAST_WEEK = [
    "This week submissions",
    "This week submissions count",
    "This week interviews",
    "This week interview count",
    "Last week submissions",
    "Last week submissions count",
    "Last week interviews",
    "Last week interview count",
    "How many submissions this week?",
    "How many interviews this week?",
    "How many submissions last week?",
    "How many interviews last week?",
    "This week confirmations",
    "Last week confirmations",
    "Students added this week",
    "Students added last week",
    "This week new students",
    "Last week new students count",
    "What happened this week?",
    "This week summary",
]

LEVEL_3_THIS_LAST_MONTH = [
    "This month submissions",
    "This month submissions count",
    "This month interviews",
    "This month interview count",
    "Last month submissions",
    "Last month submissions count",
    "Last month interviews",
    "Last month interview count",
    "How many submissions this month?",
    "How many interviews this month?",
    "How many submissions last month?",
    "How many interviews last month?",
    "This month confirmations",
    "Last month confirmations",
    "Students added this month",
    "Students added last month",
    "This month new students",
    "Last month performance",
    "Monthly submissions so far",
    "Month to date submissions",
]

LEVEL_3_LAST_N_DAYS = [
    "Last 3 days submissions",
    "Last 5 days submissions",
    "Last 7 days submissions",
    "Last 10 days submissions",
    "Last 14 days submissions",
    "Last 21 days submissions",
    "Last 30 days submissions",
    "Last 60 days submissions",
    "Last 90 days submissions",
    "Last 3 days interviews",
    "Last 5 days interviews",
    "Last 7 days interviews",
    "Last 14 days interviews",
    "Last 30 days interviews",
    "Students added in last 7 days",
    "Students added in last 14 days",
    "Students added in last 30 days",
    "Submissions in last 7 days",
    "Interviews in last 14 days",
    "Confirmations last 7 days",
]

# ═══════════════════════════════════════════════════════════════════════
# DIFFICULTY LEVEL 4 - BU-specific queries (name-based filtering)
# ═══════════════════════════════════════════════════════════════════════

LEVEL_4_BU_STUDENTS = [
    "Students under Abhijith Reddy",
    "Students under Divya Panguluri",
    "Students under Adithya Reddy Venna",
    "Students under Vinay Singh",
    "Students under Gulam Siddiqui",
    "Students under Kiran Reddy",
    "Students under Ravi Mandala",
    "Students under Prabhakar Kunreddy",
    "Students under Sriram Anunthula",
    "Students under Manoj Prabhakar Daram",
    "Students under Prem Kumar Malla",
    "Students under Sudharshan Kumar",
    "Students under Satish Reddy",
    "Students under Rakesh Ravula",
    "Students under Mukesh Ravula",
    "Students under Karthik Reddy",
    "Students under Venkata Sai",
    "How many students under Abhijith?",
    "How many students under Divya?",
    "How many students under Gulam?",
    "Abhijith Reddy student count",
    "Divya Panguluri students count",
    "Kiran Reddy student count",
    "List students under Ravi Mandala",
    "Show all students in Prabhakar's BU",
]

LEVEL_4_BU_SUBMISSIONS = [
    "Show submissions for BU Abhijith Reddy",
    "Abhijith Reddy submissions this month",
    "Divya Panguluri submissions this month",
    "Adithya Reddy submissions this month",
    "Vinay Singh submissions this month",
    "Gulam Siddiqui submissions this month",
    "Kiran Reddy submissions this month",
    "Ravi Mandala submissions this month",
    "Prabhakar submissions this month",
    "Sriram submissions this month",
    "Manoj Prabhakar submissions this month",
    "Prem Kumar submissions this month",
    "Sudharshan submissions this month",
    "Satish Reddy submissions this week",
    "Rakesh Ravula submissions this month",
    "Mukesh Ravula submissions this week",
    "Abhijith submissions today",
    "Divya submissions today",
    "Abhijith last 7 days submissions",
    "Vinay last 14 days submissions",
    "Divya last week submissions",
    "Gulam this week submissions",
    "Kiran Reddy today submissions",
    "How many submissions for Abhijith this month?",
    "How many submissions for Divya this week?",
]

LEVEL_4_BU_INTERVIEWS = [
    "Abhijith BU interviews this week",
    "Divya BU interviews this month",
    "Adithya BU interviews this week",
    "Vinay Singh interviews last week",
    "Gulam Siddiqui interviews this month",
    "Kiran Reddy interviews this month",
    "Ravi Mandala interviews this week",
    "Prabhakar interviews this month",
    "Sriram interviews this month",
    "Manoj Prabhakar interviews this week",
    "Prem Kumar interviews this month",
    "Sudharshan interviews last 7 days",
    "Satish Reddy interviews last 14 days",
    "Rakesh Ravula interviews this week",
    "Abhijith Reddy interviews last 7 days",
    "Divya Panguluri interviews last 14 days",
    "Gulam interviews last 7 days",
    "How many interviews for Abhijith this month?",
    "How many interviews for Kiran this week?",
    "Show interviews for BU Abhijith",
]

# ═══════════════════════════════════════════════════════════════════════
# DIFFICULTY LEVEL 5 - Aggregation and GROUP BY queries
# ═══════════════════════════════════════════════════════════════════════

LEVEL_5_GROUP_BY = [
    "Student count by technology",
    "Student count by visa status",
    "Student count by marketing status",
    "Submissions count BU wise this month",
    "Interviews count BU wise this week",
    "Submissions by recruiter this month",
    "Technology wise student distribution",
    "Visa wise student distribution",
    "Status wise student count",
    "BU wise student count",
    "BU wise active students",
    "BU wise students in market",
    "Technology wise submission count this month",
    "BU wise interviews this month",
    "BU wise confirmations this month",
    "Type wise interview breakdown this month",
    "Final status wise interview count",
    "Interview type breakdown",
    "Submission status breakdown",
    "Daily submission count this week",
    "BU wise submission count last week",
    "BU wise interview count last week",
    "Count of submissions per client",
    "Top clients by submission count",
    "BU ranking by submissions this month",
    "BU ranking by interviews this month",
]

LEVEL_5_TOP_N = [
    "Top 10 BUs by submission count this month",
    "Top 5 technologies by student count",
    "Top 5 BUs by interviews this month",
    "Top 10 students by days in market",
    "Top clients this month",
    "Top 3 technologies by submissions this month",
    "Which BU has most submissions this month?",
    "Which BU has most interviews this week?",
    "Which technology has most students?",
    "Which visa type has most students?",
    "Which client has most submissions?",
    "Which BU has most students in market?",
    "Which BU has most confirmations this month?",
    "Best performing BU this month",
    "Top performing BU this week",
    "Bottom 5 BUs by submissions this month",
    "BU with least submissions this month",
    "BU with zero submissions this week",
    "Worst performing BU this month",
    "Lowest activity BU",
]

LEVEL_5_AVERAGES = [
    "Average days in market by technology",
    "Average days in market by BU",
    "Max days in market",
    "Who has been in market the longest?",
    "Average rate by technology",
    "Average rate by BU",
    "Students with more than 60 days in market count",
    "Students with more than 90 days in market",
    "Students with more than 100 days in market",
    "Students with more than 30 days in market",
    "Average days in market overall",
    "Students in market more than 120 days",
    "Longest time in market",
    "Shortest time in market for confirmations",
    "Average time to confirmation",
]

# ═══════════════════════════════════════════════════════════════════════
# DIFFICULTY LEVEL 6 - Name-based lookups and student profiles
# ═══════════════════════════════════════════════════════════════════════

LEVEL_6_STUDENT_LOOKUP = [
    "Show details of Aravind Sunkara",
    "What is the status of Hemanth Reddy?",
    "Submissions for Maruthi Pawan",
    "Interviews for Vyshnavi Boppana",
    "Which BU is Aravind Sunkara in?",
    "Show Godala Rakesh Reddy details",
    "Samhitha Reddy information",
    "Nithish Kumar submissions",
    "Divya Arige interview status",
    "Show Mani Kumar details",
    "Mohammed Numair submissions",
    "Pranavi Gantla information",
    "Arpana Reddy details",
    "Vishal Kamareddy status",
    "Show all details for Sai Vardhan",
    "Varun Teja submissions",
    "Cheruku Sathvik details",
    "Kirankumar Reddy status",
    "Find student named Kumar",
    "Search for Reddy students",
    "Who is Aravind?",
    "Show me Hemanth's technology",
    "What technology is Vyshnavi in?",
    "Which manager handles Aravind?",
    "Maruthi Pawan interview history",
    "Has Nithish got any interviews?",
    "When was Mani Kumar's last submission?",
    "How many days has Arpana been in market?",
    "All students named Reddy",
    "All students named Kumar",
    "Students with name Sai",
    "Find student Pawan",
    "Search for student Chinnamsetty",
    "Look up Ganesh",
    "Is Aravind still in market?",
    "Show Maruthi's recent interviews",
    "Nithish Kumar recent submissions",
    "Show complete profile of Aravind Sunkara",
    "Give me everything about Hemanth Reddy",
    "Full details Maruthi Pawan Avula",
]

LEVEL_6_BU_MANAGER_LOOKUP = [
    "Show all BU managers",
    "List all BU names",
    "BU manager list with student count",
    "Show BU managers with their expenses",
    "Which BU manager has most students?",
    "BU managers ranked by student count",
    "Show Divya Panguluri BU details",
    "Abhijith Reddy BU performance",
    "Gulam Siddiqui team status",
    "Show BU manager leaderboard",
    "Active BU managers list",
    "BU managers with expenses",
    "Show per placement cost by BU",
    "BU expenses breakdown",
    "Which BU has highest expenses?",
]

# ═══════════════════════════════════════════════════════════════════════
# DIFFICULTY LEVEL 7 - Multi-filter and combination queries
# ═══════════════════════════════════════════════════════════════════════

LEVEL_7_MULTI_FILTER = [
    "Java students in market under Abhijith",
    "Python students with H1B visa",
    "DevOps OPT students in market",
    "Data Engineering students with more than 30 days in market",
    ".NET students with verbal confirmation",
    "Java H1B students under Vinay Singh",
    "Python GC students with submissions",
    "DevOps CPT students active",
    "Students with H4 EAD in market",
    "STEM OPT Java students",
    "USC students with DevOps",
    "H1B students in market under Divya",
    "OPT students with Data Engineering under Kiran",
    "GC students in market more than 60 days",
    "Java students in market more than 90 days",
    "Students under Abhijith with verbal confirmation",
    "Divya students with Java technology",
    "Gulam team Python developers",
    "Vinay team DevOps students in market",
    "Students added this month under Abhijith",
    "New students under Divya this month",
    "H1 students with interviews this month",
    "OPT students with submissions this week",
    "Java students with submissions today",
    "DevOps students with interviews this month",
]

LEVEL_7_FIELD_COMBINATIONS = [
    "Submissions for Java students this month",
    "Interviews for Python students this week",
    "DevOps submissions by BU today",
    "Data Engineering interviews this month",
    ".NET submissions last 7 days",
    "AWS interviews this week",
    "Which technology has most submissions today?",
    "Technology breakdown of this month's interviews",
    "Visa wise submission count this month",
    "H1B student submissions this month",
    "OPT student interviews this week",
    "GC students with confirmations",
    "Rate above 70 submissions this month",
    "Submissions with rate above 60 by BU",
    "High rate submissions for Java",
    "Average rate for Python submissions",
    "Top rated submissions this month",
    "Confirmed students technology breakdown",
    "Verbal confirmations by visa type",
    "Confirmations by technology this month",
    "BU wise confirmations with student details",
    "Recently confirmed students with technology",
    "This month confirmations with BU and technology",
    "Offshore manager wise submission count this month",
    "Recruiter performance this month",
]

# ═══════════════════════════════════════════════════════════════════════
# DIFFICULTY LEVEL 8 - Cross-table and multi-object queries
# ═══════════════════════════════════════════════════════════════════════

LEVEL_8_CROSS_TABLE = [
    "This month submissions, interviews and confirmations BU wise",
    "BU wise submissions and interviews this month",
    "Submissions and interviews comparison this week",
    "Students in market with zero submissions last 14 days",
    "Students with no interviews in 2 weeks",
    "Students with no submissions in 1 week",
    "Active students with no recent activity",
    "Dormant students - no submissions 30 days",
    "Students with most submissions this month",
    "Students with most interviews this month",
    "Which student has highest submission count?",
    "Students confirmed this month with their BU",
    "Verbal confirmations with submission details",
    "New students who already have submissions",
    "Students added this month with interviews",
    "Fresh students with no submissions yet",
    "Students in market more than 90 days without interview",
    "Students with multiple submissions this week",
    "Students with more than 5 submissions this month",
    "Students with more than 3 interviews this month",
]

LEVEL_8_PERFORMANCE_QUERIES = [
    "BU performance - subs vs ints vs confirmations",
    "Submission to interview conversion rate by BU",
    "Interview to confirmation conversion this month",
    "BU efficiency - submissions per student",
    "Top performing students by submissions",
    "Monthly submission trend by BU",
    "BU comparison last month vs this month",
    "Which BU improved most this month?",
    "Declining BUs - less submissions than last month",
    "Students added vs confirmed this month",
    "Pipeline analysis - market to submission to interview",
    "Client wise submission breakdown",
    "Top clients this month",
    "Technology demand - which tech has most submissions",
    "Technology with most interviews",
    "High rate submissions this month",
    "Rate distribution by BU",
    "Offshore manager performance ranking",
    "Top recruiter this month",
    "Recruiter submission rate",
]

# ═══════════════════════════════════════════════════════════════════════
# DIFFICULTY LEVEL 9 - Complex analytical and report queries
# ═══════════════════════════════════════════════════════════════════════

LEVEL_9_REPORTS = [
    "Weekly submission report BU wise",
    "Monthly performance report all BUs",
    "Weekly performance summary",
    "BU performance dashboard",
    "Executive summary this month",
    "Send weekly report for BU Abhijith Reddy",
    "Send weekly report for BU Divya Panguluri",
    "Send weekly report for BU Gulam Siddiqui",
    "Send weekly report for BU Vinay Singh",
    "Send weekly report for BU Adithya Reddy",
    "Send weekly report for BU Kiran Reddy",
    "Send weekly report for BU Ravi Mandala",
    "Send weekly report for BU Prabhakar",
    "Send weekly report for BU Manoj Prabhakar",
    "Send weekly report for BU Prem Kumar",
    "Monthly Submission & Interviews & confirmation & Interview Amount BU wise",
    "Weekly Submissions & Interviews by BU",
    "Weekly Submissions & Interviews by Lead",
    "Last week confirmations congratulations",
    "PreMarketing report by BU",
    "2 weeks no interviews by BU",
    "2 weeks no interviews by Lead",
    "Yesterday submissions by BU",
    "Yesterday submissions by Offshore Manager",
    "Last 3 days no submissions by BU",
    "Last 3 days no submissions by Offshore Manager",
    "Interview mandatory fields by BU",
    "Monthly student performance by BU",
    "Monthly student performance by Offshore Manager",
    "Monthly recruiter performance by BU",
    "Monthly recruiter performance by Lead",
    "Last week student performance by BU",
    "Last week student performance by Lead",
    "Last week recruiter performance by BU",
    "Last week recruiter performance by Lead",
]

LEVEL_9_ANALYTICS = [
    "Which BU has the highest interview to confirmation ratio?",
    "Compare this month vs last month submissions BU wise",
    "Which technology has highest placement rate?",
    "BUs with zero submissions this week who needs attention?",
    "Students with more than 5 submissions but no interview",
    "Which clients are we submitting to most?",
    "Technology wise submission to interview ratio",
    "Students waiting longest without any interview",
    "Which visa category gets most interviews?",
    "Top 3 technologies by demand this month",
    "How many students moved from market to confirmation this month?",
    "Interview success rate by type",
    "Which interview type leads to most confirmations?",
    "Client interview vs vendor interview success rate",
    "Students with multiple rejections",
    "BU managers with declining submission numbers",
    "Growing BUs - more submissions each week",
    "Fresh students under 7 days with submissions already",
    "Students in market over 120 days still no placement",
    "Rate analysis - average rate by BU",
    "Which BU gets highest rates?",
    "Most active clients this month",
    "Submission frequency per student",
    "Interview frequency per student",
    "BU capacity - students per manager",
    "Which technology students have shortest time to placement?",
    "Interview to offer ratio",
    "Students with gap in activity - last 2 weeks no submission",
    "BU wise fresh additions this month",
    "Students with both submissions and interviews this month",
]

# ═══════════════════════════════════════════════════════════════════════
# DIFFICULTY LEVEL 10 - Complex multi-table analysis, financial, edge cases
# ═══════════════════════════════════════════════════════════════════════

LEVEL_10_FINANCIAL = [
    "Expenses & Per Placement Cost by BU",
    "Job Payroll & Bench Payroll by BU",
    "Total expenses all BUs",
    "Which BU has highest expenses?",
    "Per placement cost ranking",
    "BU wise expenses vs confirmations",
    "Cost per student by BU",
    "Revenue vs cost analysis by BU",
    "Show active jobs with pay rates",
    "Average pay rate by technology",
    "Average bill rate by BU",
    "W2 vs C2C job distribution",
    "Profit analysis by BU",
    "Total payroll this month",
    "Payroll breakdown by project type",
    "Bill rate distribution by technology",
    "High value jobs above 80 rate",
    "Job pay rate trends",
    "BU profitability analysis",
    "Cost efficiency ranking",
]

LEVEL_10_COMPLEX_ANALYSIS = [
    "Full cycle analysis - submission to interview to confirmation time by BU",
    "Student lifecycle analysis - from pre marketing to placement",
    "BU comparison: submissions, interviews, confirmations, expenses, rates",
    "Technology demand vs supply gap analysis",
    "Visa type performance - which visa gets placed fastest",
    "Monthly trend: submissions vs interviews vs confirmations last 6 months",
    "BU health score: submissions + interviews + confirmations - exits",
    "Recruiter efficiency: submissions per recruiter vs interview conversion",
    "Client diversification: how many unique clients per BU",
    "Student utilization: percent of in-market students with submissions this month",
    "Pipeline velocity: average days from in-market to first submission by BU",
    "Interview effectiveness: good + very good results vs total interviews by BU",
    "Confirmation rate: confirmations this month / students in market per BU",
    "Activity coverage: students with at least 1 submission last 7 days / total in market",
    "BU growth trajectory: this month additions vs exits",
    "Technology saturation: technologies with more students than market demand",
    "Rate competitiveness: average rate per technology vs market",
    "Offshore manager span: students per offshore manager vs submissions generated",
    "Stagnation analysis: students 60+ days no activity breakdown by BU and technology",
    "Complete BU scorecard: students, in-market, subs, ints, confs, exits, expenses, rates",
]

LEVEL_10_EDGE_CASES = [
    "subs today",
    "ints this week",
    "how many subs?",
    "today's ints",
    "show subs",
    "list ints",
    "give me subs bu wise",
    "submissions bu wise today",
    "subs by bu this month",
    "ints by bu this week",
    "sub count",
    "int count",
    "confs this month",
    "who got confirmed?",
    "latest confirmations",
    "recent confs",
    "show me data",
    "what's happening today?",
    "any submissions today?",
    "any interviews scheduled?",
    "how are we doing?",
    "are we on track?",
    "which bu is best?",
    "worst bu this month",
    "best performer",
    "worst performer",
    "lazy BUs",
    "active BUs",
    "top BU",
    "bottom BU",
    "abhijith subs",
    "vinay ints",
    "divya students",
    "gulam team",
    "kiran performance",
    "java students count",
    "python devs",
    "dotnet count",
    "h1b count",
    "opt students how many",
    "gc students list",
    "total in market",
    "bench strength",
    "how many on bench?",
    "exits this month",
    "pre marketing count",
    "project started this month",
    "team size",
    "total headcount",
    "submission rate today",
    "interview conversion",
]

# ── Additional Level 1-3 to boost coverage ──
LEVEL_1_MORE = [
    "Show me all data",
    "Give me stats",
    "Show numbers",
    "Quick summary",
    "Brief overview",
    "Daily update",
    "Status update",
    "What is happening?",
    "Show me the dashboard data",
    "Give me KPIs",
    "Show all records",
    "How many records total?",
    "What tables do we have?",
    "Show overall statistics",
    "Give me a quick report",
    "Total records in system",
    "System summary",
    "Data overview",
    "Show me everything",
    "What data do we have?",
    "How many total students in our system?",
    "What is the total submission count?",
    "Count all interviews",
    "Total number of BU managers",
    "Show me student names only",
    "List all job titles",
    "How many organizations?",
    "How many clusters?",
    "Show me all BU manager names",
    "List all active managers with name",
    "Total student records",
    "Total interview records",
    "Total submission records",
    "How many new students?",
    "Show me all student emails",
    "List student phone numbers",
    "How many students total in system?",
    "Give me employee count",
    "What's the total headcount?",
    "Show me the numbers",
]

LEVEL_2_MORE_STATUS = [
    "How many exits this month?",
    "Who left this month?",
    "Pulled out students list",
    "Show students with project completed in market",
    "Students currently pre marketing",
    "Students ready for submissions",
    "Students not in market",
    "Students with active status",
    "Non-active students",
    "Show me bench students",
    "Students waiting for placement",
    "Students in pipeline",
    "Marketing ready students",
    "Students without any status",
    "Students with unknown status",
    "Exit students count",
    "Pre marketing students count",
    "Verbal confirmation count",
    "Project started count",
    "Project completed count",
]

LEVEL_2_MORE_TECHNOLOGY = [
    "Show me all technologies and their student counts",
    "Which technology has the most students?",
    "Which technology has the fewest students?",
    "Technologies with more than 50 students",
    "Technologies with less than 10 students",
    "Show technology distribution",
    "Technology pie chart data",
    "Technology breakdown",
    "List unique technologies",
    "Show me the most popular technology",
]

LEVEL_3_MORE_DATES = [
    "Submissions from April 2026",
    "March submissions count",
    "Submissions after April 15",
    "Submissions before April 10",
    "Interviews scheduled for tomorrow",
    "Next week interviews",
    "Upcoming interviews",
    "Past week summary",
    "Past month overview",
    "Last 2 days submissions",
    "Last 4 days interviews",
    "Show last 60 days submissions",
    "Last 90 days interviews",
    "Submission trend this month",
    "Daily submission count this week",
    "Weekly interview count this month",
    "How many submissions were made yesterday?",
    "How many interviews happened last week?",
    "Total submissions this month so far",
    "This year submissions total",
    "Confirmations this quarter",
    "Last quarter interviews count",
    "First week of this month submissions",
    "Second week of April submissions",
    "Show me April submissions",
    "January to March total submissions",
    "Last 6 months submissions count",
    "Last 3 months interviews",
    "Students modified today",
    "Students updated this week",
    "Jobs created in last 30 days",
    "New jobs this month",
    "Submissions trend last 4 weeks",
    "Interview trend last 4 weeks",
    "Show activity last 48 hours",
    "What happened in the last 72 hours?",
    "Last working day submissions",
    "This fiscal year submissions",
    "Show me weekly submission totals this month",
    "Monthly interview totals this year",
]

# ── Additional Level 5-6 queries ──
LEVEL_5_MORE_AGG = [
    "Recruiter performance this month",
    "Top recruiters by submission count",
    "Offshore manager performance this month",
    "BU wise submissions vs interviews this month",
    "How many confirmations this quarter?",
    "Quarterly submission count",
    "Submissions per day this week",
    "Interviews per day this month",
    "Count of submissions per client this month",
    "Total submissions this month",
    "Total interviews this month",
    "Total confirmations this month",
    "Total students in market",
    "Total active students",
    "Count of students per BU",
    "BU wise submission count this month",
    "BU wise interview count this month",
    "Technology wise interview count",
    "Students with more than 50 days in market",
    "Top 10 students by submissions this month",
    "Bottom 5 BUs by interview count",
    "BU with most students overall",
    "BU with fewest active students",
    "Average submissions per BU this month",
    "Median days in market",
    "Technology with highest submission count this month",
    "Visa type with most active students",
    "Client with most interviews this month",
    "Offshore manager with most submissions this month",
    "Recruiter with most interviews scheduled",
    "BU with best confirmation rate this month",
    "Technology with most confirmations",
    "Average interviews per student this month",
    "Average submissions per student this month",
    "BU submission to interview ratio",
    "Technology wise average days in market",
    "Visa type wise average days in market",
    "BU wise exit count this month",
    "BU wise new additions this month",
    "Net growth by BU this month",
]

LEVEL_6_MORE_NAMES = [
    "Student profile Divya Arige",
    "When was Samhitha added?",
    "Godala Rakesh technology and visa",
    "Show personal details of Mohammed Numair",
    "All activities for Vyshnavi",
    "Details about Boppana",
    "Status of student Sunkara",
    "When did Hemanth get confirmed?",
    "Show Pranavi's visa status",
    "Show me all Patel students",
    "Students with last name Reddy",
    "All students named Singh",
    "Find all Mohammad students",
    "Search for Venkat",
    "Look up student Priya",
    "Details for Rajesh",
    "Who is Kumar in market?",
    "Show me Srinivas details",
    "Find Lakshmi students",
    "Any student named Chandra?",
    "Show me all Sharma students",
    "Find student with name Ravi",
    "Search for Deepak in our system",
    "Who is Suresh and what is his status?",
    "Find all students with name Prasad",
    "Details of student Ramesh",
    "Show Naveen information",
    "What BU is Harish in?",
    "Look up student Karthik",
    "Find all students named Rao",
    "Student named Vijay details",
    "Search for Anand",
    "Who is Pooja in which BU?",
    "Show Sneha student profile",
    "Find all Naidu students",
    "Search for Gopi",
    "Show Srikanth details",
    "Look up Mahesh",
    "Find student Vamsi",
    "Who is Sathish and their technology?",
]

# ── Additional Level 7-8 queries ──
LEVEL_7_MORE = [
    "Java students under Kiran Reddy in market more than 30 days",
    "DevOps OPT students under Ravi Mandala",
    "H1B Python students with interviews this month",
    "GC Java students with submissions this week",
    "Students under Sriram with .NET technology in market",
    "Prem Kumar team DE students with submissions",
    "Sudharshan team Java students in market",
    "Satish Reddy team OPT students count",
    "Students with H4 EAD and Java in market",
    "USC DevOps students under any BU",
    "CPT students in Data Engineering with submissions",
    "L2 students list with their BU name",
    "STEM students with interviews this month by BU",
    "H1B students with verbal confirmation by BU",
    "OPT students added this month with their technology",
    "Active students with Java and more than 60 days in market",
    "Students in market under Abhijith with no interviews last 14 days",
    "Divya team students with submissions but no interviews",
    "Gulam team students with interviews but no confirmation",
    "Vinay team recent confirmations with technology",
    "Technology wise submission count by BU this month",
    "BU wise visa distribution for in-market students",
    "Daily submissions by BU this week",
    "Recruiter wise submissions by BU this month",
    "Offshore manager wise interviews by BU this month",
    "Abhijith Reddy H1B students in market with Java",
    "Divya team GC students with submissions this month",
    "Kiran OPT students with no interviews 14 days",
    "Ravi Mandala .NET students in market 60+ days",
    "Prabhakar team DevOps students added this month",
    "Gulam team students with rate above 70",
    "Students with H1B and DevOps under Sriram with submissions",
    "OPT Java students under Manoj with interviews this month",
    "GC Python students in market more than 45 days",
    "H4 EAD students under Satish Reddy with submissions last 7 days",
    "Students under Mukesh with .NET and verbal confirmation",
    "Rakesh team students with Data Engineering in market",
    "USC students under Venkata Sai with interviews",
    "CPT students in market under Prem Kumar with submissions today",
    "STEM OPT DevOps students under Sudharshan with interviews this week",
]

LEVEL_8_MORE = [
    "Students with submissions this month but no interviews",
    "Students with interviews this month but no submission this month",
    "BU wise students with no activity last 7 days",
    "Students under each offshore manager with submission count",
    "Recruiter wise interviews scheduled this month",
    "New clients this month",
    "Technology demand - which tech has most submissions this month",
    "High rate submissions above 80 this month",
    "Average rate by technology this month",
    "Students with back to back interviews this week",
    "Students waiting for interview after submission",
    "Pending interview results",
    "Students awaiting interview feedback",
    "Re-scheduled interviews this month",
    "Cancelled interviews this month count",
    "Interview load per BU this week",
    "Upcoming interviews next 7 days",
    "BU wise client distribution this month",
    "BU wise vendor distribution",
    "Fastest submission to interview turnaround",
    "Students with multiple submissions same day",
    "Most submitted to clients this month",
    "BU wise W2 vs C2C job split",
    "Active jobs pay rate comparison by BU",
    "Students with job vs students in market ratio by BU",
    "Students with interviews but never got confirmed",
    "Students confirmed without interviews",
    "BU wise first round to final round conversion",
    "Good feedback interviews that did not convert to confirmation",
    "Students with more than 10 submissions total",
    "Students with zero submissions ever in market 30+ days",
    "BU wise students with max days in market",
    "Compare BU submissions this week vs last week",
    "Compare BU interviews this month vs last month",
    "Students under multiple BU managers (transferred students)",
    "Offshore managers handling more than 20 students",
    "Recruiters with zero submissions this week",
    "BU wise student addition trend last 3 months",
    "Client retention - clients with submissions in consecutive months",
    "Students with declining interview frequency",
    "Technology switch analysis - students who changed technology",
    "Submission velocity trend by BU last 4 weeks",
    "Interview scheduling gap analysis",
    "BU wise average time between submissions",
    "Students ready for market but still in pre-marketing",
    "BU performance trajectory last 3 months",
    "Top 5 students by total submissions all time",
    "Students with confirmations in first 30 days of marketing",
    "Average days to first interview by BU",
    "BU wise interview type distribution this month",
]

# ── Additional Level 9-10 queries ──
LEVEL_9_MORE = [
    "Total Interviews and Amounts monthly report",
    "Expenses by BU report",
    "Complete weekly performance summary for all BUs",
    "Daily status report all BUs",
    "Monday morning report",
    "End of week summary report",
    "Give me this week's highlights",
    "Performance overview this month",
    "BU scorecard this month",
    "Submission scorecard this week",
    "Interview scorecard this month",
    "Key metrics this week",
    "Performance metrics all BUs",
    "Show me the BU performance dashboard",
    "Generate complete monthly report",
    "Show me all KPIs this month",
    "Weekly wrap-up report",
    "Today's performance summary",
    "Submission rate this month vs last month",
    "Send daily report all BUs",
    "Generate weekly report for all offshore managers",
    "Recruiter weekly performance report",
    "Client engagement report this month",
    "Technology demand report this quarter",
    "Visa category placement report",
    "New student onboarding report this month",
    "Exit analysis report this quarter",
    "BU productivity report last 30 days",
    "Stagnation report - students 60+ days no activity",
    "Conversion funnel report this month",
    "Rate analysis report by BU and technology",
    "Interview feedback summary report this month",
    "Student pipeline stage report",
    "BU capacity and utilization report",
    "Quarterly business review data",
    "Year to date summary report",
    "Top performers report this quarter",
    "Bottom performers report needing coaching",
    "Client concentration risk report",
    "Technology supply-demand gap report",
]

LEVEL_10_MORE = [
    "Show me BU Abhijith Reddy complete report",
    "Full analysis of Vinay Singh BU",
    "Divya Panguluri BU health check",
    "Gulam Siddiqui team status",
    "Which students need attention?",
    "Students falling behind",
    "Students at risk of stagnation",
    "Pipeline health check",
    "Market readiness report",
    "Students not getting interviews why?",
    "Interview pipeline status",
    "Confirmation pipeline",
    "Students close to confirmation",
    "Expected confirmations this month",
    "BU wise pipeline count",
    "Pre marketing to market conversion rate",
    "How long does it take to get first submission?",
    "Average time in market before first interview",
    "Fastest placements this month",
    "Slow moving students list",
    "Students stuck in market too long",
    "High priority students needing action",
    "Status change this month",
    "Students who changed status recently",
    "Exit students this month with reasons",
    "Student attrition this month",
    "Week over week growth",
    "Month over month trend",
    "Best day for submissions analysis",
    "Peak submission days this month",
    "Show offshore managers list",
    "Offshore manager performance ranking",
    "Best offshore manager this month",
    "Recruiter efficiency report",
    "Top recruiter this month",
    "Worst performing recruiter",
    "BU wise complete data dump",
    "All records this month export",
    "Full database overview",
    "Complete BU analysis with all metrics",
    "Comprehensive weekly report for management",
    "Board level summary with all KPIs",
    "Show me inactive students who should be reactivated",
    "Students with expired marketing status",
    "Technology migration suggestions based on demand",
    "Which students should switch technology?",
    "Overloaded BUs needing redistribution",
    "BU load balancing suggestion",
    "Recruiter assignment efficiency",
    "Client relationship depth by BU",
    "Weekend vs weekday submission patterns",
    "Morning vs evening submission analysis",
    "Seasonal submission trends",
    "Holiday impact on submissions",
    "Cross-BU collaboration opportunities",
    "Duplicate submission detection",
    "Students submitted to same client multiple times",
    "Interview preparation tracking",
    "Mock interview completion rates",
    "Technology training completion status",
]

# ── Extra BU + date combos for coverage ──
LEVEL_4_EXTRA_BU_DATE = [
    "Abhijith Reddy submissions last 3 days",
    "Abhijith Reddy interviews last 7 days",
    "Abhijith Reddy students added this month",
    "Abhijith Reddy confirmations this month",
    "Adithya Reddy submissions last 3 days",
    "Adithya Reddy interviews last 7 days",
    "Adithya Reddy students added this month",
    "Adithya Reddy confirmations this month",
    "Vinay Singh submissions last 3 days",
    "Vinay Singh interviews this month",
    "Vinay Singh students with Java",
    "Vinay Singh confirmations this quarter",
    "Divya Panguluri submissions last 3 days",
    "Divya Panguluri interviews last 14 days",
    "Divya Panguluri students in market count",
    "Divya Panguluri top students",
    "Gulam Siddiqui submissions last 3 days",
    "Gulam Siddiqui interviews last 14 days",
    "Gulam Siddiqui active students count",
    "Gulam Siddiqui students with Python",
    "Kiran Reddy submissions last 7 days",
    "Kiran Reddy interviews this month",
    "Kiran Reddy student technology breakdown",
    "Kiran Reddy dormant students",
    "Ravi Mandala submissions last 7 days",
    "Ravi Mandala interviews last 14 days",
    "Ravi Mandala students in market",
    "Ravi Mandala performance this week",
    "Prabhakar submissions last 7 days",
    "Prabhakar interviews this month",
    "Prabhakar student count",
    "Prabhakar BU submissions vs interviews",
    "Sriram submissions last 7 days",
    "Sriram interviews this month",
    "Sriram student list",
    "Sriram performance report",
    "Manoj Prabhakar submissions this week",
    "Manoj Prabhakar interviews last 14 days",
    "Manoj Prabhakar active students",
    "Prem Kumar submissions this week",
    "Prem Kumar interviews this month",
    "Prem Kumar students status",
    "Sudharshan submissions this month",
    "Sudharshan interviews last 7 days",
    "Sudharshan active students",
    "Satish Reddy submissions this week",
    "Satish Reddy interviews last 14 days",
    "Satish Reddy student list with technology",
    "Rakesh Ravula submissions this month",
    "Rakesh Ravula interviews this week",
    "Mukesh Ravula submissions this month",
    "Mukesh Ravula interviews this week",
    "Karthik Reddy submissions last 7 days",
    "Karthik Reddy interviews this month",
    "Venkata Sai submissions this month",
    "Venkata Sai interviews last 14 days",
]

# ── Additional interview detail queries ──
LEVEL_5_INTERVIEW_TYPES = [
    "Show first round interviews this month",
    "Second round interviews this week",
    "Final round interviews this month",
    "Client interviews this month",
    "Vendor interviews this week",
    "HR interviews this month",
    "Assessment interviews this week",
    "Implementation interviews this month",
    "Interviews with good feedback this month",
    "Interviews with very good feedback",
    "Interviews with average feedback",
    "Cancelled interviews this month",
    "Rescheduled interviews this week",
    "Interviews with confirmation status",
    "Expecting confirmation interviews",
    "Interviews with very bad feedback",
    "Interview types breakdown this month",
    "Final status breakdown this month",
    "Good vs very good interviews ratio",
    "Interview result distribution",
]

# ── BU comparison and leaderboard queries ──
LEVEL_8_BU_COMPARISON = [
    "Show all BU managers with their student count",
    "BU managers ranked by submissions this month",
    "BU managers ranked by interviews this month",
    "BU managers with no submissions this week",
    "BU managers with most confirmations",
    "Compare all BU managers this month",
    "BU managers active vs total students",
    "Which BU manager added most students this month?",
    "BU wise submission trend last 30 days",
    "Every BU today's submission count",
    "All BU interview count this week",
    "Complete BU breakdown this month submissions interviews confirmations",
    "Abhijith team Java students count",
    "Vinay team DevOps students",
    "Divya team Python developers",
    "Which BU has most Java students?",
    "Which BU has most DevOps students?",
    "Technology distribution across all BUs",
    "BU wise technology breakdown",
    "Visa distribution across BUs",
    "BU wise H1B student count",
    "BU wise OPT student count",
    "Which BU has most GC students?",
    "Abhijith vs Vinay vs Divya this month",
    "Top BU managers performance comparison",
    "BU ranking overall this month",
    "Interview completion rate by BU",
    "Successful interviews this month",
    "Interview pass rate by technology",
    "Students with multiple interviews same week",
]

# ── Time variation queries ──
LEVEL_3_TIME_VARIATIONS = [
    "Submissions in last 1 day",
    "Submissions in last 2 days",
    "Submissions in last 45 days",
    "Interviews in last 1 day",
    "BU wise subs last 3 days",
    "BU wise subs last 7 days",
    "BU wise subs last 14 days",
    "BU wise subs last 30 days",
    "BU wise ints last 7 days",
    "BU wise ints last 14 days",
    "Abhijith last 3 days",
    "Vinay last 7 days submissions",
    "Divya last 14 days interviews",
    "Gulam last 30 days performance",
    "This week so far submissions",
    "Quarter to date interviews",
    "Year to date confirmations",
    "This quarter submissions",
    "Last quarter submissions",
    "This year total submissions",
]

# ── Job-specific queries ──
LEVEL_2_JOB_QUERIES = [
    "Show all active jobs",
    "List W2 jobs",
    "Show C2C jobs",
    "PD type jobs",
    "Jobs with pay rate above 60",
    "Jobs with pay rate above 80",
    "Jobs by technology",
    "Java jobs active",
    "DevOps jobs",
    "Python jobs list",
    "Jobs created this month",
    "Jobs created last week",
    "Recent jobs added",
    "Show job details with student name",
    "Active jobs with bill rate",
    "Jobs sorted by pay rate",
    "Highest paying jobs",
    "Jobs by project type",
    "Total active job count",
    "Jobs with profit details",
]

# ═══════════════════════════════════════════════════════════════════════
# COMBINE ALL QUESTIONS WITH DIFFICULTY LEVELS
# ═══════════════════════════════════════════════════════════════════════

ALL_CATEGORIES = [
    # ── LEVEL 1 — Simple counts (fast) ──
    ("L1 Basic Counts", 1, LEVEL_1_BASIC_COUNTS),
    ("L1 General", 1, LEVEL_1_MORE),

    # ── LEVEL 2 — Single filter ──
    ("L2 Status Filter", 2, LEVEL_2_STATUS_FILTER),
    ("L2 Technology Filter", 2, LEVEL_2_TECHNOLOGY_FILTER),
    ("L2 Visa Filter", 2, LEVEL_2_VISA_FILTER),
    ("L2 Active Filter", 2, LEVEL_2_ACTIVE_FILTER),
    ("L2 Job Queries", 2, LEVEL_2_JOB_QUERIES),
    ("L2 More Status", 2, LEVEL_2_MORE_STATUS),
    ("L2 More Technology", 2, LEVEL_2_MORE_TECHNOLOGY),

    # ── LEVEL 3 — Date filtered ──
    ("L3 Today/Yesterday", 3, LEVEL_3_TODAY_YESTERDAY),
    ("L3 This/Last Week", 3, LEVEL_3_THIS_LAST_WEEK),
    ("L3 This/Last Month", 3, LEVEL_3_THIS_LAST_MONTH),
    ("L3 Last N Days", 3, LEVEL_3_LAST_N_DAYS),
    ("L3 Time Variations", 3, LEVEL_3_TIME_VARIATIONS),
    ("L3 More Dates", 3, LEVEL_3_MORE_DATES),
    ("L3 Extra Dates", 3, [
        "Last 15 days submissions",
        "Last 25 days interviews",
        "Last 35 days submissions",
        "This week confirmations count",
        "Last week confirmations count",
        "This month exits",
        "Last month new students",
        "Students created in last 3 days",
        "Interviews created in last 5 days",
        "Submissions created in last 10 days",
    ]),
    ("L3 Daily BU Reports", 3, [
        "PreMarketing students by BU",
        "PreMarketing count for each BU",
        "Show PreMarketing students per business unit",
        "Yesterday submission report by BU",
        "Yesterday submissions per BU",
        "How many submissions did each BU make yesterday?",
        "Yesterday submission report by offshore manager",
        "Yesterday submissions grouped by offshore manager",
        "Show yesterday submissions per offshore manager",
    ]),

    # ── LEVEL 4 — BU-specific ──
    ("L4 BU Students", 4, LEVEL_4_BU_STUDENTS),
    ("L4 BU Submissions", 4, LEVEL_4_BU_SUBMISSIONS),
    ("L4 BU Interviews", 4, LEVEL_4_BU_INTERVIEWS),
    ("L4 Extra BU Date", 4, LEVEL_4_EXTRA_BU_DATE),
    ("L4 Extra BU", 4, [
        "Abhijith team overview",
        "Divya team summary",
        "Gulam team performance",
        "Vinay team status",
        "Kiran team students list",
        "Ravi team submissions count",
        "Prabhakar team interviews today",
        "Sriram BU total students",
        "Manoj BU active students",
        "Prem Kumar BU submissions this week",
    ]),
    ("L4 Daily No Activity", 4, [
        "Interview mandatory fields by BU",
        "Interview mandatory fields missing by BU",
        "Show interviews with missing mandatory fields per BU",
        "Last 3 days no submissions by BU",
        "BUs with no submissions in last 3 days",
        "Which BU had zero submissions in the last 3 days?",
        "Last 3 days no submissions by offshore manager",
        "Offshore managers with no submissions in last 3 days",
        "Which offshore manager had zero submissions in the last 3 days?",
    ]),

    # ── LEVEL 5 — Aggregation ──
    ("L5 Group By", 5, LEVEL_5_GROUP_BY),
    ("L5 Top N", 5, LEVEL_5_TOP_N),
    ("L5 Averages", 5, LEVEL_5_AVERAGES),
    ("L5 Interview Types", 5, LEVEL_5_INTERVIEW_TYPES),
    ("L5 More Aggregation", 5, LEVEL_5_MORE_AGG),
    ("L5 Weekly Sub Int", 5, [
        "Last week submissions and interviews by BU",
        "Last week submission and interview count per BU",
        "Show last week submissions and interviews for each BU",
        "Last week submissions and interviews by offshore manager",
        "Last week submission and interview count per offshore manager",
        "Show last week submissions and interviews for each offshore manager",
    ]),
    ("L5 Weekly Lead Reports", 5, [
        "Last week confirmations with congratulations",
        "Last week confirmed students congratulations",
        "Show last week confirmations with congratulations message",
        "Last week submissions and interviews by BU",
        "Last week sub and int by BU",
        "Last week submissions and interviews by lead",
        "Last week sub and int by lead",
    ]),

    # ── LEVEL 6 — Name lookups ──
    ("L6 Student Lookup", 6, LEVEL_6_STUDENT_LOOKUP),
    ("L6 BU Manager Lookup", 6, LEVEL_6_BU_MANAGER_LOOKUP),
    ("L6 More Names", 6, LEVEL_6_MORE_NAMES),
    ("L6 Extra Names", 6, [
        "Find student Aditya",
        "Search for Rishi",
        "Who is Bhargav?",
        "Show Tanvi details",
        "Look up Akash",
        "Student named Rohit details",
        "Find Meghana",
        "Search for Dinesh",
        "Who is Chaitanya and what BU?",
        "Show Nikhil student info",
    ]),

    # ── LEVEL 7 — Multi-filter ──
    ("L7 Multi Filter", 7, LEVEL_7_MULTI_FILTER),
    ("L7 Field Combinations", 7, LEVEL_7_FIELD_COMBINATIONS),
    ("L7 More Combos", 7, LEVEL_7_MORE),
    ("L7 Weekly Performance", 7, [
        "Recruiter last week performance by BU submissions and interviews",
        "Recruiter wise last week submissions and interviews per BU",
        "Show each recruiter last week performance with submissions and interviews by BU",
        "Recruiter last week performance by offshore manager",
        "Recruiter wise last week submissions and interviews per offshore manager",
        "Show each recruiter last week performance with submissions and interviews by offshore manager",
        "Last 2 weeks no interviews by BU",
        "BUs with no interviews in last 2 weeks",
        "Which BU had zero interviews in the last 2 weeks?",
        "Last 2 weeks no interviews by offshore manager",
        "Offshore managers with no interviews in last 2 weeks",
        "Which offshore manager had zero interviews in last 2 weeks?",
    ]),
    ("L7 Weekly Lead Perf", 7, [
        "Last week student performance report by BU",
        "Last week student wise performance report per BU",
        "Show each student last week performance by BU",
        "Last week student performance report by lead",
        "Last week student wise performance report per lead",
        "Show each student last week performance by lead",
        "Last week recruiter performance report by BU",
        "Last week recruiter wise performance report per BU",
        "Show each recruiter last week performance by BU",
        "Last week recruiter performance report by lead",
        "Last week recruiter wise performance report per lead",
        "Show each recruiter last week performance by lead",
        "2 weeks no interviews by BU",
        "BUs with zero interviews in 2 weeks",
        "2 weeks no interviews by lead",
        "Leads with zero interviews in 2 weeks",
    ]),

    # ── LEVEL 8 — Cross-table ──
    ("L8 Cross Table", 8, LEVEL_8_CROSS_TABLE),
    ("L8 Performance", 8, LEVEL_8_PERFORMANCE_QUERIES),
    ("L8 BU Comparison", 8, LEVEL_8_BU_COMPARISON),
    ("L8 More Cross", 8, LEVEL_8_MORE),
    ("L8 Monthly Performance", 8, [
        "Monthly submissions and interviews and confirmations and interview amount by BU",
        "Monthly BU wise submission interview confirmation and interview amount report",
        "Show this month submissions interviews confirmations and amounts per BU",
        "Total interviews and amounts this month",
        "Total interview count and total interview amounts",
        "Monthly student wise performance by BU",
        "Student wise monthly performance report per BU",
        "Show each student performance this month grouped by BU",
        "Monthly student wise performance by offshore manager",
        "Student wise monthly performance report per offshore manager",
        "Show each student performance this month grouped by offshore manager",
        "Monthly recruiter wise performance by BU",
        "Recruiter wise monthly performance report per BU",
        "Show each recruiter performance this month grouped by BU",
        "Monthly recruiter wise performance by offshore manager",
        "Recruiter wise monthly performance report per offshore manager",
        "Show each recruiter performance this month grouped by offshore manager",
    ]),

    # ── LEVEL 9 — Reports ──
    ("L9 Reports", 9, LEVEL_9_REPORTS),
    ("L9 Analytics", 9, LEVEL_9_ANALYTICS),
    ("L9 More Reports", 9, LEVEL_9_MORE),
    ("L9 Extra Reports", 9, [
        "Complete status report for management",
        "BU efficiency report this quarter",
        "Student placement tracking report",
        "Recruiter accountability report",
        "Technology demand supply report",
        "Client engagement summary",
        "Weekly highlights for leadership",
        "Monthly business review report",
        "Operational metrics dashboard data",
        "Team productivity report this month",
    ]),
    ("L9 Monthly Financial", 9, [
        "Last month expenses and total expenses and per placement by BU",
        "Last month expense report with total and per placement cost per BU",
        "Show last month expenses total expenses and cost per placement by BU",
        "Job payroll and bench payroll by BU",
        "Job payroll vs bench payroll per BU",
        "Show job payroll and bench payroll for each BU",
    ]),

    # ── LEVEL 10 — Complex ──
    ("L10 Financial", 10, LEVEL_10_FINANCIAL),
    ("L10 Complex Analysis", 10, LEVEL_10_COMPLEX_ANALYSIS),
    ("L10 Edge Cases", 10, LEVEL_10_EDGE_CASES),
    ("L10 More Complex", 10, LEVEL_10_MORE),

    # ── LAST — Heavy "list all" queries (large result sets, slow) ──
    ("L1 Simple Lists", 1, LEVEL_1_SIMPLE_LISTS),
    ("L1 List All Extra", 1, [
        "Show student list",
        "Get all students",
        "All students data",
        "Display students",
        "Student information",
        "Students overview",
        "How many total records?",
        "Show everything we have",
        "Complete data summary",
        "Total count of all records",
    ]),
]

ALL_QUESTIONS = []
for cat, level, questions in ALL_CATEGORIES:
    for q in questions:
        ALL_QUESTIONS.append((cat, level, q))


# ═══════════════════════════════════════════════════════════════════════
# DATABASE VERIFICATION LAYER
# ═══════════════════════════════════════════════════════════════════════

import re

def get_db_connection():
    """Get a sync psycopg2 connection to PostgreSQL for verification."""
    try:
        import psycopg2
    except ImportError:
        return None

    # Load .env if DATABASE_URL not in environment
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("DATABASE_URL="):
                        db_url = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
    if not db_url:
        db_url = "postgresql://postgres:postgres@localhost:5432/fyxo"

    # Strip async driver prefix
    db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")

    try:
        return psycopg2.connect(db_url)
    except Exception as e:
        print(f"  DB connection failed: {e}")
        return None


def verify_sql_against_db(sql, answer, question):
    """
    Run the AI-generated SQL directly against PostgreSQL and verify:
    1. SQL executes without error
    2. Row count matches what the answer claims
    3. For count queries, the number in the answer matches DB result

    Returns: (verified: bool, details: str)
    """
    if not sql or not sql.strip().upper().startswith("SELECT"):
        return None, "No SQL to verify"

    # Safety: reject non-SELECT
    dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"]
    for d in dangerous:
        if d in sql.upper().split():
            return None, f"Skipped: dangerous keyword {d}"

    conn = get_db_connection()
    if not conn:
        return None, "No DB connection (psycopg2 not installed or DB unreachable)"

    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description] if cur.description else []
        row_count = len(rows)
        cur.close()
        conn.close()

        # Verification checks
        issues = []

        # Check 1: SQL executed successfully (if we got here, it did)

        # Check 2: For count questions, verify the number matches
        count_words = ["how many", "count", "total number", "total students",
                       "total submissions", "total interviews"]
        is_count_q = any(w in question.lower() for w in count_words)

        if is_count_q and row_count == 1 and len(col_names) == 1:
            db_value = rows[0][0]
            if db_value is not None:
                db_num = int(db_value)
                # Extract number from answer
                numbers_in_answer = re.findall(r'[\d,]+', answer.replace(",", ""))
                numbers_in_answer = [int(n.replace(",", "")) for n in numbers_in_answer if n.isdigit()]
                if numbers_in_answer:
                    answer_num = numbers_in_answer[0]
                    if answer_num != db_num:
                        issues.append(f"COUNT MISMATCH: DB={db_num}, Answer={answer_num}")
                    else:
                        return True, f"Verified: count={db_num} matches"
                else:
                    # Check if spelled out
                    if str(db_num) in answer or f"{db_num:,}" in answer:
                        return True, f"Verified: count={db_num} found in answer"
                    issues.append(f"Count query returned {db_num} but couldn't find it in answer")

        # Check 3: For list queries with records, verify row count is reasonable
        if row_count == 0 and "no " not in answer.lower() and "zero" not in answer.lower() and "0" not in answer:
            if is_count_q:
                issues.append(f"DB returned 0 rows but answer doesn't indicate zero/none")

        # Check 4: SQL returned data (basic success)
        if not issues:
            return True, f"Verified: SQL executed OK, {row_count} rows returned"

        return False, "; ".join(issues)

    except Exception as e:
        conn.close()
        error_msg = str(e)[:200]
        return False, f"SQL execution error: {error_msg}"


def verify_response_data(result, question, level):
    """
    Full data verification: extracts SQL from response, runs against DB,
    compares with the answer text.

    Returns: (verified: bool|None, detail: str)
      - True: data confirmed correct
      - False: data mismatch detected
      - None: couldn't verify (no SQL, no DB, etc.)
    """
    if not result.get("ok"):
        return None, "Response not OK"

    body = result.get("body", {})
    sql = body.get("soql", "") or body.get("sql", "")
    answer = body.get("answer", "")

    if not sql:
        return None, "No SQL in response to verify"

    return verify_sql_against_db(sql, answer, question)


# ═══════════════════════════════════════════════════════════════════════
# SELF-CORRECTING TEST RUNNER
# ═══════════════════════════════════════════════════════════════════════

def send_question(client, question, session_id=None):
    """Send a question to the chat API and collect response."""
    payload = {
        "session_id": session_id or str(uuid.uuid4()),
        "question": question,
    }
    try:
        response = client.post("/api/chat", json=payload)
        if response.status_code == 200:
            body = response.json()
            # Normalize: ensure 'soql' key exists for verification
            if "soql" not in body and "data" in body and isinstance(body["data"], dict):
                body["soql"] = body["data"].get("query", "")
            return {"ok": True, "body": body, "status": 200}
        else:
            return {"ok": False, "error": f"HTTP {response.status_code}: {response.text[:200]}", "status": response.status_code}
    except Exception as e:
        return {"ok": False, "error": str(e), "status": 0}


def validate_response(result, question, level):
    """
    Validate the response based on difficulty level.
    Returns (is_valid, error_message, severity).
    Severity: 'hard' = definitely wrong, 'soft' = might be acceptable.
    """
    if not result["ok"]:
        return False, result["error"], "hard"

    body = result["body"]
    if not body:
        return False, "Empty response body", "hard"

    answer = body.get("answer", "")
    if not answer:
        return False, "No answer in response", "hard"

    if len(answer) < 5:
        return False, f"Answer too short: '{answer}'", "hard"

    # Hard error indicators
    hard_errors = [
        "Schema not loaded",
        "Could not convert query",
        "database error",
        "syntax error",
        "relation does not exist",
        "column does not exist",
        "unterminated",
        "ERROR:",
    ]
    for phrase in hard_errors:
        if phrase.lower() in answer.lower():
            return False, f"Error in answer: {phrase}", "hard"

    # Soft validation: check if counts/data were returned for data queries
    soql = body.get("soql", "")
    records = body.get("records", [])

    # For count questions, verify we got a numeric answer
    count_words = ["how many", "count", "total", "number of"]
    is_count_q = any(w in question.lower() for w in count_words)
    if is_count_q and level <= 5:
        has_number = any(c.isdigit() for c in answer)
        if not has_number and "no " not in answer.lower() and "zero" not in answer.lower():
            return False, "Count question but no number in answer", "soft"

    # For list questions, check we got records or a substantive answer
    list_words = ["show", "list", "get", "display"]
    is_list_q = any(w in question.lower() for w in list_words)
    if is_list_q and len(answer) < 20 and not records:
        return False, "List question but very short answer and no records", "soft"

    return True, "OK", None


def retry_with_hint(client, question, previous_error, session_id):
    """Re-send question with a corrective hint."""
    hint_question = f"{question} (Please ensure the SQL query uses correct PostgreSQL syntax with double-quoted identifiers)"
    return send_question(client, hint_question, session_id)


def save_learning(question, sql, answer, status="good"):
    """
    Save a verified question→SQL mapping to the app's learning memory in PostgreSQL.
    This helps the AI generate better SQL for similar questions in the future.
    """
    try:
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from app.chat.memory import save_interaction_sync
        return save_interaction_sync(
            question=question,
            soql=sql,
            answer=answer[:500],
            route="SQL",
            username="test_runner",
            feedback=status,
        )
    except Exception:
        return False


CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), "test_checkpoint.json")


def _load_checkpoint():
    """Load checkpoint from previous interrupted run."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _save_checkpoint(data):
    """Save checkpoint after each question for resume support."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def run_tests(questions=None, max_questions=None, category_filter=None,
              level_filter=None, max_retries=2, delay=0.3, learn=True, resume=True):
    """
    Run test questions with self-correcting retry logic.
    - Logs EVERY question with answer + SQL + verification status
    - Failed questions are retried with hints up to max_retries times
    - Successful corrections are fed back to the app's learning memory (self-learning)
    - Resume support: if interrupted, restarts from where it left off
    """
    headers = {}
    if AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {AUTH_TOKEN}"

    client = httpx.Client(base_url=BASE_URL, timeout=60.0, headers=headers)

    # Filter questions
    test_set = questions or ALL_QUESTIONS
    if category_filter:
        test_set = [(c, l, q) for c, l, q in test_set if category_filter.lower() in c.lower()]
    if level_filter:
        test_set = [(c, l, q) for c, l, q in test_set if l == level_filter]
    if max_questions:
        test_set = test_set[:max_questions]

    total = len(test_set)

    # Resume from checkpoint if available
    start_from = 0
    passed = 0
    failed = 0
    retried_pass = 0
    errors = []
    all_results = []
    data_verified = 0
    data_mismatches = 0
    learned = 0

    if resume:
        checkpoint = _load_checkpoint()
        if checkpoint and checkpoint.get("total") == total:
            start_from = checkpoint.get("completed", 0)
            passed = checkpoint.get("passed", 0)
            failed = checkpoint.get("failed", 0)
            retried_pass = checkpoint.get("retried_pass", 0)
            errors = checkpoint.get("errors", [])
            all_results = checkpoint.get("all_results", [])
            data_verified = checkpoint.get("data_verified", 0)
            data_mismatches = checkpoint.get("data_mismatches", 0)
            learned = checkpoint.get("learned", 0)
            if start_from > 0:
                print(f"\n  >> RESUMING from question {start_from + 1}/{total} ({start_from} already done)")
                print(f"     Previous: {passed} pass | {failed} fail | {learned} learned")
                print()

    print(f"\n{'='*70}")
    print(f"  CHAT API TEST SUITE - {total} questions")
    print(f"  Server: {BASE_URL}")
    print(f"  Max retries per question: {max_retries}")
    print(f"  Self-learning: {'ENABLED' if learn else 'DISABLED'}")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

    session_id = str(uuid.uuid4())

    # Check if DB verification is available
    db_conn = get_db_connection()
    db_verify_available = db_conn is not None
    if db_conn:
        db_conn.close()
    if db_verify_available:
        print(f"  DB verification: ENABLED (direct PostgreSQL validation)")
    else:
        print(f"  DB verification: DISABLED (install psycopg2 for data validation)")
    print()

    for i, (category, level, question) in enumerate(test_set):
        if i < start_from:
            continue

        print(f"  [{i+1}/{total}] L{level} | {question[:60]}...", end="", flush=True)
        result = send_question(client, question, session_id)
        is_valid, msg, severity = validate_response(result, question, level)

        body = result.get("body", {}) or {}
        answer = body.get("answer", "")
        sql = body.get("soql", "")
        records = body.get("data", {}).get("records", []) if isinstance(body.get("data"), dict) else []

        # Build the log entry for this question
        log_entry = {
            "index": i + 1,
            "category": category,
            "level": level,
            "question": question,
            "answer": answer[:500],
            "sql": sql,
            "record_count": len(records),
            "status": "unknown",
            "verified": None,
            "verification_detail": "",
            "retries": 0,
            "self_corrected": False,
        }

        if is_valid:
            # Step 2: Verify data correctness against DB
            if db_verify_available:
                verified, detail = verify_response_data(result, question, level)
                log_entry["verified"] = verified
                log_entry["verification_detail"] = detail
                if verified is True:
                    data_verified += 1
                    passed += 1
                    log_entry["status"] = "pass_verified"
                    # Self-learning: save verified good answers
                    if learn and sql:
                        if save_learning(question, sql, answer, "good"):
                            learned += 1
                elif verified is False:
                    data_mismatches += 1
                    failed += 1
                    log_entry["status"] = "fail_data_mismatch"
                    errors.append({
                        "category": category,
                        "level": level,
                        "question": question,
                        "error": f"DATA MISMATCH: {detail}",
                        "severity": "data",
                        "answer": answer[:300],
                        "sql": sql[:200],
                    })
                    print(f"  ! DATA [L{level}|{category}]: {question[:55]}")
                    print(f"         {detail[:80]}")
                else:
                    passed += 1
                    log_entry["status"] = "pass_unverified"
                    if learn and sql:
                        if save_learning(question, sql, answer, "good"):
                            learned += 1
            else:
                passed += 1
                log_entry["status"] = "pass"
                if learn and sql:
                    if save_learning(question, sql, answer, "good"):
                        learned += 1
        else:
            # Self-correcting: retry with hints
            retry_passed = False
            for attempt in range(max_retries):
                log_entry["retries"] = attempt + 1
                time.sleep(delay * 2)
                retry_result = retry_with_hint(client, question, msg, session_id)
                is_valid_retry, msg_retry, sev_retry = validate_response(retry_result, question, level)
                if is_valid_retry:
                    # Also verify retry result against DB
                    if db_verify_available:
                        verified, detail = verify_response_data(retry_result, question, level)
                        if verified is False:
                            continue  # Retry again, data is wrong
                    retry_passed = True
                    retried_pass += 1
                    passed += 1
                    log_entry["status"] = "pass_self_corrected"
                    log_entry["self_corrected"] = True
                    # Update with corrected answer
                    retry_body = retry_result.get("body", {}) or {}
                    log_entry["answer"] = retry_body.get("answer", "")[:500]
                    log_entry["sql"] = retry_body.get("soql", "")
                    # Self-learning: save the corrected version
                    if learn and log_entry["sql"]:
                        if save_learning(question, log_entry["sql"], log_entry["answer"], "corrected"):
                            learned += 1
                    break

            if not retry_passed:
                failed += 1
                log_entry["status"] = "fail"
                errors.append({
                    "category": category,
                    "level": level,
                    "question": question,
                    "error": msg,
                    "severity": severity,
                    "answer": answer[:300],
                    "sql": sql[:200],
                })
                print(f"  X FAIL [L{level}|{category}]: {question[:55]}")
                print(f"         Error: {msg[:80]}")

        all_results.append(log_entry)

        # Print result for this question
        status_icon = "PASS" if "pass" in log_entry["status"] else "FAIL"
        print(f" -> {status_icon}", flush=True)

        # Save checkpoint after every question (so we can resume)
        _save_checkpoint({
            "total": total,
            "completed": i + 1,
            "passed": passed,
            "failed": failed,
            "retried_pass": retried_pass,
            "errors": errors,
            "all_results": all_results,
            "data_verified": data_verified,
            "data_mismatches": data_mismatches,
            "learned": learned,
        })

        # Progress summary every 50
        if (i + 1) % 50 == 0:
            pct = (i + 1) / total * 100
            print(f"\n  --- Progress: {i+1}/{total} ({pct:.0f}%) | {passed} pass | {failed} fail | {learned} learned ---\n", flush=True)

        time.sleep(delay)

    # Summary
    print(f"\n{'='*70}")
    print(f"  RESULTS")
    print(f"{'='*70}")
    print(f"  Total:           {total}")
    print(f"  Passed:          {passed} ({passed/total*100:.1f}%)")
    print(f"  Failed:          {failed} ({failed/total*100:.1f}%)")
    print(f"  Self-corrected:  {retried_pass}")
    print(f"  Data verified:   {data_verified} (SQL re-run confirmed correct)")
    print(f"  Data mismatches: {data_mismatches} (answer != DB result)")
    print(f"  Learned:         {learned} (saved to app memory for future use)")
    print(f"  Pass rate:       {passed/total*100:.1f}%")
    print(f"{'='*70}")

    # Level breakdown
    print(f"\n  Level Breakdown:")
    level_stats = {}
    for cat, lvl, q in test_set:
        if lvl not in level_stats:
            level_stats[lvl] = {"total": 0, "failed": 0}
        level_stats[lvl]["total"] += 1
    for e in errors:
        level_stats[e["level"]]["failed"] += 1

    for lvl in sorted(level_stats.keys()):
        stats = level_stats[lvl]
        p = stats["total"] - stats["failed"]
        pct = p / stats["total"] * 100 if stats["total"] > 0 else 0
        bar = "#" * int(pct / 5) + "." * (20 - int(pct / 5))
        status = "PASS" if stats["failed"] == 0 else f"{stats['failed']} FAIL"
        print(f"    Level {lvl:2d}: [{bar}] {p}/{stats['total']} ({pct:.0f}%) - {status}")

    # Category breakdown
    print(f"\n  Category Breakdown:")
    cat_results = {}
    for cat, lvl, q in test_set:
        if cat not in cat_results:
            cat_results[cat] = {"total": 0, "failed": 0}
        cat_results[cat]["total"] += 1
    for e in errors:
        cat_results[e["category"]]["failed"] += 1

    for cat, stats in sorted(cat_results.items()):
        p = stats["total"] - stats["failed"]
        pct = p / stats["total"] * 100 if stats["total"] > 0 else 0
        status = "OK" if stats["failed"] == 0 else f"{stats['failed']} FAIL"
        print(f"    {cat:25s}: {p}/{stats['total']} ({pct:.0f}%) - {status}")

    # Detailed failures
    if errors:
        print(f"\n{'='*70}")
        print(f"  FAILED QUESTIONS ({len(errors)}):")
        print(f"{'='*70}")

        # Group by severity
        hard_errors = [e for e in errors if e["severity"] == "hard"]
        soft_errors = [e for e in errors if e["severity"] == "soft"]
        data_errors = [e for e in errors if e["severity"] == "data"]

        if hard_errors:
            print(f"\n  --- HARD FAILURES ({len(hard_errors)}) - SQL/system errors ---")
            for i, e in enumerate(hard_errors[:30], 1):
                print(f"\n  [{i}] [L{e['level']}|{e['category']}] {e['question']}")
                print(f"      Error: {e['error']}")
                if e['sql']:
                    print(f"      SQL: {e['sql'][:120]}")
                if e['answer']:
                    print(f"      Answer: {e['answer'][:120]}")

        if data_errors:
            print(f"\n  --- DATA MISMATCHES ({len(data_errors)}) - Answer != Database ---")
            for i, e in enumerate(data_errors[:20], 1):
                print(f"\n  [{i}] [L{e['level']}|{e['category']}] {e['question']}")
                print(f"      Error: {e['error']}")
                if e['sql']:
                    print(f"      SQL: {e['sql'][:120]}")

        if soft_errors:
            print(f"\n  --- SOFT FAILURES ({len(soft_errors)}) - Validation warnings ---")
            for i, e in enumerate(soft_errors[:20], 1):
                print(f"\n  [{i}] [L{e['level']}|{e['category']}] {e['question']}")
                print(f"      Error: {e['error']}")

    # Save FULL results to JSON (every question logged)
    results_file = os.path.join(os.path.dirname(__file__), "test_results.json")
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "server": BASE_URL,
            "total": total,
            "passed": passed,
            "failed": failed,
            "self_corrected": retried_pass,
            "data_verified": data_verified,
            "data_mismatches": data_mismatches,
            "learned": learned,
            "db_verification": db_verify_available,
            "pass_rate": f"{passed/total*100:.1f}%",
            "level_breakdown": {
                str(lvl): {
                    "total": stats["total"],
                    "passed": stats["total"] - stats["failed"],
                    "failed": stats["failed"],
                }
                for lvl, stats in sorted(level_stats.items())
            },
            "questions": all_results,
            "errors": errors,
        }, f, indent=2, ensure_ascii=False)
    print(f"\n  Full report saved to: {results_file}")
    print(f"  (Contains every question with answer, SQL, and verification status)")

    # Clean up checkpoint since we finished
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print(f"  Checkpoint cleared (run complete)")

    client.close()
    return passed, failed, errors


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Test chat questions against API with self-correction")
    parser.add_argument("--max", type=int, help="Max questions to test")
    parser.add_argument("--category", type=str, help="Filter by category name")
    parser.add_argument("--level", type=int, help="Filter by difficulty level (1-10)")
    parser.add_argument("--retries", type=int, default=2, help="Max retries per question (default 2)")
    parser.add_argument("--url", type=str, help="API base URL")
    parser.add_argument("--delay", type=float, default=0.3, help="Delay between requests in seconds")
    parser.add_argument("--no-learn", action="store_true", help="Disable self-learning (don't save to app memory)")
    parser.add_argument("--fresh", action="store_true", help="Ignore checkpoint and start from scratch")
    args = parser.parse_args()

    if args.url:
        BASE_URL = args.url

    # Print question summary
    total = len(ALL_QUESTIONS)
    print(f"\n{'='*70}")
    print(f"  TEST SUITE SUMMARY: {total} questions across {len(ALL_CATEGORIES)} categories")
    print(f"{'='*70}")

    level_counts = {}
    for cat, lvl, q in ALL_QUESTIONS:
        level_counts[lvl] = level_counts.get(lvl, 0) + 1

    print(f"\n  Questions by Difficulty Level:")
    for lvl in sorted(level_counts.keys()):
        print(f"    Level {lvl:2d}: {level_counts[lvl]:4d} questions")
    print(f"    {'-'*30}")
    print(f"    Total:   {total:4d} questions")

    print(f"\n  Categories:")
    for cat, lvl, qs in ALL_CATEGORIES:
        print(f"    L{lvl} {cat:25s}: {len(qs)}")

    if args.fresh and os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("\n  >> Fresh start (checkpoint cleared)")

    # Run tests
    passed, failed, errors = run_tests(
        max_questions=args.max,
        category_filter=args.category,
        level_filter=args.level,
        max_retries=args.retries,
        delay=args.delay,
        learn=not args.no_learn,
        resume=not args.fresh,
    )

    sys.exit(0 if failed == 0 else 1)
