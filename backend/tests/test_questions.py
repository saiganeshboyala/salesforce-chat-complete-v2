"""
Comprehensive test suite for Salesforce Chat API - 1000+ questions
Tests the /api/chat endpoint with natural language queries a staffing company manager would ask.

Run with:
    pytest tests/test_questions.py -v --tb=short
    pytest tests/test_questions.py -k "simple" -v
    pytest tests/test_questions.py --maxfail=10

Requires the backend server running at BASE_URL (default: http://localhost:8000)
"""

import os
import uuid
import pytest
import httpx

BASE_URL = os.getenv("TEST_API_URL", "http://localhost:8000")
API_ENDPOINT = f"{BASE_URL}/api/chat"

# Optional auth token - set if your server requires authentication
AUTH_TOKEN = os.getenv("TEST_AUTH_TOKEN", "")


# ---------------------------------------------------------------------------
# Question categories
# ---------------------------------------------------------------------------

SIMPLE_SINGLE_OBJECT_QUERIES = [
    # Student queries - basic
    "Show me all active students",
    "List all students in market",
    "How many students do we have?",
    "Get all students with their technologies",
    "Show student list",
    "Who are our active students?",
    "List students who are active in market",
    "Show me students currently in marketing",
    "Get all students",
    "How many total students are there in system?",
    "Show me all students with their email",
    "List student names and phone numbers",
    "Show all students with their BU name",
    "Get students and their visa status",
    "Show me students with their marketing status",
    "List all students and their technology",
    "Who are the students on bench?",
    "Show me students with verbal confirmation",
    "List confirmed students",
    "Show students on hold",
    "Who got pulled out?",
    "Students not ready for market",
    "Show students with status confirmed",
    "List all active in market students",
    "Get students with H1B visa",
    "Show OPT students",
    "List CPT students",
    "Show all GC holders",
    "Students with citizen status",
    "H4 EAD students list",
    "Show L1 visa students",
    "List students on L2 EAD",
    "Show me Java students",
    "List all Python developers",
    "Who are the .NET students?",
    "Show Salesforce technology students",
    "AWS students list",
    "Azure technology students",
    "DevOps students in market",
    "Data Engineering students",
    "React developers list",
    "Angular students",
    "Full Stack students",
    "SAP technology students",
    "ServiceNow students",
    "Cybersecurity students",
    "AI/ML students",
    "Tableau students list",
    "Power BI students",
    "Selenium testers",
    "Manual Testing students",
    "Business Analyst students",
    "Show me student phone numbers",
    "Get student emails",
    "List students created today",
    "Students created this week",
    "Show recently added students",
    "Students modified today",
    "Show last modified students",
    "Get students sorted by name",
    "List students by creation date",
    "Show me the newest students",
    "Who was added last?",
    "Show me offshore managed students",
    "List students with their onsite manager",
    "Show students with recruiter info",
    "Get students project type",
    "List students by project type",
    "Show contract students",
    "Full time students list",
    "Students with more than 30 days in market",
    "Show students in market less than 7 days",
    "Students in market for over 60 days",
    "Who has been in market the longest?",
    "Show stale students",
    "Students in market more than 90 days",
    "Fresh students added this week",
    "Show me students with no phone number",
    "Students missing email",
    "List students without technology assigned",

    # Submission queries - basic
    "Show all submissions",
    "List recent submissions",
    "How many submissions do we have?",
    "Get all submissions today",
    "Show submissions this week",
    "List submissions this month",
    "Show me all submissions with rates",
    "Get submissions sorted by date",
    "Show latest submissions",
    "List submissions with client name",
    "Show submissions with vendor info",
    "Get submissions with prime vendor",
    "Submissions with status pending",
    "Show selected submissions",
    "Rejected submissions list",
    "Show all active submissions",
    "List submissions with technology",
    "Get submission rates",
    "Show highest rate submissions",
    "List lowest rate submissions",
    "Submissions above $80/hr",
    "Show submissions below $50/hr",
    "Get submissions between $60 and $90",
    "Show me submissions with recruiter",
    "List submissions by onsite manager",
    "Submissions by offshore manager",
    "Show all submissions for Java",
    "Python submissions",
    ".NET submissions list",
    "Salesforce submissions",
    "AWS submissions today",
    "DevOps submissions this week",
    "Data Engineering submissions",
    "React submissions this month",

    # Interview queries - basic
    "Show all interviews",
    "List today's interviews",
    "How many interviews this week?",
    "Get all scheduled interviews",
    "Show upcoming interviews",
    "List completed interviews",
    "Show interview results",
    "Get interviews with final status",
    "Technical interviews list",
    "Managerial interviews",
    "HR interviews scheduled",
    "Client interviews this week",
    "Panel interviews list",
    "Show selected interviews",
    "Rejected interviews",
    "Pending interview results",
    "No show interviews",
    "Rescheduled interviews",
    "Confirmed interviews",
    "Show interviews by technology",
    "Java interviews today",
    "Python interviews this week",
    "Salesforce interviews",
    "AWS interviews scheduled",
    "Show interviews with client name",
    "Interviews by BU",
    "List all interview types",
    "Interview schedule for today",
    "Tomorrow's interviews",
    "This week interview calendar",

    # Job queries - basic
    "Show all jobs",
    "List open jobs",
    "How many jobs are active?",
    "Get job listings",
    "Show jobs by technology",
    "Java jobs available",
    "Python job openings",
    ".NET jobs list",
    "Salesforce jobs",
    "AWS positions open",
    "DevOps jobs",
    "Show jobs by company",
    "List job titles",
    "Get jobs created today",
    "New jobs this week",
    "Jobs added this month",
    "Show closed jobs",
    "Filled positions",
    "Open positions count",
    "Show all job statuses",

    # Employee queries - basic
    "Show all employees",
    "List active employees",
    "How many employees do we have?",
    "Get employees by department",
    "Show employee designations",
    "List managers",
    "Show team leads",
    "Get recruiters list",
    "Active employees count",
    "Inactive employees",
    "New employees this month",
    "Show employees by status",
    "Employee directory",
    "List all departments",
    "Show employees hired this year",

    # More natural/casual phrasing
    "whats the student count",
    "how many ppl in market rn",
    "show me evrything about submissions",
    "gimme the interview list",
    "any new jobs today?",
    "who all are active",
    "total submissions?",
    "interviews happening today",
    "whos on OPT",
    "java devs pls",
    "list of all BAs",
    "show me fullstack guys",
    "any confirmed students?",
    "who got verbal?",
    "show confirmed ppl",
    "how many pulled out?",
    "not ready students",
    "anyone new today?",
    "latest submissions show",
    "recent interviews",
]

FILTERED_DATE_QUERIES = [
    # Today/this week/this month
    "Submissions made today",
    "How many submissions today?",
    "Students added today",
    "Interviews scheduled for today",
    "Jobs posted today",
    "Show me today's activity",
    "What happened today?",
    "Today's submission count",
    "How many interviews today?",
    "Submissions this week",
    "Students added this week",
    "Interviews this week",
    "Jobs this week",
    "This week's submissions count",
    "Weekly submission report",
    "How many submissions this week?",
    "Students created this month",
    "Monthly submissions",
    "This month submission count",
    "Interviews this month",
    "Jobs posted this month",
    "How many students added this month?",

    # Specific date ranges
    "Submissions from January 2024",
    "Submissions in last 7 days",
    "Interviews in last 30 days",
    "Students added in last 2 weeks",
    "Submissions between Jan 1 and Jan 31",
    "Interviews from March 2024",
    "Jobs posted in February 2024",
    "Submissions after March 15",
    "Students created before 2024",
    "Interviews in Q1 2024",
    "Q2 submissions",
    "Last quarter submissions",
    "This quarter interviews",
    "Year to date submissions",
    "YTD interview count",
    "Submissions from last month",
    "Previous week submissions",
    "Last 3 months submissions",
    "Past 6 months interviews",
    "Last year's data",

    # Yesterday/specific days
    "Yesterday's submissions",
    "What was submitted yesterday?",
    "Interviews yesterday",
    "Students added yesterday",
    "Day before yesterday submissions",
    "Last Monday submissions",
    "Last Friday interviews",
    "Show me Monday's data",
    "Tuesday submissions",
    "End of last week submissions",

    # Combined date + filters
    "Java submissions today",
    "Python interviews this week",
    "H1B students added this month",
    "Salesforce submissions last 7 days",
    "DevOps interviews in March",
    "AWS jobs posted this week",
    "Active students added in January",
    "Confirmed students this month",
    "Verbal confirmations this week",
    "Submissions with rate above 80 today",
    "High rate submissions this week",
    "Interviews selected this month",
    "Rejected interviews this week",
    "No shows this month",
    "Submissions by Abhijith's team today",
    "Vinay's team submissions this week",
    "Adithya's BU interviews today",

    # Time-based analysis
    "Show submission trend this week",
    "Daily submission count this month",
    "How did we do last week vs this week?",
    "Submission comparison month over month",
    "Interview frequency this month",
    "When was the last submission?",
    "When was the last interview?",
    "Most active day this week",
    "Which day had most submissions?",
    "Show daily breakdown of submissions",
    "Hourly activity today",
    "Peak submission time",
    "Average submissions per day this week",
    "Average interviews per week this month",

    # Created/Modified date filters
    "Students modified today",
    "Recently updated students",
    "Students not updated in 30 days",
    "Stale records - no update in 60 days",
    "Show records changed this week",
    "Who modified student records today?",
    "Last activity on submissions",
    "When was the last interview created?",
    "Jobs not updated in 2 weeks",
    "Dormant students - no activity 90 days",

    # Specific date formats
    "Submissions on 03/15/2024",
    "Interviews on March 15 2024",
    "Students added on 2024-01-01",
    "Show data for April 2024",
    "Submissions from 1st to 15th this month",
    "First week of March submissions",
    "Last week of February interviews",
    "Mid-month submissions March",
    "Beginning of year data",
    "End of quarter submissions",

    # Rolling windows
    "Last 24 hours submissions",
    "Past 48 hours activity",
    "Last 72 hours interviews",
    "Trailing 7 day submissions",
    "Rolling 30 day interview count",
    "Past 14 days submissions",
    "Show me last 5 days data",
    "Activity in past 3 days",
    "Submissions in the past week",
    "Everything from last 10 days",

    # Date + status
    "Pending interviews from last week",
    "Selected interviews this month",
    "Confirmed students in last 30 days",
    "Pulled out students this month",
    "On hold students since January",
    "Verbal confirmations in last 2 weeks",
    "Active in market since last month",
    "Recently confirmed students",
    "Just got verbal this week",
    "New confirms today",

    # Comparative periods
    "Compare this week vs last week submissions",
    "This month vs last month interviews",
    "Q1 vs Q2 performance",
    "January vs February submissions",
    "Week over week growth in submissions",
    "Month over month interview increase",
    "Year over year comparison",
    "How are we doing compared to last month?",
    "Are submissions up or down this week?",
    "Trend over last 4 weeks",
]

BU_SPECIFIC_QUERIES = [
    # Individual BU queries
    "Show Abhijith Reddy's team",
    "Students under Abhijith Reddy Palreddy",
    "Abhijith's BU performance",
    "How is Abhijith's team doing?",
    "Submissions by Abhijith's BU",
    "Interviews in Abhijith's team",
    "Abhijith's team active students",
    "How many students does Abhijith have?",
    "Abhijith's submission count today",
    "Abhijith team weekly submissions",

    "Show Adithya Reddy's BU",
    "Adithya Reddy Venna's students",
    "Adithya's team submissions",
    "Interviews for Adithya's BU",
    "How is Adithya performing?",
    "Adithya's team active count",
    "Students under Adithya",
    "Adithya Reddy submissions this week",
    "Adithya's confirmed students",
    "Show Adithya's BU stats",

    "Vinay Singh's team",
    "Students under Vinay",
    "Vinay's submissions today",
    "Vinay Singh BU performance",
    "How many active students does Vinay have?",
    "Vinay's team interviews this week",
    "Vinay's BU submissions this month",
    "Show me Vinay's numbers",
    "Vinay team confirmed students",
    "Vinay Singh student list",

    "Gulam Siddiqui's BU",
    "Gulam's team students",
    "Submissions by Gulam's team",
    "Gulam Siddiqui interviews",
    "How is Gulam's BU doing?",
    "Gulam's active students",
    "Gulam team performance",
    "Students under Gulam Siddiqui",
    "Gulam's weekly submissions",
    "Show Gulam's team stats",

    "Anil Kumar's students",
    "Anil's team submissions",
    "Anil Kumar BU performance",
    "Interviews in Anil's BU",
    "Anil's active student count",
    "Show Anil Kumar's team",
    "Anil's submissions today",
    "Anil team interviews this week",
    "How is Anil's BU performing?",
    "Anil Kumar confirmed students",

    "Ravi Teja's BU students",
    "Ravi Teja submissions",
    "Ravi's team performance",
    "Show Ravi Teja's active students",
    "Ravi Teja interviews this month",

    "Srinivas Reddy team",
    "Srinivas students list",
    "Srinivas Reddy BU submissions",
    "How is Srinivas doing?",
    "Srinivas team interviews",

    "Pradeep Kumar students",
    "Pradeep's BU performance",
    "Pradeep Kumar submissions this week",
    "Show Pradeep's team",
    "Pradeep active students",

    "Kiran Reddy's BU",
    "Kiran's team students",
    "Kiran Reddy submissions",
    "Kiran interviews this week",
    "Kiran Reddy BU stats",

    "Vamshi Krishna students",
    "Vamshi's team submissions",
    "Vamshi Krishna BU performance",
    "Show Vamshi's active students",
    "Vamshi submissions today",

    "Naveen Kumar BU",
    "Naveen's team students",
    "Naveen Kumar submissions this week",
    "Naveen's interviews",
    "Show Naveen's BU performance",

    "Suresh Babu students",
    "Suresh's BU submissions",
    "Suresh Babu team performance",
    "Suresh interviews this month",
    "Suresh Babu active count",

    "Rajesh Kumar's team",
    "Rajesh students list",
    "Rajesh Kumar submissions",
    "Rajesh's BU performance this week",
    "Show Rajesh team stats",

    "Mahesh Reddy BU",
    "Mahesh's students",
    "Mahesh Reddy submissions today",
    "Mahesh team interviews",
    "How is Mahesh performing?",

    "Venkat Reddy's BU",
    "Venkat's team students",
    "Venkat Reddy submissions",
    "Venkat interviews scheduled",
    "Venkat BU performance",

    "Sai Krishna team",
    "Sai Krishna students",
    "Sai Krishna submissions this week",
    "Show Sai Krishna's BU",
    "Sai Krishna active students",

    # BU comparison queries
    "Compare all BU submissions",
    "Which BU has most submissions?",
    "Top performing BU this week",
    "BU wise submission count",
    "BU ranking by submissions",
    "Which BU has most active students?",
    "BU performance comparison",
    "Best BU this month",
    "Worst performing BU",
    "Bottom 3 BUs by submissions",
    "Top 5 BUs by interviews",
    "BU wise interview count",
    "Which BU got most confirms?",
    "BU wise confirmed student count",
    "Compare Abhijith vs Vinay submissions",
    "Adithya vs Gulam BU performance",
    "Who is performing better Anil or Ravi?",
    "BU wise active student distribution",
    "Show all BU stats side by side",
    "BU leaderboard",
    "BU performance dashboard",
    "All BU metrics summary",
    "Show me BU performance report",
    "Give me a BU comparison chart",
    "Which BU needs improvement?",
    "Underperforming BUs",
    "BUs with zero submissions today",
    "BUs with no interviews this week",
    "Inactive BUs",
    "Which BU has most pulled out students?",
]

AGGREGATION_COUNT_QUERIES = [
    # Simple counts
    "How many students are active in market?",
    "Total number of submissions today",
    "Count of interviews this week",
    "Number of jobs open",
    "Total employees",
    "How many students have verbal?",
    "Count confirmed students",
    "Number of students on hold",
    "How many pulled out?",
    "Count of not ready students",
    "Total submissions this month",
    "Number of interviews scheduled",
    "How many jobs were posted this week?",
    "Count of H1B students",
    "Number of OPT students",
    "How many CPT students?",
    "GC holder count",
    "Citizen count",
    "H4 EAD student count",
    "Total Java students",
    "Python developer count",
    ".NET students total",
    "How many Salesforce students?",
    "AWS student count",
    "Number of DevOps students",

    # Group by / distribution
    "Students by technology",
    "Submission count by BU",
    "Interviews by type",
    "Students by visa status",
    "Submissions by status",
    "Interviews by final status",
    "Jobs by technology",
    "Employees by department",
    "Students by marketing status",
    "Submissions by technology",
    "Students per BU",
    "Submissions per recruiter",
    "Interviews per BU",
    "Student distribution by status",
    "Visa wise student count",
    "Technology wise submission count",
    "BU wise active students",
    "Manager wise student count",
    "Recruiter wise submission count",
    "Client wise submissions",
    "Vendor wise submissions",
    "Status wise interview breakdown",
    "Type wise interview count",
    "Technology distribution",
    "Status distribution of students",

    # Averages
    "Average rate of submissions",
    "Average submission rate by technology",
    "Average days in market",
    "Average submissions per day",
    "Average interviews per week",
    "Average rate for Java submissions",
    "Average rate for Python",
    "What's the average submission rate?",
    "Mean days in market for active students",
    "Average time to confirmation",
    "Average rate by BU",
    "Average submissions per BU per day",
    "Average interviews per student",
    "Average submissions per student",

    # Min/Max
    "Highest submission rate",
    "Lowest submission rate",
    "Maximum days in market",
    "Minimum rate submitted",
    "Top rate submission",
    "What's the highest rate we got?",
    "Lowest rate submission this week",
    "Who has been in market the longest?",
    "Shortest time in market to confirmation",
    "Maximum submissions in a day",

    # Sums and totals
    "Total submissions by all BUs",
    "Total interviews conducted",
    "Sum of all submissions this month",
    "Total confirmed students count",
    "Grand total active students",
    "Overall submission count YTD",
    "Total interviews YTD",
    "Sum of submissions last 7 days",
    "Total no-shows this month",
    "Total rejections this week",

    # Percentage/ratio queries
    "What percentage of students are active?",
    "Submission to interview conversion rate",
    "Interview to confirmation ratio",
    "Selection rate in interviews",
    "Rejection percentage",
    "What percent are on H1B?",
    "OPT vs H1B percentage",
    "Active vs total student ratio",
    "Confirmation rate",
    "No show percentage",

    # Top N queries
    "Top 5 BUs by submissions",
    "Top 3 technologies by submissions",
    "Top 10 students by submissions",
    "Top recruiters by submission count",
    "Top clients by submissions",
    "Top vendors",
    "Top performing BU",
    "Top 5 managers by confirmations",
    "Bottom 5 BUs",
    "Least active BUs",
    "Most popular technologies",
    "Most demanded skills",
    "Top rated submissions",
    "Top 10 highest rates",
    "Most interviewed students",

    # Complex aggregations
    "Submissions per day trend this week",
    "Interview success rate by technology",
    "Confirmation rate by BU",
    "Average rate by visa type",
    "Submissions by day of week",
    "Which technology has highest rate?",
    "Which BU has best conversion rate?",
    "Technology with most interviews",
    "BU with highest confirmation rate",
    "Recruiter with most submissions this week",
    "Client with most interviews",
    "Vendor giving most submissions",
    "Prime vendor performance",
    "Rate distribution by technology",
    "Submission volume by month",
    "Interview trend by week",
    "Confirmation trend this quarter",
    "Growth rate in submissions",
    "Week over week submission growth",
    "Month over month interview growth",

    # Counts with filters
    "How many Java submissions today?",
    "Count of Python interviews this week",
    "Number of H1B students active",
    "Submissions count for Abhijith's BU",
    "Interview count for Vinay's team",
    "How many confirms in last 30 days?",
    "Number of rejections this month",
    "Count selected interviews this week",
    "How many verbal this month?",
    "Active student count by BU",
    "Submission count above $80 rate",
    "Interviews pending result",
    "How many rescheduled interviews?",
    "No shows this week count",
    "Jobs filled this month count",
]

MULTI_OBJECT_COMPARISON_QUERIES = [
    # Cross-object queries
    "Show students with their submissions",
    "Students who have interviews scheduled",
    "Students with no submissions",
    "Students who haven't had any interviews",
    "Match students to their submissions",
    "Students with submissions but no interviews",
    "Students with interviews but no confirmation",
    "Submissions that led to interviews",
    "From submission to interview pipeline",
    "Full pipeline - submission to interview to confirmation",

    # Conversion funnel
    "Submission to interview conversion",
    "How many submissions converted to interviews?",
    "Interview to offer conversion rate",
    "Full funnel from marketing to placement",
    "Pipeline analysis",
    "Conversion rates by technology",
    "Conversion rates by BU",
    "Which BU converts best?",
    "Best converting technology",
    "Funnel drop-off analysis",

    # Comparison queries
    "Compare submissions vs interviews this week",
    "Ratio of submissions to interviews by BU",
    "Which students have most submissions but no interview?",
    "Students with high submissions low interviews",
    "Over-submitted under-interviewed students",
    "BU comparison - submissions and interviews",
    "Technology comparison across submissions and interviews",
    "Performance comparison - all objects",
    "Activity summary across all categories",
    "Holistic view of BU performance",

    # Relationship queries
    "Show student and their manager",
    "Students under each manager with submission count",
    "Manager wise performance including interviews",
    "BU performance with all metrics",
    "Student journey from marketing to confirmation",
    "Timeline of a student's progress",
    "Complete student activity log",
    "Show me student with all related records",
    "Student with their submissions and interviews",
    "Full history for Java students",

    # Gap analysis
    "Students in market but no submissions",
    "Students with submissions but stuck",
    "Long time in market no activity",
    "Dormant students - no submissions in 30 days",
    "Students needing attention",
    "At risk students",
    "Students with declining activity",
    "BUs with fewer submissions than students",
    "Technology gap - jobs vs students",
    "Demand vs supply by technology",

    # Multi-dimensional analysis
    "BU + Technology performance matrix",
    "Manager + Visa type distribution",
    "Technology + Status breakdown",
    "BU + Date submission heatmap",
    "Client + Technology frequency",
    "Vendor performance by technology",
    "Rate analysis by technology and BU",
    "Interview success by type and technology",
    "Confirmation rate by BU and technology",
    "Comprehensive performance scorecard",

    # Correlation queries
    "Does higher rate correlate with more interviews?",
    "Rate vs interview success",
    "Days in market vs number of submissions",
    "Technology and interview success correlation",
    "BU size vs performance",
    "Is there a pattern in confirmations?",
    "Best day for submissions?",
    "Optimal rate range for getting interviews",
    "Which visa type gets most interviews?",
    "Which technology converts best from submission to interview?",

    # Pipeline status
    "Current pipeline status",
    "How many at each stage?",
    "Pipeline breakdown by BU",
    "Overall pipeline health",
    "Where are students stuck in pipeline?",
    "Bottleneck analysis",
    "Pipeline velocity",
    "Average time at each stage",
    "Pipeline movement this week",
    "New entries vs exits in pipeline",
]

NAME_BASED_STUDENT_LOOKUPS = [
    # Direct name lookups
    "Show me details for student Rahul",
    "Find student Priya",
    "What's the status of Amit?",
    "Show Rajesh's submissions",
    "Where is Suresh in the pipeline?",
    "Get info on student Kiran",
    "Look up Venkat's details",
    "Find student named Srinivas",
    "Show me Naveen's record",
    "What's happening with Mahesh?",
    "Status update on Arun",
    "Show Deepak's interviews",
    "Find all about student Ravi",
    "Check on Anand's progress",
    "Where is Vikram in marketing?",
    "Show me Kumar's details",
    "Look up Patel student",
    "Find student Sharma",
    "Show Singh's record",
    "What about student Reddy?",

    # With context
    "What technology is Rahul on?",
    "Which BU is Priya under?",
    "Who manages Amit?",
    "What's Rajesh's visa status?",
    "How long has Suresh been in market?",
    "What's Kiran's marketing status?",
    "Show Venkat's submission history",
    "How many interviews did Srinivas have?",
    "When was Naveen added to system?",
    "What rate was Mahesh submitted at?",
    "Show me Arun's complete pipeline",
    "Deepak's interview results",
    "What clients was Ravi submitted to?",
    "Anand's vendors list",
    "Vikram's recruiter name",
    "When was Kumar's last submission?",
    "Show Patel's interview schedule",
    "Sharma's manager name",
    "Where was Singh submitted?",
    "Reddy's current status",

    # Partial name matching
    "Students named John",
    "Find all Patels",
    "Show me Reddys in system",
    "List all Kumars",
    "Students with name Singh",
    "Find student starting with A",
    "Students whose name contains Raj",
    "Look up students named Sai",
    "Find all Mohameds",
    "Students named Chris",

    # Name + filter combinations
    "Is Rahul active in market?",
    "Has Priya been submitted anywhere?",
    "Did Amit get any interviews?",
    "Is Rajesh confirmed yet?",
    "How many submissions does Suresh have?",
    "When was Kiran's last interview?",
    "Is Venkat still active?",
    "Was Srinivas pulled out?",
    "Is Naveen on hold?",
    "Did Mahesh get verbal?",

    # Manager-specific lookups
    "Who are Abhijith's students?",
    "Show all of Vinay's students",
    "List Adithya's active students",
    "Gulam's confirmed students",
    "Students assigned to Anil Kumar",
    "Who is managing Rahul?",
    "Which manager has Priya?",
    "Ravi Teja's student list",
    "Srinivas Reddy's students",
    "Pradeep's team members",

    # Name + action queries
    "Submit Rahul's profile",
    "Schedule interview for Priya",
    "Update Amit's status",
    "Move Rajesh to confirmed",
    "Pull out Suresh",
    "Put Kiran on hold",
    "Reactivate Venkat",
    "Transfer Srinivas to different BU",
    "Assign recruiter to Naveen",
    "Update Mahesh's technology",

    # Name with typos/variations
    "Show me Abhijit's team",
    "Aditya's students",
    "Viney Singh team",
    "Gulam's BU",
    "Anil's team submissions",
    "Ravi Teja team",
    "Srinivs Reddy BU",
    "Pradeep's students",
    "Kirans team",
    "Vamsi Krishna BU",
]

REPORT_STYLE_QUERIES = [
    # Daily reports
    "Give me today's summary",
    "Daily report",
    "End of day report",
    "Today's performance summary",
    "Daily dashboard",
    "What did we accomplish today?",
    "Today's numbers",
    "Daily status update",
    "Show me the daily report",
    "EOD summary",

    # Weekly reports
    "Weekly report",
    "This week's summary",
    "Weekly performance review",
    "Show me weekly numbers",
    "Week in review",
    "Weekly dashboard",
    "How did we do this week?",
    "Week's highlights",
    "Weekly submission report",
    "Weekly interview summary",

    # Monthly reports
    "Monthly report",
    "This month's performance",
    "Monthly summary",
    "Month end report",
    "Monthly dashboard",
    "Show me monthly metrics",
    "How was this month?",
    "Monthly business review data",
    "MBR data",
    "Monthly trends",

    # Specific report formats
    "Give me a BU performance report",
    "Technology demand report",
    "Visa category report",
    "Recruiter performance report",
    "Client submission report",
    "Vendor performance report",
    "Pipeline health report",
    "Stale student report",
    "Dormant accounts report",
    "Attrition report - pulled out students",
    "New additions report",
    "Confirmation report",
    "Interview success report",
    "Rate analysis report",
    "Market saturation report",
    "Competitive analysis by technology",
    "Manager scorecard",
    "Team productivity report",
    "Activity report by BU",
    "Comprehensive performance report",

    # Executive summary style
    "Executive summary",
    "High level overview",
    "KPI dashboard",
    "Key metrics this week",
    "Performance scorecard",
    "Quick stats",
    "Snapshot of current status",
    "Overview of all activities",
    "Business health check",
    "Pulse check on marketing",
]

EDGE_CASES_AND_VARIATIONS = [
    # Typos and misspellings
    "shwo me all studnets",
    "lst submissions",
    "interveiws today",
    "submisions this week",
    "actve students",
    "confimed students",
    "intrviews scheduled",
    "performace report",
    "technolgy distribution",
    "submssion count",

    # Very short queries
    "students",
    "submissions",
    "interviews",
    "jobs",
    "active",
    "today",
    "count",
    "report",
    "BU",
    "stats",

    # Very long/detailed queries
    "Can you please show me all the students who are currently active in market and have been there for more than 30 days and are on H1B visa with Java technology under Abhijith Reddy's BU?",
    "I need a complete report of all submissions made this week by all BUs showing the student name, technology, client, rate, and current status, sorted by submission date",
    "Show me the full interview pipeline for students who were submitted in January and got interviews in February with their final status and which ones eventually got confirmed",
    "Give me a detailed breakdown of each BU's performance this month including total students, active in market, submissions, interviews, and confirmations",
    "I want to see all students who have been in market for more than 60 days with no submissions at all and what technology they are on and who their manager is",

    # Ambiguous queries
    "show me everything",
    "what's the status?",
    "how are we doing?",
    "any updates?",
    "what's new?",
    "tell me something",
    "what should I know?",
    "anything interesting?",
    "how's it going?",
    "what's happening?",

    # Negative/exclusion queries
    "Students NOT on Java",
    "Everyone except H1B",
    "Submissions without interviews",
    "Students who haven't been submitted",
    "BUs with no submissions today",
    "Students not under Abhijith",
    "Non-active students",
    "Exclude confirmed students",
    "Everyone but pulled out",
    "Technologies with no submissions",

    # Comparison with numbers
    "Students with more than 5 submissions",
    "BUs with less than 3 submissions today",
    "Rate above 100",
    "Students in market over 45 days",
    "Interviews more than 2 per student",
    "BUs with at least 10 active students",
    "Submissions with rate between 70 and 90",
    "More than 20 days no activity",
    "At least 5 interviews this week",
    "Less than 2 submissions per day",

    # Questions with special characters
    "what's the .NET count?",
    "AI/ML students?",
    "C# developers",
    "students @ $80+",
    "rate > 90?",
    "rate >= 80 and <= 100",
    "50+ days in market",
    "#1 BU",
    "H4-EAD students",
    "full-stack devs",

    # Multiple questions in one
    "How many students are active and how many submissions today?",
    "Show me interviews today and also pending results from last week",
    "What's our active count and confirmation rate?",
    "List top BU and also worst BU",
    "Submissions and interviews both for this week",

    # Polite/conversational
    "Hi, can you show me the student list please?",
    "Hey, what's our submission count today?",
    "Please give me the interview schedule",
    "Thanks, now show me the BU performance",
    "Could you please pull up Abhijith's team data?",
    "I'd like to see the weekly report please",
    "Would it be possible to get a technology breakdown?",
    "Excuse me, how many active students?",
    "Hello! Show me today's numbers",
    "Good morning, daily report please",

    # Follow-up style (standalone but contextual)
    "and what about interviews?",
    "same thing but for last week",
    "now show me by technology",
    "break it down by BU",
    "what about Vinay's team?",
    "and the confirmation rate?",
    "show me the trend",
    "compare it with last month",
    "filter by Java only",
    "include pulled out also",

    # Industry jargon / shorthand
    "bench strength",
    "hot list",
    "pipeline",
    "funnel",
    "conversion",
    "hit ratio",
    "fill rate",
    "time to fill",
    "bench aging",
    "market readiness",

    # Questions about the system itself
    "What data do you have?",
    "What can I ask you?",
    "What objects are in the database?",
    "What fields does student have?",
    "Can you query submissions?",
    "Do you have interview data?",
    "What reports can you generate?",
    "What's your data range?",
    "How current is the data?",
    "When was data last synced?",
]

# Additional queries to push past 1000
ADDITIONAL_MIXED_QUERIES = [
    # Recruiter performance
    "Who is the top recruiter?",
    "Recruiter wise submissions this week",
    "Which recruiter has most submissions?",
    "Recruiter performance ranking",
    "Show all recruiters and their submission counts",
    "Best recruiter this month",
    "Recruiter leaderboard",
    "Submissions by each recruiter",
    "Who submitted the most today?",
    "Recruiter productivity report",
    "Average submissions per recruiter",
    "Inactive recruiters - no submissions in 7 days",
    "Recruiter with highest confirmation rate",
    "Which recruiter works with most BUs?",
    "New recruiters added this month",

    # Client analysis
    "Top clients by submissions",
    "Which client has most submissions?",
    "Client wise submission breakdown",
    "New clients this month",
    "Repeat clients",
    "Client with most interviews",
    "Client submission trend",
    "Top 10 clients",
    "Client rate analysis",
    "Which client pays the highest rate?",
    "Clients with pending interviews",
    "Client response rate",
    "Clients by technology demand",
    "Which clients need Java?",
    "Clients requesting Python",

    # Vendor analysis
    "Top vendors by submissions",
    "Vendor wise submission count",
    "Prime vendor performance",
    "Which vendor gives most submissions?",
    "Vendor rate comparison",
    "Best vendors this month",
    "Vendor submission trend",
    "New vendors added",
    "Vendor interview conversion",
    "Which vendor leads to most interviews?",
    "Vendor performance by technology",
    "Top prime vendors",
    "Vendor contact list",
    "Inactive vendors",
    "Vendor ranking",

    # Rate analysis
    "Average submission rate",
    "Rate trends this month",
    "Highest rate submitted today",
    "Rate distribution",
    "Rate by technology",
    "Java average rate",
    "Python rate range",
    ".NET typical rates",
    "What's the going rate for Salesforce?",
    "Rate comparison by BU",
    "Are rates going up or down?",
    "Rate trend over last 3 months",
    "Submissions above market rate",
    "Below market rate submissions",
    "Rate vs technology analysis",
    "Best rates this week",
    "Rate negotiation data",
    "Premium rate submissions",
    "Budget friendly submissions",
    "Rate brackets distribution",

    # Visa-specific deep dives
    "H1B student pipeline",
    "OPT students about to expire",
    "CPT vs OPT comparison",
    "GC holders performance",
    "Citizens in market",
    "H4 EAD challenges",
    "Visa type vs submission rate",
    "Which visa gets most interviews?",
    "Visa wise confirmation rate",
    "H1B transfer students",
    "Visa status distribution by technology",
    "OPT STEM extension students",
    "Visa category by BU",
    "H1B vs GC rate difference",
    "Citizens vs H1B rate comparison",

    # Technology deep dives
    "Java market demand",
    "Python trend this quarter",
    "Is .NET declining?",
    "Salesforce market status",
    "AWS vs Azure demand",
    "DevOps placement rate",
    "Data Engineering growth",
    "React vs Angular demand",
    "Full Stack market outlook",
    "SAP current demand",
    "ServiceNow opportunities",
    "Cybersecurity demand trend",
    "AI/ML market size",
    "Tableau vs Power BI",
    "Selenium vs Manual Testing",
    "Technology saturation analysis",
    "Emerging technologies",
    "Declining technologies",
    "Technology with fastest placement",
    "Best technology for quick confirmation",

    # Time-in-market analysis
    "Average days in market by technology",
    "Students over 100 days in market",
    "Quick placements - under 15 days",
    "Market aging report",
    "Days in market distribution",
    "Which technology places fastest?",
    "Slowest placing technology",
    "BU wise average days in market",
    "Days in market by visa type",
    "Students between 30-60 days in market",
    "Critical students - over 90 days",
    "Market aging by manager",
    "Average time to first submission",
    "Average time to first interview",
    "Time from interview to confirmation",

    # Onsite/Offshore manager queries
    "Onsite manager performance",
    "Offshore manager performance",
    "Which onsite manager has most confirms?",
    "Offshore manager wise submissions",
    "Onsite vs offshore comparison",
    "Manager productivity ranking",
    "Show onsite managers and their students",
    "Offshore managers with active students",
    "Manager with longest market time students",
    "Best performing onsite manager",

    # Status transition queries
    "Students who moved from active to verbal today",
    "Recent status changes",
    "Students who got confirmed this week",
    "Who moved to on hold recently?",
    "Status change history",
    "Students whose status changed this month",
    "Verbal to confirmed conversion time",
    "How long from verbal to confirm?",
    "Students stuck in verbal for too long",
    "Active to pulled out - why?",

    # Specific scenario queries
    "Students ready for Salesforce projects",
    "Who can we submit for AWS roles?",
    "Available Python developers for immediate submission",
    "Students matching Java Full Stack requirements",
    "Who's available for .NET positions?",
    "DevOps candidates for submission",
    "Data Engineers ready for market",
    "React developers available now",
    "Business Analysts in market",
    "Testers available for submission",

    # Quality metrics
    "Interview success rate",
    "Overall selection rate",
    "Rejection reasons analysis",
    "No show rate",
    "Rescheduling frequency",
    "First interview success rate",
    "Multiple interview students",
    "Single submission confirmations",
    "Quality of submissions by BU",
    "BU with highest selection rate",

    # Forecasting style
    "Expected confirmations this month",
    "Projected submissions for this week",
    "At current rate how many confirms this month?",
    "Based on trends what's the expected interview count?",
    "Pipeline forecast",
    "Upcoming interview results",
    "Expected placements next week",
    "Target vs actual submissions",
    "Are we on track this month?",
    "Will we hit our targets?",

    # Historical analysis
    "Best performing month this year",
    "Historical submission data",
    "Interview trend over 6 months",
    "Confirmation history",
    "All time highest submission day",
    "Record breaking week",
    "Historical BU performance",
    "Year over year growth",
    "Long term trends",
    "Performance history by technology",
]


# ---------------------------------------------------------------------------
# Combine all questions into a single list with category labels
# ---------------------------------------------------------------------------

ALL_QUESTIONS = []

_categories = [
    ("simple_single_object", SIMPLE_SINGLE_OBJECT_QUERIES),
    ("filtered_date", FILTERED_DATE_QUERIES),
    ("bu_specific", BU_SPECIFIC_QUERIES),
    ("aggregation_count", AGGREGATION_COUNT_QUERIES),
    ("multi_object_comparison", MULTI_OBJECT_COMPARISON_QUERIES),
    ("name_based_lookup", NAME_BASED_STUDENT_LOOKUPS),
    ("report_style", REPORT_STYLE_QUERIES),
    ("edge_cases", EDGE_CASES_AND_VARIATIONS),
    ("additional_mixed", ADDITIONAL_MIXED_QUERIES),
]

for category, questions in _categories:
    for q in questions:
        ALL_QUESTIONS.append((category, q))


# ---------------------------------------------------------------------------
# Pytest fixtures and helpers
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def http_client():
    """Create a shared HTTP client for all tests."""
    headers = {}
    if AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {AUTH_TOKEN}"
    with httpx.Client(base_url=BASE_URL, timeout=60.0, headers=headers) as client:
        yield client


def send_question(client: httpx.Client, question: str) -> dict:
    """Send a question to the chat API and return the response."""
    payload = {
        "session_id": str(uuid.uuid4()),
        "question": question,
    }
    response = client.post("/api/chat", json=payload)
    return {
        "status_code": response.status_code,
        "body": response.json() if response.status_code == 200 else None,
        "text": response.text,
    }


def validate_response(result: dict, question: str):
    """Validate that the API returned a meaningful response."""
    assert result["status_code"] == 200, (
        f"Question '{question}' returned status {result['status_code']}: {result['text']}"
    )
    body = result["body"]
    assert body is not None, f"Question '{question}' returned null body"
    assert "answer" in body, f"Question '{question}' missing 'answer' field"
    assert body["answer"], f"Question '{question}' returned empty answer"
    # Answer should have some substance (more than just a few characters)
    assert len(body["answer"]) > 5, (
        f"Question '{question}' returned suspiciously short answer: '{body['answer']}'"
    )


# ---------------------------------------------------------------------------
# Parametrized test - runs every question as a separate test case
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "category,question",
    ALL_QUESTIONS,
    ids=[f"{cat}_{i:04d}" for i, (cat, _) in enumerate(ALL_QUESTIONS)],
)
def test_chat_question(http_client, category, question):
    """Test that each question returns a valid response from the chat API."""
    result = send_question(http_client, question)
    validate_response(result, question)


# ---------------------------------------------------------------------------
# Category-level batch tests (for running categories independently)
# ---------------------------------------------------------------------------

class TestSimpleSingleObjectQueries:
    """Tests for simple single-object queries (200+ questions)."""

    @pytest.mark.parametrize("question", SIMPLE_SINGLE_OBJECT_QUERIES)
    def test_question(self, http_client, question):
        result = send_question(http_client, question)
        validate_response(result, question)


class TestFilteredDateQueries:
    """Tests for date-filtered queries (150+ questions)."""

    @pytest.mark.parametrize("question", FILTERED_DATE_QUERIES)
    def test_question(self, http_client, question):
        result = send_question(http_client, question)
        validate_response(result, question)


class TestBUSpecificQueries:
    """Tests for BU-specific queries (150+ questions)."""

    @pytest.mark.parametrize("question", BU_SPECIFIC_QUERIES)
    def test_question(self, http_client, question):
        result = send_question(http_client, question)
        validate_response(result, question)


class TestAggregationCountQueries:
    """Tests for aggregation and count queries (150+ questions)."""

    @pytest.mark.parametrize("question", AGGREGATION_COUNT_QUERIES)
    def test_question(self, http_client, question):
        result = send_question(http_client, question)
        validate_response(result, question)


class TestMultiObjectComparisonQueries:
    """Tests for multi-object and comparison queries (100+ questions)."""

    @pytest.mark.parametrize("question", MULTI_OBJECT_COMPARISON_QUERIES)
    def test_question(self, http_client, question):
        result = send_question(http_client, question)
        validate_response(result, question)


class TestNameBasedLookups:
    """Tests for name-based student lookups (100+ questions)."""

    @pytest.mark.parametrize("question", NAME_BASED_STUDENT_LOOKUPS)
    def test_question(self, http_client, question):
        result = send_question(http_client, question)
        validate_response(result, question)


class TestReportStyleQueries:
    """Tests for report-style queries (50+ questions)."""

    @pytest.mark.parametrize("question", REPORT_STYLE_QUERIES)
    def test_question(self, http_client, question):
        result = send_question(http_client, question)
        validate_response(result, question)


class TestEdgeCasesAndVariations:
    """Tests for edge cases and variations (100+ questions)."""

    @pytest.mark.parametrize("question", EDGE_CASES_AND_VARIATIONS)
    def test_question(self, http_client, question):
        result = send_question(http_client, question)
        validate_response(result, question)


class TestAdditionalMixedQueries:
    """Tests for additional mixed queries."""

    @pytest.mark.parametrize("question", ADDITIONAL_MIXED_QUERIES)
    def test_question(self, http_client, question):
        result = send_question(http_client, question)
        validate_response(result, question)


# ---------------------------------------------------------------------------
# Smoke test - just verify the API is reachable
# ---------------------------------------------------------------------------

class TestAPIHealth:
    """Basic connectivity tests."""

    def test_api_reachable(self, http_client):
        """Verify the API server is running and reachable."""
        response = http_client.get("/")
        # Accept any non-connection-error response
        assert response.status_code in (200, 307, 404, 405)

    def test_chat_endpoint_exists(self, http_client):
        """Verify the chat endpoint accepts POST requests."""
        payload = {"session_id": "test-health", "question": "hello"}
        response = http_client.post("/api/chat", json=payload)
        # Should not be 404 or 405
        assert response.status_code not in (404, 405), (
            f"Chat endpoint returned {response.status_code}"
        )

    def test_basic_question(self, http_client):
        """Verify a basic question returns a proper response."""
        result = send_question(http_client, "How many students are there?")
        validate_response(result, "How many students are there?")


# ---------------------------------------------------------------------------
# Summary statistics (not a test, just info)
# ---------------------------------------------------------------------------

def test_question_count():
    """Verify we have 1000+ questions in our test suite."""
    total = len(ALL_QUESTIONS)
    print(f"\n{'='*60}")
    print(f"Total questions in test suite: {total}")
    print(f"{'='*60}")
    for cat, questions in _categories:
        print(f"  {cat:30s}: {len(questions):4d} questions")
    print(f"{'='*60}")
    assert total >= 1000, f"Expected 1000+ questions, got {total}"
