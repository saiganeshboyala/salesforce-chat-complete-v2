"""
Test SOQL-to-SQL conversion — the critical layer that determines
whether queries run on PostgreSQL or fail.

This tests soql_to_sql() and soql_to_sql_with_joins() with representative
SOQL patterns that the AI generates for user questions.

Run: python tests/test_soql_conversion.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database.query import soql_to_sql, soql_to_sql_with_joins

# Each entry: (description, soql_input, should_convert: bool, must_contain: list[str])
# should_convert=True means soql_to_sql should return a valid SQL (not None)
# must_contain = substrings that MUST appear in the output SQL

TEST_CASES = [
    # ── Simple SELECT queries ──
    ("Basic student select",
     "SELECT Name, Technology__c, Student_Marketing_Status__c FROM Student__c LIMIT 100",
     True, ['"Student__c"', '"Name"', '"Technology__c"']),

    ("All submissions",
     "SELECT Student_Name__c, BU_Name__c, Client_Name__c FROM Submissions__c LIMIT 50",
     True, ['"Submissions__c"', '"Student_Name__c"', '"BU_Name__c"']),

    ("All interviews",
     "SELECT Name, Type__c, Final_Status__c, Interview_Date__c FROM Interviews__c LIMIT 100",
     True, ['"Interviews__c"', '"Type__c"', '"Final_Status__c"']),

    ("All employees",
     "SELECT Name, Department__c, Status__c FROM Employee__c LIMIT 100",
     True, ['"Employee__c"', '"Name"']),

    ("All jobs",
     "SELECT Name, Job_Title__c, Company__c, Status__c FROM Job__c LIMIT 100",
     True, ['"Job__c"', '"Job_Title__c"']),

    # ── WHERE with string filter ──
    ("Students with status filter",
     "SELECT Name, Technology__c FROM Student__c WHERE Student_Marketing_Status__c = 'Active in Market' LIMIT 200",
     True, ['"Student_Marketing_Status__c"', "'Active in Market'"]),

    ("Students with technology filter",
     "SELECT Name, Student_Marketing_Status__c FROM Student__c WHERE Technology__c = 'Java' LIMIT 100",
     True, ['"Technology__c"', "'Java'"]),

    ("Students with visa filter",
     "SELECT Name, Technology__c FROM Student__c WHERE Marketing_Visa_Status__c = 'H1B' LIMIT 100",
     True, ['"Marketing_Visa_Status__c"', "'H1B'"]),

    ("Submissions by BU name",
     "SELECT Student_Name__c, Client_Name__c FROM Submissions__c WHERE BU_Name__c = 'Abhijith Reddy Palreddy' LIMIT 100",
     True, ['"BU_Name__c"', "'Abhijith Reddy Palreddy'"]),

    ("Interviews by final status",
     "SELECT Name, Type__c FROM Interviews__c WHERE Final_Status__c = 'Selected' LIMIT 100",
     True, ['"Final_Status__c"', "'Selected'"]),

    # ── WHERE with LIKE ──
    ("Student name LIKE search",
     "SELECT Name, Technology__c FROM Student__c WHERE Name LIKE '%Aravind%' LIMIT 50",
     True, ['"Name"', "LIKE", "'%Aravind%'"]),

    ("BU name LIKE search",
     "SELECT Student_Name__c FROM Submissions__c WHERE BU_Name__c LIKE '%Vinay%' LIMIT 100",
     True, ['"BU_Name__c"', "LIKE", "'%Vinay%'"]),

    # ── Date literals: TODAY ──
    ("Submissions today",
     "SELECT Student_Name__c, BU_Name__c, Client_Name__c FROM Submissions__c WHERE Submission_Date__c = TODAY LIMIT 200",
     True, ["CURRENT_DATE", '"Submission_Date__c"']),

    ("Interviews today",
     "SELECT Name, Type__c, Final_Status__c FROM Interviews__c WHERE Interview_Date__c = TODAY LIMIT 200",
     True, ["CURRENT_DATE", '"Interview_Date__c"']),

    # ── Date literals: YESTERDAY ──
    ("Submissions yesterday",
     "SELECT Student_Name__c, BU_Name__c FROM Submissions__c WHERE Submission_Date__c = YESTERDAY LIMIT 200",
     True, ["CURRENT_DATE", "1 day", '"Submission_Date__c"']),

    # ── Date literals: THIS_WEEK ──
    ("Submissions this week",
     "SELECT Student_Name__c, BU_Name__c, Client_Name__c FROM Submissions__c WHERE Submission_Date__c = THIS_WEEK ORDER BY BU_Name__c LIMIT 2000",
     True, ["DATE_TRUNC('week'", '"Submission_Date__c"']),

    ("Interviews this week",
     "SELECT Name, Type__c, Final_Status__c FROM Interviews__c WHERE Interview_Date__c = THIS_WEEK LIMIT 200",
     True, ["DATE_TRUNC('week'", '"Interview_Date__c"']),

    # ── Date literals: LAST_WEEK ──
    ("Submissions last week",
     "SELECT Student_Name__c, BU_Name__c FROM Submissions__c WHERE Submission_Date__c = LAST_WEEK LIMIT 2000",
     True, ["DATE_TRUNC('week'", "- INTERVAL '1 week'"]),

    # ── Date literals: THIS_MONTH ──
    ("Submissions this month",
     "SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE Submission_Date__c = THIS_MONTH ORDER BY BU_Name__c LIMIT 2000",
     True, ["DATE_TRUNC('month'", '"Submission_Date__c"']),

    ("Interviews this month",
     "SELECT Name, Type__c, Final_Status__c, Interview_Date__c FROM Interviews__c WHERE Interview_Date__c = THIS_MONTH LIMIT 2000",
     True, ["DATE_TRUNC('month'", '"Interview_Date__c"']),

    # ── Date literals: LAST_MONTH ──
    ("Submissions last month",
     "SELECT Student_Name__c, BU_Name__c FROM Submissions__c WHERE Submission_Date__c = LAST_MONTH LIMIT 2000",
     True, ["DATE_TRUNC('month'", "- INTERVAL '1 month'"]),

    # ── Date literals: THIS_YEAR ──
    ("Submissions this year",
     "SELECT Student_Name__c, BU_Name__c FROM Submissions__c WHERE Submission_Date__c = THIS_YEAR LIMIT 2000",
     True, ["DATE_TRUNC('year'", '"Submission_Date__c"']),

    # ── LAST_N_DAYS ──
    ("Submissions last 7 days",
     "SELECT Student_Name__c, BU_Name__c FROM Submissions__c WHERE Submission_Date__c >= LAST_N_DAYS:7 LIMIT 2000",
     True, ["CURRENT_DATE - INTERVAL '7 days'"]),

    ("Submissions last 14 days",
     "SELECT Student_Name__c, BU_Name__c FROM Submissions__c WHERE Submission_Date__c >= LAST_N_DAYS:14 LIMIT 2000",
     True, ["CURRENT_DATE - INTERVAL '14 days'"]),

    ("Submissions last 30 days",
     "SELECT Student_Name__c, BU_Name__c, Client_Name__c FROM Submissions__c WHERE Submission_Date__c >= LAST_N_DAYS:30 LIMIT 2000",
     True, ["CURRENT_DATE - INTERVAL '30 days'"]),

    ("Interviews last 20 days",
     "SELECT Name, Type__c, Final_Status__c FROM Interviews__c WHERE Interview_Date__c >= LAST_N_DAYS:20 LIMIT 2000",
     True, ["CURRENT_DATE - INTERVAL '20 days'"]),

    # ── COUNT queries ──
    ("Count all students",
     "SELECT COUNT() FROM Student__c",
     True, ["COUNT(*)", '"Student__c"']),

    ("Count submissions this month",
     "SELECT COUNT() FROM Submissions__c WHERE Submission_Date__c = THIS_MONTH",
     True, ["COUNT(*)", "DATE_TRUNC('month'"]),

    ("Count interviews today",
     "SELECT COUNT() FROM Interviews__c WHERE Interview_Date__c = TODAY",
     True, ["COUNT(*)", "CURRENT_DATE"]),

    ("Count with ID",
     "SELECT COUNT(Id) FROM Submissions__c WHERE Submission_Date__c = THIS_WEEK",
     True, ["COUNT(*)", "DATE_TRUNC('week'"]),

    # ── GROUP BY queries ──
    ("Group submissions by BU",
     "SELECT BU_Name__c, COUNT(Id) cnt FROM Submissions__c WHERE Submission_Date__c = THIS_MONTH GROUP BY BU_Name__c ORDER BY COUNT(Id) DESC LIMIT 30",
     True, ['"BU_Name__c"', "COUNT(*)", "GROUP BY", "DATE_TRUNC('month'"]),

    ("Group students by technology",
     "SELECT Technology__c, COUNT(Id) cnt FROM Student__c WHERE Student_Marketing_Status__c = 'Active in Market' GROUP BY Technology__c ORDER BY COUNT(Id) DESC",
     True, ['"Technology__c"', "COUNT(*)", "GROUP BY"]),

    ("Group interviews by status",
     "SELECT Final_Status__c, COUNT(Id) cnt FROM Interviews__c WHERE Interview_Date__c = THIS_WEEK GROUP BY Final_Status__c",
     True, ['"Final_Status__c"', "COUNT(*)", "GROUP BY"]),

    ("Group interviews by type",
     "SELECT Type__c, COUNT(Id) cnt FROM Interviews__c WHERE Interview_Date__c = THIS_MONTH GROUP BY Type__c ORDER BY COUNT(Id) DESC",
     True, ['"Type__c"', "COUNT(*)", "GROUP BY"]),

    ("Group students by visa",
     "SELECT Marketing_Visa_Status__c, COUNT(Id) cnt FROM Student__c GROUP BY Marketing_Visa_Status__c ORDER BY COUNT(Id) DESC",
     True, ['"Marketing_Visa_Status__c"', "COUNT(*)", "GROUP BY"]),

    ("Group submissions by recruiter",
     "SELECT Recruiter_Name__c, COUNT(Id) cnt FROM Submissions__c WHERE Submission_Date__c = THIS_MONTH GROUP BY Recruiter_Name__c ORDER BY COUNT(Id) DESC LIMIT 20",
     True, ['"Recruiter_Name__c"', "COUNT(*)", "GROUP BY"]),

    # ── ORDER BY ──
    ("Order by date desc",
     "SELECT Student_Name__c, BU_Name__c, Submission_Date__c FROM Submissions__c ORDER BY Submission_Date__c DESC LIMIT 50",
     True, ['"Submission_Date__c"', "DESC"]),

    ("Order by created date",
     "SELECT Name, Technology__c, CreatedDate FROM Student__c ORDER BY CreatedDate DESC LIMIT 100",
     True, ['"CreatedDate"', "DESC"]),

    # ── Multiple WHERE conditions ──
    ("BU + date filter",
     "SELECT Student_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE BU_Name__c = 'Abhijith Reddy Palreddy' AND Submission_Date__c = THIS_MONTH ORDER BY Submission_Date__c DESC LIMIT 200",
     True, ['"BU_Name__c"', "'Abhijith Reddy Palreddy'", "DATE_TRUNC('month'"]),

    ("Status + technology filter",
     "SELECT Name, BU_Name__c FROM Student__c WHERE Student_Marketing_Status__c = 'Active in Market' AND Technology__c = 'Java' LIMIT 100",
     True, ['"Student_Marketing_Status__c"', "'Active in Market'", '"Technology__c"', "'Java'"]),

    ("BU + status + date",
     "SELECT Student_Name__c, Client_Name__c FROM Submissions__c WHERE BU_Name__c = 'Vinay Singh' AND Submission_Date__c >= LAST_N_DAYS:7 LIMIT 200",
     True, ['"BU_Name__c"', "'Vinay Singh'", "CURRENT_DATE - INTERVAL '7 days'"]),

    ("Interview status + date",
     "SELECT Name, Final_Status__c FROM Interviews__c WHERE Final_Status__c = 'Selected' AND Interview_Date__c = THIS_MONTH LIMIT 200",
     True, ['"Final_Status__c"', "'Selected'", "DATE_TRUNC('month'"]),

    # ── Relationship queries (__r) ──
    ("Student with manager relationship",
     "SELECT Name, Technology__c, Manager__r.Name FROM Student__c WHERE Student_Marketing_Status__c = 'Active in Market' LIMIT 100",
     True, ['"Student__c"']),

    ("Interviews with student relationship",
     "SELECT Student__r.Name, Type__c, Final_Status__c FROM Interviews__c WHERE Interview_Date__c = THIS_WEEK LIMIT 200",
     True, ["JOIN"]),

    # ── NULL checks ──
    ("Null check",
     "SELECT Name, Technology__c FROM Student__c WHERE Phone__c = null LIMIT 50",
     True, ["IS NULL"]),

    ("Not null check",
     "SELECT Name, Technology__c FROM Student__c WHERE Email__c != null LIMIT 50",
     True, ["IS NOT NULL"]),

    # ── IN clause ──
    ("IN clause with statuses",
     "SELECT Name, Student_Marketing_Status__c FROM Student__c WHERE Student_Marketing_Status__c IN ('Active in Market', 'Verbal Confirmation') LIMIT 200",
     True, ["IN", "'Active in Market'", "'Verbal Confirmation'"]),

    # ── Days in market comparison ──
    ("Days in market greater than",
     "SELECT Name, Technology__c, Days_in_Market_Business__c FROM Student__c WHERE Days_in_Market_Business__c > 30 AND Student_Marketing_Status__c = 'Active in Market' ORDER BY Days_in_Market_Business__c DESC LIMIT 100",
     True, ['"Days_in_Market_Business__c"', "> 30"]),

    ("Days in market greater than 60",
     "SELECT Name, Technology__c, Days_in_Market_Business__c, BU_Name__c FROM Student__c WHERE Days_in_Market_Business__c > 60 AND Student_Marketing_Status__c = 'Active in Market' ORDER BY Days_in_Market_Business__c DESC LIMIT 200",
     True, ['"Days_in_Market_Business__c"', "> 60"]),

    # ── Complex GROUP BY with date + BU ──
    ("BU wise submissions this week with count",
     "SELECT BU_Name__c, COUNT(Id) cnt FROM Submissions__c WHERE Submission_Date__c = THIS_WEEK GROUP BY BU_Name__c ORDER BY COUNT(Id) DESC",
     True, ['"BU_Name__c"', "COUNT(*)", "GROUP BY", "DATE_TRUNC('week'"]),

    ("BU wise interviews this month",
     "SELECT BU_Name__c, COUNT(Id) cnt FROM Interviews__c WHERE Interview_Date__c = THIS_MONTH GROUP BY BU_Name__c ORDER BY COUNT(Id) DESC",
     True, ['"BU_Name__c"', "COUNT(*)", "GROUP BY", "DATE_TRUNC('month'"]),

    # ── Subquery patterns ──
    ("Students with specific BU submissions",
     "SELECT Name, Technology__c FROM Student__c WHERE Name IN (SELECT Student_Name__c FROM Submissions__c WHERE Submission_Date__c = THIS_MONTH) LIMIT 100",
     True, ['"Student__c"', "SELECT"]),

    # ── Mixed case / edge cases ──
    ("Lowercase from",
     "SELECT Name from Student__c LIMIT 10",
     True, ['"Student__c"']),

    ("Multiple spaces",
     "SELECT  Name,  Technology__c  FROM  Student__c  LIMIT 100",
     True, ['"Student__c"', '"Name"']),

    # ── CreatedDate comparisons ──
    ("Created this month",
     "SELECT Name, Technology__c FROM Student__c WHERE CreatedDate = THIS_MONTH ORDER BY CreatedDate DESC LIMIT 200",
     True, ["DATE_TRUNC('month'", '"CreatedDate"']),

    ("Created last 7 days",
     "SELECT Name, Technology__c FROM Student__c WHERE CreatedDate >= LAST_N_DAYS:7 ORDER BY CreatedDate DESC LIMIT 100",
     True, ["CURRENT_DATE - INTERVAL '7 days'"]),

    # ── BU_Performance__c ──
    ("BU Performance query",
     "SELECT BU_Name__c, Name FROM BU_Performance__c LIMIT 50",
     True, ['"BU_Performance__c"']),

    # ── COUNT with alias ──
    ("Count with cnt alias",
     "SELECT BU_Name__c, COUNT(Id) cnt FROM Submissions__c GROUP BY BU_Name__c",
     True, ["COUNT(*) AS cnt", "GROUP BY"]),

    # ── Date >= TODAY ──
    ("Future dates",
     "SELECT Name, Interview_Date__c FROM Interviews__c WHERE Interview_Date__c >= TODAY LIMIT 100",
     True, ["CURRENT_DATE", '"Interview_Date__c"']),

    # ── Date <= TODAY ──
    ("Past dates",
     "SELECT Name, Interview_Date__c FROM Interviews__c WHERE Interview_Date__c <= TODAY LIMIT 100",
     True, ["CURRENT_DATE", '"Interview_Date__c"']),

    # ── LAST_N_DAYS in WHERE with other conditions ──
    ("LAST_N_DAYS with BU filter",
     "SELECT Student_Name__c, Client_Name__c FROM Submissions__c WHERE BU_Name__c = 'Gulam Siddiqui' AND Submission_Date__c >= LAST_N_DAYS:14 ORDER BY Submission_Date__c DESC LIMIT 200",
     True, ["'Gulam Siddiqui'", "CURRENT_DATE - INTERVAL '14 days'"]),

    # ── THIS_QUARTER ──
    ("Submissions this quarter",
     "SELECT Student_Name__c, BU_Name__c FROM Submissions__c WHERE Submission_Date__c = THIS_QUARTER LIMIT 2000",
     True, ["DATE_TRUNC('quarter'"]),

    # ── LAST_QUARTER ──
    ("Submissions last quarter",
     "SELECT Student_Name__c, BU_Name__c FROM Submissions__c WHERE Submission_Date__c = LAST_QUARTER LIMIT 2000",
     True, ["DATE_TRUNC('quarter'", "- INTERVAL '3 months'"]),
]


def run_tests():
    """Run all conversion tests and report results."""
    passed = 0
    failed = 0
    failures = []

    print(f"\n{'='*70}")
    print(f"  SOQL -> SQL CONVERSION TEST SUITE ({len(TEST_CASES)} tests)")
    print(f"{'='*70}\n")

    for desc, soql, should_convert, must_contain in TEST_CASES:
        # Determine which converter to use
        if '__r' in soql and 'IN (SELECT' not in soql:
            result = soql_to_sql_with_joins(soql)
        else:
            result = soql_to_sql(soql)

        # Check if conversion succeeded/failed as expected
        if should_convert and result is None:
            failed += 1
            failures.append({
                "desc": desc,
                "soql": soql,
                "error": "Conversion returned None (should have succeeded)",
                "result": None,
            })
            print(f"  X FAIL: {desc}")
            print(f"         -> Conversion returned None")
            continue

        if not should_convert and result is not None:
            failed += 1
            failures.append({
                "desc": desc,
                "soql": soql,
                "error": f"Conversion should have returned None but got: {result[:100]}",
                "result": result,
            })
            print(f"  X FAIL: {desc}")
            print(f"         -> Should not convert but got SQL")
            continue

        if not should_convert and result is None:
            passed += 1
            continue

        # Check must_contain substrings
        missing = []
        for substring in must_contain:
            if substring not in result:
                missing.append(substring)

        if missing:
            failed += 1
            failures.append({
                "desc": desc,
                "soql": soql,
                "error": f"Missing in output: {missing}",
                "result": result,
            })
            print(f"  X FAIL: {desc}")
            print(f"         SOQL: {soql[:100]}")
            print(f"         SQL:  {result[:150]}")
            print(f"         Missing: {missing}")
        else:
            passed += 1
            print(f"  OK PASS: {desc}")

    # Summary
    print(f"\n{'='*70}")
    print(f"  RESULTS: {passed} passed, {failed} failed, {len(TEST_CASES)} total")
    print(f"{'='*70}")

    if failures:
        print(f"\n{'─'*70}")
        print(f"  FAILURES DETAIL:")
        print(f"{'─'*70}")
        for i, f in enumerate(failures, 1):
            print(f"\n  [{i}] {f['desc']}")
            print(f"      SOQL: {f['soql']}")
            print(f"      Error: {f['error']}")
            if f['result']:
                print(f"      Got:  {f['result'][:200]}")

    return passed, failed, failures


if __name__ == "__main__":
    passed, failed, failures = run_tests()
    sys.exit(0 if failed == 0 else 1)
