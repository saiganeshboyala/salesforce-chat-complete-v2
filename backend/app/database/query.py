"""
PostgreSQL query executor — replacement for SOQL queries.
Accepts SQL queries and returns results in the same format as execute_soql().
"""
import logging
import re
import time
from collections import OrderedDict
from sqlalchemy import text
from app.database.engine import async_session

logger = logging.getLogger(__name__)

_CACHE_TTL = 60
_CACHE_MAX = 200
_cache: "OrderedDict[str, tuple[float, dict]]" = OrderedDict()


def _cache_key(query):
    return re.sub(r"\s+", " ", query.strip()).upper()


def _cache_get(query):
    key = _cache_key(query)
    entry = _cache.get(key)
    if not entry:
        return None
    ts, value = entry
    if time.time() - ts > _CACHE_TTL:
        _cache.pop(key, None)
        return None
    _cache.move_to_end(key)
    return value


def _cache_put(query, value):
    if "error" in value:
        return
    key = _cache_key(query)
    _cache[key] = (time.time(), value)
    _cache.move_to_end(key)
    while len(_cache) > _CACHE_MAX:
        _cache.popitem(last=False)


async def execute_sql(query: str) -> dict:
    """Execute a PostgreSQL query and return results in SOQL-compatible format."""
    q = query.strip()
    if not q.upper().startswith("SELECT"):
        return {"error": "Only SELECT queries allowed", "status": 400}

    for word in ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE"]:
        if word in q.upper().split():
            return {"error": f"Dangerous operation '{word}' not allowed", "status": 400}

    cached = _cache_get(query)
    if cached is not None:
        logger.info(f"SQL cache hit: {query[:120]}")
        return cached

    logger.info(f"SQL: {query[:200]}")

    try:
        async with async_session() as session:
            result = await session.execute(text(q))
            columns = list(result.keys())
            rows = result.fetchall()

            records = []
            for row in rows:
                record = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    if val is not None:
                        record[col] = val
                records.append(record)

            response = {
                "totalSize": len(records),
                "records": records,
                "done": True,
            }
            _cache_put(query, response)
            return response

    except Exception as e:
        error_msg = str(e)[:500]
        logger.error(f"SQL error: {error_msg}")
        return {"error": error_msg, "status": 500}


# Table/column mapping from Salesforce field names to PostgreSQL
SF_TO_PG = {
    "Student__c": {
        "table": "students",
        "fields": {
            "Id": "sf_id",
            "Name": "name",
            "Student_Marketing_Status__c": "student_marketing_status",
            "Technology__c": "technology",
            "Manager__c": "manager_id",
            "Manager__r.Name": "manager_name",
            "Phone__c": "phone",
            "Email__c": "email",
            "Marketing_Visa_Status__c": "marketing_visa_status",
            "Days_in_Market_Business__c": "days_in_market",
            "Last_Submission_Date__c": "last_submission_date",
            "PreMarketingStatus__c": "pre_marketing_status",
            "Verbal_Confirmation_Date__c": "verbal_confirmation_date",
            "Project_Start_Date__c": "project_start_date",
            "Resume_Preparation__c": "resume_preparation",
            "Resume_Verified_By_Lead__c": "resume_verified_by_lead",
            "Resume_Verified_By_Manager__c": "resume_verified_by_manager",
            "Resume_Verification__c": "resume_verification",
            "Resume_Review__c": "resume_review",
            "Otter_Screening__c": "otter_screening",
            "Otter_Final_Screening__c": "otter_final_screening",
            "Otter_Real_Time_Screeing_1__c": "otter_real_time_1",
            "Otter_Real_Time_Screeing_2__c": "otter_real_time_2",
            "Has_Linkedin_Created__c": "has_linkedin_created",
            "Student_LinkedIn_Account_Review__c": "student_linkedin_review",
            "MQ_Screening_By_Lead__c": "mq_screening_by_lead",
            "MQ_Screening_By_Manager__c": "mq_screening_by_manager",
        },
    },
    "Submissions__c": {
        "table": "submissions",
        "fields": {
            "Id": "sf_id",
            "Student_Name__c": "student_name",
            "BU_Name__c": "bu_name",
            "Client_Name__c": "client_name",
            "Submission_Date__c": "submission_date",
            "Offshore_Manager_Name__c": "offshore_manager_name",
            "Recruiter_Name__c": "recruiter_name",
            "CreatedDate": "created_date",
        },
    },
    "Interviews__c": {
        "table": "interviews",
        "fields": {
            "Id": "sf_id",
            "Student__c": "student_id",
            "Student__r.Name": "student_name",
            "Onsite_Manager__c": "onsite_manager",
            "Offshore_Manager__c": "offshore_manager",
            "Type__c": "interview_type",
            "Final_Status__c": "final_status",
            "Amount__c": "amount",
            "Amount_INR__c": "amount_inr",
            "Bill_Rate__c": "bill_rate",
            "Interview_Date__c": "interview_date",
            "CreatedDate": "created_date",
        },
    },
    "Manager__c": {
        "table": "managers",
        "fields": {
            "Id": "sf_id",
            "Name": "name",
            "Active__c": "active",
            "Total_Expenses_MIS__c": "total_expenses",
            "Each_Placement_Cost__c": "each_placement_cost",
            "Students_Count__c": "students_count",
            "In_Market_Students_Count__c": "in_market_students_count",
            "Verbal_Count__c": "verbal_count",
            "BU_Student_With_Job_Count__c": "bu_student_with_job_count",
            "IN_JOB_Students_Count__c": "in_job_students_count",
            "Cluster__c": "cluster",
            "Organization__c": "organization",
        },
    },
    "Job__c": {
        "table": "jobs",
        "fields": {
            "Id": "sf_id",
            "Student__c": "student_id",
            "Student__r.Name": "student_name",
            "Share_With__c": "share_with_id",
            "Share_With__r.Name": "share_with_name",
            "PayRate__c": "pay_rate",
            "Caluculated_Pay_Rate__c": "calculated_pay_rate",
            "Pay_Roll_Tax__c": "pay_roll_tax",
            "Profit__c": "profit",
            "Bill_Rate__c": "bill_rate",
            "Active__c": "active",
            "Project_Type__c": "project_type",
            "Technology__c": "technology",
            "Payroll_Month__c": "payroll_month",
        },
    },
    "Employee__c": {
        "table": "employees",
        "fields": {
            "Id": "sf_id",
            "Name": "name",
            "Onshore_Manager__c": "onshore_manager_id",
            "Onshore_Manager__r.Name": "onshore_manager_name",
            "Cluster__c": "cluster",
        },
    },
    "BU_Performance__c": {
        "table": "bu_performance",
        "fields": {
            "Id": "sf_id",
            "Name": "name",
            "BU__c": "bu_id",
            "In_Market_Students_Count__c": "in_market_students_count",
            "Date__c": "date",
            "Submissions_Count__c": "submissions_count",
            "Interview_Count__c": "interview_count",
            "submission__c": "submission",
            "Target_Submissions__c": "target_submissions",
        },
    },
    "BS__c": {
        "table": "bs",
        "fields": {
            "Id": "sf_id",
            "Name": "name",
            "Student__c": "student_id",
            "BU_Name__c": "bu_name",
            "Vendor_Name__c": "vendor_name",
            "Bill_Rate__c": "bill_rate",
            "PayRate__c": "pay_rate",
            "Caluculated_Pay_Rate__c": "calculated_pay_rate",
            "Month__c": "month",
            "Year__c": "year",
            "Hours__c": "hours",
            "Invoice_Amount__c": "invoice_amount",
            "Actual_Salary__c": "actual_salary",
            "Salary_Paid__c": "salary_paid",
            "Insurance__c": "insurance",
            "H1fee__c": "h1fee",
            "Other_Amounts__c": "other_amounts",
            "Pending_Amount__c": "pending_amount",
            "Payroll_Taxes__c": "payroll_taxes",
            "Gross_Profit__c": "gross_profit",
            "DOJ__c": "doj",
            "Company_Name__c": "company_name",
            "Payment_Type__c": "payment_type",
        },
    },
    "Tech_Support__c": {
        "table": "tech_support",
        "fields": {
            "Id": "sf_id",
            "Name": "name",
            "Name__c": "name_field",
            "Amnt_Per_Call__c": "amnt_per_call",
            "Availability__c": "availability",
            "Calendar_URL__c": "calendar_url",
            "Calls_Per_Day__c": "calls_per_day",
            "Confirmtion_amount__c": "confirmation_amount",
            "contact_Number1__c": "contact_number1",
            "Contact_Number2__c": "contact_number2",
            "Location__c": "location",
            "OnSiteMgrID__c": "onsite_mgr_id",
            "Payment_Type__c": "payment_type",
            "Total_Amount__c": "total_amount",
            "Account_Details__c": "account_details",
            "Technology__c": "technology",
            "Priority__c": "priority",
            "Total_Interviews_Count__c": "total_interviews_count",
        },
    },
    "New_Student__c": {
        "table": "new_students",
        "fields": {
            "Id": "sf_id",
            "Name": "name",
            "Manager__c": "manager_id",
            "Date_Of_Birth__c": "date_of_birth",
            "Visa_Status__c": "visa_status",
            "OPT_STEM_Start_Date__c": "opt_stem_start_date",
            "Interested_Tech__c": "interested_tech",
            "Phone__c": "phone",
            "CreatedDate": "created_date",
        },
    },
    "Manager_Card__c": {
        "table": "manager_cards",
        "fields": {
            "Id": "sf_id",
            "Name": "name",
            "Card__c": "card",
            "Manager__c": "manager_id",
            "Source_Type__c": "source_type",
        },
    },
    "Cluster__c": {
        "table": "clusters",
        "fields": {
            "Id": "sf_id",
            "Name": "name",
            "Email__c": "email",
            "US_Number__c": "us_number",
            "India_Number__c": "india_number",
            "Cluster_Aliaz__c": "cluster_alias",
            "Cluster_ID__c": "cluster_id",
            "IN_Job_Student_Count__c": "in_job_student_count",
        },
    },
    "Organization__c": {
        "table": "organizations",
        "fields": {
            "Id": "sf_id",
            "Name": "name",
            "Cluster__c": "cluster",
            "Domain__c": "domain",
            "OrgName__c": "org_name",
            "Website__c": "website",
            "Phone_Numbers__c": "phone_numbers",
            "Country_Incorporated__c": "country_incorporated",
            "Bank_Name__c": "bank_name",
            "EIN__c": "ein",
        },
    },
    "Pay_Off__c": {
        "table": "pay_offs",
        "fields": {
            "Id": "sf_id",
            "Name": "name",
            "Cluster__c": "cluster",
        },
    },
}


def soql_to_sql(soql: str) -> str | None:
    """
    Convert a SOQL query to PostgreSQL SQL.
    Returns None if the query can't be converted (should fall back to Salesforce).
    """
    soql = soql.strip()
    if not soql.upper().startswith("SELECT"):
        return None

    from_match = re.search(r'\bFROM\s+(\w+)', soql, re.IGNORECASE)
    if not from_match:
        return None

    sf_object = from_match.group(1)
    mapping = SF_TO_PG.get(sf_object)
    if not mapping:
        return None

    table = mapping["table"]
    fields = mapping["fields"]

    sql = soql

    # Replace object name
    sql = re.sub(r'\bFROM\s+' + re.escape(sf_object), f'FROM {table}', sql, flags=re.IGNORECASE)

    # Replace field names (longer names first to avoid partial replacements)
    sorted_fields = sorted(fields.items(), key=lambda x: -len(x[0]))
    for sf_field, pg_col in sorted_fields:
        sql = re.sub(r'\b' + re.escape(sf_field) + r'\b', pg_col, sql)

    # Handle LAST_N_DAYS:X first
    last_n_match = re.findall(r'LAST_N_DAYS:(\d+)', sql)
    for n in last_n_match:
        sql = sql.replace(f'LAST_N_DAYS:{n}', f"CURRENT_DATE - INTERVAL '{n} days'")

    # Handle date range literals: field = THIS_MONTH → field >= start AND field < end
    range_literals = {
        "THIS_WEEK": ("DATE_TRUNC('week', CURRENT_DATE)", "DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '1 week'"),
        "LAST_WEEK": ("DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week'", "DATE_TRUNC('week', CURRENT_DATE)"),
        "THIS_MONTH": ("DATE_TRUNC('month', CURRENT_DATE)", "DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'"),
        "LAST_MONTH": ("DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'", "DATE_TRUNC('month', CURRENT_DATE)"),
        "THIS_QUARTER": ("DATE_TRUNC('quarter', CURRENT_DATE)", "DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months'"),
        "LAST_QUARTER": ("DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months'", "DATE_TRUNC('quarter', CURRENT_DATE)"),
        "THIS_YEAR": ("DATE_TRUNC('year', CURRENT_DATE)", "DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year'"),
        "LAST_YEAR": ("DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'", "DATE_TRUNC('year', CURRENT_DATE)"),
    }
    for soql_lit, (start, end) in range_literals.items():
        pattern = r'(\w+)\s*=\s*' + re.escape(soql_lit)
        m = re.search(pattern, sql)
        if m:
            col = m.group(1)
            sql = sql.replace(m.group(0), f"{col} >= {start} AND {col} < {end}")

    # Handle simple date literals
    simple_dates = {
        "TODAY": "CURRENT_DATE",
        "YESTERDAY": "CURRENT_DATE - INTERVAL '1 day'",
        "TOMORROW": "CURRENT_DATE + INTERVAL '1 day'",
    }
    for soql_lit, pg_val in simple_dates.items():
        pattern = r'(\w+)\s*=\s*' + re.escape(soql_lit)
        m = re.search(pattern, sql)
        if m:
            col = m.group(1)
            sql = sql.replace(m.group(0), f"{col}::date = {pg_val}")

    # Handle COUNT(Id/sf_id) → COUNT(*)
    sql = re.sub(r'COUNT\(Id\)', 'COUNT(*)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'COUNT\(sf_id\)', 'COUNT(*)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'COUNT\(\)', 'COUNT(*)', sql, flags=re.IGNORECASE)

    # Handle NULLS LAST
    sql = re.sub(r'NULLS\s+LAST', 'NULLS LAST', sql, flags=re.IGNORECASE)

    # Handle subqueries: replace object/field names inside subqueries
    sub_matches = list(re.finditer(r'\(SELECT\s+.+?\)', sql))
    for sm in reversed(sub_matches):
        sub_sql = sm.group(0)
        sub_from_m = re.search(r'FROM\s+(\w+)', sub_sql)
        if sub_from_m:
            sub_obj = sub_from_m.group(1)
            sub_mapping = SF_TO_PG.get(sub_obj)
            if sub_mapping:
                new_sub = sub_sql
                new_sub = re.sub(r'\bFROM\s+' + re.escape(sub_obj), f'FROM {sub_mapping["table"]}', new_sub)
                for sf_f, pg_c in sorted(sub_mapping["fields"].items(), key=lambda x: -len(x[0])):
                    new_sub = re.sub(r'\b' + re.escape(sf_f) + r'\b', pg_c, new_sub)
                sql = sql[:sm.start()] + new_sub + sql[sm.end():]

    # Handle COUNT() as alias 'cnt'
    sql = re.sub(r'COUNT\(\*\)\s+cnt', 'COUNT(*) AS cnt', sql, flags=re.IGNORECASE)
    sql = re.sub(r'AVG\((\w+)\)\s+(\w+)', r'AVG(\1) AS \2', sql, flags=re.IGNORECASE)

    # Clean up any remaining Salesforce-specific syntax
    sql = sql.replace("= null", "IS NULL")
    sql = re.sub(r"!= null", "IS NOT NULL", sql)

    return sql


