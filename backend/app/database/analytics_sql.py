"""
PostgreSQL-based analytics — queries actual Salesforce-synced tables.
Uses double-quoted identifiers matching exact Salesforce field names.
"""
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import text
from app.database.engine import async_session

logger = logging.getLogger(__name__)


async def _query(sql):
    try:
        async with async_session() as session:
            result = await session.execute(text(sql))
            return [dict(row._mapping) for row in result.fetchall()]
    except Exception as e:
        logger.warning(f"Analytics SQL failed: {e}\nSQL: {sql[:200]}")
        return []


async def _count(sql):
    rows = await _query(sql)
    if rows and "cnt" in rows[0]:
        return rows[0]["cnt"]
    return 0


async def compute_analytics():
    cards = []

    # 1. Pipeline Funnel
    status_records = await _query(
        'SELECT "Student_Marketing_Status__c", COUNT(*) AS cnt '
        'FROM "Student__c" GROUP BY "Student_Marketing_Status__c" ORDER BY cnt DESC'
    )
    funnel_order = ['Pre Marketing', 'In Market', 'Verbal Confirmation', 'Project Started', 'Project Completed', 'Exit']
    status_map = {r["Student_Marketing_Status__c"]: r["cnt"] for r in status_records if r.get("Student_Marketing_Status__c")}
    funnel_data = []
    for s in funnel_order:
        if s in status_map:
            funnel_data.append({"stage": s, "count": status_map[s], "drilldown": f"List all students with status '{s}'"})
    for r in status_records:
        st = r.get("Student_Marketing_Status__c") or ""
        if st and st not in funnel_order:
            funnel_data.append({"stage": st, "count": r["cnt"], "drilldown": f"List all students with status '{st}'"})
    total_students = sum(d["count"] for d in funnel_data)
    cards.append({
        "id": "pipeline_funnel",
        "title": "Student Pipeline Funnel",
        "description": f"{total_students:,} total students across all stages",
        "chartType": "funnel",
        "data": funnel_data,
    })

    # 2. Technology Distribution (In-Market students)
    tech_records = await _query(
        'SELECT "Technology__c", COUNT(*) AS cnt FROM "Student__c" '
        'WHERE "Student_Marketing_Status__c" = \'In Market\' AND "Technology__c" IS NOT NULL '
        'GROUP BY "Technology__c" ORDER BY cnt DESC'
    )
    tech_all = [
        {"name": r["Technology__c"], "value": r["cnt"],
         "drilldown": f"List all in-market students with technology {r['Technology__c']}"}
        for r in tech_records if r.get("cnt")
    ]
    total_tech = sum(d["value"] for d in tech_all)
    tech_main = [d for d in tech_all if d["value"] / total_tech >= 0.03] if total_tech else tech_all
    others_val = total_tech - sum(d["value"] for d in tech_main)
    if others_val > 0:
        other_names = [d["name"] for d in tech_all if d not in tech_main]
        tech_main.append({
            "name": "Others", "value": others_val,
            "drilldown": f"List all in-market students with technology in {', '.join(other_names)}",
        })
    cards.append({
        "id": "tech_distribution",
        "title": "In-Market Students by Technology",
        "description": f"{len(tech_all)} technologies, {total_tech:,} students in market",
        "chartType": "pie",
        "data": tech_main,
    })

    # 3. BU Submissions This Month (top 15, rest grouped as Others)
    bu_records = await _query(
        'SELECT "BU_Name__c", COUNT(*) AS cnt FROM "Submissions__c" '
        'WHERE "Submission_Date__c" >= DATE_TRUNC(\'month\', CURRENT_DATE) '
        'AND "BU_Name__c" IS NOT NULL '
        'GROUP BY "BU_Name__c" ORDER BY cnt DESC'
    )
    bu_all = [
        {"name": r["BU_Name__c"], "value": r["cnt"],
         "drilldown": f"Show all submissions this month for BU {r['BU_Name__c']}"}
        for r in bu_records
    ]
    top_n = 15
    if len(bu_all) > top_n:
        bu_top = bu_all[:top_n]
        others_count = sum(d["value"] for d in bu_all[top_n:])
        if others_count > 0:
            bu_top.append({"name": "Others", "value": others_count, "drilldown": "Show all submissions this month by BU"})
        bu_sub_data = bu_top
    else:
        bu_sub_data = bu_all
    total_subs = sum(d["value"] for d in bu_all)
    cards.append({
        "id": "bu_submissions",
        "title": "Submissions This Month by BU",
        "description": f"{total_subs:,} submissions across {len(bu_all)} BUs",
        "chartType": "bar",
        "data": bu_sub_data,
    })

    # 4. Interview Outcomes This Month
    int_records = await _query(
        'SELECT "Final_Status__c", COUNT(*) AS cnt FROM "Interviews__c" '
        'WHERE "Interview_Date1__c" >= DATE_TRUNC(\'month\', CURRENT_DATE) '
        'AND "Final_Status__c" IS NOT NULL '
        'GROUP BY "Final_Status__c" ORDER BY cnt DESC'
    )
    int_data = [
        {"name": r["Final_Status__c"], "value": r["cnt"],
         "drilldown": f"List all interviews this month with status {r['Final_Status__c']}"}
        for r in int_records if r.get("cnt")
    ]
    total_ints = sum(d["value"] for d in int_data)
    confirmations = sum(d["value"] for d in int_data if d["name"] in ("Confirmation", "Expecting Confirmation"))
    conv_rate = round((confirmations / total_ints * 100), 1) if total_ints > 0 else 0
    cards.append({
        "id": "interview_outcomes",
        "title": "Interview Outcomes This Month",
        "description": f"{total_ints:,} interviews, {conv_rate}% confirmation rate",
        "chartType": "pie",
        "data": int_data,
        "metric": {"label": "Confirmation Rate", "value": f"{conv_rate}%"},
    })

    # 5. At-Risk Students (no submissions in 7+ days)
    at_risk_count = await _count(
        'SELECT COUNT(*) AS cnt FROM "Student__c" '
        'WHERE "Student_Marketing_Status__c" = \'In Market\' '
        'AND ("Last_Submission_Date__c" < CURRENT_DATE - INTERVAL \'7 days\' OR "Last_Submission_Date__c" IS NULL)'
    )
    in_market_total = await _count(
        'SELECT COUNT(*) AS cnt FROM "Student__c" WHERE "Student_Marketing_Status__c" = \'In Market\''
    )
    risk_pct = round((at_risk_count / in_market_total * 100), 1) if in_market_total > 0 else 0
    cards.append({
        "id": "at_risk",
        "title": "At-Risk Students (No Submissions 7+ Days)",
        "description": f"{at_risk_count:,} of {in_market_total:,} in-market students ({risk_pct}%) have no recent submissions",
        "chartType": "metric",
        "data": [
            {"label": "At Risk", "value": at_risk_count, "color": "#e85454",
             "drilldown": "List all in-market students who have no submissions in the last 7 days"},
            {"label": "Active", "value": in_market_total - at_risk_count, "color": "#4ae87a",
             "drilldown": "List all in-market students who have submissions in the last 7 days"},
            {"label": "Total In Market", "value": in_market_total, "color": "#4a9ee8",
             "drilldown": "List all students currently in market"},
        ],
        "metric": {"label": "Risk Rate", "value": f"{risk_pct}%"},
    })

    # 6. Verbal Confirmations Trend (last 6 months)
    now = datetime.now()
    six_months_ago = (now - relativedelta(months=6)).strftime('%Y-%m-01')
    conf_trend = await _query(
        'SELECT TO_CHAR("Verbal_Confirmation_Date__c", \'YYYY-MM\') AS month, COUNT(*) AS cnt '
        'FROM "Student__c" '
        'WHERE "Student_Marketing_Status__c" IN (\'Verbal Confirmation\', \'Project Started\', \'Project Completed\') '
        f'AND "Verbal_Confirmation_Date__c" >= \'{six_months_ago}\' '
        'GROUP BY TO_CHAR("Verbal_Confirmation_Date__c", \'YYYY-MM\') ORDER BY month'
    )
    conf_map = {r["month"]: r["cnt"] for r in conf_trend}
    months_data = []
    for i in range(5, -1, -1):
        dt = now - relativedelta(months=i)
        label = dt.strftime("%Y-%m")
        months_data.append({
            "month": label,
            "confirmations": conf_map.get(label, 0),
            "drilldown": f"List all verbal confirmations in {label}",
        })

    if len(months_data) >= 3:
        recent = [d["confirmations"] for d in months_data[-3:]]
        predicted = round(sum(recent) / len(recent)) if recent else 0
        months_data[-1]["predicted"] = months_data[-1]["confirmations"]
        next_dt = now + relativedelta(months=1)
        months_data.append({
            "month": next_dt.strftime("%Y-%m"),
            "confirmations": None,
            "predicted": predicted,
        })

    cards.append({
        "id": "confirmation_trend",
        "title": "Verbal Confirmations Trend & Forecast",
        "description": "Monthly trend with next month prediction based on 3-month average",
        "chartType": "line",
        "data": months_data,
    })

    # 7. Submissions Trend (last 6 months)
    sub_trend = await _query(
        'SELECT TO_CHAR("Submission_Date__c", \'YYYY-MM\') AS month, COUNT(*) AS cnt '
        'FROM "Submissions__c" '
        f'WHERE "Submission_Date__c" >= \'{six_months_ago}\' '
        'GROUP BY TO_CHAR("Submission_Date__c", \'YYYY-MM\') ORDER BY month'
    )
    sub_map = {r["month"]: r["cnt"] for r in sub_trend}
    sub_months = []
    for i in range(5, -1, -1):
        dt = now - relativedelta(months=i)
        label = dt.strftime("%Y-%m")
        sub_months.append({
            "month": label,
            "submissions": sub_map.get(label, 0),
            "drilldown": f"List all submissions in {label}",
        })

    if len(sub_months) >= 3:
        recent_s = [d["submissions"] for d in sub_months[-3:]]
        predicted_s = round(sum(recent_s) / len(recent_s)) if recent_s else 0
        sub_months[-1]["predicted"] = sub_months[-1]["submissions"]
        next_dt = now + relativedelta(months=1)
        sub_months.append({
            "month": next_dt.strftime("%Y-%m"),
            "submissions": None,
            "predicted": predicted_s,
        })

    cards.append({
        "id": "submission_trend",
        "title": "Submissions Trend & Forecast",
        "description": "Monthly submissions with next month prediction",
        "chartType": "line",
        "data": sub_months,
    })

    # 8. Visa Distribution (In-Market)
    visa_records = await _query(
        'SELECT "Marketing_Visa_Status__c", COUNT(*) AS cnt FROM "Student__c" '
        'WHERE "Student_Marketing_Status__c" = \'In Market\' AND "Marketing_Visa_Status__c" IS NOT NULL '
        'GROUP BY "Marketing_Visa_Status__c" ORDER BY cnt DESC'
    )
    visa_data = [
        {"name": r["Marketing_Visa_Status__c"], "value": r["cnt"],
         "drilldown": f"List all in-market students with visa status {r['Marketing_Visa_Status__c']}"}
        for r in visa_records
    ]
    cards.append({
        "id": "visa_distribution",
        "title": "In-Market Students by Visa Status",
        "description": f"{len(visa_data)} visa types",
        "chartType": "pie",
        "data": visa_data,
    })

    # 9. Days in Market Distribution
    dim_records = await _query(
        'SELECT '
        '  CASE '
        '    WHEN "Days_in_Market_Business__c" <= 30 THEN \'0-30 days\' '
        '    WHEN "Days_in_Market_Business__c" <= 60 THEN \'31-60 days\' '
        '    WHEN "Days_in_Market_Business__c" <= 90 THEN \'61-90 days\' '
        '    WHEN "Days_in_Market_Business__c" <= 180 THEN \'91-180 days\' '
        '    ELSE \'180+ days\' '
        '  END AS range_label, '
        '  COUNT(*) AS cnt '
        'FROM "Student__c" '
        'WHERE "Student_Marketing_Status__c" = \'In Market\' AND "Days_in_Market_Business__c" IS NOT NULL '
        'GROUP BY range_label ORDER BY MIN("Days_in_Market_Business__c")'
    )
    dim_order = ['0-30 days', '31-60 days', '61-90 days', '91-180 days', '180+ days']
    dim_map = {r["range_label"]: r["cnt"] for r in dim_records}
    dim_drills = {
        '0-30 days': "List all in-market students with 0 to 30 days in market",
        '31-60 days': "List all in-market students with 31 to 60 days in market",
        '61-90 days': "List all in-market students with 61 to 90 days in market",
        '91-180 days': "List all in-market students with 91 to 180 days in market",
        '180+ days': "List all in-market students with more than 180 days in market",
    }
    dim_data = [{"name": r, "value": dim_map.get(r, 0), "drilldown": dim_drills[r]} for r in dim_order]

    avg_rows = await _query(
        'SELECT AVG("Days_in_Market_Business__c") AS avg_days FROM "Student__c" '
        'WHERE "Student_Marketing_Status__c" = \'In Market\''
    )
    avg_days = round(avg_rows[0]["avg_days"] or 0) if avg_rows else 0
    cards.append({
        "id": "days_in_market",
        "title": "Days in Market Distribution",
        "description": f"Average {avg_days} days in market for active students",
        "chartType": "bar",
        "data": dim_data,
        "metric": {"label": "Avg Days in Market", "value": str(avg_days)},
    })

    # 10. BU Efficiency Table
    bu_expense = await _query(
        'SELECT "Name", "Total_Expenses__c", "Each_Placement_Cost__c", '
        '"Students_Count__c", "In_Market_Students_Count__c", "Verbal_Count__c" '
        'FROM "Manager__c" WHERE "Active__c" = true '
        'ORDER BY "Students_Count__c" DESC NULLS LAST LIMIT 200'
    )
    expense_data = [
        {
            "name": r["Name"],
            "expense": round(r.get("Total_Expenses__c") or 0),
            "placementCost": round(r.get("Each_Placement_Cost__c") or 0),
            "students": r.get("Students_Count__c") or 0,
            "inMarket": r.get("In_Market_Students_Count__c") or 0,
            "verbals": r.get("Verbal_Count__c") or 0,
            "drilldown": f"Show all students under BU {r['Name']}",
        }
        for r in bu_expense
    ]
    cards.append({
        "id": "bu_efficiency",
        "title": "BU Expense & Efficiency",
        "description": f"{len(expense_data)} active BUs",
        "chartType": "table",
        "data": expense_data,
    })

    return {"cards": cards, "generated_at": datetime.now().isoformat()}
