"""
PostgreSQL-based analytics — replaces SOQL queries in analytics.py with native SQL.
Much faster, no API limits, supports complex aggregations.
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
        logger.warning(f"Analytics SQL failed: {e}")
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
        "SELECT student_marketing_status, COUNT(*) AS cnt "
        "FROM students GROUP BY student_marketing_status ORDER BY cnt DESC"
    )
    funnel_order = ['Pre Marketing', 'In Market', 'Verbal Confirmation', 'Project Started', 'Project Completed', 'Exit']
    status_map = {r["student_marketing_status"]: r["cnt"] for r in status_records if r.get("student_marketing_status")}
    funnel_data = []
    for s in funnel_order:
        if s in status_map:
            funnel_data.append({"stage": s, "count": status_map[s], "drilldown": f"List all students with status '{s}'"})
    for r in status_records:
        st = r.get("student_marketing_status") or ""
        if st and st not in funnel_order:
            funnel_data.append({"stage": st, "count": r["cnt"], "drilldown": f"List all students with status '{st}'"})
    total_students = sum(d["count"] for d in funnel_data)
    cards.append({
        "id": "pipeline_funnel",
        "title": "Student Pipeline Funnel",
        "description": f"{total_students} total students across all stages",
        "chartType": "funnel",
        "data": funnel_data,
    })

    # 2. Technology Distribution
    tech_records = await _query(
        "SELECT technology, COUNT(*) AS cnt FROM students "
        "WHERE student_marketing_status = 'In Market' AND technology IS NOT NULL "
        "GROUP BY technology ORDER BY cnt DESC"
    )
    tech_all = [
        {"name": r["technology"], "value": r["cnt"],
         "drilldown": f"List all in-market students with technology {r['technology']}"}
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
        "description": f"{len(tech_all)} technologies, {total_tech} students in market",
        "chartType": "pie",
        "data": tech_main,
    })

    # 3. BU Submissions This Month
    bu_records = await _query(
        "SELECT bu_name, COUNT(*) AS cnt FROM submissions "
        "WHERE submission_date >= DATE_TRUNC('month', CURRENT_DATE) "
        "AND bu_name IS NOT NULL "
        "GROUP BY bu_name ORDER BY cnt DESC"
    )
    bu_sub_data = [
        {"name": r["bu_name"], "value": r["cnt"],
         "drilldown": f"Show all submissions this month for BU {r['bu_name']}"}
        for r in bu_records
    ]
    total_subs = sum(d["value"] for d in bu_sub_data)
    cards.append({
        "id": "bu_submissions",
        "title": "Submissions This Month by BU",
        "description": f"{total_subs} submissions across {len(bu_sub_data)} BUs",
        "chartType": "bar",
        "data": bu_sub_data,
    })

    # 4. Interview Outcomes
    int_records = await _query(
        "SELECT final_status, COUNT(*) AS cnt FROM interviews "
        "WHERE created_date >= DATE_TRUNC('month', CURRENT_DATE) "
        "AND final_status IS NOT NULL "
        "GROUP BY final_status ORDER BY cnt DESC"
    )
    int_data = [
        {"name": r["final_status"], "value": r["cnt"],
         "drilldown": f"List all interviews this month with status {r['final_status']}"}
        for r in int_records if r.get("cnt")
    ]
    total_ints = sum(d["value"] for d in int_data)
    confirmations = sum(d["value"] for d in int_data if d["name"] in ("Confirmation", "Expecting Confirmation"))
    conv_rate = round((confirmations / total_ints * 100), 1) if total_ints > 0 else 0
    cards.append({
        "id": "interview_outcomes",
        "title": "Interview Outcomes This Month",
        "description": f"{total_ints} interviews, {conv_rate}% confirmation rate",
        "chartType": "pie",
        "data": int_data,
        "metric": {"label": "Confirmation Rate", "value": f"{conv_rate}%"},
    })

    # 5. At-Risk Students
    at_risk_count = await _count(
        "SELECT COUNT(*) AS cnt FROM students "
        "WHERE student_marketing_status = 'In Market' "
        "AND (last_submission_date < CURRENT_DATE - INTERVAL '7 days' OR last_submission_date IS NULL)"
    )
    in_market_total = await _count(
        "SELECT COUNT(*) AS cnt FROM students WHERE student_marketing_status = 'In Market'"
    )
    risk_pct = round((at_risk_count / in_market_total * 100), 1) if in_market_total > 0 else 0
    cards.append({
        "id": "at_risk",
        "title": "At-Risk Students (No Submissions 7+ Days)",
        "description": f"{at_risk_count} of {in_market_total} in-market students ({risk_pct}%) have no recent submissions",
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

    # 6. Verbal Confirmations Trend (last 6 months) — single query!
    now = datetime.now()
    months_data = []
    conf_trend = await _query(
        "SELECT TO_CHAR(verbal_confirmation_date, 'YYYY-MM') AS month, COUNT(*) AS cnt "
        "FROM students "
        "WHERE student_marketing_status = 'Verbal Confirmation' "
        f"AND verbal_confirmation_date >= '{(now - relativedelta(months=6)).strftime('%Y-%m-01')}' "
        "GROUP BY TO_CHAR(verbal_confirmation_date, 'YYYY-MM') ORDER BY month"
    )
    conf_map = {r["month"]: r["cnt"] for r in conf_trend}
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

    # 7. Submissions Trend (last 6 months) — single query!
    sub_trend = await _query(
        "SELECT TO_CHAR(submission_date, 'YYYY-MM') AS month, COUNT(*) AS cnt "
        "FROM submissions "
        f"WHERE submission_date >= '{(now - relativedelta(months=6)).strftime('%Y-%m-01')}' "
        "GROUP BY TO_CHAR(submission_date, 'YYYY-MM') ORDER BY month"
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

    # 8. Visa Distribution — single query
    visa_records = await _query(
        "SELECT marketing_visa_status, COUNT(*) AS cnt FROM students "
        "WHERE student_marketing_status = 'In Market' AND marketing_visa_status IS NOT NULL "
        "GROUP BY marketing_visa_status ORDER BY cnt DESC"
    )
    visa_data = [
        {"name": r["marketing_visa_status"], "value": r["cnt"],
         "drilldown": f"List all in-market students with visa status {r['marketing_visa_status']}"}
        for r in visa_records
    ]
    cards.append({
        "id": "visa_distribution",
        "title": "In-Market Students by Visa Status",
        "description": f"{len(visa_data)} visa types",
        "chartType": "pie",
        "data": visa_data,
    })

    # 9. Days in Market — single query with CASE
    dim_records = await _query(
        "SELECT "
        "  CASE "
        "    WHEN days_in_market <= 30 THEN '0-30 days' "
        "    WHEN days_in_market <= 60 THEN '31-60 days' "
        "    WHEN days_in_market <= 90 THEN '61-90 days' "
        "    WHEN days_in_market <= 180 THEN '91-180 days' "
        "    ELSE '180+ days' "
        "  END AS range_label, "
        "  COUNT(*) AS cnt "
        "FROM students "
        "WHERE student_marketing_status = 'In Market' AND days_in_market IS NOT NULL "
        "GROUP BY range_label ORDER BY MIN(days_in_market)"
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
        "SELECT AVG(days_in_market) AS avg_days FROM students WHERE student_marketing_status = 'In Market'"
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

    # 10. BU Expense — single query
    bu_expense = await _query(
        "SELECT name, total_expenses, each_placement_cost, "
        "students_count, in_market_students_count, verbal_count "
        "FROM managers WHERE active = true "
        "ORDER BY students_count DESC NULLS LAST LIMIT 200"
    )
    expense_data = [
        {
            "name": r["name"],
            "expense": round(r.get("total_expenses") or 0),
            "placementCost": round(r.get("each_placement_cost") or 0),
            "students": r.get("students_count") or 0,
            "inMarket": r.get("in_market_students_count") or 0,
            "verbals": r.get("verbal_count") or 0,
            "drilldown": f"Show all students under BU {r['name']}",
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
