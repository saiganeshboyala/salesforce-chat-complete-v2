"""
Predictive Analytics — computes insights from live Salesforce data.
All models run real SOQL queries, no ML training needed.
"""
import logging
from datetime import datetime
from app.salesforce.soql_executor import execute_soql
from app.timezone import now_cst

logger = logging.getLogger(__name__)


async def _safe_query(soql):
    try:
        result = await execute_soql(soql)
        if "error" in result:
            return []
        records = result.get("records", [])
        for r in records:
            r.pop("attributes", None)
        return records
    except Exception as e:
        logger.warning(f"Analytics query failed: {e}")
        return []


async def _safe_count(soql):
    try:
        result = await execute_soql(soql)
        if "error" in result:
            return 0
        return result.get("totalSize", 0)
    except Exception:
        return 0


async def compute_analytics():
    cards = []

    # 1. Pipeline Funnel
    status_records = await _safe_query(
        "SELECT Student_Marketing_Status__c, COUNT(Id) cnt "
        "FROM Student__c GROUP BY Student_Marketing_Status__c ORDER BY COUNT(Id) DESC"
    )
    funnel_order = ['Pre Marketing', 'In Market', 'Verbal Confirmation', 'Project Started', 'Project Completed', 'Exit']
    funnel_data = []
    status_map = {r.get("Student_Marketing_Status__c", ""): r.get("cnt", 0) for r in status_records}
    for s in funnel_order:
        if s in status_map:
            funnel_data.append({
                "stage": s, "count": status_map[s],
                "drilldown": f"List all students with status '{s}'",
            })
    for r in status_records:
        st = r.get("Student_Marketing_Status__c", "")
        if st and st not in funnel_order:
            funnel_data.append({
                "stage": st, "count": r.get("cnt", 0),
                "drilldown": f"List all students with status '{st}'",
            })
    total_students = sum(d["count"] for d in funnel_data)
    cards.append({
        "id": "pipeline_funnel",
        "title": "Student Pipeline Funnel",
        "description": f"{total_students} total students across all stages",
        "chartType": "funnel",
        "data": funnel_data,
    })

    # 2. Technology Distribution
    tech_records = await _safe_query(
        "SELECT Technology__c, COUNT(Id) cnt FROM Student__c "
        "WHERE Student_Marketing_Status__c = 'In Market' "
        "GROUP BY Technology__c ORDER BY COUNT(Id) DESC"
    )
    tech_all = [
        {"name": r.get("Technology__c"), "value": r.get("cnt", 0),
         "drilldown": f"List all in-market students with technology {r.get('Technology__c')}"}
        for r in tech_records if r.get("cnt") and r.get("Technology__c")
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

    # 3. BU Performance — submissions this month grouped by BU_Name__c
    bu_raw = await _safe_query(
        "SELECT BU_Name__c FROM Submissions__c "
        "WHERE Submission_Date__c = THIS_MONTH "
        "ORDER BY BU_Name__c LIMIT 20000"
    )
    bu_counts = {}
    for r in bu_raw:
        bu = r.get("BU_Name__c") or "Unknown"
        bu_counts[bu] = bu_counts.get(bu, 0) + 1
    bu_sub_data = sorted(
        [{"name": k, "value": v, "drilldown": f"Show performance of {k} this month"}
         for k, v in bu_counts.items()],
        key=lambda x: -x["value"]
    )
    total_subs = sum(d["value"] for d in bu_sub_data)
    bus_with_subs = len(bu_sub_data)
    cards.append({
        "id": "bu_submissions",
        "title": "Submissions This Month by BU",
        "description": f"{total_subs} submissions across {bus_with_subs} BUs",
        "chartType": "bar",
        "data": bu_sub_data,
    })

    # 4. Interview Outcomes
    int_status = await _safe_query(
        "SELECT Final_Status__c, COUNT(Id) cnt FROM Interviews__c "
        "WHERE CreatedDate = THIS_MONTH "
        "GROUP BY Final_Status__c ORDER BY COUNT(Id) DESC"
    )
    int_data = [
        {"name": r.get("Final_Status__c"), "value": r.get("cnt", 0),
         "drilldown": f"List all interviews this month with status {r.get('Final_Status__c')}"}
        for r in int_status if r.get("cnt") and r.get("Final_Status__c")
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

    # 5. At-Risk Students (In Market, no submissions in 7+ days)
    at_risk_count = await _safe_count(
        "SELECT COUNT() FROM Student__c "
        "WHERE Student_Marketing_Status__c = 'In Market' "
        "AND (Last_Submission_Date__c < LAST_N_DAYS:7 OR Last_Submission_Date__c = null)"
    )
    in_market_total = await _safe_count(
        "SELECT COUNT() FROM Student__c WHERE Student_Marketing_Status__c = 'In Market'"
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

    # 6. Verbal Confirmations Trend (last 6 months)
    months_data = []
    now = now_cst()
    for i in range(5, -1, -1):
        m = now.month - i
        y = now.year
        while m <= 0:
            m += 12
            y -= 1
        label = f"{y}-{m:02d}"
        start = f"{y}-{m:02d}-01"
        if m == 12:
            end = f"{y + 1}-01-01"
        else:
            end = f"{y}-{m + 1:02d}-01"
        count = await _safe_count(
            f"SELECT COUNT() FROM Student__c "
            f"WHERE Student_Marketing_Status__c = 'Verbal Confirmation' "
            f"AND Verbal_Confirmation_Date__c >= {start} "
            f"AND Verbal_Confirmation_Date__c < {end}"
        )
        months_data.append({
            "month": label, "confirmations": count,
            "drilldown": f"List all verbal confirmations in {label}",
        })

    if len(months_data) >= 3:
        recent = [d["confirmations"] for d in months_data[-3:]]
        avg_recent = sum(recent) / len(recent) if recent else 0
        predicted = round(avg_recent)
        last_actual = months_data[-1]["confirmations"]
        months_data[-1]["predicted"] = last_actual
        next_m = now.month + 1
        next_y = now.year
        if next_m > 12:
            next_m = 1
            next_y += 1
        months_data.append({
            "month": f"{next_y}-{next_m:02d}",
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
    sub_months = []
    for i in range(5, -1, -1):
        m = now.month - i
        y = now.year
        while m <= 0:
            m += 12
            y -= 1
        label = f"{y}-{m:02d}"
        start = f"{y}-{m:02d}-01"
        if m == 12:
            end = f"{y + 1}-01-01"
        else:
            end = f"{y}-{m + 1:02d}-01"
        count = await _safe_count(
            f"SELECT COUNT() FROM Submissions__c "
            f"WHERE Submission_Date__c >= {start} "
            f"AND Submission_Date__c < {end}"
        )
        sub_months.append({
            "month": label, "submissions": count,
            "drilldown": f"List all submissions in {label}",
        })

    if len(sub_months) >= 3:
        recent_s = [d["submissions"] for d in sub_months[-3:]]
        avg_s = sum(recent_s) / len(recent_s) if recent_s else 0
        predicted_s = round(avg_s)
        last_actual_s = sub_months[-1]["submissions"]
        sub_months[-1]["predicted"] = last_actual_s
        next_m = now.month + 1
        next_y = now.year
        if next_m > 12:
            next_m = 1
            next_y += 1
        sub_months.append({
            "month": f"{next_y}-{next_m:02d}",
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

    # 8. Visa Status Distribution (in market)
    visa_records = await _safe_query(
        "SELECT Marketing_Visa_Status__c, COUNT(Id) cnt FROM Student__c "
        "WHERE Student_Marketing_Status__c = 'In Market' "
        "GROUP BY Marketing_Visa_Status__c ORDER BY COUNT(Id) DESC"
    )
    visa_data = [
        {"name": r.get("Marketing_Visa_Status__c"), "value": r.get("cnt", 0),
         "drilldown": f"List all in-market students with visa status {r.get('Marketing_Visa_Status__c')}"}
        for r in visa_records if r.get("cnt") and r.get("Marketing_Visa_Status__c")
    ]
    cards.append({
        "id": "visa_distribution",
        "title": "In-Market Students by Visa Status",
        "description": f"{len(visa_data)} visa types",
        "chartType": "pie",
        "data": visa_data,
    })

    # 9. Days in Market Distribution
    dim_ranges = [
        ("0-30 days", 0, 30),
        ("31-60 days", 31, 60),
        ("61-90 days", 61, 90),
        ("91-180 days", 91, 180),
        ("180+ days", 181, 9999),
    ]
    dim_data = []
    for label, lo, hi in dim_ranges:
        if hi == 9999:
            count = await _safe_count(
                f"SELECT COUNT() FROM Student__c "
                f"WHERE Student_Marketing_Status__c = 'In Market' "
                f"AND Days_in_Market_Business__c >= {lo}"
            )
            drill = f"List all in-market students with more than {lo} days in market"
        else:
            count = await _safe_count(
                f"SELECT COUNT() FROM Student__c "
                f"WHERE Student_Marketing_Status__c = 'In Market' "
                f"AND Days_in_Market_Business__c >= {lo} "
                f"AND Days_in_Market_Business__c <= {hi}"
            )
            drill = f"List all in-market students with {lo} to {hi} days in market"
        dim_data.append({"range": label, "count": count, "drilldown": drill})
    avg_dim_records = await _safe_query(
        "SELECT AVG(Days_in_Market_Business__c) avg_days FROM Student__c "
        "WHERE Student_Marketing_Status__c = 'In Market'"
    )
    avg_days = 0
    if avg_dim_records:
        avg_days = round(avg_dim_records[0].get("avg_days") or 0)
    cards.append({
        "id": "days_in_market",
        "title": "Days in Market Distribution",
        "description": f"Average {avg_days} days in market for active students",
        "chartType": "bar",
        "data": [{"name": d["range"], "value": d["count"], "drilldown": d["drilldown"]} for d in dim_data],
        "metric": {"label": "Avg Days in Market", "value": str(avg_days)},
    })

    # 10. BU Expense Efficiency
    bu_expense = await _safe_query(
        "SELECT Name, Total_Expenses_MIS__c, Each_Placement_Cost__c, "
        "Students_Count__c, In_Market_Students_Count__c, Verbal_Count__c "
        "FROM Manager__c WHERE Active__c = true "
        "ORDER BY Students_Count__c DESC NULLS LAST LIMIT 2000"
    )
    expense_data = []
    for r in bu_expense:
        name = r.get("Name", "N/A")
        expense = r.get("Total_Expenses_MIS__c") or 0
        placement_cost = r.get("Each_Placement_Cost__c") or 0
        students = r.get("Students_Count__c") or 0
        in_market = r.get("In_Market_Students_Count__c") or 0
        verbals = r.get("Verbal_Count__c") or 0
        expense_data.append({
            "name": name,
            "expense": round(expense),
            "placementCost": round(placement_cost),
            "students": students,
            "inMarket": in_market,
            "verbals": verbals,
            "drilldown": f"List all students under BU {name}",
        })
    cards.append({
        "id": "bu_efficiency",
        "title": "BU Expense & Efficiency",
        "description": f"{len(expense_data)} active BUs",
        "chartType": "table",
        "data": expense_data,
    })

    return {"cards": cards, "generated_at": now_cst().isoformat()}
