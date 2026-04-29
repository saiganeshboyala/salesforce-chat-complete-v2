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

    # 3. Monthly BU-wise Report: Submissions, Interviews, Confirmations, Amount
    bu_subs = await _query(
        'SELECT "BU_Name__c" AS bu, COUNT(*) AS subs FROM "Submissions__c" '
        'WHERE "Submission_Date__c" >= DATE_TRUNC(\'month\', CURRENT_DATE) '
        'AND "BU_Name__c" IS NOT NULL '
        'GROUP BY "BU_Name__c"'
    )
    bu_ints = await _query(
        'SELECT m."Name" AS bu, COUNT(*) AS ints, '
        'SUM(CASE WHEN i."Final_Status__c" IN (\'Confirmation\', \'Expecting Confirmation\', \'Verbal Confirmation\') THEN 1 ELSE 0 END) AS confs, '
        'COALESCE(SUM(i."Amount__c"), 0) AS amount '
        'FROM "Interviews__c" i '
        'LEFT JOIN "Student__c" s ON i."Student__c" = s."Id" '
        'LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" '
        'WHERE i."Interview_Date1__c" >= DATE_TRUNC(\'month\', CURRENT_DATE) '
        'AND m."Name" IS NOT NULL '
        'GROUP BY m."Name"'
    )
    bu_sub_map = {r["bu"]: r["subs"] for r in bu_subs}
    bu_int_map = {r["bu"]: r for r in bu_ints}
    all_bus = sorted(set(list(bu_sub_map.keys()) + list(bu_int_map.keys())))

    bu_report_data = []
    total_s, total_i, total_c, total_a = 0, 0, 0, 0
    for bu in all_bus:
        subs = bu_sub_map.get(bu, 0)
        int_r = bu_int_map.get(bu, {})
        ints = int_r.get("ints", 0) if isinstance(int_r, dict) else 0
        confs = int_r.get("confs", 0) if isinstance(int_r, dict) else 0
        amount = round(int_r.get("amount", 0) if isinstance(int_r, dict) else 0)
        total_s += subs
        total_i += ints
        total_c += confs
        total_a += amount
        bu_report_data.append({
            "name": bu,
            "submissions": subs,
            "interviews": ints,
            "confirmations": confs,
            "amount": amount,
            "drilldown": f"Show performance of {bu} this month",
        })
    bu_report_data.sort(key=lambda x: x["submissions"] + x["interviews"], reverse=True)

    cards.append({
        "id": "bu_monthly_report",
        "title": "Monthly BU Report — Submissions, Interviews, Confirmations & Amount",
        "description": f"{total_s:,} subs, {total_i:,} interviews, {total_c:,} confirmations, ${total_a:,} amount across {len(bu_report_data)} BUs",
        "chartType": "table",
        "data": bu_report_data,
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
            "drilldown": f"List all students under BU {r['Name']}",
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

    # ── 11. Submission-to-Interview Conversion by BU ──────────────
    sub_int_conv = await _query(
        'SELECT m."Name" AS bu, '
        'COUNT(DISTINCT sub."Id") AS total_subs, '
        'COUNT(DISTINCT i."Id") AS total_ints, '
        'ROUND(CASE WHEN COUNT(DISTINCT sub."Id") > 0 '
        '  THEN COUNT(DISTINCT i."Id")::numeric / COUNT(DISTINCT sub."Id") * 100 ELSE 0 END, 1) AS conv_rate '
        'FROM "Student__c" s '
        'JOIN "Manager__c" m ON s."Manager__c" = m."Id" '
        'LEFT JOIN "Submissions__c" sub ON sub."Student__c" = s."Id" '
        '  AND sub."Submission_Date__c" >= CURRENT_DATE - INTERVAL \'90 days\' '
        'LEFT JOIN "Interviews__c" i ON i."Student__c" = s."Id" '
        '  AND i."Interview_Date1__c" >= CURRENT_DATE - INTERVAL \'90 days\' '
        'WHERE s."Student_Marketing_Status__c" = \'In Market\' '
        'GROUP BY m."Name" HAVING COUNT(DISTINCT sub."Id") > 0 '
        'ORDER BY conv_rate DESC'
    )
    avg_conv = 0
    if sub_int_conv:
        total_s_all = sum(r["total_subs"] for r in sub_int_conv)
        total_i_all = sum(r["total_ints"] for r in sub_int_conv)
        avg_conv = round(total_i_all / total_s_all * 100, 1) if total_s_all else 0
    conv_data = []
    for r in sub_int_conv:
        rate = float(r["conv_rate"] or 0)
        if rate >= avg_conv * 1.3:
            verdict = "Above Average"
        elif rate >= avg_conv * 0.7:
            verdict = "Average"
        else:
            verdict = "Below Average"
        conv_data.append({
            "name": r["bu"],
            "submissions": r["total_subs"],
            "interviews": r["total_ints"],
            "conversionRate": rate,
            "verdict": verdict,
            "drilldown": f"List all submissions and interviews for BU {r['bu']} in the last 90 days",
        })
    cards.append({
        "id": "sub_to_interview_conversion",
        "title": "Submission → Interview Conversion (Last 90 Days)",
        "description": f"Avg {avg_conv}% conversion across {len(conv_data)} BUs — are submissions turning into interviews?",
        "chartType": "table",
        "data": conv_data,
        "metric": {"label": "Avg Conversion", "value": f"{avg_conv}%"},
    })

    # ── 12. Interview-to-Placement Conversion by BU ──────────────
    int_place_conv = await _query(
        'SELECT m."Name" AS bu, '
        'COUNT(DISTINCT i."Id") AS total_ints, '
        'COUNT(DISTINCT i."Id") FILTER (WHERE i."Final_Status__c" IN '
        '  (\'Confirmation\', \'Expecting Confirmation\', \'Verbal Confirmation\')) AS confirmations, '
        'COUNT(DISTINCT s."Id") FILTER (WHERE s."Student_Marketing_Status__c" IN '
        '  (\'Project Started\', \'Project Completed\', \'Verbal Confirmation\')) AS placements, '
        'ROUND(CASE WHEN COUNT(DISTINCT i."Id") > 0 '
        '  THEN COUNT(DISTINCT i."Id") FILTER (WHERE i."Final_Status__c" IN '
        '    (\'Confirmation\', \'Expecting Confirmation\', \'Verbal Confirmation\'))::numeric '
        '    / COUNT(DISTINCT i."Id") * 100 ELSE 0 END, 1) AS conf_rate '
        'FROM "Student__c" s '
        'JOIN "Manager__c" m ON s."Manager__c" = m."Id" '
        'LEFT JOIN "Interviews__c" i ON i."Student__c" = s."Id" '
        '  AND i."Interview_Date1__c" >= CURRENT_DATE - INTERVAL \'180 days\' '
        'WHERE s."Student_Marketing_Status__c" IN (\'In Market\', \'Verbal Confirmation\', \'Project Started\') '
        'GROUP BY m."Name" HAVING COUNT(DISTINCT i."Id") > 0 '
        'ORDER BY conf_rate DESC'
    )
    total_i_all2 = sum(r["total_ints"] for r in int_place_conv) if int_place_conv else 0
    total_c_all = sum(r["confirmations"] for r in int_place_conv) if int_place_conv else 0
    avg_conf = round(total_c_all / total_i_all2 * 100, 1) if total_i_all2 else 0
    int_place_data = []
    for r in int_place_conv:
        rate = float(r["conf_rate"] or 0)
        if rate >= 20:
            verdict = "Strong"
        elif rate >= 10:
            verdict = "Average"
        elif rate > 0:
            verdict = "Weak"
        else:
            verdict = "No Conversions"
        int_place_data.append({
            "name": r["bu"],
            "interviews": r["total_ints"],
            "confirmations": r["confirmations"],
            "placements": r["placements"],
            "confirmationRate": rate,
            "verdict": verdict,
            "drilldown": f"List all interviews and placements for BU {r['bu']} in the last 6 months",
        })
    cards.append({
        "id": "interview_to_placement",
        "title": "Interview → Placement Conversion (Last 6 Months)",
        "description": f"Avg {avg_conf}% confirmation rate — which BUs close deals best?",
        "chartType": "table",
        "data": int_place_data,
        "metric": {"label": "Avg Confirmation Rate", "value": f"{avg_conf}%"},
    })

    # ── 13. Recruiter Effectiveness ──────────────────────────────
    recruiter_eff = await _query(
        'SELECT s."Recruiter_Name__c" AS recruiter, '
        'COUNT(DISTINCT s."Id") AS students, '
        'COUNT(DISTINCT sub."Id") AS subs, '
        'COUNT(DISTINCT i."Id") AS ints, '
        'COUNT(DISTINCT s."Id") FILTER (WHERE s."Student_Marketing_Status__c" IN '
        '  (\'Verbal Confirmation\', \'Project Started\')) AS placed, '
        'ROUND(CASE WHEN COUNT(DISTINCT sub."Id") > 0 '
        '  THEN COUNT(DISTINCT i."Id")::numeric / COUNT(DISTINCT sub."Id") * 100 ELSE 0 END, 1) AS sub_to_int, '
        'ROUND(CASE WHEN COUNT(DISTINCT i."Id") > 0 '
        '  THEN COUNT(DISTINCT s."Id") FILTER (WHERE s."Student_Marketing_Status__c" IN '
        '    (\'Verbal Confirmation\', \'Project Started\'))::numeric / COUNT(DISTINCT i."Id") * 100 '
        '  ELSE 0 END, 1) AS int_to_place '
        'FROM "Student__c" s '
        'LEFT JOIN "Submissions__c" sub ON sub."Student__c" = s."Id" '
        '  AND sub."Submission_Date__c" >= CURRENT_DATE - INTERVAL \'90 days\' '
        'LEFT JOIN "Interviews__c" i ON i."Student__c" = s."Id" '
        '  AND i."Interview_Date1__c" >= CURRENT_DATE - INTERVAL \'90 days\' '
        'WHERE s."Recruiter_Name__c" IS NOT NULL '
        '  AND s."Student_Marketing_Status__c" IN (\'In Market\', \'Verbal Confirmation\', \'Project Started\') '
        'GROUP BY s."Recruiter_Name__c" HAVING COUNT(DISTINCT sub."Id") >= 5 '
        'ORDER BY sub_to_int DESC'
    )
    rec_data = []
    for r in recruiter_eff:
        s2i = float(r["sub_to_int"] or 0)
        i2p = float(r["int_to_place"] or 0)
        if s2i >= 15 and i2p >= 10:
            grade = "A — High Performer"
        elif s2i >= 10 or i2p >= 8:
            grade = "B — Good"
        elif s2i >= 5:
            grade = "C — Average"
        else:
            grade = "D — Needs Improvement"
        rec_data.append({
            "name": r["recruiter"],
            "students": r["students"],
            "submissions": r["subs"],
            "interviews": r["ints"],
            "placed": r["placed"],
            "subToIntRate": s2i,
            "intToPlaceRate": i2p,
            "grade": grade,
            "drilldown": f"Show all students under recruiter {r['recruiter']}",
        })
    cards.append({
        "id": "recruiter_effectiveness",
        "title": "Recruiter Effectiveness (Last 90 Days)",
        "description": f"{len(rec_data)} recruiters ranked by submission→interview and interview→placement conversion",
        "chartType": "table",
        "data": rec_data[:30],
    })

    # ── 14. Technology Market Demand — which techs convert best ──
    tech_conv = await _query(
        'SELECT s."Technology__c" AS tech, '
        'COUNT(DISTINCT s."Id") AS in_market, '
        'COUNT(DISTINCT sub."Id") AS subs, '
        'COUNT(DISTINCT i."Id") AS ints, '
        'COUNT(DISTINCT s."Id") FILTER (WHERE s."Student_Marketing_Status__c" IN '
        '  (\'Verbal Confirmation\', \'Project Started\')) AS placed, '
        'ROUND(AVG(s."Days_in_Market_Business__c"), 0) AS avg_days '
        'FROM "Student__c" s '
        'LEFT JOIN "Submissions__c" sub ON sub."Student__c" = s."Id" '
        '  AND sub."Submission_Date__c" >= CURRENT_DATE - INTERVAL \'90 days\' '
        'LEFT JOIN "Interviews__c" i ON i."Student__c" = s."Id" '
        '  AND i."Interview_Date1__c" >= CURRENT_DATE - INTERVAL \'90 days\' '
        'WHERE s."Technology__c" IS NOT NULL '
        '  AND s."Student_Marketing_Status__c" IN (\'In Market\', \'Verbal Confirmation\', \'Project Started\') '
        'GROUP BY s."Technology__c" HAVING COUNT(DISTINCT s."Id") >= 3 '
        'ORDER BY COUNT(DISTINCT i."Id")::numeric / NULLIF(COUNT(DISTINCT sub."Id"), 0) DESC NULLS LAST'
    )
    tech_demand_data = []
    for r in tech_conv:
        subs = r["subs"] or 0
        ints = r["ints"] or 0
        placed = r["placed"] or 0
        s2i = round(ints / subs * 100, 1) if subs > 0 else 0
        if s2i >= 15 and placed > 0:
            demand = "Hot"
        elif s2i >= 8:
            demand = "Warm"
        elif subs > 0:
            demand = "Cold"
        else:
            demand = "No Data"
        tech_demand_data.append({
            "name": r["tech"],
            "inMarket": r["in_market"],
            "submissions": subs,
            "interviews": ints,
            "placed": placed,
            "conversionRate": s2i,
            "avgDaysInMarket": int(r["avg_days"] or 0),
            "demand": demand,
            "drilldown": f"List all students with technology {r['tech']}",
        })
    hot_count = sum(1 for d in tech_demand_data if d["demand"] == "Hot")
    cards.append({
        "id": "tech_demand",
        "title": "Technology Demand Analysis (Last 90 Days)",
        "description": f"{hot_count} hot technologies out of {len(tech_demand_data)} — which skills convert best?",
        "chartType": "table",
        "data": tech_demand_data[:25],
    })

    # ── 15. Student Placement Probability ─────────────────────────
    student_prob = await _query(
        'SELECT s."Name" AS student, m."Name" AS bu, s."Technology__c" AS tech, '
        's."Days_in_Market_Business__c" AS dim, '
        'COALESCE(s."Submission_Count__c", 0) AS total_subs, '
        'COALESCE(s."Interviews_Count__c", 0) AS total_ints, '
        's."Last_Submission_Date__c" AS last_sub, '
        's."Recent_Past_Interview_Date__c" AS last_int, '
        's."Recruiter_Name__c" AS recruiter '
        'FROM "Student__c" s '
        'LEFT JOIN "Manager__c" m ON s."Manager__c" = m."Id" '
        'WHERE s."Student_Marketing_Status__c" = \'In Market\' '
        'ORDER BY s."Days_in_Market_Business__c" ASC NULLS LAST '
        'LIMIT 2000'
    )
    prob_data = []
    today = datetime.now().date()
    for r in student_prob:
        dim = int(r["dim"] or 0)
        total_subs = int(r["total_subs"] or 0)
        total_ints = int(r["total_ints"] or 0)
        last_sub = r.get("last_sub")
        last_int = r.get("last_int")

        score = 50
        if dim <= 30:
            score += 15
        elif dim <= 60:
            score += 10
        elif dim <= 90:
            score += 5
        elif dim > 180:
            score -= 15
        else:
            score -= 5

        if total_subs >= 30:
            score += 10
        elif total_subs >= 15:
            score += 5
        elif total_subs < 5:
            score -= 10

        if total_ints >= 5:
            score += 15
        elif total_ints >= 3:
            score += 10
        elif total_ints >= 1:
            score += 5
        else:
            score -= 10

        if last_sub:
            days_since_sub = (today - last_sub).days if hasattr(last_sub, 'days') else (today - last_sub).days
            if days_since_sub <= 3:
                score += 10
            elif days_since_sub <= 7:
                score += 5
            elif days_since_sub > 14:
                score -= 10

        if last_int:
            days_since_int = (today - last_int).days if hasattr(last_int, 'days') else (today - last_int).days
            if days_since_int <= 7:
                score += 10
            elif days_since_int <= 14:
                score += 5

        score = max(5, min(95, score))

        if score >= 70:
            outlook = "High"
        elif score >= 50:
            outlook = "Medium"
        elif score >= 30:
            outlook = "Low"
        else:
            outlook = "At Risk"

        prob_data.append({
            "name": r["student"],
            "bu": r["bu"] or "N/A",
            "technology": r["tech"] or "N/A",
            "daysInMarket": dim,
            "submissions": total_subs,
            "interviews": total_ints,
            "score": score,
            "outlook": outlook,
            "drilldown": f"Show full details for student {r['student']}",
        })

    prob_data.sort(key=lambda x: -x["score"])
    high_prob = sum(1 for d in prob_data if d["outlook"] == "High")
    at_risk_prob = sum(1 for d in prob_data if d["outlook"] == "At Risk")
    cards.append({
        "id": "placement_probability",
        "title": "Student Placement Probability Score",
        "description": f"{high_prob} high-probability, {at_risk_prob} at-risk out of {len(prob_data)} in-market students",
        "chartType": "table",
        "data": prob_data[:50],
    })

    # ── 16. Time-to-Placement by Technology ──────────────────────
    ttp_data = await _query(
        'SELECT s."Technology__c" AS tech, '
        'COUNT(*) AS placed, '
        'ROUND(AVG(s."Days_in_Market_Business__c"), 0) AS avg_days, '
        'ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s."Days_in_Market_Business__c"), 0) AS median_days, '
        'MIN(s."Days_in_Market_Business__c") AS min_days, '
        'MAX(s."Days_in_Market_Business__c") AS max_days '
        'FROM "Student__c" s '
        'WHERE s."Student_Marketing_Status__c" IN (\'Verbal Confirmation\', \'Project Started\') '
        '  AND s."Technology__c" IS NOT NULL '
        '  AND s."Days_in_Market_Business__c" IS NOT NULL '
        'GROUP BY s."Technology__c" HAVING COUNT(*) >= 2 '
        'ORDER BY avg_days ASC'
    )
    ttp_chart = []
    for r in ttp_data:
        avg_d = int(r["avg_days"] or 0)
        median_d = int(r["median_days"] or 0)
        if avg_d <= 45:
            speed = "Fast"
        elif avg_d <= 90:
            speed = "Average"
        else:
            speed = "Slow"
        ttp_chart.append({
            "name": r["tech"],
            "placed": r["placed"],
            "avgDays": avg_d,
            "medianDays": median_d,
            "minDays": int(r["min_days"] or 0),
            "maxDays": int(r["max_days"] or 0),
            "speed": speed,
            "drilldown": f"List all placed students with technology {r['tech']}",
        })
    overall_avg = round(sum(d["avgDays"] * d["placed"] for d in ttp_chart) / max(sum(d["placed"] for d in ttp_chart), 1))
    cards.append({
        "id": "time_to_placement",
        "title": "Time-to-Placement by Technology",
        "description": f"Overall avg {overall_avg} days — which technologies place fastest?",
        "chartType": "table",
        "data": ttp_chart[:20],
        "metric": {"label": "Avg Days to Place", "value": str(overall_avg)},
    })

    # ── 17. Submission Velocity (Weekly Trend, last 8 weeks) ─────
    sub_velocity = await _query(
        'SELECT DATE_TRUNC(\'week\', "Submission_Date__c")::date AS week, '
        'COUNT(*) AS subs '
        'FROM "Submissions__c" '
        'WHERE "Submission_Date__c" >= CURRENT_DATE - INTERVAL \'8 weeks\' '
        'GROUP BY DATE_TRUNC(\'week\', "Submission_Date__c") '
        'ORDER BY week'
    )
    vel_data = []
    prev_subs = None
    for r in sub_velocity:
        wk = r["week"]
        subs = r["subs"]
        label = wk.strftime("%b %d") if hasattr(wk, "strftime") else str(wk)[:10]
        change = None
        change_pct = None
        if prev_subs is not None and prev_subs > 0:
            change = subs - prev_subs
            change_pct = round(change / prev_subs * 100, 1)
        vel_data.append({
            "month": label,
            "submissions": subs,
            "drilldown": f"List all submissions for week of {label}",
        })
        prev_subs = subs

    if len(vel_data) >= 2:
        first_half = sum(d["submissions"] for d in vel_data[:len(vel_data)//2])
        second_half = sum(d["submissions"] for d in vel_data[len(vel_data)//2:])
        if first_half > 0:
            trend_pct = round((second_half - first_half) / first_half * 100, 1)
            trend_dir = "accelerating" if trend_pct > 5 else ("decelerating" if trend_pct < -5 else "stable")
        else:
            trend_pct = 0
            trend_dir = "no baseline"
    else:
        trend_pct = 0
        trend_dir = "insufficient data"

    cards.append({
        "id": "submission_velocity",
        "title": "Weekly Submission Velocity (Last 8 Weeks)",
        "description": f"Submission pace is {trend_dir} ({'+' if trend_pct > 0 else ''}{trend_pct}% second half vs first half)",
        "chartType": "line",
        "data": vel_data,
        "metric": {"label": "Trend", "value": f"{'+' if trend_pct > 0 else ''}{trend_pct}%"},
    })

    # ── 18. BU Health Scorecard ──────────────────────────────────
    bu_health_raw = await _query(
        'SELECT m."Name" AS bu, '
        'm."In_Market_Students_Count__c" AS in_market, '
        'COALESCE(m."Verbal_Count__c", 0) AS verbals, '
        'COUNT(DISTINCT sub."Id") AS subs_90d, '
        'COUNT(DISTINCT i."Id") AS ints_90d, '
        'COUNT(DISTINCT sub."Id") FILTER (WHERE sub."Submission_Date__c" >= CURRENT_DATE - INTERVAL \'7 days\') AS subs_7d, '
        'ROUND(AVG(s."Days_in_Market_Business__c"), 0) AS avg_dim '
        'FROM "Manager__c" m '
        'LEFT JOIN "Student__c" s ON s."Manager__c" = m."Id" '
        '  AND s."Student_Marketing_Status__c" = \'In Market\' '
        'LEFT JOIN "Submissions__c" sub ON sub."Student__c" = s."Id" '
        '  AND sub."Submission_Date__c" >= CURRENT_DATE - INTERVAL \'90 days\' '
        'LEFT JOIN "Interviews__c" i ON i."Student__c" = s."Id" '
        '  AND i."Interview_Date1__c" >= CURRENT_DATE - INTERVAL \'90 days\' '
        'WHERE m."Active__c" = true '
        'GROUP BY m."Name", m."In_Market_Students_Count__c", m."Verbal_Count__c" '
        'ORDER BY m."Name"'
    )
    health_data = []
    for r in bu_health_raw:
        im = int(r["in_market"] or 0)
        if im == 0:
            continue
        subs_90 = r["subs_90d"] or 0
        ints_90 = r["ints_90d"] or 0
        subs_7 = r["subs_7d"] or 0
        verbals = int(r["verbals"] or 0)
        avg_dim = int(r["avg_dim"] or 0)

        score = 50
        sub_rate = subs_90 / (im * 90) if im > 0 else 0
        if sub_rate >= 0.5:
            score += 15
        elif sub_rate >= 0.3:
            score += 10
        elif sub_rate >= 0.15:
            score += 5
        else:
            score -= 10

        int_rate = ints_90 / im if im > 0 else 0
        if int_rate >= 1.0:
            score += 15
        elif int_rate >= 0.5:
            score += 10
        elif int_rate >= 0.2:
            score += 5
        else:
            score -= 10

        if verbals >= 3:
            score += 10
        elif verbals >= 1:
            score += 5
        else:
            score -= 5

        weekly_target = im * 2 * 5
        weekly_pct = round(subs_7 / weekly_target * 100, 1) if weekly_target > 0 else 0
        if weekly_pct >= 80:
            score += 10
        elif weekly_pct >= 50:
            score += 5
        else:
            score -= 5

        score = max(0, min(100, score))
        if score >= 75:
            grade = "A — Excellent"
        elif score >= 60:
            grade = "B — Good"
        elif score >= 45:
            grade = "C — Average"
        elif score >= 30:
            grade = "D — Below Average"
        else:
            grade = "F — Critical"

        health_data.append({
            "name": r["bu"],
            "inMarket": im,
            "subs90d": subs_90,
            "ints90d": ints_90,
            "verbals": verbals,
            "avgDaysInMarket": avg_dim,
            "weeklyTargetPct": weekly_pct,
            "healthScore": score,
            "grade": grade,
            "drilldown": f"Show detailed performance breakdown for BU {r['bu']}",
        })

    health_data.sort(key=lambda x: -x["healthScore"])
    a_count = sum(1 for d in health_data if d["grade"].startswith("A"))
    f_count = sum(1 for d in health_data if d["grade"].startswith("F"))
    cards.append({
        "id": "bu_health_scorecard",
        "title": "BU Health Scorecard",
        "description": f"{a_count} excellent, {f_count} critical out of {len(health_data)} active BUs — composite score based on submissions, interviews, placements & velocity",
        "chartType": "table",
        "data": health_data,
    })

    return {"cards": cards, "generated_at": datetime.now().isoformat()}
