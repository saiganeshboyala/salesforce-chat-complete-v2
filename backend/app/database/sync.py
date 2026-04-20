"""
Salesforce → PostgreSQL sync engine.
Incremental sync: first run pulls everything, subsequent runs only fetch
records modified since the last successful sync (using LastModifiedDate).
"""
import logging
import asyncio
import threading
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.database.engine import engine, async_session
from app.database.models import (
    Base, Student, Submission, Interview, Manager, Job, Employee,
    BUPerformance, BS, TechSupport, NewStudent, ManagerCard,
    Cluster, Organization, PayOff, SyncLog
)
from app.config import settings

logger = logging.getLogger(__name__)

_sync_running = False
_last_sync: datetime | None = None


def get_sync_status():
    return {
        "running": _sync_running,
        "last_sync": _last_sync.isoformat() if _last_sync else None,
        "interval_minutes": settings.sync_interval_minutes,
    }


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created/verified")


async def _get_last_successful_sync(session, object_name):
    """Get the last successful sync time for an object."""
    result = await session.execute(
        text(
            "SELECT finished_at FROM sync_log "
            "WHERE object_name = :obj AND status = 'success' "
            "ORDER BY finished_at DESC LIMIT 1"
        ),
        {"obj": object_name},
    )
    row = result.fetchone()
    return row[0] if row else None


async def _fetch_all(soql):
    """Fetch ALL records from Salesforce with full pagination."""
    import httpx
    from app.salesforce.auth import ensure_authenticated

    creds = await ensure_authenticated()
    api_url = f"{creds.instance_url}/services/data/{settings.salesforce_api_version}/query/"
    headers = {"Authorization": f"Bearer {creds.access_token}"}

    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.get(api_url, params={"q": soql}, headers=headers)

        if resp.status_code == 401:
            from app.salesforce.auth import login_client_credentials
            creds = await login_client_credentials()
            headers = {"Authorization": f"Bearer {creds.access_token}"}
            resp = await client.get(api_url, params={"q": soql}, headers=headers)

        if resp.status_code != 200:
            raise Exception(f"SOQL error {resp.status_code}: {resp.text[:300]}")

        data = resp.json()
        records = data.get("records", [])
        next_url = data.get("nextRecordsUrl")

        while next_url:
            resp = await client.get(f"{creds.instance_url}{next_url}", headers=headers)
            if resp.status_code != 200:
                break
            page = resp.json()
            records.extend(page.get("records", []))
            next_url = page.get("nextRecordsUrl")
            if len(records) % 10000 == 0:
                logger.info(f"  ... fetched {len(records)} records so far")

    for r in records:
        r.pop("attributes", None)
    return records


def _parse_sf_date(val):
    if not val:
        return None
    try:
        if "T" in str(val):
            return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _parse_sf_datetime(val):
    if not val:
        return None
    try:
        s = str(val).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=None)
    except Exception:
        return None


def _since_clause(last_sync):
    """Build WHERE clause for incremental sync."""
    if not last_sync:
        return ""
    ts = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
    return f" WHERE LastModifiedDate > {ts}"


def _since_clause_with_and(last_sync):
    """Build AND clause for queries that already have WHERE."""
    if not last_sync:
        return ""
    ts = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
    return f" AND LastModifiedDate > {ts}"


async def _upsert_batch(session, model, records_data, batch_size=5000):
    """Upsert records using PostgreSQL ON CONFLICT DO UPDATE."""
    if not records_data:
        return 0

    total = 0
    for i in range(0, len(records_data), batch_size):
        batch = records_data[i:i + batch_size]
        stmt = pg_insert(model.__table__).values(batch)
        update_cols = {c.name: stmt.excluded[c.name] for c in model.__table__.columns if c.name != "sf_id"}
        stmt = stmt.on_conflict_do_update(index_elements=["sf_id"], set_=update_cols)
        await session.execute(stmt)
        total += len(batch)

    await session.flush()
    return total


async def _sync_students(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Student__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Name, Student_Marketing_Status__c, Technology__c, "
        "Manager__c, Manager__r.Name, Phone__c, "
        "Marketing_Visa_Status__c, Days_in_Market_Business__c, "
        "Last_Submission_Date__c, PreMarketingStatus__c, "
        "Verbal_Confirmation_Date__c, "
        "Resume_Preparation__c, Resume_Verified_By_Lead__c, "
        "Resume_Verified_By_Manager__c, Resume_Verification__c, Resume_Review__c, "
        "Otter_Screening__c, Otter_Final_Screening__c, "
        "Otter_Real_Time_Screeing_1__c, Otter_Real_Time_Screeing_2__c, "
        "Has_Linkedin_Created__c, Student_LinkedIn_Account_Review__c, "
        "MQ_Screening_By_Lead__c, MQ_Screening_By_Manager__c "
        f"FROM Student__c{since} ORDER BY Name"
    )

    now = datetime.utcnow()
    records_data = []
    for r in records:
        mgr = r.get("Manager__r") or {}
        records_data.append({
            "sf_id": r["Id"],
            "name": r.get("Name"),
            "student_marketing_status": r.get("Student_Marketing_Status__c"),
            "technology": r.get("Technology__c"),
            "manager_id": r.get("Manager__c"),
            "manager_name": mgr.get("Name") if isinstance(mgr, dict) else None,
            "phone": r.get("Phone__c"),
            "marketing_visa_status": r.get("Marketing_Visa_Status__c"),
            "days_in_market": r.get("Days_in_Market_Business__c"),
            "last_submission_date": _parse_sf_date(r.get("Last_Submission_Date__c")),
            "pre_marketing_status": r.get("PreMarketingStatus__c"),
            "verbal_confirmation_date": _parse_sf_date(r.get("Verbal_Confirmation_Date__c")),
            "resume_preparation": r.get("Resume_Preparation__c"),
            "resume_verified_by_lead": r.get("Resume_Verified_By_Lead__c"),
            "resume_verified_by_manager": r.get("Resume_Verified_By_Manager__c"),
            "resume_verification": r.get("Resume_Verification__c"),
            "resume_review": r.get("Resume_Review__c"),
            "otter_screening": r.get("Otter_Screening__c"),
            "otter_final_screening": r.get("Otter_Final_Screening__c"),
            "otter_real_time_1": r.get("Otter_Real_Time_Screeing_1__c"),
            "otter_real_time_2": r.get("Otter_Real_Time_Screeing_2__c"),
            "has_linkedin_created": r.get("Has_Linkedin_Created__c"),
            "student_linkedin_review": r.get("Student_LinkedIn_Account_Review__c"),
            "mq_screening_by_lead": r.get("MQ_Screening_By_Lead__c"),
            "mq_screening_by_manager": r.get("MQ_Screening_By_Manager__c"),
            "synced_at": now,
        })

    count = await _upsert_batch(session, Student, records_data)
    return count


async def _sync_submissions(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Submissions__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Student_Name__c, BU_Name__c, Client_Name__c, "
        "Submission_Date__c, Offshore_Manager_Name__c, Recruiter_Name__c, CreatedDate "
        f"FROM Submissions__c{since} ORDER BY Submission_Date__c DESC"
    )

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "sf_id": r["Id"],
            "student_name": r.get("Student_Name__c"),
            "bu_name": r.get("BU_Name__c"),
            "client_name": r.get("Client_Name__c"),
            "submission_date": _parse_sf_date(r.get("Submission_Date__c")),
            "offshore_manager_name": r.get("Offshore_Manager_Name__c"),
            "recruiter_name": r.get("Recruiter_Name__c"),
            "created_date": _parse_sf_datetime(r.get("CreatedDate")),
            "synced_at": now,
        })

    count = await _upsert_batch(session, Submission, records_data)
    return count


async def _sync_interviews(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Interviews__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Student__c, Student__r.Name, Onsite_Manager__c, "
        "Type__c, Final_Status__c, Amount__c, Amount_INR__c, Bill_Rate__c, "
        "Interview_Date__c, CreatedDate "
        f"FROM Interviews__c{since} ORDER BY Interview_Date__c DESC"
    )

    now = datetime.utcnow()
    records_data = []
    for r in records:
        stu = r.get("Student__r") or {}
        records_data.append({
            "sf_id": r["Id"],
            "student_id": r.get("Student__c"),
            "student_name": stu.get("Name") if isinstance(stu, dict) else None,
            "onsite_manager": r.get("Onsite_Manager__c"),
            "interview_type": r.get("Type__c"),
            "final_status": r.get("Final_Status__c"),
            "amount": r.get("Amount__c"),
            "amount_inr": r.get("Amount_INR__c"),
            "bill_rate": r.get("Bill_Rate__c"),
            "interview_date": _parse_sf_date(r.get("Interview_Date__c")),
            "created_date": _parse_sf_datetime(r.get("CreatedDate")),
            "synced_at": now,
        })

    count = await _upsert_batch(session, Interview, records_data)
    return count


async def _sync_managers(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Manager__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Name, Active__c, Total_Expenses_MIS__c, "
        "Each_Placement_Cost__c, Students_Count__c, "
        "In_Market_Students_Count__c, Verbal_Count__c, "
        "BU_Student_With_Job_Count__c, IN_JOB_Students_Count__c, "
        "Cluster__c, Organization__c "
        f"FROM Manager__c{since}"
    )

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "sf_id": r["Id"],
            "name": r.get("Name"),
            "active": r.get("Active__c", False),
            "total_expenses": r.get("Total_Expenses_MIS__c"),
            "each_placement_cost": r.get("Each_Placement_Cost__c"),
            "students_count": r.get("Students_Count__c"),
            "in_market_students_count": r.get("In_Market_Students_Count__c"),
            "verbal_count": r.get("Verbal_Count__c"),
            "bu_student_with_job_count": r.get("BU_Student_With_Job_Count__c"),
            "in_job_students_count": r.get("IN_JOB_Students_Count__c"),
            "cluster": r.get("Cluster__c"),
            "organization": r.get("Organization__c"),
            "synced_at": now,
        })

    count = await _upsert_batch(session, Manager, records_data)
    return count


async def _sync_jobs(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Job__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Student__c, Student__r.Name, Share_With__c, Share_With__r.Name, "
        "PayRate__c, Caluculated_Pay_Rate__c, Pay_Roll_Tax__c, Profit__c, "
        "Bill_Rate__c, Active__c, Project_Type__c, Technology__c, Payroll_Month__c "
        f"FROM Job__c{since}"
    )

    now = datetime.utcnow()
    records_data = []
    for r in records:
        stu = r.get("Student__r") or {}
        sw = r.get("Share_With__r") or {}
        records_data.append({
            "sf_id": r["Id"],
            "student_id": r.get("Student__c"),
            "student_name": stu.get("Name") if isinstance(stu, dict) else None,
            "share_with_id": r.get("Share_With__c"),
            "share_with_name": sw.get("Name") if isinstance(sw, dict) else None,
            "pay_rate": r.get("PayRate__c"),
            "calculated_pay_rate": r.get("Caluculated_Pay_Rate__c"),
            "pay_roll_tax": r.get("Pay_Roll_Tax__c"),
            "profit": r.get("Profit__c"),
            "bill_rate": r.get("Bill_Rate__c"),
            "active": r.get("Active__c", False),
            "project_type": r.get("Project_Type__c"),
            "technology": r.get("Technology__c"),
            "payroll_month": r.get("Payroll_Month__c"),
            "synced_at": now,
        })

    count = await _upsert_batch(session, Job, records_data)
    return count


async def _sync_employees(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Employee__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Name, Onshore_Manager__c, Onshore_Manager__r.Name, Cluster__c "
        f"FROM Employee__c{since}"
    )

    now = datetime.utcnow()
    records_data = []
    for r in records:
        mgr = r.get("Onshore_Manager__r") or {}
        records_data.append({
            "sf_id": r["Id"],
            "name": r.get("Name"),
            "onshore_manager_id": r.get("Onshore_Manager__c"),
            "onshore_manager_name": mgr.get("Name") if isinstance(mgr, dict) else None,
            "cluster": r.get("Cluster__c"),
            "synced_at": now,
        })

    count = await _upsert_batch(session, Employee, records_data)
    return count


async def _sync_bu_performance(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing BU_Performance__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Name, BU__c, In_Market_Students_Count__c, Date__c, "
        "Submissions_Count__c, Interview_Count__c, submission__c, Target_Submissions__c "
        f"FROM BU_Performance__c{since}"
    )

    now = datetime.utcnow()
    records_data = [{
        "sf_id": r["Id"], "name": r.get("Name"), "bu_id": r.get("BU__c"),
        "in_market_students_count": r.get("In_Market_Students_Count__c"),
        "date": _parse_sf_date(r.get("Date__c")),
        "submissions_count": r.get("Submissions_Count__c"),
        "interview_count": r.get("Interview_Count__c"),
        "submission": r.get("submission__c"),
        "target_submissions": r.get("Target_Submissions__c"),
        "synced_at": now,
    } for r in records]

    return await _upsert_batch(session, BUPerformance, records_data)


async def _sync_bs(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing BS__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Name, Student__c, BU_Name__c, Vendor_Name__c, "
        "Bill_Rate__c, PayRate__c, Caluculated_Pay_Rate__c, Month__c, Year__c, "
        "Hours__c, Invoice_Amount__c, Actual_Salary__c, Salary_Paid__c, "
        "Insurance__c, H1fee__c, Other_Amounts__c, Pending_Amount__c, "
        "Payroll_Taxes__c, Gross_Profit__c, DOJ__c, Company_Name__c, Payment_Type__c "
        f"FROM BS__c{since}"
    )

    now = datetime.utcnow()
    records_data = [{
        "sf_id": r["Id"], "name": r.get("Name"), "student_id": r.get("Student__c"),
        "bu_name": r.get("BU_Name__c"), "vendor_name": r.get("Vendor_Name__c"),
        "bill_rate": r.get("Bill_Rate__c"), "pay_rate": str(r.get("PayRate__c") or ""),
        "calculated_pay_rate": r.get("Caluculated_Pay_Rate__c"),
        "month": r.get("Month__c"), "year": r.get("Year__c"),
        "hours": r.get("Hours__c"), "invoice_amount": r.get("Invoice_Amount__c"),
        "actual_salary": r.get("Actual_Salary__c"), "salary_paid": r.get("Salary_Paid__c"),
        "insurance": r.get("Insurance__c"), "h1fee": r.get("H1fee__c"),
        "other_amounts": r.get("Other_Amounts__c"), "pending_amount": r.get("Pending_Amount__c"),
        "payroll_taxes": r.get("Payroll_Taxes__c"), "gross_profit": r.get("Gross_Profit__c"),
        "doj": _parse_sf_date(r.get("DOJ__c")), "company_name": r.get("Company_Name__c"),
        "payment_type": r.get("Payment_Type__c"), "synced_at": now,
    } for r in records]

    return await _upsert_batch(session, BS, records_data)


async def _sync_tech_support(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Tech_Support__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Name, Name__c, Amnt_Per_Call__c, Availability__c, "
        "Calendar_URL__c, Calls_Per_Day__c, Confirmtion_amount__c, "
        "contact_Number1__c, Contact_Number2__c, Location__c, "
        "OnSiteMgrID__c, Payment_Type__c, Total_Amount__c, "
        "Account_Details__c, Technology__c, Priority__c, Total_Interviews_Count__c "
        f"FROM Tech_Support__c{since}"
    )

    now = datetime.utcnow()
    records_data = [{
        "sf_id": r["Id"], "name": r.get("Name"), "name_field": r.get("Name__c"),
        "amnt_per_call": r.get("Amnt_Per_Call__c"),
        "availability": r.get("Availability__c"), "calendar_url": r.get("Calendar_URL__c"),
        "calls_per_day": r.get("Calls_Per_Day__c"),
        "confirmation_amount": r.get("Confirmtion_amount__c"),
        "contact_number1": r.get("contact_Number1__c"),
        "contact_number2": r.get("Contact_Number2__c"),
        "location": r.get("Location__c"), "onsite_mgr_id": r.get("OnSiteMgrID__c"),
        "payment_type": r.get("Payment_Type__c"), "total_amount": r.get("Total_Amount__c"),
        "account_details": r.get("Account_Details__c"),
        "technology": r.get("Technology__c"), "priority": r.get("Priority__c"),
        "total_interviews_count": r.get("Total_Interviews_Count__c"),
        "synced_at": now,
    } for r in records]

    return await _upsert_batch(session, TechSupport, records_data)


async def _sync_new_students(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing New_Student__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Name, Manager__c, Date_Of_Birth__c, "
        "Visa_Status__c, OPT_STEM_Start_Date__c, Interested_Tech__c, "
        "Phone__c, CreatedDate "
        f"FROM New_Student__c{since}"
    )

    now = datetime.utcnow()
    records_data = [{
        "sf_id": r["Id"], "name": r.get("Name"), "manager_id": r.get("Manager__c"),
        "date_of_birth": _parse_sf_date(r.get("Date_Of_Birth__c")),
        "visa_status": r.get("Visa_Status__c"),
        "opt_stem_start_date": _parse_sf_date(r.get("OPT_STEM_Start_Date__c")),
        "interested_tech": r.get("Interested_Tech__c"),
        "phone": r.get("Phone__c"),
        "created_date": _parse_sf_datetime(r.get("CreatedDate")),
        "synced_at": now,
    } for r in records]

    return await _upsert_batch(session, NewStudent, records_data)


async def _sync_manager_cards(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Manager_Card__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Name, Card__c, Manager__c, Source_Type__c "
        f"FROM Manager_Card__c{since}"
    )

    now = datetime.utcnow()
    records_data = [{
        "sf_id": r["Id"], "name": r.get("Name"), "card": r.get("Card__c"),
        "manager_id": r.get("Manager__c"), "source_type": r.get("Source_Type__c"),
        "synced_at": now,
    } for r in records]

    return await _upsert_batch(session, ManagerCard, records_data)


async def _sync_clusters(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Cluster__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Name, Email__c, US_Number__c, India_Number__c, "
        "Cluster_Aliaz__c, Cluster_ID__c, IN_Job_Student_Count__c "
        f"FROM Cluster__c{since}"
    )

    now = datetime.utcnow()
    records_data = [{
        "sf_id": r["Id"], "name": r.get("Name"), "email": r.get("Email__c"),
        "us_number": r.get("US_Number__c"), "india_number": r.get("India_Number__c"),
        "cluster_alias": r.get("Cluster_Aliaz__c"),
        "cluster_id": r.get("Cluster_ID__c"),
        "in_job_student_count": r.get("IN_Job_Student_Count__c"),
        "synced_at": now,
    } for r in records]

    return await _upsert_batch(session, Cluster, records_data)


async def _sync_organizations(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Organization__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Name, Cluster__c, Domain__c, OrgName__c, "
        "Website__c, Phone_Numbers__c, Country_Incorporated__c, "
        "Bank_Name__c, EIN__c "
        f"FROM Organization__c{since}"
    )

    now = datetime.utcnow()
    records_data = [{
        "sf_id": r["Id"], "name": r.get("Name"), "cluster": r.get("Cluster__c"),
        "domain": r.get("Domain__c"), "org_name": r.get("OrgName__c"),
        "website": r.get("Website__c"), "phone_numbers": r.get("Phone_Numbers__c"),
        "country_incorporated": r.get("Country_Incorporated__c"),
        "bank_name": r.get("Bank_Name__c"), "ein": r.get("EIN__c"),
        "synced_at": now,
    } for r in records]

    return await _upsert_batch(session, Organization, records_data)


async def _sync_pay_offs(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Pay_Off__c ({mode})...")

    records = await _fetch_all(
        "SELECT Id, Name, Cluster__c "
        f"FROM Pay_Off__c{since}"
    )

    now = datetime.utcnow()
    records_data = [{
        "sf_id": r["Id"], "name": r.get("Name"), "cluster": r.get("Cluster__c"),
        "synced_at": now,
    } for r in records]

    return await _upsert_batch(session, PayOff, records_data)


SYNC_TASKS = [
    ("Student__c", _sync_students),
    ("Submissions__c", _sync_submissions),
    ("Interviews__c", _sync_interviews),
    ("Manager__c", _sync_managers),
    ("Job__c", _sync_jobs),
    ("Employee__c", _sync_employees),
    ("BU_Performance__c", _sync_bu_performance),
    ("BS__c", _sync_bs),
    ("Tech_Support__c", _sync_tech_support),
    ("New_Student__c", _sync_new_students),
    ("Manager_Card__c", _sync_manager_cards),
    ("Cluster__c", _sync_clusters),
    ("Organization__c", _sync_organizations),
    ("Pay_Off__c", _sync_pay_offs),
]


async def run_sync(full=False):
    """Run sync. If full=True, ignores last sync time and pulls everything."""
    global _sync_running, _last_sync
    if _sync_running:
        logger.warning("Sync already running, skipping")
        return {"status": "already_running"}

    _sync_running = True
    results = []
    logger.info("=== Starting Salesforce → PostgreSQL sync ===")

    for obj_name, sync_fn in SYNC_TASKS:
        async with async_session() as session:
            log_entry = SyncLog(
                object_name=obj_name,
                started_at=datetime.utcnow(),
                status="running",
            )
            try:
                last_sync_time = None
                if not full:
                    last_sync_time = await _get_last_successful_sync(session, obj_name)

                count = await sync_fn(session, last_sync=last_sync_time)
                log_entry.records_synced = count
                log_entry.status = "success"
                log_entry.finished_at = datetime.utcnow()
                results.append({"object": obj_name, "records": count, "status": "success"})
                logger.info(f"  ✓ {obj_name}: {count} records synced")
            except Exception as e:
                await session.rollback()
                log_entry.status = "error"
                log_entry.error = str(e)[:500]
                log_entry.finished_at = datetime.utcnow()
                results.append({"object": obj_name, "records": 0, "status": "error", "error": str(e)[:200]})
                logger.error(f"  ✗ {obj_name}: {e}")

            session.add(log_entry)
            await session.commit()

    _sync_running = False
    _last_sync = datetime.utcnow()
    total = sum(r["records"] for r in results)
    success_count = sum(1 for r in results if r["status"] == "success")
    error_count = sum(1 for r in results if r["status"] == "error")

    logger.info("=" * 60)
    logger.info("  SYNC SUMMARY")
    logger.info("=" * 60)
    for r in results:
        status_icon = "✓" if r["status"] == "success" else "✗"
        err = f" — {r.get('error', '')[:80]}" if r["status"] == "error" else ""
        logger.info(f"  {status_icon} {r['object']:<25} {r['records']:>8,} records{err}")
    logger.info("-" * 60)
    logger.info(f"  TOTAL: {total:,} records | {success_count} succeeded | {error_count} failed")
    logger.info(f"  Completed at: {_last_sync.isoformat()}")
    logger.info("=" * 60)

    return {"status": "complete", "total_records": total, "details": results}


def start_sync_scheduler():
    interval = settings.sync_interval_minutes
    if interval <= 0:
        logger.info("Sync scheduler disabled (interval=0)")
        return

    logger.info(f"Sync scheduler started — every {interval} minutes")

    def loop():
        import time as _time
        while True:
            _time.sleep(interval * 60)
            try:
                asyncio.run(run_sync())
            except Exception as e:
                logger.error(f"Scheduled sync failed: {e}")

    threading.Thread(target=loop, daemon=True).start()
