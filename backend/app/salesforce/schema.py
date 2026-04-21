"""
Schema Discovery — builds a compact schema description
that the AI uses to write correct SOQL queries.

Caches the schema locally so it doesn't hit Salesforce on every question.
Refreshes daily via auto-sync.
"""
import json, logging, httpx
from pathlib import Path
from app.config import settings

logger = logging.getLogger(__name__)
SCHEMA_FILE = "schema_cache.json"
_cached_schema = None


PRIORITY_OBJECTS = [
    "Account", "Contact", "User", "Cluster__c", "Manager__c",
    "Student__c", "Submissions__c", "Interviews__c", "Employee__c",
    "Pay_Off__c", "Organization__c", "Job__c", "Report",
    "BU_Performance__c", "BS__c", "Tech_Support__c",
    "New_Student__c", "Manager_Card__c",
]


async def discover_schema(instance_url, access_token, only_priority=False):
    """Discover objects and their fields from Salesforce."""
    logger.info("Discovering Salesforce schema...")
    headers = {"Authorization": f"Bearer {access_token}"}

    # Get all objects
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(f"{instance_url}/services/data/{settings.salesforce_api_version}/sobjects/", headers=headers)
        if resp.status_code != 200:
            raise Exception(f"Schema discovery failed: {resp.status_code}")
        all_objects = resp.json().get("sobjects", [])

    if only_priority:
        priority_set = set(PRIORITY_OBJECTS)
        interesting_objects = [
            obj["name"] for obj in all_objects
            if obj.get("queryable", False) and obj["name"] in priority_set
        ]
    else:
        # Filter to queryable, non-system objects
        skip_suffixes = ("__History", "__Tag", "__Feed", "__Share", "ChangeEvent", "__mdt", "__e", "__x")
        skip_prefixes = ("AI", "Auth", "Async", "Content", "Data", "Duplicate", "EmailMessage",
                         "Entity", "Feed", "FieldPermissions", "Flow", "Login", "OAuth",
                         "ObjectPermissions", "Permission", "Platform", "Process", "Queue",
                         "Record", "Search", "Setup", "Site", "Stamp", "Topic", "UserApp",
                         "UserPref", "Verification", "Visitor", "Vote", "Web")

        interesting_objects = []
        for obj in all_objects:
            name = obj["name"]
            if not obj.get("queryable", False): continue
            if any(name.endswith(s) for s in skip_suffixes): continue
            if any(name.startswith(p) for p in skip_prefixes): continue
            interesting_objects.append(name)

    logger.info(f"Found {len(interesting_objects)} queryable objects")

    # Describe each to get fields
    schema = {}
    async with httpx.AsyncClient(timeout=30.0) as client:
        for obj_name in interesting_objects:
            try:
                resp = await client.get(
                    f"{instance_url}/services/data/{settings.salesforce_api_version}/sobjects/{obj_name}/describe/",
                    headers=headers
                )
                if resp.status_code != 200: continue
                data = resp.json()

                fields = []
                for f in data.get("fields", []):
                    if f["type"] in ("base64", "address", "location", "complexvalue"): continue
                    if f["name"] in ("IsDeleted", "SystemModstamp"): continue
                    fields.append({
                        "name": f["name"],
                        "type": f["type"],
                        "label": f["label"],
                        "filterable": f.get("filterable", False),
                        "groupable": f.get("groupable", False),
                        "sortable": f.get("sortable", False),
                        "referenceTo": f.get("referenceTo") or [],
                    })

                if fields:
                    schema[obj_name] = {
                        "label": data.get("label", obj_name),
                        "fields": fields,
                        "record_count": None,  # filled later
                    }
            except Exception as e:
                logger.debug(f"  {obj_name}: {e}")

    # Get record counts for objects with data
    async with httpx.AsyncClient(timeout=30.0) as client:
        for obj_name in list(schema.keys()):
            try:
                resp = await client.get(
                    f"{instance_url}/services/data/{settings.salesforce_api_version}/query/",
                    params={"q": f"SELECT COUNT() FROM {obj_name}"},
                    headers=headers
                )
                if resp.status_code == 200:
                    count = resp.json().get("totalSize", 0)
                    schema[obj_name]["record_count"] = count
                    if count == 0:
                        del schema[obj_name]  # Remove empty objects
            except:
                pass

    logger.info(f"Schema ready: {len(schema)} objects with data")

    # Save cache
    cache_path = Path(settings.data_dir) / SCHEMA_FILE
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(schema, f, indent=2)

    global _cached_schema
    _cached_schema = schema
    return schema


def get_schema():
    """Get cached schema."""
    global _cached_schema
    if _cached_schema:
        return _cached_schema
    cache_path = Path(settings.data_dir) / SCHEMA_FILE
    if cache_path.exists():
        with open(cache_path) as f:
            _cached_schema = json.load(f)
        return _cached_schema
    return {}


def get_relationships():
    """
    Build a node/edge graph of object relationships.

    Prefers explicit `referenceTo` metadata captured during discovery.
    Falls back to name-based inference for schemas cached before that
    field was stored (strip `__c`/`Id`, match against known object names).
    """
    schema = get_schema()
    if not schema:
        return {"nodes": [], "edges": []}

    object_names = set(schema.keys())
    lower_index = {n.lower(): n for n in object_names}
    edges = []
    seen = set()

    for obj_name, obj_data in schema.items():
        for f in obj_data.get("fields", []):
            if f.get("name") in ("Id", "OwnerId", "CreatedById", "LastModifiedById"):
                continue
            targets = f.get("referenceTo") or []
            # Fallback: infer from field name if no metadata
            if not targets and f.get("type") == "reference":
                base = f["name"]
                for suffix in ("__c", "Id"):
                    if base.endswith(suffix):
                        base = base[: -len(suffix)]
                        break
                guess = lower_index.get(base.lower()) or lower_index.get(f"{base}__c".lower())
                if guess:
                    targets = [guess]

            for t in targets:
                if t in object_names and t != obj_name:
                    key = (obj_name, t, f.get("name"))
                    if key in seen:
                        continue
                    seen.add(key)
                    edges.append({
                        "from": obj_name,
                        "to": t,
                        "field": f.get("name"),
                        "label": f.get("label") or f.get("name"),
                    })

    # Only include nodes that actually have data
    nodes = [
        {
            "id": name,
            "label": data.get("label", name),
            "record_count": data.get("record_count") or 0,
            "field_count": len(data.get("fields", [])),
        }
        for name, data in schema.items()
        if (data.get("record_count") or 0) > 0
    ]
    node_ids = {n["id"] for n in nodes}
    edges = [e for e in edges if e["from"] in node_ids and e["to"] in node_ids]

    # Degree per node
    degree = {n["id"]: 0 for n in nodes}
    for e in edges:
        degree[e["from"]] = degree.get(e["from"], 0) + 1
        degree[e["to"]] = degree.get(e["to"], 0) + 1
    for n in nodes:
        n["degree"] = degree.get(n["id"], 0)

    return {"nodes": nodes, "edges": edges}


def schema_to_prompt(schema=None, max_objects=30):
    """Convert schema to a compact text description for the AI prompt."""
    s = schema or get_schema()
    if not s:
        return "No schema available. Run schema discovery first."

    lines = ["DATABASE SCHEMA (PostgreSQL - use these exact quoted table/column names):\n"]

    sorted_objs = sorted(s.items(), key=lambda x: -(x[1].get("record_count") or 0))
    object_names = set(s.keys())

    for obj_name, obj_data in sorted_objs[:max_objects]:
        count = obj_data.get("record_count", "?")
        label = obj_data.get("label", obj_name)
        lines.append(f"\n{obj_name} ({label}, {count:,} records):")

        field_strs = []
        relationships = []
        for f in obj_data.get("fields", [])[:50]:
            ftype = f["type"]
            refs = f.get("referenceTo", [])
            if ftype in ("id", "reference"): ftype = "id"
            elif ftype in ("string", "textarea", "url", "email", "phone"): ftype = "text"
            elif ftype in ("double", "currency", "percent", "int"): ftype = "number"
            elif ftype in ("date", "datetime"): ftype = ftype
            elif ftype == "boolean": ftype = "bool"
            elif ftype == "picklist": ftype = "picklist"
            else: ftype = f["type"]
            flabel = f.get("label", "")
            field_strs.append(f"{f['name']}({ftype}, \"{flabel}\")")
            if refs:
                for ref in refs:
                    if ref in object_names and ref != obj_name:
                        relationships.append(f"{f['name']} → {ref}")
        lines.append("  Fields: " + ", ".join(field_strs))
        if relationships:
            lines.append("  Relationships: " + ", ".join(relationships))

    lines.append("\n" + "=" * 60)
    lines.append("COMPLETE OBJECT RELATIONSHIP MAP:")
    lines.append("  Student__c.Manager__c -> Manager__c (BU manager). Use Manager__r.Name to get BU name.")
    lines.append("  Student__c.Recruiter__c -> Employee__c. Student__c.Marketing_Company__c -> Organization__c")
    lines.append("  Student__c.Conformation_Submission_ID__c -> Submissions__c")
    lines.append("  Student__c.Conformation_Interview_ID__c -> Interviews__c")
    lines.append("  Submissions__c.Student__c -> Student__c. Use Student__r.Name to get student name from submissions.")
    lines.append("  Submissions__c.BU_Name__c = BU name (text). Submissions__c.Recuter__c -> Employee__c")
    lines.append("  Submissions__c.Vendor_Company__c -> Account. Submissions__c.Vendor_Contact__c -> Contact")
    lines.append("  Interviews__c.Student__c -> Student__c. Interviews__c.Submissions__c -> Submissions__c")
    lines.append("  IMPORTANT: Interviews__c has NO BU_Name__c field! For BU-wise interview reports:")
    lines.append("    JOIN Interviews__c -> Student__c -> Manager__c to get BU name (Manager__c.Name)")
    lines.append("    Interviews__c.Onsite_Manager__c is NOT the BU manager — do NOT use it as BU name.")
    lines.append("  Interviews__c.Tech_Support__c -> Tech_Support__c")
    lines.append("  Job__c.Student__c -> Student__c. Job__c.Share_With__c -> Manager__c (BU)")
    lines.append("  Employee__c.Onshore_Manager__c -> Manager__c (BU). Employee__c.Organization__c -> Organization__c")
    lines.append("  Employee__c.Cluster__c -> Cluster__c. Employee__c.Contact__c -> Contact")
    lines.append("  Manager__c.Cluster__c -> Cluster__c. Manager__c.Organization__c -> Organization__c")
    lines.append("  Manager__c.Offshore_Manager__c -> Employee__c. Manager__c.Offshore_Floor_Manager__c -> Employee__c")
    lines.append("  BU_Performance__c.BU__c -> Manager__c (monthly metrics per BU)")
    lines.append("  New_Student__c.Manager__c -> Manager__c (BU)")
    lines.append("  Tech_Support__c.OnSiteMgrID__c -> Manager__c")
    lines.append("  BS__c.Student__c -> Student__c (billing/salary)")
    lines.append("  Organization__c.Cluster__c -> Cluster__c")

    lines.append("\nCROSS-OBJECT QUERY SYNTAX (use __r for parent lookups):")
    lines.append("  Student -> BU name: SELECT Name, Manager__r.Name FROM Student__c WHERE Manager__r.Name LIKE '%Divya%'")
    lines.append("  Submission -> Student name: SELECT Student__r.Name, BU_Name__c FROM Submissions__c")
    lines.append("  Interview -> Student + Submission: SELECT Student__r.Name, Submissions__r.Name FROM Interviews__c")
    lines.append("  Job -> Student + BU: SELECT Student__r.Name, Share_With__r.Name FROM Job__c")
    lines.append("  Employee -> BU: SELECT Name, Onshore_Manager__r.Name FROM Employee__c")
    lines.append("  BU Performance -> BU name: SELECT BU__r.Name, Submissions_Count__c FROM BU_Performance__c")

    lines.append("\nFIELD-TO-QUESTION MAPPING (when user says X, use this):")
    lines.append("  'students under BU X' -> Student__c WHERE Manager__r.Name LIKE '%X%' (BEST, gets student fields)")
    lines.append("     OR Submissions__c WHERE BU_Name__c LIKE '%X%' (gets submission details)")
    lines.append("  'student status' / 'in market' / 'exit' -> Student__c.Student_Marketing_Status__c")
    lines.append("  'days in market' -> Student__c.Days_in_Market_Business__c (number)")
    lines.append("  'technology' / 'Java' / 'DevOps' / 'DE' -> Student__c.Technology__c")
    lines.append("  'visa status' / 'GC' / 'H1' / 'OPT' -> Student__c.Marketing_Visa_Status__c")
    lines.append("  'submission status' -> Submissions__c.Submission_Status__c")
    lines.append("  'interview type' -> Interviews__c.Type__c")
    lines.append("  'interview result' -> Interviews__c.Final_Status__c")
    lines.append("  'project type' / 'W2' / 'C2C' -> Job__c.Project_Type__c")
    lines.append("  'batch' -> Student__c.Batch__c")
    lines.append("  'recruiter' -> Student__c.Recruiter__r.Name or Submissions__c.Recuter__r.Name")
    lines.append("  'cluster' -> Manager__c.Cluster__r.Name or Employee__c.Cluster__r.Name")
    lines.append("  'organization' / 'company' -> Employee__c.Organization__r.Name or Manager__c.Organization__r.Name")

    lines.append("\nACTUAL PICKLIST VALUES (use exact spelling):")
    lines.append("  Student_Marketing_Status__c: 'In Market', 'Exit', 'Pre Marketing', 'Project Started', 'Project Completed', 'Project Completed-In Market', 'Verbal Confirmation'")
    lines.append("  Submission_Status__c: 'Submitted', 'Interview Scheduled'")
    lines.append("  Interviews__c.Type__c: 'First Round', 'Client', 'Implementation', 'Vendor', 'Second Round', 'Final Round', 'Assessment', 'HR', 'Third Round'")
    lines.append("  Interviews__c.Final_Status__c: 'Good', 'Very Good', 'Average', 'Re-Scheduled', 'Cancelled', 'Confirmation', 'Very Bad', 'Expecting Confirmation'")
    lines.append("  Technology__c: 'DE', 'JAVA', 'DS/AI', 'DevOps', '.NET', 'SFDC', 'CS', 'Service Now', 'AEM', 'Business Analyst', 'SAP BTP', 'RPA', 'PowerBI'")
    lines.append("  Marketing_Visa_Status__c: 'GC', 'H1', 'OPT', 'H4 EAD', 'STEM', 'USC', 'CPT', 'L2'")
    lines.append("  Job__c.Project_Type__c: 'W2', 'PD', 'C2C'")

    lines.append("\nBU MANAGER NAMES (use LIKE '%LastName%' for matching):")
    lines.append("  Divya Panguluri, Adithya Reddy Venna, Prabhakar Kunreddy, Satish Reddy Mutthana,")
    lines.append("  Kiran Reddy Voorukonda, Ravi Mandala, Sriram Anunthula, Karthik Reddy Chinthakuntla,")
    lines.append("  Venkata Sai Yadlapalli, Gulam Siddiqui, Manoj Prabhakar Daram, Prem Kumar Malla,")
    lines.append("  Sudharshan Kumar Chebrolu, Rakesh Ravula, Mukesh Ravula, and ~60 more")

    lines.append("\nNON-GROUPABLE FIELDS (formula/text, CANNOT use in GROUP BY):")
    lines.append("  Submissions__c: Student_Name__c, BU_Name__c, Onsite_Manager_Name__c, Offshore_Manager_Name__c, Recruiter_Name__c, Onsite_Lead_Name__c")
    lines.append("  Interviews__c: Offshore_Manager__c, Onsite_Manager__c, Onsite_Lead__c, Lead_Name__c, Student_Technology__c")
    lines.append("  Student__c: Recruiter_Name__c, Offshore_Manager_Name__c, Onsite_Lead_Name__c, Onsite_Manager_Name__c")
    lines.append("  WORKAROUND: Fetch raw records with ORDER BY and let the answer AI summarize/group them.")
    lines.append("  For GROUP BY on BU/Lead, use reference fields: Student__c.Manager__c, Student__c.Recruiter__c (groupable)")

    lines.append("\nTERMINOLOGY MAPPING:")
    lines.append("  'BU' = Onsite Manager = Manager__c (the BU head). Name field: Manager__r.Name or BU_Name__c or Onsite_Manager__c")
    lines.append("  'Lead' = Offshore Manager/Lead (manages students from India). Fields: Offshore_Manager__c, Offshore_Manager_Name__c")
    lines.append("  'Recruiter' = Employee who recruits. Fields: Recruiter_Name__c, Recuter__c (on Submissions)")
    lines.append("  'Confirmation' = Verbal Confirmation status + Verbal_Confirmation_Date__c on Student__c")
    lines.append("  'Sub' = Submission (Submissions__c). 'Int' = Interview (Interviews__c)")

    lines.append("\n" + "=" * 60)
    lines.append("CRITICAL PostgreSQL RULES:")
    lines.append("  1. ALWAYS double-quote table and column names: \"Student__c\", \"Name\", \"Technology__c\"")
    lines.append("  2. Use PostgreSQL date functions: CURRENT_DATE, DATE_TRUNC('month', CURRENT_DATE), INTERVAL")
    lines.append("  3. NEVER use SOQL date literals (TODAY, THIS_MONTH, LAST_N_DAYS). Use PostgreSQL equivalents.")
    lines.append("  4. For 'days in market' use \"Days_in_Market_Business__c\" (number on Student__c)")
    lines.append("  5. For student status use \"Student_Marketing_Status__c\" (NOT Final_Marketing_Status__c)")
    lines.append("  6. For names, use ILIKE '%Name%' (case-insensitive) not exact = match.")
    lines.append("  7. Always include \"Name\" in SELECT for readability.")
    lines.append("  8. Use LEFT JOIN for cross-table queries (not __r syntax).")
    lines.append("  9. LIMIT 2000 max. Use ORDER BY for sorting.")
    lines.append("  10. For GROUP BY, use COUNT(*) AS cnt, not COUNT(Id).")

    lines.append("\n" + "=" * 60)
    lines.append("BUSINESS REPORT PATTERNS (exact queries for common reports):")
    lines.append("")
    lines.append("  1. Last week confirmations (congratulations):")
    lines.append("     SELECT Name, Manager__r.Name, Technology__c, Verbal_Confirmation_Date__c FROM Student__c WHERE Student_Marketing_Status__c = 'Verbal Confirmation' AND Verbal_Confirmation_Date__c = LAST_WEEK LIMIT 200")
    lines.append("")
    lines.append("  2. Last week Submissions & Interviews by BU:")
    lines.append("     Subs: SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE CreatedDate = LAST_WEEK ORDER BY BU_Name__c LIMIT 200")
    lines.append("     Ints: SELECT Student__r.Name, Onsite_Manager__c, Type__c, Final_Status__c, Interview_Date__c FROM Interviews__c WHERE CreatedDate = LAST_WEEK ORDER BY Onsite_Manager__c LIMIT 200")
    lines.append("")
    lines.append("  3. Last week Submissions & Interviews by Lead (Offshore Manager):")
    lines.append("     Subs: SELECT Student_Name__c, Offshore_Manager_Name__c, BU_Name__c, Client_Name__c FROM Submissions__c WHERE CreatedDate = LAST_WEEK ORDER BY Offshore_Manager_Name__c LIMIT 200")
    lines.append("     Ints: SELECT Student__r.Name, Offshore_Manager__c, Type__c, Final_Status__c FROM Interviews__c WHERE CreatedDate = LAST_WEEK ORDER BY Offshore_Manager__c LIMIT 200")
    lines.append("")
    lines.append("  4. Last week Student performance by BU:")
    lines.append("     SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE CreatedDate = LAST_WEEK ORDER BY BU_Name__c, Student_Name__c LIMIT 200")
    lines.append("     (Answer AI: group by BU, then by student, count submissions per student)")
    lines.append("")
    lines.append("  5. Last week Student performance by Lead:")
    lines.append("     SELECT Student_Name__c, Offshore_Manager_Name__c, BU_Name__c, Client_Name__c FROM Submissions__c WHERE CreatedDate = LAST_WEEK ORDER BY Offshore_Manager_Name__c, Student_Name__c LIMIT 200")
    lines.append("")
    lines.append("  6. Last week Recruiter performance by BU:")
    lines.append("     SELECT Recruiter_Name__c, Student_Name__c, BU_Name__c, Client_Name__c FROM Submissions__c WHERE CreatedDate = LAST_WEEK ORDER BY BU_Name__c, Recruiter_Name__c LIMIT 200")
    lines.append("     (Answer AI: group by BU, then by recruiter, count submissions per recruiter)")
    lines.append("")
    lines.append("  7. Last week Recruiter performance by Lead:")
    lines.append("     SELECT Recruiter_Name__c, Student_Name__c, Offshore_Manager_Name__c, BU_Name__c FROM Submissions__c WHERE CreatedDate = LAST_WEEK ORDER BY Offshore_Manager_Name__c, Recruiter_Name__c LIMIT 200")
    lines.append("")
    lines.append("  8. 2 Weeks no interviews by BU:")
    lines.append("     SELECT Name, Manager__r.Name, Technology__c, Days_in_Market_Business__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' AND Id NOT IN (SELECT Student__c FROM Interviews__c WHERE CreatedDate >= LAST_N_DAYS:14) ORDER BY Manager__r.Name LIMIT 200")
    lines.append("")
    lines.append("  9. 2 Weeks no interviews by Lead:")
    lines.append("     SELECT Name, Manager__r.Name, Offshore_Manager_Name__c, Technology__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' AND Id NOT IN (SELECT Student__c FROM Interviews__c WHERE CreatedDate >= LAST_N_DAYS:14) ORDER BY Offshore_Manager_Name__c LIMIT 200")

    lines.append("")
    lines.append("DAILY REPORT PATTERNS:")
    lines.append("")
    lines.append("  10. PreMarketing report by BU:")
    lines.append("     SELECT Name, Manager__r.Name, PreMarketingStatus__c, Resume_Preparation__c, Resume_Verified_By_Lead__c, Resume_Verified_By_Manager__c, Resume_Verification__c, Resume_Review__c, Otter_Screening__c, Otter_Final_Screening__c, Otter_Real_Time_Screeing_1__c, Otter_Real_Time_Screeing_2__c, Has_Linkedin_Created__c, Student_LinkedIn_Account_Review__c, MQ_Screening_By_Lead__c, MQ_Screening_By_Manager__c FROM Student__c WHERE Student_Marketing_Status__c = 'Pre Marketing' ORDER BY Manager__r.Name LIMIT 200")
    lines.append("     (Answer AI: group by BU name, show each student's pre-marketing checklist status)")
    lines.append("")
    lines.append("  11. Interview Mandatory Fields by BU (interviews missing key data):")
    lines.append("     SELECT Student__r.Name, Onsite_Manager__c, Type__c, Interview_Date__c, Amount__c, Bill_Rate__c, Final_Status__c FROM Interviews__c WHERE (Amount__c = null OR Bill_Rate__c = null OR Final_Status__c = null) AND CreatedDate = THIS_WEEK ORDER BY Onsite_Manager__c LIMIT 200")
    lines.append("     (Answer AI: group by BU, highlight which mandatory fields are missing)")
    lines.append("")
    lines.append("  12. Yesterday Submissions by BU:")
    lines.append("     SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c, Offshore_Manager_Name__c FROM Submissions__c WHERE Submission_Date__c = YESTERDAY ORDER BY BU_Name__c LIMIT 200")
    lines.append("")
    lines.append("  13. Yesterday Submissions by Offshore Manager:")
    lines.append("     SELECT Student_Name__c, Offshore_Manager_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE Submission_Date__c = YESTERDAY ORDER BY Offshore_Manager_Name__c LIMIT 200")
    lines.append("")
    lines.append("  14. Last 3 days no Submissions by BU:")
    lines.append("     SELECT Name, Manager__r.Name, Technology__c, Last_Submission_Date__c, Days_in_Market_Business__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' AND (Last_Submission_Date__c < LAST_N_DAYS:3 OR Last_Submission_Date__c = null) ORDER BY Manager__r.Name LIMIT 200")
    lines.append("     (Answer AI: group by BU, show students with no recent submissions)")
    lines.append("")
    lines.append("  15. Last 3 days no Submissions by Offshore Manager:")
    lines.append("     SELECT Name, Offshore_Manager_Name__c, Manager__r.Name, Technology__c, Last_Submission_Date__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' AND (Last_Submission_Date__c < LAST_N_DAYS:3 OR Last_Submission_Date__c = null) ORDER BY Offshore_Manager_Name__c LIMIT 200")
    lines.append("")

    lines.append("MONTHLY REPORT PATTERNS:")
    lines.append("")
    lines.append("  16. Monthly Submissions & Interviews & Confirmations & Amount by BU:")
    lines.append("     Subs: SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE Submission_Date__c = THIS_MONTH ORDER BY BU_Name__c LIMIT 200")
    lines.append("     Ints: SELECT Student__r.Name, Onsite_Manager__c, Type__c, Final_Status__c, Amount__c, Interview_Date__c FROM Interviews__c WHERE CreatedDate = THIS_MONTH ORDER BY Onsite_Manager__c LIMIT 200")
    lines.append("     Confirmations: SELECT Name, Manager__r.Name, Technology__c, Verbal_Confirmation_Date__c FROM Student__c WHERE Student_Marketing_Status__c = 'Verbal Confirmation' AND Verbal_Confirmation_Date__c = THIS_MONTH ORDER BY Manager__r.Name LIMIT 200")
    lines.append("     (Answer AI: group all by BU, show sub count + int count + confirmations + total amounts per BU)")
    lines.append("")
    lines.append("  17. Total Interviews and Amounts (monthly):")
    lines.append("     SELECT Student__r.Name, Onsite_Manager__c, Type__c, Amount__c, Amount_INR__c, Bill_Rate__c, Final_Status__c, Interview_Date__c FROM Interviews__c WHERE CreatedDate = THIS_MONTH ORDER BY Onsite_Manager__c LIMIT 200")
    lines.append("     (Answer AI: show total interview count, sum of Amount USD, sum of Amount INR, grouped by BU)")
    lines.append("")
    lines.append("  18. Monthly Student performance by BU:")
    lines.append("     SELECT Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE Submission_Date__c = THIS_MONTH ORDER BY BU_Name__c, Student_Name__c LIMIT 200")
    lines.append("     (Answer AI: group by BU then student, count submissions per student, rank by count)")
    lines.append("")
    lines.append("  19. Monthly Student performance by Offshore Manager:")
    lines.append("     SELECT Student_Name__c, Offshore_Manager_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE Submission_Date__c = THIS_MONTH ORDER BY Offshore_Manager_Name__c, Student_Name__c LIMIT 200")
    lines.append("")
    lines.append("  20. Monthly Recruiter performance by BU:")
    lines.append("     SELECT Recruiter_Name__c, Student_Name__c, BU_Name__c, Client_Name__c, Submission_Date__c FROM Submissions__c WHERE Submission_Date__c = THIS_MONTH ORDER BY BU_Name__c, Recruiter_Name__c LIMIT 200")
    lines.append("     (Answer AI: group by BU then recruiter, count submissions per recruiter)")
    lines.append("")
    lines.append("  21. Monthly Recruiter performance by Offshore Manager:")
    lines.append("     SELECT Recruiter_Name__c, Student_Name__c, Offshore_Manager_Name__c, BU_Name__c, Submission_Date__c FROM Submissions__c WHERE Submission_Date__c = THIS_MONTH ORDER BY Offshore_Manager_Name__c, Recruiter_Name__c LIMIT 200")
    lines.append("")
    lines.append("  22. Expenses & Per Placement Cost by BU:")
    lines.append("     SELECT Name, Total_Expenses_MIS__c, Each_Placement_Cost__c, BU_Student_With_Job_Count__c, Students_Count__c, In_Market_Students_Count__c, Verbal_Count__c, IN_JOB_Students_Count__c FROM Manager__c WHERE Active__c = true ORDER BY Name LIMIT 200")
    lines.append("     (Answer AI: show each BU with expenses, per-placement cost, student counts)")
    lines.append("")
    lines.append("  23. Job Payroll & Bench Payroll by BU:")
    lines.append("     Active jobs: SELECT Student__r.Name, Share_With__r.Name, PayRate__c, Caluculated_Pay_Rate__c, Pay_Roll_Tax__c, Profit__c, Bill_Rate__c, Payroll_Month__c, Project_Type__c, Technology__c FROM Job__c WHERE Active__c = true ORDER BY Share_With__r.Name LIMIT 200")
    lines.append("     (Answer AI: group by BU (Share_With__r.Name), show payroll totals, calculate bench payroll = students in market with no job)")
    lines.append("     Bench: SELECT Name, Manager__r.Name, Technology__c, Days_in_Market_Business__c FROM Student__c WHERE Student_Marketing_Status__c = 'In Market' ORDER BY Manager__r.Name LIMIT 200")
    lines.append("     (Answer AI: combine job payroll + bench count per BU)")
    lines.append("")

    lines.append("\nOTHER COMMON QUERIES:")
    lines.append("  Students by status: SELECT Student_Marketing_Status__c, COUNT(Id) FROM Student__c GROUP BY Student_Marketing_Status__c")
    lines.append("  Students under BU: SELECT Name, Manager__r.Name, Technology__c, Student_Marketing_Status__c FROM Student__c WHERE Manager__r.Name LIKE '%Divya%' LIMIT 200")
    lines.append("  Students by technology: SELECT Technology__c, COUNT(Id) FROM Student__c GROUP BY Technology__c")
    lines.append("  BU performance: SELECT BU__r.Name, In_Market_Students_Count__c, Submissions_Count__c, Interview_Count__c FROM BU_Performance__c ORDER BY Submissions_Count__c DESC LIMIT 20")
    lines.append("  Jobs: SELECT Student__r.Name, PayRate__c, Project_Type__c, Technology__c FROM Job__c LIMIT 200")

    return "\n".join(lines)
