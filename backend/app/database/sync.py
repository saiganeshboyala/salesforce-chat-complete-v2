"""
Salesforce -> PostgreSQL sync engine.
Incremental sync with exact Salesforce field names preserved.
Auto-generated from schema_cache.json
"""
import logging
import asyncio
import threading
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.database.engine import engine, async_session
from app.database.models import (
    Base, Account, Contact, SFUser, SFReport, Student, Submission, Interview, Manager, Job, Employee, BUPerformance, BS, TechSupport, NewStudent, ManagerCard, Cluster, Organization, PayOff, SyncLog
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
    if not last_sync:
        return ""
    ts = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
    return f" WHERE LastModifiedDate > {ts}"


async def _upsert_batch(session, model, records_data, batch_size=5000):
    if not records_data:
        return 0

    data_keys = set(records_data[0].keys()) - {"Id"}
    num_cols = len(records_data[0])
    safe_batch = min(batch_size, 32000 // num_cols)
    if safe_batch < 1:
        safe_batch = 1

    total = 0
    for i in range(0, len(records_data), safe_batch):
        batch = records_data[i:i + safe_batch]
        stmt = pg_insert(model.__table__).values(batch)
        update_cols = {k: stmt.excluded[k] for k in data_keys}
        stmt = stmt.on_conflict_do_update(index_elements=["Id"], set_=update_cols)
        await session.execute(stmt)
        total += len(batch)

    await session.flush()
    return total


async def _sync_account(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Account ({mode})...")

    soql_fields = "Id, MasterRecordId, Name, Type, ParentId, BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, BillingLatitude, BillingLongitude, BillingGeocodeAccuracy, ShippingStreet, ShippingCity, ShippingState, ShippingPostalCode, ShippingCountry, ShippingLatitude, ShippingLongitude, ShippingGeocodeAccuracy, Phone, Fax, Website, PhotoUrl, Industry, AnnualRevenue, NumberOfEmployees, Description, OwnerId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastViewedDate, LastReferencedDate, IsCustomerPortal, Jigsaw, JigsawCompanyId, AccountSource, SicDesc, Domain_Name__c, Account_Type__c, Cluster__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Account{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "MasterRecordId": r.get("MasterRecordId"),
            "Name": r.get("Name"),
            "Type": r.get("Type"),
            "ParentId": r.get("ParentId"),
            "BillingStreet": r.get("BillingStreet"),
            "BillingCity": r.get("BillingCity"),
            "BillingState": r.get("BillingState"),
            "BillingPostalCode": r.get("BillingPostalCode"),
            "BillingCountry": r.get("BillingCountry"),
            "BillingLatitude": r.get("BillingLatitude"),
            "BillingLongitude": r.get("BillingLongitude"),
            "BillingGeocodeAccuracy": r.get("BillingGeocodeAccuracy"),
            "ShippingStreet": r.get("ShippingStreet"),
            "ShippingCity": r.get("ShippingCity"),
            "ShippingState": r.get("ShippingState"),
            "ShippingPostalCode": r.get("ShippingPostalCode"),
            "ShippingCountry": r.get("ShippingCountry"),
            "ShippingLatitude": r.get("ShippingLatitude"),
            "ShippingLongitude": r.get("ShippingLongitude"),
            "ShippingGeocodeAccuracy": r.get("ShippingGeocodeAccuracy"),
            "Phone": r.get("Phone"),
            "Fax": r.get("Fax"),
            "Website": r.get("Website"),
            "PhotoUrl": r.get("PhotoUrl"),
            "Industry": r.get("Industry"),
            "AnnualRevenue": r.get("AnnualRevenue"),
            "NumberOfEmployees": r.get("NumberOfEmployees"),
            "Description": r.get("Description"),
            "OwnerId": r.get("OwnerId"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "IsCustomerPortal": r.get("IsCustomerPortal", False),
            "Jigsaw": r.get("Jigsaw"),
            "JigsawCompanyId": r.get("JigsawCompanyId"),
            "AccountSource": r.get("AccountSource"),
            "SicDesc": r.get("SicDesc"),
            "Domain_Name__c": r.get("Domain_Name__c"),
            "Account_Type__c": r.get("Account_Type__c"),
            "Cluster__c": r.get("Cluster__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, Account, records_data)


async def _sync_contact(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Contact ({mode})...")

    soql_fields = "Id, MasterRecordId, AccountId, LastName, FirstName, Salutation, Name, OtherStreet, OtherCity, OtherState, OtherPostalCode, OtherCountry, OtherLatitude, OtherLongitude, OtherGeocodeAccuracy, MailingStreet, MailingCity, MailingState, MailingPostalCode, MailingCountry, MailingLatitude, MailingLongitude, MailingGeocodeAccuracy, Phone, Fax, MobilePhone, HomePhone, OtherPhone, AssistantPhone, ReportsToId, Email, Title, Department, AssistantName, LeadSource, Birthdate, Description, OwnerId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastCURequestDate, LastCUUpdateDate, LastViewedDate, LastReferencedDate, EmailBouncedReason, EmailBouncedDate, IsEmailBounced, PhotoUrl, Jigsaw, JigsawContactId, IndividualId, IsPriorityRecord, Lead__c, Contact_Type__c, Ext__c, Domain_Formula__c, Contact_User_Type__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Contact{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "MasterRecordId": r.get("MasterRecordId"),
            "AccountId": r.get("AccountId"),
            "LastName": r.get("LastName"),
            "FirstName": r.get("FirstName"),
            "Salutation": r.get("Salutation"),
            "Name": r.get("Name"),
            "OtherStreet": r.get("OtherStreet"),
            "OtherCity": r.get("OtherCity"),
            "OtherState": r.get("OtherState"),
            "OtherPostalCode": r.get("OtherPostalCode"),
            "OtherCountry": r.get("OtherCountry"),
            "OtherLatitude": r.get("OtherLatitude"),
            "OtherLongitude": r.get("OtherLongitude"),
            "OtherGeocodeAccuracy": r.get("OtherGeocodeAccuracy"),
            "MailingStreet": r.get("MailingStreet"),
            "MailingCity": r.get("MailingCity"),
            "MailingState": r.get("MailingState"),
            "MailingPostalCode": r.get("MailingPostalCode"),
            "MailingCountry": r.get("MailingCountry"),
            "MailingLatitude": r.get("MailingLatitude"),
            "MailingLongitude": r.get("MailingLongitude"),
            "MailingGeocodeAccuracy": r.get("MailingGeocodeAccuracy"),
            "Phone": r.get("Phone"),
            "Fax": r.get("Fax"),
            "MobilePhone": r.get("MobilePhone"),
            "HomePhone": r.get("HomePhone"),
            "OtherPhone": r.get("OtherPhone"),
            "AssistantPhone": r.get("AssistantPhone"),
            "ReportsToId": r.get("ReportsToId"),
            "Email": r.get("Email"),
            "Title": r.get("Title"),
            "Department": r.get("Department"),
            "AssistantName": r.get("AssistantName"),
            "LeadSource": r.get("LeadSource"),
            "Birthdate": _parse_sf_date(r.get("Birthdate")),
            "Description": r.get("Description"),
            "OwnerId": r.get("OwnerId"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastCURequestDate": _parse_sf_datetime(r.get("LastCURequestDate")),
            "LastCUUpdateDate": _parse_sf_datetime(r.get("LastCUUpdateDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "EmailBouncedReason": r.get("EmailBouncedReason"),
            "EmailBouncedDate": _parse_sf_datetime(r.get("EmailBouncedDate")),
            "IsEmailBounced": r.get("IsEmailBounced", False),
            "PhotoUrl": r.get("PhotoUrl"),
            "Jigsaw": r.get("Jigsaw"),
            "JigsawContactId": r.get("JigsawContactId"),
            "IndividualId": r.get("IndividualId"),
            "IsPriorityRecord": r.get("IsPriorityRecord", False),
            "Lead__c": r.get("Lead__c"),
            "Contact_Type__c": r.get("Contact_Type__c"),
            "Ext__c": r.get("Ext__c"),
            "Domain_Formula__c": r.get("Domain_Formula__c"),
            "Contact_User_Type__c": r.get("Contact_User_Type__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, Contact, records_data)


async def _sync_user(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing User ({mode})...")

    soql_fields = "Id, Username, LastName, FirstName, Name, CompanyName, Division, Department, Title, Street, City, State, PostalCode, Country, Latitude, Longitude, GeocodeAccuracy, Email, EmailPreferencesAutoBcc, EmailPreferencesAutoBccStayInTouch, EmailPreferencesStayInTouchReminder, SenderEmail, SenderName, Signature, StayInTouchSubject, StayInTouchSignature, StayInTouchNote, Phone, Fax, MobilePhone, Alias, CommunityNickname, BadgeText, IsActive, TimeZoneSidKey, UserRoleId, LocaleSidKey, ReceivesInfoEmails, ReceivesAdminInfoEmails, EmailEncodingKey, ProfileId, UserType, StartDay, EndDay, LanguageLocaleKey, EmployeeNumber, DelegatedApproverId, ManagerId, LastLoginDate, LastPasswordChangeDate, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, PasswordExpirationDate, NumberOfFailedLogins, SuAccessExpirationDate, OfflineTrialExpirationDate, OfflinePdaTrialExpirationDate, UserPermissionsMarketingUser, UserPermissionsOfflineUser, UserPermissionsAvantgoUser, UserPermissionsCallCenterAutoLogin, UserPermissionsSFContentUser, UserPermissionsInteractionUser, UserPermissionsSupportUser, ForecastEnabled, UserPreferencesActivityRemindersPopup, UserPreferencesEventRemindersCheckboxDefault, UserPreferencesTaskRemindersCheckboxDefault, UserPreferencesReminderSoundOff, UserPreferencesDisableAllFeedsEmail, UserPreferencesDisableFollowersEmail, UserPreferencesDisableProfilePostEmail, UserPreferencesDisableChangeCommentEmail, UserPreferencesDisableLaterCommentEmail, UserPreferencesDisProfPostCommentEmail, UserPreferencesApexPagesDeveloperMode, UserPreferencesReceiveNoNotificationsAsApprover, UserPreferencesReceiveNotificationsAsDelegatedApprover, UserPreferencesHideCSNGetChatterMobileTask, UserPreferencesDisableMentionsPostEmail, UserPreferencesDisMentionsCommentEmail, UserPreferencesHideCSNDesktopTask, UserPreferencesHideChatterOnboardingSplash, UserPreferencesHideSecondChatterOnboardingSplash, UserPreferencesDisCommentAfterLikeEmail, UserPreferencesDisableLikeEmail, UserPreferencesSortFeedByComment, UserPreferencesDisableMessageEmail, UserPreferencesDisableBookmarkEmail, UserPreferencesDisableSharePostEmail, UserPreferencesEnableAutoSubForFeeds, UserPreferencesDisableFileShareNotificationsForApi, UserPreferencesShowTitleToExternalUsers, UserPreferencesShowManagerToExternalUsers, UserPreferencesShowEmailToExternalUsers, UserPreferencesShowWorkPhoneToExternalUsers, UserPreferencesShowMobilePhoneToExternalUsers, UserPreferencesShowFaxToExternalUsers, UserPreferencesShowStreetAddressToExternalUsers, UserPreferencesShowCityToExternalUsers, UserPreferencesShowStateToExternalUsers, UserPreferencesShowPostalCodeToExternalUsers, UserPreferencesShowCountryToExternalUsers, UserPreferencesShowProfilePicToGuestUsers, UserPreferencesShowTitleToGuestUsers, UserPreferencesShowCityToGuestUsers, UserPreferencesShowStateToGuestUsers, UserPreferencesShowPostalCodeToGuestUsers, UserPreferencesShowCountryToGuestUsers, UserPreferencesShowForecastingChangeSignals, UserPreferencesLiveAgentMiawSetupDeflection, UserPreferencesHideS1BrowserUI, UserPreferencesDisableEndorsementEmail, UserPreferencesPathAssistantCollapsed, UserPreferencesCacheDiagnostics, UserPreferencesShowEmailToGuestUsers, UserPreferencesShowManagerToGuestUsers, UserPreferencesShowWorkPhoneToGuestUsers, UserPreferencesShowMobilePhoneToGuestUsers, UserPreferencesShowFaxToGuestUsers, UserPreferencesShowStreetAddressToGuestUsers, UserPreferencesLightningExperiencePreferred, UserPreferencesPreviewLightning, UserPreferencesHideEndUserOnboardingAssistantModal, UserPreferencesHideLightningMigrationModal, UserPreferencesHideSfxWelcomeMat, UserPreferencesHideBiggerPhotoCallout, UserPreferencesGlobalNavBarWTShown, UserPreferencesGlobalNavGridMenuWTShown, UserPreferencesCreateLEXAppsWTShown, UserPreferencesFavoritesWTShown, UserPreferencesRecordHomeSectionCollapseWTShown, UserPreferencesRecordHomeReservedWTShown, UserPreferencesFavoritesShowTopFavorites, UserPreferencesExcludeMailAppAttachments, UserPreferencesSuppressTaskSFXReminders, UserPreferencesSuppressEventSFXReminders, UserPreferencesPreviewCustomTheme, UserPreferencesHasCelebrationBadge, UserPreferencesUserDebugModePref, UserPreferencesSRHOverrideActivities, UserPreferencesNewLightningReportRunPageEnabled, UserPreferencesReverseOpenActivitiesView, UserPreferencesHasSentWarningEmail, UserPreferencesHasSentWarningEmail238, UserPreferencesHasSentWarningEmail240, UserPreferencesNativeEmailClient, UserPreferencesHideBrowseProductRedirectConfirmation, UserPreferencesHideOnlineSalesAppWelcomeMat, UserPreferencesShowForecastingRoundedAmounts, ContactId, AccountId, CallCenterId, Extension, PortalRole, IsPortalEnabled, FederationIdentifier, AboutMe, FullPhotoUrl, SmallPhotoUrl, IsExtIndicatorVisible, OutOfOfficeMessage, MediumPhotoUrl, DigestFrequency, DefaultGroupNotificationFrequency, LastViewedDate, LastReferencedDate, BannerPhotoUrl, SmallBannerPhotoUrl, MediumBannerPhotoUrl, IsProfilePhotoActive, IndividualId, Company__c, Last_Login_Date__c, UserLiscence__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM User{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "Username": r.get("Username"),
            "LastName": r.get("LastName"),
            "FirstName": r.get("FirstName"),
            "Name": r.get("Name"),
            "CompanyName": r.get("CompanyName"),
            "Division": r.get("Division"),
            "Department": r.get("Department"),
            "Title": r.get("Title"),
            "Street": r.get("Street"),
            "City": r.get("City"),
            "State": r.get("State"),
            "PostalCode": r.get("PostalCode"),
            "Country": r.get("Country"),
            "Latitude": r.get("Latitude"),
            "Longitude": r.get("Longitude"),
            "GeocodeAccuracy": r.get("GeocodeAccuracy"),
            "Email": r.get("Email"),
            "EmailPreferencesAutoBcc": r.get("EmailPreferencesAutoBcc", False),
            "EmailPreferencesAutoBccStayInTouch": r.get("EmailPreferencesAutoBccStayInTouch", False),
            "EmailPreferencesStayInTouchReminder": r.get("EmailPreferencesStayInTouchReminder", False),
            "SenderEmail": r.get("SenderEmail"),
            "SenderName": r.get("SenderName"),
            "Signature": r.get("Signature"),
            "StayInTouchSubject": r.get("StayInTouchSubject"),
            "StayInTouchSignature": r.get("StayInTouchSignature"),
            "StayInTouchNote": r.get("StayInTouchNote"),
            "Phone": r.get("Phone"),
            "Fax": r.get("Fax"),
            "MobilePhone": r.get("MobilePhone"),
            "Alias": r.get("Alias"),
            "CommunityNickname": r.get("CommunityNickname"),
            "BadgeText": r.get("BadgeText"),
            "IsActive": r.get("IsActive", False),
            "TimeZoneSidKey": r.get("TimeZoneSidKey"),
            "UserRoleId": r.get("UserRoleId"),
            "LocaleSidKey": r.get("LocaleSidKey"),
            "ReceivesInfoEmails": r.get("ReceivesInfoEmails", False),
            "ReceivesAdminInfoEmails": r.get("ReceivesAdminInfoEmails", False),
            "EmailEncodingKey": r.get("EmailEncodingKey"),
            "ProfileId": r.get("ProfileId"),
            "UserType": r.get("UserType"),
            "StartDay": r.get("StartDay"),
            "EndDay": r.get("EndDay"),
            "LanguageLocaleKey": r.get("LanguageLocaleKey"),
            "EmployeeNumber": r.get("EmployeeNumber"),
            "DelegatedApproverId": r.get("DelegatedApproverId"),
            "ManagerId": r.get("ManagerId"),
            "LastLoginDate": _parse_sf_datetime(r.get("LastLoginDate")),
            "LastPasswordChangeDate": _parse_sf_datetime(r.get("LastPasswordChangeDate")),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "PasswordExpirationDate": _parse_sf_datetime(r.get("PasswordExpirationDate")),
            "NumberOfFailedLogins": r.get("NumberOfFailedLogins"),
            "SuAccessExpirationDate": _parse_sf_date(r.get("SuAccessExpirationDate")),
            "OfflineTrialExpirationDate": _parse_sf_datetime(r.get("OfflineTrialExpirationDate")),
            "OfflinePdaTrialExpirationDate": _parse_sf_datetime(r.get("OfflinePdaTrialExpirationDate")),
            "UserPermissionsMarketingUser": r.get("UserPermissionsMarketingUser", False),
            "UserPermissionsOfflineUser": r.get("UserPermissionsOfflineUser", False),
            "UserPermissionsAvantgoUser": r.get("UserPermissionsAvantgoUser", False),
            "UserPermissionsCallCenterAutoLogin": r.get("UserPermissionsCallCenterAutoLogin", False),
            "UserPermissionsSFContentUser": r.get("UserPermissionsSFContentUser", False),
            "UserPermissionsInteractionUser": r.get("UserPermissionsInteractionUser", False),
            "UserPermissionsSupportUser": r.get("UserPermissionsSupportUser", False),
            "ForecastEnabled": r.get("ForecastEnabled", False),
            "UserPreferencesActivityRemindersPopup": r.get("UserPreferencesActivityRemindersPopup", False),
            "UserPreferencesEventRemindersCheckboxDefault": r.get("UserPreferencesEventRemindersCheckboxDefault", False),
            "UserPreferencesTaskRemindersCheckboxDefault": r.get("UserPreferencesTaskRemindersCheckboxDefault", False),
            "UserPreferencesReminderSoundOff": r.get("UserPreferencesReminderSoundOff", False),
            "UserPreferencesDisableAllFeedsEmail": r.get("UserPreferencesDisableAllFeedsEmail", False),
            "UserPreferencesDisableFollowersEmail": r.get("UserPreferencesDisableFollowersEmail", False),
            "UserPreferencesDisableProfilePostEmail": r.get("UserPreferencesDisableProfilePostEmail", False),
            "UserPreferencesDisableChangeCommentEmail": r.get("UserPreferencesDisableChangeCommentEmail", False),
            "UserPreferencesDisableLaterCommentEmail": r.get("UserPreferencesDisableLaterCommentEmail", False),
            "UserPreferencesDisProfPostCommentEmail": r.get("UserPreferencesDisProfPostCommentEmail", False),
            "UserPreferencesApexPagesDeveloperMode": r.get("UserPreferencesApexPagesDeveloperMode", False),
            "UserPreferencesReceiveNoNotificationsAsApprover": r.get("UserPreferencesReceiveNoNotificationsAsApprover", False),
            "UserPreferencesReceiveNotificationsAsDelegatedApprover": r.get("UserPreferencesReceiveNotificationsAsDelegatedApprover", False),
            "UserPreferencesHideCSNGetChatterMobileTask": r.get("UserPreferencesHideCSNGetChatterMobileTask", False),
            "UserPreferencesDisableMentionsPostEmail": r.get("UserPreferencesDisableMentionsPostEmail", False),
            "UserPreferencesDisMentionsCommentEmail": r.get("UserPreferencesDisMentionsCommentEmail", False),
            "UserPreferencesHideCSNDesktopTask": r.get("UserPreferencesHideCSNDesktopTask", False),
            "UserPreferencesHideChatterOnboardingSplash": r.get("UserPreferencesHideChatterOnboardingSplash", False),
            "UserPreferencesHideSecondChatterOnboardingSplash": r.get("UserPreferencesHideSecondChatterOnboardingSplash", False),
            "UserPreferencesDisCommentAfterLikeEmail": r.get("UserPreferencesDisCommentAfterLikeEmail", False),
            "UserPreferencesDisableLikeEmail": r.get("UserPreferencesDisableLikeEmail", False),
            "UserPreferencesSortFeedByComment": r.get("UserPreferencesSortFeedByComment", False),
            "UserPreferencesDisableMessageEmail": r.get("UserPreferencesDisableMessageEmail", False),
            "UserPreferencesDisableBookmarkEmail": r.get("UserPreferencesDisableBookmarkEmail", False),
            "UserPreferencesDisableSharePostEmail": r.get("UserPreferencesDisableSharePostEmail", False),
            "UserPreferencesEnableAutoSubForFeeds": r.get("UserPreferencesEnableAutoSubForFeeds", False),
            "UserPreferencesDisableFileShareNotificationsForApi": r.get("UserPreferencesDisableFileShareNotificationsForApi", False),
            "UserPreferencesShowTitleToExternalUsers": r.get("UserPreferencesShowTitleToExternalUsers", False),
            "UserPreferencesShowManagerToExternalUsers": r.get("UserPreferencesShowManagerToExternalUsers", False),
            "UserPreferencesShowEmailToExternalUsers": r.get("UserPreferencesShowEmailToExternalUsers", False),
            "UserPreferencesShowWorkPhoneToExternalUsers": r.get("UserPreferencesShowWorkPhoneToExternalUsers", False),
            "UserPreferencesShowMobilePhoneToExternalUsers": r.get("UserPreferencesShowMobilePhoneToExternalUsers", False),
            "UserPreferencesShowFaxToExternalUsers": r.get("UserPreferencesShowFaxToExternalUsers", False),
            "UserPreferencesShowStreetAddressToExternalUsers": r.get("UserPreferencesShowStreetAddressToExternalUsers", False),
            "UserPreferencesShowCityToExternalUsers": r.get("UserPreferencesShowCityToExternalUsers", False),
            "UserPreferencesShowStateToExternalUsers": r.get("UserPreferencesShowStateToExternalUsers", False),
            "UserPreferencesShowPostalCodeToExternalUsers": r.get("UserPreferencesShowPostalCodeToExternalUsers", False),
            "UserPreferencesShowCountryToExternalUsers": r.get("UserPreferencesShowCountryToExternalUsers", False),
            "UserPreferencesShowProfilePicToGuestUsers": r.get("UserPreferencesShowProfilePicToGuestUsers", False),
            "UserPreferencesShowTitleToGuestUsers": r.get("UserPreferencesShowTitleToGuestUsers", False),
            "UserPreferencesShowCityToGuestUsers": r.get("UserPreferencesShowCityToGuestUsers", False),
            "UserPreferencesShowStateToGuestUsers": r.get("UserPreferencesShowStateToGuestUsers", False),
            "UserPreferencesShowPostalCodeToGuestUsers": r.get("UserPreferencesShowPostalCodeToGuestUsers", False),
            "UserPreferencesShowCountryToGuestUsers": r.get("UserPreferencesShowCountryToGuestUsers", False),
            "UserPreferencesShowForecastingChangeSignals": r.get("UserPreferencesShowForecastingChangeSignals", False),
            "UserPreferencesLiveAgentMiawSetupDeflection": r.get("UserPreferencesLiveAgentMiawSetupDeflection", False),
            "UserPreferencesHideS1BrowserUI": r.get("UserPreferencesHideS1BrowserUI", False),
            "UserPreferencesDisableEndorsementEmail": r.get("UserPreferencesDisableEndorsementEmail", False),
            "UserPreferencesPathAssistantCollapsed": r.get("UserPreferencesPathAssistantCollapsed", False),
            "UserPreferencesCacheDiagnostics": r.get("UserPreferencesCacheDiagnostics", False),
            "UserPreferencesShowEmailToGuestUsers": r.get("UserPreferencesShowEmailToGuestUsers", False),
            "UserPreferencesShowManagerToGuestUsers": r.get("UserPreferencesShowManagerToGuestUsers", False),
            "UserPreferencesShowWorkPhoneToGuestUsers": r.get("UserPreferencesShowWorkPhoneToGuestUsers", False),
            "UserPreferencesShowMobilePhoneToGuestUsers": r.get("UserPreferencesShowMobilePhoneToGuestUsers", False),
            "UserPreferencesShowFaxToGuestUsers": r.get("UserPreferencesShowFaxToGuestUsers", False),
            "UserPreferencesShowStreetAddressToGuestUsers": r.get("UserPreferencesShowStreetAddressToGuestUsers", False),
            "UserPreferencesLightningExperiencePreferred": r.get("UserPreferencesLightningExperiencePreferred", False),
            "UserPreferencesPreviewLightning": r.get("UserPreferencesPreviewLightning", False),
            "UserPreferencesHideEndUserOnboardingAssistantModal": r.get("UserPreferencesHideEndUserOnboardingAssistantModal", False),
            "UserPreferencesHideLightningMigrationModal": r.get("UserPreferencesHideLightningMigrationModal", False),
            "UserPreferencesHideSfxWelcomeMat": r.get("UserPreferencesHideSfxWelcomeMat", False),
            "UserPreferencesHideBiggerPhotoCallout": r.get("UserPreferencesHideBiggerPhotoCallout", False),
            "UserPreferencesGlobalNavBarWTShown": r.get("UserPreferencesGlobalNavBarWTShown", False),
            "UserPreferencesGlobalNavGridMenuWTShown": r.get("UserPreferencesGlobalNavGridMenuWTShown", False),
            "UserPreferencesCreateLEXAppsWTShown": r.get("UserPreferencesCreateLEXAppsWTShown", False),
            "UserPreferencesFavoritesWTShown": r.get("UserPreferencesFavoritesWTShown", False),
            "UserPreferencesRecordHomeSectionCollapseWTShown": r.get("UserPreferencesRecordHomeSectionCollapseWTShown", False),
            "UserPreferencesRecordHomeReservedWTShown": r.get("UserPreferencesRecordHomeReservedWTShown", False),
            "UserPreferencesFavoritesShowTopFavorites": r.get("UserPreferencesFavoritesShowTopFavorites", False),
            "UserPreferencesExcludeMailAppAttachments": r.get("UserPreferencesExcludeMailAppAttachments", False),
            "UserPreferencesSuppressTaskSFXReminders": r.get("UserPreferencesSuppressTaskSFXReminders", False),
            "UserPreferencesSuppressEventSFXReminders": r.get("UserPreferencesSuppressEventSFXReminders", False),
            "UserPreferencesPreviewCustomTheme": r.get("UserPreferencesPreviewCustomTheme", False),
            "UserPreferencesHasCelebrationBadge": r.get("UserPreferencesHasCelebrationBadge", False),
            "UserPreferencesUserDebugModePref": r.get("UserPreferencesUserDebugModePref", False),
            "UserPreferencesSRHOverrideActivities": r.get("UserPreferencesSRHOverrideActivities", False),
            "UserPreferencesNewLightningReportRunPageEnabled": r.get("UserPreferencesNewLightningReportRunPageEnabled", False),
            "UserPreferencesReverseOpenActivitiesView": r.get("UserPreferencesReverseOpenActivitiesView", False),
            "UserPreferencesHasSentWarningEmail": r.get("UserPreferencesHasSentWarningEmail", False),
            "UserPreferencesHasSentWarningEmail238": r.get("UserPreferencesHasSentWarningEmail238", False),
            "UserPreferencesHasSentWarningEmail240": r.get("UserPreferencesHasSentWarningEmail240", False),
            "UserPreferencesNativeEmailClient": r.get("UserPreferencesNativeEmailClient", False),
            "UserPreferencesHideBrowseProductRedirectConfirmation": r.get("UserPreferencesHideBrowseProductRedirectConfirmation", False),
            "UserPreferencesHideOnlineSalesAppWelcomeMat": r.get("UserPreferencesHideOnlineSalesAppWelcomeMat", False),
            "UserPreferencesShowForecastingRoundedAmounts": r.get("UserPreferencesShowForecastingRoundedAmounts", False),
            "ContactId": r.get("ContactId"),
            "AccountId": r.get("AccountId"),
            "CallCenterId": r.get("CallCenterId"),
            "Extension": r.get("Extension"),
            "PortalRole": r.get("PortalRole"),
            "IsPortalEnabled": r.get("IsPortalEnabled", False),
            "FederationIdentifier": r.get("FederationIdentifier"),
            "AboutMe": r.get("AboutMe"),
            "FullPhotoUrl": r.get("FullPhotoUrl"),
            "SmallPhotoUrl": r.get("SmallPhotoUrl"),
            "IsExtIndicatorVisible": r.get("IsExtIndicatorVisible", False),
            "OutOfOfficeMessage": r.get("OutOfOfficeMessage"),
            "MediumPhotoUrl": r.get("MediumPhotoUrl"),
            "DigestFrequency": r.get("DigestFrequency"),
            "DefaultGroupNotificationFrequency": r.get("DefaultGroupNotificationFrequency"),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "BannerPhotoUrl": r.get("BannerPhotoUrl"),
            "SmallBannerPhotoUrl": r.get("SmallBannerPhotoUrl"),
            "MediumBannerPhotoUrl": r.get("MediumBannerPhotoUrl"),
            "IsProfilePhotoActive": r.get("IsProfilePhotoActive", False),
            "IndividualId": r.get("IndividualId"),
            "Company__c": r.get("Company__c"),
            "Last_Login_Date__c": _parse_sf_datetime(r.get("Last_Login_Date__c")),
            "UserLiscence__c": r.get("UserLiscence__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, SFUser, records_data)


async def _sync_report(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Report ({mode})...")

    soql_fields = "Id, OwnerId, FolderName, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, Name, Description, DeveloperName, NamespacePrefix, LastRunDate, Format, LastViewedDate, LastReferencedDate"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Report{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "OwnerId": r.get("OwnerId"),
            "FolderName": r.get("FolderName"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "Name": r.get("Name"),
            "Description": r.get("Description"),
            "DeveloperName": r.get("DeveloperName"),
            "NamespacePrefix": r.get("NamespacePrefix"),
            "LastRunDate": _parse_sf_datetime(r.get("LastRunDate")),
            "Format": r.get("Format"),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "synced_at": now,
        })

    return await _upsert_batch(session, SFReport, records_data)


async def _sync_student(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Student__c ({mode})...")

    soql_fields = "Id, OwnerId, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastViewedDate, LastReferencedDate, Batch__c, Comments__c, DL_Expiry_Date__c, DL_Issue_Date__c, DL_Photo_Uploaded__c, DOB__c, District_In_India__c, EAD_Card_Number__c, Father_First_Name__c, Father_Last_Name__c, In_Market_Student_Count__c, Final_Marketing_Status__c, Folder_Sharing__c, Folder_in_Drive__c, GC_Back_Ref_Number__c, GC_Back_Verified__c, GC_Catogery__c, GC_Expiry_Date__c, GC_Issued_Date__c, GHID__c, Gender__c, Google_Voice_Number__c, Guest_House__c, Has_Linkedin_Created__c, IS_DL_ID_CHANGED__c, India_Address_Line1__c, India_Address_Line2__c, Total_Count_not_Exit__c, Is_All_Docs_Reviewed_by_Manager__c, Is_DL_Ready__c, Is_DOC_Ready__c, Is_GC_Front_Verified__c, Is_Marketing_Sheet_Updated_by_Manager_Le__c, Is_Offer_Letter_Issued__c, LeadID__c, Linkedin_Connection_Count__c, Linkedin_URL__c, MQ_Screening_By_Lead__c, MQ_Screening_By_Manager__c, MS_Sheet_Explanation__c, MS_Uploaded_By_Student__c, Marketing_Company__c, Marketing_DOB__c, Marketing_Email__c, Marketing_End_Date__c, Marketing_Sheet_Screening_by_Lead__c, Marketing_Sheet_Screening_by_Manager__c, Marketing_Start_Date__c, Marketing_Visa_Status__c, Mother_First_Name__c, Mother_Last_Name__c, Offer_Issued_By_Name__c, Offer_Issued_By_Phone__c, Offer_Type__c, Onboarding_Company__c, Onboarding_End_Date__c, Onboarding_Start_Date__c, Onboarding_Visa_Status__c, Otter_Final_Screening__c, Otter_Real_Time_Screeing_1__c, Otter_Real_Time_Screeing_2__c, Otter_Real_Time_Screeing_3__c, Otter_Real_Time_Screeing_4__c, PayRate__c, Passport_Number__c, Total_Interview_Amount__c, Personal_Email__c, Pin_In_India__c, PreMarketingStatus__c, Present_Client_Location__c, Recruiter__c, Ref1__c, Ref2__c, Resume_Preparation__c, Resume_Verified_By_Lead__c, Resume_Verified_By_Manager__c, Review_comments__c, Shared_Drive_URL_To_Student__c, State_In_India__c, Student_First_Name__c, Student_GH_Arrival_Date__c, Student_GH_Departure_Date__c, Student_Last_Name__c, Student_Marketing_Status__c, Student_Personal_Mobile__c, Technology__c, Town_City_In_India__c, USCIS_Number__c, US_Emergency_Number__c, University__c, Visa_End_Date__c, Visa_Start_Date__c, Interviews_Count__c, Submission_Count__c, Total_Count__c, Paid_Offer_Start_Date__c, MarketingCompanyName__c, Month_Of_VC__c, Student_Full_Name__c, Manager__c, Cluster_Account__c, Submission_Count_URL__c, Interviews_Count_URL__c, Lead_Contact__c, Manager_Contact__c, Phone__c, Recruiter_Name__c, Offshore_Manager_Name__c, Students_Sitting_in_Office_GH_9AM_5PM__c, Basic_Knowledge_on_Subject__c, Communication_Skills__c, Dummy_Vendor_Call_to_Candidate__c, Recruiter_Screening__c, Prepared_Steps_need_to_Follow_Doc__c, Company_Mismatch__c, Recruiter_Contact__c, OffShore_Manager_Contact__c, OffShore_Floor_Manager_Contact__c, Bill_Rate__c, Verbal_Conf_Count__c, Client_Name__c, Job_End_Date__c, Prime_Vendor_Name__c, Job_Start_Date__c, Vendor_Compnay_Name__c, Job_Location__c, Vendor_Person_Name__c, Vendor_Person_Email__c, Implementation_Partner__c, Interview_Date__c, Vendor_Person_Phone__c, Cluster_Contact__c, Onsite_Lead_Name__c, Onsite_Manager_Name__c, Conf_Month__c, Marketing_Company_Name__c, Report_Students_Count__c, Cluster_Name__c, Verbal_Confirmation_Date__c, Job_Title__c, Days_in_Market_Business__c, Submissions__c, Interviews__c, Incentive_Amount__c, Recent_Interview_Date__c, Interview_Remarks__c, Recent_Past_Interview_Date__c, Students_Count_In_Market__c, Last_week_Submissions__c, Last_week_Interviews__c, RecentUpcoming_Interview_Datetime__c, RecentUpcoming_Interview_Date__c, Ceipal_Status__c, Last_Week_Interview_Count__c, Cluster__c, Verbal_or_Project_Started_Count__c, Lead_Bill_Rate__c, Project_Start_Month__c, Aug_2025_Submissions__c, Management__c, Old_Student__c, Old_Manager_Name__c, Old_Student_Name__c, Old_Student_Phone__c, Student_ID__c, Project_Type__c, Conformation_Submission_ID__c, Conformation_Interview_ID__c, Father_Mobile_Number__c, India_Emergency_Contact_number__c, Parent_Mobile__c, Trigger_Check__c, Aug_2025_Interviews__c, June_2025_Interviews__c, June_2025_Submissions__c, July_2025_Interviews__c, July_2025_Submissions__c, Last_Submission_Date__c, Submissions_Count_From_Specific_Date__c, Vendor_Manager_Name__c, Onboarding_Cmpny__c, Last_End_To_End_Screening_Date__c, Availability__c, MS_Experience_Details_Screening__c, MS_Visa_Details_Screening__c, Resume_Verification__c, General_Marketing_Questions_Screening__c, Documents_Verification__c, Technical_Marketing_Questions_Screening__c, Otter_Screening__c, Daily_Tracker_Sheet_Review__c, Student_LinkedIn_Account_Review__c, Prime_Vendor_Database__c, Answering_To_Vendor__c, Reverse_Questions_To_Vendor__c, Recruiter_Department__c, Documents_Review__c, Resume_Review__c, Marketing_Sheet_Review__c, Month_Of_Paid_Offer__c, Vendor_Manager_Email__c, Recruiter_Start_Date__c, Supervisor_Name__c, Vendor_Manager_Phone__c, Domain_Name__c, Lead_Incentive_Applicable__c, Employee_Approval_Status__c, This_week_Interviews__c, This_week_Submissions__c, Lead_Incentive_Employee__c, Sum_of_Amount_USD_Student__c, Reason_for_Exit__c, Active_Job_Count__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Student__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "OwnerId": r.get("OwnerId"),
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "Batch__c": r.get("Batch__c"),
            "Comments__c": r.get("Comments__c"),
            "DL_Expiry_Date__c": _parse_sf_date(r.get("DL_Expiry_Date__c")),
            "DL_Issue_Date__c": _parse_sf_date(r.get("DL_Issue_Date__c")),
            "DL_Photo_Uploaded__c": r.get("DL_Photo_Uploaded__c"),
            "DOB__c": _parse_sf_date(r.get("DOB__c")),
            "District_In_India__c": r.get("District_In_India__c"),
            "EAD_Card_Number__c": r.get("EAD_Card_Number__c"),
            "Father_First_Name__c": r.get("Father_First_Name__c"),
            "Father_Last_Name__c": r.get("Father_Last_Name__c"),
            "In_Market_Student_Count__c": r.get("In_Market_Student_Count__c"),
            "Final_Marketing_Status__c": r.get("Final_Marketing_Status__c"),
            "Folder_Sharing__c": r.get("Folder_Sharing__c"),
            "Folder_in_Drive__c": r.get("Folder_in_Drive__c"),
            "GC_Back_Ref_Number__c": r.get("GC_Back_Ref_Number__c"),
            "GC_Back_Verified__c": r.get("GC_Back_Verified__c"),
            "GC_Catogery__c": r.get("GC_Catogery__c"),
            "GC_Expiry_Date__c": _parse_sf_date(r.get("GC_Expiry_Date__c")),
            "GC_Issued_Date__c": _parse_sf_date(r.get("GC_Issued_Date__c")),
            "GHID__c": r.get("GHID__c"),
            "Gender__c": r.get("Gender__c"),
            "Google_Voice_Number__c": r.get("Google_Voice_Number__c"),
            "Guest_House__c": r.get("Guest_House__c"),
            "Has_Linkedin_Created__c": r.get("Has_Linkedin_Created__c"),
            "IS_DL_ID_CHANGED__c": r.get("IS_DL_ID_CHANGED__c"),
            "India_Address_Line1__c": r.get("India_Address_Line1__c"),
            "India_Address_Line2__c": r.get("India_Address_Line2__c"),
            "Total_Count_not_Exit__c": r.get("Total_Count_not_Exit__c"),
            "Is_All_Docs_Reviewed_by_Manager__c": r.get("Is_All_Docs_Reviewed_by_Manager__c"),
            "Is_DL_Ready__c": r.get("Is_DL_Ready__c"),
            "Is_DOC_Ready__c": r.get("Is_DOC_Ready__c"),
            "Is_GC_Front_Verified__c": r.get("Is_GC_Front_Verified__c"),
            "Is_Marketing_Sheet_Updated_by_Manager_Le__c": r.get("Is_Marketing_Sheet_Updated_by_Manager_Le__c"),
            "Is_Offer_Letter_Issued__c": r.get("Is_Offer_Letter_Issued__c"),
            "LeadID__c": r.get("LeadID__c"),
            "Linkedin_Connection_Count__c": r.get("Linkedin_Connection_Count__c"),
            "Linkedin_URL__c": r.get("Linkedin_URL__c"),
            "MQ_Screening_By_Lead__c": r.get("MQ_Screening_By_Lead__c"),
            "MQ_Screening_By_Manager__c": r.get("MQ_Screening_By_Manager__c"),
            "MS_Sheet_Explanation__c": r.get("MS_Sheet_Explanation__c"),
            "MS_Uploaded_By_Student__c": r.get("MS_Uploaded_By_Student__c"),
            "Marketing_Company__c": r.get("Marketing_Company__c"),
            "Marketing_DOB__c": _parse_sf_date(r.get("Marketing_DOB__c")),
            "Marketing_Email__c": r.get("Marketing_Email__c"),
            "Marketing_End_Date__c": _parse_sf_date(r.get("Marketing_End_Date__c")),
            "Marketing_Sheet_Screening_by_Lead__c": r.get("Marketing_Sheet_Screening_by_Lead__c"),
            "Marketing_Sheet_Screening_by_Manager__c": r.get("Marketing_Sheet_Screening_by_Manager__c"),
            "Marketing_Start_Date__c": _parse_sf_date(r.get("Marketing_Start_Date__c")),
            "Marketing_Visa_Status__c": r.get("Marketing_Visa_Status__c"),
            "Mother_First_Name__c": r.get("Mother_First_Name__c"),
            "Mother_Last_Name__c": r.get("Mother_Last_Name__c"),
            "Offer_Issued_By_Name__c": r.get("Offer_Issued_By_Name__c"),
            "Offer_Issued_By_Phone__c": r.get("Offer_Issued_By_Phone__c"),
            "Offer_Type__c": r.get("Offer_Type__c"),
            "Onboarding_Company__c": r.get("Onboarding_Company__c"),
            "Onboarding_End_Date__c": _parse_sf_date(r.get("Onboarding_End_Date__c")),
            "Onboarding_Start_Date__c": _parse_sf_date(r.get("Onboarding_Start_Date__c")),
            "Onboarding_Visa_Status__c": r.get("Onboarding_Visa_Status__c"),
            "Otter_Final_Screening__c": r.get("Otter_Final_Screening__c"),
            "Otter_Real_Time_Screeing_1__c": r.get("Otter_Real_Time_Screeing_1__c"),
            "Otter_Real_Time_Screeing_2__c": r.get("Otter_Real_Time_Screeing_2__c"),
            "Otter_Real_Time_Screeing_3__c": r.get("Otter_Real_Time_Screeing_3__c"),
            "Otter_Real_Time_Screeing_4__c": r.get("Otter_Real_Time_Screeing_4__c"),
            "PayRate__c": r.get("PayRate__c"),
            "Passport_Number__c": r.get("Passport_Number__c"),
            "Total_Interview_Amount__c": r.get("Total_Interview_Amount__c"),
            "Personal_Email__c": r.get("Personal_Email__c"),
            "Pin_In_India__c": r.get("Pin_In_India__c"),
            "PreMarketingStatus__c": r.get("PreMarketingStatus__c"),
            "Present_Client_Location__c": r.get("Present_Client_Location__c"),
            "Recruiter__c": r.get("Recruiter__c"),
            "Ref1__c": r.get("Ref1__c"),
            "Ref2__c": r.get("Ref2__c"),
            "Resume_Preparation__c": r.get("Resume_Preparation__c"),
            "Resume_Verified_By_Lead__c": r.get("Resume_Verified_By_Lead__c"),
            "Resume_Verified_By_Manager__c": r.get("Resume_Verified_By_Manager__c"),
            "Review_comments__c": r.get("Review_comments__c"),
            "Shared_Drive_URL_To_Student__c": r.get("Shared_Drive_URL_To_Student__c"),
            "State_In_India__c": r.get("State_In_India__c"),
            "Student_First_Name__c": r.get("Student_First_Name__c"),
            "Student_GH_Arrival_Date__c": _parse_sf_date(r.get("Student_GH_Arrival_Date__c")),
            "Student_GH_Departure_Date__c": _parse_sf_date(r.get("Student_GH_Departure_Date__c")),
            "Student_Last_Name__c": r.get("Student_Last_Name__c"),
            "Student_Marketing_Status__c": r.get("Student_Marketing_Status__c"),
            "Student_Personal_Mobile__c": r.get("Student_Personal_Mobile__c"),
            "Technology__c": r.get("Technology__c"),
            "Town_City_In_India__c": r.get("Town_City_In_India__c"),
            "USCIS_Number__c": r.get("USCIS_Number__c"),
            "US_Emergency_Number__c": r.get("US_Emergency_Number__c"),
            "University__c": r.get("University__c"),
            "Visa_End_Date__c": _parse_sf_date(r.get("Visa_End_Date__c")),
            "Visa_Start_Date__c": _parse_sf_date(r.get("Visa_Start_Date__c")),
            "Interviews_Count__c": r.get("Interviews_Count__c"),
            "Submission_Count__c": r.get("Submission_Count__c"),
            "Total_Count__c": r.get("Total_Count__c"),
            "Paid_Offer_Start_Date__c": _parse_sf_date(r.get("Paid_Offer_Start_Date__c")),
            "MarketingCompanyName__c": r.get("MarketingCompanyName__c"),
            "Month_Of_VC__c": r.get("Month_Of_VC__c"),
            "Student_Full_Name__c": r.get("Student_Full_Name__c"),
            "Manager__c": r.get("Manager__c"),
            "Cluster_Account__c": r.get("Cluster_Account__c"),
            "Submission_Count_URL__c": r.get("Submission_Count_URL__c"),
            "Interviews_Count_URL__c": r.get("Interviews_Count_URL__c"),
            "Lead_Contact__c": r.get("Lead_Contact__c"),
            "Manager_Contact__c": r.get("Manager_Contact__c"),
            "Phone__c": r.get("Phone__c"),
            "Recruiter_Name__c": r.get("Recruiter_Name__c"),
            "Offshore_Manager_Name__c": r.get("Offshore_Manager_Name__c"),
            "Students_Sitting_in_Office_GH_9AM_5PM__c": r.get("Students_Sitting_in_Office_GH_9AM_5PM__c"),
            "Basic_Knowledge_on_Subject__c": r.get("Basic_Knowledge_on_Subject__c"),
            "Communication_Skills__c": r.get("Communication_Skills__c"),
            "Dummy_Vendor_Call_to_Candidate__c": r.get("Dummy_Vendor_Call_to_Candidate__c"),
            "Recruiter_Screening__c": r.get("Recruiter_Screening__c"),
            "Prepared_Steps_need_to_Follow_Doc__c": r.get("Prepared_Steps_need_to_Follow_Doc__c"),
            "Company_Mismatch__c": r.get("Company_Mismatch__c", False),
            "Recruiter_Contact__c": r.get("Recruiter_Contact__c"),
            "OffShore_Manager_Contact__c": r.get("OffShore_Manager_Contact__c"),
            "OffShore_Floor_Manager_Contact__c": r.get("OffShore_Floor_Manager_Contact__c"),
            "Bill_Rate__c": r.get("Bill_Rate__c"),
            "Verbal_Conf_Count__c": r.get("Verbal_Conf_Count__c"),
            "Client_Name__c": r.get("Client_Name__c"),
            "Job_End_Date__c": _parse_sf_date(r.get("Job_End_Date__c")),
            "Prime_Vendor_Name__c": r.get("Prime_Vendor_Name__c"),
            "Job_Start_Date__c": _parse_sf_date(r.get("Job_Start_Date__c")),
            "Vendor_Compnay_Name__c": r.get("Vendor_Compnay_Name__c"),
            "Job_Location__c": r.get("Job_Location__c"),
            "Vendor_Person_Name__c": r.get("Vendor_Person_Name__c"),
            "Vendor_Person_Email__c": r.get("Vendor_Person_Email__c"),
            "Implementation_Partner__c": r.get("Implementation_Partner__c"),
            "Interview_Date__c": _parse_sf_datetime(r.get("Interview_Date__c")),
            "Vendor_Person_Phone__c": r.get("Vendor_Person_Phone__c"),
            "Cluster_Contact__c": r.get("Cluster_Contact__c"),
            "Onsite_Lead_Name__c": r.get("Onsite_Lead_Name__c"),
            "Onsite_Manager_Name__c": r.get("Onsite_Manager_Name__c"),
            "Conf_Month__c": r.get("Conf_Month__c"),
            "Marketing_Company_Name__c": r.get("Marketing_Company_Name__c"),
            "Report_Students_Count__c": r.get("Report_Students_Count__c"),
            "Cluster_Name__c": r.get("Cluster_Name__c"),
            "Verbal_Confirmation_Date__c": _parse_sf_date(r.get("Verbal_Confirmation_Date__c")),
            "Job_Title__c": r.get("Job_Title__c"),
            "Days_in_Market_Business__c": r.get("Days_in_Market_Business__c"),
            "Submissions__c": r.get("Submissions__c"),
            "Interviews__c": r.get("Interviews__c"),
            "Incentive_Amount__c": r.get("Incentive_Amount__c"),
            "Recent_Interview_Date__c": _parse_sf_date(r.get("Recent_Interview_Date__c")),
            "Interview_Remarks__c": r.get("Interview_Remarks__c"),
            "Recent_Past_Interview_Date__c": _parse_sf_date(r.get("Recent_Past_Interview_Date__c")),
            "Students_Count_In_Market__c": r.get("Students_Count_In_Market__c"),
            "Last_week_Submissions__c": r.get("Last_week_Submissions__c"),
            "Last_week_Interviews__c": r.get("Last_week_Interviews__c"),
            "RecentUpcoming_Interview_Datetime__c": _parse_sf_datetime(r.get("RecentUpcoming_Interview_Datetime__c")),
            "RecentUpcoming_Interview_Date__c": _parse_sf_date(r.get("RecentUpcoming_Interview_Date__c")),
            "Ceipal_Status__c": r.get("Ceipal_Status__c"),
            "Last_Week_Interview_Count__c": r.get("Last_Week_Interview_Count__c"),
            "Cluster__c": r.get("Cluster__c"),
            "Verbal_or_Project_Started_Count__c": r.get("Verbal_or_Project_Started_Count__c"),
            "Lead_Bill_Rate__c": r.get("Lead_Bill_Rate__c"),
            "Project_Start_Month__c": r.get("Project_Start_Month__c"),
            "Aug_2025_Submissions__c": r.get("Aug_2025_Submissions__c"),
            "Management__c": r.get("Management__c"),
            "Old_Student__c": r.get("Old_Student__c"),
            "Old_Manager_Name__c": r.get("Old_Manager_Name__c"),
            "Old_Student_Name__c": r.get("Old_Student_Name__c"),
            "Old_Student_Phone__c": r.get("Old_Student_Phone__c"),
            "Student_ID__c": r.get("Student_ID__c"),
            "Project_Type__c": r.get("Project_Type__c"),
            "Conformation_Submission_ID__c": r.get("Conformation_Submission_ID__c"),
            "Conformation_Interview_ID__c": r.get("Conformation_Interview_ID__c"),
            "Father_Mobile_Number__c": r.get("Father_Mobile_Number__c"),
            "India_Emergency_Contact_number__c": r.get("India_Emergency_Contact_number__c"),
            "Parent_Mobile__c": r.get("Parent_Mobile__c"),
            "Trigger_Check__c": r.get("Trigger_Check__c"),
            "Aug_2025_Interviews__c": r.get("Aug_2025_Interviews__c"),
            "June_2025_Interviews__c": r.get("June_2025_Interviews__c"),
            "June_2025_Submissions__c": r.get("June_2025_Submissions__c"),
            "July_2025_Interviews__c": r.get("July_2025_Interviews__c"),
            "July_2025_Submissions__c": r.get("July_2025_Submissions__c"),
            "Last_Submission_Date__c": _parse_sf_date(r.get("Last_Submission_Date__c")),
            "Submissions_Count_From_Specific_Date__c": r.get("Submissions_Count_From_Specific_Date__c"),
            "Vendor_Manager_Name__c": r.get("Vendor_Manager_Name__c"),
            "Onboarding_Cmpny__c": r.get("Onboarding_Cmpny__c"),
            "Last_End_To_End_Screening_Date__c": _parse_sf_date(r.get("Last_End_To_End_Screening_Date__c")),
            "Availability__c": r.get("Availability__c"),
            "MS_Experience_Details_Screening__c": r.get("MS_Experience_Details_Screening__c"),
            "MS_Visa_Details_Screening__c": r.get("MS_Visa_Details_Screening__c"),
            "Resume_Verification__c": r.get("Resume_Verification__c"),
            "General_Marketing_Questions_Screening__c": r.get("General_Marketing_Questions_Screening__c"),
            "Documents_Verification__c": r.get("Documents_Verification__c"),
            "Technical_Marketing_Questions_Screening__c": r.get("Technical_Marketing_Questions_Screening__c"),
            "Otter_Screening__c": r.get("Otter_Screening__c"),
            "Daily_Tracker_Sheet_Review__c": r.get("Daily_Tracker_Sheet_Review__c"),
            "Student_LinkedIn_Account_Review__c": r.get("Student_LinkedIn_Account_Review__c"),
            "Prime_Vendor_Database__c": r.get("Prime_Vendor_Database__c"),
            "Answering_To_Vendor__c": r.get("Answering_To_Vendor__c"),
            "Reverse_Questions_To_Vendor__c": r.get("Reverse_Questions_To_Vendor__c"),
            "Recruiter_Department__c": r.get("Recruiter_Department__c"),
            "Documents_Review__c": r.get("Documents_Review__c"),
            "Resume_Review__c": r.get("Resume_Review__c"),
            "Marketing_Sheet_Review__c": r.get("Marketing_Sheet_Review__c"),
            "Month_Of_Paid_Offer__c": r.get("Month_Of_Paid_Offer__c"),
            "Vendor_Manager_Email__c": r.get("Vendor_Manager_Email__c"),
            "Recruiter_Start_Date__c": _parse_sf_date(r.get("Recruiter_Start_Date__c")),
            "Supervisor_Name__c": r.get("Supervisor_Name__c"),
            "Vendor_Manager_Phone__c": r.get("Vendor_Manager_Phone__c"),
            "Domain_Name__c": r.get("Domain_Name__c"),
            "Lead_Incentive_Applicable__c": r.get("Lead_Incentive_Applicable__c", False),
            "Employee_Approval_Status__c": r.get("Employee_Approval_Status__c"),
            "This_week_Interviews__c": r.get("This_week_Interviews__c"),
            "This_week_Submissions__c": r.get("This_week_Submissions__c"),
            "Lead_Incentive_Employee__c": r.get("Lead_Incentive_Employee__c"),
            "Sum_of_Amount_USD_Student__c": r.get("Sum_of_Amount_USD_Student__c"),
            "Reason_for_Exit__c": r.get("Reason_for_Exit__c"),
            "Active_Job_Count__c": r.get("Active_Job_Count__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, Student, records_data)


async def _sync_submissions(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Submissions__c ({mode})...")

    soql_fields = "Id, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastViewedDate, LastReferencedDate, Student__c, Client_Name__c, Implement_Company_Name__c, Prime_Vendor_Name__c, Rate__c, Recuter__c, Submission_Date__c, Submission_Status__c, Vendor_Compnay_Name__c, Vendor_Person_Email__c, Vendor_Person_Name__c, Vendor_Person_Phone__c, Vendor_Company__c, Vendor_Contact__c, Ext__c, Last_3_Business_Days_Check__c, Job_Description__c, Onsite_Manager_Name__c, Offshore_Manager_Name__c, Recruiter_Name__c, Onsite_Lead_Name__c, Student_Name__c, Technology__c, Domain_Formula__c, Is_Today_Submission__c, BU_Name__c, Last_3_Business_Days__c, Vendor_Client_Student__c, Sub_Count__c, Is_Yesterday_s_Submission__c, Is_Last_Week_Sub__c, Comments__c, Last_Week_Business_Day_Check__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Submissions__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "Student__c": r.get("Student__c"),
            "Client_Name__c": r.get("Client_Name__c"),
            "Implement_Company_Name__c": r.get("Implement_Company_Name__c"),
            "Prime_Vendor_Name__c": r.get("Prime_Vendor_Name__c"),
            "Rate__c": r.get("Rate__c"),
            "Recuter__c": r.get("Recuter__c"),
            "Submission_Date__c": _parse_sf_date(r.get("Submission_Date__c")),
            "Submission_Status__c": r.get("Submission_Status__c"),
            "Vendor_Compnay_Name__c": r.get("Vendor_Compnay_Name__c"),
            "Vendor_Person_Email__c": r.get("Vendor_Person_Email__c"),
            "Vendor_Person_Name__c": r.get("Vendor_Person_Name__c"),
            "Vendor_Person_Phone__c": r.get("Vendor_Person_Phone__c"),
            "Vendor_Company__c": r.get("Vendor_Company__c"),
            "Vendor_Contact__c": r.get("Vendor_Contact__c"),
            "Ext__c": r.get("Ext__c"),
            "Last_3_Business_Days_Check__c": r.get("Last_3_Business_Days_Check__c"),
            "Job_Description__c": r.get("Job_Description__c"),
            "Onsite_Manager_Name__c": r.get("Onsite_Manager_Name__c"),
            "Offshore_Manager_Name__c": r.get("Offshore_Manager_Name__c"),
            "Recruiter_Name__c": r.get("Recruiter_Name__c"),
            "Onsite_Lead_Name__c": r.get("Onsite_Lead_Name__c"),
            "Student_Name__c": r.get("Student_Name__c"),
            "Technology__c": r.get("Technology__c"),
            "Domain_Formula__c": r.get("Domain_Formula__c"),
            "Is_Today_Submission__c": r.get("Is_Today_Submission__c"),
            "BU_Name__c": r.get("BU_Name__c"),
            "Last_3_Business_Days__c": _parse_sf_date(r.get("Last_3_Business_Days__c")),
            "Vendor_Client_Student__c": r.get("Vendor_Client_Student__c"),
            "Sub_Count__c": r.get("Sub_Count__c"),
            "Is_Yesterday_s_Submission__c": r.get("Is_Yesterday_s_Submission__c", False),
            "Is_Last_Week_Sub__c": r.get("Is_Last_Week_Sub__c", False),
            "Comments__c": r.get("Comments__c"),
            "Last_Week_Business_Day_Check__c": r.get("Last_Week_Business_Day_Check__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, Submission, records_data)


async def _sync_interviews(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Interviews__c ({mode})...")

    soql_fields = "Id, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastViewedDate, LastReferencedDate, Student__c, Amount__c, Duration__c, Interview_Date__c, Interview_Q_A__c, Month__c, Interviewer_Email__c, Interviewer_Name__c, Otter_Link__c, Submissions__c, Support_First_Name__c, Support_Last_Name__c, Tech_Support__c, Type__c, TechSupportName__c, Interview_End_Date_Time__c, Final_Status__c, Calender_Prefix__c, Lead_Manager_Joined__c, Student_Otter_Performance__c, Student_Technical_Explanation_Skill__c, Proxy_General_Issues__c, Any_Technical_Issues__c, Recruiter_Name__c, Offshore_Manager__c, Offshore_Floor_Manager__c, Onsite_Lead__c, Onsite_Manager__c, Cluster__c, Vendor_Company_Name__c, Vendor_Person_Name__c, Vendor_Person_Phone__c, Vendor_Email__c, Prime_Vendor_Name__c, Implementation_Partner__c, Client_Name__c, Bill_Rate__c, Student_Technology__c, Lead_Name__c, Project_Start_Date__c, Amount_INR__c, Verbal_Conf_Count__c, Interview_Date1__c, Paid__c, Int_Count__c, Manager_Tech__c, Interviews_Count__c, Final_Feedback__c, Job_Status__c, Verbal_Intw_Date__c, Paid_By__c, Is_Last_Week__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Interviews__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "Student__c": r.get("Student__c"),
            "Amount__c": r.get("Amount__c"),
            "Duration__c": r.get("Duration__c"),
            "Interview_Date__c": _parse_sf_datetime(r.get("Interview_Date__c")),
            "Interview_Q_A__c": r.get("Interview_Q_A__c"),
            "Month__c": r.get("Month__c"),
            "Interviewer_Email__c": r.get("Interviewer_Email__c"),
            "Interviewer_Name__c": r.get("Interviewer_Name__c"),
            "Otter_Link__c": r.get("Otter_Link__c"),
            "Submissions__c": r.get("Submissions__c"),
            "Support_First_Name__c": r.get("Support_First_Name__c"),
            "Support_Last_Name__c": r.get("Support_Last_Name__c"),
            "Tech_Support__c": r.get("Tech_Support__c"),
            "Type__c": r.get("Type__c"),
            "TechSupportName__c": r.get("TechSupportName__c"),
            "Interview_End_Date_Time__c": _parse_sf_datetime(r.get("Interview_End_Date_Time__c")),
            "Final_Status__c": r.get("Final_Status__c"),
            "Calender_Prefix__c": r.get("Calender_Prefix__c"),
            "Lead_Manager_Joined__c": r.get("Lead_Manager_Joined__c"),
            "Student_Otter_Performance__c": r.get("Student_Otter_Performance__c"),
            "Student_Technical_Explanation_Skill__c": r.get("Student_Technical_Explanation_Skill__c"),
            "Proxy_General_Issues__c": r.get("Proxy_General_Issues__c"),
            "Any_Technical_Issues__c": r.get("Any_Technical_Issues__c"),
            "Recruiter_Name__c": r.get("Recruiter_Name__c"),
            "Offshore_Manager__c": r.get("Offshore_Manager__c"),
            "Offshore_Floor_Manager__c": r.get("Offshore_Floor_Manager__c"),
            "Onsite_Lead__c": r.get("Onsite_Lead__c"),
            "Onsite_Manager__c": r.get("Onsite_Manager__c"),
            "Cluster__c": r.get("Cluster__c"),
            "Vendor_Company_Name__c": r.get("Vendor_Company_Name__c"),
            "Vendor_Person_Name__c": r.get("Vendor_Person_Name__c"),
            "Vendor_Person_Phone__c": r.get("Vendor_Person_Phone__c"),
            "Vendor_Email__c": r.get("Vendor_Email__c"),
            "Prime_Vendor_Name__c": r.get("Prime_Vendor_Name__c"),
            "Implementation_Partner__c": r.get("Implementation_Partner__c"),
            "Client_Name__c": r.get("Client_Name__c"),
            "Bill_Rate__c": r.get("Bill_Rate__c"),
            "Student_Technology__c": r.get("Student_Technology__c"),
            "Lead_Name__c": r.get("Lead_Name__c"),
            "Project_Start_Date__c": _parse_sf_date(r.get("Project_Start_Date__c")),
            "Amount_INR__c": r.get("Amount_INR__c"),
            "Verbal_Conf_Count__c": r.get("Verbal_Conf_Count__c"),
            "Interview_Date1__c": _parse_sf_date(r.get("Interview_Date1__c")),
            "Paid__c": r.get("Paid__c", False),
            "Int_Count__c": r.get("Int_Count__c"),
            "Manager_Tech__c": r.get("Manager_Tech__c"),
            "Interviews_Count__c": r.get("Interviews_Count__c"),
            "Final_Feedback__c": r.get("Final_Feedback__c"),
            "Job_Status__c": r.get("Job_Status__c"),
            "Verbal_Intw_Date__c": _parse_sf_date(r.get("Verbal_Intw_Date__c")),
            "Paid_By__c": r.get("Paid_By__c"),
            "Is_Last_Week__c": r.get("Is_Last_Week__c", False),
            "synced_at": now,
        })

    return await _upsert_batch(session, Interview, records_data)


async def _sync_manager(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Manager__c ({mode})...")

    soql_fields = "Id, OwnerId, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastViewedDate, LastReferencedDate, Cluster__c, Email__c, Offshore_Floor_Manager__c, Offshore_Location__c, Offshore_Manager__c, Operation_Location__c, Organization__c, User__c, Cluster_Account__c, Total_Expenses__c, Exit_Student_Count__c, Students_Count_In_Market__c, Project_Started_Count__c, Pre_Marketing_Student_Count__c, Total_Expenses_MIS__c, Each_Placement_Cost__c, Approval_Status__c, Visa__c, Type__c, Offshore_Google_Pin__c, Offshore_POC_Number__c, Website__c, Offshore_Location_Type__c, US_Office_Address__c, US_Office_Rent__c, Offshore_Office_Rent__c, Active__c, BU_Student_With_Job_Count__c, HR_Contact__c, USA_HR_Phone__c, USA_HR_Email__c, Supervisor_Phone__c, Supervisor_Email__c, Docs_Upload_Drive_Link__c, Project_Status__c, Supervisor_Name__c, Organization_Name__c, HR_Name__c, Offshore_Floor_Manager_Contact__c, Manager_ID__c, US_Operation_CITY__c, US_Operation_State__c, Alias_Name__c, Old_Phone__c, New_Phone__c, Students_Count__c, In_Market_Students_Count__c, Verbal_Count__c, IN_JOB_Students_Count__c, India_HR__c, India_HR_Mail__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Manager__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "OwnerId": r.get("OwnerId"),
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "Cluster__c": r.get("Cluster__c"),
            "Email__c": r.get("Email__c"),
            "Offshore_Floor_Manager__c": r.get("Offshore_Floor_Manager__c"),
            "Offshore_Location__c": r.get("Offshore_Location__c"),
            "Offshore_Manager__c": r.get("Offshore_Manager__c"),
            "Operation_Location__c": r.get("Operation_Location__c"),
            "Organization__c": r.get("Organization__c"),
            "User__c": r.get("User__c"),
            "Cluster_Account__c": r.get("Cluster_Account__c"),
            "Total_Expenses__c": r.get("Total_Expenses__c"),
            "Exit_Student_Count__c": r.get("Exit_Student_Count__c"),
            "Students_Count_In_Market__c": r.get("Students_Count_In_Market__c"),
            "Project_Started_Count__c": r.get("Project_Started_Count__c"),
            "Pre_Marketing_Student_Count__c": r.get("Pre_Marketing_Student_Count__c"),
            "Total_Expenses_MIS__c": r.get("Total_Expenses_MIS__c"),
            "Each_Placement_Cost__c": r.get("Each_Placement_Cost__c"),
            "Approval_Status__c": r.get("Approval_Status__c"),
            "Visa__c": r.get("Visa__c"),
            "Type__c": r.get("Type__c"),
            "Offshore_Google_Pin__c": r.get("Offshore_Google_Pin__c"),
            "Offshore_POC_Number__c": r.get("Offshore_POC_Number__c"),
            "Website__c": r.get("Website__c"),
            "Offshore_Location_Type__c": r.get("Offshore_Location_Type__c"),
            "US_Office_Address__c": r.get("US_Office_Address__c"),
            "US_Office_Rent__c": r.get("US_Office_Rent__c"),
            "Offshore_Office_Rent__c": r.get("Offshore_Office_Rent__c"),
            "Active__c": r.get("Active__c", False),
            "BU_Student_With_Job_Count__c": r.get("BU_Student_With_Job_Count__c"),
            "HR_Contact__c": r.get("HR_Contact__c"),
            "USA_HR_Phone__c": r.get("USA_HR_Phone__c"),
            "USA_HR_Email__c": r.get("USA_HR_Email__c"),
            "Supervisor_Phone__c": r.get("Supervisor_Phone__c"),
            "Supervisor_Email__c": r.get("Supervisor_Email__c"),
            "Docs_Upload_Drive_Link__c": r.get("Docs_Upload_Drive_Link__c"),
            "Project_Status__c": r.get("Project_Status__c"),
            "Supervisor_Name__c": r.get("Supervisor_Name__c"),
            "Organization_Name__c": r.get("Organization_Name__c"),
            "HR_Name__c": r.get("HR_Name__c"),
            "Offshore_Floor_Manager_Contact__c": r.get("Offshore_Floor_Manager_Contact__c"),
            "Manager_ID__c": r.get("Manager_ID__c"),
            "US_Operation_CITY__c": r.get("US_Operation_CITY__c"),
            "US_Operation_State__c": r.get("US_Operation_State__c"),
            "Alias_Name__c": r.get("Alias_Name__c"),
            "Old_Phone__c": r.get("Old_Phone__c"),
            "New_Phone__c": r.get("New_Phone__c"),
            "Students_Count__c": r.get("Students_Count__c"),
            "In_Market_Students_Count__c": r.get("In_Market_Students_Count__c"),
            "Verbal_Count__c": r.get("Verbal_Count__c"),
            "IN_JOB_Students_Count__c": r.get("IN_JOB_Students_Count__c"),
            "India_HR__c": r.get("India_HR__c"),
            "India_HR_Mail__c": r.get("India_HR_Mail__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, Manager, records_data)


async def _sync_job(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Job__c ({mode})...")

    soql_fields = "Id, OwnerId, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastViewedDate, LastReferencedDate, Active__c, Student_Name_Manager_Name__c, Month_SD__c, Month_ED__c, PayRate__c, Caluculated_Pay_Rate__c, Pay_Roll_Tax__c, Profit__c, Project_Type__c, Visa_Status__c, Company_Manager__c, Total_Interview_Amount__c, Month_Of_VC__c, Job_Title__c, Ceipal_Status__c, Triggered_Ceipal__c, Ceipal_Pay_Rate__c, Supervisor_Name__c, Hr_Contact__c, Batch__c, Bill_Rate__c, Technology__c, Client_Name__c, Company__c, Implementation_Partner__c, Job_Location__c, PO_End_Dt__c, Verbal_Confirmation_Date__c, Payroll_Month__c, Supervisor_Name_Share__c, Percentage__c, Prime_Vendor_Name__c, Project_End_Date__c, Project_Start_Date__c, Recruiter__c, Share_With__c, Student__c, Vendor_Compnay_Name__c, Vendor_Person_Email__c, Vendor_Person_Name__c, Vendor_Person_Phone__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Job__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "OwnerId": r.get("OwnerId"),
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "Active__c": r.get("Active__c", False),
            "Student_Name_Manager_Name__c": r.get("Student_Name_Manager_Name__c"),
            "Month_SD__c": r.get("Month_SD__c"),
            "Month_ED__c": r.get("Month_ED__c"),
            "PayRate__c": r.get("PayRate__c"),
            "Caluculated_Pay_Rate__c": r.get("Caluculated_Pay_Rate__c"),
            "Pay_Roll_Tax__c": r.get("Pay_Roll_Tax__c"),
            "Profit__c": r.get("Profit__c"),
            "Project_Type__c": r.get("Project_Type__c"),
            "Visa_Status__c": r.get("Visa_Status__c"),
            "Company_Manager__c": r.get("Company_Manager__c"),
            "Total_Interview_Amount__c": r.get("Total_Interview_Amount__c"),
            "Month_Of_VC__c": r.get("Month_Of_VC__c"),
            "Job_Title__c": r.get("Job_Title__c"),
            "Ceipal_Status__c": r.get("Ceipal_Status__c"),
            "Triggered_Ceipal__c": r.get("Triggered_Ceipal__c", False),
            "Ceipal_Pay_Rate__c": r.get("Ceipal_Pay_Rate__c"),
            "Supervisor_Name__c": r.get("Supervisor_Name__c"),
            "Hr_Contact__c": r.get("Hr_Contact__c"),
            "Batch__c": r.get("Batch__c"),
            "Bill_Rate__c": r.get("Bill_Rate__c"),
            "Technology__c": r.get("Technology__c"),
            "Client_Name__c": r.get("Client_Name__c"),
            "Company__c": r.get("Company__c"),
            "Implementation_Partner__c": r.get("Implementation_Partner__c"),
            "Job_Location__c": r.get("Job_Location__c"),
            "PO_End_Dt__c": _parse_sf_date(r.get("PO_End_Dt__c")),
            "Verbal_Confirmation_Date__c": _parse_sf_date(r.get("Verbal_Confirmation_Date__c")),
            "Payroll_Month__c": r.get("Payroll_Month__c"),
            "Supervisor_Name_Share__c": r.get("Supervisor_Name_Share__c"),
            "Percentage__c": r.get("Percentage__c"),
            "Prime_Vendor_Name__c": r.get("Prime_Vendor_Name__c"),
            "Project_End_Date__c": _parse_sf_date(r.get("Project_End_Date__c")),
            "Project_Start_Date__c": _parse_sf_date(r.get("Project_Start_Date__c")),
            "Recruiter__c": r.get("Recruiter__c"),
            "Share_With__c": r.get("Share_With__c"),
            "Student__c": r.get("Student__c"),
            "Vendor_Compnay_Name__c": r.get("Vendor_Compnay_Name__c"),
            "Vendor_Person_Email__c": r.get("Vendor_Person_Email__c"),
            "Vendor_Person_Name__c": r.get("Vendor_Person_Name__c"),
            "Vendor_Person_Phone__c": r.get("Vendor_Person_Phone__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, Job, records_data)


async def _sync_employee(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Employee__c ({mode})...")

    soql_fields = "Id, OwnerId, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastViewedDate, LastReferencedDate, Deptment__c, Email__c, End_Dt__c, First_Name__c, Last_Name__c, OffShoreMgrID__c, OffshoreFloorManager__c, OffshoreLeadId__c, Offshore_Location__c, Onshore_Manager__c, Organization__c, Phone__c, User__c, Start_Dt__c, workPhoneNum__c, Full_Name__c, Keka_EMP_ID__c, Contact__c, Cluster__c, Offshore_Lead_Contact__c, OffShore_Manager_Contact__c, Offshore_Floor_Manager_Contact__c, OnShore_Manager_Contact__c, Approval_Status__c, Org_Account__c, Cluster_Contact__c, Latest_Verbal_Confirmation_Date__c, Total_Number_Of_Students__c, Confirmation_Count__c, Last_Login_Date__c, Cluster_Account__c, Offshore_Manager_Lead_Name__c, Supervisor_Name__c, Organization_Name__c, Cluster_Name__c, Offshore_Floor_Manager__c, Employee_Duration__c, In_Market_Students_Count__c, Pre_Marketing_Student_Count__c, BU_Mail__c, India_HR_Mail__c, Offshore_Manager_Mail__c, BU_Name__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Employee__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "OwnerId": r.get("OwnerId"),
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "Deptment__c": r.get("Deptment__c"),
            "Email__c": r.get("Email__c"),
            "End_Dt__c": _parse_sf_date(r.get("End_Dt__c")),
            "First_Name__c": r.get("First_Name__c"),
            "Last_Name__c": r.get("Last_Name__c"),
            "OffShoreMgrID__c": r.get("OffShoreMgrID__c"),
            "OffshoreFloorManager__c": r.get("OffshoreFloorManager__c"),
            "OffshoreLeadId__c": r.get("OffshoreLeadId__c"),
            "Offshore_Location__c": r.get("Offshore_Location__c"),
            "Onshore_Manager__c": r.get("Onshore_Manager__c"),
            "Organization__c": r.get("Organization__c"),
            "Phone__c": r.get("Phone__c"),
            "User__c": r.get("User__c"),
            "Start_Dt__c": _parse_sf_date(r.get("Start_Dt__c")),
            "workPhoneNum__c": r.get("workPhoneNum__c"),
            "Full_Name__c": r.get("Full_Name__c"),
            "Keka_EMP_ID__c": r.get("Keka_EMP_ID__c"),
            "Contact__c": r.get("Contact__c"),
            "Cluster__c": r.get("Cluster__c"),
            "Offshore_Lead_Contact__c": r.get("Offshore_Lead_Contact__c"),
            "OffShore_Manager_Contact__c": r.get("OffShore_Manager_Contact__c"),
            "Offshore_Floor_Manager_Contact__c": r.get("Offshore_Floor_Manager_Contact__c"),
            "OnShore_Manager_Contact__c": r.get("OnShore_Manager_Contact__c"),
            "Approval_Status__c": r.get("Approval_Status__c"),
            "Org_Account__c": r.get("Org_Account__c"),
            "Cluster_Contact__c": r.get("Cluster_Contact__c"),
            "Latest_Verbal_Confirmation_Date__c": _parse_sf_date(r.get("Latest_Verbal_Confirmation_Date__c")),
            "Total_Number_Of_Students__c": r.get("Total_Number_Of_Students__c"),
            "Confirmation_Count__c": r.get("Confirmation_Count__c"),
            "Last_Login_Date__c": _parse_sf_date(r.get("Last_Login_Date__c")),
            "Cluster_Account__c": r.get("Cluster_Account__c"),
            "Offshore_Manager_Lead_Name__c": r.get("Offshore_Manager_Lead_Name__c"),
            "Supervisor_Name__c": r.get("Supervisor_Name__c"),
            "Organization_Name__c": r.get("Organization_Name__c"),
            "Cluster_Name__c": r.get("Cluster_Name__c"),
            "Offshore_Floor_Manager__c": r.get("Offshore_Floor_Manager__c"),
            "Employee_Duration__c": r.get("Employee_Duration__c"),
            "In_Market_Students_Count__c": r.get("In_Market_Students_Count__c"),
            "Pre_Marketing_Student_Count__c": r.get("Pre_Marketing_Student_Count__c"),
            "BU_Mail__c": r.get("BU_Mail__c"),
            "India_HR_Mail__c": r.get("India_HR_Mail__c"),
            "Offshore_Manager_Mail__c": r.get("Offshore_Manager_Mail__c"),
            "BU_Name__c": r.get("BU_Name__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, Employee, records_data)


async def _sync_bu_performance(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing BU_Performance__c ({mode})...")

    soql_fields = "Id, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, BU__c, In_Market_Students_Count__c, Date__c, Submissions_Count__c, Interview_Count__c, submission__c, Target_Submissions__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM BU_Performance__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "BU__c": r.get("BU__c"),
            "In_Market_Students_Count__c": r.get("In_Market_Students_Count__c"),
            "Date__c": _parse_sf_date(r.get("Date__c")),
            "Submissions_Count__c": r.get("Submissions_Count__c"),
            "Interview_Count__c": r.get("Interview_Count__c"),
            "submission__c": r.get("submission__c"),
            "Target_Submissions__c": r.get("Target_Submissions__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, BUPerformance, records_data)


async def _sync_bs(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing BS__c ({mode})...")

    soql_fields = "Id, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastViewedDate, LastReferencedDate, Student__c, BU_Name__c, Vendor_Name__c, Bill_Rate__c, PayRate__c, Caluculated_Pay_Rate__c, Month__c, Year__c, Invoice_Amount__c, Actual_Salary__c, Salary_Paid__c, Insurance__c, H1fee__c, Other_Amounts__c, Pending_Amount__c, Payroll_Taxes__c, Gross_Profit__c, DOJ__c, Company_Name__c, Payment_Type__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM BS__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "Student__c": r.get("Student__c"),
            "BU_Name__c": r.get("BU_Name__c"),
            "Vendor_Name__c": r.get("Vendor_Name__c"),
            "Bill_Rate__c": r.get("Bill_Rate__c"),
            "PayRate__c": r.get("PayRate__c"),
            "Caluculated_Pay_Rate__c": r.get("Caluculated_Pay_Rate__c"),
            "Month__c": r.get("Month__c"),
            "Year__c": r.get("Year__c"),
            "Invoice_Amount__c": r.get("Invoice_Amount__c"),
            "Actual_Salary__c": r.get("Actual_Salary__c"),
            "Salary_Paid__c": r.get("Salary_Paid__c"),
            "Insurance__c": r.get("Insurance__c"),
            "H1fee__c": r.get("H1fee__c"),
            "Other_Amounts__c": r.get("Other_Amounts__c"),
            "Pending_Amount__c": r.get("Pending_Amount__c"),
            "Payroll_Taxes__c": r.get("Payroll_Taxes__c"),
            "Gross_Profit__c": r.get("Gross_Profit__c"),
            "DOJ__c": _parse_sf_date(r.get("DOJ__c")),
            "Company_Name__c": r.get("Company_Name__c"),
            "Payment_Type__c": r.get("Payment_Type__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, BS, records_data)


async def _sync_tech_support(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Tech_Support__c ({mode})...")

    soql_fields = "Id, OwnerId, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastViewedDate, LastReferencedDate, Amnt_Per_Call__c, Availability__c, Calendar_URL__c, Calls_Per_Day__c, Confirmtion_amount__c, Contact_Number2__c, Location__c, Name__c, OnSiteMgrID__c, Payment_Type__c, Total_Amount__c, contact_Number1__c, Account_Details__c, Technology__c, Priority__c, Total_Interviews_Count__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Tech_Support__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "OwnerId": r.get("OwnerId"),
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "Amnt_Per_Call__c": r.get("Amnt_Per_Call__c"),
            "Availability__c": r.get("Availability__c"),
            "Calendar_URL__c": r.get("Calendar_URL__c"),
            "Calls_Per_Day__c": r.get("Calls_Per_Day__c"),
            "Confirmtion_amount__c": r.get("Confirmtion_amount__c"),
            "Contact_Number2__c": r.get("Contact_Number2__c"),
            "Location__c": r.get("Location__c"),
            "Name__c": r.get("Name__c"),
            "OnSiteMgrID__c": r.get("OnSiteMgrID__c"),
            "Payment_Type__c": r.get("Payment_Type__c"),
            "Total_Amount__c": r.get("Total_Amount__c"),
            "contact_Number1__c": r.get("contact_Number1__c"),
            "Account_Details__c": r.get("Account_Details__c"),
            "Technology__c": r.get("Technology__c"),
            "Priority__c": r.get("Priority__c"),
            "Total_Interviews_Count__c": r.get("Total_Interviews_Count__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, TechSupport, records_data)


async def _sync_new_student(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing New_Student__c ({mode})...")

    soql_fields = "Id, OwnerId, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastViewedDate, LastReferencedDate, Manager__c, Date_Of_Birth__c, Visa_Status__c, OPT_STEM_Start_Date__c, Interested_Tech__c, Phone__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM New_Student__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "OwnerId": r.get("OwnerId"),
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "Manager__c": r.get("Manager__c"),
            "Date_Of_Birth__c": _parse_sf_date(r.get("Date_Of_Birth__c")),
            "Visa_Status__c": r.get("Visa_Status__c"),
            "OPT_STEM_Start_Date__c": _parse_sf_date(r.get("OPT_STEM_Start_Date__c")),
            "Interested_Tech__c": r.get("Interested_Tech__c"),
            "Phone__c": r.get("Phone__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, NewStudent, records_data)


async def _sync_manager_card(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Manager_Card__c ({mode})...")

    soql_fields = "Id, OwnerId, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastViewedDate, LastReferencedDate, Card__c, Manager__c, Source_Type__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Manager_Card__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "OwnerId": r.get("OwnerId"),
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "Card__c": r.get("Card__c"),
            "Manager__c": r.get("Manager__c"),
            "Source_Type__c": r.get("Source_Type__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, ManagerCard, records_data)


async def _sync_cluster(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Cluster__c ({mode})...")

    soql_fields = "Id, OwnerId, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastViewedDate, LastReferencedDate, Email__c, US_Number__c, India_Number__c, Cluster_Aliaz__c, Cluster_ID__c, IN_Job_Student_Count__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Cluster__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "OwnerId": r.get("OwnerId"),
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "Email__c": r.get("Email__c"),
            "US_Number__c": r.get("US_Number__c"),
            "India_Number__c": r.get("India_Number__c"),
            "Cluster_Aliaz__c": r.get("Cluster_Aliaz__c"),
            "Cluster_ID__c": r.get("Cluster_ID__c"),
            "IN_Job_Student_Count__c": r.get("IN_Job_Student_Count__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, Cluster, records_data)


async def _sync_organization(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Organization__c ({mode})...")

    soql_fields = "Id, OwnerId, Name, RecordTypeId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastActivityDate, LastViewedDate, LastReferencedDate, ACCOUNT_NUMBER__c, Bank_Name__c, Bank_Phone_Number__c, Cluster__c, Contract_Mgr__c, Current_Address__c, DUNS_Number__c, Date_Incorporated__c, EIN__c, E_Verify_Number__c, File_Number__c, Major_Shareholder__c, Minor_Shareholder1_DIN_Number__c, Minor_Shareholder1__c, Minor_Shareholder1_s_CIN_NUMBER__c, Minor_Shareholder1_s_Date_of_Appointment__c, Minor_Shareholder2__c, Offler_Letter_format_Google_Drive_Link__c, Domain__c, OrgName__c, Owner_s_Mail_ID__c, Phone_Numbers__c, Registered_Address__c, Routing__c, Sales_Head__c, Tax_Payer_Number__c, Web_File_Number__c, Website__c, Cluster_Account__c, ORG_ID__c, Country_Incorporated__c, Current_Owner__c, Registered_Date__c, Registered_Owner__c, E_Verify_Login_ID__c, Swift_Code__c, IFSC_Code__c, Owner_Name__c, Cluster1__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Organization__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "OwnerId": r.get("OwnerId"),
            "Name": r.get("Name"),
            "RecordTypeId": r.get("RecordTypeId"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastActivityDate": _parse_sf_date(r.get("LastActivityDate")),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "ACCOUNT_NUMBER__c": r.get("ACCOUNT_NUMBER__c"),
            "Bank_Name__c": r.get("Bank_Name__c"),
            "Bank_Phone_Number__c": r.get("Bank_Phone_Number__c"),
            "Cluster__c": r.get("Cluster__c"),
            "Contract_Mgr__c": r.get("Contract_Mgr__c"),
            "Current_Address__c": r.get("Current_Address__c"),
            "DUNS_Number__c": r.get("DUNS_Number__c"),
            "Date_Incorporated__c": _parse_sf_date(r.get("Date_Incorporated__c")),
            "EIN__c": r.get("EIN__c"),
            "E_Verify_Number__c": r.get("E_Verify_Number__c"),
            "File_Number__c": r.get("File_Number__c"),
            "Major_Shareholder__c": r.get("Major_Shareholder__c"),
            "Minor_Shareholder1_DIN_Number__c": r.get("Minor_Shareholder1_DIN_Number__c"),
            "Minor_Shareholder1__c": r.get("Minor_Shareholder1__c"),
            "Minor_Shareholder1_s_CIN_NUMBER__c": r.get("Minor_Shareholder1_s_CIN_NUMBER__c"),
            "Minor_Shareholder1_s_Date_of_Appointment__c": r.get("Minor_Shareholder1_s_Date_of_Appointment__c"),
            "Minor_Shareholder2__c": r.get("Minor_Shareholder2__c"),
            "Offler_Letter_format_Google_Drive_Link__c": r.get("Offler_Letter_format_Google_Drive_Link__c"),
            "Domain__c": r.get("Domain__c"),
            "OrgName__c": r.get("OrgName__c"),
            "Owner_s_Mail_ID__c": r.get("Owner_s_Mail_ID__c"),
            "Phone_Numbers__c": r.get("Phone_Numbers__c"),
            "Registered_Address__c": r.get("Registered_Address__c"),
            "Routing__c": r.get("Routing__c"),
            "Sales_Head__c": r.get("Sales_Head__c"),
            "Tax_Payer_Number__c": r.get("Tax_Payer_Number__c"),
            "Web_File_Number__c": r.get("Web_File_Number__c"),
            "Website__c": r.get("Website__c"),
            "Cluster_Account__c": r.get("Cluster_Account__c"),
            "ORG_ID__c": r.get("ORG_ID__c"),
            "Country_Incorporated__c": r.get("Country_Incorporated__c"),
            "Current_Owner__c": r.get("Current_Owner__c"),
            "Registered_Date__c": _parse_sf_date(r.get("Registered_Date__c")),
            "Registered_Owner__c": r.get("Registered_Owner__c"),
            "E_Verify_Login_ID__c": r.get("E_Verify_Login_ID__c"),
            "Swift_Code__c": r.get("Swift_Code__c"),
            "IFSC_Code__c": r.get("IFSC_Code__c"),
            "Owner_Name__c": r.get("Owner_Name__c"),
            "Cluster1__c": r.get("Cluster1__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, Organization, records_data)


async def _sync_pay_off(session, last_sync=None):
    since = _since_clause(last_sync)
    mode = "incremental" if last_sync else "full"
    logger.info(f"Syncing Pay_Off__c ({mode})...")

    soql_fields = "Id, OwnerId, Name, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, LastViewedDate, LastReferencedDate, Cluster__c"
    records = await _fetch_all(f"SELECT {soql_fields} FROM Pay_Off__c{since}")

    now = datetime.utcnow()
    records_data = []
    for r in records:
        records_data.append({
            "Id": r["Id"],
            "OwnerId": r.get("OwnerId"),
            "Name": r.get("Name"),
            "CreatedDate": _parse_sf_datetime(r.get("CreatedDate")),
            "CreatedById": r.get("CreatedById"),
            "LastModifiedDate": _parse_sf_datetime(r.get("LastModifiedDate")),
            "LastModifiedById": r.get("LastModifiedById"),
            "LastViewedDate": _parse_sf_datetime(r.get("LastViewedDate")),
            "LastReferencedDate": _parse_sf_datetime(r.get("LastReferencedDate")),
            "Cluster__c": r.get("Cluster__c"),
            "synced_at": now,
        })

    return await _upsert_batch(session, PayOff, records_data)


SYNC_TASKS = [
    ("Account", _sync_account),
    ("Contact", _sync_contact),
    ("User", _sync_user),
    ("Report", _sync_report),
    ("Student__c", _sync_student),
    ("Submissions__c", _sync_submissions),
    ("Interviews__c", _sync_interviews),
    ("Manager__c", _sync_manager),
    ("Job__c", _sync_job),
    ("Employee__c", _sync_employee),
    ("BU_Performance__c", _sync_bu_performance),
    ("BS__c", _sync_bs),
    ("Tech_Support__c", _sync_tech_support),
    ("New_Student__c", _sync_new_student),
    ("Manager_Card__c", _sync_manager_card),
    ("Cluster__c", _sync_cluster),
    ("Organization__c", _sync_organization),
    ("Pay_Off__c", _sync_pay_off),
]


async def run_sync(full=False):
    global _sync_running, _last_sync
    if _sync_running:
        logger.warning("Sync already running, skipping")
        return {"status": "already_running"}

    try:
        async with async_session() as lock_session:
            result = await lock_session.execute(text("SELECT pg_try_advisory_lock(12345)"))
            got_lock = result.scalar()
            if not got_lock:
                logger.warning("Another worker is syncing, skipping")
                return {"status": "already_running"}
    except Exception:
        pass

    _sync_running = True
    results = []
    logger.info("=== Starting Salesforce -> PostgreSQL sync ===")

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
                logger.info(f"  OK {obj_name}: {count} records synced")
            except Exception as e:
                await session.rollback()
                log_entry.status = "error"
                log_entry.error = str(e)[:500]
                log_entry.finished_at = datetime.utcnow()
                results.append({"object": obj_name, "records": 0, "status": "error", "error": str(e)[:200]})
                logger.error(f"  FAIL {obj_name}: {e}")

            session.add(log_entry)
            await session.commit()

    try:
        async with async_session() as lock_session:
            await lock_session.execute(text("SELECT pg_advisory_unlock(12345)"))
            await lock_session.commit()
    except Exception:
        pass

    _sync_running = False
    _last_sync = datetime.utcnow()
    total = sum(r["records"] for r in results)
    success_count = sum(1 for r in results if r["status"] == "success")
    error_count = sum(1 for r in results if r["status"] == "error")

    logger.info("=" * 60)
    logger.info("  SYNC SUMMARY")
    logger.info("=" * 60)
    for r in results:
        status_icon = "OK" if r["status"] == "success" else "FAIL"
        err = f" -- {r.get('error', '')[:80]}" if r["status"] == "error" else ""
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

    logger.info(f"Sync scheduler started -- every {interval} minutes")

    def loop():
        import time as _time
        while True:
            _time.sleep(interval * 60)
            try:
                asyncio.run(run_sync())
            except Exception as e:
                logger.error(f"Scheduled sync failed: {e}")

    threading.Thread(target=loop, daemon=True).start()
