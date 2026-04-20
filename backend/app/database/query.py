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
    "Account": {
        "table": "Account",
        "fields": {
            "Id": "Id",
            "MasterRecordId": "MasterRecordId",
            "Name": "Name",
            "Type": "Type",
            "ParentId": "ParentId",
            "BillingStreet": "BillingStreet",
            "BillingCity": "BillingCity",
            "BillingState": "BillingState",
            "BillingPostalCode": "BillingPostalCode",
            "BillingCountry": "BillingCountry",
            "BillingLatitude": "BillingLatitude",
            "BillingLongitude": "BillingLongitude",
            "BillingGeocodeAccuracy": "BillingGeocodeAccuracy",
            "ShippingStreet": "ShippingStreet",
            "ShippingCity": "ShippingCity",
            "ShippingState": "ShippingState",
            "ShippingPostalCode": "ShippingPostalCode",
            "ShippingCountry": "ShippingCountry",
            "ShippingLatitude": "ShippingLatitude",
            "ShippingLongitude": "ShippingLongitude",
            "ShippingGeocodeAccuracy": "ShippingGeocodeAccuracy",
            "Phone": "Phone",
            "Fax": "Fax",
            "Website": "Website",
            "PhotoUrl": "PhotoUrl",
            "Industry": "Industry",
            "AnnualRevenue": "AnnualRevenue",
            "NumberOfEmployees": "NumberOfEmployees",
            "Description": "Description",
            "OwnerId": "OwnerId",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "IsCustomerPortal": "IsCustomerPortal",
            "Jigsaw": "Jigsaw",
            "JigsawCompanyId": "JigsawCompanyId",
            "AccountSource": "AccountSource",
            "SicDesc": "SicDesc",
            "Domain_Name__c": "Domain_Name__c",
            "Account_Type__c": "Account_Type__c",
            "Cluster__c": "Cluster__c",
        },
    },
    "Contact": {
        "table": "Contact",
        "fields": {
            "Id": "Id",
            "MasterRecordId": "MasterRecordId",
            "AccountId": "AccountId",
            "LastName": "LastName",
            "FirstName": "FirstName",
            "Salutation": "Salutation",
            "Name": "Name",
            "OtherStreet": "OtherStreet",
            "OtherCity": "OtherCity",
            "OtherState": "OtherState",
            "OtherPostalCode": "OtherPostalCode",
            "OtherCountry": "OtherCountry",
            "OtherLatitude": "OtherLatitude",
            "OtherLongitude": "OtherLongitude",
            "OtherGeocodeAccuracy": "OtherGeocodeAccuracy",
            "MailingStreet": "MailingStreet",
            "MailingCity": "MailingCity",
            "MailingState": "MailingState",
            "MailingPostalCode": "MailingPostalCode",
            "MailingCountry": "MailingCountry",
            "MailingLatitude": "MailingLatitude",
            "MailingLongitude": "MailingLongitude",
            "MailingGeocodeAccuracy": "MailingGeocodeAccuracy",
            "Phone": "Phone",
            "Fax": "Fax",
            "MobilePhone": "MobilePhone",
            "HomePhone": "HomePhone",
            "OtherPhone": "OtherPhone",
            "AssistantPhone": "AssistantPhone",
            "ReportsToId": "ReportsToId",
            "Email": "Email",
            "Title": "Title",
            "Department": "Department",
            "AssistantName": "AssistantName",
            "LeadSource": "LeadSource",
            "Birthdate": "Birthdate",
            "Description": "Description",
            "OwnerId": "OwnerId",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastCURequestDate": "LastCURequestDate",
            "LastCUUpdateDate": "LastCUUpdateDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "EmailBouncedReason": "EmailBouncedReason",
            "EmailBouncedDate": "EmailBouncedDate",
            "IsEmailBounced": "IsEmailBounced",
            "PhotoUrl": "PhotoUrl",
            "Jigsaw": "Jigsaw",
            "JigsawContactId": "JigsawContactId",
            "IndividualId": "IndividualId",
            "IsPriorityRecord": "IsPriorityRecord",
            "Lead__c": "Lead__c",
            "Contact_Type__c": "Contact_Type__c",
            "Ext__c": "Ext__c",
            "Domain_Formula__c": "Domain_Formula__c",
            "Contact_User_Type__c": "Contact_User_Type__c",
        },
    },
    "User": {
        "table": "User",
        "fields": {
            "Id": "Id",
            "Username": "Username",
            "LastName": "LastName",
            "FirstName": "FirstName",
            "Name": "Name",
            "CompanyName": "CompanyName",
            "Division": "Division",
            "Department": "Department",
            "Title": "Title",
            "Street": "Street",
            "City": "City",
            "State": "State",
            "PostalCode": "PostalCode",
            "Country": "Country",
            "Latitude": "Latitude",
            "Longitude": "Longitude",
            "GeocodeAccuracy": "GeocodeAccuracy",
            "Email": "Email",
            "EmailPreferencesAutoBcc": "EmailPreferencesAutoBcc",
            "EmailPreferencesAutoBccStayInTouch": "EmailPreferencesAutoBccStayInTouch",
            "EmailPreferencesStayInTouchReminder": "EmailPreferencesStayInTouchReminder",
            "SenderEmail": "SenderEmail",
            "SenderName": "SenderName",
            "Signature": "Signature",
            "StayInTouchSubject": "StayInTouchSubject",
            "StayInTouchSignature": "StayInTouchSignature",
            "StayInTouchNote": "StayInTouchNote",
            "Phone": "Phone",
            "Fax": "Fax",
            "MobilePhone": "MobilePhone",
            "Alias": "Alias",
            "CommunityNickname": "CommunityNickname",
            "BadgeText": "BadgeText",
            "IsActive": "IsActive",
            "TimeZoneSidKey": "TimeZoneSidKey",
            "UserRoleId": "UserRoleId",
            "LocaleSidKey": "LocaleSidKey",
            "ReceivesInfoEmails": "ReceivesInfoEmails",
            "ReceivesAdminInfoEmails": "ReceivesAdminInfoEmails",
            "EmailEncodingKey": "EmailEncodingKey",
            "ProfileId": "ProfileId",
            "UserType": "UserType",
            "StartDay": "StartDay",
            "EndDay": "EndDay",
            "LanguageLocaleKey": "LanguageLocaleKey",
            "EmployeeNumber": "EmployeeNumber",
            "DelegatedApproverId": "DelegatedApproverId",
            "ManagerId": "ManagerId",
            "LastLoginDate": "LastLoginDate",
            "LastPasswordChangeDate": "LastPasswordChangeDate",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "PasswordExpirationDate": "PasswordExpirationDate",
            "NumberOfFailedLogins": "NumberOfFailedLogins",
            "SuAccessExpirationDate": "SuAccessExpirationDate",
            "OfflineTrialExpirationDate": "OfflineTrialExpirationDate",
            "OfflinePdaTrialExpirationDate": "OfflinePdaTrialExpirationDate",
            "UserPermissionsMarketingUser": "UserPermissionsMarketingUser",
            "UserPermissionsOfflineUser": "UserPermissionsOfflineUser",
            "UserPermissionsAvantgoUser": "UserPermissionsAvantgoUser",
            "UserPermissionsCallCenterAutoLogin": "UserPermissionsCallCenterAutoLogin",
            "UserPermissionsSFContentUser": "UserPermissionsSFContentUser",
            "UserPermissionsInteractionUser": "UserPermissionsInteractionUser",
            "UserPermissionsSupportUser": "UserPermissionsSupportUser",
            "ForecastEnabled": "ForecastEnabled",
            "UserPreferencesActivityRemindersPopup": "UserPreferencesActivityRemindersPopup",
            "UserPreferencesEventRemindersCheckboxDefault": "UserPreferencesEventRemindersCheckboxDefault",
            "UserPreferencesTaskRemindersCheckboxDefault": "UserPreferencesTaskRemindersCheckboxDefault",
            "UserPreferencesReminderSoundOff": "UserPreferencesReminderSoundOff",
            "UserPreferencesDisableAllFeedsEmail": "UserPreferencesDisableAllFeedsEmail",
            "UserPreferencesDisableFollowersEmail": "UserPreferencesDisableFollowersEmail",
            "UserPreferencesDisableProfilePostEmail": "UserPreferencesDisableProfilePostEmail",
            "UserPreferencesDisableChangeCommentEmail": "UserPreferencesDisableChangeCommentEmail",
            "UserPreferencesDisableLaterCommentEmail": "UserPreferencesDisableLaterCommentEmail",
            "UserPreferencesDisProfPostCommentEmail": "UserPreferencesDisProfPostCommentEmail",
            "UserPreferencesApexPagesDeveloperMode": "UserPreferencesApexPagesDeveloperMode",
            "UserPreferencesReceiveNoNotificationsAsApprover": "UserPreferencesReceiveNoNotificationsAsApprover",
            "UserPreferencesReceiveNotificationsAsDelegatedApprover": "UserPreferencesReceiveNotificationsAsDelegatedApprover",
            "UserPreferencesHideCSNGetChatterMobileTask": "UserPreferencesHideCSNGetChatterMobileTask",
            "UserPreferencesDisableMentionsPostEmail": "UserPreferencesDisableMentionsPostEmail",
            "UserPreferencesDisMentionsCommentEmail": "UserPreferencesDisMentionsCommentEmail",
            "UserPreferencesHideCSNDesktopTask": "UserPreferencesHideCSNDesktopTask",
            "UserPreferencesHideChatterOnboardingSplash": "UserPreferencesHideChatterOnboardingSplash",
            "UserPreferencesHideSecondChatterOnboardingSplash": "UserPreferencesHideSecondChatterOnboardingSplash",
            "UserPreferencesDisCommentAfterLikeEmail": "UserPreferencesDisCommentAfterLikeEmail",
            "UserPreferencesDisableLikeEmail": "UserPreferencesDisableLikeEmail",
            "UserPreferencesSortFeedByComment": "UserPreferencesSortFeedByComment",
            "UserPreferencesDisableMessageEmail": "UserPreferencesDisableMessageEmail",
            "UserPreferencesDisableBookmarkEmail": "UserPreferencesDisableBookmarkEmail",
            "UserPreferencesDisableSharePostEmail": "UserPreferencesDisableSharePostEmail",
            "UserPreferencesEnableAutoSubForFeeds": "UserPreferencesEnableAutoSubForFeeds",
            "UserPreferencesDisableFileShareNotificationsForApi": "UserPreferencesDisableFileShareNotificationsForApi",
            "UserPreferencesShowTitleToExternalUsers": "UserPreferencesShowTitleToExternalUsers",
            "UserPreferencesShowManagerToExternalUsers": "UserPreferencesShowManagerToExternalUsers",
            "UserPreferencesShowEmailToExternalUsers": "UserPreferencesShowEmailToExternalUsers",
            "UserPreferencesShowWorkPhoneToExternalUsers": "UserPreferencesShowWorkPhoneToExternalUsers",
            "UserPreferencesShowMobilePhoneToExternalUsers": "UserPreferencesShowMobilePhoneToExternalUsers",
            "UserPreferencesShowFaxToExternalUsers": "UserPreferencesShowFaxToExternalUsers",
            "UserPreferencesShowStreetAddressToExternalUsers": "UserPreferencesShowStreetAddressToExternalUsers",
            "UserPreferencesShowCityToExternalUsers": "UserPreferencesShowCityToExternalUsers",
            "UserPreferencesShowStateToExternalUsers": "UserPreferencesShowStateToExternalUsers",
            "UserPreferencesShowPostalCodeToExternalUsers": "UserPreferencesShowPostalCodeToExternalUsers",
            "UserPreferencesShowCountryToExternalUsers": "UserPreferencesShowCountryToExternalUsers",
            "UserPreferencesShowProfilePicToGuestUsers": "UserPreferencesShowProfilePicToGuestUsers",
            "UserPreferencesShowTitleToGuestUsers": "UserPreferencesShowTitleToGuestUsers",
            "UserPreferencesShowCityToGuestUsers": "UserPreferencesShowCityToGuestUsers",
            "UserPreferencesShowStateToGuestUsers": "UserPreferencesShowStateToGuestUsers",
            "UserPreferencesShowPostalCodeToGuestUsers": "UserPreferencesShowPostalCodeToGuestUsers",
            "UserPreferencesShowCountryToGuestUsers": "UserPreferencesShowCountryToGuestUsers",
            "UserPreferencesShowForecastingChangeSignals": "UserPreferencesShowForecastingChangeSignals",
            "UserPreferencesLiveAgentMiawSetupDeflection": "UserPreferencesLiveAgentMiawSetupDeflection",
            "UserPreferencesHideS1BrowserUI": "UserPreferencesHideS1BrowserUI",
            "UserPreferencesDisableEndorsementEmail": "UserPreferencesDisableEndorsementEmail",
            "UserPreferencesPathAssistantCollapsed": "UserPreferencesPathAssistantCollapsed",
            "UserPreferencesCacheDiagnostics": "UserPreferencesCacheDiagnostics",
            "UserPreferencesShowEmailToGuestUsers": "UserPreferencesShowEmailToGuestUsers",
            "UserPreferencesShowManagerToGuestUsers": "UserPreferencesShowManagerToGuestUsers",
            "UserPreferencesShowWorkPhoneToGuestUsers": "UserPreferencesShowWorkPhoneToGuestUsers",
            "UserPreferencesShowMobilePhoneToGuestUsers": "UserPreferencesShowMobilePhoneToGuestUsers",
            "UserPreferencesShowFaxToGuestUsers": "UserPreferencesShowFaxToGuestUsers",
            "UserPreferencesShowStreetAddressToGuestUsers": "UserPreferencesShowStreetAddressToGuestUsers",
            "UserPreferencesLightningExperiencePreferred": "UserPreferencesLightningExperiencePreferred",
            "UserPreferencesPreviewLightning": "UserPreferencesPreviewLightning",
            "UserPreferencesHideEndUserOnboardingAssistantModal": "UserPreferencesHideEndUserOnboardingAssistantModal",
            "UserPreferencesHideLightningMigrationModal": "UserPreferencesHideLightningMigrationModal",
            "UserPreferencesHideSfxWelcomeMat": "UserPreferencesHideSfxWelcomeMat",
            "UserPreferencesHideBiggerPhotoCallout": "UserPreferencesHideBiggerPhotoCallout",
            "UserPreferencesGlobalNavBarWTShown": "UserPreferencesGlobalNavBarWTShown",
            "UserPreferencesGlobalNavGridMenuWTShown": "UserPreferencesGlobalNavGridMenuWTShown",
            "UserPreferencesCreateLEXAppsWTShown": "UserPreferencesCreateLEXAppsWTShown",
            "UserPreferencesFavoritesWTShown": "UserPreferencesFavoritesWTShown",
            "UserPreferencesRecordHomeSectionCollapseWTShown": "UserPreferencesRecordHomeSectionCollapseWTShown",
            "UserPreferencesRecordHomeReservedWTShown": "UserPreferencesRecordHomeReservedWTShown",
            "UserPreferencesFavoritesShowTopFavorites": "UserPreferencesFavoritesShowTopFavorites",
            "UserPreferencesExcludeMailAppAttachments": "UserPreferencesExcludeMailAppAttachments",
            "UserPreferencesSuppressTaskSFXReminders": "UserPreferencesSuppressTaskSFXReminders",
            "UserPreferencesSuppressEventSFXReminders": "UserPreferencesSuppressEventSFXReminders",
            "UserPreferencesPreviewCustomTheme": "UserPreferencesPreviewCustomTheme",
            "UserPreferencesHasCelebrationBadge": "UserPreferencesHasCelebrationBadge",
            "UserPreferencesUserDebugModePref": "UserPreferencesUserDebugModePref",
            "UserPreferencesSRHOverrideActivities": "UserPreferencesSRHOverrideActivities",
            "UserPreferencesNewLightningReportRunPageEnabled": "UserPreferencesNewLightningReportRunPageEnabled",
            "UserPreferencesReverseOpenActivitiesView": "UserPreferencesReverseOpenActivitiesView",
            "UserPreferencesHasSentWarningEmail": "UserPreferencesHasSentWarningEmail",
            "UserPreferencesHasSentWarningEmail238": "UserPreferencesHasSentWarningEmail238",
            "UserPreferencesHasSentWarningEmail240": "UserPreferencesHasSentWarningEmail240",
            "UserPreferencesNativeEmailClient": "UserPreferencesNativeEmailClient",
            "UserPreferencesHideBrowseProductRedirectConfirmation": "UserPreferencesHideBrowseProductRedirectConfirmation",
            "UserPreferencesHideOnlineSalesAppWelcomeMat": "UserPreferencesHideOnlineSalesAppWelcomeMat",
            "UserPreferencesShowForecastingRoundedAmounts": "UserPreferencesShowForecastingRoundedAmounts",
            "ContactId": "ContactId",
            "AccountId": "AccountId",
            "CallCenterId": "CallCenterId",
            "Extension": "Extension",
            "PortalRole": "PortalRole",
            "IsPortalEnabled": "IsPortalEnabled",
            "FederationIdentifier": "FederationIdentifier",
            "AboutMe": "AboutMe",
            "FullPhotoUrl": "FullPhotoUrl",
            "SmallPhotoUrl": "SmallPhotoUrl",
            "IsExtIndicatorVisible": "IsExtIndicatorVisible",
            "OutOfOfficeMessage": "OutOfOfficeMessage",
            "MediumPhotoUrl": "MediumPhotoUrl",
            "DigestFrequency": "DigestFrequency",
            "DefaultGroupNotificationFrequency": "DefaultGroupNotificationFrequency",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "BannerPhotoUrl": "BannerPhotoUrl",
            "SmallBannerPhotoUrl": "SmallBannerPhotoUrl",
            "MediumBannerPhotoUrl": "MediumBannerPhotoUrl",
            "IsProfilePhotoActive": "IsProfilePhotoActive",
            "IndividualId": "IndividualId",
            "Company__c": "Company__c",
            "Last_Login_Date__c": "Last_Login_Date__c",
            "UserLiscence__c": "UserLiscence__c",
        },
    },
    "Report": {
        "table": "Report",
        "fields": {
            "Id": "Id",
            "OwnerId": "OwnerId",
            "FolderName": "FolderName",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "Name": "Name",
            "Description": "Description",
            "DeveloperName": "DeveloperName",
            "NamespacePrefix": "NamespacePrefix",
            "LastRunDate": "LastRunDate",
            "Format": "Format",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
        },
    },
    "Student__c": {
        "table": "Student__c",
        "fields": {
            "Id": "Id",
            "OwnerId": "OwnerId",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "Batch__c": "Batch__c",
            "Comments__c": "Comments__c",
            "DL_Expiry_Date__c": "DL_Expiry_Date__c",
            "DL_Issue_Date__c": "DL_Issue_Date__c",
            "DL_Photo_Uploaded__c": "DL_Photo_Uploaded__c",
            "DOB__c": "DOB__c",
            "District_In_India__c": "District_In_India__c",
            "EAD_Card_Number__c": "EAD_Card_Number__c",
            "Father_First_Name__c": "Father_First_Name__c",
            "Father_Last_Name__c": "Father_Last_Name__c",
            "In_Market_Student_Count__c": "In_Market_Student_Count__c",
            "Final_Marketing_Status__c": "Final_Marketing_Status__c",
            "Folder_Sharing__c": "Folder_Sharing__c",
            "Folder_in_Drive__c": "Folder_in_Drive__c",
            "GC_Back_Ref_Number__c": "GC_Back_Ref_Number__c",
            "GC_Back_Verified__c": "GC_Back_Verified__c",
            "GC_Catogery__c": "GC_Catogery__c",
            "GC_Expiry_Date__c": "GC_Expiry_Date__c",
            "GC_Issued_Date__c": "GC_Issued_Date__c",
            "GHID__c": "GHID__c",
            "Gender__c": "Gender__c",
            "Google_Voice_Number__c": "Google_Voice_Number__c",
            "Guest_House__c": "Guest_House__c",
            "Has_Linkedin_Created__c": "Has_Linkedin_Created__c",
            "IS_DL_ID_CHANGED__c": "IS_DL_ID_CHANGED__c",
            "India_Address_Line1__c": "India_Address_Line1__c",
            "India_Address_Line2__c": "India_Address_Line2__c",
            "Total_Count_not_Exit__c": "Total_Count_not_Exit__c",
            "Is_All_Docs_Reviewed_by_Manager__c": "Is_All_Docs_Reviewed_by_Manager__c",
            "Is_DL_Ready__c": "Is_DL_Ready__c",
            "Is_DOC_Ready__c": "Is_DOC_Ready__c",
            "Is_GC_Front_Verified__c": "Is_GC_Front_Verified__c",
            "Is_Marketing_Sheet_Updated_by_Manager_Le__c": "Is_Marketing_Sheet_Updated_by_Manager_Le__c",
            "Is_Offer_Letter_Issued__c": "Is_Offer_Letter_Issued__c",
            "LeadID__c": "LeadID__c",
            "Linkedin_Connection_Count__c": "Linkedin_Connection_Count__c",
            "Linkedin_URL__c": "Linkedin_URL__c",
            "MQ_Screening_By_Lead__c": "MQ_Screening_By_Lead__c",
            "MQ_Screening_By_Manager__c": "MQ_Screening_By_Manager__c",
            "MS_Sheet_Explanation__c": "MS_Sheet_Explanation__c",
            "MS_Uploaded_By_Student__c": "MS_Uploaded_By_Student__c",
            "Marketing_Company__c": "Marketing_Company__c",
            "Marketing_DOB__c": "Marketing_DOB__c",
            "Marketing_Email__c": "Marketing_Email__c",
            "Marketing_End_Date__c": "Marketing_End_Date__c",
            "Marketing_Sheet_Screening_by_Lead__c": "Marketing_Sheet_Screening_by_Lead__c",
            "Marketing_Sheet_Screening_by_Manager__c": "Marketing_Sheet_Screening_by_Manager__c",
            "Marketing_Start_Date__c": "Marketing_Start_Date__c",
            "Marketing_Visa_Status__c": "Marketing_Visa_Status__c",
            "Mother_First_Name__c": "Mother_First_Name__c",
            "Mother_Last_Name__c": "Mother_Last_Name__c",
            "Offer_Issued_By_Name__c": "Offer_Issued_By_Name__c",
            "Offer_Issued_By_Phone__c": "Offer_Issued_By_Phone__c",
            "Offer_Type__c": "Offer_Type__c",
            "Onboarding_Company__c": "Onboarding_Company__c",
            "Onboarding_End_Date__c": "Onboarding_End_Date__c",
            "Onboarding_Start_Date__c": "Onboarding_Start_Date__c",
            "Onboarding_Visa_Status__c": "Onboarding_Visa_Status__c",
            "Otter_Final_Screening__c": "Otter_Final_Screening__c",
            "Otter_Real_Time_Screeing_1__c": "Otter_Real_Time_Screeing_1__c",
            "Otter_Real_Time_Screeing_2__c": "Otter_Real_Time_Screeing_2__c",
            "Otter_Real_Time_Screeing_3__c": "Otter_Real_Time_Screeing_3__c",
            "Otter_Real_Time_Screeing_4__c": "Otter_Real_Time_Screeing_4__c",
            "PayRate__c": "PayRate__c",
            "Passport_Number__c": "Passport_Number__c",
            "Total_Interview_Amount__c": "Total_Interview_Amount__c",
            "Personal_Email__c": "Personal_Email__c",
            "Pin_In_India__c": "Pin_In_India__c",
            "PreMarketingStatus__c": "PreMarketingStatus__c",
            "Present_Client_Location__c": "Present_Client_Location__c",
            "Recruiter__c": "Recruiter__c",
            "Ref1__c": "Ref1__c",
            "Ref2__c": "Ref2__c",
            "Resume_Preparation__c": "Resume_Preparation__c",
            "Resume_Verified_By_Lead__c": "Resume_Verified_By_Lead__c",
            "Resume_Verified_By_Manager__c": "Resume_Verified_By_Manager__c",
            "Review_comments__c": "Review_comments__c",
            "Shared_Drive_URL_To_Student__c": "Shared_Drive_URL_To_Student__c",
            "State_In_India__c": "State_In_India__c",
            "Student_First_Name__c": "Student_First_Name__c",
            "Student_GH_Arrival_Date__c": "Student_GH_Arrival_Date__c",
            "Student_GH_Departure_Date__c": "Student_GH_Departure_Date__c",
            "Student_Last_Name__c": "Student_Last_Name__c",
            "Student_Marketing_Status__c": "Student_Marketing_Status__c",
            "Student_Personal_Mobile__c": "Student_Personal_Mobile__c",
            "Technology__c": "Technology__c",
            "Town_City_In_India__c": "Town_City_In_India__c",
            "USCIS_Number__c": "USCIS_Number__c",
            "US_Emergency_Number__c": "US_Emergency_Number__c",
            "University__c": "University__c",
            "Visa_End_Date__c": "Visa_End_Date__c",
            "Visa_Start_Date__c": "Visa_Start_Date__c",
            "Interviews_Count__c": "Interviews_Count__c",
            "Submission_Count__c": "Submission_Count__c",
            "Total_Count__c": "Total_Count__c",
            "Paid_Offer_Start_Date__c": "Paid_Offer_Start_Date__c",
            "MarketingCompanyName__c": "MarketingCompanyName__c",
            "Month_Of_VC__c": "Month_Of_VC__c",
            "Student_Full_Name__c": "Student_Full_Name__c",
            "Manager__c": "Manager__c",
            "Cluster_Account__c": "Cluster_Account__c",
            "Submission_Count_URL__c": "Submission_Count_URL__c",
            "Interviews_Count_URL__c": "Interviews_Count_URL__c",
            "Lead_Contact__c": "Lead_Contact__c",
            "Manager_Contact__c": "Manager_Contact__c",
            "Phone__c": "Phone__c",
            "Recruiter_Name__c": "Recruiter_Name__c",
            "Offshore_Manager_Name__c": "Offshore_Manager_Name__c",
            "Students_Sitting_in_Office_GH_9AM_5PM__c": "Students_Sitting_in_Office_GH_9AM_5PM__c",
            "Basic_Knowledge_on_Subject__c": "Basic_Knowledge_on_Subject__c",
            "Communication_Skills__c": "Communication_Skills__c",
            "Dummy_Vendor_Call_to_Candidate__c": "Dummy_Vendor_Call_to_Candidate__c",
            "Recruiter_Screening__c": "Recruiter_Screening__c",
            "Prepared_Steps_need_to_Follow_Doc__c": "Prepared_Steps_need_to_Follow_Doc__c",
            "Company_Mismatch__c": "Company_Mismatch__c",
            "Recruiter_Contact__c": "Recruiter_Contact__c",
            "OffShore_Manager_Contact__c": "OffShore_Manager_Contact__c",
            "OffShore_Floor_Manager_Contact__c": "OffShore_Floor_Manager_Contact__c",
            "Bill_Rate__c": "Bill_Rate__c",
            "Verbal_Conf_Count__c": "Verbal_Conf_Count__c",
            "Client_Name__c": "Client_Name__c",
            "Job_End_Date__c": "Job_End_Date__c",
            "Prime_Vendor_Name__c": "Prime_Vendor_Name__c",
            "Job_Start_Date__c": "Job_Start_Date__c",
            "Vendor_Compnay_Name__c": "Vendor_Compnay_Name__c",
            "Job_Location__c": "Job_Location__c",
            "Vendor_Person_Name__c": "Vendor_Person_Name__c",
            "Vendor_Person_Email__c": "Vendor_Person_Email__c",
            "Implementation_Partner__c": "Implementation_Partner__c",
            "Interview_Date__c": "Interview_Date__c",
            "Vendor_Person_Phone__c": "Vendor_Person_Phone__c",
            "Cluster_Contact__c": "Cluster_Contact__c",
            "Onsite_Lead_Name__c": "Onsite_Lead_Name__c",
            "Onsite_Manager_Name__c": "Onsite_Manager_Name__c",
            "Conf_Month__c": "Conf_Month__c",
            "Marketing_Company_Name__c": "Marketing_Company_Name__c",
            "Report_Students_Count__c": "Report_Students_Count__c",
            "Cluster_Name__c": "Cluster_Name__c",
            "Verbal_Confirmation_Date__c": "Verbal_Confirmation_Date__c",
            "Job_Title__c": "Job_Title__c",
            "Days_in_Market_Business__c": "Days_in_Market_Business__c",
            "Submissions__c": "Submissions__c",
            "Interviews__c": "Interviews__c",
            "Incentive_Amount__c": "Incentive_Amount__c",
            "Recent_Interview_Date__c": "Recent_Interview_Date__c",
            "Interview_Remarks__c": "Interview_Remarks__c",
            "Recent_Past_Interview_Date__c": "Recent_Past_Interview_Date__c",
            "Students_Count_In_Market__c": "Students_Count_In_Market__c",
            "Last_week_Submissions__c": "Last_week_Submissions__c",
            "Last_week_Interviews__c": "Last_week_Interviews__c",
            "RecentUpcoming_Interview_Datetime__c": "RecentUpcoming_Interview_Datetime__c",
            "RecentUpcoming_Interview_Date__c": "RecentUpcoming_Interview_Date__c",
            "Ceipal_Status__c": "Ceipal_Status__c",
            "Last_Week_Interview_Count__c": "Last_Week_Interview_Count__c",
            "Cluster__c": "Cluster__c",
            "Verbal_or_Project_Started_Count__c": "Verbal_or_Project_Started_Count__c",
            "Lead_Bill_Rate__c": "Lead_Bill_Rate__c",
            "Project_Start_Month__c": "Project_Start_Month__c",
            "Aug_2025_Submissions__c": "Aug_2025_Submissions__c",
            "Management__c": "Management__c",
            "Old_Student__c": "Old_Student__c",
            "Old_Manager_Name__c": "Old_Manager_Name__c",
            "Old_Student_Name__c": "Old_Student_Name__c",
            "Old_Student_Phone__c": "Old_Student_Phone__c",
            "Student_ID__c": "Student_ID__c",
            "Project_Type__c": "Project_Type__c",
            "Conformation_Submission_ID__c": "Conformation_Submission_ID__c",
            "Conformation_Interview_ID__c": "Conformation_Interview_ID__c",
            "Father_Mobile_Number__c": "Father_Mobile_Number__c",
            "India_Emergency_Contact_number__c": "India_Emergency_Contact_number__c",
            "Parent_Mobile__c": "Parent_Mobile__c",
            "Trigger_Check__c": "Trigger_Check__c",
            "Aug_2025_Interviews__c": "Aug_2025_Interviews__c",
            "June_2025_Interviews__c": "June_2025_Interviews__c",
            "June_2025_Submissions__c": "June_2025_Submissions__c",
            "July_2025_Interviews__c": "July_2025_Interviews__c",
            "July_2025_Submissions__c": "July_2025_Submissions__c",
            "Last_Submission_Date__c": "Last_Submission_Date__c",
            "Submissions_Count_From_Specific_Date__c": "Submissions_Count_From_Specific_Date__c",
            "Vendor_Manager_Name__c": "Vendor_Manager_Name__c",
            "Onboarding_Cmpny__c": "Onboarding_Cmpny__c",
            "Last_End_To_End_Screening_Date__c": "Last_End_To_End_Screening_Date__c",
            "Availability__c": "Availability__c",
            "MS_Experience_Details_Screening__c": "MS_Experience_Details_Screening__c",
            "MS_Visa_Details_Screening__c": "MS_Visa_Details_Screening__c",
            "Resume_Verification__c": "Resume_Verification__c",
            "General_Marketing_Questions_Screening__c": "General_Marketing_Questions_Screening__c",
            "Documents_Verification__c": "Documents_Verification__c",
            "Technical_Marketing_Questions_Screening__c": "Technical_Marketing_Questions_Screening__c",
            "Otter_Screening__c": "Otter_Screening__c",
            "Daily_Tracker_Sheet_Review__c": "Daily_Tracker_Sheet_Review__c",
            "Student_LinkedIn_Account_Review__c": "Student_LinkedIn_Account_Review__c",
            "Prime_Vendor_Database__c": "Prime_Vendor_Database__c",
            "Answering_To_Vendor__c": "Answering_To_Vendor__c",
            "Reverse_Questions_To_Vendor__c": "Reverse_Questions_To_Vendor__c",
            "Recruiter_Department__c": "Recruiter_Department__c",
            "Documents_Review__c": "Documents_Review__c",
            "Resume_Review__c": "Resume_Review__c",
            "Marketing_Sheet_Review__c": "Marketing_Sheet_Review__c",
            "Month_Of_Paid_Offer__c": "Month_Of_Paid_Offer__c",
            "Vendor_Manager_Email__c": "Vendor_Manager_Email__c",
            "Recruiter_Start_Date__c": "Recruiter_Start_Date__c",
            "Supervisor_Name__c": "Supervisor_Name__c",
            "Vendor_Manager_Phone__c": "Vendor_Manager_Phone__c",
            "Domain_Name__c": "Domain_Name__c",
            "Lead_Incentive_Applicable__c": "Lead_Incentive_Applicable__c",
            "Employee_Approval_Status__c": "Employee_Approval_Status__c",
            "This_week_Interviews__c": "This_week_Interviews__c",
            "This_week_Submissions__c": "This_week_Submissions__c",
            "Lead_Incentive_Employee__c": "Lead_Incentive_Employee__c",
            "Sum_of_Amount_USD_Student__c": "Sum_of_Amount_USD_Student__c",
            "Reason_for_Exit__c": "Reason_for_Exit__c",
            "Active_Job_Count__c": "Active_Job_Count__c",
        },
    },
    "Submissions__c": {
        "table": "Submissions__c",
        "fields": {
            "Id": "Id",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "Student__c": "Student__c",
            "Client_Name__c": "Client_Name__c",
            "Implement_Company_Name__c": "Implement_Company_Name__c",
            "Prime_Vendor_Name__c": "Prime_Vendor_Name__c",
            "Rate__c": "Rate__c",
            "Recuter__c": "Recuter__c",
            "Submission_Date__c": "Submission_Date__c",
            "Submission_Status__c": "Submission_Status__c",
            "Vendor_Compnay_Name__c": "Vendor_Compnay_Name__c",
            "Vendor_Person_Email__c": "Vendor_Person_Email__c",
            "Vendor_Person_Name__c": "Vendor_Person_Name__c",
            "Vendor_Person_Phone__c": "Vendor_Person_Phone__c",
            "Vendor_Company__c": "Vendor_Company__c",
            "Vendor_Contact__c": "Vendor_Contact__c",
            "Ext__c": "Ext__c",
            "Last_3_Business_Days_Check__c": "Last_3_Business_Days_Check__c",
            "Job_Description__c": "Job_Description__c",
            "Onsite_Manager_Name__c": "Onsite_Manager_Name__c",
            "Offshore_Manager_Name__c": "Offshore_Manager_Name__c",
            "Recruiter_Name__c": "Recruiter_Name__c",
            "Onsite_Lead_Name__c": "Onsite_Lead_Name__c",
            "Student_Name__c": "Student_Name__c",
            "Technology__c": "Technology__c",
            "Domain_Formula__c": "Domain_Formula__c",
            "Is_Today_Submission__c": "Is_Today_Submission__c",
            "BU_Name__c": "BU_Name__c",
            "Last_3_Business_Days__c": "Last_3_Business_Days__c",
            "Vendor_Client_Student__c": "Vendor_Client_Student__c",
            "Sub_Count__c": "Sub_Count__c",
            "Is_Yesterday_s_Submission__c": "Is_Yesterday_s_Submission__c",
            "Is_Last_Week_Sub__c": "Is_Last_Week_Sub__c",
            "Comments__c": "Comments__c",
            "Last_Week_Business_Day_Check__c": "Last_Week_Business_Day_Check__c",
        },
    },
    "Interviews__c": {
        "table": "Interviews__c",
        "fields": {
            "Id": "Id",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "Student__c": "Student__c",
            "Amount__c": "Amount__c",
            "Duration__c": "Duration__c",
            "Interview_Date__c": "Interview_Date__c",
            "Interview_Q_A__c": "Interview_Q_A__c",
            "Month__c": "Month__c",
            "Interviewer_Email__c": "Interviewer_Email__c",
            "Interviewer_Name__c": "Interviewer_Name__c",
            "Otter_Link__c": "Otter_Link__c",
            "Submissions__c": "Submissions__c",
            "Support_First_Name__c": "Support_First_Name__c",
            "Support_Last_Name__c": "Support_Last_Name__c",
            "Tech_Support__c": "Tech_Support__c",
            "Type__c": "Type__c",
            "TechSupportName__c": "TechSupportName__c",
            "Interview_End_Date_Time__c": "Interview_End_Date_Time__c",
            "Final_Status__c": "Final_Status__c",
            "Calender_Prefix__c": "Calender_Prefix__c",
            "Lead_Manager_Joined__c": "Lead_Manager_Joined__c",
            "Student_Otter_Performance__c": "Student_Otter_Performance__c",
            "Student_Technical_Explanation_Skill__c": "Student_Technical_Explanation_Skill__c",
            "Proxy_General_Issues__c": "Proxy_General_Issues__c",
            "Any_Technical_Issues__c": "Any_Technical_Issues__c",
            "Recruiter_Name__c": "Recruiter_Name__c",
            "Offshore_Manager__c": "Offshore_Manager__c",
            "Offshore_Floor_Manager__c": "Offshore_Floor_Manager__c",
            "Onsite_Lead__c": "Onsite_Lead__c",
            "Onsite_Manager__c": "Onsite_Manager__c",
            "Cluster__c": "Cluster__c",
            "Vendor_Company_Name__c": "Vendor_Company_Name__c",
            "Vendor_Person_Name__c": "Vendor_Person_Name__c",
            "Vendor_Person_Phone__c": "Vendor_Person_Phone__c",
            "Vendor_Email__c": "Vendor_Email__c",
            "Prime_Vendor_Name__c": "Prime_Vendor_Name__c",
            "Implementation_Partner__c": "Implementation_Partner__c",
            "Client_Name__c": "Client_Name__c",
            "Bill_Rate__c": "Bill_Rate__c",
            "Student_Technology__c": "Student_Technology__c",
            "Lead_Name__c": "Lead_Name__c",
            "Project_Start_Date__c": "Project_Start_Date__c",
            "Amount_INR__c": "Amount_INR__c",
            "Verbal_Conf_Count__c": "Verbal_Conf_Count__c",
            "Interview_Date1__c": "Interview_Date1__c",
            "Paid__c": "Paid__c",
            "Int_Count__c": "Int_Count__c",
            "Manager_Tech__c": "Manager_Tech__c",
            "Interviews_Count__c": "Interviews_Count__c",
            "Final_Feedback__c": "Final_Feedback__c",
            "Job_Status__c": "Job_Status__c",
            "Verbal_Intw_Date__c": "Verbal_Intw_Date__c",
            "Paid_By__c": "Paid_By__c",
            "Is_Last_Week__c": "Is_Last_Week__c",
        },
    },
    "Manager__c": {
        "table": "Manager__c",
        "fields": {
            "Id": "Id",
            "OwnerId": "OwnerId",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "Cluster__c": "Cluster__c",
            "Email__c": "Email__c",
            "Offshore_Floor_Manager__c": "Offshore_Floor_Manager__c",
            "Offshore_Location__c": "Offshore_Location__c",
            "Offshore_Manager__c": "Offshore_Manager__c",
            "Operation_Location__c": "Operation_Location__c",
            "Organization__c": "Organization__c",
            "User__c": "User__c",
            "Cluster_Account__c": "Cluster_Account__c",
            "Total_Expenses__c": "Total_Expenses__c",
            "Exit_Student_Count__c": "Exit_Student_Count__c",
            "Students_Count_In_Market__c": "Students_Count_In_Market__c",
            "Project_Started_Count__c": "Project_Started_Count__c",
            "Pre_Marketing_Student_Count__c": "Pre_Marketing_Student_Count__c",
            "Total_Expenses_MIS__c": "Total_Expenses_MIS__c",
            "Each_Placement_Cost__c": "Each_Placement_Cost__c",
            "Approval_Status__c": "Approval_Status__c",
            "Visa__c": "Visa__c",
            "Type__c": "Type__c",
            "Offshore_Google_Pin__c": "Offshore_Google_Pin__c",
            "Offshore_POC_Number__c": "Offshore_POC_Number__c",
            "Website__c": "Website__c",
            "Offshore_Location_Type__c": "Offshore_Location_Type__c",
            "US_Office_Address__c": "US_Office_Address__c",
            "US_Office_Rent__c": "US_Office_Rent__c",
            "Offshore_Office_Rent__c": "Offshore_Office_Rent__c",
            "Active__c": "Active__c",
            "BU_Student_With_Job_Count__c": "BU_Student_With_Job_Count__c",
            "HR_Contact__c": "HR_Contact__c",
            "USA_HR_Phone__c": "USA_HR_Phone__c",
            "USA_HR_Email__c": "USA_HR_Email__c",
            "Supervisor_Phone__c": "Supervisor_Phone__c",
            "Supervisor_Email__c": "Supervisor_Email__c",
            "Docs_Upload_Drive_Link__c": "Docs_Upload_Drive_Link__c",
            "Project_Status__c": "Project_Status__c",
            "Supervisor_Name__c": "Supervisor_Name__c",
            "Organization_Name__c": "Organization_Name__c",
            "HR_Name__c": "HR_Name__c",
            "Offshore_Floor_Manager_Contact__c": "Offshore_Floor_Manager_Contact__c",
            "Manager_ID__c": "Manager_ID__c",
            "US_Operation_CITY__c": "US_Operation_CITY__c",
            "US_Operation_State__c": "US_Operation_State__c",
            "Alias_Name__c": "Alias_Name__c",
            "Old_Phone__c": "Old_Phone__c",
            "New_Phone__c": "New_Phone__c",
            "Students_Count__c": "Students_Count__c",
            "In_Market_Students_Count__c": "In_Market_Students_Count__c",
            "Verbal_Count__c": "Verbal_Count__c",
            "IN_JOB_Students_Count__c": "IN_JOB_Students_Count__c",
            "India_HR__c": "India_HR__c",
            "India_HR_Mail__c": "India_HR_Mail__c",
        },
    },
    "Job__c": {
        "table": "Job__c",
        "fields": {
            "Id": "Id",
            "OwnerId": "OwnerId",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "Active__c": "Active__c",
            "Student_Name_Manager_Name__c": "Student_Name_Manager_Name__c",
            "Month_SD__c": "Month_SD__c",
            "Month_ED__c": "Month_ED__c",
            "PayRate__c": "PayRate__c",
            "Caluculated_Pay_Rate__c": "Caluculated_Pay_Rate__c",
            "Pay_Roll_Tax__c": "Pay_Roll_Tax__c",
            "Profit__c": "Profit__c",
            "Project_Type__c": "Project_Type__c",
            "Visa_Status__c": "Visa_Status__c",
            "Company_Manager__c": "Company_Manager__c",
            "Total_Interview_Amount__c": "Total_Interview_Amount__c",
            "Month_Of_VC__c": "Month_Of_VC__c",
            "Job_Title__c": "Job_Title__c",
            "Ceipal_Status__c": "Ceipal_Status__c",
            "Triggered_Ceipal__c": "Triggered_Ceipal__c",
            "Ceipal_Pay_Rate__c": "Ceipal_Pay_Rate__c",
            "Supervisor_Name__c": "Supervisor_Name__c",
            "Hr_Contact__c": "Hr_Contact__c",
            "Batch__c": "Batch__c",
            "Bill_Rate__c": "Bill_Rate__c",
            "Technology__c": "Technology__c",
            "Client_Name__c": "Client_Name__c",
            "Company__c": "Company__c",
            "Implementation_Partner__c": "Implementation_Partner__c",
            "Job_Location__c": "Job_Location__c",
            "PO_End_Dt__c": "PO_End_Dt__c",
            "Verbal_Confirmation_Date__c": "Verbal_Confirmation_Date__c",
            "Payroll_Month__c": "Payroll_Month__c",
            "Supervisor_Name_Share__c": "Supervisor_Name_Share__c",
            "Percentage__c": "Percentage__c",
            "Prime_Vendor_Name__c": "Prime_Vendor_Name__c",
            "Project_End_Date__c": "Project_End_Date__c",
            "Project_Start_Date__c": "Project_Start_Date__c",
            "Recruiter__c": "Recruiter__c",
            "Share_With__c": "Share_With__c",
            "Student__c": "Student__c",
            "Vendor_Compnay_Name__c": "Vendor_Compnay_Name__c",
            "Vendor_Person_Email__c": "Vendor_Person_Email__c",
            "Vendor_Person_Name__c": "Vendor_Person_Name__c",
            "Vendor_Person_Phone__c": "Vendor_Person_Phone__c",
        },
    },
    "Employee__c": {
        "table": "Employee__c",
        "fields": {
            "Id": "Id",
            "OwnerId": "OwnerId",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "Deptment__c": "Deptment__c",
            "Email__c": "Email__c",
            "End_Dt__c": "End_Dt__c",
            "First_Name__c": "First_Name__c",
            "Last_Name__c": "Last_Name__c",
            "OffShoreMgrID__c": "OffShoreMgrID__c",
            "OffshoreFloorManager__c": "OffshoreFloorManager__c",
            "OffshoreLeadId__c": "OffshoreLeadId__c",
            "Offshore_Location__c": "Offshore_Location__c",
            "Onshore_Manager__c": "Onshore_Manager__c",
            "Organization__c": "Organization__c",
            "Phone__c": "Phone__c",
            "User__c": "User__c",
            "Start_Dt__c": "Start_Dt__c",
            "workPhoneNum__c": "workPhoneNum__c",
            "Full_Name__c": "Full_Name__c",
            "Keka_EMP_ID__c": "Keka_EMP_ID__c",
            "Contact__c": "Contact__c",
            "Cluster__c": "Cluster__c",
            "Offshore_Lead_Contact__c": "Offshore_Lead_Contact__c",
            "OffShore_Manager_Contact__c": "OffShore_Manager_Contact__c",
            "Offshore_Floor_Manager_Contact__c": "Offshore_Floor_Manager_Contact__c",
            "OnShore_Manager_Contact__c": "OnShore_Manager_Contact__c",
            "Approval_Status__c": "Approval_Status__c",
            "Org_Account__c": "Org_Account__c",
            "Cluster_Contact__c": "Cluster_Contact__c",
            "Latest_Verbal_Confirmation_Date__c": "Latest_Verbal_Confirmation_Date__c",
            "Total_Number_Of_Students__c": "Total_Number_Of_Students__c",
            "Confirmation_Count__c": "Confirmation_Count__c",
            "Last_Login_Date__c": "Last_Login_Date__c",
            "Cluster_Account__c": "Cluster_Account__c",
            "Offshore_Manager_Lead_Name__c": "Offshore_Manager_Lead_Name__c",
            "Supervisor_Name__c": "Supervisor_Name__c",
            "Organization_Name__c": "Organization_Name__c",
            "Cluster_Name__c": "Cluster_Name__c",
            "Offshore_Floor_Manager__c": "Offshore_Floor_Manager__c",
            "Employee_Duration__c": "Employee_Duration__c",
            "In_Market_Students_Count__c": "In_Market_Students_Count__c",
            "Pre_Marketing_Student_Count__c": "Pre_Marketing_Student_Count__c",
            "BU_Mail__c": "BU_Mail__c",
            "India_HR_Mail__c": "India_HR_Mail__c",
            "Offshore_Manager_Mail__c": "Offshore_Manager_Mail__c",
            "BU_Name__c": "BU_Name__c",
        },
    },
    "BU_Performance__c": {
        "table": "BU_Performance__c",
        "fields": {
            "Id": "Id",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "BU__c": "BU__c",
            "In_Market_Students_Count__c": "In_Market_Students_Count__c",
            "Date__c": "Date__c",
            "Submissions_Count__c": "Submissions_Count__c",
            "Interview_Count__c": "Interview_Count__c",
            "submission__c": "submission__c",
            "Target_Submissions__c": "Target_Submissions__c",
        },
    },
    "BS__c": {
        "table": "BS__c",
        "fields": {
            "Id": "Id",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "Student__c": "Student__c",
            "BU_Name__c": "BU_Name__c",
            "Vendor_Name__c": "Vendor_Name__c",
            "Bill_Rate__c": "Bill_Rate__c",
            "PayRate__c": "PayRate__c",
            "Caluculated_Pay_Rate__c": "Caluculated_Pay_Rate__c",
            "Month__c": "Month__c",
            "Year__c": "Year__c",
            "Hours__c": "Hours__c",
            "Invoice_Amount__c": "Invoice_Amount__c",
            "Actual_Salary__c": "Actual_Salary__c",
            "Salary_Paid__c": "Salary_Paid__c",
            "Insurance__c": "Insurance__c",
            "H1fee__c": "H1fee__c",
            "Other_Amounts__c": "Other_Amounts__c",
            "Pending_Amount__c": "Pending_Amount__c",
            "Payroll_Taxes__c": "Payroll_Taxes__c",
            "Gross_Profit__c": "Gross_Profit__c",
            "DOJ__c": "DOJ__c",
            "Company_Name__c": "Company_Name__c",
            "Payment_Type__c": "Payment_Type__c",
        },
    },
    "Tech_Support__c": {
        "table": "Tech_Support__c",
        "fields": {
            "Id": "Id",
            "OwnerId": "OwnerId",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "Amnt_Per_Call__c": "Amnt_Per_Call__c",
            "Availability__c": "Availability__c",
            "Calendar_URL__c": "Calendar_URL__c",
            "Calls_Per_Day__c": "Calls_Per_Day__c",
            "Confirmtion_amount__c": "Confirmtion_amount__c",
            "Contact_Number2__c": "Contact_Number2__c",
            "Location__c": "Location__c",
            "Name__c": "Name__c",
            "OnSiteMgrID__c": "OnSiteMgrID__c",
            "Payment_Type__c": "Payment_Type__c",
            "Total_Amount__c": "Total_Amount__c",
            "contact_Number1__c": "contact_Number1__c",
            "Account_Details__c": "Account_Details__c",
            "Technology__c": "Technology__c",
            "Priority__c": "Priority__c",
            "Total_Interviews_Count__c": "Total_Interviews_Count__c",
        },
    },
    "New_Student__c": {
        "table": "New_Student__c",
        "fields": {
            "Id": "Id",
            "OwnerId": "OwnerId",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "Manager__c": "Manager__c",
            "Date_Of_Birth__c": "Date_Of_Birth__c",
            "Visa_Status__c": "Visa_Status__c",
            "OPT_STEM_Start_Date__c": "OPT_STEM_Start_Date__c",
            "Interested_Tech__c": "Interested_Tech__c",
            "Phone__c": "Phone__c",
        },
    },
    "Manager_Card__c": {
        "table": "Manager_Card__c",
        "fields": {
            "Id": "Id",
            "OwnerId": "OwnerId",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "Card__c": "Card__c",
            "Manager__c": "Manager__c",
            "Source_Type__c": "Source_Type__c",
        },
    },
    "Cluster__c": {
        "table": "Cluster__c",
        "fields": {
            "Id": "Id",
            "OwnerId": "OwnerId",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "Email__c": "Email__c",
            "US_Number__c": "US_Number__c",
            "India_Number__c": "India_Number__c",
            "Cluster_Aliaz__c": "Cluster_Aliaz__c",
            "Cluster_ID__c": "Cluster_ID__c",
            "IN_Job_Student_Count__c": "IN_Job_Student_Count__c",
        },
    },
    "Organization__c": {
        "table": "Organization__c",
        "fields": {
            "Id": "Id",
            "OwnerId": "OwnerId",
            "Name": "Name",
            "RecordTypeId": "RecordTypeId",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastActivityDate": "LastActivityDate",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "ACCOUNT_NUMBER__c": "ACCOUNT_NUMBER__c",
            "Bank_Name__c": "Bank_Name__c",
            "Bank_Phone_Number__c": "Bank_Phone_Number__c",
            "Cluster__c": "Cluster__c",
            "Contract_Mgr__c": "Contract_Mgr__c",
            "Current_Address__c": "Current_Address__c",
            "DUNS_Number__c": "DUNS_Number__c",
            "Date_Incorporated__c": "Date_Incorporated__c",
            "EIN__c": "EIN__c",
            "E_Verify_Number__c": "E_Verify_Number__c",
            "File_Number__c": "File_Number__c",
            "Major_Shareholder__c": "Major_Shareholder__c",
            "Minor_Shareholder1_DIN_Number__c": "Minor_Shareholder1_DIN_Number__c",
            "Minor_Shareholder1__c": "Minor_Shareholder1__c",
            "Minor_Shareholder1_s_CIN_NUMBER__c": "Minor_Shareholder1_s_CIN_NUMBER__c",
            "Minor_Shareholder1_s_Date_of_Appointment__c": "Minor_Shareholder1_s_Date_of_Appointment__c",
            "Minor_Shareholder2__c": "Minor_Shareholder2__c",
            "Offler_Letter_format_Google_Drive_Link__c": "Offler_Letter_format_Google_Drive_Link__c",
            "Domain__c": "Domain__c",
            "OrgName__c": "OrgName__c",
            "Owner_s_Mail_ID__c": "Owner_s_Mail_ID__c",
            "Phone_Numbers__c": "Phone_Numbers__c",
            "Registered_Address__c": "Registered_Address__c",
            "Routing__c": "Routing__c",
            "Sales_Head__c": "Sales_Head__c",
            "Tax_Payer_Number__c": "Tax_Payer_Number__c",
            "Web_File_Number__c": "Web_File_Number__c",
            "Website__c": "Website__c",
            "Cluster_Account__c": "Cluster_Account__c",
            "ORG_ID__c": "ORG_ID__c",
            "Country_Incorporated__c": "Country_Incorporated__c",
            "Current_Owner__c": "Current_Owner__c",
            "Registered_Date__c": "Registered_Date__c",
            "Registered_Owner__c": "Registered_Owner__c",
            "E_Verify_Login_ID__c": "E_Verify_Login_ID__c",
            "Swift_Code__c": "Swift_Code__c",
            "IFSC_Code__c": "IFSC_Code__c",
            "Owner_Name__c": "Owner_Name__c",
            "Cluster1__c": "Cluster1__c",
        },
    },
    "Pay_Off__c": {
        "table": "Pay_Off__c",
        "fields": {
            "Id": "Id",
            "OwnerId": "OwnerId",
            "Name": "Name",
            "CreatedDate": "CreatedDate",
            "CreatedById": "CreatedById",
            "LastModifiedDate": "LastModifiedDate",
            "LastModifiedById": "LastModifiedById",
            "LastViewedDate": "LastViewedDate",
            "LastReferencedDate": "LastReferencedDate",
            "Cluster__c": "Cluster__c",
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

    table = f'"{mapping["table"]}"'
    fields = mapping["fields"]

    sql = soql

    # Replace object name with quoted table
    sql = re.sub(r'\bFROM\s+' + re.escape(sf_object), f'FROM {table}', sql, flags=re.IGNORECASE)

    # Replace field names with quoted columns (longer names first to avoid partial replacements)
    sorted_fields = sorted(fields.items(), key=lambda x: -len(x[0]))
    for sf_field, pg_col in sorted_fields:
        sql = re.sub(r'\b' + re.escape(sf_field) + r'\b', f'"{pg_col}"', sql)

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
        pattern = r'([\w."]+)\s*=\s*' + re.escape(soql_lit)
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
        pattern = r'([\w."]+)\s*=\s*' + re.escape(soql_lit)
        m = re.search(pattern, sql)
        if m:
            col = m.group(1)
            sql = sql.replace(m.group(0), f"{col}::date = {pg_val}")
        pattern_gte = r'([\w."]+)\s*>=\s*' + re.escape(soql_lit)
        m2 = re.search(pattern_gte, sql)
        if m2:
            col = m2.group(1)
            sql = sql.replace(m2.group(0), f"{col}::date >= {pg_val}")
        pattern_lte = r'([\w."]+)\s*<=\s*' + re.escape(soql_lit)
        m3 = re.search(pattern_lte, sql)
        if m3:
            col = m3.group(1)
            sql = sql.replace(m3.group(0), f"{col}::date <= {pg_val}")

    # Catch-all: replace any remaining bare date literals not caught above
    sql = re.sub(r'\bTODAY\b', 'CURRENT_DATE', sql)
    sql = re.sub(r'\bYESTERDAY\b', "CURRENT_DATE - INTERVAL '1 day'", sql)
    sql = re.sub(r'\bTOMORROW\b', "CURRENT_DATE + INTERVAL '1 day'", sql)

    # Handle COUNT(Id/sf_id) → COUNT(*)
    sql = re.sub(r'COUNT\("?Id"?\)', 'COUNT(*)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'COUNT\("?sf_id"?\)', 'COUNT(*)', sql, flags=re.IGNORECASE)
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
                new_sub = re.sub(r'\bFROM\s+' + re.escape(sub_obj), f'FROM "{sub_mapping["table"]}"', new_sub)
                for sf_f, pg_c in sorted(sub_mapping["fields"].items(), key=lambda x: -len(x[0])):
                    new_sub = re.sub(r'\b' + re.escape(sf_f) + r'\b', f'"{pg_c}"', new_sub)
                sql = sql[:sm.start()] + new_sub + sql[sm.end():]

    # Handle COUNT() as alias 'cnt'
    sql = re.sub(r'COUNT\(\*\)\s+cnt', 'COUNT(*) AS cnt', sql, flags=re.IGNORECASE)
    sql = re.sub(r'AVG\((\w+)\)\s+(\w+)', r'AVG(\1) AS \2', sql, flags=re.IGNORECASE)

    # Clean up any remaining Salesforce-specific syntax
    sql = sql.replace("= null", "IS NULL")
    sql = re.sub(r"!= null", "IS NOT NULL", sql)

    # Fix double-quoting that may occur
    sql = sql.replace('""', '"')

    return sql


# Relationship mappings: object -> { relationship_name -> (target_table, fk_column) }
RELATIONSHIP_MAP = {
    "Student__c": {
        "Manager__r": ("\"Manager__c\"", "\"Manager__c\""),
    },
    "Interviews__c": {
        "Student__r": ("\"Student__c\"", "\"Student__c\""),
    },
    "Submissions__c": {
        "Student__r": ("\"Student__c\"", "\"Student__c\""),
    },
    "Job__c": {
        "Student__r": ("\"Student__c\"", "\"Student__c\""),
        "Share_With__r": ("\"Manager__c\"", "\"Share_With__c\""),
    },
    "Employee__c": {
        "Onshore_Manager__r": ("\"Manager__c\"", "\"Onshore_Manager__c\""),
        "Cluster__r": ("\"Cluster__c\"", "\"Cluster__c\""),
    },
    "BU_Performance__c": {
        "BU__r": ("\"Manager__c\"", "\"BU__c\""),
    },
}


def soql_to_sql_with_joins(soql: str) -> str | None:
    """
    Convert SOQL with __r relationship traversals to PostgreSQL with JOINs.
    Handles patterns like Manager__r.Name, Student__r.Name.
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

    rel_map = RELATIONSHIP_MAP.get(sf_object, {})

    # Find all __r.Field references
    rel_refs = re.findall(r'(\w+__r)\.(\w+)', soql)
    if not rel_refs:
        return soql_to_sql(soql)

    # Check all relationships are known
    needed_joins = {}
    for rel_name, field_name in rel_refs:
        if rel_name not in rel_map:
            return None  # Unknown relationship, can't convert
        needed_joins[rel_name] = rel_map[rel_name]

    table = f'"{mapping["table"]}"'
    sql = soql

    # Build JOIN clauses and replace __r.Field with alias.Field
    join_clauses = []
    for rel_name, (target_table, fk_col) in needed_joins.items():
        alias = rel_name.replace("__r", "_j")
        join_clauses.append(f'LEFT JOIN {target_table} "{alias}" ON {table}."{fk_col}" = "{alias}"."Id"')
        # Replace all rel_name.Field with alias."Field"
        sql = re.sub(
            rf'\b{re.escape(rel_name)}\.(\w+)',
            lambda m: f'"{alias}"."{m.group(1)}"',
            sql
        )

    # Replace FROM object with FROM table + JOINs
    join_sql = " ".join(join_clauses)
    sql = re.sub(
        r'\bFROM\s+' + re.escape(sf_object),
        f'FROM {table} {join_sql}',
        sql, flags=re.IGNORECASE
    )

    # Now handle remaining field replacements (non-relationship fields)
    fields = mapping["fields"]
    sorted_fields = sorted(fields.items(), key=lambda x: -len(x[0]))
    for sf_field, pg_col in sorted_fields:
        # Only replace standalone field references (not already quoted)
        sql = re.sub(r'(?<!")\b' + re.escape(sf_field) + r'\b(?!")', f'{table}."{pg_col}"', sql)

    # Fix: don't double-quote already quoted fields from JOIN replacements
    sql = re.sub(r'""', '"', sql)

    # Handle LAST_N_DAYS:X
    last_n_match = re.findall(r'LAST_N_DAYS:(\d+)', sql)
    for n in last_n_match:
        sql = sql.replace(f'LAST_N_DAYS:{n}', f"CURRENT_DATE - INTERVAL '{n} days'")

    # Handle date range literals
    range_literals = {
        "THIS_WEEK": ("DATE_TRUNC('week', CURRENT_DATE)", "DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '1 week'"),
        "LAST_WEEK": ("DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week'", "DATE_TRUNC('week', CURRENT_DATE)"),
        "THIS_MONTH": ("DATE_TRUNC('month', CURRENT_DATE)", "DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'"),
        "LAST_MONTH": ("DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'", "DATE_TRUNC('month', CURRENT_DATE)"),
        "THIS_YEAR": ("DATE_TRUNC('year', CURRENT_DATE)", "DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year'"),
        "LAST_YEAR": ("DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'", "DATE_TRUNC('year', CURRENT_DATE)"),
    }
    for soql_lit, (start, end) in range_literals.items():
        pattern = r'([\w."]+)\s*=\s*' + re.escape(soql_lit)
        m = re.search(pattern, sql)
        if m:
            col = m.group(1)
            sql = sql.replace(m.group(0), f"{col} >= {start} AND {col} < {end}")

    # Handle simple date literals
    simple_dates = {
        "TODAY": "CURRENT_DATE",
        "YESTERDAY": "CURRENT_DATE - INTERVAL '1 day'",
    }
    for soql_lit, pg_val in simple_dates.items():
        pattern = r'([\w."]+)\s*=\s*' + re.escape(soql_lit)
        m = re.search(pattern, sql)
        if m:
            col = m.group(1)
            sql = sql.replace(m.group(0), f"{col}::date = {pg_val}")

    # Handle >= comparisons with LAST_N_DAYS
    sql = re.sub(r'>= LAST_N_DAYS:(\d+)', lambda m: f">= CURRENT_DATE - INTERVAL '{m.group(1)} days'", sql)

    # Catch-all: replace any remaining bare date literals
    sql = re.sub(r'\bTODAY\b', 'CURRENT_DATE', sql)
    sql = re.sub(r'\bYESTERDAY\b', "CURRENT_DATE - INTERVAL '1 day'", sql)

    # Clean up
    sql = sql.replace("= null", "IS NULL")
    sql = re.sub(r"!= null", "IS NOT NULL", sql)
    sql = re.sub(r'COUNT\(Id\)', 'COUNT(*)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'COUNT\(\)', 'COUNT(*)', sql, flags=re.IGNORECASE)

    # Handle subqueries
    sub_matches = list(re.finditer(r'\(SELECT\s+.+?\)', sql))
    for sm in reversed(sub_matches):
        sub_sql = sm.group(0)
        sub_from_m = re.search(r'FROM\s+(\w+)', sub_sql)
        if sub_from_m:
            sub_obj = sub_from_m.group(1)
            sub_mapping = SF_TO_PG.get(sub_obj)
            if sub_mapping:
                new_sub = sub_sql
                new_sub = re.sub(r'\bFROM\s+' + re.escape(sub_obj), f'FROM "{sub_mapping["table"]}"', new_sub)
                for sf_f, pg_c in sorted(sub_mapping["fields"].items(), key=lambda x: -len(x[0])):
                    new_sub = re.sub(r'\b' + re.escape(sf_f) + r'\b', f'"{pg_c}"', new_sub)
                sql = sql[:sm.start()] + new_sub + sql[sm.end():]

    return sql


async def execute_query(query: str) -> dict:
    """
    Execute query on local PostgreSQL.
    Accepts either direct SQL (from AI) or legacy SOQL (auto-converted).
    """
    # If query already looks like proper PostgreSQL SQL (has double-quoted identifiers), run directly
    if '"' in query and query.strip().upper().startswith("SELECT"):
        logger.info(f"PostgreSQL (direct): {query[:200]}")
        result = await execute_sql(query)
        if "error" in result:
            logger.warning(f"PostgreSQL query failed: {result['error'][:200]}")
        return result

    # Legacy: try SOQL-to-SQL conversion for backward compatibility
    sql = soql_to_sql_with_joins(query) if '__r' in query else soql_to_sql(query)

    if not sql:
        logger.warning(f"Could not convert to SQL: {query[:200]}")
        return {"error": f"Could not convert query to PostgreSQL SQL: {query[:200]}", "records": [], "totalSize": 0}

    logger.info(f"PostgreSQL (converted): {sql[:200]}")
    result = await execute_sql(sql)
    if "error" in result:
        logger.warning(f"PostgreSQL query failed: {result['error'][:200]}")
    return result


