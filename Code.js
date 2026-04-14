/**
 * Code.gs
 * Main entry point and configuration for G2N Request Management System
 * Handles web app routing (doGet), custom menus, workbook connections,
 * audit logging, monthly archive triggers, and form response sync.
 * v3.6 - Phase 4: Added HYGIENE_STATS_WORKBOOK_ID to CONFIG
 * v4.0 - Phase 5: Added LOGIN_FOLDER_ID, ARCHIVES_BACKUPS_FOLDER_ID to CONFIG.
 *         Added archiveHealthCheck() for AM/Products archive integrity.
 *         Added multi-archive rollover support (year-based workbooks).
 * v4.1 - Phase 5 fix: Rollover now splits G2N_Archive by year into separate
 *         workbooks (G2N_Archive_YYYY), each with Archive + Products_Archive sheets.
 *         Removed yearLabel parameter — years auto-detected from Request Date.
 *         Added previewArchiveRollover() for preview before execute.
 *         Existing year workbooks are appended to (not overwritten).
 * v4.2 - Added getArchiveCapacityPct(), checkAndAutoRollover() for auto year-split
 *         when G2N_Archive >= 85% capacity. Added getAdminEmails() and
 *         sendArchiveSummaryEmail() — emails all active admins after scheduled
 *         archive with record counts, file deletions, audit backup, and rollover status.
 * v4.3 - Added createAuditLog() — was called by logAudit() but never defined.
 *         Creates AuditLog sheet with styled headers if missing from AM.
 *         Fixed getSheetHeaders() to trim whitespace — matches all other header reads.
 * v4.4 - Performance: Per-execution caching for getMasterWorkbook(),
 *         getLookupsWorkbook(), getDataWorkbook() — eliminates redundant
 *         SpreadsheetApp.openById() calls (19x for getAllLookups alone).
 *         Optimized getNextId() to read only ID column instead of full sheet.
 *         Added shared utilities: trimHeaders(), htmlDateToSheet().
 *         Consolidated 3 showXxxUrl() functions into showPortalUrlDialog_() helper.
 * v4.5 - Added testScheduledArchive() — prompts for cutoff date in Apps Script
 *         editor UI, runs full archive cycle with email notification for testing.
 * v4.6 - Simplified runScheduledArchive() and testScheduledArchive() — removed
 *         unnecessary while loop since executeArchiveBatch is single-pass.
 *         Added try/catch around getUi() in testScheduledArchive with helpful
 *         error when run outside spreadsheet context.
 * v4.7 - Split testScheduledArchive into two phases: UI prompts save cutoff
 *         to ScriptProperties, then a chained 1-second trigger fires
 *         executeTestArchive() in a fresh 6-minute window. Prevents user
 *         think-time from burning the execution clock.
 * v4.8 - Removed preview/confirm dialogs from testScheduledArchive UI phase
 *         to prevent timeout. Preview now logged in executeTestArchive().
 * v4.9 - Added extractYear(dateVal) — replaces 6+ inline date-year extraction
 *         patterns across archive functions. Added isRowActive(row, activeCol)
 *         — replaces 10+ inline Active column checks. Added
 *         getArchiveWorkbooksForRange(fromDate, toDate) — iterates G2N_Archive
 *         plus year-based G2N_Archive_YYYY workbooks from ARCHIVES_BACKUPS_FOLDER_ID.
 *         Fixes critical gap where getCombinedData() and loadProductLookupData()
 * v5.0 - Moved DISTRIBUTED_PRODUCTS from CONFIG.LOOKUPS to CONFIG.DATA_SHEETS.
 *         Distributed_Products lives in G2N_Data and is only read during
 *         reporting; it should not be in the preloaded lookup cache.
 *         missed rolled-over year archives in grant/AI reports.
 * v4.10 - Adopted extractYear() in performArchiveRollover and previewArchiveRollover
 *          (replaces 4 inline date-year extraction blocks).
 *          Adopted trimHeaders() in archiveHealthCheck, performArchiveRollover,
 *          previewArchiveRollover (replaces 6 inline header trim calls).
 * v4.11 - Added onEdit trigger: clears lookup cache when any sheet
 *          in G2N_Lookups is edited directly (covers manual edits outside portals).
 *          Added Setup menu items for trigger management and manual cache clear.
 * v4.12 - Added CONFIG.TIMEZONE constant; replaces 81 hardcoded 'America/Chicago'
 *          strings across all .gs files (#8). Extracted trimHeaders(), htmlDateToSheet(),
 *          extractYear(), isRowActive() to SharedUtils.gs (#15). Replaced inline
 *          folder-move in archive rollover with shared moveToFolder() (#4).
 *          Updated onLookupSheetEdit() to also clear FieldMapService cache (#16).
 *          Deprecated getSheetHeaders() (#14).
 * v5.2 - Added CONFIG.LOOKUPS.SAVED_REPORTS → 'LU_SavedReports' sheet.
 *         Stores saved Custom Data Sheet specs for API-free re-run.
 * v5.3 - Added CONFIG.DEV_URL for Test Deployment (/dev) environment.
 *         Added Dev Environment submenu with dev portal URL dialogs.
 *         Added showStaffPortalDevUrl(), showAdminPortalDevUrl(),
 *         showIntakeFormDevUrl() functions.
 * v5.4 - Setup submenu now owner-only: compares Session.getActiveUser()
 *         to spreadsheet owner; Setup items hidden from non-owners.
 *         Added "Validate Addresses (Skip HIGH)" and
 *         "Continue Address Validation" to Setup submenu.
 * v5.5 - Added CONFIG.DB ({ USE_MYSQL }), CONFIG.CLOUD_RUN_URL, and
 *         CONFIG.BRIDGE_API_KEY. DB/CLOUD_RUN_URL were referenced by DbService
 *         but missing from CONFIG (USE_MYSQL guards always false → writes went
 *         to AM). BRIDGE_API_KEY replaces OIDC identity token auth — Cloud Run
 *         must allow unauthenticated; key validated by Program.cs middleware.
 * v5.6 - getWebAppUrl() now auto-detects dev vs production deployment using
 *         ScriptApp.getService().getUrl(). Returns CONFIG.DEV_URL when running
 *         under the /dev test deployment, CONFIG.WEB_APP_URL for production.
 *         Fixes PP always opening on production Version 46 when launched from
 *         the dev SV portal — PP now runs in the same environment as its caller.
 * v5.7 - doGet() 'staff' and 'admin' cases: changed from createHtmlOutputFromFile
 *         to createTemplateFromFile and inject serverWebAppUrl = getWebAppUrl().
 *         REVERTED in v5.8 — SV now pre-fetches webAppUrl client-side via
 *         getWebAppUrlForPortal() (same pattern as AP), no template injection needed.
 * v5.8 - doGet() 'staff' and 'admin' reverted to createHtmlOutputFromFile.
 * v5.9 - Restored createTemplateFromFile + serverWebAppUrl for 'staff' case.
 *         'admin' remains createHtmlOutputFromFile (AP fetches URL client-side).
 * v6.0 - One-time data fix macros added under 'One-Time Data Fixes' menu.
 * v6.1 - Macros read source sheets from G2N_Lookups (no external openById).
 *         Test run skips archive scan — only AM searched in dryRun mode.
 * v6.2 - Removed one-time data fix macros from Setup menu (work complete).
 * v6.3 - CONFIG.LOOKUPS: added SAVED_SCHEDULE_ID → 'LU_LastScheduled' (required by
 *         appendLastScheduledId/getLastScheduledId in LookupService) and
 *         REPORT_COLUMNS → 'LU_ReportColumns' (required by ReportColumnService).
 *         Removed One-Time Data Fixes submenu from onOpen() — macros no longer needed.
 * v6.4 - logAudit(): replaced appendRow() with getLastRow()+1 + setValues() per
 *         coding standards. appendRow() holds a spreadsheet-wide write lock which
 *         caused updateArchiveRecord() to deadlock — archive setValues() + AM
 *         appendRow() would both wait for each other's lock, hanging GAS indefinitely
 *         and triggering the SV 45-second save watchdog.
 */

// ============ CONFIGURATION ============
const CONFIG = {
    // Main Data Workbook (Applicants_Master)
    MASTER_WORKBOOK_ID: '1rsAth63Q7lUehaNlFFDOhSK4ljWfzi9kFFCuY6uLqMU',
    MASTER_SHEET: 'Applicants_Master',
    AUDIT_LOG: 'AuditLog',
    TIMEZONE: 'America/Chicago',

    // Lookups Workbook (G2N_Lookups)
    LOOKUPS_WORKBOOK_ID: '1jahhI4JoDntwrAcv_E6B4te8jI6RZkn0eNbnt-pThX0',

    // Data Workbook (G2N_Data) - for PF_Products and future Archive
    DATA_WORKBOOK_ID: '1r4yV9sDfYI2QRqQfAwk8x-V_0TrGhfNghMaZw5XwgwQ',
    PF_PRODUCTS_SHEET: 'DR/PF_Products',

    // Archive Workbook (GEn_Archive)
    ARCHIVE_WORKBOOK_ID: '1v3FbN6uXzD4KTpgvU7Szau0LFF9PeNdlkfWMTu5Q6po',

    // DEPLOYED Web App URL - UPDATE THIS after each new deployment!
    WEB_APP_URL: 'https://script.google.com/macros/s/AKfycbwgD2WubKDAaeoydsjlHiyjT3PPD5irZ0CW4F5e5ftqsA9l5LyXhj0JZRW2qzbBRzV-fQ/exec',

    // DEV (Test Deployment) URL - same script ID, /dev suffix. Owner access only.
    DEV_URL: 'https://script.google.com/macros/s/AKfycbwwcu0UGFlmHlgzdq3Gppv6-MEjGzScY81WS4mpf8Kv/dev',

    // Folder IDs for reports
    REPORTS_FOLDER_ID: '16nOMIDsT59XNN_dxE0SOrQUVm1UL7nGX',
    DISTRIBUTION_FOLDER_ID: '1rsABKl4G5rmFbdWK6L16dtpRla01ezrZ',
    SCHEDULING_FOLDER_ID: '1clZnkQ6K12KWrWqvacDAVQlwhlCfrVU0',
    ARCHIVE_FOLDER_ID: '1D0iRBEpdzlOoLFjQbZmsEJKmYJ0IX5kH',
    GRANTS_FOLDER_ID: '10m8w9hfjNLwZvdRV-Bs6gf2pQ-Vq1S-G',
    LOGIN_FOLDER_ID: '17CLCUucnGYeYYKF6Pse_n0VcJEEcsEy7',
    ARCHIVES_BACKUPS_FOLDER_ID: '1IUXgPfsv1Uxwik5UBOKTPYDqy2q7ffK-',

    // Hygiene Box Distribution Stats workbook (Phase 4B)
    HYGIENE_STATS_WORKBOOK_ID: '1tsqS6vfUBYSnZeJVzU5OCpBnrfmABLCUgDGLemymOQk',

    // Lookup sheet names (in G2N_Lookups workbook)
    LOOKUPS: {
        STAFF: 'LU_Staff',
        STATES: 'LU_States',
        DISTRIB_CODES: 'LU_DistribCodes',
        BOX_CODES: 'LU_BoxCodes',
        PRODUCTS: 'LU_Products',
        SERVICE_STATUS: 'LU_ServiceStatus',
        SERVICE_HOW: 'LU_ServiceHow',
        FUNDING_SOURCES: 'LU_FundingSources',
        LEARNED_HOW: 'LU_LearnedHow',
        PERSON_ORG_REFERRED: 'LU_PersonOrgReferred',
        TITLE_REFERRED: 'LU_TitlePositionReferred',
        MILITARY: 'LU_Military',
        RACE: 'LU_Race',
        ETHNICITY: 'LU_Ethnicity',
        INCOME_LEVELS: 'LU_IncomeLevels',
        INCOME_SOURCES: 'LU_IncomeSources',
        PHONE_TYPE: 'LU_PhoneType',
        ADDRESS_TYPE: 'LU_AddressType',
        SCHED_DISB_CODES: 'LU_SchedDisbCodes',
        EVENT_INFO: 'LU_EventInfo',
        SAVED_REPORTS: 'LU_SavedReports',        // v5.2: Saved Custom Data Sheet specs
        SAVED_SCHEDULE_ID: 'LU_LastScheduled',   // v6.3: Last scheduled ID for Beginning ID auto-fill
        REPORT_COLUMNS: 'LU_ReportColumns',       // v6.3: Report column definitions for grant reports
    },

    // Data sheet names (in G2N_Data workbook — loaded on demand, not preloaded)
    DATA_SHEETS: {
        DISTRIBUTED_PRODUCTS: 'Distributed_Products'
    },

    // MySQL / Cloud Run bridge configuration
    // Set USE_MYSQL: true to route all data operations through the G2N Bridge API
    // instead of reading/writing Google Sheets directly.
    CLOUD_RUN_URL: 'https://g2n-bridge-oa4nu3c2sa-uc.a.run.app',  // Replace with deployed Cloud Run URL
    BRIDGE_API_KEY: '92df8beb2a3e48bfb763c688ca267d375673a11eac5b46d28f10b7435aa96544',                  // Must match Bridge:ApiKey in appsettings.json
    DB: {
        USE_MYSQL: false  // Set to true to activate MySQL path
    }
};

/**
 * Creates the G2N custom menu in the spreadsheet toolbar
 * Adds links to display portal URLs and run diagnostics
 */
function onOpen() {
    const ui = SpreadsheetApp.getUi();
    const menu = ui.createMenu('G2N Management')
        .addItem('Get Staff Portal URL', 'showStaffPortalUrl')
        .addItem('Get Admin Portal URL', 'showAdminPortalUrl')
        .addItem('Get Intake Form URL', 'showIntakeFormUrl')
        .addSeparator();

    // v5.4: Owner-only items — Dev Environment and Setup
    try {
        const ownerEmail = SpreadsheetApp.getActiveSpreadsheet().getOwner().getEmail();
        const userEmail = Session.getActiveUser().getEmail();
        if (userEmail === ownerEmail) {
            menu
                .addSeparator()
                .addSubMenu(ui.createMenu('Dev Environment (Owner Only)')
                    .addItem('Get Staff Portal DEV URL', 'showStaffPortalDevUrl')
                    .addItem('Get Admin Portal DEV URL', 'showAdminPortalDevUrl')
                    .addItem('Get Intake Form DEV URL', 'showIntakeFormDevUrl'))
                .addSeparator()
                .addSubMenu(ui.createMenu('Setup')
                    .addItem('Setup Monthly Archive Trigger', 'setupMonthlyArchiveTrigger')
                    .addItem('Remove Archive Trigger', 'removeArchiveTrigger')
                    .addSeparator()
                    .addItem('Setup Lookup Cache Trigger', 'setupLookupCacheTrigger')
                    .addItem('Remove Lookup Cache Trigger', 'removeLookupCacheTrigger')
                    .addItem('Clear Lookup Cache Now', 'invalidateLookupCache')
                    .addSeparator()
                    .addItem('Validate Addresses (Skip HIGH)', 'validateAddressesMenu')
                    .addItem('Continue Address Validation', 'continueAddressValidationMenu'));
        }
    } catch (e) {
        // Owner check failed (shared spreadsheet context) — skip owner-only items
        Logger.log('onOpen owner check error: ' + e.message);
    }

    menu.addToUi();
}

/**
 * Web app entry point â€” routes to the appropriate portal HTML
 * based on the 'portal' URL parameter.
 * Portals: intake (default), staff, admin, product
 * @param {Object} e - Event object with parameter property
 * @returns {HtmlOutput} The rendered HTML page
 */
function doGet(e) {
    const page = e.parameter.page || 'intake';

    switch (page) {
        case 'staff':
            var staffTemplate = HtmlService.createTemplateFromFile('StaffVolunteerPortalWeb');
            staffTemplate.serverWebAppUrl = getWebAppUrl();
            return staffTemplate.evaluate()
                .setTitle('G2N Staff Portal')
                .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
        case 'admin':
            return HtmlService.createHtmlOutputFromFile('AdminPortalWeb')
                .setTitle('G2N Administrator Portal')
                .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
        case 'product':
            var productTemplate = HtmlService.createTemplateFromFile('ProductPortalWeb');
            productTemplate.serverRecordId = e.parameter.recordId || '';
            productTemplate.serverRequestDate = e.parameter.requestDate || '';
            productTemplate.serverReturnUrl = e.parameter.returnUrl || '';
            return productTemplate.evaluate()
                .setTitle('G2N Product Entry')
                .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
        default:
            var intakeTemplate = HtmlService.createTemplateFromFile('ApplicantIntake');
            intakeTemplate.serverMode = e.parameter.mode || '';
            intakeTemplate.serverReturnPage = e.parameter.returnPage || '';
            intakeTemplate.serverWebAppUrl = CONFIG.WEB_APP_URL;
            return intakeTemplate.evaluate()
                .setTitle('G2N Applicant Intake')
                .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
    }
}

/**
 * Get the deployed web app URL.
 * v5.6: Auto-detects dev vs production using ScriptApp.getService().getUrl().
 * Returns CONFIG.DEV_URL when running under /dev test deployment so that
 * portals launched from dev (e.g. PP from SV) stay in the dev environment
 * and run Head code instead of the published production version.
 */
function getWebAppUrl() {
    try {
        var serviceUrl = ScriptApp.getService().getUrl();
        if (serviceUrl && serviceUrl.endsWith('/dev')) return CONFIG.DEV_URL;
    } catch (e) { /* non-fatal — fall through to production URL */ }
    return CONFIG.WEB_APP_URL;
}

/**
 * Get the Product Portal URL
 */
function getProductPortalUrl() {
    return getWebAppUrl() + '?page=product';
}


/**
 * Shared helper to display a portal URL dialog in the spreadsheet UI
 * Replaces three near-identical showXxxUrl() functions
 * v4.4 - Consolidated from showStaffPortalUrl, showAdminPortalUrl, showIntakeFormUrl
 * @param {string} title - Dialog heading (e.g. 'Staff/Volunteer Portal')
 * @param {string} url - Full URL to display
 * @param {string} color - Primary accent color hex (e.g. '#1a73e8')
 * @param {string} hoverColor - Button hover color hex
 * @param {string} footnote - Footer tip text
 */
function showPortalUrlDialog_(title, url, color, hoverColor, footnote) {
    const html = HtmlService.createHtmlOutput(
        '<html><head><style>' +
        'body{font-family:Arial,sans-serif;padding:20px;text-align:center;}' +
        'h3{color:' + color + ';margin-bottom:15px;}' +
        '.url-box{background:#f1f3f4;padding:15px;border-radius:8px;margin:15px 0;word-break:break-all;font-size:12px;}' +
        'a{color:' + color + ';font-weight:bold;text-decoration:none;}' +
        'a:hover{text-decoration:underline;}' +
        '.btn{display:inline-block;padding:12px 24px;background:' + color + ';color:white;' +
        'border-radius:6px;text-decoration:none;margin-top:15px;font-weight:bold;cursor:pointer;border:none;font-size:14px;}' +
        '.btn:hover{background:' + hoverColor + ';}' +
        'p{color:#666;font-size:13px;margin-top:15px;}' +
        '</style></head><body>' +
        '<h3>' + title + '</h3>' +
        '<div class="url-box"><a href="' + url + '" target="_blank">' + url + '</a></div>' +
        '<button class="btn" onclick="window.open(\'' + url + '\', \'_blank\'); google.script.host.close();">Open ' + title + '</button>' +
        '<p>' + footnote + '</p>' +
        '</body></html>'
    )
        .setWidth(550)
        .setHeight(280);
    SpreadsheetApp.getUi().showModalDialog(html, title + ' URL');
}

/**
 * Displays the deployed web app URL for the Staff/Volunteer portal
 * Called from the custom spreadsheet menu
 */
function showStaffPortalUrl() {
    showPortalUrlDialog_('Staff/Volunteer Portal', getWebAppUrl() + '?page=staff', '#1a73e8', '#1557b0', 'Tip: Bookmark this URL for quick access!');
}

/**
 * Displays the deployed web app URL for the Admin portal
 * Called from the custom spreadsheet menu
 */
function showAdminPortalUrl() {
    showPortalUrlDialog_('Administrator Portal', getWebAppUrl() + '?page=admin', '#ea4335', '#c5221f', 'Tip: Bookmark this URL for quick access!');
}

/**
 * Displays the deployed web app URL for the Applicant Intake portal
 * Called from the custom spreadsheet menu
 */
function showIntakeFormUrl() {
    showPortalUrlDialog_('Applicant Intake Form', getWebAppUrl(), '#34a853', '#2d8f47', 'Share this URL with applicants!');
}

/**
 * Displays the DEV (Test Deployment) URL for the Staff/Volunteer portal
 * Accessible by script owner only — do not share externally
 * v5.3 - Added for dev/test environment access
 */
function showStaffPortalDevUrl() {
    showPortalUrlDialog_('Staff/Volunteer Portal [DEV]', CONFIG.DEV_URL + '?page=staff', '#e37400', '#b35a00', '⚠ DEV only — accessible by script owner account only.');
}

/**
 * Displays the DEV (Test Deployment) URL for the Admin portal
 * Accessible by script owner only — do not share externally
 * v5.3 - Added for dev/test environment access
 */
function showAdminPortalDevUrl() {
    showPortalUrlDialog_('Administrator Portal [DEV]', CONFIG.DEV_URL + '?page=admin', '#e37400', '#b35a00', '⚠ DEV only — accessible by script owner account only.');
}

/**
 * Displays the DEV (Test Deployment) URL for the Applicant Intake portal
 * Accessible by script owner only — do not share externally
 * v5.3 - Added for dev/test environment access
 */
function showIntakeFormDevUrl() {
    showPortalUrlDialog_('Applicant Intake Form [DEV]', CONFIG.DEV_URL, '#e37400', '#b35a00', '⚠ DEV only — accessible by script owner account only.');
}
/**
 * Returns a cached reference to the Applicants_Master workbook
 * Per-execution cache avoids repeated SpreadsheetApp.openById() calls
 * v4.4 - Added per-execution caching
 * @returns {Spreadsheet}
 */
var _masterWbCache = null;
function getMasterWorkbook() {
    if (!_masterWbCache) _masterWbCache = SpreadsheetApp.openById(CONFIG.MASTER_WORKBOOK_ID);
    return _masterWbCache;
}

/**
 * Returns a cached reference to the G2N_Lookups workbook
 * Per-execution cache — critical for getAllLookups() which previously opened 19 times
 * v4.4 - Added per-execution caching
 * @returns {Spreadsheet}
 */
var _lookupsWbCache = null;
function getLookupsWorkbook() {
    if (!_lookupsWbCache) _lookupsWbCache = SpreadsheetApp.openById(CONFIG.LOOKUPS_WORKBOOK_ID);
    return _lookupsWbCache;
}

/**
 * Returns a cached reference to the G2N_Data workbook
 * v4.4 - Added per-execution caching
 * @returns {Spreadsheet}
 */
var _dataWbCache = null;
function getDataWorkbook() {
    if (!_dataWbCache) _dataWbCache = SpreadsheetApp.openById(CONFIG.DATA_WORKBOOK_ID);
    return _dataWbCache;
}

/**
 * Get the Master sheet
 */
function getMasterSheet() {
    return getMasterWorkbook().getSheetByName(CONFIG.MASTER_SHEET);
}

/**
 * Writes an entry to the AuditLog sheet in Applicants_Master
 * @param {string} action - Action type (LOGIN, LOGOUT, UPDATE, CREATE, etc.)
 * @param {string|null} recordId - Associated record ID, or null
 * @param {string} details - Description of the action
 */
function logAudit(action, recordId, details) {
    try {
        const ss = getMasterWorkbook();
        let auditSheet = ss.getSheetByName(CONFIG.AUDIT_LOG);

        if (!auditSheet) {
            createAuditLog();
            auditSheet = ss.getSheetByName(CONFIG.AUDIT_LOG);
        }

        if (auditSheet) {
            const user = Session.getActiveUser().getEmail() || 'System';
            // v6.4: Use getLastRow()+1 + setValues() — appendRow() holds a spreadsheet-wide
            // write lock which caused archive saves to deadlock (archive setValues running
            // concurrently with appendRow on AM). setValues on a new row is lock-free.
            const newRow = auditSheet.getLastRow() + 1;
            auditSheet.getRange(newRow, 1, 1, 5).setValues([[
                new Date(),
                action,
                recordId || '',
                details,
                user
            ]]);
        }
    } catch (e) {
        Logger.log('Audit log error: ' + e.message);
    }
}

/**
 * Creates the AuditLog sheet in Applicants_Master if it doesn't exist
 * Called automatically by logAudit() on first write
 * Headers match the 5-column structure used by logAudit() and backupAndClearAuditLog()
 * v4.3 - Initial creation (was previously referenced but never defined)
 */
function createAuditLog() {
    const ss = getMasterWorkbook();
    let sheet = ss.getSheetByName(CONFIG.AUDIT_LOG);
    if (sheet) return sheet;

    sheet = ss.insertSheet(CONFIG.AUDIT_LOG);
    const headers = ['Timestamp', 'Action', 'Record ID', 'Details', 'User'];
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(1, 1, 1, headers.length)
        .setFontWeight('bold')
        .setBackground('#ea4335')
        .setFontColor('white');
    sheet.setFrozenRows(1);
    sheet.setColumnWidth(1, 160); // Timestamp
    sheet.setColumnWidth(4, 400); // Details
    Logger.log('Created AuditLog sheet in Applicants_Master');
    return sheet;
}

/**
 * Get the next available ID
 * Reads only the ID column (col A) instead of all sheet data for performance
 * v4.4 - Optimized: reads single column instead of getDataRange().getValues()
 */
function getNextId() {
    const sheet = getMasterSheet();
    if (!sheet) return 1;
    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return 1;

    const ids = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
    let maxId = 0;
    for (let i = 0; i < ids.length; i++) {
        const id = parseInt(ids[i][0]);
        if (id > maxId) maxId = id;
    }
    return maxId + 1;
}

/**
 * @deprecated Use trimHeaders(getMasterSheet().getRange(...).getValues()[0]) directly.
 * Retained for backward compatibility with RequestService.getPortalData().
 * Get the actual column headers from the Master sheet
 * Returns trimmed headers for consistent field matching across portals
 * v4.3 - Added .trim() to match all other header reads in the codebase
 */
function getSheetHeaders() {
    const sheet = getMasterSheet();
    if (!sheet) return [];
    return sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0]
        .map(function (h) { return h.toString().trim(); });
}

// ============ SHARED UTILITIES ============
// trimHeaders(), htmlDateToSheet(), extractYear(), isRowActive() moved to SharedUtils.gs v1.0 (#15)

/**
 * Returns an array of archive workbook references relevant to a date range.
 * Includes: G2N_Archive (from ARCHIVE_FOLDER_ID), plus any G2N_Archive_YYYY
 * workbooks from ARCHIVES_BACKUPS_FOLDER_ID whose year overlaps [fromDate..toDate].
 * Used by getCombinedData() and loadProductLookupData() to ensure reports
 * include data from year-based archives created by performArchiveRollover().
 * v4.9 - New shared utility (fixes missing year-archive data in grant/AI reports)
 * @param {Date} fromDate - Range start date
 * @param {Date} toDate - Range end date
 * @returns {Array} Array of { workbook: Spreadsheet, name: string }
 */
function getArchiveWorkbooksForRange(fromDate, toDate) {
    var results = [];

    // 1) Main G2N_Archive workbook
    try {
        if (CONFIG.ARCHIVE_FOLDER_ID) {
            var archiveFolder = DriveApp.getFolderById(CONFIG.ARCHIVE_FOLDER_ID);
            var files = archiveFolder.getFilesByName('G2N_Archive');
            if (files.hasNext()) {
                results.push({
                    workbook: SpreadsheetApp.openById(files.next().getId()),
                    name: 'G2N_Archive'
                });
            }
        }
    } catch (e) {
        Logger.log('getArchiveWorkbooksForRange: G2N_Archive open error: ' + e.message);
    }

    // 2) Year-based G2N_Archive_YYYY workbooks (created by rollover)
    try {
        if (CONFIG.ARCHIVES_BACKUPS_FOLDER_ID) {
            var fromYear = fromDate instanceof Date ? fromDate.getFullYear() : new Date(fromDate).getFullYear();
            var toYear = toDate instanceof Date ? toDate.getFullYear() : new Date(toDate).getFullYear();

            var backupsFolder = DriveApp.getFolderById(CONFIG.ARCHIVES_BACKUPS_FOLDER_ID);
            var bFiles = backupsFolder.getFiles();

            while (bFiles.hasNext()) {
                var file = bFiles.next();
                if (file.getMimeType() !== 'application/vnd.google-apps.spreadsheet') continue;
                var fileName = file.getName();
                var match = fileName.match(/^G2N_Archive_(\d{4})$/);
                if (match) {
                    var fileYear = parseInt(match[1]);
                    if (fileYear >= fromYear && fileYear <= toYear) {
                        results.push({
                            workbook: SpreadsheetApp.openById(file.getId()),
                            name: fileName
                        });
                    }
                }
            }
        }
    } catch (e) {
        Logger.log('getArchiveWorkbooksForRange: year-archive scan error: ' + e.message);
    }

    return results;
}

/**
 * Sets up the monthly archive trigger (1st of each month)
 * Called from Admin Portal; removes any existing archive trigger first
 * @returns {Object} Result with trigger ID
 */
function setupMonthlyArchiveTrigger() {
    // First remove any existing archive triggers
    removeArchiveTrigger();

    // Create new monthly trigger
    ScriptApp.newTrigger('runScheduledArchive')
        .timeBased()
        .onMonthDay(1)
        .atHour(1)
        .create();

    try {
        SpreadsheetApp.getUi().alert('Monthly archive trigger created.\n\nArchive will run automatically at 1:00 AM on the 1st of each month.');
    } catch (e) {
        Logger.log('Monthly archive trigger created');
    }
}

/**
 * Removes the monthly archive trigger if it exists
 * @returns {Object} Result with removal status
 */
function removeArchiveTrigger() {
    const triggers = ScriptApp.getProjectTriggers();
    let removed = 0;

    for (const trigger of triggers) {
        if (trigger.getHandlerFunction() === 'runScheduledArchive') {
            ScriptApp.deleteTrigger(trigger);
            removed++;
        }
    }

    try {
        if (removed > 0) {
            SpreadsheetApp.getUi().alert('Archive trigger removed.');
        } else {
            SpreadsheetApp.getUi().alert('No archive trigger found.');
        }
    } catch (e) {
        Logger.log('Removed ' + removed + ' archive triggers');
    }
}

// ============ LOOKUP CACHE TRIGGER ============

/**
 * Creates an installable onEdit trigger on G2N_Lookups workbook.
 * When any cell in G2N_Lookups is edited directly (outside portals),
 * onLookupSheetEdit fires and clears the CacheService lookup cache.
 * Run once from G2N Management > Setup > Setup Lookup Cache Trigger.
 * v4.11 - New function
 */
function setupLookupCacheTrigger() {
    removeLookupCacheTrigger();

    ScriptApp.newTrigger('onLookupSheetEdit')
        .forSpreadsheet(CONFIG.LOOKUPS_WORKBOOK_ID)
        .onEdit()
        .create();

    try {
        SpreadsheetApp.getUi().alert(
            'Lookup cache trigger created.\n\n' +
            'The lookup cache will now auto-clear when G2N_Lookups is edited directly.\n' +
            'Portal-based edits already clear the cache automatically.'
        );
    } catch (e) {
        Logger.log('Lookup cache trigger created for G2N_Lookups');
    }
}

/**
 * Removes the installable onEdit trigger for G2N_Lookups if it exists.
 * v4.11 - New function
 */
function removeLookupCacheTrigger() {
    var triggers = ScriptApp.getProjectTriggers();
    var removed = 0;

    for (var i = 0; i < triggers.length; i++) {
        if (triggers[i].getHandlerFunction() === 'onLookupSheetEdit') {
            ScriptApp.deleteTrigger(triggers[i]);
            removed++;
        }
    }

    try {
        if (removed > 0) {
            SpreadsheetApp.getUi().alert('Lookup cache trigger removed.');
        } else {
            SpreadsheetApp.getUi().alert('No lookup cache trigger found.');
        }
    } catch (e) {
        Logger.log('Removed ' + removed + ' lookup cache triggers');
    }
}

/**
 * Installable onEdit handler for G2N_Lookups workbook.
 * Fires when any cell is edited directly in the Lookups workbook,
 * clearing the CacheService lookup cache and FieldMapService cache so portals get fresh data.
 * v4.11 - New function
 * v4.12 - Added clearFieldMapCache() call (#16)
 * @param {Object} e - Edit event object (unused but required by trigger)
 */
function onLookupSheetEdit(e) {
    invalidateLookupCache();
    clearFieldMapCache();
}

/**
 * TEST: Interactive archive test — Phase 1: UI prompt only.
 * Prompts for cutoff date, saves it to ScriptProperties, then creates a
 * 1-second trigger to run executeTestArchive() in a fresh 6-minute window.
 * Keep this function as lightweight as possible — no preview, no confirmation
 * dialog — to avoid burning clock time on user think-time.
 * Results appear in Executions log and admin notification email.
 * IMPORTANT: Must be run from Apps Script editor with an active spreadsheet.
 * v4.5 - Initial creation
 * v4.6 - Removed unnecessary while loop.
 * v4.7 - Split into two phases via chained trigger.
 * v4.8 - Removed preview/confirm from UI phase to prevent timeout.
 *         Preview info now logged in executeTestArchive() instead.
 */
function testScheduledArchive() {
    var ui;
    try {
        ui = SpreadsheetApp.getUi();
    } catch (e) {
        Logger.log('ERROR: testScheduledArchive must be run from the Apps Script editor ' +
            'while the Applicants_Master spreadsheet is open. ' +
            'Open the spreadsheet first, then go to Extensions → Apps Script → Run.');
        return;
    }

    // Prompt for cutoff date
    var dateResponse = ui.prompt(
        'Test Archive — Cutoff Date',
        'Enter the cutoff date (YYYY-MM-DD format).\n' +
        'Records with Request Date BEFORE this date will be archived.\n\n' +
        'Example: 2024-03-01',
        ui.ButtonSet.OK_CANCEL
    );
    if (dateResponse.getSelectedButton() !== ui.Button.OK) {
        Logger.log('Test archive cancelled by user.');
        return;
    }
    var cutoffStr = dateResponse.getResponseText().trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(cutoffStr)) {
        ui.alert('Invalid date format. Please use YYYY-MM-DD.');
        return;
    }

    // Save cutoff and schedule execution in a fresh 6-minute window
    PropertiesService.getScriptProperties().setProperty('TEST_ARCHIVE_CUTOFF', cutoffStr);

    // Remove any previous test archive triggers
    var triggers = ScriptApp.getProjectTriggers();
    for (var i = 0; i < triggers.length; i++) {
        if (triggers[i].getHandlerFunction() === 'executeTestArchive') {
            ScriptApp.deleteTrigger(triggers[i]);
        }
    }

    // Fire in ~1 second — gets its own 6-minute execution window
    ScriptApp.newTrigger('executeTestArchive')
        .timeBased()
        .after(1000)
        .create();

    ui.alert('Test archive scheduled for cutoff ' + cutoffStr + '.\n\n' +
        'Execution starts in ~1 second in a separate process.\n' +
        'Check Apps Script → Executions for progress.\n' +
        'Admin email will be sent when complete.');
    Logger.log('Test archive trigger created for cutoff: ' + cutoffStr);
}

/**
 * TEST: Interactive archive test — Phase 2: Execution.
 * Called by chained trigger from testScheduledArchive(). Reads cutoff date
 * from ScriptProperties, runs archive batch, audit backup, auto-rollover,
 * and sends admin notification email. Self-cleans trigger when done.
 * v4.7 - Extracted from testScheduledArchive for separate execution window.
 * v4.8 - Added preview logging before execution.
 */
function executeTestArchive() {
    // Clean up the one-shot trigger
    var triggers = ScriptApp.getProjectTriggers();
    for (var i = 0; i < triggers.length; i++) {
        if (triggers[i].getHandlerFunction() === 'executeTestArchive') {
            ScriptApp.deleteTrigger(triggers[i]);
        }
    }

    var props = PropertiesService.getScriptProperties();
    var cutoffStr = props.getProperty('TEST_ARCHIVE_CUTOFF');
    props.deleteProperty('TEST_ARCHIVE_CUTOFF');

    if (!cutoffStr) {
        Logger.log('executeTestArchive: No cutoff date found in ScriptProperties. Aborting.');
        return;
    }

    Logger.log('=== TEST ARCHIVE START (cutoff: ' + cutoffStr + ') ===');

    // Log preview info (moved here from UI phase to save clock time)
    try {
        var preview = previewArchive(cutoffStr);
        if (preview.success) {
            Logger.log('Preview: ' + (preview.recordCount || 0) + ' records to archive, ' + (preview.fileCount || 0) + ' files to delete');
        }
    } catch (previewErr) {
        Logger.log('Preview skipped: ' + previewErr.message);
    }

    var emailSummary = { archivedRecords: 0, deletedFiles: 0, auditBackedUp: 0, rolloverResult: null, errors: [] };

    try {
        // Single-pass archive batch
        var result = executeArchiveBatch(cutoffStr, 0);

        if (!result.success) {
            Logger.log('Archive batch failed: ' + result.error);
            emailSummary.errors.push('Archive batch failed: ' + result.error);
        } else {
            emailSummary.archivedRecords = result.archivedInBatch || 0;
            emailSummary.deletedFiles = result.deletedInBatch || 0;
            if (result.rolloverResult) {
                emailSummary.rolloverResult = result.rolloverResult;
            }
            Logger.log('Archived ' + emailSummary.archivedRecords + ' records, deleted ' + emailSummary.deletedFiles + ' files');
        }

        // Audit backup
        var auditResult = backupAndClearAuditLog();
        if (auditResult.success) {
            emailSummary.auditBackedUp = auditResult.backedUp || 0;
        } else {
            emailSummary.errors.push('AuditLog backup failed: ' + auditResult.error);
        }

        // Auto-rollover check (if not already triggered inside executeArchiveBatch)
        if (!emailSummary.rolloverResult) {
            var rolloverResult = checkAndAutoRollover();
            if (rolloverResult) {
                emailSummary.rolloverResult = rolloverResult;
            }
        }

        // Send admin notification email
        sendArchiveSummaryEmail(emailSummary);

        Logger.log('=== TEST ARCHIVE COMPLETE ===');
        Logger.log(JSON.stringify(emailSummary, null, 2));
        logAudit('TEST_ARCHIVE', null, 'Test archive complete (cutoff: ' + cutoffStr + '): ' +
            emailSummary.archivedRecords + ' archived, ' + emailSummary.deletedFiles + ' files deleted');

    } catch (e) {
        Logger.log('executeTestArchive error: ' + e.message);
        logAudit('TEST_ARCHIVE_ERROR', null, 'Test archive failed: ' + e.message);
    }
}

/**
 * Scheduled archive function — called by monthly time trigger.
 * Uses 12 months for record cutoff, 3 months for file deletion.
 * Single-pass: executeArchiveBatch handles all records in one bulk operation.
 * v4.2 - Initial creation with admin email notification
 * v4.6 - Removed unnecessary while loop (executeArchiveBatch is single-pass).
 */
function runScheduledArchive() {
    Logger.log('Starting scheduled archive at ' + new Date());

    var emailSummary = { archivedRecords: 0, deletedFiles: 0, auditBackedUp: 0, rolloverResult: null, errors: [] };

    try {
        // Calculate 12 months ago for record cutoff
        var recordCutoff = new Date();
        recordCutoff.setMonth(recordCutoff.getMonth() - 12);
        var cutoffStr = recordCutoff.toISOString().split('T')[0]; // YYYY-MM-DD format

        // Single-pass archive batch
        var result = executeArchiveBatch(cutoffStr, 0);

        if (!result.success) {
            Logger.log('Archive batch failed: ' + result.error);
            logAudit('ARCHIVE_ERROR', null, 'Scheduled archive failed: ' + result.error);
            emailSummary.errors.push('Archive batch failed: ' + result.error);
        } else {
            emailSummary.archivedRecords = result.archivedInBatch || 0;
            emailSummary.deletedFiles = result.deletedInBatch || 0;
            if (result.rolloverResult) {
                emailSummary.rolloverResult = result.rolloverResult;
            }
            Logger.log('Archived ' + emailSummary.archivedRecords + ' records, deleted ' + emailSummary.deletedFiles + ' files');
        }

        // Backup and clear audit log after archive completes
        var auditResult = backupAndClearAuditLog();
        if (auditResult.success) {
            Logger.log('Audit log backup: ' + auditResult.backedUp + ' entries');
            emailSummary.auditBackedUp = auditResult.backedUp || 0;
        } else {
            Logger.log('Audit log backup failed: ' + auditResult.error);
            emailSummary.errors.push('AuditLog backup failed: ' + auditResult.error);
        }

        Logger.log('Scheduled archive complete: ' + emailSummary.archivedRecords + ' records archived, ' + emailSummary.deletedFiles + ' files deleted');

        // Auto-rollover if G2N_Archive capacity >= 85% (if not already triggered inside executeArchiveBatch)
        if (!emailSummary.rolloverResult) {
            var rolloverResult = checkAndAutoRollover();
            if (rolloverResult) {
                emailSummary.rolloverResult = rolloverResult;
                Logger.log('Auto-rollover result: ' + (rolloverResult.success ? rolloverResult.message : rolloverResult.error));
            }
        }

    } catch (error) {
        Logger.log('Scheduled archive error: ' + error.message);
        logAudit('ARCHIVE_ERROR', null, 'Scheduled archive error: ' + error.message);
        emailSummary.errors.push('Scheduled archive error: ' + error.message);
    }

    // Send summary email to administrators
    try {
        sendArchiveSummaryEmail(emailSummary);
    } catch (mailError) {
        Logger.log('Failed to send archive summary email: ' + mailError.message);
    }
}

/**
 * Backup AuditLog to G2N_Archive workbook and clear from Applicants_Master.
 * Called as part of archive process.
 * v4.8 - Uses Sheets.Spreadsheets.Values.batchUpdate to write header + data rows in
 *         one API call when creating a new sheet. SpreadsheetApp.flush() after data
 *         write commits before clearing the source.
 */
function backupAndClearAuditLog() {
    const ARCHIVE_WORKBOOK_NAME = 'G2N_Archive';
    const AUDIT_BACKUP_SHEET = 'AuditLog_Backup';

    try {
        const masterSS = getMasterWorkbook();
        const auditSheet = masterSS.getSheetByName(CONFIG.AUDIT_LOG);

        if (!auditSheet) {
            return { success: true, message: 'No AuditLog to backup', backedUp: 0 };
        }

        const data = auditSheet.getDataRange().getValues();
        if (data.length <= 1) {
            return { success: true, message: 'AuditLog is empty', backedUp: 0 };
        }

        // Get or find archive workbook
        const archiveFolderId = CONFIG.ARCHIVE_FOLDER_ID;
        if (!archiveFolderId) {
            return { success: false, error: 'Archive folder not configured' };
        }

        const archiveFolder = DriveApp.getFolderById(archiveFolderId);
        const files = archiveFolder.getFilesByName(ARCHIVE_WORKBOOK_NAME);

        let archiveWorkbook;
        if (files.hasNext()) {
            archiveWorkbook = SpreadsheetApp.openById(files.next().getId());
        } else {
            return { success: false, error: 'G2N_Archive workbook not found. Run archive first.' };
        }

        const dataRows = data.slice(1);

        // Get or create AuditLog_Backup sheet
        let backupSheet = archiveWorkbook.getSheetByName(AUDIT_BACKUP_SHEET);
        if (!backupSheet) {
            backupSheet = archiveWorkbook.insertSheet(AUDIT_BACKUP_SHEET);
            backupSheet.getRange(1, 1, 1, data[0].length)
                .setFontWeight('bold').setBackground('#ea4335').setFontColor('white');
            backupSheet.setFrozenRows(1);

            // batchUpdate: write header + all data rows in a single API call
            if (dataRows.length > 0) {
                var numCols = data[0].length;
                var lastDataRow = 1 + dataRows.length;
                Sheets.Spreadsheets.Values.batchUpdate({
                    valueInputOption: 'RAW',
                    data: [
                        {
                            range: AUDIT_BACKUP_SHEET + '!A1:' + columnLetter_(numCols) + (1 + dataRows.length),
                            values: [data[0]].concat(dataRows)
                        }
                    ]
                }, archiveWorkbook.getId());
            }
        } else {
            // Existing sheet — append data rows only (already one setValues call, no batchUpdate needed)
            if (dataRows.length > 0) {
                const lastRow = backupSheet.getLastRow();
                backupSheet.getRange(lastRow + 1, 1, dataRows.length, dataRows[0].length).setValues(dataRows);
            }
        }

        // Flush to commit backup writes before clearing source
        SpreadsheetApp.flush();

        // Clear AuditLog in master (keep header)
        if (auditSheet.getLastRow() > 1) {
            auditSheet.deleteRows(2, auditSheet.getLastRow() - 1);
        }

        logAudit('AUDIT_BACKUP', null, 'Backed up and cleared ' + dataRows.length + ' audit log entries');

        return { success: true, backedUp: dataRows.length };

    } catch (error) {
        Logger.log('Audit backup error: ' + error.message);
        return { success: false, error: error.message };
    }
}

/**
 * Converts a 1-based column number to an A1-notation column letter (e.g. 27 → AA).
 * Used for building range strings for Sheets Advanced Service calls.
 * @param {number} n - Column number (1-based)
 * @returns {string} Column letter(s)
 */
function columnLetter_(n) {
    var s = '';
    while (n > 0) {
        n--;
        s = String.fromCharCode(65 + (n % 26)) + s;
        n = Math.floor(n / 26);
    }
    return s;
}

/**
 * Calculates current G2N_Archive capacity as a percentage of the 10M cell limit.
 * @returns {Object} { capacityPct, totalRows, estimatedCells }
 */
function getArchiveCapacityPct() {
    try {
        var archiveFolder = DriveApp.getFolderById(CONFIG.ARCHIVE_FOLDER_ID);
        var files = archiveFolder.getFilesByName('G2N_Archive');
        if (!files.hasNext()) return { capacityPct: 0, totalRows: 0, estimatedCells: 0 };

        var archiveWB = SpreadsheetApp.openById(files.next().getId());
        var sheets = archiveWB.getSheets();
        var totalRows = 0;
        for (var s = 0; s < sheets.length; s++) {
            totalRows += Math.max(0, sheets[s].getLastRow() - 1);
        }
        var estimatedCols = 70;
        var estimatedCells = totalRows * estimatedCols;
        var capacityPct = Math.round((estimatedCells / 10000000) * 100);
        return { capacityPct: capacityPct, totalRows: totalRows, estimatedCells: estimatedCells };
    } catch (e) {
        Logger.log('getArchiveCapacityPct error: ' + e.message);
        return { capacityPct: 0, totalRows: 0, estimatedCells: 0 };
    }
}

/**
 * Checks G2N_Archive capacity and auto-triggers year-based rollover if >= 85%.
 * Called after archive execution (scheduled or manual).
 * @returns {Object|null} Rollover result if triggered, null otherwise
 */
function checkAndAutoRollover() {
    try {
        if (!CONFIG.ARCHIVES_BACKUPS_FOLDER_ID) {
            Logger.log('Auto-rollover skipped: ARCHIVES_BACKUPS_FOLDER_ID not configured');
            return null;
        }
        var capacity = getArchiveCapacityPct();
        Logger.log('Archive capacity check: ' + capacity.capacityPct + '% (' + capacity.totalRows + ' rows)');

        if (capacity.capacityPct >= 85) {
            Logger.log('Capacity >= 85%, triggering auto-rollover...');
            logAudit('AUTO_ROLLOVER', null, 'Archive at ' + capacity.capacityPct + '% capacity — auto-rollover triggered');
            var result = performArchiveRollover();
            if (result.success) {
                Logger.log('Auto-rollover complete: ' + result.message);
            } else {
                Logger.log('Auto-rollover failed: ' + result.error);
                logAudit('AUTO_ROLLOVER_ERROR', null, 'Auto-rollover failed: ' + result.error);
            }
            result.autoTriggered = true;
            result.capacityPct = capacity.capacityPct;
            return result;
        }
        return null;
    } catch (e) {
        Logger.log('checkAndAutoRollover error: ' + e.message);
        return null;
    }
}

/**
 * Retrieves email addresses of all active Administrators from LU_Staff.
 * Requires an "Email" column in the LU_Staff sheet.
 * @returns {string[]} Array of admin email addresses
 */
function getAdminEmails() {
    try {
        var result = getStaffRecords(false); // Active only
        if (!result.success || !result.records) return [];
        var emails = [];
        for (var i = 0; i < result.records.length; i++) {
            var rec = result.records[i];
            if (rec.Role === 'Administrator' && rec.Email && rec.Email.trim() !== '') {
                emails.push(rec.Email.trim());
            }
        }
        return emails;
    } catch (e) {
        Logger.log('getAdminEmails error: ' + e.message);
        return [];
    }
}

/**
 * Sends an archive summary email to all active administrators.
 * Called at the end of runScheduledArchive() with archive results.
 * @param {Object} summary - { archivedRecords, deletedFiles, auditBackedUp, rolloverResult, errors[] }
 */
function sendArchiveSummaryEmail(summary) {
    try {
        var adminEmails = getAdminEmails();
        if (adminEmails.length === 0) {
            Logger.log('No admin emails configured — archive summary email not sent');
            return;
        }

        var runDate = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'EEEE, MMMM d, yyyy \'at\' h:mm a');
        var subject = 'G2N Archive Summary — ' + Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yyyy');

        // Build HTML email body
        var html = '<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">';
        html += '<div style="background:#1a73e8;color:white;padding:16px 20px;border-radius:8px 8px 0 0;">';
        html += '<h2 style="margin:0;font-size:18px;">G2N Archive Summary</h2>';
        html += '<p style="margin:4px 0 0;font-size:13px;opacity:0.9;">' + runDate + '</p>';
        html += '</div>';
        html += '<div style="border:1px solid #dadce0;border-top:none;padding:20px;border-radius:0 0 8px 8px;">';

        // Records section
        html += '<h3 style="color:#1a73e8;margin:0 0 12px;font-size:15px;">📦 Archive Results</h3>';
        html += '<table style="width:100%;border-collapse:collapse;margin-bottom:16px;">';
        html += '<tr><td style="padding:6px 12px;border-bottom:1px solid #f1f3f4;">AM Records Archived</td>';
        html += '<td style="padding:6px 12px;border-bottom:1px solid #f1f3f4;text-align:right;font-weight:bold;">' + (summary.archivedRecords || 0) + '</td></tr>';
        html += '<tr><td style="padding:6px 12px;border-bottom:1px solid #f1f3f4;">Report Files Deleted (>3 months)</td>';
        html += '<td style="padding:6px 12px;border-bottom:1px solid #f1f3f4;text-align:right;font-weight:bold;">' + (summary.deletedFiles || 0) + '</td></tr>';
        html += '<tr><td style="padding:6px 12px;border-bottom:1px solid #f1f3f4;">AuditLog Entries Backed Up</td>';
        html += '<td style="padding:6px 12px;border-bottom:1px solid #f1f3f4;text-align:right;font-weight:bold;">' + (summary.auditBackedUp || 0) + '</td></tr>';
        html += '</table>';

        // Auto-rollover section (if triggered)
        if (summary.rolloverResult) {
            var rr = summary.rolloverResult;
            if (rr.success) {
                html += '<h3 style="color:#e67e22;margin:0 0 12px;font-size:15px;">🔄 Auto-Rollover Triggered (' + rr.capacityPct + '% capacity)</h3>';
                html += '<p style="margin:0 0 8px;">' + rr.message + '</p>';
                if (rr.yearDetails && rr.yearDetails.length > 0) {
                    html += '<table style="width:100%;border-collapse:collapse;margin-bottom:16px;">';
                    html += '<tr style="background:#f8f9fa;"><th style="padding:6px 12px;text-align:left;border-bottom:1px solid #dadce0;">Year</th>';
                    html += '<th style="padding:6px 12px;text-align:right;border-bottom:1px solid #dadce0;">AM Rows</th>';
                    html += '<th style="padding:6px 12px;text-align:right;border-bottom:1px solid #dadce0;">Product Rows</th></tr>';
                    for (var y = 0; y < rr.yearDetails.length; y++) {
                        var yd = rr.yearDetails[y];
                        html += '<tr><td style="padding:6px 12px;border-bottom:1px solid #f1f3f4;">' + yd.year + '</td>';
                        html += '<td style="padding:6px 12px;border-bottom:1px solid #f1f3f4;text-align:right;">' + yd.amRows + '</td>';
                        html += '<td style="padding:6px 12px;border-bottom:1px solid #f1f3f4;text-align:right;">' + yd.prodRows + '</td></tr>';
                    }
                    html += '</table>';
                }
            } else {
                html += '<h3 style="color:#ea4335;margin:0 0 12px;font-size:15px;">⚠️ Auto-Rollover Failed (' + rr.capacityPct + '% capacity)</h3>';
                html += '<p style="margin:0 0 16px;color:#ea4335;">' + rr.error + '</p>';
            }
        }

        // Errors section
        if (summary.errors && summary.errors.length > 0) {
            html += '<h3 style="color:#ea4335;margin:0 0 12px;font-size:15px;">⚠️ Errors</h3>';
            html += '<ul style="margin:0 0 16px;padding-left:20px;">';
            for (var e = 0; e < summary.errors.length; e++) {
                html += '<li style="color:#ea4335;margin-bottom:4px;">' + summary.errors[e] + '</li>';
            }
            html += '</ul>';
        }

        // Footer
        html += '<hr style="border:none;border-top:1px solid #f1f3f4;margin:16px 0;">';
        html += '<p style="color:#80868b;font-size:12px;margin:0;">This is an automated message from the G2N Request Management System.</p>';
        html += '</div></div>';

        // Send to all admin emails
        for (var a = 0; a < adminEmails.length; a++) {
            try {
                MailApp.sendEmail({
                    to: adminEmails[a],
                    subject: subject,
                    htmlBody: html
                });
                Logger.log('Archive summary emailed to: ' + adminEmails[a]);
            } catch (mailErr) {
                Logger.log('Failed to send archive email to ' + adminEmails[a] + ': ' + mailErr.message);
            }
        }

    } catch (e) {
        Logger.log('sendArchiveSummaryEmail error: ' + e.message);
    }
}

/**
 * Performs a health check on archive workbooks
 * Reports row counts, date ranges, and integrity for AM and DR/PF Products archives
 * @returns {Object} { success, amArchive: { rows, minDate, maxDate }, prodArchive: { rows, minDate, maxDate }, warnings[] }
 */
function archiveHealthCheck() {
    try {
        var warnings = [];
        var amArchive = { rows: 0, minDate: '', maxDate: '', sheetFound: false };
        var prodArchive = { rows: 0, minDate: '', maxDate: '', sheetFound: false };
        var archiveWorkbookName = '';
        var archiveWorkbookRows = 0;

        // Find the G2N_Archive workbook(s)
        var archiveFolderId = CONFIG.ARCHIVE_FOLDER_ID;
        if (!archiveFolderId) {
            return { success: false, error: 'Archive folder not configured (ARCHIVE_FOLDER_ID)' };
        }

        var archiveFolder = DriveApp.getFolderById(archiveFolderId);
        var files = archiveFolder.getFiles();
        var archiveWorkbooks = [];

        while (files.hasNext()) {
            var file = files.next();
            if (file.getMimeType() === 'application/vnd.google-apps.spreadsheet') {
                archiveWorkbooks.push({ id: file.getId(), name: file.getName() });
            }
        }

        if (archiveWorkbooks.length === 0) {
            return { success: true, amArchive: amArchive, prodArchive: prodArchive, warnings: ['No archive workbooks found in archive folder.'], workbookCount: 0 };
        }

        // Also check Archives_Backups folder for year-based rollovers
        var backupWorkbooks = [];
        if (CONFIG.ARCHIVES_BACKUPS_FOLDER_ID) {
            try {
                var backupFolder = DriveApp.getFolderById(CONFIG.ARCHIVES_BACKUPS_FOLDER_ID);
                var bFiles = backupFolder.getFiles();
                while (bFiles.hasNext()) {
                    var bf = bFiles.next();
                    if (bf.getMimeType() === 'application/vnd.google-apps.spreadsheet') {
                        backupWorkbooks.push({ id: bf.getId(), name: bf.getName() });
                    }
                }
            } catch (e) {
                warnings.push('Could not access Archives_Backups folder: ' + e.message);
            }
        }

        // Analyze main archive workbook (first one found, should be G2N_Archive)
        var mainArchive = archiveWorkbooks.find(function (wb) { return wb.name === 'G2N_Archive'; }) || archiveWorkbooks[0];
        archiveWorkbookName = mainArchive.name;

        var archiveWB = SpreadsheetApp.openById(mainArchive.id);

        // Check Archive sheet (AM records)
        var archiveSheet = archiveWB.getSheetByName('Archive');
        if (archiveSheet && archiveSheet.getLastRow() > 1) {
            amArchive.sheetFound = true;
            var amData = archiveSheet.getDataRange().getValues();
            var amHeaders = trimHeaders(amData[0]);
            var reqDateCol = amHeaders.indexOf('Request Date');
            amArchive.rows = amData.length - 1;
            archiveWorkbookRows += amArchive.rows;

            if (reqDateCol !== -1) {
                var dates = [];
                for (var i = 1; i < amData.length; i++) {
                    var d = amData[i][reqDateCol];
                    if (d instanceof Date && !isNaN(d.getTime())) {
                        dates.push(d);
                    } else if (d) {
                        var pd = new Date(d);
                        if (!isNaN(pd.getTime())) dates.push(pd);
                    }
                }
                if (dates.length > 0) {
                    dates.sort(function (a, b) { return a - b; });
                    amArchive.minDate = Utilities.formatDate(dates[0], CONFIG.TIMEZONE, 'M/d/yyyy');
                    amArchive.maxDate = Utilities.formatDate(dates[dates.length - 1], CONFIG.TIMEZONE, 'M/d/yyyy');
                }
            }
        } else {
            warnings.push('Archive sheet not found or empty in ' + archiveWorkbookName);
        }

        // Check Products_Archive sheet
        var prodArchiveSheet = archiveWB.getSheetByName('Products_Archive');
        if (prodArchiveSheet && prodArchiveSheet.getLastRow() > 1) {
            prodArchive.sheetFound = true;
            var prodData = prodArchiveSheet.getDataRange().getValues();
            var prodHeaders = trimHeaders(prodData[0]);
            var prodReqDateCol = prodHeaders.indexOf('Request Date');
            if (prodReqDateCol === -1) prodReqDateCol = prodHeaders.indexOf('RequestDate');
            prodArchive.rows = prodData.length - 1;
            archiveWorkbookRows += prodArchive.rows;

            if (prodReqDateCol !== -1) {
                var pDates = [];
                for (var p = 1; p < prodData.length; p++) {
                    var pd2 = prodData[p][prodReqDateCol];
                    if (pd2 instanceof Date && !isNaN(pd2.getTime())) {
                        pDates.push(pd2);
                    } else if (pd2) {
                        var ppd = new Date(pd2);
                        if (!isNaN(ppd.getTime())) pDates.push(ppd);
                    }
                }
                if (pDates.length > 0) {
                    pDates.sort(function (a, b) { return a - b; });
                    prodArchive.minDate = Utilities.formatDate(pDates[0], CONFIG.TIMEZONE, 'M/d/yyyy');
                    prodArchive.maxDate = Utilities.formatDate(pDates[pDates.length - 1], CONFIG.TIMEZONE, 'M/d/yyyy');
                }
            }
        }

        // Check AuditLog_Backup
        var auditBackupSheet = archiveWB.getSheetByName('AuditLog_Backup');
        var auditBackupRows = 0;
        if (auditBackupSheet && auditBackupSheet.getLastRow() > 1) {
            auditBackupRows = auditBackupSheet.getLastRow() - 1;
            archiveWorkbookRows += auditBackupRows;
        }

        // Capacity warning (10 million cells ≈ rows × cols)
        var estimatedCols = 70; // typical AM column count
        var estimatedCells = archiveWorkbookRows * estimatedCols;
        var capacityPct = Math.round((estimatedCells / 10000000) * 100);
        if (capacityPct > 70) {
            warnings.push('Archive workbook at ~' + capacityPct + '% capacity (' + archiveWorkbookRows + ' total rows). Consider year-based rollover.');
        }

        // Current AM row count for context
        var masterSheet = getMasterSheet();
        var masterRows = masterSheet ? Math.max(0, masterSheet.getLastRow() - 1) : 0;

        return {
            success: true,
            archiveWorkbook: archiveWorkbookName,
            amArchive: amArchive,
            prodArchive: prodArchive,
            auditBackupRows: auditBackupRows,
            archiveWorkbookRows: archiveWorkbookRows,
            capacityPct: capacityPct,
            masterRows: masterRows,
            backupWorkbookCount: backupWorkbooks.length,
            backupWorkbooks: backupWorkbooks.map(function (wb) { return wb.name; }),
            warnings: warnings,
            workbookCount: archiveWorkbooks.length
        };

    } catch (error) {
        Logger.log('archiveHealthCheck error: ' + error.message);
        return { success: false, error: 'Health check failed: ' + error.message };
    }
}

/**
 * Splits G2N_Archive records into year-based workbooks in Archives_Backups folder.
 * Each year gets its own workbook (G2N_Archive_{year}) containing:
 *   - Archive sheet with AM records for that year (by Request Date)
 *   - Products_Archive sheet with matching DR/PF product records
 * Records moved to year workbooks are removed from G2N_Archive.
 * @returns {Object} { success, message, yearDetails: [{ year, amRows, prodRows }] }
 */
function performArchiveRollover() {
    try {
        if (!CONFIG.ARCHIVES_BACKUPS_FOLDER_ID) {
            return { success: false, error: 'ARCHIVES_BACKUPS_FOLDER_ID not configured in Code.gs' };
        }

        var archiveFolderId = CONFIG.ARCHIVE_FOLDER_ID;
        if (!archiveFolderId) {
            return { success: false, error: 'ARCHIVE_FOLDER_ID not configured' };
        }

        // Find G2N_Archive workbook
        var archiveFolder = DriveApp.getFolderById(archiveFolderId);
        var files = archiveFolder.getFilesByName('G2N_Archive');
        if (!files.hasNext()) {
            return { success: false, error: 'G2N_Archive workbook not found in archive folder' };
        }

        var archiveWB = SpreadsheetApp.openById(files.next().getId());
        var backupsFolder = DriveApp.getFolderById(CONFIG.ARCHIVES_BACKUPS_FOLDER_ID);

        // ===== Read Archive (AM) sheet =====
        var archiveSheet = archiveWB.getSheetByName('Archive');
        var amHeaders = [];
        var amDataByYear = {}; // { year: [rows] }
        var amKeepRows = [];

        if (archiveSheet && archiveSheet.getLastRow() > 1) {
            var amData = archiveSheet.getDataRange().getValues();
            amHeaders = amData[0];
            var amHeadersTrimmed = trimHeaders(amHeaders);
            var reqDateCol = amHeadersTrimmed.indexOf('Request Date');

            if (reqDateCol === -1) {
                return { success: false, error: 'Request Date column not found in Archive sheet' };
            }

            for (var i = 1; i < amData.length; i++) {
                var rd = amData[i][reqDateCol];
                var year = extractYear(rd);

                if (year) {
                    if (!amDataByYear[year]) amDataByYear[year] = [];
                    amDataByYear[year].push(amData[i]);
                } else {
                    // No valid date — keep in G2N_Archive
                    amKeepRows.push(amData[i]);
                }
            }
        }

        // ===== Read Products_Archive sheet =====
        var prodArchiveSheet = archiveWB.getSheetByName('Products_Archive');
        var prodHeaders = [];
        var prodDataByYear = {}; // { year: [rows] }
        var prodKeepRows = [];

        if (prodArchiveSheet && prodArchiveSheet.getLastRow() > 1) {
            var prodData = prodArchiveSheet.getDataRange().getValues();
            prodHeaders = prodData[0];
            var prodHeadersTrimmed = trimHeaders(prodHeaders);
            var prodReqDateCol = prodHeadersTrimmed.indexOf('Request Date');
            if (prodReqDateCol === -1) prodReqDateCol = prodHeadersTrimmed.indexOf('RequestDate');

            if (prodReqDateCol !== -1) {
                for (var p = 1; p < prodData.length; p++) {
                    var prd = prodData[p][prodReqDateCol];
                    var pYear = extractYear(prd);

                    if (pYear) {
                        if (!prodDataByYear[pYear]) prodDataByYear[pYear] = [];
                        prodDataByYear[pYear].push(prodData[p]);
                    } else {
                        prodKeepRows.push(prodData[p]);
                    }
                }
            }
        }

        // Collect all years from both AM and Products
        var allYears = {};
        Object.keys(amDataByYear).forEach(function (y) { allYears[y] = true; });
        Object.keys(prodDataByYear).forEach(function (y) { allYears[y] = true; });
        var years = Object.keys(allYears).sort();

        if (years.length === 0) {
            return { success: false, error: 'No records with valid Request Dates found in G2N_Archive.' };
        }

        // ===== Create/update year workbooks =====
        var yearDetails = [];

        for (var yi = 0; yi < years.length; yi++) {
            var year = years[yi];
            var wbName = 'G2N_Archive_' + year;
            var amYearRows = amDataByYear[year] || [];
            var prodYearRows = prodDataByYear[year] || [];

            // Find or create the year workbook
            var yearWB = null;
            var existingFiles = backupsFolder.getFilesByName(wbName);
            if (existingFiles.hasNext()) {
                yearWB = SpreadsheetApp.openById(existingFiles.next().getId());
            } else {
                yearWB = SpreadsheetApp.create(wbName);
                moveToFolder(yearWB.getId(), CONFIG.ARCHIVES_BACKUPS_FOLDER_ID);
            }

            // Build batchUpdate payload — write header+data for new sheets, append for existing
            var batchRanges = [];

            // --- Archive (AM) sheet ---
            if (amYearRows.length > 0 && amHeaders.length > 0) {
                var amSheet = yearWB.getSheetByName('Archive');
                var amIsNew = !amSheet;
                if (amIsNew) {
                    amSheet = yearWB.getSheets()[0];
                    if (amSheet.getName() === 'Sheet1') {
                        amSheet.setName('Archive');
                    } else {
                        amSheet = yearWB.insertSheet('Archive');
                    }
                    amSheet.getRange(1, 1, 1, amHeaders.length)
                        .setFontWeight('bold').setBackground('#f4b400').setFontColor('white');
                    amSheet.setFrozenRows(1);
                    batchRanges.push({
                        range: 'Archive!A1:' + columnLetter_(amHeaders.length) + (1 + amYearRows.length),
                        values: [amHeaders].concat(amYearRows)
                    });
                } else {
                    var amLastRow = amSheet.getLastRow();
                    batchRanges.push({
                        range: 'Archive!A' + (amLastRow + 1) + ':' + columnLetter_(amYearRows[0].length) + (amLastRow + amYearRows.length),
                        values: amYearRows
                    });
                }
            }

            // --- Products_Archive sheet ---
            if (prodYearRows.length > 0 && prodHeaders.length > 0) {
                var prodSheet = yearWB.getSheetByName('Products_Archive');
                var prodIsNew = !prodSheet;
                if (prodIsNew) {
                    prodSheet = yearWB.insertSheet('Products_Archive');
                    prodSheet.getRange(1, 1, 1, prodHeaders.length)
                        .setFontWeight('bold').setBackground('#f4b400').setFontColor('white');
                    prodSheet.setFrozenRows(1);
                    batchRanges.push({
                        range: 'Products_Archive!A1:' + columnLetter_(prodHeaders.length) + (1 + prodYearRows.length),
                        values: [prodHeaders].concat(prodYearRows)
                    });
                } else {
                    var prodLastRow = prodSheet.getLastRow();
                    batchRanges.push({
                        range: 'Products_Archive!A' + (prodLastRow + 1) + ':' + columnLetter_(prodYearRows[0].length) + (prodLastRow + prodYearRows.length),
                        values: prodYearRows
                    });
                }
            }

            // Single Sheets API call writes all ranges for this workbook at once
            if (batchRanges.length > 0) {
                Sheets.Spreadsheets.Values.batchUpdate({
                    valueInputOption: 'RAW',
                    data: batchRanges
                }, yearWB.getId());
                SpreadsheetApp.flush();
            }

            yearDetails.push({ year: year, amRows: amYearRows.length, prodRows: prodYearRows.length });
        }

        // ===== Rewrite G2N_Archive with only keep rows =====
        if (archiveSheet) {
            var amTotalRows = archiveSheet.getMaxRows();
            var amTotalCols = archiveSheet.getMaxColumns();
            if (amTotalRows > 1) {
                archiveSheet.getRange(2, 1, amTotalRows - 1, amTotalCols).clearContent();
            }
            if (amKeepRows.length > 0) {
                archiveSheet.getRange(2, 1, amKeepRows.length, amKeepRows[0].length).setValues(amKeepRows);
            }
            SpreadsheetApp.flush(); // Commit AM rewrite before touching Products_Archive
        }

        if (prodArchiveSheet) {
            var prodTotalRows = prodArchiveSheet.getMaxRows();
            var prodTotalCols = prodArchiveSheet.getMaxColumns();
            if (prodTotalRows > 1) {
                prodArchiveSheet.getRange(2, 1, prodTotalRows - 1, prodTotalCols).clearContent();
            }
            if (prodKeepRows.length > 0) {
                prodArchiveSheet.getRange(2, 1, prodKeepRows.length, prodKeepRows[0].length).setValues(prodKeepRows);
            }
            SpreadsheetApp.flush(); // Commit Products_Archive rewrite
        }

        // Build summary
        var totalAm = 0, totalProd = 0;
        var yearSummary = [];
        for (var d = 0; d < yearDetails.length; d++) {
            totalAm += yearDetails[d].amRows;
            totalProd += yearDetails[d].prodRows;
            yearSummary.push(yearDetails[d].year + ': ' + yearDetails[d].amRows + ' AM, ' + yearDetails[d].prodRows + ' Products');
        }

        var message = 'Rolled over ' + totalAm + ' AM records and ' + totalProd + ' product records into ' + years.length + ' year workbook(s): ' + years.join(', ');
        logAudit('ARCHIVE_ROLLOVER', null, message);

        return {
            success: true,
            message: message,
            yearDetails: yearDetails,
            totalAmMoved: totalAm,
            totalProdMoved: totalProd,
            amKept: amKeepRows.length,
            prodKept: prodKeepRows.length
        };

    } catch (error) {
        Logger.log('performArchiveRollover error: ' + error.message);
        return { success: false, error: 'Rollover failed: ' + error.message };
    }
}

/**
 * Preview what the archive rollover would do without making changes.
 * Returns year breakdown of AM and Products records in G2N_Archive.
 * @returns {Object} { success, years: [{ year, amRows, prodRows }], totalAm, totalProd }
 */
function previewArchiveRollover() {
    try {
        var archiveFolderId = CONFIG.ARCHIVE_FOLDER_ID;
        if (!archiveFolderId) {
            return { success: false, error: 'ARCHIVE_FOLDER_ID not configured' };
        }

        var archiveFolder = DriveApp.getFolderById(archiveFolderId);
        var files = archiveFolder.getFilesByName('G2N_Archive');
        if (!files.hasNext()) {
            return { success: false, error: 'G2N_Archive workbook not found' };
        }

        var archiveWB = SpreadsheetApp.openById(files.next().getId());
        var yearCounts = {}; // { year: { am: n, prod: n } }

        // Count AM records by year
        var archiveSheet = archiveWB.getSheetByName('Archive');
        if (archiveSheet && archiveSheet.getLastRow() > 1) {
            var amData = archiveSheet.getDataRange().getValues();
            var amHeaders = trimHeaders(amData[0]);
            var reqDateCol = amHeaders.indexOf('Request Date');
            if (reqDateCol !== -1) {
                for (var i = 1; i < amData.length; i++) {
                    var rd = amData[i][reqDateCol];
                    var year = extractYear(rd);
                    if (year) {
                        if (!yearCounts[year]) yearCounts[year] = { am: 0, prod: 0 };
                        yearCounts[year].am++;
                    }
                }
            }
        }

        // Count Products records by year
        var prodSheet = archiveWB.getSheetByName('Products_Archive');
        if (prodSheet && prodSheet.getLastRow() > 1) {
            var prodData = prodSheet.getDataRange().getValues();
            var prodHeaders = trimHeaders(prodData[0]);
            var prodReqDateCol = prodHeaders.indexOf('Request Date');
            if (prodReqDateCol === -1) prodReqDateCol = prodHeaders.indexOf('RequestDate');
            if (prodReqDateCol !== -1) {
                for (var j = 1; j < prodData.length; j++) {
                    var prd = prodData[j][prodReqDateCol];
                    var pYear = extractYear(prd);
                    if (pYear) {
                        if (!yearCounts[pYear]) yearCounts[pYear] = { am: 0, prod: 0 };
                        yearCounts[pYear].prod++;
                    }
                }
            }
        }

        var years = Object.keys(yearCounts).sort();
        var details = [];
        var totalAm = 0, totalProd = 0;
        for (var k = 0; k < years.length; k++) {
            var y = years[k];
            totalAm += yearCounts[y].am;
            totalProd += yearCounts[y].prod;
            details.push({ year: y, amRows: yearCounts[y].am, prodRows: yearCounts[y].prod });
        }

        // Check which year workbooks already exist
        var backupsFolder = null;
        var existingWorkbooks = [];
        if (CONFIG.ARCHIVES_BACKUPS_FOLDER_ID) {
            try {
                backupsFolder = DriveApp.getFolderById(CONFIG.ARCHIVES_BACKUPS_FOLDER_ID);
                var bFiles = backupsFolder.getFiles();
                while (bFiles.hasNext()) {
                    existingWorkbooks.push(bFiles.next().getName());
                }
            } catch (e) { /* ignore */ }
        }

        return {
            success: true,
            years: details,
            totalAm: totalAm,
            totalProd: totalProd,
            existingBackups: existingWorkbooks
        };

    } catch (error) {
        Logger.log('previewArchiveRollover error: ' + error.message);
        return { success: false, error: 'Preview failed: ' + error.message };
    }
}

/**
 * Syncs responses from the linked Google Form to Applicants_Master
 * MANUAL UTILITY â€” can be removed if form trigger is automated
 */
function syncFormResponses() {
    const ss = getMasterWorkbook();
    const responsesSheet = ss.getSheetByName('Form Responses 1');
    const masterSheet = ss.getSheetByName(CONFIG.MASTER_SHEET);

    if (!responsesSheet) {
        Logger.log('Form Responses sheet not found');
        return { synced: 0, error: 'Responses sheet not found' };
    }

    if (!masterSheet) {
        Logger.log('Master sheet not found');
        return { synced: 0, error: 'Master sheet not found' };
    }

    const responsesData = responsesSheet.getDataRange().getValues();
    const masterData = masterSheet.getDataRange().getValues();
    const masterHeaders = masterData[0];

    const timestampCol = masterHeaders.indexOf('Timestamp');
    const existingTimestamps = new Set();

    for (let i = 1; i < masterData.length; i++) {
        if (masterData[i][timestampCol]) {
            existingTimestamps.add(masterData[i][timestampCol].toString());
        }
    }

    let nextId = getNextId();
    const responseHeaders = responsesData[0];
    const newRows = [];

    for (let i = 1; i < responsesData.length; i++) {
        const timestamp = responsesData[i][0];
        if (timestamp && !existingTimestamps.has(timestamp.toString())) {
            const newRow = new Array(masterHeaders.length).fill('');
            newRow[0] = nextId++;

            for (let j = 0; j < responseHeaders.length; j++) {
                const responseHeader = responseHeaders[j];
                const masterColIndex = masterHeaders.indexOf(responseHeader);
                if (masterColIndex !== -1) {
                    newRow[masterColIndex] = responsesData[i][j];
                }
            }

            newRows.push(newRow);
        }
    }

    if (newRows.length > 0) {
        masterSheet.getRange(masterSheet.getLastRow() + 1, 1, 1, 1).setNumberFormat('0');
        masterSheet.getRange(
            masterSheet.getLastRow() + 1,
            1,
            newRows.length,
            masterHeaders.length
        ).setValues(newRows);

        logAudit('SYNC', null, 'Synced ' + newRows.length + ' new records from Form Responses');
    }

    Logger.log('Synced ' + newRows.length + ' new records');

    try {
        SpreadsheetApp.getUi().alert('Synced ' + newRows.length + ' new records from Form Responses.');
    } catch (e) {
        // Running from trigger
    }

    return { synced: newRows.length };
}


// ============ ONE-TIME DATA FIX MACROS ============

/** Test-run wrappers — no writes, results preview only. */
function testRunFixedDistribCodes() { applyFixedDistribCodes(true); }
function testRunHygieneSched() { applyHygieneSched(true); }

/**
 * Writes a results summary to a 'MacroResults' sheet in the active spreadsheet.
 */
function writeMacroResults_(title, rows) {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = ss.getSheetByName('MacroResults');
    if (!sheet) sheet = ss.insertSheet('MacroResults');
    else sheet.clearContents();
    var maxCols = rows.reduce(function (m, r) { return Math.max(m, r.length); }, 2);
    var titleRow = [title, new Date().toLocaleString()];
    while (titleRow.length < maxCols) titleRow.push('');
    var allRows = [titleRow].concat(rows);
    sheet.getRange(1, 1, allRows.length, maxCols).setValues(allRows);
    ss.setActiveSheet(sheet);
}

/**
 * Macro 1: Apply Fixed Distribution Codes to Applicants_Master (+ Archives for live run).
 * Source: 'FixedDistribCodes' sheet in G2N_Lookups workbook.
 * v1.3 - Reads source from G2N_Lookups (no external openById).
 * v1.4 - dryRun skips archive scan (AM only) — avoids timeout opening all archive workbooks.
 *         Live run still searches archives.
 * @param {boolean} [dryRun=false]
 */
function applyFixedDistribCodes(dryRun) {
    dryRun = (dryRun === true);
    var ui = SpreadsheetApp.getUi();
    var label = dryRun ? '[TEST RUN] ' : '';
    try {
        var srcSheet = getLookupsWorkbook().getSheetByName('FixedDistribCodes');
        if (!srcSheet) { ui.alert('FixedDistribCodes sheet not found in G2N_Lookups.'); return; }
        var srcData = srcSheet.getDataRange().getValues();
        if (srcData.length < 2) { ui.alert('FixedDistribCodes sheet is empty.'); return; }

        var srcHdrs = srcData[0].map(function (h) { return h.toString().toLowerCase().trim(); });
        var sc = {
            fn: srcHdrs.indexOf('fn'), ln: srcHdrs.indexOf('ln'),
            rpc1: srcHdrs.indexOf('rpc1'), rpc2: srcHdrs.indexOf('rpc2'),
            fc: srcHdrs.indexOf('fc'), fs: srcHdrs.indexOf('fs'),
            gdc: srcHdrs.indexOf('gdc'), sdc: srcHdrs.indexOf('sdc'),
            rs: srcHdrs.indexOf('rs'), bb: srcHdrs.indexOf('bb')
        };
        var missing = Object.keys(sc).filter(function (k) { return sc[k] === -1; });
        if (missing.length) { ui.alert('Missing source columns: ' + missing.join(', ')); return; }

        // Target: AM always; archives only on live run to avoid timeout
        var targets = [];
        var amSheet = getMasterSheet();
        var amData = amSheet.getDataRange().getValues();
        targets.push({ sheet: amSheet, label: 'Applicants_Master', data: amData, headers: trimHeaders(amData[0]) });

        if (!dryRun) {
            try {
                var archWBs = getArchiveWorkbooksForRange(new Date('2000-01-01'), new Date('2099-12-31'));
                for (var a = 0; a < archWBs.length; a++) {
                    var aSheet = archWBs[a].workbook.getSheetByName('Archive');
                    if (!aSheet) continue;
                    var aData = aSheet.getDataRange().getValues();
                    if (aData.length < 2) continue;
                    targets.push({ sheet: aSheet, label: archWBs[a].name, data: aData, headers: trimHeaders(aData[0]) });
                }
            } catch (ae) { Logger.log('archive scan error: ' + ae.message); }
        }

        function getAMCols(hdrs) {
            return {
                fn: hdrs.indexOf(resolveAMField_('First Name')),
                ln: hdrs.indexOf(resolveAMField_('Last Name')),
                bc: hdrs.indexOf(resolveAMField_('Box Code')),
                sb1: hdrs.indexOf(resolveAMField_('Scheduled Box Code 1')),
                rp1: hdrs.indexOf(resolveAMField_('Received Product Code 1')),
                sb2: hdrs.indexOf(resolveAMField_('Scheduled Box Code 2')),
                rp2: hdrs.indexOf(resolveAMField_('Received Product Code 2')),
                fc: hdrs.indexOf(resolveAMField_('Funding Code')),
                fs: hdrs.indexOf(resolveAMField_('Funding Source')),
                gdc: hdrs.indexOf(resolveAMField_('Generic Distribution Code')),
                sdc: hdrs.indexOf(resolveAMField_('Scheduled Distribution Code')),
                rs: hdrs.indexOf(resolveAMField_('Service Status')),
                bb: hdrs.indexOf(resolveAMField_('Take Baby Box?'))
            };
        }

        var resultRows = [['Name', 'Sheet', 'Row', 'Field', 'Old Value', 'New Value', 'Action']];
        var totalCells = 0, notFound = [];

        for (var si = 1; si < srcData.length; si++) {
            var fn = (srcData[si][sc.fn] || '').toString().trim();
            var ln = (srcData[si][sc.ln] || '').toString().trim();
            if (!fn && !ln) continue;
            var rpc1 = (srcData[si][sc.rpc1] || '').toString().trim();
            var rpc2 = (srcData[si][sc.rpc2] || '').toString().trim();
            var fc = (srcData[si][sc.fc] || '').toString().trim();
            var fs = (srcData[si][sc.fs] || '').toString().trim();
            var gdc = (srcData[si][sc.gdc] || '').toString().trim();
            var sdc = (srcData[si][sc.sdc] || '').toString().trim();
            var rs = (srcData[si][sc.rs] || '').toString().trim();
            var bb = (srcData[si][sc.bb] || '').toString().trim().toUpperCase();

            var foundAny = false;
            for (var t = 0; t < targets.length; t++) {
                var tgt = targets[t];
                var ac = getAMCols(tgt.headers);
                for (var r = 1; r < tgt.data.length; r++) {
                    var rowFn = (tgt.data[r][ac.fn] || '').toString().trim().toLowerCase();
                    var rowLn = (tgt.data[r][ac.ln] || '').toString().trim().toLowerCase();
                    if (rowFn !== fn.toLowerCase() || rowLn !== ln.toLowerCase()) continue;
                    foundAny = true;
                    var sheetRow = r + 1;
                    var changes = [];
                    var qc = function (col, val, field) {
                        if (col === -1 || val === '') return;
                        changes.push({ col: col, val: val, field: field, old: (tgt.data[r][col] || '').toString() });
                    };
                    if (rpc1 !== '') { qc(ac.bc, rpc1, 'Box Code'); qc(ac.sb1, rpc1, 'Scheduled Box Code 1'); qc(ac.rp1, rpc1, 'Received Product Code 1'); }
                    if (rpc2 !== '') { qc(ac.sb2, rpc2, 'Scheduled Box Code 2'); qc(ac.rp2, rpc2, 'Received Product Code 2'); }
                    qc(ac.fc, fc, 'Funding Code'); qc(ac.fs, fs, 'Funding Source');
                    qc(ac.gdc, gdc, 'Generic Distribution Code'); qc(ac.sdc, sdc, 'Scheduled Distribution Code');
                    qc(ac.rs, rs, 'Service Status');
                    if (bb === 'Y' && ac.bb !== -1) changes.push({ col: ac.bb, val: 'X', field: 'Take Baby Box?', old: (tgt.data[r][ac.bb] || '').toString() });
                    for (var c = 0; c < changes.length; c++) {
                        var ch = changes[c];
                        resultRows.push([fn + ' ' + ln, tgt.label, sheetRow, ch.field, ch.old, ch.val, dryRun ? 'WOULD UPDATE' : 'UPDATED']);
                        if (!dryRun) tgt.sheet.getRange(sheetRow, ch.col + 1).setValue(ch.val);
                        totalCells++;
                    }
                }
            }
            if (!foundAny) notFound.push(fn + ' ' + ln);
        }

        if (!dryRun) SpreadsheetApp.flush();
        notFound.forEach(function (n) { resultRows.push([n, '', '', '', '', '', 'NOT FOUND']); });
        var archNote = dryRun ? ' (AM only — archives skipped in test run)' : '';
        writeMacroResults_(label + 'Fixed Distrib Codes' + archNote + ' — ' + totalCells + ' cell(s) ' + (dryRun ? 'would update' : 'updated') + ', ' + notFound.length + ' not found', resultRows);
        ui.alert(label + 'Complete. See MacroResults sheet.\n' + totalCells + ' cell(s) ' + (dryRun ? 'would be updated' : 'updated') + '.\n' + notFound.length + ' name(s) not found.' + (dryRun ? '\n\nNote: Archives skipped in test run.' : ''));
    } catch (e) { Logger.log('applyFixedDistribCodes: ' + e.message); ui.alert('Error: ' + e.message); }
}

/**
 * Macro 2: Apply Hygiene Schedule to LU_SchedDisbCodes.
 * Source: 'HygieneSched' sheet in G2N_Lookups workbook.
 * v1.3 - Reads from G2N_Lookups.
 * @param {boolean} [dryRun=false]
 */
function applyHygieneSched(dryRun) {
    dryRun = (dryRun === true);
    var ui = SpreadsheetApp.getUi();
    var label = dryRun ? '[TEST RUN] ' : '';
    try {
        var srcSheet = getLookupsWorkbook().getSheetByName('HygieneSched');
        if (!srcSheet) { ui.alert('HygieneSched sheet not found in G2N_Lookups.'); return; }
        var srcData = srcSheet.getDataRange().getValues();
        if (srcData.length < 2) { ui.alert('HygieneSched sheet is empty.'); return; }

        var srcHdrs = srcData[0].map(function (h) { return h.toString().toLowerCase().trim(); });
        var sc = {
            sdc: srcHdrs.indexOf('sdc'), rpc1: srcHdrs.indexOf('rpc1'),
            rpc2: srcHdrs.indexOf('rpc2'), fc: srcHdrs.indexOf('fc'), fs: srcHdrs.indexOf('fs')
        };
        var missing = Object.keys(sc).filter(function (k) { return sc[k] === -1; });
        if (missing.length) { ui.alert('Missing source columns: ' + missing.join(', ')); return; }

        var luSheet = getLookupsWorkbook().getSheetByName(CONFIG.LOOKUPS.SCHED_DISB_CODES);
        if (!luSheet) { ui.alert('LU_SchedDisbCodes not found.'); return; }

        var luData = luSheet.getDataRange().getValues();
        var luHdrs = trimHeaders(luData[0]);
        var codeCol = luHdrs.indexOf('SchedDisbCode');
        var box1Col = luHdrs.indexOf('Box 1'); if (box1Col === -1) box1Col = luHdrs.indexOf('Box1');
        var box2Col = luHdrs.indexOf('Box 2'); if (box2Col === -1) box2Col = luHdrs.indexOf('Box2');
        var fscCol = luHdrs.indexOf('FundingSource');
        var fdcCol = luHdrs.indexOf('FundingDescription');
        if (codeCol === -1) { ui.alert('SchedDisbCode column not found.'); return; }

        var idx = {};
        for (var r = 1; r < luData.length; r++) {
            var code = (luData[r][codeCol] || '').toString().trim();
            if (code) idx[code] = r;
        }

        var resultRows = [['SDC', 'Row', 'Field', 'Old Value', 'New Value', 'Action']];
        var totalCells = 0, notFound = [];

        for (var i = 1; i < srcData.length; i++) {
            var sdc = (srcData[i][sc.sdc] || '').toString().trim(); if (!sdc) continue;
            var di = idx[sdc];
            if (di === undefined) { notFound.push(sdc); continue; }
            var sheetRow = di + 1;
            var rpc1 = (srcData[i][sc.rpc1] || '').toString().trim();
            var rpc2 = (srcData[i][sc.rpc2] || '').toString().trim();
            var fc = (srcData[i][sc.fc] || '').toString().trim();
            var fs = (srcData[i][sc.fs] || '').toString().trim();
            var changes = [];
            var ql = function (col, val, field) {
                if (col === -1 || val === '') return;
                changes.push({ col: col, val: val, field: field, old: (luData[di][col] || '').toString() });
            };
            ql(box1Col, rpc1, 'Box 1'); ql(box2Col, rpc2, 'Box 2');
            ql(fscCol, fc, 'FundingSource'); ql(fdcCol, fs, 'FundingDescription');
            for (var c = 0; c < changes.length; c++) {
                var ch = changes[c];
                resultRows.push([sdc, sheetRow, ch.field, ch.old, ch.val, dryRun ? 'WOULD UPDATE' : 'UPDATED']);
                if (!dryRun) luSheet.getRange(sheetRow, ch.col + 1).setValue(ch.val);
                totalCells++;
            }
        }

        if (!dryRun) SpreadsheetApp.flush();
        notFound.forEach(function (n) { resultRows.push([n, '', '', '', '', 'NOT FOUND']); });
        writeMacroResults_(label + 'Hygiene Sched — ' + totalCells + ' cell(s) ' + (dryRun ? 'would update' : 'updated') + ', ' + notFound.length + ' not found', resultRows);
        ui.alert(label + 'Complete. See MacroResults sheet.\n' + totalCells + ' cell(s) ' + (dryRun ? 'would be updated' : 'updated') + '.\n' + notFound.length + ' code(s) not found.');
    } catch (e) { Logger.log('applyHygieneSched: ' + e.message); ui.alert('Error: ' + e.message); }
}

/**
 * Diagnostic: shows headers from FixedDistribCodes and HygieneSched in G2N_Lookups.
 */
function diagSourceHeaders() {
    var ui = SpreadsheetApp.getUi();
    var out = '';
    try {
        var sh1 = getLookupsWorkbook().getSheetByName('FixedDistribCodes');
        out += sh1 ? ('FixedDistribCodes:\n' + sh1.getRange(1, 1, 1, sh1.getLastColumn()).getValues()[0].join(' | '))
            : 'FixedDistribCodes NOT FOUND in G2N_Lookups';
    } catch (e) { out += 'Error: ' + e.message; }
    out += '\n\n';
    try {
        var sh2 = getLookupsWorkbook().getSheetByName('HygieneSched');
        out += sh2 ? ('HygieneSched:\n' + sh2.getRange(1, 1, 1, sh2.getLastColumn()).getValues()[0].join(' | '))
            : 'HygieneSched NOT FOUND in G2N_Lookups';
    } catch (e) { out += 'Error: ' + e.message; }
    ui.alert(out);
}

function diagStep1() {
    Logger.log('step 1 - start');
    var lu = getLookupsWorkbook();
    Logger.log('step 2 - got lookups workbook: ' + lu.getName());
    var sh = lu.getSheetByName('FixedDistribCodes');
    Logger.log('step 3 - sheet: ' + (sh ? sh.getName() : 'NOT FOUND'));
    var data = sh ? sh.getLastRow() : 0;
    Logger.log('step 4 - rows: ' + data);
    SpreadsheetApp.getUi().alert('Done - check Logs');
}

function diagSheetNames() {
    var sheets = getLookupsWorkbook().getSheets();
    var names = sheets.map(function (s) { return '"' + s.getName() + '"'; });
    SpreadsheetApp.getUi().alert(names.join('\n'));
}

function diagRaw() {
    var lu = SpreadsheetApp.openById('1jahhI4JoDntwrAcv_E6B4te8jI6RZkn0eNbnt-pThX0');
    var names = lu.getSheets().map(function (s) { return s.getName(); });
    Logger.log(names.join(', '));
    SpreadsheetApp.getUi().alert(names.join('\n'));
}