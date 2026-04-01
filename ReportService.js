/**
 * ReportService.gs
 * Operational report generation and data management for G2N System
 *
 * Reports:
 *   - Distribution Report: Filtered by SchedDisbCode, sorted by name
 *   - Scheduling Report: ID range based with combined age brackets,
 *       History marker, data validation dropdowns, magenta highlight
 *   - Login Report: Session duration analysis from AuditLog
 *
 * Operations:
 *   - Process Distribution: Updates AM records from pickup status,
 *       inserts stats into Hygiene Box Distribution Stats workbook
 *   - Process Scheduling Report: Reads report, updates AM SchedDisbCode/Status
 *   - Archive: Bulk moves aged records to G2N_Archive workbook
 *   - File Cleanup: Deletes report files older than 3 months (excl. Distribution)
 *
 * v4.0 - Phase 4: Scheduling Report enhancements (History column,
 *         data validation, sort by ID, magenta highlight, auto-detect
 *         last AM ID), Distribution Report times from LU_DistribCodes,
 *         editable report data retrieval
 * v4.1 - Process Scheduling Report: LU_SchedDisbCodes lookup writes
 *         Distribution Start Date, Distribution Interval, Funding Source,
 *         Generic Distribution Code to AM.
 *         Process Distribution: Box 1→Box Code/Sched Box Code 1/Received
 *         Product Code 1, Box 2→Sched Box Code 2/Received Product Code 2.
 *         Date fallback: ending date from Available Dates when Date Picked
 *         Up is blank for Last Date Served, Final SCD, Next Service Avail.
 *         Updated baby box footer text with word wrap.
 * v4.2 - Process Scheduling Report: Service Status write-back now also
 *         updates Last Date Served, Final SCD with current date and
 *         Next Service Availability Date with current date + 90 days
 * v4.3 - Hygiene Box Distribution Stats: finds sheet by distribution code
 *         (tries full code then generic), writes by header name:
 *         Completed in Spreadsheet, Distribution Date Range, Scheduled
 *         Distrib Code, Total Scheduled-Recipients/Boxes, With Baby Box,
 *         Picked Up-Generic/Baby, Restock-# Recipients, % No Pick Up
 * v4.4 - Fixed Hygiene Stats column name: Restock-# Recipients (dash not =)
 * v4.5 - Added Box 3 support: Scheduled Box Code 3, Received Product Code 3
 * v5.0 - Phase 5: Login Report saves to Google Sheet with Year/Month/Day
 *         timestamp breakout, saved to Login folder with Open/Download links.
 *         Archive file deletion changed from 6 months to 3 months,
 *         excluding Distribution folder files.
 * v5.1 - Phase 5 fix: executeArchiveBatch now calls checkAndAutoRollover()
 *         after completion — auto-splits G2N_Archive by year if >= 85% capacity.
 * v5.2 - Login Report: Added INTAKE_SESSION and CREATE events to Detail sheet
 *         and new "Intake Activity" sheet with summary (opens, creates, conversion
 *         rate by mode) and event detail. Record ID column added to Detail sheet.
 *         Rollover result included in batch return for AP display.
 * v5.3 - Adopted trimHeaders() across all 16 inline header trim calls.
 * v5.4 - Removed dead legacy executeArchive() function (superseded by
 *         executeArchiveBatch; no server-side callers remained).
 * v5.5 - CRITICAL: Adopted trimHeaders() across 4 remaining raw header reads
 *         in generateDistributionReport(), scheduling report, scheduling report
 *         processing, and login report functions. Prevents silent column-match
 *         failures from whitespace in AM headers (#1).
 *         Replaced 4 inline folder-move blocks with shared moveToFolder() (#4).
 *         Adopted CONFIG.TIMEZONE across 24 formatDate() calls (#8).
 *         Replaced applicable inline report header styling with shared
 *         styleReportHeader() (#10).
 * v5.6 - Added 'Admin Notes' column to Scheduling Report (generateSchedulingReport):
 *         inserted after 'Service Status' in reportColumns; updated colWidths
 *         with 200px for the new column.
 * v5.7 - Distribution Report: Added formatted Phone Number column to the right
 *         of City. Formatted as (XXX) XXX-XXXX for 10-digit numbers; raw value
 *         otherwise. Added formatPhoneNumber_() helper.
 * v5.8 - previewDistributionReport: Added try/catch so GAS exceptions return
 *         {success:false, error:...} instead of triggering client withFailureHandler
 *         with no visible message.
 * v5.9 - Integrated FieldMapService: all AM column lookups across generateDistributionReport,
 *         previewDistributionReport, getDistributionInfo, generateSchedulingReport,
 *         processSchedulingReport, processDistribution, and executeArchiveBatch now use
 *         resolveAMField_() via SharedUtils for LU_FieldMap-driven header resolution.
 * v5.10 - Distribution Report: Address 1 and Address 2 column headers now resolved
 *          via getReportHeader_() so the report label matches LU_FieldMap Report Header
 *          (e.g. "Apt/Unit/Lot" instead of hardcoded "Address 2").
 * v5.11 - executeArchiveBatch: SpreadsheetApp.flush() added after writing rows to
 *          archive sheet and after rewriting master sheet for commit ordering safety.
 * v5.12 - Distribution Report: Added "Extra Box" column to the right of "Baby Box".
 *          Shows "X" when Scheduled Box Code 3 has any value. hasExtraBox flag mirrors
 *          hasBabyBox pattern; column and footer note only appear when needed.
 *          createDistributionReportSpreadsheet() 
 * v5.13 - processDistributionSheet: Extra Box column now detected per-row.
 *          Box Code 3 fields (Scheduled Box Code 3, Received Product Code 3)
 *          written only for rows where Extra Box = "X". Rows without "X"
 *          are left unchanged. Older reports without Extra Box column retain
 *          original behaviour (luBox3 applied to all rows).
 *          column widths now built dynamically.
 *          Scheduling Report: Phone Number formatted as (XXX) XXX-XXXX via
 *          formatPhoneNumber_(). Standard applied to all report and portal phone displays.
 * v5.14 - processDistributionSheet: For Extra Box rows (Extra Box = "X"),
 *          copies QtyRequested → QtyReceived in DR/PF_Products for that
 *          ID + RequestDate. requestDateCol added to masterHeaders lookups.
 *          Updates collected during main loop; applied in one bulk setValues()
 *          after all AM writes complete.
 * v5.15 - processDistributionSheet: Received Product Code 3 now read from
 *          AM Scheduled Box Code 3 directly (per-client value), not luBox3
 *          from LU_SchedDisbCodes (typically empty for Box 3).
 *          DR/PF_Products update now triggers for ANY row where Scheduled
 *          Box Code 1, 2, or 3 in AM starts with DR or PF, not just Extra
 *          Box rows. luBox1/luBox2 checked; AM Box3 checked per-row.
 * v5.16 - processDistributionSheet: DR/PF_Products update block now emits
 *          individual per-ID log entries (id, requestDate, rows updated)
 *          plus phase markers "Updating DR/PF Products..." and
 *          "DR/PF Products saved" so spinner-phase log is accurate.
 * v5.17 - saveDistributionReportEdits: now accepts distribCode param.
 *          Writes Extra Box column back to report sheet.
 *          Syncs AM Scheduled Box Code 3: X → luBox3 value;
 *          blank → clears AM field. Uses bulk setValues() for AM writes.
 * v5.18 - generateSchedulingReport: added Referrer Title column after Referral Info.
 *          Added How Products Help as last column after Briefly Explain.
 *          Fixed History flag: now per-name (highest ID = current, all lower IDs
 *          for same name = History) rather than range-boundary-only check.
 *          colWidths updated for 2 new columns; both long-text columns wrapped.
 *          After successful generation calls appendLastScheduledId() to record
 *          endId + report date in LU_LastScheduled for Beginning ID auto-population.
 * v5.19 - generateSchedulingReport: removed beginId/endId parameters entirely.
 *          Report now includes ALL records from Applicants_Master (no archive,
 *          no ID range, no 1-year date filter).
 *          Report name is date-only (no ID range in title).
 *          Removed magenta AM highlight and appendLastScheduledId() call.
 *          Added 'Learned How' column after 'Referrer Title' in reportColumns.
 *          Confirmed 'How Products Help' immediately after 'Current Situation'
 *          in additionalColumns.
 *          Report column headers now written via getReportHeader_() from
 *          LU_FieldMap; rawKeyHeaders array used internally for column position
 *          tracking (data validation, wrap, etc.) so no raw-name indexOf needed.
 *          Assistance column text wrapping applied.
 *          colWidths updated to 45 columns (inserted Learned How at index 14).
 *          processSchedulingReport: rptIdCol / rptSchedCodeCol / rptStatusCol
 *          lookups changed from resolveAMField_() to getReportHeader_() to match
 *          the report headers now written by generateSchedulingReport.
 *          AdminPortalWeb v7.3: removed Beginning ID field, loadLastScheduledId(),
 *          and related client-side logic.
 * v5.20 - LU_ReportColumns integration for generateSchedulingReport().
 *          Column definitions (previously hardcoded reportColumns,
 *          ageBracketPairs, additionalColumns arrays) now read from
 *          LU_ReportColumns 'Scheduling' via ReportColumnService.
 *          Column widths applied via applyReportColumnFormatting().
 *          Falls back to v5.19 hardcoded arrays when LU_ReportColumns empty.
 *          processSchedulingReport() unchanged — still uses getReportHeader_()
 *          for column lookups since it reads the generated report back.
 * v5.21 - createDistributionReportSpreadsheet(): replaced per-row setValue()
 *         loop with single batch setValues() call for data rows.
 *         Alternating row background applied as a single loop of setBackground()
 *         per-row (not per-cell). Eliminates 800+ API calls for a 80-record
 *         report; fixes "running forever" / 6-minute GAS timeout.
 * v5.22 - Re-added generateDistributionReport() which was missing from the file.
 *         Filters AM by Scheduled Distribution Code, builds record objects,
 *         calls createDistributionReportSpreadsheet() (batch write, v5.21).
 * v5.23 - Added lastRecordId parameter. Non-History rows with rowId > lastRecordId
 *         are highlighted pink (#FFB6C1) after data rows are written.
 */

'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// Age bracket pair map for combined M+F columns
// Maps each [Calc] column key to the two raw AM headers whose values are summed.
// Used by _buildSchedRow_() to resolve combined bracket columns.
// ─────────────────────────────────────────────────────────────────────────────

/** @type {Object.<string, {male: string, female: string}>} */
var SCHED_AGE_BRACKET_MAP_ = {
    '[Calc] Under 5': { male: '[Males under 5]', female: '[Females under 5]' },
    '[Calc] 5-9': { male: '[Males 5-9]', female: '[Females 5-9]' },
    '[Calc] 10-14': { male: '[Males 10-14]', female: '[Females 10-14]' },
    '[Calc] 15-17': { male: '[Males 15-17]', female: '[Females 15-17]' },
    '[Calc] 18-24': { male: '[Males 18-24]', female: '[Females 18-24]' },
    '[Calc] 25-34': { male: '[Males 25-34]', female: '[Females 25-34]' },
    '[Calc] 35-44': { male: '[Males 35-44]', female: '[Females 35-44]' },
    '[Calc] 45-54': { male: '[Males 45-54]', female: '[Females 45-54]' },
    '[Calc] 55-64': { male: '[Males 55-64]', female: '[Females 55-64]' },
    '[Calc] 65-84': { male: '[Males 65-84]', female: '[Females 65-84]' },
    '[Calc] 85 and Over': { male: '[Males 85 and Over]', female: '[Females 85 and Over]' }
};

/**
 * v5.19 hardcoded column fallback for the Scheduling Report.
 * Returned by _getSchedFallbackCols_() when LU_ReportColumns is empty.
 * These exactly match the v5.19 reportColumns + ageBracketPairs + additionalColumns
 * arrays, reshaped to {key, label, width, wrapText} format.
 *
 * Once LU_ReportColumns is populated, this function is never called.
 *
 * @private
 * @returns {Object[]} [{key, label, width, wrapText}]
 */
function _getSchedFallbackCols_() {
    var raw = [
        // History flag (special — not an AM column)
        { key: 'History', width: 55 },
        // Report columns (first block)
        { key: 'Scheduled Distribution Code', width: 99 },
        { key: 'Service Status', width: 91 },
        { key: 'Admin Notes', width: 200 },
        { key: 'Take Baby Box?', width: 44 },
        { key: 'ID', width: 59 },
        { key: 'Request Type', width: 63 },
        { key: 'Request Date', width: 85 },
        { key: 'Last Date Served', width: 106 },
        { key: 'Next Service Availability Date', width: 101 },
        { key: 'Last Name', width: 95 },
        { key: 'First Name', width: 75 },
        { key: 'More information about the person or organization that referred you.', width: 172 },
        { key: 'What is the title or position of the person who referred you?', width: 172 },
        { key: 'How did you learn about our program?', width: 128 },
        { key: "What is the person's name who referred you?", width: 128 },
        { key: 'Please tell us anything else you would like us to know about how you heard about Giving to the Nations or our program.', width: 244 },
        { key: 'Are you currently homeless?', width: 87 },
        { key: 'Street Address', width: 145 },
        { key: 'Apartment #, Upper, Lower, or Lot #', width: 84 },
        { key: 'City', width: 76 },
        { key: 'State', width: 66 },
        { key: 'County', width: 57 },
        { key: 'Address type?', width: 126 },
        { key: 'Phone Number', width: 94 },
        { key: 'Phone type?', width: 79 },
        { key: 'Email', width: 186 },
        { key: 'Best contact method (What is the best way to get ahold of you?)', width: 141 },
        // Age bracket columns (combined M+F)
        { key: '[Calc] Under 5', width: 52 },
        { key: '[Calc] 5-9', width: 26 },
        { key: '[Calc] 10-14', width: 41 },
        { key: '[Calc] 15-17', width: 41 },
        { key: '[Calc] 18-24', width: 41 },
        { key: '[Calc] 25-34', width: 41 },
        { key: '[Calc] 35-44', width: 41 },
        { key: '[Calc] 45-54', width: 41 },
        { key: '[Calc] 55-64', width: 41 },
        { key: '[Calc] 65-84', width: 41 },
        { key: '[Calc] 85 and Over', width: 77 },
        // Additional columns (after age brackets)
        { key: 'Are you receiving any assistance? Please select ALL boxes that apply to ANY ASSISTANCE your family is receiving.', width: 221, wrapText: true },
        { key: 'Are you currently employed?', width: 86 },
        { key: 'Total annual household income. (How much money does your family make in 1 year?)', width: 172 },
        { key: 'Was there an emergency situation which caused you to contact us?', width: 97 },
        { key: 'Briefly explain your current situation.', width: 732, wrapText: true },
        { key: 'How will receiving personal and home cleaning products help you?', width: 732, wrapText: true }
    ];
    return raw.map(function (d) {
        return {
            key: d.key,
            // getReportHeader_() resolves LU_FieldMap Report Header; 'History' has no LU entry so passes through
            label: getReportHeader_(d.key) || d.key,
            width: d.width || 70,
            wrapText: d.wrapText || false
        };
    });
}

/**
 * Build a single Scheduling Report output row from a record and column definitions.
 *
 * Three types of columns are handled:
 *   1. 'History'          → rec.isHistory ? 'History' : ''
 *   2. '[Calc] x-y' keys  → combined sum of male + female bracket from SCHED_AGE_BRACKET_MAP_
 *   3. All other keys     → direct AM column value via colIndexMap
 *
 * Special value transforms applied (same as v5.19):
 *   - Dates → formatted M/d/yyyy
 *   - Scheduled Distribution Code → uppercase
 *   - Phone Number → (XXX) XXX-XXXX via formatPhoneNumber_()
 *
 * @private
 * @param {Object}  rec        - { rowData: Array, isHistory: boolean }
 * @param {Object[]} cols      - From getReportColumns('Scheduling') or _getSchedFallbackCols_()
 * @param {Object}  colIndexMap- { rawKey: colIndex } built from AM headers
 * @returns {Array} Single row of cell values
 */
function _buildSchedRow_(rec, cols, colIndexMap) {
    return cols.map(function (col) {
        var key = col.key;

        // ── History flag ──────────────────────────────────────────────────────
        if (key === 'History') {
            return rec.isHistory ? 'History' : '';
        }

        // ── Combined age bracket (M + F) ──────────────────────────────────────
        var bracketPair = SCHED_AGE_BRACKET_MAP_[key];
        if (bracketPair) {
            var mi = colIndexMap[bracketPair.male];
            var fi = colIndexMap[bracketPair.female];
            var mv = mi !== undefined ? (parseInt(rec.rowData[mi]) || 0) : 0;
            var fv = fi !== undefined ? (parseInt(rec.rowData[fi]) || 0) : 0;
            var tot = mv + fv;
            return tot > 0 ? tot : '';  // blank (not 0) when no one in this bracket
        }

        // ── Direct AM column lookup ───────────────────────────────────────────
        var idx = colIndexMap[key];
        if (idx === undefined) return '';

        var val = rec.rowData[idx];
        if (val instanceof Date) {
            return Utilities.formatDate(val, CONFIG.TIMEZONE, 'M/d/yyyy');
        }
        if (key === 'Scheduled Distribution Code' && val) {
            return val.toString().toUpperCase();
        }
        if (key === 'Phone Number') {
            return formatPhoneNumber_(val);
        }
        return val || '';
    });
}

/**
 * Get all ACTIVE Scheduled Distribution Codes for data validation dropdowns.
 *
 * Reads from LU_SchedDisbCodes (CONFIG.LOOKUPS.SCHED_DISB_CODES), which is the
 * authoritative source for scheduled distribution codes. Filters to Active = TRUE
 * rows only, matching the same logic used by the Distribution Reports tab dropdown.
 *
 * Delegates to getActiveSchedDisbCodes() in LookupService.gs which already
 * handles Active column filtering, date formatting, and caching correctly.
 *
 * NOTE: This was previously reading from LU_DistribCodes (wrong sheet — that table
 * holds generic distribution/pickup codes, not scheduled distribution codes).
 *
 * Only add this function if it doesn't already exist in your ReportService.js.
 *
 * @private
 * @returns {string[]} Sorted array of active SchedDisbCode strings
 */
function getAllDistribCodes_() {
    try {
        // getActiveSchedDisbCodes() is in LookupService.gs — reads LU_SchedDisbCodes,
        // filters Active = TRUE, returns [{code, startDate, interval, ...}]
        var activeCodes = getActiveSchedDisbCodes();
        if (!activeCodes || activeCodes.length === 0) return [];
        return activeCodes
            .map(function (c) { return (c.code || '').toString().trim().toUpperCase(); })
            .filter(function (c) { return c !== ''; })
            .sort();
    } catch (e) {
        Logger.log('getAllDistribCodes_ error (non-fatal): ' + e.message);
        return [];
    }
}

/**
 * Generate Scheduling Report for ALL Applicants_Master records.
 *
 * ID RANGE TRACKING (v5.23):
 *   startId = last EndId stored in LU_SchedID + 1 (0 on first run).
 *   endId   = highest ID in the current AM data set.
 *   Both values are appended to LU_SchedID; the endId row in AM is highlighted magenta.
 *
 * HISTORY FLAG (v5.23 revised):
 *   For names appearing >1 time:
 *     - If any record for that name has NO Service Status (open/pending), all
 *       records WITH a Service Status are marked History.
 *     - If ALL records for that name have a Service Status, the highest ID is
 *       current and all lower IDs are History.
 *   For names appearing exactly once: never History.
 *
 * NEW RECORD HIGHLIGHTING (v5.23):
 *   Rows with rowId >= startId AND rowId <= endId:
 *     - Entire row: pink background (#FFB6C1), bold.
 *     - First Name and Last Name cells: yellow (#FFFF00).
 *   All rows: First Name and Last Name cells yellow (#FFFF00).
 *
 * DATA VALIDATION:
 *   Scheduled Distribution Code: active codes from LU_SchedDisbCodes (alphabetical).
 *   Service Status: active values from LU_ServiceStatus (not hardcoded).
 *
 * v5.19 — All AM rows; Learned How; no ID range params.
 * v5.20 — LU_ReportColumns integration.
 * v5.23 — LU_SchedID tracking; revised history; dynamic dropdowns; new-record highlights.
 *
 * @returns {{ success, reportUrl, downloadUrl, reportId, recordCount, startId, endId }}
 */
function generateSchedulingReport() {
    try {
        var sheet = getMasterSheet();
        if (!sheet) return { success: false, error: 'Master sheet not found' };

        var data = sheet.getDataRange().getValues();
        var headers = trimHeaders(data[0]);
        var idCol = headers.indexOf(resolveAMField_('ID'));
        var fnCol = headers.indexOf(resolveAMField_('First Name'));
        var lnCol = headers.indexOf(resolveAMField_('Last Name'));
        var svcStatusCol = headers.indexOf(resolveAMField_('Service Status'));
        if (idCol === -1) return { success: false, error: 'ID column not found in Applicants_Master' };

        // ── Get column definitions from LU_ReportColumns (with v5.19 fallback) ──
        var cols = getReportColumns('Scheduling');
        if (cols.length === 0) {
            Logger.log('generateSchedulingReport: LU_ReportColumns empty — using v5.19 fallback');
            cols = _getSchedFallbackCols_();
        }

        // ── Determine ID range from LU_SchedID ────────────────────────────────
        var schedIdInfo = getLastSchedId();
        var startId = (schedIdInfo.lastEndId || 0) + 1;

        // Find the actual last row ID in AM
        var endId = 0;
        var endIdRowNum = -1; // 1-based sheet row of endId record
        for (var r = 1; r < data.length; r++) {
            var rowId = parseInt(data[r][idCol]) || 0;
            if (rowId > endId) {
                endId = rowId;
                endIdRowNum = r + 1; // +1 for 1-based sheet row
            }
        }

        // ── Build colIndexMap ─────────────────────────────────────────────────
        var SCHED_AGE_BRACKET_MAP_ = {};
        var colIndexMap = {};
        cols.forEach(function (col) {
            var key = col.key;
            if (key === 'History') return;
            var bracketPair = SCHED_AGE_BRACKET_MAP_[key];
            if (bracketPair) {
                colIndexMap[bracketPair.male] = headers.indexOf(resolveAMField_(bracketPair.male));
                colIndexMap[bracketPair.female] = headers.indexOf(resolveAMField_(bracketPair.female));
            } else {
                var resolved = resolveAMField_(key);
                colIndexMap[key] = headers.indexOf(resolved);
            }
        });

        // ── Build records array ────────────────────────────────────────────────
        var records = [];
        for (var i = 1; i < data.length; i++) {
            var rowId = parseInt(data[i][idCol]) || 0;
            if (!rowId) continue;
            var firstName = fnCol !== -1 ? (data[i][fnCol] || '').toString().trim() : '';
            var lastName = lnCol !== -1 ? (data[i][lnCol] || '').toString().trim() : '';
            var svcStatus = svcStatusCol !== -1 ? (data[i][svcStatusCol] || '').toString().trim() : '';
            records.push({
                rowId: rowId,
                firstName: firstName,
                lastName: lastName,
                svcStatus: svcStatus,
                isHistory: false,
                rowData: data[i]
            });
        }

        // ── Sort: Last Name, First Name, ID ascending ──────────────────────────
        records.sort(function (a, b) {
            var lc = a.lastName.toLowerCase().localeCompare(b.lastName.toLowerCase());
            if (lc !== 0) return lc;
            var fc = a.firstName.toLowerCase().localeCompare(b.firstName.toLowerCase());
            return fc !== 0 ? fc : a.rowId - b.rowId;
        });

        // ── History flag (v5.23 revised logic) ────────────────────────────────
        // Group records by normalized full name
        var nameGroups = {};
        records.forEach(function (rec) {
            var k = rec.firstName.toLowerCase() + '|' + rec.lastName.toLowerCase();
            if (!nameGroups[k]) nameGroups[k] = [];
            nameGroups[k].push(rec);
        });

        Object.keys(nameGroups).forEach(function (k) {
            var group = nameGroups[k];
            if (group.length <= 1) return; // single record — never History

            // Check if any record in group has NO service status (open/pending)
            var hasOpenRecord = group.some(function (r) { return !r.svcStatus; });

            if (hasOpenRecord) {
                // All records WITH a service status are History
                group.forEach(function (r) {
                    if (r.svcStatus) r.isHistory = true;
                });
            } else {
                // All have service status — highest ID is current, lower IDs are History
                var maxId = Math.max.apply(null, group.map(function (r) { return r.rowId; }));
                group.forEach(function (r) {
                    if (r.rowId < maxId) r.isHistory = true;
                });
            }
        });

        // ── Create spreadsheet ─────────────────────────────────────────────────
        var reportDate = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd');
        var reportName = 'Scheduling_' + reportDate;
        var spreadsheet = SpreadsheetApp.create(reportName);
        var reportSheet = spreadsheet.getActiveSheet();
        if (CONFIG.SCHEDULING_FOLDER_ID && CONFIG.SCHEDULING_FOLDER_ID.length > 0) {
            try { moveToFolder(spreadsheet.getId(), CONFIG.SCHEDULING_FOLDER_ID); }
            catch (fe) { Logger.log('ERROR moving to Scheduling folder: ' + fe.message); }
        }

        // ── Write header row ───────────────────────────────────────────────────
        var allReportHeaders = cols.map(function (c) { return c.label; });
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setNumberFormat('@');
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setValues([allReportHeaders]);
        reportSheet.getRange(1, 1, 1, allReportHeaders.length)
            .setFontWeight('bold')
            .setBackground('#4a86e8')
            .setFontColor('white');

        // ── Build and write data rows ──────────────────────────────────────────
        var dataRows = records.map(function (rec) {
            return _buildSchedRow_(rec, cols, colIndexMap);
        });
        if (dataRows.length > 0) {
            reportSheet.getRange(2, 1, dataRows.length, allReportHeaders.length).setValues(dataRows);
        }

        // ── Apply column widths and wrap ───────────────────────────────────────
        applyReportColumnFormatting(reportSheet, cols, 2, dataRows.length + 1);

        // ── Find first/last name column positions (1-based) ────────────────────
        var fnLabel = getReportHeader_('First Name') || 'First Name';
        var lnLabel = getReportHeader_('Last Name') || 'Last Name';
        var fnColNum = allReportHeaders.indexOf(fnLabel) + 1;
        var lnColNum = allReportHeaders.indexOf(lnLabel) + 1;

        // ── Highlight rows and name cells ──────────────────────────────────────
        for (var pi = 0; pi < records.length; pi++) {
            var rec = records[pi];
            var sheetRow = pi + 2; // 1-based, row 1 = header
            var isNewRecord = (rec.rowId >= startId && rec.rowId <= endId);

            if (isNewRecord) {
                // Pink background + bold for entire row
                reportSheet.getRange(sheetRow, 1, 1, allReportHeaders.length)
                    .setBackground('#FFB6C1')
                    .setFontWeight('bold');
            }

            // Yellow name cells for ALL rows
            if (fnColNum > 0) reportSheet.getRange(sheetRow, fnColNum).setBackground('#FFFF00');
            if (lnColNum > 0) reportSheet.getRange(sheetRow, lnColNum).setBackground('#FFFF00');
        }

        // ── Data validation dropdowns ──────────────────────────────────────────
        var schedCodeLabel = getReportHeader_('Scheduled Distribution Code') || 'Scheduled Distribution Code';
        var svcStatusLabel = getReportHeader_('Service Status') || 'Service Status';
        var schedCodeColNum = allReportHeaders.indexOf(schedCodeLabel) + 1;
        var svcStatusColNum = allReportHeaders.indexOf(svcStatusLabel) + 1;

        if (dataRows.length > 0) {
            // Scheduled Distribution Code — active codes from LU_SchedDisbCodes (alphabetical)
            var distribCodes = getAllDistribCodes_();
            if (schedCodeColNum > 0 && distribCodes.length > 0) {
                var schedRule = SpreadsheetApp.newDataValidation()
                    .requireValueInList(distribCodes, true)
                    .setAllowInvalid(true)
                    .build();
                reportSheet.getRange(2, schedCodeColNum, dataRows.length, 1).setDataValidation(schedRule);
            }

            // Service Status — active values from LU_ServiceStatus (not hardcoded)
            var statusValues = getLookupValues('SERVICE_STATUS', 'Status');
            if (!statusValues || statusValues.length === 0) {
                statusValues = ['Picked Up', 'Delivered', 'Cancelled', 'Open', 'Pending'];
            }
            if (svcStatusColNum > 0) {
                var statusRule = SpreadsheetApp.newDataValidation()
                    .requireValueInList(statusValues, true)
                    .setAllowInvalid(true)
                    .build();
                reportSheet.getRange(2, svcStatusColNum, dataRows.length, 1).setDataValidation(statusRule);
            }
        }

        // ── Freeze header row and set wrap ─────────────────────────────────────
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setWrap(true);
        reportSheet.setFrozenRows(1);

        // ── Append ID range to LU_SchedID ─────────────────────────────────────
        appendSchedId(startId, endId,
            Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yyyy HH:mm'));

        // ── Highlight endId row in AM magenta ─────────────────────────────────
        if (endIdRowNum > 0) {
            try {
                sheet.getRange(endIdRowNum, 1, 1, sheet.getLastColumn())
                    .setBackground('#FF00FF');
            } catch (me) {
                Logger.log('generateSchedulingReport: AM magenta highlight error: ' + me.message);
            }
        }

        logAudit('REPORT', null,
            'Generated Scheduling Report — ' + records.length + ' records' +
            ' (new records: ID ' + startId + ' to ' + endId + ')');

        return {
            success: true,
            message: 'Scheduling Report generated with ' + records.length + ' records',
            reportUrl: spreadsheet.getUrl(),
            downloadUrl: 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx',
            reportId: spreadsheet.getId(),
            recordCount: records.length,
            startId: startId,
            endId: endId
        };

    } catch (e) {
        Logger.log('generateSchedulingReport error: ' + e.message);
        return { success: false, error: 'Report generation failed: ' + e.message };
    }
}

/**
 * Creates the Distribution Report spreadsheet with formatted headers and data.
 *
 * v5.12: Baby Box and Extra Box are optional columns inserted after Date Picked Up.
 * v5.21: Data rows now written as a single batch setValues() call instead of
 *        per-row setValue() loops. Eliminates 800+ individual API calls per
 *        report generation; fixes GAS 6-minute execution timeout.
 *
 * @param {string}   distribCode  - Distribution code
 * @param {Object[]} records      - Filtered/sorted record objects
 * @param {boolean}  hasBabyBox   - Whether to include Baby Box column
 * @param {boolean}  hasExtraBox  - Whether to include Extra Box column (v5.12)
 * @param {string}   startDate    - Report start date string
 * @param {string}   endDate      - Report end date string
 * @param {string}   pickupTimes  - Pickup times text
 * @returns {GoogleAppsScript.Spreadsheet.Spreadsheet} The created spreadsheet
 */
function createDistributionReportSpreadsheet(distribCode, records, hasBabyBox, hasExtraBox,
    startDate, endDate, pickupTimes) {
    const reportName = 'Distribution_' + distribCode + '_' +
        Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd');
    const spreadsheet = SpreadsheetApp.create(reportName);
    const sheet = spreadsheet.getActiveSheet();

    // Move to Distribution folder
    const folderId = CONFIG.DISTRIBUTION_FOLDER_ID;
    if (folderId && folderId.length > 0) {
        try {
            moveToFolder(spreadsheet.getId(), folderId);
        } catch (folderError) {
            Logger.log('ERROR moving to Distribution folder: ' + folderError.message);
        }
    } else {
        Logger.log('WARNING: DISTRIBUTION_FOLDER_ID not configured');
    }

    let currentRow = 1;

    // ── Header section (few cells — individual writes are fine here) ──────────
    sheet.getRange(currentRow, 1).setValue('Total Recipients: ' + records.length)
        .setFontWeight('bold');
    currentRow++;

    sheet.getRange(currentRow, 1).setValue('Giving to the Nations')
        .setFontWeight('bold').setFontSize(14);
    currentRow++;

    sheet.getRange(currentRow, 1).setValue('Cleaning Box Distribution')
        .setFontWeight('bold').setFontSize(12);
    currentRow++;

    sheet.getRange(currentRow, 1).setValue('Available Dates: ' + startDate + '-' + endDate);
    currentRow++;

    if (pickupTimes) {
        sheet.getRange(currentRow, 1).setValue('Available Pick Up Times: ' + pickupTimes);
        currentRow++;
    }

    sheet.getRange(currentRow, 1).setValue(
        'Created: ' + Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yy'));
    currentRow++;

    currentRow++; // blank row before data header

    // ── Data header row ───────────────────────────────────────────────────────
    const headerRow = currentRow;

    // v5.12: Baby Box and Extra Box are optional columns
    const dataHeaders = ['Date Picked Up'];
    if (hasBabyBox) dataHeaders.push('Baby Box');
    if (hasExtraBox) dataHeaders.push('Extra Box');
    dataHeaders.push(
        'First Name', 'Last Name',
        getReportHeader_('Street Address'),
        getReportHeader_('Apartment #, Upper, Lower, or Lot #'),
        'City', 'Phone', 'Distribution Code', 'Submission #'
    );

    const numCols = dataHeaders.length;
    sheet.getRange(headerRow, 1, 1, numCols).setValues([dataHeaders]);
    styleReportHeader(sheet, headerRow, numCols);
    currentRow++;

    // ── Data rows — SINGLE batch write (v5.21) ────────────────────────────────
    // Build entire 2D array in memory first, then write in one setValues() call.
    // Previous implementation used ~11 setValue() calls per record; for 80 records
    // that was 880+ API calls. This version uses exactly 1 regardless of row count.
    if (records.length > 0) {
        const dataRows = records.map(function (record) {
            const row = [
                ''    // Date Picked Up — blank for staff to fill in
            ];
            if (hasBabyBox) row.push(record.babyBox || '');
            if (hasExtraBox) row.push(record.extraBox || '');
            row.push(
                record.firstName || '',
                record.lastName || '',
                record.address1 || '',
                record.address2 || '',
                record.city || '',
                formatPhoneNumber_(record.phone),
                record.distribCode || '',
                record.submissionId || ''
            );
            return row;
        });

        // Single API call for all data
        sheet.getRange(currentRow, 1, dataRows.length, numCols).setValues(dataRows);

        // Border around entire data range
        sheet.getRange(headerRow + 1, 1, records.length, numCols)
            .setBorder(true, true, true, true, true, true);

        // Alternating row shading — one setBackground() per other-row (not per cell)
        for (let i = 1; i < records.length; i += 2) {
            sheet.getRange(headerRow + 1 + i, 1, 1, numCols).setBackground('#f3f3f3');
        }

        currentRow += records.length;
    }

    // ── Footer notes ──────────────────────────────────────────────────────────
    const footerLines = [];
    if (hasBabyBox) {
        footerLines.push(
            "Note: Recipients with \"X\" in the 'Baby Box' column will receive 2 boxes, " +
            "1 large box and 1 small box. All others receive 1 large box.");
    }
    if (hasExtraBox) {
        footerLines.push(
            "Note: Recipients with \"X\" in the 'Extra Box' column will receive a special " +
            "box in addition to the Scheduled Box.");
    }
    for (const line of footerLines) {
        currentRow++;
        sheet.getRange(currentRow, 1).setValue(line);
        sheet.getRange(currentRow, 1, 1, numCols).merge();
        sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(10).setWrap(true);
    }

    // ── Column widths ─────────────────────────────────────────────────────────
    // Built dynamically to match actual column order:
    // [Date Picked Up, (Baby Box?), (Extra Box?), First Name, Last Name,
    //  Addr1, Addr2, City, Phone, Distrib Code, Submission #]
    const widths = [100];
    if (hasBabyBox) widths.push(70);
    if (hasExtraBox) widths.push(70);
    widths.push(100, 100, 180, 80, 120, 120, 100, 90);
    for (let c = 0; c < widths.length; c++) {
        sheet.setColumnWidth(c + 1, widths[c]);
    }

    return spreadsheet;
}

/**
 * Preview of distribution report â€” returns record count without generating report
 * @param {string} distribCode - Distribution code to preview
 * @returns {Object} { success, count }
 */
function previewDistributionReport(distribCode) {
  try {
  if (!distribCode) {
    return { success: false, error: 'Distribution Code is required' };
  }
  
  // Force uppercase
  distribCode = distribCode.toString().toUpperCase();
  
  const sheet = getMasterSheet();
  if (!sheet) {
    return { success: false, error: 'Master sheet not found' };
  }
  
  const data = sheet.getDataRange().getValues();
  const headers = trimHeaders(data[0]);
  
  const colIndices = {
    firstName:        headers.indexOf(resolveAMField_('First Name')),
    lastName:         headers.indexOf(resolveAMField_('Last Name')),
    city:             headers.indexOf(resolveAMField_('City')),
    schedDistribCode: headers.indexOf(resolveAMField_('Scheduled Distribution Code')),
    babyBox:          headers.indexOf(resolveAMField_('Take Baby Box?'))
  };
  
  let recordCount = 0;
  let hasBabyBox = false;
  
  for (let i = 1; i < data.length; i++) {
    const rowDistribCode = data[i][colIndices.schedDistribCode];
    if (rowDistribCode && rowDistribCode.toString().toUpperCase() === distribCode) {
      recordCount++;
      if (data[i][colIndices.babyBox] === 'X' || data[i][colIndices.babyBox] === 'x') {
        hasBabyBox = true;
      }
    }
  }
  
  const distribInfo = getDistributionInfo(distribCode);
  
  return {
    success: true,
    distribCode: distribCode,
    recordCount: recordCount,
    hasBabyBox: hasBabyBox,
    startDate: distribInfo.startDate,
    endDate: distribInfo.endDate,
    interval: distribInfo.interval
  };
  } catch (e) {
    Logger.log('previewDistributionReport error: ' + e.message);
    return { success: false, error: 'Preview failed: ' + e.message };
  }
}

/**
 * Generate Distribution Report Google Sheet for a given Scheduled Distribution Code.
 *
 * Reads all Applicants_Master rows where Scheduled Distribution Code matches
 * distribCode, builds a record object per row, sorts by Last Name / First Name,
 * and calls createDistributionReportSpreadsheet() to write the sheet.
 *
 * hasBabyBox  — true if ANY matched row has Take Baby Box? = 'X'
 * hasExtraBox — true if ANY matched row has a non-blank Scheduled Box Code 3
 *
 * Called from AP: generateDistributionReport(distribCode, startDate, endDate, pickupTimes)
 *
 * v5.22 — Restored (was missing from file; caused indefinite hang on generate)
 *
 * @param {string} distribCode  - Scheduled Distribution Code (will be uppercased)
 * @param {string} startDate    - Display start date string (e.g. '3/15/26')
 * @param {string} endDate      - Display end date string (e.g. '3/29/26')
 * @param {string} pickupTimes  - Pickup times text for report header
 * @returns {{ success, recordCount, reportUrl, downloadUrl, reportId }}
 */
function generateDistributionReport(distribCode, startDate, endDate, pickupTimes) {
    try {
        if (!distribCode) {
            return { success: false, error: 'Distribution Code is required' };
        }

        distribCode = distribCode.toString().toUpperCase();

        var sheet = getMasterSheet();
        if (!sheet) {
            return { success: false, error: 'Master sheet not found' };
        }

        var data = sheet.getDataRange().getValues();
        var headers = trimHeaders(data[0]);

        var colIdx = {
            id: headers.indexOf(resolveAMField_('ID')),
            firstName: headers.indexOf(resolveAMField_('First Name')),
            lastName: headers.indexOf(resolveAMField_('Last Name')),
            address1: headers.indexOf(resolveAMField_('Street Address')),
            address2: headers.indexOf(resolveAMField_('Apartment #, Upper, Lower, or Lot #')),
            city: headers.indexOf(resolveAMField_('City')),
            phone: headers.indexOf(resolveAMField_('Phone Number')),
            schedDistribCode: headers.indexOf(resolveAMField_('Scheduled Distribution Code')),
            babyBox: headers.indexOf(resolveAMField_('Take Baby Box?')),
            schedBoxCode3: headers.indexOf(resolveAMField_('Scheduled Box Code 3'))

        };

        if (colIdx.schedDistribCode === -1) {
            return { success: false, error: 'Scheduled Distribution Code column not found in Applicants_Master' };
        }

        var filteredRecords = [];
        var hasBabyBox = false;
        var hasExtraBox = false;

        for (var i = 1; i < data.length; i++) {
            var rowCode = (data[i][colIdx.schedDistribCode] || '').toString().toUpperCase();
            if (rowCode !== distribCode) continue;

            var babyVal = colIdx.babyBox !== -1 ? (data[i][colIdx.babyBox] || '').toString().trim().toUpperCase() : '';
            var box3Val = colIdx.schedBoxCode3 !== -1 ? (data[i][colIdx.schedBoxCode3] || '').toString().trim() : '';

            if (babyVal === 'X') hasBabyBox = true;
            if (box3Val !== '') hasExtraBox = true;

            filteredRecords.push({
                submissionId: colIdx.id !== -1 ? (data[i][colIdx.id] || '').toString().trim() : '',
                firstName: colIdx.firstName !== -1 ? (data[i][colIdx.firstName] || '').toString().trim() : '',
                lastName: colIdx.lastName !== -1 ? (data[i][colIdx.lastName] || '').toString().trim() : '',
                address1: colIdx.address1 !== -1 ? (data[i][colIdx.address1] || '').toString().trim() : '',
                address2: colIdx.address2 !== -1 ? (data[i][colIdx.address2] || '').toString().trim() : '',
                city: colIdx.city !== -1 ? (data[i][colIdx.city] || '').toString().trim() : '',
                phone: colIdx.phone !== -1 ? (data[i][colIdx.phone] || '').toString().trim() : '',
                distribCode: distribCode,
                babyBox: babyVal === 'X' ? 'X' : '',
                extraBox: box3Val !== '' ? 'X' : ''
            });
        }

        if (filteredRecords.length === 0) {
            return { success: false, error: 'No records found for distribution code: ' + distribCode };
        }

        // Sort: Last Name, First Name
        filteredRecords.sort(function (a, b) {
            var lc = a.lastName.localeCompare(b.lastName);
            return lc !== 0 ? lc : a.firstName.localeCompare(b.firstName);
        });

        // createDistributionReportSpreadsheet() uses single batch setValues() (v5.21)
        var spreadsheet = createDistributionReportSpreadsheet(
            distribCode, filteredRecords, hasBabyBox, hasExtraBox,
            startDate, endDate, pickupTimes
        );

        logAudit('REPORT', null,
            'Generated Distribution Report for ' + distribCode + ': ' + filteredRecords.length + ' records');

        return {
            success: true,
            recordCount: filteredRecords.length,
            reportUrl: spreadsheet.getUrl(),
            downloadUrl: 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx',
            reportId: spreadsheet.getId()
        };

    } catch (e) {
        Logger.log('generateDistributionReport error: ' + e.message);
        return { success: false, error: 'Report generation failed: ' + e.message };
    }
}

/**
 * Returns metadata about a distribution code (start date, interval, funding source)
 * @param {string} distribCode - The code to look up
 * @returns {Object} { success, info: { startDate, interval, fundingSource } }
 */
function getDistributionInfo(distribCode) {
  const sheet = getMasterSheet();
  if (!sheet) return { distribCode: distribCode, startDate: '', interval: '14', endDate: '' };
  
  distribCode = distribCode.toString().toUpperCase();
  
  const data = sheet.getDataRange().getValues();
  const headers = trimHeaders(data[0]);
  
  const codeCol      = headers.indexOf(resolveAMField_('Scheduled Distribution Code'));
  const startDateCol = headers.indexOf(resolveAMField_('Distribution Start Date'));
  const intervalCol  = headers.indexOf(resolveAMField_('Distribution Interval'));
  
  for (let i = 1; i < data.length; i++) {
    const code = data[i][codeCol];
    if (code && code.toString().toUpperCase() === distribCode) {
      const startDate = data[i][startDateCol];
      const interval = data[i][intervalCol];
      
      if (startDate) {
        let endDate = '';
        if (startDate && interval) {
          const start = new Date(startDate);
          const days = parseInt(interval) || 14;
          const end = new Date(start.getTime() + (days * 24 * 60 * 60 * 1000));
          endDate = Utilities.formatDate(end, CONFIG.TIMEZONE, 'M/d/yy');
        }
        
        return {
          distribCode: distribCode,
          startDate: startDate instanceof Date ? 
            Utilities.formatDate(startDate, CONFIG.TIMEZONE, 'M/d/yy') : startDate,
          interval: interval || '14',
          endDate: endDate
        };
      }
    }
  }
  
  return {
    distribCode: distribCode,
    startDate: '',
    interval: '14',
    endDate: ''
  };
}

/**
 * Returns the last (maximum) ID value in Applicants_Master
 * Used when Ending ID is omitted from Scheduling Report UI
 * @returns {number} The highest ID in AM, or 0 if not found
 */
function getLastAmId() {
  try {
    var sheet = getMasterSheet();
    if (!sheet) return 0;
    
    var data = sheet.getDataRange().getValues();
    var headers = trimHeaders(data[0]);
    var idCol = headers.indexOf(resolveAMField_('ID'));
    if (idCol === -1) return 0;
    for (var i = 1; i < data.length; i++) {
      var id = parseInt(data[i][idCol]);
      if (!isNaN(id) && id > maxId) {
        maxId = id;
      }
    }
    return maxId;
  } catch (e) {
    Logger.log('getLastAmId error: ' + e.message);
    return 0;
  }
}

/**
 * Generates a Scheduling Report spreadsheet for ALL records in Applicants_Master.
 * Combines male/female age brackets, sorts by Last Name / First Name / ID,
 * marks History per-name (highest ID = current; all lower IDs = History).
 * Phase 4 enhancements retained:
 *   - History column (first column)
 *   - Data validation dropdowns for Sched Distrib Code and Service Status
 *   - Sorts by Last Name, First Name, ID
 *   - Column headers use LU_FieldMap Report Header values via getReportHeader_()
 * v5.19 - No ID range params. All AM rows included. Date filter removed.
 *          Learned How column added after Referrer Title.
 *          How Products Help confirmed after Current Situation.
 *          Assistance column wrapped. colWidths updated (45 cols).
 * @returns {Object} { success, reportUrl, reportId, recordCount }
 */
/**
 * Generate Scheduling Report for ALL Applicants_Master records.
 *
 * Report includes every active AM record — no ID range, no date filter,
 * no archive workbooks. Columns are driven by LU_ReportColumns 'Scheduling'.
 *
 * HISTORY FLAG:
 *   For each unique first+last name, the highest ID = current record.
 *   All lower IDs for the same name are marked 'History' in the first column.
 *
 * DATA VALIDATION:
 *   Scheduled Distribution Code column gets a dropdown from LU_DistribCodes.
 *   Service Status column gets a dropdown with standard status values.
 *
 * SORT ORDER: Last Name, First Name, ID (ascending).
 *
 * COLUMN HEADERS: Resolved via LU_FieldMap Report Headers (getReportHeader_()).
 *   processSchedulingReport() also uses getReportHeader_() when reading the
 *   report back, so both functions stay in sync automatically.
 *
 * v5.19 — All AM rows included; Learned How column added; no ID range params
 * v5.20 — Column definitions from LU_ReportColumns 'Scheduling' via
 *          ReportColumnService. _buildSchedRow_() replaces inline row builder.
 *          applyReportColumnFormatting() replaces hardcoded colWidths array.
 *          Falls back to v5.19 hardcoded column set when LU_ReportColumns empty.
 *
 * @returns {{ success, reportUrl, downloadUrl, reportId, recordCount }}
 */

/**
 * v5.23 - Added lastRecordId parameter. Non-History rows with rowId > lastRecordId
 *         are highlighted pink (#FFB6C1) after data rows are written.
 * @param {number} [lastRecordId=0] - Highest ID from the previous scheduling report.
 */
function generateSchedulingReport(lastRecordId) {
    var newRecordThreshold = parseInt(lastRecordId) || 0;
    try {
        var sheet = getMasterSheet();
        if (!sheet) return { success: false, error: 'Master sheet not found' };

        var data = sheet.getDataRange().getValues();
        var headers = trimHeaders(data[0]);
        var idCol = headers.indexOf(resolveAMField_('ID'));
        var fnCol = headers.indexOf(resolveAMField_('First Name'));
        var lnCol = headers.indexOf(resolveAMField_('Last Name'));
        if (idCol === -1) return { success: false, error: 'ID column not found in Applicants_Master' };

        // ── Get column definitions from LU_ReportColumns (with v5.19 fallback) ──
        var cols = getReportColumns('Scheduling');
        if (cols.length === 0) {
            Logger.log('generateSchedulingReport: LU_ReportColumns empty — using v5.19 fallback');
            cols = _getSchedFallbackCols_();
        }

        // ── Build colIndexMap: maps each column key → AM column index ──────────
        // 'History' skipped (not an AM column).
        // [Calc] age bracket keys: index their constituent M/F columns instead.
        // All other keys: look up the resolved raw AM header in the headers array.
        var colIndexMap = {};
        cols.forEach(function (col) {
            var key = col.key;
            if (key === 'History') return;          // handled as special case in _buildSchedRow_

            var bracketPair = SCHED_AGE_BRACKET_MAP_[key];
            if (bracketPair) {
                // Index both constituent columns so _buildSchedRow_ can sum them
                var mi = headers.indexOf(resolveAMField_(bracketPair.male));
                var fi = headers.indexOf(resolveAMField_(bracketPair.female));
                if (mi !== -1) colIndexMap[bracketPair.male] = mi;
                if (fi !== -1) colIndexMap[bracketPair.female] = fi;
                return;
            }

            var idx = headers.indexOf(resolveAMField_(key));
            if (idx !== -1) colIndexMap[key] = idx;
        });

        // ── Collect records (all AM rows, skip blank name rows) ────────────────
        var records = [];
        for (var i = 1; i < data.length; i++) {
            var fn = (data[i][fnCol] || '').toString().trim();
            var ln = (data[i][lnCol] || '').toString().trim();
            if (!fn && !ln) continue;
            records.push({
                rowData: data[i],
                lastName: ln,
                firstName: fn,
                rowId: parseInt(data[i][idCol]) || 0,
                isHistory: false                          // set below
            });
        }
        if (records.length === 0)
            return { success: false, error: 'No records found in Applicants_Master' };

        // ── Sort: Last Name, First Name, ID ───────────────────────────────────
        records.sort(function (a, b) {
            var lc = a.lastName.localeCompare(b.lastName); if (lc) return lc;
            var fc = a.firstName.localeCompare(b.firstName); if (fc) return fc;
            return a.rowId - b.rowId;
        });

        // ── History flag: per-name, highest ID = current ──────────────────────
        var maxIdPerName = {};
        records.forEach(function (rec) {
            var k = rec.firstName.toLowerCase() + '|' + rec.lastName.toLowerCase();
            maxIdPerName[k] = Math.max(maxIdPerName[k] || 0, rec.rowId);
        });
        records.forEach(function (rec) {
            var k = rec.firstName.toLowerCase() + '|' + rec.lastName.toLowerCase();
            rec.isHistory = rec.rowId < (maxIdPerName[k] || rec.rowId);
        });

        // ── Create spreadsheet ─────────────────────────────────────────────────
        var reportName = 'Scheduling_' + Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd');
        var spreadsheet = SpreadsheetApp.create(reportName);
        var reportSheet = spreadsheet.getActiveSheet();
        if (CONFIG.SCHEDULING_FOLDER_ID && CONFIG.SCHEDULING_FOLDER_ID.length > 0) {
            try { moveToFolder(spreadsheet.getId(), CONFIG.SCHEDULING_FOLDER_ID); }
            catch (fe) { Logger.log('ERROR moving to Scheduling folder: ' + fe.message); }
        }

        // ── Write header row ───────────────────────────────────────────────────
        // Force plain text (@) so values like '5-9' are not interpreted as dates
        var allReportHeaders = cols.map(function (c) { return c.label; });
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setNumberFormat('@');
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setValues([allReportHeaders]);
        reportSheet.getRange(1, 1, 1, allReportHeaders.length)
            .setFontWeight('bold')
            .setBackground('#4a86e8')
            .setFontColor('white');

        // ── Build and write data rows ──────────────────────────────────────────
        var dataRows = records.map(function (rec) {
            return _buildSchedRow_(rec, cols, colIndexMap);
        });
        if (dataRows.length > 0) {
            reportSheet.getRange(2, 1, dataRows.length, allReportHeaders.length).setValues(dataRows);
        }

        // ── Apply column widths and wrap from LU_ReportColumns ─────────────────
        // applyReportColumnFormatting() replaces the v5.19 hardcoded colWidths array
        applyReportColumnFormatting(reportSheet, cols, 2, dataRows.length + 1);

        // ── Data validation dropdowns ──────────────────────────────────────────
        // Find column numbers by matching the resolved header labels
        var schedCodeLabel = getReportHeader_('Scheduled Distribution Code') || 'Scheduled Distribution Code';
        var svcStatusLabel = getReportHeader_('Service Status') || 'Service Status';
        var schedCodeColNum = allReportHeaders.indexOf(schedCodeLabel) + 1;   // 1-based
        var svcStatusColNum = allReportHeaders.indexOf(svcStatusLabel) + 1;

        if (dataRows.length > 0) {
            // Scheduled Distribution Code dropdown from LU_DistribCodes
            var distribCodes = getAllDistribCodes_();
            if (schedCodeColNum > 0 && distribCodes.length > 0) {
                var schedRule = SpreadsheetApp.newDataValidation()
                    .requireValueInList(distribCodes, true)
                    .setAllowInvalid(true)
                    .build();
                reportSheet.getRange(2, schedCodeColNum, dataRows.length, 1).setDataValidation(schedRule);
            }

            // Service Status dropdown
            var statusValues = ['Picked Up', 'Delivered', 'Cancelled', 'Open', 'Pending'];
            if (svcStatusColNum > 0) {
                var statusRule = SpreadsheetApp.newDataValidation()
                    .requireValueInList(statusValues, true)
                    .setAllowInvalid(true)
                    .build();
                reportSheet.getRange(2, svcStatusColNum, dataRows.length, 1).setDataValidation(statusRule);
            }
        }

        // ── Freeze header row and set wrap ─────────────────────────────────────
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setWrap(true);
        reportSheet.setFrozenRows(1);

        logAudit('REPORT', null, 'Generated Scheduling Report — ' + records.length + ' records');

        return {
            success: true,
            message: 'Scheduling Report generated with ' + records.length + ' records',
            reportUrl: spreadsheet.getUrl(),
            downloadUrl: 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx',
            reportId: spreadsheet.getId(),
            recordCount: records.length
        };

    } catch (e) {
        Logger.log('generateSchedulingReport error: ' + e.message);
        return { success: false, error: 'Report generation failed: ' + e.message };
    }
}
function processSchedulingReport() {
  try {
    var folderId = CONFIG.SCHEDULING_FOLDER_ID;
    if (!folderId) {
      return { success: false, error: 'Scheduling folder not configured' };
    }
    
    // Find the latest scheduling report
    var folder = DriveApp.getFolderById(folderId);
    var files = folder.getFiles();
    var latestFile = null;
    var latestDate = new Date(0);
    
    while (files.hasNext()) {
      var file = files.next();
      if (file.getMimeType() === MimeType.GOOGLE_SHEETS && file.getName().indexOf('Scheduling_') === 0) {
        var updated = file.getLastUpdated();
        if (updated > latestDate) {
          latestDate = updated;
          latestFile = file;
        }
      }
    }
    
    if (!latestFile) {
      return { success: false, error: 'No Scheduling report found in folder' };
    }
    
    // Read the report
    var reportSS = SpreadsheetApp.openById(latestFile.getId());
    var reportSheet = reportSS.getActiveSheet();
    var reportData = reportSheet.getDataRange().getValues();
    
    if (reportData.length < 2) {
      return { success: false, error: 'Report is empty' };
    }
    
    var rptHeaders = trimHeaders(reportData[0]);
    var historyCol = rptHeaders.indexOf('History');
    var rptIdCol        = rptHeaders.indexOf(getReportHeader_('ID'));
    var rptSchedCodeCol = rptHeaders.indexOf(getReportHeader_('Scheduled Distribution Code'));
    var rptStatusCol = rptHeaders.indexOf(getReportHeader_('Service Status'));
    var rptAdminNotesCol = rptHeaders.indexOf(getReportHeader_('Admin Notes'))
    
    if (rptIdCol === -1) {
      return { success: false, error: 'Report missing ID column' };
    }
    
    // Read master sheet
    var masterSheet = getMasterSheet();
    if (!masterSheet) {
      return { success: false, error: 'Master sheet not found' };
    }
    
    var masterData = masterSheet.getDataRange().getValues();
    var masterHeaders = trimHeaders(masterData[0]);
    var amIdCol               = masterHeaders.indexOf(resolveAMField_('ID'));
    var amSchedCodeCol        = masterHeaders.indexOf(resolveAMField_('Scheduled Distribution Code'));
    var amStatusCol           = masterHeaders.indexOf(resolveAMField_('Service Status'));
    var amDistribStartDateCol = masterHeaders.indexOf(resolveAMField_('Distribution Start Date'));
    var amDistribIntervalCol  = masterHeaders.indexOf(resolveAMField_('Distribution Interval'));
    var amFundingSourceCol    = masterHeaders.indexOf(resolveAMField_('Funding Source'));
    var amGenericDistribCodeCol = masterHeaders.indexOf(resolveAMField_('Generic Distribution Code'));
    var amLastDateServedCol   = masterHeaders.indexOf(resolveAMField_('Last Date Served'));
    var amFinalServiceContactCol = masterHeaders.indexOf(resolveAMField_('Final Service Contact Date'));
    var amNextServiceAvailCol = masterHeaders.indexOf(resolveAMField_('Next Service Availability Date'));
    
    if (amIdCol === -1 || amStatusCol === -1) {
      return { success: false, error: 'Required columns not found in Master sheet' };
    }
    
    // Build ID→row map for master sheet
    var idToRow = {};
    for (var m = 1; m < masterData.length; m++) {
      var id = masterData[m][amIdCol];
      if (id) idToRow[id.toString()] = m + 1;
    }
    
    // Cache for LU_SchedDisbCodes lookups (avoid repeated lookups for same code)
    var codeDetailsCache = {};
    
    var log = [];
    var updatedCount = 0;
    
    for (var i = 1; i < reportData.length; i++) {
      // Skip History rows
      if (historyCol !== -1) {
        var histVal = (reportData[i][historyCol] || '').toString().trim();
        if (histVal === 'History') continue;
      }
      
      var recId = (reportData[i][rptIdCol] || '').toString().trim();
      if (!recId) continue;
      
      var rowNum = idToRow[recId];
      if (!rowNum) {
        log.push({ status: 'error', message: 'ID ' + recId + ': Not found in Master' });
        continue;
      }
      
      var schedCode = rptSchedCodeCol !== -1 ? (reportData[i][rptSchedCodeCol] || '').toString().trim() : '';
      var rptStatus = rptStatusCol !== -1 ? (reportData[i][rptStatusCol] || '').toString().trim() : '';
      
      // If SchedDisbCode is present → look up in LU_SchedDisbCodes and update AM
      if (schedCode) {
        var upperCode = schedCode.toUpperCase();
        
        // Write SchedDisbCode to AM
        if (amSchedCodeCol !== -1) {
          masterSheet.getRange(rowNum, amSchedCodeCol + 1).setValue(upperCode);
        }
        
        // Set status to "Scheduled"
        masterSheet.getRange(rowNum, amStatusCol + 1).setValue('Scheduled');
        
        // Look up code details from LU_SchedDisbCodes (cached)
        if (!codeDetailsCache[upperCode]) {
          try {
            codeDetailsCache[upperCode] = getSchedDisbCodeDetails(upperCode);
          } catch (e) {
            codeDetailsCache[upperCode] = { success: false, error: e.message };
          }
        }
        
        var details = codeDetailsCache[upperCode];
        var logMsg = 'ID ' + recId + ': Scheduled (' + upperCode + ')';
        
        if (details && details.success) {
          // Distribution Start Date = StartDate from LU_SchedDisbCodes
          if (amDistribStartDateCol !== -1 && details.startDate) {
            masterSheet.getRange(rowNum, amDistribStartDateCol + 1).setValue(details.startDate);
            logMsg += ', Distrib Start: ' + details.startDate;
          }
          
          // Distribution Interval from LU_SchedDisbCodes
          if (amDistribIntervalCol !== -1 && details.interval) {
            masterSheet.getRange(rowNum, amDistribIntervalCol + 1).setValue(details.interval);
            logMsg += ', Interval: ' + details.interval;
          }
          
          // Funding Source from LU_SchedDisbCodes
          if (amFundingSourceCol !== -1 && details.fundingCode) {
            masterSheet.getRange(rowNum, amFundingSourceCol + 1).setValue(details.fundingCode);
            logMsg += ', Funding: ' + details.fundingCode;
          }
          
          // Generic Distribution Code = alpha prefix of SchedDisbCode (chars before first digit)
          if (amGenericDistribCodeCol !== -1) {
            var genericMatch = upperCode.match(/^([A-Z]+)/i);
            var genericCode = genericMatch ? genericMatch[1] : upperCode.replace(/[0-9\-]/g, '');
            if (genericCode) {
              masterSheet.getRange(rowNum, amGenericDistribCodeCol + 1).setValue(genericCode);
              logMsg += ', Generic: ' + genericCode;
            }
          }
        } else {
          logMsg += ' (lookup failed: ' + (details ? details.error : 'unknown') + ')';
        }
        
        log.push({ status: 'success', message: logMsg });
        updatedCount++;
      } else if (rptStatus) {
        // If status was edited in report, write it back
        masterSheet.getRange(rowNum, amStatusCol + 1).setValue(rptStatus);
        
        // Update date fields with current date
        var today = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yyyy');
        var logMsg = 'ID ' + recId + ': Status → ' + rptStatus;
        
        if (amLastDateServedCol !== -1) {
          masterSheet.getRange(rowNum, amLastDateServedCol + 1).setValue(today);
          logMsg += ', Last Date Served: ' + today;
        }
        if (amFinalServiceContactCol !== -1) {
          masterSheet.getRange(rowNum, amFinalServiceContactCol + 1).setValue(today);
          logMsg += ', Final SCD: ' + today;
        }
        if (amNextServiceAvailCol !== -1) {
          var nextAvail = new Date();
          nextAvail.setDate(nextAvail.getDate() + 90);
          var nextAvailFormatted = Utilities.formatDate(nextAvail, CONFIG.TIMEZONE, 'M/d/yyyy');
          masterSheet.getRange(rowNum, amNextServiceAvailCol + 1).setValue(nextAvailFormatted);
          logMsg += ', Next Avail: ' + nextAvailFormatted;
        }
        
        log.push({ status: 'success', message: logMsg });
        updatedCount++;
      }
    }
    
    logAudit('PROCESS', null, 'Processed scheduling report: ' + latestFile.getName() + ', updated ' + updatedCount + ' records');
    
    return {
      success: true,
      updatedCount: updatedCount,
      reportName: latestFile.getName(),
      log: log
    };
    
  } catch (error) {
    Logger.log('processSchedulingReport error: ' + error.message);
    return { success: false, error: 'Processing failed: ' + error.message };
  }
}

/**
 * Finds a Distribution Report spreadsheet in the Distribution folder by code
 * @param {string} distribCode - Distribution code to search for
 * @returns {Object} { success, reportId, reportName }
 */
function findDistributionReportByCode(distribCode) {
  try {
    if (!distribCode) {
      return { success: false, error: 'Distribution Code is required' };
    }
    
    // Force uppercase
    distribCode = distribCode.toString().toUpperCase();
    
    const folderId = CONFIG.DISTRIBUTION_FOLDER_ID;
    if (!folderId) {
      return { success: false, error: 'Distribution folder not configured. Please check CONFIG.DISTRIBUTION_FOLDER_ID in Code.gs' };
    }
    
    let folder;
    try {
      folder = DriveApp.getFolderById(folderId);
    } catch (permError) {
      Logger.log('Permission error accessing folder: ' + permError.message);
      return { 
        success: false, 
        error: 'Cannot access Distribution folder. Please re-authorize the script: In Apps Script editor, go to Run menu > Run function > select any function, then approve permissions.' 
      };
    }
    
    const files = folder.getFiles();
    
    while (files.hasNext()) {
      const file = files.next();
      const fileName = file.getName().toUpperCase();
      
      if (fileName.indexOf(distribCode) > -1 && file.getMimeType() === MimeType.GOOGLE_SHEETS) {
        // Count records in the report
        let recordCount = 0;
        try {
          const reportSS = SpreadsheetApp.openById(file.getId());
          const reportSheet = reportSS.getActiveSheet();
          const reportData = reportSheet.getDataRange().getValues();
          
          // Find the header row (contains "Submission #")
          let headerRowIndex = -1;
          for (let i = 0; i < reportData.length; i++) {
            for (let j = 0; j < reportData[i].length; j++) {
              if ((reportData[i][j] || '').toString().trim() === 'Submission #') {
                headerRowIndex = i;
                break;
              }
            }
            if (headerRowIndex !== -1) break;
          }
          
          if (headerRowIndex !== -1) {
            for (let i = headerRowIndex + 1; i < reportData.length; i++) {
              let hasData = false;
              for (let j = 0; j < reportData[i].length; j++) {
                if (reportData[i][j] && reportData[i][j].toString().trim() !== '') {
                  hasData = true;
                  break;
                }
              }
              if (hasData) {
                recordCount++;
              } else {
                break;
              }
            }
          }
        } catch (e) {
          Logger.log('Error counting records: ' + e.message);
        }
        
        return {
          success: true,
          reportId: file.getId(),
          reportName: file.getName(),
          reportUrl: file.getUrl(),
          recordCount: recordCount
        };
      }
    }
    
    return { success: false, error: 'No report found containing "' + distribCode + '" in Distribution folder' };
    
  } catch (error) {
    Logger.log('Find report error: ' + error.message);
    if (error.message.indexOf('permission') > -1 || error.message.indexOf('authorization') > -1) {
      return { 
        success: false, 
        error: 'Authorization required. Please re-authorize the script in Apps Script editor.' 
      };
    }
    return { success: false, error: 'Search failed: ' + error.message };
  }
}

/**
 * Phase 4B.4: Returns distribution report data as structured rows for editable display
 * Reads the report spreadsheet and returns headers + data rows
 * @param {string} reportId - Google Sheets ID of the Distribution Report
 * @returns {Object} { success, headers: string[], rows: Object[], reportName }
 */
function getDistributionReportData(reportId) {
  try {
    if (!reportId) {
      return { success: false, error: 'Report ID is required' };
    }
    
    var reportSS = SpreadsheetApp.openById(reportId);
    var reportSheet = reportSS.getActiveSheet();
    var reportData = reportSheet.getDataRange().getValues();
    
    // Find header row (contains "Submission #")
    var headerRowIndex = -1;
    for (var i = 0; i < reportData.length; i++) {
      for (var j = 0; j < reportData[i].length; j++) {
        if ((reportData[i][j] || '').toString().trim() === 'Submission #') {
          headerRowIndex = i;
          break;
        }
      }
      if (headerRowIndex !== -1) break;
    }
    
    if (headerRowIndex === -1) {
      return { success: false, error: 'Could not find header row in report' };
    }
    
    var headers = [];
    for (var h = 0; h < reportData[headerRowIndex].length; h++) {
      headers.push((reportData[headerRowIndex][h] || '').toString().trim());
    }
    
    // Collect data rows
    var rows = [];
    for (var r = headerRowIndex + 1; r < reportData.length; r++) {
      var hasData = false;
      var rowObj = {};
      for (var c = 0; c < headers.length; c++) {
        var val = reportData[r][c];
        if (val instanceof Date) {
          val = Utilities.formatDate(val, CONFIG.TIMEZONE, 'M/d/yyyy');
        }
        var valStr = (val || '').toString().trim();
        if (valStr) hasData = true;
        rowObj[headers[c]] = valStr;
      }
      
      // Stop at first empty row (past data section) but skip footer
      if (!hasData) break;
      
      // Skip footer/note rows (check if Submission # is numeric)
      var subId = rowObj['Submission #'] || '';
      if (!subId || isNaN(parseInt(subId))) continue;
      
      rows.push(rowObj);
    }
    
    return {
      success: true,
      headers: headers,
      rows: rows,
      reportName: reportSS.getName()
    };
    
  } catch (error) {
    Logger.log('getDistributionReportData error: ' + error.message);
    return { success: false, error: 'Failed to read report: ' + error.message };
  }
}

/**
 * Phase 4B.5: Saves edited distribution report data back to the report spreadsheet.
 * v5.17 - Also accepts distribCode to sync AM Scheduled Box Code 3 when Extra Box
 *          is added (X → luBox3 written to AM) or removed (blank → AM field cleared).
 *          Extra Box column written back to report sheet.
 * @param {string} reportId   - Google Sheets ID of the Distribution Report
 * @param {Object[]} rows     - Array of row objects with edited data
 * @param {string} [distribCode] - Distribution code; used to look up luBox3
 * @returns {Object} { success, message }
 */
function saveDistributionReportEdits(reportId, rows, distribCode) {
  try {
    if (!reportId || !rows || rows.length === 0) {
      return { success: false, error: 'Report ID and data are required' };
    }

    var reportSS    = SpreadsheetApp.openById(reportId);
    var reportSheet = reportSS.getActiveSheet();
    var reportData  = reportSheet.getDataRange().getValues();

    // Find header row and column positions
    var headerRowIndex = -1;
    var datePickedUpCol = -1;
    var extraBoxCol     = -1;
    var submissionCol   = -1;
    for (var i = 0; i < reportData.length; i++) {
      for (var j = 0; j < reportData[i].length; j++) {
        var cellVal = (reportData[i][j] || '').toString().trim();
        if (cellVal === 'Submission #')  { headerRowIndex = i; submissionCol   = j; }
        if (cellVal === 'Date Picked Up') datePickedUpCol = j;
        if (cellVal === 'Extra Box')      extraBoxCol     = j;
      }
      if (headerRowIndex !== -1) break;
    }

    if (headerRowIndex === -1 || datePickedUpCol === -1) {
      return { success: false, error: 'Report structure not recognized' };
    }

    // Write back Date Picked Up (and Extra Box if present) to report sheet
    for (var r = 0; r < rows.length; r++) {
      var sheetRow = headerRowIndex + 2 + r; // 1-based + skip header row
      reportSheet.getRange(sheetRow, datePickedUpCol + 1).setValue(rows[r]['Date Picked Up'] || '');
      if (extraBoxCol !== -1) {
        var xVal = (rows[r]['Extra Box'] || '').toString().trim().toUpperCase();
        reportSheet.getRange(sheetRow, extraBoxCol + 1).setValue(xVal === 'X' ? 'X' : '');
      }
    }

    // v5.17: Sync AM Scheduled Box Code 3 when Extra Box was edited
    if (extraBoxCol !== -1 && submissionCol !== -1) {
      // Resolve luBox3 from LU_SchedDisbCodes (the DR/PF code to set when X is present)
      var luBox3 = '';
      if (distribCode) {
        try {
          var codeDetails = getSchedDisbCodeDetails(distribCode.toString().toUpperCase());
          if (codeDetails && codeDetails.success) luBox3 = codeDetails.box3 || '';
        } catch (e) {
          Logger.log('saveDistributionReportEdits: getSchedDisbCodeDetails error: ' + e.message);
        }
      }

      var masterSheet  = getMasterSheet();
      var masterData   = masterSheet.getDataRange().getValues();
      var masterHdrs   = trimHeaders(masterData[0]);
      var amIdCol      = masterHdrs.indexOf(resolveAMField_('ID'));
      var amBox3Col    = masterHdrs.indexOf(resolveAMField_('Scheduled Box Code 3'));

      if (amIdCol !== -1 && amBox3Col !== -1) {
        // Build ID → AM row index map
        var idToRow = {};
        for (var m = 1; m < masterData.length; m++) {
          var mid = (masterData[m][amIdCol] || '').toString().trim();
          if (mid) idToRow[mid] = m; // 0-based masterData index
        }

        // Collect all AM Scheduled Box Code 3 changes
        var amBox3Changes = []; // { rowNum (1-based sheet row), value }
        for (var r2 = 0; r2 < rows.length; r2++) {
          var rowId  = (rows[r2]['Submission #'] || '').toString().trim();
          var xVal2  = (rows[r2]['Extra Box'] || '').toString().trim().toUpperCase();
          var newBox3 = xVal2 === 'X' ? luBox3 : '';
          var amRow   = idToRow[rowId];
          if (amRow !== undefined) {
            var currentBox3 = (masterData[amRow][amBox3Col] || '').toString().trim();
            if (currentBox3 !== newBox3) {
              amBox3Changes.push({ rowNum: amRow + 1, value: newBox3 }); // 1-based
            }
          }
        }

        // Apply changes — individual setValues per changed row (typically few rows)
        for (var c = 0; c < amBox3Changes.length; c++) {
          masterSheet.getRange(amBox3Changes[c].rowNum, amBox3Col + 1).setValue(amBox3Changes[c].value);
        }
        Logger.log('saveDistributionReportEdits: updated AM Scheduled Box Code 3 for ' + amBox3Changes.length + ' row(s)');
      }
    }

    return { success: true, message: 'Report saved' };

  } catch (error) {
    Logger.log('saveDistributionReportEdits error: ' + error.message);
    return { success: false, error: 'Save failed: ' + error.message };
  }
}

/**
 * Processes a Distribution Report: updates Applicants_Master records
 * For each row:
 *   - Uses Date Picked Up as effective date; falls back to ending date from
 *     Available Dates header if Date Picked Up is blank
 *   - Updates Last Date Served, Final Service Contact Date, Next Service
 *     Availability (+90 days) using the effective date
 *   - Sets First Service Contact Date from LU_SchedDisbCodes StartDate (picked up only)
 *   - Writes Box 1 from LU_SchedDisbCodes → Box Code, Scheduled Box Code 1,
 *     Received Product Code 1
 *   - Writes Box 2 from LU_SchedDisbCodes → Scheduled Box Code 2,
 *     Received Product Code 2
 *   - Deactivates the SchedDisbCode in LU_SchedDisbCodes
 *   - Inserts stats row into Hygiene Box Distribution Stats workbook
 * @param {string} reportId - Google Sheets ID of the Distribution Report
 * @param {string} distribCode - The distribution code being processed
 * @returns {Object} { success, processedCount, skippedCount, log[] }
 */
function processDistributionSheet(reportId, distribCode) {
  try {
    if (!reportId) {
      return { success: false, error: 'Report ID is required' };
    }
    
    const reportSS = SpreadsheetApp.openById(reportId);
    const reportSheet = reportSS.getActiveSheet();
    const reportData = reportSheet.getDataRange().getValues();
    
    // Extract end date from Available Dates in header area
    let restockEndDate = '';
    for (let i = 0; i < Math.min(10, reportData.length); i++) {
      const cellValue = (reportData[i][0] || '').toString();
      if (cellValue.indexOf('Available Dates:') > -1) {
        const datesPart = cellValue.replace('Available Dates:', '').trim();
        const dateParts = datesPart.split('-');
        if (dateParts.length === 2) {
          restockEndDate = dateParts[1].trim();
          Logger.log('Found end date for Restock: ' + restockEndDate);
        }
        break;
      }
    }
    
    let headerRowIndex = -1;
    let submissionCol = -1;
    let datePickedUpCol = -1;
    let extraBoxCol = -1;  // v5.13: per-row Extra Box flag column
    
    for (let i = 0; i < reportData.length; i++) {
      for (let j = 0; j < reportData[i].length; j++) {
        const cellValue = (reportData[i][j] || '').toString().trim();
        if (cellValue === 'Submission #') {
          headerRowIndex = i;
          submissionCol = j;
        }
        if (cellValue === 'Date Picked Up') {
          datePickedUpCol = j;
        }
        if (cellValue === 'Extra Box') {
          extraBoxCol = j;  // v5.13
        }
      }
      if (headerRowIndex !== -1) break;
    }
    
    if (headerRowIndex === -1 || submissionCol === -1) {
      return { success: false, error: 'Could not find "Submission #" column in report' };
    }
    
    if (datePickedUpCol === -1) {
      return { success: false, error: 'Could not find "Date Picked Up" column in report' };
    }
    
    const masterSheet = getMasterSheet();
    if (!masterSheet) {
      return { success: false, error: 'Master sheet not found' };
    }
    
    const masterData = masterSheet.getDataRange().getValues();
    const masterHeaders = trimHeaders(masterData[0]);
    
    const idCol                    = masterHeaders.indexOf(resolveAMField_('ID'));
    const serviceStatusCol         = masterHeaders.indexOf(resolveAMField_('Service Status'));
    const lastDateServedCol        = masterHeaders.indexOf(resolveAMField_('Last Date Served'));
    const finalServiceContactDateCol = masterHeaders.indexOf(resolveAMField_('Final Service Contact Date'));
    const nextServiceAvailCol      = masterHeaders.indexOf(resolveAMField_('Next Service Availability Date'));
    const firstServiceContactDateCol = masterHeaders.indexOf(resolveAMField_('First Service Contact Date'));
    const boxCodeCol               = masterHeaders.indexOf(resolveAMField_('Box Code'));
    const receivedProductCode1Col  = masterHeaders.indexOf(resolveAMField_('Received Product Code 1'));
    const receivedProductCode2Col  = masterHeaders.indexOf(resolveAMField_('Received Product Code 2'));
    const receivedProductCode3Col  = masterHeaders.indexOf(resolveAMField_('Received Product Code 3'));
    const schedBoxCode1Col         = masterHeaders.indexOf(resolveAMField_('Scheduled Box Code 1'));
    const schedBoxCode2Col         = masterHeaders.indexOf(resolveAMField_('Scheduled Box Code 2'));
    const schedBoxCode3Col         = masterHeaders.indexOf(resolveAMField_('Scheduled Box Code 3'));
    const requestDateCol            = masterHeaders.indexOf(resolveAMField_('Request Date')); // v5.14
    
    if (idCol === -1 || serviceStatusCol === -1 || lastDateServedCol === -1) {
      return { success: false, error: 'Required columns not found in Master sheet' };
    }
    
    // Look up full code details from LU_SchedDisbCodes (StartDate, Box1, Box2, Box3)
    let scheduledStartDate = '';
    let luBox1 = '';
    let luBox2 = '';
    let luBox3 = '';
    if (distribCode) {
      distribCode = distribCode.toString().toUpperCase();
      try {
        var codeDetails = getSchedDisbCodeDetails(distribCode);
        if (codeDetails && codeDetails.success) {
          scheduledStartDate = codeDetails.startDate || '';
          luBox1 = codeDetails.box1 || '';
          luBox2 = codeDetails.box2 || '';
          luBox3 = codeDetails.box3 || '';
        }
        Logger.log('LU_SchedDisbCodes for ' + distribCode + ': Start=' + scheduledStartDate + ', Box1=' + luBox1 + ', Box2=' + luBox2 + ', Box3=' + luBox3);
      } catch (e) {
        Logger.log('Error getting code details: ' + e.message);
      }
    }
    
    const idToRow = {};
    for (let i = 1; i < masterData.length; i++) {
      const id = masterData[i][idCol];
      if (id) {
        idToRow[id.toString()] = i + 1;
      }
    }
    
    const log = [];
    let updatedCount = 0;
    const extraBoxProductUpdates = []; // v5.14: { id, requestDate } for DR/PF_Products QtyReceived copy
    
    for (let i = headerRowIndex + 1; i < reportData.length; i++) {
      const submissionId = reportData[i][submissionCol];
      
      if (!submissionId || submissionId.toString().trim() === '') {
        break;
      }
      
      const datePickedUp = reportData[i][datePickedUpCol];
      const hasDate = datePickedUp && datePickedUp.toString().trim() !== '';
      
      const rowNum = idToRow[submissionId.toString()];
      
      if (!rowNum) {
        log.push({ status: 'error', message: 'ID ' + submissionId + ': Not found in Master' });
        continue;
      }
      
      const newStatus = hasDate ? 'Picked Up' : 'Restock';
      
      // Determine the effective date: Date Picked Up if present, else ending date from Available Dates
      let effectiveDate = '';
      let effectiveDateObj = null;
      if (hasDate) {
        if (datePickedUp instanceof Date) {
          effectiveDate = Utilities.formatDate(datePickedUp, CONFIG.TIMEZONE, 'M/d/yyyy');
          effectiveDateObj = datePickedUp;
        } else {
          effectiveDate = datePickedUp.toString();
          effectiveDateObj = new Date(datePickedUp);
        }
      } else if (restockEndDate) {
        effectiveDate = restockEndDate;
        effectiveDateObj = new Date(restockEndDate);
      }
      
      // Set Service Status
      masterSheet.getRange(rowNum, serviceStatusCol + 1).setValue(newStatus);
      
      // Set date fields using the effective date (Picked Up date or ending date)
      if (effectiveDate) {
        // Last Date Served
        masterSheet.getRange(rowNum, lastDateServedCol + 1).setValue(effectiveDate);
        
        // Final Service Contact Date
        if (finalServiceContactDateCol !== -1) {
          masterSheet.getRange(rowNum, finalServiceContactDateCol + 1).setValue(effectiveDate);
        }
        
        // Next Service Availability Date = effective date + 90 days
        if (nextServiceAvailCol !== -1 && effectiveDateObj && !isNaN(effectiveDateObj.getTime())) {
          const nextAvail = new Date(effectiveDateObj.getTime() + (90 * 24 * 60 * 60 * 1000));
          const nextAvailFormatted = Utilities.formatDate(nextAvail, CONFIG.TIMEZONE, 'M/d/yyyy');
          masterSheet.getRange(rowNum, nextServiceAvailCol + 1).setValue(nextAvailFormatted);
        }
        
        // First Service Contact Date = Scheduled Start Date from LU_SchedDisbCodes
        if (hasDate && firstServiceContactDateCol !== -1 && scheduledStartDate) {
          masterSheet.getRange(rowNum, firstServiceContactDateCol + 1).setValue(scheduledStartDate);
        }
      }
      
      // Write Box 1 from LU_SchedDisbCodes → Box Code, Scheduled Box Code 1, Received Product Code 1
      if (luBox1) {
        if (boxCodeCol !== -1) {
          masterSheet.getRange(rowNum, boxCodeCol + 1).setValue(luBox1);
        }
        if (schedBoxCode1Col !== -1) {
          masterSheet.getRange(rowNum, schedBoxCode1Col + 1).setValue(luBox1);
        }
        if (receivedProductCode1Col !== -1) {
          masterSheet.getRange(rowNum, receivedProductCode1Col + 1).setValue(luBox1);
        }
      }
      
      // Write Box 2 from LU_SchedDisbCodes → Scheduled Box Code 2, Received Product Code 2
      if (luBox2) {
        if (schedBoxCode2Col !== -1) {
          masterSheet.getRange(rowNum, schedBoxCode2Col + 1).setValue(luBox2);
        }
        if (receivedProductCode2Col !== -1) {
          masterSheet.getRange(rowNum, receivedProductCode2Col + 1).setValue(luBox2);
        }
      }
      
      // v5.15: Box 3 is per-client (set individually when scheduled), not from LU_SchedDisbCodes.
      // Read AM Scheduled Box Code 3 directly; Extra Box = "X" in report confirms a value is present.
      const rowHasExtraBox = extraBoxCol !== -1
        ? (reportData[i][extraBoxCol] || '').toString().trim().toUpperCase() === 'X'
        : false;  // no Extra Box column → assume no Extra Box rows
      let amBox3Written = '';
      if (rowHasExtraBox) {
        // Read the per-client Scheduled Box Code 3 already in AM
        const amBox3 = schedBoxCode3Col !== -1
          ? (masterData[rowNum - 1][schedBoxCode3Col] || '').toString().trim()
          : (luBox3 || '');  // fallback to LU value if column missing
        const effectiveBox3 = amBox3 || luBox3;
        if (effectiveBox3) {
          // Scheduled Box Code 3 already has the value; write it to Received Product Code 3
          if (receivedProductCode3Col !== -1) {
            masterSheet.getRange(rowNum, receivedProductCode3Col + 1).setValue(effectiveBox3);
          }
          amBox3Written = effectiveBox3;
        }
      }
      
      // v5.15: Queue DR/PF_Products update if ANY scheduled box code in AM starts with DR or PF.
      // luBox1/luBox2 are distribution-wide (same for all rows); amBox3 is per-row.
      const amBox1Effective = luBox1 || (schedBoxCode1Col !== -1 ? (masterData[rowNum - 1][schedBoxCode1Col] || '').toString().trim() : '');
      const amBox2Effective = luBox2 || (schedBoxCode2Col !== -1 ? (masterData[rowNum - 1][schedBoxCode2Col] || '').toString().trim() : '');
      const isDRPF = function(v) { var u = v.toUpperCase(); return u.indexOf('DR') === 0 || u.indexOf('PF') === 0; };
      const needsProductUpdate = (amBox1Effective && isDRPF(amBox1Effective))
        || (amBox2Effective && isDRPF(amBox2Effective))
        || (amBox3Written && isDRPF(amBox3Written));
      if (needsProductUpdate && requestDateCol !== -1) {
        const rawReqDate = masterData[rowNum - 1][requestDateCol];
        const normReqDate = normalizeDate(rawReqDate);
        if (normReqDate) {
          extraBoxProductUpdates.push({ id: submissionId.toString(), requestDate: normReqDate });
        }
      }
      
      updatedCount++;
      let logMsg = 'ID ' + submissionId + ': Set to "' + newStatus + '"';
      if (effectiveDate) {
        logMsg += ', Date: ' + effectiveDate + (hasDate ? ' (picked up)' : ' (end date)');
        if (nextServiceAvailCol !== -1) logMsg += ', Next Avail: +90d';
        if (hasDate && scheduledStartDate) logMsg += ', First SCD: ' + scheduledStartDate;
      }
      if (luBox1) logMsg += ', Box1: ' + luBox1;
      if (luBox2) logMsg += ', Box2: ' + luBox2;
      if (amBox3Written) logMsg += ', Box3(Rcvd): ' + amBox3Written + ' (Extra Box)';
      if (needsProductUpdate) logMsg += ', DR/PF queued';
      log.push({ status: 'success', message: logMsg });
    }
    
    // v5.16: Copy QtyRequested → QtyReceived in DR/PF_Products for all DR/PF-coded rows.
    // Emits phase markers and per-ID entries so AP log shows full detail.
    if (extraBoxProductUpdates.length > 0) {
      log.push({ status: 'info', message: 'Updating DR/PF Products (' + extraBoxProductUpdates.length + ' record(s))...' });
      try {
        var pfSheet = getDataWorkbook().getSheetByName(CONFIG.PF_PRODUCTS_SHEET);
        if (pfSheet && pfSheet.getLastRow() >= 2) {
          var pfData = pfSheet.getDataRange().getValues();
          var pfHdrs = trimHeaders(pfData[0]);
          var pfIdCol      = pfHdrs.indexOf('ID');
          var pfDateCol    = pfHdrs.indexOf('RequestDate');
          var pfQtyReqCol  = pfHdrs.indexOf('QtyRequested');
          var pfQtyRecCol  = pfHdrs.indexOf('QtyReceived');
          if (pfIdCol !== -1 && pfDateCol !== -1 && pfQtyReqCol !== -1 && pfQtyRecCol !== -1) {
            // Build per-ID row count map: { "id|date" → updatedCount }
            var updateKeys = {};
            for (var u = 0; u < extraBoxProductUpdates.length; u++) {
              updateKeys[extraBoxProductUpdates[u].id + '|' + extraBoxProductUpdates[u].requestDate] = 0;
            }
            var pfTotalUpdated = 0;
            for (var p = 1; p < pfData.length; p++) {
              var pfRowId   = (pfData[p][pfIdCol]   || '').toString().trim();
              var pfRowDate = normalizeDate(pfData[p][pfDateCol]);
              var pfKey     = pfRowId + '|' + pfRowDate;
              if (pfKey in updateKeys) {
                pfData[p][pfQtyRecCol] = pfData[p][pfQtyReqCol]; // copy Requested → Received
                updateKeys[pfKey]++;
                pfTotalUpdated++;
              }
            }
            if (pfTotalUpdated > 0) {
              pfSheet.getRange(2, 1, pfData.length - 1, pfData[0].length).setValues(pfData.slice(1));
            }
            // Per-ID log entries
            for (var u2 = 0; u2 < extraBoxProductUpdates.length; u2++) {
              var eu = extraBoxProductUpdates[u2];
              var euKey = eu.id + '|' + eu.requestDate;
              var euCount = updateKeys[euKey] || 0;
              if (euCount > 0) {
                log.push({ status: 'success', message: '  DR/PF Products — ID ' + eu.id + ' (' + eu.requestDate + '): ' + euCount + ' product row(s) QtyReceived updated' });
              } else {
                log.push({ status: 'info', message: '  DR/PF Products — ID ' + eu.id + ' (' + eu.requestDate + '): no matching product rows found' });
              }
            }
            log.push({ status: 'success', message: 'DR/PF Products saved — ' + pfTotalUpdated + ' product row(s) updated across ' + extraBoxProductUpdates.length + ' record(s)' });
          } else {
            log.push({ status: 'error', message: 'DR/PF Products: required columns not found (ID, RequestDate, QtyRequested, QtyReceived)' });
          }
        } else {
          log.push({ status: 'info', message: 'DR/PF Products: sheet empty or not found — no product rows updated' });
        }
      } catch (pfErr) {
        log.push({ status: 'error', message: 'DR/PF Products update failed: ' + pfErr.message });
      }
    }

    // Deactivate the SchedDisbCode in LU_SchedDisbCodes
    if (distribCode) {
      try {
        const deactivated = deactivateSchedDisbCode(distribCode);
        if (deactivated) {
          log.push({ status: 'success', message: 'Deactivated SchedDisbCode: ' + distribCode });
        } else {
          log.push({ status: 'info', message: 'SchedDisbCode ' + distribCode + ' not found in LU_SchedDisbCodes or already inactive' });
        }
      } catch (e) {
        log.push({ status: 'error', message: 'Error deactivating SchedDisbCode: ' + e.message });
      }
    }
    
    // Phase 4B.5: Insert stats into Hygiene Box Distribution Stats workbook
    if (CONFIG.HYGIENE_STATS_WORKBOOK_ID) {
      try {
        // Find Baby Box column from report header row
        var bbCol = -1;
        for (var bc = 0; bc < reportData[headerRowIndex].length; bc++) {
          if ((reportData[headerRowIndex][bc] || '').toString().trim() === 'Baby Box') {
            bbCol = bc;
            break;
          }
        }
        
        // Extract Total Recipients and Available Dates from report header area
        var totalRecipientsFromHeader = 0;
        var availableDatesRange = '';
        for (var h = 0; h < Math.min(10, reportData.length); h++) {
          var cellVal = (reportData[h][0] || '').toString();
          if (cellVal.indexOf('Total Recipients:') > -1) {
            totalRecipientsFromHeader = parseInt(cellVal.replace('Total Recipients:', '').trim()) || 0;
          }
          if (cellVal.indexOf('Available Dates:') > -1) {
            availableDatesRange = cellVal.replace('Available Dates:', '').trim();
          }
        }
        
        // Count stats from report data rows
        var pickedUpGeneric = 0;
        var pickedUpBaby = 0;
        var restockRecipients = 0;
        var babyBoxTotal = 0;
        var totalRows = 0;
        
        for (var s = headerRowIndex + 1; s < reportData.length; s++) {
          var subId = reportData[s][submissionCol];
          if (!subId || subId.toString().trim() === '') break;
          totalRows++;
          
          var dpDate = reportData[s][datePickedUpCol];
          var hasPickedUp = dpDate && dpDate.toString().trim() !== '';
          
          var hasBaby = false;
          if (bbCol !== -1) {
            var bbVal = (reportData[s][bbCol] || '').toString().trim().toUpperCase();
            if (bbVal === 'X') {
              hasBaby = true;
              babyBoxTotal++;
            }
          }
          
          if (hasPickedUp) {
            pickedUpGeneric++;
            if (hasBaby) pickedUpBaby++;
          } else {
            restockRecipients++;
          }
        }
        
        // Use header Total Recipients if available, else counted rows
        var totalScheduledRecipients = totalRecipientsFromHeader || totalRows;
        var totalScheduledBoxes = totalScheduledRecipients + babyBoxTotal;
        var pctNoPickUp = totalScheduledRecipients > 0
          ? (restockRecipients / totalScheduledRecipients * 100).toFixed(2) + '%'
          : '0.00%';
        
        // Find the correct sheet by distribution code
        // Extract generic distribution code from SchedDisbCode (e.g., "2026-02A" → "A")
        var genericCode = distribCode.replace(/[0-9\-]/g, '');
        
        var statsWB = SpreadsheetApp.openById(CONFIG.HYGIENE_STATS_WORKBOOK_ID);
        var statsSheet = null;
        
        // Try full code first, then generic code
        statsSheet = statsWB.getSheetByName(distribCode);
        if (!statsSheet && genericCode) {
          statsSheet = statsWB.getSheetByName(genericCode);
        }
        
        if (!statsSheet) {
          log.push({ status: 'error', message: 'Hygiene Stats: No sheet found for "' + distribCode + '" or "' + genericCode + '"' });
        } else {
          // Find column indices by header name
          var statsData = statsSheet.getRange(1, 1, 1, statsSheet.getLastColumn()).getValues();
          var statsHeaders = trimHeaders(statsData[0]);
          
          var colMap = {
            'Completed in Spreadsheet': statsHeaders.indexOf('Completed in Spreadsheet'),
            'Distribution Date Range': statsHeaders.indexOf('Distribution Date Range'),
            'Scheduled Distrib Code': statsHeaders.indexOf('Scheduled Distrib Code'),
            'Total Scheduled-Recipients': statsHeaders.indexOf('Total Scheduled-Recipients'),
            'Total Scheduled-Boxes': statsHeaders.indexOf('Total Scheduled-Boxes'),
            'With Baby Box': statsHeaders.indexOf('With Baby Box'),
            'Picked Up-Generic': statsHeaders.indexOf('Picked Up-Generic'),
            'Picked Up-Baby': statsHeaders.indexOf('Picked Up-Baby'),
            'Restock-# Recipients': statsHeaders.indexOf('Restock-# Recipients'),
            '% Recipients-No Pick Up': statsHeaders.indexOf('% Recipients-No Pick Up')
          };
          
          // Build new row (fill all columns with empty, then set values by index)
          var newRow = new Array(statsHeaders.length).fill('');
          
          if (colMap['Completed in Spreadsheet'] !== -1) newRow[colMap['Completed in Spreadsheet']] = 'Y';
          if (colMap['Distribution Date Range'] !== -1) newRow[colMap['Distribution Date Range']] = availableDatesRange;
          if (colMap['Scheduled Distrib Code'] !== -1) newRow[colMap['Scheduled Distrib Code']] = distribCode;
          if (colMap['Total Scheduled-Recipients'] !== -1) newRow[colMap['Total Scheduled-Recipients']] = totalScheduledRecipients;
          if (colMap['Total Scheduled-Boxes'] !== -1) newRow[colMap['Total Scheduled-Boxes']] = totalScheduledBoxes;
          if (colMap['With Baby Box'] !== -1) newRow[colMap['With Baby Box']] = babyBoxTotal;
          if (colMap['Picked Up-Generic'] !== -1) newRow[colMap['Picked Up-Generic']] = pickedUpGeneric;
          if (colMap['Picked Up-Baby'] !== -1) newRow[colMap['Picked Up-Baby']] = pickedUpBaby;
          if (colMap['Restock-# Recipients'] !== -1) newRow[colMap['Restock-# Recipients']] = restockRecipients;
          if (colMap['% Recipients-No Pick Up'] !== -1) newRow[colMap['% Recipients-No Pick Up']] = pctNoPickUp;
          
          // Append using setValues to avoid data validation issues
          var lastRow = statsSheet.getLastRow();
          statsSheet.getRange(lastRow + 1, 1, 1, newRow.length).setValues([newRow]);
          
          log.push({ status: 'success', message: 'Inserted stats into Hygiene Stats sheet "' + statsSheet.getName() + 
            '": ' + totalScheduledRecipients + ' recipients, ' + totalScheduledBoxes + ' boxes, ' + 
            pickedUpGeneric + ' picked up, ' + restockRecipients + ' restock (' + pctNoPickUp + ')' });
        }
        
      } catch (statsError) {
        Logger.log('Hygiene Stats insert error: ' + statsError.message);
        log.push({ status: 'error', message: 'Hygiene Stats insert failed: ' + statsError.message });
      }
    }
    
    logAudit('PROCESS', null, 'Processed distribution sheet for ' + (distribCode || 'unknown') + ', updated ' + updatedCount + ' records');
    
    return {
      success: true,
      updatedCount: updatedCount,
      log: log
    };
    
  } catch (error) {
    Logger.log('Process distribution error: ' + error.message);
    return { success: false, error: 'Processing failed: ' + error.message };
  }
}

/**
 * Preview archive â€” READ-ONLY count of records and files eligible for archiving
 * Criteria: Request Date < cutoff AND Final Service Contact Date is not empty
 * @param {string} cutoffDateStr - Cutoff date in YYYY-MM-DD format
 * @returns {Object} { success, recordCount, fileCount }
 */
function previewArchive(cutoffDateStr) {
  try {
    let recordCutoffDate;
    if (cutoffDateStr) {
      recordCutoffDate = new Date(cutoffDateStr);
    } else {
      recordCutoffDate = new Date();
      recordCutoffDate.setMonth(recordCutoffDate.getMonth() - 12);
    }
    
    // Phase 5: File deletion is 3 months from today (except Distribution folder)
    const fileCutoffDate = new Date();
    fileCutoffDate.setMonth(fileCutoffDate.getMonth() - 3);
    
    const sheet = getMasterSheet();
    if (!sheet) {
      return { success: false, error: 'Master sheet not found' };
    }
    
    const data = sheet.getDataRange().getValues();
    const headers = trimHeaders(data[0]);
    const requestDateCol             = headers.indexOf(resolveAMField_('Request Date'));
    const finalServiceContactDateCol = headers.indexOf(resolveAMField_('Final Service Contact Date'));
    const idCol                      = headers.indexOf(resolveAMField_('ID'));
    
    let recordCount = 0;
    const archiveIds = []; // Collect IDs + Request Dates for product archive count
    if (requestDateCol !== -1) {
      for (let i = 1; i < data.length; i++) {
        const requestDate = data[i][requestDateCol];
        if (requestDate) {
          const reqDate = new Date(requestDate);
          if (reqDate < recordCutoffDate) {
            // Check Final Service Contact Date is not empty
            const finalSCD = finalServiceContactDateCol !== -1 ? data[i][finalServiceContactDateCol] : '';
            if (finalSCD && finalSCD.toString().trim() !== '') {
              recordCount++;
              archiveIds.push({
                id: data[i][idCol],
                requestDate: requestDate
              });
            }
          }
        }
      }
    }
    
    // Count matching product records
    let productRecordCount = 0;
    let productDebug = { sheetFound: false, totalProdRows: 0, sampleArchiveKeys: [], sampleProdKeys: [], headers: [], error: '' };
    try {
      const dataWB = getDataWorkbook();
      const prodSheet = dataWB.getSheetByName(CONFIG.PF_PRODUCTS_SHEET);
      if (prodSheet && prodSheet.getLastRow() > 1) {
        productDebug.sheetFound = true;
        const prodData = prodSheet.getDataRange().getValues();
        const prodHeaders = trimHeaders(prodData[0]);
        productDebug.headers = prodHeaders.slice(0, 10);
        productDebug.totalProdRows = prodData.length - 1;
        const prodIdCol = prodHeaders.indexOf('ID');
        var prodReqDateCol = prodHeaders.indexOf('Request Date');
        if (prodReqDateCol === -1) prodReqDateCol = prodHeaders.indexOf('RequestDate');
        
        if (prodIdCol !== -1 && prodReqDateCol !== -1) {
          // Build set of ID|Date keys from records to archive
          const archiveKeys = new Set();
          archiveIds.forEach(function(rec) {
            var dateKey = '';
            if (rec.requestDate instanceof Date && !isNaN(rec.requestDate.getTime())) {
              dateKey = Utilities.formatDate(rec.requestDate, CONFIG.TIMEZONE, 'M/d/yyyy');
            } else if (rec.requestDate) {
              var parsed = new Date(rec.requestDate);
              if (!isNaN(parsed.getTime())) {
                dateKey = Utilities.formatDate(parsed, CONFIG.TIMEZONE, 'M/d/yyyy');
              }
            }
            archiveKeys.add(rec.id.toString().trim() + '|' + dateKey);
          });
          
          // Capture sample archive keys for diagnostics
          var archiveKeyArr = Array.from(archiveKeys);
          productDebug.sampleArchiveKeys = archiveKeyArr.slice(0, 5);
          
          for (let i = 1; i < prodData.length; i++) {
            var prodId = (prodData[i][prodIdCol] || '').toString().trim();
            var prodDate = prodData[i][prodReqDateCol];
            var prodDateKey = '';
            if (prodDate instanceof Date && !isNaN(prodDate.getTime())) {
              prodDateKey = Utilities.formatDate(prodDate, CONFIG.TIMEZONE, 'M/d/yyyy');
            } else if (prodDate) {
              var parsed = new Date(prodDate);
              if (!isNaN(parsed.getTime())) {
                prodDateKey = Utilities.formatDate(parsed, CONFIG.TIMEZONE, 'M/d/yyyy');
              }
            }
            var prodKey = prodId + '|' + prodDateKey;
            // Capture sample product keys for diagnostics
            if (productDebug.sampleProdKeys.length < 5) {
              productDebug.sampleProdKeys.push(prodKey);
            }
            if (archiveKeys.has(prodKey)) {
              productRecordCount++;
            }
          }
        } else {
          productDebug.error = 'ID col: ' + prodIdCol + ', Request Date col: ' + prodReqDateCol;
        }
      } else {
        productDebug.error = prodSheet ? 'Sheet empty (rows: ' + (prodSheet ? prodSheet.getLastRow() : 0) + ')' : 'Sheet not found: ' + CONFIG.PF_PRODUCTS_SHEET;
      }
    } catch (e) {
      Logger.log('Error counting product records: ' + e.message);
      productDebug.error = e.message;
    }
    
    let fileCount = 0;
    const reportsFolderId = CONFIG.REPORTS_FOLDER_ID;
    if (reportsFolderId) {
      try {
        fileCount = countOldFilesInFolder(reportsFolderId, fileCutoffDate);
      } catch (folderError) {
        Logger.log('Error counting files: ' + folderError.message);
      }
    }
    
    // Count audit log entries
    let auditLogCount = 0;
    try {
      const masterSS = getMasterWorkbook();
      const auditSheet = masterSS.getSheetByName(CONFIG.AUDIT_LOG);
      if (auditSheet) {
        auditLogCount = Math.max(0, auditSheet.getLastRow() - 1);
      }
    } catch (e) {
      Logger.log('Error counting audit log: ' + e.message);
    }
    
    return {
      success: true,
      recordCount: recordCount,
      productRecordCount: productRecordCount,
      productDebug: productDebug,
      fileCount: fileCount,
      auditLogCount: auditLogCount,
      recordCutoffDate: Utilities.formatDate(recordCutoffDate, CONFIG.TIMEZONE, 'M/d/yyyy'),
      fileCutoffDate: Utilities.formatDate(fileCutoffDate, CONFIG.TIMEZONE, 'M/d/yyyy')
    };
    
  } catch (error) {
    Logger.log('Preview archive error: ' + error.message);
    if (error.message.indexOf('permission') > -1 || error.message.indexOf('authorization') > -1) {
      return { 
        success: false, 
        error: 'Authorization required. Please re-authorize the script in Apps Script editor.' 
      };
    }
    return { success: false, error: 'Preview failed: ' + error.message };
  }
}

/**
 * Counts files older than cutoff date in a Drive folder (recursive)
 * Phase 5: Skips the Distribution folder
 * @param {string} folderId - Google Drive folder ID
 * @param {Date} cutoffDate - Files older than this are counted
 * @returns {number} Count of old files
 */
function countOldFilesInFolder(folderId, cutoffDate) {
  let count = 0;
  
  try {
    const folder = DriveApp.getFolderById(folderId);
    
    const files = folder.getFiles();
    while (files.hasNext()) {
      const file = files.next();
      if (file.getLastUpdated() < cutoffDate) {
        count++;
      }
    }
    
    const subfolders = folder.getFolders();
    while (subfolders.hasNext()) {
      const subfolder = subfolders.next();
      // Phase 5: Skip Distribution folder
      if (subfolder.getId() === CONFIG.DISTRIBUTION_FOLDER_ID) continue;
      count += countOldFilesInFolder(subfolder.getId(), cutoffDate);
    }
  } catch (e) {
    Logger.log('Error counting files in folder ' + folderId + ': ' + e.message);
  }
  
  return count;
}

/**
 * Executes bulk archive operation:
 * 1. Reads all AM data, splits into keep/archive arrays
 * 2. Writes archive rows to G2N_Archive workbook
 * 3. Archives matching product records from DR/PF_Products
 * 4. Rewrites AM with keep rows only (bulk clear + write)
 * 5. Deletes report files older than 3 months (excluding Distribution folder)
 * 6. Checks G2N_Archive capacity — auto-triggers year rollover if >= 85%
 * @param {string} cutoffDateStr - Cutoff date in YYYY-MM-DD format
 * @param {number} alreadyArchived - Count of previously archived records (for batching)
 * @returns {Object} { success, archivedInBatch, deletedInBatch, log[] }
 */
function executeArchiveBatch(cutoffDateStr, alreadyArchived) {
  const ARCHIVE_WORKBOOK_NAME = 'G2N_Archive';
  
  try {
    let recordCutoffDate;
    if (cutoffDateStr) {
      recordCutoffDate = new Date(cutoffDateStr);
    } else {
      recordCutoffDate = new Date();
      recordCutoffDate.setMonth(recordCutoffDate.getMonth() - 12);
    }
    
    // Phase 5: File deletion is 3 months from today (except Distribution folder)
    const fileCutoffDate = new Date();
    fileCutoffDate.setMonth(fileCutoffDate.getMonth() - 3);
    
    const log = [];
    
    const masterSS = getMasterWorkbook();
    const masterSheet = masterSS.getSheetByName(CONFIG.MASTER_SHEET);
    
    if (!masterSheet) {
      return { success: false, error: 'Master sheet not found' };
    }
    
    // Get or create Archive workbook in the Archive folder
    let archiveWorkbook = null;
    let archiveSheet = null;
    const archiveFolderId = CONFIG.ARCHIVE_FOLDER_ID;
    
    if (!archiveFolderId) {
      return { success: false, error: 'Archive folder not configured. Please check CONFIG.ARCHIVE_FOLDER_ID in Code.gs' };
    }
    
    try {
      const archiveFolder = DriveApp.getFolderById(archiveFolderId);
      const files = archiveFolder.getFilesByName(ARCHIVE_WORKBOOK_NAME);
      
      if (files.hasNext()) {
        const file = files.next();
        archiveWorkbook = SpreadsheetApp.openById(file.getId());
        archiveSheet = archiveWorkbook.getSheetByName('Archive');
        if (!archiveSheet) {
          archiveSheet = archiveWorkbook.getActiveSheet();
        }
        log.push({ status: 'info', message: 'Using existing archive workbook: ' + ARCHIVE_WORKBOOK_NAME });
      } else {
        archiveWorkbook = SpreadsheetApp.create(ARCHIVE_WORKBOOK_NAME);
        archiveSheet = archiveWorkbook.getActiveSheet();
        archiveSheet.setName('Archive');
        
        const headers = masterSheet.getRange(1, 1, 1, masterSheet.getLastColumn()).getValues();
        archiveSheet.getRange(1, 1, 1, headers[0].length).setValues(headers);
        archiveSheet.getRange(1, 1, 1, headers[0].length).setFontWeight('bold').setBackground('#f4b400').setFontColor('white');
        archiveSheet.setFrozenRows(1);
        
        moveToFolder(archiveWorkbook.getId(), archiveFolderId);
        
        log.push({ status: 'success', message: 'Created new archive workbook: ' + ARCHIVE_WORKBOOK_NAME });
      }
    } catch (folderError) {
      Logger.log('Archive folder error: ' + folderError.message);
      return { success: false, error: 'Cannot access archive folder: ' + folderError.message };
    }
    
    // ===== BULK READ all master data =====
    const data = masterSheet.getDataRange().getValues();
    const headers = trimHeaders(data[0]);
    const requestDateCol             = headers.indexOf(resolveAMField_('Request Date'));
    const idCol                      = headers.indexOf(resolveAMField_('ID'));
    const finalServiceContactDateCol = headers.indexOf(resolveAMField_('Final Service Contact Date'));
    
    // ===== SPLIT into keep vs archive arrays =====
    const keepRows = [];
    const archiveRows = [];
    const archiveIdDates = []; // For product matching
    
    if (requestDateCol !== -1) {
      for (let i = 1; i < data.length; i++) {
        const requestDate = data[i][requestDateCol];
        let shouldArchive = false;
        
        if (requestDate) {
          const reqDate = new Date(requestDate);
          if (reqDate < recordCutoffDate) {
            const finalSCD = finalServiceContactDateCol !== -1 ? data[i][finalServiceContactDateCol] : '';
            if (finalSCD && finalSCD.toString().trim() !== '') {
              shouldArchive = true;
            }
          }
        }
        
        if (shouldArchive) {
          archiveRows.push(data[i]);
          archiveIdDates.push({ id: data[i][idCol], requestDate: requestDate });
        } else {
          keepRows.push(data[i]);
        }
      }
    }
    
    let archivedInBatch = archiveRows.length;
    log.push({ status: 'info', message: 'Found ' + archivedInBatch + ' AM records to archive, ' + keepRows.length + ' to keep' });
    
    // ===== BULK WRITE archive rows to Archive sheet =====
    if (archiveRows.length > 0) {
      const lastRow = archiveSheet.getLastRow();
      archiveSheet.getRange(lastRow + 1, 1, archiveRows.length, archiveRows[0].length).setValues(archiveRows);
      log.push({ status: 'success', message: 'Wrote ' + archiveRows.length + ' records to Archive sheet' });
    }
    
    // ===== ARCHIVE PRODUCT RECORDS (bulk) =====
    if (archiveIdDates.length > 0) {
      try {
        const archivedProductCount = archiveProductRecords(archiveIdDates, archiveWorkbook, log);
        if (archivedProductCount > 0) {
          log.push({ status: 'success', message: 'Archived ' + archivedProductCount + ' product records to Products_Archive' });
        }
      } catch (prodError) {
        log.push({ status: 'error', message: 'Product archive error: ' + prodError.message });
      }
    }
    
    // ===== BULK REWRITE master sheet (clear + write back keep rows) =====
    if (archiveRows.length > 0) {
      const totalRows = masterSheet.getMaxRows();
      const totalCols = masterSheet.getMaxColumns();
      
      // Clear all data rows (keep header)
      if (totalRows > 1) {
        masterSheet.getRange(2, 1, totalRows - 1, totalCols).clearContent();
      }
      
      // Write back keep rows
      if (keepRows.length > 0) {
        masterSheet.getRange(2, 1, keepRows.length, keepRows[0].length).setValues(keepRows);
      }
      
      // Delete excess blank rows (if more than 100 extra)
      const newLastRow = keepRows.length + 1; // +1 for header
      const excessRows = totalRows - newLastRow;
      if (excessRows > 100) {
        try {
          masterSheet.deleteRows(newLastRow + 1, excessRows);
        } catch (e) {
          Logger.log('Could not delete excess rows: ' + e.message);
        }
      }
      
      log.push({ status: 'success', message: 'Rewrote master sheet: ' + keepRows.length + ' rows remaining' });
    }
    
    // ===== DELETE OLD FILES =====
    let deletedInBatch = 0;
    log.push({ status: 'info', message: 'Now deleting files older than 3 months (excluding Distribution)...' });
    
    const reportsFolderId = CONFIG.REPORTS_FOLDER_ID;
    if (reportsFolderId) {
      try {
        deletedInBatch = deleteOldFilesInFolder(reportsFolderId, fileCutoffDate, log);
        if (deletedInBatch === 0) {
          log.push({ status: 'info', message: 'No files older than 3 months found to delete' });
        }
      } catch (folderError) {
        log.push({ status: 'error', message: 'Error deleting files: ' + folderError.message });
      }
    }
    
    logAudit('ARCHIVE', null, 'Archive complete: ' + archivedInBatch + ' records archived to G2N_Archive, ' + deletedInBatch + ' files deleted');
    
    // Auto-rollover if G2N_Archive capacity >= 85%
    var rolloverResult = null;
    try {
      rolloverResult = checkAndAutoRollover();
      if (rolloverResult) {
        log.push({ status: rolloverResult.success ? 'success' : 'error',
          message: 'Auto-rollover (' + rolloverResult.capacityPct + '% capacity): ' +
            (rolloverResult.success ? rolloverResult.message : rolloverResult.error) });
      }
    } catch (rollErr) {
      log.push({ status: 'error', message: 'Auto-rollover check failed: ' + rollErr.message });
    }
    
    return {
      success: true,
      archivedInBatch: archivedInBatch,
      deletedInBatch: deletedInBatch,
      hasMore: false, // Bulk does everything in one pass
      log: log,
      rolloverResult: rolloverResult
    };
    
  } catch (error) {
    Logger.log('Execute archive batch error: ' + error.message);
    return { success: false, error: 'Archive failed: ' + error.message };
  }
}

/**
 * Archives product records from DR/PF_Products to Products_Archive in G2N_Archive
 * Uses bulk clear-and-rewrite instead of row-by-row deletion
 * @param {Array} archiveIdDates - Array of {id, requestDate} from archived AM records
 * @param {Spreadsheet} archiveWorkbook - The G2N_Archive workbook
 * @param {Array} log - Log array for messages
 * @returns {number} Count of product records archived
 */
function archiveProductRecords(archiveIdDates, archiveWorkbook, log) {
  const dataWB = getDataWorkbook();
  const prodSheet = dataWB.getSheetByName(CONFIG.PF_PRODUCTS_SHEET);
  
  if (!prodSheet || prodSheet.getLastRow() <= 1) {
    return 0;
  }
  
  const prodData = prodSheet.getDataRange().getValues();
  const prodHeaders = trimHeaders(prodData[0]);
  const prodIdCol = prodHeaders.indexOf('ID');
  var prodReqDateCol = prodHeaders.indexOf('Request Date');
  if (prodReqDateCol === -1) prodReqDateCol = prodHeaders.indexOf('RequestDate');
  
  if (prodIdCol === -1 || prodReqDateCol === -1) {
    log.push({ status: 'info', message: 'DR/PF_Products missing ID or Request Date column' });
    return 0;
  }
  
  // Build set of ID|Date keys from AM records being archived
  const archiveKeys = new Set();
  archiveIdDates.forEach(function(rec) {
    var dateKey = '';
    if (rec.requestDate instanceof Date && !isNaN(rec.requestDate.getTime())) {
      dateKey = Utilities.formatDate(rec.requestDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    } else if (rec.requestDate) {
      var parsed = new Date(rec.requestDate);
      if (!isNaN(parsed.getTime())) {
        dateKey = Utilities.formatDate(parsed, CONFIG.TIMEZONE, 'M/d/yyyy');
      }
    }
    archiveKeys.add(rec.id.toString().trim() + '|' + dateKey);
  });
  
  // Split product rows into keep vs archive
  const keepRows = [];
  const archiveRows = [];
  
  for (let i = 1; i < prodData.length; i++) {
    var prodId = (prodData[i][prodIdCol] || '').toString().trim();
    var prodDate = prodData[i][prodReqDateCol];
    var prodDateKey = '';
    if (prodDate instanceof Date && !isNaN(prodDate.getTime())) {
      prodDateKey = Utilities.formatDate(prodDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    } else if (prodDate) {
      var parsed = new Date(prodDate);
      if (!isNaN(parsed.getTime())) {
        prodDateKey = Utilities.formatDate(parsed, CONFIG.TIMEZONE, 'M/d/yyyy');
      }
    }
    if (archiveKeys.has(prodId + '|' + prodDateKey)) {
      archiveRows.push(prodData[i]);
    } else {
      keepRows.push(prodData[i]);
    }
  }
  
  if (archiveRows.length === 0) {
    log.push({ status: 'info', message: 'No matching product records found to archive' });
    return 0;
  }
  
  log.push({ status: 'info', message: 'Found ' + archiveRows.length + ' product records to archive, ' + keepRows.length + ' to keep' });
  
  // Get or create Products_Archive sheet in archive workbook
  let prodArchiveSheet = archiveWorkbook.getSheetByName('Products_Archive');
  if (!prodArchiveSheet) {
    prodArchiveSheet = archiveWorkbook.insertSheet('Products_Archive');
    prodArchiveSheet.getRange(1, 1, 1, prodHeaders.length).setValues([prodHeaders]);
    prodArchiveSheet.getRange(1, 1, 1, prodHeaders.length).setFontWeight('bold').setBackground('#f4b400').setFontColor('white');
    prodArchiveSheet.setFrozenRows(1);
    log.push({ status: 'info', message: 'Created Products_Archive sheet in G2N_Archive' });
  }
  
  // BULK WRITE archive rows to Products_Archive
  const lastRow = prodArchiveSheet.getLastRow();
  prodArchiveSheet.getRange(lastRow + 1, 1, archiveRows.length, archiveRows[0].length).setValues(archiveRows);
  
  // BULK REWRITE DR/PF_Products (clear + write back keep rows)
  const totalRows = prodSheet.getMaxRows();
  const totalCols = prodSheet.getMaxColumns();
  
  if (totalRows > 1) {
    prodSheet.getRange(2, 1, totalRows - 1, totalCols).clearContent();
  }
  
  if (keepRows.length > 0) {
    prodSheet.getRange(2, 1, keepRows.length, keepRows[0].length).setValues(keepRows);
  }
  
  // Clean up excess blank rows
  const newLastRow = keepRows.length + 1;
  const excessRows = totalRows - newLastRow;
  if (excessRows > 100) {
    try {
      prodSheet.deleteRows(newLastRow + 1, excessRows);
    } catch (e) {
      Logger.log('Could not delete excess product rows: ' + e.message);
    }
  }
  
  return archiveRows.length;
}

/**
 * Recursively deletes files older than cutoff date in a Drive folder
 * Phase 5: Skips the Distribution folder
 * @param {string} folderId - Google Drive folder ID
 * @param {Date} cutoffDate - Files older than this are trashed
 * @param {Array} log - Log array for messages
 * @returns {number} Count of deleted files
 */
function deleteOldFilesInFolder(folderId, cutoffDate, log) {
  let count = 0;
  
  try {
    const folder = DriveApp.getFolderById(folderId);
    
    const files = folder.getFiles();
    while (files.hasNext()) {
      const file = files.next();
      if (file.getLastUpdated() < cutoffDate) {
        const fileName = file.getName();
        file.setTrashed(true);
        count++;
        log.push({ status: 'success', message: 'Deleted file: ' + fileName });
      }
    }
    
    const subfolders = folder.getFolders();
    while (subfolders.hasNext()) {
      const subfolder = subfolders.next();
      // Phase 5: Skip Distribution folder
      if (subfolder.getId() === CONFIG.DISTRIBUTION_FOLDER_ID) {
        log.push({ status: 'info', message: 'Skipped Distribution folder (excluded from cleanup)' });
        continue;
      }
      count += deleteOldFilesInFolder(subfolder.getId(), cutoffDate, log);
    }
  } catch (e) {
    Logger.log('Error deleting files in folder ' + folderId + ': ' + e.message);
    log.push({ status: 'error', message: 'Error in folder: ' + e.message });
  }
  
  return count;
}

/**
 * Returns unique Scheduled Distribution Codes from Applicants_Master
 * LEGACY â€” consider replacing with LU_SchedDisbCodes lookup
 * @returns {string[]} Sorted array of unique codes
 */
function getScheduledDistributionCodes() {
  const sheet = getMasterSheet();
  if (!sheet) return [];
  
  const data = sheet.getDataRange().getValues();
  const headers = trimHeaders(data[0]);
  const codeCol = headers.indexOf('Scheduled Distribution Code');
  
  if (codeCol === -1) return [];
  
  const codes = new Set();
  for (let i = 1; i < data.length; i++) {
    const code = data[i][codeCol];
    if (code && code.toString().trim() !== '') {
      codes.add(code.toString().toUpperCase());
    }
  }
  
  return Array.from(codes).sort();
}

// ============ Login Report ============

/**
 * Generates login session report from AuditLog + AuditLog_Backup
 * Parses LOGIN/LOGOUT events, deduplicates, calculates session durations
 * @param {string} startDate - YYYY-MM-DD
 * @param {string} endDate - YYYY-MM-DD
 * @returns {Object} { success, data: [{ code, name, loginCount, totalMinutes, avgMinutes }] }
 */
function getLoginReport(startDate, endDate) {
  try {
    var rangeStart = new Date(startDate + 'T00:00:00');
    var rangeEnd = new Date(endDate + 'T23:59:59');
    
    // Collect audit rows from both sources
    var allRows = [];
    
    // 1) Read from active AuditLog in Applicants_Master
    var ss = getMasterWorkbook();
    var sheet = ss.getSheetByName(CONFIG.AUDIT_LOG);
    if (sheet && sheet.getLastRow() > 1) {
      var data = sheet.getDataRange().getValues();
      var headers = trimHeaders(data[0]);
      for (var i = 1; i < data.length; i++) {
        allRows.push({ headers: headers, row: data[i] });
      }
    }
    
    // 2) Read from AuditLog_Backup in G2N_Archive
    try {
      if (CONFIG.ARCHIVE_FOLDER_ID) {
        var archiveFolder = DriveApp.getFolderById(CONFIG.ARCHIVE_FOLDER_ID);
        var files = archiveFolder.getFilesByName('G2N_Archive');
        if (files.hasNext()) {
          var archiveWb = SpreadsheetApp.openById(files.next().getId());
          var backupSheet = archiveWb.getSheetByName('AuditLog_Backup');
          if (backupSheet && backupSheet.getLastRow() > 1) {
            var bData = backupSheet.getDataRange().getValues();
            var bHeaders = trimHeaders(bData[0]);
            for (var b = 1; b < bData.length; b++) {
              allRows.push({ headers: bHeaders, row: bData[b] });
            }
          }
        }
      }
    } catch (archiveErr) {
      Logger.log('AuditLog_Backup read warning (non-fatal): ' + archiveErr.message);
    }
    
    if (allRows.length === 0) {
      return { success: true, data: [] };
    }
    
    // Parse LOGIN/LOGOUT events from combined rows
    var events = [];
    for (var r = 0; r < allRows.length; r++) {
      var h = allRows[r].headers;
      var row = allRows[r].row;
      var tsCol = h.indexOf('Timestamp');
      var actionCol = h.indexOf('Action');
      var detailsCol = h.indexOf('Details');
      if (tsCol === -1 || actionCol === -1 || detailsCol === -1) continue;
      
      var action = (row[actionCol] || '').toString().trim();
      if (action !== 'LOGIN' && action !== 'LOGOUT') continue;
      
      var ts = row[tsCol];
      if (!(ts instanceof Date)) {
        ts = new Date(ts);
      }
      if (isNaN(ts.getTime())) continue;
      if (ts < rangeStart || ts > rangeEnd) continue;
      
      var details = (row[detailsCol] || '').toString();
      var code = '';
      var name = '';
      
      if (action === 'LOGIN') {
        var loginMatch = details.match(/User logged in:\s*(\S+)\s*\(([^)]+)\)/);
        if (loginMatch) {
          code = loginMatch[1];
          name = loginMatch[2];
        } else {
          var loginSimple = details.match(/User logged in:\s*(\S+)/);
          if (loginSimple) code = loginSimple[1];
        }
      } else {
        var logoutMatch = details.match(/User logged out:\s*(\S+)/);
        if (logoutMatch) code = logoutMatch[1];
      }
      
      if (!code) continue;
      
      events.push({
        timestamp: ts,
        action: action,
        code: code.toUpperCase(),
        name: name
      });
    }
    
    // Deduplicate (same timestamp+action+code from both sources)
    events.sort(function(a, b) { return a.timestamp - b.timestamp; });
    var deduped = [];
    for (var d = 0; d < events.length; d++) {
      if (d === 0) {
        deduped.push(events[d]);
        continue;
      }
      var prev = events[d - 1];
      var curr = events[d];
      if (curr.code === prev.code && curr.action === prev.action &&
          Math.abs(curr.timestamp - prev.timestamp) < 1000) {
        continue; // duplicate within 1 second
      }
      deduped.push(curr);
    }
    events = deduped;
    
    // Group by user
    var userEvents = {};
    for (var j = 0; j < events.length; j++) {
      var evt = events[j];
      if (!userEvents[evt.code]) {
        userEvents[evt.code] = { name: evt.name || evt.code, events: [] };
      }
      userEvents[evt.code].events.push(evt);
      if (evt.action === 'LOGIN' && evt.name) {
        userEvents[evt.code].name = evt.name;
      }
    }
    
    // Calculate sessions per user
    var results = [];
    var userCodes = Object.keys(userEvents).sort();
    
    for (var k = 0; k < userCodes.length; k++) {
      var userCode = userCodes[k];
      var userInfo = userEvents[userCode];
      var userEvts = userInfo.events;
      
      var loginCount = 0;
      var totalMs = 0;
      var pendingLogin = null;
      
      for (var e = 0; e < userEvts.length; e++) {
        var ev = userEvts[e];
        if (ev.action === 'LOGIN') {
          if (pendingLogin) {
            loginCount++;
          }
          pendingLogin = ev;
        } else if (ev.action === 'LOGOUT') {
          if (pendingLogin) {
            var sessionMs = ev.timestamp - pendingLogin.timestamp;
            if (sessionMs > 0 && sessionMs < 24 * 60 * 60 * 1000) {
              totalMs += sessionMs;
            }
            loginCount++;
            pendingLogin = null;
          }
        }
      }
      if (pendingLogin) {
        loginCount++;
      }
      
      var totalMinutes = totalMs / (1000 * 60);
      var avgMinutes = loginCount > 0 ? totalMinutes / loginCount : 0;
      
      results.push({
        code: userCode,
        name: userInfo.name,
        loginCount: loginCount,
        totalMinutes: Math.round(totalMinutes * 10) / 10,
        avgMinutes: Math.round(avgMinutes * 10) / 10
      });
    }
    
    results.sort(function(a, b) { return b.totalMinutes - a.totalMinutes; });
    
    return { success: true, data: results };
    
  } catch (error) {
    Logger.log('getLoginReport error: ' + error.message);
    return { success: false, error: 'Report failed: ' + error.message };
  }
}

/**
 * Phase 5: Generates a Login Report as a Google Sheet saved to the Login folder
 * Includes Year, Month, Day timestamp breakout columns
 * @param {string} startDate - YYYY-MM-DD
 * @param {string} endDate - YYYY-MM-DD
 * @returns {Object} { success, reportUrl, downloadUrl, recordCount, message }
 */
function generateLoginReportSheet(startDate, endDate) {
  try {
    // First get the login data using existing logic
    var reportData = getLoginReport(startDate, endDate);
    if (!reportData.success) {
      return reportData;
    }

    if (!reportData.data || reportData.data.length === 0) {
      return { success: false, error: 'No login activity found for this date range.' };
    }

    // Also get raw events for the detailed sheet with timestamp breakout
    var rawEvents = getLoginRawEvents(startDate, endDate);

    // Create the report spreadsheet
    var formattedStart = startDate.replace(/-/g, '');
    var formattedEnd = endDate.replace(/-/g, '');
    var reportName = 'Login_Report_' + formattedStart + '_' + formattedEnd;

    var ss = SpreadsheetApp.create(reportName);

    // Sheet 1: Summary (same data as the in-page table)
    var summarySheet = ss.getActiveSheet();
    summarySheet.setName('Summary');

    var summaryHeaders = ['User Code', 'Name', 'Logins', 'Total Time (min)', 'Avg Session (min)'];
    summarySheet.getRange(1, 1, 1, summaryHeaders.length).setValues([summaryHeaders]);
    summarySheet.getRange(1, 1, 1, summaryHeaders.length)
      .setFontWeight('bold')
      .setBackground('#4a86e8')
      .setFontColor('white')
      .setHorizontalAlignment('center');
    summarySheet.setFrozenRows(1);

    var summaryRows = [];
    var totalLogins = 0;
    var totalMinutes = 0;
    for (var i = 0; i < reportData.data.length; i++) {
      var d = reportData.data[i];
      totalLogins += d.loginCount;
      totalMinutes += d.totalMinutes;
      summaryRows.push([d.code, d.name, d.loginCount, d.totalMinutes, d.avgMinutes]);
    }

    if (summaryRows.length > 0) {
      summarySheet.getRange(2, 1, summaryRows.length, summaryHeaders.length).setValues(summaryRows);
    }

    // Add totals row
    var totalsRow = ['TOTALS', reportData.data.length + ' user(s)', totalLogins,
      Math.round(totalMinutes * 10) / 10,
      totalLogins > 0 ? Math.round((totalMinutes / totalLogins) * 10) / 10 : 0];
    var totalsRowNum = summaryRows.length + 2;
    summarySheet.getRange(totalsRowNum, 1, 1, summaryHeaders.length).setValues([totalsRow]);
    summarySheet.getRange(totalsRowNum, 1, 1, summaryHeaders.length)
      .setFontWeight('bold')
      .setBackground('#e8f0fe');

    // Auto-resize columns
    for (var c = 1; c <= summaryHeaders.length; c++) {
      summarySheet.autoResizeColumn(c);
    }

    // Sheet 2: Detail with timestamp breakout
    var detailSheet = ss.insertSheet('Detail');
    var detailHeaders = ['User Code', 'Name', 'Action', 'Record ID', 'Timestamp', 'Year', 'Month', 'Day', 'Time'];
    detailSheet.getRange(1, 1, 1, detailHeaders.length).setValues([detailHeaders]);
    detailSheet.getRange(1, 1, 1, detailHeaders.length)
      .setFontWeight('bold')
      .setBackground('#4a86e8')
      .setFontColor('white')
      .setHorizontalAlignment('center');
    detailSheet.setFrozenRows(1);

    if (rawEvents && rawEvents.length > 0) {
      var detailRows = [];
      for (var e = 0; e < rawEvents.length; e++) {
        var evt = rawEvents[e];
        var ts = evt.timestamp;
        detailRows.push([
          evt.code,
          evt.name || '',
          evt.action,
          evt.recordId || '',
          ts,
          ts.getFullYear(),
          ts.getMonth() + 1,
          ts.getDate(),
          Utilities.formatDate(ts, CONFIG.TIMEZONE, 'h:mm:ss a')
        ]);
      }
      if (detailRows.length > 0) {
        detailSheet.getRange(2, 1, detailRows.length, detailHeaders.length).setValues(detailRows);
      }
    }

    for (var dc = 1; dc <= detailHeaders.length; dc++) {
      detailSheet.autoResizeColumn(dc);
    }

    // Sheet 3: Intake Activity — INTAKE_SESSION and CREATE events
    var intakeEvents = (rawEvents || []).filter(function(ev) {
      return ev.action === 'INTAKE_SESSION' || ev.action === 'CREATE';
    });

    if (intakeEvents.length > 0) {
      var intakeSheet = ss.insertSheet('Intake Activity');

      // Summary section
      var intakeSessions = intakeEvents.filter(function(ev) { return ev.action === 'INTAKE_SESSION'; });
      var intakeCreates = intakeEvents.filter(function(ev) { return ev.action === 'CREATE'; });

      // Count by mode
      var modeCounts = {};
      for (var m = 0; m < intakeSessions.length; m++) {
        var mode = intakeSessions[m].name || 'unknown';
        modeCounts[mode] = (modeCounts[mode] || 0) + 1;
      }

      // Write summary header
      intakeSheet.getRange(1, 1).setValue('Intake Activity Summary');
      intakeSheet.getRange(1, 1).setFontWeight('bold').setFontSize(12);

      intakeSheet.getRange(3, 1, 1, 2).setValues([['Metric', 'Count']]);
      intakeSheet.getRange(3, 1, 1, 2).setFontWeight('bold').setBackground('#4a86e8').setFontColor('white');

      var summaryData = [
        ['Total Form Opens', intakeSessions.length],
        ['Total Records Created', intakeCreates.length]
      ];
      var modes = Object.keys(modeCounts).sort();
      for (var mi = 0; mi < modes.length; mi++) {
        summaryData.push(['  Opens - ' + modes[mi].charAt(0).toUpperCase() + modes[mi].slice(1) + ' Mode', modeCounts[modes[mi]]]);
      }
      // Conversion rate
      if (intakeSessions.length > 0) {
        var convRate = Math.round((intakeCreates.length / intakeSessions.length) * 1000) / 10;
        summaryData.push(['Conversion Rate', convRate + '%']);
      }

      // Average time from Open to Create
      // Pair each CREATE with the closest preceding unmatched INTAKE_SESSION
      var sessionPool = intakeSessions.map(function(s) { return { ts: s.timestamp.getTime(), matched: false }; });
      var pairDurations = [];
      for (var cr = 0; cr < intakeCreates.length; cr++) {
        var createTs = intakeCreates[cr].timestamp.getTime();
        var bestIdx = -1;
        var bestGap = Infinity;
        // Find closest preceding unmatched session (within 2 hours max)
        for (var sp = 0; sp < sessionPool.length; sp++) {
          if (sessionPool[sp].matched) continue;
          var gap = createTs - sessionPool[sp].ts;
          if (gap >= 0 && gap < 2 * 60 * 60 * 1000 && gap < bestGap) {
            bestGap = gap;
            bestIdx = sp;
          }
        }
        if (bestIdx !== -1) {
          sessionPool[bestIdx].matched = true;
          pairDurations.push(bestGap);
        }
      }
      if (pairDurations.length > 0) {
        var totalPairMs = 0;
        for (var pd = 0; pd < pairDurations.length; pd++) totalPairMs += pairDurations[pd];
        var avgSec = Math.round(totalPairMs / pairDurations.length / 1000);
        var avgMin = Math.floor(avgSec / 60);
        var avgRemSec = avgSec % 60;
        var avgDisplay = avgMin > 0
          ? avgMin + ' min ' + avgRemSec + ' sec'
          : avgSec + ' sec';
        summaryData.push(['Avg Time Open → Created', avgDisplay]);
        summaryData.push(['  Paired Sessions', pairDurations.length + ' of ' + intakeCreates.length + ' creates matched']);
      }

      intakeSheet.getRange(4, 1, summaryData.length, 2).setValues(summaryData);

      // Detail section below summary
      var detailStartRow = summaryData.length + 6;
      intakeSheet.getRange(detailStartRow, 1).setValue('Intake Event Detail');
      intakeSheet.getRange(detailStartRow, 1).setFontWeight('bold').setFontSize(12);

      var intakeDetailHeaders = ['Action', 'Mode', 'Record ID', 'Timestamp', 'Year', 'Month', 'Day', 'Time'];
      var headerRow = detailStartRow + 1;
      intakeSheet.getRange(headerRow, 1, 1, intakeDetailHeaders.length).setValues([intakeDetailHeaders]);
      intakeSheet.getRange(headerRow, 1, 1, intakeDetailHeaders.length)
        .setFontWeight('bold')
        .setBackground('#4a86e8')
        .setFontColor('white')
        .setHorizontalAlignment('center');

      var intakeDetailRows = [];
      for (var ie = 0; ie < intakeEvents.length; ie++) {
        var iev = intakeEvents[ie];
        var its = iev.timestamp;
        intakeDetailRows.push([
          iev.action,
          iev.name || '',
          iev.recordId || '',
          its,
          its.getFullYear(),
          its.getMonth() + 1,
          its.getDate(),
          Utilities.formatDate(its, CONFIG.TIMEZONE, 'h:mm:ss a')
        ]);
      }
      if (intakeDetailRows.length > 0) {
        intakeSheet.getRange(headerRow + 1, 1, intakeDetailRows.length, intakeDetailHeaders.length).setValues(intakeDetailRows);
      }

      for (var ic = 1; ic <= intakeDetailHeaders.length; ic++) {
        intakeSheet.autoResizeColumn(ic);
      }
    }

    // Move report to Login folder
    var loginFolderId = CONFIG.LOGIN_FOLDER_ID;
    if (loginFolderId) {
      try {
        moveToFolder(ss.getId(), loginFolderId);
      } catch (folderErr) {
        Logger.log('Could not move login report to Login folder: ' + folderErr.message);
      }
    }

    var reportUrl = ss.getUrl();
    var downloadUrl = 'https://docs.google.com/spreadsheets/d/' + ss.getId() + '/export?format=xlsx';

    logAudit('LOGIN_REPORT', null, 'Generated login report: ' + reportName + ' (' + reportData.data.length + ' users, ' + (rawEvents ? rawEvents.length : 0) + ' events, ' + (intakeEvents ? intakeEvents.length : 0) + ' intake events)');

    return {
      success: true,
      reportUrl: reportUrl,
      downloadUrl: downloadUrl,
      recordCount: reportData.data.length,
      eventCount: rawEvents ? rawEvents.length : 0,
      message: 'Login report generated with ' + reportData.data.length + ' users'
    };

  } catch (error) {
    Logger.log('generateLoginReportSheet error: ' + error.message);
    return { success: false, error: 'Report generation failed: ' + error.message };
  }
}

/**
 * Phase 5: Returns raw LOGIN/LOGOUT/INTAKE_SESSION/CREATE events for the detail sheet
 * Reuses the same parsing logic as getLoginReport but returns individual events
 * v5.2: Added INTAKE_SESSION and CREATE event types
 * @param {string} startDate - YYYY-MM-DD
 * @param {string} endDate - YYYY-MM-DD
 * @returns {Array} Array of { timestamp, action, code, name, recordId, details }
 */
function getLoginRawEvents(startDate, endDate) {
  try {
    var rangeStart = new Date(startDate + 'T00:00:00');
    var rangeEnd = new Date(endDate + 'T23:59:59');

    var allRows = [];

    // Read from active AuditLog
    var ss = getMasterWorkbook();
    var sheet = ss.getSheetByName(CONFIG.AUDIT_LOG);
    if (sheet && sheet.getLastRow() > 1) {
      var data = sheet.getDataRange().getValues();
      var headers = trimHeaders(data[0]);
      for (var i = 1; i < data.length; i++) {
        allRows.push({ headers: headers, row: data[i] });
      }
    }

    // Read from AuditLog_Backup in G2N_Archive
    try {
      if (CONFIG.ARCHIVE_FOLDER_ID) {
        var archiveFolder = DriveApp.getFolderById(CONFIG.ARCHIVE_FOLDER_ID);
        var files = archiveFolder.getFilesByName('G2N_Archive');
        if (files.hasNext()) {
          var archiveWb = SpreadsheetApp.openById(files.next().getId());
          var backupSheet = archiveWb.getSheetByName('AuditLog_Backup');
          if (backupSheet && backupSheet.getLastRow() > 1) {
            var bData = backupSheet.getDataRange().getValues();
            var bHeaders = trimHeaders(bData[0]);
            for (var b = 1; b < bData.length; b++) {
              allRows.push({ headers: bHeaders, row: bData[b] });
            }
          }
        }
      }
    } catch (archiveErr) {
      Logger.log('AuditLog_Backup read warning: ' + archiveErr.message);
    }

    // v5.2: Accepted action types now include INTAKE_SESSION and CREATE
    var ACCEPTED_ACTIONS = { 'LOGIN': true, 'LOGOUT': true, 'INTAKE_SESSION': true, 'CREATE': true };

    // Parse events
    var events = [];
    for (var r = 0; r < allRows.length; r++) {
      var h = allRows[r].headers;
      var row = allRows[r].row;
      var tsCol = h.indexOf('Timestamp');
      var actionCol = h.indexOf('Action');
      var detailsCol = h.indexOf('Details');
      var recordIdCol = h.indexOf('Record ID');
      var userCol = h.indexOf('User');
      if (tsCol === -1 || actionCol === -1 || detailsCol === -1) continue;

      var action = (row[actionCol] || '').toString().trim();
      if (!ACCEPTED_ACTIONS[action]) continue;

      // For CREATE, only include intake portal creates (not staff edits etc.)
      var details = (row[detailsCol] || '').toString();
      if (action === 'CREATE' && details.indexOf('Intake Portal') === -1) continue;

      var ts = row[tsCol];
      if (!(ts instanceof Date)) ts = new Date(ts);
      if (isNaN(ts.getTime())) continue;
      if (ts < rangeStart || ts > rangeEnd) continue;

      var code = '';
      var name = '';
      var recordId = recordIdCol !== -1 ? (row[recordIdCol] || '').toString() : '';
      var user = userCol !== -1 ? (row[userCol] || '').toString() : '';

      if (action === 'LOGIN') {
        var loginMatch = details.match(/User logged in:\s*(\S+)\s*\(([^)]+)\)/);
        if (loginMatch) {
          code = loginMatch[1];
          name = loginMatch[2];
        } else {
          var loginSimple = details.match(/User logged in:\s*(\S+)/);
          if (loginSimple) code = loginSimple[1];
        }
      } else if (action === 'LOGOUT') {
        var logoutMatch = details.match(/User logged out:\s*(\S+)/);
        if (logoutMatch) code = logoutMatch[1];

      } else if (action === 'INTAKE_SESSION') {
        // Parse: "Intake form opened | Mode: public | User: Anonymous | UA: ..."
        var modeMatch = details.match(/Mode:\s*(\w+)/);
        var userMatch = details.match(/User:\s*(\S+)/);
        code = userMatch ? userMatch[1] : 'Anonymous';
        name = modeMatch ? modeMatch[1] : 'unknown';

      } else if (action === 'CREATE') {
        // Parse: "New applicant submitted via Intake Portal" or "(Event Mode)"
        code = user || 'Anonymous';
        name = details.indexOf('Event Mode') > -1 ? 'event' : 'public';
      }

      if (!code) continue;

      events.push({
        timestamp: ts,
        action: action,
        code: action === 'LOGIN' || action === 'LOGOUT' ? code.toUpperCase() : code,
        name: name,
        recordId: recordId,
        details: details
      });
    }

    // Deduplicate
    events.sort(function(a, b) { return a.timestamp - b.timestamp; });
    var deduped = [];
    for (var d = 0; d < events.length; d++) {
      if (d === 0) { deduped.push(events[d]); continue; }
      var prev = events[d - 1];
      var curr = events[d];
      if (curr.code === prev.code && curr.action === prev.action &&
          Math.abs(curr.timestamp - prev.timestamp) < 1000) continue;
      deduped.push(curr);
    }

    return deduped;

  } catch (error) {
    Logger.log('getLoginRawEvents error: ' + error.message);
    return [];
  }
}
