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
 * v5.19 - createDistributionReportSpreadsheet: added Sheet 2 "Quick Reference" —
 *          compact printable table of Last Name, First Name, Baby Box, Extra Box,
 *          Distribution Code, sorted by Last Name/First Name. Useful for distribution
 *          day quick lookup without scrolling address columns.
 *          getLastAmId: added missing var maxId = 0 declaration (was ReferenceError).
 */

/**
 * Generates a Distribution Report spreadsheet filtered by distribution code
 * Sorts by Last Name, First Name; includes Baby Box indicator if applicable
 * Moves report to Distribution folder in Google Drive
 * @param {string} distribCode - Scheduled Distribution Code to filter by
 * @param {string} startDate - Report start date
 * @param {string} endDate - Report end date
 * @param {string} pickupTimes - Pickup time text for report header
 * @returns {Object} { success, reportUrl, downloadUrl, recordCount }
 */
function generateDistributionReport(distribCode, startDate, endDate, pickupTimes) {
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
            id: headers.indexOf(resolveAMField_('ID')),
            firstName: headers.indexOf(resolveAMField_('First Name')),
            lastName: headers.indexOf(resolveAMField_('Last Name')),
            address1: headers.indexOf(resolveAMField_('Street Address')),
            address2: headers.indexOf(resolveAMField_('Apartment #, Upper, Lower, or Lot #')),
            city: headers.indexOf(resolveAMField_('City')),
            phone: headers.indexOf(resolveAMField_('Phone Number')),
            schedDistribCode: headers.indexOf(resolveAMField_('Scheduled Distribution Code')),
            originalFormId: headers.indexOf(resolveAMField_('Original Form ID')),
            babyBox: headers.indexOf(resolveAMField_('Take Baby Box?')),
            schedBoxCode3: headers.indexOf(resolveAMField_('Scheduled Box Code 3')),  // v5.12
            distribStartDate: headers.indexOf(resolveAMField_('Distribution Start Date')),
            distribInterval: headers.indexOf(resolveAMField_('Distribution Interval'))
        };

        const filteredRecords = [];
        let hasBabyBox = false;
        let hasExtraBox = false;  // v5.12: true when any row has Scheduled Box Code 3

        for (let i = 1; i < data.length; i++) {
            const row = data[i];
            const rowDistribCode = row[colIndices.schedDistribCode];

            if (rowDistribCode && rowDistribCode.toString().toUpperCase() === distribCode) {
                // v5.12: Extra Box = 'X' when Scheduled Box Code 3 has any non-empty value
                const boxCode3 = colIndices.schedBoxCode3 !== -1
                    ? (row[colIndices.schedBoxCode3] || '').toString().trim() : '';
                const extraBox = boxCode3.length > 0 ? 'X' : '';
                if (extraBox) hasExtraBox = true;

                const record = {
                    firstName: row[colIndices.firstName] || '',
                    lastName: row[colIndices.lastName] || '',
                    address1: row[colIndices.address1] || '',
                    address2: row[colIndices.address2] || '',
                    city: row[colIndices.city] || '',
                    phone: row[colIndices.phone] || '',
                    distribCode: rowDistribCode,
                    submissionId: row[colIndices.id] || '',
                    babyBox: row[colIndices.babyBox] || '',
                    extraBox: extraBox  // v5.12
                };

                if (record.babyBox === 'X' || record.babyBox === 'x') {
                    hasBabyBox = true;
                    record.babyBox = 'X';
                } else {
                    record.babyBox = '';
                }

                filteredRecords.push(record);
            }
        }

        if (filteredRecords.length === 0) {
            return { success: false, error: 'No records found for Distribution Code: ' + distribCode };
        }

        filteredRecords.sort(function (a, b) {
            const lastNameCompare = a.lastName.localeCompare(b.lastName);
            if (lastNameCompare !== 0) return lastNameCompare;
            return a.firstName.localeCompare(b.firstName);
        });

        const reportSpreadsheet = createDistributionReportSpreadsheet(
            distribCode,
            filteredRecords,
            hasBabyBox,
            hasExtraBox,
            startDate,
            endDate,
            pickupTimes
        );

        logAudit('REPORT', null, 'Generated Distribution Report for ' + distribCode + ' with ' + filteredRecords.length + ' records');

        return {
            success: true,
            message: 'Report generated successfully',
            recordCount: filteredRecords.length,
            hasBabyBox: hasBabyBox,
            reportUrl: reportSpreadsheet.getUrl(),
            reportId: reportSpreadsheet.getId()
        };

    } catch (error) {
        Logger.log('Report generation error: ' + error.message);
        return { success: false, error: 'Report generation failed: ' + error.message };
    }
}

/**
 * Formats a phone number as (XXX) XXX-XXXX for 10-digit values.
 * Returns the raw value unchanged for any other length.
 * @param {string|number} raw - Raw phone number value
 * @returns {string} Formatted phone number string
 */
function formatPhoneNumber_(raw) {
    var digits = (raw || '').toString().replace(/\D/g, '');
    if (digits.length === 10) {
        return '(' + digits.substring(0, 3) + ') ' + digits.substring(3, 6) + '-' + digits.substring(6);
    }
    return (raw || '').toString().trim();
}

/**
 * Creates the Distribution Report spreadsheet with formatted headers and data
 * @param {string} distribCode - Distribution code
 * @param {Object[]} records - Filtered/sorted record objects
 * @param {boolean} hasBabyBox  - Whether to include Baby Box column
 * @param {boolean} hasExtraBox - v5.12: Whether to include Extra Box column (any row has Box Code 3)
 * @param {string} startDate - Report start date
 * @param {string} endDate - Report end date
 * @param {string} pickupTimes - Pickup times text
 * @returns {Object} { reportUrl, downloadUrl, spreadsheetId }
 */
function createDistributionReportSpreadsheet(distribCode, records, hasBabyBox, hasExtraBox, startDate, endDate, pickupTimes) {
    const reportName = 'Distribution_' + distribCode + '_' + Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd');
    const spreadsheet = SpreadsheetApp.create(reportName);
    const sheet = spreadsheet.getActiveSheet();

    // Move to Distribution Reports folder
    const folderId = CONFIG.DISTRIBUTION_FOLDER_ID;
    Logger.log('DISTRIBUTION_FOLDER_ID: ' + folderId);

    if (folderId && folderId.length > 0) {
        try {
            moveToFolder(spreadsheet.getId(), folderId);
            Logger.log('Report successfully moved to Distribution folder');
        } catch (folderError) {
            Logger.log('ERROR moving to Distribution folder: ' + folderError.message);
        }
    } else {
        Logger.log('WARNING: DISTRIBUTION_FOLDER_ID not configured');
    }

    let currentRow = 1;

    // === HEADER SECTION ===
    sheet.getRange(currentRow, 1).setValue('Total Recipients: ' + records.length);
    sheet.getRange(currentRow, 1).setFontWeight('bold');
    currentRow++;

    sheet.getRange(currentRow, 1).setValue('Giving to the Nations');
    sheet.getRange(currentRow, 1).setFontWeight('bold').setFontSize(14);
    currentRow++;

    sheet.getRange(currentRow, 1).setValue('Cleaning Box Distribution');
    sheet.getRange(currentRow, 1).setFontWeight('bold').setFontSize(12);
    currentRow++;

    let dateLabel = 'Available Dates: ' + startDate + '-' + endDate;
    sheet.getRange(currentRow, 1).setValue(dateLabel);
    currentRow++;

    if (pickupTimes) {
        sheet.getRange(currentRow, 1).setValue('Available Pick Up Times: ' + pickupTimes);
        currentRow++;
    }

    const createdDate = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yy');
    sheet.getRange(currentRow, 1).setValue('Created: ' + createdDate);
    currentRow++;

    currentRow++;

    // === DATA HEADER ROW ===
    const headerRow = currentRow;
    let col = 1;

    // v5.12: Baby Box and Extra Box are optional columns inserted after Date Picked Up
    const dataHeaders = ['Date Picked Up'];
    if (hasBabyBox) dataHeaders.push('Baby Box');
    if (hasExtraBox) dataHeaders.push('Extra Box');
    dataHeaders.push('First Name', 'Last Name', getReportHeader_('Street Address'), getReportHeader_('Apartment #, Upper, Lower, or Lot #'), 'City', 'Phone', 'Distribution Code', 'Submission #');

    for (let i = 0; i < dataHeaders.length; i++) {
        sheet.getRange(headerRow, col + i).setValue(dataHeaders[i]);
    }

    styleReportHeader(sheet, headerRow, dataHeaders.length);

    currentRow++;

    // === DATA ROWS ===
    for (let i = 0; i < records.length; i++) {
        const record = records[i];
        col = 1;

        sheet.getRange(currentRow, col++).setValue('');           // Date Picked Up (blank for staff)
        if (hasBabyBox) sheet.getRange(currentRow, col++).setValue(record.babyBox);
        if (hasExtraBox) sheet.getRange(currentRow, col++).setValue(record.extraBox);  // v5.12

        sheet.getRange(currentRow, col++).setValue(record.firstName);
        sheet.getRange(currentRow, col++).setValue(record.lastName);
        sheet.getRange(currentRow, col++).setValue(record.address1);
        sheet.getRange(currentRow, col++).setValue(record.address2);
        sheet.getRange(currentRow, col++).setValue(record.city);
        sheet.getRange(currentRow, col++).setValue(formatPhoneNumber_(record.phone));
        sheet.getRange(currentRow, col++).setValue(record.distribCode);
        sheet.getRange(currentRow, col++).setValue(record.submissionId);

        currentRow++;
    }

    const dataRange = sheet.getRange(headerRow + 1, 1, records.length, dataHeaders.length);
    dataRange.setBorder(true, true, true, true, true, true);

    for (let i = 0; i < records.length; i++) {
        if (i % 2 === 1) {
            sheet.getRange(headerRow + 1 + i, 1, 1, dataHeaders.length).setBackground('#f3f3f3');
        }
    }

    // Footer notes for Baby Box and/or Extra Box
    const footerLines = [];
    if (hasBabyBox) {
        footerLines.push('Note: Recipients with "X" in the \'Baby Box\' column will receive 2 boxes, 1 large box and 1 small box. All others receive 1 large box.');
    }
    if (hasExtraBox) {
        footerLines.push('Note: Recipients with "X" in the \'Extra Box\' column will receive a special box in addition to the Scheduled Box.');

    }
    if (footerLines.length > 0) {
        for (const line of footerLines) {
            currentRow++;
            sheet.getRange(currentRow, 1).setValue(line);
            sheet.getRange(currentRow, 1, 1, dataHeaders.length).merge();
            sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(10).setWrap(true);
        }
    }

    // Column widths — built dynamically to match actual column order
    // Cols: [Date Picked Up, (Baby Box?), (Extra Box?), First Name, Last Name, Addr1, Addr2, City, Phone, Distrib Code, Submission #]
    const widths = [100];
    if (hasBabyBox) widths.push(70);
    if (hasExtraBox) widths.push(70);
    widths.push(100, 100, 180, 80, 120, 120, 100, 90);
    for (let c = 0; c < widths.length; c++) {
        sheet.setColumnWidth(c + 1, widths[c]);
    }

    // v5.19: Sheet 2 — Quick Reference (compact printable table for distribution day)
    const refSheet = spreadsheet.insertSheet('Quick Reference');

    // Header block
    refSheet.getRange(1, 1).setValue('Distribution Code: ' + distribCode).setFontWeight('bold');
    refSheet.getRange(2, 1).setValue('Available Dates: ' + startDate + '-' + endDate);
    if (pickupTimes) {
        refSheet.getRange(3, 1).setValue('Available Pick Up Times: ' + pickupTimes);
    }
    refSheet.getRange(4, 1).setValue('Total Recipients: ' + records.length).setFontWeight('bold');
    const refCreatedDate = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yy');
    refSheet.getRange(5, 1).setValue('Created: ' + refCreatedDate);
    const refStartRow = 7;

    // Build reference headers — Last Name, First Name; optional Baby/Extra Box columns
    const refHeaders = ['Last Name', 'First Name'];
    if (hasBabyBox) refHeaders.push('Baby Box');
    if (hasExtraBox) refHeaders.push('Extra Box');
    refHeaders.push('Distribution Code', 'Submission #');

    refSheet.getRange(refStartRow, 1, 1, refHeaders.length).setValues([refHeaders]);
    styleReportHeader(refSheet, refStartRow, refHeaders.length);

    // Data rows (records already sorted Last Name / First Name)
    const refDataRows = [];
    for (let i = 0; i < records.length; i++) {
        const rec = records[i];
        const row = [rec.lastName, rec.firstName];
        if (hasBabyBox) row.push(rec.babyBox);
        if (hasExtraBox) row.push(rec.extraBox);
        row.push(rec.distribCode, rec.submissionId);
        refDataRows.push(row);
    }
    if (refDataRows.length > 0) {
        refSheet.getRange(refStartRow + 1, 1, refDataRows.length, refHeaders.length).setValues(refDataRows);
        refSheet.getRange(refStartRow + 1, 1, refDataRows.length, refHeaders.length)
            .setBorder(true, true, true, true, true, true);
        for (let i = 0; i < refDataRows.length; i++) {
            if (i % 2 === 1) {
                refSheet.getRange(refStartRow + 1 + i, 1, 1, refHeaders.length).setBackground('#f3f3f3');
            }
        }
    }

    // Column widths for reference sheet
    const refWidths = [130, 110];
    if (hasBabyBox) refWidths.push(70);
    if (hasExtraBox) refWidths.push(70);
    refWidths.push(110, 90);
    for (let c = 0; c < refWidths.length; c++) {
        refSheet.setColumnWidth(c + 1, refWidths[c]);
    }
    refSheet.setFrozenRows(refStartRow);

    // Return to Sheet 1 as active
    spreadsheet.setActiveSheet(sheet);

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
            firstName: headers.indexOf(resolveAMField_('First Name')),
            lastName: headers.indexOf(resolveAMField_('Last Name')),
            city: headers.indexOf(resolveAMField_('City')),
            schedDistribCode: headers.indexOf(resolveAMField_('Scheduled Distribution Code')),
            babyBox: headers.indexOf(resolveAMField_('Take Baby Box?'))
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

    const codeCol = headers.indexOf(resolveAMField_('Scheduled Distribution Code'));
    const startDateCol = headers.indexOf(resolveAMField_('Distribution Start Date'));
    const intervalCol = headers.indexOf(resolveAMField_('Distribution Interval'));

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
        var maxId = 0;  // v5.19: was missing, caused ReferenceError
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
 * Generates a Scheduling Report spreadsheet for a range of record IDs
 * Combines male/female age brackets, reorders columns for printing,
 * filters to records within 1 year, auto-uppercases distribution codes
 * Phase 4 enhancements:
 *   - If endId is 0 or omitted, uses last ID in AM
 *   - Adds "History" column marking rows with IDs outside beginId–endId range
 *   - Adds data validation dropdowns for Sched Distrib Code and Service Status
 *   - Sorts by Last Name, First Name, ID (not Request Date)
 *   - Highlights the endId row in AM with magenta background
 * @param {string|number} beginId - Start of ID range
 * @param {string|number} endId - End of ID range (0 = auto-detect last AM row)
 * @param {string} distribCode - Optional distribution code filter
 * @param {string} serviceStatus - Optional Service Status filter
 * @returns {Object} { success, reportUrl, downloadUrl, recordCount }
 */
function generateSchedulingReport(beginId, endId, distribCode, serviceStatus) {
    try {
        if (!beginId) {
            return { success: false, error: 'Beginning ID is required' };
        }

        const sheet = getMasterSheet();
        if (!sheet) {
            return { success: false, error: 'Master sheet not found' };
        }

        const data = sheet.getDataRange().getValues();
        const rawHeaders = data[0];
        const headers = trimHeaders(rawHeaders);

        const idCol = headers.indexOf(resolveAMField_('ID'));
        const firstNameCol = headers.indexOf(resolveAMField_('First Name'));
        const lastNameCol = headers.indexOf(resolveAMField_('Last Name'));

        // Phase 4A.2: Auto-detect endId if 0 or omitted
        if (!endId || parseInt(endId) === 0) {
            var maxId = 0;
            for (var i = 1; i < data.length; i++) {
                var rowId = parseInt(data[i][idCol]);
                if (!isNaN(rowId) && rowId > maxId) maxId = rowId;
            }
            endId = maxId;
            if (endId === 0) {
                return { success: false, error: 'No records found in Applicants_Master' };
            }
        }

        beginId = parseInt(beginId);
        endId = parseInt(endId);

        if (beginId > endId) {
            return { success: false, error: 'Beginning ID must be less than or equal to Ending ID' };
        }

        const namesInRange = new Map();

        for (let i = 1; i < data.length; i++) {
            const id = parseInt(data[i][idCol]);
            if (id >= beginId && id <= endId) {
                const firstName = (data[i][firstNameCol] || '').toString().trim().toLowerCase();
                const lastName = (data[i][lastNameCol] || '').toString().trim().toLowerCase();
                if (firstName || lastName) {
                    namesInRange.set(firstName + '|' + lastName, true);
                }
            }
        }

        if (namesInRange.size === 0) {
            return { success: false, error: 'No records found in ID range ' + beginId + ' to ' + endId };
        }

        const oneYearAgo = new Date();
        oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);

        // Report columns - reordered with Last Name, First Name after Next Service Availability Date
        const reportColumns = [
            'Scheduled Distribution Code',
            'Service Status',
            'Admin Notes',
            'Take Baby Box?',
            'ID',
            'Request Type',
            'Request Date',
            'Last Date Served',
            'Next Service Availability Date',
            'Last Name',
            'First Name',
            'More information about the person or organization that referred you.',
            'What is the title or position of the person who referred you?',    // v5.18: added after Referral Info
            "What is the person's name who referred you?",
            'Please tell us anything else you would like us to know about how you heard about Giving to the Nations or our program.',
            'Are you currently homeless?',
            'Street Address',
            'Apartment #, Upper, Lower, or Lot #',
            'City',
            'State',
            'County',
            'Address type?',
            'Phone Number',
            'Phone type?',
            'Email',
            'Best contact method (What is the best way to get ahold of you?)'
        ];

        // Age brackets - we'll combine males and females
        const ageBracketPairs = [
            { label: 'Under 5', male: '[Males under 5]', female: '[Females under 5]' },
            { label: '5-9', male: '[Males 5-9]', female: '[Females 5-9]' },
            { label: '10-14', male: '[Males 10-14]', female: '[Females 10-14]' },
            { label: '15-17', male: '[Males 15-17]', female: '[Females 15-17]' },
            { label: '18-24', male: '[Males 18-24]', female: '[Females 18-24]' },
            { label: '25-34', male: '[Males 25-34]', female: '[Females 25-34]' },
            { label: '35-44', male: '[Males 35-44]', female: '[Females 35-44]' },
            { label: '45-54', male: '[Males 45-54]', female: '[Females 45-54]' },
            { label: '55-64', male: '[Males 55-64]', female: '[Females 55-64]' },
            { label: '65-84', male: '[Males 65-84]', female: '[Females 65-84]' },
            { label: '85 and Over', male: '[Males 85 and Over]', female: '[Females 85 and Over]' }
        ];

        // Additional columns after age brackets
        const additionalColumns = [
            'Are you receiving any assistance? Please select ALL boxes that apply to ANY ASSISTANCE your family is receiving.',
            'Are you currently employed?',
            'Total annual household income. (How much money does your family make in 1 year?)',
            'Was there an emergency situation which caused you to contact us?',
            'Briefly explain your current situation.',
            'How will receiving personal and home cleaning products help you?'   // v5.18: added as last column
        ];

        const colIndexMap = {};
        reportColumns.forEach(col => {
            const idx = headers.indexOf(resolveAMField_(col));
            if (idx !== -1) colIndexMap[col] = idx;
        });
        ageBracketPairs.forEach(pair => {
            const maleIdx = headers.indexOf(resolveAMField_(pair.male));
            const femaleIdx = headers.indexOf(resolveAMField_(pair.female));
            if (maleIdx !== -1) colIndexMap[pair.male] = maleIdx;
            if (femaleIdx !== -1) colIndexMap[pair.female] = femaleIdx;
        });
        additionalColumns.forEach(col => {
            const idx = headers.indexOf(resolveAMField_(col));
            if (idx !== -1) colIndexMap[col] = idx;
        });

        const requestDateCol = headers.indexOf(resolveAMField_('Request Date'));
        const schedDistribCodeCol = headers.indexOf(resolveAMField_('Scheduled Distribution Code'));

        const records = [];
        for (let i = 1; i < data.length; i++) {
            const firstName = (data[i][firstNameCol] || '').toString().trim().toLowerCase();
            const lastName = (data[i][lastNameCol] || '').toString().trim().toLowerCase();
            const nameKey = firstName + '|' + lastName;

            if (!namesInRange.has(nameKey)) continue;

            const requestDate = data[i][requestDateCol];
            if (requestDate) {
                const reqDate = new Date(requestDate);
                if (reqDate < oneYearAgo) continue;
            }

            // Phase 4A.4: Determine if this row is "History" (ID outside the requested range)
            const rowId = parseInt(data[i][idCol]) || 0;
            const isHistory = rowId < beginId || rowId > endId;

            records.push({
                rowData: data[i],
                lastName: data[i][lastNameCol] || '',
                firstName: data[i][firstNameCol] || '',
                requestDate: requestDate,
                rowId: rowId,
                isHistory: isHistory
            });
        }

        if (records.length === 0) {
            return { success: false, error: 'No records found within 1 year for the specified criteria' };
        }

        // Phase 4A.6: Sort by Last Name, First Name, ID (not Request Date)
        records.sort((a, b) => {
            const lastCmp = a.lastName.toString().localeCompare(b.lastName.toString());
            if (lastCmp !== 0) return lastCmp;
            const firstCmp = a.firstName.toString().localeCompare(b.firstName.toString());
            if (firstCmp !== 0) return firstCmp;
            return a.rowId - b.rowId;
        });

        // v5.18: Recompute History flag per name — the highest ID for each name is "current";
        // all lower IDs for the same name are History, regardless of whether they fall in the
        // beginId-endId range. This correctly flags older records for clients with multiple IDs.
        const maxIdPerName = new Map();
        for (const rec of records) {
            const nameKey = rec.firstName.toString().toLowerCase() + '|' + rec.lastName.toString().toLowerCase();
            const cur = maxIdPerName.get(nameKey) || 0;
            if (rec.rowId > cur) maxIdPerName.set(nameKey, rec.rowId);
        }
        for (const rec of records) {
            const nameKey = rec.firstName.toString().toLowerCase() + '|' + rec.lastName.toString().toLowerCase();
            const maxId = maxIdPerName.get(nameKey) || rec.rowId;
            rec.isHistory = rec.rowId < maxId;
        }

        const titleCode = beginId + '-' + endId;
        const reportName = 'Scheduling_' + titleCode + '_' + Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd');
        const spreadsheet = SpreadsheetApp.create(reportName);
        const reportSheet = spreadsheet.getActiveSheet();

        // Move to Scheduling folder
        const folderId = CONFIG.SCHEDULING_FOLDER_ID;
        if (folderId && folderId.length > 0) {
            try {
                moveToFolder(spreadsheet.getId(), folderId);
            } catch (folderError) {
                Logger.log('ERROR moving to Scheduling folder: ' + folderError.message);
            }
        }

        // Build header row - History + reportColumns + combined age bracket labels + additionalColumns
        // Phase 4A.4: Add History as first column
        const allReportHeaders = ['History', ...reportColumns];
        ageBracketPairs.forEach(pair => {
            allReportHeaders.push(pair.label);
        });
        allReportHeaders.push(...additionalColumns);

        // Set headers - force plain text format first to prevent "5-9" being interpreted as dates
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setNumberFormat('@');
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setValues([allReportHeaders]);
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setFontWeight('bold').setBackground('#4a86e8').setFontColor('white');

        // Build data rows
        const dataRows = [];
        for (const rec of records) {
            const row = [];

            // Phase 4A.4: History column first
            row.push(rec.isHistory ? 'History' : '');

            // Add regular columns
            for (const col of reportColumns) {
                const idx = colIndexMap[col];
                if (idx !== undefined) {
                    let val = rec.rowData[idx];
                    if (val instanceof Date) {
                        val = Utilities.formatDate(val, CONFIG.TIMEZONE, 'M/d/yyyy');
                    }
                    // Auto-uppercase Scheduled Distribution Code values
                    if (col === 'Scheduled Distribution Code' && val && typeof val === 'string') {
                        val = val.toUpperCase();
                    }
                    // v5.12: Format phone number as (XXX) XXX-XXXX
                    if (col === 'Phone Number') {
                        val = formatPhoneNumber_(val);
                    }
                    row.push(val || '');
                } else {
                    row.push('');
                }
            }

            // Add combined age brackets
            for (const pair of ageBracketPairs) {
                const maleIdx = colIndexMap[pair.male];
                const femaleIdx = colIndexMap[pair.female];
                const maleVal = maleIdx !== undefined ? (parseInt(rec.rowData[maleIdx]) || 0) : 0;
                const femaleVal = femaleIdx !== undefined ? (parseInt(rec.rowData[femaleIdx]) || 0) : 0;
                const combined = maleVal + femaleVal;
                row.push(combined > 0 ? combined : '');
            }

            // Add additional columns
            for (const col of additionalColumns) {
                const idx = colIndexMap[col];
                if (idx !== undefined) {
                    let val = rec.rowData[idx];
                    if (val instanceof Date) {
                        val = Utilities.formatDate(val, CONFIG.TIMEZONE, 'M/d/yyyy');
                    }
                    row.push(val || '');
                } else {
                    row.push('');
                }
            }

            dataRows.push(row);
        }

        if (dataRows.length > 0) {
            reportSheet.getRange(2, 1, dataRows.length, allReportHeaders.length).setValues(dataRows);
        }

        // Set explicit column widths (History col first, then rest shifted by 1)
        // v5.18: Added Referrer Title (172) after Referral Info at position 14,
        //        and How Products Help (732) at the end as last column.
        const colWidths = [55, 99, 91, 200, 44, 59, 63, 85, 106, 101, 95, 75, 172, 172, 128, 244, 87, 145, 84, 76, 66, 57, 126, 94, 79, 186, 141, 52, 26, 41, 41, 41, 41, 41, 41, 41, 41, 77, 221, 86, 172, 97, 732, 732];
        for (let i = 0; i < colWidths.length && i < allReportHeaders.length; i++) {
            reportSheet.setColumnWidth(i + 1, colWidths[i]);
        }

        // Wrap header row and freeze it
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setWrap(true);
        reportSheet.setFrozenRows(1);

        // Wrap last 2 long-text columns (Briefly Explain + How Products Help) if present
        const lastCol = allReportHeaders.length;
        if (dataRows.length > 0) {
            reportSheet.getRange(2, lastCol, dataRows.length, 1).setWrap(true);       // How Products Help
            if (lastCol > 1) {
                reportSheet.getRange(2, lastCol - 1, dataRows.length, 1).setWrap(true); // Briefly Explain
            }
        }

        // Fix row 2 height (set to default height of 21)
        if (dataRows.length > 0) {
            reportSheet.setRowHeight(2, 21);
        }

        // Phase 4A.5: Add data validation dropdowns for Sched Distrib Code and Service Status
        if (dataRows.length > 0) {
            try {
                // SchedDistribCode dropdown (column index in allReportHeaders)
                var schedCodeColIdx = allReportHeaders.indexOf('Scheduled Distribution Code');
                if (schedCodeColIdx !== -1) {
                    var schedCodes = getLookupValues('SCHED_DISB_CODES', 'SchedDisbCode');
                    if (schedCodes && schedCodes.length > 0) {
                        var schedRule = SpreadsheetApp.newDataValidation()
                            .requireValueInList(schedCodes, true)
                            .setAllowInvalid(true) // Allow keeping existing values
                            .build();
                        reportSheet.getRange(2, schedCodeColIdx + 1, dataRows.length, 1).setDataValidation(schedRule);
                    }
                }

                // Service Status dropdown
                var statusColIdx = allReportHeaders.indexOf('Service Status');
                if (statusColIdx !== -1) {
                    var statusValues = getLookupValues('SERVICE_STATUS', 'Status');
                    if (statusValues && statusValues.length > 0) {
                        var statusRule = SpreadsheetApp.newDataValidation()
                            .requireValueInList(statusValues, true)
                            .setAllowInvalid(true)
                            .build();
                        reportSheet.getRange(2, statusColIdx + 1, dataRows.length, 1).setDataValidation(statusRule);
                    }
                }
            } catch (valError) {
                Logger.log('Data validation warning: ' + valError.message);
            }
        }

        // Phase 4A.7: Highlight the endId row in AM with Magenta background
        try {
            var masterSheet = getMasterSheet();
            if (masterSheet) {
                var amData = masterSheet.getDataRange().getValues();
                var amHeaders = trimHeaders(amData[0]);
                var amIdCol = amHeaders.indexOf(resolveAMField_('ID'));
                if (amIdCol !== -1) {
                    for (var r = 1; r < amData.length; r++) {
                        if (parseInt(amData[r][amIdCol]) === endId) {
                            masterSheet.getRange(r + 1, 1, 1, amHeaders.length).setBackground('#FF00FF');
                            break;
                        }
                    }
                }
            }
        } catch (highlightError) {
            Logger.log('Magenta highlight warning: ' + highlightError.message);
        }

        logAudit('REPORT', null, 'Generated Scheduling Report for IDs ' + beginId + '-' + endId + ' with ' + records.length + ' records');

        // v5.18: Record endId and report date in LU_LastScheduled for next Beginning ID auto-fill
        try {
            var reportDate = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yyyy');
            appendLastScheduledId(endId, reportDate);
        } catch (luErr) {
            Logger.log('appendLastScheduledId warning (non-fatal): ' + luErr.message);
        }

        return {
            success: true,
            message: 'Scheduling report generated',
            recordCount: records.length,
            beginId: beginId,
            endId: endId,
            reportUrl: spreadsheet.getUrl(),
            reportId: spreadsheet.getId()
        };

    } catch (error) {
        Logger.log('Scheduling report error: ' + error.message);
        return { success: false, error: 'Report generation failed: ' + error.message };
    }
}

/**
 * Phase 4A.8: Processes the latest Scheduling Report
 * Finds the most recent report in the Scheduling folder, reads non-History rows,
 * and updates AM records:
 *   - If SchedDisbCode is present → looks up LU_SchedDisbCodes to get
 *     StartDate, Interval, FundingSource; writes SchedDisbCode to AM,
 *     sets Service Status to "Scheduled", sets Distribution Start Date
 *     to StartDate, sets Distribution Interval, sets Funding Source,
 *     extracts Generic Distribution Code from alpha prefix of code
 *   - If SchedDisbCode is blank but Service Status edited → writes status back,
 *     sets Last Date Served and Final Service Contact Date to current date,
 *     sets Next Service Availability Date to current date + 90 days
 * Uses caching to avoid repeated lookups for the same code
 * @returns {Object} { success, updatedCount, reportName, log[] }
 */
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
            if (file.getMimeType() === 'application/vnd.google-apps.spreadsheet' && file.getName().indexOf('Scheduling_') === 0) {
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
        var rptIdCol = rptHeaders.indexOf(resolveAMField_('ID'));
        var rptSchedCodeCol = rptHeaders.indexOf(resolveAMField_('Scheduled Distribution Code'));
        var rptStatusCol = rptHeaders.indexOf(resolveAMField_('Service Status'));

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
        var amIdCol = masterHeaders.indexOf(resolveAMField_('ID'));
        var amSchedCodeCol = masterHeaders.indexOf(resolveAMField_('Scheduled Distribution Code'));
        var amStatusCol = masterHeaders.indexOf(resolveAMField_('Service Status'));
        var amDistribStartDateCol = masterHeaders.indexOf(resolveAMField_('Distribution Start Date'));
        var amDistribIntervalCol = masterHeaders.indexOf(resolveAMField_('Distribution Interval'));
        var amFundingSourceCol = masterHeaders.indexOf(resolveAMField_('Funding Source'));
        var amGenericDistribCodeCol = masterHeaders.indexOf(resolveAMField_('Generic Distribution Code'));
        var amLastDateServedCol = masterHeaders.indexOf(resolveAMField_('Last Date Served'));
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

            if (fileName.indexOf(distribCode) > -1 && file.getMimeType() === 'application/vnd.google-apps.spreadsheet') {
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

        var reportSS = SpreadsheetApp.openById(reportId);
        var reportSheet = reportSS.getActiveSheet();
        var reportData = reportSheet.getDataRange().getValues();

        // Find header row and column positions
        var headerRowIndex = -1;
        var datePickedUpCol = -1;
        var extraBoxCol = -1;
        var submissionCol = -1;
        for (var i = 0; i < reportData.length; i++) {
            for (var j = 0; j < reportData[i].length; j++) {
                var cellVal = (reportData[i][j] || '').toString().trim();
                if (cellVal === 'Submission #') { headerRowIndex = i; submissionCol = j; }
                if (cellVal === 'Date Picked Up') datePickedUpCol = j;
                if (cellVal === 'Extra Box') extraBoxCol = j;
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

            var masterSheet = getMasterSheet();
            var masterData = masterSheet.getDataRange().getValues();
            var masterHdrs = trimHeaders(masterData[0]);
            var amIdCol = masterHdrs.indexOf(resolveAMField_('ID'));
            var amBox3Col = masterHdrs.indexOf(resolveAMField_('Scheduled Box Code 3'));

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
                    var rowId = (rows[r2]['Submission #'] || '').toString().trim();
                    var xVal2 = (rows[r2]['Extra Box'] || '').toString().trim().toUpperCase();
                    var newBox3 = xVal2 === 'X' ? luBox3 : '';
                    var amRow = idToRow[rowId];
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

        const idCol = masterHeaders.indexOf(resolveAMField_('ID'));
        const serviceStatusCol = masterHeaders.indexOf(resolveAMField_('Service Status'));
        const lastDateServedCol = masterHeaders.indexOf(resolveAMField_('Last Date Served'));
        const finalServiceContactDateCol = masterHeaders.indexOf(resolveAMField_('Final Service Contact Date'));
        const nextServiceAvailCol = masterHeaders.indexOf(resolveAMField_('Next Service Availability Date'));
        const firstServiceContactDateCol = masterHeaders.indexOf(resolveAMField_('First Service Contact Date'));
        const boxCodeCol = masterHeaders.indexOf(resolveAMField_('Box Code'));
        const receivedProductCode1Col = masterHeaders.indexOf(resolveAMField_('Received Product Code 1'));
        const receivedProductCode2Col = masterHeaders.indexOf(resolveAMField_('Received Product Code 2'));
        const receivedProductCode3Col = masterHeaders.indexOf(resolveAMField_('Received Product Code 3'));
        const schedBoxCode1Col = masterHeaders.indexOf(resolveAMField_('Scheduled Box Code 1'));
        const schedBoxCode2Col = masterHeaders.indexOf(resolveAMField_('Scheduled Box Code 2'));
        const schedBoxCode3Col = masterHeaders.indexOf(resolveAMField_('Scheduled Box Code 3'));
        const requestDateCol = masterHeaders.indexOf(resolveAMField_('Request Date')); // v5.14

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
            const isDRPF = function (v) { var u = v.toUpperCase(); return u.indexOf('DR') === 0 || u.indexOf('PF') === 0; };
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
                    var pfIdCol = pfHdrs.indexOf('ID');
                    var pfDateCol = pfHdrs.indexOf('RequestDate');
                    var pfQtyReqCol = pfHdrs.indexOf('QtyRequested');
                    var pfQtyRecCol = pfHdrs.indexOf('QtyReceived');
                    if (pfIdCol !== -1 && pfDateCol !== -1 && pfQtyReqCol !== -1 && pfQtyRecCol !== -1) {
                        // Build per-ID row count map: { "id|date" → updatedCount }
                        var updateKeys = {};
                        for (var u = 0; u < extraBoxProductUpdates.length; u++) {
                            updateKeys[extraBoxProductUpdates[u].id + '|' + extraBoxProductUpdates[u].requestDate] = 0;
                        }
                        var pfTotalUpdated = 0;
                        for (var p = 1; p < pfData.length; p++) {
                            var pfRowId = (pfData[p][pfIdCol] || '').toString().trim();
                            var pfRowDate = normalizeDate(pfData[p][pfDateCol]);
                            var pfKey = pfRowId + '|' + pfRowDate;
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

                    log.push({
                        status: 'success', message: 'Inserted stats into Hygiene Stats sheet "' + statsSheet.getName() +
                            '": ' + totalScheduledRecipients + ' recipients, ' + totalScheduledBoxes + ' boxes, ' +
                            pickedUpGeneric + ' picked up, ' + restockRecipients + ' restock (' + pctNoPickUp + ')'
                    });
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
        const requestDateCol = headers.indexOf(resolveAMField_('Request Date'));
        const finalServiceContactDateCol = headers.indexOf(resolveAMField_('Final Service Contact Date'));
        const idCol = headers.indexOf(resolveAMField_('ID'));

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
                    archiveIds.forEach(function (rec) {
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
        const requestDateCol = headers.indexOf(resolveAMField_('Request Date'));
        const idCol = headers.indexOf(resolveAMField_('ID'));
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
                log.push({
                    status: rolloverResult.success ? 'success' : 'error',
                    message: 'Auto-rollover (' + rolloverResult.capacityPct + '% capacity): ' +
                        (rolloverResult.success ? rolloverResult.message : rolloverResult.error)
                });
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
    archiveIdDates.forEach(function (rec) {
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
        events.sort(function (a, b) { return a.timestamp - b.timestamp; });
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

        results.sort(function (a, b) { return b.totalMinutes - a.totalMinutes; });

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
        var intakeEvents = (rawEvents || []).filter(function (ev) {
            return ev.action === 'INTAKE_SESSION' || ev.action === 'CREATE';
        });

        if (intakeEvents.length > 0) {
            var intakeSheet = ss.insertSheet('Intake Activity');

            // Summary section
            var intakeSessions = intakeEvents.filter(function (ev) { return ev.action === 'INTAKE_SESSION'; });
            var intakeCreates = intakeEvents.filter(function (ev) { return ev.action === 'CREATE'; });

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
            var sessionPool = intakeSessions.map(function (s) { return { ts: s.timestamp.getTime(), matched: false }; });
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
        events.sort(function (a, b) { return a.timestamp - b.timestamp; });
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
