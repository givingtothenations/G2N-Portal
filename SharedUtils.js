/**
 * SharedUtils.gs
 * Shared utility functions for the G2N System.
 * Extracted from Code.gs and GrantsReportService.gs to centralize
 * reusable helpers and eliminate duplication across service files.
 *
 * v1.0 - Initial creation. Consolidated from:
 * v1.1 - Added resolveAMField_() and getReportHeader_() — FieldMapService
 *         integration helpers shared across all service files.
 *         Code.gs: trimHeaders(), htmlDateToSheet(), extractYear(), isRowActive()
 *         GrantsReportService.gs: parseDateInput(), getStr(), getApplicantType(),
 *           getBabyBoxIndicator(), moveToFolder(), styleReportHeader(),
 *           styleGroupRow(), writeReportTitleSection()
 *         ProductService.gs: normalizeProductDate() → renamed normalizeDate()
 * v1.2 - isRowActive() aligned with AP isBooleanChecked() — now accepts
 *         boolean true, string 'TRUE'/'true'/'Y'/'YES'/'Yes', number 1,
 *         string '1'. Previously accepted only boolean true and 'TRUE',
 *         causing getActiveSchedDisbCodes() to return empty when Active
 *         column contained 'Y' or other truthy variants.
 *         Fixes Distribution Report / Process Distribution dropdown empty.
 * 
 * Sections:
 *   DATE UTILITIES — trimHeaders, htmlDateToSheet, extractYear, parseDateInput, normalizeDate
 *   ROW UTILITIES — isRowActive, getStr, getApplicantType, getBabyBoxIndicator
 *   FILE UTILITIES — moveToFolder
 *   REPORT STYLING UTILITIES — styleReportHeader, styleGroupRow, writeReportTitleSection
 */

// ============ FIELDMAP HELPERS ============

/**
 * Resolve an Applicants_Master raw column header via LU_FieldMap.
 * Returns the canonical rawHeader from FieldMapService if the field is mapped,
 * otherwise returns the input unchanged. Used by all service files to avoid
 * hardcoded AM field name strings scattered across the codebase.
 * @param {string} rawHeader - Raw AM column header (e.g. 'First Name')
 * @returns {string} Canonical raw header from LU_FieldMap, or rawHeader if unmapped
 */
function resolveAMField_(rawHeader) {
    try {
        var map = loadFieldMap();
        var entry = map.byRawHeader[rawHeader];
        return entry ? entry.rawHeader : rawHeader;
    } catch (e) {
        return rawHeader;
    }
}

/**
 * Get the report-friendly display label for an AM column.
 * Returns Report Header from LU_FieldMap if defined, then Display Label,
 * then the raw header as fallback. Used when writing column headers to
 * generated report spreadsheets.
 * @param {string} rawHeader - Raw AM column header
 * @returns {string} Friendly label for report output
 */
function getReportHeader_(rawHeader) {
    try {
        var map = loadFieldMap();
        var entry = map.byRawHeader[rawHeader];
        if (!entry) return rawHeader;
        return entry.reportHeader || entry.displayLabel || rawHeader;
    } catch (e) {
        return rawHeader;
    }
}


// ============ DATE UTILITIES ============

/**
 * Trims all header values in an array — replaces the 43+ inline
 * .map(function(h) { return h.toString().trim(); }) calls across the codebase.
 * @param {Array} rawHeaders - Raw header row from sheet.getValues()[0]
 * @returns {string[]} Trimmed header strings
 */
function trimHeaders(rawHeaders) {
    return rawHeaders.map(function (h) { return h.toString().trim(); });
}

/**
 * Converts HTML date input (YYYY-MM-DD) to sheet format (MM/DD/YYYY).
 * Returns the original value unchanged if it doesn't match the pattern.
 * @param {string} dateStr - Date string, possibly in YYYY-MM-DD format
 * @returns {string} Converted MM/DD/YYYY string, or original value
 */
function htmlDateToSheet(dateStr) {
    if (!dateStr || typeof dateStr !== 'string') return dateStr;
    var m = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    return m ? m[2] + '/' + m[3] + '/' + m[1] : dateStr;
}

/**
 * Extracts a 4-digit year string from a date value.
 * Handles Date objects and parseable date strings.
 * @param {*} dateVal - Date object or parseable date string
 * @returns {string|null} 4-digit year string, or null if unparseable
 */
function extractYear(dateVal) {
    if (!dateVal) return null;
    if (dateVal instanceof Date && !isNaN(dateVal.getTime())) {
        return dateVal.getFullYear().toString();
    }
    var parsed = new Date(dateVal);
    return !isNaN(parsed.getTime()) ? parsed.getFullYear().toString() : null;
}

/**
 * Parse a date input string (YYYY-MM-DD from HTML date input) to Date object.
 * Sets time to start of day for fromDate, end of day for toDate.
 * Moved from GrantsReportService.gs to centralize date parsing.
 * @param {string} dateStr - Date string in YYYY-MM-DD format
 * @param {boolean} endOfDay - If true, set to 23:59:59.999
 * @returns {Date}
 */
function parseDateInput(dateStr, endOfDay) {
    var parts = dateStr.split('-');
    var d = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
    if (endOfDay) {
        d.setHours(23, 59, 59, 999);
    } else {
        d.setHours(0, 0, 0, 0);
    }
    return d;
}

/**
 * Normalizes various date formats to M/d/yyyy for consistent comparison.
 * Handles: Date objects, YYYY-MM-DD (HTML), M/D/YYYY, MM/DD/YYYY.
 * Renamed from normalizeProductDate() in ProductService.gs for general use.
 * Uses CONFIG.TIMEZONE for all date formatting.
 * @param {*} dateVal - Date value in any supported format
 * @returns {string} Normalized date (M/d/yyyy) or empty string
 */
function normalizeDate(dateVal) {
    if (!dateVal) return '';

    try {
        if (dateVal instanceof Date && !isNaN(dateVal.getTime())) {
            return Utilities.formatDate(dateVal, CONFIG.TIMEZONE, 'M/d/yyyy');
        }

        var str = dateVal.toString().trim();

        // YYYY-MM-DD format (from HTML date input)
        if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
            var parts = str.split('-');
            var d = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
            return Utilities.formatDate(d, CONFIG.TIMEZONE, 'M/d/yyyy');
        }

        // M/D/YYYY or MM/DD/YYYY format
        if (str.indexOf('/') > -1) {
            var parsed = new Date(str);
            if (!isNaN(parsed.getTime())) {
                return Utilities.formatDate(parsed, CONFIG.TIMEZONE, 'M/d/yyyy');
            }
        }

        // Try generic parse
        var generic = new Date(str);
        if (!isNaN(generic.getTime())) {
            return Utilities.formatDate(generic, CONFIG.TIMEZONE, 'M/d/yyyy');
        }

        return '';
    } catch (e) {
        Logger.log('normalizeDate error: ' + e.message);
        return '';
    }
}


// ============ ROW UTILITIES ============

/**
 * Checks if a data row is "active" based on the Active column value.
 * @param {Array}  row            - Data row from sheet
 * @param {number} activeColIndex - Column index of Active field (-1 if absent)
 * @returns {boolean}
 */
function isRowActive(row, activeColIndex) {
    if (activeColIndex === -1) return true;
    var val = row[activeColIndex];
    if (val === true || val === 1) return true;
    if (val === false || val === 0 || val === '' || val === null || val === undefined) return false;
    var s = val.toString().trim().toUpperCase();
    return s === 'TRUE' || s === 'Y' || s === 'YES' || s === '1';
}

/**
 * Safe string getter from row by column index.
 * Returns trimmed string or empty string if index is -1.
 * Moved from GrantsReportService.gs to centralize row access.
 * @param {Array} row - Data row
 * @param {number} idx - Column index (-1 returns '')
 * @returns {string}
 */
function getStr(row, idx) {
    return idx !== -1 ? (row[idx] || '').toString().trim() : '';
}

/**
 * Get Applicant Type from "Have you used our services before?" value.
 * Returns "Existing" for "yes", "New" for "no", or the original value.
 * Moved from GrantsReportService.gs to centralize applicant type logic.
 * @param {string} usedBefore - Raw value from sheet
 * @returns {string} "Existing", "New", or original value
 */
function getApplicantType(usedBefore) {
    var val = (usedBefore || '').toString().trim().toLowerCase();
    if (val === 'yes') return 'Existing';
    if (val === 'no') return 'New';
    return usedBefore || '';
}

/**
 * Get Children 2 and Under indicator from Take Baby Box value.
 * Returns "X" for affirmative values, empty string otherwise.
 * Moved from GrantsReportService.gs to centralize baby box logic.
 * @param {string} babyBoxVal - Raw value from sheet
 * @returns {string} "X" or ""
 */
function getBabyBoxIndicator(babyBoxVal) {
    var val = (babyBoxVal || '').toString().trim().toUpperCase();
    return (val === 'X' || val === 'YES' || val === 'TRUE') ? 'X' : '';
}


// ============ FILE UTILITIES ============

/**
 * Move a file to a specified Google Drive folder.
 * Removes the file from the root folder after adding to target.
 * Moved from GrantsReportService.gs — replaces 4 inline duplicates in
 * ReportService.gs and 1 in Code.gs.
 * @param {string} fileId - The file ID (spreadsheet, doc, etc.)
 * @param {string} folderId - Target folder ID
 */
function moveToFolder(fileId, folderId) {
    if (folderId && folderId.length > 0) {
        try {
            var file = DriveApp.getFileById(fileId);
            var folder = DriveApp.getFolderById(folderId);
            folder.addFile(file);
            DriveApp.getRootFolder().removeFile(file);
        } catch (e) {
            Logger.log('Error moving file to folder: ' + e.message);
        }
    }
}


// ============ REPORT STYLING UTILITIES ============

/**
 * Apply standard report header styling (blue background, white bold text).
 * Moved from GrantsReportService.gs to centralize report styling.
 * @param {Sheet} sheet - The sheet to style
 * @param {number} headerRow - Row number of the data header
 * @param {number} colCount - Number of columns
 */
function styleReportHeader(sheet, headerRow, colCount) {
    var range = sheet.getRange(headerRow, 1, 1, colCount);
    range.setFontWeight('bold');
    range.setBackground('#4a86e8');
    range.setFontColor('white');
    range.setBorder(true, true, true, true, true, true);
    range.setWrap(true);
}

/**
 * Apply group header row styling with merge and background color.
 * Moved from GrantsReportService.gs to centralize report styling.
 * @param {Sheet} sheet - The sheet to style
 * @param {number} row - Row number
 * @param {number} colCount - Number of columns
 * @param {string} bgColor - Background color (default: '#e8f0fe')
 */
function styleGroupRow(sheet, row, colCount, bgColor) {
    var range = sheet.getRange(row, 1, 1, colCount);
    range.setFontWeight('bold');
    range.setBackground(bgColor || '#e8f0fe');
    range.merge();
}

/**
 * Write a standard report title section with org name, report title,
 * date period, record counts, data source note, and generation timestamp.
 * Uses CONFIG.TIMEZONE for all date formatting.
 * Moved from GrantsReportService.gs to centralize report title generation.
 * @param {Sheet} sheet - Target sheet
 * @param {string} reportTitle - Report name
 * @param {Date} fromDate - Start date
 * @param {Date} toDate - End date
 * @param {number} totalCount - Total record count
 * @param {number} masterCount - Master sheet count
 * @param {number} archiveCount - Archive count
 * @returns {number} Next row number after title section
 */
function writeReportTitleSection(sheet, reportTitle, fromDate, toDate, totalCount, masterCount, archiveCount) {
    var currentRow = 1;

    sheet.getRange(currentRow, 1).setValue('Giving to the Nations');
    sheet.getRange(currentRow, 1).setFontWeight('bold').setFontSize(14);
    currentRow++;

    sheet.getRange(currentRow, 1).setValue(reportTitle);
    sheet.getRange(currentRow, 1).setFontWeight('bold').setFontSize(12);
    currentRow++;

    var fromDisplay = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    var toDisplay = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    sheet.getRange(currentRow, 1).setValue('Report Period: ' + fromDisplay + ' to ' + toDisplay);
    currentRow++;

    sheet.getRange(currentRow, 1).setValue('Total Records: ' + totalCount);
    sheet.getRange(currentRow, 1).setFontWeight('bold');
    currentRow++;

    var sourceNote = 'Data Sources: Applicants_Master (' + masterCount + ' records)';
    if (archiveCount > 0) {
        sourceNote += ', G2N_Archive (' + archiveCount + ' records)';
    }
    sheet.getRange(currentRow, 1).setValue(sourceNote);
    sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(10).setFontColor('#666666');
    currentRow++;

    var createdDate = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yyyy h:mm a');
    sheet.getRange(currentRow, 1).setValue('Generated: ' + createdDate);
    sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(10).setFontColor('#666666');
    currentRow++;

    currentRow++; // Blank row
    return currentRow;
}

function formatPhoneNumber_(raw) {
    var digits = (raw || '').toString().replace(/\D/g, '');
    if (digits.length === 10) {
        return '(' + digits.substring(0, 3) + ') ' + digits.substring(3, 6) + '-' + digits.substring(6);
    }
    return (raw || '').toString().trim();
}