/**
 * RequestService.gs
 * CRUD operations for Applicants_Master records via the Staff/Volunteer Portal
 * Handles record updates with change tracking, new record creation,
 * and provides field definitions and lookup data for the portal UI.
 * v2.3 - Corrected field mappings to match Applicants_Master headers
 * v2.4 - Removed excessive debug Logger.log calls (#8).
 *         Adopted shared trimHeaders() (#11) and htmlDateToSheet() (#12).
 *         Uses cached getMasterWorkbook() instead of direct openById().
 * v2.5 - Performance: updateRequest() now collects all changes in memory
 *         and writes back the entire row in a single setValues() call,
 *         replacing per-field setValue() calls (10 fields = 10→1 API call).
 * v2.6 - Fixed appendRow race in createNewRequest(): replaced
 *         setNumberFormat-before-appendRow with setValues at deterministic row.
 * v2.7 - Fixed EDITABLE_FIELDS: added 'Scheduled Box Code 3' and
 *         'Received Product Code 3' for v4.5 parity (#2). Updated
 *         getFieldDefinitions() with Box Code 3 fields (#2). Adopted
 *         CONFIG.TIMEZONE (#8). Added TODO notes for future FieldMapService
 *         migration (#6, #7). Marked getSheetHeaders() usage as deprecated (#14).
 * v2.8 - Added 'Admin Notes' to EDITABLE_FIELDS and getFieldDefinitions()
 *         adminNotes section. Displayed as editable textarea in the
 *         Information section of StaffVolunteerPortalWeb. Included in
 *         Scheduling Report via ReportService.gs generateSchedulingReport().
 * v2.9 - Migrated EDITABLE_FIELDS and getFieldDefinitions() to LU_FieldMap sheet-
 *         driven sources. Removed hardcoded EDITABLE_FIELDS const (resolved TODO #7);
 *         getFieldDefinitions() now delegates to FieldMapService.getStaffPortalSections()
 *         (resolved TODO #6). getPortalData() returns full portal data set including
 *         fieldMap, editableFields, and dropdownFields for use by SV portal.
 * v3.0 - Added updateIntakeRecord(): updates AM via AI form field IDs, writes 3 new
 *         audit raw headers (Updated By, Updated Date, Update Applied). Adds those
 *         columns to AM header row if they don't yet exist. Update Applied stores
 *         a semicolon-separated before→after string for every changed field.
 *         Added getApplicantEditData(): returns lookups + dropdownFields + fieldMapping
 *         for the Admin Portal Edit Applicant tab.
 * v3.4 - updateArchiveRecord(): added temporary diagnostic Logger.log calls.
 *         Console.log added to SV saveRecord() for client-side visibility.
 * v3.5 - Removed all temporary diagnostic Logger.log calls from updateArchiveRecord()
 *         and console.log from SV saveRecord(). No logic change.
 * v3.3 - getDemographicsStatus(recordId, archiveSource): reads Demographics-group
 *         fields from LU_FieldMap, checks the given record (AM or archive), and
 *         returns blankCount / totalCount / isMostlyBlank (>= 80%) so the SV portal
 *         can prompt staff to complete missing intake data.
 * v3.2 - updateArchiveRecord(): replaced full getDataRange().getValues() with
 *         two-step targeted read (ID column only, then single row). Eliminates
 *         GAS timeout on large archive workbooks. ID column looked up as plain
 *         'ID' string (not via COL_ID constant) — archive sheet always uses
 *         this header, and avoiding the constant eliminates file-load-order
 *         dependency with SharedUtils.gs.
 * v3.1 - Added updateArchiveRecord().
 *         in an archive workbook by ID. Mirrors searchArchiveSheets_ workbook-location
 *         logic (G2N_Archive by CONFIG.ARCHIVE_WORKBOOK_ID; G2N_Archive_YYYY by
 *         ARCHIVES_BACKUPS_FOLDER_ID folder scan). Same change-tracking / bulk-write
 *         / AuditLog pattern as updateRequest(). Called by SV saveRecord() when
 *         currentRecord._archived is true.
 */

/**
 * Updates an existing record in Applicants_Master
 * Reads the row, modifies changed fields in memory, writes back in a
 * single setValues() call. Logs each change to AuditLog.
 * v2.5 - Performance: single bulk write replaces per-field setValue()
 * @param {number} rowIndex - 1-based sheet row number to update
 * @param {Object} formData - Key/value pairs of field names and new values
 * @returns {Object} { success: boolean, changesCount: number }
 */
function updateRequest(rowIndex, formData) {
    if (CONFIG.DB && CONFIG.DB.USE_MYSQL) {
        var user = getCurrentUser();
        return DbService.updateRequest(rowIndex, formData, user ? user.code : null);
    }
    try {
        // Validate parameters
        if (rowIndex === undefined || rowIndex === null || isNaN(rowIndex)) {
            return { success: false, error: 'Invalid row index: ' + rowIndex };
        }

        if (!formData || typeof formData !== 'object') {
            return { success: false, error: 'Invalid form data' };
        }

        const ss = getMasterWorkbook();
        const sheet = ss.getSheetByName(CONFIG.MASTER_SHEET);

        if (!sheet) {
            return { success: false, error: 'Master sheet not found' };
        }

        const rawHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
        const headers = trimHeaders(rawHeaders);
        const currentRow = sheet.getRange(rowIndex, 1, 1, sheet.getLastColumn()).getValues()[0];
        const recordId = currentRow[0];

        // Work on a copy of the row in memory
        const updatedRow = currentRow.slice();
        const changes = [];

        // Update each field that was provided
        for (var fieldName in formData) {
            var colIndex = headers.indexOf(fieldName.trim());

            if (colIndex !== -1) {
                var oldValue = currentRow[colIndex];
                var newValue = formData[fieldName];

                // Convert HTML date format (YYYY-MM-DD) to MM/DD/YYYY for sheet
                newValue = htmlDateToSheet(newValue);

                // Normalize old value for comparison
                var oldValueStr = '';
                if (oldValue !== null && oldValue !== undefined) {
                    if (oldValue instanceof Date) {
                        oldValueStr = Utilities.formatDate(oldValue, CONFIG.TIMEZONE, 'M/d/yyyy');
                    } else {
                        oldValueStr = oldValue.toString();
                    }
                }

                // Only update if value changed
                if (oldValueStr !== newValue && (newValue !== '' || oldValueStr !== '')) {
                    updatedRow[colIndex] = newValue;
                    changes.push(fieldName + ': "' + oldValueStr + '" \u2192 "' + newValue + '"');
                }
            }
        }

        // Single bulk write if anything changed
        if (changes.length > 0) {
            sheet.getRange(rowIndex, 1, 1, headers.length).setValues([updatedRow]);
            logAudit('UPDATE', recordId, changes.join('; '));
        }

        return {
            success: true,
            message: 'Record ' + recordId + ' updated successfully',
            changesCount: changes.length
        };

    } catch (error) {
        Logger.log('Update error: ' + error.message);
        return { success: false, error: 'Update failed: ' + error.message };
    }
}

/**
 * Creates a new request record in Applicants_Master via Staff Portal
 * Generates next sequential ID, applies field values, logs creation
 * @param {Object} formData - Field values from the Staff Portal new request form
 * @returns {Object} { success: boolean, recordId: string }
 */
function createNewRequest(formData) {
    if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.createNewRequest(formData);
    try {
        const ss = getMasterWorkbook();
        const sheet = ss.getSheetByName(CONFIG.MASTER_SHEET);

        if (!sheet) {
            return { success: false, error: 'Master sheet not found' };
        }

        const rawHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
        const headers = trimHeaders(rawHeaders);

        // Get next ID
        const nextId = getNextId();

        // Create new row array
        const newRow = new Array(headers.length).fill('');
        newRow[0] = nextId;

        // Map form fields to columns
        for (var fieldName in formData) {
            var colIndex = headers.indexOf(fieldName);

            if (colIndex !== -1) {
                newRow[colIndex] = htmlDateToSheet(formData[fieldName]);
            }
        }

        // Set timestamp if not provided
        const timestampCol = headers.indexOf('Timestamp');
        if (timestampCol !== -1 && !newRow[timestampCol]) {
            newRow[timestampCol] = new Date();
        }

        // Write at deterministic row, then format ID column
        // v2.6 - Replaced appendRow with setValues to prevent race condition
        const newRowNum = sheet.getLastRow() + 1;
        sheet.getRange(newRowNum, 1, 1, headers.length).setValues([newRow]);
        sheet.getRange(newRowNum, 1, 1, 1).setNumberFormat('0');

        // Log creation
        logAudit('CREATE', nextId, 'New record created via Staff Portal');

        return {
            success: true,
            message: 'New record created with ID: ' + nextId,
            recordId: nextId
        };

    } catch (error) {
        Logger.log('Create error: ' + error.message);
        return { success: false, error: 'Create failed: ' + error.message };
    }
}

/**
 * Updates a record in an archive workbook by Record ID.
 * Called by SV saveRecord() when the loaded record came from an archive sheet.
 * Locates the workbook using the same strategy as searchArchiveSheets_():
 *   - 'G2N_Archive' → CONFIG.ARCHIVE_WORKBOOK_ID
 *   - 'G2N_Archive_YYYY' → scan CONFIG.ARCHIVES_BACKUPS_FOLDER_ID for matching file
 * Applies changes in memory and writes back as a single bulk setValues() call.
 * Change-tracking and AuditLog match updateRequest() behaviour.
 * v3.1 - New function.
 * @param {string|number} recordId    - The ID value of the record (used to locate the row)
 * @param {string}        archiveSource - Workbook name, e.g. 'G2N_Archive' or 'G2N_Archive_2023'
 * @param {Object}        formData    - Key/value field updates (raw AM column names)
 * @returns {Object} { success, changesCount, message }
 */
function updateArchiveRecord(recordId, archiveSource, formData) {
    try {
        if (!recordId) return { success: false, error: 'Record ID is required' };
        if (!archiveSource) return { success: false, error: 'Archive source is required' };
        if (!formData || typeof formData !== 'object') return { success: false, error: 'Invalid form data' };

        // ── Locate the archive workbook ─────────────────────────────────────────
        var archiveWB = null;
        if (archiveSource === 'G2N_Archive') {
            if (!CONFIG.ARCHIVE_WORKBOOK_ID) return { success: false, error: 'ARCHIVE_WORKBOOK_ID not configured' };
            archiveWB = SpreadsheetApp.openById(CONFIG.ARCHIVE_WORKBOOK_ID);
        } else {
            // Year-based workbook — scan ARCHIVES_BACKUPS_FOLDER_ID
            if (!CONFIG.ARCHIVES_BACKUPS_FOLDER_ID) return { success: false, error: 'ARCHIVES_BACKUPS_FOLDER_ID not configured' };
            var folder = DriveApp.getFolderById(CONFIG.ARCHIVES_BACKUPS_FOLDER_ID);
            var files = folder.getFiles();
            while (files.hasNext()) {
                var file = files.next();
                if (file.getMimeType() === 'application/vnd.google-apps.spreadsheet' && file.getName() === archiveSource) {
                    archiveWB = SpreadsheetApp.openById(file.getId());
                    break;
                }
            }
            if (!archiveWB) return { success: false, error: 'Archive workbook "' + archiveSource + '" not found' };
        }

        // ── Locate the Archive sheet and the target row ─────────────────────────
        var sheet = archiveWB.getSheetByName('Archive');
        if (!sheet) return { success: false, error: 'Archive sheet not found in "' + archiveSource + '"' };

        var lastRow = sheet.getLastRow();
        if (lastRow < 2) return { success: false, error: 'Archive sheet is empty' };

        // v3.2: Read only the header row + ID column (col A) to find the target row —
        // avoids sheet.getDataRange().getValues() which reads the full archive and can
        // cause GAS timeouts on large workbooks (10k+ rows × 100+ columns).
        var headers = trimHeaders(sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0]);
        var idCol = headers.indexOf('ID');
        if (idCol === -1) return { success: false, error: 'ID column not found in archive sheet' };

        // Read just the ID column (1-based colIndex = idCol + 1)
        var idColData = sheet.getRange(2, idCol + 1, lastRow - 1, 1).getValues();
        var targetSheetRow = -1; // 1-based sheet row
        for (var i = 0; i < idColData.length; i++) {
            if (parseInt(idColData[i][0]) === parseInt(recordId)) {
                targetSheetRow = i + 2; // +1 for header, +1 for 1-based
                break;
            }
        }
        if (targetSheetRow === -1) {
            return { success: false, error: 'Record ID ' + recordId + ' not found in "' + archiveSource + '"' };
        }

        // Read only the target row (all columns)
        var currentRow = sheet.getRange(targetSheetRow, 1, 1, headers.length).getValues()[0];

        // ── Apply changes in memory (same logic as updateRequest) ───────────────
        var updatedRow = currentRow.slice();
        var changes = [];

        for (var fieldName in formData) {
            var colIndex = headers.indexOf(fieldName.trim());
            if (colIndex === -1) continue;

            var oldValue = currentRow[colIndex];
            var newValue = htmlDateToSheet(formData[fieldName]);

            var oldValueStr = '';
            if (oldValue !== null && oldValue !== undefined) {
                oldValueStr = (oldValue instanceof Date)
                    ? Utilities.formatDate(oldValue, CONFIG.TIMEZONE, 'M/d/yyyy')
                    : oldValue.toString();
            }

            if (oldValueStr !== newValue && (newValue !== '' || oldValueStr !== '')) {
                updatedRow[colIndex] = newValue;
                changes.push(fieldName + ': "' + oldValueStr + '" \u2192 "' + newValue + '"');
            }
        }

        // ── Bulk write if anything changed ──────────────────────────────────────
        if (changes.length > 0) {
            sheet.getRange(targetSheetRow, 1, 1, headers.length).setValues([updatedRow]);
            logAudit('ARCHIVE_UPDATE', recordId, archiveSource + ': ' + changes.join('; '));
        } else {
            Logger.log('updateArchiveRecord: no changes detected for ID ' + recordId);
        }

        return {
            success: true,
            message: 'Archive record ' + recordId + ' updated in ' + archiveSource,
            changesCount: changes.length
        };

    } catch (e) {
        Logger.log('updateArchiveRecord error: ' + e.message);
        return { success: false, error: 'Archive update failed: ' + e.message };
    }
}


/**
 * Get field definitions for the Staff Portal form.
 * Delegates to FieldMapService.getStaffPortalSections() — sheet-driven via LU_FieldMap.
 * Resolved TODOs #6 (Form Section / Lookup Source columns) and #7 (Editable column).
 * @returns {Array} Ordered sections array: [{sectionKey, label, readOnly, fields[]}]
 */
function getFieldDefinitions() {
    return getStaffPortalSections().sections;
}

/**
 * Returns all data needed by the Staff Portal to initialize.
 * Combines lookups with sheet-driven field map, editable fields list,
 * section definitions, and dropdown field→lookupSource map from LU_FieldMap.
 * Called by StaffVolunteerPortalWeb on portal load.
 * @returns {Object} {
 *   lookups:        Object    - all lookup arrays (getAllLookups),
 *   fieldMap:       Object    - { formFieldId: rawHeader } replaces hardcoded FIELD_MAP,
 *   dropdownFields: Object    - { formFieldId: lookupSource } for populateDropdowns,
 *   sections:       Array     - ordered form section definitions,
 *   editableFields: string[]  - raw headers where Editable=Y
 * }
 */
function getPortalData() {
    var portalSections = getStaffPortalSections();
    var lookups = (CONFIG.DB && CONFIG.DB.USE_MYSQL) ? DbService.getAllLookups() : getAllLookups();
    return {
        lookups: lookups,
        fieldMap: portalSections.fieldMap,
        dropdownFields: portalSections.dropdownFields,
        sections: portalSections.sections,
        editableFields: portalSections.editableFields
    };
}

/**
 * Updates an existing applicant record in Applicants_Master using AI intake form field IDs.
 * Translates form field IDs → AM column headers via getIntakeFieldMapping().
 * Writes all changed field values, then writes 3 audit columns:
 *   "Updated By"     - staff code of the admin saving the record
 *   "Updated Date"   - timestamp of the update
 *   "Update Applied" - semicolon-separated "Header: 'before' → 'after'" for every change
 * If any of the 3 audit columns do not exist in AM, they are appended to the header row.
 * v3.0 - Added for Admin Portal Edit Applicant feature
 * @param {number} rowIndex  - 1-based sheet row to update
 * @param {Object} formData  - AI form field IDs (e.g. firstName) → new values
 * @param {string} updatedBy - Staff code of the admin making the change
 * @returns {Object} { success: boolean, changesCount: number, message: string }
 */
function updateIntakeRecord(rowIndex, formData, updatedBy) {
    try {
        if (!rowIndex || isNaN(rowIndex)) {
            return { success: false, error: 'Invalid row index: ' + rowIndex };
        }
        if (!formData || typeof formData !== 'object') {
            return { success: false, error: 'Invalid form data' };
        }

        var ss = getMasterWorkbook();
        var sheet = ss.getSheetByName(CONFIG.MASTER_SHEET);
        if (!sheet) return { success: false, error: 'Master sheet not found' };

        // Read current headers and row
        var lastCol = sheet.getLastColumn();
        var rawHeaders = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
        var headers = trimHeaders(rawHeaders);
        var currentRow = sheet.getRange(rowIndex, 1, 1, lastCol).getValues()[0];
        var recordId = currentRow[0];

        // Ensure the 3 audit columns exist; append to sheet header row if missing
        var auditCols = ['Updated By', 'Updated Date', 'Update Applied'];
        auditCols.forEach(function (colName) {
            if (headers.indexOf(colName) === -1) {
                lastCol++;
                sheet.getRange(1, lastCol).setValue(colName);
                headers.push(colName);
                currentRow.push('');
            }
        });

        // Working copy of row extended to full header length
        var updatedRow = currentRow.slice();
        while (updatedRow.length < headers.length) updatedRow.push('');

        // Get intake field mapping: AI form field ID → AM column header
        var fieldMapping = getIntakeFieldMapping();
        var changes = [];   // For logAudit()
        var updateApplied = [];   // "Header: 'old' → 'new'" entries for Update Applied column

        for (var formField in formData) {
            var amHeader = fieldMapping[formField] || null;
            if (!amHeader) continue;
            var colIndex = headers.indexOf(amHeader);
            if (colIndex === -1) continue;

            var oldValue = updatedRow[colIndex];
            var newValue = formData[formField];

            if (Array.isArray(newValue)) newValue = newValue.join(', ');
            newValue = htmlDateToSheet(newValue);

            // Normalize old value for comparison
            var oldValueStr = '';
            if (oldValue !== null && oldValue !== undefined) {
                oldValueStr = (oldValue instanceof Date)
                    ? Utilities.formatDate(oldValue, CONFIG.TIMEZONE, 'M/d/yyyy')
                    : oldValue.toString();
            }

            if (oldValueStr !== newValue && (newValue !== '' || oldValueStr !== '')) {
                updatedRow[colIndex] = newValue;
                changes.push(amHeader + ': "' + oldValueStr + '" \u2192 "' + newValue + '"');
                updateApplied.push(amHeader + ": '" + oldValueStr + "' \u2192 '" + newValue + "'");
            }
        }

        if (changes.length === 0) {
            return { success: true, message: 'No changes detected', changesCount: 0 };
        }

        // Write audit columns
        var now = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yyyy HH:mm:ss');
        var ubIdx = headers.indexOf('Updated By');
        var udIdx = headers.indexOf('Updated Date');
        var uaIdx = headers.indexOf('Update Applied');
        if (ubIdx !== -1) updatedRow[ubIdx] = updatedBy || '';
        if (udIdx !== -1) updatedRow[udIdx] = now;
        if (uaIdx !== -1) updatedRow[uaIdx] = updateApplied.join('; ');

        // Single bulk write
        sheet.getRange(rowIndex, 1, 1, updatedRow.length).setValues([updatedRow]);
        logAudit('UPDATE_INTAKE', recordId,
            'Admin edit by ' + (updatedBy || 'unknown') + ': ' + changes.join('; '));

        return {
            success: true,
            message: 'Record ' + recordId + ' updated (' + changes.length + ' field(s) changed)',
            changesCount: changes.length
        };

    } catch (e) {
        Logger.log('updateIntakeRecord error: ' + e.message);
        return { success: false, error: 'Update failed: ' + e.message };
    }
}

/**
 * Returns all data needed by the Admin Portal Edit Applicant tab.
 * Extends getIntakePortalData() with the full intake field mapping so the
 * client can build a reverse map (AM header → AI form field ID) for form population.
 * v3.0 - Added for Admin Portal Edit Applicant feature
 * @returns {Object} { lookups, dropdownFields, fieldMapping }
 */
/**
 * Checks how many Demographics-group fields are blank in a given record.
 * Used by the Staff Portal to decide whether to show the "Complete Intake Data" prompt.
 *
 * Reads the record from AM or the specified archive workbook, then loads
 * Demographics fields from LU_FieldMap and counts blank vs. populated values.
 *
 * v3.3 - New function.
 *
 * @param {string} recordId     - Applicant record ID
 * @param {string} archiveSource - Archive workbook name ('G2N_Archive', 'G2N_Archive_YYYY') or ''
 * @returns {Object} {
 *   success: boolean,
 *   isMostlyBlank: boolean,   - true when blankPct >= 80
 *   blankCount: number,
 *   totalCount: number,
 *   blankPct: number,         - 0–100 integer
 *   demographicsFields: string[] - display labels of blank fields (for tooltip/message)
 * }
 */
function getDemographicsStatus(recordId, archiveSource) {
    try {
        // ── Get Demographics fields from LU_FieldMap ─────────────────────────
        var map = loadFieldMap();
        var demoEntries = map.byGroup['Demographics'] || [];
        if (demoEntries.length === 0) {
            return { success: true, isMostlyBlank: false, blankCount: 0, totalCount: 0, blankPct: 0 };
        }

        // ── Read the record row ───────────────────────────────────────────────
        var sheet, headers, row;

        if (archiveSource) {
            var archiveWB = null;
            if (archiveSource === 'G2N_Archive') {
                archiveWB = SpreadsheetApp.openById(CONFIG.ARCHIVE_WORKBOOK_ID);
            } else {
                var folder = DriveApp.getFolderById(CONFIG.ARCHIVES_BACKUPS_FOLDER_ID);
                var files = folder.getFiles();
                while (files.hasNext()) {
                    var f = files.next();
                    if (f.getMimeType() === 'application/vnd.google-apps.spreadsheet' &&
                        f.getName() === archiveSource) {
                        archiveWB = SpreadsheetApp.openById(f.getId());
                        break;
                    }
                }
            }
            if (!archiveWB) return { success: false, error: 'Archive workbook not found' };
            sheet = archiveWB.getSheetByName('Archive') || archiveWB.getSheets()[0];
        } else {
            sheet = getMasterSheet();
        }

        if (!sheet || sheet.getLastRow() < 2) {
            return { success: false, error: 'Sheet not found or empty' };
        }

        var data = sheet.getDataRange().getValues();
        headers = trimHeaders(data[0]);
        var idCol = headers.indexOf('ID');
        if (idCol === -1) return { success: false, error: 'ID column not found' };

        row = null;
        for (var i = 1; i < data.length; i++) {
            if ((data[i][idCol] || '').toString().trim() === recordId.toString().trim()) {
                row = data[i]; break;
            }
        }
        if (!row) return { success: false, error: 'Record not found' };

        // ── Count blank Demographics fields ────────────────────────────────
        var totalCount = 0;
        var blankCount = 0;
        var blankLabels = [];

        demoEntries.forEach(function (entry) {
            var colIdx = headers.indexOf(entry.rawHeader);
            if (colIdx === -1) return; // column not in this sheet — skip
            totalCount++;
            var val = (row[colIdx] !== null && row[colIdx] !== undefined)
                ? row[colIdx].toString().trim() : '';
            if (!val) {
                blankCount++;
                blankLabels.push(entry.displayLabel || entry.rawHeader);
            }
        });

        var blankPct = totalCount > 0 ? Math.round((blankCount / totalCount) * 100) : 0;

        return {
            success: true,
            isMostlyBlank: blankPct >= 80,
            blankCount: blankCount,
            totalCount: totalCount,
            blankPct: blankPct,
            demographicsFields: blankLabels
        };

    } catch (e) {
        Logger.log('getDemographicsStatus error: ' + e.message);
        return { success: false, error: e.message };
    }
}

function getApplicantEditData() {
    var data = getIntakePortalData();
    data.fieldMapping = getIntakeFieldMapping();
    return data;
}
