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
    lookups:        lookups,
    fieldMap:       portalSections.fieldMap,
    dropdownFields: portalSections.dropdownFields,
    sections:       portalSections.sections,
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

    var ss    = getMasterWorkbook();
    var sheet = ss.getSheetByName(CONFIG.MASTER_SHEET);
    if (!sheet) return { success: false, error: 'Master sheet not found' };

    // Read current headers and row
    var lastCol    = sheet.getLastColumn();
    var rawHeaders = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
    var headers    = trimHeaders(rawHeaders);
    var currentRow = sheet.getRange(rowIndex, 1, 1, lastCol).getValues()[0];
    var recordId   = currentRow[0];

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
    var fieldMapping  = getIntakeFieldMapping();
    var changes       = [];   // For logAudit()
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
function getApplicantEditData() {
  var data = getIntakePortalData();
  data.fieldMapping = getIntakeFieldMapping();
  return data;
}
