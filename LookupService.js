/**
 * LookupService.gs
 * Manages all reference/lookup data from G2N_Lookups workbook
 * Provides dropdown values for portals, event info, distributed products,
 * Scheduled Distribution Code management, Event Information management,
 * and Staff/Volunteer record management.
 * v3.3 - Phase 4: Added getDistribCodeTimes, getActiveSchedDisbCodes
 * v3.4 - Added Box 1/Box 2 support to getSchedDisbCodeDetails,
 *         getActiveSchedDisbCodes, saveSchedDisbCodes with BoxCodes validation
 * v3.5 - Added fallback header lookups for Box1/Box2 (no space) naming,
 *         added Times field to getSchedDisbCodeDetails and getActiveSchedDisbCodes
 * v3.6 - getDistribCodeTimes now accepts optional distribCode parameter
 *         to filter Times by specific distribution code from LU_DistribCodes
 * v3.7 - Added Box 3 support across getSchedDisbCodeDetails,
 *         getActiveSchedDisbCodes, saveSchedDisbCodes
 * v4.0 - Phase 5: Staff code forced uppercase on save (5.1).
 *         getStaffRecords accepts includeInactive parameter (5.3).
 * v4.1 - Added Email column support to getStaffRecords and saveStaffRecords.
 *         Email used for archive notification emails to administrators.
 * v4.2 - Fixed redundant requestTypes lookup in getAllLookups(): now aliases
 *         serviceHow instead of making a duplicate getLookupValues() call (#9).
 *         Added getEventInfoForDate(dateStr) for server-side event detection
 *         by signature date (supports IntakeService auto-event-detect).
 * v4.3 - Adopted trimHeaders() across all header reads (8 inline trims).
 *         Adopted isRowActive() for Active column checks in getLookupValues,
 *         getLookupData, getStaffRecords, getActiveSchedDisbCodes.
 *         Refactored getEventInfo() to delegate to getEventInfoForDate()
 *         (eliminates ~40 lines of duplicate date-comparison logic).
 *         Fixed getActiveSchedDisbCodes: moved box/times column lookups
 *         outside the loop (were recomputed on every iteration).
 * v4.4 - Lookup caching via CacheService (6-hour TTL, ~20-40x faster portal loads).
 *         getAllLookups() and getIntakeLookups() now check ScriptCache before
 *         reading sheets. invalidateLookupCache() clears cache and is called
 *         automatically by all admin save/deactivate operations.
 *         Removed unused getStaffLookups() (zero callers found).
 * v4.5 - Adopted CONFIG.TIMEZONE across all Utilities.formatDate() calls (#8).
 * v4.6 - getDistributedProducts() and getBoxCodeSummary() now use getDataWorkbook()
 *         and CONFIG.DATA_SHEETS.DISTRIBUTED_PRODUCTS (was incorrectly using
 *         getLookupsWorkbook() and CONFIG.LOOKUPS.DISTRIBUTED_PRODUCTS).
 * v4.7 - Added generic getLookupRecords(sheetKey) and saveLookupRecords(sheetKey,
 *         updates, newRecords) for Admin Portal generic LU editor.
 *         Added getDistributedProductRecords() and saveDistributedProductRecords()
 *         for Distributed_Products sheet in G2N_Data workbook.
 * v4.8 - Extracted getAllLookupsFromSheets_() and getIntakeLookupsFromSheets_()
 *         as private helpers. getAllLookups() and getIntakeLookups() now fall back
 *         to Sheets when MySQL bridge returns empty, ensuring portal dropdowns
 *         always populate. Logs WARNING on fallback.
 * v4.9 - invalidateLookupCache() now also calls DbService.invalidateFieldMapCache()
 *         when USE_MYSQL=true, busting the GAS CacheService field map entry and
 *         notifying the bridge via DELETE /lookups/field-map/cache. Eliminates
 *         the 1-hour TTL lag when admin changes lu_field_map in MySQL.
 * v5.0 - getLookupRecords() / saveLookupRecords() add FIELD_MAP branch for the
 *         LU_FieldMap sheet editor in AP Manage Dropdowns.
 *         saveLookupRecords() calls DbService.invalidateLookupTableCache() when
 *         USE_MYSQL=true so MySQL lookup table cache is busted immediately on save.
 * v5.1 - Added getLastScheduledId(): reads the last row of LU_LastScheduled,
 *         returns the ApplicantId value + 1 for pre-filling Beginning ID.
 *         Added appendLastScheduledId(lastId, reportDate): appends a new row to
 *         LU_LastScheduled with ApplicantId and ReportDate after a Scheduling
 *         Report is successfully generated.
 * v5.2 - getEventInfoForDate(): added optional timeStr parameter to check
 *         Event Begins Time / Event Ends Time columns in LU_EventInfo when
 *         date matches. Added parseTimeToMinutes_() and
 *         getFundingSourceDescription_() private helpers.
 *         saveEventInfoRecords(): added eventBeginsTime / eventEndsTime
 *         to colMap and newRow builder.
 *         Added getLastSchedId(): reads LU_SchedID last row EndId.
 *         Added appendSchedId(): appends StartId/EndId/ReportDate to LU_SchedID.
 */

// ============ LOOKUP CACHE ============
var LOOKUP_CACHE_KEY_ALL = 'G2N_LOOKUPS_ALL';
var LOOKUP_CACHE_KEY_INTAKE = 'G2N_LOOKUPS_INTAKE';
var LOOKUP_CACHE_TTL = 21600; // 6 hours in seconds

/**
 * Returns active values from a specified lookup sheet
 * Filters by Active column if present (TRUE/Y values only)
 * @param {string} sheetName - Name of the lookup sheet in G2N_Lookups
 * @returns {string[]} Array of active lookup values
 */
function getLookupValues(lookupKey, valueColumn) {
  const sheetName = CONFIG.LOOKUPS[lookupKey];
  if (!sheetName) {
    Logger.log('Unknown lookup key: ' + lookupKey);
    return [];
  }
  
  const lookups = getLookupsWorkbook();
  const sheet = lookups.getSheetByName(sheetName);
  
  if (!sheet) {
    Logger.log('Lookup sheet not found: ' + sheetName);
    return [];
  }
  
  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return [];
  
  const headers = data[0];
  const activeCol = headers.indexOf('Active');
  
  // Determine which column to return values from
  let valueColIndex = 0; // Default to first column
  if (valueColumn) {
    valueColIndex = headers.indexOf(valueColumn);
    if (valueColIndex === -1) valueColIndex = 0;
  }
  
  const values = [];
  for (let i = 1; i < data.length; i++) {
    // Check if active (if Active column exists)
    if (isRowActive(data[i], activeCol)) {
      const val = data[i][valueColIndex];
      if (val !== null && val !== '' && val !== undefined) {
        values.push(val);
      }
    }
  }
  
  return values;
}

/**
 * Checks if a specific date (and optionally time) falls within an active event
 * period in LU_EventInfo.
 * v4.2 - New for server-side event detection by date.
 * v5.2 - Added optional timeStr parameter for time range check.
 *         If Event Begins Time / Event Ends Time columns exist and timeStr is
 *         provided, verifies the time falls within the event window.
 *         Also looks up fundingSourceDescription from LU_FundingSources.
 * @param {string} dateStr  - Date to check (YYYY-MM-DD or M/D/YYYY format)
 * @param {string} [timeStr] - Optional time string (HH:MM or H:MM, 24-hr or 12-hr)
 * @returns {Object} { isActive, boxCode, fundingSource, fundingSourceDescription }
 */
function getEventInfoForDate(dateStr, timeStr) {
    if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.getEventInfoForDate(dateStr, timeStr);
    try {
        if (!dateStr) return { isActive: false };

        // Parse the date string
        var checkDate;
        if (typeof dateStr === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
            var parts = dateStr.split('-');
            checkDate = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
        } else {
            checkDate = new Date(dateStr);
        }
        if (isNaN(checkDate.getTime())) return { isActive: false };
        checkDate.setHours(0, 0, 0, 0);

        // Parse the optional time string to minutes since midnight
        var checkMinutes = -1;
        if (timeStr && typeof timeStr === 'string') {
            var tMatch = timeStr.match(/(\d{1,2}):(\d{2})\s*(AM|PM)?/i);
            if (tMatch) {
                var tHr = parseInt(tMatch[1]);
                var tMin = parseInt(tMatch[2]);
                var tAmPm = (tMatch[3] || '').toUpperCase();
                if (tAmPm === 'PM' && tHr < 12) tHr += 12;
                if (tAmPm === 'AM' && tHr === 12) tHr = 0;
                checkMinutes = tHr * 60 + tMin;
            }
        }

        var lookups = getLookupsWorkbook();
        var sheet = lookups.getSheetByName('LU_EventInfo');
        if (!sheet) return { isActive: false };

        var data = sheet.getDataRange().getValues();
        if (data.length < 2) return { isActive: false };

        var headers = trimHeaders(data[0]);
        var beginsCol = headers.indexOf('Event Begins Date');
        var endsCol = headers.indexOf('Event Ends Date');
        var beginsTimeCol = headers.indexOf('Event Begins Time');
        var endsTimeCol = headers.indexOf('Event Ends Time');
        var boxCodeCol = headers.indexOf('BoxCode');
        var fundingCol = headers.indexOf('Funding Source');

        if (beginsCol === -1 || endsCol === -1) return { isActive: false };

        for (var i = 1; i < data.length; i++) {
            var begins = new Date(data[i][beginsCol]);
            var ends = new Date(data[i][endsCol]);
            begins.setHours(0, 0, 0, 0);
            ends.setHours(23, 59, 59, 999);

            if (checkDate < begins || checkDate > ends) continue;

            // Date matches — now check time range if time columns exist and timeStr provided
            if (checkMinutes >= 0 && beginsTimeCol !== -1 && endsTimeCol !== -1) {
                var beginTimeStr = (data[i][beginsTimeCol] || '').toString().trim();
                var endTimeStr = (data[i][endsTimeCol] || '').toString().trim();

                if (beginTimeStr && endTimeStr) {
                    var beginMin = parseTimeToMinutes_(beginTimeStr);
                    var endMin = parseTimeToMinutes_(endTimeStr);

                    if (beginMin >= 0 && endMin >= 0) {
                        if (checkMinutes < beginMin || checkMinutes > endMin) continue; // time outside range
                    }
                }
            }

            // Date (and time if provided) matches — build result
            var fundingSource = fundingCol !== -1 ? (data[i][fundingCol] || '').toString() : '';
            var fundingDescription = '';

            if (fundingSource) {
                fundingDescription = getFundingSourceDescription_(fundingSource);
            }

            return {
                isActive: true,
                boxCode: boxCodeCol !== -1 ? (data[i][boxCodeCol] || '').toString() : '',
                fundingSource: fundingSource,
                fundingSourceDescription: fundingDescription
            };
        }

        return { isActive: false };
    } catch (e) {
        Logger.log('getEventInfoForDate error: ' + e.message);
        return { isActive: false };
    }
}

/**
 * Parses a time string ("8:00 AM", "14:30", "2:30 PM") to minutes since midnight.
 * @private
 * @param {string} timeStr
 * @returns {number} Minutes since midnight, or -1 if unparseable
 */
function parseTimeToMinutes_(timeStr) {
    if (!timeStr) return -1;
    var m = timeStr.toString().match(/(\d{1,2}):(\d{2})\s*(AM|PM)?/i);
    if (!m) return -1;
    var hr = parseInt(m[1]);
    var min = parseInt(m[2]);
    var ap = (m[3] || '').toUpperCase();
    if (ap === 'PM' && hr < 12) hr += 12;
    if (ap === 'AM' && hr === 12) hr = 0;
    return hr * 60 + min;
}

/**
 * Returns the Description for a Funding Source Code from LU_FundingSources.
 * @private
 * @param {string} code - Funding source code
 * @returns {string} Description, or '' if not found
 */
function getFundingSourceDescription_(code) {
    try {
        var lookups = getLookupsWorkbook();
        var sheet = lookups.getSheetByName(CONFIG.LOOKUPS.FUNDING_SOURCES);
        if (!sheet) return '';
        var data = sheet.getDataRange().getValues();
        if (data.length < 2) return '';
        var headers = trimHeaders(data[0]);
        var codeCol = headers.indexOf('Code');
        var descCol = headers.indexOf('Description');
        if (codeCol === -1 || descCol === -1) return '';
        var searchCode = code.toString().trim().toUpperCase();
        for (var i = 1; i < data.length; i++) {
            var rowCode = (data[i][codeCol] || '').toString().trim().toUpperCase();
            if (rowCode === searchCode) return (data[i][descCol] || '').toString().trim();
        }
        return '';
    } catch (e) {
        Logger.log('getFundingSourceDescription_ error: ' + e.message);
        return '';
    }
}

/**
 * Reads the last row of LU_SchedID and returns the EndId stored there.
 * Used by generateSchedulingReport() to determine Start of New Records (lastEndId + 1).
 * Returns 0 if the sheet is empty or not found (first run).
 * v5.2 - New function; replaces LU_LastScheduled approach.
 * @returns {{ lastEndId: number }}
 */
function getLastSchedId() {
    try {
        var key = CONFIG.LOOKUPS.SCHED_ID;
        if (!key) {
            Logger.log('getLastSchedId: SCHED_ID not in CONFIG.LOOKUPS');
            return { lastEndId: 0 };
        }
        var lookups = getLookupsWorkbook();
        var sheet = lookups.getSheetByName(key);
        if (!sheet || sheet.getLastRow() < 2) return { lastEndId: 0 };

        var data = sheet.getDataRange().getValues();
        var headers = trimHeaders(data[0]);
        var endIdCol = headers.indexOf('EndId');
        if (endIdCol === -1) return { lastEndId: 0 };

        // Return the EndId from the last data row
        var lastRow = data[data.length - 1];
        var lastEndId = parseInt(lastRow[endIdCol]) || 0;
        return { lastEndId: lastEndId };
    } catch (e) {
        Logger.log('getLastSchedId error: ' + e.message);
        return { lastEndId: 0 };
    }
}

/**
 * Appends a new row to LU_SchedID recording the ID range and date of a
 * Scheduling Report run.
 * v5.2 - New function.
 * @param {number} startId    - First ID of the new records window (lastEndId + 1)
 * @param {number} endId      - Last row ID in AM at time of report generation
 * @param {string} reportDate - Formatted date string of the report run
 * @returns {boolean} true on success
 */
function appendSchedId(startId, endId, reportDate) {
    try {
        var key = CONFIG.LOOKUPS.SCHED_ID;
        if (!key) { Logger.log('appendSchedId: SCHED_ID not in CONFIG.LOOKUPS'); return false; }
        var lookups = getLookupsWorkbook();
        var sheet = lookups.getSheetByName(key);
        if (!sheet) { Logger.log('appendSchedId: LU_SchedID sheet not found'); return false; }

        var data = sheet.getDataRange().getValues();
        var headers = trimHeaders(data[0]);
        var startIdCol = headers.indexOf('StartId');
        var endIdCol = headers.indexOf('EndId');
        var reportDateCol = headers.indexOf('ReportDate');

        if (endIdCol === -1) { Logger.log('appendSchedId: EndId column not found in LU_SchedID'); return false; }

        var newRow = new Array(headers.length).fill('');
        if (startIdCol !== -1) newRow[startIdCol] = startId;
        if (endIdCol !== -1) newRow[endIdCol] = endId;
        if (reportDateCol !== -1) newRow[reportDateCol] = reportDate;

        sheet.getRange(sheet.getLastRow() + 1, 1, 1, newRow.length).setValues([newRow]);
        return true;
    } catch (e) {
        Logger.log('appendSchedId error: ' + e.message);
        return false;
    }
}

/**
 * Get full lookup data as objects (for detailed lookups)
 * @param {string} lookupKey - Key from CONFIG.LOOKUPS
 * @returns {Array} Array of objects with all columns
 */
function getLookupData(lookupKey) {
  const sheetName = CONFIG.LOOKUPS[lookupKey];
  if (!sheetName) return [];
  
  const lookups = getLookupsWorkbook();
  const sheet = lookups.getSheetByName(sheetName);
  
  if (!sheet) return [];
  
  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return [];
  
  const headers = data[0];
  const activeCol = headers.indexOf('Active');
  const results = [];
  
  for (let i = 1; i < data.length; i++) {
    // Check if active
    if (isRowActive(data[i], activeCol)) {
      const obj = {};
      for (let j = 0; j < headers.length; j++) {
        obj[headers[j]] = data[i][j];
      }
      results.push(obj);
    }
  }
  
  return results;
}

/**
 * Returns all dropdown lookup values for the Staff/Volunteer Portal.
 * Checks CacheService first; builds from sheets on miss and caches for 6 hours.
 * v4.4 - Added CacheService caching
 * @returns {Object} Keyed object with arrays of values per lookup type
 */
function getAllLookups() {
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.getAllLookups();
  try {
    var cache = CacheService.getScriptCache();
    var cached = cache.get(LOOKUP_CACHE_KEY_ALL);
    if (cached) {
      return JSON.parse(cached);
    }
  } catch (e) {
    Logger.log('getAllLookups cache read error (non-fatal): ' + e.message);
  }

  var serviceHowValues = getLookupValues('SERVICE_HOW', 'Type');

  var result = {
    staffList: getLookupValues('STAFF', 'Code'),
    schedDisbCodes: getLookupValues('SCHED_DISB_CODES', 'SchedDisbCode'),
    fundingSourcesData: getLookupData('FUNDING_SOURCES'),
    distribCodes: getLookupValues('DISTRIB_CODES', 'Code'),
    boxCodes: getLookupValues('BOX_CODES', 'Code'),
    serviceStatus: getLookupValues('SERVICE_STATUS', 'Status'),
    serviceHow: serviceHowValues,
    requestTypes: serviceHowValues,
    fundingSources: getLookupValues('FUNDING_SOURCES', 'Code'),
    states: getLookupValues('STATES', 'Name'),
    learnedHow: getLookupValues('LEARNED_HOW', 'LearnedHow'),
    personOrgReferred: getLookupValues('PERSON_ORG_REFERRED', 'PersonOrgReferred'),
    titleReferred: getLookupValues('TITLE_REFERRED', 'TitlePositionWhoReferred'),
    military: getLookupValues('MILITARY', 'Military'),
    race: getLookupValues('RACE', 'Race'),
    ethnicity: getLookupValues('ETHNICITY', 'Ethnicity'),
    incomeLevels: getLookupValues('INCOME_LEVELS', 'LevelName'),
    incomeSources: getLookupValues('INCOME_SOURCES', 'Source'),
    phoneType: getLookupValues('PHONE_TYPE', 'PhoneType'),
    addressType: getLookupValues('ADDRESS_TYPE', 'HousingType'),
    products: getLookupValues('PRODUCTS', 'ProductName')
  };

  try {
    cache.put(LOOKUP_CACHE_KEY_ALL, JSON.stringify(result), LOOKUP_CACHE_TTL);
  } catch (e) {
    Logger.log('getAllLookups cache write error (non-fatal): ' + e.message);
  }

  return result;
}

/**
 * Returns lightweight lookup values for the Applicant Intake form.
 * Checks CacheService first; builds from sheets on miss and caches for 6 hours.
 * v4.4 - Added CacheService caching
 * @returns {Object} Keyed object with intake-relevant lookup arrays
 */
function getIntakeLookups() {
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.getIntakeLookups();
  try {
    var cache = CacheService.getScriptCache();
    var cached = cache.get(LOOKUP_CACHE_KEY_INTAKE);
    if (cached) {
      return JSON.parse(cached);
    }
  } catch (e) {
    Logger.log('getIntakeLookups cache read error (non-fatal): ' + e.message);
  }

  var result = {
    states: getLookupValues('STATES', 'Name'),
    learnedHow: getLookupValues('LEARNED_HOW', 'LearnedHow'),
    personOrgReferred: getLookupValues('PERSON_ORG_REFERRED', 'PersonOrgReferred'),
    titleReferred: getLookupValues('TITLE_REFERRED', 'TitlePositionWhoReferred'),
    military: getLookupValues('MILITARY', 'Military'),
    race: getLookupValues('RACE', 'Race'),
    ethnicity: getLookupValues('ETHNICITY', 'Ethnicity'),
    incomeLevels: getLookupValues('INCOME_LEVELS', 'LevelName'),
    incomeSources: getLookupValues('INCOME_SOURCES', 'Source'),
    phoneType: getLookupValues('PHONE_TYPE', 'PhoneType'),
    addressType: getLookupValues('ADDRESS_TYPE', 'HousingType')
  };

  try {
    cache.put(LOOKUP_CACHE_KEY_INTAKE, JSON.stringify(result), LOOKUP_CACHE_TTL);
  } catch (e) {
    Logger.log('getIntakeLookups cache write error (non-fatal): ' + e.message);
  }

  return result;
}

/**
 * Clears all lookup caches from CacheService.
 * Called automatically after any admin save/deactivate operation,
 * and from the onEdit trigger when G2N_Lookups is edited directly.
 * v4.4 - New function
 * v4.9 - When USE_MYSQL=true, also calls DbService.invalidateFieldMapCache()
 *         which clears the GAS-side field map CacheService key and notifies
 *         the bridge via DELETE /lookups/field-map/cache. This eliminates the
 *         1-hour lag when an admin changes lu_field_map in MySQL — changes
 *         become live immediately after the next admin save operation.
 */
function invalidateLookupCache() {
  try {
    var cache = CacheService.getScriptCache();
    cache.removeAll([LOOKUP_CACHE_KEY_ALL, LOOKUP_CACHE_KEY_INTAKE]);
    Logger.log('Lookup cache invalidated');
  } catch (e) {
    Logger.log('invalidateLookupCache error (non-fatal): ' + e.message);
  }
  // When MySQL is active, also bust the field map cache so admin LU_FieldMap
  // changes take effect immediately instead of waiting for the 1-hour TTL
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) {
    try {
      DbService.invalidateFieldMapCache();
    } catch (e) {
      Logger.log('invalidateLookupCache: field map cache bust (non-fatal): ' + e.message);
    }
  }
}

/**
 * Returns product list for a given box code from LU_BoxCode lookup
 * Used for product distribution tracking
 * @param {string} boxCode - The box code to look up
 * @returns {Object[]} Array of product objects with names and quantities
 */
function getDistributedProducts(boxCode) {
  const dataWb = getDataWorkbook();
  const sheet = dataWb.getSheetByName(CONFIG.DATA_SHEETS.DISTRIBUTED_PRODUCTS);
  
  if (!sheet) {
    Logger.log('Distributed_Products sheet not found');
    return [];
  }
  
  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return [];
  
  const headers = data[0];
  const boxCodeCol = headers.indexOf('BoxCode');
  const productIdCol = headers.indexOf('ProductID');
  const productNameCol = headers.indexOf('ProductName');
  const quantityCol = headers.indexOf('Quantity');
  
  const products = [];
  for (let i = 1; i < data.length; i++) {
    if (data[i][boxCodeCol] === boxCode) {
      products.push({
        productId: data[i][productIdCol],
        productName: data[i][productNameCol],
        quantity: data[i][quantityCol]
      });
    }
  }
  
  return products;
}

/**
 * Get all box codes with their product counts
 */
function getBoxCodeSummary() {
  const dataWb = getDataWorkbook();
  const sheet = dataWb.getSheetByName(CONFIG.DATA_SHEETS.DISTRIBUTED_PRODUCTS);
  
  if (!sheet) return [];
  
  const data = sheet.getDataRange().getValues();
  const boxCounts = {};
  
  for (let i = 1; i < data.length; i++) {
    const boxCode = data[i][0];
    if (boxCode) {
      boxCounts[boxCode] = (boxCounts[boxCode] || 0) + 1;
    }
  }
  
  return Object.entries(boxCounts).map(([code, count]) => ({
    code: code,
    productCount: count
  }));
}

/**
 * Lookup state name from abbreviation
 */
function getStateName(abbrev) {
  const data = getLookupData('STATES');
  const state = data.find(s => s.Abbrev === abbrev);
  return state ? state.Name : abbrev;
}

/**
 * Lookup income level description from key
 */
function getIncomeLevelName(key) {
  const data = getLookupData('INCOME_LEVELS');
  const level = data.find(l => l.Key == key);
  return level ? level.LevelName : '';
}

/**
 * Add new value to lookup table (Admin function)
 */
function addLookupValue(lookupKey, values) {
  const sheetName = CONFIG.LOOKUPS[lookupKey];
  if (!sheetName) {
    return { success: false, error: 'Unknown lookup key' };
  }
  
  const lookups = getLookupsWorkbook();
  const sheet = lookups.getSheetByName(sheetName);
  
  if (!sheet) {
    return { success: false, error: 'Sheet not found: ' + sheetName };
  }
  
  // Append new row
  sheet.appendRow(values);
  
  logAudit('LOOKUP_ADD', null, 'Added to ' + sheetName + ': ' + values.join(', '));
  invalidateLookupCache();
  
  return { success: true, message: 'Value added to ' + sheetName };
}

/**
 * Deactivate a lookup value (set Active = FALSE)
 */
function deactivateLookupValue(lookupKey, keyValue) {
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.deactivateLookupValue(lookupKey, keyValue);
  const sheetName = CONFIG.LOOKUPS[lookupKey];
  if (!sheetName) {
    return { success: false, error: 'Unknown lookup key' };
  }
  
  const lookups = getLookupsWorkbook();
  const sheet = lookups.getSheetByName(sheetName);
  
  if (!sheet) {
    return { success: false, error: 'Sheet not found' };
  }
  
  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  const activeCol = headers.indexOf('Active');
  
  if (activeCol === -1) {
    return { success: false, error: 'No Active column in this lookup' };
  }
  
  // Find and update the row
  for (let i = 1; i < data.length; i++) {
    if (data[i][0] === keyValue) {
      sheet.getRange(i + 1, activeCol + 1).setValue(false);
      logAudit('LOOKUP_DEACTIVATE', null, 'Deactivated in ' + sheetName + ': ' + keyValue);
      invalidateLookupCache();
      return { success: true, message: 'Value deactivated' };
    }
  }
  
  return { success: false, error: 'Value not found' };
}

/**
 * Get details for a selected SchedDisbCode
 * Returns StartDate, Interval, FundingCode, and FundingDescription
 * @param {string} code - The SchedDisbCode value
 * @returns {Object} { success, schedDisbCode, startDate, interval, fundingCode, fundingDescription }
 */
function getSchedDisbCodeDetails(code) {
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.getSchedDisbCodeDetails(code);
  if (!code) {
    return { success: false, error: 'Code is required' };
  }

  try {
    var data = getLookupData('SCHED_DISB_CODES');
    var match = null;

    for (var i = 0; i < data.length; i++) {
      if (data[i].SchedDisbCode === code) {
        match = data[i];
        break;
      }
    }

    if (!match) {
      return { success: false, error: 'Scheduled Distribution Code not found: ' + code };
    }

    // Get the funding source code from the SchedDisbCode record
    var fundingCode = (match.FundingSource || '').toString().trim();
    var fundingDescription = '';

    // Look up the description from LU_FundingSources
    if (fundingCode) {
      fundingDescription = getFundingSourceDescription(fundingCode);
    }

    // Format the start date for return
    var startDate = match.StartDate || '';
    if (startDate instanceof Date && !isNaN(startDate.getTime())) {
      startDate = Utilities.formatDate(startDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    }

    return {
      success: true,
      schedDisbCode: match.SchedDisbCode,
      startDate: startDate,
      interval: match.Interval || '',
      fundingCode: fundingCode,
      fundingDescription: fundingDescription,
      box1: (match['Box 1'] || match['Box1'] || '').toString().trim(),
      box2: (match['Box 2'] || match['Box2'] || '').toString().trim(),
      box3: (match['Box 3'] || match['Box3'] || '').toString().trim(),
      times: (match['Times'] || '').toString().trim()
    };

  } catch (error) {
    Logger.log('getSchedDisbCodeDetails error: ' + error.message);
    return { success: false, error: 'Lookup failed: ' + error.message };
  }
}

/**
 * Get the Description for a Funding Source Code from LU_FundingSources
 * @param {string} code - The Funding Source Code
 * @returns {string} The Description, or empty string if not found
 */
function getFundingSourceDescription(code) {
  if (!code) return '';

  try {
    var data = getLookupData('FUNDING_SOURCES');
    for (var i = 0; i < data.length; i++) {
      if (data[i].Code === code) {
        return data[i].Description || '';
      }
    }
    return '';
  } catch (e) {
    Logger.log('getFundingSourceDescription error: ' + e.message);
    return '';
  }
}

// ============ NEW: SchedDisbCodes Management Functions ============

/**
 * Bulk saves Scheduled Distribution Code records to LU_SchedDisbCodes
 * Checks for duplicates, auto-uppercases codes, sets Active=TRUE
 * @param {Object[]} records - Array of { code, startDate, interval, fundingSource }
 * @returns {Object} { success: boolean, added: number, duplicates: string[] }
 */
function saveSchedDisbCodes(records) {
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.saveSchedDisbCodes(records);
  try {
    if (!records || records.length === 0) {
      return { success: false, error: 'No records to save' };
    }
    
    const lookups = getLookupsWorkbook();
    const sheetName = CONFIG.LOOKUPS.SCHED_DISB_CODES;
    const sheet = lookups.getSheetByName(sheetName);
    
    if (!sheet) {
      return { success: false, error: 'LU_SchedDisbCodes sheet not found' };
    }
    
    const existingData = sheet.getDataRange().getValues();
    const headers = trimHeaders(existingData[0]);
    
    // Determine column indices
    const codeCol = headers.indexOf('SchedDisbCode');
    const startDateCol = headers.indexOf('StartDate');
    const intervalCol = headers.indexOf('Interval');
    var box1Col = headers.indexOf('Box 1');
    if (box1Col === -1) box1Col = headers.indexOf('Box1');
    var box2Col = headers.indexOf('Box 2');
    if (box2Col === -1) box2Col = headers.indexOf('Box2');
    var box3Col = headers.indexOf('Box 3');
    if (box3Col === -1) box3Col = headers.indexOf('Box3');
    var fundingSourceCol = headers.indexOf('FundingSource');
    if (fundingSourceCol === -1) fundingSourceCol = headers.indexOf('Funding Source');
    var fundingDescCol = headers.indexOf('FundingDescription');
    if (fundingDescCol === -1) fundingDescCol = headers.indexOf('Funding Description');
    const activeCol = headers.indexOf('Active');
    
    // Check for FundingDescription column, add if missing
    let fundingDescColIndex = fundingDescCol;
    if (fundingDescColIndex === -1) {
      // Add column header
      const lastCol = headers.length;
      sheet.getRange(1, lastCol + 1).setValue('FundingDescription');
      sheet.getRange(1, lastCol + 1).setFontWeight('bold').setBackground('#4a86c8').setFontColor('white').setHorizontalAlignment('center');
      sheet.setColumnWidth(lastCol + 1, 180);
      fundingDescColIndex = lastCol;
    }
    
    // Check for duplicates
    const existingCodes = new Set();
    for (var i = 1; i < existingData.length; i++) {
      var code = (existingData[i][codeCol] || '').toString().trim().toUpperCase();
      if (code) existingCodes.add(code);
    }
    
    let savedCount = 0;
    const skipped = [];
    const newRows = [];
    
    for (var r = 0; r < records.length; r++) {
      var rec = records[r];
      var code = (rec.schedDisbCode || '').toString().trim().toUpperCase();
      
      if (!code) continue;
      
      if (existingCodes.has(code)) {
        skipped.push(code);
        continue;
      }
      
      // Convert date from YYYY-MM-DD to Date object for proper sheet formatting
      var startDate = '';
      if (rec.startDate) {
        var parts = rec.startDate.split('-');
        if (parts.length === 3) {
          startDate = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
        }
      }
      
      // Build row in column order
      var newRow = new Array(Math.max(headers.length, fundingDescColIndex + 1)).fill('');
      if (codeCol !== -1) newRow[codeCol] = code;
      if (startDateCol !== -1) newRow[startDateCol] = startDate;
      if (intervalCol !== -1) newRow[intervalCol] = rec.interval || 14;
      if (box1Col !== -1) newRow[box1Col] = rec.box1 || '';
      if (box2Col !== -1) newRow[box2Col] = rec.box2 || '';
      if (box3Col !== -1) newRow[box3Col] = rec.box3 || '';
      if (fundingSourceCol !== -1) newRow[fundingSourceCol] = rec.fundingSource || '';
      if (activeCol !== -1) newRow[activeCol] = true;
      newRow[fundingDescColIndex] = rec.fundingDescription || '';
      
      newRows.push(newRow);
      existingCodes.add(code);
      savedCount++;
    }
    
    // Bulk write all new rows at once using setValues (avoids data validation conflicts with appendRow)
    if (newRows.length > 0) {
      var lastRow = sheet.getLastRow();
      var writeRange = sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length);
      
      // Temporarily clear any data validation on the target range to prevent write failures
      try {
        writeRange.clearDataValidations();
      } catch (e) {
        Logger.log('Could not clear validations: ' + e.message);
      }
      
      writeRange.setValues(newRows);
      
      // Re-apply data validation on FundingSource column (Col D) for newly written rows
      if (fundingSourceCol !== -1) {
        try {
          var fundingSheet = lookups.getSheetByName('LU_FundingSources');
          if (fundingSheet) {
            var fundingRange = fundingSheet.getRange('A2:A');
            var rule = SpreadsheetApp.newDataValidation()
              .requireValueInRange(fundingRange, true)
              .setAllowInvalid(false)
              .build();
            sheet.getRange(lastRow + 1, fundingSourceCol + 1, newRows.length, 1).setDataValidation(rule);
          }
        } catch (valErr) {
          Logger.log('Could not reapply validation: ' + valErr.message);
        }
      }
      
      // Re-apply checkbox validation on Active column for newly written rows
      if (activeCol !== -1) {
        try {
          sheet.getRange(lastRow + 1, activeCol + 1, newRows.length, 1).setDataValidation(
            SpreadsheetApp.newDataValidation().requireCheckbox().build()
          );
        } catch (valErr) {
          Logger.log('Could not reapply checkbox validation: ' + valErr.message);
        }
      }
      
      // Apply BoxCodes data validation on Box 1 and Box 2 columns for newly written rows
      if (box1Col !== -1 || box2Col !== -1 || box3Col !== -1) {
        try {
          var boxCodesSheet = lookups.getSheetByName('LU_BoxCodes');
          if (boxCodesSheet) {
            var boxRange = boxCodesSheet.getRange('A2:A');
            var boxRule = SpreadsheetApp.newDataValidation()
              .requireValueInRange(boxRange, true)
              .setAllowInvalid(true)
              .build();
            if (box1Col !== -1) {
              sheet.getRange(lastRow + 1, box1Col + 1, newRows.length, 1).setDataValidation(boxRule);
            }
            if (box2Col !== -1) {
              sheet.getRange(lastRow + 1, box2Col + 1, newRows.length, 1).setDataValidation(boxRule);
            }
            if (box3Col !== -1) {
              sheet.getRange(lastRow + 1, box3Col + 1, newRows.length, 1).setDataValidation(boxRule);
            }
          }
        } catch (valErr) {
          Logger.log('Could not apply box code validation: ' + valErr.message);
        }
      }
    }
    
    var message = 'Saved ' + savedCount + ' record(s)';
    if (skipped.length > 0) {
      message += '. Skipped duplicates: ' + skipped.join(', ');
    }
    
    logAudit('SCHED_DISB_ADD', null, message);
    invalidateLookupCache();
    
    return { success: true, savedCount: savedCount, message: message };
    
  } catch (error) {
    Logger.log('saveSchedDisbCodes error: ' + error.message);
    return { success: false, error: 'Save failed: ' + error.message };
  }
}

/**
 * Looks up the StartDate for a Scheduled Distribution Code
 * @param {string} code - The SchedDisbCode to look up
 * @returns {Object} { success: boolean, startDate: string }
 */
function getSchedDisbStartDate(code) {
  if (!code) return '';
  
  try {
    const lookups = getLookupsWorkbook();
    const sheet = lookups.getSheetByName(CONFIG.LOOKUPS.SCHED_DISB_CODES);
    
    if (!sheet) return '';
    
    const data = sheet.getDataRange().getValues();
    if (data.length < 2) return '';
    
    const headers = trimHeaders(data[0]);
    const codeCol = headers.indexOf('SchedDisbCode');
    const startDateCol = headers.indexOf('StartDate');
    
    if (codeCol === -1 || startDateCol === -1) return '';
    
    code = code.toString().toUpperCase();
    
    for (var i = 1; i < data.length; i++) {
      var rowCode = (data[i][codeCol] || '').toString().trim().toUpperCase();
      if (rowCode === code) {
        var startDate = data[i][startDateCol];
        if (startDate instanceof Date && !isNaN(startDate.getTime())) {
          return Utilities.formatDate(startDate, CONFIG.TIMEZONE, 'M/d/yyyy');
        } else if (startDate) {
          var parsed = new Date(startDate);
          if (!isNaN(parsed.getTime())) {
            return Utilities.formatDate(parsed, CONFIG.TIMEZONE, 'M/d/yyyy');
          }
        }
        return startDate ? startDate.toString() : '';
      }
    }
    
    return '';
  } catch (e) {
    Logger.log('getSchedDisbStartDate error: ' + e.message);
    return '';
  }
}

/**
 * Sets Active=FALSE for a Scheduled Distribution Code in LU_SchedDisbCodes
 * Called after distribution processing is complete
 * @param {string} code - The SchedDisbCode to deactivate
 * @returns {Object} { success: boolean }
 */
function deactivateSchedDisbCode(code) {
  if (!code) return false;
  
  try {
    const lookups = getLookupsWorkbook();
    const sheet = lookups.getSheetByName(CONFIG.LOOKUPS.SCHED_DISB_CODES);
    
    if (!sheet) return false;
    
    const data = sheet.getDataRange().getValues();
    if (data.length < 2) return false;
    
    const headers = trimHeaders(data[0]);
    const codeCol = headers.indexOf('SchedDisbCode');
    const activeCol = headers.indexOf('Active');
    
    if (codeCol === -1 || activeCol === -1) return false;
    
    code = code.toString().toUpperCase();
    
    for (var i = 1; i < data.length; i++) {
      var rowCode = (data[i][codeCol] || '').toString().trim().toUpperCase();
      if (rowCode === code) {
        sheet.getRange(i + 1, activeCol + 1).setValue(false);
        logAudit('SCHED_DISB_DEACTIVATE', null, 'Deactivated SchedDisbCode: ' + code);
        invalidateLookupCache();
        return true;
      }
    }
    
    return false;
  } catch (e) {
    Logger.log('deactivateSchedDisbCode error: ' + e.message);
    return false;
  }
}

// ============ Event Information Management Functions ============

/**
 * Returns pickup Times from LU_DistribCodes column C (Times)
 * Only returns time slots that have non-empty values
 * Used by Distribution Report to populate available pickup times
 * @returns {string[]} Array of non-empty time strings
 */
/**
 * Returns pickup Times from LU_DistribCodes, optionally filtered by Code
 * @param {string} [distribCode] - Optional distribution code to filter by
 * @returns {string[]} Array of unique time strings
 */
function getDistribCodeTimes(distribCode) {
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.getDistribCodeTimes(distribCode);
  try {
    var lookups = getLookupsWorkbook();
    var sheetName = CONFIG.LOOKUPS.DISTRIB_CODES;
    var sheet = lookups.getSheetByName(sheetName);
    
    if (!sheet) {
      Logger.log('LU_DistribCodes sheet not found');
      return [];
    }
    
    var data = sheet.getDataRange().getValues();
    if (data.length < 2) return [];
    
    var headers = trimHeaders(data[0]);
    var codeCol = headers.indexOf('Code');
    var timesCol = headers.indexOf('Times');
    
    // Fall back to column C (index 2) if no 'Times' header found
    if (timesCol === -1 && headers.length > 2) {
      timesCol = 2;
    }
    
    if (timesCol === -1) return [];
    
    var filterCode = distribCode ? distribCode.toString().trim().toUpperCase() : '';
    
    var times = [];
    var seen = {};
    for (var i = 1; i < data.length; i++) {
      // If a code was provided, only include rows matching that code
      if (filterCode && codeCol !== -1) {
        var rowCode = (data[i][codeCol] || '').toString().trim().toUpperCase();
        if (rowCode !== filterCode) continue;
      }
      
      var val = (data[i][timesCol] || '').toString().trim();
      if (val && !seen[val]) {
        times.push(val);
        seen[val] = true;
      }
    }
    
    return times;
  } catch (e) {
    Logger.log('getDistribCodeTimes error: ' + e.message);
    return [];
  }
}

/**
 * Returns all active Scheduled Distribution Codes with details
 * Used to populate dropdowns in Distribution Reports and Process Distribution tabs
 * @returns {Object[]} Array of { code, startDate, interval, fundingSource, box1, box2, times }
 */
function getActiveSchedDisbCodes() {
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.getActiveSchedDisbCodes();
  try {
    var lookups = getLookupsWorkbook();
    var sheet = lookups.getSheetByName(CONFIG.LOOKUPS.SCHED_DISB_CODES);
    
    if (!sheet) return [];
    
    var data = sheet.getDataRange().getValues();
    if (data.length < 2) return [];
    
    var headers = trimHeaders(data[0]);
    var codeCol = headers.indexOf('SchedDisbCode');
    var startDateCol = headers.indexOf('StartDate');
    var intervalCol = headers.indexOf('Interval');
    var fundingCol = headers.indexOf('FundingSource');
    if (fundingCol === -1) fundingCol = headers.indexOf('Funding Source');
    var activeCol = headers.indexOf('Active');
    
    if (codeCol === -1) return [];
    
    // Resolve box/times columns once (handle both naming conventions)
    var box1Col = headers.indexOf('Box 1');
    if (box1Col === -1) box1Col = headers.indexOf('Box1');
    var box2Col = headers.indexOf('Box 2');
    if (box2Col === -1) box2Col = headers.indexOf('Box2');
    var box3Col = headers.indexOf('Box 3');
    if (box3Col === -1) box3Col = headers.indexOf('Box3');
    var timesCol = headers.indexOf('Times');
    
    var results = [];
    for (var i = 1; i < data.length; i++) {
      var isActive = isRowActive(data[i], activeCol);
      if (!isActive) continue;
      
      var code = (data[i][codeCol] || '').toString().trim();
      if (!code) continue;
      
      var startDate = '';
      if (startDateCol !== -1 && data[i][startDateCol]) {
        var sd = data[i][startDateCol];
        if (sd instanceof Date && !isNaN(sd.getTime())) {
          startDate = Utilities.formatDate(sd, CONFIG.TIMEZONE, 'M/d/yyyy');
        } else {
          startDate = sd.toString();
        }
      }
      
      results.push({
        code: code,
        startDate: startDate,
        interval: intervalCol !== -1 ? (data[i][intervalCol] || '') : '',
        fundingSource: fundingCol !== -1 ? (data[i][fundingCol] || '') : '',
        box1: box1Col !== -1 ? (data[i][box1Col] || '').toString().trim() : '',
        box2: box2Col !== -1 ? (data[i][box2Col] || '').toString().trim() : '',
        box3: box3Col !== -1 ? (data[i][box3Col] || '').toString().trim() : '',
        times: timesCol !== -1 ? (data[i][timesCol] || '').toString().trim() : ''
      });
    }
    
    return results;
  } catch (e) {
    Logger.log('getActiveSchedDisbCodes error: ' + e.message);
    return [];
  }
}

/**
 * Save multiple Event Information records to LU_EventInfo
 * Requires CONFIG.LOOKUPS.EVENT_INFO = 'LU_EventInfo' in Code.gs
 * @param {Object[]} records - Array of event info objects
 * @returns {Object} { success: boolean, savedCount: number }
 */
function saveEventInfoRecords(records) {
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.saveEventInfoRecords(records);
  try {
    if (!records || records.length === 0) {
      return { success: false, error: 'No records to save' };
    }
    
    var lookupKey = CONFIG.LOOKUPS.EVENT_INFO;
    if (!lookupKey) {
      return { success: false, error: 'EVENT_INFO not configured in CONFIG.LOOKUPS. Add EVENT_INFO: \'LU_EventInfo\' to Code.gs' };
    }
    
    const lookups = getLookupsWorkbook();
    const sheet = lookups.getSheetByName(lookupKey);
    
    if (!sheet) {
      return { success: false, error: 'LU_EventInfo sheet not found in G2N_Lookups' };
    }
    
    const existingData = sheet.getDataRange().getValues();
    const headers = trimHeaders(existingData[0]);
    
    // Map column indices
    const colMap = {
      eventYear:        headers.indexOf('Event Year'),
      distributionType: headers.indexOf('Distribution Type'),
      typeOfItems:      headers.indexOf('Type Of Items Distributed'),
      eventBeginsDate:  headers.indexOf('Event Begins Date'),
      eventBeginsTime:  headers.indexOf('Event Begins Time'),  // v5.2
      eventEndsDate:    headers.indexOf('Event Ends Date'),
      eventEndsTime:    headers.indexOf('Event Ends Time'),    // v5.2
      boxCode:          headers.indexOf('BoxCode'),
      numberServed:     headers.indexOf('Number Served'),
      location:         headers.indexOf('Location'),
      fundingSource:    headers.indexOf('Funding Source'),
      notes:            headers.indexOf('Notes')
    };
    
    var savedCount = 0;
    var newRows = [];
    
    for (var r = 0; r < records.length; r++) {
      var rec = records[r];
      
      // Convert dates from YYYY-MM-DD to Date objects
      var beginsDate = '';
      if (rec.eventBeginsDate) {
        var parts = rec.eventBeginsDate.split('-');
        if (parts.length === 3) {
          beginsDate = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
        }
      }
      
      var endsDate = '';
      if (rec.eventEndsDate) {
        var parts2 = rec.eventEndsDate.split('-');
        if (parts2.length === 3) {
          endsDate = new Date(parseInt(parts2[0]), parseInt(parts2[1]) - 1, parseInt(parts2[2]));
        }
      }
      
      // Build row in column order
      var newRow = new Array(headers.length).fill('');
      if (colMap.eventYear !== -1) newRow[colMap.eventYear] = rec.eventYear || '';
      if (colMap.distributionType !== -1) newRow[colMap.distributionType] = rec.distributionType || '';
      if (colMap.typeOfItems !== -1) newRow[colMap.typeOfItems] = rec.typeOfItems || '';
      if (colMap.eventBeginsDate !== -1) newRow[colMap.eventBeginsDate] = beginsDate;
      if (colMap.eventEndsDate !== -1) newRow[colMap.eventEndsDate] = endsDate;
      if (colMap.boxCode !== -1) newRow[colMap.boxCode] = rec.boxCode || '';
      if (colMap.numberServed !== -1) newRow[colMap.numberServed] = rec.numberServed || '';
      if (colMap.location !== -1) newRow[colMap.location] = rec.location || '';
      if (colMap.fundingSource !== -1) newRow[colMap.fundingSource] = rec.fundingSource || '';
      if (colMap.notes !== -1) newRow[colMap.notes] = rec.notes || '';
      
      newRows.push(newRow);
      savedCount++;
    }
    
    // Bulk write all new rows
    if (newRows.length > 0) {
      var lastRow = sheet.getLastRow();
      sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
    }
    
    var message = 'Saved ' + savedCount + ' event record(s)';
    logAudit('EVENT_INFO_ADD', null, message);
    invalidateLookupCache();
    
    return { success: true, savedCount: savedCount, message: message };
    
  } catch (error) {
    Logger.log('saveEventInfoRecords error: ' + error.message);
    return { success: false, error: 'Save failed: ' + error.message };
  }
}

// ============ Staff/Volunteer Management Functions ============

/**
 * Returns Staff/Volunteer records with row numbers from LU_Staff
 * @param {boolean} includeInactive - If true, returns all records; if false, active only
 * @returns {Object} { success: boolean, records: Object[] }
 */
function getStaffRecords(includeInactive) {
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.getStaffRecords(includeInactive);
  try {
    var sheetName = CONFIG.LOOKUPS.STAFF;
    if (!sheetName) {
      return { success: false, error: 'STAFF not configured in CONFIG.LOOKUPS' };
    }
    
    var lookups = getLookupsWorkbook();
    var sheet = lookups.getSheetByName(sheetName);
    
    if (!sheet) {
      return { success: false, error: 'LU_Staff sheet not found' };
    }
    
    var data = sheet.getDataRange().getValues();
    if (data.length < 2) {
      return { success: true, records: [] };
    }
    
    var headers = trimHeaders(data[0]);
    var codeCol = headers.indexOf('Code');
    var nameCol = headers.indexOf('Name');
    var roleCol = headers.indexOf('Role');
    var activeCol = headers.indexOf('Active');
    var emailCol = headers.indexOf('Email');
    
    var records = [];
    for (var i = 1; i < data.length; i++) {
      var isActive = isRowActive(data[i], activeCol);
      
      // Include record if active, or if includeInactive is true and record is inactive
      if (isActive || includeInactive) {
        records.push({
          rowNum: i + 1, // 1-based sheet row
          Code: codeCol !== -1 ? (data[i][codeCol] || '').toString() : '',
          Name: nameCol !== -1 ? (data[i][nameCol] || '').toString() : '',
          Role: roleCol !== -1 ? (data[i][roleCol] || '').toString() : '',
          Email: emailCol !== -1 ? (data[i][emailCol] || '').toString() : '',
          Active: isActive
        });
      }
    }
    
    // Sort by Name
    records.sort(function(a, b) {
      return a.Name.localeCompare(b.Name);
    });
    
    return { success: true, records: records };
    
  } catch (error) {
    Logger.log('getStaffRecords error: ' + error.message);
    return { success: false, error: 'Load failed: ' + error.message };
  }
}

/**
 * Updates existing and adds new Staff/Volunteer records in LU_Staff
 * @param {Object} data - { updates: Object[], newRecords: Object[] }
 * @returns {Object} { success: boolean, updatedCount: number, addedCount: number }
 */
function saveStaffRecords(updates, newRecords) {
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.saveStaffRecords(updates, newRecords);
  try {
    var sheetName = CONFIG.LOOKUPS.STAFF;
    if (!sheetName) {
      return { success: false, error: 'STAFF not configured in CONFIG.LOOKUPS' };
    }
    
    var lookups = getLookupsWorkbook();
    var sheet = lookups.getSheetByName(sheetName);
    
    if (!sheet) {
      return { success: false, error: 'LU_Staff sheet not found' };
    }
    
    var data = sheet.getDataRange().getValues();
    var headers = trimHeaders(data[0]);
    var codeCol = headers.indexOf('Code');
    var nameCol = headers.indexOf('Name');
    var roleCol = headers.indexOf('Role');
    var activeCol = headers.indexOf('Active');
    var emailCol = headers.indexOf('Email');
    
    var updatedCount = 0;
    var addedCount = 0;
    
    // Process updates (modify existing rows in the data array)
    if (updates && updates.length > 0) {
      for (var u = 0; u < updates.length; u++) {
        var upd = updates[u];
        var rowIdx = upd.rowNum - 1; // Convert to 0-based array index
        if (rowIdx >= 1 && rowIdx < data.length) {
          if (codeCol !== -1) data[rowIdx][codeCol] = (upd.code || '').toUpperCase(); // Phase 5: Force uppercase
          if (nameCol !== -1) data[rowIdx][nameCol] = upd.name || '';
          if (roleCol !== -1) data[rowIdx][roleCol] = upd.role || '';
          if (activeCol !== -1) data[rowIdx][activeCol] = upd.active ? true : false;
          if (emailCol !== -1) data[rowIdx][emailCol] = upd.email || '';
          updatedCount++;
        }
      }
      
      // Bulk rewrite all data rows
      if (data.length > 1) {
        var dataRows = data.slice(1);
        sheet.getRange(2, 1, dataRows.length, dataRows[0].length).setValues(dataRows);
      }
    }
    
    // Process new records (append)
    if (newRecords && newRecords.length > 0) {
      // Check for duplicate codes
      var existingCodes = new Set();
      for (var i = 1; i < data.length; i++) {
        var c = codeCol !== -1 ? (data[i][codeCol] || '').toString().trim().toUpperCase() : '';
        if (c) existingCodes.add(c);
      }
      
      var newRows = [];
      var skipped = [];
      
      for (var n = 0; n < newRecords.length; n++) {
        var rec = newRecords[n];
        var code = (rec.code || '').toString().trim().toUpperCase(); // Phase 5: Force uppercase
        
        if (!code) continue;
        
        if (existingCodes.has(code.toUpperCase())) {
          skipped.push(code);
          continue;
        }
        
        var newRow = new Array(headers.length).fill('');
        if (codeCol !== -1) newRow[codeCol] = code; // Already uppercased above
        if (nameCol !== -1) newRow[nameCol] = rec.name || '';
        if (roleCol !== -1) newRow[roleCol] = rec.role || '';
        if (activeCol !== -1) newRow[activeCol] = rec.active ? true : false;
        if (emailCol !== -1) newRow[emailCol] = rec.email || '';
        
        newRows.push(newRow);
        existingCodes.add(code.toUpperCase());
        addedCount++;
      }
      
      if (newRows.length > 0) {
        var lastRow = sheet.getLastRow();
        sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
      }
      
      if (skipped.length > 0) {
        var message = 'Updated ' + updatedCount + ', added ' + addedCount + '. Skipped duplicate codes: ' + skipped.join(', ');
        logAudit('STAFF_EDIT', null, message);
        invalidateLookupCache();
        return { success: true, message: message };
      }
    }
    
    var message = 'Updated ' + updatedCount + ' record(s), added ' + addedCount + ' new record(s)';
    logAudit('STAFF_EDIT', null, message);
    invalidateLookupCache();
    
    return { success: true, message: message };
    
  } catch (error) {
    Logger.log('saveStaffRecords error: ' + error.message);
    return { success: false, error: 'Save failed: ' + error.message };
  }
}

// ============ Generic Lookup Editor Functions (v4.7) ============

/**
 * Returns all rows (including inactive) with headers and row numbers for a lookup sheet.
 * Delegates to getDistributedProductRecords() for the DISTRIBUTED_PRODUCTS key.
 * Handles the FIELD_MAP key for the LU_FieldMap sheet.
 * v4.7 - New function for Admin Portal generic LU editor
 * v5.0 - Added FIELD_MAP branch for LU_FieldMap admin editing
 * @param {string} sheetKey - Key from CONFIG.LOOKUPS, 'DISTRIBUTED_PRODUCTS', or 'FIELD_MAP'
 * @returns {Object} { success, headers: string[], records: Object[] }
 */
function getLookupRecords(sheetKey) {
  if (sheetKey === 'DISTRIBUTED_PRODUCTS') {
    return getDistributedProductRecords();
  }

  // LU_FieldMap lives in G2N_Lookups but is not in CONFIG.LOOKUPS
  if (sheetKey === 'FIELD_MAP') {
    return getLookupRecords_('LU_FieldMap', getLookupsWorkbook());
  }

  try {
    var sheetName = CONFIG.LOOKUPS[sheetKey];
    if (!sheetName) return { success: false, error: 'Unknown lookup key: ' + sheetKey };
    return getLookupRecords_(sheetName, getLookupsWorkbook());
  } catch (e) {
    Logger.log('getLookupRecords error: ' + e.message);
    return { success: false, error: 'Load failed: ' + e.message };
  }
}

/**
 * Internal helper — reads all non-empty rows from a sheet into records array.
 * @private
 */
function getLookupRecords_(sheetName, workbook) {
  try {
    var sheet = workbook.getSheetByName(sheetName);
    if (!sheet) return { success: false, error: 'Sheet not found: ' + sheetName };

    var data = sheet.getDataRange().getValues();
    if (data.length < 1) return { success: true, headers: [], records: [] };

    var headers = trimHeaders(data[0]);
    var records = [];

    for (var i = 1; i < data.length; i++) {
      var row = data[i];
      var isEmpty = row.every(function(v) { return v === '' || v === null || v === undefined; });
      if (isEmpty) continue;

      var rec = { rowNum: i + 1 };
      for (var j = 0; j < headers.length; j++) {
        var val = row[j];
        if (val instanceof Date && !isNaN(val.getTime())) {
            var hdr = headers[j] || '';
            val = /Time$/i.test(hdr)
                ? Utilities.formatDate(val, CONFIG.TIMEZONE, 'h:mm a')
                : Utilities.formatDate(val, CONFIG.TIMEZONE, 'M/d/yyyy');
        }
        rec[headers[j]] = (val !== null && val !== undefined) ? val : '';
      }
      records.push(rec);
    }

    return { success: true, headers: headers, records: records };
  } catch (e) {
    return { success: false, error: 'Load failed: ' + e.message };
  }
}

/**
 * Generic save for any LU sheet in G2N_Lookups workbook.
 * Handles row updates (by rowNum) and new record appends.
 * Delegates to saveDistributedProductRecords() for DISTRIBUTED_PRODUCTS key.
 * v4.7 - New function for Admin Portal generic LU editor
 * v5.0 - Added FIELD_MAP branch for LU_FieldMap sheet.
 *         After save, calls DbService.invalidateLookupTableCache() when
 *         USE_MYSQL=true to bust the MySQL-side lookup cache immediately.
 * @param {string} sheetKey - Key from CONFIG.LOOKUPS, 'DISTRIBUTED_PRODUCTS', or 'FIELD_MAP'
 * @param {Object[]} updates - Existing rows to update, each with rowNum property
 * @param {Object[]} newRecords - New rows to append (no rowNum)
 * @returns {Object} { success, message, updatedCount, addedCount }
 */
function saveLookupRecords(sheetKey, updates, newRecords) {
  if (sheetKey === 'DISTRIBUTED_PRODUCTS') {
    return saveDistributedProductRecords(updates, newRecords);
  }

  try {
    // Resolve sheet name — FIELD_MAP is not in CONFIG.LOOKUPS
    var sheetName = (sheetKey === 'FIELD_MAP') ? 'LU_FieldMap' : CONFIG.LOOKUPS[sheetKey];
    if (!sheetName) return { success: false, error: 'Unknown lookup key: ' + sheetKey };

    var workbook = getLookupsWorkbook();
    var sheet = workbook.getSheetByName(sheetName);
    if (!sheet) return { success: false, error: 'Sheet not found: ' + sheetName };

    var data = sheet.getDataRange().getValues();
    var headers = trimHeaders(data[0]);
    var updatedCount = 0;
    var addedCount = 0;

    // Apply updates to existing rows
    if (updates && updates.length > 0) {
      for (var u = 0; u < updates.length; u++) {
        var upd = updates[u];
        var rowIdx = upd.rowNum - 1; // Convert to 0-based array index
        if (rowIdx >= 1 && rowIdx < data.length) {
          for (var j = 0; j < headers.length; j++) {
            var h = headers[j];
            if (h === 'Active') {
              data[rowIdx][j] = (upd[h] === true || upd[h] === 'true' || upd[h] === 'TRUE');
            } else if (upd[h] !== undefined) {
              data[rowIdx][j] = upd[h];
            }
          }
          updatedCount++;
        }
      }
      if (data.length > 1) {
        sheet.getRange(2, 1, data.length - 1, data[0].length).setValues(data.slice(1));
      }
    }

    // Append new records
    if (newRecords && newRecords.length > 0) {
      var newRows = [];
      for (var n = 0; n < newRecords.length; n++) {
        var rec = newRecords[n];
        var newRow = new Array(headers.length).fill('');
        for (var k = 0; k < headers.length; k++) {
          var hk = headers[k];
          if (hk === 'Active') {
            // Default Active to true for new records unless explicitly false
            newRow[k] = !(rec[hk] === false || rec[hk] === 'false' || rec[hk] === 'FALSE');
          } else if (rec[hk] !== undefined) {
            newRow[k] = rec[hk];
          }
        }
        newRows.push(newRow);
        addedCount++;
      }
      if (newRows.length > 0) {
        sheet.getRange(sheet.getLastRow() + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
      }
    }

    var message = 'Updated ' + updatedCount + ', added ' + addedCount;
    logAudit('LOOKUP_EDIT', null, sheetName + ': ' + message);

    // Bust GAS-side cache + MySQL lookup/field-map caches
    invalidateLookupCache();

    // When MySQL active, also bust the general lookup table cache on the bridge
    // so portal dropdowns reflect the saved changes without TTL lag
    if (CONFIG.DB && CONFIG.DB.USE_MYSQL) {
      try {
        DbService.invalidateLookupTableCache();
      } catch (e) {
        Logger.log('saveLookupRecords: MySQL lookup cache bust (non-fatal): ' + e.message);
      }
    }

    return { success: true, message: message, updatedCount: updatedCount, addedCount: addedCount };

  } catch (e) {
    Logger.log('saveLookupRecords error: ' + e.message);
    return { success: false, error: 'Save failed: ' + e.message };
  }
}

/**
 * Returns all rows with headers and row numbers from Distributed_Products in G2N_Data.
 * v4.7 - New function for Admin Portal generic LU editor
 * @returns {Object} { success, headers: string[], records: Object[] }
 */
function getDistributedProductRecords() {
  try {
    var sheet = getDataWorkbook().getSheetByName(CONFIG.DATA_SHEETS.DISTRIBUTED_PRODUCTS);
    if (!sheet) return { success: false, error: 'Distributed_Products sheet not found' };

    var data = sheet.getDataRange().getValues();
    if (data.length < 1) return { success: true, headers: [], records: [] };

    var headers = trimHeaders(data[0]);
    var records = [];

    for (var i = 1; i < data.length; i++) {
      var row = data[i];
      var isEmpty = row.every(function(v) { return v === '' || v === null || v === undefined; });
      if (isEmpty) continue;

      var rec = { rowNum: i + 1 };
      for (var j = 0; j < headers.length; j++) {
        var val = row[j];
        if (val instanceof Date && !isNaN(val.getTime())) {
          val = Utilities.formatDate(val, CONFIG.TIMEZONE, 'M/d/yyyy');
        }
        rec[headers[j]] = (val !== null && val !== undefined) ? val : '';
      }
      records.push(rec);
    }

    return { success: true, headers: headers, records: records };

  } catch (e) {
    Logger.log('getDistributedProductRecords error: ' + e.message);
    return { success: false, error: 'Load failed: ' + e.message };
  }
}

/**
 * Saves updates and new rows to Distributed_Products sheet in G2N_Data workbook.
 * v4.7 - New function for Admin Portal generic LU editor
 * @param {Object[]} updates - Existing rows to update, each with rowNum property
 * @param {Object[]} newRecords - New rows to append
 * @returns {Object} { success, message, updatedCount, addedCount }
 */
function saveDistributedProductRecords(updates, newRecords) {
  try {
    var sheet = getDataWorkbook().getSheetByName(CONFIG.DATA_SHEETS.DISTRIBUTED_PRODUCTS);
    if (!sheet) return { success: false, error: 'Distributed_Products sheet not found' };

    var data = sheet.getDataRange().getValues();
    var headers = trimHeaders(data[0]);
    var updatedCount = 0;
    var addedCount = 0;

    if (updates && updates.length > 0) {
      for (var u = 0; u < updates.length; u++) {
        var upd = updates[u];
        var rowIdx = upd.rowNum - 1;
        if (rowIdx >= 1 && rowIdx < data.length) {
          for (var j = 0; j < headers.length; j++) {
            if (upd[headers[j]] !== undefined) data[rowIdx][j] = upd[headers[j]];
          }
          updatedCount++;
        }
      }
      if (data.length > 1) {
        sheet.getRange(2, 1, data.length - 1, data[0].length).setValues(data.slice(1));
      }
    }

    if (newRecords && newRecords.length > 0) {
      var newRows = [];
      for (var n = 0; n < newRecords.length; n++) {
        var rec = newRecords[n];
        var newRow = new Array(headers.length).fill('');
        for (var k = 0; k < headers.length; k++) {
          if (rec[headers[k]] !== undefined) newRow[k] = rec[headers[k]];
        }
        newRows.push(newRow);
        addedCount++;
      }
      if (newRows.length > 0) {
        sheet.getRange(sheet.getLastRow() + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
      }
    }

    var message = 'Updated ' + updatedCount + ', added ' + addedCount;
    logAudit('DISTRIBUTED_PRODUCTS_EDIT', null, message);

    return { success: true, message: message, updatedCount: updatedCount, addedCount: addedCount };

  } catch (e) {
    Logger.log('saveDistributedProductRecords error: ' + e.message);
    return { success: false, error: 'Save failed: ' + e.message };
  }
}

/**
 * Reads LU_LastScheduled and returns the last ApplicantId value + 1.
 * Used to pre-fill the Beginning ID field on the Scheduling Report tab.
 * Returns 0 if the sheet is empty or not found (caller should leave field blank).
 * v5.1 - Added for Scheduling Report auto-populate Beginning ID
 * @returns {Object} { success: boolean, nextId: number }
 */
function getLastScheduledId() {
  try {
    var sheetName = CONFIG.LOOKUPS.SAVED_SCHEDULE_ID;
    if (!sheetName) return { success: false, error: 'SAVED_SCHEDULE_ID not configured', nextId: 0 };

    var sheet = getLookupsWorkbook().getSheetByName(sheetName);
    if (!sheet) return { success: false, error: 'LU_LastScheduled sheet not found', nextId: 0 };

    var lastRow = sheet.getLastRow();
    if (lastRow < 2) return { success: true, nextId: 0 }; // No data rows yet

    var headers = trimHeaders(sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0]);
    var applicantIdCol = headers.indexOf('ApplicantId');
    if (applicantIdCol === -1) return { success: false, error: 'ApplicantId column not found', nextId: 0 };

    var lastVal = sheet.getRange(lastRow, applicantIdCol + 1).getValue();
    var lastId  = parseInt(lastVal);
    if (isNaN(lastId)) return { success: true, nextId: 0 };

    return { success: true, nextId: lastId + 1 };
  } catch (e) {
    Logger.log('getLastScheduledId error: ' + e.message);
    return { success: false, error: e.message, nextId: 0 };
  }
}

/**
 * Appends a new row to LU_LastScheduled with the last AM ID used in a
 * Scheduling Report and the report generation date.
 * Called after generateSchedulingReport() succeeds.
 * v5.1 - Added for Scheduling Report audit trail
 * @param {number} lastId     - The ending AM ID used in the report (result.endId)
 * @param {string} reportDate - Formatted date string (M/d/yyyy)
 * @returns {Object} { success: boolean }
 */
function appendLastScheduledId(lastId, reportDate) {
  try {
    var sheetName = CONFIG.LOOKUPS.SAVED_SCHEDULE_ID;
    if (!sheetName) return { success: false, error: 'SAVED_SCHEDULE_ID not configured' };

    var sheet = getLookupsWorkbook().getSheetByName(sheetName);
    if (!sheet) return { success: false, error: 'LU_LastScheduled sheet not found' };

    var headers = trimHeaders(sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0]);
    var applicantIdCol = headers.indexOf('ApplicantId');
    var reportDateCol  = headers.indexOf('ReportDate');

    var newRow = new Array(headers.length).fill('');
    if (applicantIdCol !== -1) newRow[applicantIdCol] = lastId;
    if (reportDateCol  !== -1) newRow[reportDateCol]  = reportDate;

    sheet.getRange(sheet.getLastRow() + 1, 1, 1, newRow.length).setValues([newRow]);
    return { success: true };
  } catch (e) {
    Logger.log('appendLastScheduledId error: ' + e.message);
    return { success: false, error: e.message };
  }
}
