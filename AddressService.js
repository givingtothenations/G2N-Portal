/**
 * AddressService.gs
 * Google Maps Geocoding API integration for address verification and standardization.
 * Uses UrlFetchApp to call Google Maps Geocoding API, returns standardized address
 * components including auto-resolved county.
 *
 * SETUP: Store your Google Maps API key in Script Properties:
 *   Project Settings → Script Properties → GOOGLE_MAPS_API_KEY
 *
 * Version History:
 *   v1.0.0 - 2025-02-27 - Initial creation. Phase 1: server-side verification with test harness.
 *   v1.1.0 - 2025-02-27 - Batch validation: validateAllAddresses() iterates AM rows,
 *            calls Geocoding API per row, updates Street Address/City/State/Zip Code/County
 *            on HIGH/MEDIUM confidence, skips updates on LOW. Always writes confidence
 *            level to "Address Validity" column (auto-created if missing). Rate-limited
 *            at 200ms per call. Supports skip-already-validated and force-revalidate modes.
 *            Added getValidationSummary() for quick status check.
 *   v1.2.0 - 2025-02-27 - Fix timeout: Time-aware loop processes rows for ~5 min per
 *            run, saves progress to ScriptProperties. Run continueValidation() to resume.
 *            No triggers needed — just re-run manually until done.
 *            Fix state: Writes full state name (stateLong) instead of abbreviation.
 *            Fix column: Address Validity inserted right after County (col S) instead
 *            of appended at end. Added moveAddressValidityColumn() utility to relocate
 *            existing column. Added stopValidation() to cancel in-progress batch.
 *   v1.3.0 - Adopted shared trimHeaders() utility (#11).
 *            Note: Test functions (lines 720+) are candidates for extraction
 *            to a separate AddressService_Tests.gs file (#10).
 *   v1.4.0 - 2025-03-01 - Extracted test functions to AddressService_Tests.gs (#10).
 *            Removed moveAddressValidityColumn() — one-time utility no longer needed.
 *            Removed forceRevalidate mode from validateAllAddresses/processValidationRows_.
 *            Batch validation now always skips rows that already have an Address Validity
 *            value, preventing interference with AI portal single-row validation.
 *   v1.5.0 - Skip condition changed: validateAllAddresses() and processValidationRows_()
 *            now skip only rows where Address Validity is already HIGH. Rows with
 *            MEDIUM, LOW, ERROR, NO_ADDRESS, UNKNOWN, or empty are re-validated.
 *            Added validateAddressesMenu() and continueAddressValidationMenu() —
 *            public wrappers called from G2N Management > Setup menu; show alert
 *            with result summary or continuation status.
 */

// ============================================================
// CONFIGURATION
// ============================================================

/**
 * Retrieves the Google Maps API key from Script Properties.
 * @returns {string} The API key
 * @throws {Error} If the key is not configured
 */
function getGoogleMapsApiKey_() {
  var key = PropertiesService.getScriptProperties().getProperty('GOOGLE_MAPS_API_KEY');
  if (!key) {
    throw new Error('GOOGLE_MAPS_API_KEY not found in Script Properties. ' +
      'Go to Project Settings → Script Properties to add it.');
  }
  return key;
}

// ============================================================
// CORE API FUNCTIONS
// ============================================================

/**
 * Validates and standardizes an address using Google Maps Geocoding API.
 * Parses the response into structured components including county.
 *
 * @param {Object} addressInput - Address fields to verify
 * @param {string} addressInput.street - Street address
 * @param {string} addressInput.city - City name
 * @param {string} addressInput.state - State (name or abbreviation)
 * @param {string} addressInput.zip - 5-digit zip code
 * @returns {Object} Result object with status, original input, and verified components
 */
function verifyAddress(addressInput) {
  try {
    var apiKey = getGoogleMapsApiKey_();
    var queryAddress = [
      addressInput.street || '',
      addressInput.city || '',
      addressInput.state || '',
      addressInput.zip || ''
    ].filter(function(p) { return p.trim() !== ''; }).join(', ');

    if (!queryAddress) {
      return { success: false, error: 'No address provided', original: addressInput };
    }

    var url = 'https://maps.googleapis.com/maps/api/geocode/json'
      + '?address=' + encodeURIComponent(queryAddress)
      + '&key=' + apiKey;

    var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    var json = JSON.parse(response.getContentText());

    // Check API-level errors (invalid key, over quota, etc.)
    if (json.status === 'REQUEST_DENIED') {
      return { success: false, error: 'API request denied: ' + (json.error_message || 'Check API key and billing'), original: addressInput };
    }
    if (json.status === 'OVER_QUERY_LIMIT') {
      return { success: false, error: 'API quota exceeded. Try again later.', original: addressInput };
    }
    if (json.status === 'ZERO_RESULTS' || !json.results || json.results.length === 0) {
      return { success: false, error: 'Address not found. Please check and re-enter.', original: addressInput };
    }

    // Parse the top result
    var result = json.results[0];
    var parsed = parseAddressComponents_(result.address_components);

    return {
      success: true,
      original: addressInput,
      verified: {
        street: buildStreetAddress_(parsed),
        city: parsed.city || '',
        state: parsed.stateShort || '',
        stateLong: parsed.stateLong || '',
        zip: parsed.zip || '',
        county: parsed.county || '',
        formattedAddress: result.formatted_address || '',
        locationType: result.geometry ? result.geometry.location_type : '',
        lat: result.geometry ? result.geometry.location.lat : null,
        lng: result.geometry ? result.geometry.location.lng : null
      },
      confidence: getConfidenceLevel_(result.geometry ? result.geometry.location_type : ''),
      resultCount: json.results.length
    };

  } catch (e) {
    return { success: false, error: 'Verification failed: ' + e.message, original: addressInput };
  }
}

// ============================================================
// PARSING HELPERS
// ============================================================

/**
 * Parses Google Geocoding address_components array into a flat object.
 * Maps component types to friendly field names.
 *
 * @param {Array} components - address_components from Geocoding API response
 * @returns {Object} Flat object with street_number, route, city, county, stateShort, stateLong, zip, country
 */
function parseAddressComponents_(components) {
  var map = {
    street_number: '',
    route: '',
    city: '',
    county: '',
    stateShort: '',
    stateLong: '',
    zip: '',
    country: ''
  };

  (components || []).forEach(function(comp) {
    var types = comp.types || [];
    if (types.indexOf('street_number') > -1)                    map.street_number = comp.long_name;
    if (types.indexOf('route') > -1)                            map.route = comp.long_name;
    if (types.indexOf('locality') > -1)                         map.city = comp.long_name;
    if (types.indexOf('sublocality_level_1') > -1 && !map.city) map.city = comp.long_name;
    if (types.indexOf('administrative_area_level_2') > -1)      map.county = comp.long_name.replace(/ County$/i, '');
    if (types.indexOf('administrative_area_level_1') > -1) {
      map.stateShort = comp.short_name;
      map.stateLong = comp.long_name;
    }
    if (types.indexOf('postal_code') > -1)                      map.zip = comp.long_name;
    if (types.indexOf('country') > -1)                          map.country = comp.short_name;
  });

  return map;
}

/**
 * Builds a street address string from parsed components (number + route).
 *
 * @param {Object} parsed - Output from parseAddressComponents_
 * @returns {string} Combined street address
 */
function buildStreetAddress_(parsed) {
  return [parsed.street_number, parsed.route]
    .filter(function(p) { return p && p.trim() !== ''; })
    .join(' ');
}

/**
 * Maps Google's location_type to a user-friendly confidence level.
 * ROOFTOP = exact match, RANGE_INTERPOLATED = good, GEOMETRIC_CENTER/APPROXIMATE = low.
 *
 * @param {string} locationType - geometry.location_type from API response
 * @returns {string} HIGH, MEDIUM, or LOW
 */
function getConfidenceLevel_(locationType) {
  switch (locationType) {
    case 'ROOFTOP':             return 'HIGH';
    case 'RANGE_INTERPOLATED':  return 'MEDIUM';
    case 'GEOMETRIC_CENTER':    return 'LOW';
    case 'APPROXIMATE':         return 'LOW';
    default:                    return 'UNKNOWN';
  }
}

// ============================================================
// BATCH VALIDATION — Applicants_Master
// ============================================================

// Address column names in Applicants_Master
var ADDR_COL_STREET = 'Street Address';
var ADDR_COL_CITY = 'City';
var ADDR_COL_STATE = 'State';
var ADDR_COL_ZIP = 'Zip Code';
var ADDR_COL_COUNTY = 'County';
var ADDR_COL_VALIDITY = 'Address Validity';

// Rate limit delay between API calls (ms) — 200ms = 5 calls/sec, well under quota
var ADDR_API_DELAY_MS = 200;

// ScriptProperties keys for tracking chunked progress
var ADDR_PROP_NEXT_ROW = 'ADDR_VALIDATE_NEXT_ROW';
var ADDR_PROP_SUMMARY = 'ADDR_VALIDATE_SUMMARY';

// ============================================================
// COLUMN MANAGEMENT
// ============================================================

/**
 * Ensure "Address Validity" column exists right after County.
 * If it already exists in the correct position, does nothing.
 * If missing, inserts it after County.
 * v1.4.0 — No longer attempts to move an existing column; logs a warning instead.
 *
 * @returns {number} 0-based column index of Address Validity
 */
function ensureValidityColumn_() {
  var sheet = getMasterSheet();
  var lastCol = sheet.getLastColumn();
  var headers = trimHeaders(sheet.getRange(1, 1, 1, lastCol).getValues()[0]);

  var countyIdx = headers.indexOf(ADDR_COL_COUNTY);
  var validityIdx = headers.indexOf(ADDR_COL_VALIDITY);
  var targetIdx = countyIdx + 1; // 0-based, right after County

  if (countyIdx === -1) {
    throw new Error('County column not found in Applicants_Master');
  }

  // Already exists (any position) — use it where it is
  if (validityIdx !== -1) {
    return validityIdx;
  }

  // Doesn't exist — insert after County (1-based position = countyIdx + 2)
  sheet.insertColumnAfter(countyIdx + 1); // 1-based
  sheet.getRange(1, countyIdx + 2).setValue(ADDR_COL_VALIDITY);
  sheet.getRange(1, countyIdx + 2).setFontWeight('bold');
  Logger.log('Created "Address Validity" column at position ' + (countyIdx + 2) + ' (after County)');
  return countyIdx + 1; // 0-based
}

// ============================================================
// CHUNKED BATCH VALIDATION
// ============================================================

/**
 * Resolve AM column indices for address fields. Reads headers fresh each call.
 * @returns {Object} { sheet, colIdx, lastCol, lastRow, headers }
 */
function getAddrColLayout_() {
  var sheet = getMasterSheet();
  if (!sheet) throw new Error('Applicants_Master sheet not found');

  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  var headers = trimHeaders(sheet.getRange(1, 1, 1, lastCol).getValues()[0]);

  var colIdx = {
    street: headers.indexOf(ADDR_COL_STREET),
    city: headers.indexOf(ADDR_COL_CITY),
    state: headers.indexOf(ADDR_COL_STATE),
    zip: headers.indexOf(ADDR_COL_ZIP),
    county: headers.indexOf(ADDR_COL_COUNTY),
    validity: headers.indexOf(ADDR_COL_VALIDITY)
  };

  if (colIdx.street === -1 || colIdx.city === -1 || colIdx.state === -1 || colIdx.zip === -1) {
    throw new Error('Missing required address columns (need Street Address, City, State, Zip Code)');
  }
  if (colIdx.county === -1) {
    throw new Error('County column not found');
  }
  if (colIdx.validity === -1) {
    throw new Error('Address Validity column not found. Run ensureValidityColumn_() first.');
  }

  return { sheet: sheet, colIdx: colIdx, lastCol: lastCol, lastRow: lastRow, headers: headers };
}

/**
 * Start batch address validation across all AM rows.
 * Skips only rows where Address Validity is already HIGH. Rows with MEDIUM,
 * LOW, ERROR, NO_ADDRESS, UNKNOWN, or empty are re-validated.
 * Processes rows until ~5 minutes elapsed, saves progress, then stops.
 * Run continueValidation() (or menu item) to resume from where it left off.
 * v1.5.0 — Skip condition changed from any non-empty to HIGH-only.
 *
 * @returns {Object} { success, message, done, summary }
 */
function validateAllAddresses() {
  try {
    // Ensure column is in the right place
    ensureValidityColumn_();

    // Initialize progress tracking
    var props = PropertiesService.getScriptProperties();
    props.setProperty(ADDR_PROP_NEXT_ROW, '2'); // start at first data row
    props.setProperty(ADDR_PROP_SUMMARY, JSON.stringify({
      validated: 0, skipped: 0, high: 0, medium: 0, low: 0, errors: 0
    }));

    Logger.log('Starting address validation (skip existing values)...');

    // Process rows until time runs out
    return processValidationRows_();

  } catch (e) {
    Logger.log('validateAllAddresses error: ' + e.message);
    return { success: false, error: 'Batch validation failed: ' + e.message };
  }
}

/**
 * Continue a previously started batch validation from where it left off.
 * Just run this repeatedly until it reports done=true.
 *
 * @returns {Object} { success, message, done, summary }
 */
function continueValidation() {
  var props = PropertiesService.getScriptProperties();
  var nextRow = props.getProperty(ADDR_PROP_NEXT_ROW);
  if (!nextRow || parseInt(nextRow) < 2) {
    return { success: true, message: 'No validation in progress. Run validateAllAddresses() first.', done: true };
  }
  Logger.log('Continuing validation from row ' + nextRow + '...');
  return processValidationRows_();
}

/**
 * Core processing loop — runs until ~5 minutes elapsed or all rows done.
 * Reads/writes progress via ScriptProperties between runs.
 * Skips only rows where Address Validity is already HIGH (v1.5.0).
 * Flushes write buffer every 50 API calls for reliability.
 * v1.5.0 — Skip condition changed from any non-empty to HIGH-only.
 *
 * @returns {Object} { success, message, done, summary }
 */
function processValidationRows_() {
  var MAX_RUN_MS = 5 * 60 * 1000; // 5 minutes (safe under 6-minute GAS limit)
  var FLUSH_INTERVAL = 50;         // flush writes every 50 API calls
  var startTime = Date.now();

  try {
    var props = PropertiesService.getScriptProperties();
    var nextRow = parseInt(props.getProperty(ADDR_PROP_NEXT_ROW) || '0');
    var summary = JSON.parse(props.getProperty(ADDR_PROP_SUMMARY) || '{}');

    if (!nextRow || nextRow < 2) {
      return { success: true, message: 'No validation in progress', done: true };
    }

    var layout = getAddrColLayout_();
    var sheet = layout.sheet;
    var colIdx = layout.colIdx;
    var lastRow = layout.lastRow;
    var totalRows = lastRow - 1;

    if (nextRow > lastRow) {
      return finishValidation_(props, summary, totalRows);
    }

    // Read remaining rows from nextRow to end
    var remainingCount = lastRow - nextRow + 1;
    var data = sheet.getRange(nextRow, 1, remainingCount, layout.lastCol).getValues();
    var writeBuffer = [];
    var apiCallsInBatch = 0;
    var lastProcessedRow = nextRow - 1;

    Logger.log('Processing from row ' + nextRow + ', ' + remainingCount + ' rows remaining');

    for (var i = 0; i < data.length; i++) {
      // Time check — stop if approaching limit
      if (Date.now() - startTime > MAX_RUN_MS) {
        Logger.log('Time limit approaching at row ' + (nextRow + i) + ', saving progress...');
        break;
      }

      var row = data[i];
      var sheetRow = nextRow + i;
      lastProcessedRow = sheetRow;

      // v1.5.0: Skip only rows already marked HIGH; re-validate all others
      var existingValidity = (row[colIdx.validity] || '').toString().trim();
      if (existingValidity === 'HIGH') {
        summary.skipped++;
        continue;
      }

      var street = (row[colIdx.street] || '').toString().trim();
      var city = (row[colIdx.city] || '').toString().trim();
      var state = (row[colIdx.state] || '').toString().trim();
      var zip = (row[colIdx.zip] || '').toString().trim();

      // Skip empty address rows
      if (!street && !city && !state && !zip) {
        writeBuffer.push({ row: sheetRow, col: colIdx.validity, val: 'NO_ADDRESS' });
        summary.skipped++;
        continue;
      }

      // Call Geocoding API
      var result = verifyAddress({ street: street, city: city, state: state, zip: zip });
      apiCallsInBatch++;

      if (!result.success) {
        writeBuffer.push({ row: sheetRow, col: colIdx.validity, val: 'ERROR: ' + (result.error || 'Unknown').substring(0, 80) });
        summary.errors++;
        summary.validated++;
      } else {
        var confidence = result.confidence || 'UNKNOWN';
        writeBuffer.push({ row: sheetRow, col: colIdx.validity, val: confidence });

        if (confidence === 'HIGH' || confidence === 'MEDIUM') {
          var v = result.verified;
          if (v.street)    writeBuffer.push({ row: sheetRow, col: colIdx.street, val: v.street });
          if (v.city)      writeBuffer.push({ row: sheetRow, col: colIdx.city, val: v.city });
          if (v.stateLong) writeBuffer.push({ row: sheetRow, col: colIdx.state, val: v.stateLong });
          if (v.zip)       writeBuffer.push({ row: sheetRow, col: colIdx.zip, val: v.zip });
          if (v.county)    writeBuffer.push({ row: sheetRow, col: colIdx.county, val: v.county });

          if (confidence === 'HIGH') summary.high++;
          else summary.medium++;
        } else {
          summary.low++;
        }
        summary.validated++;
      }

      Utilities.sleep(ADDR_API_DELAY_MS);

      // Periodic flush
      if (apiCallsInBatch % FLUSH_INTERVAL === 0 && writeBuffer.length > 0) {
        flushWriteBuffer_(sheet, writeBuffer);
        writeBuffer = [];
        Logger.log('Progress: row ' + sheetRow + '/' + lastRow +
          ' | validated: ' + summary.validated + ' | skipped: ' + summary.skipped);
      }
    }

    // Final flush for this run
    if (writeBuffer.length > 0) {
      flushWriteBuffer_(sheet, writeBuffer);
    }

    // Save progress
    var newNextRow = lastProcessedRow + 1;

    if (newNextRow > lastRow) {
      return finishValidation_(props, summary, totalRows);
    }

    // More rows remain — save state for continueValidation()
    props.setProperty(ADDR_PROP_NEXT_ROW, newNextRow.toString());
    props.setProperty(ADDR_PROP_SUMMARY, JSON.stringify(summary));

    var elapsed = Math.round((Date.now() - startTime) / 1000);
    var progressMsg = 'Paused at row ' + newNextRow + ' of ' + lastRow +
      ' (' + elapsed + 's elapsed). Run continueValidation() to resume.' +
      ' | validated: ' + summary.validated + ', skipped: ' + summary.skipped +
      ', H:' + summary.high + ' M:' + summary.medium + ' L:' + summary.low + ' E:' + summary.errors;
    Logger.log(progressMsg);
    return { success: true, message: progressMsg, done: false, summary: summary };

  } catch (e) {
    Logger.log('processValidationRows_ error: ' + e.message);
    return { success: false, error: 'Processing failed: ' + e.message };
  }
}

/**
 * Finalize a completed validation run. Clears progress props and logs audit.
 * @param {PropertiesService.Properties} props - Script properties
 * @param {Object} summary - Running totals
 * @param {number} totalRows - Total data rows in AM
 * @returns {Object} { success, message, done, summary }
 */
function finishValidation_(props, summary, totalRows) {
  props.deleteProperty(ADDR_PROP_NEXT_ROW);
  props.deleteProperty(ADDR_PROP_SUMMARY);
  summary.total = totalRows;
  var doneMsg = 'Validation complete! ' + summary.validated + ' validated, ' +
    summary.skipped + ' skipped, H:' + summary.high + ' M:' + summary.medium +
    ' L:' + summary.low + ' E:' + summary.errors;
  Logger.log(doneMsg);
  logAudit('ADDR_VALIDATE', null, doneMsg);
  return { success: true, message: doneMsg, done: true, summary: summary };
}

/**
 * Stop any in-progress batch validation.
 * Clears progress tracking properties.
 *
 * @returns {Object} { success, message, lastSummary }
 */
function stopValidation() {
  var props = PropertiesService.getScriptProperties();
  var summary = props.getProperty(ADDR_PROP_SUMMARY);
  var nextRow = props.getProperty(ADDR_PROP_NEXT_ROW);

  props.deleteProperty(ADDR_PROP_NEXT_ROW);
  props.deleteProperty(ADDR_PROP_SUMMARY);

  var msg = nextRow ? 'Stopped validation at row ' + nextRow : 'No validation was in progress';
  Logger.log(msg);
  return { success: true, message: msg, lastSummary: summary ? JSON.parse(summary) : null };
}

/**
 * Flush buffered cell writes to the sheet.
 * @param {Sheet} sheet - The AM sheet
 * @param {Array} buffer - Array of { row, col, val } objects (col is 0-based)
 */
function flushWriteBuffer_(sheet, buffer) {
  for (var i = 0; i < buffer.length; i++) {
    var w = buffer[i];
    sheet.getRange(w.row, w.col + 1).setValue(w.val);
  }
  SpreadsheetApp.flush();
}

// ============================================================
// SINGLE ROW VALIDATION
// ============================================================

/**
 * Validate a single row in Applicants_Master by row number (1-based, data starts at 2).
 * HIGH/MEDIUM: updates address fields + county, writes full state name.
 * LOW/ERROR: writes confidence only, leaves address untouched.
 * Used by AI intake portal for individual record validation.
 *
 * @param {number} rowNum - 1-based sheet row number (minimum 2 for first data row)
 * @returns {Object} { success, confidence, updated, verified }
 */
function validateAddressRow(rowNum) {
  try {
    if (!rowNum || rowNum < 2) return { success: false, error: 'Invalid row number (must be >= 2)' };

    var layout = getAddrColLayout_();
    var sheet = layout.sheet;
    var colIdx = layout.colIdx;

    var row = sheet.getRange(rowNum, 1, 1, layout.lastCol).getValues()[0];
    var street = (row[colIdx.street] || '').toString().trim();
    var city = (row[colIdx.city] || '').toString().trim();
    var state = (row[colIdx.state] || '').toString().trim();
    var zip = (row[colIdx.zip] || '').toString().trim();

    if (!street && !city && !state && !zip) {
      sheet.getRange(rowNum, colIdx.validity + 1).setValue('NO_ADDRESS');
      return { success: true, confidence: 'NO_ADDRESS', updated: false };
    }

    var result = verifyAddress({ street: street, city: city, state: state, zip: zip });

    if (!result.success) {
      sheet.getRange(rowNum, colIdx.validity + 1).setValue('ERROR: ' + (result.error || 'Unknown').substring(0, 80));
      return { success: false, error: result.error, confidence: 'ERROR', updated: false };
    }

    var confidence = result.confidence || 'UNKNOWN';
    sheet.getRange(rowNum, colIdx.validity + 1).setValue(confidence);

    var updated = false;
    if (confidence === 'HIGH' || confidence === 'MEDIUM') {
      var v = result.verified;
      if (v.street)    sheet.getRange(rowNum, colIdx.street + 1).setValue(v.street);
      if (v.city)      sheet.getRange(rowNum, colIdx.city + 1).setValue(v.city);
      if (v.stateLong) sheet.getRange(rowNum, colIdx.state + 1).setValue(v.stateLong);
      if (v.zip)       sheet.getRange(rowNum, colIdx.zip + 1).setValue(v.zip);
      if (v.county)    sheet.getRange(rowNum, colIdx.county + 1).setValue(v.county);
      updated = true;
    }

    return { success: true, confidence: confidence, updated: updated, verified: result.verified };

  } catch (e) {
    return { success: false, error: 'Row validation failed: ' + e.message };
  }
}

// ============================================================
// STATUS & SUMMARY
// ============================================================

/**
 * Menu wrapper: starts address batch validation (skips HIGH rows).
 * Called from G2N Management > Setup > Validate Addresses (Skip HIGH).
 * Shows an alert with the result / progress.
 * v1.5.0 — Initial creation.
 */
function validateAddressesMenu() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.toast('Starting address validation (skipping rows already marked HIGH)...', 'Address Validation', 5);
  var result = validateAllAddresses();
  if (!result.success) {
    SpreadsheetApp.getUi().alert('Address Validation Error', result.error || 'Unknown error', SpreadsheetApp.getUi().ButtonSet.OK);
    return;
  }
  var s = result.summary || {};
  var msg = result.done
    ? 'Validation complete!\n'
    : 'Batch complete — more rows remain. Run Continue Address Validation to resume.\n';
  msg += 'Validated: ' + (s.validated || 0)
       + '  |  Skipped (HIGH): ' + (s.skipped || 0)
       + '\nHIGH: ' + (s.high || 0)
       + '  MEDIUM: ' + (s.medium || 0)
       + '  LOW: ' + (s.low || 0)
       + '  Errors: ' + (s.errors || 0);
  SpreadsheetApp.getUi().alert('Address Validation', msg, SpreadsheetApp.getUi().ButtonSet.OK);
}

/**
 * Menu wrapper: continues a previously started batch validation.
 * Called from G2N Management > Setup > Continue Address Validation.
 * v1.5.0 — Initial creation.
 */
function continueAddressValidationMenu() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.toast('Continuing address validation...', 'Address Validation', 5);
  var result = continueValidation();
  if (!result.success) {
    SpreadsheetApp.getUi().alert('Address Validation Error', result.error || result.message || 'Unknown error', SpreadsheetApp.getUi().ButtonSet.OK);
    return;
  }
  var s = result.summary || {};
  var msg = result.done
    ? 'Validation complete!\n'
    : 'Batch complete — more rows remain. Run Continue Address Validation again.\n';
  msg += 'Validated: ' + (s.validated || 0)
       + '  |  Skipped (HIGH): ' + (s.skipped || 0)
       + '\nHIGH: ' + (s.high || 0)
       + '  MEDIUM: ' + (s.medium || 0)
       + '  LOW: ' + (s.low || 0)
       + '  Errors: ' + (s.errors || 0);
  SpreadsheetApp.getUi().alert('Address Validation', msg, SpreadsheetApp.getUi().ButtonSet.OK);
}

/**
 * Get a summary of current Address Validity values in AM.
 * Quick status check — no API calls, just reads existing column.
 *
 * @returns {Object} { success, counts: { HIGH, MEDIUM, LOW, ERROR, NO_ADDRESS, empty, total } }
 */
function getValidationSummary() {
  try {
    var sheet = getMasterSheet();
    if (!sheet) return { success: false, error: 'Applicants_Master sheet not found' };

    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();
    if (lastRow < 2) return { success: true, counts: { total: 0 } };

    var headers = trimHeaders(sheet.getRange(1, 1, 1, lastCol).getValues()[0]);
    var validityIdx = headers.indexOf(ADDR_COL_VALIDITY);

    if (validityIdx === -1) {
      return { success: true, counts: { total: lastRow - 1, empty: lastRow - 1, note: 'Address Validity column does not exist yet' } };
    }

    // Also check if a batch is currently running
    var inProgress = PropertiesService.getScriptProperties().getProperty(ADDR_PROP_NEXT_ROW);

    var values = sheet.getRange(2, validityIdx + 1, lastRow - 1, 1).getValues();
    var counts = { HIGH: 0, MEDIUM: 0, LOW: 0, ERROR: 0, NO_ADDRESS: 0, UNKNOWN: 0, empty: 0, total: values.length };

    for (var i = 0; i < values.length; i++) {
      var val = (values[i][0] || '').toString().trim();
      if (!val) counts.empty++;
      else if (val === 'HIGH') counts.HIGH++;
      else if (val === 'MEDIUM') counts.MEDIUM++;
      else if (val === 'LOW') counts.LOW++;
      else if (val === 'NO_ADDRESS') counts.NO_ADDRESS++;
      else if (val === 'UNKNOWN') counts.UNKNOWN++;
      else if (val.indexOf('ERROR') === 0) counts.ERROR++;
      else counts.UNKNOWN++;
    }

    if (inProgress) {
      counts.batchInProgress = true;
      counts.batchNextRow = parseInt(inProgress);
    }

    return { success: true, counts: counts };

  } catch (e) {
    return { success: false, error: 'Summary failed: ' + e.message };
  }
}
