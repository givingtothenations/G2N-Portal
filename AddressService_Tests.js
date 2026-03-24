/**
 * AddressService_Tests.gs
 * Test harness for AddressService.gs — run from Apps Script Editor.
 * Extracted from AddressService.gs v1.3.0 (#10).
 *
 * Version History:
 *   v1.0.0 - 2025-03-01 - Extracted from AddressService.gs.
 *            Removed testMoveValidityColumn (moveAddressValidityColumn removed).
 *            Removed testValidateBatchForce (force-revalidate mode removed).
 */

// ============================================================
// TEST FUNCTIONS — Core API Verification
// ============================================================

/**
 * TEST: Verifies the API key is accessible from Script Properties.
 * Run this first if other tests fail.
 */
function testApiKeyExists() {
  try {
    var key = getGoogleMapsApiKey_();
    Logger.log('API key found. Length: ' + key.length + ', starts with: ' + key.substring(0, 8) + '...');
  } catch (e) {
    Logger.log('ERROR: ' + e.message);
  }
}

/**
 * TEST: Verifies a known valid address.
 * Run this from the editor to confirm API key and billing are working.
 * Expected: success=true, HIGH confidence, county populated, stateLong filled.
 */
function testVerifyAddress_Valid() {
  var result = verifyAddress({
    street: '1600 Amphitheatre Parkway',
    city: 'Mountain View',
    state: 'CA',
    zip: '94043'
  });
  Logger.log('=== VALID ADDRESS TEST ===');
  Logger.log('Success: ' + result.success);
  if (result.success) {
    Logger.log('Formatted:  ' + result.verified.formattedAddress);
    Logger.log('Street:     ' + result.verified.street);
    Logger.log('City:       ' + result.verified.city);
    Logger.log('State:      ' + result.verified.state + ' / ' + result.verified.stateLong);
    Logger.log('Zip:        ' + result.verified.zip);
    Logger.log('County:     ' + result.verified.county);
    Logger.log('Confidence: ' + result.confidence);
  } else {
    Logger.log('Error: ' + result.error);
  }
}

/**
 * TEST: Verifies a misspelled address to check correction capability.
 * Expected: success=true, address corrected/standardized.
 */
function testVerifyAddress_Misspelled() {
  var result = verifyAddress({
    street: '123 Mainn Stret',
    city: 'Anytown',
    state: 'OH',
    zip: '43123'
  });
  Logger.log('=== MISSPELLED ADDRESS TEST ===');
  Logger.log('Success: ' + result.success);
  if (result.success) {
    Logger.log('Formatted:  ' + result.verified.formattedAddress);
    Logger.log('County:     ' + result.verified.county);
    Logger.log('State Long: ' + result.verified.stateLong);
    Logger.log('Confidence: ' + result.confidence);
  } else {
    Logger.log('Error: ' + result.error);
  }
}

/**
 * TEST: Sends a completely invalid address.
 * Expected: success=false or LOW confidence.
 */
function testVerifyAddress_Invalid() {
  var result = verifyAddress({
    street: 'ZZZZ Nowhere Blvd',
    city: 'Fakeville',
    state: 'XX',
    zip: '00000'
  });
  Logger.log('=== INVALID ADDRESS TEST ===');
  Logger.log('Success: ' + result.success);
  if (result.success) {
    Logger.log('Confidence: ' + result.confidence);
    Logger.log('Formatted:  ' + result.verified.formattedAddress);
  } else {
    Logger.log('Error: ' + result.error);
  }
}

/**
 * TEST: Sends an empty address.
 * Expected: success=false, "No address provided" error.
 */
function testVerifyAddress_Empty() {
  var result = verifyAddress({ street: '', city: '', state: '', zip: '' });
  Logger.log('=== EMPTY ADDRESS TEST ===');
  Logger.log('Success: ' + result.success);
  Logger.log('Error:   ' + (result.error || 'none'));
}

// ============================================================
// TEST FUNCTIONS — Batch & Status
// ============================================================

/**
 * TEST: Check current validation summary (no API calls).
 */
function testValidationSummary() {
  var result = getValidationSummary();
  Logger.log('=== VALIDATION SUMMARY ===');
  Logger.log(JSON.stringify(result, null, 2));
}

/**
 * TEST: Validate a single row (row 2 = first data row).
 */
function testValidateSingleRow() {
  var result = validateAddressRow(2);
  Logger.log('=== SINGLE ROW VALIDATION (Row 2) ===');
  Logger.log(JSON.stringify(result, null, 2));
}

/**
 * TEST: Start batch validation (skips rows that already have Address Validity).
 * Processes as many rows as possible in ~5 minutes, then stops.
 * Run testContinueValidation() to resume.
 */
function testValidateBatch() {
  Logger.log('=== BATCH VALIDATION START ===');
  var result = validateAllAddresses();
  Logger.log(JSON.stringify(result, null, 2));
}

/**
 * TEST: Continue a paused validation run.
 * Keep running this until the log shows done=true.
 */
function testContinueValidation() {
  Logger.log('=== CONTINUE VALIDATION ===');
  var result = continueValidation();
  Logger.log(JSON.stringify(result, null, 2));
}

/**
 * TEST: Stop any in-progress batch validation.
 */
function testStopValidation() {
  var result = stopValidation();
  Logger.log('=== STOP VALIDATION ===');
  Logger.log(JSON.stringify(result, null, 2));
}
