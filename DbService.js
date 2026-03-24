/**
 * DbService.gs
 * HTTP client for the G2N Bridge Cloud Run API.
 * Provides typed wrappers for every bridge endpoint with return values
 * shaped to match what the existing GAS service functions already return,
 * so callers need only a 1-line flag check at the top of each function.
 *
 * Usage pattern in any service function:
 *   if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.xxx(...);
 *
 * Authentication: uses ScriptApp.getIdentityToken() which Cloud Run IAM
 * validates automatically — no API key or secret needed in GAS.
 *
 * Field map conversion: MySQL rows use snake_case db_field_name keys.
 * Portal code expects raw Applicants_Master header names (portal_label).
 * DbService loads lu_field_map once per hour and converts automatically.
 *
 * v1.0 - Initial implementation
 * v1.1 - toPortalRecord_() now falls back to snakeCaseToLabel_() for DB columns
 *         absent from the lu_field_map reverse map, so fields not yet synced to
 *         lu_field_map (e.g. Scheduled Box Code 3, Received Product Code 3) still
 *         arrive at the portal keyed by their raw AM header instead of snake_case.
 * v1.2 - Replaced OIDC identity token auth with X-Api-Key header.
 *         Cloud Run must allow unauthenticated invocations; the key in
 *         CONFIG.BRIDGE_API_KEY (GAS) / Bridge:ApiKey (appsettings.json)
 *         enforces access at the application layer. Removes Cloud Run IAM
 *         dependency that caused HTTP 401 rejections for GAS callers.
 * v1.3 - Fixed authenticateUser() and getStaffRecords() snake_case column mapping.
 *         Bridge returns staff_code/staff_name/staff_role/staff_email but code was
 *         reading data.code/data.name/data.role (undefined), causing setProperty()
 *         to throw "Invalid argument: value" on SV portal login.
 * v1.4 - Added getRequestFrequencyData() and getGrantSummaryStats() for MySQL-offloaded
 *         grants report generation. Calls /reports/frequency (sp_get_request_frequency_data)
 *         and /reports/grant-summary (sp_get_grant_summary_stats) respectively.
 * v1.5 - Fixed snakeCaseToLabel_() — added explicit alias map for all known column
 *         renames (signature_date→'Request Date', request_type→'Request Type', etc.)
 *         so toPortalRecord_() maps DB rows to correct AM header keys when columns
 *         are absent from lu_field_map reverse map.
 *         Fixed submitIntakeForm() — service_how renamed to request_type in MySQL
 *         schema; now sets payload.request_type, deletes payload.service_how.
 * v1.6 - Option C cache-bust: invalidateFieldMapCache() now also calls
 *         DELETE /lookups/field-map/cache on the bridge, eliminating the 1-hour
 *         TTL lag when admin changes lu_field_map. LookupService.gs v4.9 calls
 *         this automatically from invalidateLookupCache() when USE_MYSQL=true.
 * v1.7 - submitIntakeForm(): serialize array values (e.g. assistanceReceiving
 *         checkboxes) to comma-separated strings before sending to bridge.
 *         Previously sent as JSON array ["NONE"] instead of plain string "NONE".
 * v1.8 - toPortalRecord_(): also stores each DB value under its original snake_case
 *         key so SV/AI code that uses rec['signature_date'], rec['request_type'] etc.
 *         as fallbacks always finds a value regardless of lu_field_map raw_header
 *         naming. Also explicitly maps 'id' → 'ID' and 'record_id' → 'ID' (fallback)
 *         so SV rec['ID'] is always populated — required for launchProductPortal()
 *         and the hidden recordId field that drives PP and save operations.
 * v1.9 - getProductsForRecord(): bridge /products/{id} now returns a full object
 *         { found, mode, products[], recordId, requestDate } (ProductService.cs v2.2+).
 *         Previous GAS code tried to .map() the response as a plain array → threw
 *         → fell through to Sheets path. Now unpacks the object correctly.
 *         Products now include id, applicantId, source for edit-mode routing.
 */

// eslint-disable-next-line no-unused-vars
var DbService = (function () {

  var FIELD_MAP_CACHE_KEY = 'G2N_DB_FIELD_MAP_REV'; // db_field_name → portal_label
  var FIELD_MAP_CACHE_TTL = 3600;

  // ── Core HTTP ────────────────────────────────────────────────────────────

  /**
   * Call the bridge. Throws on HTTP error or { success: false }.
   * Authenticates via X-Api-Key header matching Bridge:ApiKey in appsettings.json.
   * Cloud Run must be set to allow unauthenticated invocations; the API key
   * enforces access at the application layer instead of Cloud Run IAM.
   * @param {string} method  GET | POST | PUT | PATCH
   * @param {string} path    e.g. '/applicants/search'
   * @param {Object} [body]  JSON body for non-GET requests
   * @returns {*} data field from { success: true, data: ... }
   */
  function call_(method, path, body) {
    var url    = CONFIG.CLOUD_RUN_URL + path;
    var apiKey = CONFIG.BRIDGE_API_KEY || '';
    var options = {
      method: method.toLowerCase(),
      headers: {
        'X-Api-Key':    apiKey,
        'Content-Type': 'application/json'
      },
      muteHttpExceptions: true
    };
    if (body !== undefined && body !== null && method !== 'GET') {
      options.payload = JSON.stringify(body);
    }

    var response;
    try {
      response = UrlFetchApp.fetch(url, options);
    } catch (e) {
      throw new Error('Bridge network error: ' + e.message);
    }

    var code = response.getResponseCode();
    var json;
    try {
      json = JSON.parse(response.getContentText());
    } catch (e) {
      throw new Error('Bridge returned non-JSON (HTTP ' + code + ')');
    }

    if (!json.success) {
      throw new Error(json.error || 'Bridge error (HTTP ' + code + ')');
    }
    return json.data;
  }

  // ── Field map conversion ─────────────────────────────────────────────────

  /** Returns reverse field map: { db_field_name → portal_label } */
  function getRevFieldMap_() {
    var cache = CacheService.getScriptCache();
    var cached = cache.get(FIELD_MAP_CACHE_KEY);
    if (cached) {
      try { return JSON.parse(cached); } catch (e) { /* fall through */ }
    }
    // /lookups/field-map returns { portal_label: db_field_name }
    var fwdMap = call_('GET', '/lookups/field-map');
    var rev = {};
    for (var label in fwdMap) {
      var col = fwdMap[label];
      if (col) rev[col] = label;
    }
    try { cache.put(FIELD_MAP_CACHE_KEY, JSON.stringify(rev), FIELD_MAP_CACHE_TTL); } catch (e) { /* non-fatal */ }
    return rev;
  }

  /**
   * Convert a snake_case DB column name to the raw AM header / portal label.
   * Handles known column renames that don't follow simple Title Case conversion,
   * then falls back to generic Title Case with spaces.
   * Used as a fallback in toPortalRecord_() for columns absent from lu_field_map.
   * @private
   * @param {string} col - snake_case column name
   * @returns {string} Portal-compatible label
   */
  function snakeCaseToLabel_(col) {
    // Explicit aliases for columns whose DB name differs from the AM header
    var ALIASES = {
      'signature_date':       'Request Date',
      'request_type':         'Request Type',
      'racial_category':      'Please Select Your Racial Category',
      'ethnic_category':      'Please Select Your Ethnic Category',
      'annual_income':        'Total annual household income. (How much money does your family make in 1 year?)',
      'assistance_receiving': 'Are you receiving any assistance? Please select ALL boxes that apply to ANY ASSISTANCE your family is receiving.',
      'used_services_before': 'Have you used our services before?',
      'take_baby_box':        'Take Baby Box?',
      'outdoor_event':        'Are you completing this form at a Giving to the Nations OUTDOOR event?',
      'currently_homeless':   'Are you currently homeless?',
      'currently_employed':   'Are you currently employed?',
      'street_address':       'Street Address',
      'zip_code':             'Zip Code',
      'generic_distrib_code': 'Scheduled Distribution Code',
      'sched_distrib_code':   'Scheduled Distribution Code',
      'sched_box_code1':      'Scheduled Box Code 1',
      'sched_box_code2':      'Scheduled Box Code 2',
      'sched_box_code3':      'Scheduled Box Code 3',
      'received_prod_code1':  'Received Product Code 1',
      'received_prod_code2':  'Received Product Code 2',
      'received_prod_code3':  'Received Product Code 3',
      'distrib_start_date':   'Distribution Start Date',
      'funding_code':         'Funding Code',
      'generic_distrib_code': 'Distribution Code',
      'date_picked_up':       'Date Picked Up',
      'last_date_served':     'Last Date Served',
      'next_serv_avail_date': 'Next Service Availability Date',
      'first_serv_contact_date': 'First Service Contact Date',
      'final_serv_contact_date': 'Final Service Contact Date',
      'admin_notes':          'Admin Notes',
      'staff_notes':          'Staff Notes',
      'time_stamp':           'Timestamp',
      'original_form_id':     'Original Form ID',
      'phone_number':         'Phone Number',
      'phone_type':           'Phone Type',
      'address_type':         'Address Type',
      'address_validity':     'Address Validity',
      'military_status':      'Military Status',
      'household_size':       'Household Size',
      'approval_date':        'Approval Date',
      'approved_by':          'Approved By',
      'entered_by':           'Entered By',
      'service_status':       'Service Status',
      'funding_source':       'Funding Source'
    };
    if (ALIASES[col]) return ALIASES[col];
    // Generic fallback: snake_case → Title Case With Spaces
    return col.split('_').map(function(w) {
      if (!w) return '';
      return w.charAt(0).toUpperCase() + w.slice(1);
    }).join(' ').trim();
  }

  /**
   * Convert a DB row (snake_case keys) to portal format (raw AM header keys).
   * Uses reverse lu_field_map for known columns; falls back to snakeCaseToLabel_()
   * for columns absent from the map so stale lu_field_map data does not silently
   * drop fields from the portal record.
   * @private
   * @param {Object} dbRow - Raw DB row with snake_case column keys
   * @returns {Object} Portal record keyed by raw AM header names
   */
  function toPortalRecord_(dbRow) {
    if (!dbRow) return null;
    var rev = getRevFieldMap_();
    var out = {};
    for (var col in dbRow) {
      var label = rev[col] || snakeCaseToLabel_(col);
      out[label] = dbRow[col];
      // v1.8: also keep original snake_case key as fallback so portal code using
      // rec['signature_date'], rec['request_type'] etc. always finds the value
      // regardless of lu_field_map raw_header naming.
      if (label !== col) out[col] = dbRow[col];
    }
    // v1.8: explicitly set 'ID' from record_id (G2N-format) or id (integer).
    // SV uses rec['ID'] for the hidden recordId field that drives launchProductPortal()
    // and saveRecord(). snakeCaseToLabel_('id') produces 'Id' not 'ID', causing
    // rec['ID'] to be undefined → PP receives undefined recordId → applicantId
    // lookup fails → all products silently skipped.
    if (!out['ID']) {
      out['ID'] = dbRow['record_id'] || dbRow['id'] || '';
    }
    return out;
  }

  /**
   * Convert an array of DB rows to the { record, rowIndex } shape used by
   * getRecentRecords / getRecordsByStatus / getRecordsByDistribCode.
   * rowIndex is set to the record_id string so updateRequest() can extract it.
   */
  function toPortalResults_(rows) {
    return (rows || []).map(function (row) {
      return {
        record: toPortalRecord_(row),
        rowIndex: row.record_id || null
      };
    });
  }

  // ── Auth / Staff ─────────────────────────────────────────────────────────

  /**
   * Mirrors authenticateUser(code) return shape:
   *   { success, user: { code, name, role, isAdmin } }
   */
  function authenticateUser(code) {
    try {
      var data = call_('POST', '/auth/login', { code: code });
      if (!data) return { success: false, error: 'Invalid staff code' };
      // Bridge returns snake_case column names: staff_code, staff_name, staff_role
      var userCode = data.staff_code || data.code || '';
      var userName = data.staff_name || data.name || '';
      var userRole = data.staff_role || data.role || 'Staff';
      return {
        success: true,
        user: {
          code:    userCode,
          name:    userName,
          role:    userRole,
          isAdmin: userRole === 'Administrator'
        }
      };
    } catch (e) {
      return { success: false, error: e.message };
    }
  }

  /**
   * Mirrors getStaffRecords(includeInactive) return shape:
   *   { success, records: [{ Code, Name, Role, Email, Active }] }
   */
  function getStaffRecords(includeInactive) {
    try {
      var rows = call_('GET', '/auth/staff?includeInactive=' + (includeInactive ? 'true' : 'false'));
      return {
        success: true,
        records: (rows || []).map(function (r) {
          return {
            Code:   r.staff_code  || r.code  || '',
            Name:   r.staff_name  || r.name  || '',
            Role:   r.staff_role  || r.role  || 'Staff',
            Email:  r.staff_email || r.email || '',
            Active: r.active === 1 || r.active === true || r.active === '1'
          };
        })
      };
    } catch (e) {
      return { success: false, error: e.message, records: [] };
    }
  }

  function saveStaffRecords(updates, newRecords) {
    try {
      call_('PUT', '/auth/staff', { updates: updates, newRecords: newRecords });
      return { success: true };
    } catch (e) {
      return { success: false, error: e.message };
    }
  }

  // ── Search ───────────────────────────────────────────────────────────────

  /**
   * Mirrors searchRecords(criteria) return shapes:
   *   Single: { success, record, rowIndex }
   *   Multi:  { success, results: [{ record, rowIndex }] }
   *   Error:  { success: false, notFound, error }
   *
   * rowIndex is set to record_id (G2N-XXXXX) so updateRequest() works.
   */
  function searchRecords(criteria) {
    try {
      var searchType = criteria.searchType || 'id';

      if (searchType === 'id' && criteria.id) {
        // Pad to G2N-NNNNN format
        var paddedId = 'G2N-' + String(criteria.id).padStart(5, '0');
        var row = call_('GET', '/applicants/' + paddedId);
        if (!row) return { success: false, notFound: true, error: 'Record not found' };
        return { success: true, record: toPortalRecord_(row), rowIndex: row.record_id };
      }

      if (searchType === 'formId' && criteria.formId) {
        var results = call_('POST', '/applicants/search', { formId: criteria.formId });
        if (!results || results.length === 0)
          return { success: false, notFound: true, error: 'Form ID not found' };
        return { success: true, record: toPortalRecord_(results[0]), rowIndex: results[0].record_id };
      }

      if (searchType === 'nameDate') {
        var rows = call_('POST', '/applicants/search', {
          firstName: criteria.firstName,
          lastName:  criteria.lastName,
          date:      criteria.date
        });
        if (!rows || rows.length === 0)
          return { success: false, notFound: true, error: 'No matching records found' };
        var portalResults = toPortalResults_(rows);
        if (portalResults.length === 1)
          return { success: true, record: portalResults[0].record, rowIndex: portalResults[0].rowIndex };
        return { success: true, results: portalResults };
      }

      return { success: false, error: 'Invalid search criteria' };
    } catch (e) {
      return { success: false, error: 'Search failed: ' + e.message };
    }
  }

  function getRecentRecords(limit) {
    try {
      var rows = call_('GET', '/applicants/recent?limit=' + (limit || 10));
      return toPortalResults_(rows);
    } catch (e) {
      Logger.log('DbService.getRecentRecords error: ' + e.message);
      return [];
    }
  }

  function getRecordsByStatus(status) {
    try {
      var rows = call_('GET', '/applicants/by-status/' + encodeURIComponent(status));
      return toPortalResults_(rows);
    } catch (e) {
      Logger.log('DbService.getRecordsByStatus error: ' + e.message);
      return [];
    }
  }

  function getRecordsByDistribCode(distribCode) {
    try {
      var rows = call_('GET', '/applicants/by-distrib/' + encodeURIComponent(distribCode));
      return toPortalResults_(rows);
    } catch (e) {
      Logger.log('DbService.getRecordsByDistribCode error: ' + e.message);
      return [];
    }
  }

  // ── Record create / update ───────────────────────────────────────────────

  /**
   * Mirrors createNewRequest(formData) return shape:
   *   { success, recordId, message }
   */
  function createNewRequest(formData) {
    try {
      var recordId = call_('POST', '/applicants', formData);
      if (typeof recordId === 'object' && recordId.recordId) recordId = recordId.recordId;
      return { success: true, recordId: recordId, message: 'New record created: ' + recordId };
    } catch (e) {
      return { success: false, error: 'Create failed: ' + e.message };
    }
  }

  /**
   * Mirrors updateRequest(rowIndex, formData) return shape:
   *   { success, changesCount, message }
   *
   * rowIndex should be the record_id (G2N-XXXXX) — set automatically
   * by searchRecords() MySQL path. formData keys are portal_labels;
   * the bridge accepts both portal_label and snake_case keys.
   */
  function updateRequest(rowIndex, formData, userCode) {
    try {
      // rowIndex in MySQL path is set to record_id by searchRecords()
      var recordId = rowIndex;
      if (!recordId || typeof recordId !== 'string' || !recordId.startsWith('G2N-')) {
        // Try to find it in formData
        recordId = formData['Record ID'] || formData['record_id'] || formData['recordId'];
      }
      if (!recordId) return { success: false, error: 'No record ID found for MySQL update' };

      var headers = { 'X-User-Code': userCode || '' };
      var result = call_('PATCH', '/applicants/' + recordId, formData);
      return {
        success: true,
        message: 'Record ' + recordId + ' updated',
        changesCount: (result && result.rowsUpdated) || 0
      };
    } catch (e) {
      return { success: false, error: 'Update failed: ' + e.message };
    }
  }

  /**
   * Intake form submission — mirrors submitIntakeForm() return shape:
   *   { success, recordId, message }
   */
  function submitIntakeForm(formData, eventInfo) {
    try {
      // Merge eventInfo into formData fields the bridge understands.
      // Column is request_type in MySQL (was service_how in old schema).
      var payload = Object.assign({}, formData);

      // Serialize any array values (e.g. assistanceReceiving checkboxes) to
      // comma-separated strings — the bridge expects plain strings, not JSON arrays.
      for (var key in payload) {
        if (Array.isArray(payload[key])) {
          payload[key] = payload[key].join(', ');
        }
      }

      if (eventInfo && eventInfo.isActive) {
        payload.service_status = 'Picked Up';
        payload.request_type   = 'Event';       // DB column: request_type (not service_how)
        payload.box_code       = eventInfo.boxCode       || '';
        payload.funding_source = eventInfo.fundingSource || '';
        payload.outdoor_event  = 'Yes';
        payload.entered_by     = payload.entered_by || 'Applicant';
      } else {
        payload.request_type = payload.request_type || payload.service_how || 'Direct';
        payload.entered_by   = payload.entered_by || 'Applicant';
      }
      // Remove old key to avoid duplicate field confusion in bridge
      delete payload.service_how;

      var result = call_('POST', '/applicants', payload);
      var recordId = (result && result.recordId) ? result.recordId : result;
      return {
        success:  true,
        recordId: recordId,
        message:  'Application submitted successfully'
      };
    } catch (e) {
      return { success: false, error: 'Submission failed: ' + e.message };
    }
  }

  // ── Lookups ──────────────────────────────────────────────────────────────

  function getAllLookups() {
    try { return call_('GET', '/lookups/all'); }
    catch (e) { Logger.log('DbService.getAllLookups error: ' + e.message); return {}; }
  }

  function getIntakeLookups() {
    try { return call_('GET', '/lookups/intake'); }
    catch (e) { Logger.log('DbService.getIntakeLookups error: ' + e.message); return {}; }
  }

  function getEventInfoForDate(dateStr) {
    try { return call_('GET', '/lookups/event?date=' + encodeURIComponent(dateStr)); }
    catch (e) { Logger.log('DbService.getEventInfoForDate error: ' + e.message); return { isActive: false }; }
  }

  function getActiveSchedDisbCodes() {
    try { return call_('GET', '/lookups/sched-disb'); }
    catch (e) { Logger.log('DbService.getActiveSchedDisbCodes error: ' + e.message); return []; }
  }

  function getSchedDisbCodeDetails(code) {
    try { return call_('GET', '/lookups/sched-disb/' + encodeURIComponent(code)); }
    catch (e) { Logger.log('DbService.getSchedDisbCodeDetails error: ' + e.message); return null; }
  }

  function getDistribCodeTimes(distribCode) {
    try {
      var qs = distribCode ? '?code=' + encodeURIComponent(distribCode) : '';
      return call_('GET', '/lookups/distrib-times' + qs);
    } catch (e) { Logger.log('DbService.getDistribCodeTimes error: ' + e.message); return []; }
  }

  function saveSchedDisbCodes(records) {
    try { call_('PUT', '/lookups/sched-disb', { records: records }); return { success: true }; }
    catch (e) { return { success: false, error: e.message }; }
  }

  function saveEventInfoRecords(records) {
    try { call_('PUT', '/lookups/event-info', { records: records }); return { success: true }; }
    catch (e) { return { success: false, error: e.message }; }
  }

  function deactivateLookupValue(lookupKey, code) {
    try {
      call_('PATCH', '/lookups/' + lookupKey + '/' + encodeURIComponent(code) + '/deactivate', null);
      return true;
    } catch (e) { Logger.log('DbService.deactivateLookupValue error: ' + e.message); return false; }
  }

  /**
   * Mirrors getIntakeFieldMapping() return shape: { formFieldId: rawAMHeader }
   */
  function getIntakeFieldMapping() {
    try { return call_('GET', '/lookups/field-map'); }
    catch (e) { Logger.log('DbService.getIntakeFieldMapping error: ' + e.message); return {}; }
  }

  // ── Products ─────────────────────────────────────────────────────────────

  /**
   * Mirrors getProductsForRecord() return shape:
   *   { success, found, mode, products: [{id, applicantId, source, productId, productName,
   *     qtyRequested, qtyReceived}], recordId, requestDate }
   * v1.9: bridge returns full object {found,mode,products[],recordId,requestDate}.
   *   Previous code treated response as plain array → threw → fell to Sheets path.
   */
  function getProductsForRecord(recordId, requestDate) {
    try {
      var qs = requestDate ? '?date=' + encodeURIComponent(requestDate) : '';
      var result = call_('GET', '/products/' + encodeURIComponent(recordId) + qs);

      // v1.9: bridge returns full object — unpack it
      if (result && typeof result === 'object' && !Array.isArray(result)) {
        return {
          success:     true,
          found:       result.found  || false,
          mode:        result.mode   || 'new',
          products:    (result.products || []).map(function (r) {
            return {
              id:           r.id           != null ? r.id           : null,
              applicantId:  r.applicantId  != null ? r.applicantId  : null,
              source:       r.source       || '',
              productId:    r.productId    || r.product_code || '',
              productName:  r.productName  || r.product_name || '',
              qtyRequested: r.qtyRequested != null ? r.qtyRequested : '',
              qtyReceived:  r.qtyReceived  != null ? r.qtyReceived  : ''
            };
          }),
          recordId:    result.recordId    || recordId,
          requestDate: result.requestDate || requestDate
        };
      }

      // Fallback: legacy plain-array response (pre-v2.2 bridge)
      var rows = Array.isArray(result) ? result : [];
      return {
        success:     true,
        found:       rows.length > 0,
        mode:        rows.length > 0 ? 'existing' : 'new',
        products:    rows.map(function (r) {
          return {
            id: null, applicantId: null, source: '',
            productId:    r.product_code || r.productId   || '',
            productName:  r.product_name || r.productName || '',
            qtyRequested: '',
            qtyReceived:  ''
          };
        }),
        recordId:    recordId,
        requestDate: requestDate
      };
    } catch (e) {
      return { success: false, error: 'Failed to load products: ' + e.message };
    }
  }

  function addProductRecords(recordId, requestDate, products) {
    try {
      call_('POST', '/products', { recordId: recordId, requestDate: requestDate, products: products });
      return { success: true };
    } catch (e) { return { success: false, error: e.message }; }
  }

  function updateProductRecords(products) {
    try {
      call_('PATCH', '/products', { products: products });
      return { success: true };
    } catch (e) { return { success: false, error: e.message }; }
  }

  // ── Reports / Archive ────────────────────────────────────────────────────

  /**
   * Mirrors getCombinedData() return shape:
   *   { headers: [], rows: [], masterCount: n, archiveCount: n, totalCount: n }
   *
   * Filters are pushed into sp_get_report_data so MySQL does the filtering —
   * rows returned already match the report's criteria.
   *
   * Extra columns appended to every row:
   *   calc_qty_requested, calc_qty_received  — combined product totals
   *   calc_drpf_requested, calc_drpf_received — DR/PF portion only
   *   calc_box_qty1/2/3                       — per BoxCode slot
   *
   * @param {string}  fromStr          ISO date 'yyyy-MM-dd'
   * @param {string}  toStr            ISO date 'yyyy-MM-dd'
   * @param {boolean} includeArchive   true to include active=0 records
   * @param {string}  statusCsv        CSV of allowed statuses, e.g. 'Picked Up,Delivered' (null=all)
   * @param {boolean} statusIncludeNull true to also include null/empty status (for Open Requests)
   * @param {string}  countyCsv        CSV of counties (null=all)
   * @param {string}  cityCsv          CSV of cities (null=all)
   * @param {string}  zipCsv           CSV of zip codes (null=all)
   */
  function getReportData(fromStr, toStr, includeArchive,
                         statusCsv, statusIncludeNull, countyCsv, cityCsv, zipCsv) {
    try {
      var qs = '?from='           + encodeURIComponent(fromStr)
             + '&to='             + encodeURIComponent(toStr)
             + '&includeArchive=' + (includeArchive ? '1' : '0');

      if (statusCsv)         qs += '&status='             + encodeURIComponent(statusCsv);
      if (statusIncludeNull) qs += '&statusIncludeNull=1';
      if (countyCsv)         qs += '&county='             + encodeURIComponent(countyCsv);
      if (cityCsv)           qs += '&city='               + encodeURIComponent(cityCsv);
      if (zipCsv)            qs += '&zip='                + encodeURIComponent(zipCsv);

      var data = call_('GET', '/reports/data' + qs);
      var rev  = getRevFieldMap_();

      // Convert db_col headers → portal/raw AM headers
      var remappedHeaders = (data.headers || []).map(function(col) {
        // Preserve calc_* columns as-is — they're not in the field map
        if (col.indexOf('calc_') === 0) return col;
        return rev[col] || col;
      });

      return {
        headers:      remappedHeaders,
        rows:         data.rows || [],
        masterCount:  data.masterCount  || 0,
        archiveCount: data.archiveCount || 0,
        totalCount:   data.totalCount   || (data.rows || []).length
      };
    } catch (e) {
      Logger.log('DbService.getReportData error: ' + e.message);
      return { headers: [], rows: [], masterCount: 0, archiveCount: 0, totalCount: 0 };
    }
  }

  /**
   * Mirrors loadProductLookupData() return shape:
   *   { distProdByBox: { boxCode: [qty,...] },
   *     drPfByIdDate:  { 'id|date': { qtyRequested, qtyReceived } } }
   *
   * @param {string} fromStr  ISO date — scopes drpf query to applicants in range
   * @param {string} toStr    ISO date
   */
  function getProductLookupData(fromStr, toStr) {
    try {
      var qs = (fromStr && toStr)
        ? '?from=' + encodeURIComponent(fromStr) + '&to=' + encodeURIComponent(toStr)
        : '';
      return call_('GET', '/reports/products' + qs);
    } catch (e) {
      Logger.log('DbService.getProductLookupData error: ' + e.message);
      return { distProdByBox: {}, drPfByIdDate: {} };
    }
  }

  /**
   * Calls /reports/frequency which invokes sp_get_request_frequency_data.
   * Returns pre-grouped rows with LAG-computed avg_freq_days — eliminates the
   * multi-pass GAS grouping loop in generateRequestFrequencyReport().
   * Shape: { headers: [], rows: [[...]], totalCount: n }
   * @param {string} fromStr - ISO date 'yyyy-MM-dd'
   * @param {string} toStr   - ISO date 'yyyy-MM-dd'
   * @returns {Object} { headers, rows, totalCount }
   */
  function getRequestFrequencyData(fromStr, toStr) {
    try {
      var qs = '?from=' + encodeURIComponent(fromStr) + '&to=' + encodeURIComponent(toStr);
      return call_('GET', '/reports/frequency' + qs);
    } catch (e) {
      Logger.log('DbService.getRequestFrequencyData error: ' + e.message);
      return { headers: [], rows: [], totalCount: 0 };
    }
  }

  /**
   * Calls /reports/grant-summary which invokes sp_get_grant_summary_stats.
   * Returns aggregate totals + income breakdown — eliminates the GAS aggregation
   * loop in generateGrantSummaryReport().
   * Shape: {
   *   householdCount, totalRequests, totalChildren, totalAdults, totalSeniors,
   *   totalProductsDistributed,
   *   incomeBreakdown: [{ incomeLevel, incomeCount }]
   * }
   * @param {string} fromStr    - ISO date 'yyyy-MM-dd'
   * @param {string} toStr      - ISO date 'yyyy-MM-dd'
   * @param {string} countyCsv  - CSV county filter (null = all)
   * @param {string} cityCsv    - CSV city filter (null = all)
   * @param {string} zipCsv     - CSV zip filter (null = all)
   * @returns {Object} Aggregate stats + income breakdown
   */
  function getGrantSummaryStats(fromStr, toStr, countyCsv, cityCsv, zipCsv) {
    try {
      var qs = '?from=' + encodeURIComponent(fromStr) + '&to=' + encodeURIComponent(toStr);
      if (countyCsv) qs += '&county=' + encodeURIComponent(countyCsv);
      if (cityCsv)   qs += '&city='   + encodeURIComponent(cityCsv);
      if (zipCsv)    qs += '&zip='    + encodeURIComponent(zipCsv);
      return call_('GET', '/reports/grant-summary' + qs);
    } catch (e) {
      Logger.log('DbService.getGrantSummaryStats error: ' + e.message);
      return null;
    }
  }

  // ── Cache management ─────────────────────────────────────────────────────

  /**
   * Clears the GAS-side field map cache key from CacheService, and calls
   * DELETE /lookups/field-map/cache on the bridge to signal a bust.
   * The bridge itself is stateless — the DELETE endpoint exists so future
   * server-side caching can be invalidated here too without a code change.
   * Called automatically by invalidateLookupCache() when USE_MYSQL = true.
   */
  function invalidateFieldMapCache() {
    try { CacheService.getScriptCache().remove(FIELD_MAP_CACHE_KEY); } catch (e) { /* non-fatal */ }
    // Also notify the bridge (fire-and-forget — failure is non-fatal)
    try { call_('DELETE', '/lookups/field-map/cache', null); } catch (e) {
      Logger.log('DbService.invalidateFieldMapCache bridge call (non-fatal): ' + e.message);
    }
  }

  // ── Public API ───────────────────────────────────────────────────────────

  return {
    // Core (for custom calls if needed)
    call: call_,

    // Auth / Staff
    authenticateUser:    authenticateUser,
    getStaffRecords:     getStaffRecords,
    saveStaffRecords:    saveStaffRecords,

    // Search
    searchRecords:           searchRecords,
    getRecentRecords:        getRecentRecords,
    getRecordsByStatus:      getRecordsByStatus,
    getRecordsByDistribCode: getRecordsByDistribCode,

    // Create / Update
    createNewRequest:  createNewRequest,
    updateRequest:     updateRequest,
    submitIntakeForm:  submitIntakeForm,

    // Lookups
    getAllLookups:           getAllLookups,
    getIntakeLookups:        getIntakeLookups,
    getEventInfoForDate:     getEventInfoForDate,
    getActiveSchedDisbCodes: getActiveSchedDisbCodes,
    getSchedDisbCodeDetails: getSchedDisbCodeDetails,
    getDistribCodeTimes:     getDistribCodeTimes,
    saveSchedDisbCodes:      saveSchedDisbCodes,
    saveEventInfoRecords:    saveEventInfoRecords,
    deactivateLookupValue:   deactivateLookupValue,
    getIntakeFieldMapping:   getIntakeFieldMapping,

    // Products
    getProductsForRecord: getProductsForRecord,
    addProductRecords:    addProductRecords,
    updateProductRecords: updateProductRecords,

    // Reports / Archive
    getReportData:            getReportData,
    getProductLookupData:     getProductLookupData,
    getRequestFrequencyData:  getRequestFrequencyData,
    getGrantSummaryStats:     getGrantSummaryStats,

    // Cache
    invalidateFieldMapCache: invalidateFieldMapCache
  };

})();
