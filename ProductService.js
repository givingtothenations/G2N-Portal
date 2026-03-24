/**
 * ProductService.gs
 * Backend CRUD services for the Product Portal (ProductPortalWeb.html)
 * Manages product records in DR/PF_Products sheet within G2N_Data workbook.
 * Supports search by ID+Date composite key, edit mode for existing records,
 * new entry mode with LU_Products catalog, and date normalization.
 * v1.0
 * v1.1 - Performance: Replaced 3 direct SpreadsheetApp.openById() calls with
 *         cached getDataWorkbook(). Adopted trimHeaders() utility (replaces
 *         3 inline .map trim calls). updateProductRecords() now reads full
 *         sheet, modifies in memory, writes back in single setValues() call
 *         instead of 2 setValue() calls per product row.
 * v1.2 - normalizeProductDate() now delegates to shared normalizeDate() in
 *         SharedUtils.gs (#3). Adopted CONFIG.TIMEZONE (#8).
 * v1.3 - getProductsForRecord() now checks AM first. If ID found in AM,
 *         searches DR/PF_Products only. If ID not in AM, searches
 *         Products_Archive in all archive workbooks via
 *         getArchiveWorkbooksForRange(). Returns archived:true flag
 *         so PP can render archive records as read-only.
 *         Added isIdInMaster_() and findProductsInArchives_() helpers.
 * v1.4 - CacheService index for faster PP loads:
 *         AM IDs cached as JSON Set (5-min TTL) in PRODUCT_CACHE_KEY_AM_IDS.
 *         DR/PF_Products composite-key index cached as JSON map
 *         (id|date → row details array) in PRODUCT_CACHE_KEY_PF_INDEX (5-min TTL).
 *         invalidateProductCache_() called from addProductRecords() and
 *         updateProductRecords() to keep cache consistent after writes.
 *         normalizeProductDate() called once per row at index-build time, not
 *         on every lookup — eliminates O(n) per-row normalization on searches.
 * v1.5 - Two-phase DR/PF_Products index:
 *         PRODUCT_CACHE_KEY_PF_IDS caches just the Set of IDs present in
 *         DR/PF_Products (Phase 1, tiny payload). If ID not found, getNewProductList
 *         is called immediately — no full composite index built for brand-new records.
 *         PRODUCT_CACHE_KEY_PF_INDEX (Phase 2) built only when ID IS in PF_IDS.
 *         PRODUCT_CACHE_KEY_PRODUCTS caches LU_Products list (5-min TTL) so
 *         getNewProductList avoids a sheet read on repeated new-entry calls.
 * v1.6 - Persistent PF_IDS via ScriptProperties (no TTL expiry):
 *         PRODUCT_PROPS_PF_IDS stores the DR/PF_Products ID set in ScriptProperties
 *         so it survives cache evictions and product saves. appendPFIdToProps_()
 *         adds each newly saved ID instead of wiping the set.
 *         AM_IDS TTL raised to 6 hours (21600 s); stale AM_IDS is safe because
 *         a brand-new record not yet in AM_IDS simply falls through to new-entry mode.
 *         invalidateProductCache_() now only clears PF_INDEX and PRODUCTS from
 *         CacheService; AM_IDS and PF_IDS survive saves to avoid cold rebuilds.
 * v1.7 - addProductRecords(): strip G2N- prefix from recordId before writing to
 *         DR/PF_Products sheet ID column. Prevents "G2N-05216" being stored when
 *         the SV portal passes the MySQL-format record_id on the Sheets path.
 *         Sheet ID column stores bare integers matching AM column A format.
 * v1.8 - addProductRecords() and updateProductRecords(): moved USE_MYSQL guard
 *         outside the try/catch block. Previously a bridge error (thrown by
 *         DbService.addProductRecords → call_()) was caught by the surrounding
 *         try/catch and silently fell through to the Sheets write path, causing
 *         PP to write to DR/PF_Products Google Sheet instead of drpf_products MySQL.
 * v1.9 - getNewProductList(): filters out inactive products when LU_Products has
 *         an 'Active' column (Y/YES/TRUE/1 = active). Previously all rows were
 *         included regardless of active status.
 */

// ============ PRODUCT CACHE KEYS ============
var PRODUCT_CACHE_KEY_AM_IDS   = 'G2N_PRODUCT_AM_IDS';
var PRODUCT_CACHE_KEY_PF_INDEX = 'G2N_PRODUCT_PF_INDEX';
var PRODUCT_CACHE_KEY_PRODUCTS = 'G2N_PRODUCT_CATALOG';   // v1.5: LU_Products catalog
var PRODUCT_CACHE_TTL          = 21600; // v1.6: 6-hour TTL (was 5 min) for CacheService entries
// v1.6: PF_IDS stored in ScriptProperties (persistent, no TTL)
var PRODUCT_PROPS_PF_IDS       = 'G2N_PROD_PF_IDS';

/**
 * Searches for product records for a given ID + Request Date.
 * v1.3 - Source-aware: checks AM first. If ID is active, searches DR/PF_Products only.
 *         If ID is not in AM (archived record), searches Products_Archive in all
 *         archive workbooks. Archive results are returned with archived:true (read-only).
 * @param {string|number} recordId - The applicant record ID
 * @param {string} requestDate - Request date (YYYY-MM-DD or M/D/YYYY)
 * @returns {Object} { success, found, mode, products[], recordId, requestDate, archived? }
 */
function getProductsForRecord(recordId, requestDate) {
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.getProductsForRecord(recordId, requestDate);
  try {
    var id = (recordId || '').toString().trim();
    if (!id) {
      return { success: false, error: 'Record ID is required' };
    }

    var searchDate = normalizeProductDate(requestDate);
    if (!searchDate) {
      return { success: false, error: 'Valid Request Date is required' };
    }

    // v1.3: Determine if record is active (in AM) or archived
    if (isIdInMaster_(id)) {
      // Active record — search DR/PF_Products only
      return searchActiveDRPFProducts_(id, searchDate);
    } else {
      // Archived record — search Products_Archive in all archive workbooks
      var archiveResult = findProductsInArchives_(id, searchDate);
      if (archiveResult) return archiveResult;
      // Not found anywhere — fall back to new product list
      return getNewProductList(id, searchDate);
    }

  } catch (error) {
    Logger.log('getProductsForRecord error: ' + error.message);
    return { success: false, error: 'Failed to load products: ' + error.message };
  }
}

/**
 * Clears volatile product caches after a write to DR/PF_Products.
 * v1.4 - New helper; v1.5 - Also cleared PF_IDS and PRODUCTS keys.
 * v1.6 - No longer clears AM_IDS (safe stale; 6-h TTL) or PF_IDS
 *         (ScriptProperties; updated incrementally by appendPFIdToProps_()).
 *         Only clears PF_INDEX (composite key map) and PRODUCTS catalog.
 */
function invalidateProductCache_() {
  try {
    CacheService.getScriptCache().removeAll([
      PRODUCT_CACHE_KEY_PF_INDEX,
      PRODUCT_CACHE_KEY_PRODUCTS
    ]);
  } catch (e) {
    Logger.log('invalidateProductCache_ error (non-fatal): ' + e.message);
  }
}

/**
 * Returns the PF_IDS set (array of ID strings) from ScriptProperties.
 * Falls back to an empty array if not yet built.
 * v1.6 - New helper supporting persistent PF_IDS.
 * @returns {Array<string>}
 */
function getPFIdsFromProps_() {
  try {
    var raw = PropertiesService.getScriptProperties().getProperty(PRODUCT_PROPS_PF_IDS);
    if (raw) return JSON.parse(raw);
  } catch (e) { /* non-fatal */ }
  return null; // null signals "not yet built" to callers
}

/**
 * Adds a single ID to the persistent PF_IDS set in ScriptProperties if not already present.
 * Called from addProductRecords() after a successful write.
 * v1.6 - New helper.
 * @param {string} id - The record ID just written to DR/PF_Products
 */
function appendPFIdToProps_(id) {
  try {
    var props = PropertiesService.getScriptProperties();
    var raw = props.getProperty(PRODUCT_PROPS_PF_IDS);
    var idSet = raw ? JSON.parse(raw) : [];
    if (idSet.indexOf(id) === -1) {
      idSet.push(id);
      props.setProperty(PRODUCT_PROPS_PF_IDS, JSON.stringify(idSet));
    }
  } catch (e) {
    Logger.log('appendPFIdToProps_ error (non-fatal): ' + e.message);
  }
}

/**
 * Checks if a record ID exists in Applicants_Master.
 * v1.4 - CacheService-backed: builds and caches a Set of AM IDs.
 * v1.6 - TTL raised to 6 hours (PRODUCT_CACHE_TTL = 21600). Stale AM_IDS is safe:
 *         a brand-new record not yet cached simply falls through to new-entry mode.
 * @param {string} id - Record ID to check
 * @returns {boolean}
 */
function isIdInMaster_(id) {
  try {
    var cache = CacheService.getScriptCache();
    var cached = cache.get(PRODUCT_CACHE_KEY_AM_IDS);
    var idSet;

    if (cached) {
      try {
        idSet = JSON.parse(cached); // Array of id strings
      } catch (e) {
        idSet = null;
      }
    }

    if (!idSet) {
      // Build from sheet
      var sheet = getMasterSheet();
      if (!sheet || sheet.getLastRow() < 2) return false;
      var ids = sheet.getRange(2, 1, sheet.getLastRow() - 1, 1).getValues();
      idSet = [];
      for (var i = 0; i < ids.length; i++) {
        var v = (ids[i][0] || '').toString().trim();
        if (v) idSet.push(v);
      }
      try {
        cache.put(PRODUCT_CACHE_KEY_AM_IDS, JSON.stringify(idSet), PRODUCT_CACHE_TTL);
      } catch (e) {
        Logger.log('isIdInMaster_ cache write (non-fatal): ' + e.message);
      }
    }

    // O(1) lookup via object map built from the array
    for (var j = 0; j < idSet.length; j++) {
      if (idSet[j] === id) return true;
    }
    return false;

  } catch (e) {
    Logger.log('isIdInMaster_ error: ' + e.message);
    return false;
  }
}

/**
 * Searches DR/PF_Products in G2N_Data for a matching ID + date.
 * Returns edit mode if found, new-entry mode if not.
 * v1.4 - CacheService composite-key index, O(1) lookup after first call.
 * v1.5 - Two-phase approach: Phase 1 (ID-only set) + Phase 2 (composite index).
 * v1.6 - Phase 1 PF_IDS moved to ScriptProperties (persistent, no TTL).
 *         getPFIdsFromProps_() checked first; sheet scan only on first-ever call or
 *         after ScriptProperties are cleared. appendPFIdToProps_() keeps set current.
 * @param {string} id - Record ID
 * @param {string} searchDate - Normalized date (M/D/YYYY)
 * @returns {Object} { success, found, mode, products[], recordId, requestDate }
 */
function searchActiveDRPFProducts_(id, searchDate) {
  var cache = CacheService.getScriptCache();

  // ── Phase 1: ID-only check (v1.6: ScriptProperties — persistent, no TTL) ──
  // Priority: ScriptProperties → CacheService → sheet scan
  var idSet = getPFIdsFromProps_();

  if (idSet === null) {
    // Neither ScriptProperties nor cache has it — build from sheet
    idSet = [];
    var allData, hdrs;
    var dataWB = getDataWorkbook();
    var sheet = dataWB.getSheetByName(CONFIG.PF_PRODUCTS_SHEET);
    if (sheet && sheet.getLastRow() >= 2) {
      allData = sheet.getDataRange().getValues();
      hdrs = trimHeaders(allData[0]);
      var idCol = hdrs.indexOf('ID');
      if (idCol !== -1) {
        var seen = {};
        for (var r = 1; r < allData.length; r++) {
          var rid = (allData[r][idCol] || '').toString().trim();
          if (rid && !seen[rid]) { idSet.push(rid); seen[rid] = true; }
        }
      }
    }
    // Persist to ScriptProperties so future calls skip this scan entirely
    try {
      PropertiesService.getScriptProperties().setProperty(PRODUCT_PROPS_PF_IDS, JSON.stringify(idSet));
    } catch (e) { /* non-fatal */ }

    // Reuse the already-read data for Phase 2 if this ID is present
    if (idSet.indexOf(id) !== -1) {
      return searchDRPFProductsWithData_(id, searchDate, allData, hdrs, cache);
    }
    return getNewProductList(id, searchDate);
  }

  // ScriptProperties warm — fast path
  if (idSet.indexOf(id) === -1) {
    return getNewProductList(id, searchDate); // Brand-new ID — skip Phase 2 entirely
  }

  // ── Phase 2: composite key lookup ────────────────────────────────────────
  return searchDRPFProductsWithData_(id, searchDate, null, null, cache);
}

/**
 * Phase 2 helper: builds or uses cached composite-key index to find matching rows.
 * @param {string} id - Record ID (known to exist in DR/PF_Products)
 * @param {string} searchDate - Normalized date (M/D/YYYY)
 * @param {Array|null} preloadedData - Full sheet data if already read in Phase 1; null to read fresh
 * @param {Array|null} preloadedHeaders - Trimmed headers if preloaded; null otherwise
 * @param {ScriptCache} cache - Script cache instance
 * @returns {Object}
 */
function searchDRPFProductsWithData_(id, searchDate, preloadedData, preloadedHeaders, cache) {
  var indexMap = null;
  try {
    var cachedIdx = cache.get(PRODUCT_CACHE_KEY_PF_INDEX);
    if (cachedIdx) indexMap = JSON.parse(cachedIdx);
  } catch (e) { /* non-fatal */ }

  if (!indexMap) {
    indexMap = {};
    var data = preloadedData;
    var headers = preloadedHeaders;

    if (!data) {
      var dataWB = getDataWorkbook();
      var sheet = dataWB.getSheetByName(CONFIG.PF_PRODUCTS_SHEET);
      if (!sheet || sheet.getLastRow() < 2) return getNewProductList(id, searchDate);
      data = sheet.getDataRange().getValues();
      headers = trimHeaders(data[0]);
    }

    var idCol          = headers.indexOf('ID');
    var reqDateCol     = headers.indexOf('RequestDate');
    var productIdCol   = headers.indexOf('ProductId');
    var productNameCol = headers.indexOf('ProductName');
    var qtyReqCol      = headers.indexOf('QtyRequested');
    var qtyRecCol      = headers.indexOf('QtyReceived');

    if (idCol === -1 || reqDateCol === -1) {
      return { success: false, error: 'DR/PF_Products sheet is missing required columns (ID, RequestDate)' };
    }

    for (var i = 1; i < data.length; i++) {
      var rowId   = (data[i][idCol]  || '').toString().trim();
      var rowDate = normalizeProductDate(data[i][reqDateCol]);
      if (!rowId || !rowDate) continue;
      var key = rowId + '|' + rowDate;
      if (!indexMap[key]) indexMap[key] = [];
      indexMap[key].push({
        sheetRow:     i + 1,
        productId:    productIdCol   !== -1 ? (data[i][productIdCol]   || '').toString() : '',
        productName:  productNameCol !== -1 ? (data[i][productNameCol] || '').toString() : '',
        qtyRequested: qtyReqCol      !== -1 ? (data[i][qtyReqCol]      || '') : '',
        qtyReceived:  qtyRecCol      !== -1 ? (data[i][qtyRecCol]      || '') : ''
      });
    }
    try { cache.put(PRODUCT_CACHE_KEY_PF_INDEX, JSON.stringify(indexMap), PRODUCT_CACHE_TTL); } catch (e) { /* non-fatal */ }
  }

  var matches = indexMap[id + '|' + searchDate];
  if (matches && matches.length > 0) {
    return { success: true, found: true, mode: 'edit', products: matches, recordId: id, requestDate: searchDate };
  }
  return getNewProductList(id, searchDate);
}

/**
 * Searches Products_Archive sheets across all archive workbooks for a matching ID + date.
 * Uses getArchiveWorkbooksForRange() with far-past/far-future dates to cover all workbooks.
 * Returns null if not found (caller falls through to new-entry mode).
 * Archive records carry archived:true so PP renders them read-only.
 * v1.3 - New helper.
 * @param {string} id - Record ID
 * @param {string} searchDate - Normalized date (M/D/YYYY)
 * @returns {Object|null} Result object if found, null if not found in any archive
 */
function findProductsInArchives_(id, searchDate) {
  try {
    var farPast = new Date('2000-01-01');
    var farFuture = new Date('2099-12-31');
    var archiveWorkbooks = getArchiveWorkbooksForRange(farPast, farFuture);

    for (var w = 0; w < archiveWorkbooks.length; w++) {
      var wb = archiveWorkbooks[w].workbook;
      var wbName = archiveWorkbooks[w].name;
      var sheet = wb.getSheetByName('Products_Archive');
      if (!sheet || sheet.getLastRow() < 2) continue;

      var data = sheet.getDataRange().getValues();
      var headers = trimHeaders(data[0]);
      var idCol = headers.indexOf('ID');
      var reqDateCol = headers.indexOf('RequestDate');
      var productIdCol = headers.indexOf('ProductId');
      var productNameCol = headers.indexOf('ProductName');
      var qtyReqCol = headers.indexOf('QtyRequested');
      var qtyRecCol = headers.indexOf('QtyReceived');

      if (idCol === -1 || reqDateCol === -1) continue;

      var matches = [];
      for (var i = 1; i < data.length; i++) {
        var rowId = (data[i][idCol] || '').toString().trim();
        var rowDate = normalizeProductDate(data[i][reqDateCol]);
        if (rowId === id && rowDate === searchDate) {
          matches.push({
            sheetRow: i + 1,
            productId: productIdCol !== -1 ? (data[i][productIdCol] || '').toString() : '',
            productName: productNameCol !== -1 ? (data[i][productNameCol] || '').toString() : '',
            qtyRequested: qtyReqCol !== -1 ? (data[i][qtyReqCol] || '') : '',
            qtyReceived: qtyRecCol !== -1 ? (data[i][qtyRecCol] || '') : ''
          });
        }
      }

      if (matches.length > 0) {
        Logger.log('findProductsInArchives_: found ' + matches.length + ' records in ' + wbName);
        return {
          success: true, found: true, mode: 'edit',
          archived: true,          // PP renders these read-only
          archiveSource: wbName,
          products: matches, recordId: id, requestDate: searchDate
        };
      }
    }
    return null;
  } catch (e) {
    Logger.log('findProductsInArchives_ error: ' + e.message);
    return null;
  }
}

/**
 * Returns full product catalog from LU_Products for new entry mode.
 * v1.5 - Caches LU_Products list (PRODUCT_CACHE_KEY_PRODUCTS, 5-min TTL)
 *         so repeated new-entry calls avoid repeated sheet reads.
 * @param {string} recordId - The applicant record ID
 * @param {string} normalizedDate - Normalized date string (M/D/YYYY)
 * @returns {Object} { success, found: false, mode: 'new', products[] }
 */
function getNewProductList(recordId, normalizedDate) {
  try {
    var cache = CacheService.getScriptCache();
    var products = null;

    try {
      var cachedProds = cache.get(PRODUCT_CACHE_KEY_PRODUCTS);
      if (cachedProds) products = JSON.parse(cachedProds);
    } catch (e) { /* non-fatal */ }

    if (!products) {
      var lookups = getLookupsWorkbook();
      var sheet = lookups.getSheetByName(CONFIG.LOOKUPS.PRODUCTS);

      if (!sheet || sheet.getLastRow() < 2) {
        return { success: false, error: 'LU_Products sheet not found or empty' };
      }

      var data = sheet.getDataRange().getValues();
      var headers = trimHeaders(data[0]);
      var nameCol   = headers.indexOf('ProductName');
      var activeCol = headers.indexOf('Active');
      if (nameCol === -1) nameCol = 0;

      products = [];
      for (var i = 1; i < data.length; i++) {
        var name = (data[i][nameCol] || '').toString().trim();
        if (!name) continue;
        // v1.8: Skip inactive products when Active column is present
        if (activeCol !== -1) {
          var activeVal = (data[i][activeCol] || '').toString().trim().toUpperCase();
          if (activeVal !== 'Y' && activeVal !== 'YES' && activeVal !== 'TRUE' && activeVal !== '1') continue;
        }
        products.push({
          sheetRow:     i + 1,
          productId:    (i + 1).toString(),  // stable row-based ID for new entries
          productName:  name,
          qtyRequested: '',
          qtyReceived:  ''
        });
      }
      try { cache.put(PRODUCT_CACHE_KEY_PRODUCTS, JSON.stringify(products), PRODUCT_CACHE_TTL); } catch (e) { /* non-fatal */ }
    }

    return {
      success: true,
      found: false,
      mode: 'new',
      products: products,
      recordId: recordId,
      requestDate: normalizedDate
    };

  } catch (error) {
    Logger.log('getNewProductList error: ' + error.message);
    return { success: false, error: 'Failed to load product list: ' + error.message };
  }
}

/**
 * Updates existing product records in DR/PF_Products (edit mode)
 * Reads full sheet, modifies QtyRequested/QtyReceived in memory,
 * writes back in a single setValues() call instead of per-cell setValue().
 * v1.1 - Performance: batched writes, cached workbook, trimHeaders()
 * @param {Array} products - Array of { sheetRow, qtyRequested, qtyReceived }
 * @returns {Object} { success, message, updatedCount }
 */
function updateProductRecords(products) {
  // v1.8: USE_MYSQL guard outside try/catch — prevents bridge errors falling through to Sheets path.
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.updateProductRecords(products);
  try {
    if (!products || products.length === 0) {
      return { success: false, error: 'No products to update' };
    }

    var dataWB = getDataWorkbook();
    var sheet = dataWB.getSheetByName(CONFIG.PF_PRODUCTS_SHEET);

    if (!sheet) {
      return { success: false, error: 'DR/PF_Products sheet not found' };
    }

    var data = sheet.getDataRange().getValues();
    var headers = trimHeaders(data[0]);
    var qtyReqCol = headers.indexOf('QtyRequested');
    var qtyRecCol = headers.indexOf('QtyReceived');

    if (qtyReqCol === -1 || qtyRecCol === -1) {
      return { success: false, error: 'QtyRequested or QtyReceived column not found' };
    }

    var updated = 0;
    for (var i = 0; i < products.length; i++) {
      var p = products[i];
      var rowIdx = parseInt(p.sheetRow) - 1; // Convert to 0-based array index
      if (rowIdx >= 1 && rowIdx < data.length) {
        data[rowIdx][qtyReqCol] = p.qtyRequested || '';
        data[rowIdx][qtyRecCol] = p.qtyReceived || '';
        updated++;
      }
    }

    // Single bulk write of all data rows
    if (updated > 0 && data.length > 1) {
      var dataRows = data.slice(1);
      sheet.getRange(2, 1, dataRows.length, dataRows[0].length).setValues(dataRows);
    }

    logAudit('PRODUCT_UPDATE', null, 'Updated ' + updated + ' product records in DR/PF_Products');
    invalidateProductCache_();

    return {
      success: true,
      message: 'Updated ' + updated + ' product records',
      updatedCount: updated
    };

  } catch (error) {
    Logger.log('updateProductRecords error: ' + error.message);
    return { success: false, error: 'Failed to update products: ' + error.message };
  }
}

/**
 * Adds new product records to DR/PF_Products (new mode)
 * Only writes rows where QtyRequested or QtyReceived has a value
 * Creates the sheet with headers if it doesn't exist
 * @param {string} recordId - The applicant record ID
 * @param {string} requestDate - Normalized date (M/D/YYYY)
 * @param {Array} products - Array of { productId, productName, qtyRequested, qtyReceived }
 * @returns {Object} { success, message, addedCount }
 */
function addProductRecords(recordId, requestDate, products) {
  // v1.8: USE_MYSQL guard is OUTSIDE the try/catch so bridge errors propagate
  // to the caller instead of falling through to the Sheets write path.
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) return DbService.addProductRecords(recordId, requestDate, products);
  try {
    if (!recordId || !requestDate) {
      return { success: false, error: 'Record ID and Request Date are required' };
    }

    // v1.7: Strip G2N- prefix so the sheet ID column stores bare integers
    // matching AM column A format. SV portal may pass "G2N-05216" when
    // USE_MYSQL was previously true and records were opened in that session.
    var sheetId = (recordId || '').toString().trim();
    if (sheetId.toUpperCase().startsWith('G2N-')) {
      sheetId = parseInt(sheetId.substring(4), 10).toString();
    }

    if (!products || products.length === 0) {
      return { success: false, error: 'No products to add' };
    }

    var dataWB = getDataWorkbook();
    var sheet = dataWB.getSheetByName(CONFIG.PF_PRODUCTS_SHEET);

    if (!sheet) {
      // Create the sheet if it doesn't exist
      sheet = dataWB.insertSheet(CONFIG.PF_PRODUCTS_SHEET);
      sheet.getRange(1, 1, 1, 7).setValues([['ID', 'RequestDate', 'ProductId', 'ProductName', 'QtyRequested', 'QtyReceived', 'Active']]);
      sheet.getRange(1, 1, 1, 7).setFontWeight('bold').setBackground('#4a86e8').setFontColor('white');
      sheet.setFrozenRows(1);
    }

    // Filter to only products with qty values
    var rowsToAdd = [];
    for (var i = 0; i < products.length; i++) {
      var p = products[i];
      var qtyReq = (p.qtyRequested || '').toString().trim();
      var qtyRec = (p.qtyReceived || '').toString().trim();

      if (qtyReq !== '' || qtyRec !== '') {
        rowsToAdd.push([
          sheetId,
          requestDate,
          p.productId || '',
          p.productName || '',
          qtyReq ? parseInt(qtyReq) || 0 : '',
          qtyRec ? parseInt(qtyRec) || 0 : '',
          'Y'
        ]);
      }
    }

    if (rowsToAdd.length === 0) {
      return { success: true, message: 'No products had quantities entered. Nothing was saved.', addedCount: 0 };
    }

    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, rowsToAdd.length, 7).setValues(rowsToAdd);

    logAudit('PRODUCT_ADD', sheetId, 'Added ' + rowsToAdd.length + ' product records to DR/PF_Products');
    invalidateProductCache_();
    appendPFIdToProps_(sheetId); // v1.6: keep PF_IDS props current

    return {
      success: true,
      message: 'Added ' + rowsToAdd.length + ' product records',
      addedCount: rowsToAdd.length
    };

  } catch (error) {
    Logger.log('addProductRecords error: ' + error.message);
    return { success: false, error: 'Failed to add products: ' + error.message };
  }
}

/**
 * Normalizes various date formats to M/D/YYYY for consistent comparison.
 * v1.2: Delegates to shared normalizeDate() in SharedUtils.gs (#3).
 * @param {*} dateVal - Date value in any supported format
 * @returns {string} Normalized date (M/D/YYYY) or empty string
 */
function normalizeProductDate(dateVal) {
  return normalizeDate(dateVal);
}