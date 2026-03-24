/**
 * GrantsReportService.gs
 * Handles grant/analytics report generation for G2N System
 * v2.0 - Added Funding Sources, Income Sources, Collaboration Source
 *         Shared helpers for AM + Archive data combining
 *         Shared age bracket calculation helper
 * v3.0 - Major report restructure per Grant & AI Reporting Changes doc:
 *         REMOVED: Funding Sources, Collaboration Source, Income Levels, Income Sources
 *         MODIFIED: Applicants Open Requests, Households, Request Frequency,
 *                   Distribution Stats, Testimonials - new column sets, sort orders,
 *                   full age bracket detail where specified
 *         ADDED: Grant Summary (narrative template report)
 *         UPDATED: Product calculation logic - checks code value for DR/PF routing,
 *                  supports Received Product Code 3
 * v3.1 - Removed Report Totals from: Applicants Open Requests, Request Frequency,
 *         Households, Distribution Stats
 *       - Open Requests, Households, Distribution Stats: Number of Households always 1
 *       - All reports: Batch write data rows (single setValues call) to prevent timeout
 *       - Grant Summary: Added Income Level threshold dropdown (from LU_IncomeLevels),
 *         multi-value support for County/City/Zip filters (comma-separated),
 *         dynamic income threshold with below-threshold % breakdowns
 *       - Added getIncomeLevelOptions(), parseIncomeLowEnd() helpers
 *       - Fixed DR/PF double-count: only counted once per record regardless
 *         of how many codes are DR or PF
 * v3.2 - Added FIELD_DISPLAY_MAP and getFieldDisplayLabel() — centralized mapping
 *         of raw AM column headers to friendly grant-report-style output labels.
 *         Shared across GrantsReportService and AIReportService.
 * v3.3 - Critical fix: getCombinedData() and loadProductLookupData() now iterate
 *         year-based G2N_Archive_YYYY workbooks via getArchiveWorkbooksForRange(),
 *         fixing missing data in reports when date range spans rolled-over years.
 *         Performance: Replaced 3 uncached SpreadsheetApp.openById() calls with
 *         getDataWorkbook() and getLookupsWorkbook(). Adopted trimHeaders() utility
 *         in getCombinedData() and loadProductLookupData().
 * v3.4 - FieldMapService migration: Removed hardcoded FIELD_DISPLAY_MAP object
 *         and getFieldDisplayLabel() function — now delegates to FieldMapService
 *         which reads from LU_FieldMap sheet in G2N_Lookups. COL_* constants
 *         retained as deprecated aliases (still functional, sourced from strings).
 *         getApplicantType(), getBabyBoxIndicator(), getStr() remain here as
 *         shared value-transform helpers used by multiple report functions.
 * v3.5 - SharedUtils extraction: Removed parseDateInput(), moveToFolder(),
 *         styleReportHeader(), styleGroupRow(), writeReportTitleSection(),
 *         getStr(), getApplicantType(), getBabyBoxIndicator() -- all moved to
 *         SharedUtils.gs (#15). Adopted CONFIG.TIMEZONE across all 33
 *         Utilities.formatDate() calls (#8). COL_* constants remain
 *         deprecated-in-place (#12).
 * v3.6 - FieldMapService integration: all AM column lookups in generateOpenRequestsReport,
 *         generateHouseholdsReport, generateDistributionStatsReport, generateRequestFrequencyReport,
 *         generateGrantSummaryReport, and generateTestimonialsReport now use resolveAMField_()
 *         via SharedUtils for LU_FieldMap-driven header resolution.
 * v3.7 - MySQL offload for Request Frequency and Grant Summary reports.
 *         generateRequestFrequencyReport: when USE_MYSQL=true, calls
 *         DbService.getRequestFrequencyData() → sp_get_request_frequency_data
 *         (LAG window function); skips GAS grouping loop entirely.
 *         generateGrantSummaryReport: when USE_MYSQL=true, calls
 *         DbService.getGrantSummaryStats() → sp_get_grant_summary_stats;
 *         skips GAS aggregation loop. Both fall back to Sheets path on error.
 *         Extracted _writeRequestFrequencySpreadsheet_() and _buildGrantSummaryDoc_()
 *         as private helpers shared by both MySQL and Sheets paths.
 *
 * Reports:
 *   1. Applicants Open Requests
 *   2. Households (full age detail + products)
 *   3. Request Frequency (per-applicant frequency calculation)
 *   4. Testimonials
 *   5. Distribution Stats (expanded with referral, code 3, full demographics)
 *   6. Grant Summary (narrative)
 */

// ============ SHARED HELPERS ============

/**
 * Get combined data from Applicants_Master and all archive workbooks
 * (G2N_Archive + year-based G2N_Archive_YYYY) filtered by Request Date.
 * v3.3 - Now iterates year-based archives via getArchiveWorkbooksForRange()
 *         to include rolled-over data. Uses trimHeaders().
 * @param {Date} fromDate - Start date for filtering
 * @param {Date} toDate - End date for filtering
 * @param {Array} additionalFilters - Optional array of {column, values} filters
 * @returns {Object} { headers: [], rows: [], masterCount: n, archiveCount: n }
 */
function getCombinedData(fromDate, toDate, additionalFilters) {
  // ── MySQL path ────────────────────────────────────────────────────────────
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) {
    try {
      var fromStr = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
      var toStr   = Utilities.formatDate(toDate,   CONFIG.TIMEZONE, 'yyyy-MM-dd');

      // Convert additionalFilters to CSV params for sp_get_report_data
      var statusCsv         = null;
      var statusIncludeNull = false;
      var countyCsv         = null;
      var cityCsv           = null;
      var zipCsv            = null;

      if (additionalFilters) {
        for (var f = 0; f < additionalFilters.length; f++) {
          var filter = additionalFilters[f];
          if (filter.column === 'Service Status') {
            if (filter.values) {
              statusCsv = filter.values.join(',');
            } else if (filter.emptyOrValues) {
              statusCsv         = filter.emptyOrValues.join(',');
              statusIncludeNull = true;   // emptyOrValues also matches null/blank rows
            }
          } else if (filter.column === 'County'   && filter.values) {
            countyCsv = filter.values.join(',');
          } else if (filter.column === 'City'     && filter.values) {
            cityCsv   = filter.values.join(',');
          } else if (filter.column === 'Zip Code' && filter.values) {
            zipCsv    = filter.values.join(',');
          }
        }
      }

      var result = DbService.getReportData(
        fromStr, toStr, /*includeArchive=*/true,
        statusCsv, statusIncludeNull, countyCsv, cityCsv, zipCsv);

      // Filters already applied in SQL — no client-side filtering needed.
      // Count active vs archived using the active column.
      var masterCount  = 0;
      var archiveCount = 0;
      var activeIdx    = result.headers.indexOf(resolveAMField_('Active'));

      for (var i = 0; i < result.rows.length; i++) {
        var isArchived = activeIdx !== -1 &&
          (result.rows[i][activeIdx] === 0   ||
           result.rows[i][activeIdx] === '0' ||
           result.rows[i][activeIdx] === false);
        if (isArchived) { archiveCount++; } else { masterCount++; }
      }

      return {
        headers:      result.headers,
        rows:         result.rows,
        masterCount:  masterCount,
        archiveCount: archiveCount,
        totalCount:   result.rows.length
      };
    } catch (e) {
      Logger.log('getCombinedData MySQL error (falling back to Sheets): ' + e.message);
      // Fall through to Sheets path on error
    }
  }

  // ── Sheets path (original) ────────────────────────────────────────────────
  var masterCount = 0;
  var archiveCount = 0;
  var allRows = [];
  var headers = [];
  
  // --- Read from Applicants_Master ---
  var masterSheet = getMasterSheet();
  if (masterSheet) {
    var masterData = masterSheet.getDataRange().getValues();
    headers = trimHeaders(masterData[0]);
    var requestDateCol = headers.indexOf(resolveAMField_('Request Date'));
    
    if (requestDateCol !== -1) {
      for (var i = 1; i < masterData.length; i++) {
        var requestDate = masterData[i][requestDateCol];
        if (requestDate) {
          var reqDate = new Date(requestDate);
          if (reqDate >= fromDate && reqDate <= toDate) {
            if (passesFilters(headers, masterData[i], additionalFilters)) {
              allRows.push(masterData[i]);
              masterCount++;
            }
          }
        }
      }
    }
  }
  
  // --- Read from all archive workbooks (G2N_Archive + year-based) ---
  try {
    var archiveWorkbooks = getArchiveWorkbooksForRange(fromDate, toDate);
    
    for (var aw = 0; aw < archiveWorkbooks.length; aw++) {
      try {
        var archiveWB = archiveWorkbooks[aw].workbook;
        var archiveSheet = archiveWB.getSheetByName('Archive');
        
        if (archiveSheet && archiveSheet.getLastRow() > 1) {
          var archiveData = archiveSheet.getDataRange().getValues();
          var archiveHeaders = trimHeaders(archiveData[0]);
          var archiveRequestDateCol = archiveHeaders.indexOf(resolveAMField_('Request Date'));
          
          if (archiveRequestDateCol !== -1) {
            // Build column index mapping from archive headers to master headers
            var colMap = [];
            for (var h = 0; h < headers.length; h++) {
              colMap.push(archiveHeaders.indexOf(headers[h]));
            }
            
            for (var j = 1; j < archiveData.length; j++) {
              var requestDate = archiveData[j][archiveRequestDateCol];
              if (requestDate) {
                var reqDate = new Date(requestDate);
                if (reqDate >= fromDate && reqDate <= toDate) {
                  // Map archive row to master column order
                  var mappedRow = [];
                  for (var c = 0; c < headers.length; c++) {
                    mappedRow.push(colMap[c] !== -1 ? archiveData[j][colMap[c]] : '');
                  }
                  
                  if (passesFilters(headers, mappedRow, additionalFilters)) {
                    allRows.push(mappedRow);
                    archiveCount++;
                  }
                }
              }
            }
          }
        }
      } catch (wbErr) {
        Logger.log('Archive workbook read error (' + archiveWorkbooks[aw].name + '): ' + wbErr.message);
      }
    }
  } catch (e) {
    Logger.log('Archive read error: ' + e.message);
    // Continue with master data only
  }
  
  return {
    headers: headers,
    rows: allRows,
    masterCount: masterCount,
    archiveCount: archiveCount,
    totalCount: masterCount + archiveCount
  };
}

/**
 * Check if a row passes additional filters
 * @param {Array} headers - Column headers
 * @param {Array} row - Data row
 * @param {Array} filters - Array of {column: 'colName', values: ['val1', 'val2']} or {column: 'colName', emptyOrValues: ['val1']}
 * @returns {boolean}
 */
function passesFilters(headers, row, filters) {
  if (!filters || filters.length === 0) return true;
  
  for (var f = 0; f < filters.length; f++) {
    var filter = filters[f];
    var colIdx = headers.indexOf(filter.column);
    if (colIdx === -1) continue;
    
    var cellValue = (row[colIdx] || '').toString().trim();
    
    if (filter.emptyOrValues) {
      // Match if cell is empty/blank OR matches one of the values
      if (cellValue === '' || cellValue === null) continue; // passes - it's empty
      var found = false;
      for (var v = 0; v < filter.emptyOrValues.length; v++) {
        if (cellValue.toLowerCase() === filter.emptyOrValues[v].toLowerCase()) {
          found = true;
          break;
        }
      }
      if (!found) return false;
    } else if (filter.values) {
      // Match if cell value is in the allowed values list
      var found = false;
      for (var v = 0; v < filter.values.length; v++) {
        if (cellValue.toLowerCase() === filter.values[v].toLowerCase()) {
          found = true;
          break;
        }
      }
      if (!found) return false;
    }
  }
  
  return true;
}

// parseDateInput() moved to SharedUtils.gs v1.0 (#15)

/**
 * Get month name from month number (0-11)
 */
function getMonthName(monthNum) {
  var months = ['January', 'February', 'March', 'April', 'May', 'June',
                'July', 'August', 'September', 'October', 'November', 'December'];
  return months[monthNum] || '';
}

// moveToFolder() moved to SharedUtils.gs v1.0 (#15)

// styleReportHeader() moved to SharedUtils.gs v1.0 (#15)

// styleGroupRow() moved to SharedUtils.gs v1.0 (#15)

/**
 * Get quarter number (1-4) from month (0-11)
 * Month < 4 = Q1, Month 4-6 = Q2, Month 7-9 = Q3, Month > 9 = Q4
 */
function getQuarter(monthNum) {
  if (monthNum < 3) return 1;
  if (monthNum < 6) return 2;
  if (monthNum < 9) return 3;
  return 4;
}

// writeReportTitleSection() moved to SharedUtils.gs v1.0 (#15)


// ============ SHARED AGE BRACKET HELPERS ============

/**
 * Age bracket column definitions matching Applicants_Master headers
 * maxAge = upper bound of each bracket (used for child/adult/senior classification)
 */
var AGE_BRACKETS = [
  { male: '[Males under 5]', female: '[Females under 5]', maxAge: 4 },
  { male: '[Males 5-9]', female: '[Females 5-9]', maxAge: 9 },
  { male: '[Males 10-14]', female: '[Females 10-14]', maxAge: 14 },
  { male: '[Males 15-17]', female: '[Females 15-17]', maxAge: 17 },
  { male: '[Males 18-24]', female: '[Females 18-24]', maxAge: 24 },
  { male: '[Males 25-34]', female: '[Females 25-34]', maxAge: 34 },
  { male: '[Males 35-44]', female: '[Females 35-44]', maxAge: 44 },
  { male: '[Males 45-54]', female: '[Females 45-54]', maxAge: 54 },
  { male: '[Males 55-64]', female: '[Females 55-64]', maxAge: 64 },
  { male: '[Males 65-84]', female: '[Females 65-84]', maxAge: 84 },
  { male: '[Males 85 and Over]', female: '[Females 85 and Over]', maxAge: 999 }
];

/**
 * Calculate simple age bracket sums for a data row
 * @param {Array} headers - Column headers
 * @param {Array} row - Data row
 * @param {number} adultMaxAge - Max age for adult category (e.g., 54 or 64)
 * @returns {Object} { people, children, adults, seniors }
 */
function calculateAgeBrackets(headers, row, adultMaxAge) {
  var people = 0, children = 0, adults = 0, seniors = 0;
  
  for (var b = 0; b < AGE_BRACKETS.length; b++) {
    var bracket = AGE_BRACKETS[b];
    var maleIdx = headers.indexOf(bracket.male);
    var femaleIdx = headers.indexOf(bracket.female);
    var maleVal = maleIdx !== -1 ? (parseInt(row[maleIdx]) || 0) : 0;
    var femaleVal = femaleIdx !== -1 ? (parseInt(row[femaleIdx]) || 0) : 0;
    var bracketTotal = maleVal + femaleVal;
    
    people += bracketTotal;
    
    if (bracket.maxAge <= 17) {
      children += bracketTotal;
    } else if (bracket.maxAge <= adultMaxAge) {
      adults += bracketTotal;
    } else {
      seniors += bracketTotal;
    }
  }
  
  return { people: people, children: children, adults: adults, seniors: seniors };
}

/**
 * Calculate detailed age bracket breakdowns for reports requiring full age detail
 * Returns male/female/total for each category plus individual bracket totals
 * Children <18, Adults 18-64, Seniors >64
 * @param {Array} headers - Column headers
 * @param {Array} row - Data row
 * @returns {Object} Detailed age breakdown
 */
function calculateDetailedAgeBrackets(headers, row) {
  var result = {
    people: 0,
    totalChildren: 0, maleChildren: 0, femaleChildren: 0,
    under5: 0, aged5t9: 0, aged10t14: 0, aged15t17: 0,
    totalAdults: 0, maleAdults: 0, femaleAdults: 0,
    aged18t24: 0, aged25t34: 0, aged35t44: 0, aged45t54: 0, aged55t64: 0,
    totalSeniors: 0, maleSeniors: 0, femaleSeniors: 0,
    aged65t84: 0, aged85up: 0
  };
  
  for (var b = 0; b < AGE_BRACKETS.length; b++) {
    var bracket = AGE_BRACKETS[b];
    var maleIdx = headers.indexOf(bracket.male);
    var femaleIdx = headers.indexOf(bracket.female);
    var maleVal = maleIdx !== -1 ? (parseInt(row[maleIdx]) || 0) : 0;
    var femaleVal = femaleIdx !== -1 ? (parseInt(row[femaleIdx]) || 0) : 0;
    var bracketTotal = maleVal + femaleVal;
    
    result.people += bracketTotal;
    
    if (bracket.maxAge <= 17) {
      result.totalChildren += bracketTotal;
      result.maleChildren += maleVal;
      result.femaleChildren += femaleVal;
      
      if (bracket.maxAge <= 4) result.under5 += bracketTotal;
      else if (bracket.maxAge <= 9) result.aged5t9 += bracketTotal;
      else if (bracket.maxAge <= 14) result.aged10t14 += bracketTotal;
      else result.aged15t17 += bracketTotal;
      
    } else if (bracket.maxAge <= 64) {
      result.totalAdults += bracketTotal;
      result.maleAdults += maleVal;
      result.femaleAdults += femaleVal;
      
      if (bracket.maxAge <= 24) result.aged18t24 += bracketTotal;
      else if (bracket.maxAge <= 34) result.aged25t34 += bracketTotal;
      else if (bracket.maxAge <= 44) result.aged35t44 += bracketTotal;
      else if (bracket.maxAge <= 54) result.aged45t54 += bracketTotal;
      else result.aged55t64 += bracketTotal;
      
    } else {
      result.totalSeniors += bracketTotal;
      result.maleSeniors += maleVal;
      result.femaleSeniors += femaleVal;
      if (bracket.maxAge <= 84) result.aged65t84 += bracketTotal;
      else result.aged85up += bracketTotal;
    }
  }
  
  return result;
}


// ============ PRODUCT LOOKUP HELPERS ============

/**
 * Load product lookup data from G2N_Data workbook and all archive workbooks
 * Returns maps for Distributed_Products (by BoxCode) and DR/PF_Products +
 * Products_Archive from G2N_Archive and year-based G2N_Archive_YYYY (by ID|RequestDate).
 * v3.3 - Performance: uses getDataWorkbook(). Iterates year-based archives
 *         via getArchiveWorkbooksForRange() for complete product coverage.
 *         Adopted trimHeaders().
 * @returns {Object} { distProdByBox: {boxCode: [qty]}, drPfByIdDate: {id|date: {qtyRequested, qtyReceived}} }
 */
function loadProductLookupData(fromDate, toDate) {
  // ── MySQL path ────────────────────────────────────────────────────────────
  if (CONFIG.DB && CONFIG.DB.USE_MYSQL) {
    try {
      // Pass date range so drpf query is scoped to relevant applicants only
      var fromStr = (fromDate instanceof Date)
        ? Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd') : null;
      var toStr = (toDate instanceof Date)
        ? Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'yyyy-MM-dd') : null;
      return DbService.getProductLookupData(fromStr, toStr);
    } catch (e) {
      Logger.log('loadProductLookupData MySQL error (falling back to Sheets): ' + e.message);
      // Fall through to Sheets path on error
    }
  }

  // ── Sheets path (original) ────────────────────────────────────────────────
  var result = {
    distProdByBox: {},
    drPfByIdDate: {}
  };
  
  try {
    var dataWB = getDataWorkbook();
    
    // --- Distributed_Products sheet ---
    var dpSheet = dataWB.getSheetByName('Distributed_Products');
    if (dpSheet && dpSheet.getLastRow() > 1) {
      var dpData = dpSheet.getDataRange().getValues();
      var dpHeaders = trimHeaders(dpData[0]);
      var boxCodeCol = dpHeaders.indexOf('BoxCode');
      var qtyCol = dpHeaders.indexOf('Quantity');
      
      if (boxCodeCol !== -1 && qtyCol !== -1) {
        for (var i = 1; i < dpData.length; i++) {
          var boxCode = (dpData[i][boxCodeCol] || '').toString().trim();
          if (boxCode === '') continue;
          var qty = parseInt(dpData[i][qtyCol]) || 0;
          if (!result.distProdByBox[boxCode]) {
            result.distProdByBox[boxCode] = [];
          }
          result.distProdByBox[boxCode].push(qty);
        }
      }
    }
    
    // --- DR/PF_Products sheet (keyed by ID + Request Date) ---
    var drPfSheet = dataWB.getSheetByName('DR/PF_Products');
    if (drPfSheet && drPfSheet.getLastRow() > 1) {
      var drPfData = drPfSheet.getDataRange().getValues();
      var drPfHeaders = trimHeaders(drPfData[0]);
      var idCol = drPfHeaders.indexOf('ID');
      var reqDateCol = drPfHeaders.indexOf('Request Date');
      if (reqDateCol === -1) reqDateCol = drPfHeaders.indexOf('RequestDate');
      var qtyReqCol = drPfHeaders.indexOf('QtyRequested');
      var qtyRecCol = drPfHeaders.indexOf('QtyReceived');
      
      if (idCol !== -1 && reqDateCol !== -1) {
        for (var i = 1; i < drPfData.length; i++) {
          var id = (drPfData[i][idCol] || '').toString().trim();
          if (id === '') continue;
          
          // Normalize date to M/d/yyyy for consistent matching
          var dateVal = drPfData[i][reqDateCol];
          var dateKey = '';
          if (dateVal instanceof Date && !isNaN(dateVal.getTime())) {
            dateKey = Utilities.formatDate(dateVal, CONFIG.TIMEZONE, 'M/d/yyyy');
          } else if (dateVal) {
            var parsed = new Date(dateVal);
            if (!isNaN(parsed.getTime())) {
              dateKey = Utilities.formatDate(parsed, CONFIG.TIMEZONE, 'M/d/yyyy');
            }
          }
          
          var compositeKey = id + '|' + dateKey;
          var qtyReq = qtyReqCol !== -1 ? (parseInt(drPfData[i][qtyReqCol]) || 0) : 0;
          var qtyRec = qtyRecCol !== -1 ? (parseInt(drPfData[i][qtyRecCol]) || 0) : 0;
          
          if (!result.drPfByIdDate[compositeKey]) {
            result.drPfByIdDate[compositeKey] = { qtyRequested: 0, qtyReceived: 0 };
          }
          result.drPfByIdDate[compositeKey].qtyRequested += qtyReq;
          result.drPfByIdDate[compositeKey].qtyReceived += qtyRec;
        }
      }
    }
    
    // --- Products_Archive from all archive workbooks (G2N_Archive + year-based) ---
    // Use a wide date range to capture all product archives
    try {
      var farPast = new Date(2000, 0, 1);
      var farFuture = new Date(2099, 11, 31);
      var archiveWorkbooks = getArchiveWorkbooksForRange(farPast, farFuture);
      
      for (var aw = 0; aw < archiveWorkbooks.length; aw++) {
        try {
          var prodArchiveSheet = archiveWorkbooks[aw].workbook.getSheetByName('Products_Archive');
          if (prodArchiveSheet && prodArchiveSheet.getLastRow() > 1) {
            var paData = prodArchiveSheet.getDataRange().getValues();
            var paHeaders = trimHeaders(paData[0]);
            var paIdCol = paHeaders.indexOf('ID');
            var paReqDateCol = paHeaders.indexOf('Request Date');
            if (paReqDateCol === -1) paReqDateCol = paHeaders.indexOf('RequestDate');
            var paQtyReqCol = paHeaders.indexOf('QtyRequested');
            var paQtyRecCol = paHeaders.indexOf('QtyReceived');
            
            if (paIdCol !== -1 && paReqDateCol !== -1) {
              for (var j = 1; j < paData.length; j++) {
                var paId = (paData[j][paIdCol] || '').toString().trim();
                if (paId === '') continue;
                
                var paDateVal = paData[j][paReqDateCol];
                var paDateKey = '';
                if (paDateVal instanceof Date && !isNaN(paDateVal.getTime())) {
                  paDateKey = Utilities.formatDate(paDateVal, CONFIG.TIMEZONE, 'M/d/yyyy');
                } else if (paDateVal) {
                  var paParsed = new Date(paDateVal);
                  if (!isNaN(paParsed.getTime())) {
                    paDateKey = Utilities.formatDate(paParsed, CONFIG.TIMEZONE, 'M/d/yyyy');
                  }
                }
                
                var paCompositeKey = paId + '|' + paDateKey;
                var paQtyReq = paQtyReqCol !== -1 ? (parseInt(paData[j][paQtyReqCol]) || 0) : 0;
                var paQtyRec = paQtyRecCol !== -1 ? (parseInt(paData[j][paQtyRecCol]) || 0) : 0;
                
                if (!result.drPfByIdDate[paCompositeKey]) {
                  result.drPfByIdDate[paCompositeKey] = { qtyRequested: 0, qtyReceived: 0 };
                }
                result.drPfByIdDate[paCompositeKey].qtyRequested += paQtyReq;
                result.drPfByIdDate[paCompositeKey].qtyReceived += paQtyRec;
              }
            }
            Logger.log('Products_Archive (' + archiveWorkbooks[aw].name + '): loaded ' + (paData.length - 1) + ' records');
          }
        } catch (wbErr) {
          Logger.log('Products_Archive read error (' + archiveWorkbooks[aw].name + '): ' + wbErr.message);
        }
      }
    } catch (archErr) {
      Logger.log('Products_Archive iteration error (non-fatal): ' + archErr.message);
    }
    
  } catch (e) {
    Logger.log('Product lookup data load error: ' + e.message);
  }
  
  return result;
}

/**
 * Calculate Products Requested and Products Distributed for a record
 * v3.0 Logic: Check code VALUE to determine lookup path:
 *   If code IS "DR" or "PF" -> use DR/PF_Products by ID + RequestDate -> QtyRequested/QtyReceived
 *     (counted once per record even if multiple codes are DR/PF)
 *   If code is NOT "DR" or "PF" -> use Distributed_Products by BoxCode -> sum Quantity
 * Applied to Received Product Codes 1, 2, and 3
 * Products only counted for Service Status = Picked Up or Delivered
 * @param {string} id - Record ID
 * @param {string} productCode1 - Received Product Code 1
 * @param {string} productCode2 - Received Product Code 2
 * @param {string} productCode3 - Received Product Code 3
 * @param {Date} requestDate - Request Date for DR/PF_Products matching
 * @param {Object} productData - From loadProductLookupData()
 * @returns {Object} { productsRequested: n, productsDistributed: n }
 */
function calculateProductCounts(id, productCode1, productCode2, productCode3, requestDate, productData, headers, row) {
  // ── MySQL path: use pre-computed columns from sp_get_report_data ──────────
  // The stored procedure appends calc_qty_requested / calc_qty_received to every
  // applicant row, eliminating the need for dictionary lookups entirely.
  if (headers && row) {
    var reqIdx = headers.indexOf('calc_qty_requested');
    var recIdx = headers.indexOf('calc_qty_received');
    if (reqIdx !== -1 && recIdx !== -1) {
      return {
        productsRequested:  parseInt(row[reqIdx])  || 0,
        productsDistributed:parseInt(row[recIdx])  || 0
      };
    }
  }

  // ── Sheets path: original dictionary lookup ───────────────────────────────
  var requested = 0;
  var distributed = 0;
  
  var codes = [
    (productCode1 || '').toString().trim(),
    (productCode2 || '').toString().trim(),
    (productCode3 || '').toString().trim()
  ];
  var recId = (id || '').toString().trim();
  
  // Build composite key for DR/PF_Products lookup
  var dateKey = '';
  if (requestDate instanceof Date && !isNaN(requestDate.getTime())) {
    dateKey = Utilities.formatDate(requestDate, CONFIG.TIMEZONE, 'M/d/yyyy');
  }
  var compositeKey = recId + '|' + dateKey;
  
  var drPfCounted = false; // Only count DR/PF once per record
  
  for (var c = 0; c < codes.length; c++) {
    var code = codes[c];
    if (code === '') continue;
    
    var codeUpper = code.toUpperCase();
    
    if (codeUpper === 'DR' || codeUpper === 'PF') {
      // DR/PF codes: lookup by ID + Request Date in DR/PF_Products (once per record)
      if (!drPfCounted) {
        var drPf = productData.drPfByIdDate[compositeKey];
        if (drPf) {
          requested += drPf.qtyRequested;
          distributed += drPf.qtyReceived;
        }
        drPfCounted = true;
      }
    } else {
      // Non-DR/PF codes: lookup BoxCode in Distributed_Products and sum Quantity
      var dpMatch = productData.distProdByBox[code];
      if (dpMatch && dpMatch.length > 0) {
        var sum = 0;
        for (var i = 0; i < dpMatch.length; i++) sum += dpMatch[i];
        requested += sum;
        distributed += sum;
      }
    }
  }
  
  return { productsRequested: requested, productsDistributed: distributed };
}


// ============ SHARED COLUMN NAME CONSTANTS (DEPRECATED) ============
// v3.4: These string constants are retained for backward compatibility.
// Prefer using FieldMapService.getFieldRawHeader(displayLabel) for new code.
// Values here MUST match the "Raw Header" column in LU_FieldMap.

var COL_INCOME = 'Total annual household income. (How much money does your family make in 1 year?)';
var COL_USED_BEFORE = 'Have you used our services before?';
var COL_ASSISTANCE = 'Are you receiving any assistance? Please select ALL boxes that apply to ANY ASSISTANCE your family is receiving.';
var COL_BABY_BOX = 'Take Baby Box?';
var COL_EMERGENCY = 'Was there an emergency situation which caused you to contact us?';
var COL_SITUATION = 'Briefly explain your current situation.';
var COL_HELP_1 = 'How will receiving personal and home cleaning products help you?';
var COL_HELP_2 = 'If you have received cleaning products from us in the past, how has receiving these products helped you?';
var COL_PERMISSION = 'May we use the information you have provided in the 2 questions above about HOW WILL and HOW DID receiving cleaning products help? ONLY the information from these 2 areas will be shared to help us show potential donors your need is real.';
var COL_HOW_LEARNED = 'How did you learn about our program?';
var COL_PERSON_ORG = 'More information about the person or organization that referred you.';
var COL_TITLE_POS = 'What is the title or position of the person who referred you?';
var COL_WHO_REFERRED = "What is the person's name who referred you?";
var COL_MORE_INFO = 'Please tell us anything else you would like us to know about how you heard about Giving to the Nations or our program.';

/**
 * v3.4: Get the friendly display label for a raw AM column header.
 * Delegates to FieldMapService.getFieldDisplayLabelFromMap() which reads
 * from LU_FieldMap sheet. Falls back to raw field name if not mapped.
 * @param {string} fieldName - Raw AM column header or computed field name
 * @returns {string} Friendly display label
 */
function getFieldDisplayLabel(fieldName) {
  return getFieldDisplayLabelFromMap(fieldName);
}

// getApplicantType() moved to SharedUtils.gs v1.0 (#11)

// getBabyBoxIndicator() moved to SharedUtils.gs v1.0 (#11)

// getStr() moved to SharedUtils.gs v1.0 (#9)


// ============ REPORT: APPLICANTS OPEN REQUESTS ============

/**
 * Generate Applicants Open Requests report
 * Shows requests where Service Status is blank, null, or "Open"
 * Columns: Quarter, Year, Month, Day, Address1, City, State, Zip, County,
 *          Income Level, Applicant Type, Request Type, Service Status,
 *          First Name, Last Name, Children 2 and Under, Number of Households,
 *          Number of Requests, ID, Original Form ID
 * Order: Quarter, Year, Month, Day, Address1, City, State, Zip, County
 * @param {string} fromDateStr - Start date (YYYY-MM-DD)
 * @param {string} toDateStr - End date (YYYY-MM-DD)
 * @returns {Object} Result with report URL and download URL
 */
function generateApplicantsOpenRequests(fromDateStr, toDateStr) {
  try {
    if (!fromDateStr || !toDateStr) {
      return { success: false, error: 'From Date and To Date are required' };
    }
    
    var fromDate = parseDateInput(fromDateStr, false);
    var toDate = parseDateInput(toDateStr, true);
    
    if (fromDate > toDate) {
      return { success: false, error: 'From Date must be before To Date' };
    }
    
    var filters = [
      { column: 'Service Status', emptyOrValues: ['Open'] }
    ];
    
    var combined = getCombinedData(fromDate, toDate, filters);
    
    if (combined.totalCount === 0) {
      return { success: false, error: 'No open requests found for the specified date range' };
    }
    
    var headers = combined.headers;
    var rows = combined.rows;
    
    var colIdx = {
      id:            headers.indexOf(resolveAMField_('ID')),
      originalFormId:headers.indexOf(resolveAMField_('Original Form ID')),
      firstName:     headers.indexOf(resolveAMField_('First Name')),
      lastName:      headers.indexOf(resolveAMField_('Last Name')),
      requestDate:   headers.indexOf(resolveAMField_('Request Date')),
      requestType:   headers.indexOf(resolveAMField_('Request Type')),
      serviceStatus: headers.indexOf(resolveAMField_('Service Status')),
      address:       headers.indexOf(resolveAMField_('Street Address')),
      city:          headers.indexOf(resolveAMField_('City')),
      state:         headers.indexOf(resolveAMField_('State')),
      zipCode:       headers.indexOf(resolveAMField_('Zip Code')),
      county:        headers.indexOf(resolveAMField_('County')),
      incomeLevel:   headers.indexOf(resolveAMField_(COL_INCOME)),
      usedBefore:    headers.indexOf(resolveAMField_(COL_USED_BEFORE)),
      babyBox:       headers.indexOf(resolveAMField_(COL_BABY_BOX))
    };
    
    // Build records
    var records = [];
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var reqDate = new Date(row[colIdx.requestDate]);
      var firstName = getStr(row, colIdx.firstName);
      var lastName = getStr(row, colIdx.lastName);
      var clientKey = (firstName + '|' + lastName).toLowerCase();
      
      records.push({
        quarter: getQuarter(reqDate.getMonth()),
        year: reqDate.getFullYear(),
        monthNum: reqDate.getMonth(),
        month: getMonthName(reqDate.getMonth()),
        day: reqDate.getDate(),
        address1: getStr(row, colIdx.address),
        city: getStr(row, colIdx.city),
        state: getStr(row, colIdx.state),
        zip: getStr(row, colIdx.zipCode),
        county: getStr(row, colIdx.county),
        incomeLevel: getStr(row, colIdx.incomeLevel),
        applicantType: getApplicantType(getStr(row, colIdx.usedBefore)),
        requestType: getStr(row, colIdx.requestType),
        serviceStatus: getStr(row, colIdx.serviceStatus),
        firstName: firstName,
        lastName: lastName,
        children2Under: getBabyBoxIndicator(getStr(row, colIdx.babyBox)),
        clientKey: clientKey,
        id: getStr(row, colIdx.id),
        originalFormId: getStr(row, colIdx.originalFormId)
      });
    }
    
    // Sort: Quarter, Year, Month, Day, Address1, City, State, Zip, County
    records.sort(function(a, b) {
      if (a.quarter !== b.quarter) return a.quarter - b.quarter;
      if (a.year !== b.year) return a.year - b.year;
      if (a.monthNum !== b.monthNum) return a.monthNum - b.monthNum;
      if (a.day !== b.day) return a.day - b.day;
      var cmp = a.address1.localeCompare(b.address1);
      if (cmp !== 0) return cmp;
      cmp = a.city.localeCompare(b.city);
      if (cmp !== 0) return cmp;
      cmp = a.state.localeCompare(b.state);
      if (cmp !== 0) return cmp;
      cmp = a.zip.localeCompare(b.zip);
      if (cmp !== 0) return cmp;
      return a.county.localeCompare(b.county);
    });
    
    // Create spreadsheet
    var fromFormatted = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var toFormatted = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var reportName = 'Applicants_Open_Requests_' + fromFormatted + '_to_' + toFormatted;
    var spreadsheet = SpreadsheetApp.create(reportName);
    var sheet = spreadsheet.getActiveSheet();
    sheet.setName('Open Requests');
    
    moveToFolder(spreadsheet.getId(), CONFIG.GRANTS_FOLDER_ID);
    
    var dataHeaders = ['Quarter', 'Year', 'Month', 'Day', 'Address1', 'City', 'State', 'Zip',
                       'County', 'Income Level', 'Applicant Type', 'Request Type', 'Service Status',
                       'First Name', 'Last Name', 'Children 2 and Under',
                       'Number of Households', 'Number of Requests', 'ID', 'Original Form ID'];
    var reportColCount = dataHeaders.length;
    
    var currentRow = writeReportTitleSection(sheet, 'Applicants Open Requests', fromDate, toDate,
      records.length, combined.masterCount, combined.archiveCount);
    
    var headerRowNum = currentRow;
    sheet.getRange(headerRowNum, 1, 1, reportColCount).setValues([dataHeaders]);
    styleReportHeader(sheet, headerRowNum, reportColCount);
    currentRow++;
    
    // Build all data rows in memory
    var allRows = [];
    for (var i = 0; i < records.length; i++) {
      var rec = records[i];
      allRows.push([
        'Q' + rec.quarter, rec.year, rec.month, rec.day, rec.address1, rec.city, rec.state, rec.zip,
        rec.county, rec.incomeLevel, rec.applicantType, rec.requestType, rec.serviceStatus,
        rec.firstName, rec.lastName, rec.children2Under,
        1, 1, rec.id, rec.originalFormId
      ]);
    }
    
    // Batch write all data rows
    if (allRows.length > 0) {
      sheet.getRange(currentRow, 1, allRows.length, reportColCount).setValues(allRows);
      // Alternating row colors
      for (var i = 1; i < allRows.length; i += 2) {
        sheet.getRange(currentRow + i, 1, 1, reportColCount).setBackground('#f8f9fa');
      }
      currentRow += allRows.length;
    }
    
    currentRow++;
    sheet.getRange(currentRow, 1).setValue('End of Report - ' + records.length + ' open requests');
    sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(10).setFontColor('#666666');
    
    // Column widths
    sheet.setColumnWidth(1, 60); sheet.setColumnWidth(2, 50); sheet.setColumnWidth(3, 80);
    sheet.setColumnWidth(4, 40); sheet.setColumnWidth(5, 160); sheet.setColumnWidth(6, 100);
    sheet.setColumnWidth(7, 50); sheet.setColumnWidth(8, 60); sheet.setColumnWidth(9, 100);
    sheet.setColumnWidth(10, 130); sheet.setColumnWidth(11, 90); sheet.setColumnWidth(12, 100);
    sheet.setColumnWidth(13, 90); sheet.setColumnWidth(14, 100); sheet.setColumnWidth(15, 100);
    sheet.setColumnWidth(16, 120); sheet.setColumnWidth(17, 120); sheet.setColumnWidth(18, 110);
    sheet.setColumnWidth(19, 50); sheet.setColumnWidth(20, 110);
    
    sheet.setFrozenRows(headerRowNum);
    
    var fromDisplay = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    var toDisplay = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    logAudit('GRANTS_REPORT', null, 'Generated Applicants Open Requests report: ' + 
      fromDisplay + ' to ' + toDisplay + ' (' + records.length + ' records)');
    
    var reportUrl = spreadsheet.getUrl();
    var downloadUrl = 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx';
    
    return {
      success: true,
      message: 'Report generated with ' + records.length + ' open requests',
      recordCount: records.length,
      masterCount: combined.masterCount,
      archiveCount: combined.archiveCount,
      reportUrl: reportUrl,
      downloadUrl: downloadUrl,
      reportId: spreadsheet.getId()
    };
    
  } catch (error) {
    Logger.log('Applicants Open Requests report error: ' + error.message);
    return { success: false, error: 'Report generation failed: ' + error.message };
  }
}


// ============ TESTIMONIALS REPORT ============

/**
 * Generate Testimonials report
 * Columns: Quarter, Year, Month, Day, City, State, Zip, County, Income Level,
 *          Applicant Type, Request Type, Service Status, Emergency Situation,
 *          Current Situation, 1st Request-How Help, Additional Request-How Help,
 *          Permission to Share
 * Order: Quarter, Year, Month, Day, City, State, Zip, County
 * Filter: "How will receiving..." (1st Request-How Help) is not blank
 * @param {string} fromDateStr - Start date (YYYY-MM-DD)
 * @param {string} toDateStr - End date (YYYY-MM-DD)
 */
function generateTestimonialsReport(fromDateStr, toDateStr) {
  try {
    if (!fromDateStr || !toDateStr) {
      return { success: false, error: 'Both From Date and To Date are required' };
    }
    
    var fromDate = parseDateInput(fromDateStr, false);
    var toDate = parseDateInput(toDateStr, true);
    
    if (fromDate > toDate) {
      return { success: false, error: 'From Date must be before To Date' };
    }
    
    var combined = getCombinedData(fromDate, toDate);
    
    if (combined.totalCount === 0) {
      return { success: false, error: 'No records found for the specified date range' };
    }
    
    var headers = combined.headers;
    var rows = combined.rows;
    
    var colIdx = {
      requestDate:  headers.indexOf(resolveAMField_('Request Date')),
      city:         headers.indexOf(resolveAMField_('City')),
      state:        headers.indexOf(resolveAMField_('State')),
      zipCode:      headers.indexOf(resolveAMField_('Zip Code')),
      county:       headers.indexOf(resolveAMField_('County')),
      incomeLevel:  headers.indexOf(resolveAMField_(COL_INCOME)),
      usedBefore:   headers.indexOf(resolveAMField_(COL_USED_BEFORE)),
      requestType:  headers.indexOf(resolveAMField_('Request Type')),
      serviceStatus:headers.indexOf(resolveAMField_('Service Status')),
      emergency:    headers.indexOf(resolveAMField_(COL_EMERGENCY)),
      situation:    headers.indexOf(resolveAMField_(COL_SITUATION)),
      help1:        headers.indexOf(resolveAMField_(COL_HELP_1)),
      help2:        headers.indexOf(resolveAMField_(COL_HELP_2)),
      permission:   headers.indexOf(resolveAMField_(COL_PERMISSION))
    };
    
    // Build records - filter: 1st Request-How Help not blank
    var records = [];
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var help1Val = getStr(row, colIdx.help1);
      if (help1Val === '') continue;
      
      var reqDate = new Date(row[colIdx.requestDate]);
      
      records.push({
        quarter: getQuarter(reqDate.getMonth()),
        year: reqDate.getFullYear(),
        monthNum: reqDate.getMonth(),
        month: getMonthName(reqDate.getMonth()),
        day: reqDate.getDate(),
        city: getStr(row, colIdx.city),
        state: getStr(row, colIdx.state),
        zip: getStr(row, colIdx.zipCode),
        county: getStr(row, colIdx.county),
        incomeLevel: getStr(row, colIdx.incomeLevel),
        applicantType: getApplicantType(getStr(row, colIdx.usedBefore)),
        requestType: getStr(row, colIdx.requestType),
        serviceStatus: getStr(row, colIdx.serviceStatus),
        emergency: getStr(row, colIdx.emergency),
        situation: getStr(row, colIdx.situation),
        help1: help1Val,
        help2: getStr(row, colIdx.help2),
        permission: getStr(row, colIdx.permission)
      });
    }
    
    if (records.length === 0) {
      return { success: false, error: 'No records with testimonials found for the specified date range' };
    }
    
    // Sort: Quarter, Year, Month, Day, City, State, Zip, County
    records.sort(function(a, b) {
      if (a.quarter !== b.quarter) return a.quarter - b.quarter;
      if (a.year !== b.year) return a.year - b.year;
      if (a.monthNum !== b.monthNum) return a.monthNum - b.monthNum;
      if (a.day !== b.day) return a.day - b.day;
      var cmp = a.city.localeCompare(b.city);
      if (cmp !== 0) return cmp;
      cmp = a.state.localeCompare(b.state);
      if (cmp !== 0) return cmp;
      cmp = a.zip.localeCompare(b.zip);
      if (cmp !== 0) return cmp;
      return a.county.localeCompare(b.county);
    });
    
    // Create spreadsheet
    var fromFormatted = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var toFormatted = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var reportName = 'Testimonials_' + fromFormatted + '_to_' + toFormatted;
    var spreadsheet = SpreadsheetApp.create(reportName);
    var sheet = spreadsheet.getActiveSheet();
    sheet.setName('Testimonials');
    
    moveToFolder(spreadsheet.getId(), CONFIG.GRANTS_FOLDER_ID);
    
    var dataHeaders = ['Quarter', 'Year', 'Month', 'Day', 'City', 'State', 'Zip', 'County',
                       'Income Level', 'Applicant Type', 'Request Type', 'Service Status',
                       'Emergency Situation', 'Current Situation', '1st Request-How Help',
                       'Additional Request-How Help', 'Permission to Share'];
    var reportColCount = dataHeaders.length;
    
    var currentRow = writeReportTitleSection(sheet, 'Testimonials', fromDate, toDate,
      records.length, combined.masterCount, combined.archiveCount);
    
    var headerRowNum = currentRow;
    sheet.getRange(headerRowNum, 1, 1, reportColCount).setValues([dataHeaders]);
    styleReportHeader(sheet, headerRowNum, reportColCount);
    currentRow++;
    
    // Build all data rows in memory
    var allRows = [];
    for (var i = 0; i < records.length; i++) {
      var rec = records[i];
      allRows.push([
        'Q' + rec.quarter, rec.year, rec.month, rec.day, rec.city, rec.state, rec.zip, rec.county,
        rec.incomeLevel, rec.applicantType, rec.requestType, rec.serviceStatus,
        rec.emergency, rec.situation, rec.help1, rec.help2, rec.permission
      ]);
    }
    
    // Batch write all data rows
    if (allRows.length > 0) {
      sheet.getRange(currentRow, 1, allRows.length, reportColCount).setValues(allRows);
      // Alternating row colors
      for (var i = 1; i < allRows.length; i += 2) {
        sheet.getRange(currentRow + i, 1, 1, reportColCount).setBackground('#f8f9fa');
      }
      // Wrap text for long-text columns (emergency, situation, help1, help2)
      sheet.getRange(currentRow, 13, allRows.length, 4).setWrap(true);
      currentRow += allRows.length;
    }
    
    currentRow++;
    sheet.getRange(currentRow, 1).setValue('End of Report - ' + records.length + ' testimonials');
    sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(10).setFontColor('#666666');
    
    // Column widths
    sheet.setColumnWidth(1, 60); sheet.setColumnWidth(2, 50); sheet.setColumnWidth(3, 80);
    sheet.setColumnWidth(4, 40); sheet.setColumnWidth(5, 100); sheet.setColumnWidth(6, 50);
    sheet.setColumnWidth(7, 60); sheet.setColumnWidth(8, 100); sheet.setColumnWidth(9, 130);
    sheet.setColumnWidth(10, 90); sheet.setColumnWidth(11, 100); sheet.setColumnWidth(12, 90);
    sheet.setColumnWidth(13, 200); sheet.setColumnWidth(14, 250); sheet.setColumnWidth(15, 300);
    sheet.setColumnWidth(16, 300); sheet.setColumnWidth(17, 120);
    
    sheet.setFrozenRows(headerRowNum);
    
    var fromDisplay = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    var toDisplay = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    logAudit('GRANTS_REPORT', null, 'Generated Testimonials report: ' + 
      fromDisplay + ' to ' + toDisplay + ' (' + records.length + ' records)');
    
    var reportUrl = spreadsheet.getUrl();
    var downloadUrl = 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx';
    
    return {
      success: true, message: 'Report generated with ' + records.length + ' testimonials',
      recordCount: records.length, masterCount: combined.masterCount,
      archiveCount: combined.archiveCount, reportUrl: reportUrl,
      downloadUrl: downloadUrl, reportId: spreadsheet.getId()
    };
    
  } catch (error) {
    Logger.log('Testimonials report error: ' + error.message);
    return { success: false, error: 'Report generation failed: ' + error.message };
  }
}


// ============ REQUEST FREQUENCY REPORT ============

/**
 * Generate Request Frequency report
 * Columns: Quarter, Year, Month, Day, City, State, Zip, County, Income Level,
 *          Request Type, Service Status, Number of Requests, Avg Frequency Days
 * Order: Quarter, Year, Month, Day, City, State, Zip, County
 * Filter: Service Status = "Picked Up" or "Delivered"
 * Avg Frequency Days: Per applicant (First+Last Name), average days between
 *   current Request Date and prior request's Last Date Served
 * @param {string} fromDateStr - Start date (YYYY-MM-DD)
 * @param {string} toDateStr - End date (YYYY-MM-DD)
 */
function generateRequestFrequencyReport(fromDateStr, toDateStr) {
  try {
    if (!fromDateStr || !toDateStr) {
      return { success: false, error: 'Both From Date and To Date are required' };
    }
    
    var fromDate = parseDateInput(fromDateStr, false);
    var toDate = parseDateInput(toDateStr, true);
    
    if (fromDate > toDate) {
      return { success: false, error: 'From Date must be before To Date' };
    }

    // ── MySQL path: sp_get_request_frequency_data does all the grouping/LAG work ──
    if (CONFIG.DB && CONFIG.DB.USE_MYSQL) {
      try {
        var fromStr = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
        var toStr   = Utilities.formatDate(toDate,   CONFIG.TIMEZONE, 'yyyy-MM-dd');
        var freqData = DbService.getRequestFrequencyData(fromStr, toStr);

        if (!freqData || !freqData.rows || freqData.rows.length === 0) {
          return { success: false, error: 'No records found for the specified date range with Picked Up or Delivered status' };
        }

        // freqData.rows is a 2D array; headers map: calc_quarter(0), calc_year(1),
        // calc_month_num(2), calc_month_name(3), calc_day(4), city(5), state(6),
        // zip_code(7), county(8), annual_income(9), request_type(10),
        // service_status(11), request_count(12), avg_freq_days(13)
        var hdrs = freqData.headers || [];
        var qi   = hdrs.indexOf('calc_quarter');
        var yi   = hdrs.indexOf('calc_year');
        var mni  = hdrs.indexOf('calc_month_name');
        var di   = hdrs.indexOf('calc_day');
        var cii  = hdrs.indexOf('city');
        var sti  = hdrs.indexOf('state');
        var zii  = hdrs.indexOf('zip_code');
        var coi  = hdrs.indexOf('county');
        var inci = hdrs.indexOf('annual_income');
        var rti  = hdrs.indexOf('request_type');
        var ssi  = hdrs.indexOf('service_status');
        var rci  = hdrs.indexOf('request_count');
        var afi  = hdrs.indexOf('avg_freq_days');

        var records = freqData.rows.map(function(r) {
          return {
            quarter:       r[qi]  !== null ? 'Q' + r[qi]  : '',
            year:          r[yi]  !== null ? r[yi]         : '',
            month:         r[mni] || '',
            day:           r[di]  !== null ? r[di]         : '',
            city:          r[cii] || '', state:  r[sti] || '',
            zip:           r[zii] || '', county: r[coi] || '',
            incomeLevel:   r[inci] || '',
            requestType:   r[rti]  || '',
            serviceStatus: r[ssi]  || '',
            requestCount:  r[rci]  || 0,
            avgFreqDays:   r[afi] !== null ? r[afi] : ''
          };
        });

        var totalRequestCount = records.reduce(function(s, r) { return s + (r.requestCount || 0); }, 0);
        return _writeRequestFrequencySpreadsheet_(records, totalRequestCount, freqData.totalCount, 0, fromDate, toDate);
      } catch (e) {
        Logger.log('generateRequestFrequencyReport MySQL error (falling back to Sheets): ' + e.message);
        // Fall through to Sheets path
      }
    }

    // ── Sheets path ─────────────────────────────────────────────────────────
    var filters = [
      { column: 'Service Status', values: ['Picked Up', 'Delivered'] }
    ];
    
    var combined = getCombinedData(fromDate, toDate, filters);
    
    if (combined.totalCount === 0) {
      return { success: false, error: 'No records found for the specified date range with Picked Up or Delivered status' };
    }
    
    var headers = combined.headers;
    var rows = combined.rows;
    
    var colIdx = {
      requestDate:    headers.indexOf(resolveAMField_('Request Date')),
      lastDateServed: headers.indexOf(resolveAMField_('Last Date Served')),
      serviceStatus:  headers.indexOf(resolveAMField_('Service Status')),
      county:         headers.indexOf(resolveAMField_('County')),
      zipCode:        headers.indexOf(resolveAMField_('Zip Code')),
      city:           headers.indexOf(resolveAMField_('City')),
      state:          headers.indexOf(resolveAMField_('State')),
      incomeLevel:    headers.indexOf(resolveAMField_(COL_INCOME)),
      requestType:    headers.indexOf(resolveAMField_('Request Type')),
      firstName:      headers.indexOf(resolveAMField_('First Name')),
      lastName:       headers.indexOf(resolveAMField_('Last Name'))
    };
    
    // Step 1: Collect all raw records per applicant to compute frequency
    var applicantRequests = {}; // clientKey -> [{reqDate, lastDateServed, groupKey, ...}]
    var rawRecords = [];
    
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var reqDate = new Date(row[colIdx.requestDate]);
      var firstName = getStr(row, colIdx.firstName);
      var lastName = getStr(row, colIdx.lastName);
      var clientKey = (firstName + '|' + lastName).toLowerCase();
      
      var lastServed = null;
      if (colIdx.lastDateServed !== -1 && row[colIdx.lastDateServed]) {
        var ls = new Date(row[colIdx.lastDateServed]);
        if (!isNaN(ls.getTime())) lastServed = ls;
      }
      
      var quarter = getQuarter(reqDate.getMonth());
      var year = reqDate.getFullYear();
      var monthNum = reqDate.getMonth();
      var day = reqDate.getDate();
      var city = getStr(row, colIdx.city);
      var state = getStr(row, colIdx.state);
      var zip = getStr(row, colIdx.zipCode);
      var county = getStr(row, colIdx.county);
      var incomeLevel = getStr(row, colIdx.incomeLevel);
      var requestType = getStr(row, colIdx.requestType);
      var serviceStatus = getStr(row, colIdx.serviceStatus);
      
      var groupKey = quarter + '|' + year + '|' + monthNum + '|' + day + '|' + city + '|' + state + '|' + zip + '|' + county + '|' + incomeLevel + '|' + requestType + '|' + serviceStatus;
      
      var rec = {
        clientKey: clientKey, reqDate: reqDate, lastDateServed: lastServed,
        groupKey: groupKey, quarter: quarter, year: year, monthNum: monthNum,
        month: getMonthName(monthNum), day: day, city: city, state: state,
        zip: zip, county: county, incomeLevel: incomeLevel,
        requestType: requestType, serviceStatus: serviceStatus
      };
      
      rawRecords.push(rec);
      
      if (!applicantRequests[clientKey]) applicantRequests[clientKey] = [];
      applicantRequests[clientKey].push(rec);
    }
    
    // Step 2: For each applicant, sort by request date and compute frequency gaps
    // Frequency = days between this Request Date and prior request's Last Date Served
    var recordFreqDays = {}; // index in rawRecords -> freqDays or null
    
    for (var ck in applicantRequests) {
      var reqs = applicantRequests[ck];
      reqs.sort(function(a, b) { return a.reqDate - b.reqDate; });
      
      for (var r = 0; r < reqs.length; r++) {
        var freqDays = null;
        if (r > 0 && reqs[r - 1].lastDateServed) {
          var diffMs = reqs[r].reqDate.getTime() - reqs[r - 1].lastDateServed.getTime();
          freqDays = Math.round(diffMs / (1000 * 60 * 60 * 24));
          if (freqDays < 0) freqDays = null;
        }
        reqs[r]._freqDays = freqDays;
      }
    }
    
    // Step 3: Group by report dimensions
    var groups = {};
    
    for (var i = 0; i < rawRecords.length; i++) {
      var rec = rawRecords[i];
      var key = rec.groupKey;
      
      if (!groups[key]) {
        groups[key] = {
          quarter: rec.quarter, year: rec.year, monthNum: rec.monthNum,
          month: rec.month, day: rec.day, city: rec.city, state: rec.state,
          zip: rec.zip, county: rec.county, incomeLevel: rec.incomeLevel,
          requestType: rec.requestType, serviceStatus: rec.serviceStatus,
          count: 0, freqDays: []
        };
      }
      
      groups[key].count++;
      if (rec._freqDays !== null && rec._freqDays !== undefined) {
        groups[key].freqDays.push(rec._freqDays);
      }
    }
    
    // Convert to sorted array
    var records = [];
    for (var key in groups) {
      var g = groups[key];
      var avgDays = '';
      if (g.freqDays.length > 0) {
        var sum = 0;
        for (var d = 0; d < g.freqDays.length; d++) sum += g.freqDays[d];
        avgDays = Math.round((sum / g.freqDays.length) * 10) / 10;
      }
      records.push({
        quarter: g.quarter, year: g.year, monthNum: g.monthNum, month: g.month,
        day: g.day, city: g.city, state: g.state, zip: g.zip, county: g.county,
        incomeLevel: g.incomeLevel, requestType: g.requestType, serviceStatus: g.serviceStatus,
        requestCount: g.count, avgFreqDays: avgDays
      });
    }
    
    // Sort: Quarter, Year, Month, Day, City, State, Zip, County
    records.sort(function(a, b) {
      if (a.quarter !== b.quarter) return a.quarter - b.quarter;
      if (a.year !== b.year) return a.year - b.year;
      if (a.monthNum !== b.monthNum) return a.monthNum - b.monthNum;
      if (a.day !== b.day) return a.day - b.day;
      var cmp = a.city.localeCompare(b.city);
      if (cmp !== 0) return cmp;
      cmp = a.state.localeCompare(b.state);
      if (cmp !== 0) return cmp;
      cmp = a.zip.localeCompare(b.zip);
      if (cmp !== 0) return cmp;
      return a.county.localeCompare(b.county);
    });
    
    // Convert grouped results to records array
    var totalRequestCount = 0;
    for (var i = 0; i < records.length; i++) {
      totalRequestCount += records[i].requestCount;
    }

    return _writeRequestFrequencySpreadsheet_(records, totalRequestCount, combined.masterCount, combined.archiveCount, fromDate, toDate);

  } catch (error) {
    Logger.log('Request Frequency report error: ' + error.message);
    return { success: false, error: 'Report generation failed: ' + error.message };
  }
}

/**
 * Writes the Request Frequency spreadsheet and returns the result object.
 * Shared by the MySQL and Sheets paths of generateRequestFrequencyReport.
 * @private
 */
function _writeRequestFrequencySpreadsheet_(records, totalRequestCount, masterCount, archiveCount, fromDate, toDate) {
    var fromFormatted = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var toFormatted   = Utilities.formatDate(toDate,   CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var reportName    = 'Request_Frequency_' + fromFormatted + '_to_' + toFormatted;
    var spreadsheet   = SpreadsheetApp.create(reportName);
    var sheet         = spreadsheet.getActiveSheet();
    sheet.setName('Request Frequency');

    moveToFolder(spreadsheet.getId(), CONFIG.GRANTS_FOLDER_ID);

    var dataHeaders = ['Quarter', 'Year', 'Month', 'Day', 'City', 'State', 'Zip', 'County',
                       'Income Level', 'Request Type', 'Service Status',
                       'Number of Requests', 'Avg Frequency Days'];
    var reportColCount = dataHeaders.length;

    var currentRow = writeReportTitleSection(sheet, 'Request Frequency', fromDate, toDate,
      totalRequestCount, masterCount, archiveCount);

    var headerRowNum = currentRow;
    sheet.getRange(headerRowNum, 1, 1, reportColCount).setValues([dataHeaders]);
    styleReportHeader(sheet, headerRowNum, reportColCount);
    currentRow++;

    var allRows = [];
    for (var i = 0; i < records.length; i++) {
      var rec = records[i];
      allRows.push([
        rec.quarter, rec.year, rec.month, rec.day,
        rec.city, rec.state, rec.zip, rec.county,
        rec.incomeLevel, rec.requestType, rec.serviceStatus,
        rec.requestCount, rec.avgFreqDays
      ]);
    }

    if (allRows.length > 0) {
      sheet.getRange(currentRow, 1, allRows.length, reportColCount).setValues(allRows);
      for (var i = 1; i < allRows.length; i += 2) {
        sheet.getRange(currentRow + i, 1, 1, reportColCount).setBackground('#f8f9fa');
      }
      currentRow += allRows.length;
    }

    currentRow++;
    sheet.getRange(currentRow, 1).setValue('End of Report - ' + records.length + ' grouped rows, ' + totalRequestCount + ' total requests');
    sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(10).setFontColor('#666666');

    sheet.setColumnWidth(1, 60);  sheet.setColumnWidth(2, 50);  sheet.setColumnWidth(3, 80);
    sheet.setColumnWidth(4, 40);  sheet.setColumnWidth(5, 100); sheet.setColumnWidth(6, 50);
    sheet.setColumnWidth(7, 60);  sheet.setColumnWidth(8, 100); sheet.setColumnWidth(9, 130);
    sheet.setColumnWidth(10, 100); sheet.setColumnWidth(11, 90); sheet.setColumnWidth(12, 110);
    sheet.setColumnWidth(13, 120);
    sheet.setFrozenRows(headerRowNum);

    var fromDisplay = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    var toDisplay   = Utilities.formatDate(toDate,   CONFIG.TIMEZONE, 'M/d/yyyy');
    logAudit('GRANTS_REPORT', null, 'Generated Request Frequency report: ' +
      fromDisplay + ' to ' + toDisplay + ' (' + totalRequestCount + ' requests, ' + records.length + ' groups)');

    var reportUrl   = spreadsheet.getUrl();
    var downloadUrl = 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx';

    return {
      success: true,
      message: 'Report generated with ' + totalRequestCount + ' requests in ' + records.length + ' groups',
      recordCount: totalRequestCount, masterCount: masterCount,
      archiveCount: archiveCount, reportUrl: reportUrl,
      downloadUrl: downloadUrl, reportId: spreadsheet.getId()
    };
}


// ============ HOUSEHOLDS REPORT ============

/**
 * Generate Households report with full age bracket detail
 * Columns: Quarter, Year, Month, Day, City, State, Zip, County, Applicant Type,
 *          Request Type, Service Status, Military Status, Race, Ethnicity,
 *          Homeless, Employed, Income Level, Income Source Type,
 *          Number of Households, Number of Requests, Products Requested,
 *          Products Distributed, Total People, Total Children, Male Children,
 *          Female Children, Children 2 and Under, Under 5, 5-9, 10-14, 15-17,
 *          Total Adults, Male Adults, Female Adults, 18-24, 25-34, 35-44,
 *          45-54, 55-64, Total Seniors, Male Seniors, Female Seniors, 65-84, 85 & Up
 * Order: Quarter, Year, Month, Day, City, State, Zip, County
 * Filter: Service Status = "Picked Up" or "Delivered"
 * @param {string} fromDateStr - Start date (YYYY-MM-DD)
 * @param {string} toDateStr - End date (YYYY-MM-DD)
 */
function generateHouseholdsReport(fromDateStr, toDateStr) {
  try {
    if (!fromDateStr || !toDateStr) {
      return { success: false, error: 'Both From Date and To Date are required' };
    }
    
    var fromDate = parseDateInput(fromDateStr, false);
    var toDate = parseDateInput(toDateStr, true);
    
    if (fromDate > toDate) {
      return { success: false, error: 'From Date must be before To Date' };
    }
    
    var filters = [
      { column: 'Service Status', values: ['Picked Up', 'Delivered'] }
    ];
    
    var combined = getCombinedData(fromDate, toDate, filters);
    
    if (combined.totalCount === 0) {
      return { success: false, error: 'No records found for the specified date range with Picked Up or Delivered status' };
    }
    
    var headers = combined.headers;
    var rows = combined.rows;
    
    var productData = loadProductLookupData(fromDate, toDate);
    
    var colIdx = {
      id:             headers.indexOf(resolveAMField_('ID')),
      requestDate:    headers.indexOf(resolveAMField_('Request Date')),
      firstName:      headers.indexOf(resolveAMField_('First Name')),
      lastName:       headers.indexOf(resolveAMField_('Last Name')),
      county:         headers.indexOf(resolveAMField_('County')),
      zipCode:        headers.indexOf(resolveAMField_('Zip Code')),
      city:           headers.indexOf(resolveAMField_('City')),
      state:          headers.indexOf(resolveAMField_('State')),
      serviceStatus:  headers.indexOf(resolveAMField_('Service Status')),
      incomeLevel:    headers.indexOf(resolveAMField_(COL_INCOME)),
      usedBefore:     headers.indexOf(resolveAMField_(COL_USED_BEFORE)),
      requestType:    headers.indexOf(resolveAMField_('Request Type')),
      militaryStatus: headers.indexOf(resolveAMField_('Military Status')),
      race:           headers.indexOf(resolveAMField_('Please Select Your Racial Category')),
      ethnicity:      headers.indexOf(resolveAMField_('Please Select Your Ethnic Category')),
      homeless:       headers.indexOf(resolveAMField_('Are you currently homeless?')),
      employed:       headers.indexOf(resolveAMField_('Are you currently employed?')),
      assistance:     headers.indexOf(resolveAMField_(COL_ASSISTANCE)),
      babyBox:        headers.indexOf(resolveAMField_(COL_BABY_BOX)),
      productCode1:   headers.indexOf(resolveAMField_('Received Product Code 1')),
      productCode2:   headers.indexOf(resolveAMField_('Received Product Code 2')),
      productCode3:   headers.indexOf(resolveAMField_('Received Product Code 3'))
    };
    
    // Build row-level records
    var records = [];
    
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var reqDate = new Date(row[colIdx.requestDate]);
      var ages = calculateDetailedAgeBrackets(headers, row);
      
      var firstName = getStr(row, colIdx.firstName);
      var lastName = getStr(row, colIdx.lastName);
      var clientKey = (firstName + '|' + lastName).toLowerCase();
      
      var recId = getStr(row, colIdx.id);
      var code1 = getStr(row, colIdx.productCode1);
      var code2 = getStr(row, colIdx.productCode2);
      var code3 = getStr(row, colIdx.productCode3);
      var products = calculateProductCounts(recId, code1, code2, code3, reqDate, productData, headers, row);
      
      // Split Income Source Type into separate line per item
      var rawAssistance = getStr(row, colIdx.assistance);
      var sourceTypes = rawAssistance.indexOf(', ') !== -1 ? rawAssistance.split(', ') : [rawAssistance];
      
      var baseRec = {
        quarter: getQuarter(reqDate.getMonth()),
        year: reqDate.getFullYear(),
        monthNum: reqDate.getMonth(),
        month: getMonthName(reqDate.getMonth()),
        day: reqDate.getDate(),
        city: getStr(row, colIdx.city),
        state: getStr(row, colIdx.state),
        zip: getStr(row, colIdx.zipCode),
        county: getStr(row, colIdx.county),
        applicantType: getApplicantType(getStr(row, colIdx.usedBefore)),
        requestType: getStr(row, colIdx.requestType),
        serviceStatus: getStr(row, colIdx.serviceStatus),
        militaryStatus: getStr(row, colIdx.militaryStatus),
        race: getStr(row, colIdx.race),
        ethnicity: getStr(row, colIdx.ethnicity),
        homeless: getStr(row, colIdx.homeless),
        employed: getStr(row, colIdx.employed),
        incomeLevel: getStr(row, colIdx.incomeLevel),
        clientKey: clientKey,
        children2Under: getBabyBoxIndicator(getStr(row, colIdx.babyBox)),
        productsRequested: products.productsRequested,
        productsDistributed: products.productsDistributed,
        ages: ages
      };
      
      for (var s = 0; s < sourceTypes.length; s++) {
        var rec = {};
        for (var key in baseRec) rec[key] = baseRec[key];
        rec.incomeSourceType = sourceTypes[s].trim();
        records.push(rec);
      }
    }
    
    // Sort: Quarter, Year, Month, Day, City, State, Zip, County
    records.sort(function(a, b) {
      if (a.quarter !== b.quarter) return a.quarter - b.quarter;
      if (a.year !== b.year) return a.year - b.year;
      if (a.monthNum !== b.monthNum) return a.monthNum - b.monthNum;
      if (a.day !== b.day) return a.day - b.day;
      var cmp = a.city.localeCompare(b.city);
      if (cmp !== 0) return cmp;
      cmp = a.state.localeCompare(b.state);
      if (cmp !== 0) return cmp;
      cmp = a.zip.localeCompare(b.zip);
      if (cmp !== 0) return cmp;
      return a.county.localeCompare(b.county);
    });
    
    
    // Create spreadsheet
    var fromFormatted = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var toFormatted = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var reportName = 'Households_' + fromFormatted + '_to_' + toFormatted;
    var spreadsheet = SpreadsheetApp.create(reportName);
    var sheet = spreadsheet.getActiveSheet();
    sheet.setName('Households');
    
    moveToFolder(spreadsheet.getId(), CONFIG.GRANTS_FOLDER_ID);
    
    var dataHeaders = ['Quarter', 'Year', 'Month', 'Day', 'City', 'State', 'Zip', 'County',
                       'Applicant Type', 'Request Type', 'Service Status', 'Military Status',
                       'Race', 'Ethnicity', 'Homeless', 'Employed', 'Income Level', 'Income Source Type',
                       'Number of Households', 'Number of Requests', 'Products Requested', 'Products Distributed',
                       'Total People', 'Total Children', 'Male Children', 'Female Children',
                       'Children 2 and Under', 'Under 5', '5-9', '10-14', '15-17',
                       'Total Adults', 'Male Adults', 'Female Adults',
                       '18-24', '25-34', '35-44', '45-54', '55-64',
                       'Total Seniors', 'Male Seniors', 'Female Seniors', '65-84', '85 & Up'];
    var reportColCount = dataHeaders.length;
    
    var currentRow = writeReportTitleSection(sheet, 'Households', fromDate, toDate,
      combined.totalCount, combined.masterCount, combined.archiveCount);
    
    var headerRowNum = currentRow;
    sheet.getRange(headerRowNum, 1, 1, reportColCount).setValues([dataHeaders]);
    styleReportHeader(sheet, headerRowNum, reportColCount);
    currentRow++;
    
    // Build all data rows in memory
    var allRows = [];
    for (var i = 0; i < records.length; i++) {
      var rec = records[i];
      var a = rec.ages;
      allRows.push([
        'Q' + rec.quarter, rec.year, rec.month, rec.day, rec.city, rec.state, rec.zip, rec.county,
        rec.applicantType, rec.requestType, rec.serviceStatus, rec.militaryStatus,
        rec.race, rec.ethnicity, rec.homeless, rec.employed, rec.incomeLevel, rec.incomeSourceType,
        1, 1, rec.productsRequested, rec.productsDistributed,
        a.people, a.totalChildren, a.maleChildren, a.femaleChildren,
        rec.children2Under, a.under5, a.aged5t9, a.aged10t14, a.aged15t17,
        a.totalAdults, a.maleAdults, a.femaleAdults,
        a.aged18t24, a.aged25t34, a.aged35t44, a.aged45t54, a.aged55t64,
        a.totalSeniors, a.maleSeniors, a.femaleSeniors, a.aged65t84, a.aged85up
      ]);
    }
    
    // Batch write all data rows
    if (allRows.length > 0) {
      sheet.getRange(currentRow, 1, allRows.length, reportColCount).setValues(allRows);
      // Alternating row colors
      for (var i = 1; i < allRows.length; i += 2) {
        sheet.getRange(currentRow + i, 1, 1, reportColCount).setBackground('#f8f9fa');
      }
      currentRow += allRows.length;
    }
    
    currentRow++;
    sheet.getRange(currentRow, 1).setValue('End of Report - ' + records.length + ' rows, ' + combined.totalCount + ' source records');
    sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(10).setFontColor('#666666');
    
    // Column widths
    for (var c = 1; c <= 4; c++) sheet.setColumnWidth(c, c === 3 ? 80 : (c === 1 ? 60 : (c === 2 ? 50 : 40)));
    sheet.setColumnWidth(5, 100); sheet.setColumnWidth(6, 50); sheet.setColumnWidth(7, 60); sheet.setColumnWidth(8, 100);
    for (var c = 9; c <= 18; c++) sheet.setColumnWidth(c, c === 18 ? 150 : (c >= 12 ? 100 : 90));
    for (var c = 19; c <= 22; c++) sheet.setColumnWidth(c, 100);
    for (var c = 23; c <= 44; c++) sheet.setColumnWidth(c, 70);
    
    sheet.setFrozenRows(headerRowNum);
    
    var fromDisplay = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    var toDisplay = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    logAudit('GRANTS_REPORT', null, 'Generated Households report: ' + 
      fromDisplay + ' to ' + toDisplay + ' (' + combined.totalCount + ' records, ' + records.length + ' rows)');
    
    var reportUrl = spreadsheet.getUrl();
    var downloadUrl = 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx';
    
    return {
      success: true,
      message: 'Report generated with ' + combined.totalCount + ' records in ' + records.length + ' rows',
      recordCount: combined.totalCount, masterCount: combined.masterCount,
      archiveCount: combined.archiveCount, reportUrl: reportUrl,
      downloadUrl: downloadUrl, reportId: spreadsheet.getId()
    };
    
  } catch (error) {
    Logger.log('Households report error: ' + error.message);
    return { success: false, error: 'Report generation failed: ' + error.message };
  }
}


// ============ DISTRIBUTION STATS REPORT ============

/**
 * Generate Distribution Stats report with full demographics, referral, and product detail
 * Columns: Quarter, Year, Month, Day, ID, First Name, Last Name, City, State, Zip, County,
 *          Funding Code, Request Type, Service Status, Final Service Contact Date,
 *          Received Product Code 1, Received Product Code 2, Received Product Code 3,
 *          Products Distributed, Program How Learned, Person/Organization,
 *          Title/Position, Who Referred-Name, More Information-How Learned,
 *          Military Status, Race, Ethnicity, Homeless, Employed, Income Level,
 *          Income Source Type, Number of Households, Number of Requests,
 *          Total People, Total Children, Male Children, Female Children,
 *          Children 2 and Under, Under 5, 5-9, 10-14, 15-17,
 *          Total Adults, Male Adults, Female Adults, 18-24, 25-34, 35-44, 45-54, 55-64,
 *          Total Seniors, Male Seniors, Female Seniors, 65-84, 85 & Up
 * Order: Quarter, Year, Month, Day, First Name, Last Name, City, State, Zip, County
 * No filter on Service Status
 * @param {string} fromDateStr - Start date (YYYY-MM-DD)
 * @param {string} toDateStr - End date (YYYY-MM-DD)
 */
function generateDistributionStatsReport(fromDateStr, toDateStr) {
  try {
    if (!fromDateStr || !toDateStr) {
      return { success: false, error: 'Both From Date and To Date are required' };
    }
    
    var fromDate = parseDateInput(fromDateStr, false);
    var toDate = parseDateInput(toDateStr, true);
    
    if (fromDate > toDate) {
      return { success: false, error: 'From Date must be before To Date' };
    }
    
    var combined = getCombinedData(fromDate, toDate);
    
    if (combined.totalCount === 0) {
      return { success: false, error: 'No records found for the specified date range' };
    }
    
    var headers = combined.headers;
    var rows = combined.rows;
    
    var productData = loadProductLookupData(fromDate, toDate);
    
    var colIdx = {
      id:              headers.indexOf(resolveAMField_('ID')),
      requestDate:     headers.indexOf(resolveAMField_('Request Date')),
      firstName:       headers.indexOf(resolveAMField_('First Name')),
      lastName:        headers.indexOf(resolveAMField_('Last Name')),
      county:          headers.indexOf(resolveAMField_('County')),
      zipCode:         headers.indexOf(resolveAMField_('Zip Code')),
      city:            headers.indexOf(resolveAMField_('City')),
      state:           headers.indexOf(resolveAMField_('State')),
      serviceStatus:   headers.indexOf(resolveAMField_('Service Status')),
      fundingCode:     headers.indexOf(resolveAMField_('Funding Code')),
      incomeLevel:     headers.indexOf(resolveAMField_(COL_INCOME)),
      militaryStatus:  headers.indexOf(resolveAMField_('Military Status')),
      race:            headers.indexOf(resolveAMField_('Please Select Your Racial Category')),
      ethnicity:       headers.indexOf(resolveAMField_('Please Select Your Ethnic Category')),
      homeless:        headers.indexOf(resolveAMField_('Are you currently homeless?')),
      employed:        headers.indexOf(resolveAMField_('Are you currently employed?')),
      requestType:     headers.indexOf(resolveAMField_('Request Type')),
      finalServiceDate:headers.indexOf(resolveAMField_('Final Service Contact Date')),
      productCode1:    headers.indexOf(resolveAMField_('Received Product Code 1')),
      productCode2:    headers.indexOf(resolveAMField_('Received Product Code 2')),
      productCode3:    headers.indexOf(resolveAMField_('Received Product Code 3')),
      assistance:      headers.indexOf(resolveAMField_(COL_ASSISTANCE)),
      babyBox:         headers.indexOf(resolveAMField_(COL_BABY_BOX)),
      howLearned:      headers.indexOf(resolveAMField_(COL_HOW_LEARNED)),
      personOrg:       headers.indexOf(resolveAMField_(COL_PERSON_ORG)),
      titlePos:        headers.indexOf(resolveAMField_(COL_TITLE_POS)),
      whoReferred:     headers.indexOf(resolveAMField_(COL_WHO_REFERRED)),
      moreInfo:        headers.indexOf(resolveAMField_(COL_MORE_INFO))
    };
    
    var records = [];
    
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var reqDate = new Date(row[colIdx.requestDate]);
      var ages = calculateDetailedAgeBrackets(headers, row);
      
      var firstName = getStr(row, colIdx.firstName);
      var lastName = getStr(row, colIdx.lastName);
      var clientKey = (firstName + '|' + lastName).toLowerCase();
      
      var recId = getStr(row, colIdx.id);
      var code1 = getStr(row, colIdx.productCode1);
      var code2 = getStr(row, colIdx.productCode2);
      var code3 = getStr(row, colIdx.productCode3);
      
      // Products only counted for Picked Up or Delivered
      var svcStatus = getStr(row, colIdx.serviceStatus);
      var svcUpper = svcStatus.toUpperCase();
      var products = { productsRequested: 0, productsDistributed: 0 };
      if (svcUpper === 'PICKED UP' || svcUpper === 'DELIVERED') {
        products = calculateProductCounts(recId, code1, code2, code3, reqDate, productData, headers, row);
      }
      
      var finalDate = '';
      if (colIdx.finalServiceDate !== -1 && row[colIdx.finalServiceDate]) {
        var fd = new Date(row[colIdx.finalServiceDate]);
        if (!isNaN(fd.getTime())) finalDate = Utilities.formatDate(fd, CONFIG.TIMEZONE, 'M/d/yyyy');
      }
      
      // Split Income Source Type
      var rawAssistance = getStr(row, colIdx.assistance);
      var sourceTypes = rawAssistance.indexOf(', ') !== -1 ? rawAssistance.split(', ') : [rawAssistance];
      
      var baseRec = {
        quarter: getQuarter(reqDate.getMonth()),
        year: reqDate.getFullYear(),
        monthNum: reqDate.getMonth(),
        month: getMonthName(reqDate.getMonth()),
        day: reqDate.getDate(),
        id: recId, firstName: firstName, lastName: lastName,
        city: getStr(row, colIdx.city), state: getStr(row, colIdx.state),
        zip: getStr(row, colIdx.zipCode), county: getStr(row, colIdx.county),
        fundingCode: getStr(row, colIdx.fundingCode),
        requestType: getStr(row, colIdx.requestType),
        serviceStatus: svcStatus, finalServiceDate: finalDate,
        code1: code1, code2: code2, code3: code3,
        productsDistributed: products.productsDistributed,
        howLearned: getStr(row, colIdx.howLearned),
        personOrg: getStr(row, colIdx.personOrg),
        titlePos: getStr(row, colIdx.titlePos),
        whoReferred: getStr(row, colIdx.whoReferred),
        moreInfo: getStr(row, colIdx.moreInfo),
        militaryStatus: getStr(row, colIdx.militaryStatus),
        race: getStr(row, colIdx.race),
        ethnicity: getStr(row, colIdx.ethnicity),
        homeless: getStr(row, colIdx.homeless),
        employed: getStr(row, colIdx.employed),
        incomeLevel: getStr(row, colIdx.incomeLevel),
        clientKey: clientKey,
        children2Under: getBabyBoxIndicator(getStr(row, colIdx.babyBox)),
        ages: ages
      };
      
      for (var s = 0; s < sourceTypes.length; s++) {
        var rec = {};
        for (var key in baseRec) rec[key] = baseRec[key];
        rec.incomeSourceType = sourceTypes[s].trim();
        records.push(rec);
      }
    }
    
    // Sort: Quarter, Year, Month, Day, First Name, Last Name, City, State, Zip, County
    records.sort(function(a, b) {
      if (a.quarter !== b.quarter) return a.quarter - b.quarter;
      if (a.year !== b.year) return a.year - b.year;
      if (a.monthNum !== b.monthNum) return a.monthNum - b.monthNum;
      if (a.day !== b.day) return a.day - b.day;
      var cmp = a.firstName.localeCompare(b.firstName);
      if (cmp !== 0) return cmp;
      cmp = a.lastName.localeCompare(b.lastName);
      if (cmp !== 0) return cmp;
      cmp = a.city.localeCompare(b.city);
      if (cmp !== 0) return cmp;
      cmp = a.state.localeCompare(b.state);
      if (cmp !== 0) return cmp;
      cmp = a.zip.localeCompare(b.zip);
      if (cmp !== 0) return cmp;
      return a.county.localeCompare(b.county);
    });
    
    
    // Create spreadsheet
    var fromFormatted = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var toFormatted = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var reportName = 'Distribution_Stats_' + fromFormatted + '_to_' + toFormatted;
    var spreadsheet = SpreadsheetApp.create(reportName);
    var sheet = spreadsheet.getActiveSheet();
    sheet.setName('Distribution Stats');
    
    moveToFolder(spreadsheet.getId(), CONFIG.GRANTS_FOLDER_ID);
    
    var dataHeaders = [
      'Quarter', 'Year', 'Month', 'Day', 'ID', 'First Name', 'Last Name',
      'City', 'State', 'Zip', 'County', 'Funding Code', 'Request Type', 'Service Status',
      'Final Service Contact Date', 'Received Product Code 1', 'Received Product Code 2',
      'Received Product Code 3', 'Products Distributed',
      'Program How Learned', 'Person/Organization', 'Title/Position',
      'Who Referred-Name', 'More Information-How Learned',
      'Military Status', 'Race', 'Ethnicity', 'Homeless', 'Employed',
      'Income Level', 'Income Source Type',
      'Number of Households', 'Number of Requests',
      'Total People', 'Total Children', 'Male Children', 'Female Children',
      'Children 2 and Under', 'Under 5', '5-9', '10-14', '15-17',
      'Total Adults', 'Male Adults', 'Female Adults',
      '18-24', '25-34', '35-44', '45-54', '55-64',
      'Total Seniors', 'Male Seniors', 'Female Seniors', '65-84', '85 & Up'
    ];
    var reportColCount = dataHeaders.length;
    
    var currentRow = writeReportTitleSection(sheet, 'Distribution Stats', fromDate, toDate,
      records.length, combined.masterCount, combined.archiveCount);
    
    var headerRowNum = currentRow;
    sheet.getRange(headerRowNum, 1, 1, reportColCount).setValues([dataHeaders]);
    styleReportHeader(sheet, headerRowNum, reportColCount);
    currentRow++;
    
    // Build all data rows in memory
    var allRows = [];
    for (var i = 0; i < records.length; i++) {
      var rec = records[i];
      var a = rec.ages;
      allRows.push([
        'Q' + rec.quarter, rec.year, rec.month, rec.day, rec.id, rec.firstName, rec.lastName,
        rec.city, rec.state, rec.zip, rec.county, rec.fundingCode, rec.requestType, rec.serviceStatus,
        rec.finalServiceDate, rec.code1, rec.code2, rec.code3, rec.productsDistributed,
        rec.howLearned, rec.personOrg, rec.titlePos, rec.whoReferred, rec.moreInfo,
        rec.militaryStatus, rec.race, rec.ethnicity, rec.homeless, rec.employed,
        rec.incomeLevel, rec.incomeSourceType,
        1, 1,
        a.people, a.totalChildren, a.maleChildren, a.femaleChildren,
        rec.children2Under, a.under5, a.aged5t9, a.aged10t14, a.aged15t17,
        a.totalAdults, a.maleAdults, a.femaleAdults,
        a.aged18t24, a.aged25t34, a.aged35t44, a.aged45t54, a.aged55t64,
        a.totalSeniors, a.maleSeniors, a.femaleSeniors, a.aged65t84, a.aged85up
      ]);
    }
    
    // Batch write all data rows
    if (allRows.length > 0) {
      sheet.getRange(currentRow, 1, allRows.length, reportColCount).setValues(allRows);
      // Alternating row colors
      for (var i = 1; i < allRows.length; i += 2) {
        sheet.getRange(currentRow + i, 1, 1, reportColCount).setBackground('#f8f9fa');
      }
      currentRow += allRows.length;
    }
    
    currentRow++;
    sheet.getRange(currentRow, 1).setValue('End of Report - ' + records.length + ' records');
    sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(10).setFontColor('#666666');
    
    // Column widths - compact for the many columns
    sheet.setColumnWidth(1, 60); sheet.setColumnWidth(2, 50); sheet.setColumnWidth(3, 80); sheet.setColumnWidth(4, 40);
    sheet.setColumnWidth(5, 50); sheet.setColumnWidth(6, 100); sheet.setColumnWidth(7, 100);
    sheet.setColumnWidth(8, 100); sheet.setColumnWidth(9, 50); sheet.setColumnWidth(10, 60); sheet.setColumnWidth(11, 100);
    sheet.setColumnWidth(12, 90); sheet.setColumnWidth(13, 100); sheet.setColumnWidth(14, 90);
    sheet.setColumnWidth(15, 120); sheet.setColumnWidth(16, 110); sheet.setColumnWidth(17, 110); sheet.setColumnWidth(18, 110);
    sheet.setColumnWidth(19, 100);
    for (var c = 20; c <= 24; c++) sheet.setColumnWidth(c, 130);
    for (var c = 25; c <= 31; c++) sheet.setColumnWidth(c, 100);
    for (var c = 32; c <= 55; c++) sheet.setColumnWidth(c, 70);
    
    sheet.setFrozenRows(headerRowNum);
    
    var fromDisplay = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    var toDisplay = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    logAudit('GRANTS_REPORT', null, 'Generated Distribution Stats report: ' + 
      fromDisplay + ' to ' + toDisplay + ' (' + records.length + ' records)');
    
    var reportUrl = spreadsheet.getUrl();
    var downloadUrl = 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx';
    
    return {
      success: true, message: 'Report generated with ' + records.length + ' records',
      recordCount: records.length, masterCount: combined.masterCount,
      archiveCount: combined.archiveCount, reportUrl: reportUrl,
      downloadUrl: downloadUrl, reportId: spreadsheet.getId()
    };
    
  } catch (error) {
    Logger.log('Distribution Stats report error: ' + error.message);
    return { success: false, error: 'Report generation failed: ' + error.message };
  }
}


// ============ GRANT SUMMARY HELPERS ============

/**
 * Load Income Level options from LU_IncomeLevels lookup sheet
 * Returns array of income level strings for dropdown population
 * @returns {string[]} Array of income level values
 */
function getIncomeLevelOptions() {
  try {
    var ss = getLookupsWorkbook();
    var sheet = ss.getSheetByName(CONFIG.LOOKUPS.INCOME_LEVELS);
    if (!sheet) return [];
    var data = sheet.getRange(2, 1, sheet.getLastRow() - 1, 1).getValues();
    var levels = [];
    for (var i = 0; i < data.length; i++) {
      var val = String(data[i][0]).trim();
      if (val !== '') levels.push(val);
    }
    return levels;
  } catch (e) {
    Logger.log('Error loading income levels: ' + e.message);
    return [];
  }
}

/**
 * Parse the low-end dollar amount from an income level string
 * Examples:
 *   "No Income" → 0
 *   "Less than $10,000" → 0
 *   "$10,000 - $19,999" → 10000
 *   "$30,000 - $39,999" → 30000
 *   "$80,000 or More" → 80000
 * @param {string} str - Income level string
 * @returns {number} Low-end dollar amount
 */
function parseIncomeLowEnd(str) {
  if (!str) return -1;
  var s = str.toLowerCase().trim();
  if (s.indexOf('no income') !== -1 || s.indexOf('less than') !== -1 || s.indexOf('under') !== -1) {
    return 0;
  }
  // Extract first dollar amount: find $X,XXX pattern
  var match = str.match(/\$([0-9,]+)/);
  if (match) {
    return parseInt(match[1].replace(/,/g, ''), 10);
  }
  return -1;
}


// ============ GRANT SUMMARY (NARRATIVE) REPORT ============

/**
 * Generate Grant Summary narrative report
 * Template-based narrative summarizing households, income percentages,
 * age group counts, and product totals
 * Filters: Date range + optional County/City/Zip (comma-separated multi-value)
 * Filter: Service Status = "Picked Up" or "Delivered"
 * Income threshold: Uses low end of selected income level to determine
 *   what % of households fall below that threshold, with per-range breakdowns
 * @param {string} fromDateStr - Start date (YYYY-MM-DD)
 * @param {string} toDateStr - End date (YYYY-MM-DD)
 * @param {string} filterCounty - Optional county filter (comma-separated for multiple)
 * @param {string} filterCity - Optional city filter (comma-separated for multiple)
 * @param {string} filterZip - Optional zip filter (comma-separated for multiple)
 * @param {string} incomeLevel - Selected income level threshold (e.g., "$30,000 - $39,999")
 * @returns {Object} Result with narrative text and report URL
 */
function generateGrantSummaryReport(fromDateStr, toDateStr, filterCounty, filterCity, filterZip, incomeLevel) {
  try {
    if (!fromDateStr || !toDateStr) {
      return { success: false, error: 'Both From Date and To Date are required' };
    }
    
    var fromDate = parseDateInput(fromDateStr, false);
    var toDate = parseDateInput(toDateStr, true);
    
    if (fromDate > toDate) {
      return { success: false, error: 'From Date must be before To Date' };
    }

    // ── MySQL path: sp_get_grant_summary_stats returns pre-aggregated results ──
    if (CONFIG.DB && CONFIG.DB.USE_MYSQL) {
      try {
        var fromStr   = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
        var toStr     = Utilities.formatDate(toDate,   CONFIG.TIMEZONE, 'yyyy-MM-dd');
        var countyCsv = (filterCounty || '').trim() || null;
        var cityCsv   = (filterCity   || '').trim() || null;
        var zipCsv    = (filterZip    || '').trim() || null;

        var stats = DbService.getGrantSummaryStats(fromStr, toStr, countyCsv, cityCsv, zipCsv);
        if (!stats) throw new Error('getGrantSummaryStats returned null');

        if (stats.householdCount === 0) {
          return { success: false, error: 'No records found for the specified filters and date range' };
        }

        // Build incomeCounts map from incomeBreakdown array
        var incomeCounts = {};
        var totalIncomeRecords = 0;
        (stats.incomeBreakdown || []).forEach(function(b) {
          if (b.incomeLevel !== '') {
            incomeCounts[b.incomeLevel] = b.incomeCount;
            totalIncomeRecords += b.incomeCount;
          }
        });

        return _buildGrantSummaryDoc_(
          fromDate, toDate, filterCounty, filterCity, filterZip, incomeLevel,
          stats.householdCount, stats.totalRequests,
          stats.totalChildren, stats.totalAdults, stats.totalSeniors,
          stats.totalProductsDistributed,
          incomeCounts, totalIncomeRecords);
      } catch (e) {
        Logger.log('generateGrantSummaryReport MySQL error (falling back to Sheets): ' + e.message);
        // Fall through to Sheets path
      }
    }

    // ── Sheets path ─────────────────────────────────────────────────────────
    var filters = [
      { column: 'Service Status', values: ['Picked Up', 'Delivered'] }
    ];
    
    // Add geographic filters if provided (support comma-separated multi-values)
    if (filterCounty && filterCounty.trim() !== '') {
      var counties = filterCounty.split(',').map(function(v) { return v.trim(); }).filter(function(v) { return v !== ''; });
      if (counties.length > 0) filters.push({ column: 'County', values: counties });
    }
    if (filterCity && filterCity.trim() !== '') {
      var cities = filterCity.split(',').map(function(v) { return v.trim(); }).filter(function(v) { return v !== ''; });
      if (cities.length > 0) filters.push({ column: 'City', values: cities });
    }
    if (filterZip && filterZip.trim() !== '') {
      var zips = filterZip.split(',').map(function(v) { return v.trim(); }).filter(function(v) { return v !== ''; });
      if (zips.length > 0) filters.push({ column: 'Zip Code', values: zips });
    }
    
    var combined = getCombinedData(fromDate, toDate, filters);
    
    if (combined.totalCount === 0) {
      return { success: false, error: 'No records found for the specified filters and date range' };
    }
    
    var headers = combined.headers;
    var rows = combined.rows;
    
    var productData = loadProductLookupData(fromDate, toDate);
    
    var colIdx = {
      id:           headers.indexOf(resolveAMField_('ID')),
      requestDate:  headers.indexOf(resolveAMField_('Request Date')),
      firstName:    headers.indexOf(resolveAMField_('First Name')),
      lastName:     headers.indexOf(resolveAMField_('Last Name')),
      incomeLevel:  headers.indexOf(resolveAMField_(COL_INCOME)),
      serviceStatus:headers.indexOf(resolveAMField_('Service Status')),
      productCode1: headers.indexOf(resolveAMField_('Received Product Code 1')),
      productCode2: headers.indexOf(resolveAMField_('Received Product Code 2')),
      productCode3: headers.indexOf(resolveAMField_('Received Product Code 3'))
    };
    
    // Compute metrics
    var uniqueHouseholds = new Set();
    var incomeCounts = {};
    var totalChildren = 0, totalAdults = 0, totalSeniors = 0;
    var totalProductsDistributed = 0;
    
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var firstName = getStr(row, colIdx.firstName);
      var lastName = getStr(row, colIdx.lastName);
      var clientKey = (firstName + '|' + lastName).toLowerCase();
      uniqueHouseholds.add(clientKey);
      
      // Income levels
      var income = getStr(row, colIdx.incomeLevel);
      if (income !== '') {
        if (!incomeCounts[income]) incomeCounts[income] = 0;
        incomeCounts[income]++;
      }
      
      // Age brackets
      var ages = calculateAgeBrackets(headers, row, 64);
      totalChildren += ages.children;
      totalAdults += ages.adults;
      totalSeniors += ages.seniors;
      
      // Products
      var recId = getStr(row, colIdx.id);
      var reqDate = new Date(row[colIdx.requestDate]);
      var code1 = getStr(row, colIdx.productCode1);
      var code2 = getStr(row, colIdx.productCode2);
      var code3 = getStr(row, colIdx.productCode3);
      var products = calculateProductCounts(recId, code1, code2, code3, reqDate, productData, headers, row);
      totalProductsDistributed += products.productsDistributed;
    }
    
    var householdCount = uniqueHouseholds.size;
    var totalIncomeRecords = 0;
    for (var k in incomeCounts) totalIncomeRecords += incomeCounts[k];

    return _buildGrantSummaryDoc_(
      fromDate, toDate, filterCounty, filterCity, filterZip, incomeLevel,
      householdCount, combined.totalCount,
      totalChildren, totalAdults, totalSeniors,
      totalProductsDistributed,
      incomeCounts, totalIncomeRecords);

  } catch (error) {
    Logger.log('Grant Summary report error: ' + error.message);
    return { success: false, error: 'Report generation failed: ' + error.message };
  }
}

/**
 * Builds the Grant Summary Google Doc from aggregated metrics.
 * Shared by the MySQL and Sheets paths of generateGrantSummaryReport.
 * @private
 */
function _buildGrantSummaryDoc_(fromDate, toDate, filterCounty, filterCity, filterZip, incomeLevel,
    householdCount, totalCount, totalChildren, totalAdults, totalSeniors,
    totalProductsDistributed, incomeCounts, totalIncomeRecords) {

    // Determine income threshold from selected income level
    var selectedIncomeLevel = incomeLevel || '$30,000 - $39,999';
    var threshold = parseIncomeLowEnd(selectedIncomeLevel);
    if (threshold < 0) threshold = 30000;
    var thresholdFormatted = '$' + threshold.toLocaleString();

    // Determine which income levels fall below the threshold
    var belowThresholdLevels = [];
    var belowThresholdCount = 0;
    var incomeArr = [];
    for (var k in incomeCounts) {
      var lowEnd = parseIncomeLowEnd(k);
      var isBelow = (lowEnd >= 0 && lowEnd < threshold);
      incomeArr.push({ level: k, count: incomeCounts[k], lowEnd: lowEnd, isBelow: isBelow });
      if (isBelow) {
        belowThresholdCount += incomeCounts[k];
        belowThresholdLevels.push({ level: k, count: incomeCounts[k] });
      }
    }

    // Sort all income levels by low end ascending for display
    incomeArr.sort(function(a, b) { return a.lowEnd - b.lowEnd; });
    belowThresholdLevels.sort(function(a, b) {
      return parseIncomeLowEnd(a.level) - parseIncomeLowEnd(b.level);
    });
    
    // Overall % of households below threshold
    var belowThresholdPct = householdCount > 0 ? Math.round((belowThresholdCount / householdCount) * 100) : 0;
    
    // Build below-threshold breakdown lines
    var incomeLines = [];
    for (var j = 0; j < belowThresholdLevels.length; j++) {
      var pct = belowThresholdCount > 0 ? Math.round((belowThresholdLevels[j].count / belowThresholdCount) * 100) : 0;
      incomeLines.push(pct + '% ' + belowThresholdLevels[j].level);
    }
    
    // Date range description
    var fromDisplay = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'MMMM yyyy');
    var toDisplay = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'MMMM yyyy');
    
    // Geographic filter description
    var geoFilter = '';
    if (filterCounty && filterCounty.trim() !== '') geoFilter += filterCounty.trim();
    if (filterCity   && filterCity.trim()   !== '') geoFilter += (geoFilter ? ', ' : '') + filterCity.trim();
    if (filterZip    && filterZip.trim()    !== '') geoFilter += (geoFilter ? ', Zip ' : 'Zip ') + filterZip.trim();
    var geoDesc = geoFilter ? ' in ' + geoFilter : '';
    
    // Build narrative
    var narrative = 'Assessing the ' + householdCount.toLocaleString() + ' households our Healthy Essentials Pantry has served' +
      geoDesc + ' from ' + fromDisplay + ' to ' + toDisplay + ', ' +
      belowThresholdPct + '% of the households served had an annual income of less than ' + thresholdFormatted + '. ' +
      (incomeLines.length > 0 ? 'Within these households, ' + incomeLines.join(', ') + '. ' : '') +
      'We positively impacted the lives of ' + totalChildren.toLocaleString() + ' children, ' +
      totalAdults.toLocaleString() + ' adults, and ' + totalSeniors.toLocaleString() + ' seniors with ' +
      totalProductsDistributed.toLocaleString() + ' hygiene products. ' +
      'We believe each item represents not just cleanliness, but a step toward restoring dignity and promoting health.';
    
    // Create a Google Doc with the narrative
    var fromFormatted = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var toFormatted   = Utilities.formatDate(toDate,   CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var reportName    = 'Grant_Summary_' + fromFormatted + '_to_' + toFormatted;
    var doc  = DocumentApp.create(reportName);
    var body = doc.getBody();
    
    body.appendParagraph('Giving to the Nations').setHeading(DocumentApp.ParagraphHeading.HEADING1);
    body.appendParagraph('Grant Summary').setHeading(DocumentApp.ParagraphHeading.HEADING2);
    body.appendParagraph('Report Period: ' + fromDisplay + ' to ' + toDisplay);
    if (geoFilter) body.appendParagraph('Filter: ' + geoFilter);
    body.appendParagraph('Income Threshold: Less than ' + thresholdFormatted + ' (from ' + selectedIncomeLevel + ')');
    body.appendParagraph('');
    body.appendParagraph(narrative);
    body.appendParagraph('');
    
    body.appendParagraph('Summary Data').setHeading(DocumentApp.ParagraphHeading.HEADING3);
    var table = body.appendTable();
    var headerRow = table.appendTableRow();
    headerRow.appendTableCell('Metric').setBackgroundColor('#4a86e8');
    headerRow.appendTableCell('Value').setBackgroundColor('#4a86e8');
    
    var metrics = [
      ['Households Served',                      householdCount.toLocaleString()],
      ['Total Requests',                         totalCount.toLocaleString()],
      ['Households Below ' + thresholdFormatted, belowThresholdCount.toLocaleString() + ' (' + belowThresholdPct + '%)'],
      ['Children',                               totalChildren.toLocaleString()],
      ['Adults',                                 totalAdults.toLocaleString()],
      ['Seniors',                                totalSeniors.toLocaleString()],
      ['Products Distributed',                   totalProductsDistributed.toLocaleString()]
    ];
    
    for (var m = 0; m < metrics.length; m++) {
      var dataRow = table.appendTableRow();
      dataRow.appendTableCell(metrics[m][0]);
      dataRow.appendTableCell(metrics[m][1]);
    }
    
    body.appendParagraph('');
    body.appendParagraph('Income Level Breakdown (threshold: ' + thresholdFormatted + ')').setHeading(DocumentApp.ParagraphHeading.HEADING3);
    var incTable  = body.appendTable();
    var incHeader = incTable.appendTableRow();
    incHeader.appendTableCell('Income Level').setBackgroundColor('#4a86e8');
    incHeader.appendTableCell('Count').setBackgroundColor('#4a86e8');
    incHeader.appendTableCell('% of All').setBackgroundColor('#4a86e8');
    incHeader.appendTableCell('% of Below Threshold').setBackgroundColor('#4a86e8');
    
    for (var j = 0; j < incomeArr.length; j++) {
      var pctAll   = totalIncomeRecords > 0 ? Math.round((incomeArr[j].count / totalIncomeRecords) * 100) : 0;
      var pctBelow = (incomeArr[j].isBelow && belowThresholdCount > 0)
        ? Math.round((incomeArr[j].count / belowThresholdCount) * 100) + '%' : '';
      var iRow = incTable.appendTableRow();
      iRow.appendTableCell(incomeArr[j].level);
      iRow.appendTableCell(incomeArr[j].count.toString());
      iRow.appendTableCell(pctAll + '%');
      iRow.appendTableCell(pctBelow);
    }
    
    doc.saveAndClose();
    moveToFolder(doc.getId(), CONFIG.GRANTS_FOLDER_ID);
    
    logAudit('GRANTS_REPORT', null, 'Generated Grant Summary report: ' + 
      fromDisplay + ' to ' + toDisplay + geoDesc + ' (' + householdCount + ' households)');
    
    var reportUrl   = doc.getUrl();
    var downloadUrl = 'https://docs.google.com/document/d/' + doc.getId() + '/export?format=docx';
    
    return {
      success:      true,
      message:      'Grant Summary generated for ' + householdCount + ' households',
      narrative:    narrative,
      recordCount:  totalCount,
      reportUrl:    reportUrl,
      downloadUrl:  downloadUrl,
      reportId:     doc.getId()
    };
}
