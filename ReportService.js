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
 *   - File Cleanup: Deletes report files older than 1 month (excl. Distribution);
 *                  Distribution folder deletes files for inactive SchedDisbCodes
 *
 * v4.3 - Hygiene Box Distribution Stats: finds sheet by distribution code
 *         (tries full code then generic), writes by header name:
 *         Completed in Spreadsheet, Distribution Date Range, Scheduled
 *         Distrib Code, Total Scheduled-Recipients/Boxes, With Baby Box,
 *         Picked Up-Generic/Baby, Restock-# Recipients, % No Pick Up
 * v4.4 - Fixed Hygiene Stats column name: Restock-# Recipients (dash not =)
 * v5.40 - executeArchiveBatch(): added syncArchiveHeaders_() + remapRowToHeaders_()
 *         before every archive write. syncArchiveHeaders_() appends any AM columns
 *         missing from Archive. remapRowToHeaders_() remaps each AM row to Archive
 *         column order by header name — eliminates positional write risk entirely.
 * v5.41 - syncArchiveHeaders_(): added rename detection via LU_FieldMap
 *         byPreviousHeaderName. Renames Archive column headers in place when a
 *         rename is recorded in Previous Header Name column; falls back to append.
 * v5.42 - generateRestockReport(): response now includes totalRows, centers[]
 *         per-center array, grandTotalScheduled, grandTotalRestock, grandAvgPct.
 * v5.43 - diagProductCounts(): diagnostic report for AP → Grants tab. For a
 *         given date range, pulls all Picked Up/Delivered records and shows
 *         per-record, per-box-code actual product count vs expected. Flags
 *         missing box codes in Distributed_Products, missing DR/PF entries,
 *         and any code-slot that resolved to zero products. Writes a Google
 *         Sheet to the Grants folder with three tabs: Summary (box codes vs
 *         expected), Detail (one row per record per code slot), Mismatches
 *         (records whose totals don't match expected).
 * v5.44 - diagProductCounts(): added expected counts for box code E (10) and
 *         DG1 (18). Added Income Level Bracket summary block as the uppermost
 *         section of the Summary tab — count of Picked Up/Delivered records
 *         per income bracket, sorted low-to-high, NONE first.
 * v5.45 - diagProductCounts(): restructured as crosstab. Summary tab: one row
 *         per income level, columns = Record Count + one column per unique box
 *         code; cell = count of records at that level that used that code;
 *         difference column flags where code count < record count. Mismatches
 *         tab: one row per record with at least one code mismatch; columns =
 *         ID, First Name, Last Name, Income Level, Code 1, Code 2, Code 3 +
 *         per-code actual vs expected. Detail tab retained as full row-level log.
 * v5.46 - diagProductCounts(): added Tab 4 "Gaps by Income Level". For each
 *         income level × box code combination where the matched record count
 *         is less than the income level record count, lists each gap record
 *         with ID, First Name, Last Name, Code 1, Code 2, Code 3 and the
 *         specific issue (code absent from slots, count mismatch, or NOT IN
 *         Distributed_Products). Group summary columns (income level, box code,
 *         gap count) appear only on the first row of each group.
 * v5.47 - diagProductCounts(): added Tab 5 "All Slots Trace". For every gap
 *         record, traces all three code slots through the product calc logic.
 * v5.48 - All Slots Trace: per-cell issue highlighting.
 * v5.49 - Tab 5 replaced with "Gap Root Cause". One row per unique gap record.
 * v5.50 - Complete rebuild of diagProductCounts. Replaced flawed cross-record
 *         crosstab approach (which produced spurious "gaps" whenever records
 *         used different box codes) with per-record slot analysis. Each record
 *         is evaluated against only its own Code 1/2/3. Results: OK, WRONG
 *         COUNT, NOT FOUND, DEDUPED, BABY CODE, DR/PF, EMPTY. Issues = NOT
 *         FOUND or WRONG COUNT only. Three tabs: Summary (by income level),
 *         All Records (every record with slot results), Issues Only (filtered).
 * v5.51 - diagProductCounts() rebuilt around the Grant Summary code path.
 * v5.52 - diagProductCounts(): added Last Date Served, Processed By, Entered By.
 * v5.53 - Fixed blank values for Processed By / Entered By via SV Field ID lookup.
 * v5.54 - Split Missing Records into two tabs: "Needs Attention" (codes present
 *         but productsDistributed=0 — actionable) and "By Design" (baby-only or
 *         no codes — correct behaviour, not errors). Summary updated to show
 *         both counts separately. Clarified notes so baby-only records are
 *         clearly labelled "correct — no action needed".
 * v5.55 - generateSchedulingReport(): fixed History flag and LU_SchedID integration.
 *         History now uses Request Date (not AM ID) as the recency criterion.
 *         beginId from LU_SchedID now drives Tier 1 history (rowId < beginId =
 *         from a prior run = always History). Sort updated to Request Date asc.
 */

'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// Age bracket pair map for combined M+F columns
// Maps each [Calc] column key to the two raw AM headers whose values are summed.
// Used by _buildSchedRow_() to resolve combined bracket columns.
// ─────────────────────────────────────────────────────────────────────────────

/** @type {Object.<string, {male: string, female: string}>} */
var SCHED_AGE_BRACKET_MAP_ = {
    '[Calc] Under 5': { male: '[Males under 5]', female: '[Females under 5]' },
    '[Calc] 5-9': { male: '[Males 5-9]', female: '[Females 5-9]' },
    '[Calc] 10-14': { male: '[Males 10-14]', female: '[Females 10-14]' },
    '[Calc] 15-17': { male: '[Males 15-17]', female: '[Females 15-17]' },
    '[Calc] 18-24': { male: '[Males 18-24]', female: '[Females 18-24]' },
    '[Calc] 25-34': { male: '[Males 25-34]', female: '[Females 25-34]' },
    '[Calc] 35-44': { male: '[Males 35-44]', female: '[Females 35-44]' },
    '[Calc] 45-54': { male: '[Males 45-54]', female: '[Females 45-54]' },
    '[Calc] 55-64': { male: '[Males 55-64]', female: '[Females 55-64]' },
    '[Calc] 65-84': { male: '[Males 65-84]', female: '[Females 65-84]' },
    '[Calc] 85 and Over': { male: '[Males 85 and Over]', female: '[Females 85 and Over]' }
};

/**
 * v5.19 hardcoded column fallback for the Scheduling Report.
 * Returned by _getSchedFallbackCols_() when LU_ReportColumns is empty.
 * These exactly match the v5.19 reportColumns + ageBracketPairs + additionalColumns
 * arrays, reshaped to {key, label, width, wrapText} format.
 *
 * Once LU_ReportColumns is populated, this function is never called.
 *
 * @private
 * @returns {Object[]} [{key, label, width, wrapText}]
 */
function _getSchedFallbackCols_() {
    var raw = [
        // History flag (special — not an AM column)
        { key: 'History', width: 55 },
        // Report columns (first block)
        { key: 'Scheduled Distribution Code', width: 99 },
        { key: 'Service Status', width: 91 },
        { key: 'Admin Notes', width: 200 },
        { key: 'Take Baby Box?', width: 44 },
        { key: 'ID', width: 59 },
        { key: 'Request Type', width: 63 },
        { key: 'Request Date', width: 85 },
        { key: 'Last Date Served', width: 106 },
        { key: 'Next Service Availability Date', width: 101 },
        { key: 'Last Name', width: 95 },
        { key: 'First Name', width: 75 },
        { key: 'More information about the person or organization that referred you.', width: 172 },
        { key: 'What is the title or position of the person who referred you?', width: 172 },
        { key: 'How did you learn about our program?', width: 128 },
        { key: "What is the person's name who referred you?", width: 128 },
        { key: 'Please tell us anything else you would like us to know about how you heard about Giving to the Nations or our program.', width: 244 },
        { key: 'Are you currently homeless?', width: 87 },
        { key: 'Street Address', width: 145 },
        { key: 'Apartment #, Upper, Lower, or Lot #', width: 84 },
        { key: 'City', width: 76 },
        { key: 'State', width: 66 },
        { key: 'County', width: 57 },
        { key: 'Address type?', width: 126 },
        { key: 'Phone Number', width: 94 },
        { key: 'Phone type?', width: 79 },
        { key: 'Email', width: 186 },
        { key: 'Best contact method (What is the best way to get ahold of you?)', width: 141 },
        // Age bracket columns (combined M+F)
        { key: '[Calc] Under 5', width: 52 },
        { key: '[Calc] 5-9', width: 26 },
        { key: '[Calc] 10-14', width: 41 },
        { key: '[Calc] 15-17', width: 41 },
        { key: '[Calc] 18-24', width: 41 },
        { key: '[Calc] 25-34', width: 41 },
        { key: '[Calc] 35-44', width: 41 },
        { key: '[Calc] 45-54', width: 41 },
        { key: '[Calc] 55-64', width: 41 },
        { key: '[Calc] 65-84', width: 41 },
        { key: '[Calc] 85 and Over', width: 77 },
        // Additional columns (after age brackets)
        { key: 'Are you receiving any assistance? Please select ALL boxes that apply to ANY ASSISTANCE your family is receiving.', width: 221, wrapText: true },
        { key: 'Are you currently employed?', width: 86 },
        { key: 'Total annual household income. (How much money does your family make in 1 year?)', width: 172 },
        { key: 'Was there an emergency situation which caused you to contact us?', width: 97 },
        { key: 'Briefly explain your current situation.', width: 732, wrapText: true },
        { key: 'How will receiving personal and home cleaning products help you?', width: 732, wrapText: true }
    ];
    return raw.map(function (d) {
        return {
            key: d.key,
            // getReportHeader_() resolves LU_FieldMap Report Header; 'History' has no LU entry so passes through
            label: getReportHeader_(d.key) || d.key,
            width: d.width || 70,
            wrapText: d.wrapText || false
        };
    });
}

/**
 * Build a single Scheduling Report output row from a record and column definitions.
 * @param {Object}  rec        - { rowData: Array, isHistory: boolean }
 * @param {Object[]} cols      - From getReportColumns('Scheduling') or _getSchedFallbackCols_()
 * @param {Object}  colIndexMap- { rawKey: colIndex } built from AM headers
 * @returns {Array} Single row of cell values
 */
function _buildSchedRow_(rec, cols, colIndexMap) {
    return cols.map(function (col) {
        var key = col.key;

        // ── History flag ──────────────────────────────────────────────────────
        if (key === 'History') {
            return rec.isHistory ? 'History' : '';
        }

        // ── Combined age bracket (M + F) ──────────────────────────────────────
        var bracketPair = SCHED_AGE_BRACKET_MAP_[key];
        if (bracketPair) {
            var mi = colIndexMap[bracketPair.male];
            var fi = colIndexMap[bracketPair.female];
            var mv = mi !== undefined ? (parseInt(rec.rowData[mi]) || 0) : 0;
            var fv = fi !== undefined ? (parseInt(rec.rowData[fi]) || 0) : 0;
            var tot = mv + fv;
            return tot > 0 ? tot : '';  // blank (not 0) when no one in this bracket
        }

        // ── Direct AM column lookup ───────────────────────────────────────────
        var idx = colIndexMap[key];
        if (idx === undefined) return '';

        var val = rec.rowData[idx];
        if (val instanceof Date) {
            return Utilities.formatDate(val, CONFIG.TIMEZONE, 'M/d/yyyy');
        }
        if (key === 'Scheduled Distribution Code' && val) {
            return val.toString().toUpperCase();
        }
        if (key === 'Phone Number') {
            return formatPhoneNumber_(val);
        }
        return val || '';
    });
}

/**
 * Get all ACTIVE Scheduled Distribution Codes for data validation dropdowns.
 * @returns {string[]} Sorted array of active SchedDisbCode strings
 */
function getAllDistribCodes_() {
    try {
        // getActiveSchedDisbCodes() is in LookupService.gs — reads LU_SchedDisbCodes,
        // filters Active = TRUE, returns [{code, startDate, interval, ...}]
        var activeCodes = getActiveSchedDisbCodes();
        if (!activeCodes || activeCodes.length === 0) return [];
        return activeCodes
            .map(function (c) { return (c.code || '').toString().trim().toUpperCase(); })
            .filter(function (c) { return c !== ''; })
            .sort();
    } catch (e) {
        Logger.log('getAllDistribCodes_ error (non-fatal): ' + e.message);
        return [];
    }
}

/**
 * Generate a Returned to Stock (Restock) Products report.
 * @returns {Object} { success, centerCount, totalRestockUnits, reportUrl, downloadUrl, error }
 */
function generateRestockReport() {
    try {
        if (!CONFIG.HYGIENE_STATS_WORKBOOK_ID) {
            return { success: false, error: 'HYGIENE_STATS_WORKBOOK_ID not configured.' };
        }

        // ── Step 1: Load LU_SchedDisbCodes → { code: {box1,box2,box3} } ─────
        var lookupsWb = getLookupsWorkbook();
        var schedSheet = lookupsWb.getSheetByName(CONFIG.LOOKUPS.SCHED_DISB_CODES);
        if (!schedSheet) return { success: false, error: 'LU_SchedDisbCodes sheet not found.' };

        var schedData = schedSheet.getDataRange().getValues();
        var schedHeaders = trimHeaders(schedData[0]);
        var scCode = schedHeaders.indexOf('SchedDisbCode');
        var scBox1 = schedHeaders.indexOf('Box 1'); if (scBox1 === -1) scBox1 = schedHeaders.indexOf('Box1');
        var scBox2 = schedHeaders.indexOf('Box 2'); if (scBox2 === -1) scBox2 = schedHeaders.indexOf('Box2');
        var scBox3 = schedHeaders.indexOf('Box 3'); if (scBox3 === -1) scBox3 = schedHeaders.indexOf('Box3');
        var scGeneric = schedHeaders.indexOf('Generic Distribution Code');
        if (scCode === -1) return { success: false, error: 'SchedDisbCode column not found in LU_SchedDisbCodes.' };

        // Build index: SchedDisbCode (upper) → {box1,box2,box3,genericCode}
        // Also index by generic code as fallback
        var codeIndex = {};  // full code → boxes
        var genericIndex = {}; // generic code → boxes (last one wins — same generic=same boxes)
        for (var si = 1; si < schedData.length; si++) {
            var code = (schedData[si][scCode] || '').toString().trim().toUpperCase();
            if (!code) continue;
            var entry = {
                box1: scBox1 !== -1 ? (schedData[si][scBox1] || '').toString().trim() : '',
                box2: scBox2 !== -1 ? (schedData[si][scBox2] || '').toString().trim() : '',
                box3: scBox3 !== -1 ? (schedData[si][scBox3] || '').toString().trim() : '',
                genericCode: scGeneric !== -1 ? (schedData[si][scGeneric] || '').toString().trim() : ''
            };
            codeIndex[code] = entry;
            if (entry.genericCode) genericIndex[entry.genericCode.toUpperCase()] = entry;
        }

        // ── Step 2: Load Distributed_Products → { boxCode: [{name,qty}] } ──
        var dataWb = getDataWorkbook();
        var dpSheet = dataWb.getSheetByName(CONFIG.DATA_SHEETS.DISTRIBUTED_PRODUCTS);
        if (!dpSheet || dpSheet.getLastRow() < 2) {
            return { success: false, error: 'Distributed_Products sheet not found or empty.' };
        }

        var dpData = dpSheet.getDataRange().getValues();
        var dpHeaders = trimHeaders(dpData[0]);
        var dpBoxCol = dpHeaders.indexOf('BoxCode');
        var dpNameCol = dpHeaders.indexOf('ProductName');
        var dpQtyCol = dpHeaders.indexOf('Quantity');
        if (dpQtyCol === -1) dpQtyCol = dpHeaders.indexOf('QtyPerBox');
        if (dpQtyCol === -1) dpQtyCol = dpHeaders.indexOf('Qty');

        if (dpBoxCol === -1 || dpNameCol === -1 || dpQtyCol === -1) {
            return { success: false, error: 'Distributed_Products missing required columns (BoxCode, ProductName, Quantity).' };
        }

        // Build product map: boxCode (upper) → [{productName, qtyPerBox}]
        var productMap = {};
        for (var di = 1; di < dpData.length; di++) {
            var bc = (dpData[di][dpBoxCol] || '').toString().trim().toUpperCase();
            var name = (dpData[di][dpNameCol] || '').toString().trim();
            var qty = parseFloat(dpData[di][dpQtyCol]) || 0;
            if (!bc || !name) continue;
            if (!productMap[bc]) productMap[bc] = [];
            productMap[bc].push({ productName: name, qtyPerBox: qty });
        }

        // ── Step 3: Read Hygiene Stats workbook ──────────────────────────────
        var statsWB = SpreadsheetApp.openById(CONFIG.HYGIENE_STATS_WORKBOOK_ID);
        var sheets = statsWB.getSheets();

        var detailRows = []; // {center, schedCode, dateRange, box, productName, restockRecipients, qtyPerBox, totalQty}
        var noBoxWarnings = [];

        sheets.forEach(function (sheet) {
            var centerName = sheet.getName();
            if (sheet.getLastRow() < 2) return;

            var data = sheet.getDataRange().getValues();
            var headers = trimHeaders(data[0]);

            var colSched = headers.indexOf('Scheduled Distrib Code');
            var colDate = headers.indexOf('Distribution Date Range');
            var colRestock = headers.indexOf('Restock-# Recipients');
            if (colRestock === -1) return;

            for (var i = 1; i < data.length; i++) {
                var row = data[i];
                var restock = parseInt(row[colRestock]) || 0;
                if (restock <= 0) continue;

                var schedCode = colSched !== -1 ? (row[colSched] || '').toString().trim().toUpperCase() : '';
                var dateRange = colDate !== -1 ? (row[colDate] || '').toString().trim() : '';

                // Look up boxes: try full code, then strip to generic code
                var boxes = codeIndex[schedCode];
                if (!boxes) {
                    var gen = schedCode.replace(/[0-9\-]/g, '').toUpperCase();
                    boxes = genericIndex[gen] || null;
                }

                if (!boxes) {
                    noBoxWarnings.push(centerName + ' / ' + schedCode);
                    // Still record a row with no product detail so the recipient count isn't lost
                    detailRows.push({
                        center: centerName, schedCode: schedCode, dateRange: dateRange,
                        box: '', productName: '(no box code found)', restockRecipients: restock,
                        qtyPerBox: 0, totalQty: 0
                    });
                    continue;
                }

                // For each box that exists, expand to one row per product
                [
                    { label: 'Box 1', code: boxes.box1 },
                    { label: 'Box 2', code: boxes.box2 },
                    { label: 'Box 3', code: boxes.box3 }
                ].forEach(function (b) {
                    if (!b.code) return;
                    var products = productMap[b.code.toUpperCase()] || [];
                    if (products.length === 0) {
                        detailRows.push({
                            center: centerName, schedCode: schedCode, dateRange: dateRange,
                            box: b.label + ' (' + b.code + ')',
                            productName: '(no products in Distributed_Products)',
                            restockRecipients: restock, qtyPerBox: 0, totalQty: 0
                        });
                    } else {
                        products.forEach(function (p) {
                            detailRows.push({
                                center: centerName, schedCode: schedCode, dateRange: dateRange,
                                box: b.label + ' (' + b.code + ')',
                                productName: p.productName,
                                restockRecipients: restock,
                                qtyPerBox: p.qtyPerBox,
                                totalQty: p.qtyPerBox * restock
                            });
                        });
                    }
                });
            }
        });

        // ── Step 4: Build center summary { center → { product → totalQty, totalRecipients } }
        var centerProductMap = {};  // center|product → {box, totalQty, totalRecipients}
        detailRows.forEach(function (r) {
            var key = r.center + '||' + r.productName;
            if (!centerProductMap[key]) {
                centerProductMap[key] = {
                    center: r.center, productName: r.productName, box: r.box,
                    totalQty: 0, totalRecipients: 0
                };
            }
            centerProductMap[key].totalQty += r.totalQty;
            centerProductMap[key].totalRecipients = Math.max(
                centerProductMap[key].totalRecipients, r.restockRecipients
            );
        });

        var summaryRows = [];
        for (var k in centerProductMap) summaryRows.push(centerProductMap[k]);
        summaryRows.sort(function (a, b) {
            var cc = a.center.localeCompare(b.center);
            return cc !== 0 ? cc : b.totalQty - a.totalQty;
        });

        var grandTotalUnits = summaryRows.reduce(function (s, r) { return s + r.totalQty; }, 0);

        // ── Step 5: Write report spreadsheet ─────────────────────────────────
        var datestamp = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd_HHmm');
        var ss = SpreadsheetApp.create('Restock_Products_Report_' + datestamp);

        // Sheet 1 — Summary by Center & Product
        var sumSheet = ss.getActiveSheet();
        sumSheet.setName('By Center & Product');

        var sumHdrs = ['Distribution Center', 'Product Name', 'Box', 'Total Units Returned to Stock'];
        sumSheet.getRange(1, 1, 1, sumHdrs.length).setValues([sumHdrs])
            .setFontWeight('bold').setBackground('#1a73e8').setFontColor('white')
            .setHorizontalAlignment('center');
        sumSheet.setFrozenRows(1);

        if (summaryRows.length > 0) {
            var sumData = summaryRows.map(function (r) {
                return [r.center, r.productName, r.box, r.totalQty];
            });
            // Total row
            sumData.push(['GRAND TOTAL', '', '', grandTotalUnits]);
            sumSheet.getRange(2, 1, sumData.length, sumHdrs.length).setValues(sumData);

            // Alternate shading + bold total
            for (var ri = 1; ri < sumData.length - 1; ri += 2) {
                sumSheet.getRange(ri + 2, 1, 1, sumHdrs.length).setBackground('#f8f9fa');
            }
            sumSheet.getRange(sumData.length + 1, 1, 1, sumHdrs.length)
                .setFontWeight('bold').setBackground('#e8f0fe');

            // Color quantity column
            sumSheet.getRange(2, 4, sumData.length, 1).setFontColor('#d93025').setFontWeight('bold');
        }
        for (var sc = 1; sc <= sumHdrs.length; sc++) sumSheet.autoResizeColumn(sc);

        // Sheet 2 — All Run Detail
        var detSheet = ss.insertSheet('All Run Detail');
        var detHdrs = ['Distribution Center', 'Sched Distrib Code', 'Date Range',
            'Box', 'Product Name', 'Restock Recipients', 'Qty per Box', 'Total Units Returned'];
        detSheet.getRange(1, 1, 1, detHdrs.length).setValues([detHdrs])
            .setFontWeight('bold').setBackground('#4a86e8').setFontColor('white')
            .setHorizontalAlignment('center');
        detSheet.setFrozenRows(1);

        if (detailRows.length > 0) {
            var detData = detailRows.map(function (r) {
                return [r.center, r.schedCode, r.dateRange, r.box, r.productName,
                r.restockRecipients, r.qtyPerBox, r.totalQty];
            });
            detSheet.getRange(2, 1, detData.length, detHdrs.length).setValues(detData);
            for (var dr = 1; dr < detData.length; dr += 2) {
                detSheet.getRange(dr + 2, 1, 1, detHdrs.length).setBackground('#f8f9fa');
            }
        }
        for (var dc = 1; dc <= detHdrs.length; dc++) detSheet.autoResizeColumn(dc);

        if (noBoxWarnings.length > 0) {
            Logger.log('generateRestockReport: no box codes found for: ' + noBoxWarnings.join('; '));
        }

        // v5.42: Build per-center summary for the AP restock table.
        // The client expects: centers[{center, distributions, totalScheduled,
        // totalRestock, avgPctNoPickup}], plus grand totals.
        //
        // Distributions  = unique (schedCode + dateRange) per center
        // Total Scheduled = sum of Total Scheduled-Recipients per that unique run
        // Total Restock   = sum of Restock-# Recipients per that unique run
        // Avg % No Pick Up = Total Restock / Total Scheduled × 100 (per center)
        //
        // We re-read the Hygiene Stats workbook once more to pull Total
        // Scheduled-Recipients per run (not carried through detailRows,
        // which are product-expanded). Doing it here avoids restructuring
        // the detailRows pipeline above.
        var centerStats = {}; // centerName → {runs:{runKey:{scheduled,restock}}, scheduledSum, restockSum}
        sheets.forEach(function (sheet) {
            var centerName = sheet.getName();
            if (sheet.getLastRow() < 2) return;
            var sdata = sheet.getDataRange().getValues();
            var shdrs = trimHeaders(sdata[0]);
            var cSched = shdrs.indexOf('Scheduled Distrib Code');
            var cDate = shdrs.indexOf('Distribution Date Range');
            var cRestock = shdrs.indexOf('Restock-# Recipients');
            var cTotalRec = shdrs.indexOf('Total Scheduled-Recipients');
            if (cRestock === -1 && cTotalRec === -1) return;

            for (var ri = 1; ri < sdata.length; ri++) {
                var srow = sdata[ri];
                var schedV = cSched !== -1 ? (srow[cSched] || '').toString().trim().toUpperCase() : '';
                var dateV = cDate !== -1 ? (srow[cDate] || '').toString().trim() : '';
                var restockV = cRestock !== -1 ? (parseInt(srow[cRestock]) || 0) : 0;
                var schedRec = cTotalRec !== -1 ? (parseInt(srow[cTotalRec]) || 0) : 0;
                if (schedRec === 0 && restockV === 0) continue;

                if (!centerStats[centerName]) {
                    centerStats[centerName] = { runs: {}, scheduledSum: 0, restockSum: 0 };
                }
                var runKey = schedV + '||' + dateV;
                if (!centerStats[centerName].runs[runKey]) {
                    centerStats[centerName].runs[runKey] = { scheduled: 0, restock: 0 };
                    centerStats[centerName].runs[runKey].scheduled = schedRec;
                    centerStats[centerName].runs[runKey].restock = restockV;
                    centerStats[centerName].scheduledSum += schedRec;
                    centerStats[centerName].restockSum += restockV;
                }
            }
        });

        // Shape into client-friendly array, sorted by center name
        var centers = [];
        var centerNames = Object.keys(centerStats).sort();
        var grandTotalScheduled = 0;
        var grandTotalRestock = 0;
        var totalDistributions = 0;
        centerNames.forEach(function (cn) {
            var cs = centerStats[cn];
            var runCount = Object.keys(cs.runs).length;
            var pct = cs.scheduledSum > 0
                ? Math.round((cs.restockSum / cs.scheduledSum) * 1000) / 10
                : 0;
            centers.push({
                center: cn,
                distributions: runCount,
                totalScheduled: cs.scheduledSum,
                totalRestock: cs.restockSum,
                avgPctNoPickup: pct
            });
            grandTotalScheduled += cs.scheduledSum;
            grandTotalRestock += cs.restockSum;
            totalDistributions += runCount;
        });

        var grandAvgPct = grandTotalScheduled > 0
            ? Math.round((grandTotalRestock / grandTotalScheduled) * 1000) / 10
            : 0;

        var centerCount = centers.length;

        moveToFolder(ss.getId(), CONFIG.REPORTS_FOLDER_ID);
        logAudit('REPORT', null, 'Generated Restock Products Report: ' + centerCount +
            ' centers, ' + grandTotalUnits + ' total units returned to stock');

        return {
            success: true,
            // v5.42: fields expected by AP restock table builder
            totalRows: totalDistributions,   // total run count across all centers
            centerCount: centerCount,
            centers: centers,                // per-center summary rows
            grandTotalScheduled: grandTotalScheduled,
            grandTotalRestock: grandTotalRestock,
            grandAvgPct: grandAvgPct,
            // Legacy fields retained for backward compatibility
            totalRestockUnits: grandTotalUnits,
            detailCount: detailRows.length,
            warnings: noBoxWarnings.length,
            reportUrl: ss.getUrl(),
            downloadUrl: 'https://docs.google.com/spreadsheets/d/' + ss.getId() + '/export?format=xlsx'
        };

    } catch (e) {
        Logger.log('generateRestockReport error: ' + e.message);
        return { success: false, error: 'Restock report failed: ' + e.message };
    }
}


/**
 * Returns fully-computed distribution report data for a given SchedDisbCode
 * @param {string} distribCode - Scheduled Distribution Code (will be uppercased)
 * @returns {Object} { success, distribCode, scheduledHeaders, scheduledRows,
 */
function getDistributionRecordsForBuilder(distribCode) {
    try {
        if (!distribCode) return { success: false, error: 'Distribution Code is required.' };
        distribCode = distribCode.toString().toUpperCase();

        var sheet = getMasterSheet();
        if (!sheet) return { success: false, error: 'Applicants_Master not found.' };

        var data = sheet.getDataRange().getValues();
        var headers = trimHeaders(data[0]);

        var colIdx = {
            id: headers.indexOf(resolveAMField_('ID')),
            firstName: headers.indexOf(resolveAMField_('First Name')),
            lastName: headers.indexOf(resolveAMField_('Last Name')),
            address1: headers.indexOf(resolveAMField_('Street Address')),
            address2: headers.indexOf(resolveAMField_('Apartment # or Upper or Lower or Lot #')),
            city: headers.indexOf(resolveAMField_('City')),
            phone: headers.indexOf(resolveAMField_('Phone Number')),
            schedDistribCode: headers.indexOf(resolveAMField_('Scheduled Distribution Code')),
            babyBox: headers.indexOf(resolveAMField_('Take Baby Box?')),
            serviceStatus: headers.indexOf(resolveAMField_('Service Status')),
            adminNotes: headers.indexOf(resolveAMField_('Admin Notes')),
            requestDate: headers.indexOf(resolveAMField_('Request Date')),
            email: headers.indexOf(resolveAMField_('Email')),
            incomeLevel: headers.indexOf(resolveAMField_('Total annual household income. (How much money does your family make in 1 year?)'))
        };

        if (colIdx.schedDistribCode === -1)
            return { success: false, error: 'Scheduled Distribution Code column not found.' };

        // ── Box3 from LU_SchedDisbCodes ───────────────────────────────────────
        var hasExtraBox = false;
        try {
            var luDet = getSchedDisbCodeDetails(distribCode);
            if (luDet && luDet.success) hasExtraBox = !!(luDet.box3);
        } catch (e) { /* non-fatal */ }

        // ── Build record objects ──────────────────────────────────────────────
        var allRecords = [];
        for (var i = 1; i < data.length; i++) {
            var rowCode = (data[i][colIdx.schedDistribCode] || '').toString().toUpperCase();
            if (rowCode !== distribCode) continue;
            var babyVal = colIdx.babyBox !== -1
                ? (data[i][colIdx.babyBox] || '').toString().trim().toUpperCase() : '';
            var reqDate = colIdx.requestDate !== -1 ? data[i][colIdx.requestDate] : '';
            var reqDateStr = '';
            if (reqDate instanceof Date && !isNaN(reqDate.getTime()))
                reqDateStr = Utilities.formatDate(reqDate, CONFIG.TIMEZONE, 'M/d/yyyy');
            else if (reqDate) reqDateStr = reqDate.toString().trim();

            allRecords.push({
                id: colIdx.id !== -1 ? (data[i][colIdx.id] || '').toString().trim() : '',
                firstName: colIdx.firstName !== -1 ? (data[i][colIdx.firstName] || '').toString().trim() : '',
                lastName: colIdx.lastName !== -1 ? (data[i][colIdx.lastName] || '').toString().trim() : '',
                address1: colIdx.address1 !== -1 ? (data[i][colIdx.address1] || '').toString().trim() : '',
                address2: colIdx.address2 !== -1 ? (data[i][colIdx.address2] || '').toString().trim() : '',
                city: colIdx.city !== -1 ? (data[i][colIdx.city] || '').toString().trim() : '',
                phone: colIdx.phone !== -1 ? (data[i][colIdx.phone] || '').toString().trim() : '',
                babyBox: babyVal === 'X' ? 'X' : '',
                extraBox: hasExtraBox ? 'X' : '',
                serviceStatus: colIdx.serviceStatus !== -1 ? (data[i][colIdx.serviceStatus] || '').toString().trim() : '',
                adminNotes: colIdx.adminNotes !== -1 ? (data[i][colIdx.adminNotes] || '').toString().trim() : '',
                email: colIdx.email !== -1 ? (data[i][colIdx.email] || '').toString().trim() : '',
                incomeLevel: colIdx.incomeLevel !== -1 ? (data[i][colIdx.incomeLevel] || '').toString().trim() : '',
                requestDate: reqDateStr
            });
        }

        allRecords.sort(function (a, b) {
            var lc = a.lastName.localeCompare(b.lastName);
            return lc !== 0 ? lc : a.firstName.localeCompare(b.firstName);
        });

        var scheduledRecords = allRecords.filter(function (r) {
            var s = r.serviceStatus.toUpperCase();
            return s === 'SCHEDULED' || s === '';
        });

        // ── Flat headers/rows for Report Builder consumption ──────────────────
        var recHeaders = ['ID', 'First Name', 'Last Name', 'Address', 'City', 'Phone',
            'Baby Box', 'Extra Box', 'Service Status', 'Admin Notes',
            'Email', 'Income Level', 'Request Date'];
        var toRow = function (r) {
            return [r.id, r.firstName, r.lastName,
            r.address1 + (r.address2 ? ' ' + r.address2 : ''),
            r.city, r.phone, r.babyBox, r.extraBox,
            r.serviceStatus, r.adminNotes, r.email, r.incomeLevel, r.requestDate];
        };

        // ── Products Needed ───────────────────────────────────────────────────
        var productsData = [];
        try {
            var lookupsWb = getLookupsWorkbook();
            var schedSheet2 = lookupsWb.getSheetByName(CONFIG.LOOKUPS.SCHED_DISB_CODES);
            if (schedSheet2 && schedSheet2.getLastRow() > 1) {
                var sd = schedSheet2.getDataRange().getValues();
                var sdH = trimHeaders(sd[0]);
                var scCodeCol = sdH.indexOf('SchedDisbCode');
                var scBox1Col = sdH.indexOf('Box 1'); if (scBox1Col === -1) scBox1Col = sdH.indexOf('Box1');
                var scBox2Col = sdH.indexOf('Box 2'); if (scBox2Col === -1) scBox2Col = sdH.indexOf('Box2');
                var scBox3Col = sdH.indexOf('Box 3'); if (scBox3Col === -1) scBox3Col = sdH.indexOf('Box3');
                var box1Code = '', box2Code = '', box3Code = '';
                for (var si = 1; si < sd.length; si++) {
                    if ((sd[si][scCodeCol] || '').toString().trim().toUpperCase() === distribCode) {
                        box1Code = scBox1Col !== -1 ? (sd[si][scBox1Col] || '').toString().trim() : '';
                        box2Code = scBox2Col !== -1 ? (sd[si][scBox2Col] || '').toString().trim() : '';
                        box3Code = scBox3Col !== -1 ? (sd[si][scBox3Col] || '').toString().trim() : '';
                        break;
                    }
                }
                var dataWb2 = getDataWorkbook();
                var dpSheet2 = dataWb2.getSheetByName(CONFIG.DATA_SHEETS.DISTRIBUTED_PRODUCTS);
                if (dpSheet2 && dpSheet2.getLastRow() > 1) {
                    var dpData2 = dpSheet2.getDataRange().getValues();
                    var dpH2 = trimHeaders(dpData2[0]);
                    var dpBoxCol2 = dpH2.indexOf('BoxCode');
                    var dpNameCol2 = dpH2.indexOf('ProductName');
                    var dpQtyCol2 = dpH2.indexOf('Quantity');
                    if (dpQtyCol2 === -1) dpQtyCol2 = dpH2.indexOf('QtyPerBox');
                    if (dpQtyCol2 === -1) dpQtyCol2 = dpH2.indexOf('Qty');
                    var sumByBox = function (boxCode) {
                        var totals = {};
                        if (!boxCode) return totals;
                        var ub = boxCode.toUpperCase();
                        for (var di = 1; di < dpData2.length; di++) {
                            var rb = (dpData2[di][dpBoxCol2] || '').toString().trim().toUpperCase();
                            if (rb !== ub) continue;
                            var nm = (dpData2[di][dpNameCol2] || '').toString().trim();
                            var qt = parseFloat(dpData2[di][dpQtyCol2]) || 0;
                            if (nm) totals[nm] = (totals[nm] || 0) + qt;
                        }
                        return totals;
                    };
                    var allCount2 = scheduledRecords.length;
                    var babyCount2 = scheduledRecords.filter(function (r) { return r.babyBox === 'X'; }).length;
                    if (box1Code) productsData.push({
                        label: 'Box 1', boxCode: box1Code.toUpperCase(),
                        recipients: allCount2, products: sumByBox(box1Code)
                    });
                    if (box2Code) productsData.push({
                        label: 'Box 2', boxCode: box2Code.toUpperCase(),
                        recipients: babyCount2, products: sumByBox(box2Code)
                    });
                    if (box3Code) productsData.push({
                        label: 'Box 3', boxCode: box3Code.toUpperCase(),
                        recipients: allCount2, products: sumByBox(box3Code)
                    });
                }
            }
        } catch (prodErr) {
            Logger.log('getDistributionReportData products (non-fatal): ' + prodErr.message);
        }

        return {
            success: true,
            distribCode: distribCode,
            scheduledHeaders: recHeaders,
            scheduledRows: scheduledRecords.map(toRow),
            allHeaders: recHeaders,
            allRows: allRecords.map(toRow),
            scheduledCount: scheduledRecords.length,
            totalCount: allRecords.length,
            hasBabyBox: allRecords.some(function (r) { return r.babyBox === 'X'; }),
            products: productsData
        };

    } catch (e) {
        Logger.log('getDistributionReportData error: ' + e.message);
        return { success: false, error: 'Failed: ' + e.message };
    }
}


/**
 * Generate Scheduling Report for ALL Applicants_Master records.
 * @param {number} [beginId=0] - First AM ID considered a "new" record (from LU_SchedID)
 * @returns {{ success, reportUrl, downloadUrl, reportId, recordCount, endId }}
 */
function generateSchedulingReport(beginId) {
    try {
        beginId = parseInt(beginId) || 0;

        var sheet = getMasterSheet();
        if (!sheet) return { success: false, error: 'Master sheet not found' };

        var data = sheet.getDataRange().getValues();
        var headers = trimHeaders(data[0]);
        var idCol = headers.indexOf(resolveAMField_('ID'));
        var fnCol = headers.indexOf(resolveAMField_('First Name'));
        var lnCol = headers.indexOf(resolveAMField_('Last Name'));
        if (idCol === -1) return { success: false, error: 'ID column not found in Applicants_Master' };

        // ── Auto-detect endId as maximum AM ID ────────────────────────────────
        var endId = 0;
        for (var r = 1; r < data.length; r++) {
            var rid = parseInt(data[r][idCol]) || 0;
            if (rid > endId) endId = rid;
        }

        // ── Get column definitions from LU_ReportColumns (with v5.19 fallback) ──
        var cols = getReportColumns('Scheduling');
        if (cols.length === 0) {
            cols = _getSchedFallbackCols_();
        }

        // ── Build colIndexMap ─────────────────────────────────────────────────
        var colIndexMap = {};
        cols.forEach(function (col) {
            var key = col.key;
            if (key === 'History') return;
            var bracketPair = SCHED_AGE_BRACKET_MAP_[key];
            if (bracketPair) {
                var mi = headers.indexOf(resolveAMField_(bracketPair.male));
                var fi = headers.indexOf(resolveAMField_(bracketPair.female));
                if (mi !== -1) colIndexMap[bracketPair.male] = mi;
                if (fi !== -1) colIndexMap[bracketPair.female] = fi;
                return;
            }
            var idx = headers.indexOf(resolveAMField_(key));
            if (idx !== -1) colIndexMap[key] = idx;
        });

        // ── Collect records — skip rows with inactive SchedDisbCodes ─────────
        // v5.36: Build set of active codes; rows whose stored Scheduled Distribution
        // Code is present but inactive are excluded from the report.
        var activeCodeSet = {};
        try {
            getActiveSchedDisbCodes().forEach(function (c) {
                if (c && c.code) activeCodeSet[c.code.toString().trim().toUpperCase()] = true;
            });
        } catch (acErr) {
            Logger.log('generateSchedulingReport: active codes lookup (non-fatal): ' + acErr.message);
        }
        var schedDisbCodeColIdx = headers.indexOf(resolveAMField_('Scheduled Distribution Code'));
        // v5.55: Resolve Request Date column for History flag calculation
        var rdColIdx = headers.indexOf(resolveAMField_('Request Date'));

        var records = [];
        for (var i = 1; i < data.length; i++) {
            var fn = (data[i][fnCol] || '').toString().trim();
            var ln = (data[i][lnCol] || '').toString().trim();
            if (!fn && !ln) continue;

            // v5.36: Exclude rows where SchedDisbCode is set to an inactive code
            if (schedDisbCodeColIdx !== -1) {
                var rowCode = (data[i][schedDisbCodeColIdx] || '').toString().trim().toUpperCase();
                if (rowCode && !activeCodeSet[rowCode]) continue;
            }

            // v5.55: Store requestDate as a timestamp for History flag comparison
            var rdRaw = rdColIdx !== -1 ? data[i][rdColIdx] : '';
            var rdDate = (rdRaw instanceof Date) ? rdRaw : new Date(rdRaw);
            var rdTs = (!rdDate || isNaN(rdDate.getTime())) ? 0 : rdDate.getTime();

            records.push({
                rowData: data[i],
                lastName: ln,
                firstName: fn,
                rowId: parseInt(data[i][idCol]) || 0,
                requestDateTs: rdTs,   // v5.55: timestamp for sort + History flag
                amRowIndex: i + 1,
                isHistory: false
            });
        }
        if (records.length === 0)
            return { success: false, error: 'No records found in Applicants_Master' };

        // ── Sort: Last Name, First Name, Request Date (asc), ID as tiebreaker ──
        // v5.55: Changed tiebreaker from ID to Request Date so History flag below
        // correctly identifies the most-recent request per person by date.
        records.sort(function (a, b) {
            var lc = a.lastName.localeCompare(b.lastName); if (lc) return lc;
            var fc = a.firstName.localeCompare(b.firstName); if (fc) return fc;
            var dc = a.requestDateTs - b.requestDateTs; if (dc) return dc;
            return a.rowId - b.rowId;
        });

        // ── History flag ──────────────────────────────────────────────────────
        // v5.55: Two-tier History logic incorporating beginId (from LU_SchedID):
        //
        //   Tier 1 — Previous-run records: any record whose AM ID < beginId was
        //     included in a prior Scheduling Report run → always marked History.
        //     beginId is read from LU_LastScheduled by AP (via getLastScheduledId)
        //     and passed as the first parameter to this function.
        //     NOTE: if CONFIG.LOOKUPS.SAVED_SCHEDULE_ID does not match the actual
        //     sheet name in G2N_Lookups, getLastScheduledId/appendLastScheduledId
        //     silently fail. Verify CONFIG.LOOKUPS.SAVED_SCHEDULE_ID = 'LU_SchedID'
        //     (or whatever the sheet is named).
        //
        //   Tier 2 — Per-person recency: among records with ID >= beginId (new
        //     this run), the most recent Request Date per person = current;
        //     all earlier Request Dates for the same person = History.
        //
        // Result: only one non-History row per person (the latest request from
        // the current run); all prior requests — whether from old runs or within
        // the current run — are marked History.
        var maxNewDatePerName = {};
        records.forEach(function (rec) {
            if (rec.rowId >= beginId) {
                var k = rec.firstName.toLowerCase() + '|' + rec.lastName.toLowerCase();
                if (!maxNewDatePerName[k] || rec.requestDateTs > maxNewDatePerName[k]) {
                    maxNewDatePerName[k] = rec.requestDateTs;
                }
            }
        });
        records.forEach(function (rec) {
            if (rec.rowId < beginId) {
                // Tier 1: from a previous scheduling run
                rec.isHistory = true;
            } else {
                // Tier 2: new this run — History if a newer request exists for same person
                var k = rec.firstName.toLowerCase() + '|' + rec.lastName.toLowerCase();
                rec.isHistory = rec.requestDateTs < (maxNewDatePerName[k] || rec.requestDateTs);
            }
        });

        // ── Create spreadsheet ────────────────────────────────────────────────
        var reportDate = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd');
        var reportName = 'Scheduling_' + reportDate;
        var spreadsheet = SpreadsheetApp.create(reportName);
        var reportSheet = spreadsheet.getActiveSheet();
        if (CONFIG.SCHEDULING_FOLDER_ID && CONFIG.SCHEDULING_FOLDER_ID.length > 0) {
            try { moveToFolder(spreadsheet.getId(), CONFIG.SCHEDULING_FOLDER_ID); }
            catch (fe) { Logger.log('ERROR moving to Scheduling folder: ' + fe.message); }
        }

        // ── Write header row ──────────────────────────────────────────────────
        var allReportHeaders = cols.map(function (c) { return c.label; });
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setNumberFormat('@');
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setValues([allReportHeaders]);
        reportSheet.getRange(1, 1, 1, allReportHeaders.length)
            .setFontWeight('bold')
            .setBackground('#4a86e8')
            .setFontColor('white');

        // ── Build and write data rows ─────────────────────────────────────────
        var dataRows = records.map(function (rec) {
            return _buildSchedRow_(rec, cols, colIndexMap);
        });
        if (dataRows.length > 0) {
            reportSheet.getRange(2, 1, dataRows.length, allReportHeaders.length).setValues(dataRows);
        }

        // ── Apply column widths and wrap ──────────────────────────────────────
        applyReportColumnFormatting(reportSheet, cols, 2, dataRows.length + 1);

        // ── Data validation dropdowns ─────────────────────────────────────────
        var schedCodeLabel = getReportHeader_('Scheduled Distribution Code') || 'Scheduled Distribution Code';
        var svcStatusLabel = getReportHeader_('Service Status') || 'Service Status';
        var schedCodeColNum = allReportHeaders.indexOf(schedCodeLabel) + 1;
        var svcStatusColNum = allReportHeaders.indexOf(svcStatusLabel) + 1;

        if (dataRows.length > 0) {
            var distribCodes = getAllDistribCodes_();
            if (schedCodeColNum > 0 && distribCodes.length > 0) {
                var schedRule = SpreadsheetApp.newDataValidation()
                    .requireValueInList(distribCodes, true)
                    .setAllowInvalid(true).build();
                reportSheet.getRange(2, schedCodeColNum, dataRows.length, 1).setDataValidation(schedRule);
            }
            // v5.27: read from LU_ServiceStatus instead of hardcoded list
            var statusValues = getLookupValues('SERVICE_STATUS', 'Status');
            if (!statusValues || statusValues.length === 0) {
                statusValues = ['Picked Up', 'Delivered', 'Cancelled', 'Open', 'Pending', 'Scheduled'];
            }
            if (svcStatusColNum > 0) {
                var statusRule = SpreadsheetApp.newDataValidation()
                    .requireValueInList(statusValues, true)
                    .setAllowInvalid(true).build();
                reportSheet.getRange(2, svcStatusColNum, dataRows.length, 1).setDataValidation(statusRule);
            }
        }

        // ── v5.23: Row highlighting ───────────────────────────────────────────
        // Find First Name and Last Name column positions (1-based)
        var fnLabel = getReportHeader_('First Name') || 'First Name';
        var lnLabel = getReportHeader_('Last Name') || 'Last Name';
        var fnColNum = allReportHeaders.indexOf(fnLabel) + 1;  // 1-based; 0 if not found
        var lnColNum = allReportHeaders.indexOf(lnLabel) + 1;

        for (var ri = 0; ri < records.length; ri++) {
            var reportRowNum = ri + 2;  // 1-based; row 1 is header
            var rec = records[ri];

            // Pink + bold for new records (ID within [beginId, endId])
            if (beginId > 0 && rec.rowId >= beginId && rec.rowId <= endId) {
                reportSheet.getRange(reportRowNum, 1, 1, allReportHeaders.length)
                    .setBackground('#FFD9D9')
                    .setFontWeight('bold');
            }

            // Yellow on First Name and Last Name cells for ALL rows
            if (fnColNum > 0) reportSheet.getRange(reportRowNum, fnColNum).setBackground('#FFFF00');
            if (lnColNum > 0) reportSheet.getRange(reportRowNum, lnColNum).setBackground('#FFFF00');
        }

        // ── Freeze header row ─────────────────────────────────────────────────
        reportSheet.getRange(1, 1, 1, allReportHeaders.length).setWrap(true);
        reportSheet.setFrozenRows(1);

        // ── v5.23: Highlight AM row at endId magenta ──────────────────────────
        if (endId > 0) {
            for (var ai = 1; ai < data.length; ai++) {
                if ((parseInt(data[ai][idCol]) || 0) === endId) {
                    sheet.getRange(ai + 1, 1, 1, headers.length).setBackground('#FF00FF');
                    break;
                }
            }
        }

        // ── v5.23: Record this run in LU_SchedID ─────────────────────────────
        var reportDateFormatted = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yyyy');
        try { appendLastScheduledId(endId, reportDateFormatted); }
        catch (ae) { Logger.log('appendLastScheduledId error (non-fatal): ' + ae.message); }

        logAudit('REPORT', null, 'Generated Scheduling Report — ' + records.length +
            ' records, beginId=' + beginId + ', endId=' + endId);

        return {
            success: true,
            message: 'Scheduling Report generated with ' + records.length + ' records',
            reportUrl: spreadsheet.getUrl(),
            downloadUrl: 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx',
            reportId: spreadsheet.getId(),
            recordCount: records.length,
            endId: endId
        };

    } catch (e) {
        Logger.log('generateSchedulingReport error: ' + e.message);
        return { success: false, error: 'Report generation failed: ' + e.message };
    }
}

/**
 * Creates the Distribution Report spreadsheet with formatted headers and data.
 * @param {string}   distribCode  - Distribution code
 * @param {Object[]} records      - Filtered/sorted record objects (Scheduled status only)
 * @param {boolean}  hasBabyBox   - Whether to include Baby Box column
 * @param {boolean}  hasExtraBox  - Whether to include Extra Box column (v5.12)
 */
function createDistributionReportSpreadsheet(distribCode, records, hasBabyBox, hasExtraBox,
    startDate, endDate, pickupTimes, allRecords) {
    const reportName = 'Distribution_' + distribCode + '_' +
        Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd');
    const spreadsheet = SpreadsheetApp.create(reportName);
    const sheet = spreadsheet.getActiveSheet();
    sheet.setName('Scheduled'); // v5.26: rename from default "Sheet1"

    // Move to Distribution folder
    const folderId = CONFIG.DISTRIBUTION_FOLDER_ID;
    if (folderId && folderId.length > 0) {
        try {
            moveToFolder(spreadsheet.getId(), folderId);
        } catch (folderError) {
            Logger.log('ERROR moving to Distribution folder: ' + folderError.message);
        }
    } else {
        Logger.log('WARNING: DISTRIBUTION_FOLDER_ID not configured');
    }

    let currentRow = 1;

    // ── Header section (few cells — individual writes are fine here) ──────────
    sheet.getRange(currentRow, 1).setValue('Total Recipients: ' + records.length)
        .setFontWeight('bold');
    currentRow++;

    sheet.getRange(currentRow, 1).setValue('Giving to the Nations')
        .setFontWeight('bold').setFontSize(14);
    currentRow++;

    sheet.getRange(currentRow, 1).setValue('Cleaning Box Distribution')
        .setFontWeight('bold').setFontSize(12);
    currentRow++;

    sheet.getRange(currentRow, 1).setValue('Available Dates: ' + startDate + '-' + endDate);
    currentRow++;

    if (pickupTimes) {
        sheet.getRange(currentRow, 1).setValue('Available Pick Up Times: ' + pickupTimes);
        currentRow++;
    }

    sheet.getRange(currentRow, 1).setValue(
        'Created: ' + Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yy'));
    currentRow++;

    currentRow++; // blank row before data header

    // ── Data header row ───────────────────────────────────────────────────────
    const headerRow = currentRow;

    // v5.12: Baby Box and Extra Box are optional columns
    const dataHeaders = ['Date Picked Up'];
    if (hasBabyBox) dataHeaders.push('Baby Box');
    if (hasExtraBox) dataHeaders.push('Extra Box');
    dataHeaders.push(
        'First Name', 'Last Name',
        getReportHeader_('Street Address'),
        getReportHeader_('Apartment #, Upper, Lower, or Lot #'),
        'City', 'Phone', 'Distribution Code', 'Submission #'
    );

    const numCols = dataHeaders.length;
    sheet.getRange(headerRow, 1, 1, numCols).setValues([dataHeaders]);
    styleReportHeader(sheet, headerRow, numCols);
    currentRow++;

    // ── Data rows — SINGLE batch write (v5.21) ────────────────────────────────
    // Build entire 2D array in memory first, then write in one setValues() call.
    // Previous implementation used ~11 setValue() calls per record; for 80 records
    // that was 880+ API calls. This version uses exactly 1 regardless of row count.
    if (records.length > 0) {
        const dataRows = records.map(function (record) {
            const row = [
                ''    // Date Picked Up — blank for staff to fill in
            ];
            if (hasBabyBox) row.push(record.babyBox || '');
            if (hasExtraBox) row.push(record.extraBox || '');
            row.push(
                record.firstName || '',
                record.lastName || '',
                record.address1 || '',
                record.address2 || '',
                record.city || '',
                formatPhoneNumber_(record.phone),
                record.distribCode || '',
                record.submissionId || ''
            );
            return row;
        });

        // Single API call for all data
        sheet.getRange(currentRow, 1, dataRows.length, numCols).setValues(dataRows);

        // Border around entire data range
        sheet.getRange(headerRow + 1, 1, records.length, numCols)
            .setBorder(true, true, true, true, true, true);

        // Alternating row shading — one setBackground() per other-row (not per cell)
        for (let i = 1; i < records.length; i += 2) {
            sheet.getRange(headerRow + 1 + i, 1, 1, numCols).setBackground('#f3f3f3');
        }

        currentRow += records.length;
    }

    // ── Footer notes ──────────────────────────────────────────────────────────
    const footerLines = [];
    if (hasBabyBox) {
        footerLines.push(
            "Note: Recipients with \"X\" in the 'Baby Box' column will receive 2 boxes, " +
            "1 large box and 1 small box. All others receive 1 large box.");
    }
    if (hasExtraBox) {
        footerLines.push(
            "Note: Recipients with \"X\" in the 'Extra Box' column will receive a special " +
            "box in addition to the Scheduled Box.");
    }
    for (const line of footerLines) {
        currentRow++;
        sheet.getRange(currentRow, 1).setValue(line);
        sheet.getRange(currentRow, 1, 1, numCols).merge();
        sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(10).setWrap(true);
    }

    // ── Column widths ─────────────────────────────────────────────────────────
    const widths = [100];
    if (hasBabyBox) widths.push(70);
    if (hasExtraBox) widths.push(70);
    widths.push(100, 100, 180, 80, 120, 120, 100, 90);
    for (let c = 0; c < widths.length; c++) {
        sheet.setColumnWidth(c + 1, widths[c]);
    }

    // ── v5.24: Distribution Contact Info second sheet ─────────────────────────
    // v5.27: pass allRecords (unfiltered by status) so Contact Info shows everyone
    _addDistributionContactInfoSheet_(spreadsheet, distribCode, allRecords || records);

    // ── v5.26: Products Needed third sheet ───────────────────────────────────
    // v5.29: counts based on scheduled-sheet records (records), not allRecords
    _addProductsNeededSheet_(spreadsheet, distribCode, records);

    return spreadsheet;
}


/**
 * Adds a "Distribution Contact Info" sheet to the distribution report spreadsheet.
 * Sorted in the same Last Name / First Name order as the primary sheet.
 * v5.24 — New helper called by createDistributionReportSpreadsheet().
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss - The report spreadsheet
 * @param {string} distribCode - Scheduled Distribution Code
 * @param {Object[]} records   - Filtered/sorted record objects
 * @private
 */
function _addDistributionContactInfoSheet_(ss, distribCode, records) {
    try {
        var ciSheet = ss.insertSheet('Distribution Contact Info');
        // v5.27: added Service Status after Income Level
        var ciHeaders = [
            'Sched Distrib Code', 'Admin Notes', 'Take Baby Box', 'ID',
            'Service How', 'Request Date', 'Last Name', 'First Name',
            'Referral Info', 'Referrer Name', 'Additional Info',
            'Street Address', 'Apartment #, Upper, Lower, or Lot #',
            'Phone Number', 'Phone Type', 'Email', 'Best Contact', 'Income Level',
            'Service Status'
        ];
        var numCols = ciHeaders.length;
        ciSheet.getRange(1, 1, 1, numCols).setValues([ciHeaders]);
        styleReportHeader(ciSheet, 1, numCols);
        ciSheet.setFrozenRows(1);
        if (records.length > 0) {
            var ciRows = records.map(function (rec) {
                return [
                    rec.distribCode || '',
                    rec.adminNotes || '',
                    rec.babyBox || '',
                    rec.submissionId || '',
                    rec.serviceHow || '',
                    rec.requestDate || '',
                    rec.lastName || '',
                    rec.firstName || '',
                    rec.referralInfo || '',
                    rec.referrerName || '',
                    rec.additionalInfo || '',
                    rec.address1 || '',
                    rec.address2 || '',
                    formatPhoneNumber_(rec.phone),
                    rec.phoneType || '',
                    rec.email || '',
                    rec.bestContact || '',
                    rec.incomeLevel || '',
                    rec.serviceStatus || ''   // v5.27
                ];
            });
            ciSheet.getRange(2, 1, ciRows.length, numCols).setValues(ciRows);
            for (var i = 1; i < ciRows.length; i += 2) {
                ciSheet.getRange(i + 2, 1, 1, numCols).setBackground('#f3f3f3');
            }
        }
        // v5.27: added width for Service Status
        var ciWidths = [120, 140, 90, 60, 100, 90, 110, 110, 120, 120, 140, 160, 100, 120, 80, 160, 90, 120, 90];
        for (var c = 0; c < ciWidths.length && c < numCols; c++) {
            ciSheet.setColumnWidth(c + 1, ciWidths[c]);
        }
        ciSheet.getRange(1, 1, 1, numCols).setWrap(true);
    } catch (e) {
        Logger.log('_addDistributionContactInfoSheet_ error (non-fatal): ' + e.message);
    }
}

/**
 * Adds a "Products Needed" sheet to the distribution report spreadsheet.
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss          - The report spreadsheet
 * @param {string}                                   distribCode  - Scheduled Distribution Code
 * @param {Object[]}                                 records      - Scheduled-sheet records
 */
function _addProductsNeededSheet_(ss, distribCode, records) {
    try {
        // ── Step 1: look up Box1/Box2/Box3 for this distribCode ──────────────
        var lookupsWb = getLookupsWorkbook();
        var schedSheet = lookupsWb.getSheetByName(CONFIG.LOOKUPS.SCHED_DISB_CODES);
        if (!schedSheet || schedSheet.getLastRow() < 2) {
            Logger.log('_addProductsNeededSheet_: LU_SchedDisbCodes not found or empty');
            return;
        }

        var schedData = schedSheet.getDataRange().getValues();
        var schedHeaders = trimHeaders(schedData[0]);
        var scCodeCol = schedHeaders.indexOf('SchedDisbCode');
        var scBox1Col = schedHeaders.indexOf('Box 1'); if (scBox1Col === -1) scBox1Col = schedHeaders.indexOf('Box1');
        var scBox2Col = schedHeaders.indexOf('Box 2'); if (scBox2Col === -1) scBox2Col = schedHeaders.indexOf('Box2');
        var scBox3Col = schedHeaders.indexOf('Box 3'); if (scBox3Col === -1) scBox3Col = schedHeaders.indexOf('Box3');

        if (scCodeCol === -1) {
            Logger.log('_addProductsNeededSheet_: SchedDisbCode column not found');
            return;
        }

        var box1Code = '', box2Code = '', box3Code = '';
        var upperCode = distribCode.toString().toUpperCase();
        for (var si = 1; si < schedData.length; si++) {
            if ((schedData[si][scCodeCol] || '').toString().trim().toUpperCase() === upperCode) {
                box1Code = scBox1Col !== -1 ? (schedData[si][scBox1Col] || '').toString().trim() : '';
                box2Code = scBox2Col !== -1 ? (schedData[si][scBox2Col] || '').toString().trim() : '';
                box3Code = scBox3Col !== -1 ? (schedData[si][scBox3Col] || '').toString().trim() : '';
                break;
            }
        }

        // ── Step 2: read Distributed_Products once ───────────────────────────
        var dataWb = getDataWorkbook();
        var dpSheet = dataWb.getSheetByName(CONFIG.DATA_SHEETS.DISTRIBUTED_PRODUCTS);
        if (!dpSheet || dpSheet.getLastRow() < 2) {
            Logger.log('_addProductsNeededSheet_: Distributed_Products sheet not found or empty');
            return;
        }

        var dpData = dpSheet.getDataRange().getValues();
        var dpHeaders = trimHeaders(dpData[0]);
        var dpBoxCol = dpHeaders.indexOf('BoxCode');
        var dpNameCol = dpHeaders.indexOf('ProductName');
        // v5.36: support multiple possible quantity column names
        var dpQtyCol = dpHeaders.indexOf('Quantity');
        if (dpQtyCol === -1) dpQtyCol = dpHeaders.indexOf('QtyPerBox');
        if (dpQtyCol === -1) dpQtyCol = dpHeaders.indexOf('QtyDistributed');
        if (dpQtyCol === -1) dpQtyCol = dpHeaders.indexOf('Qty');

        if (dpBoxCol === -1 || dpNameCol === -1 || dpQtyCol === -1) {
            Logger.log('_addProductsNeededSheet_: Distributed_Products missing columns. Headers found: ' + dpHeaders.join(', '));
            return;
        }

        // v5.37: Log all unique BoxCodes in Distributed_Products for diagnosis
        var dpBoxCodes = {};
        for (var dbi = 1; dbi < dpData.length; dbi++) {
            var bc = (dpData[dbi][dpBoxCol] || '').toString().trim();
            if (bc) dpBoxCodes[bc] = true;
        }

        /**
         * Build {ProductName: summedQty} for a given box code from Distributed_Products.
         * Case-insensitive BoxCode match (v5.32).
         * @param {string} boxCode
         * @returns {Object} { productName: totalQty }
         */
        function sumProductsByBox(boxCode) {
            var totals = {};
            if (!boxCode) return totals;
            var upperBox = boxCode.toUpperCase();
            for (var di = 1; di < dpData.length; di++) {
                var rowBox = (dpData[di][dpBoxCol] || '').toString().trim().toUpperCase();
                if (rowBox !== upperBox) continue;
                var name = (dpData[di][dpNameCol] || '').toString().trim();
                var qty = parseFloat(dpData[di][dpQtyCol]) || 0;
                if (!name) continue;
                totals[name] = (totals[name] || 0) + qty;
            }
            return totals;
        }

        // ── Step 3: compute recipient counts per box ─────────────────────────
        var allCount = records.length;
        // Box2 = recipients with Take Baby Box = X
        var babyCount = records.filter(function (r) { return r.babyBox === 'X'; }).length;

        // Build an ordered list of box sections to render
        // [ {label, boxCode, products: {name:qty}, recipientCount} ]
        var sections = [];
        if (box1Code) sections.push({ label: 'Box 1', boxCode: box1Code.toUpperCase(), products: sumProductsByBox(box1Code), recipients: allCount });
        if (box2Code) sections.push({ label: 'Box 2', boxCode: box2Code.toUpperCase(), products: sumProductsByBox(box2Code), recipients: babyCount });
        if (box3Code) sections.push({ label: 'Box 3', boxCode: box3Code.toUpperCase(), products: sumProductsByBox(box3Code), recipients: allCount });

        if (sections.length === 0) {
            Logger.log('_addProductsNeededSheet_: no box codes found for ' + distribCode + ' — skipping sheet');
            return;
        }

        // ── Step 4: write the sheet ──────────────────────────────────────────
        var pnSheet = ss.insertSheet('Products Needed');
        var row = 1;

        // Sheet title
        pnSheet.getRange(row, 1).setValue('Products Needed — ' + distribCode);
        pnSheet.getRange(row, 1, 1, 4)
            .setFontWeight('bold').setFontSize(13).setBackground('#1a73e8').setFontColor('white').merge();
        row++;

        pnSheet.getRange(row, 1).setValue(
            'Generated: ' + Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'MMMM d, yyyy h:mm a') +
            '  |  Total Recipients: ' + allCount +
            (babyCount > 0 ? '  |  Baby Box Recipients: ' + babyCount : ''));
        pnSheet.getRange(row, 1, 1, 4).merge().setFontStyle('italic').setFontSize(10);
        row++;

        // Column headers (written per section)
        var colHeaders = ['Product Name', 'Qty per Box', 'Recipients', 'Total Needed'];
        var grandTotal = 0;

        sections.forEach(function (section) {
            var productNames = Object.keys(section.products);

            row++; // blank row before each section

            // Box Code header row — v5.33: explicit "Box N — CODE (N recipients)" label
            pnSheet.getRange(row, 1).setValue(
                section.label + ' — ' + section.boxCode +
                '  (' + section.recipients + ' recipient' + (section.recipients !== 1 ? 's' : '') + ')');
            pnSheet.getRange(row, 1, 1, 4)
                .setFontWeight('bold').setBackground('#4a86e8').setFontColor('white').merge();
            row++;

            if (productNames.length === 0) {
                // v5.37: show diagnostic instead of silently skipping
                pnSheet.getRange(row, 1).setValue(
                    'No products found in Distributed_Products for box code: ' + section.boxCode);
                pnSheet.getRange(row, 1, 1, 4).merge().setFontStyle('italic').setFontColor('#ea4335');
                row++;
                return;
            }

            // Sort product names alphabetically
            productNames.sort();

            // Column header row
            pnSheet.getRange(row, 1, 1, 4).setValues([colHeaders])
                .setFontWeight('bold').setBackground('#d0e4ff');
            row++;

            // Data rows
            var sectionTotal = 0;
            var dataRows = [];
            productNames.forEach(function (name) {
                var qtyPerBox = section.products[name];
                var totalNeeded = qtyPerBox * section.recipients;
                sectionTotal += totalNeeded;
                dataRows.push([name, qtyPerBox, section.recipients, totalNeeded]);
            });
            if (dataRows.length > 0) {
                pnSheet.getRange(row, 1, dataRows.length, 4).setValues(dataRows);
                // Alternate shading
                for (var dr = 0; dr < dataRows.length; dr += 2) {
                    pnSheet.getRange(row + dr, 1, 1, 4).setBackground('#f0f4ff');
                }
                row += dataRows.length;
            }

            // Sub-total row
            pnSheet.getRange(row, 1, 1, 4)
                .setValues([['Sub-Total — ' + section.label + ' (' + section.boxCode + ')', '', '', sectionTotal]])
                .setFontWeight('bold').setBackground('#cfe2ff');
            grandTotal += sectionTotal;
            row++;
        });

        // Grand total row
        row++;
        pnSheet.getRange(row, 1, 1, 4)
            .setValues([['GRAND TOTAL (all boxes)', '', '', grandTotal]])
            .setFontWeight('bold').setFontSize(11).setBackground('#1a73e8').setFontColor('white');

        // Column widths
        pnSheet.setColumnWidth(1, 220); // Product Name
        pnSheet.setColumnWidth(2, 100); // Qty per Box
        pnSheet.setColumnWidth(3, 110); // Recipients
        pnSheet.setColumnWidth(4, 120); // Total Needed

        Logger.log('_addProductsNeededSheet_: wrote ' + sections.length + ' box sections for ' + distribCode);

    } catch (e) {
        Logger.log('_addProductsNeededSheet_ error (non-fatal): ' + e.message);
    }
}

/**
 * Preview of distribution report - returns record count without generating report
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
 * Generate Distribution Report Google Sheet for a given Scheduled Distribution Code.
 * @param {string} distribCode  - Scheduled Distribution Code (will be uppercased)
 * @param {string} startDate    - Display start date string (e.g. '3/15/26')
 * @param {string} endDate      - Display end date string (e.g. '3/29/26')
 * @param {string} pickupTimes  - Pickup times text for report header
 */
function generateDistributionReport(distribCode, startDate, endDate, pickupTimes) {
    try {
        if (!distribCode) {
            return { success: false, error: 'Distribution Code is required' };
        }

        distribCode = distribCode.toString().toUpperCase();

        var sheet = getMasterSheet();
        if (!sheet) {
            return { success: false, error: 'Master sheet not found' };
        }

        var data = sheet.getDataRange().getValues();
        var headers = trimHeaders(data[0]);

        var colIdx = {
            id: headers.indexOf(resolveAMField_('ID')),
            firstName: headers.indexOf(resolveAMField_('First Name')),
            lastName: headers.indexOf(resolveAMField_('Last Name')),
            address1: headers.indexOf(resolveAMField_('Street Address')),
            // v5.27: corrected to actual AM column name from LU_FieldMap
            address2: headers.indexOf(resolveAMField_('Apartment # or Upper or Lower or Lot #')),
            city: headers.indexOf(resolveAMField_('City')),
            phone: headers.indexOf(resolveAMField_('Phone Number')),
            schedDistribCode: headers.indexOf(resolveAMField_('Scheduled Distribution Code')),
            babyBox: headers.indexOf(resolveAMField_('Take Baby Box?')),
            schedBoxCode3: headers.indexOf(resolveAMField_('Scheduled Box Code 3')),
            serviceStatus: headers.indexOf(resolveAMField_('Service Status')),  // v5.27
            // v5.24: Contact Info sheet fields
            adminNotes: headers.indexOf(resolveAMField_('Admin Notes')),
            serviceHow: headers.indexOf(resolveAMField_('Service How')),
            requestDate: headers.indexOf(resolveAMField_('Request Date')),
            // v5.27: corrected to actual AM raw headers per LU_FieldMap
            referralInfo: headers.indexOf(resolveAMField_('More information about the person or organization that referred you.')),
            referrerName: headers.indexOf(resolveAMField_("What is the person's name who referred you?")),
            additionalInfo: headers.indexOf(resolveAMField_('Please tell us anything else you would like us to know about how you heard about Giving to the Nations or our program.')),
            phoneType: headers.indexOf(resolveAMField_('Phone type?')),
            email: headers.indexOf(resolveAMField_('Email')),
            bestContact: headers.indexOf(resolveAMField_('Best contact method (What is the best way to get ahold of you?)')),
            incomeLevel: headers.indexOf(resolveAMField_('Total annual household income. (How much money does your family make in 1 year?)'))
        };

        if (colIdx.schedDistribCode === -1) {
            return { success: false, error: 'Scheduled Distribution Code column not found in Applicants_Master' };
        }

        // v5.29: hasExtraBox driven by LU_SchedDisbCodes Box3 (not per-row AM Scheduled Box Code 3).
        // If Box3 has a value for this distribCode every scheduled recipient gets an 'X'.
        var luBox3ForReport = '';
        try {
            var luDetForReport = getSchedDisbCodeDetails(distribCode);
            if (luDetForReport && luDetForReport.success) {
                luBox3ForReport = luDetForReport.box3 || '';
            }
        } catch (luDetErr) {
            Logger.log('generateDistributionReport: Box3 LU lookup (non-fatal): ' + luDetErr.message);
        }
        var hasExtraBox = luBox3ForReport !== '';

        var filteredRecords = [];
        var hasBabyBox = false;

        for (var i = 1; i < data.length; i++) {
            var rowCode = (data[i][colIdx.schedDistribCode] || '').toString().toUpperCase();
            if (rowCode !== distribCode) continue;

            var babyVal = colIdx.babyBox !== -1 ? (data[i][colIdx.babyBox] || '').toString().trim().toUpperCase() : '';
            if (babyVal === 'X') hasBabyBox = true;

            var reqDate = colIdx.requestDate !== -1 ? data[i][colIdx.requestDate] : '';
            var reqDateStr = '';
            if (reqDate instanceof Date && !isNaN(reqDate.getTime())) {
                reqDateStr = Utilities.formatDate(reqDate, CONFIG.TIMEZONE, 'M/d/yyyy');
            } else if (reqDate) {
                reqDateStr = reqDate.toString().trim();
            }

            filteredRecords.push({
                submissionId: colIdx.id !== -1 ? (data[i][colIdx.id] || '').toString().trim() : '',
                firstName: colIdx.firstName !== -1 ? (data[i][colIdx.firstName] || '').toString().trim() : '',
                lastName: colIdx.lastName !== -1 ? (data[i][colIdx.lastName] || '').toString().trim() : '',
                address1: colIdx.address1 !== -1 ? (data[i][colIdx.address1] || '').toString().trim() : '',
                address2: colIdx.address2 !== -1 ? (data[i][colIdx.address2] || '').toString().trim() : '',
                city: colIdx.city !== -1 ? (data[i][colIdx.city] || '').toString().trim() : '',
                phone: colIdx.phone !== -1 ? (data[i][colIdx.phone] || '').toString().trim() : '',
                distribCode: distribCode,
                babyBox: babyVal === 'X' ? 'X' : '',
                extraBox: hasExtraBox ? 'X' : '',  // v5.29: X for ALL when LU Box3 present
                // v5.27: include service status for filtering and Contact Info sheet
                serviceStatus: colIdx.serviceStatus !== -1 ? (data[i][colIdx.serviceStatus] || '').toString().trim() : '',
                // Contact Info fields (v5.24)
                adminNotes: colIdx.adminNotes !== -1 ? (data[i][colIdx.adminNotes] || '').toString().trim() : '',
                serviceHow: colIdx.serviceHow !== -1 ? (data[i][colIdx.serviceHow] || '').toString().trim() : '',
                requestDate: reqDateStr,
                referralInfo: colIdx.referralInfo !== -1 ? (data[i][colIdx.referralInfo] || '').toString().trim() : '',
                referrerName: colIdx.referrerName !== -1 ? (data[i][colIdx.referrerName] || '').toString().trim() : '',
                additionalInfo: colIdx.additionalInfo !== -1 ? (data[i][colIdx.additionalInfo] || '').toString().trim() : '',
                phoneType: colIdx.phoneType !== -1 ? (data[i][colIdx.phoneType] || '').toString().trim() : '',
                email: colIdx.email !== -1 ? (data[i][colIdx.email] || '').toString().trim() : '',
                bestContact: colIdx.bestContact !== -1 ? (data[i][colIdx.bestContact] || '').toString().trim() : '',
                incomeLevel: colIdx.incomeLevel !== -1 ? (data[i][colIdx.incomeLevel] || '').toString().trim() : ''
            });
        }

        if (filteredRecords.length === 0) {
            return { success: false, error: 'No records found for distribution code: ' + distribCode };
        }

        // Sort: Last Name, First Name (applied to all records)
        filteredRecords.sort(function (a, b) {
            var lc = a.lastName.localeCompare(b.lastName);
            return lc !== 0 ? lc : a.firstName.localeCompare(b.firstName);
        });

        // v5.27: Scheduled sheet only includes rows with Service Status = 'Scheduled' or blank.
        // Contact Info sheet uses all records (filteredRecords).
        var scheduledRecords = filteredRecords.filter(function (r) {
            var s = r.serviceStatus.toUpperCase();
            return s === 'SCHEDULED' || s === '';
        });

        // v5.24: createDistributionReportSpreadsheet now also adds the Contact Info sheet
        // v5.27: pass scheduledRecords for the Scheduled sheet, filteredRecords for Contact Info
        var spreadsheet = createDistributionReportSpreadsheet(
            distribCode, scheduledRecords, hasBabyBox, hasExtraBox,
            startDate, endDate, pickupTimes, filteredRecords
        );

        logAudit('REPORT', null,
            'Generated Distribution Report for ' + distribCode + ': ' +
            filteredRecords.length + ' total, ' + scheduledRecords.length + ' scheduled');

        return {
            success: true,
            recordCount: filteredRecords.length,
            reportUrl: spreadsheet.getUrl(),
            downloadUrl: 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx',
            reportId: spreadsheet.getId()
        };

    } catch (e) {
        Logger.log('generateDistributionReport error: ' + e.message);
        return { success: false, error: 'Report generation failed: ' + e.message };
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
 * Processes the latest Scheduling Report from the Scheduling folder.
 * Reads report rows, updates Applicants_Master SchedDisbCode/ServiceStatus,
 * looks up LU_SchedDisbCodes for StartDate/Interval/FundingSource.
 *
 * v5.29 - Active-code filter: rows with SchedDisbCode not active in LU_SchedDisbCodes
 *          are skipped (logged as 'skip'). Status written as report's Service Status
 *          when present; defaults to 'Scheduled' when blank. Fixed corrupted MIME type.
 *
 * @returns {Object} { success, updatedCount, skippedCount, reportName, log[] }
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
        var rptIdCol = rptHeaders.indexOf(getReportHeader_('ID'));
        var rptSchedCodeCol = rptHeaders.indexOf(getReportHeader_('Scheduled Distribution Code'));
        var rptStatusCol = rptHeaders.indexOf(getReportHeader_('Service Status'));

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

        // v5.29: build Set of active SchedDisbCodes — only active codes trigger AM updates
        var activeDisbCodeSet = {};
        try {
            getActiveSchedDisbCodes().forEach(function (c) {
                if (c.code) activeDisbCodeSet[c.code.toString().trim().toUpperCase()] = true;
            });
        } catch (acErr) {
            Logger.log('processSchedulingReport: active codes lookup (non-fatal): ' + acErr.message);
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
            var logMsg = ''; // hoisted to avoid redeclaration across if/else blocks

            // If SchedDisbCode is present → validate active, then update AM
            if (schedCode) {
                var upperCode = schedCode.toUpperCase();

                // v5.29: skip rows whose SchedDisbCode is not active in LU_SchedDisbCodes
                if (!activeDisbCodeSet[upperCode]) {
                    log.push({ status: 'skip', message: 'ID ' + recId + ': SchedDisbCode ' + upperCode + ' not active — skipped' });
                    continue;
                }

                // Write SchedDisbCode to AM
                if (amSchedCodeCol !== -1) {
                    masterSheet.getRange(rowNum, amSchedCodeCol + 1).setValue(upperCode);
                }

                // v5.29: use report's Service Status if present; default to 'Scheduled' if blank
                var newStatus = rptStatus || 'Scheduled';
                masterSheet.getRange(rowNum, amStatusCol + 1).setValue(newStatus);

                // Look up code details from LU_SchedDisbCodes (cached)
                if (!codeDetailsCache[upperCode]) {
                    try {
                        codeDetailsCache[upperCode] = getSchedDisbCodeDetails(upperCode);
                    } catch (e) {
                        codeDetailsCache[upperCode] = { success: false, error: e.message };
                    }
                }

                var details = codeDetailsCache[upperCode];
                logMsg = 'ID ' + recId + ': Status → ' + newStatus + ' (' + upperCode + ')';

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
                logMsg = 'ID ' + recId + ': Status → ' + rptStatus;

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
            skippedCount: log.filter(function (l) { return l.status === 'skip'; }).length,
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
        const changedRows = {}; // v5.35: track which masterData rows were modified → { sheetRow: true }

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

            // v5.35: Apply all changes in memory (masterData array), track changed rows
            const dataRowIdx = rowNum - 1; // 0-based index into masterData

            // Set Service Status
            masterData[dataRowIdx][serviceStatusCol] = newStatus;
            changedRows[rowNum] = true;

            // Set date fields using the effective date (Picked Up date or ending date)
            if (effectiveDate) {
                // Last Date Served
                masterData[dataRowIdx][lastDateServedCol] = effectiveDate;

                // Final Service Contact Date
                if (finalServiceContactDateCol !== -1) {
                    masterData[dataRowIdx][finalServiceContactDateCol] = effectiveDate;
                }

                // Next Service Availability Date = effective date + 90 days
                if (nextServiceAvailCol !== -1 && effectiveDateObj && !isNaN(effectiveDateObj.getTime())) {
                    const nextAvail = new Date(effectiveDateObj.getTime() + (90 * 24 * 60 * 60 * 1000));
                    masterData[dataRowIdx][nextServiceAvailCol] = Utilities.formatDate(nextAvail, CONFIG.TIMEZONE, 'M/d/yyyy');
                }

                // First Service Contact Date = Scheduled Start Date (Picked Up rows only)
                if (hasDate && firstServiceContactDateCol !== -1 && scheduledStartDate) {
                    masterData[dataRowIdx][firstServiceContactDateCol] = scheduledStartDate;
                }
            }

            // Box 1 from LU_SchedDisbCodes → Box Code, Scheduled Box Code 1, Received Product Code 1
            if (luBox1) {
                if (boxCodeCol !== -1) masterData[dataRowIdx][boxCodeCol] = luBox1;
                if (schedBoxCode1Col !== -1) masterData[dataRowIdx][schedBoxCode1Col] = luBox1;
                if (receivedProductCode1Col !== -1) masterData[dataRowIdx][receivedProductCode1Col] = luBox1;
            }

            // Box 2 from LU_SchedDisbCodes → Scheduled Box Code 2, Received Product Code 2
            if (luBox2) {
                if (schedBoxCode2Col !== -1) masterData[dataRowIdx][schedBoxCode2Col] = luBox2;
                if (receivedProductCode2Col !== -1) masterData[dataRowIdx][receivedProductCode2Col] = luBox2;
            }

            // v5.15: Box 3 is per-client; Extra Box = "X" in report confirms a value is present.
            const rowHasExtraBox = extraBoxCol !== -1
                ? (reportData[i][extraBoxCol] || '').toString().trim().toUpperCase() === 'X'
                : false;
            let amBox3Written = '';
            if (rowHasExtraBox) {
                const amBox3 = schedBoxCode3Col !== -1
                    ? (masterData[dataRowIdx][schedBoxCode3Col] || '').toString().trim()
                    : (luBox3 || '');
                const effectiveBox3 = amBox3 || luBox3;
                if (effectiveBox3) {
                    if (receivedProductCode3Col !== -1) {
                        masterData[dataRowIdx][receivedProductCode3Col] = effectiveBox3;
                    }
                    amBox3Written = effectiveBox3;
                }
            }

            // v5.15: Queue DR/PF_Products update for DR/PF-coded rows
            const amBox1Effective = luBox1 || (schedBoxCode1Col !== -1 ? (masterData[dataRowIdx][schedBoxCode1Col] || '').toString().trim() : '');
            const amBox2Effective = luBox2 || (schedBoxCode2Col !== -1 ? (masterData[dataRowIdx][schedBoxCode2Col] || '').toString().trim() : '');
            const isDRPF = function (v) { var u = v.toUpperCase(); return u.indexOf('DR') === 0 || u.indexOf('PF') === 0; };
            const needsProductUpdate = (amBox1Effective && isDRPF(amBox1Effective))
                || (amBox2Effective && isDRPF(amBox2Effective))
                || (amBox3Written && isDRPF(amBox3Written));
            if (needsProductUpdate && requestDateCol !== -1) {
                const rawReqDate = masterData[dataRowIdx][requestDateCol];
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

        // v5.35: Bulk write — one setValues() per changed AM row (replaces per-field setValue() calls)
        const changedRowNums = Object.keys(changedRows).map(Number).sort(function (a, b) { return a - b; });
        for (let cr = 0; cr < changedRowNums.length; cr++) {
            const rn = changedRowNums[cr];
            masterSheet.getRange(rn, 1, 1, masterData[0].length).setValues([masterData[rn - 1]]);
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

        // File deletion: Reports folder = 1 month; Distribution folder = inactive codes
        const fileCutoffDate = new Date();
        fileCutoffDate.setMonth(fileCutoffDate.getMonth() - 1);

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
        try {
            const dataWB = getDataWorkbook();
            const prodSheet = dataWB.getSheetByName(CONFIG.PF_PRODUCTS_SHEET);
            if (prodSheet && prodSheet.getLastRow() > 1) {
                const prodData = prodSheet.getDataRange().getValues();
                const prodHeaders = trimHeaders(prodData[0]);
                const prodIdCol = prodHeaders.indexOf('ID');
                var prodReqDateCol = prodHeaders.indexOf('Request Date');
                if (prodReqDateCol === -1) prodReqDateCol = prodHeaders.indexOf('RequestDate');

                if (prodIdCol !== -1 && prodReqDateCol !== -1) {
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
                            productRecordCount++;
                        }
                    }
                }
            }
        } catch (e) {
            Logger.log('Error counting product records: ' + e.message);
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

        // v5.25: Count Distribution folder files for inactive SchedDisbCodes separately
        let distribFileCount = 0;
        if (CONFIG.DISTRIBUTION_FOLDER_ID) {
            try {
                distribFileCount = countInactiveDistributionFiles();
            } catch (e) {
                Logger.log('Error counting inactive distribution files: ' + e.message);
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
            fileCount: fileCount,
            distribFileCount: distribFileCount,
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
 * 5. Deletes report files older than 1 month (Reports folder); deletes Distribution
 *    folder files for inactive SchedDisbCodes
 * 6. Checks G2N_Archive capacity — auto-triggers year rollover if >= 85%
 * @param {string} cutoffDateStr - Cutoff date in YYYY-MM-DD format
 * @param {number} alreadyArchived - Count of previously archived records (for batching)
 * @returns {Object} { success, archivedInBatch, deletedInBatch, log[] }
 */
/**
 * Syncs an Archive sheet's header row to match AM headers.
 * - Renames Archive columns whose Previous Header Name matches an AM column
 *   that has since been renamed (detected via LU_FieldMap byPreviousHeaderName).
 * - Adds any AM columns still missing from Archive after rename resolution.
 * - Skips columns with blank AM headers.
 * - Never removes Archive columns (data preservation).
 * - Logs all changes to the archive log array.
 * Called by executeArchiveBatch() before every write.
 * v5.40 - New function.
 * v5.41 - Added rename detection via LU_FieldMap Previous Header Name column.
 * @param {Sheet}  archiveSheet - The Archive sheet to sync
 * @param {Array}  amHeaderRow  - Raw AM header row (data[0], not yet trimmed)
 * @param {Array}  log          - Archive log array for status messages
 * @returns {Object} { added, renamed, alreadyInSync }
 */
function syncArchiveHeaders_(archiveSheet, amHeaderRow, log) {
    var amHeaders = trimHeaders(amHeaderRow);
    var archLastCol = archiveSheet.getLastColumn();
    var archRaw = archiveSheet.getRange(1, 1, 1, archLastCol).getValues()[0];
    var archHeaders = trimHeaders(archRaw);

    // Build set of current AM headers (non-blank)
    var amSet = {};
    amHeaders.forEach(function (h) { if (h) amSet[h] = true; });

    // Build set of current Archive headers
    var archSet = {};
    archHeaders.forEach(function (h, i) { if (h) archSet[h] = i; }); // name → 0-based index

    // Load rename registry from LU_FieldMap
    var renameMap = {}; // { oldName: newName }
    try {
        var fm = loadFieldMap();
        if (fm.byPreviousHeaderName) {
            for (var oldName in fm.byPreviousHeaderName) {
                renameMap[oldName] = fm.byPreviousHeaderName[oldName];
            }
        }
    } catch (fmErr) {
        Logger.log('syncArchiveHeaders_: Could not load FieldMap for rename detection: ' + fmErr.message);
    }

    var renamedCount = 0;
    var addedCount = 0;

    // ── Step 1: Rename Archive columns where Previous Header Name matches ─────
    archHeaders.forEach(function (archH, i) {
        if (!archH) return;
        // Is this an old name that has since been renamed in AM?
        if (renameMap[archH] && amSet[renameMap[archH]] && !archSet[renameMap[archH]]) {
            var newName = renameMap[archH];
            // Rename the Archive header cell in place
            archiveSheet.getRange(1, i + 1).setValue(newName);
            log.push({
                status: 'success',
                message: 'Archive header sync: renamed col ' + (i + 1) + ' "' + archH + '" → "' + newName + '"'
            });
            Logger.log('syncArchiveHeaders_: renamed Archive col ' + (i + 1) + ' "' + archH + '" → "' + newName + '"');
            // Update local tracking
            delete archSet[archH];
            archSet[newName] = i;
            archHeaders[i] = newName;
            renamedCount++;
        }
    });

    // ── Step 2: Append AM columns still missing from Archive ──────────────────
    var toAdd = [];
    amHeaders.forEach(function (h) {
        if (h && !archSet.hasOwnProperty(h)) toAdd.push(h);
    });

    if (toAdd.length > 0) {
        var startCol = archiveSheet.getLastColumn() + 1;
        archiveSheet.getRange(1, startCol, 1, toAdd.length).setValues([toAdd])
            .setFontWeight('bold').setBackground('#f4b400').setFontColor('white');
        log.push({
            status: 'success',
            message: 'Archive header sync: added ' + toAdd.length + ' new column(s): ' + toAdd.join(', ')
        });
        Logger.log('syncArchiveHeaders_: added ' + toAdd.length + ' column(s): ' + toAdd.join(', '));
        addedCount = toAdd.length;
    }

    if (renamedCount === 0 && addedCount === 0) {
        log.push({ status: 'info', message: 'Archive header sync: already in sync (' + archHeaders.length + ' columns)' });
        return { added: 0, renamed: 0, alreadyInSync: true };
    }

    return { added: addedCount, renamed: renamedCount, alreadyInSync: false };
}


/**
 * Remaps an AM data row to match a target header order.
 * For each target column, finds the value from the source row by header name.
 * Missing source columns produce empty string. Extra source columns are dropped.
 * Used by executeArchiveBatch() to write correctly regardless of column order.
 * v5.40 - New function.
 * @param {Array}  sourceRow     - Source data row (AM row values)
 * @param {Array}  sourceHeaders - Trimmed AM header names
 * @param {Array}  targetHeaders - Trimmed Archive header names
 * @returns {Array} Row values aligned to targetHeaders
 */
function remapRowToHeaders_(sourceRow, sourceHeaders, targetHeaders) {
    // Build source column index map
    var srcIdx = {};
    sourceHeaders.forEach(function (h, i) { if (h) srcIdx[h] = i; });

    return targetHeaders.map(function (h) {
        if (!h) return '';
        var i = srcIdx[h];
        return (i !== undefined && i < sourceRow.length)
            ? (sourceRow[i] !== null && sourceRow[i] !== undefined ? sourceRow[i] : '')
            : '';
    });
}


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

        // v5.25: File deletion: Reports folder = 1 month; Distribution folder = inactive codes
        const fileCutoffDate = new Date();
        fileCutoffDate.setMonth(fileCutoffDate.getMonth() - 1);

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

        // ===== SYNC ARCHIVE HEADERS TO AM =====
        // Compares AM headers to Archive headers and adds any missing columns so
        // positional writes always land in the correct column. Runs every time —
        // if already in sync this costs one header read and exits immediately.
        var archSyncResult = syncArchiveHeaders_(archiveSheet, data[0], log);
        // After sync, re-read archive headers so we know the current column order
        var archHeaders = trimHeaders(archiveSheet.getRange(1, 1, 1, archiveSheet.getLastColumn()).getValues()[0]);

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
                    // Remap AM row to Archive column order (handles any column differences)
                    archiveRows.push(remapRowToHeaders_(data[i], headers, archHeaders));
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
                // v5.34: ID-only sweep catches rows missed by composite-key mismatch
                const archivedIds = archiveIdDates.map(function (r) { return r.id.toString().trim(); });
                archiveOrphanedProducts(archivedIds, archiveWorkbook, log);
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
        log.push({ status: 'info', message: 'Deleting report files older than 1 month...' });

        const reportsFolderId = CONFIG.REPORTS_FOLDER_ID;
        if (reportsFolderId) {
            try {
                deletedInBatch = deleteOldFilesInFolder(reportsFolderId, fileCutoffDate, log);
                if (deletedInBatch === 0) {
                    log.push({ status: 'info', message: 'No report files older than 1 month found' });
                }
            } catch (folderError) {
                log.push({ status: 'error', message: 'Error deleting report files: ' + folderError.message });
            }
        }

        // v5.25: Delete Distribution folder files for inactive SchedDisbCodes
        if (CONFIG.DISTRIBUTION_FOLDER_ID) {
            log.push({ status: 'info', message: 'Deleting distribution files for inactive SchedDisbCodes...' });
            try {
                var distribDeleted = deleteInactiveDistributionFiles(log);
                deletedInBatch += distribDeleted;
                if (distribDeleted === 0) {
                    log.push({ status: 'info', message: 'No inactive distribution files found to delete' });
                }
            } catch (folderError) {
                log.push({ status: 'error', message: 'Error deleting inactive distribution files: ' + folderError.message });
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
 * ID-only sweep of DR/PF_Products for a set of archived applicant IDs.
 * @param {string[]}   archivedIds   - Array of applicant ID strings just archived
 * @param {Spreadsheet} archiveWorkbook - Already-open G2N_Archive workbook
 * @param {Array}      log           - Log array for messages
 * @returns {number} Count of additional rows moved
 */
function archiveOrphanedProducts(archivedIds, archiveWorkbook, log) {
    try {
        if (!archivedIds || archivedIds.length === 0) return 0;

        var dataWB = getDataWorkbook();
        var prodSheet = dataWB.getSheetByName(CONFIG.PF_PRODUCTS_SHEET);
        if (!prodSheet || prodSheet.getLastRow() <= 1) return 0;

        var prodData = prodSheet.getDataRange().getValues();
        var prodHeaders = trimHeaders(prodData[0]);
        var idCol = prodHeaders.indexOf('ID');
        if (idCol === -1) return 0;

        var archivedSet = {};
        archivedIds.forEach(function (id) { archivedSet[id.toString().trim()] = true; });

        var keepRows = [];
        var orphanRows = [];

        for (var i = 1; i < prodData.length; i++) {
            var rowId = (prodData[i][idCol] || '').toString().trim();
            if (rowId && archivedSet[rowId]) {
                orphanRows.push(prodData[i]);
            } else {
                keepRows.push(prodData[i]);
            }
        }

        if (orphanRows.length === 0) {
            log.push({ status: 'info', message: 'Orphaned product sweep: no additional rows found' });
            return 0;
        }

        // Append to Products_Archive
        var prodArchiveSheet = archiveWorkbook.getSheetByName('Products_Archive');
        if (!prodArchiveSheet) {
            prodArchiveSheet = archiveWorkbook.insertSheet('Products_Archive');
            prodArchiveSheet.getRange(1, 1, 1, prodHeaders.length).setValues([prodData[0]]);
            prodArchiveSheet.getRange(1, 1, 1, prodHeaders.length)
                .setFontWeight('bold').setBackground('#f4b400').setFontColor('white');
            prodArchiveSheet.setFrozenRows(1);
        }
        var lastRow = prodArchiveSheet.getLastRow();
        prodArchiveSheet.getRange(lastRow + 1, 1, orphanRows.length, orphanRows[0].length)
            .setValues(orphanRows);

        // Rewrite DR/PF_Products without orphan rows
        var totalRows = prodSheet.getMaxRows();
        if (totalRows > 1) {
            prodSheet.getRange(2, 1, totalRows - 1, prodSheet.getMaxColumns()).clearContent();
        }
        if (keepRows.length > 0) {
            prodSheet.getRange(2, 1, keepRows.length, keepRows[0].length).setValues(keepRows);
        }

        log.push({ status: 'success', message: 'Orphaned product sweep: moved ' + orphanRows.length + ' additional rows to Products_Archive' });
        return orphanRows.length;

    } catch (e) {
        log.push({ status: 'error', message: 'Orphaned product sweep error: ' + e.message });
        return 0;
    }
}

/**
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
 * Deletes files in the Distribution folder whose SchedDisbCode is inactive
 * in LU_SchedDisbCodes. Filename pattern: Distribution_<code>_yyyy-MM-dd.
 * v5.25 - New function replacing the previous "skip Distribution folder" logic.
 * @param {Array} log - Log array for messages
 * @returns {number} Count of deleted files
 */
function deleteInactiveDistributionFiles(log) {
    var inactiveCodes = getInactiveSchedDisbCodes_();
    if (inactiveCodes.length === 0) {
        log.push({ status: 'info', message: 'Distribution folder: no inactive SchedDisbCodes — nothing to delete' });
        return 0;
    }

    var count = 0;
    try {
        var folder = DriveApp.getFolderById(CONFIG.DISTRIBUTION_FOLDER_ID);
        var files = folder.getFiles();
        while (files.hasNext()) {
            var file = files.next();
            var name = file.getName();
            var match = name.match(/^Distribution_(.+)_\d{4}-\d{2}-\d{2}/i);
            if (match) {
                var fileCode = match[1].toUpperCase();
                if (inactiveCodes.indexOf(fileCode) !== -1) {
                    file.setTrashed(true);
                    count++;
                    log.push({ status: 'success', message: 'Deleted inactive distribution file: ' + name });
                }
            }
        }
    } catch (e) {
        Logger.log('deleteInactiveDistributionFiles error: ' + e.message);
        log.push({ status: 'error', message: 'Error accessing Distribution folder: ' + e.message });
    }
    return count;
}

/**
 * Counts Distribution folder files for inactive SchedDisbCodes (used by previewArchive).
 * v5.25 - New function.
 * @returns {number} Count of files that would be deleted
 */
function countInactiveDistributionFiles() {
    var inactiveCodes = getInactiveSchedDisbCodes_();
    if (inactiveCodes.length === 0) return 0;

    var count = 0;
    try {
        var folder = DriveApp.getFolderById(CONFIG.DISTRIBUTION_FOLDER_ID);
        var files = folder.getFiles();
        while (files.hasNext()) {
            var file = files.next();
            var match = file.getName().match(/^Distribution_(.+)_\d{4}-\d{2}-\d{2}/i);
            if (match && inactiveCodes.indexOf(match[1].toUpperCase()) !== -1) {
                count++;
            }
        }
    } catch (e) {
        Logger.log('countInactiveDistributionFiles error: ' + e.message);
    }
    return count;
}

/**
 * Returns uppercase SchedDisbCode strings that are inactive in LU_SchedDisbCodes.
 * v5.25 - New private helper for distribution file cleanup.
 * @returns {string[]}
 */
function getInactiveSchedDisbCodes_() {
    try {
        var sheet = getLookupsWorkbook().getSheetByName(CONFIG.LOOKUPS.SCHED_DISB_CODES);
        if (!sheet || sheet.getLastRow() < 2) return [];
        var data = sheet.getDataRange().getValues();
        var headers = trimHeaders(data[0]);
        var codeCol = headers.indexOf('SchedDisbCode');
        var activeCol = headers.indexOf('Active');
        if (codeCol === -1) return [];
        var inactive = [];
        for (var i = 1; i < data.length; i++) {
            var code = (data[i][codeCol] || '').toString().trim().toUpperCase();
            if (!code) continue;
            if (activeCol !== -1 && isRowActive(data[i][activeCol])) continue;
            inactive.push(code);
        }
        return inactive;
    } catch (e) {
        Logger.log('getInactiveSchedDisbCodes_ error: ' + e.message);
        return [];
    }
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




// ─────────────────────────────────────────────────────────────────────────────
// v5.43 — PRODUCT COUNT DIAGNOSTIC
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Diagnostic: compares the two ID lists that Grant Summary builds internally
 * and identifies the records counted in Total Requests that contribute zero
 * to Products Distributed.
 *
 * List 1 (Total Requests): every record where Service Status = Picked Up or
 *   Delivered and Request Date is in range — exactly the same getCombinedData()
 *   call Grant Summary uses.
 *
 * List 2 (Products Distributed contributors): records from List 1 where
 *   calculateProductCounts() returns productsDistributed > 0, using the
 *   same loadProductLookupData() call Grant Summary uses.
 *
 * Missing = List 1 minus List 2.
 *
 * For missing records the diagnostic notes why productsDistributed was 0:
 *   - "No codes" — all three slots blank
 *   - "Baby codes only" — all non-blank slots start with DB (baby products
 *      go to babyDistributed, not productsDistributed — by design)
 *   - "Codes not in lookup" — codes present but distProdByBox / drPfByIdDate
 *      returned nothing
 *   - "Mixed" — combination of the above
 *
 * Output: Google Sheet in Grants folder with two tabs:
 *   1. Summary  — record counts and % missing
 *   2. Missing Records — ID, First Name, Last Name, Service Status,
 *                        Request Date, Code 1, Code 2, Code 3, Baby Dist, Note
 *
 * @param {string} fromDateStr - YYYY-MM-DD
 * @param {string} toDateStr   - YYYY-MM-DD
 * @returns {Object} { success, recordCount, mismatchCount, reportUrl, downloadUrl, error }
 */
function diagProductCounts(fromDateStr, toDateStr) {
    try {
        if (!fromDateStr || !toDateStr) {
            return { success: false, error: 'Both From Date and To Date are required.' };
        }
        var fromDate = parseDateInput(fromDateStr, false);
        var toDate = parseDateInput(toDateStr, true);
        if (fromDate > toDate) {
            return { success: false, error: 'From Date must be before To Date.' };
        }

        // ── Step 1: Same getCombinedData call as generateGrantSummaryReport ──
        // Filter: Service Status = Picked Up or Delivered (List 1)
        var combined = getCombinedData(fromDate, toDate, [
            { column: 'Service Status', values: ['Picked Up', 'Delivered'] }
        ]);

        if (combined.totalCount === 0) {
            return {
                success: false,
                error: 'No Picked Up / Delivered records for ' +
                    fromDateStr + ' to ' + toDateStr + '.'
            };
        }

        var headers = combined.headers;
        var rows = combined.rows;

        // ── Step 2: Same loadProductLookupData call as generateGrantSummaryReport
        var productData = loadProductLookupData(fromDate, toDate);

        // Column indices — identical to Grant Summary
        var colIdx = {
            id: headers.indexOf(resolveAMField_('ID')),
            firstName: headers.indexOf(resolveAMField_('First Name')),
            lastName: headers.indexOf(resolveAMField_('Last Name')),
            requestDate: headers.indexOf(resolveAMField_('Request Date')),
            serviceStatus: headers.indexOf(resolveAMField_('Service Status')),
            lastDateServed: headers.indexOf(resolveAMField_('Last Date Served')),
            productCode1: headers.indexOf(resolveAMField_('Received Product Code 1')),
            productCode2: headers.indexOf(resolveAMField_('Received Product Code 2')),
            productCode3: headers.indexOf(resolveAMField_('Received Product Code 3'))
        };

        // v5.53: Processed By and Entered By raw AM headers are driven by LU_FieldMap
        // which we can't query statically. Resolve via FieldMapService SV Field ID
        // lookup first, then fall back to a case-insensitive header scan so the
        // diagnostic works regardless of how the columns were named in the sheet.
        function resolveByFieldId_(fieldId, fallbackSubstring) {
            // Try resolving through FieldMapService by SV Field ID
            try {
                var fm = loadFieldMap();
                var entry = fm.bySvFieldId[fieldId];
                if (entry) {
                    var idx = headers.indexOf(entry.rawHeader);
                    if (idx !== -1) return idx;
                }
            } catch (e) { /* non-fatal */ }
            // Fallback: case-insensitive partial match on header name
            var lower = fallbackSubstring.toLowerCase();
            for (var hi = 0; hi < headers.length; hi++) {
                if (headers[hi].toLowerCase().indexOf(lower) !== -1) return hi;
            }
            return -1;
        }
        colIdx.processedBy = resolveByFieldId_('fld_ProcessedBy', 'processed by');
        colIdx.enteredBy = resolveByFieldId_('fld_EnteredBy', 'entered by');

        Logger.log('diagProductCounts colIdx: processedBy=' + colIdx.processedBy +
            ' enteredBy=' + colIdx.enteredBy +
            ' lastDateServed=' + colIdx.lastDateServed);

        if (colIdx.id === -1) {
            return { success: false, error: 'ID column not found in record set.' };
        }

        // ── Step 3: Walk every row exactly as Grant Summary does.
        // Collect List 1 (all IDs) and List 2 (IDs where productsDistributed > 0).
        var missingRecords = [];   // in List 1 but not List 2
        var totalCount = rows.length;
        var contributorCount = 0;

        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            var recId = getStr(row, colIdx.id);
            var reqDate = new Date(row[colIdx.requestDate]);
            var code1 = getStr(row, colIdx.productCode1);
            var code2 = getStr(row, colIdx.productCode2);
            var code3 = getStr(row, colIdx.productCode3);

            // Same calculateProductCounts call Grant Summary uses
            var products = calculateProductCounts(
                recId, code1, code2, code3, reqDate, productData, headers, row);

            if (products.productsDistributed > 0) {
                contributorCount++;
            } else {
                // Classify why productsDistributed = 0
                var codes = [code1, code2, code3].filter(function (c) { return c !== ''; });
                var category = '';  // 'BY_DESIGN' or 'NEEDS_ATTENTION'
                var note = '';
                if (codes.length === 0) {
                    category = 'BY_DESIGN';
                    note = 'No product codes assigned — all three slots blank. ' +
                        'This record has no box codes so there is nothing to count. ' +
                        'Expected if this was a baby-box-only visit tracked elsewhere.';
                } else {
                    var allBaby = codes.every(function (c) {
                        return c.toUpperCase().indexOf('DB') === 0;
                    });
                    if (allBaby) {
                        category = 'BY_DESIGN';
                        note = 'Baby codes only (' + codes.join(', ') + '). ' +
                            'Baby products go to Baby Products Distributed (=' +
                            products.babyDistributed + '), not Products Distributed. ' +
                            'This is correct — no action needed.';
                    } else {
                        category = 'NEEDS_ATTENTION';
                        note = 'Codes present (' + codes.join(', ') + ') but ' +
                            'productsDistributed=0. ' +
                            'Check that these box codes exist in Distributed_Products ' +
                            'with the correct product rows.';
                    }
                }

                missingRecords.push({
                    id: recId,
                    fn: getStr(row, colIdx.firstName),
                    ln: getStr(row, colIdx.lastName),
                    ss: getStr(row, colIdx.serviceStatus),
                    rdFmt: isNaN(reqDate.getTime()) ? '' :
                        Utilities.formatDate(reqDate, CONFIG.TIMEZONE, 'M/d/yyyy'),
                    lastServed: (function () {
                        var v = colIdx.lastDateServed !== -1 ? row[colIdx.lastDateServed] : '';
                        if (!v) return '';
                        var d = (v instanceof Date) ? v : new Date(v);
                        return isNaN(d.getTime()) ? '' :
                            Utilities.formatDate(d, CONFIG.TIMEZONE, 'M/d/yyyy');
                    })(),
                    processedBy: getStr(row, colIdx.processedBy),
                    enteredBy: getStr(row, colIdx.enteredBy),
                    code1: code1,
                    code2: code2,
                    code3: code3,
                    babyDist: products.babyDistributed,
                    category: category,
                    note: note
                });
            }
        }

        // Split missingRecords into two lists
        var needsAttention = missingRecords.filter(function (r) { return r.category === 'NEEDS_ATTENTION'; });
        var byDesign = missingRecords.filter(function (r) { return r.category === 'BY_DESIGN'; });
        var missingCount = missingRecords.length;

        // ── Step 4: Write Google Sheet ────────────────────────────────────────
        var stamp = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd_HHmm');
        var wb = SpreadsheetApp.create(
            'Product_Count_Diagnostic_' + fromDateStr + '_to_' + toDateStr + '_' + stamp);

        // ── Tab 1: Summary ────────────────────────────────────────────────────
        var sumSheet = wb.getActiveSheet();
        sumSheet.setName('Summary');

        var sumHdrs = ['Metric', 'Count'];
        sumSheet.getRange(1, 1, 1, 2).setValues([sumHdrs])
            .setFontWeight('bold').setBackground('#1a73e8').setFontColor('white');

        var pctAttn = totalCount > 0
            ? Math.round((needsAttention.length / totalCount) * 100) + '%' : '0%';

        var sumData = [
            ['Date Range', fromDateStr + ' to ' + toDateStr],
            ['Source', 'Applicants_Master + Archive (' + combined.masterCount +
                ' master, ' + combined.archiveCount + ' archive)'],
            ['', ''],
            ['Total Qualifying Records (List 1 — same as Grant Summary Total Requests)',
                totalCount],
            ['Records Contributing to Products Distributed (List 2)',
                contributorCount],
            ['', ''],
            ['Records NOT Contributing — NEEDS ATTENTION (codes present but 0 products)',
                needsAttention.length],
            ['% of Total Needing Attention', pctAttn],
            ['', ''],
            ['Records NOT Contributing — BY DESIGN (baby-only or no codes assigned)',
                byDesign.length],
            ['  — Baby codes only (correct — counted in Baby Products Distributed)',
                byDesign.filter(function (r) { return r.note.indexOf('Baby codes') === 0; }).length],
            ['  — No codes assigned (no box codes in any slot)',
                byDesign.filter(function (r) { return r.note.indexOf('No product codes') === 0; }).length]
        ];

        sumSheet.getRange(2, 1, sumData.length, 2).setValues(sumData);

        // Highlight Needs Attention rows red, By Design rows blue
        sumSheet.getRange(8, 1, 1, 2)
            .setBackground(needsAttention.length > 0 ? '#fce8e6' : '#e6f4ea')
            .setFontWeight('bold')
            .setFontColor(needsAttention.length > 0 ? '#d93025' : '#34a853');
        sumSheet.getRange(9, 1, 1, 2)
            .setBackground(needsAttention.length > 0 ? '#fce8e6' : '#e6f4ea')
            .setFontWeight('bold')
            .setFontColor(needsAttention.length > 0 ? '#d93025' : '#34a853');
        sumSheet.getRange(11, 1, 1, 2).setBackground('#e8f0fe').setFontWeight('bold');
        sumSheet.getRange(12, 1, 1, 2).setBackground('#f8f9fa');
        sumSheet.getRange(13, 1, 1, 2).setBackground('#f8f9fa');
        sumSheet.autoResizeColumn(1);
        sumSheet.autoResizeColumn(2);
        sumSheet.setColumnWidth(1, 580);

        // ── Common headers for detail tabs ────────────────────────────────────
        var missHdrs = [
            'Record ID', 'First Name', 'Last Name',
            'Service Status', 'Request Date', 'Last Date Served',
            'Processed By', 'Entered By',
            'Received Product Code 1',
            'Received Product Code 2',
            'Received Product Code 3',
            'Baby Products Distributed',
            'Note'
        ];

        function writeMissTab_(tabName, headerBg, records) {
            var sh = wb.insertSheet(tabName);
            sh.getRange(1, 1, 1, missHdrs.length).setValues([missHdrs])
                .setFontWeight('bold').setBackground(headerBg).setFontColor('white');
            sh.setFrozenRows(1);
            if (records.length > 0) {
                var data = records.map(function (r) {
                    return [r.id, r.fn, r.ln, r.ss, r.rdFmt, r.lastServed,
                    r.processedBy, r.enteredBy,
                    r.code1, r.code2, r.code3, r.babyDist, r.note];
                });
                sh.getRange(2, 1, data.length, missHdrs.length).setValues(data);
                for (var mi = 0; mi < data.length; mi++) {
                    var bg = (headerBg === '#d93025') ? '#fce8e6' :
                        (data[mi][11] > 0) ? '#e8f0fe' : '#f1f3f4';
                    sh.getRange(mi + 2, 1, 1, missHdrs.length).setBackground(bg);
                    sh.getRange(mi + 2, 13).setFontWeight('bold');
                }
            } else {
                sh.getRange(2, 1).setValue('None.');
                sh.getRange(2, 1).setFontStyle('italic').setFontColor('#34a853');
            }
            for (var mc = 1; mc <= missHdrs.length; mc++) sh.autoResizeColumn(mc);
            return sh;
        }

        // ── Tab 2: Needs Attention ────────────────────────────────────────────
        writeMissTab_('Needs Attention', '#d93025', needsAttention);

        // ── Tab 3: By Design (not errors) ────────────────────────────────────
        writeMissTab_('By Design', '#4a86e8', byDesign);

        // Reorder: Summary first
        wb.setActiveSheet(sumSheet);
        wb.moveActiveSheet(1);

        moveToFolder(wb.getId(), CONFIG.GRANTS_FOLDER_ID);
        logAudit('REPORT', null, 'Generated Product Count Diagnostic: ' +
            fromDateStr + ' to ' + toDateStr + ' — ' +
            totalCount + ' qualifying, ' + needsAttention.length + ' need attention, ' +
            byDesign.length + ' by design');

        return {
            success: true,
            recordCount: totalCount,
            detailCount: totalCount,
            mismatchCount: missingCount,
            reportUrl: wb.getUrl(),
            downloadUrl: 'https://docs.google.com/spreadsheets/d/' + wb.getId() + '/export?format=xlsx'
        };

    } catch (e) {
        Logger.log('diagProductCounts error: ' + e.message + '\n' + e.stack);
        return { success: false, error: 'Diagnostic failed: ' + e.message };
    }
}
