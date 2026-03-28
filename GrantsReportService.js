/**
 * @file GrantsReportService.js
 * @description Grant report generation for G2N System.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * VS CODE / CLASP INSTRUCTIONS
 * ─────────────────────────────────────────────────────────────────────────────
 * ACTION:  FULL FILE REPLACEMENT of GrantsReportService.gs
 * HOW:     Delete all content in your existing GrantsReportService.js and
 *          paste this entire file in its place.
 *
 * ⚠️  IMPORTANT — generateGrantSummaryReport() and _buildGrantSummaryDoc_() are
 *     NOT in this file. They are unchanged from v3.7 and must remain in your
 *     existing file below the content you paste. Do NOT delete them.
 *     This file ends with a comment reminding you of that.
 *
 * PREREQUISITES BEFORE THIS FILE WILL WORK:
 *   1. LU_ReportColumns tab added to G2N_Lookups (see CHANGES_INSTRUCTIONS.md)
 *   2. ReportColumnService.js added as a new file in this project
 *   3. clasp push completed
 *
 * NEW DEPENDENCIES THIS FILE ADDS:
 *   - getReportColumns()           from ReportColumnService.js
 *   - applyReportColumnFormatting() from ReportColumnService.js
 *   - getReportHeader_()           from SharedUtils.js (already exists)
 *   - resolveAMField_()            from SharedUtils.js (already exists)
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * VERSION HISTORY
 * v2.0 - Added Funding Sources, Income Sources, Collaboration Source
 * v3.0 - Major report restructure; product DR/PF routing; code 3 support
 * v3.1 - Removed Report Totals; batch write; Grant Summary improvements
 * v3.2 - FIELD_DISPLAY_MAP and getFieldDisplayLabel()
 * v3.3 - Year-based archive iteration; trimHeaders()
 * v3.4 - FieldMapService migration; COL_* constants retained
 * v3.5 - SharedUtils extraction; CONFIG.TIMEZONE
 * v3.6 - resolveAMField_() for all AM column lookups
 * v3.7 - MySQL offload for Request Frequency and Grant Summary
 * v3.8 - Baby product detection (BoxCode 2nd char = B);
 *         calculateDetailedAgeBrackets() gains totalMales/totalFemales;
 *         Households: Funding Code + Baby Products columns added
 * v3.9 - LU_FieldMap Report Header alignment; Children 2 and Under fixed
 *         (Take Baby Box? X→1, not a.under5); Products Requested added to
 *         Distribution Stats; referral column headers corrected
 * v4.0 - LU_ReportColumns integration replaces all hardcoded dataHeaders arrays.
 *         NEW: _getGrantResolvers_()  — maps every column key to a value fn(rec)
 *         NEW: _buildGrantRows_()     — generic LU-driven row builder
 *         NEW: _writeGrantReport_()   — shared spreadsheet writer (4 reports share it)
 *         NEW: _getColsWithFallback_()— returns LU cols or v3.9 hardcoded fallback
 *         All 5 report functions: data collection unchanged; header/row output LU-driven
 *         generateGrantSummaryReport() unchanged (narrative Doc — no column structure)
 */

'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// AGE BRACKET DEFINITIONS
// These match the exact raw header names in Applicants_Master.
// maxAge is used to classify brackets into children / adults / seniors.
// ─────────────────────────────────────────────────────────────────────────────
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

// ─────────────────────────────────────────────────────────────────────────────
// DEPRECATED COL_* CONSTANTS
// These long raw AM headers are referenced by name in this file.
// Kept as named constants so they appear only once. Do not remove —
// they are still used by resolveAMField_() lookups throughout.
// ─────────────────────────────────────────────────────────────────────────────
var COL_INCOME = 'Total annual household income. (How much money does your family make in 1 year?)';
var COL_USED_BEFORE = 'Have you used our services before?';
var COL_ASSISTANCE = 'Are you receiving any assistance? Please select ALL boxes that apply to ANY ASSISTANCE your family is receiving.';
var COL_BABY_BOX = 'Take Baby Box?';
var COL_EMERGENCY = 'Was there an emergency situation which caused you to contact us?';
var COL_SITUATION = 'Briefly explain your current situation.';
var COL_HELP_1 = 'How will receiving personal and home cleaning products help you?';
var COL_HELP_2 = 'If you have received cleaning products from us in the past, how has receiving these products helped you?';
var COL_PERMISSION = 'May we use the information you have provided in the 2 questions above about HOW WILL and HOW DID receiving cleaning products help? ONLY the information from these 2 areas will be shared to help us show potential donors your need is real';
var COL_HOW_LEARNED = 'How did you learn about our program?';
var COL_PERSON_ORG = 'More information about the person or organization that referred you.';
var COL_TITLE_POS = 'What is the title or position of the person who referred you?';
var COL_WHO_REFERRED = "What is the person's name who referred you?";
var COL_MORE_INFO = 'Please tell us anything else you would like us to know about how you heard about Giving to the Nations or our program.';


// ─────────────────────────────────────────────────────────────────────────────
// FIELD DISPLAY LABEL (delegates to FieldMapService — unchanged from v3.4)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Get the friendly display label for a raw AM column header.
 * Delegates to FieldMapService which reads from LU_FieldMap.
 * @param {string} fieldName - Raw AM column header
 * @returns {string} Friendly label, or fieldName if not mapped
 */
function getFieldDisplayLabel(fieldName) {
    return getFieldDisplayLabelFromMap(fieldName);
}


// ─────────────────────────────────────────────────────────────────────────────
// AGE BRACKET CALCULATIONS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Calculate simple age bracket sums for a single data row.
 * Used by Grant Summary report (which doesn't need the full breakdown).
 *
 * @param {string[]} headers     - Trimmed column headers from the data source
 * @param {Array}    row         - Single data row
 * @param {number}   adultMaxAge - Max age counted as adult (e.g. 64)
 * @returns {{ people, children, adults, seniors }}
 */
function calculateAgeBrackets(headers, row, adultMaxAge) {
    var people = 0, children = 0, adults = 0, seniors = 0;
    for (var b = 0; b < AGE_BRACKETS.length; b++) {
        var bracket = AGE_BRACKETS[b];
        var maleIdx = headers.indexOf(bracket.male);
        var femaleIdx = headers.indexOf(bracket.female);
        var maleVal = maleIdx !== -1 ? (parseInt(row[maleIdx]) || 0) : 0;
        var femaleVal = femaleIdx !== -1 ? (parseInt(row[femaleIdx]) || 0) : 0;
        var total = maleVal + femaleVal;
        people += total;
        if (bracket.maxAge <= 17) children += total;
        else if (bracket.maxAge <= adultMaxAge) adults += total;
        else seniors += total;
    }
    return { people: people, children: children, adults: adults, seniors: seniors };
}

/**
 * Calculate the full detailed age breakdown for a single data row.
 * Used by Households, Distribution Stats, and Open Requests reports.
 *
 * Returns individual bracket totals plus rolled-up totals by category
 * (children/adults/seniors) and by gender (totalMales/totalFemales).
 *
 * Classification:
 *   Children  — maxAge ≤ 17   (under 5, 5-9, 10-14, 15-17)
 *   Adults    — maxAge 18-64  (18-24, 25-34, 35-44, 45-54, 55-64)
 *   Seniors   — maxAge ≥ 65   (65-84, 85+)
 *
 * v3.8: Added totalMales and totalFemales (sum across all brackets).
 *
 * @param {string[]} headers - Trimmed column headers
 * @param {Array}    row     - Single data row
 * @returns {Object} Full breakdown — see result object for all fields
 */
function calculateDetailedAgeBrackets(headers, row) {
    var r = {
        people: 0,
        // Children totals + individual brackets
        totalChildren: 0, maleChildren: 0, femaleChildren: 0,
        under5: 0, aged5t9: 0, aged10t14: 0, aged15t17: 0,
        // Adult totals + individual brackets
        totalAdults: 0, maleAdults: 0, femaleAdults: 0,
        aged18t24: 0, aged25t34: 0, aged35t44: 0, aged45t54: 0, aged55t64: 0,
        // Senior totals + individual brackets
        totalSeniors: 0, maleSeniors: 0, femaleSeniors: 0,
        aged65t84: 0, aged85up: 0,
        // Cross-category gender totals (v3.8)
        totalMales: 0, totalFemales: 0
    };

    for (var b = 0; b < AGE_BRACKETS.length; b++) {
        var br = AGE_BRACKETS[b];
        var mi = headers.indexOf(br.male);
        var fi = headers.indexOf(br.female);
        var mv = mi !== -1 ? (parseInt(row[mi]) || 0) : 0;
        var fv = fi !== -1 ? (parseInt(row[fi]) || 0) : 0;
        var tot = mv + fv;

        r.people += tot;
        r.totalMales += mv;
        r.totalFemales += fv;

        if (br.maxAge <= 17) {
            // Children bracket
            r.totalChildren += tot; r.maleChildren += mv; r.femaleChildren += fv;
            if (br.maxAge <= 4) r.under5 += tot;
            else if (br.maxAge <= 9) r.aged5t9 += tot;
            else if (br.maxAge <= 14) r.aged10t14 += tot;
            else r.aged15t17 += tot;

        } else if (br.maxAge <= 64) {
            // Adult bracket
            r.totalAdults += tot; r.maleAdults += mv; r.femaleAdults += fv;
            if (br.maxAge <= 24) r.aged18t24 += tot;
            else if (br.maxAge <= 34) r.aged25t34 += tot;
            else if (br.maxAge <= 44) r.aged35t44 += tot;
            else if (br.maxAge <= 54) r.aged45t54 += tot;
            else r.aged55t64 += tot;

        } else {
            // Senior bracket
            r.totalSeniors += tot; r.maleSeniors += mv; r.femaleSeniors += fv;
            if (br.maxAge <= 84) r.aged65t84 += tot;
            else r.aged85up += tot;
        }
    }
    return r;
}


// ─────────────────────────────────────────────────────────────────────────────
// PRODUCT CALCULATIONS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Load product lookup dictionaries from G2N_Data and all year-based archives.
 *
 * Returns two lookup objects used by calculateProductCounts():
 *   distProdByBox  — { BoxCode: [qty, ...] }         from Distributed_Products
 *   drPfByIdDate   — { 'id|date': {qtyRequested, qtyReceived} }  from DR/PF_Products
 *                    (+ Products_Archive sheets in all archive workbooks)
 *
 * MySQL path: delegates to DbService.getProductLookupData() which queries
 * the bridge endpoint — no sheet reads needed.
 *
 * Sheets path: reads Distributed_Products once, then DR/PF_Products, then
 * iterates all getArchiveWorkbooksForRange() workbooks for Products_Archive.
 * v3.3: Added year-based archive iteration for complete coverage.
 *
 * @param {Date} fromDate - Report start date (used to scope archive search)
 * @param {Date} toDate   - Report end date
 * @returns {{ distProdByBox: Object, drPfByIdDate: Object }}
 */
function loadProductLookupData(fromDate, toDate) {
    // ── MySQL path ────────────────────────────────────────────────────────────
    if (CONFIG.DB && CONFIG.DB.USE_MYSQL) {
        try {
            var fs = (fromDate instanceof Date) ? Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd') : null;
            var ts = (toDate instanceof Date) ? Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'yyyy-MM-dd') : null;
            return DbService.getProductLookupData(fs, ts);
        } catch (e) {
            Logger.log('loadProductLookupData MySQL error — falling back to Sheets: ' + e.message);
        }
    }

    // ── Sheets path ───────────────────────────────────────────────────────────
    var result = { distProdByBox: {}, drPfByIdDate: {} };
    try {
        var dataWB = getDataWorkbook();

        // Distributed_Products — static reference/lookup table (no archive)
        var dpSheet = dataWB.getSheetByName('Distributed_Products');
        if (dpSheet && dpSheet.getLastRow() > 1) {
            var dpData = dpSheet.getDataRange().getValues();
            var dpH = trimHeaders(dpData[0]);
            var boxCol = dpH.indexOf('BoxCode');
            var qtyCol = dpH.indexOf('Quantity');
            if (boxCol !== -1 && qtyCol !== -1) {
                for (var i = 1; i < dpData.length; i++) {
                    var bc = (dpData[i][boxCol] || '').toString().trim();
                    if (!bc) continue;
                    var qty = parseInt(dpData[i][qtyCol]) || 0;
                    if (!result.distProdByBox[bc]) result.distProdByBox[bc] = [];
                    result.distProdByBox[bc].push(qty);
                }
            }
        }

        // DR/PF_Products — active records only
        var pfSheet = dataWB.getSheetByName('DR/PF_Products');
        if (pfSheet && pfSheet.getLastRow() > 1) {
            var pfData = pfSheet.getDataRange().getValues();
            var pfH = trimHeaders(pfData[0]);
            var idC = pfH.indexOf('ID');
            var rdC = pfH.indexOf('Request Date');
            if (rdC === -1) rdC = pfH.indexOf('RequestDate');
            var qrC = pfH.indexOf('QtyRequested');
            var qvC = pfH.indexOf('QtyReceived');
            if (idC !== -1 && rdC !== -1) {
                for (var i = 1; i < pfData.length; i++) {
                    var id = (pfData[i][idC] || '').toString().trim();
                    if (!id) continue;
                    var dv = pfData[i][rdC];
                    var dk = '';
                    if (dv instanceof Date && !isNaN(dv.getTime()))
                        dk = Utilities.formatDate(dv, CONFIG.TIMEZONE, 'M/d/yyyy');
                    else if (dv) {
                        var pd = new Date(dv);
                        if (!isNaN(pd.getTime())) dk = Utilities.formatDate(pd, CONFIG.TIMEZONE, 'M/d/yyyy');
                    }
                    var ck = id + '|' + dk;
                    if (!result.drPfByIdDate[ck]) result.drPfByIdDate[ck] = { qtyRequested: 0, qtyReceived: 0 };
                    result.drPfByIdDate[ck].qtyRequested += qrC !== -1 ? (parseInt(pfData[i][qrC]) || 0) : 0;
                    result.drPfByIdDate[ck].qtyReceived += qvC !== -1 ? (parseInt(pfData[i][qvC]) || 0) : 0;
                }
            }
        }

        // Products_Archive — iterate all year-based archive workbooks (v3.3)
        try {
            var archiveWBs = getArchiveWorkbooksForRange(fromDate, toDate);
            for (var aw = 0; aw < archiveWBs.length; aw++) {
                try {
                    var paSheet = archiveWBs[aw].workbook.getSheetByName('Products_Archive');
                    if (!paSheet || paSheet.getLastRow() < 2) continue;
                    var paData = paSheet.getDataRange().getValues();
                    var paH = trimHeaders(paData[0]);
                    var paId = paH.indexOf('ID');
                    var paRd = paH.indexOf('RequestDate');
                    var paQr = paH.indexOf('QtyRequested');
                    var paQv = paH.indexOf('QtyReceived');
                    if (paId === -1 || paRd === -1) continue;
                    for (var j = 1; j < paData.length; j++) {
                        var pai = (paData[j][paId] || '').toString().trim();
                        if (!pai) continue;
                        var paDv = paData[j][paRd];
                        var paDk = '';
                        if (paDv instanceof Date && !isNaN(paDv.getTime()))
                            paDk = Utilities.formatDate(paDv, CONFIG.TIMEZONE, 'M/d/yyyy');
                        else if (paDv) {
                            var paPd = new Date(paDv);
                            if (!isNaN(paPd.getTime())) paDk = Utilities.formatDate(paPd, CONFIG.TIMEZONE, 'M/d/yyyy');
                        }
                        var paCk = pai + '|' + paDk;
                        if (!result.drPfByIdDate[paCk]) result.drPfByIdDate[paCk] = { qtyRequested: 0, qtyReceived: 0 };
                        result.drPfByIdDate[paCk].qtyRequested += paQr !== -1 ? (parseInt(paData[j][paQr]) || 0) : 0;
                        result.drPfByIdDate[paCk].qtyReceived += paQv !== -1 ? (parseInt(paData[j][paQv]) || 0) : 0;
                    }
                } catch (wbErr) {
                    Logger.log('Products_Archive read error (' + archiveWBs[aw].name + '): ' + wbErr.message);
                }
            }
        } catch (archErr) {
            Logger.log('Products_Archive iteration error (non-fatal): ' + archErr.message);
        }

    } catch (e) {
        Logger.log('loadProductLookupData error: ' + e.message);
    }
    return result;
}

/**
 * Calculate Products Requested, Products Distributed, Baby Products Requested,
 * and Baby Products Distributed for a single applicant record.
 *
 * ROUTING LOGIC (applied to each of the 3 received product code slots):
 *
 *   Code = 'DR' or 'PF'
 *     → Look up drPfByIdDate['id|date'] — QtyRequested / QtyReceived
 *     → Counted only ONCE per record, even if multiple slots are DR/PF
 *
 *   Code 2nd character = 'B' (e.g. 'TB001', 'XB002')
 *     → Look up distProdByBox[code] — sum of Quantity
 *     → Counted as BABY products (babyRequested / babyDistributed)
 *
 *   All other non-empty codes
 *     → Look up distProdByBox[code] — sum of Quantity
 *     → Counted as REGULAR products (productsRequested / productsDistributed)
 *
 * Products are only counted when Service Status = 'Picked Up' or 'Delivered'.
 * Callers are responsible for pre-filtering rows.
 *
 * MYSQL PATH: If sp_get_report_data appends calc_qty_requested / calc_qty_received
 * (and optionally calc_baby_requested / calc_baby_received) as extra columns,
 * this function uses those pre-computed values and skips dictionary lookups.
 * Note: calc_baby_* columns require a follow-on sp_get_report_data update.
 *
 * v3.0: DR/PF routing; codes 1-3
 * v3.1: Fixed DR/PF double-count
 * v3.8: Baby detection (2nd char = B); babyRequested / babyDistributed added
 *
 * @param {string} id           - Applicant record ID
 * @param {string} productCode1 - Received Product Code 1
 * @param {string} productCode2 - Received Product Code 2
 * @param {string} productCode3 - Received Product Code 3
 * @param {Date}   requestDate  - For DR/PF key matching
 * @param {Object} productData  - From loadProductLookupData()
 * @param {Array}  [headers]    - Optional: enables MySQL pre-computed path
 * @param {Array}  [row]        - Optional: enables MySQL pre-computed path
 * @returns {{ productsRequested, productsDistributed, babyRequested, babyDistributed }}
 */
function calculateProductCounts(id, productCode1, productCode2, productCode3,
    requestDate, productData, headers, row) {
    // ── MySQL pre-computed path ───────────────────────────────────────────────
    if (headers && row) {
        var ri = headers.indexOf('calc_qty_requested');
        var rci = headers.indexOf('calc_qty_received');
        var bri = headers.indexOf('calc_baby_requested');   // requires SP update
        var bdi = headers.indexOf('calc_baby_received');    // requires SP update
        if (ri !== -1 && rci !== -1) {
            return {
                productsRequested: parseInt(row[ri]) || 0,
                productsDistributed: parseInt(row[rci]) || 0,
                babyRequested: bri !== -1 ? (parseInt(row[bri]) || 0) : 0,
                babyDistributed: bdi !== -1 ? (parseInt(row[bdi]) || 0) : 0
            };
        }
    }

    // ── Sheets dictionary lookup path ─────────────────────────────────────────
    var req = 0, dist = 0, babyReq = 0, babyDist = 0;
    var codes = [
        (productCode1 || '').toString().trim(),
        (productCode2 || '').toString().trim(),
        (productCode3 || '').toString().trim()
    ];
    var recId = (id || '').toString().trim();
    var dateKey = '';
    if (requestDate instanceof Date && !isNaN(requestDate.getTime())) {
        dateKey = Utilities.formatDate(requestDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    }
    var compositeKey = recId + '|' + dateKey;
    var drPfCounted = false;

    for (var c = 0; c < codes.length; c++) {
        var code = codes[c];
        if (!code) continue;
        var cu = code.toUpperCase();

        if (cu === 'DR' || cu === 'PF') {
            // DR/PF — look up by ID + date; only once per record
            if (!drPfCounted) {
                var dp = productData.drPfByIdDate[compositeKey];
                if (dp) { req += dp.qtyRequested; dist += dp.qtyReceived; }
                drPfCounted = true;
            }

        } else if (code.length >= 2 && code.charAt(1).toUpperCase() === 'B') {
            // Baby product — 2nd character of BoxCode is 'B'
            var bm = productData.distProdByBox[code];
            if (bm && bm.length > 0) {
                var bs = 0;
                for (var bi = 0; bi < bm.length; bi++) bs += bm[bi];
                babyReq += bs; babyDist += bs;
            }

        } else {
            // Regular BoxCode
            var dm = productData.distProdByBox[code];
            if (dm && dm.length > 0) {
                var ds = 0;
                for (var di = 0; di < dm.length; di++) ds += dm[di];
                req += ds; dist += ds;
            }
        }
    }

    return {
        productsRequested: req,
        productsDistributed: dist,
        babyRequested: babyReq,
        babyDistributed: babyDist
    };
}


// ─────────────────────────────────────────────────────────────────────────────
// v4.0 — COLUMN RESOLVER MAP
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Returns the exhaustive map of column key → value resolver function.
 *
 * Every key that appears in LU_ReportColumns for any grant report must have
 * an entry here. The resolver receives the per-row record object (rec) built
 * inside each report function and returns the cell value for that column.
 *
 * If a column key from LU_ReportColumns is NOT in this map, the column will
 * silently output '' — check Logger output for "unknown key" warnings.
 *
 * HOW TO ADD A NEW COMPUTED COLUMN:
 *   1. Add a row to LU_ReportColumns with the [Calc] key
 *   2. Add an entry here: '[Calc] My New Field': function(r) { return r.myField; }
 *   3. Add the field to the record object in whichever report function(s) need it
 *   4. Add the field to LU_FieldMap (Computed group) for documentation
 *
 * @returns {Object.<string, function(Object): *>}
 */
function _getGrantResolvers_() {
    return {
        // ── Date prefix (computed from Request Date) ──────────────────────────
        'Quarter': function (r) { return 'Q' + r.quarter; },
        'Year': function (r) { return r.year; },
        'Month': function (r) { return r.month; },
        'Day': function (r) { return r.day; },

        // ── Identity ─────────────────────────────────────────────────────────
        'ID': function (r) { return r.id || ''; },
        'Original Form ID': function (r) { return r.originalFormId || ''; },
        'First Name': function (r) { return r.firstName || ''; },
        'Last Name': function (r) { return r.lastName || ''; },

        // ── Geography ────────────────────────────────────────────────────────
        'Street Address': function (r) { return r.address1 || ''; },
        'City': function (r) { return r.city || ''; },
        'State': function (r) { return r.state || ''; },
        'Zip Code': function (r) { return r.zip || ''; },
        'County': function (r) { return r.county || ''; },

        // ── Service ──────────────────────────────────────────────────────────
        'Request Type': function (r) { return r.requestType || ''; },
        'Service Status': function (r) { return r.serviceStatus || ''; },
        'Funding Code': function (r) { return r.fundingCode || ''; },
        'Final Service Contact Date': function (r) { return r.finalServiceDate || ''; },
        'Received Product Code 1': function (r) { return r.code1 || ''; },
        'Received Product Code 2': function (r) { return r.code2 || ''; },
        'Received Product Code 3': function (r) { return r.code3 || ''; },

        // ── Demographics ─────────────────────────────────────────────────────
        'Military Status': function (r) { return r.militaryStatus || ''; },
        'Please Select Your Racial Category': function (r) { return r.race || ''; },
        'Please Select Your Ethnic Category': function (r) { return r.ethnicity || ''; },
        'Are you currently homeless?': function (r) { return r.homeless || ''; },
        'Are you currently employed?': function (r) { return r.employed || ''; },
        'Total annual household income. (How much money does your family make in 1 year?)':
            function (r) { return r.incomeLevel || ''; },
        'Are you receiving any assistance? Please select ALL boxes that apply to ANY ASSISTANCE your family is receiving.':
            function (r) { return r.incomeSourceType || ''; },

        // ── Referral ─────────────────────────────────────────────────────────
        'How did you learn about our program?': function (r) { return r.howLearned || ''; },
        'More information about the person or organization that referred you.':
            function (r) { return r.personOrg || ''; },
        'What is the title or position of the person who referred you?':
            function (r) { return r.titlePos || ''; },
        "What is the person's name who referred you?":
            function (r) { return r.whoReferred || ''; },
        'Please tell us anything else you would like us to know about how you heard about Giving to the Nations or our program.':
            function (r) { return r.moreInfo || ''; },

        // ── Testimonial ──────────────────────────────────────────────────────
        'Was there an emergency situation which caused you to contact us?':
            function (r) { return r.emergency || ''; },
        'Briefly explain your current situation.':
            function (r) { return r.situation || ''; },
        'How will receiving personal and home cleaning products help you?':
            function (r) { return r.help1 || ''; },
        'If you have received cleaning products from us in the past, how has receiving these products helped you?':
            function (r) { return r.help2 || ''; },
        'May we use the information you have provided in the 2 questions above about HOW WILL and HOW DID receiving cleaning products help? ONLY the information from these 2 areas will be shared to help us show potential donors your need is real':
            function (r) { return r.permission || ''; },

        // ── Computed: applicant type ──────────────────────────────────────────
        '[Calc] Applicant Type': function (r) { return r.applicantType || ''; },

        // ── Summary counts ────────────────────────────────────────────────────
        // Number of Households and Number of Requests are always 1 per row in
        // row-level reports (Households, Dist Stats, Open Requests).
        '[Summary] Number of Households': function (r) { return 1; },
        '[Summary] Number of Requests': function (r) { return 1; },
        // Request Frequency uses these non-[Summary] keys for its grouped totals
        'Number of Requests': function (r) { return r.requestCount || 0; },
        'Avg Frequency Days': function (r) { return r.avgFreqDays !== undefined ? r.avgFreqDays : ''; },

        // ── Products ──────────────────────────────────────────────────────────
        '[Calc] Products Requested': function (r) { return r.productsRequested || 0; },
        '[Calc] Products Distributed': function (r) { return r.productsDistributed || 0; },
        '[Calc] Baby Products Requested': function (r) { return r.babyRequested || 0; },
        '[Calc] Baby Products Received': function (r) { return r.babyDistributed || 0; },

        // ── Age bracket totals (from rec.ages returned by calculateDetailedAgeBrackets) ──
        '[Calc] People in Household': function (r) { return r.ages ? r.ages.people : 0; },
        '[Calc] Children (<18)': function (r) { return r.ages ? r.ages.totalChildren : 0; },
        '[Calc] Male Children': function (r) { return r.ages ? r.ages.maleChildren : 0; },
        '[Calc] Female Children': function (r) { return r.ages ? r.ages.femaleChildren : 0; },
        // Children 2 and Under = Take Baby Box? X→1 (NOT the under-5 age bracket)
        '[Calc] Children 2 and Under': function (r) { return r.children2Under !== undefined ? r.children2Under : 0; },
        '[Calc] Under 5': function (r) { return r.ages ? r.ages.under5 : 0; },
        '[Calc] 5-9': function (r) { return r.ages ? r.ages.aged5t9 : 0; },
        '[Calc] 10-14': function (r) { return r.ages ? r.ages.aged10t14 : 0; },
        '[Calc] 15-17': function (r) { return r.ages ? r.ages.aged15t17 : 0; },
        '[Calc] Adults (18-64)': function (r) { return r.ages ? r.ages.totalAdults : 0; },
        '[Calc] Male Adults': function (r) { return r.ages ? r.ages.maleAdults : 0; },
        '[Calc] Female Adults': function (r) { return r.ages ? r.ages.femaleAdults : 0; },
        '[Calc] 18-24': function (r) { return r.ages ? r.ages.aged18t24 : 0; },
        '[Calc] 25-34': function (r) { return r.ages ? r.ages.aged25t34 : 0; },
        '[Calc] 35-44': function (r) { return r.ages ? r.ages.aged35t44 : 0; },
        '[Calc] 45-54': function (r) { return r.ages ? r.ages.aged45t54 : 0; },
        '[Calc] 55-64': function (r) { return r.ages ? r.ages.aged55t64 : 0; },
        '[Calc] Seniors (65+)': function (r) { return r.ages ? r.ages.totalSeniors : 0; },
        '[Calc] Male Seniors': function (r) { return r.ages ? r.ages.maleSeniors : 0; },
        '[Calc] Female Seniors': function (r) { return r.ages ? r.ages.femaleSeniors : 0; },
        '[Calc] 65-84': function (r) { return r.ages ? r.ages.aged65t84 : 0; },
        '[Calc] 85 and Over': function (r) { return r.ages ? r.ages.aged85up : 0; },
        '[Calc] Total Males': function (r) { return r.ages ? r.ages.totalMales : 0; },
        '[Calc] Total Females': function (r) { return r.ages ? r.ages.totalFemales : 0; },
        '[Calc] Household Size': function (r) { return r.ages ? r.ages.people : 0; }
    };
}

/**
 * Build the 2D array of output rows from a records array and column definitions.
 *
 * This is the single row-building function for all grant reports.
 * It replaces the per-report allRows.push([...]) blocks.
 *
 * For any column key not in _getGrantResolvers_(), the cell value is ''.
 * Check Logger for warning messages if columns are unexpectedly blank.
 *
 * @param {Object[]} records - Per-row record objects built inside each report function
 * @param {Object[]} cols    - From getReportColumns() or _getFallbackCols_()
 * @returns {Array[]} 2D array ready for sheet.getRange(...).setValues()
 */
function _buildGrantRows_(records, cols) {
    var resolvers = _getGrantResolvers_();
    return records.map(function (rec) {
        return cols.map(function (col) {
            var fn = resolvers[col.key];
            if (!fn) {
                Logger.log('_buildGrantRows_: no resolver for key "' + col.key + '"');
                return '';
            }
            return fn(rec);
        });
    });
}

/**
 * v3.9 hardcoded column fallback — used only when LU_ReportColumns is empty.
 * These are the exact column sets from v3.9, reshaped to match the
 * {key, label, width, wrapText} format expected by _buildGrantRows_().
 * @param {string} reportName
 * @returns {Object[]}
 */
function _getColsWithFallback_(reportName) {
    var cols = getReportColumns(reportName);
    if (cols.length === 0) {
        Logger.log('_getColsWithFallback_: LU_ReportColumns empty for "' + reportName + '" — using hardcoded v3.9 fallback');
        cols = _getFallbackCols_(reportName);
    }
    return cols;
}

/**
 * v3.9 hardcoded column fallback — used only when LU_ReportColumns is empty.
 * These are the exact column sets from v3.9, reshaped to match the
 * {key, label, width, wrapText} format expected by _buildGrantRows_().
 * @param {string} reportName
 * @returns {Object[]}
 */
function _getFallbackCols_(reportName) {
    // v4.0: Complete fallback column sets for all 5 grant reports.
    // Used only when LU_ReportColumns sheet is empty or not yet populated.
    // Column keys match _getGrantResolvers_() exactly.
    // Column labels are resolved at runtime via getReportHeader_() → LU_FieldMap.

    // ── Shared age block (22 columns) — appended to Households and Distribution Stats ──
    var AGE_BLOCK = [
        { key: '[Calc] People in Household', width: 70 },
        { key: '[Calc] Children (<18)', width: 70 },
        { key: '[Calc] Male Children', width: 70 },
        { key: '[Calc] Female Children', width: 70 },
        { key: '[Calc] Children 2 and Under', width: 70 },
        { key: '[Calc] Under 5', width: 70 }, { key: '[Calc] 5-9', width: 70 },
        { key: '[Calc] 10-14', width: 70 }, { key: '[Calc] 15-17', width: 70 },
        { key: '[Calc] Adults (18-64)', width: 70 },
        { key: '[Calc] Male Adults', width: 70 },
        { key: '[Calc] Female Adults', width: 70 },
        { key: '[Calc] 18-24', width: 70 }, { key: '[Calc] 25-34', width: 70 },
        { key: '[Calc] 35-44', width: 70 }, { key: '[Calc] 45-54', width: 70 },
        { key: '[Calc] 55-64', width: 70 },
        { key: '[Calc] Seniors (65+)', width: 70 },
        { key: '[Calc] Male Seniors', width: 70 },
        { key: '[Calc] Female Seniors', width: 70 },
        { key: '[Calc] 65-84', width: 70 },
        { key: '[Calc] 85 and Over', width: 70 }
    ];

    // ── Shared date prefix (4 columns) ───────────────────────────────────────
    var DATE_PFX = [
        { key: 'Quarter', width: 60 }, { key: 'Year', width: 50 },
        { key: 'Month', width: 60 }, { key: 'Day', width: 40 }
    ];

    var fallbacks = {

        // ── Households (47 columns — v3.9 exact) ────────────────────────────────
        'Households': DATE_PFX.concat([
            { key: 'City', width: 120 }, { key: 'State', width: 50 }, { key: 'Zip Code', width: 65 }, { key: 'County', width: 100 },
            { key: '[Calc] Applicant Type', width: 80 },
            { key: 'Request Type', width: 90 }, { key: 'Service Status', width: 90 }, { key: 'Funding Code', width: 90 },
            { key: 'Military Status', width: 100 },
            { key: 'Please Select Your Racial Category', width: 100 },
            { key: 'Please Select Your Ethnic Category', width: 80 },
            { key: 'Are you currently homeless?', width: 120 },
            { key: 'Are you currently employed?', width: 115 },
            { key: 'Total annual household income. (How much money does your family make in 1 year?)', width: 120 },
            { key: 'Are you receiving any assistance? Please select ALL boxes that apply to ANY ASSISTANCE your family is receiving.', width: 130, wrapText: true },
            { key: '[Summary] Number of Households', width: 90 },
            { key: '[Summary] Number of Requests', width: 90 },
            { key: '[Calc] Products Requested', width: 90 },
            { key: '[Calc] Products Distributed', width: 90 },
            { key: '[Calc] Baby Products Requested', width: 130 },
            { key: '[Calc] Baby Products Received', width: 130 }
        ]).concat(AGE_BLOCK),

        // ── Distribution Stats (58 columns — v3.9 exact) ────────────────────────
        'Distribution Stats': DATE_PFX.concat([
            { key: 'ID', width: 50 }, { key: 'First Name', width: 100 }, { key: 'Last Name', width: 100 },
            { key: 'City', width: 120 }, { key: 'State', width: 50 }, { key: 'Zip Code', width: 65 }, { key: 'County', width: 100 },
            { key: 'Funding Code', width: 90 }, { key: 'Request Type', width: 90 }, { key: 'Service Status', width: 90 },
            { key: 'Final Service Contact Date', width: 110 },
            { key: 'Received Product Code 1', width: 80 },
            { key: 'Received Product Code 2', width: 80 },
            { key: 'Received Product Code 3', width: 80 },
            { key: '[Calc] Products Requested', width: 90 },
            { key: '[Calc] Products Distributed', width: 90 },
            { key: '[Calc] Baby Products Requested', width: 130 },
            { key: '[Calc] Baby Products Received', width: 130 },
            { key: 'How did you learn about our program?', width: 120 },
            { key: 'More information about the person or organization that referred you.', width: 130 },
            { key: 'What is the title or position of the person who referred you?', width: 120 },
            { key: "What is the person's name who referred you?", width: 120 },
            { key: 'Please tell us anything else you would like us to know about how you heard about Giving to the Nations or our program.', width: 140 },
            { key: 'Military Status', width: 100 },
            { key: 'Please Select Your Racial Category', width: 100 },
            { key: 'Please Select Your Ethnic Category', width: 80 },
            { key: 'Are you currently homeless?', width: 120 },
            { key: 'Are you currently employed?', width: 115 },
            { key: 'Total annual household income. (How much money does your family make in 1 year?)', width: 120 },
            { key: 'Are you receiving any assistance? Please select ALL boxes that apply to ANY ASSISTANCE your family is receiving.', width: 130, wrapText: true },
            { key: '[Summary] Number of Households', width: 90 },
            { key: '[Summary] Number of Requests', width: 90 }
        ]).concat(AGE_BLOCK),

        // ── Open Requests (20 columns — v3.9 exact) ─────────────────────────────
        'Open Requests': DATE_PFX.concat([
            { key: 'Street Address', width: 160 },
            { key: 'City', width: 120 }, { key: 'State', width: 50 }, { key: 'Zip Code', width: 65 }, { key: 'County', width: 100 },
            { key: 'Total annual household income. (How much money does your family make in 1 year?)', width: 120 },
            { key: '[Calc] Applicant Type', width: 80 },
            { key: 'Request Type', width: 90 }, { key: 'Service Status', width: 90 },
            { key: 'First Name', width: 100 }, { key: 'Last Name', width: 100 },
            { key: '[Calc] Children 2 and Under', width: 70 },
            { key: '[Summary] Number of Households', width: 90 },
            { key: '[Summary] Number of Requests', width: 90 },
            { key: 'ID', width: 50 }, { key: 'Original Form ID', width: 110 }
        ]),

        // ── Request Frequency (13 columns — v3.9 exact) ──────────────────────────
        'Request Frequency': DATE_PFX.concat([
            { key: 'City', width: 120 }, { key: 'State', width: 50 }, { key: 'Zip Code', width: 65 }, { key: 'County', width: 100 },
            { key: 'Total annual household income. (How much money does your family make in 1 year?)', width: 120 },
            { key: 'Request Type', width: 90 }, { key: 'Service Status', width: 90 },
            { key: 'Number of Requests', width: 90 },
            { key: 'Avg Frequency Days', width: 90 }
        ]),

        // ── Testimonials (17 columns — v3.9 exact) ───────────────────────────────
        'Testimonials': DATE_PFX.concat([
            { key: 'City', width: 120 }, { key: 'State', width: 50 }, { key: 'Zip Code', width: 65 }, { key: 'County', width: 100 },
            { key: 'Total annual household income. (How much money does your family make in 1 year?)', width: 130 },
            { key: '[Calc] Applicant Type', width: 80 },
            { key: 'Request Type', width: 90 }, { key: 'Service Status', width: 90 },
            { key: 'Was there an emergency situation which caused you to contact us?', width: 90 },
            { key: 'Briefly explain your current situation.', width: 250, wrapText: true },
            { key: 'How will receiving personal and home cleaning products help you?', width: 300, wrapText: true },
            { key: 'If you have received cleaning products from us in the past, how has receiving these products helped you?', width: 300, wrapText: true },
            { key: 'May we use the information you have provided in the 2 questions above about HOW WILL and HOW DID receiving cleaning products help? ONLY the information from these 2 areas will be shared to help us show potential donors your need is real', width: 120 }
        ])

    };

    var defs = fallbacks[reportName] || [];
    if (defs.length === 0) {
        Logger.log('_getFallbackCols_: no fallback defined for report "' + reportName + '"');
    }
    return defs.map(function (d) {
        return {
            key: d.key,
            label: d.label || getReportHeader_(d.key) || d.key,
            width: d.width || 70,
            wrapText: d.wrapText || false
        };
    });
}


// ─────────────────────────────────────────────────────────────────────────────
// SHARED DATA HELPERS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Get combined data from Applicants_Master and all year-based archive workbooks,
 * filtered by Request Date range and optional column filters.
 *
 * MySQL path: delegates to DbService.getReportData() — returns same shape.
 * Sheets path: reads AM, then iterates getArchiveWorkbooksForRange() for archives.
 *
 * @param {Date}     fromDate
 * @param {Date}     toDate
 * @param {Object[]} [filters] - e.g. [{column:'Service Status', values:['Picked Up']}]
 * @returns {{ headers, rows, masterCount, archiveCount, totalCount }}
 */
function getCombinedData(fromDate, toDate, filters) {
    if (CONFIG.DB && CONFIG.DB.USE_MYSQL) {
        try {
            var fs = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
            var ts = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
            var statusCsv = null, statusIncludeNull = false;
            if (filters) {
                for (var f = 0; f < filters.length; f++) {
                    if (filters[f].column === 'Service Status') {
                        if (filters[f].values) statusCsv = filters[f].values.join(',');
                        if (filters[f].emptyOrValues) { statusCsv = filters[f].emptyOrValues.join(','); statusIncludeNull = true; }
                    }
                }
            }
            return DbService.getReportData(fs, ts, true, statusCsv, statusIncludeNull, null, null, null);
        } catch (e) { Logger.log('getCombinedData MySQL error (fallback): ' + e.message); }
    }

    var result = { headers: [], rows: [], masterCount: 0, archiveCount: 0, totalCount: 0 };
    try {
        var masterSheet = getMasterSheet();
        if (!masterSheet) return result;
        var masterData = masterSheet.getDataRange().getValues();
        var headers = trimHeaders(masterData[0]);
        result.headers = headers;
        var reqDateCol = headers.indexOf(resolveAMField_('Request Date'));
        if (reqDateCol === -1) return result;
        var activeCol = headers.indexOf('Active');
        var filterCols = _buildFilterCols_(headers, filters);

        for (var i = 1; i < masterData.length; i++) {
            var row = masterData[i];
            if (activeCol !== -1 && (row[activeCol] || '').toString() === '0') continue;
            var rd = new Date(row[reqDateCol]);
            if (isNaN(rd.getTime()) || rd < fromDate || rd > toDate) continue;
            if (!_rowMatchesFilters_(row, filterCols)) continue;
            result.rows.push(row);
            result.masterCount++;
        }

        var archiveWBs = getArchiveWorkbooksForRange(fromDate, toDate);
        for (var aw = 0; aw < archiveWBs.length; aw++) {
            try {
                var archSheet = archiveWBs[aw].workbook.getSheetByName('Applicants_Master');
                if (!archSheet || archSheet.getLastRow() < 2) continue;
                var archData = archSheet.getDataRange().getValues();
                var archHeaders = trimHeaders(archData[0]);
                var archRdCol = archHeaders.indexOf(resolveAMField_('Request Date'));
                if (archRdCol === -1) continue;
                var archFilters = _buildFilterCols_(archHeaders, filters);
                for (var j = 1; j < archData.length; j++) {
                    var aRow = archData[j];
                    var ard = new Date(aRow[archRdCol]);
                    if (isNaN(ard.getTime()) || ard < fromDate || ard > toDate) continue;
                    if (!_rowMatchesFilters_(aRow, archFilters)) continue;
                    var paddedRow = aRow.slice();
                    while (paddedRow.length < headers.length) paddedRow.push('');
                    result.rows.push(paddedRow);
                    result.archiveCount++;
                }
            } catch (awErr) { Logger.log('Archive error (' + archiveWBs[aw].name + '): ' + awErr.message); }
        }
        result.totalCount = result.masterCount + result.archiveCount;
    } catch (e) { Logger.log('getCombinedData error: ' + e.message); }
    return result;
}

// Internal helpers for getCombinedData filter logic
function _buildFilterCols_(headers, filters) {
    if (!filters) return [];
    return filters.map(function (f) {
        return { colIdx: headers.indexOf(resolveAMField_(f.column)), filter: f };
    });
}
function _rowMatchesFilters_(row, filterCols) {
    for (var f = 0; f < filterCols.length; f++) {
        var fc = filterCols[f];
        if (fc.colIdx === -1) continue;
        var val = (row[fc.colIdx] || '').toString().trim();
        if (fc.filter.values && fc.filter.values.indexOf(val) === -1) return false;
        if (fc.filter.emptyOrValues && val !== '' && fc.filter.emptyOrValues.indexOf(val) === -1) return false;
    }
    return true;
}

/** Quarter number from 0-based month index */
function getQuarter(monthIndex) { return Math.ceil((monthIndex + 1) / 3); }
/** Short month name (Jan-Dec) from 0-based month index */
function getMonthName(monthIndex) {
    return ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][monthIndex] || '';
}


// ─────────────────────────────────────────────────────────────────────────────
// INCOME LEVEL HELPERS (unchanged from v3.1)
// ─────────────────────────────────────────────────────────────────────────────

function getIncomeLevelOptions() {
    try {
        var wb = getLookupsWorkbook();
        var sh = wb.getSheetByName('LU_IncomeLevels');
        if (!sh || sh.getLastRow() < 2) return [];
        var data = sh.getDataRange().getValues();
        var opts = [];
        for (var i = 1; i < data.length; i++) {
            var v = (data[i][0] || '').toString().trim();
            if (v) opts.push(v);
        }
        return opts;
    } catch (e) { return []; }
}

function parseIncomeLowEnd(incomeLabel) {
    var m = incomeLabel.replace(/[$,]/g, '').match(/\d+/);
    return m ? parseInt(m[0]) : 0;
}


// ─────────────────────────────────────────────────────────────────────────────
// v4.0 — GENERIC GRANT REPORT WRITER
// Shared by Open Requests, Households, Distribution Stats, and Testimonials.
// Eliminates ~80 lines of duplicated spreadsheet-creation boilerplate.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Create a Grant report spreadsheet, write headers and data rows, apply
 * column formatting from LU_ReportColumns, log the audit event, and return
 * the standard result object.
 *
 * Column headers come from cols[].label (resolved via LU_FieldMap Report Headers).
 * Row data comes from _buildGrantRows_(records, cols).
 * Column widths and wrap settings come from cols[].width and cols[].wrapText.
 *
 * @param {string}   reportKey   - Matches 'Report Name' in LU_ReportColumns
 * @param {string}   reportTitle - Shown in writeReportTitleSection() and sheet tab name
 * @param {string}   filePrefix  - Spreadsheet filename prefix (e.g. 'Households')
 * @param {Object[]} records     - Per-row record objects
 * @param {Object[]} cols        - From _getColsWithFallback_()
 * @param {Object}   combined    - From getCombinedData()
 * @param {Date}     fromDate
 * @param {Date}     toDate
 * @param {string}   auditAction - e.g. 'GRANTS_REPORT'
 * @returns {{ success, message, recordCount, masterCount, archiveCount, reportUrl, downloadUrl, reportId }}
 */
function _writeGrantReport_(reportKey, reportTitle, filePrefix, records, cols,
    combined, fromDate, toDate, auditAction) {
    var fromFormatted = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var toFormatted = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var spreadsheet = SpreadsheetApp.create(filePrefix + '_' + fromFormatted + '_to_' + toFormatted);
    var sheet = spreadsheet.getActiveSheet();
    sheet.setName(reportTitle);
    moveToFolder(spreadsheet.getId(), CONFIG.GRANTS_FOLDER_ID);

    var dataHeaders = cols.map(function (c) { return c.label; });
    var reportColCount = dataHeaders.length;

    // Title section (record count, date range, source counts)
    var currentRow = writeReportTitleSection(sheet, reportTitle, fromDate, toDate,
        combined.totalCount, combined.masterCount, combined.archiveCount);

    // Header row — force plain text to prevent date-interpretation of values like '5-9'
    var headerRowNum = currentRow;
    sheet.getRange(headerRowNum, 1, 1, reportColCount).setNumberFormat('@').setValues([dataHeaders]);
    styleReportHeader(sheet, headerRowNum, reportColCount);
    currentRow++;

    // Data rows — single setValues call (batch write, no timeout risk)
    var allRows = _buildGrantRows_(records, cols);
    if (allRows.length > 0) {
        sheet.getRange(currentRow, 1, allRows.length, reportColCount).setValues(allRows);
        for (var i = 1; i < allRows.length; i += 2) {
            sheet.getRange(currentRow + i, 1, 1, reportColCount).setBackground('#f8f9fa');
        }
        currentRow += allRows.length;
    }

    // Footer
    currentRow++;
    sheet.getRange(currentRow, 1).setValue('End of Report - ' + records.length + ' records');
    sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(10).setFontColor('#666666');

    // Column widths + wrap from LU_ReportColumns
    applyReportColumnFormatting(sheet, cols, headerRowNum + 1, headerRowNum + allRows.length);
    sheet.setFrozenRows(headerRowNum);

    var fromDisplay = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    var toDisplay = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    logAudit(auditAction, null,
        'Generated ' + reportTitle + ': ' + fromDisplay + ' to ' + toDisplay +
        ' (' + records.length + ' records)');

    var reportUrl = spreadsheet.getUrl();
    var downloadUrl = 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx';
    return {
        success: true,
        message: 'Report generated with ' + records.length + ' records',
        recordCount: records.length,
        masterCount: combined.masterCount,
        archiveCount: combined.archiveCount,
        reportUrl: reportUrl,
        downloadUrl: downloadUrl,
        reportId: spreadsheet.getId()
    };
}


// ─────────────────────────────────────────────────────────────────────────────
// REPORT: APPLICANTS OPEN REQUESTS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Generate Applicants Open Requests report.
 *
 * Shows requests where Service Status is blank, null, or 'Open'.
 * Columns: driven by LU_ReportColumns 'Open Requests'.
 * Order: Quarter, Year, Month, Day, Address, City, State, Zip, County.
 *
 * v4.0: dataHeaders replaced with LU_ReportColumns lookup.
 *
 * @param {string} fromDateStr - YYYY-MM-DD
 * @param {string} toDateStr   - YYYY-MM-DD
 * @returns {Object} Standard report result
 */
function generateApplicantsOpenRequests(fromDateStr, toDateStr) {
    try {
        if (!fromDateStr || !toDateStr)
            return { success: false, error: 'From Date and To Date are required' };
        var fromDate = parseDateInput(fromDateStr, false);
        var toDate = parseDateInput(toDateStr, true);
        if (fromDate > toDate)
            return { success: false, error: 'From Date must be before To Date' };

        var combined = getCombinedData(fromDate, toDate,
            [{ column: 'Service Status', emptyOrValues: ['Open'] }]);
        if (combined.totalCount === 0)
            return { success: false, error: 'No open requests found for the specified date range' };

        var headers = combined.headers;
        var rows = combined.rows;
        var colIdx = {
            id: headers.indexOf(resolveAMField_('ID')),
            originalFormId: headers.indexOf(resolveAMField_('Original Form ID')),
            firstName: headers.indexOf(resolveAMField_('First Name')),
            lastName: headers.indexOf(resolveAMField_('Last Name')),
            requestDate: headers.indexOf(resolveAMField_('Request Date')),
            requestType: headers.indexOf(resolveAMField_('Request Type')),
            serviceStatus: headers.indexOf(resolveAMField_('Service Status')),
            address: headers.indexOf(resolveAMField_('Street Address')),
            city: headers.indexOf(resolveAMField_('City')),
            state: headers.indexOf(resolveAMField_('State')),
            zipCode: headers.indexOf(resolveAMField_('Zip Code')),
            county: headers.indexOf(resolveAMField_('County')),
            incomeLevel: headers.indexOf(resolveAMField_(COL_INCOME)),
            usedBefore: headers.indexOf(resolveAMField_(COL_USED_BEFORE)),
            babyBox: headers.indexOf(resolveAMField_(COL_BABY_BOX))
        };

        var records = [];
        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            var reqDate = new Date(row[colIdx.requestDate]);
            var fn = getStr(row, colIdx.firstName);
            var ln = getStr(row, colIdx.lastName);
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
                firstName: fn,
                lastName: ln,
                // Children 2 and Under = Take Baby Box? X→1 (displayed as 1 or blank)
                children2Under: getBabyBoxIndicator(getStr(row, colIdx.babyBox)),
                id: getStr(row, colIdx.id),
                originalFormId: getStr(row, colIdx.originalFormId)
            });
        }

        records.sort(function (a, b) {
            if (a.quarter !== b.quarter) return a.quarter - b.quarter;
            if (a.year !== b.year) return a.year - b.year;
            if (a.monthNum !== b.monthNum) return a.monthNum - b.monthNum;
            if (a.day !== b.day) return a.day - b.day;
            var c = a.address1.localeCompare(b.address1); if (c) return c;
            c = a.city.localeCompare(b.city); if (c) return c;
            c = a.state.localeCompare(b.state); if (c) return c;
            c = a.zip.localeCompare(b.zip); if (c) return c;
            return a.county.localeCompare(b.county);
        });

        var cols = _getColsWithFallback_('Open Requests');
        return _writeGrantReport_('Open Requests', 'Applicants Open Requests',
            'Applicants_Open_Requests', records, cols, combined, fromDate, toDate, 'GRANTS_REPORT');

    } catch (e) {
        Logger.log('Open Requests error: ' + e.message);
        return { success: false, error: 'Report generation failed: ' + e.message };
    }
}


// ─────────────────────────────────────────────────────────────────────────────
// REPORT: HOUSEHOLDS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Generate Households report with full age bracket and product detail.
 *
 * Filter: Service Status = 'Picked Up' or 'Delivered'.
 * Assistance field expanded: one row per comma-separated assistance type.
 * Columns: driven by LU_ReportColumns 'Households'.
 * Order: Quarter, Year, Month, Day, City, State, Zip Code, County.
 *
 * v3.8: Funding Code column; Baby Products columns; babyBox added to colIdx
 * v3.9: Children 2 and Under fixed — Take Baby Box? X→1 (not a.under5)
 * v4.0: dataHeaders replaced with LU_ReportColumns lookup
 *
 * @param {string} fromDateStr - YYYY-MM-DD
 * @param {string} toDateStr   - YYYY-MM-DD
 * @returns {Object} Standard report result
 */
function generateHouseholdsReport(fromDateStr, toDateStr) {
    try {
        if (!fromDateStr || !toDateStr)
            return { success: false, error: 'Both From Date and To Date are required' };
        var fromDate = parseDateInput(fromDateStr, false);
        var toDate = parseDateInput(toDateStr, true);
        if (fromDate > toDate)
            return { success: false, error: 'From Date must be before To Date' };

        var combined = getCombinedData(fromDate, toDate,
            [{ column: 'Service Status', values: ['Picked Up', 'Delivered'] }]);
        if (combined.totalCount === 0)
            return { success: false, error: 'No Picked Up or Delivered records found for the specified date range' };

        var headers = combined.headers;
        var rows = combined.rows;
        var productData = loadProductLookupData(fromDate, toDate);

        var colIdx = {
            id: headers.indexOf(resolveAMField_('ID')),
            requestDate: headers.indexOf(resolveAMField_('Request Date')),
            firstName: headers.indexOf(resolveAMField_('First Name')),
            lastName: headers.indexOf(resolveAMField_('Last Name')),
            city: headers.indexOf(resolveAMField_('City')),
            state: headers.indexOf(resolveAMField_('State')),
            zipCode: headers.indexOf(resolveAMField_('Zip Code')),
            county: headers.indexOf(resolveAMField_('County')),
            requestType: headers.indexOf(resolveAMField_('Request Type')),
            serviceStatus: headers.indexOf(resolveAMField_('Service Status')),
            fundingCode: headers.indexOf(resolveAMField_('Funding Code')),
            militaryStatus: headers.indexOf(resolveAMField_('Military Status')),
            race: headers.indexOf(resolveAMField_('Please Select Your Racial Category')),
            ethnicity: headers.indexOf(resolveAMField_('Please Select Your Ethnic Category')),
            homeless: headers.indexOf(resolveAMField_('Are you currently homeless?')),
            employed: headers.indexOf(resolveAMField_('Are you currently employed?')),
            incomeLevel: headers.indexOf(resolveAMField_(COL_INCOME)),
            assistance: headers.indexOf(resolveAMField_(COL_ASSISTANCE)),
            usedBefore: headers.indexOf(resolveAMField_(COL_USED_BEFORE)),
            babyBox: headers.indexOf(resolveAMField_(COL_BABY_BOX)),
            productCode1: headers.indexOf(resolveAMField_('Received Product Code 1')),
            productCode2: headers.indexOf(resolveAMField_('Received Product Code 2')),
            productCode3: headers.indexOf(resolveAMField_('Received Product Code 3'))
        };

        var records = [];
        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            var reqDate = new Date(row[colIdx.requestDate]);
            var ages = calculateDetailedAgeBrackets(headers, row);
            var recId = getStr(row, colIdx.id);
            var products = calculateProductCounts(recId,
                getStr(row, colIdx.productCode1), getStr(row, colIdx.productCode2),
                getStr(row, colIdx.productCode3), reqDate, productData, headers, row);

            // Children 2 and Under: Take Baby Box? X=1, blank=0
            // This is NOT the same as Under 5 age bracket (ages.under5 covers 0-4 yrs)
            var children2Under = (getStr(row, colIdx.babyBox).toUpperCase() === 'X') ? 1 : 0;

            // Expand Assistance into one record per item (comma-separated)
            var rawAssist = getStr(row, colIdx.assistance);
            var sources = rawAssist.indexOf(', ') !== -1 ? rawAssist.split(', ') : [rawAssist];
            var applicantType = getApplicantType(getStr(row, colIdx.usedBefore));

            for (var s = 0; s < sources.length; s++) {
                records.push({
                    quarter: getQuarter(reqDate.getMonth()),
                    year: reqDate.getFullYear(),
                    month: getMonthName(reqDate.getMonth()),
                    day: reqDate.getDate(),
                    city: getStr(row, colIdx.city),
                    state: getStr(row, colIdx.state),
                    zip: getStr(row, colIdx.zipCode),
                    county: getStr(row, colIdx.county),
                    applicantType: applicantType,
                    requestType: getStr(row, colIdx.requestType),
                    serviceStatus: getStr(row, colIdx.serviceStatus),
                    fundingCode: getStr(row, colIdx.fundingCode),
                    militaryStatus: getStr(row, colIdx.militaryStatus),
                    race: getStr(row, colIdx.race),
                    ethnicity: getStr(row, colIdx.ethnicity),
                    homeless: getStr(row, colIdx.homeless),
                    employed: getStr(row, colIdx.employed),
                    incomeLevel: getStr(row, colIdx.incomeLevel),
                    incomeSourceType: sources[s].trim(),
                    productsRequested: products.productsRequested,
                    productsDistributed: products.productsDistributed,
                    babyRequested: products.babyRequested,
                    babyDistributed: products.babyDistributed,
                    children2Under: children2Under,
                    ages: ages
                });
            }
        }

        records.sort(function (a, b) {
            var c = a.quarter - b.quarter; if (c) return c;
            c = a.year - b.year; if (c) return c;
            c = a.month - b.month; if (c) return c;
            c = a.day - b.day; if (c) return c;
            c = a.city.localeCompare(b.city); if (c) return c;
            c = a.state.localeCompare(b.state); if (c) return c;
            c = a.zip.localeCompare(b.zip); if (c) return c;
            return a.county.localeCompare(b.county);
        });

        var cols = _getColsWithFallback_('Households');
        return _writeGrantReport_('Households', 'Households', 'Households',
            records, cols, combined, fromDate, toDate, 'GRANTS_REPORT');

    } catch (e) {
        Logger.log('Households error: ' + e.message);
        return { success: false, error: 'Report generation failed: ' + e.message };
    }
}


// ─────────────────────────────────────────────────────────────────────────────
// REPORT: DISTRIBUTION STATS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Generate Distribution Stats report — all records, full demographics.
 *
 * No filter on Service Status (all records included).
 * Products counted only when Service Status = 'Picked Up' or 'Delivered'.
 * Assistance field expanded: one row per comma-separated assistance type.
 * Columns: driven by LU_ReportColumns 'Distribution Stats'.
 * Order: Quarter, Year, Month, Day, First Name, Last Name, City, State, Zip, County.
 *
 * v3.9: Products Requested added; Baby columns added; Children 2 and Under fixed;
 *       referral column raw headers corrected to match LU_FieldMap COL_* constants
 * v4.0: dataHeaders replaced with LU_ReportColumns lookup
 *
 * @param {string} fromDateStr - YYYY-MM-DD
 * @param {string} toDateStr   - YYYY-MM-DD
 * @returns {Object} Standard report result
 */
function generateDistributionStatsReport(fromDateStr, toDateStr) {
    try {
        if (!fromDateStr || !toDateStr)
            return { success: false, error: 'Both From Date and To Date are required' };
        var fromDate = parseDateInput(fromDateStr, false);
        var toDate = parseDateInput(toDateStr, true);
        if (fromDate > toDate)
            return { success: false, error: 'From Date must be before To Date' };

        var combined = getCombinedData(fromDate, toDate); // no status filter
        if (combined.totalCount === 0)
            return { success: false, error: 'No records found for the specified date range' };

        var headers = combined.headers;
        var rows = combined.rows;
        var productData = loadProductLookupData(fromDate, toDate);

        var colIdx = {
            id: headers.indexOf(resolveAMField_('ID')),
            requestDate: headers.indexOf(resolveAMField_('Request Date')),
            firstName: headers.indexOf(resolveAMField_('First Name')),
            lastName: headers.indexOf(resolveAMField_('Last Name')),
            city: headers.indexOf(resolveAMField_('City')),
            state: headers.indexOf(resolveAMField_('State')),
            zipCode: headers.indexOf(resolveAMField_('Zip Code')),
            county: headers.indexOf(resolveAMField_('County')),
            serviceStatus: headers.indexOf(resolveAMField_('Service Status')),
            fundingCode: headers.indexOf(resolveAMField_('Funding Code')),
            requestType: headers.indexOf(resolveAMField_('Request Type')),
            finalServiceDate: headers.indexOf(resolveAMField_('Final Service Contact Date')),
            incomeLevel: headers.indexOf(resolveAMField_(COL_INCOME)),
            militaryStatus: headers.indexOf(resolveAMField_('Military Status')),
            race: headers.indexOf(resolveAMField_('Please Select Your Racial Category')),
            ethnicity: headers.indexOf(resolveAMField_('Please Select Your Ethnic Category')),
            homeless: headers.indexOf(resolveAMField_('Are you currently homeless?')),
            employed: headers.indexOf(resolveAMField_('Are you currently employed?')),
            assistance: headers.indexOf(resolveAMField_(COL_ASSISTANCE)),
            babyBox: headers.indexOf(resolveAMField_(COL_BABY_BOX)),
            howLearned: headers.indexOf(resolveAMField_(COL_HOW_LEARNED)),
            personOrg: headers.indexOf(resolveAMField_(COL_PERSON_ORG)),
            titlePos: headers.indexOf(resolveAMField_(COL_TITLE_POS)),
            whoReferred: headers.indexOf(resolveAMField_(COL_WHO_REFERRED)),
            moreInfo: headers.indexOf(resolveAMField_(COL_MORE_INFO)),
            productCode1: headers.indexOf(resolveAMField_('Received Product Code 1')),
            productCode2: headers.indexOf(resolveAMField_('Received Product Code 2')),
            productCode3: headers.indexOf(resolveAMField_('Received Product Code 3'))
        };

        var records = [];
        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            var reqDate = new Date(row[colIdx.requestDate]);
            var svcStatus = getStr(row, colIdx.serviceStatus);
            var svcUpper = svcStatus.toUpperCase();

            // Products only counted for Picked Up or Delivered
            var products = { productsRequested: 0, productsDistributed: 0, babyRequested: 0, babyDistributed: 0 };
            if (svcUpper === 'PICKED UP' || svcUpper === 'DELIVERED') {
                products = calculateProductCounts(getStr(row, colIdx.id),
                    getStr(row, colIdx.productCode1), getStr(row, colIdx.productCode2),
                    getStr(row, colIdx.productCode3), reqDate, productData, headers, row);
            }

            var finalDate = '';
            if (colIdx.finalServiceDate !== -1 && row[colIdx.finalServiceDate]) {
                var fd = new Date(row[colIdx.finalServiceDate]);
                if (!isNaN(fd.getTime())) finalDate = Utilities.formatDate(fd, CONFIG.TIMEZONE, 'M/d/yyyy');
            }

            // Children 2 and Under: Take Baby Box? X=1, blank=0
            var children2Under = (getStr(row, colIdx.babyBox).toUpperCase() === 'X') ? 1 : 0;

            // Expand Assistance into one record per item
            var rawAssist = getStr(row, colIdx.assistance);
            var sources = rawAssist.indexOf(', ') !== -1 ? rawAssist.split(', ') : [rawAssist];
            var ages = calculateDetailedAgeBrackets(headers, row);

            for (var s = 0; s < sources.length; s++) {
                records.push({
                    quarter: getQuarter(reqDate.getMonth()),
                    year: reqDate.getFullYear(),
                    month: getMonthName(reqDate.getMonth()),
                    day: reqDate.getDate(),
                    id: getStr(row, colIdx.id),
                    firstName: getStr(row, colIdx.firstName),
                    lastName: getStr(row, colIdx.lastName),
                    city: getStr(row, colIdx.city),
                    state: getStr(row, colIdx.state),
                    zip: getStr(row, colIdx.zipCode),
                    county: getStr(row, colIdx.county),
                    fundingCode: getStr(row, colIdx.fundingCode),
                    requestType: getStr(row, colIdx.requestType),
                    serviceStatus: svcStatus,
                    finalServiceDate: finalDate,
                    code1: getStr(row, colIdx.productCode1),
                    code2: getStr(row, colIdx.productCode2),
                    code3: getStr(row, colIdx.productCode3),
                    productsRequested: products.productsRequested,
                    productsDistributed: products.productsDistributed,
                    babyRequested: products.babyRequested,
                    babyDistributed: products.babyDistributed,
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
                    incomeSourceType: sources[s].trim(),
                    children2Under: children2Under,
                    ages: ages
                });
            }
        }

        records.sort(function (a, b) {
            var c = a.quarter - b.quarter; if (c) return c;
            c = a.year - b.year; if (c) return c;
            c = a.month - b.month; if (c) return c;
            c = a.day - b.day; if (c) return c;
            c = a.firstName.localeCompare(b.firstName); if (c) return c;
            c = a.lastName.localeCompare(b.lastName); if (c) return c;
            c = a.city.localeCompare(b.city); if (c) return c;
            c = a.state.localeCompare(b.state); if (c) return c;
            c = a.zip.localeCompare(b.zip); if (c) return c;
            return a.county.localeCompare(b.county);
        });

        var cols = _getColsWithFallback_('Distribution Stats');
        return _writeGrantReport_('Distribution Stats', 'Distribution Stats', 'Distribution_Stats',
            records, cols, combined, fromDate, toDate, 'GRANTS_REPORT');

    } catch (e) {
        Logger.log('Distribution Stats error: ' + e.message);
        return { success: false, error: 'Report generation failed: ' + e.message };
    }
}


// ─────────────────────────────────────────────────────────────────────────────
// REPORT: TESTIMONIALS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Generate Testimonials report.
 *
 * Filter: 'How will receiving products help?' (COL_HELP_1) is not blank.
 * Columns: driven by LU_ReportColumns 'Testimonials'.
 * Order: Quarter, Year, Month, Day, City, State, Zip, County.
 *
 * v4.0: dataHeaders replaced with LU_ReportColumns lookup
 *
 * @param {string} fromDateStr - YYYY-MM-DD
 * @param {string} toDateStr   - YYYY-MM-DD
 * @returns {Object} Standard report result
 */
function generateTestimonialsReport(fromDateStr, toDateStr) {
    try {
        if (!fromDateStr || !toDateStr)
            return { success: false, error: 'Both From Date and To Date are required' };
        var fromDate = parseDateInput(fromDateStr, false);
        var toDate = parseDateInput(toDateStr, true);
        if (fromDate > toDate)
            return { success: false, error: 'From Date must be before To Date' };

        var combined = getCombinedData(fromDate, toDate);
        if (combined.totalCount === 0)
            return { success: false, error: 'No records found for the specified date range' };

        var headers = combined.headers;
        var rows = combined.rows;
        var colIdx = {
            requestDate: headers.indexOf(resolveAMField_('Request Date')),
            city: headers.indexOf(resolveAMField_('City')),
            state: headers.indexOf(resolveAMField_('State')),
            zipCode: headers.indexOf(resolveAMField_('Zip Code')),
            county: headers.indexOf(resolveAMField_('County')),
            incomeLevel: headers.indexOf(resolveAMField_(COL_INCOME)),
            usedBefore: headers.indexOf(resolveAMField_(COL_USED_BEFORE)),
            requestType: headers.indexOf(resolveAMField_('Request Type')),
            serviceStatus: headers.indexOf(resolveAMField_('Service Status')),
            emergency: headers.indexOf(resolveAMField_(COL_EMERGENCY)),
            situation: headers.indexOf(resolveAMField_(COL_SITUATION)),
            help1: headers.indexOf(resolveAMField_(COL_HELP_1)),
            help2: headers.indexOf(resolveAMField_(COL_HELP_2)),
            permission: headers.indexOf(resolveAMField_(COL_PERMISSION))
        };

        var records = [];
        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            var help1 = getStr(row, colIdx.help1);
            if (!help1) continue;                    // filter: only records with testimonial
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
                help1: help1,
                help2: getStr(row, colIdx.help2),
                permission: getStr(row, colIdx.permission)
            });
        }
        if (records.length === 0)
            return { success: false, error: 'No records with testimonials found for the specified date range' };

        records.sort(function (a, b) {
            if (a.quarter !== b.quarter) return a.quarter - b.quarter;
            if (a.year !== b.year) return a.year - b.year;
            if (a.monthNum !== b.monthNum) return a.monthNum - b.monthNum;
            if (a.day !== b.day) return a.day - b.day;
            var c = a.city.localeCompare(b.city); if (c) return c;
            c = a.state.localeCompare(b.state); if (c) return c;
            c = a.zip.localeCompare(b.zip); if (c) return c;
            return a.county.localeCompare(b.county);
        });

        var cols = _getColsWithFallback_('Testimonials');
        return _writeGrantReport_('Testimonials', 'Testimonials', 'Testimonials',
            records, cols, combined, fromDate, toDate, 'GRANTS_REPORT');

    } catch (e) {
        Logger.log('Testimonials error: ' + e.message);
        return { success: false, error: 'Report generation failed: ' + e.message };
    }
}


// ─────────────────────────────────────────────────────────────────────────────
// REPORT: REQUEST FREQUENCY
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Generate Request Frequency report.
 *
 * Calculates average days between requests per applicant.
 * Filter: Service Status = 'Picked Up' or 'Delivered'.
 * Columns: driven by LU_ReportColumns 'Request Frequency'.
 *
 * MySQL fast path: delegates to sp_get_request_frequency_data (LAG window fn).
 * Sheets path: groups rows by name, computes avg from Request Date - Last Date Served.
 *
 * v4.0: _writeRequestFrequencySpreadsheet_() now accepts cols parameter
 *
 * @param {string} fromDateStr - YYYY-MM-DD
 * @param {string} toDateStr   - YYYY-MM-DD
 * @returns {Object} Standard report result
 */
function generateRequestFrequencyReport(fromDateStr, toDateStr) {
    try {
        if (!fromDateStr || !toDateStr)
            return { success: false, error: 'Both From Date and To Date are required' };
        var fromDate = parseDateInput(fromDateStr, false);
        var toDate = parseDateInput(toDateStr, true);
        if (fromDate > toDate)
            return { success: false, error: 'From Date must be before To Date' };

        var cols = _getColsWithFallback_('Request Frequency');

        // ── MySQL fast path ───────────────────────────────────────────────────
        if (CONFIG.DB && CONFIG.DB.USE_MYSQL) {
            try {
                var fs = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
                var ts = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
                var freqData = DbService.getRequestFrequencyData(fs, ts);
                var mH = freqData.headers || [];
                var qi = mH.indexOf('quarter'), yi = mH.indexOf('year'), mni = mH.indexOf('month'),
                    di = mH.indexOf('day'), cii = mH.indexOf('city'), sti = mH.indexOf('state'),
                    zii = mH.indexOf('zip'), coi = mH.indexOf('county'), inci = mH.indexOf('income_level'),
                    rti = mH.indexOf('request_type'), ssi = mH.indexOf('service_status'),
                    rci = mH.indexOf('request_count'), afi = mH.indexOf('avg_freq_days');
                var records = (freqData.rows || []).map(function (r) {
                    return {
                        quarter: qi !== -1 ? r[qi] : '', year: yi !== -1 ? r[yi] : '',
                        month: mni !== -1 ? r[mni] : '', day: di !== -1 ? r[di] : '',
                        city: cii !== -1 ? r[cii] : '', state: sti !== -1 ? r[sti] : '',
                        zip: zii !== -1 ? r[zii] : '', county: coi !== -1 ? r[coi] : '',
                        incomeLevel: inci !== -1 ? r[inci] : '', requestType: rti !== -1 ? r[rti] : '',
                        serviceStatus: ssi !== -1 ? r[ssi] : '',
                        requestCount: rci !== -1 ? r[rci] : 0,
                        avgFreqDays: afi !== -1 && r[afi] !== null ? r[afi] : ''
                    };
                });
                var totalReqs = records.reduce(function (s, r) { return s + (r.requestCount || 0); }, 0);
                return _writeRequestFrequencySpreadsheet_(records, totalReqs,
                    freqData.totalCount, 0, fromDate, toDate, cols);
            } catch (e) {
                Logger.log('Request Frequency MySQL error (fallback to Sheets): ' + e.message);
            }
        }

        // ── Sheets path ───────────────────────────────────────────────────────
        var combined = getCombinedData(fromDate, toDate,
            [{ column: 'Service Status', values: ['Picked Up', 'Delivered'] }]);
        if (combined.totalCount === 0)
            return { success: false, error: 'No records found for the specified date range with Picked Up or Delivered status' };

        var headers = combined.headers;
        var rows = combined.rows;
        var colIdx = {
            requestDate: headers.indexOf(resolveAMField_('Request Date')),
            lastDateServed: headers.indexOf(resolveAMField_('Last Date Served')),
            city: headers.indexOf(resolveAMField_('City')),
            state: headers.indexOf(resolveAMField_('State')),
            zipCode: headers.indexOf(resolveAMField_('Zip Code')),
            county: headers.indexOf(resolveAMField_('County')),
            incomeLevel: headers.indexOf(resolveAMField_(COL_INCOME)),
            requestType: headers.indexOf(resolveAMField_('Request Type')),
            serviceStatus: headers.indexOf(resolveAMField_('Service Status')),
            firstName: headers.indexOf(resolveAMField_('First Name')),
            lastName: headers.indexOf(resolveAMField_('Last Name'))
        };

        // Group by applicant name, compute frequency
        var applicantMap = {};
        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            var reqDate = new Date(row[colIdx.requestDate]);
            var ldsDate = colIdx.lastDateServed !== -1 && row[colIdx.lastDateServed]
                ? new Date(row[colIdx.lastDateServed]) : null;
            var fn = getStr(row, colIdx.firstName);
            var ln = getStr(row, colIdx.lastName);
            var key = (fn + '|' + ln).toLowerCase();
            if (!applicantMap[key]) applicantMap[key] = {
                quarter: getQuarter(reqDate.getMonth()), year: reqDate.getFullYear(),
                month: getMonthName(reqDate.getMonth()), day: reqDate.getDate(),
                city: getStr(row, colIdx.city), state: getStr(row, colIdx.state),
                zip: getStr(row, colIdx.zipCode), county: getStr(row, colIdx.county),
                incomeLevel: getStr(row, colIdx.incomeLevel),
                requestType: getStr(row, colIdx.requestType),
                serviceStatus: getStr(row, colIdx.serviceStatus),
                requestCount: 0, totalFreqDays: 0, freqCount: 0
            };
            applicantMap[key].requestCount++;
            if (ldsDate && !isNaN(ldsDate.getTime())) {
                var diff = Math.round((reqDate - ldsDate) / (1000 * 60 * 60 * 24));
                if (diff > 0) {
                    applicantMap[key].totalFreqDays += diff;
                    applicantMap[key].freqCount++;
                }
            }
        }

        var records = [];
        for (var k in applicantMap) {
            var ap = applicantMap[k];
            ap.avgFreqDays = ap.freqCount > 0 ? Math.round(ap.totalFreqDays / ap.freqCount) : '';
            records.push(ap);
        }
        var totalReqs = records.reduce(function (s, r) { return s + r.requestCount; }, 0);
        return _writeRequestFrequencySpreadsheet_(records, totalReqs,
            combined.masterCount, combined.archiveCount, fromDate, toDate, cols);

    } catch (e) {
        Logger.log('Request Frequency error: ' + e.message);
        return { success: false, error: 'Report generation failed: ' + e.message };
    }
}

/**
 * Write Request Frequency spreadsheet.
 * Extracted as a private helper shared by the MySQL and Sheets paths above.
 * v4.0: Accepts cols parameter from LU_ReportColumns instead of hardcoded array.
 * @private
 */
function _writeRequestFrequencySpreadsheet_(records, totalRequestCount,
    masterCount, archiveCount,
    fromDate, toDate, cols) {
    var fromFormatted = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var toFormatted = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
    var spreadsheet = SpreadsheetApp.create('Request_Frequency_' + fromFormatted + '_to_' + toFormatted);
    var sheet = spreadsheet.getActiveSheet();
    sheet.setName('Request Frequency');
    moveToFolder(spreadsheet.getId(), CONFIG.GRANTS_FOLDER_ID);

    var dataHeaders = cols.map(function (c) { return c.label; });
    var reportColCount = dataHeaders.length;

    var currentRow = writeReportTitleSection(sheet, 'Request Frequency', fromDate, toDate,
        totalRequestCount, masterCount, archiveCount);
    var headerRowNum = currentRow;
    sheet.getRange(headerRowNum, 1, 1, reportColCount).setNumberFormat('@').setValues([dataHeaders]);
    styleReportHeader(sheet, headerRowNum, reportColCount);
    currentRow++;

    var allRows = _buildGrantRows_(records, cols);
    if (allRows.length > 0) {
        sheet.getRange(currentRow, 1, allRows.length, reportColCount).setValues(allRows);
        for (var i = 1; i < allRows.length; i += 2) {
            sheet.getRange(currentRow + i, 1, 1, reportColCount).setBackground('#f8f9fa');
        }
        currentRow += allRows.length;
    }

    currentRow++;
    sheet.getRange(currentRow, 1).setValue(
        'End of Report - ' + records.length + ' rows, ' + totalRequestCount + ' total requests');
    sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(10).setFontColor('#666666');

    applyReportColumnFormatting(sheet, cols, headerRowNum + 1, headerRowNum + allRows.length);
    sheet.setFrozenRows(headerRowNum);

    var fromDisplay = Utilities.formatDate(fromDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    var toDisplay = Utilities.formatDate(toDate, CONFIG.TIMEZONE, 'M/d/yyyy');
    logAudit('GRANTS_REPORT', null,
        'Generated Request Frequency: ' + fromDisplay + ' to ' + toDisplay +
        ' (' + records.length + ' rows, ' + totalRequestCount + ' requests)');

    var reportUrl = spreadsheet.getUrl();
    var downloadUrl = 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx';
    return {
        success: true,
        message: 'Report generated with ' + records.length + ' rows',
        recordCount: totalRequestCount,
        masterCount: masterCount,
        archiveCount: archiveCount,
        reportUrl: reportUrl,
        downloadUrl: downloadUrl,
        reportId: spreadsheet.getId()
    };
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
