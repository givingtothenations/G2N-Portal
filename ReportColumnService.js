/**
 * ReportColumnService.gs  [AP — AdminPortalWeb / shared GAS project]
 * Sheet-driven report column definitions for all G2N generated reports.
 * Reads LU_ReportColumns from G2N_Lookups workbook, replacing all hardcoded
 * dataHeaders arrays and reportColumns/ageBracketPairs/additionalColumns arrays
 * across GrantsReportService and ReportService.
 *
 * LU_ReportColumns sheet columns:
 *   Report Name    — matches report key used in getReportColumns() calls
 *   Sort Order     — integer; controls left-to-right column position
 *   Column Key     — raw AM header, [Calc]/[Summary] key, or special literal
 *                    (e.g. 'Quarter', 'Year', 'History', 'Avg Frequency Days')
 *   Override Label — optional; if blank, label resolved via getReportHeader_(key)
 *   Column Width   — pixel width integer applied to generated sheet
 *   Wrap Text      — Y/N; whether to wrap cell content in generated sheet
 *   Active         — Y/N; inactive columns are excluded from output
 *   Notes          — admin documentation; not used in code
 *
 * Admin use: To reorder columns, change Sort Order values. To hide a column,
 * set Active = N. To rename a column header, set Override Label. To add a new
 * computed column, add a row with the [Calc] key and ensure a resolver exists
 * in GrantsReportService._getGrantResolvers_() or ReportService.
 *
 * v1.0 - Initial implementation.
 *         loadReportColumns() — reads LU_ReportColumns, caches per execution.
 *         getReportColumns(reportName) — returns [{key, label, width, wrapText}]
 *           sorted by Sort Order, Active = Y only.
 *         getReportHeaders(reportName) — returns display label array only.
 *         clearReportColumnCache() — cache invalidation hook.
 */

// ============ CACHE ============

/** @type {Object|null} Per-execution cache */
var _reportColumnCache = null;

// ============ CORE LOADER ============

/**
 * Load all report column definitions from LU_ReportColumns sheet.
 * Caches per execution. Falls back gracefully if sheet not found.
 * @returns {Object} { byReport: { reportName: [{key,label,width,wrapText,sortOrder}] } }
 */
function loadReportColumns() {
    if (_reportColumnCache) return _reportColumnCache;

    var result = { byReport: {} };

    try {
        var wb = getLookupsWorkbook();
        var sheet = wb.getSheetByName('LU_ReportColumns');

        if (!sheet || sheet.getLastRow() < 2) {
            Logger.log('ReportColumnService: LU_ReportColumns not found or empty — using hardcoded fallback');
            _reportColumnCache = result;
            return result;
        }

        var data = sheet.getDataRange().getValues();
        var headers = trimHeaders(data[0]);

        var ci = {
            reportName: headers.indexOf('Report Name'),
            sortOrder: headers.indexOf('Sort Order'),
            columnKey: headers.indexOf('Column Key'),
            overrideLabel: headers.indexOf('Override Label'),
            columnWidth: headers.indexOf('Column Width'),
            wrapText: headers.indexOf('Wrap Text'),
            active: headers.indexOf('Active')
        };

        for (var i = 1; i < data.length; i++) {
            var row = data[i];
            var reportName = ci.reportName !== -1 ? (row[ci.reportName] || '').toString().trim() : '';
            if (!reportName) continue;

            var active = ci.active !== -1 ? (row[ci.active] || '').toString().trim().toUpperCase() : 'Y';
            if (active !== 'Y') continue;

            var key = ci.columnKey !== -1 ? (row[ci.columnKey] || '').toString().trim() : '';
            var overrideLbl = ci.overrideLabel !== -1 ? (row[ci.overrideLabel] || '').toString().trim() : '';
            var sortOrder = ci.sortOrder !== -1 ? (parseInt(row[ci.sortOrder]) || 0) : 0;
            var columnWidth = ci.columnWidth !== -1 ? (parseInt(row[ci.columnWidth]) || 70) : 70;
            var wrapText = ci.wrapText !== -1 ? (row[ci.wrapText] || '').toString().trim().toUpperCase() === 'Y' : false;

            if (!key) continue;

            // Resolve display label: Override Label → getReportHeader_(key) → key
            var label = overrideLbl || getReportHeader_(key) || key;

            if (!result.byReport[reportName]) result.byReport[reportName] = [];
            result.byReport[reportName].push({
                key: key,
                label: label,
                width: columnWidth,
                wrapText: wrapText,
                sortOrder: sortOrder
            });
        }

        // Sort each report's columns by Sort Order
        for (var rn in result.byReport) {
            result.byReport[rn].sort(function (a, b) { return a.sortOrder - b.sortOrder; });
        }

        Logger.log('ReportColumnService: Loaded definitions for reports: ' + Object.keys(result.byReport).join(', '));

    } catch (e) {
        Logger.log('ReportColumnService: loadReportColumns error — ' + e.message);
    }

    _reportColumnCache = result;
    return result;
}


// ============ PUBLIC API ============

/**
 * Get ordered, active column definitions for a named report.
 * Returns empty array (not an error) if report name not found — callers
 * should fall back to hardcoded defaults when the array is empty.
 *
 * @param  {string} reportName - Must match 'Report Name' in LU_ReportColumns
 *                               e.g. 'Households', 'Distribution Stats',
 *                               'Open Requests', 'Request Frequency',
 *                               'Testimonials', 'Scheduling',
 *                               'Login Summary', 'Login Detail', 'Login Intake'
 * @returns {Array} [{key:string, label:string, width:number, wrapText:boolean}]
 */
function getReportColumns(reportName) {
    var cache = loadReportColumns();
    return (cache.byReport[reportName] || []).map(function (c) {
        return { key: c.key, label: c.label, width: c.width, wrapText: c.wrapText };
    });
}

/**
 * Get only the display labels for a named report in column order.
 * Convenience wrapper used for setValues([headers]) calls.
 * @param  {string} reportName
 * @returns {string[]}
 */
function getReportHeaders(reportName) {
    return getReportColumns(reportName).map(function (c) { return c.label; });
}

/**
 * Apply column widths and optional text wrap from a cols array to a sheet.
 * Typically called immediately after writing the header row.
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet
 * @param {Array}  cols      - From getReportColumns()
 * @param {number} dataStart - First data row number (for wrap range)
 * @param {number} lastRow   - Last data row number (for wrap range)
 */
function applyReportColumnFormatting(sheet, cols, dataStart, lastRow) {
    for (var c = 0; c < cols.length; c++) {
        sheet.setColumnWidth(c + 1, cols[c].width);
        if (cols[c].wrapText && dataStart <= lastRow) {
            sheet.getRange(dataStart, c + 1, lastRow - dataStart + 1, 1).setWrap(true);
        }
    }
}

/**
 * Clear the report column cache.
 * Call when LU_ReportColumns sheet is edited (e.g. from onEdit trigger).
 */
function clearReportColumnCache() {
    _reportColumnCache = null;
    Logger.log('ReportColumnService: Cache cleared');
}
