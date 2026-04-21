/**
 * FieldMapService.gs
 * Centralized field mapping service for G2N System.
 * Reads field definitions from LU_FieldMap sheet in G2N_Lookups workbook.
 *
 * v1.2 - Added SV Field ID column support. bySvFieldId lookup added to result.
 * v1.3 - Added Db Column Name column support. byDbColumnName lookup added.
 * v1.4 - getStaffPortalSections() logs diagnostic warning when fieldMap is empty.
 * v1.5 - loadFieldMap(): added previousHeaderName column support.
 *         byPreviousHeaderName index { oldName → currentRawHeader } built for
 *         Archive rename detection in syncArchiveHeaders_() (ReportService).
 * v1.6 - Removed MySQL-only LU_FieldMap columns: Db Column Name, Db Table Name,
 *         Table Field Name, Nullable, Index Type. Removed dbColumnName from ci,
 *         entry object, byDbColumnName index, and _buildFallbackFieldMap().
 *         LU_FieldMap sheet: remove those columns manually (MySQL path removed).
 *         WorkbookSheet Name column also removed (unused by any service function).
 */

// ============ CACHE ============

/**
 * Per-execution cache for the field map data.
 * Cleared automatically when execution ends.
 * @type {Object|null}
 */
var _fieldMapCache = null;

// ============ CORE LOADER ============

/**
 * Load and cache the LU_FieldMap sheet from G2N_Lookups workbook.
 * Returns an object with multiple lookup structures for efficient access.
 * Caches per execution to avoid redundant reads.
 *
 * @returns {Object} {
 *   byRawHeader: { rawHeader: {displayLabel, reportHeader, fieldGroup, dataType, formFieldId, portalVisibility, searchable, required, colConstant, notes} },
 *   byDisplayLabel: { displayLabel: rawHeader },
 *   byFormFieldId: { formFieldId: rawHeader },
 *   byColConstant: { colConstant: rawHeader },
 *   byGroup: { groupName: [{rawHeader, displayLabel, ...}] },
 *   allFields: [{rawHeader, displayLabel, reportHeader, fieldGroup, dataType, formFieldId, portalVisibility, searchable, required, colConstant, notes}]
 * }
 */
function loadFieldMap() {
    if (_fieldMapCache) return _fieldMapCache;

    var wb = getLookupsWorkbook();
    var sheet = wb.getSheetByName('LU_FieldMap');
    if (!sheet || sheet.getLastRow() < 2) {
        Logger.log('FieldMapService: LU_FieldMap sheet not found or empty — falling back to hardcoded map');
        _fieldMapCache = _buildFallbackFieldMap();
        return _fieldMapCache;
    }

    var data = sheet.getDataRange().getValues();
    var headers = trimHeaders(data[0]);

    // Map column indices
    var ci = {
        rawHeader: headers.indexOf('Raw Header'),
        displayLabel: headers.indexOf('Display Label'),
        reportHeader: headers.indexOf('Report Header'),
        fieldGroup: headers.indexOf('Field Group'),
        dataType: headers.indexOf('Data Type'),
        formFieldId: headers.indexOf('Form Field ID'),
        svFieldId: headers.indexOf('SV Field ID'),
        portalVisibility: headers.indexOf('Portal Visibility'),
        searchable: headers.indexOf('Searchable'),
        required: headers.indexOf('Required'),
        colConstant: headers.indexOf('COL Constant'),
        notes: headers.indexOf('Notes'),
        editable: headers.indexOf('Editable'),
        formSection: headers.indexOf('Form Section'),
        lookupSource: headers.indexOf('Lookup Source'),
        previousHeaderName: headers.indexOf('Previous Header Name')
    };

    var result = {
        byRawHeader: {},
        byDisplayLabel: {},
        byFormFieldId: {},
        bySvFieldId: {},
        byColConstant: {},
        byPreviousHeaderName: {},
        byGroup: {},
        allFields: []
    };

    for (var i = 1; i < data.length; i++) {
        var row = data[i];
        var rawHeader = (ci.rawHeader !== -1 ? (row[ci.rawHeader] || '').toString().trim() : '');
        if (rawHeader === '') continue;

        var entry = {
            rawHeader: rawHeader,
            displayLabel: ci.displayLabel !== -1 ? (row[ci.displayLabel] || '').toString().trim() : rawHeader,
            reportHeader: ci.reportHeader !== -1 ? (row[ci.reportHeader] || '').toString().trim() : '',
            fieldGroup: ci.fieldGroup !== -1 ? (row[ci.fieldGroup] || '').toString().trim() : 'Other',
            dataType: ci.dataType !== -1 ? (row[ci.dataType] || '').toString().trim() : 'text',
            formFieldId: ci.formFieldId !== -1 ? (row[ci.formFieldId] || '').toString().trim() : '',
            svFieldId: ci.svFieldId !== -1 ? (row[ci.svFieldId] || '').toString().trim() : '',
            portalVisibility: ci.portalVisibility !== -1 ? (row[ci.portalVisibility] || '').toString().trim() : '',
            searchable: ci.searchable !== -1 ? (row[ci.searchable] || '').toString().trim().toUpperCase() === 'Y' : false,
            required: ci.required !== -1 ? (row[ci.required] || '').toString().trim().toUpperCase() === 'Y' : false,
            colConstant: ci.colConstant !== -1 ? (row[ci.colConstant] || '').toString().trim() : '',
            notes: ci.notes !== -1 ? (row[ci.notes] || '').toString().trim() : '',
            editable: ci.editable !== -1 ? (row[ci.editable] || '').toString().trim().toUpperCase() === 'Y' : false,
            formSection: ci.formSection !== -1 ? (row[ci.formSection] || '').toString().trim() : '',
            lookupSource: ci.lookupSource !== -1 ? (row[ci.lookupSource] || '').toString().trim() : '',
            previousHeaderName: ci.previousHeaderName !== -1 ? (row[ci.previousHeaderName] || '').toString().trim() : ''
        };

        result.allFields.push(entry);
        result.byRawHeader[rawHeader] = entry;

        // Display label reverse lookup
        if (entry.displayLabel && entry.displayLabel !== rawHeader) {
            result.byDisplayLabel[entry.displayLabel] = rawHeader;
        }
        // Also map raw header to itself for backward compatibility
        result.byDisplayLabel[rawHeader] = rawHeader;

        // Form field ID lookup (intake form)
        if (entry.formFieldId) {
            result.byFormFieldId[entry.formFieldId] = rawHeader;
        }

        // SV Field ID lookup (Staff/Volunteer Portal HTML element IDs)
        if (entry.svFieldId) {
            result.bySvFieldId[entry.svFieldId] = rawHeader;
        }

        // COL constant lookup
        if (entry.colConstant) {
            result.byColConstant[entry.colConstant] = rawHeader;
        }

        // Previous header name lookup — used by syncArchiveHeaders_ for rename detection
        if (entry.previousHeaderName) {
            result.byPreviousHeaderName[entry.previousHeaderName] = rawHeader;
        }

        // Group lookup
        var group = entry.fieldGroup;
        if (!result.byGroup[group]) {
            result.byGroup[group] = [];
        }
        result.byGroup[group].push(entry);
    }

    _fieldMapCache = result;
    Logger.log('FieldMapService: Loaded ' + result.allFields.length + ' field definitions from LU_FieldMap (v1.3 — Editable/FormSection/LookupSource/DbColumnName columns active)');
    return result;
}


// ============ DISPLAY LABEL FUNCTIONS ============

/**
 * Get the friendly display label for a raw AM column header.
 * Sheet-driven replacement for the hardcoded FIELD_DISPLAY_MAP in GrantsReportService.
 * Returns the mapped label if one exists, otherwise returns the original name.
 *
 * @param {string} fieldName - Raw AM column header or computed field name
 * @returns {string} Friendly display label
 */
function getFieldDisplayLabelFromMap(fieldName) {
    var map = loadFieldMap();
    var entry = map.byRawHeader[fieldName];
    return entry ? entry.displayLabel : fieldName;
}

/**
 * Get the raw AM column header for a display label.
 * Reverse lookup used by AI reports to resolve Claude's display-label references
 * back to actual column headers.
 *
 * @param {string} displayLabel - Friendly display label
 * @returns {string} Raw AM column header, or the input if no mapping found
 */
function getFieldRawHeader(displayLabel) {
    var map = loadFieldMap();
    return map.byDisplayLabel[displayLabel] || displayLabel;
}

/**
 * Get the report header for a raw AM column header.
 * Falls back to display label, then raw header.
 *
 * @param {string} fieldName - Raw AM column header
 * @returns {string} Report column header
 */
function getFieldReportHeader(fieldName) {
    var map = loadFieldMap();
    var entry = map.byRawHeader[fieldName];
    if (!entry) return fieldName;
    return entry.reportHeader || entry.displayLabel || fieldName;
}

/**
 * Build a complete display-label-to-raw-header map for a set of fields.
 * Used by AI report generation to resolve field references.
 *
 * @param {Array} fieldNames - Array of raw AM header names
 * @returns {Object} { displayLabel: rawHeader, rawHeader: rawHeader }
 */
function buildLabelToRawMap(fieldNames) {
    var map = loadFieldMap();
    var result = {};
    for (var i = 0; i < fieldNames.length; i++) {
        var raw = fieldNames[i];
        var entry = map.byRawHeader[raw];
        var label = entry ? entry.displayLabel : raw;
        result[label] = raw;
        result[raw] = raw; // self-reference for backward compat
    }
    return result;
}

/**
 * Build a parallel array of display labels for a set of raw field names.
 *
 * @param {Array} fieldNames - Array of raw AM header names
 * @returns {Array} Display labels in same order
 */
function buildDisplayLabels(fieldNames) {
    var map = loadFieldMap();
    return fieldNames.map(function (raw) {
        var entry = map.byRawHeader[raw];
        return entry ? entry.displayLabel : raw;
    });
}


// ============ FIELD GROUP FUNCTIONS ============

/**
 * Get fields organized by group, with display labels.
 * Sheet-driven replacement for hardcoded groupDefs in AIReportService.getAMFieldGroups().
 * Only includes fields that actually exist in the current AM headers.
 *
 * @param {Array} amHeaders - Current Applicants_Master headers (trimmed)
 * @returns {Array} [{name: groupName, fields: [{value: rawHeader, label: displayLabel}]}]
 */
function getFieldsByGroup(amHeaders) {
    var map = loadFieldMap();
    var headerSet = {};
    for (var h = 0; h < amHeaders.length; h++) {
        headerSet[amHeaders[h]] = true;
    }

    // Ordered group names (non-computed groups only)
    var groupOrder = ['Identity', 'Contact', 'Address', 'Demographics', 'Age Brackets',
        'Income', 'Service', 'Distribution', 'Referral', 'Testimonial', 'System'];

    var groups = [];
    for (var g = 0; g < groupOrder.length; g++) {
        var groupName = groupOrder[g];
        var entries = map.byGroup[groupName] || [];
        var validFields = [];

        for (var f = 0; f < entries.length; f++) {
            if (headerSet[entries[f].rawHeader]) {
                validFields.push({
                    value: entries[f].rawHeader,
                    label: entries[f].displayLabel
                });
            }
        }

        if (validFields.length > 0) {
            groups.push({ name: groupName, fields: validFields });
        }
    }

    // Collect any AM headers not in the map into "Other"
    var mappedHeaders = {};
    for (var key in map.byRawHeader) {
        mappedHeaders[key] = true;
    }
    var otherFields = amHeaders.filter(function (h) {
        return !mappedHeaders[h] && h !== 'Timestamp' && !h.match(/^\[(?:Males|Females)\s/);
    });
    if (otherFields.length > 0) {
        groups.push({
            name: 'Other',
            fields: otherFields.map(function (f) {
                return { value: f, label: f };
            })
        });
    }

    return groups;
}

/**
 * Get computed fields (calculated at report time, not in AM).
 *
 * @returns {Array} [{value: fieldName, label: fieldName}]
 */
function getComputedFields() {
    var map = loadFieldMap();
    var entries = map.byGroup['Computed'] || [];
    return entries.map(function (e) {
        return { value: e.rawHeader, label: e.displayLabel };
    });
}

/**
 * Get summary computed fields (appear in Summary section only).
 *
 * @returns {Array} [{value: fieldName, label: fieldName}]
 */
function getSummaryFields() {
    var map = loadFieldMap();
    var entries = map.byGroup['Summary'] || [];
    return entries.map(function (e) {
        return { value: e.rawHeader, label: e.displayLabel };
    });
}


// ============ INTAKE / FORM MAPPING ============

/**
 * Get the intake form field mapping (formFieldId → raw AM header).
 * Sheet-driven replacement for hardcoded getIntakeFieldMapping() in IntakeService.
 *
 * @returns {Object} { formFieldId: rawAMHeader }
 */
function getIntakeFieldMappingFromMap() {
    var map = loadFieldMap();
    return Object.assign({}, map.byFormFieldId);
}


// ============ PORTAL / SEARCH / REQUIRED LOOKUPS ============

/**
 * Get fields visible in a specific portal.
 *
 * @param {string} portalCode - Portal code: AI, SV, AP, PP
 * @returns {Array} [{rawHeader, displayLabel, dataType, ...}]
 */
function getFieldsByPortal(portalCode) {
    var map = loadFieldMap();
    var code = portalCode.toUpperCase();
    return map.allFields.filter(function (entry) {
        return entry.portalVisibility.toUpperCase().indexOf(code) !== -1;
    });
}

/**
 * Get fields marked as searchable.
 *
 * @returns {Array} Array of raw header names
 */
function getSearchableFields() {
    var map = loadFieldMap();
    return map.allFields.filter(function (e) { return e.searchable; })
        .map(function (e) { return e.rawHeader; });
}

/**
 * Get fields marked as required for intake.
 *
 * @returns {Array} Array of raw header names
 */
function getRequiredFields() {
    var map = loadFieldMap();
    return map.allFields.filter(function (e) { return e.required; })
        .map(function (e) { return e.rawHeader; });
}

/**
 * Get the data type for a field.
 *
 * @param {string} fieldName - Raw AM column header
 * @returns {string} Data type (text, date, number, dropdown, multiselect, textarea)
 */
function getFieldDataType(fieldName) {
    var map = loadFieldMap();
    var entry = map.byRawHeader[fieldName];
    return entry ? entry.dataType : 'text';
}


// ============ STAFF PORTAL SECTION DEFINITIONS ============

/**
 * Canonical configuration for Staff Portal form sections.
 * sectionKey values must match Form Section column values in LU_FieldMap.
 * @type {Object}
 */
var STAFF_SECTION_CONFIG = {
    clientInfo: { label: 'Client Information (Read Only)', readOnly: true },
    staffEntry: { label: 'Staff Entry', readOnly: false },
    scheduling: { label: 'Distribution Scheduling', readOnly: false },
    completion: { label: 'Distribution Completion', readOnly: false },
    approvals: { label: 'Approvals', readOnly: false },
    adminNotes: { label: 'Admin Notes', readOnly: false }
};

/**
 * Returns raw AM column headers where Editable=Y in LU_FieldMap.
 * Sheet-driven replacement for the hardcoded EDITABLE_FIELDS array
 * in RequestService. Resolves RequestService TODO Item #7.
 *
 * @returns {string[]} Array of raw AM column header names marked Editable=Y
 */
function getEditableFields() {
    var map = loadFieldMap();
    return map.allFields
        .filter(function (e) { return e.editable; })
        .map(function (e) { return e.rawHeader; });
}

/**
 * Builds the complete Staff Portal section/field/mapping data from LU_FieldMap.
 * Sheet-driven replacement for getFieldDefinitions() and EDITABLE_FIELDS in
 * RequestService. Resolves RequestService TODOs #6 and #7.
 *
 * sections      — ordered array of form sections (mirrors old getFieldDefinitions() shape)
 * fieldMap      — { formFieldId: rawHeader } replaces hardcoded FIELD_MAP in SV portal
 * editableFields — string[] of editable raw AM headers
 * dropdownFields — { formFieldId: lookupSource } drives server-side populateDropdowns() in SV
 *
 * @returns {Object} { sections, fieldMap, editableFields, dropdownFields }
 */
function getStaffPortalSections() {
    var map = loadFieldMap();

    var sectionFieldsMap = {};  // sectionKey → field entry array
    var fieldMap = {};  // svFieldId → rawHeader
    var editableFields = [];  // rawHeaders where Editable=Y
    var dropdownFields = {};  // svFieldId → lookupSource (selects only)

    for (var i = 0; i < map.allFields.length; i++) {
        var entry = map.allFields[i];

        // Build editableFields list
        if (entry.editable) {
            editableFields.push(entry.rawHeader);
        }

        // Build fieldMap and dropdownFields from SV Field ID (HTML element IDs)
        // svFieldId is distinct from formFieldId which is intake-form-only
        if (entry.svFieldId) {
            fieldMap[entry.svFieldId] = entry.rawHeader;
            var inputType = _normalizeInputType(entry.dataType);
            if (inputType === 'select' && entry.lookupSource) {
                dropdownFields[entry.svFieldId] = entry.lookupSource;
            }
        }

        // Group fields by Form Section
        if (entry.formSection) {
            if (!sectionFieldsMap[entry.formSection]) {
                sectionFieldsMap[entry.formSection] = [];
            }
            sectionFieldsMap[entry.formSection].push({
                name: entry.rawHeader,
                label: entry.displayLabel || entry.rawHeader,
                type: _normalizeInputType(entry.dataType),
                lookup: entry.lookupSource || '',
                svFieldId: entry.svFieldId || ''
            });
        }
    }

    // Build ordered sections array
    var sectionOrder = ['clientInfo', 'staffEntry', 'scheduling', 'completion', 'approvals', 'adminNotes'];
    var sections = [];
    for (var s = 0; s < sectionOrder.length; s++) {
        var key = sectionOrder[s];
        if (sectionFieldsMap[key]) {
            var cfg = STAFF_SECTION_CONFIG[key] || { label: key, readOnly: false };
            sections.push({
                sectionKey: key,
                label: cfg.label,
                readOnly: cfg.readOnly || false,
                fields: sectionFieldsMap[key]
            });
        }
    }

    // Diagnostic: warn if fieldMap came up empty so the cause is visible in Logs
    if (Object.keys(fieldMap).length === 0) {
        var svColPresent = (loadFieldMap().allFields.some(function (e) { return e.svFieldId !== ''; }));
        if (!svColPresent) {
            Logger.log('FieldMapService WARNING: getStaffPortalSections() produced an empty fieldMap. ' +
                'The "SV Field ID" column appears to be absent or unpopulated in LU_FieldMap. ' +
                'All editable SV portal fields will be blank until SV Field ID values are added.');
        } else {
            Logger.log('FieldMapService WARNING: getStaffPortalSections() produced an empty fieldMap. ' +
                'SV Field ID values exist but no fields matched. Check Form Section column values.');
        }
    }

    return {
        sections: sections,
        fieldMap: fieldMap,
        editableFields: editableFields,
        dropdownFields: dropdownFields
    };
}

/**
 * Normalizes a LU_FieldMap Data Type value to the HTML input type
 * used in the Staff Portal form.
 * dropdown/select/multiselect → 'select'
 * textarea → 'textarea'; date → 'date'; number → 'number'; else → 'text'
 * @private
 * @param {string} dataType - Raw Data Type value from LU_FieldMap
 * @returns {string} Normalized HTML input type string
 */
function _normalizeInputType(dataType) {
    var dt = (dataType || '').toLowerCase().trim();
    if (dt === 'dropdown' || dt === 'select' || dt === 'multiselect') return 'select';
    if (dt === 'textarea') return 'textarea';
    if (dt === 'date') return 'date';
    if (dt === 'number') return 'number';
    return 'text';
}


// ============ BACKWARD COMPATIBILITY ============

/**
 * Get raw AM header by legacy COL_ constant name.
 * Supports gradual migration from COL_INCOME, COL_USED_BEFORE, etc.
 *
 * @param {string} constantName - Legacy constant name, e.g. "COL_INCOME"
 * @returns {string} Raw AM column header, or empty string if not found
 */
function getCOLConstant(constantName) {
    var map = loadFieldMap();
    return map.byColConstant[constantName] || '';
}

/**
 * Clear the field map cache. Call when LU_FieldMap is edited.
 * Typically invoked by the onEdit trigger for G2N_Lookups.
 */
function clearFieldMapCache() {
    _fieldMapCache = null;
    Logger.log('FieldMapService: Cache cleared');
}


// ============ FALLBACK (for when LU_FieldMap sheet doesn't exist yet) ============

/**
 * Build a fallback field map from the existing hardcoded FIELD_DISPLAY_MAP.
 * Used during migration before LU_FieldMap sheet is created.
 * @private
 * @returns {Object} Same structure as loadFieldMap()
 */
function _buildFallbackFieldMap() {
    // Use existing FIELD_DISPLAY_MAP if available
    var hardcoded = {};
    if (typeof FIELD_DISPLAY_MAP !== 'undefined') {
        hardcoded = FIELD_DISPLAY_MAP;
    }

    var result = {
        byRawHeader: {},
        byDisplayLabel: {},
        byFormFieldId: {},
        byColConstant: {},
        byGroup: {},
        allFields: []
    };

    for (var rawHeader in hardcoded) {
        var displayLabel = hardcoded[rawHeader];
        var entry = {
            rawHeader: rawHeader,
            displayLabel: displayLabel,
            reportHeader: '',
            fieldGroup: 'Other',
            dataType: 'text',
            formFieldId: '',
            portalVisibility: 'AI,SV,AP',
            searchable: false,
            required: false,
            colConstant: '',
            notes: 'Fallback from hardcoded FIELD_DISPLAY_MAP',
            editable: false,
            formSection: '',
            lookupSource: '',
            svFieldId: '',
            previousHeaderName: ''
        };
        result.allFields.push(entry);
        result.byRawHeader[rawHeader] = entry;
        result.byDisplayLabel[displayLabel] = rawHeader;
        result.byDisplayLabel[rawHeader] = rawHeader;
    }

    Logger.log('FieldMapService: Using fallback hardcoded map (' + result.allFields.length + ' entries)');
    return result;
}
