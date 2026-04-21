/**
 * IntakeService.gs
 * Handles Applicant Intake form submissions from ApplicantIntake.html
 * Creates new records in Applicants_Master with auto-generated IDs,
 * field validation, date formatting, event mode handling, and Baby Box support.
 * v3.3 - Use Signature Date as Request Date also
 * v3.4 - Added addressValidity field mapping to save verification confidence to AM
 *       - Added logIntakeSession() to record public/staff/event form opens in AuditLog
 *       - Fixed duplicate appendRow causing 2 rows per submission
 * v3.5 - Fixed submitIntakeFormWithValidation() to accept and pass eventInfo parameter.
 *       - Adopted shared utilities: trimHeaders() for header reads, htmlDateToSheet()
 *         for date conversions (replaces inline pattern duplication).
 * v3.6 - Set "Entered By" to "Applicant" for public intake submissions.
 *       - Auto-detect event mode from signature date: if no eventInfo passed (or not
 *         active), checks the submitted signatureDate against LU_EventInfo server-side.
 *         Sets outdoorEvent=Yes, boxCode, fundingSource, Received Product Code 1 when matched.
 * v3.7 - Fixed appendRow race condition.
 * v3.8 - Non-event intake now defaults Service Status to "Open". Applies to
 *         public AI submissions and SV staff-mode submissions. Event mode
 *         continues to set "Picked Up" as before.
 * v3.9 - Added getApplicantForIntake(recordId, archiveSource): loads an existing
 *         AM record mapped to AI form field IDs for edit-mode pre-population.
 *         AI detects ?recordId= param, pre-fills all fields, and calls
 *         updateIntakeRecord() on save instead of submitIntakeForm().
 */

/**
 * Submit a new applicant intake form
 * Maps form fields to Applicants_Master columns, generates next ID,
 * handles event mode (sets status to "Picked Up"), converts date formats,
 * and strips emoji characters before writing.
 * @param {Object} formData - All form field values from ApplicantIntake.html
 * @param {Object} eventInfo - Optional event info { isActive, boxCode, fundingSource }
 * @returns {Object} { success: boolean, recordId: string, message: string }
 */
function submitIntakeForm(formData, eventInfo) {
    try {
        const sheet = getMasterSheet();
        if (!sheet) {
            return { success: false, error: 'Master sheet not found' };
        }

        const rawHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
        const headers = trimHeaders(rawHeaders);
        const nextId = getNextId();

        // Create new row array
        const newRow = new Array(headers.length).fill('');

        // Set ID
        newRow[0] = nextId;

        // Set Timestamp
        const timestampCol = headers.indexOf('Timestamp');
        if (timestampCol !== -1) {
            newRow[timestampCol] = new Date();
        }

        // Map form fields to columns
        const fieldMapping = getIntakeFieldMapping();

        for (const formField in formData) {
            const sheetColumn = fieldMapping[formField] || formField;
            const colIndex = headers.indexOf(sheetColumn);

            if (colIndex !== -1) {
                let value = formData[formField];

                // Handle special transformations
                if (formField === 'childrenNewbornTo2') {
                    // Convert Yes/No to X/blank for Baby Box indicator
                    value = (value === 'Yes' || value === true) ? 'X' : '';
                }

                // Convert date fields from YYYY-MM-DD to MM/DD/YYYY
                if (formField === 'formDate' || formField === 'signatureDate') {
                    value = htmlDateToSheet(value);
                }

                // Handle multi-select fields (arrays)
                if (Array.isArray(value)) {
                    value = value.join(', ');
                }

                newRow[colIndex] = value;
            }
        }

        // Set fields based on event mode
        const today = new Date();
        // v4.1: Use Utilities.formatDate with CONFIG.TIMEZONE to avoid UTC offset bug.
        // bare getMonth()/getDate() returns UTC date, which is -1 day after ~6pm Chicago time.
        const todayFormatted = Utilities.formatDate(today, CONFIG.TIMEZONE, 'M/d/yyyy');

        // Calculate Next Service Availability Date (today + 90 days)
        const next90 = new Date(today.getTime() + (90 * 24 * 60 * 60 * 1000));
        const next90Formatted = Utilities.formatDate(next90, CONFIG.TIMEZONE, 'M/d/yyyy');

        // v3.6: Auto-detect event from signature date if not already in event mode
        // v4.3: Also pass current time for time-window check against LU_EventInfo
        if (!eventInfo || !eventInfo.isActive) {
            var sigDateStr = formData.signatureDate || '';
            if (sigDateStr) {
                // Build current time string (HH:MM) in configured timezone
                var nowForTime = new Date();
                var nowTimeStr = Utilities.formatDate(nowForTime, CONFIG.TIMEZONE, 'HH:mm');
                var detectedEvent = getEventInfoForDate(sigDateStr, nowTimeStr);
                if (detectedEvent && detectedEvent.isActive) {
                    eventInfo = detectedEvent;
                    setColumnValue(newRow, headers,
                        'Are you completing this form at a Giving to the Nations OUTDOOR event?', 'Yes');
                }
            }
        }

        if (eventInfo && eventInfo.isActive) {
            // Event mode - set all event-related fields
            setColumnValue(newRow, headers, 'Service Status', 'Picked Up');
            setColumnValue(newRow, headers, 'Request Type', 'Event');
            setColumnValue(newRow, headers, 'Box Code', eventInfo.boxCode || '');
            setColumnValue(newRow, headers, 'Scheduled Box Code 1', eventInfo.boxCode || '');
            setColumnValue(newRow, headers, 'Received Product Code 1', eventInfo.boxCode || '');
            // v4.3: Use Description from LU_FundingSources, not the raw Code
            var fundingSrcValue = eventInfo.fundingSourceDescription ||
                eventInfo.fundingSource || '';
            setColumnValue(newRow, headers, 'Funding Source', fundingSrcValue);
            setColumnValue(newRow, headers, 'Last Date Served', todayFormatted);
            setColumnValue(newRow, headers, 'Final Service Contact Date', todayFormatted);
            setColumnValue(newRow, headers, 'Next Service Availability Date', next90Formatted);
        } else {
            // Non-event mode — set Request Type to Direct and Service Status to Open
            setColumnValue(newRow, headers, 'Request Type', 'Direct');
            setColumnValue(newRow, headers, 'Service Status', 'Open');
        }

        // v3.6: Default "Entered By" to "Applicant" for public intake submissions
        // v4.2: In staff mode, formData.enteredBy contains the staff member's code —
        //        use it when provided so Entered By reflects who submitted on behalf of applicant
        var enteredBy = (formData.enteredBy && formData.enteredBy.toString().trim())
            ? formData.enteredBy.toString().trim()
            : 'Applicant';
        setColumnValue(newRow, headers,
            'Entered By (Who entered the data into the database?)', enteredBy);

        // Write row at a deterministic position, then format
        // v3.7 - Replaced appendRow with setValues to prevent race condition
        const newRowNum = sheet.getLastRow() + 1;
        sheet.getRange(newRowNum, 1, 1, headers.length).setValues([newRow]);
        sheet.getRange(newRowNum, 1, 1, 1).setNumberFormat('0');
        sheet.getRange(newRowNum, 1, 1, headers.length).setBackground(null).setFontColor(null).setFontWeight(null);

        // Log the creation
        const logMsg = eventInfo && eventInfo.isActive
            ? 'New applicant submitted via Intake Portal (Event Mode)'
            : 'New applicant submitted via Intake Portal';
        logAudit('CREATE', nextId, logMsg);

        return {
            success: true,
            message: 'Application submitted successfully',
            recordId: nextId
        };

    } catch (error) {
        Logger.log('Intake submission error: ' + error.message);
        return { success: false, error: 'Submission failed: ' + error.message };
    }
}

/**
 * Helper function to set a column value in the row array
 */
function setColumnValue(rowArray, headers, columnName, value) {
    const colIndex = headers.indexOf(columnName);
    if (colIndex !== -1) {
        rowArray[colIndex] = value;
    }
}

/**
 * Get field mapping from HTML form field IDs to Applicants_Master column headers.
 * v3.8: Delegates to FieldMapService.getIntakeFieldMappingFromMap() which reads
 * from LU_FieldMap sheet. Falls back to hardcoded mapping if sheet unavailable.
 * @returns {Object} { formFieldId: rawAMColumnHeader }
 */
function getIntakeFieldMapping() {
    var mapping = getIntakeFieldMappingFromMap();

    // If FieldMapService returned entries, use them
    if (mapping && Object.keys(mapping).length > 0) {
        return mapping;
    }

    // Fallback: hardcoded mapping (retained for migration safety)
    Logger.log('IntakeService: LU_FieldMap not available, using hardcoded fallback');
    return _getIntakeFieldMappingFallback();
}

// TODO: Item #13 -- Remove _getIntakeFieldMappingFallback() after 2-4 weeks
// of stable LU_FieldMap production use
/**
 * Hardcoded intake field mapping fallback.
 * Retained during migration period. Remove once LU_FieldMap is confirmed stable.
 * @private
 * @returns {Object} { formFieldId: rawAMColumnHeader }
 */
function _getIntakeFieldMappingFallback() {
    return {
        // Section 1: How did you hear about us
        'outdoorEvent': 'Are you completing this form at a Giving to the Nations OUTDOOR event?',
        'learnedHow': 'How did you learn about our program?',
        'referralInfo': 'More information about the person or organization that referred you.',
        'referrerTitle': 'What is the title or position of the person who referred you?',
        'referrerName': "What is the person's name who referred you?",
        'additionalInfo': 'Please tell us anything else you would like us to know about how you heard about Giving to the Nations or our program.',
        'usedServicesBefore': 'Have you used our services before?',
        'currentlyHomeless': 'Are you currently homeless?',

        // Section 2: Applicant Information
        'firstName': 'First Name',
        'lastName': 'Last Name',
        'streetAddress': 'Street Address',
        'apartment': 'Apartment #, Upper, Lower, or Lot #',
        'city': 'City',
        'state': 'State',
        'zipCode': 'Zip Code',
        'county': 'County',
        'addressValidity': 'Address Validity',
        'addressType': 'Address type?',
        'phoneNumber': 'Phone Number',
        'phoneType': 'Phone type?',
        'email': 'Email',
        'bestContact': 'Best contact method (What is the best way to get ahold of you?)',

        // Section 3: Household Demographics
        'malesUnder5': '[Males under 5]',
        'femalesUnder5': '[Females under 5]',
        'males5to9': '[Males 5-9]',
        'females5to9': '[Females 5-9]',
        'males10to14': '[Males 10-14]',
        'females10to14': '[Females 10-14]',
        'males15to17': '[Males 15-17]',
        'females15to17': '[Females 15-17]',
        'males18to24': '[Males 18-24]',
        'females18to24': '[Females 18-24]',
        'males25to34': '[Males 25-34]',
        'females25to34': '[Females 25-34]',
        'males35to44': '[Males 35-44]',
        'females35to44': '[Females 35-44]',
        'males45to54': '[Males 45-54]',
        'females45to54': '[Females 45-54]',
        'males55to64': '[Males 55-64]',
        'females55to64': '[Females 55-64]',
        'males65to84': '[Males 65-84]',
        'females65to84': '[Females 65-84]',
        'males85plus': '[Males 85 and Over]',
        'females85plus': '[Females 85 and Over]',
        'childrenNewbornTo2': 'Take Baby Box?',

        // Section 4: About You
        'militaryStatus': 'Military Status',
        'racialCategory': 'Please Select Your Racial Category',
        'ethnicCategory': 'Please Select Your Ethnic Category',
        'assistanceReceiving': 'Are you receiving any assistance? Please select ALL boxes that apply to ANY ASSISTANCE your family is receiving.',
        'currentlyEmployed': 'Are you currently employed?',
        'annualIncome': 'Total annual household income. (How much money does your family make in 1 year?)',

        // Section 5: About contacting
        'emergencySituation': 'Was there an emergency situation which caused you to contact us?',
        'currentSituation': 'Briefly explain your current situation.',
        'howProductsHelp': 'How will receiving personal and home cleaning products help you?',
        'pastProductsHelped': 'If you have received cleaning products from us in the past, how has receiving these products helped you?',
        'mayUseInfo': 'May we use the information you have provided in the 2 questions above about HOW WILL and HOW DID receiving cleaning products help? ONLY the information from these 2 areas will be shared to help us show potential donors your need is real.',

        // Section 7: Acknowledgement
        'applicantSignature': 'Applicant Signature',
        'formDate': "Please enter today's date.",
        'signatureDate': 'Request Date'
    };
}

/**
 * Validates form data server-side
 * Checks: required fields, phone format, email format, zip code format
 * @param {Object} formData - Form field values
 * @returns {Object} { valid: boolean, errors: string[] }
 */
function validateIntakeForm(formData, enforceRequired = true) {
    const errors = [];

    // Phone validation - must be 10 digits if provided
    if (formData.phoneNumber) {
        const phoneDigits = formData.phoneNumber.replace(/\D/g, '');
        if (phoneDigits.length !== 10) {
            errors.push('Phone number must be exactly 10 digits');
        }
    } else if (enforceRequired) {
        errors.push('Phone number is required');
    }

    // Email validation - basic format check if provided
    if (formData.email) {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!emailRegex.test(formData.email)) {
            errors.push('Please enter a valid email address');
        }
    } else if (enforceRequired) {
        errors.push('Email is required');
    }

    // Zip code validation - must be 5 digits if provided
    if (formData.zipCode) {
        const zipDigits = formData.zipCode.replace(/\D/g, '');
        if (zipDigits.length !== 5) {
            errors.push('Zip code must be exactly 5 digits');
        }
    }

    // Required fields check
    if (enforceRequired) {
        const requiredFields = [
            { field: 'firstName', label: 'First Name' },
            { field: 'lastName', label: 'Last Name' },
            { field: 'streetAddress', label: 'Street Address' },
            { field: 'city', label: 'City' },
            { field: 'state', label: 'State' },
            { field: 'zipCode', label: 'Zip Code' },
            { field: 'county', label: 'County' }
        ];

        for (const req of requiredFields) {
            if (!formData[req.field] || formData[req.field].trim() === '') {
                errors.push(req.label + ' is required');
            }
        }
    }

    return {
        valid: errors.length === 0,
        errors: errors
    };
}

/**
 * Submit intake form with validation
 * Validates formData, then delegates to submitIntakeForm with eventInfo
 * v3.5 - Fixed: now accepts and passes eventInfo (was silently dropped)
 * @param {Object} formData - Form field values from ApplicantIntake.html
 * @param {boolean} enforceRequired - Whether to enforce required field checks
 * @param {Object} eventInfo - Optional event info { isActive, boxCode, fundingSource }
 * @returns {Object} { success: boolean, recordId: string, errors: string[] }
 */
function submitIntakeFormWithValidation(formData, enforceRequired, eventInfo) {
    // Default enforceRequired to true if not provided
    if (enforceRequired === undefined || enforceRequired === null) {
        enforceRequired = true;
    }

    // Validate first
    const validation = validateIntakeForm(formData, enforceRequired);
    if (!validation.valid) {
        return {
            success: false,
            error: 'Validation failed',
            errors: validation.errors
        };
    }

    // Submit if valid — pass eventInfo through
    return submitIntakeForm(formData, eventInfo);
}

/**
 * Get intake form for editing (staff use - when creating new from portal)
 * Returns empty form structure with lookups
 */
function getBlankIntakeForm() {
    return {
        lookups: getIntakeLookups(),
        formData: {},
        enforceRequired: false
    };
}

/**
 * Returns lookups + LU_FieldMap-driven dropdownFields for the intake form.
 * Data-driven replacement for hardcoded populateDropdowns() in ApplicantIntake.html.
 * dropdownFields: { htmlElementId: lookupSourceKey } for all AI-visible select fields.
 * v2.0 — added for data-driven dropdown population (mirrors getPortalData() pattern).
 * v2.1 — Fixed portal visibility filter: was 'AP', corrected to 'AI'.
 *         Added Sheets fallback when MySQL getIntakeLookups() returns empty
 *         (bridge unreachable or IAM error) so AI dropdowns always populate.
 */
function getIntakePortalData() {
    var lookups = getIntakeLookups();
    // Build dropdownFields from LU_FieldMap: formFieldId → lookupSource
    // for all fields visible in AI portal with dataType = dropdown.
    // v2.1: corrected filter from 'AP' to 'AI'.
    var dropdownFields = {};
    try {
        var map = loadFieldMap();
        map.allFields.forEach(function (entry) {
            if (entry.formFieldId
                && entry.lookupSource
                && entry.dataType === 'dropdown'
                && entry.portalVisibility.toUpperCase().indexOf('AI') !== -1) {
                dropdownFields[entry.formFieldId] = entry.lookupSource;
            }
        });
    } catch (e) {
        Logger.log('getIntakePortalData: LU_FieldMap load error (non-fatal): ' + e.message);
    }

    return {
        lookups: lookups,
        dropdownFields: dropdownFields
    };
}

/**
 * Log an intake form session start to the AuditLog.
 * Called from ApplicantIntake.html on DOMContentLoaded.
 * For anonymous (public) users, Session.getActiveUser() returns blank,
 * so we capture mode and client-provided metadata instead.
 *
 * @param {Object} sessionInfo - { mode: 'public'|'staff'|'event', userAgent: string }
 * @returns {Object} { success: boolean }
 */
function logIntakeSession(sessionInfo) {
    try {
        var mode = (sessionInfo && sessionInfo.mode) || 'unknown';
        var userAgent = (sessionInfo && sessionInfo.userAgent) || '';
        // Truncate user agent to prevent bloat
        if (userAgent.length > 200) userAgent = userAgent.substring(0, 200);

        var email = '';
        try {
            email = Session.getActiveUser().getEmail() || '';
        } catch (e) {
            // getActiveUser() can throw for anonymous access
        }

        var details = 'Intake form opened | Mode: ' + mode;
        if (email) {
            details += ' | User: ' + email;
        } else {
            details += ' | User: Anonymous';
        }
        if (userAgent) {
            details += ' | UA: ' + userAgent;
        }

        logAudit('INTAKE_SESSION', null, details);
        return { success: true };
    } catch (e) {
        Logger.log('logIntakeSession error: ' + e.message);
        return { success: false, error: e.message };
    }
}

/**
 * Loads an existing AM record mapped to AI form field IDs for edit-mode pre-population.
 * Called by ApplicantIntake.html when opened with ?recordId=XXX from SV or AP.
 * Uses SearchService.searchRecords() so archive fall-through works automatically.
 * Returns formData keyed by AI HTML element IDs so the form can set each field directly.
 * v3.9 - New function.
 * @param {string} recordId - The numeric record ID to load
 * @param {string} archiveSource - Archive workbook name if record is archived (optional)
 * @returns {Object} { success, formData, rowIndex, recordId, archiveSource, isArchived }
 */
function getApplicantForIntake(recordId, archiveSource) {
    try {
        if (!recordId) {
            return { success: false, error: 'No record ID provided.' };
        }

        // Search finds the record in AM or any archive, returns rowIndex and _archiveSource
        var searchResult = searchRecords({ searchType: 'id', recordId: String(recordId) });
        if (!searchResult.success || !searchResult.results || searchResult.results.length === 0) {
            return { success: false, error: 'Record not found: ' + recordId };
        }

        var hit = searchResult.results[0];
        var record = hit.record;
        var rowIndex = hit.rowIndex;
        var foundArchSrc = (record['_archiveSource'] || '').toString();
        var isArchived = !!(foundArchSrc);

        // Build reverse map: AM raw header → AI form field ID
        var fieldMapping = getIntakeFieldMapping();   // { formFieldId: amHeader }
        var reverseMap = {};
        for (var fid in fieldMapping) {
            reverseMap[fieldMapping[fid]] = fid;
        }

        // Build formData: { formFieldId: value } for every field the mapping covers
        var formData = {};
        for (var amHeader in record) {
            // Skip internal keys
            if (amHeader.charAt(0) === '_') continue;

            var formFieldId = reverseMap[amHeader];
            if (!formFieldId) continue;

            var value = record[amHeader];
            if (value === null || value === undefined) {
                value = '';
            } else if (value instanceof Date) {
                // Convert Date → YYYY-MM-DD for HTML date inputs
                try {
                    value = Utilities.formatDate(value, CONFIG.TIMEZONE, 'yyyy-MM-dd');
                } catch (e) { value = ''; }
            } else {
                value = value.toString();
            }

            // Special field transformations
            if (formFieldId === 'childrenNewbornTo2') {
                // AM stores 'X' for yes, '' for no
                value = (value === 'X') ? 'Yes' : 'No';
            }

            // M/D/YYYY date strings → YYYY-MM-DD for date inputs
            if ((formFieldId === 'formDate' || formFieldId === 'signatureDate') && value) {
                var parts = value.split('/');
                if (parts.length === 3) {
                    value = parts[2] + '-' +
                        (parts[0].length === 1 ? '0' : '') + parts[0] + '-' +
                        (parts[1].length === 1 ? '0' : '') + parts[1];
                }
            }

            formData[formFieldId] = value;
        }

        return {
            success: true,
            formData: formData,
            rowIndex: rowIndex,
            recordId: recordId,
            archiveSource: foundArchSrc,
            isArchived: isArchived
        };

    } catch (e) {
        Logger.log('getApplicantForIntake error: ' + e.message);
        return { success: false, error: 'Failed to load record: ' + e.message };
    }
}
