/**
 * SearchService.gs
 * Search and retrieval operations for Applicants_Master records
 * Supports search by Record ID, Original Form ID, and Name+Date.
 * Provides filtered record lists for dashboard, status, and distribution code views.
 * v2.1 - Corrected field references to match Applicants_Master headers
 * v2.2 - Adopted shared trimHeaders() utility (#11).
 *         Removed redundant trim in rowToRecord() — callers pass trimmed headers (#16).
 *         Moved JSDoc comments out of searchRecords() function body (#17).
 * v2.3 - Adopted CONFIG.TIMEZONE (#8).
 * v2.4 - Integrated FieldMapService: all AM column lookups use resolveAMField_()
 *         via SharedUtils so column names are driven by LU_FieldMap.
 * v2.5 - Archive fallback: when AM search yields notFound, searchArchiveSheets_()
 *         scans G2N_Archive and all G2N_Archive_YYYY workbooks. For nameDate,
 *         archive hits are appended to AM results. Archive records carry
 *         _archived:true and _archiveSource:workbookName; rowIndex is null.
 */

/**
 * Searches Applicants_Master by Record ID, Original Form ID, or Name+Date.
 * Falls through to archive workbooks when AM yields no results.
 * @param {Object} criteria - { searchType: 'id'|'formId'|'nameDate', ... }
 * @returns {Object} { success, record|results, rowIndex }
 */
function searchRecords(criteria) {
    try {
        const sheet = getMasterSheet();
        if (!sheet) {
            return { success: false, error: 'Master sheet not found' };
        }

        const data = sheet.getDataRange().getValues();
        const headers = trimHeaders(data[0]);

        // Get column indices via FieldMapService for resilience to header renames
        const colIndices = {
            id: headers.indexOf(resolveAMField_('ID')),
            firstName: headers.indexOf(resolveAMField_('First Name')),
            lastName: headers.indexOf(resolveAMField_('Last Name')),
            formId: headers.indexOf(resolveAMField_('Original Form ID')),
            requestDate: headers.indexOf(resolveAMField_('Request Date')),
            phone: headers.indexOf(resolveAMField_('Phone Number')),
            email: headers.indexOf(resolveAMField_('Email')),
            city: headers.indexOf(resolveAMField_('City')),
            state: headers.indexOf(resolveAMField_('State')),
            serviceStatus: headers.indexOf(resolveAMField_('Service Status'))
        };

        const searchType = criteria.searchType || 'id';

        // Search by Record ID (exact match)
        if (searchType === 'id' && criteria.id) {
            const searchId = parseInt(criteria.id);
            for (let i = 1; i < data.length; i++) {
                if (parseInt(data[i][colIndices.id]) === searchId) {
                    return {
                        success: true,
                        record: rowToRecord(headers, data[i]),
                        rowIndex: i + 1 // Convert to 1-based for sheet
                    };
                }
            }
            // v2.5: Not in AM — search archives
            var archiveHit = searchArchiveSheets_(criteria);
            if (archiveHit) return archiveHit;
            return { success: false, notFound: true, error: 'Record ID not found' };
        }

        // Search by Original Form ID (exact match — returns highest numeric ID when duplicates exist)
        if (searchType === 'formId' && criteria.formId) {
            // v2.7: Guard — if resolveAMField_ didn't find the column, try a direct
            // case-insensitive header scan as fallback before giving up.
            var fIdColIdx = colIndices.formId;
            if (fIdColIdx === -1) {
                var lowerHeaders = headers.map(function (h) { return h.toLowerCase(); });
                fIdColIdx = lowerHeaders.indexOf('original form id');
            }
            if (fIdColIdx === -1) {
                Logger.log('SearchService: Original Form ID column not found in AM headers');
                return { success: false, notFound: true, error: 'Form ID column not found in master sheet' };
            }

            const searchFormId = criteria.formId.toString().toLowerCase().trim();
            // v2.8: Collect all matches and return the one with the highest numeric ID
            // (most recent record) so re-submissions don't return stale older rows.
            var bestMatch = null;
            var bestId = -1;
            for (let i = 1; i < data.length; i++) {
                const formId = (data[i][fIdColIdx] || '').toString().toLowerCase().trim();
                if (formId === searchFormId) {
                    const rowNumId = parseInt(data[i][colIndices.id]) || 0;
                    if (rowNumId > bestId) {
                        bestId = rowNumId;
                        bestMatch = { record: rowToRecord(headers, data[i]), rowIndex: i + 1 };
                    }
                }
            }
            if (bestMatch) {
                return { success: true, record: bestMatch.record, rowIndex: bestMatch.rowIndex };
            }
            // v2.5: Not in AM — search archives
            var archiveHit = searchArchiveSheets_(criteria);
            if (archiveHit) return archiveHit;
            return { success: false, notFound: true, error: 'Form ID not found' };
        }

        // Search by Last Name, First Name, and optional Request Date
        if (searchType === 'nameDate') {
            const firstName = (criteria.firstName || '').toLowerCase().trim();
            const lastName = (criteria.lastName || '').toLowerCase().trim();
            const searchDate = criteria.date || '';

            const results = [];

            for (let i = 1; i < data.length; i++) {
                const rowFirstName = (data[i][colIndices.firstName] || '').toString().toLowerCase();
                const rowLastName = (data[i][colIndices.lastName] || '').toString().toLowerCase();

                let nameMatch = false;

                // Check name match
                if (firstName && lastName) {
                    nameMatch = rowFirstName.includes(firstName) && rowLastName.includes(lastName);
                } else if (firstName) {
                    nameMatch = rowFirstName.includes(firstName);
                } else if (lastName) {
                    nameMatch = rowLastName.includes(lastName);
                }

                // Check date match if provided
                let dateMatch = true;
                if (searchDate && nameMatch) {
                    const rowDate = data[i][colIndices.requestDate];
                    if (rowDate) {
                        let rowDateStr = '';
                        if (rowDate instanceof Date) {
                            rowDateStr = Utilities.formatDate(rowDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
                        } else {
                            const parts = rowDate.toString().split('/');
                            if (parts.length === 3) {
                                const year = parts[2].length === 2 ? '20' + parts[2] : parts[2];
                                rowDateStr = year + '-' + parts[0].padStart(2, '0') + '-' + parts[1].padStart(2, '0');
                            }
                        }
                        dateMatch = rowDateStr === searchDate;
                    } else {
                        dateMatch = false;
                    }
                }

                if (nameMatch && dateMatch) {
                    results.push({
                        record: rowToRecord(headers, data[i]),
                        rowIndex: i + 1
                    });
                }
            }

            // v2.5: Also search archives and append results
            var archiveResults = searchArchiveSheets_(criteria);
            if (archiveResults && archiveResults.results) {
                results.push.apply(results, archiveResults.results);
            } else if (archiveResults && archiveResults.record) {
                results.push({ record: archiveResults.record, rowIndex: null });
            }

            if (results.length === 0) {
                return { success: false, notFound: true, error: 'No matching records found' };
            }

            if (results.length === 1) {
                return {
                    success: true,
                    record: results[0].record,
                    rowIndex: results[0].rowIndex
                };
            }

            return {
                success: true,
                results: results
            };
        }

        return { success: false, error: 'Invalid search criteria' };

    } catch (error) {
        Logger.log('Search error: ' + error.message);
        return { success: false, error: 'Search failed: ' + error.message };
    }
}

/**
 * Searches all archive workbooks (G2N_Archive + G2N_Archive_YYYY) for a record.
 * Opens each workbook's 'Archive' sheet and applies the same criteria logic.
 * Results carry _archived:true and _archiveSource:<workbookName>; rowIndex is null.
 * v2.5 - Added as part of archive-fallback search
 * @param {Object} criteria - Same criteria object as searchRecords()
 * @returns {Object|null} searchRecords-compatible result, or null if nothing found
 */
function searchArchiveSheets_(criteria) {
    var searchType = criteria.searchType || 'id';
    var allResults = [];

    // Collect all archive workbooks to search
    var workbooksToSearch = [];
    try {
        if (CONFIG.ARCHIVE_WORKBOOK_ID) {
            workbooksToSearch.push({
                workbook: SpreadsheetApp.openById(CONFIG.ARCHIVE_WORKBOOK_ID),
                name: 'G2N_Archive'
            });
        }
    } catch (e) {
        Logger.log('searchArchiveSheets_: G2N_Archive open error: ' + e.message);
    }
    try {
        if (CONFIG.ARCHIVES_BACKUPS_FOLDER_ID) {
            var folder = DriveApp.getFolderById(CONFIG.ARCHIVES_BACKUPS_FOLDER_ID);
            var files = folder.getFiles();
            while (files.hasNext()) {
                var file = files.next();
                if (file.getMimeType() !== MimeType.GOOGLE_SHEETS) continue;
                if (/^G2N_Archive(_\d{4})?$/.test(file.getName())) {
                    try {
                        workbooksToSearch.push({
                            workbook: SpreadsheetApp.openById(file.getId()),
                            name: file.getName()
                        });
                    } catch (we) {
                        Logger.log('searchArchiveSheets_: cannot open ' + file.getName() + ': ' + we.message);
                    }
                }
            }
        }
    } catch (e) {
        Logger.log('searchArchiveSheets_: folder scan error: ' + e.message);
    }

    for (var w = 0; w < workbooksToSearch.length; w++) {
        var wb = workbooksToSearch[w];
        try {
            var sheet = wb.workbook.getSheetByName('Archive');
            if (!sheet) continue;
            var data = sheet.getDataRange().getValues();
            if (data.length < 2) continue;
            var headers = trimHeaders(data[0]);

            var idCol = headers.indexOf(resolveAMField_('ID'));
            var firstNameCol = headers.indexOf(resolveAMField_('First Name'));
            var lastNameCol = headers.indexOf(resolveAMField_('Last Name'));
            var formIdCol = headers.indexOf(resolveAMField_('Original Form ID'));
            // v2.7: fallback header scan if resolveAMField_ doesn't find formId
            if (formIdCol === -1) {
                var lh = headers.map(function (h) { return h.toLowerCase(); });
                formIdCol = lh.indexOf('original form id');
            }
            var reqDateCol = headers.indexOf(resolveAMField_('Request Date'));

            for (var i = 1; i < data.length; i++) {
                var matched = false;

                if (searchType === 'id' && criteria.id) {
                    matched = (parseInt(data[i][idCol]) === parseInt(criteria.id));

                } else if (searchType === 'formId' && criteria.formId) {
                    if (formIdCol === -1) { matched = false; } // column absent in this archive sheet — skip
                    else {
                        var rowFormId = (data[i][formIdCol] || '').toString().toLowerCase().trim();
                        matched = (rowFormId === criteria.formId.toString().toLowerCase().trim());
                    }

                } else if (searchType === 'nameDate') {
                    var fn = (criteria.firstName || '').toLowerCase().trim();
                    var ln = (criteria.lastName || '').toLowerCase().trim();
                    var rowFn = (data[i][firstNameCol] || '').toString().toLowerCase();
                    var rowLn = (data[i][lastNameCol] || '').toString().toLowerCase();

                    var nameMatch = false;
                    if (fn && ln) nameMatch = rowFn.includes(fn) && rowLn.includes(ln);
                    else if (fn) nameMatch = rowFn.includes(fn);
                    else if (ln) nameMatch = rowLn.includes(ln);

                    var dateMatch = true;
                    if (criteria.date && nameMatch) {
                        var rowDate = data[i][reqDateCol];
                        if (rowDate) {
                            var rowDateStr = '';
                            if (rowDate instanceof Date) {
                                rowDateStr = Utilities.formatDate(rowDate, CONFIG.TIMEZONE, 'yyyy-MM-dd');
                            } else {
                                var parts = rowDate.toString().split('/');
                                if (parts.length === 3) {
                                    var yr = parts[2].length === 2 ? '20' + parts[2] : parts[2];
                                    rowDateStr = yr + '-' + parts[0].padStart(2, '0') + '-' + parts[1].padStart(2, '0');
                                }
                            }
                            dateMatch = (rowDateStr === criteria.date);
                        } else {
                            dateMatch = false;
                        }
                    }
                    matched = nameMatch && dateMatch;
                }

                if (matched) {
                    var rec = rowToRecord(headers, data[i]);
                    rec['_archived'] = true;
                    rec['_archiveSource'] = wb.name;
                    allResults.push({ record: rec, rowIndex: null });
                }
            }
        } catch (sheetErr) {
            Logger.log('searchArchiveSheets_: error reading ' + wb.name + ': ' + sheetErr.message);
        }
    }

    if (allResults.length === 0) return null;
    if (allResults.length === 1) {
        return { success: true, record: allResults[0].record, rowIndex: null };
    }
    return { success: true, results: allResults };
}
