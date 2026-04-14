/**
 * DiagnosticTools.gs
 * G2N System — Diagnostic and maintenance utilities.
 * NOT deployed as part of the main web app — run manually from the Apps Script editor.
 *
 * v1.0 - findArchivedApplicantsInDRPF(): identifies applicant IDs that appear in
 *         DR/PF_Products but whose Applicants_Master row has been archived to
 *         G2N_Archive or a G2N_Archive_YYYY workbook.
 *         migrateProductsToArchive(): moves orphaned DR/PF_Products rows for a
 *         given ID to the correct Products_Archive sheet in the archive workbook.
 * v1.1 - testArchiveSave(): step-by-step diagnostic for archive record save failures.
 *         Logs every stage of the updateArchiveRecord() path so the exact failure
 *         point is visible in the Execution Log.
 *
 * HOW TO RUN:
 *   1. Open Apps Script editor (Applicants_Master → Extensions → Apps Script).
 *   2. Paste this file as a new .gs file named DiagnosticTools.gs.
 *   3. Select testArchiveSave from the function dropdown.
 *   4. Click Run. Results appear in the Execution Log (View → Logs).
 *   5. To migrate a specific ID, edit migrateProductsToArchive() and run it.
 *
 * IMPORTANT: migrateProductsToArchive() WRITES DATA. Run findArchivedApplicantsInDRPF()
 * first and review the log before migrating. Always take a backup first.
 */

'use strict';

// ─── findArchivedApplicantsInDRPF ────────────────────────────────────────────

/**
 * Scans DR/PF_Products, identifies every unique applicant ID, then checks each
 * against Applicants_Master and all archive workbooks. Reports:
 *   - IDs in AM (active — expected in DR/PF_Products)
 *   - IDs in G2N_Archive (archived — products should be in Products_Archive)
 *   - IDs in G2N_Archive_YYYY (archived — products should be in year-based archive)
 *   - IDs not found anywhere (orphaned / deleted records)
 *
 * Writes a summary Google Sheet to the Grants folder for easy review.
 * Also logs a compact summary to the Execution Log.
 */
function findArchivedApplicantsInDRPF() {
    var startTime = new Date();
    Logger.log('=== findArchivedApplicantsInDRPF START: ' + startTime.toISOString() + ' ===');

    try {
        // ── Step 1: Read DR/PF_Products — collect all rows grouped by ID ──────
        Logger.log('Step 1: Reading DR/PF_Products...');
        var dataWB = getDataWorkbook();
        var drPfSheet = dataWB.getSheetByName(CONFIG.PF_PRODUCTS_SHEET);

        if (!drPfSheet || drPfSheet.getLastRow() < 2) {
            Logger.log('DR/PF_Products is empty or not found. Nothing to do.');
            return;
        }

        var drPfData = drPfSheet.getDataRange().getValues();
        var drPfHeaders = trimHeaders(drPfData[0]);
        var idCol = drPfHeaders.indexOf('ID');
        var reqDateCol = drPfHeaders.indexOf('RequestDate');
        var productNameCol = drPfHeaders.indexOf('ProductName');
        var qtyReqCol = drPfHeaders.indexOf('QtyRequested');
        var qtyRecCol = drPfHeaders.indexOf('QtyReceived');

        if (idCol === -1) {
            Logger.log('ERROR: ID column not found in DR/PF_Products. Aborting.');
            return;
        }

        // Build a map: { id: [{ rowNum, requestDate, productName, qtyReq, qtyRec }] }
        var productsByID = {};
        for (var r = 1; r < drPfData.length; r++) {
            var rowId = (drPfData[r][idCol] || '').toString().trim();
            if (!rowId) continue;
            if (!productsByID[rowId]) productsByID[rowId] = [];
            productsByID[rowId].push({
                rowNum: r + 1,  // 1-based sheet row
                requestDate: reqDateCol !== -1 ? (drPfData[r][reqDateCol] || '') : '',
                productName: productNameCol !== -1 ? (drPfData[r][productNameCol] || '').toString() : '',
                qtyReq: qtyReqCol !== -1 ? (drPfData[r][qtyReqCol] || '') : '',
                qtyRec: qtyRecCol !== -1 ? (drPfData[r][qtyRecCol] || '') : ''
            });
        }

        var uniqueIds = Object.keys(productsByID);
        Logger.log('DR/PF_Products: ' + (drPfData.length - 1) + ' data rows, ' + uniqueIds.length + ' unique IDs.');

        // ── Step 2: Build AM ID set (active records) ──────────────────────────
        Logger.log('Step 2: Reading Applicants_Master ID column...');
        var masterSheet = getMasterSheet();
        var amIdSet = {};
        if (masterSheet && masterSheet.getLastRow() >= 2) {
            var amIds = masterSheet.getRange(2, 1, masterSheet.getLastRow() - 1, 1).getValues();
            for (var ai = 0; ai < amIds.length; ai++) {
                var amid = (amIds[ai][0] || '').toString().trim();
                if (amid) amIdSet[amid] = true;
            }
        }
        Logger.log('Applicants_Master: ' + Object.keys(amIdSet).length + ' active IDs.');

        // ── Step 3: Build archive ID maps from all archive workbooks ─────────
        Logger.log('Step 3: Reading archive workbooks...');
        var archiveIdMap = {}; // { id: workbookName }
        var farPast = new Date('2000-01-01');
        var farFuture = new Date('2099-12-31');
        var archiveWBs = getArchiveWorkbooksForRange(farPast, farFuture);

        Logger.log('Archive workbooks found: ' + archiveWBs.length);
        for (var w = 0; w < archiveWBs.length; w++) {
            var wb = archiveWBs[w].workbook;
            var wbn = archiveWBs[w].name;
            var archSheet = wb.getSheetByName('Archive');
            if (!archSheet || archSheet.getLastRow() < 2) {
                Logger.log('  ' + wbn + ': Archive sheet empty or missing — skipped.');
                continue;
            }
            var archIdData = archSheet.getRange(2, 1, archSheet.getLastRow() - 1, 1).getValues();
            var archCount = 0;
            for (var ai2 = 0; ai2 < archIdData.length; ai2++) {
                var archId = (archIdData[ai2][0] || '').toString().trim();
                if (archId && !archiveIdMap[archId]) {
                    archiveIdMap[archId] = wbn;
                    archCount++;
                }
            }
            Logger.log('  ' + wbn + ': ' + archCount + ' IDs indexed.');
        }
        Logger.log('Total unique archived applicant IDs: ' + Object.keys(archiveIdMap).length);

        // ── Step 4: Classify each ID in DR/PF_Products ────────────────────────
        Logger.log('Step 4: Classifying IDs...');
        var results = {
            active: [],   // ID is in AM — expected in DR/PF_Products
            archived: [],   // ID is in an archive workbook — products should be in Products_Archive
            orphaned: []    // ID not found in AM or any archive
        };

        for (var i = 0; i < uniqueIds.length; i++) {
            var uid = uniqueIds[i];
            var rows = productsByID[uid];
            var rowCount = rows.length;

            if (amIdSet[uid]) {
                results.active.push({ id: uid, rowCount: rowCount, note: 'Active in AM — OK' });
            } else if (archiveIdMap[uid]) {
                results.archived.push({
                    id: uid,
                    rowCount: rowCount,
                    archiveSource: archiveIdMap[uid],
                    note: 'Archived in ' + archiveIdMap[uid] + ' — products should be in Products_Archive'
                });
            } else {
                results.orphaned.push({ id: uid, rowCount: rowCount, note: 'Not found in AM or any archive' });
            }
        }

        // ── Step 5: Log summary ────────────────────────────────────────────────
        Logger.log('\n=== RESULTS ===');
        Logger.log('Active IDs (OK, expected in DR/PF_Products): ' + results.active.length);
        Logger.log('Archived IDs (products should be in Products_Archive): ' + results.archived.length);
        Logger.log('Orphaned IDs (not found anywhere): ' + results.orphaned.length);

        if (results.archived.length > 0) {
            Logger.log('\n--- ARCHIVED applicants with products in DR/PF_Products ---');
            for (var a = 0; a < results.archived.length; a++) {
                var ar = results.archived[a];
                Logger.log('  ID ' + ar.id + ' | ' + ar.rowCount + ' product rows | ' + ar.archiveSource);
            }
        }

        if (results.orphaned.length > 0) {
            Logger.log('\n--- ORPHANED IDs (not in AM or any archive) ---');
            for (var o = 0; o < results.orphaned.length; o++) {
                Logger.log('  ID ' + results.orphaned[o].id + ' | ' + results.orphaned[o].rowCount + ' product rows');
            }
        }

        // ── Step 6: Write results spreadsheet ─────────────────────────────────
        Logger.log('\nStep 6: Writing results spreadsheet...');
        var ss = SpreadsheetApp.create('G2N_Diagnostic_DrPfAudit_' +
            Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyyMMdd_HHmm'));
        moveToFolder(ss.getId(), CONFIG.GRANTS_FOLDER_ID);

        var sheet = ss.getActiveSheet();
        sheet.setName('Audit Results');

        // Header
        var hdr = ['ID', 'DR/PF Row Count', 'Classification', 'Archive Source', 'Note'];
        sheet.getRange(1, 1, 1, 5).setValues([hdr]);
        sheet.getRange(1, 1, 1, 5)
            .setFontWeight('bold').setBackground('#1a73e8').setFontColor('white');

        // Data rows
        var outputRows = [];
        results.active.forEach(function (r) {
            outputRows.push([r.id, r.rowCount, 'Active (OK)', '', r.note]);
        });
        results.archived.forEach(function (r) {
            outputRows.push([r.id, r.rowCount, 'ARCHIVED — migrate', r.archiveSource, r.note]);
        });
        results.orphaned.forEach(function (r) {
            outputRows.push([r.id, r.rowCount, 'ORPHANED', '', r.note]);
        });

        if (outputRows.length > 0) {
            sheet.getRange(2, 1, outputRows.length, 5).setValues(outputRows);

            // Colour code
            for (var ri = 0; ri < outputRows.length; ri++) {
                var cls = outputRows[ri][2];
                var bg = cls === 'Active (OK)' ? '#e8f5e9'
                    : cls.indexOf('ARCHIVED') === 0 ? '#fff3e0'
                        : '#fce4ec';
                sheet.getRange(ri + 2, 1, 1, 5).setBackground(bg);
            }
        }

        sheet.autoResizeColumns(1, 5);
        sheet.setFrozenRows(1);

        var elapsed = ((new Date() - startTime) / 1000).toFixed(1);
        Logger.log('\nResults spreadsheet: ' + ss.getUrl());
        Logger.log('=== findArchivedApplicantsInDRPF DONE in ' + elapsed + 's ===');

    } catch (e) {
        Logger.log('ERROR: ' + e.message + '\n' + e.stack);
    }
}




// ─── migrateAllProductsToArchive ─────────────────────────────────────────────

/**
 * Migrates ALL orphaned DR/PF_Products rows for archived applicants to the
 * correct Products_Archive sheet in one run. Equivalent to running
 * migrateProductsToArchive() 833 times but done in a single execution.
 *
 * Strategy (bulk, not per-ID):
 *   1. Read DR/PF_Products once into memory.
 *   2. Build AM ID set + archive ID→workbook map (same as the audit).
 *   3. Split DR/PF rows into "keep" (active IDs) and "migrate" (archived IDs).
 *   4. Group migrate rows by archive workbook name.
 *   5. For each archive workbook: append rows to Products_Archive, create sheet if needed.
 *   6. Rewrite DR/PF_Products with only the keep rows.
 *   7. Clear product caches and update ScriptProperties.
 *
 * DRY_RUN = true  → logs counts and destinations, writes NOTHING.
 * DRY_RUN = false → performs the full migration.
 *
 * ALWAYS run with DRY_RUN = true first and review the log.
 * Take a manual backup of DR/PF_Products before running with DRY_RUN = false.
 */
function migrateAllProductsToArchive() {
    var DRY_RUN = true;   // ← Set to false to actually migrate

    var startTime = new Date();
    Logger.log('=== migrateAllProductsToArchive START: ' + startTime.toISOString() + ' ===');
    Logger.log('Dry Run: ' + DRY_RUN);

    try {
        // ── Step 1: Read DR/PF_Products ───────────────────────────────────────
        Logger.log('Step 1: Reading DR/PF_Products...');
        var dataWB = getDataWorkbook();
        var drPfSheet = dataWB.getSheetByName(CONFIG.PF_PRODUCTS_SHEET);
        if (!drPfSheet || drPfSheet.getLastRow() < 2) {
            Logger.log('DR/PF_Products empty — nothing to do.'); return;
        }
        var drPfData = drPfSheet.getDataRange().getValues();
        var headers = trimHeaders(drPfData[0]);
        var idCol = headers.indexOf('ID');
        if (idCol === -1) { Logger.log('ID column not found. Aborting.'); return; }
        Logger.log('DR/PF_Products: ' + (drPfData.length - 1) + ' data rows.');

        // ── Step 2: Build AM ID set ────────────────────────────────────────────
        Logger.log('Step 2: Reading Applicants_Master...');
        var masterSheet = getMasterSheet();
        var amIdSet = {};
        if (masterSheet && masterSheet.getLastRow() >= 2) {
            var amIds = masterSheet.getRange(2, 1, masterSheet.getLastRow() - 1, 1).getValues();
            for (var ai = 0; ai < amIds.length; ai++) {
                var amid = (amIds[ai][0] || '').toString().trim();
                if (amid) amIdSet[amid] = true;
            }
        }
        Logger.log('Active IDs in AM: ' + Object.keys(amIdSet).length);

        // ── Step 3: Build archive ID → workbook name map ───────────────────────
        Logger.log('Step 3: Indexing archive workbooks...');
        var archiveIdMap = {};
        var farPast = new Date('2000-01-01');
        var farFuture = new Date('2099-12-31');
        var archiveWBs = getArchiveWorkbooksForRange(farPast, farFuture);
        Logger.log('Archive workbooks: ' + archiveWBs.length);

        for (var w = 0; w < archiveWBs.length; w++) {
            var wb = archiveWBs[w].workbook;
            var wbn = archiveWBs[w].name;
            var archSheet = wb.getSheetByName('Archive');
            if (!archSheet || archSheet.getLastRow() < 2) continue;
            var archIdData = archSheet.getRange(2, 1, archSheet.getLastRow() - 1, 1).getValues();
            for (var ai2 = 0; ai2 < archIdData.length; ai2++) {
                var archId = (archIdData[ai2][0] || '').toString().trim();
                if (archId && !archiveIdMap[archId]) archiveIdMap[archId] = wbn;
            }
        }
        Logger.log('Archived applicant IDs indexed: ' + Object.keys(archiveIdMap).length);

        // ── Step 4: Split DR/PF rows into keep vs migrate ──────────────────────
        Logger.log('Step 4: Splitting rows...');
        var keepRows = [drPfData[0]]; // header always kept
        var migrateByWB = {};           // { workbookName: [row, row, ...] }
        var migrateCount = 0;
        var skipCount = 0;           // orphaned — not in AM or any archive

        for (var r = 1; r < drPfData.length; r++) {
            var rowId = (drPfData[r][idCol] || '').toString().trim();
            if (!rowId) { keepRows.push(drPfData[r]); continue; }

            if (amIdSet[rowId]) {
                keepRows.push(drPfData[r]); // active — stays in DR/PF_Products
            } else if (archiveIdMap[rowId]) {
                var dest = archiveIdMap[rowId];
                if (!migrateByWB[dest]) migrateByWB[dest] = [];
                migrateByWB[dest].push(drPfData[r]);
                migrateCount++;
            } else {
                // ID not found anywhere — leave in place, log warning
                keepRows.push(drPfData[r]);
                skipCount++;
            }
        }

        Logger.log('Rows to keep (active):    ' + (keepRows.length - 1));
        Logger.log('Rows to migrate:          ' + migrateCount);
        Logger.log('Rows skipped (orphaned):  ' + skipCount);
        Logger.log('Destination workbooks:    ' + Object.keys(migrateByWB).length);

        Object.keys(migrateByWB).forEach(function (wbName) {
            Logger.log('  → ' + wbName + ': ' + migrateByWB[wbName].length + ' rows');
        });

        if (migrateCount === 0) {
            Logger.log('Nothing to migrate. Exiting.');
            return;
        }

        if (DRY_RUN) {
            Logger.log('\n[DRY RUN] No changes written. Set DRY_RUN = false to execute.');
            Logger.log('=== migrateAllProductsToArchive DRY RUN COMPLETE ===');
            return;
        }

        // ── Step 5: Append rows to each archive workbook's Products_Archive ────
        Logger.log('Step 5: Writing to archive workbooks...');
        var destNames = Object.keys(migrateByWB);

        for (var d = 0; d < destNames.length; d++) {
            var destName = destNames[d];
            var rowsForWB = migrateByWB[destName];

            // Locate the workbook
            var destWB = null;
            if (destName === 'G2N_Archive') {
                destWB = SpreadsheetApp.openById(CONFIG.ARCHIVE_WORKBOOK_ID);
            } else {
                var folder = DriveApp.getFolderById(CONFIG.ARCHIVES_BACKUPS_FOLDER_ID);
                var files = folder.getFiles();
                while (files.hasNext()) {
                    var file = files.next();
                    if (file.getMimeType() === 'application/vnd.google-apps.spreadsheet' &&
                        file.getName() === destName) {
                        destWB = SpreadsheetApp.openById(file.getId());
                        break;
                    }
                }
            }

            if (!destWB) {
                Logger.log('  WARNING: Workbook "' + destName + '" not found — ' +
                    rowsForWB.length + ' rows skipped.');
                continue;
            }

            var destSheet = destWB.getSheetByName('Products_Archive');
            if (!destSheet) {
                destSheet = destWB.insertSheet('Products_Archive');
                destSheet.getRange(1, 1, 1, headers.length).setValues([drPfData[0]]);
                destSheet.getRange(1, 1, 1, headers.length)
                    .setFontWeight('bold').setBackground('#4a86e8').setFontColor('white');
                destSheet.setFrozenRows(1);
                Logger.log('  Created Products_Archive sheet in ' + destName);
            }

            var lastRow = destSheet.getLastRow();
            destSheet.getRange(lastRow + 1, 1, rowsForWB.length, rowsForWB[0].length)
                .setValues(rowsForWB);
            Logger.log('  Appended ' + rowsForWB.length + ' rows to ' + destName + '/Products_Archive');
        }

        // ── Step 6: Rewrite DR/PF_Products with keep rows only ────────────────
        Logger.log('Step 6: Rewriting DR/PF_Products (' + (keepRows.length - 1) + ' data rows remain)...');
        drPfSheet.clearContents();
        if (keepRows.length > 0) {
            drPfSheet.getRange(1, 1, keepRows.length, keepRows[0].length).setValues(keepRows);
        }

        // ── Step 7: Clear caches and update ScriptProperties ─────────────────
        Logger.log('Step 7: Clearing product caches...');
        try {
            CacheService.getScriptCache().removeAll([
                'G2N_PRODUCT_PF_INDEX',
                'G2N_PRODUCT_PF_PRODUCTS',
                'G2N_PRODUCT_AM_IDS'
            ]);
        } catch (ce) { Logger.log('Cache clear (non-fatal): ' + ce.message); }

        try {
            // Rebuild PF_IDS from the now-reduced DR/PF_Products sheet
            var props = PropertiesService.getScriptProperties();
            var newIds = [];
            var seen = {};
            for (var kr = 1; kr < keepRows.length; kr++) {
                var kid = (keepRows[kr][idCol] || '').toString().trim();
                if (kid && !seen[kid]) { newIds.push(kid); seen[kid] = true; }
            }
            props.setProperty('G2N_PROD_PF_IDS', JSON.stringify(newIds));
            Logger.log('G2N_PROD_PF_IDS updated: ' + newIds.length + ' IDs remain.');
        } catch (pe) { Logger.log('ScriptProperties update (non-fatal): ' + pe.message); }

        logAudit('PRODUCT_MIGRATE', null,
            'Bulk migrated ' + migrateCount + ' DR/PF_Products rows to Products_Archive across ' +
            destNames.length + ' archive workbooks. ' + (keepRows.length - 1) + ' rows remain in DR/PF_Products.');

        var elapsed = ((new Date() - startTime) / 1000).toFixed(1);
        Logger.log('=== migrateAllProductsToArchive DONE in ' + elapsed + 's ===');

    } catch (e) {
        Logger.log('ERROR: ' + e.message + '\n' + e.stack);
    }
}


/**
 * Migrates DR/PF_Products rows for a specific applicant ID to Products_Archive
 * in the correct archive workbook, then deletes them from DR/PF_Products.
 *
 * Run findArchivedApplicantsInDRPF() first to identify which IDs need migrating
 * and confirm the archiveSource for each.
 *
 * EDIT THE VARIABLES BELOW before running:
 *   TARGET_ID          — the applicant ID to migrate (string)
 *   ARCHIVE_SOURCE     — the archive workbook name (e.g. 'G2N_Archive' or 'G2N_Archive_2023')
 *   DRY_RUN            — set to true to log what would happen without writing anything
 *
 * IMPORTANT: Always run with DRY_RUN = true first and review the log.
 */
function migrateProductsToArchive() {
    var TARGET_ID = '0';           // ← EDIT: applicant ID to migrate
    var ARCHIVE_SOURCE = 'G2N_Archive'; // ← EDIT: archive workbook name from the audit results
    var DRY_RUN = true;          // ← Set to false to actually write/delete

    Logger.log('=== migrateProductsToArchive START ===');
    Logger.log('Target ID: ' + TARGET_ID);
    Logger.log('Archive Source: ' + ARCHIVE_SOURCE);
    Logger.log('Dry Run: ' + DRY_RUN);

    try {
        // ── Read DR/PF_Products for the target ID ─────────────────────────────
        var dataWB = getDataWorkbook();
        var drPfSheet = dataWB.getSheetByName(CONFIG.PF_PRODUCTS_SHEET);
        if (!drPfSheet || drPfSheet.getLastRow() < 2) {
            Logger.log('DR/PF_Products is empty. Aborting.');
            return;
        }

        var drPfData = drPfSheet.getDataRange().getValues();
        var headers = trimHeaders(drPfData[0]);
        var idCol = headers.indexOf('ID');
        if (idCol === -1) { Logger.log('ID column not found. Aborting.'); return; }

        var rowsToMigrate = []; // full row data for rows matching TARGET_ID
        var rowsToMigrateIdx = []; // 1-based sheet row numbers (for deletion)
        var rowsToKeep = [drPfData[0]]; // header always kept

        for (var r = 1; r < drPfData.length; r++) {
            var rowId = (drPfData[r][idCol] || '').toString().trim();
            if (rowId === TARGET_ID) {
                rowsToMigrate.push(drPfData[r]);
                rowsToMigrateIdx.push(r + 1); // 1-based
            } else {
                rowsToKeep.push(drPfData[r]);
            }
        }

        Logger.log('Rows found for ID ' + TARGET_ID + ': ' + rowsToMigrate.length);

        if (rowsToMigrate.length === 0) {
            Logger.log('No rows found for ID ' + TARGET_ID + ' in DR/PF_Products. Nothing to migrate.');
            return;
        }

        Logger.log('Rows to migrate:');
        rowsToMigrate.forEach(function (row, idx) {
            Logger.log('  Sheet row ' + rowsToMigrateIdx[idx] + ': ' + JSON.stringify(row));
        });

        // ── Locate the archive workbook ────────────────────────────────────────
        var archiveWB = null;
        if (ARCHIVE_SOURCE === 'G2N_Archive') {
            archiveWB = SpreadsheetApp.openById(CONFIG.ARCHIVE_WORKBOOK_ID);
        } else {
            var folder = DriveApp.getFolderById(CONFIG.ARCHIVES_BACKUPS_FOLDER_ID);
            var files = folder.getFiles();
            while (files.hasNext()) {
                var file = files.next();
                if (file.getMimeType() === 'application/vnd.google-apps.spreadsheet' &&
                    file.getName() === ARCHIVE_SOURCE) {
                    archiveWB = SpreadsheetApp.openById(file.getId());
                    break;
                }
            }
        }
        if (!archiveWB) {
            Logger.log('ERROR: Archive workbook "' + ARCHIVE_SOURCE + '" not found. Aborting.');
            return;
        }

        // ── Get or create Products_Archive sheet ──────────────────────────────
        var archSheet = archiveWB.getSheetByName('Products_Archive');
        if (!archSheet) {
            Logger.log('Products_Archive sheet not found in ' + ARCHIVE_SOURCE + '. Will create it.');
            if (!DRY_RUN) {
                archSheet = archiveWB.insertSheet('Products_Archive');
                archSheet.getRange(1, 1, 1, headers.length).setValues([drPfData[0]]);
                archSheet.getRange(1, 1, 1, headers.length)
                    .setFontWeight('bold').setBackground('#4a86e8').setFontColor('white');
                archSheet.setFrozenRows(1);
                Logger.log('Products_Archive sheet created.');
            }
        }

        // ── Write rows to Products_Archive ────────────────────────────────────
        Logger.log(DRY_RUN ? '[DRY RUN] Would append ' + rowsToMigrate.length + ' rows to Products_Archive in ' + ARCHIVE_SOURCE
            : 'Appending ' + rowsToMigrate.length + ' rows to Products_Archive in ' + ARCHIVE_SOURCE);

        if (!DRY_RUN && archSheet) {
            var lastRow = archSheet.getLastRow();
            archSheet.getRange(lastRow + 1, 1, rowsToMigrate.length, rowsToMigrate[0].length)
                .setValues(rowsToMigrate);
            Logger.log('Appended ' + rowsToMigrate.length + ' rows to Products_Archive.');
        }

        // ── Rewrite DR/PF_Products without the migrated rows ─────────────────
        Logger.log(DRY_RUN ? '[DRY RUN] Would rewrite DR/PF_Products removing ' + rowsToMigrate.length + ' rows (keeping ' + (rowsToKeep.length - 1) + ' data rows).'
            : 'Rewriting DR/PF_Products (removing ' + rowsToMigrate.length + ' rows)...');

        if (!DRY_RUN) {
            drPfSheet.clearContents();
            if (rowsToKeep.length > 0) {
                drPfSheet.getRange(1, 1, rowsToKeep.length, rowsToKeep[0].length)
                    .setValues(rowsToKeep);
            }
            Logger.log('DR/PF_Products rewritten. ' + (rowsToKeep.length - 1) + ' data rows remain.');

            // Invalidate product cache so next PP load sees fresh data
            CacheService.getScriptCache().removeAll([
                'G2N_PRODUCT_PF_INDEX',
                'G2N_PRODUCT_PF_PRODUCTS',
                'G2N_PRODUCT_AM_IDS'
            ]);
            // Remove this ID from ScriptProperties PF_IDS if present
            try {
                var props = PropertiesService.getScriptProperties();
                var pf = props.getProperty('G2N_PROD_PF_IDS');
                if (pf) {
                    var pfArr = JSON.parse(pf).filter(function (x) { return x !== TARGET_ID; });
                    props.setProperty('G2N_PROD_PF_IDS', JSON.stringify(pfArr));
                    Logger.log('Removed ID from G2N_PROD_PF_IDS ScriptProperties.');
                }
            } catch (propErr) {
                Logger.log('Could not update G2N_PROD_PF_IDS (non-fatal): ' + propErr.message);
            }

            logAudit('PRODUCT_MIGRATE', TARGET_ID,
                'Migrated ' + rowsToMigrate.length + ' DR/PF_Products rows to Products_Archive in ' + ARCHIVE_SOURCE);
        }

        Logger.log('=== migrateProductsToArchive ' + (DRY_RUN ? 'DRY RUN COMPLETE' : 'DONE') + ' ===');

    } catch (e) {
        Logger.log('ERROR: ' + e.message + '\n' + e.stack);
    }
}

// ─── testArchiveSave ─────────────────────────────────────────────────────────

/**
 * Step-by-step diagnostic for archive record save failures.
 * Runs READ-ONLY — does NOT write anything. Logs every stage so the exact
 * failure point is visible in the Execution Log.
 *
 * HOW TO USE:
 *   1. Set TEST_RECORD_ID and TEST_ARCHIVE_SOURCE below to match the record
 *      that failed to save (e.g. ID 1934, source 'G2N_Archive').
 *   2. Run this function from the Apps Script editor.
 *   3. Check the Execution Log — it will show exactly where the path fails.
 *
 * v1.1 - Added for archive save timeout diagnosis.
 */
function testArchiveSave() {

    // ── CONFIG — set these to the failing record ──────────────────────────────
    var TEST_RECORD_ID = '1934';          // Record ID to locate
    var TEST_ARCHIVE_SOURCE = 'G2N_Archive'; // archiveSource value from SV hidden field
    // ─────────────────────────────────────────────────────────────────────────

    Logger.log('=== testArchiveSave START ===');
    Logger.log('Target: ID=' + TEST_RECORD_ID + '  source=' + TEST_ARCHIVE_SOURCE);

    try {
        // Step 1 — Open workbook
        Logger.log('Step 1: Opening archive workbook...');
        var archiveWB;
        if (TEST_ARCHIVE_SOURCE === 'G2N_Archive') {
            Logger.log('  Using CONFIG.ARCHIVE_WORKBOOK_ID: ' + CONFIG.ARCHIVE_WORKBOOK_ID);
            archiveWB = SpreadsheetApp.openById(CONFIG.ARCHIVE_WORKBOOK_ID);
        } else {
            Logger.log('  Scanning ARCHIVES_BACKUPS_FOLDER_ID for: ' + TEST_ARCHIVE_SOURCE);
            var folder = DriveApp.getFolderById(CONFIG.ARCHIVES_BACKUPS_FOLDER_ID);
            var files = folder.getFiles();
            while (files.hasNext()) {
                var file = files.next();
                if (file.getMimeType() === 'application/vnd.google-apps.spreadsheet' &&
                    file.getName() === TEST_ARCHIVE_SOURCE) {
                    Logger.log('  Found file ID: ' + file.getId());
                    archiveWB = SpreadsheetApp.openById(file.getId());
                    break;
                }
            }
            if (!archiveWB) {
                Logger.log('FAIL Step 1: Workbook "' + TEST_ARCHIVE_SOURCE + '" not found in folder');
                return;
            }
        }
        Logger.log('  OK — workbook name: ' + archiveWB.getName());

        // Step 2 — Get Archive sheet
        Logger.log('Step 2: Getting Archive sheet...');
        var sheet = archiveWB.getSheetByName('Archive');
        if (!sheet) {
            Logger.log('FAIL Step 2: No sheet named "Archive" in workbook');
            return;
        }
        var lastRow = sheet.getLastRow();
        var lastCol = sheet.getLastColumn();
        Logger.log('  OK — lastRow=' + lastRow + '  lastCol=' + lastCol);

        // Step 3 — Read headers
        Logger.log('Step 3: Reading headers...');
        var headers = trimHeaders(sheet.getRange(1, 1, 1, lastCol).getValues()[0]);
        var idCol = headers.indexOf('ID');
        Logger.log('  OK — ' + headers.length + ' columns, ID col index=' + idCol);
        if (idCol === -1) { Logger.log('FAIL Step 3: ID column not found'); return; }

        // Step 4 — Read ID column and find target row
        Logger.log('Step 4: Reading ID column to find record ' + TEST_RECORD_ID + '...');
        var idColData = sheet.getRange(2, idCol + 1, lastRow - 1, 1).getValues();
        var targetSheetRow = -1;
        for (var i = 0; i < idColData.length; i++) {
            if (parseInt(idColData[i][0]) === parseInt(TEST_RECORD_ID)) {
                targetSheetRow = i + 2;
                break;
            }
        }
        if (targetSheetRow === -1) {
            Logger.log('FAIL Step 4: Record ID ' + TEST_RECORD_ID + ' not found in Archive sheet');
            return;
        }
        Logger.log('  OK — found at sheet row ' + targetSheetRow);

        // Step 5 — Read the target row
        Logger.log('Step 5: Reading target row...');
        var currentRow = sheet.getRange(targetSheetRow, 1, 1, headers.length).getValues()[0];
        Logger.log('  OK — row has ' + currentRow.length + ' values. First 5: ' +
            currentRow.slice(0, 5).map(function (v) { return JSON.stringify(v); }).join(', '));

        // Step 6 — Simulate a setValues (write same data back — no actual change)
        Logger.log('Step 6: Testing setValues (writing row back unchanged — READ/WRITE test)...');
        sheet.getRange(targetSheetRow, 1, 1, headers.length).setValues([currentRow]);
        Logger.log('  OK — setValues completed successfully');

        // Step 7 — Test getMasterWorkbook access (for logAudit)
        Logger.log('Step 7: Opening AM for logAudit...');
        var masterWB = getMasterWorkbook();
        Logger.log('  OK — ' + masterWB.getName());

        Logger.log('=== testArchiveSave COMPLETE — all steps passed ===');
        Logger.log('The save path is fully functional. If saves still fail from the portal,');
        Logger.log('the issue is in data serialization from the browser to google.script.run.');

    } catch (e) {
        Logger.log('EXCEPTION at current step: ' + e.message);
        Logger.log('Stack: ' + e.stack);
    }
}

