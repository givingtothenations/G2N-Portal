/**
 * AIReportService.gs
 * AI-powered report generation using Anthropic Claude API for G2N System
 * Generates narrative grant reports, trend analysis, testimonial summaries,
 * and executive summaries from existing G2N data.
 *
 * Reports:
 *   1. Grant Narrative: Polished grant-ready narrative from demographics,
 *      income, distribution, and testimonial data
 *   2. Data Trend Analysis: Period-over-period insights on service demand,
 *      demographics, geographic patterns
 *   3. Testimonial Summary: Synthesized impact statements from individual
 *      client testimonials
 *   4. Executive Summary: High-level operational overview from all data sources
 *
 * Output Formats:
 *   - Google Doc (narrative-friendly with formatting)
 *   - Google Sheet (structured with AI Analysis tab)
 *
 * Configuration:
 *   API key stored in Script Properties (ANTHROPIC_API_KEY)
 *   Model configurable via AI_MODEL property (default: claude-sonnet-4-20250514)
 *
 * v1.0 - Phase 6: Initial AI reporting integration with Anthropic Claude API.
 *         Grant narrative, trend analysis, testimonial summary, executive summary.
 *         Google Doc and Google Sheet output. API key in Script Properties.
 * v1.1 - Phase 6: AI Custom Data Sheets — field picker + free-text prompt modes.
 *         Dynamic AM header retrieval for field picker checkboxes.
 *         Claude structures/computes/groups raw data into formatted Sheets.
 *         Grouped field categories for intuitive UI.
 * v1.2 - Fix: Data Sheet JSON parsing — robust extractJSON with 3 fallback
 *         strategies (direct, brace-matching, line-scanning). Field name
 *         aliasing for long column names to reduce tokens. Array-based data
 *         format instead of objects. Max tokens raised to 8192 for data sheets.
 *         Row limit reduced to 200 to prevent input token overflow.
 * v1.3 - Fix: Complete rewrite of Data Sheet to two-step architecture.
 *         Step 1: Claude receives ONLY field names, unique values, 3 sample
 *         rows, and user instructions — returns small JSON spec (filters,
 *         sort, computations). Step 2: GAS applies spec to actual data.
 *         Eliminates token overflow and truncation issues. Added verbose
 *         logging for debugging. Robust blank/empty filtering.
 * v1.4 - Computed Fields: Added per-row calculated columns from Grants Reports
 *         (People, Children, Adults, Seniors, Male/Female Children, Applicant
 *         Type, Products Requested/Distributed). Computed at report time from
 *         age brackets and product lookups. Selectable in field picker UI.
 */

// ============ AI CONFIGURATION ============

/**
 * Get AI configuration from Script Properties
 * @returns {Object} { apiKey, model, maxTokens }
 */
function getAIConfig() {
    var props = PropertiesService.getScriptProperties();
    return {
        apiKey: props.getProperty('ANTHROPIC_API_KEY') || '',
        model: props.getProperty('AI_MODEL') || 'claude-sonnet-4-20250514',
        maxTokens: parseInt(props.getProperty('AI_MAX_TOKENS') || '4096')
    };
}

/**
 * Check if AI reporting is configured (API key exists)
 * Called by AP on load to show/hide AI Reports tab
 * @returns {Object} { configured: boolean, model: string }
 */
function checkAIConfig() {
    var config = getAIConfig();
    return {
        configured: config.apiKey.length > 0,
        model: config.model
    };
}

/**
 * Save the Anthropic API key to Script Properties
 * Admin-only function called from AP Settings
 * @param {string} apiKey - Anthropic API key
 * @returns {Object} { success: boolean, message: string }
 */
function saveAIApiKey(apiKey) {
    try {
        if (!apiKey || apiKey.trim().length === 0) {
            return { success: false, error: 'API key cannot be empty' };
        }
        PropertiesService.getScriptProperties().setProperty('ANTHROPIC_API_KEY', apiKey.trim());
        logAudit('AI_CONFIG', null, 'Anthropic API key updated');
        return { success: true, message: 'API key saved successfully' };
    } catch (e) {
        return { success: false, error: 'Failed to save API key: ' + e.message };
    }
}

// ============ CORE API FUNCTION ============
// v1.9: Field display labels and group definitions are now driven by
// FieldMapService.gs which reads from LU_FieldMap sheet in G2N_Lookups.

/**
 * Call the Anthropic Claude API with a prompt and return the response text
 * Uses UrlFetchApp for HTTP request to Anthropic Messages API
 * @param {string} systemPrompt - System-level instructions for Claude
 * @param {string} userMessage - The data/question to analyze
 * @returns {Object} { success: boolean, text: string, error: string, tokensUsed: number }
 */
function callClaudeAPI(systemPrompt, userMessage) {
    var config = getAIConfig();

    if (!config.apiKey) {
        return { success: false, error: 'Anthropic API key not configured. Go to AI Reports tab → Setup to add your key.' };
    }

    try {
        var payload = {
            model: config.model,
            max_tokens: config.maxTokens,
            system: systemPrompt,
            messages: [
                { role: 'user', content: userMessage }
            ]
        };

        var options = {
            method: 'post',
            contentType: 'application/json',
            headers: {
                'x-api-key': config.apiKey,
                'anthropic-version': '2023-06-01'
            },
            payload: JSON.stringify(payload),
            muteHttpExceptions: true
        };

        var response = UrlFetchApp.fetch('https://api.anthropic.com/v1/messages', options);
        var responseCode = response.getResponseCode();
        var responseBody = JSON.parse(response.getContentText());

        if (responseCode !== 200) {
            var errorMsg = responseBody.error ? responseBody.error.message : 'HTTP ' + responseCode;
            Logger.log('Claude API error: ' + errorMsg);
            return { success: false, error: 'API error: ' + errorMsg };
        }

        var text = '';
        var tokensUsed = 0;
        if (responseBody.content && responseBody.content.length > 0) {
            text = responseBody.content
                .filter(function (block) { return block.type === 'text'; })
                .map(function (block) { return block.text; })
                .join('\n');
        }
        if (responseBody.usage) {
            tokensUsed = (responseBody.usage.input_tokens || 0) + (responseBody.usage.output_tokens || 0);
        }

        return { success: true, text: text, tokensUsed: tokensUsed };

    } catch (e) {
        Logger.log('Claude API call failed: ' + e.message);
        return { success: false, error: 'API call failed: ' + e.message };
    }
}

// ============ DATA GATHERING ============

/**
 * Gather comprehensive data for AI analysis from G2N data sources
 * Pulls demographics, income, distributions, and testimonials for date range
 * @param {string} fromDateStr - YYYY-MM-DD start date
 * @param {string} toDateStr - YYYY-MM-DD end date
 * @returns {Object} Structured data object with all report sections
 */
function gatherAIReportData(fromDateStr, toDateStr) {
    var fromDate = parseDateInput(fromDateStr, false);
    var toDate = parseDateInput(toDateStr, true);
    var combined = getCombinedData(fromDate, toDate);

    if (combined.totalCount === 0) {
        return { empty: true, error: 'No records found for the specified date range' };
    }

    var headers = combined.headers;
    var rows = combined.rows;

    // Build column index map
    var col = {};
    var colNames = [
        'ID', 'First Name', 'Last Name', 'Request Date', 'Request Type',
        'County', 'State', 'City', 'Age', 'Race', 'Ethnicity', 'Gender',
        'Military Status', 'Income Level', 'Income Source', 'Household Members',
        'Number in Household', 'Service Status', 'Funding Source',
        'Scheduled Distribution Code', 'Date Picked Up', 'Last Date Served',
        'Testimonial', 'How did they learn about us?',
        'Person/Organization that Referred Them', 'Box Code',
        'Scheduled Box Code 1', 'Scheduled Box Code 2', 'Scheduled Box Code 3',
        'Take Baby Box?'
    ];
    for (var c = 0; c < colNames.length; c++) {
        col[colNames[c]] = headers.indexOf(colNames[c]);
    }

    // Aggregate data
    var demographics = { races: {}, ethnicities: {}, genders: {}, ages: {}, military: {} };
    var geography = { counties: {}, cities: {}, states: {} };
    var income = { levels: {}, sources: {} };
    var service = { statuses: {}, fundingSources: {}, requestTypes: {} };
    var testimonials = [];
    var referralSources = {};
    var householdSizes = [];
    var distributionCodes = {};
    var babyBoxCount = 0;
    var totalPickedUp = 0;
    var monthlyVolume = {};

    for (var i = 0; i < rows.length; i++) {
        var row = rows[i];

        // Demographics
        incrementCount(demographics.races, getVal(row, col, 'Race'));
        incrementCount(demographics.ethnicities, getVal(row, col, 'Ethnicity'));
        incrementCount(demographics.genders, getVal(row, col, 'Gender'));
        incrementCount(demographics.military, getVal(row, col, 'Military Status'));

        // Age brackets
        var age = parseInt(getVal(row, col, 'Age'));
        if (!isNaN(age)) {
            var bracket = getAgeBracketLabel(age);
            incrementCount(demographics.ages, bracket);
        }

        // Geography
        incrementCount(geography.counties, getVal(row, col, 'County'));
        incrementCount(geography.cities, getVal(row, col, 'City'));
        incrementCount(geography.states, getVal(row, col, 'State'));

        // Income
        incrementCount(income.levels, getVal(row, col, 'Income Level'));
        incrementCount(income.sources, getVal(row, col, 'Income Source'));

        // Service
        incrementCount(service.statuses, getVal(row, col, 'Service Status'));
        incrementCount(service.fundingSources, getVal(row, col, 'Funding Source'));
        incrementCount(service.requestTypes, getVal(row, col, 'Request Type'));

        // Distribution codes
        var schedCode = getVal(row, col, 'Scheduled Distribution Code');
        if (schedCode) incrementCount(distributionCodes, schedCode);

        // Baby box
        var bb = getVal(row, col, 'Take Baby Box?');
        if (bb === 'X' || bb === 'x') babyBoxCount++;

        // Picked up
        var pickedUp = getVal(row, col, 'Date Picked Up');
        if (pickedUp) totalPickedUp++;

        // Household
        var hhSize = parseInt(getVal(row, col, 'Number in Household'));
        if (!isNaN(hhSize) && hhSize > 0) householdSizes.push(hhSize);

        // Testimonials
        var testimonial = getVal(row, col, 'Testimonial');
        if (testimonial && testimonial.length > 5) {
            testimonials.push(testimonial);
        }

        // Referrals
        var referral = getVal(row, col, 'How did they learn about us?');
        if (referral) incrementCount(referralSources, referral);

        // Monthly volume
        var reqDate = row[col['Request Date']];
        if (reqDate) {
            var d = new Date(reqDate);
            var monthKey = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0');
            incrementCount(monthlyVolume, monthKey);
        }
    }

    // Calculate household stats
    var avgHousehold = 0;
    var totalIndividualsServed = 0;
    if (householdSizes.length > 0) {
        var sum = householdSizes.reduce(function (a, b) { return a + b; }, 0);
        avgHousehold = Math.round((sum / householdSizes.length) * 10) / 10;
        totalIndividualsServed = sum;
    }

    return {
        empty: false,
        period: { from: fromDateStr, to: toDateStr },
        totalRecords: combined.totalCount,
        masterCount: combined.masterCount,
        archiveCount: combined.archiveCount,
        demographics: demographics,
        geography: geography,
        income: income,
        service: service,
        testimonials: testimonials,
        referralSources: referralSources,
        distributionCodes: distributionCodes,
        babyBoxCount: babyBoxCount,
        totalPickedUp: totalPickedUp,
        householdStats: {
            avgSize: avgHousehold,
            totalIndividuals: totalIndividualsServed,
            reportedCount: householdSizes.length
        },
        monthlyVolume: monthlyVolume
    };
}

/**
 * Helper: Get cell value as trimmed string
 * @param {Array} row - Data row
 * @param {Object} colMap - Column index map
 * @param {string} colName - Column name
 * @returns {string} Trimmed cell value or empty string
 */
function getVal(row, colMap, colName) {
    var idx = colMap[colName];
    if (idx === undefined || idx === -1) return '';
    return (row[idx] || '').toString().trim();
}

/**
 * Helper: Increment a count in a frequency object
 * @param {Object} obj - Frequency counting object
 * @param {string} key - Key to increment
 */
function incrementCount(obj, key) {
    if (!key || key === '') return;
    obj[key] = (obj[key] || 0) + 1;
}

/**
 * Helper: Get age bracket label matching GrantsReportService pattern
 * @param {number} age - Age in years
 * @returns {string} Age bracket label
 */
function getAgeBracketLabel(age) {
    if (age < 18) return 'Under 18';
    if (age <= 24) return '18-24';
    if (age <= 34) return '25-34';
    if (age <= 44) return '35-44';
    if (age <= 54) return '45-54';
    if (age <= 64) return '55-64';
    return '65+';
}

/**
 * Helper: Convert frequency object to sorted array of [key, count] pairs
 * Sorted by count descending
 * @param {Object} obj - Frequency counting object
 * @returns {Array} Sorted [key, count] pairs
 */
function sortedEntries(obj) {
    return Object.keys(obj).map(function (k) {
        return [k, obj[k]];
    }).sort(function (a, b) {
        return b[1] - a[1];
    });
}

/**
 * Helper: Format frequency object as readable text block for AI prompt
 * @param {Object} obj - Frequency counting object
 * @param {number} total - Total count for percentage calculation
 * @returns {string} Formatted text
 */
function formatFrequencyBlock(obj, total) {
    return sortedEntries(obj).map(function (entry) {
        var pct = total > 0 ? Math.round((entry[1] / total) * 1000) / 10 : 0;
        return '  ' + entry[0] + ': ' + entry[1] + ' (' + pct + '%)';
    }).join('\n');
}

// ============ REPORT GENERATORS ============

/**
 * Generate AI Grant Narrative report
 * Sends comprehensive demographic, service, and impact data to Claude
 * for polished grant-ready narrative generation
 * @param {string} fromDate - YYYY-MM-DD start date
 * @param {string} toDate - YYYY-MM-DD end date
 * @param {string} outputFormat - 'doc' or 'sheet'
 * @param {string} customInstructions - Optional additional prompt instructions
 * @returns {Object} { success, reportUrl, downloadUrl, message }
 */
function generateAIGrantNarrative(fromDate, toDate, outputFormat, customInstructions) {
    try {
        var data = gatherAIReportData(fromDate, toDate);
        if (data.empty) return { success: false, error: data.error };

        var systemPrompt = 'You are a professional grant writer for Giving to the Nations, ' +
            'a nonprofit that distributes cleaning products, personal care items, and hygiene ' +
            'supplies to families in need. Write compelling, data-driven grant narratives. ' +
            'Use specific numbers and percentages from the data provided. ' +
            'Structure with clear sections: Program Overview, Population Served, ' +
            'Service Delivery, Impact & Outcomes, Community Need. ' +
            'Write in a warm, professional tone appropriate for grant applications. ' +
            'Do NOT make up any data — use only what is provided.';

        if (customInstructions) {
            systemPrompt += '\n\nAdditional instructions: ' + customInstructions;
        }

        var userMessage = buildGrantNarrativePrompt(data);
        var result = callClaudeAPI(systemPrompt, userMessage);

        if (!result.success) return result;

        return saveAIReport(result.text, 'Grant_Narrative', fromDate, toDate, outputFormat, result.tokensUsed);

    } catch (e) {
        Logger.log('generateAIGrantNarrative error: ' + e.message);
        return { success: false, error: 'Grant narrative generation failed: ' + e.message };
    }
}

/**
 * Generate AI Trend Analysis report
 * Analyzes period-over-period changes in service demand, demographics, geography
 * @param {string} fromDate - YYYY-MM-DD start date
 * @param {string} toDate - YYYY-MM-DD end date
 * @param {string} outputFormat - 'doc' or 'sheet'
 * @param {string} customInstructions - Optional additional prompt instructions
 * @returns {Object} { success, reportUrl, downloadUrl, message }
 */
function generateAITrendAnalysis(fromDate, toDate, outputFormat, customInstructions) {
    try {
        var data = gatherAIReportData(fromDate, toDate);
        if (data.empty) return { success: false, error: data.error };

        var systemPrompt = 'You are a data analyst for Giving to the Nations nonprofit. ' +
            'Analyze service data to identify trends, patterns, and actionable insights. ' +
            'Focus on: monthly volume trends, geographic distribution shifts, demographic ' +
            'patterns, service completion rates, and operational efficiency indicators. ' +
            'Present findings with specific numbers. Highlight concerning trends and ' +
            'opportunities. Structure as: Key Findings, Volume Trends, Demographic Shifts, ' +
            'Geographic Patterns, Service Delivery Analysis, Recommendations. ' +
            'Do NOT make up any data — use only what is provided.';

        if (customInstructions) {
            systemPrompt += '\n\nAdditional instructions: ' + customInstructions;
        }

        var userMessage = buildTrendAnalysisPrompt(data);
        var result = callClaudeAPI(systemPrompt, userMessage);

        if (!result.success) return result;

        return saveAIReport(result.text, 'Trend_Analysis', fromDate, toDate, outputFormat, result.tokensUsed);

    } catch (e) {
        Logger.log('generateAITrendAnalysis error: ' + e.message);
        return { success: false, error: 'Trend analysis generation failed: ' + e.message };
    }
}

/**
 * Generate AI Testimonial Summary report
 * Synthesizes individual testimonials into cohesive impact statements
 * @param {string} fromDate - YYYY-MM-DD start date
 * @param {string} toDate - YYYY-MM-DD end date
 * @param {string} outputFormat - 'doc' or 'sheet'
 * @param {string} customInstructions - Optional additional prompt instructions
 * @returns {Object} { success, reportUrl, downloadUrl, message }
 */
function generateAITestimonialSummary(fromDate, toDate, outputFormat, customInstructions) {
    try {
        var data = gatherAIReportData(fromDate, toDate);
        if (data.empty) return { success: false, error: data.error };

        if (data.testimonials.length === 0) {
            return { success: false, error: 'No testimonials found for the specified date range' };
        }

        var systemPrompt = 'You are a communications specialist for Giving to the Nations nonprofit. ' +
            'Synthesize individual client testimonials into compelling impact narratives suitable ' +
            'for grant applications, annual reports, and donor communications. ' +
            'Group themes (gratitude, life impact, family benefit, community connection). ' +
            'Create: 1) A 2-3 paragraph overall impact summary, 2) 3-5 theme-based sections ' +
            'with representative quotes, 3) Key statistics from the testimonials. ' +
            'Preserve the authentic voice of clients while maintaining privacy — ' +
            'do NOT use real names. Do NOT make up any quotes — paraphrase or quote from provided data only.';

        if (customInstructions) {
            systemPrompt += '\n\nAdditional instructions: ' + customInstructions;
        }

        var userMessage = buildTestimonialPrompt(data);
        var result = callClaudeAPI(systemPrompt, userMessage);

        if (!result.success) return result;

        return saveAIReport(result.text, 'Testimonial_Summary', fromDate, toDate, outputFormat, result.tokensUsed);

    } catch (e) {
        Logger.log('generateAITestimonialSummary error: ' + e.message);
        return { success: false, error: 'Testimonial summary generation failed: ' + e.message };
    }
}

/**
 * Generate AI Executive Summary report
 * High-level operational overview combining all data dimensions
 * @param {string} fromDate - YYYY-MM-DD start date
 * @param {string} toDate - YYYY-MM-DD end date
 * @param {string} outputFormat - 'doc' or 'sheet'
 * @param {string} customInstructions - Optional additional prompt instructions
 * @returns {Object} { success, reportUrl, downloadUrl, message }
 */
function generateAIExecutiveSummary(fromDate, toDate, outputFormat, customInstructions) {
    try {
        var data = gatherAIReportData(fromDate, toDate);
        if (data.empty) return { success: false, error: data.error };

        var systemPrompt = 'You are preparing a concise executive summary for the leadership ' +
            'of Giving to the Nations, a nonprofit distributing cleaning and hygiene products. ' +
            'Write a 1-2 page executive summary covering: Program Snapshot (key metrics), ' +
            'Service Highlights, Population Demographics, Operational Performance, ' +
            'and Forward-Looking Recommendations. Use bullet points for key metrics. ' +
            'Keep the tone confident and professional. Prioritize the most impactful ' +
            'numbers. Do NOT make up any data — use only what is provided.';

        if (customInstructions) {
            systemPrompt += '\n\nAdditional instructions: ' + customInstructions;
        }

        var userMessage = buildExecutiveSummaryPrompt(data);
        var result = callClaudeAPI(systemPrompt, userMessage);

        if (!result.success) return result;

        return saveAIReport(result.text, 'Executive_Summary', fromDate, toDate, outputFormat, result.tokensUsed);

    } catch (e) {
        Logger.log('generateAIExecutiveSummary error: ' + e.message);
        return { success: false, error: 'Executive summary generation failed: ' + e.message };
    }
}

// ============ PROMPT BUILDERS ============

/**
 * Build the user prompt for Grant Narrative generation
 * Formats gathered data into a structured prompt with all relevant metrics
 * @param {Object} data - Output from gatherAIReportData
 * @returns {string} Formatted prompt text
 */
function buildGrantNarrativePrompt(data) {
    var total = data.totalRecords;
    var prompt = 'Generate a grant narrative report for Giving to the Nations.\n\n';
    prompt += 'REPORTING PERIOD: ' + data.period.from + ' to ' + data.period.to + '\n';
    prompt += 'TOTAL REQUESTS: ' + total + '\n\n';

    prompt += '=== DEMOGRAPHICS ===\n';
    prompt += 'Race/Ethnicity Breakdown:\n' + formatFrequencyBlock(data.demographics.races, total) + '\n';
    prompt += 'Ethnicity:\n' + formatFrequencyBlock(data.demographics.ethnicities, total) + '\n';
    prompt += 'Gender:\n' + formatFrequencyBlock(data.demographics.genders, total) + '\n';
    prompt += 'Age Brackets:\n' + formatFrequencyBlock(data.demographics.ages, total) + '\n';
    prompt += 'Military Status:\n' + formatFrequencyBlock(data.demographics.military, total) + '\n\n';

    prompt += '=== GEOGRAPHY ===\n';
    prompt += 'Counties Served:\n' + formatFrequencyBlock(data.geography.counties, total) + '\n';
    prompt += 'Top Cities:\n' + formatFrequencyBlock(data.geography.cities, total) + '\n\n';

    prompt += '=== INCOME & ECONOMIC DATA ===\n';
    prompt += 'Income Levels:\n' + formatFrequencyBlock(data.income.levels, total) + '\n';
    prompt += 'Income Sources:\n' + formatFrequencyBlock(data.income.sources, total) + '\n\n';

    prompt += '=== SERVICE DELIVERY ===\n';
    prompt += 'Total Requests: ' + total + '\n';
    prompt += 'Picked Up: ' + data.totalPickedUp + '\n';
    prompt += 'Baby Boxes: ' + data.babyBoxCount + '\n';
    prompt += 'Service Statuses:\n' + formatFrequencyBlock(data.service.statuses, total) + '\n';
    prompt += 'Funding Sources:\n' + formatFrequencyBlock(data.service.fundingSources, total) + '\n';
    prompt += 'Request Types:\n' + formatFrequencyBlock(data.service.requestTypes, total) + '\n\n';

    prompt += '=== HOUSEHOLD DATA ===\n';
    prompt += 'Households Reporting Size: ' + data.householdStats.reportedCount + '\n';
    prompt += 'Average Household Size: ' + data.householdStats.avgSize + '\n';
    prompt += 'Total Individuals Served (estimated): ' + data.householdStats.totalIndividuals + '\n\n';

    prompt += '=== REFERRAL SOURCES ===\n';
    prompt += formatFrequencyBlock(data.referralSources, total) + '\n\n';

    if (data.testimonials.length > 0) {
        prompt += '=== CLIENT TESTIMONIALS (' + data.testimonials.length + ' total) ===\n';
        var maxTestimonials = Math.min(data.testimonials.length, 25);
        for (var t = 0; t < maxTestimonials; t++) {
            prompt += '- "' + data.testimonials[t] + '"\n';
        }
        if (data.testimonials.length > 25) {
            prompt += '(Showing 25 of ' + data.testimonials.length + ' testimonials)\n';
        }
    }

    return prompt;
}

/**
 * Build the user prompt for Trend Analysis generation
 * Emphasizes monthly volume patterns and distribution metrics
 * @param {Object} data - Output from gatherAIReportData
 * @returns {string} Formatted prompt text
 */
function buildTrendAnalysisPrompt(data) {
    var total = data.totalRecords;
    var prompt = 'Analyze trends in the following service data for Giving to the Nations.\n\n';
    prompt += 'REPORTING PERIOD: ' + data.period.from + ' to ' + data.period.to + '\n';
    prompt += 'TOTAL REQUESTS: ' + total + '\n\n';

    prompt += '=== MONTHLY VOLUME ===\n';
    var months = Object.keys(data.monthlyVolume).sort();
    for (var m = 0; m < months.length; m++) {
        prompt += '  ' + months[m] + ': ' + data.monthlyVolume[months[m]] + ' requests\n';
    }
    prompt += '\n';

    prompt += '=== GEOGRAPHIC DISTRIBUTION ===\n';
    prompt += 'Counties:\n' + formatFrequencyBlock(data.geography.counties, total) + '\n';
    prompt += 'Cities:\n' + formatFrequencyBlock(data.geography.cities, total) + '\n\n';

    prompt += '=== SERVICE METRICS ===\n';
    prompt += 'Total Requests: ' + total + '\n';
    prompt += 'Picked Up: ' + data.totalPickedUp + ' (' + Math.round((data.totalPickedUp / total) * 100) + '%)\n';
    prompt += 'Baby Boxes: ' + data.babyBoxCount + '\n';
    prompt += 'Service Statuses:\n' + formatFrequencyBlock(data.service.statuses, total) + '\n';
    prompt += 'Distribution Codes:\n' + formatFrequencyBlock(data.distributionCodes, total) + '\n';
    prompt += 'Request Types:\n' + formatFrequencyBlock(data.service.requestTypes, total) + '\n\n';

    prompt += '=== DEMOGRAPHIC PATTERNS ===\n';
    prompt += 'Race:\n' + formatFrequencyBlock(data.demographics.races, total) + '\n';
    prompt += 'Age Brackets:\n' + formatFrequencyBlock(data.demographics.ages, total) + '\n';
    prompt += 'Income Levels:\n' + formatFrequencyBlock(data.income.levels, total) + '\n\n';

    prompt += '=== HOUSEHOLD DATA ===\n';
    prompt += 'Avg Household Size: ' + data.householdStats.avgSize + '\n';
    prompt += 'Total Individuals Served: ' + data.householdStats.totalIndividuals + '\n';

    return prompt;
}

/**
 * Build the user prompt for Testimonial Summary generation
 * Includes all available testimonials with count context
 * @param {Object} data - Output from gatherAIReportData
 * @returns {string} Formatted prompt text
 */
function buildTestimonialPrompt(data) {
    var prompt = 'Synthesize the following client testimonials from Giving to the Nations.\n\n';
    prompt += 'REPORTING PERIOD: ' + data.period.from + ' to ' + data.period.to + '\n';
    prompt += 'TOTAL TESTIMONIALS: ' + data.testimonials.length + '\n';
    prompt += 'TOTAL REQUESTS IN PERIOD: ' + data.totalRecords + '\n\n';

    prompt += 'CONTEXT: The organization serves ' + data.householdStats.totalIndividuals +
        ' individuals across ' + Object.keys(data.geography.counties).length + ' counties.\n';
    prompt += 'Top counties: ' + sortedEntries(data.geography.counties).slice(0, 5)
        .map(function (e) { return e[0]; }).join(', ') + '\n\n';

    prompt += '=== TESTIMONIALS ===\n';
    var maxTestimonials = Math.min(data.testimonials.length, 50);
    for (var t = 0; t < maxTestimonials; t++) {
        prompt += (t + 1) + '. "' + data.testimonials[t] + '"\n\n';
    }
    if (data.testimonials.length > 50) {
        prompt += '(Showing 50 of ' + data.testimonials.length + ' testimonials)\n';
    }

    return prompt;
}

/**
 * Build the user prompt for Executive Summary generation
 * Provides high-level metrics across all dimensions
 * @param {Object} data - Output from gatherAIReportData
 * @returns {string} Formatted prompt text
 */
function buildExecutiveSummaryPrompt(data) {
    var total = data.totalRecords;
    var prompt = 'Create an executive summary for Giving to the Nations leadership.\n\n';
    prompt += 'REPORTING PERIOD: ' + data.period.from + ' to ' + data.period.to + '\n\n';

    prompt += '=== KEY METRICS ===\n';
    prompt += 'Total Requests: ' + total + '\n';
    prompt += 'Requests Fulfilled (Picked Up): ' + data.totalPickedUp + '\n';
    prompt += 'Fulfillment Rate: ' + (total > 0 ? Math.round((data.totalPickedUp / total) * 100) : 0) + '%\n';
    prompt += 'Baby Boxes Distributed: ' + data.babyBoxCount + '\n';
    prompt += 'Counties Served: ' + Object.keys(data.geography.counties).length + '\n';
    prompt += 'Cities Served: ' + Object.keys(data.geography.cities).length + '\n';
    prompt += 'Estimated Individuals Served: ' + data.householdStats.totalIndividuals + '\n';
    prompt += 'Average Household Size: ' + data.householdStats.avgSize + '\n';
    prompt += 'Testimonials Received: ' + data.testimonials.length + '\n\n';

    prompt += '=== MONTHLY VOLUME ===\n';
    var months = Object.keys(data.monthlyVolume).sort();
    for (var m = 0; m < months.length; m++) {
        prompt += '  ' + months[m] + ': ' + data.monthlyVolume[months[m]] + '\n';
    }
    prompt += '\n';

    prompt += '=== TOP DEMOGRAPHICS ===\n';
    prompt += 'Top 3 Race Categories:\n';
    sortedEntries(data.demographics.races).slice(0, 3).forEach(function (e) {
        prompt += '  ' + e[0] + ': ' + e[1] + ' (' + Math.round((e[1] / total) * 100) + '%)\n';
    });
    prompt += 'Top 3 Age Brackets:\n';
    sortedEntries(data.demographics.ages).slice(0, 3).forEach(function (e) {
        prompt += '  ' + e[0] + ': ' + e[1] + ' (' + Math.round((e[1] / total) * 100) + '%)\n';
    });
    prompt += '\n';

    prompt += '=== TOP COUNTIES ===\n';
    sortedEntries(data.geography.counties).slice(0, 5).forEach(function (e) {
        prompt += '  ' + e[0] + ': ' + e[1] + ' (' + Math.round((e[1] / total) * 100) + '%)\n';
    });
    prompt += '\n';

    prompt += '=== INCOME DISTRIBUTION ===\n';
    prompt += formatFrequencyBlock(data.income.levels, total) + '\n\n';

    prompt += '=== FUNDING SOURCES ===\n';
    prompt += formatFrequencyBlock(data.service.fundingSources, total) + '\n\n';

    prompt += '=== SERVICE STATUSES ===\n';
    prompt += formatFrequencyBlock(data.service.statuses, total) + '\n';

    return prompt;
}

// ============ OUTPUT / SAVE ============

/**
 * Save AI-generated report as Google Doc or Google Sheet
 * Moves to Grants folder and returns Open/Download URLs
 * @param {string} aiText - Generated text from Claude API
 * @param {string} reportType - Report type name for filename
 * @param {string} fromDate - Start date for filename
 * @param {string} toDate - End date for filename
 * @param {string} outputFormat - 'doc' or 'sheet'
 * @param {number} tokensUsed - API tokens consumed
 * @returns {Object} { success, reportUrl, downloadUrl, message, tokensUsed }
 */
function saveAIReport(aiText, reportType, fromDate, toDate, outputFormat, tokensUsed) {
    var datestamp = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd_HHmm');
    var reportName = 'AI_' + reportType + '_' + fromDate + '_to_' + toDate + '_' + datestamp;

    if (outputFormat === 'doc') {
        return saveAsGoogleDoc(aiText, reportName, reportType, tokensUsed);
    } else {
        return saveAsGoogleSheet(aiText, reportName, reportType, tokensUsed);
    }
}

/**
 * Save AI text as a formatted Google Doc
 * Applies basic formatting: title bold/large, section headers bold
 * @param {string} text - AI-generated text
 * @param {string} reportName - Filename for the document
 * @param {string} reportType - Report type for audit log
 * @param {number} tokensUsed - API tokens consumed
 * @returns {Object} { success, reportUrl, downloadUrl, message, tokensUsed }
 */
function saveAsGoogleDoc(text, reportName, reportType, tokensUsed) {
    try {
        var doc = DocumentApp.create(reportName);
        var body = doc.getBody();

        // Add title
        var title = body.appendParagraph('Giving to the Nations');
        title.setHeading(DocumentApp.ParagraphHeading.HEADING1);
        title.setAlignment(DocumentApp.HorizontalAlignment.CENTER);

        var subtitle = body.appendParagraph(reportType.replace(/_/g, ' '));
        subtitle.setHeading(DocumentApp.ParagraphHeading.HEADING2);
        subtitle.setAlignment(DocumentApp.HorizontalAlignment.CENTER);

        var dateLine = body.appendParagraph('Generated: ' +
            Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'MMMM d, yyyy h:mm a'));
        dateLine.setAlignment(DocumentApp.HorizontalAlignment.CENTER);
        dateLine.setItalic(true);

        body.appendParagraph(''); // spacer

        // Parse AI text and apply basic formatting
        var lines = text.split('\n');
        for (var i = 0; i < lines.length; i++) {
            var line = lines[i];

            // Detect markdown-style headers
            if (line.match(/^#{1,3}\s/)) {
                var headerText = line.replace(/^#{1,3}\s*/, '');
                var level = (line.match(/^(#{1,3})/)[1]).length;
                var heading;
                if (level === 1) {
                    heading = body.appendParagraph(headerText);
                    heading.setHeading(DocumentApp.ParagraphHeading.HEADING1);
                } else if (level === 2) {
                    heading = body.appendParagraph(headerText);
                    heading.setHeading(DocumentApp.ParagraphHeading.HEADING2);
                } else {
                    heading = body.appendParagraph(headerText);
                    heading.setHeading(DocumentApp.ParagraphHeading.HEADING3);
                }
            } else if (line.match(/^\*\*[^*]+\*\*$/)) {
                // Bold-only lines as subheaders
                var boldText = line.replace(/\*\*/g, '');
                var boldPara = body.appendParagraph(boldText);
                boldPara.setBold(true);
            } else if (line.match(/^[-•]\s/)) {
                // Bullet points
                var bulletText = line.replace(/^[-•]\s*/, '');
                // Clean inline bold markers
                bulletText = bulletText.replace(/\*\*/g, '');
                body.appendListItem(bulletText).setGlyphType(DocumentApp.GlyphType.BULLET);
            } else if (line.trim() === '') {
                body.appendParagraph('');
            } else {
                // Regular paragraph — clean inline markdown
                var cleanLine = line.replace(/\*\*/g, '');
                body.appendParagraph(cleanLine);
            }
        }

        // Add footer
        body.appendParagraph('');
        var footer = body.appendParagraph('Generated by AI (Anthropic Claude) — ' +
            'Tokens used: ' + tokensUsed);
        footer.setItalic(true);
        footer.setFontSize(9);
        footer.setForegroundColor('#888888');

        doc.saveAndClose();

        // Move to Grants folder
        moveToFolder(doc.getId(), CONFIG.GRANTS_FOLDER_ID);

        logAudit('AI_REPORT', null, 'Generated AI ' + reportType + ' (Doc) — ' + tokensUsed + ' tokens');

        return {
            success: true,
            reportUrl: doc.getUrl(),
            downloadUrl: 'https://docs.google.com/document/d/' + doc.getId() + '/export?format=docx',
            message: reportType.replace(/_/g, ' ') + ' generated as Google Doc',
            tokensUsed: tokensUsed,
            format: 'doc'
        };

    } catch (e) {
        Logger.log('saveAsGoogleDoc error: ' + e.message);
        return { success: false, error: 'Failed to create Google Doc: ' + e.message };
    }
}

/**
 * Save AI text as a Google Sheet with AI Analysis tab
 * First tab contains the narrative text in wrapped cells
 * @param {string} text - AI-generated text
 * @param {string} reportName - Filename for the spreadsheet
 * @param {string} reportType - Report type for audit log
 * @param {number} tokensUsed - API tokens consumed
 * @returns {Object} { success, reportUrl, downloadUrl, message, tokensUsed }
 */
function saveAsGoogleSheet(text, reportName, reportType, tokensUsed) {
    try {
        var ss = SpreadsheetApp.create(reportName);
        var sheet = ss.getActiveSheet();
        sheet.setName('AI Analysis');

        // Title row
        sheet.getRange(1, 1).setValue('Giving to the Nations — AI ' + reportType.replace(/_/g, ' '));
        sheet.getRange(1, 1).setFontWeight('bold').setFontSize(14);

        sheet.getRange(2, 1).setValue('Generated: ' +
            Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'MMMM d, yyyy h:mm a'));
        sheet.getRange(2, 1).setFontStyle('italic');

        sheet.getRange(3, 1).setValue('Tokens Used: ' + tokensUsed);
        sheet.getRange(3, 1).setFontSize(9).setFontColor('#888888');

        // Write AI text starting at row 5
        var lines = text.split('\n');
        var currentRow = 5;
        for (var i = 0; i < lines.length; i++) {
            var cell = sheet.getRange(currentRow, 1);
            var line = lines[i].replace(/\*\*/g, ''); // Clean markdown bold

            if (lines[i].match(/^#{1,3}\s/)) {
                cell.setValue(line.replace(/^#{1,3}\s*/, ''));
                cell.setFontWeight('bold').setFontSize(12).setBackground('#e8f0fe');
            } else {
                cell.setValue(line);
            }
            cell.setWrap(true);
            currentRow++;
        }

        // Set column width for readability
        sheet.setColumnWidth(1, 800);

        // Move to Grants folder
        moveToFolder(ss.getId(), CONFIG.GRANTS_FOLDER_ID);

        logAudit('AI_REPORT', null, 'Generated AI ' + reportType + ' (Sheet) — ' + tokensUsed + ' tokens');

        return {
            success: true,
            reportUrl: ss.getUrl(),
            downloadUrl: 'https://docs.google.com/spreadsheets/d/' + ss.getId() + '/export?format=xlsx',
            message: reportType.replace(/_/g, ' ') + ' generated as Google Sheet',
            tokensUsed: tokensUsed,
            format: 'sheet'
        };

    } catch (e) {
        Logger.log('saveAsGoogleSheet error: ' + e.message);
        return { success: false, error: 'Failed to create Google Sheet: ' + e.message };
    }
}

// ============ AI CUSTOM DATA SHEETS ============

/**
 * Get grouped AM column headers for field picker UI
 * v1.9: Delegates to FieldMapService.getFieldsByGroup() for sheet-driven
 * field groups from LU_FieldMap. Computed and Summary fields also sourced
 * from FieldMapService. Reads headers dynamically from Applicants_Master
 * to validate which fields actually exist.
 * @returns {Object} { success, groups: [{name, fields: [{value, label}]}] }
 */
function getAMFieldGroups() {
    try {
        var sheet = getMasterSheet();
        if (!sheet) return { success: false, error: 'Master sheet not found' };

        var headers = trimHeaders(
            sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0]
        ).filter(function (h) { return h.length > 0; });

        // v1.9: Get field groups from FieldMapService (reads LU_FieldMap sheet)
        var groups = getFieldsByGroup(headers);

        // Add Computed Fields group (calculated at report time, not in AM)
        var computedFields = getComputedFields();
        if (computedFields.length > 0) {
            groups.push({ name: 'Computed Fields (Per Row)', fields: computedFields });
        }

        // Add Summary-only computed fields (appear in Summary section, not as columns)
        var summaryFields = getSummaryFields();
        if (summaryFields.length > 0) {
            groups.push({ name: 'Computed Summaries', fields: summaryFields });
        }

        return { success: true, groups: groups };

    } catch (e) {
        Logger.log('getAMFieldGroups error: ' + e.message);
        return { success: false, error: 'Failed to retrieve field list: ' + e.message };
    }
}

/**
 * Generate AI Custom Data Sheet — Two-step approach:
 *   Step 1: Claude interprets natural language instructions into a small JSON spec
 *           (filters, sort order, computed columns, title) — NO raw data sent
 *   Step 2: GAS applies the spec to actual data and writes the formatted Sheet
 * @param {string} fromDate - YYYY-MM-DD start date
 * @param {string} toDate - YYYY-MM-DD end date
 * @param {string[]} selectedFields - Array of column header names (can be empty for free-text mode)
 * @param {string} prompt - Free-text instructions for structuring/computing/grouping
 * @param {string} incomeBracketRange - Income bracket grouping: 'asis','3tier','5tier' (optional)
 * @returns {Object} { success, reportUrl, downloadUrl, message, tokensUsed }
 */
function generateAIDataSheet(fromDate, toDate, selectedFields, prompt, incomeBracketRange) {
    try {
        if ((!selectedFields || selectedFields.length === 0) && (!prompt || prompt.trim().length === 0)) {
            return { success: false, error: 'Please select fields and/or provide instructions' };
        }

        var from = parseDateInput(fromDate, false);
        var to = parseDateInput(toDate, true);
        var combined = getCombinedData(from, to);

        if (combined.totalCount === 0) {
            return { success: false, error: 'No records found for the specified date range' };
        }

        var headers = combined.headers;
        var rows = combined.rows;

        // Determine which fields to use
        var useFields = (selectedFields && selectedFields.length > 0)
            ? selectedFields
            : headers.filter(function (h) { return h && h.trim().length > 0 && h !== 'Timestamp'; });

        // Separate computed fields, summary fields, and regular AM fields
        var computedFieldNames = [];
        var summaryFieldNames = [];
        var regularFields = [];
        var hasIncomeSourceExpanded = false;
        for (var sf = 0; sf < useFields.length; sf++) {
            if (useFields[sf].indexOf('[Summary]') === 0) {
                summaryFieldNames.push(useFields[sf]);
            } else if (useFields[sf].indexOf('[Calc]') === 0) {
                computedFieldNames.push(useFields[sf]);
                if (useFields[sf] === '[Calc] Income Source (Expanded)') hasIncomeSourceExpanded = true;
            } else {
                regularFields.push(useFields[sf]);
            }
        }

        // Map and validate regular field indices
        var fieldIndices = [];
        var validFields = [];
        for (var f = 0; f < regularFields.length; f++) {
            var idx = headers.indexOf(regularFields[f]);
            if (idx !== -1) {
                fieldIndices.push(idx);
                validFields.push(regularFields[f]);
            }
        }

        if (validFields.length === 0 && computedFieldNames.length === 0 && summaryFieldNames.length === 0) {
            return { success: false, error: 'None of the selected fields were found in the data' };
        }

        // === COMPUTE CALCULATED FIELDS ===
        var computedHeaders = [];
        var computedData = []; // array of arrays, one per row

        if (computedFieldNames.length > 0) {
            // Pre-load product data using shared function if Products fields are selected
            var needProducts = computedFieldNames.some(function (f) {
                return f.indexOf('Products') !== -1;
            });
            var productData = null;
            if (needProducts) {
                productData = loadProductLookupData(from, to);
            }

            // Column indices needed for computations
            var idIdx = headers.indexOf('ID');
            var reqDateIdx = headers.indexOf('Request Date');
            var usedBeforeIdx = headers.indexOf('Have you used our services before?');
            var incomeColIdx = headers.indexOf('Total annual household income. (How much money does your family make in 1 year?)');
            var assistanceColIdx = headers.indexOf('Are you receiving any assistance? Please select ALL boxes that apply to ANY ASSISTANCE your family is receiving.');
            var prodCode1Idx = headers.indexOf('Received Product Code 1');
            var prodCode2Idx = headers.indexOf('Received Product Code 2');
            var prodCode3Idx = headers.indexOf('Received Product Code 3');
            var bracketRange = incomeBracketRange || 'asis';

            // Check which age-based fields are needed
            var needAges = computedFieldNames.some(function (f) {
                return f.indexOf('People') !== -1 || f.indexOf('Children') !== -1 ||
                    f.indexOf('Adults') !== -1 || f.indexOf('Seniors') !== -1;
            });

            // Compute each row
            for (var cr = 0; cr < rows.length; cr++) {
                var row = rows[cr];
                var computedRow = [];

                // Calculate age brackets once per row if needed (shared function)
                var ages = null;
                if (needAges) {
                    ages = calculateDetailedAgeBrackets(headers, row);
                }

                // Calculate products once per row if needed (shared function)
                var products = null;
                if (needProducts && productData && idIdx !== -1 && reqDateIdx !== -1) {
                    var recId = (row[idIdx] || '').toString().trim();
                    var code1 = prodCode1Idx !== -1 ? (row[prodCode1Idx] || '').toString().trim() : '';
                    var code2 = prodCode2Idx !== -1 ? (row[prodCode2Idx] || '').toString().trim() : '';
                    var code3 = prodCode3Idx !== -1 ? (row[prodCode3Idx] || '').toString().trim() : '';
                    var reqDate = new Date(row[reqDateIdx]);
                    products = calculateProductCounts(recId, code1, code2, code3, reqDate, productData, headers, row);
                }

                for (var cf = 0; cf < computedFieldNames.length; cf++) {
                    var calcName = computedFieldNames[cf];
                    var calcVal = '';

                    switch (calcName) {
                        case '[Calc] People in Household':
                            calcVal = ages ? ages.people : 0;
                            break;
                        case '[Calc] Children (<18)':
                            calcVal = ages ? ages.totalChildren : 0;
                            break;
                        case '[Calc] Adults (18-64)':
                            calcVal = ages ? ages.totalAdults : 0;
                            break;
                        case '[Calc] Seniors (65+)':
                            calcVal = ages ? ages.totalSeniors : 0;
                            break;
                        case '[Calc] Male Children':
                            calcVal = ages ? ages.maleChildren : 0;
                            break;
                        case '[Calc] Female Children':
                            calcVal = ages ? ages.femaleChildren : 0;
                            break;
                        case '[Calc] Applicant Type':
                            var usedBefore = usedBeforeIdx !== -1 ? (row[usedBeforeIdx] || '').toString().trim() : '';
                            calcVal = getApplicantType(usedBefore);
                            break;
                        case '[Calc] Income Bracket':
                            var rawIncome = incomeColIdx !== -1 ? (row[incomeColIdx] || '').toString().trim() : '';
                            calcVal = mapIncomeBracket(rawIncome, bracketRange);
                            break;
                        case '[Calc] Income Source (Expanded)':
                            // Store raw multi-value; expansion happens after computed columns are built
                            calcVal = assistanceColIdx !== -1 ? (row[assistanceColIdx] || '').toString().trim() : '';
                            break;
                        case '[Calc] Products Requested':
                            calcVal = products ? products.productsRequested : 0;
                            break;
                        case '[Calc] Products Distributed':
                            calcVal = products ? products.productsDistributed : 0;
                            break;
                    }
                    computedRow.push(calcVal);
                }
                computedData.push(computedRow);
            }

            computedHeaders = computedFieldNames;
        }

        // Extend headers and rows with computed columns
        var allHeaders = headers.concat(computedHeaders);
        var allRows = [];
        for (var ar = 0; ar < rows.length; ar++) {
            var extRow = rows[ar].slice();
            if (computedData[ar]) {
                extRow = extRow.concat(computedData[ar]);
            }
            allRows.push(extRow);
        }

        // === INCOME SOURCE EXPANSION ===
        // If [Calc] Income Source (Expanded) is selected, split rows by comma-separated values
        if (hasIncomeSourceExpanded) {
            var srcColIdx = allHeaders.indexOf('[Calc] Income Source (Expanded)');
            if (srcColIdx !== -1) {
                var expandedRows = [];
                for (var er = 0; er < allRows.length; er++) {
                    var srcVal = (allRows[er][srcColIdx] || '').toString().trim();
                    if (srcVal.indexOf(', ') !== -1) {
                        var sources = srcVal.split(', ');
                        for (var es = 0; es < sources.length; es++) {
                            var newRow = allRows[er].slice();
                            newRow[srcColIdx] = sources[es].trim();
                            expandedRows.push(newRow);
                        }
                    } else {
                        expandedRows.push(allRows[er]);
                    }
                }
                allRows = expandedRows;
                Logger.log('Income Source expansion: ' + rows.length + ' → ' + allRows.length + ' rows');
            }
        }

        // Add computed fields to validFields and fieldIndices
        for (var ch = 0; ch < computedHeaders.length; ch++) {
            var chIdx = allHeaders.indexOf(computedHeaders[ch]);
            if (chIdx !== -1) {
                validFields.push(computedHeaders[ch]);
                fieldIndices.push(chIdx);
            }
        }

        // === BUILD SUMMARY COMPUTATIONS ===
        // These are auto-added to the summary section, not as columns
        var autoSummary = [];
        if (summaryFieldNames.length > 0) {
            var firstNameIdx = headers.indexOf('First Name');
            var lastNameIdx = headers.indexOf('Last Name');

            for (var sm = 0; sm < summaryFieldNames.length; sm++) {
                switch (summaryFieldNames[sm]) {
                    case '[Summary] Number of Households':
                        // Count unique First+Last Name combinations
                        var householdKeys = {};
                        for (var hr = 0; hr < allRows.length; hr++) {
                            var fn = firstNameIdx !== -1 ? (allRows[hr][firstNameIdx] || '').toString().trim().toLowerCase() : '';
                            var ln = lastNameIdx !== -1 ? (allRows[hr][lastNameIdx] || '').toString().trim().toLowerCase() : '';
                            if (fn || ln) householdKeys[fn + '|' + ln] = true;
                        }
                        autoSummary.push(['Number of Households (Unique Names)', Object.keys(householdKeys).length]);
                        break;

                    case '[Summary] Number of Requests':
                        autoSummary.push(['Number of Requests', allRows.length]);
                        break;

                    case '[Summary] Age Bracket Totals':
                        // Sum each age bracket across all rows
                        var bracketTotals = {};
                        for (var ab = 0; ab < AGE_BRACKETS.length; ab++) {
                            var brk = AGE_BRACKETS[ab];
                            var mIdx = headers.indexOf(brk.male);
                            var fIdx = headers.indexOf(brk.female);
                            var mTotal = 0, fTotal = 0;
                            for (var br = 0; br < allRows.length; br++) {
                                mTotal += mIdx !== -1 ? (parseInt(allRows[br][mIdx]) || 0) : 0;
                                fTotal += fIdx !== -1 ? (parseInt(allRows[br][fIdx]) || 0) : 0;
                            }
                            // Extract bracket label from column name like "[Males under 5]" → "Under 5"
                            var label = brk.male.replace('[Males ', '').replace(']', '');
                            label = label.charAt(0).toUpperCase() + label.slice(1);
                            bracketTotals[label] = { male: mTotal, female: fTotal, total: mTotal + fTotal };
                        }
                        var totalPeople = 0;
                        for (var bKey in bracketTotals) {
                            var bt = bracketTotals[bKey];
                            autoSummary.push(['Age: ' + bKey, bt.total + ' (M:' + bt.male + ' F:' + bt.female + ')']);
                            totalPeople += bt.total;
                        }
                        autoSummary.push(['Total People', totalPeople]);
                        break;
                }
            }
        }

        // === STEP 1: Ask Claude ONLY to interpret instructions into a spec ===
        // NO raw data — just field names, unique values for filter fields, and sample rows

        // v1.9: Build display labels and reverse map from FieldMapService
        var displayLabels = buildDisplayLabels(validFields);
        var labelToRaw = buildLabelToRawMap(validFields);

        // Get unique values for filterable fields (using display labels as keys)
        var uniqueValues = {};
        var filterCandidates = ['Service Status', 'County', 'City', 'State', 'Funding Source',
            'Request Type', 'Military Status', 'Please Select Your Racial Category',
            'Please Select Your Ethnic Category', 'Are you currently employed?',
            'Have you used our services before?', 'Are you currently homeless?',
            '[Calc] Applicant Type'];
        for (var fc = 0; fc < filterCandidates.length; fc++) {
            var fcName = filterCandidates[fc];
            var fcIdx = allHeaders.indexOf(fcName);
            if (fcIdx !== -1 && validFields.indexOf(fcName) !== -1) {
                var uVals = {};
                for (var ur = 0; ur < allRows.length; ur++) {
                    var uv = (allRows[ur][fcIdx] || '').toString().trim();
                    if (uv) uVals[uv] = true;
                }
                // v1.9: Use display label as key (via FieldMapService)
                uniqueValues[getFieldDisplayLabel(fcName)] = Object.keys(uVals).sort();
            }
        }

        // Get 3 sample rows using display labels as keys
        var sampleRows = [];
        var sampleCount = Math.min(allRows.length, 3);
        for (var s = 0; s < sampleCount; s++) {
            var sRow = {};
            for (var sc = 0; sc < validFields.length; sc++) {
                var sVal = allRows[s][fieldIndices[sc]];
                if (sVal instanceof Date) sVal = Utilities.formatDate(sVal, CONFIG.TIMEZONE, 'M/d/yyyy');
                // v1.12: Format phone number in samples so Claude sees the display format
                if (validFields[sc] === 'Phone Number') sVal = formatPhoneNumber_(sVal);
                sRow[displayLabels[sc]] = (sVal !== null && sVal !== undefined) ? sVal.toString() : '';
            }
            sampleRows.push(sRow);
        }

        var systemPrompt =
            'You translate data report instructions into a JSON spec. Return ONLY valid JSON.\n' +
            'CRITICAL: Output MUST start with { and end with }. No text before or after.\n\n' +
            'JSON structure:\n' +
            '{\n' +
            '  "title": "Report title",\n' +
            '  "outputColumns": ["Field1", "Field2"],\n' +
            '  "filters": [{"field":"FieldName","op":"exclude|include|notBlank|isBlank","values":["val1"]}],\n' +
            '  "sort": [{"field":"FieldName","dir":"asc|desc"}],\n' +
            '  "computations": [{"name":"Display Name","type":"count|sum|avg|pct|countDistinct","field":"FieldName"}],\n' +
            '  "notes": "Brief description of what this report shows"\n' +
            '}\n\n' +
            'Rules:\n' +
            '- outputColumns: field names for the output (from AVAILABLE FIELDS only)\n' +
            '- filters op: "exclude" removes rows matching values, "include" keeps only matching, "notBlank" removes empty, "isBlank" keeps only empty\n' +
            '- For blank/empty filtering, use values:[""] with exclude, or use "notBlank" op\n' +
            '- sort: array of sort levels. Date fields will be parsed as dates automatically.\n' +
            '- computations: summary stats shown below the data table\n' +
            '- Use EXACT field names from the AVAILABLE FIELDS list\n' +
            '- Respond with ONLY the JSON object, nothing else';

        var userMessage = 'AVAILABLE FIELDS:\n' + JSON.stringify(displayLabels) + '\n\n';

        if (Object.keys(uniqueValues).length > 0) {
            userMessage += 'UNIQUE VALUES IN FILTERABLE FIELDS:\n';
            for (var uvKey in uniqueValues) {
                userMessage += uvKey + ': ' + JSON.stringify(uniqueValues[uvKey]) + '\n';
            }
            userMessage += '\n';
        }

        userMessage += 'SAMPLE DATA (' + sampleCount + ' of ' + allRows.length + ' total rows):\n';
        userMessage += JSON.stringify(sampleRows) + '\n\n';
        userMessage += 'USER INSTRUCTIONS: ' + (prompt || 'Show all selected fields, sorted logically. Add summary counts.');

        Logger.log('AI Data Sheet prompt length: ' + userMessage.length + ' chars');

        var specResult = callClaudeAPI(systemPrompt, userMessage);
        if (!specResult.success) return specResult;

        Logger.log('AI raw response (first 500): ' + specResult.text.substring(0, 500));

        // Parse spec JSON with robust extraction
        var spec = extractJSON(specResult.text);
        if (!spec) {
            Logger.log('Spec parse FAILED. Full response: ' + specResult.text);
            return { success: false, error: 'AI could not interpret instructions. Check Apps Script execution log for details.' };
        }

        Logger.log('AI Spec parsed OK: ' + JSON.stringify(spec).substring(0, 300));

        // === STEP 2: GAS applies spec to data (using extended headers/rows with computed columns) ===
        // v1.9: labelToRaw and displayLabels built by FieldMapService
        var sheetResult = applySpecAndWriteSheet(spec, allHeaders, allRows, validFields, fieldIndices,
            fromDate, toDate, specResult.tokensUsed, autoSummary, labelToRaw, displayLabels);

        // v1.11: Include spec + inputs in result so AP can offer "Save as Template"
        if (sheetResult.success) {
            sheetResult.savedSpec = spec;
            sheetResult.savedFields = selectedFields || [];
            sheetResult.savedPrompt = prompt || '';
            sheetResult.savedBracket = incomeBracketRange || 'asis';
        }
        return sheetResult;

    } catch (e) {
        Logger.log('generateAIDataSheet error: ' + e.message);
        return { success: false, error: 'Data sheet generation failed: ' + e.message };
    }
}

/**
 * Robustly extract JSON object from Claude's response text
 * Handles: raw JSON, markdown code fences, leading/trailing text
 * @param {string} text - Raw response from Claude API
 * @returns {Object|null} Parsed JSON object or null if extraction fails
 */
function extractJSON(text) {
    if (!text || text.trim().length === 0) return null;

    // Strip markdown code fences
    var clean = text.replace(/```json\s*/gi, '').replace(/```\s*/gi, '').trim();

    // Attempt 1: Direct parse
    try {
        return JSON.parse(clean);
    } catch (e) { /* continue */ }

    // Attempt 2: Find outermost { ... }
    var firstBrace = clean.indexOf('{');
    var lastBrace = clean.lastIndexOf('}');
    if (firstBrace !== -1 && lastBrace > firstBrace) {
        try {
            return JSON.parse(clean.substring(firstBrace, lastBrace + 1));
        } catch (e) { /* continue */ }
    }

    Logger.log('extractJSON: All parse attempts failed');
    return null;
}

/**
 * Apply AI-generated spec to raw data and write formatted Google Sheet
 * @param {Object} spec - AI spec { title, outputColumns, filters, sort, computations, notes }
 * @param {Array} headers - All headers (including computed)
 * @param {Array} rows - All data rows (including computed columns)
 * @param {Array} validFields - Selected field names (raw AM headers)
 */
function applySpecAndWriteSheet(spec, headers, rows, validFields, fieldIndices, fromDate, toDate, tokensUsed, autoSummary, labelToRaw, displayLabels) {
    try {
        // v1.7: Helper to resolve a field name (display label or raw) to raw header name
        labelToRaw = labelToRaw || {};
        displayLabels = displayLabels || validFields;
        function resolveField(name) {
            return labelToRaw[name] || name;
        }

        // Determine output columns — Claude returns display labels, resolve to raw
        var outCols = spec.outputColumns && spec.outputColumns.length > 0
            ? spec.outputColumns : displayLabels;

        // Map output column indices from master headers
        var outIndices = [];
        var finalCols = [];       // display labels for sheet headers
        var finalRawCols = [];    // raw names for data lookup
        for (var oc = 0; oc < outCols.length; oc++) {
            var rawName = resolveField(outCols[oc]);
            var oi = headers.indexOf(rawName);
            if (oi !== -1) {
                outIndices.push(oi);
                finalCols.push(outCols[oc]);  // keep display label for output
                finalRawCols.push(rawName);
            }
        }

        // Fall back to validFields if no output columns matched
        if (finalCols.length === 0) {
            for (var vf = 0; vf < validFields.length; vf++) {
                outIndices.push(fieldIndices[vf]);
                finalCols.push(displayLabels[vf]);
                finalRawCols.push(validFields[vf]);
            }
        }

        // Apply filters (v1.7: resolve display labels to raw headers)
        var filteredRows = rows;
        if (spec.filters && spec.filters.length > 0) {
            filteredRows = rows.filter(function (row) {
                for (var fi = 0; fi < spec.filters.length; fi++) {
                    var filter = spec.filters[fi];
                    var fIdx = headers.indexOf(resolveField(filter.field));
                    if (fIdx === -1) continue;

                    var cellVal = (row[fIdx] !== null && row[fIdx] !== undefined)
                        ? row[fIdx].toString().trim() : '';
                    var cellLower = cellVal.toLowerCase();
                    var filterVals = (filter.values || []).map(function (v) { return v.toString().trim().toLowerCase(); });

                    switch (filter.op) {
                        case 'exclude':
                            // Exclude matching values AND empty if "" is in values
                            if (filterVals.indexOf(cellLower) !== -1) return false;
                            if (filterVals.indexOf('') !== -1 && cellVal === '') return false;
                            break;
                        case 'include':
                            if (filterVals.indexOf(cellLower) === -1) return false;
                            break;
                        case 'notBlank':
                            if (cellVal === '') return false;
                            break;
                        case 'isBlank':
                            if (cellVal !== '') return false;
                            break;
                    }
                }
                return true;
            });
        }

        // Apply sort (v1.7: resolve display labels to raw headers)
        if (spec.sort && spec.sort.length > 0) {
            filteredRows.sort(function (a, b) {
                for (var si = 0; si < spec.sort.length; si++) {
                    var sortSpec = spec.sort[si];
                    var sIdx = headers.indexOf(resolveField(sortSpec.field));
                    if (sIdx === -1) continue;

                    var aVal = a[sIdx];
                    var bVal = b[sIdx];
                    var dir = sortSpec.dir === 'desc' ? -1 : 1;

                    // Date comparison
                    if (aVal instanceof Date && bVal instanceof Date) {
                        if (aVal.getTime() !== bVal.getTime()) return (aVal - bVal) * dir;
                        continue;
                    }
                    // Try parsing as dates if strings look like dates
                    if (typeof aVal === 'string' && typeof bVal === 'string' &&
                        aVal.match(/^\d{1,2}\/\d{1,2}\/\d{4}/) && bVal.match(/^\d{1,2}\/\d{1,2}\/\d{4}/)) {
                        var aDate = new Date(aVal);
                        var bDate = new Date(bVal);
                        if (!isNaN(aDate) && !isNaN(bDate) && aDate.getTime() !== bDate.getTime()) {
                            return (aDate - bDate) * dir;
                        }
                        continue;
                    }

                    // Numeric comparison
                    var aNum = parseFloat(aVal);
                    var bNum = parseFloat(bVal);
                    if (!isNaN(aNum) && !isNaN(bNum)) {
                        if (aNum !== bNum) return (aNum - bNum) * dir;
                        continue;
                    }

                    // String comparison
                    var aStr = (aVal || '').toString().toLowerCase();
                    var bStr = (bVal || '').toString().toLowerCase();
                    var cmp = aStr.localeCompare(bStr);
                    if (cmp !== 0) return cmp * dir;
                }
                return 0;
            });
        }

        // Extract output data
        var outputRows = [];

        // ── v1.16: groupBy aggregation ────────────────────────────────────────────
        // If spec.groupBy is set, produce an aggregated summary table instead of raw rows.
        // spec.groupBy  = "FieldName"                            — group-by column
        // spec.aggCols  = [{field, type, label}]                 — columns to aggregate
        //   type: "count" | "countDistinct" | "sum" | "avg" | "pct"
        // outputColumns is ignored when groupBy is active; aggCols defines the columns.
        var isGrouped = !!(spec.groupBy && spec.aggCols && spec.aggCols.length > 0);

        if (isGrouped) {
            var gbRaw = resolveField(spec.groupBy);
            var gbIdx = headers.indexOf(gbRaw);
            if (gbIdx === -1) {
                // Try resolveAMField_ fallback
                gbIdx = headers.indexOf(resolveAMField_(spec.groupBy));
            }

            if (gbIdx !== -1) {
                // Build groups: { groupValue: [rows] }
                var groups = {};
                var groupOrder = [];
                filteredRows.forEach(function (row) {
                    var gv = (row[gbIdx] !== null && row[gbIdx] !== undefined)
                        ? row[gbIdx].toString().trim() : '(blank)';
                    if (!groups[gv]) { groups[gv] = []; groupOrder.push(gv); }
                    groups[gv].push(row);
                });

                // Sort groups alphabetically (numbers sort numerically)
                groupOrder.sort(function (a, b) {
                    var an = parseFloat(a), bn = parseFloat(b);
                    if (!isNaN(an) && !isNaN(bn)) return an - bn;
                    return a.localeCompare(b);
                });

                // Build column headers: group-by field label + agg col labels
                var gbLabel = spec.groupBy;
                var aggHeaders = [gbLabel].concat(spec.aggCols.map(function (ac) { return ac.label || ac.field; }));
                finalCols = aggHeaders;
                numCols = aggHeaders.length;
                outIndices = [];  // not used in grouped path

                // Build output rows
                groupOrder.forEach(function (gv) {
                    var gRows = groups[gv];
                    var outRow = [gv];
                    spec.aggCols.forEach(function (ac) {
                        var acRaw = resolveField(ac.field);
                        var acIdx = headers.indexOf(acRaw);
                        if (acIdx === -1) acIdx = headers.indexOf(resolveAMField_(ac.field));
                        var vals = gRows.map(function (r) {
                            return acIdx !== -1 ? r[acIdx] : '';
                        });
                        var agg = '';
                        switch (ac.type) {
                            case 'count':
                                agg = gRows.length;
                                break;
                            case 'countDistinct':
                                var uniq = {};
                                vals.forEach(function (v) { var s = (v || '').toString().trim(); if (s) uniq[s] = true; });
                                agg = Object.keys(uniq).length;
                                break;
                            case 'sum':
                                agg = vals.reduce(function (s, v) { var n = parseFloat(v); return s + (isNaN(n) ? 0 : n); }, 0);
                                agg = Math.round(agg * 100) / 100;
                                break;
                            case 'avg':
                                var nums2 = vals.filter(function (v) { return !isNaN(parseFloat(v)) && v !== ''; });
                                agg = nums2.length > 0
                                    ? Math.round(nums2.reduce(function (s, v) { return s + parseFloat(v); }, 0) / nums2.length * 100) / 100
                                    : 'N/A';
                                break;
                            case 'pct':
                                var nb = vals.filter(function (v) { return v !== null && v !== undefined && v.toString().trim() !== ''; }).length;
                                agg = filteredRows.length > 0 ? Math.round(nb / filteredRows.length * 1000) / 10 + '%' : '0%';
                                break;
                            default:
                                agg = gRows.length;
                        }
                        outRow.push(agg);
                    });
                    outputRows.push(outRow);
                });

                // Totals row
                var totalsRow = ['TOTAL'];
                spec.aggCols.forEach(function (ac) {
                    switch (ac.type) {
                        case 'count': totalsRow.push(filteredRows.length); break;
                        case 'countDistinct':
                            var acRaw2 = resolveField(ac.field);
                            var acIdx2 = headers.indexOf(acRaw2);
                            var uniqAll = {};
                            filteredRows.forEach(function (r) {
                                var v = (acIdx2 !== -1 ? r[acIdx2] : '');
                                var s = (v || '').toString().trim(); if (s) uniqAll[s] = true;
                            });
                            totalsRow.push(Object.keys(uniqAll).length);
                            break;
                        case 'sum':
                            var acIdx3 = headers.indexOf(resolveField(ac.field));
                            var tot = filteredRows.reduce(function (s, r) {
                                var n = parseFloat(acIdx3 !== -1 ? r[acIdx3] : 0); return s + (isNaN(n) ? 0 : n);
                            }, 0);
                            totalsRow.push(Math.round(tot * 100) / 100);
                            break;
                        default: totalsRow.push('');
                    }
                });
                outputRows.push(totalsRow);
                summaryData = [['Source Records', rows.length], ['Filtered Records', filteredRows.length],
                ['Groups', groupOrder.length]];
            } else {
                // groupBy field not found — fall through to raw row output
                isGrouped = false;
            }
        }

        if (!isGrouped) {
            for (var r = 0; r < filteredRows.length; r++) {
                var outRow2 = [];
                for (var c = 0; c < outIndices.length; c++) {
                    var val = filteredRows[r][outIndices[c]];
                    if (val instanceof Date) {
                        val = Utilities.formatDate(val, CONFIG.TIMEZONE, 'M/d/yyyy');
                    }
                    // v1.12: Format phone number as (XXX) XXX-XXXX for Phone Number column
                    if (finalRawCols[c] === 'Phone Number') {
                        val = formatPhoneNumber_(val);
                    }
                    outRow2.push(val !== null && val !== undefined ? val : '');
                }
                outputRows.push(outRow2);
            }
        }

        // Compute summaries
        var summaryData = [];
        summaryData.push(['Total Records (before filter)', rows.length]);
        summaryData.push(['Filtered Records', filteredRows.length]);

        // Add auto-computed summary fields (Households, Requests, Age Brackets)
        if (autoSummary && autoSummary.length > 0) {
            summaryData.push(['', '']); // spacer
            for (var as = 0; as < autoSummary.length; as++) {
                summaryData.push(autoSummary[as]);
            }
        }

        if (spec.computations && spec.computations.length > 0) {
            for (var ci = 0; ci < spec.computations.length; ci++) {
                var comp = spec.computations[ci];
                var compIdx = headers.indexOf(resolveField(comp.field));
                if (compIdx === -1) continue;

                var compValues = filteredRows.map(function (row) { return row[compIdx]; });
                var compResult = '';

                switch (comp.type) {
                    case 'count':
                        compResult = compValues.filter(function (v) {
                            return v !== null && v !== undefined && v.toString().trim() !== '';
                        }).length;
                        break;
                    case 'countDistinct':
                        var unique = {};
                        compValues.forEach(function (v) {
                            var sv = (v || '').toString().trim();
                            if (sv) unique[sv] = true;
                        });
                        compResult = Object.keys(unique).length;
                        break;
                    case 'sum':
                        compResult = compValues.reduce(function (acc, v) {
                            var n = parseFloat(v);
                            return acc + (isNaN(n) ? 0 : n);
                        }, 0);
                        compResult = Math.round(compResult * 100) / 100;
                        break;
                    case 'avg':
                        var nums = compValues.filter(function (v) { return !isNaN(parseFloat(v)) && v !== ''; });
                        if (nums.length > 0) {
                            var total = nums.reduce(function (acc, v) { return acc + parseFloat(v); }, 0);
                            compResult = Math.round((total / nums.length) * 100) / 100;
                        } else {
                            compResult = 'N/A';
                        }
                        break;
                    case 'pct':
                        var nonBlank = compValues.filter(function (v) {
                            return v !== null && v !== undefined && v.toString().trim() !== '';
                        }).length;
                        compResult = filteredRows.length > 0
                            ? Math.round((nonBlank / filteredRows.length) * 1000) / 10 + '%'
                            : '0%';
                        break;
                }

                summaryData.push([comp.name || comp.field, compResult]);
            }
        }

        // === Write to Google Sheet ===
        var datestamp = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd_HHmm');
        var title = spec.title || 'Custom Data Report';
        var reportName = 'AI_DataSheet_' + fromDate + '_to_' + toDate + '_' + datestamp;

        var ss = SpreadsheetApp.create(reportName);
        var sheet = ss.getActiveSheet();
        sheet.setName('Data');

        var currentRow = 1;
        var numCols = finalCols.length;

        // Title section
        sheet.getRange(currentRow, 1).setValue('Giving to the Nations');
        sheet.getRange(currentRow, 1).setFontWeight('bold').setFontSize(14);
        currentRow++;

        sheet.getRange(currentRow, 1).setValue(title);
        sheet.getRange(currentRow, 1).setFontWeight('bold').setFontSize(12);
        currentRow++;

        var metaText = 'Period: ' + fromDate + ' to ' + toDate +
            ' | Source: ' + rows.length + ' | Filtered: ' + filteredRows.length +
            ' | Generated: ' + Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yyyy h:mm a');
        sheet.getRange(currentRow, 1).setValue(metaText);
        sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(9).setFontColor('#666666');
        currentRow += 2;

        // Column headers
        sheet.getRange(currentRow, 1, 1, numCols).setValues([finalCols]);
        sheet.getRange(currentRow, 1, 1, numCols)
            .setFontWeight('bold')
            .setBackground('#4a86e8')
            .setFontColor('white')
            .setHorizontalAlignment('center');
        sheet.setFrozenRows(currentRow);
        currentRow++;

        // Data rows — batch write for performance
        if (outputRows.length > 0) {
            sheet.getRange(currentRow, 1, outputRows.length, numCols).setValues(outputRows);

            if (isGrouped) {
                // Alternate shading on group rows (not the totals row)
                for (var ar = 1; ar < outputRows.length - 1; ar += 2) {
                    sheet.getRange(currentRow + ar, 1, 1, numCols).setBackground('#f8f9fa');
                }
                // Bold + highlight totals row (last row)
                sheet.getRange(currentRow + outputRows.length - 1, 1, 1, numCols)
                    .setFontWeight('bold').setBackground('#e8f0fe');
            } else {
                // Alternate row shading for raw rows
                for (var ar = 1; ar < outputRows.length; ar += 2) {
                    sheet.getRange(currentRow + ar, 1, 1, numCols).setBackground('#f8f9fa');
                }
            }
            currentRow += outputRows.length;
        }

        // Summary section
        if (summaryData.length > 0) {
            currentRow += 1;
            sheet.getRange(currentRow, 1).setValue('SUMMARY');
            sheet.getRange(currentRow, 1, 1, 2).setFontWeight('bold').setFontSize(11).setBackground('#e8f0fe');
            currentRow++;

            for (var sd = 0; sd < summaryData.length; sd++) {
                sheet.getRange(currentRow, 1).setValue(summaryData[sd][0]).setFontWeight('bold');
                sheet.getRange(currentRow, 2).setValue(summaryData[sd][1]);
                currentRow++;
            }
        }

        // Notes
        if (spec.notes) {
            currentRow += 1;
            sheet.getRange(currentRow, 1).setValue('AI Notes: ' + spec.notes);
            sheet.getRange(currentRow, 1).setFontStyle('italic').setFontSize(9).setFontColor('#888888');
        }

        // v1.9: Auto-resize columns — Column A sized to data values only (not title rows)
        // Calculate Column A width from header + data rows
        var maxLen = (finalCols[0] || '').toString().length;
        for (var ar2 = 0; ar2 < outputRows.length; ar2++) {
            var cellLen = (outputRows[ar2][0] || '').toString().length;
            if (cellLen > maxLen) maxLen = cellLen;
        }
        // ~7px per character + 20px padding, min 80, max 400
        var colAWidth = Math.max(80, Math.min(400, maxLen * 7 + 20));
        sheet.setColumnWidth(1, colAWidth);

        // Auto-resize remaining columns normally
        for (var col = 2; col <= numCols; col++) {
            sheet.autoResizeColumn(col);
        }

        // Move to Grants folder
        moveToFolder(ss.getId(), CONFIG.GRANTS_FOLDER_ID);

        logAudit('AI_REPORT', null, 'Generated AI Data Sheet (' + outputRows.length +
            ' rows, ' + numCols + ' cols) — ' + tokensUsed + ' tokens');

        return {
            success: true,
            reportUrl: ss.getUrl(),
            downloadUrl: 'https://docs.google.com/spreadsheets/d/' + ss.getId() + '/export?format=xlsx',
            message: 'Custom data sheet: ' + outputRows.length + ' rows, ' + numCols + ' columns (filtered from ' + rows.length + ')',
            tokensUsed: tokensUsed,
            format: 'sheet',
            rowCount: outputRows.length,
            colCount: numCols
        };

    } catch (e) {
        Logger.log('applySpecAndWriteSheet error: ' + e.message);
        return { success: false, error: 'Failed to write data sheet: ' + e.message };
    }
}

// ============ DATA SHEET HELPER FUNCTIONS ============

/**
 * Map a raw income level string to a bracket range category
 * Parses dollar amount from strings like "$10,001 - $20,000" and categorizes
 * @param {string} rawIncome - Original income level string from AM
 * @param {string} rangeType - 'asis' (no change), '3tier', or '5tier'
 * @returns {string} Categorized income bracket label
 */
function mapIncomeBracket(rawIncome, rangeType) {
    if (!rawIncome || rawIncome.trim() === '') return '';
    if (rangeType === 'asis') return rawIncome;

    // Extract first dollar amount from the string
    var match = rawIncome.replace(/,/g, '').match(/\$(\d+)/);
    if (!match) return rawIncome; // Can't parse, return as-is

    var amount = parseInt(match[1]);

    if (rangeType === '3tier') {
        if (amount <= 20000) return 'Low (Under $20,000)';
        if (amount <= 50000) return 'Medium ($20,001 - $50,000)';
        return 'High (Over $50,000)';
    }

    if (rangeType === '5tier') {
        if (amount <= 10000) return 'Very Low (Under $10,000)';
        if (amount <= 25000) return 'Low ($10,001 - $25,000)';
        if (amount <= 50000) return 'Medium ($25,001 - $50,000)';
        if (amount <= 75000) return 'High ($50,001 - $75,000)';
        return 'Very High (Over $75,000)';
    }

    return rawIncome;
}

// ============ SAVED CUSTOM DATA SHEET REPORTS ============
// v1.11: Save AI-generated specs for API-free re-run via runSavedDataSheetReport().
// Specs stored in LU_SavedReports sheet in G2N_Lookups workbook.
// Sheet columns: ReportId | ReportName | Description | SelectedFields | IncomeBracketRange | Prompt | Spec | CreatedDate | CreatedBy | Active

/**
 * Creates the LU_SavedReports sheet if it doesn't exist, with styled headers.
 * @param {Spreadsheet} wb - G2N_Lookups workbook
 * @returns {Sheet}
 */
function getOrCreateSavedReportsSheet_(wb) {
    var sheet = wb.getSheetByName(CONFIG.LOOKUPS.SAVED_REPORTS);
    if (!sheet) {
        sheet = wb.insertSheet(CONFIG.LOOKUPS.SAVED_REPORTS);
        var headers = ['ReportId', 'ReportName', 'Description', 'SelectedFields', 'IncomeBracketRange', 'Prompt', 'Spec', 'CreatedDate', 'CreatedBy', 'Active'];
        sheet.getRange(1, 1, 1, headers.length).setValues([headers])
            .setFontWeight('bold').setBackground('#1a73e8').setFontColor('white');
        sheet.setFrozenRows(1);
        sheet.setColumnWidth(2, 180);
        sheet.setColumnWidth(3, 220);
        sheet.setColumnWidth(7, 300);
    }
    return sheet;
}

/**
/**
 * Call Claude API with a full message history (multi-turn conversation).
 * Used by Report Builder to maintain conversational context.
 * v1.13 - New function.
 * @param {string} systemPrompt
 * @param {Array}  messages - [{role:'user'|'assistant', content:'...'}]
 * @returns {Object} { success, text, tokensUsed, inputTokens, outputTokens, error }
 */
function callClaudeAPIMultiTurn(systemPrompt, messages) {
    var config = getAIConfig();
    if (!config.apiKey) {
        return { success: false, error: 'Anthropic API key not configured.' };
    }
    try {
        var payload = {
            model: config.model,
            max_tokens: config.maxTokens,
            system: systemPrompt,
            messages: messages
        };
        var options = {
            method: 'post',
            contentType: 'application/json',
            headers: { 'x-api-key': config.apiKey, 'anthropic-version': '2023-06-01' },
            payload: JSON.stringify(payload),
            muteHttpExceptions: true
        };
        var response = UrlFetchApp.fetch('https://api.anthropic.com/v1/messages', options);
        var body = JSON.parse(response.getContentText());
        if (response.getResponseCode() !== 200) {
            return { success: false, error: 'API error: ' + (body.error ? body.error.message : 'HTTP ' + response.getResponseCode()) };
        }
        var text = (body.content || []).filter(function (b) { return b.type === 'text'; }).map(function (b) { return b.text; }).join('\n');
        var inputTokens = body.usage ? (body.usage.input_tokens || 0) : 0;
        var outputTokens = body.usage ? (body.usage.output_tokens || 0) : 0;
        var tokensUsed = inputTokens + outputTokens;
        return { success: true, text: text, tokensUsed: tokensUsed, inputTokens: inputTokens, outputTokens: outputTokens };
    } catch (e) {
        return { success: false, error: 'API call failed: ' + e.message };
    }
}

// ── Cost tracking ─────────────────────────────────────────────────────────────

/**
 * Estimate the dollar cost of a single API request from token counts and model.
 * Pricing per million tokens — update if Anthropic changes rates.
 * v1.14 - New function.
 * @param {string} model
 * @param {number} inputTokens
 * @param {number} outputTokens
 * @returns {number} Estimated cost in USD
 */
function calculateRequestCost_(model, inputTokens, outputTokens) {
    var pricing = {
        'claude-sonnet-4-6': { input: 3.00, output: 15.00 },
        'claude-sonnet-4-20250514': { input: 3.00, output: 15.00 },
        'claude-sonnet-4-5': { input: 3.00, output: 15.00 },
        'claude-opus-4-6': { input: 15.00, output: 75.00 },
        'claude-opus-4-20250514': { input: 15.00, output: 75.00 },
        'claude-haiku-4-5-20251001': { input: 0.25, output: 1.25 }
    };
    var p = pricing[model] || { input: 3.00, output: 15.00 };
    return (inputTokens * p.input / 1000000) + (outputTokens * p.output / 1000000);
}

/**
 * Add a request cost to the cumulative total in Script Properties.
 * Non-fatal — returns 0 on any error so callers are never blocked.
 * v1.14 - New function.
 * @param {number} cost
 * @returns {number} New cumulative total
 */
function trackCumulativeCost_(cost) {
    try {
        var props = PropertiesService.getScriptProperties();
        var current = parseFloat(props.getProperty('ANTHROPIC_CUMULATIVE_COST') || '0');
        var newTotal = current + cost;
        props.setProperty('ANTHROPIC_CUMULATIVE_COST', newTotal.toFixed(6));
        return newTotal;
    } catch (e) { return 0; }
}

/**
 * Return the estimated cumulative AI spend and a link to Anthropic Console billing.
 * Note: Anthropic has no balance API endpoint — user must check console directly.
 * v1.14 - New function.
 * @returns {Object} { success, cumulativeCost, consoleUrl }
 */
function getAICostSummary() {
    try {
        var props = PropertiesService.getScriptProperties();
        return {
            success: true,
            cumulativeCost: parseFloat(props.getProperty('ANTHROPIC_CUMULATIVE_COST') || '0'),
            consoleUrl: 'https://console.anthropic.com/settings/billing'
        };
    } catch (e) {
        return { success: false, cumulativeCost: 0, consoleUrl: 'https://console.anthropic.com/settings/billing' };
    }
}

/**
 * Reset the cumulative cost counter to zero. Admin-only.
 * v1.14 - New function.
 * @returns {Object} { success, message }
 */
function resetAICostTracker() {
    try {
        PropertiesService.getScriptProperties().setProperty('ANTHROPIC_CUMULATIVE_COST', '0');
        logAudit('AI_COST_RESET', null, 'Cumulative AI cost tracker reset to zero');
        return { success: true, message: 'Cost tracker reset to $0.00' };
    } catch (e) {
        return { success: false, error: e.message };
    }
}


/**
 * List spreadsheet files in the Grants folder created in the last 30 days.
 * Used by the Report Builder "Find recent reports" button.
 * v1.15 - New function.
 * @returns {Object} { success, files: [{name, url, date}] }
 */
function listRecentGrantsReports() {
    try {
        var folderId = CONFIG.GRANTS_FOLDER_ID;
        if (!folderId) return { success: false, error: 'Grants folder not configured.' };

        var folder = DriveApp.getFolderById(folderId);
        var cutoff = new Date();
        cutoff.setDate(cutoff.getDate() - 30);

        var files = [];
        var iter = folder.getFilesByType('application/vnd.google-apps.spreadsheet');
        while (iter.hasNext()) {
            var f = iter.next();
            if (f.getDateCreated() >= cutoff) {
                files.push({
                    name: f.getName(),
                    url: f.getUrl(),
                    date: Utilities.formatDate(f.getDateCreated(), CONFIG.TIMEZONE, 'M/d/yyyy h:mm a')
                });
            }
        }

        // Sort newest first
        files.sort(function (a, b) { return b.date.localeCompare(a.date); });

        return { success: true, files: files };
    } catch (e) {
        Logger.log('listRecentGrantsReports error: ' + e.message);
        return { success: false, error: e.message };
    }
}


/**
 * Generate a report sheet from pre-built headers+rows (grant or distribution data).
 * Applies spec filters and sort, then writes a formatted Google Sheet.
 * v1.18 - New function.
 * @private
 */
function _generateFromPrebuiltData_(spec, headers, rows, fromDate, toDate, titleSuffix) {
    try {
        // Apply filters
        var filteredRows = rows;
        if (spec.filters && spec.filters.length > 0) {
            filteredRows = rows.filter(function (row) {
                for (var fi = 0; fi < spec.filters.length; fi++) {
                    var f = spec.filters[fi];
                    var idx = headers.indexOf(f.field);
                    if (idx === -1) continue;
                    var cv = (row[idx] !== null && row[idx] !== undefined) ? row[idx].toString().trim() : '';
                    var cvl = cv.toLowerCase();
                    var fv = (f.values || []).map(function (v) { return v.toString().trim().toLowerCase(); });
                    switch (f.op) {
                        case 'include': if (fv.indexOf(cvl) === -1) return false; break;
                        case 'exclude': if (fv.indexOf(cvl) !== -1) return false; break;
                        case 'notBlank': if (cv === '') return false; break;
                        case 'isBlank': if (cv !== '') return false; break;
                    }
                }
                return true;
            });
        }

        // Apply sort
        if (spec.sort && spec.sort.length > 0) {
            filteredRows.sort(function (a, b) {
                for (var si = 0; si < spec.sort.length; si++) {
                    var s = spec.sort[si];
                    var idx2 = headers.indexOf(s.field);
                    if (idx2 === -1) continue;
                    var av = (a[idx2] || '').toString(), bv = (b[idx2] || '').toString();
                    var dir = s.dir === 'desc' ? -1 : 1;
                    var an = parseFloat(av), bn = parseFloat(bv);
                    if (!isNaN(an) && !isNaN(bn) && an !== bn) return (an - bn) * dir;
                    var cmp = av.localeCompare(bv);
                    if (cmp !== 0) return cmp * dir;
                }
                return 0;
            });
        }

        // Determine output columns
        var outCols = (spec.outputColumns && spec.outputColumns.length > 0)
            ? spec.outputColumns.filter(function (c) { return headers.indexOf(c) !== -1; })
            : headers;
        var outIndices = outCols.map(function (c) { return headers.indexOf(c); });

        var outputRows = filteredRows.map(function (row) {
            return outIndices.map(function (idx) {
                var v = idx !== -1 ? row[idx] : '';
                return v !== null && v !== undefined ? v : '';
            });
        });

        // Write sheet
        var datestamp = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd_HHmm');
        var reportName = 'AI_' + (spec.title || titleSuffix).replace(/\s+/g, '_') + '_' + fromDate + '_to_' + toDate + '_' + datestamp;
        var ss = SpreadsheetApp.create(reportName);
        var sheet = ss.getActiveSheet();
        sheet.setName(spec.title || titleSuffix);

        var numCols = outCols.length;
        var currentRow = 1;
        sheet.getRange(currentRow, 1).setValue('Giving to the Nations').setFontWeight('bold').setFontSize(14);
        currentRow++;
        sheet.getRange(currentRow, 1).setValue(spec.title || titleSuffix).setFontWeight('bold').setFontSize(12);
        currentRow++;
        sheet.getRange(currentRow, 1)
            .setValue('Period: ' + fromDate + ' to ' + toDate +
                ' | Source records: ' + rows.length + ' | Filtered: ' + filteredRows.length +
                ' | Generated: ' + Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yyyy h:mm a'))
            .setFontStyle('italic').setFontSize(9).setFontColor('#666666');
        currentRow += 2;

        sheet.getRange(currentRow, 1, 1, numCols).setValues([outCols])
            .setFontWeight('bold').setBackground('#4a86e8').setFontColor('white').setHorizontalAlignment('center');
        sheet.setFrozenRows(currentRow);
        currentRow++;

        if (outputRows.length > 0) {
            sheet.getRange(currentRow, 1, outputRows.length, numCols).setValues(outputRows);
            for (var ri = 1; ri < outputRows.length; ri += 2)
                sheet.getRange(currentRow + ri, 1, 1, numCols).setBackground('#f8f9fa');
        }

        for (var ci = 1; ci <= numCols; ci++) sheet.autoResizeColumn(ci);

        if (spec.notes) {
            sheet.getRange(currentRow + outputRows.length + 1, 1)
                .setValue('Notes: ' + spec.notes).setFontStyle('italic').setFontSize(9).setFontColor('#888888');
        }

        moveToFolder(ss.getId(), CONFIG.GRANTS_FOLDER_ID);
        logAudit('AI_REPORT', null, 'Generated Report Builder sheet from ' + titleSuffix +
            ' (' + outputRows.length + ' rows)');

        var cumulative = parseFloat(
            PropertiesService.getScriptProperties().getProperty('ANTHROPIC_CUMULATIVE_COST') || '0'
        );
        return {
            success: true,
            reportUrl: ss.getUrl(),
            downloadUrl: 'https://docs.google.com/spreadsheets/d/' + ss.getId() + '/export?format=xlsx',
            cumulativeCost: cumulative
        };
    } catch (e) {
        Logger.log('_generateFromPrebuiltData_ error: ' + e.message);
        return { success: false, error: 'Report generation failed: ' + e.message };
    }
}


/**
 * Generate a report from Hygiene Box Distribution Stats workbook data.
 * Called when spec.source === 'hygiene'.
 * v1.18 - New function.
 * v1.20 - Added groupBy/aggCols support for grouped summaries (e.g. by Center+Month).
 *         Extracts Month Name and Month Number from Distribution Date Range start date.
 *         Supports sum/avg/count aggCol types. Sorts by multiple fields.
 * @private
 */
function _generateFromHygieneSpec_(spec, fromDate, toDate) {
    try {
        if (!CONFIG.HYGIENE_STATS_WORKBOOK_ID) {
            return { success: false, error: 'HYGIENE_STATS_WORKBOOK_ID not configured.' };
        }

        var hWB = SpreadsheetApp.openById(CONFIG.HYGIENE_STATS_WORKBOOK_ID);
        var hSheets = hWB.getSheets();

        // ── Build master flat dataset with derived columns ────────────────────────
        // Internal columns available for groupBy/filtering/sorting:
        var INTERNAL_COLS = [
            'Center', 'Month Name', 'Month Number', 'Year',
            'Distribution Date Range', 'Scheduled Distrib Code',
            'Total Scheduled-Recipients', 'Total Scheduled-Boxes', 'With Baby Box',
            'Picked Up-Generic', 'Picked Up-Baby', 'Restock-# Recipients',
            '% Recipients-No Pick Up', '% Recipients-Picked Up'
        ];

        var allRows = []; // each row = array aligned to INTERNAL_COLS
        var CI = {}; // column index map
        INTERNAL_COLS.forEach(function (c, i) { CI[c] = i; });

        hSheets.forEach(function (hs) {
            var hLast = hs.getLastRow();
            if (hLast < 2) return;
            var hData = hs.getDataRange().getValues();
            var hHdrs = trimHeaders(hData[0]);
            var colDate = hHdrs.indexOf('Distribution Date Range');
            var colSched = hHdrs.indexOf('Total Scheduled-Recipients');
            var colBoxes = hHdrs.indexOf('Total Scheduled-Boxes');
            var colBaby = hHdrs.indexOf('With Baby Box');
            var colPuGen = hHdrs.indexOf('Picked Up-Generic');
            var colPuBaby = hHdrs.indexOf('Picked Up-Baby');
            var colRst = hHdrs.indexOf('Restock-# Recipients');
            var colPct = hHdrs.indexOf('% Recipients-No Pick Up');
            var colCode = hHdrs.indexOf('Scheduled Distrib Code');

            for (var hi = 1; hi < hData.length; hi++) {
                var src = hData[hi];
                var drRaw = colDate !== -1 ? src[colDate] : '';
                var sc = parseInt(colSched !== -1 ? src[colSched] : 0) || 0;

                // Distribution Date Range may be a Date object (GAS date-formatted cell)
                // or a string like "1/6/2026 - 1/19/2026"
                var dr = '';
                var startDateObj = null;
                if (drRaw instanceof Date && !isNaN(drRaw.getTime())) {
                    // Cell is a single Date — use it directly as the start date
                    startDateObj = drRaw;
                    dr = Utilities.formatDate(drRaw, CONFIG.TIMEZONE, 'M/d/yyyy');
                } else {
                    dr = (drRaw || '').toString().trim();
                    // Try to parse start date from "M/d/yyyy - M/d/yyyy" range string
                    var datePart = dr.split(' - ')[0].trim();
                    if (datePart) {
                        var parsed = new Date(datePart);
                        if (!isNaN(parsed.getTime())) startDateObj = parsed;
                    }
                }

                if (!dr && sc === 0) continue; // blank row

                // Extract month/year from start date
                var monthName = '', monthNum = 0, year = 0;
                if (startDateObj) {
                    monthNum = startDateObj.getMonth() + 1;
                    monthName = ['January', 'February', 'March', 'April', 'May', 'June',
                        'July', 'August', 'September', 'October', 'November', 'December'][startDateObj.getMonth()];
                    year = startDateObj.getFullYear();
                }

                var pu = parseInt(colPuGen !== -1 ? src[colPuGen] : 0) || 0;
                var rst = parseInt(colRst !== -1 ? src[colRst] : 0) || 0;

                // % Recipients-No Pick Up is stored as a decimal fraction (e.g. 0.35 = 35%)
                // Convert to true percentage for display
                var pctNoRaw = colPct !== -1 ? src[colPct] : 0;
                var pctNo = 0;
                if (typeof pctNoRaw === 'number') {
                    // If value <= 1 it's a fraction, multiply by 100; otherwise already a percentage
                    pctNo = pctNoRaw <= 1 ? Math.round(pctNoRaw * 10000) / 100 : Math.round(pctNoRaw * 100) / 100;
                } else {
                    var pctNoStr = (pctNoRaw || '').toString().replace('%', '').trim();
                    var pctNoParsed = parseFloat(pctNoStr) || 0;
                    pctNo = pctNoParsed <= 1 ? Math.round(pctNoParsed * 10000) / 100 : Math.round(pctNoParsed * 100) / 100;
                }
                var pctPu = sc > 0 ? Math.round(pu / sc * 10000) / 100 : 0;

                var row = new Array(INTERNAL_COLS.length).fill('');
                row[CI['Center']] = hs.getName();
                row[CI['Month Name']] = monthName;
                row[CI['Month Number']] = monthNum;
                row[CI['Year']] = year;
                row[CI['Distribution Date Range']] = dr;
                row[CI['Scheduled Distrib Code']] = colCode !== -1 ? (src[colCode] || '').toString().trim() : '';
                row[CI['Total Scheduled-Recipients']] = sc;
                row[CI['Total Scheduled-Boxes']] = parseInt(colBoxes !== -1 ? src[colBoxes] : 0) || 0;
                row[CI['With Baby Box']] = parseInt(colBaby !== -1 ? src[colBaby] : 0) || 0;
                row[CI['Picked Up-Generic']] = pu;
                row[CI['Picked Up-Baby']] = parseInt(colPuBaby !== -1 ? src[colPuBaby] : 0) || 0;
                row[CI['Restock-# Recipients']] = rst;
                row[CI['% Recipients-No Pick Up']] = pctNo;
                row[CI['% Recipients-Picked Up']] = pctPu;
                allRows.push(row);
            }
        });

        if (allRows.length === 0) {
            return { success: false, error: 'No data found in Hygiene Box Distribution Stats workbook.' };
        }

        // ── GroupBy aggregation ───────────────────────────────────────────────────
        var outputHeaders, outputRows;
        var isGrouped = !!(spec.groupBy && spec.aggCols && spec.aggCols.length > 0);

        if (isGrouped) {
            // Support multi-field groupBy (comma-separated or array)
            var gbFields = Array.isArray(spec.groupBy)
                ? spec.groupBy : spec.groupBy.split(',').map(function (s) { return s.trim(); });

            var groups = {}, groupOrder = [];
            allRows.forEach(function (row) {
                var key = gbFields.map(function (f) {
                    return CI[f] !== undefined ? row[CI[f]] : '';
                }).join('|||');
                if (!groups[key]) { groups[key] = []; groupOrder.push(key); }
                groups[key].push(row);
            });

            // Sort groups
            groupOrder.sort(function (a, b) {
                var ar = groups[a][0], br = groups[b][0];
                for (var si = 0; si < (spec.sort || []).length; si++) {
                    var sf = spec.sort[si];
                    var idx = CI[sf.field];
                    if (idx === undefined) continue;
                    var av = ar[idx], bv = br[idx];
                    var dir = sf.dir === 'desc' ? -1 : 1;
                    var an = parseFloat(av), bn = parseFloat(bv);
                    if (!isNaN(an) && !isNaN(bn) && an !== bn) return (an - bn) * dir;
                    var cmp = (av || '').toString().localeCompare((bv || '').toString());
                    if (cmp !== 0) return cmp * dir;
                }
                return 0;
            });

            // Build output headers: groupBy fields + aggCol labels
            outputHeaders = gbFields.concat(spec.aggCols.map(function (ac) { return ac.label || ac.field; }));
            outputRows = groupOrder.map(function (key) {
                var gRows = groups[key];
                var outRow = gbFields.map(function (f) {
                    return CI[f] !== undefined ? gRows[0][CI[f]] : '';
                });
                spec.aggCols.forEach(function (ac) {
                    var idx = CI[ac.field];
                    var vals = idx !== undefined ? gRows.map(function (r) { return r[idx]; }) : [];
                    var agg;
                    switch (ac.type) {
                        case 'sum':
                            agg = vals.reduce(function (s, v) { var n = parseFloat(v); return s + (isNaN(n) ? 0 : n); }, 0);
                            agg = Math.round(agg * 100) / 100;
                            break;
                        case 'avg':
                            var nums = vals.filter(function (v) { return !isNaN(parseFloat(v)); });
                            agg = nums.length > 0
                                ? Math.round(nums.reduce(function (s, v) { return s + parseFloat(v); }, 0) / nums.length * 100) / 100
                                : 0;
                            // Format pct fields as "XX.XX%"
                            if (ac.field.indexOf('%') !== -1) agg = agg.toFixed(2) + '%';
                            break;
                        case 'count': agg = gRows.length; break;
                        default: agg = gRows.length;
                    }
                    outRow.push(agg);
                });
                return outRow;
            });

            // Default sort when spec.sort not set: Center asc, Month Number asc
            if (!spec.sort || spec.sort.length === 0) {
                var centerIdx = outputHeaders.indexOf('Center');
                var monthIdx = outputHeaders.indexOf('Month Number');
                outputRows.sort(function (a, b) {
                    if (centerIdx !== -1) {
                        var cmp = (a[centerIdx] || '').localeCompare(b[centerIdx] || '');
                        if (cmp !== 0) return cmp;
                    }
                    if (monthIdx !== -1) {
                        return (parseInt(a[monthIdx]) || 0) - (parseInt(b[monthIdx]) || 0);
                    }
                    return 0;
                });
            }

        } else {
            // ── Raw row output ──────────────────────────────────────────────────────
            var outCols = (spec.outputColumns && spec.outputColumns.length > 0)
                ? spec.outputColumns : INTERNAL_COLS;
            outputHeaders = outCols;

            // Apply sort
            if (spec.sort && spec.sort.length > 0) {
                allRows.sort(function (a, b) {
                    for (var si = 0; si < spec.sort.length; si++) {
                        var sf = spec.sort[si];
                        var idx = CI[sf.field];
                        if (idx === undefined) continue;
                        var av = (a[idx] || '').toString(), bv = (b[idx] || '').toString();
                        var dir = sf.dir === 'desc' ? -1 : 1;
                        var an = parseFloat(av), bn = parseFloat(bv);
                        if (!isNaN(an) && !isNaN(bn) && an !== bn) return (an - bn) * dir;
                        var cmp = av.localeCompare(bv);
                        if (cmp !== 0) return cmp * dir;
                    }
                    return 0;
                });
            }

            outputRows = allRows.map(function (row) {
                return outCols.map(function (col) {
                    return CI[col] !== undefined ? row[CI[col]] : '';
                });
            });
        }

        // ── Write spreadsheet ─────────────────────────────────────────────────────
        var datestamp = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd_HHmm');
        var reportName = 'AI_HygieneStats_' + datestamp;
        var ss = SpreadsheetApp.create(reportName);
        var sheet = ss.getActiveSheet();
        sheet.setName(spec.title || 'Hygiene Stats Summary');

        var currentRow = 1;
        sheet.getRange(currentRow, 1).setValue('Giving to the Nations').setFontWeight('bold').setFontSize(14);
        currentRow++;
        sheet.getRange(currentRow, 1).setValue(spec.title || 'Hygiene Box Distribution Stats Summary')
            .setFontWeight('bold').setFontSize(12);
        currentRow++;
        sheet.getRange(currentRow, 1).setValue(
            'Generated: ' + Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yyyy h:mm a') +
            (isGrouped ? ' | Groups: ' + outputRows.length : ' | Rows: ' + outputRows.length))
            .setFontStyle('italic').setFontSize(9).setFontColor('#666666');
        currentRow += 2;

        var numCols = outputHeaders.length;
        sheet.getRange(currentRow, 1, 1, numCols).setValues([outputHeaders])
            .setFontWeight('bold').setBackground('#1a73e8').setFontColor('white').setHorizontalAlignment('center');
        sheet.setFrozenRows(currentRow);
        currentRow++;

        if (outputRows.length > 0) {
            sheet.getRange(currentRow, 1, outputRows.length, numCols).setValues(outputRows);
            for (var ri = 1; ri < outputRows.length; ri += 2)
                sheet.getRange(currentRow + ri, 1, 1, numCols).setBackground('#f8f9fa');
        }

        for (var ci2 = 1; ci2 <= numCols; ci2++) sheet.autoResizeColumn(ci2);

        if (spec.notes) {
            sheet.getRange(currentRow + outputRows.length + 1, 1)
                .setValue('Notes: ' + spec.notes).setFontStyle('italic').setFontSize(9).setFontColor('#888888');
        }

        moveToFolder(ss.getId(), CONFIG.GRANTS_FOLDER_ID);
        logAudit('AI_REPORT', null, 'Generated Hygiene Stats report (' + outputRows.length + ' rows, grouped=' + isGrouped + ')');

        var cumulative = parseFloat(
            PropertiesService.getScriptProperties().getProperty('ANTHROPIC_CUMULATIVE_COST') || '0'
        );
        return {
            success: true,
            reportUrl: ss.getUrl(),
            downloadUrl: 'https://docs.google.com/spreadsheets/d/' + ss.getId() + '/export?format=xlsx',
            cumulativeCost: cumulative
        };

    } catch (e) {
        Logger.log('_generateFromHygieneSpec_ error: ' + e.message);
        return { success: false, error: 'Hygiene Stats report failed: ' + e.message };
    }
}


/**
 * Generate a Google Sheet directly from a Report Builder spec object.
 * No API call — spec was already produced by processReportBuilderTurn conversation.
 * Called by AP generateReportBuilderReport() when user clicks Generate Report.
 * v1.15 - New function.
 * @param {Object} spec     - JSON spec {title, outputColumns, filters, sort, computations, notes}
 * @param {string} fromDate - YYYY-MM-DD
 * @param {string} toDate   - YYYY-MM-DD
 * @returns {Object} { success, reportUrl, downloadUrl, cumulativeCost, error }
 */
function generateFromReportBuilderSpec(spec, fromDate, toDate) {
    try {
        if (!spec) {
            return { success: false, error: 'No spec available. Complete the conversation with Claude first.' };
        }
        // Allow grouped specs (groupBy+aggCols) and source-routed specs (hygiene/grant/distribution)
        // that don't require outputColumns
        var hasOutput = (spec.outputColumns && spec.outputColumns.length > 0) ||
            (spec.groupBy) ||
            (spec.source === 'hygiene') ||
            (spec.source === 'grant' && spec.reportDataName) ||
            (spec.source === 'distribution' && spec.distribCode);
        if (!hasOutput) {
            return { success: false, error: 'Spec has no output columns or groupBy. Ask Claude to regenerate the spec.' };
        }

        var fromDateObj = parseDateInput(fromDate, false);
        var toDateObj = parseDateInput(toDate, true);

        // ── Grant report source — use GrantsReportService data pipeline ──────────
        if (spec.source === 'grant' && spec.reportDataName) {
            var gResult = getGrantReportData(spec.reportDataName, fromDate, toDate);
            if (!gResult.success) return gResult;
            return _generateFromPrebuiltData_(spec, gResult.headers, gResult.rows, fromDate, toDate,
                spec.reportDataName + ' (Grant Report Data)');
        }

        // ── Distribution report source ────────────────────────────────────────────
        if (spec.source === 'distribution' && spec.distribCode) {
            var dResult = getDistributionRecordsForBuilder(spec.distribCode);
            if (!dResult.success) return dResult;
            return _generateFromPrebuiltData_(spec, dResult.scheduledHeaders, dResult.scheduledRows,
                fromDate, toDate, 'Distribution Report — ' + spec.distribCode);
        }

        // ── Hygiene Stats source — bypass AM entirely ─────────────────────────────
        if (spec.source === 'hygiene') {
            return _generateFromHygieneSpec_(spec, fromDate, toDate);
        }

        var combined = getCombinedData(fromDateObj, toDateObj);
        if (combined.totalCount === 0) {
            return { success: false, error: 'No records found for the date range ' + fromDate + ' to ' + toDate };
        }

        var headers = combined.headers;
        var rows = combined.rows;

        // Build label→raw map from FieldMapService for resolving display labels
        var fieldGroupResult = getAMFieldGroups();
        var labelToRawForSpec = {};
        if (fieldGroupResult.success && fieldGroupResult.groups) {
            fieldGroupResult.groups.forEach(function (grp) {
                (grp.fields || []).forEach(function (f) {
                    var lbl = f.label || f.value || '';
                    var raw = f.value || lbl;
                    if (lbl) labelToRawForSpec[lbl] = raw;
                });
            });
        }

        // Map spec.outputColumns to raw headers
        var validFields = spec.outputColumns
            .map(function (lbl) { return labelToRawForSpec[lbl] || lbl; })
            .filter(function (f) { return headers.indexOf(f) !== -1; });

        // Fallback: if labels don't resolve (e.g. LU_ReportColumns Override Label = display label),
        // try resolveAMField_ on each to catch LU_FieldMap Report Header mappings
        if (validFields.length === 0) {
            validFields = spec.outputColumns.map(function (lbl) {
                var resolved = resolveAMField_(lbl);
                return resolved;
            }).filter(function (f) { return headers.indexOf(f) !== -1; });
        }

        if (validFields.length === 0) {
            return {
                success: false, error: 'None of the spec columns could be matched to Applicants_Master headers. ' +
                    'Columns in spec: ' + spec.outputColumns.join(', ')
            };
        }

        var fieldIndices = validFields.map(function (f) { return headers.indexOf(f); });
        var dispLabels = spec.outputColumns.filter(function (lbl) {
            var raw = labelToRawForSpec[lbl] || resolveAMField_(lbl) || lbl;
            return headers.indexOf(raw) !== -1;
        });
        if (dispLabels.length !== validFields.length) dispLabels = validFields;

        var result = applySpecAndWriteSheet(
            spec, headers, rows, validFields, fieldIndices,
            fromDate, toDate, 0, [], labelToRawForSpec, dispLabels
        );

        // Track cumulative cost (0 tokens — no API call)
        var cumulative = parseFloat(
            PropertiesService.getScriptProperties().getProperty('ANTHROPIC_CUMULATIVE_COST') || '0'
        );

        if (result.success) {
            logAudit('REPORT_BUILDER_GENERATE', null,
                'Generated Report Builder sheet: ' + (spec.title || 'Untitled') +
                ' (' + fromDate + ' to ' + toDate + ')');
        }

        return {
            success: result.success,
            reportUrl: result.reportUrl || '',
            downloadUrl: result.downloadUrl || '',
            cumulativeCost: cumulative,
            error: result.error || ''
        };

    } catch (e) {
        Logger.log('generateFromReportBuilderSpec error: ' + e.message);
        return { success: false, error: 'Report generation failed: ' + e.message };
    }
}


/**
 * Build a KNOWN REPORT TEMPLATES string for the Report Builder system prompt
 * by reading the actual column definitions from LU_ReportColumns via getReportColumns().
 * This ensures Claude sees the exact same columns the real reports use,
 * including Override Labels (display names), not hardcoded guesses.
 * v1.15 - New function.
 * @private
 * @returns {string} Multi-line string listing each report's column labels
 */
function _buildReportTemplateContext_() {
    var reportNames = [
        { key: 'Households', aliases: '"Households", "Households report"' },
        { key: 'Distribution Stats', aliases: '"Distribution Stats", "Distribution Statistics"' },
        { key: 'Open Requests', aliases: '"Open Requests"' },
        { key: 'Request Frequency', aliases: '"Request Frequency", "Frequency"' },
        { key: 'Testimonials', aliases: '"Testimonials"' }
    ];

    var lines = [];
    reportNames.forEach(function (r) {
        try {
            var cols = getReportColumns(r.key);
            if (cols && cols.length > 0) {
                var labels = cols.map(function (c) { return c.label || c.key; });
                lines.push('- ' + r.aliases + ':\n  Columns: ' + labels.join(', '));
            }
        } catch (e) {
            Logger.log('_buildReportTemplateContext_: error loading ' + r.key + ': ' + e.message);
        }
    });

    return lines.length > 0 ? lines.join('\n') + '\n\n' : '';
}


/**
 * Process one turn of the Report Builder conversation.
 * @param {Array}   messages   - Full conversation history [{role, content}]
 * @param {string}  fromDate   - YYYY-MM-DD
 * @param {string}  toDate     - YYYY-MM-DD
 * @param {boolean} generate   - If true and spec ready, generate the report now
 */
function processReportBuilderTurn(messages, fromDate, toDate, generate) {
    try {
        var fromDateObj = parseDateInput(fromDate, false);
        var toDateObj = parseDateInput(toDate, true);

        // ── Load data context (fields + unique values) once ──────────────────────
        var combined = getCombinedData(fromDateObj, toDateObj);
        if (combined.totalCount === 0) {
            return { success: false, error: 'No records found for date range ' + fromDate + ' to ' + toDate };
        }

        // Get all available display labels via FieldMapService
        // getAMFieldGroups() returns { success, groups: [{name, fields: [{value, label}]}] }
        var fieldGroupResult = getAMFieldGroups();
        var allDisplayLabels = [];
        var labelToRawForBuilder = {};
        if (fieldGroupResult.success && fieldGroupResult.groups) {
            fieldGroupResult.groups.forEach(function (grp) {
                (grp.fields || []).forEach(function (f) {
                    var lbl = f.label || f.value || f.name || '';
                    var raw = f.value || f.name || lbl;
                    if (lbl) {
                        allDisplayLabels.push(lbl);
                        labelToRawForBuilder[lbl] = raw;
                    }
                });
            });
        }
        // Fallback: use raw AM headers directly if FieldMapService returned nothing
        if (allDisplayLabels.length === 0) {
            headers.forEach(function (h) {
                if (h) { allDisplayLabels.push(h); labelToRawForBuilder[h] = h; }
            });
        }

        // Build unique values for key filterable fields (County, City, Service Status, etc.)
        var headers = combined.headers;
        var rows = combined.rows;
        var filterableFields = ['County', 'City', 'State', 'Service Status', 'Request Type',
            'Funding Source', 'Scheduled Distribution Code'];
        var uniqueVals = {};
        filterableFields.forEach(function (fname) {
            var idx = headers.indexOf(resolveAMField_(fname));
            if (idx === -1) return;
            var vals = {};
            rows.forEach(function (r) { var v = (r[idx] || '').toString().trim(); if (v) vals[v] = true; });
            var vArr = Object.keys(vals).sort();
            if (vArr.length > 0 && vArr.length <= 30) uniqueVals[fname] = vArr;
        });

        // ── Pre-read grant report data if user references a named report ──────────
        // Scan last user message for known report names and distribCodes
        var grantReportContext = '';
        var distReportContext = '';
        try {
            var lastUserMsg = '';
            for (var mi = messages.length - 1; mi >= 0; mi--) {
                if (messages[mi].role === 'user') { lastUserMsg = (messages[mi].content || '').toLowerCase(); break; }
            }
            var grantNames = ['households', 'distribution stats', 'open requests', 'request frequency', 'testimonials'];
            var matchedGrant = '';
            grantNames.forEach(function (gn) { if (lastUserMsg.indexOf(gn) !== -1) matchedGrant = gn; });
            if (matchedGrant) {
                var gData = getGrantReportData(matchedGrant, fromDate, toDate);
                if (gData.success && gData.rows && gData.rows.length > 0) {
                    var sampleRows = gData.rows.slice(0, 5);
                    grantReportContext =
                        'GRANT REPORT DATA — "' + gData.reportName + '" (' + gData.recordCount + ' records):\n' +
                        'Headers: ' + JSON.stringify(gData.headers) + '\n' +
                        'Sample rows (first 5):\n' + JSON.stringify(sampleRows) + '\n' +
                        'Full dataset available for report generation with source:"grant" and reportDataName:"' + gData.reportName + '".\n\n';
                }
            }
            // Check for SchedDisbCode reference (e.g. "2026-02A", "DG1", etc.)
            var codeMatch = lastUserMsg.match(/\b([a-z0-9]{1,4}-\d{2,4}[a-z]?|[a-z]{1,3}\d{1,3})\b/i);
            if (codeMatch) {
                var possibleCode = codeMatch[1].toUpperCase();
                var dData = getDistributionRecordsForBuilder(possibleCode);
                if (dData.success) {
                    var prodSummary = dData.products.map(function (p) {
                        var items = Object.keys(p.products).map(function (nm) {
                            return nm + ': qty=' + p.products[nm] + ' × ' + p.recipients + ' = ' + (p.products[nm] * p.recipients);
                        });
                        return p.label + ' (' + p.boxCode + ', ' + p.recipients + ' recipients): ' + items.join(', ');
                    }).join('\n');
                    distReportContext =
                        'DISTRIBUTION REPORT DATA — Code: ' + possibleCode + ':\n' +
                        '  Scheduled recipients: ' + dData.scheduledCount + '\n' +
                        '  Total matching records: ' + dData.totalCount + '\n' +
                        '  Has Baby Box: ' + dData.hasBabyBox + '\n' +
                        (prodSummary ? '  Products Needed:\n' + prodSummary + '\n' : '') +
                        'Headers: ' + JSON.stringify(dData.scheduledHeaders) + '\n' +
                        'Use source:"distribution" and distribCode:"' + possibleCode + '" in the spec.\n\n';
                }
            }
        } catch (preReadErr) {
            Logger.log('processReportBuilderTurn pre-read (non-fatal): ' + preReadErr.message);
        }
        var hygieneContext = '';
        try {
            if (CONFIG.HYGIENE_STATS_WORKBOOK_ID) {
                var hWB = SpreadsheetApp.openById(CONFIG.HYGIENE_STATS_WORKBOOK_ID);
                var hSheets = hWB.getSheets();
                var hLines = [];
                hSheets.forEach(function (hs) {
                    var hLast = hs.getLastRow();
                    if (hLast < 2) return;
                    var hData = hs.getDataRange().getValues();
                    var hHdrs = trimHeaders(hData[0]);
                    var colDate = hHdrs.indexOf('Distribution Date Range');
                    var colSched = hHdrs.indexOf('Total Scheduled-Recipients');
                    var colRst = hHdrs.indexOf('Restock-# Recipients');
                    var colPu = hHdrs.indexOf('Picked Up-Generic');
                    var colPct = hHdrs.indexOf('% Recipients-No Pick Up');
                    var rowLines = [];
                    for (var hi = 1; hi < hData.length; hi++) {
                        var dr = colDate !== -1 ? (hData[hi][colDate] || '').toString().trim() : '';
                        var sc = colSched !== -1 ? (parseInt(hData[hi][colSched]) || 0) : 0;
                        var rs = colRst !== -1 ? (parseInt(hData[hi][colRst]) || 0) : 0;
                        var pu = colPu !== -1 ? (parseInt(hData[hi][colPu]) || 0) : 0;
                        var pc = colPct !== -1 ? (hData[hi][colPct] || '').toString().trim() : '';
                        if (!dr && sc === 0) continue;
                        rowLines.push('    Date: ' + dr + ' | Scheduled: ' + sc + ' | PickedUp: ' + pu +
                            ' | Restock: ' + rs + ' | %NoPU: ' + pc);
                    }
                    if (rowLines.length > 0) {
                        hLines.push('  Center "' + hs.getName() + '" (' + rowLines.length + ' distributions):\n' + rowLines.join('\n'));
                    }
                });
                if (hLines.length > 0) {
                    hygieneContext = 'HYGIENE BOX DISTRIBUTION STATS WORKBOOK DATA:\n' +
                        '(This is pre-read data from the Stats workbook — you CAN summarize this data.)\n' +
                        hLines.join('\n') + '\n\n';
                }
            }
        } catch (hyErr) {
            Logger.log('processReportBuilderTurn: Hygiene Stats pre-read (non-fatal): ' + hyErr.message);
        }

        // ── System prompt ─────────────────────────────────────────────────────────
        var systemPrompt =
            'You are a data report assistant for Giving to the Nations nonprofit.\n' +
            'You help users build Custom Data Sheet reports from applicant data.\n\n' +
            'DATA SOURCE SCOPE — CRITICAL:\n' +
            'You have access to TWO data sources in this conversation:\n' +
            '1. APPLICANTS_MASTER DATA — ' + combined.totalCount + ' applicant records (' +
            combined.masterCount + ' active, ' + combined.archiveCount + ' archive). ' +
            'These are the records described in AVAILABLE FIELDS below.\n' +
            '2. HYGIENE STATS DATA — pre-read distribution statistics from the Hygiene Box ' +
            'Distribution Stats workbook (see HYGIENE BOX DISTRIBUTION STATS WORKBOOK DATA below).\n' +
            '3. GRANT REPORT DATA — if a named grant report is referenced (Households, Distribution\n' +
            '   Stats, Open Requests, Request Frequency, Testimonials), its fully-computed data\n' +
            '   is pre-read and shown below under GRANT REPORT DATA.\n' +
            '4. DISTRIBUTION REPORT DATA — if a SchedDisbCode is mentioned, its scheduled records\n' +
            '   and Products Needed data are pre-read and shown under DISTRIBUTION REPORT DATA.\n' +
            'You CANNOT access any other workbook or Google Sheet beyond these four sources.\n' +
            'If the user asks for data from another workbook, tell them clearly and suggest ' +
            'which of the two available sources is closest to what they need.\n\n' +
            'AVAILABLE FIELDS (from Applicants_Master):\n' + JSON.stringify(allDisplayLabels) + '\n\n' +
            'FILTERABLE FIELD VALUES (actual data values for ' + fromDate + ' to ' + toDate + '):\n' +
            JSON.stringify(uniqueVals) + '\n\n' +
            hygieneContext +
            grantReportContext +
            distReportContext +
            'KNOWN REPORT TEMPLATES (use these exact column sets when the user references a named report):\n' +
            _buildReportTemplateContext_() +
            '═══════════════════════════════════════════════\n' +
            'REPORT ENGINE CAPABILITIES — READ CAREFULLY\n' +
            '═══════════════════════════════════════════════\n\n' +
            'The report engine supports TWO modes. You MUST choose the right one:\n\n' +
            'MODE 1 — ROW LIST (default)\n' +
            'Outputs one row per applicant record. Use for: detail lists, scheduling,\n' +
            'distribution reports, testimonials, open requests.\n' +
            'Spec keys: outputColumns, filters, sort, computations (summary stats at bottom only)\n\n' +
            'MODE 2 — GROUPED SUMMARY (use when user asks for totals/counts/breakdowns by a field)\n' +
            'Outputs one row per unique value of a grouping field. Use for: "count by county",\n' +
            '"how many families per income level", "requests by city", "breakdown by status".\n' +
            'REQUIRED spec keys: groupBy, aggCols (outputColumns is ignored in this mode)\n\n' +
            'SPEC JSON STRUCTURE:\n' +
            '{\n' +
            '  "title": "Report title",\n' +
            '  "outputColumns": ["Field1","Field2"],        // MODE 1 only — exact names from AVAILABLE FIELDS\n' +
            '  "groupBy": "FieldName",                       // MODE 2 only — field to group rows by\n' +
            '  "aggCols": [                                  // MODE 2 only — columns to aggregate per group\n' +
            '    {"field":"FieldName","type":"count","label":"# Records"},\n' +
            '    {"field":"FieldName","type":"countDistinct","label":"Unique Households"},\n' +
            '    {"field":"FieldName","type":"sum","label":"Total"},\n' +
            '    {"field":"FieldName","type":"avg","label":"Average"},\n' +
            '    {"field":"FieldName","type":"pct","label":"% Non-blank"}\n' +
            '  ],\n' +
            '  "filters": [{"field":"FieldName","op":"include|exclude|notBlank","values":["val"]}],\n' +
            '  "sort": [{"field":"FieldName","dir":"asc|desc"}],       // MODE 1: sort data rows; MODE 2: sort groups\n' +
            '  "computations": [{"name":"Label","type":"count|sum|avg|countDistinct","field":"F"}], // MODE 1 only\n' +
            '  "notes": "Description"\n' +
            '}\n\n' +
            'aggCols type reference:\n' +
            '  count        — number of rows in the group (use for "# records", "# requests")\n' +
            '  countDistinct — unique non-blank values (use for "# unique households" by name)\n' +
            '  sum          — numeric total (use for household member counts, product counts)\n' +
            '  avg          — numeric average\n' +
            '  pct          — percentage of non-blank values across ALL rows\n\n' +
            'DECISION GUIDE:\n' +
            '  "show me all families in Racine County" → MODE 1 (row list, filter County=Racine)\n' +
            '  "how many requests per county" → MODE 2 (groupBy County, aggCols count)\n' +
            '  "count of households by income level" → MODE 2 (groupBy Income Level, aggCols count)\n' +
            '  "breakdown of service status" → MODE 2 (groupBy Service Status, aggCols count)\n' +
            '  "use the Households report" → MODE 1 (use template columns from KNOWN REPORT TEMPLATES)\n' +
            '  "summary from Hygiene Stats workbook by month/center" → use HYGIENE STATS source (see rule 14)\n\n' +
            'CONVERSATION RULES:\n' +
            '1. Choose MODE 1 or MODE 2 based on whether the user wants rows or aggregated counts.\n' +
            '2. If the user references a named template, use those columns (MODE 1).\n' +
            '3. Apply filters and modifications the user mentions.\n' +
            '4. If you have enough info on the first message, output the spec immediately.\n' +
            '5. Only ask ONE clarifying question if truly needed.\n' +
            '6. Output the spec inside <SPEC> tags. The content between the tags must be\n' +
            '   VALID JSON ONLY — no markdown, no backticks, no explanation text inside the tags:\n' +
            '   <SPEC>{"source":"hygiene","title":"...","groupBy":[...],"aggCols":[...]}</SPEC>\n' +
            '7. After the closing </SPEC> tag, write a one-sentence plain English summary.\n' +
            '8. Use ONLY field names from AVAILABLE FIELDS in outputColumns, groupBy, aggCols.field, filters, sort.\n' +
            '   EXCEPTION: for Hygiene Stats reports (rule 14), use column names from the Stats workbook.\n' +
            '9. If revising, output the complete revised <SPEC> again.\n' +
            '10. Do not list all available fields back to the user.\n' +
            '11. FIELD REMOVAL — if the user says "ignore X", "exclude X", "remove X", "without X",\n' +
            '    "don\'t include X", or "drop X" (where X is a field or category like "products"),\n' +
            '    immediately remove ALL matching fields from outputColumns/aggCols and re-emit <SPEC>.\n' +
            '    Match broadly: "ignore the products" removes any field containing "Product" or "Baby"\n' +
            '    in its name. "ignore age brackets" removes all [Calc] age fields. Do NOT ask for\n' +
            '    clarification — just remove and re-emit the revised spec.\n' +
            '12. FIELD ADDITION — if the user says "add X" or "also include X", add matching fields\n' +
            '    to outputColumns and re-emit <SPEC> immediately.\n' +
            '13. APPROVAL PHRASES — when the user says "appears correct", "looks good",\n' +
            '    "generate it", "yes", "that works", "create the spec", "create the updated spec",\n' +
            '    "new spec", or any similar confirmation, you MUST output <SPEC>...</SPEC> in your\n' +
            '    response. This is NON-NEGOTIABLE. Do not describe the spec in words — output the\n' +
            '    actual JSON. If you already have a spec from a previous turn, re-emit it inside\n' +
            '    <SPEC> tags unchanged. Never respond to an approval phrase without a <SPEC> block.\n' +
            '14. HYGIENE STATS REPORTS — use HYGIENE BOX DISTRIBUTION STATS WORKBOOK DATA above.\n' +
            '    Include "source":"hygiene" in the spec. Available internal columns (use exact names):\n' +
            '    "Center", "Month Name", "Month Number", "Year", "Distribution Date Range",\n' +
            '    "Total Scheduled-Recipients", "Picked Up-Generic", "Picked Up-Baby",\n' +
            '    "Restock-# Recipients", "% Recipients-No Pick Up", "% Recipients-Picked Up"\n' +
            '    For grouped summaries (by Center+Month): use groupBy (array or comma-separated string)\n' +
            '    and aggCols. Example spec for "summary by Center and Month":\n' +
            '    {"source":"hygiene","title":"Center Monthly Summary",\n' +
            '     "groupBy":["Center","Month Name"],\n' +
            '     "aggCols":[\n' +
            '       {"field":"Total Scheduled-Recipients","type":"sum","label":"Scheduled"},\n' +
            '       {"field":"Picked Up-Generic","type":"sum","label":"Picked Up"},\n' +
            '       {"field":"Restock-# Recipients","type":"sum","label":"Restock"},\n' +
            '       {"field":"% Recipients-No Pick Up","type":"avg","label":"% No Pick Up"},\n' +
            '       {"field":"% Recipients-Picked Up","type":"avg","label":"% Picked Up"}\n' +
            '     ],\n' +
            '     "sort":[{"field":"Center","dir":"asc"},{"field":"Month Number","dir":"asc"}],\n' +
            '     "notes":"Monthly distribution summary by center"}\n' +
            '    Do NOT include Month Number in outputColumns/groupBy if you do not want it shown.\n' +
            '    Use Month Number ONLY in sort, not in groupBy, to sort by calendar order.\n' +
            '    Do NOT ask clarifying questions — generate the spec immediately.\n' +
            '15. GRANT REPORT SOURCES — if the user references a named grant report (Households,\n' +
            '    Distribution Stats, Open Requests, Request Frequency, Testimonials), the pre-computed\n' +
            '    data is shown above under GRANT REPORT DATA. Include "source":"grant" and\n' +
            '    "reportDataName":"<exact report name>" in the spec. Use column names from the\n' +
            '    Headers shown in GRANT REPORT DATA for outputColumns and filters.\n' +
            '16. DISTRIBUTION REPORT SOURCES — if a SchedDisbCode is mentioned (e.g. "2026-02A"),\n' +
            '    the scheduled records and Products Needed data are shown above under DISTRIBUTION\n' +
            '    REPORT DATA. Include "source":"distribution" and "distribCode":"<CODE>" in the spec.\n' +
            '    Use column names from DISTRIBUTION REPORT DATA headers for outputColumns and filters.';

        // ── Call Claude with full history ─────────────────────────────────────────
        var result = callClaudeAPIMultiTurn(systemPrompt, messages);
        if (!result.success) return result;

        var reply = result.text;
        var tokensUsed = result.tokensUsed;
        var inputTokens = result.inputTokens || 0;
        var outputTokens = result.outputTokens || 0;

        // Track cost
        var config = getAIConfig();
        var requestCost = calculateRequestCost_(config.model, inputTokens, outputTokens);
        var cumulative = trackCumulativeCost_(requestCost);

        // ── Detect spec in response ───────────────────────────────────────────────
        var specReady = false;
        var spec = null;
        var specMatch = reply.match(/<SPEC>([\s\S]*?)<\/SPEC>/);
        if (specMatch) {
            spec = extractJSON(specMatch[1]);
            if (spec) {
                specReady = true;
            } else {
                Logger.log('processReportBuilderTurn: <SPEC> found but JSON parse failed. Content: ' +
                    specMatch[1].substring(0, 500));
            }
        } else if (reply.indexOf('"source"') !== -1 || reply.indexOf('"outputColumns"') !== -1 ||
            reply.indexOf('"groupBy"') !== -1) {
            // Claude included JSON-like content but forgot the <SPEC> tags — try to extract anyway
            Logger.log('processReportBuilderTurn: No <SPEC> tags but JSON-like content found. Reply snippet: ' +
                reply.substring(0, 500));
            spec = extractJSON(reply);
            if (spec && (spec.outputColumns || spec.groupBy || spec.source)) {
                specReady = true;
                Logger.log('processReportBuilderTurn: Recovered spec from reply without <SPEC> tags.');
            }
        }

        // ── If spec ready and generate=true, run the report ──────────────────────
        var reportUrl = '';
        var downloadUrl = '';
        if (specReady && generate) {
            // Build validFields + fieldIndices from spec.outputColumns using locally built map
            var outCols2 = spec.outputColumns || [];
            var validFields2 = outCols2
                .map(function (lbl) { return labelToRawForBuilder[lbl] || lbl; })
                .filter(function (f) { return headers.indexOf(f) !== -1; });
            // Fallback: if none matched, use all headers
            if (validFields2.length === 0) {
                validFields2 = headers.filter(function (h) { return h; });
            }
            var fieldIndices2 = validFields2.map(function (f) { return headers.indexOf(f); });
            var dispLabels2 = outCols2.length > 0 ? outCols2 : validFields2;

            var genResult = applySpecAndWriteSheet(
                spec, headers, rows, validFields2, fieldIndices2,
                fromDate, toDate, tokensUsed, [], labelToRawForBuilder, dispLabels2
            );
            if (genResult.success) {
                reportUrl = genResult.reportUrl;
                downloadUrl = genResult.downloadUrl;
                tokensUsed = genResult.tokensUsed || tokensUsed;
            } else {
                Logger.log('processReportBuilderTurn: applySpecAndWriteSheet failed: ' + (genResult.error || ''));
            }
        }

        // Strip <SPEC>...</SPEC> from the reply shown to the user
        var displayReply = reply.replace(/<SPEC>[\s\S]*?<\/SPEC>/, '').trim();

        return {
            success: true,
            reply: displayReply,
            specReady: specReady,
            spec: spec,
            reportUrl: reportUrl,
            downloadUrl: downloadUrl,
            tokensUsed: tokensUsed,
            requestCost: requestCost,
            cumulativeCost: cumulative,
            consoleUrl: 'https://console.anthropic.com/settings/billing'
        };

    } catch (e) {
        Logger.log('processReportBuilderTurn error: ' + e.message);
        return { success: false, error: 'Report Builder error: ' + e.message };
    }
}


/**
 * Saves a Custom Data Sheet spec to LU_SavedReports for API-free re-use.
 * Called from AP after a successful generateAIDataSheet run.
 * v1.11 - New function.
 * @param {string} reportName - User-supplied name for this saved report
 * @param {Array}  selectedFields - Field names selected in the UI
 * @param {string} prompt - Free-text instructions the user entered
 * @param {string} incomeBracketRange - 'asis'|'3tier'|'5tier'
 * @param {Object} spec - The parsed spec JSON returned by Claude
 * @returns {Object} { success, reportId, message }
 */
function saveCustomDataSheetSpec(reportName, selectedFields, prompt, incomeBracketRange, spec) {
    try {
        if (!reportName || reportName.trim().length === 0) {
            return { success: false, error: 'Report name is required' };
        }
        if (!spec || typeof spec !== 'object') {
            return { success: false, error: 'No spec to save — generate the report first' };
        }

        var wb = getLookupsWorkbook();
        var sheet = getOrCreateSavedReportsSheet_(wb);

        // Auto-generate a unique ReportId
        var lastRow = sheet.getLastRow();
        var nextNum = lastRow; // rows include header, so lastRow == count of data rows + 1
        var reportId = 'SAVED_' + String(nextNum).padStart(3, '0');

        // Guard against duplicate names (warn only — allow saving anyway)
        var data = lastRow > 1 ? sheet.getRange(2, 1, lastRow - 1, 2).getValues() : [];
        var duplicate = data.some(function (r) {
            return r[1].toString().toLowerCase() === reportName.trim().toLowerCase();
        });

        var createdBy = '';
        try {
            createdBy = Session.getActiveUser().getEmail() || '';
        } catch (e) { /* non-fatal */ }

        var newRow = [
            reportId,
            reportName.trim(),
            spec.notes || spec.title || '',
            JSON.stringify(selectedFields || []),
            incomeBracketRange || 'asis',
            prompt || '',
            JSON.stringify(spec),
            Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'M/d/yyyy h:mm a'),
            createdBy,
            true
        ];

        sheet.appendRow(newRow);
        invalidateLookupCache();
        logAudit('SAVED_REPORT_CREATE', null, 'Saved report "' + reportName.trim() + '" (' + reportId + ')');

        return {
            success: true,
            reportId: reportId,
            duplicate: duplicate,
            message: 'Report template "' + reportName.trim() + '" saved as ' + reportId +
                (duplicate ? ' (note: another report with this name already exists)' : '')
        };
    } catch (e) {
        Logger.log('saveCustomDataSheetSpec error: ' + e.message);
        return { success: false, error: 'Failed to save template: ' + e.message };
    }
}

/**
 * Returns all active saved report definitions for populating the AP dropdown.
 * v1.11 - New function.
 * @returns {Object} { success, reports: [{ reportId, reportName, description }] }
 */
function listSavedDataSheetReports() {
    try {
        var wb = getLookupsWorkbook();
        var sheet = wb.getSheetByName(CONFIG.LOOKUPS.SAVED_REPORTS);
        if (!sheet || sheet.getLastRow() < 2) {
            return { success: true, reports: [] };
        }

        var data = sheet.getDataRange().getValues();
        var headers = trimHeaders(data[0]);
        var idCol = headers.indexOf('ReportId');
        var nameCol = headers.indexOf('ReportName');
        var descCol = headers.indexOf('Description');
        var activeCol = headers.indexOf('Active');

        var reports = [];
        for (var i = 1; i < data.length; i++) {
            var active = activeCol !== -1 ? data[i][activeCol] : true;
            if (active === false || active === 'FALSE' || active === 'false') continue;
            reports.push({
                reportId: idCol !== -1 ? data[i][idCol].toString() : '',
                reportName: nameCol !== -1 ? data[i][nameCol].toString() : '',
                description: descCol !== -1 ? data[i][descCol].toString() : ''
            });
        }

        return { success: true, reports: reports };
    } catch (e) {
        Logger.log('listSavedDataSheetReports error: ' + e.message);
        return { success: false, reports: [], error: e.message };
    }
}

/**
 * Runs a saved Custom Data Sheet report by loading its spec from LU_SavedReports
 * and applying it to fresh data — no API call required.
 * v1.11 - New function.
 * @param {string} reportId - The ReportId to run (e.g. 'SAVED_001')
 * @param {string} fromDate - Date range start (YYYY-MM-DD)
 * @param {string} toDate   - Date range end (YYYY-MM-DD)
 * @returns {Object} Same shape as generateAIDataSheet result
 */
function runSavedDataSheetReport(reportId, fromDate, toDate) {
    try {
        // Load spec row from LU_SavedReports
        var wb = getLookupsWorkbook();
        var sheet = wb.getSheetByName(CONFIG.LOOKUPS.SAVED_REPORTS);
        if (!sheet || sheet.getLastRow() < 2) {
            return { success: false, error: 'No saved reports found' };
        }

        var data = sheet.getDataRange().getValues();
        var headers = trimHeaders(data[0]);
        var idCol = headers.indexOf('ReportId');
        var nameCol = headers.indexOf('ReportName');
        var fieldsCol = headers.indexOf('SelectedFields');
        var bracketCol = headers.indexOf('IncomeBracketRange');
        var promptCol = headers.indexOf('Prompt');
        var specCol = headers.indexOf('Spec');

        var row = null;
        for (var i = 1; i < data.length; i++) {
            if (idCol !== -1 && data[i][idCol].toString() === reportId) { row = data[i]; break; }
        }
        if (!row) {
            return { success: false, error: 'Saved report "' + reportId + '" not found' };
        }

        var reportName = nameCol !== -1 ? row[nameCol].toString() : reportId;
        var selectedFields = [];
        var incomeBracketRange = bracketCol !== -1 ? row[bracketCol].toString() : 'asis';
        var prompt = promptCol !== -1 ? row[promptCol].toString() : '';
        var spec = null;

        try { selectedFields = JSON.parse(row[fieldsCol] || '[]'); } catch (e) { selectedFields = []; }
        try { spec = JSON.parse(row[specCol] || '{}'); }
        catch (e) { return { success: false, error: 'Saved spec JSON is invalid: ' + e.message }; }

        if (!spec || !spec.outputColumns) {
            return { success: false, error: 'Saved spec is missing required fields' };
        }

        // Re-run the same data pipeline as generateAIDataSheet (minus the API call)
        var from = parseDateInput(fromDate, false);
        var to = parseDateInput(toDate, true);
        var combined = getCombinedData(from, to);

        if (combined.totalCount === 0) {
            return { success: false, error: 'No records found for the specified date range' };
        }

        var rawHeaders = combined.headers;
        var rows = combined.rows;

        var useFields = (selectedFields && selectedFields.length > 0)
            ? selectedFields
            : rawHeaders.filter(function (h) { return h && h.trim().length > 0 && h !== 'Timestamp'; });

        // Separate computed / summary / regular fields (same logic as generateAIDataSheet)
        var computedFieldNames = [], summaryFieldNames = [], regularFields = [];
        var hasIncomeSourceExpanded = false;
        useFields.forEach(function (f) {
            if (f.indexOf('[Summary]') === 0) summaryFieldNames.push(f);
            else if (f.indexOf('[Calc]') === 0) {
                computedFieldNames.push(f);
                if (f === '[Calc] Income Source (Expanded)') hasIncomeSourceExpanded = true;
            } else regularFields.push(f);
        });

        var fieldIndices = [], validFields = [];
        regularFields.forEach(function (f) {
            var idx = rawHeaders.indexOf(f);
            if (idx !== -1) { fieldIndices.push(idx); validFields.push(f); }
        });

        // Delegate to generateAIDataSheet for computed columns — reuse by calling the shared
        // helper block inline. For simplicity we call generateAIDataSheet with a special flag
        // via the regular path but skip the API using the pre-built spec.
        // Instead, reconstruct allHeaders/allRows + autoSummary using the same pattern.

        // Computed columns (abbreviated — same switch as generateAIDataSheet)
        var computedHeaders = [], computedData = [];
        if (computedFieldNames.length > 0) {
            var needProducts = computedFieldNames.some(function (f) { return f.indexOf('Products') !== -1; });
            var productData = needProducts ? loadProductLookupData(from, to) : null;
            var idIdx = rawHeaders.indexOf('ID');
            var reqDateIdx = rawHeaders.indexOf('Request Date');
            var usedBeforeIdx = rawHeaders.indexOf('Have you used our services before?');
            var incomeColIdx = rawHeaders.indexOf('Total annual household income. (How much money does your family make in 1 year?)');
            var assistanceColIdx = rawHeaders.indexOf('Are you receiving any assistance? Please select ALL boxes that apply to ANY ASSISTANCE your family is receiving.');
            var prodCode1Idx = rawHeaders.indexOf('Received Product Code 1');
            var prodCode2Idx = rawHeaders.indexOf('Received Product Code 2');
            var prodCode3Idx = rawHeaders.indexOf('Received Product Code 3');
            var bracketRange = incomeBracketRange || 'asis';
            var needAges = computedFieldNames.some(function (f) {
                return f.indexOf('People') !== -1 || f.indexOf('Children') !== -1 ||
                    f.indexOf('Adults') !== -1 || f.indexOf('Seniors') !== -1;
            });

            for (var cr = 0; cr < rows.length; cr++) {
                var row2 = rows[cr];
                var computedRow = [];
                var ages = needAges ? calculateDetailedAgeBrackets(rawHeaders, row2) : null;
                var products = null;
                if (needProducts && productData && idIdx !== -1 && reqDateIdx !== -1) {
                    var recId = (row2[idIdx] || '').toString().trim();
                    var code1 = prodCode1Idx !== -1 ? (row2[prodCode1Idx] || '').toString().trim() : '';
                    var code2 = prodCode2Idx !== -1 ? (row2[prodCode2Idx] || '').toString().trim() : '';
                    var code3 = prodCode3Idx !== -1 ? (row2[prodCode3Idx] || '').toString().trim() : '';
                    var reqDate2 = new Date(row2[reqDateIdx]);
                    products = calculateProductCounts(recId, code1, code2, code3, reqDate2, productData, rawHeaders, row2);
                }
                computedFieldNames.forEach(function (calcName) {
                    var calcVal = '';
                    switch (calcName) {
                        case '[Calc] People in Household': calcVal = ages ? ages.people : 0; break;
                        case '[Calc] Children (<18)': calcVal = ages ? ages.totalChildren : 0; break;
                        case '[Calc] Adults (18-64)': calcVal = ages ? ages.totalAdults : 0; break;
                        case '[Calc] Seniors (65+)': calcVal = ages ? ages.totalSeniors : 0; break;
                        case '[Calc] Male Children': calcVal = ages ? ages.maleChildren : 0; break;
                        case '[Calc] Female Children': calcVal = ages ? ages.femaleChildren : 0; break;
                        case '[Calc] Applicant Type':
                            var usedBefore = usedBeforeIdx !== -1 ? (row2[usedBeforeIdx] || '').toString().trim() : '';
                            calcVal = getApplicantType(usedBefore); break;
                        case '[Calc] Income Bracket':
                            var rawInc = incomeColIdx !== -1 ? (row2[incomeColIdx] || '').toString().trim() : '';
                            calcVal = mapIncomeBracket(rawInc, bracketRange); break;
                        case '[Calc] Income Source (Expanded)':
                            calcVal = assistanceColIdx !== -1 ? (row2[assistanceColIdx] || '').toString().trim() : ''; break;
                        case '[Calc] Products Requested': calcVal = products ? products.productsRequested : 0; break;
                        case '[Calc] Products Distributed': calcVal = products ? products.productsDistributed : 0; break;
                    }
                    computedRow.push(calcVal);
                });
                computedData.push(computedRow);
            }
            computedHeaders = computedFieldNames;
        }

        var allHeaders = rawHeaders.concat(computedHeaders);
        var allRows = rows.map(function (r, idx) {
            return r.slice().concat(computedData[idx] || []);
        });

        // Income Source expansion
        if (hasIncomeSourceExpanded) {
            var srcColIdx2 = allHeaders.indexOf('[Calc] Income Source (Expanded)');
            if (srcColIdx2 !== -1) {
                var expandedRows2 = [];
                allRows.forEach(function (r) {
                    var srcVal = (r[srcColIdx2] || '').toString().trim();
                    if (srcVal.indexOf(', ') !== -1) {
                        srcVal.split(', ').forEach(function (src) {
                            var nr = r.slice(); nr[srcColIdx2] = src.trim(); expandedRows2.push(nr);
                        });
                    } else expandedRows2.push(r);
                });
                allRows = expandedRows2;
            }
        }

        computedHeaders.forEach(function (ch) {
            var chIdx = allHeaders.indexOf(ch);
            if (chIdx !== -1) { validFields.push(ch); fieldIndices.push(chIdx); }
        });

        // Auto-summary
        var autoSummary = [];
        if (summaryFieldNames.length > 0) {
            var fnIdx = rawHeaders.indexOf('First Name');
            var lnIdx = rawHeaders.indexOf('Last Name');
            summaryFieldNames.forEach(function (sm) {
                switch (sm) {
                    case '[Summary] Number of Households':
                        var hk = {};
                        allRows.forEach(function (r) {
                            var fn = fnIdx !== -1 ? (r[fnIdx] || '').toString().trim().toLowerCase() : '';
                            var ln = lnIdx !== -1 ? (r[lnIdx] || '').toString().trim().toLowerCase() : '';
                            if (fn || ln) hk[fn + '|' + ln] = true;
                        });
                        autoSummary.push(['Number of Households (Unique Names)', Object.keys(hk).length]); break;
                    case '[Summary] Number of Requests':
                        autoSummary.push(['Number of Requests', allRows.length]); break;
                    case '[Summary] Age Bracket Totals':
                        var bt2 = {}, tp = 0;
                        AGE_BRACKETS.forEach(function (brk) {
                            var mI = rawHeaders.indexOf(brk.male);
                            var fI = rawHeaders.indexOf(brk.female);
                            var m = 0, f = 0;
                            allRows.forEach(function (r) {
                                m += mI !== -1 ? (parseInt(r[mI]) || 0) : 0;
                                f += fI !== -1 ? (parseInt(r[fI]) || 0) : 0;
                            });
                            var lbl = brk.male.replace('[Males ', '').replace(']', '');
                            lbl = lbl.charAt(0).toUpperCase() + lbl.slice(1);
                            bt2[lbl] = { male: m, female: f, total: m + f };
                        });
                        for (var bk in bt2) {
                            autoSummary.push(['Age: ' + bk, bt2[bk].total + ' (M:' + bt2[bk].male + ' F:' + bt2[bk].female + ')']);
                            tp += bt2[bk].total;
                        }
                        autoSummary.push(['Total People', tp]); break;
                }
            });
        }

        // Build display labels via FieldMapService
        var displayLabels2 = buildDisplayLabels(validFields);
        var labelToRaw2 = buildLabelToRawMap(validFields);

        // Apply spec to data (no API call)
        var result = applySpecAndWriteSheet(
            spec, allHeaders, allRows, validFields, fieldIndices,
            fromDate, toDate, 0, autoSummary, labelToRaw2, displayLabels2
        );

        if (result.success) {
            result.tokensUsed = 0;
            result.savedReportName = reportName;
            result.message = '"' + reportName + '" report generated (no API tokens used)';
            logAudit('SAVED_REPORT_RUN', null, 'Ran saved report "' + reportName + '" (' + reportId + ')');
        }
        return result;

    } catch (e) {
        Logger.log('runSavedDataSheetReport error: ' + e.message);
        return { success: false, error: 'Failed to run saved report: ' + e.message };
    }
}
