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
 * v1.5 - Added [Summary] fields: Number of Households (unique names), Number
 *         of Requests, Age Bracket Totals with M/F breakdowns.
 *         Added [Calc] Income Bracket with 3-tier/5-tier/as-is range choice.
 *         Added [Calc] Income Source (Expanded) — splits comma-separated
 *         assistance values into individual rows.
 *         Changed Products Req/Dist to use DR/PF_Products sheet directly
 *         (loadDRPFProductsSimple) instead of BoxCode lookup.
 * v1.6 - Shared calculations: Removed loadDRPFProductsSimple, formatDateKey,
 *         buildProductKey — now uses shared loadProductLookupData() and
 *         calculateProductCounts() from GrantsReportService.gs (routes by code
 *         value: DR/PF vs BoxCode, counts DR/PF once per record).
 *         Applicant Type uses shared getApplicantType() ("Existing"/"New").
 *         Fixed age labels: Adults (18-64), Seniors (65+) to match actual
 *         calculateDetailedAgeBrackets() cutoffs.
 * v1.7 - Field Display Labels: Uses shared FIELD_DISPLAY_MAP from GrantsReportService
 *         to map raw AM column headers to friendly grant-report-style labels.
 *         Applied to field picker UI, Claude prompt field names, and output sheet
 *         column headers. All labels match GrantsReportService output columns exactly.
 * v1.8 - Adopted trimHeaders() for header read in getAmFieldGroups().
 * v1.9 - FieldMapService migration: getAMFieldGroups() now delegates to
 *         FieldMapService.getFieldsByGroup() for sheet-driven field groups
 *         instead of hardcoded groupDefs. Computed/Summary fields pulled from
 *         FieldMapService. Label/map building in generateAIDataSheet() now
 *         uses FieldMapService.buildDisplayLabels() and buildLabelToRawMap().
 *         Removed inline FIELD_DISPLAY_MAP reference comment.
 * v1.10 - Adopted CONFIG.TIMEZONE across all Utilities.formatDate() calls (#8).
 * v1.11 - Saved Custom Data Sheet Reports: generateAIDataSheet() returns savedSpec,
 *         savedFields, savedPrompt, savedBracket in result for AP to offer "Save as
 *         Template". New functions: saveCustomDataSheetSpec(), listSavedDataSheetReports(),
 *         runSavedDataSheetReport(). Specs stored in LU_SavedReports (G2N_Lookups).
 *         Re-running a saved report applies the spec to fresh data with zero API tokens.
 * v1.12 - Phone Number formatted as (XXX) XXX-XXXX in Custom Data Sheet output
 *         (applySpecAndWriteSheet output rows) and in sample data sent to Claude
 *         (generateAIDataSheet sample row builder). Uses formatPhoneNumber_() from
 *         ReportService.gs (shared GAS project scope).
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
        .filter(function(block) { return block.type === 'text'; })
        .map(function(block) { return block.text; })
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
    var sum = householdSizes.reduce(function(a, b) { return a + b; }, 0);
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
  return Object.keys(obj).map(function(k) {
    return [k, obj[k]];
  }).sort(function(a, b) {
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
  return sortedEntries(obj).map(function(entry) {
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
    .map(function(e) { return e[0]; }).join(', ') + '\n\n';

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
  sortedEntries(data.demographics.races).slice(0, 3).forEach(function(e) {
    prompt += '  ' + e[0] + ': ' + e[1] + ' (' + Math.round((e[1] / total) * 100) + '%)\n';
  });
  prompt += 'Top 3 Age Brackets:\n';
  sortedEntries(data.demographics.ages).slice(0, 3).forEach(function(e) {
    prompt += '  ' + e[0] + ': ' + e[1] + ' (' + Math.round((e[1] / total) * 100) + '%)\n';
  });
  prompt += '\n';

  prompt += '=== TOP COUNTIES ===\n';
  sortedEntries(data.geography.counties).slice(0, 5).forEach(function(e) {
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
    ).filter(function(h) { return h.length > 0; });

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
      : headers.filter(function(h) { return h && h.trim().length > 0 && h !== 'Timestamp'; });

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
      var needProducts = computedFieldNames.some(function(f) {
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
      var needAges = computedFieldNames.some(function(f) {
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
      sheetResult.savedSpec       = spec;
      sheetResult.savedFields     = selectedFields || [];
      sheetResult.savedPrompt     = prompt || '';
      sheetResult.savedBracket    = incomeBracketRange || 'asis';
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
 * Handles filtering, sorting, and computed summary metrics.
 * v1.7: Resolves display label field names back to raw AM headers for data lookup.
 *        Uses display labels for output sheet column headers.
 * @param {Object} spec - AI spec { title, outputColumns, filters, sort, computations, notes }
 * @param {Array} headers - All headers (including computed)
 * @param {Array} rows - All data rows (including computed columns)
 * @param {Array} validFields - Selected field names (raw AM headers)
 * @param {Array} fieldIndices - Column indices for selected fields
 * @param {string} fromDate - Report start date
 * @param {string} toDate - Report end date
 * @param {number} tokensUsed - API tokens consumed
 * @param {Array} autoSummary - Pre-computed summary rows [[label,value],...]
 * @param {Object} labelToRaw - Map of display labels to raw header names
 * @param {Array} displayLabels - Friendly labels parallel to validFields
 * @returns {Object} { success, reportUrl, downloadUrl, message, tokensUsed }
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
      filteredRows = rows.filter(function(row) {
        for (var fi = 0; fi < spec.filters.length; fi++) {
          var filter = spec.filters[fi];
          var fIdx = headers.indexOf(resolveField(filter.field));
          if (fIdx === -1) continue;

          var cellVal = (row[fIdx] !== null && row[fIdx] !== undefined)
            ? row[fIdx].toString().trim() : '';
          var cellLower = cellVal.toLowerCase();
          var filterVals = (filter.values || []).map(function(v) { return v.toString().trim().toLowerCase(); });

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
      filteredRows.sort(function(a, b) {
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
    for (var r = 0; r < filteredRows.length; r++) {
      var outRow = [];
      for (var c = 0; c < outIndices.length; c++) {
        var val = filteredRows[r][outIndices[c]];
        if (val instanceof Date) {
          val = Utilities.formatDate(val, CONFIG.TIMEZONE, 'M/d/yyyy');
        }
        // v1.12: Format phone number as (XXX) XXX-XXXX for Phone Number column
        if (finalRawCols[c] === 'Phone Number') {
          val = formatPhoneNumber_(val);
        }
        outRow.push(val !== null && val !== undefined ? val : '');
      }
      outputRows.push(outRow);
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

        var compValues = filteredRows.map(function(row) { return row[compIdx]; });
        var compResult = '';

        switch (comp.type) {
          case 'count':
            compResult = compValues.filter(function(v) {
              return v !== null && v !== undefined && v.toString().trim() !== '';
            }).length;
            break;
          case 'countDistinct':
            var unique = {};
            compValues.forEach(function(v) {
              var sv = (v || '').toString().trim();
              if (sv) unique[sv] = true;
            });
            compResult = Object.keys(unique).length;
            break;
          case 'sum':
            compResult = compValues.reduce(function(acc, v) {
              var n = parseFloat(v);
              return acc + (isNaN(n) ? 0 : n);
            }, 0);
            compResult = Math.round(compResult * 100) / 100;
            break;
          case 'avg':
            var nums = compValues.filter(function(v) { return !isNaN(parseFloat(v)) && v !== ''; });
            if (nums.length > 0) {
              var total = nums.reduce(function(acc, v) { return acc + parseFloat(v); }, 0);
              compResult = Math.round((total / nums.length) * 100) / 100;
            } else {
              compResult = 'N/A';
            }
            break;
          case 'pct':
            var nonBlank = compValues.filter(function(v) {
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

      // Alternate row shading (batch by 100 for performance)
      for (var ar = 1; ar < outputRows.length; ar += 2) {
        sheet.getRange(currentRow + ar, 1, 1, numCols).setBackground('#f8f9fa');
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
    var headers = ['ReportId','ReportName','Description','SelectedFields','IncomeBracketRange','Prompt','Spec','CreatedDate','CreatedBy','Active'];
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
    var duplicate = data.some(function(r) {
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
    var idCol     = headers.indexOf('ReportId');
    var nameCol   = headers.indexOf('ReportName');
    var descCol   = headers.indexOf('Description');
    var activeCol = headers.indexOf('Active');

    var reports = [];
    for (var i = 1; i < data.length; i++) {
      var active = activeCol !== -1 ? data[i][activeCol] : true;
      if (active === false || active === 'FALSE' || active === 'false') continue;
      reports.push({
        reportId:    idCol     !== -1 ? data[i][idCol].toString()   : '',
        reportName:  nameCol   !== -1 ? data[i][nameCol].toString() : '',
        description: descCol   !== -1 ? data[i][descCol].toString() : ''
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
    var idCol      = headers.indexOf('ReportId');
    var nameCol    = headers.indexOf('ReportName');
    var fieldsCol  = headers.indexOf('SelectedFields');
    var bracketCol = headers.indexOf('IncomeBracketRange');
    var promptCol  = headers.indexOf('Prompt');
    var specCol    = headers.indexOf('Spec');

    var row = null;
    for (var i = 1; i < data.length; i++) {
      if (idCol !== -1 && data[i][idCol].toString() === reportId) { row = data[i]; break; }
    }
    if (!row) {
      return { success: false, error: 'Saved report "' + reportId + '" not found' };
    }

    var reportName      = nameCol    !== -1 ? row[nameCol].toString()   : reportId;
    var selectedFields  = [];
    var incomeBracketRange = bracketCol !== -1 ? row[bracketCol].toString() : 'asis';
    var prompt          = promptCol  !== -1 ? row[promptCol].toString()  : '';
    var spec            = null;

    try { selectedFields = JSON.parse(row[fieldsCol] || '[]'); } catch (e) { selectedFields = []; }
    try { spec = JSON.parse(row[specCol] || '{}'); }
    catch (e) { return { success: false, error: 'Saved spec JSON is invalid: ' + e.message }; }

    if (!spec || !spec.outputColumns) {
      return { success: false, error: 'Saved spec is missing required fields' };
    }

    // Re-run the same data pipeline as generateAIDataSheet (minus the API call)
    var from = parseDateInput(fromDate, false);
    var to   = parseDateInput(toDate, true);
    var combined = getCombinedData(from, to);

    if (combined.totalCount === 0) {
      return { success: false, error: 'No records found for the specified date range' };
    }

    var rawHeaders = combined.headers;
    var rows = combined.rows;

    var useFields = (selectedFields && selectedFields.length > 0)
      ? selectedFields
      : rawHeaders.filter(function(h) { return h && h.trim().length > 0 && h !== 'Timestamp'; });

    // Separate computed / summary / regular fields (same logic as generateAIDataSheet)
    var computedFieldNames = [], summaryFieldNames = [], regularFields = [];
    var hasIncomeSourceExpanded = false;
    useFields.forEach(function(f) {
      if (f.indexOf('[Summary]') === 0) summaryFieldNames.push(f);
      else if (f.indexOf('[Calc]') === 0) {
        computedFieldNames.push(f);
        if (f === '[Calc] Income Source (Expanded)') hasIncomeSourceExpanded = true;
      } else regularFields.push(f);
    });

    var fieldIndices = [], validFields = [];
    regularFields.forEach(function(f) {
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
      var needProducts = computedFieldNames.some(function(f) { return f.indexOf('Products') !== -1; });
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
      var needAges = computedFieldNames.some(function(f) {
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
        computedFieldNames.forEach(function(calcName) {
          var calcVal = '';
          switch (calcName) {
            case '[Calc] People in Household':   calcVal = ages ? ages.people : 0; break;
            case '[Calc] Children (<18)':        calcVal = ages ? ages.totalChildren : 0; break;
            case '[Calc] Adults (18-64)':        calcVal = ages ? ages.totalAdults : 0; break;
            case '[Calc] Seniors (65+)':         calcVal = ages ? ages.totalSeniors : 0; break;
            case '[Calc] Male Children':         calcVal = ages ? ages.maleChildren : 0; break;
            case '[Calc] Female Children':       calcVal = ages ? ages.femaleChildren : 0; break;
            case '[Calc] Applicant Type':
              var usedBefore = usedBeforeIdx !== -1 ? (row2[usedBeforeIdx] || '').toString().trim() : '';
              calcVal = getApplicantType(usedBefore); break;
            case '[Calc] Income Bracket':
              var rawInc = incomeColIdx !== -1 ? (row2[incomeColIdx] || '').toString().trim() : '';
              calcVal = mapIncomeBracket(rawInc, bracketRange); break;
            case '[Calc] Income Source (Expanded)':
              calcVal = assistanceColIdx !== -1 ? (row2[assistanceColIdx] || '').toString().trim() : ''; break;
            case '[Calc] Products Requested':    calcVal = products ? products.productsRequested : 0; break;
            case '[Calc] Products Distributed':  calcVal = products ? products.productsDistributed : 0; break;
          }
          computedRow.push(calcVal);
        });
        computedData.push(computedRow);
      }
      computedHeaders = computedFieldNames;
    }

    var allHeaders = rawHeaders.concat(computedHeaders);
    var allRows = rows.map(function(r, idx) {
      return r.slice().concat(computedData[idx] || []);
    });

    // Income Source expansion
    if (hasIncomeSourceExpanded) {
      var srcColIdx2 = allHeaders.indexOf('[Calc] Income Source (Expanded)');
      if (srcColIdx2 !== -1) {
        var expandedRows2 = [];
        allRows.forEach(function(r) {
          var srcVal = (r[srcColIdx2] || '').toString().trim();
          if (srcVal.indexOf(', ') !== -1) {
            srcVal.split(', ').forEach(function(src) {
              var nr = r.slice(); nr[srcColIdx2] = src.trim(); expandedRows2.push(nr);
            });
          } else expandedRows2.push(r);
        });
        allRows = expandedRows2;
      }
    }

    computedHeaders.forEach(function(ch) {
      var chIdx = allHeaders.indexOf(ch);
      if (chIdx !== -1) { validFields.push(ch); fieldIndices.push(chIdx); }
    });

    // Auto-summary
    var autoSummary = [];
    if (summaryFieldNames.length > 0) {
      var fnIdx = rawHeaders.indexOf('First Name');
      var lnIdx = rawHeaders.indexOf('Last Name');
      summaryFieldNames.forEach(function(sm) {
        switch (sm) {
          case '[Summary] Number of Households':
            var hk = {};
            allRows.forEach(function(r) {
              var fn = fnIdx !== -1 ? (r[fnIdx] || '').toString().trim().toLowerCase() : '';
              var ln = lnIdx !== -1 ? (r[lnIdx] || '').toString().trim().toLowerCase() : '';
              if (fn || ln) hk[fn + '|' + ln] = true;
            });
            autoSummary.push(['Number of Households (Unique Names)', Object.keys(hk).length]); break;
          case '[Summary] Number of Requests':
            autoSummary.push(['Number of Requests', allRows.length]); break;
          case '[Summary] Age Bracket Totals':
            var bt2 = {}, tp = 0;
            AGE_BRACKETS.forEach(function(brk) {
              var mI = rawHeaders.indexOf(brk.male);
              var fI = rawHeaders.indexOf(brk.female);
              var m = 0, f = 0;
              allRows.forEach(function(r) {
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
    var labelToRaw2    = buildLabelToRawMap(validFields);

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
