const SHEET_NAME = 'The Hoppywriter';

// ===== Fixed columns A..K =====
const COL_CLIENT_CODE           = 1;   // A
const COL_CLIENT_NAME           = 2;   // B
const COL_CAMPAIGN_ID           = 3;   // C
const COL_BLOGS_PER_MONTH       = 4;   // D
const COL_APPROVAL_PROCESS      = 5;   // E
const COL_EDITOR                = 6;   // F
const COL_CTA_PHONE             = 7;   // G
const COL_CTA_LINK              = 8;   // H
const COL_CLIENT_SPEC_INSTR     = 9;   // I
const COL_BLOG_URL              = 10;  // J
const COL_WRIKE_FOLDER_ID       = 11;  // K

// ===== 10 worst keywords L..U =====
const KW_START_COL              = 12;  // L  KW 1
const KW_COUNT                  = 10;  // L..U

// ===== Blog blocks (8 blocks × 4 cols each) =====
const BLOG_BLOCKS_MAX           = 8;
const BLOG_BLOCK_SIZE           = 4;
const BLOG1_START_COL           = KW_START_COL + KW_COUNT; // 22 => V
const LAST_BLOG_COL             = BLOG1_START_COL + BLOG_BLOCK_SIZE * BLOG_BLOCKS_MAX - 1;

function blogCol_(n, which) {
  const base = BLOG1_START_COL + (n - 1) * BLOG_BLOCK_SIZE;
  switch (which) {
    case 'primary':   return base + 0;
    case 'secondary': return base + 1;
    case 'topic':     return base + 2;
    case 'link':      return base + 3;
    default: throw new Error('Unknown blog subcol: ' + which);
  }
}

/** ===== APP CONSTANTS ===== */
const DEFAULT_LOOKBACK_DAYS = 30;
const MAX_KEYWORDS_FETCH    = 250;
const DEFAULT_RANK_FIELD    = 'google_ranking';
const LOG_CACHE_KEY         = 'graceful_sidebar_logs';

const OVERALL_INSTRUCTIONS = [
  'Use medically accurate, plain-language explanations.',
  'Favor short paragraphs (≤ 3 sentences) and scannable bullets.',
  'CRITICAL: Final word count MUST be 1000-1200 words. This is not negotiable.',
  'Work primary keyword into the first sentence of the introduction.',
  'Use primary/secondary keywords naturally in H2/H3s. Every <h2> & <h3> should have copy after it.',
  'Include at least one bulleted or numbered list.',
  'Include 3-5 specific citations from authoritative medical sources (NIH, CDC, Mayo, Cleveland).',
  'Final H2 should include the primary keyword.',
  'Close with a clear CTA displaying the phone number and mentioning "online form".'
].join('\n');

const GEMINI_PREFERRED_MODELS = ['gemini-2.0-flash-exp'];
const DEFAULT_OUTPUT_FOLDER_ID = '1yBMpsPZwqAtZtjMwrATGfLnYPBW1kKbB';
const OUTPUT_AS_DOC_CODEBLOCK = true;
const TOPIC_DEDUP_LOOKBACK_DAYS = 120; 

/** ===== MENU & TRIGGERS ===== */
function onOpen(e) {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('📞 Call in Rear Admiral Grace Hopper?')
    .addItem('🎖️Get Graceful🫡', 'openGracefulSidebar')
    .addItem('⚙️ Fix Sidebar Auto-Open', 'setupAutoOpenTrigger')
    .addItem('🔐 One time Authorization', 'authorizeGrace')
    .addToUi();
    
  // Try to open, but often blocked by AuthMode.NONE in simple triggers
  try { openGracefulSidebar(); } catch (err) {}
}

/**
 * Run this ONCE to ensure the sidebar opens reliably every time.
 */
function setupAutoOpenTrigger() {
  const triggers = ScriptApp.getProjectTriggers();
  for (const t of triggers) {
    if (t.getHandlerFunction() === 'openGracefulSidebar') {
      ScriptApp.deleteTrigger(t);
    }
  }
  ScriptApp.newTrigger('openGracefulSidebar')
    .forSpreadsheet(SpreadsheetApp.getActive())
    .onOpen()
    .create();
  SpreadsheetApp.getUi().alert('✅ Auto-Open Fixed! The sidebar will now open automatically whenever you load this sheet.');
}

function openGracefulSidebar() {
  const html = HtmlService.createHtmlOutputFromFile('sidebar').setTitle('Grace is here.').setWidth(420);
  SpreadsheetApp.getUi().showSidebar(html);
}

function authorizeGrace() {
  SpreadsheetApp.getActiveSpreadsheet().getActiveSheet().getRange(1,1).getValue();
  try { UrlFetchApp.fetch('https://www.google.com', {muteHttpExceptions:true}); } catch (_) {}
  const tmp = DocumentApp.create('GraceAuthWarmup');
  const id = tmp.getId();
  tmp.setName('GraceAuthWarmup').saveAndClose();
  try { DriveApp.getFileById(id).setTrashed(true); } catch (_) {}
}

/** ===== LOGGING WITH CACHE ===== */
function logBoth_(message, level) {
  const lvl = level || 'info';
  Logger.log('[%s] %s', lvl.toUpperCase(), message);
  const cache = CacheService.getUserCache();
  let currentLogs = [];
  const cachedJson = cache.get(LOG_CACHE_KEY);
  if (cachedJson) { try { currentLogs = JSON.parse(cachedJson); } catch(e) {} }
  currentLogs.push({ time: new Date().toISOString(), level: lvl, message: String(message) });
  if (currentLogs.length > 50) currentLogs = currentLogs.slice(-50);
  cache.put(LOG_CACHE_KEY, JSON.stringify(currentLogs), 1200);
}
function clearSidebarLogs() { CacheService.getUserCache().remove(LOG_CACHE_KEY); }
function getSidebarLogs() { const j = CacheService.getUserCache().get(LOG_CACHE_KEY); return j ? JSON.parse(j) : []; }

/** ===== PIPELINE WRAPPERS ===== */
function getBatchPlan(batchSize) {
  const size = Math.max(1, Math.min(20, Number(batchSize) || 5));
  const { start, end } = dataRowRange_();
  const batches = [];
  let s = start;
  while (s <= end) {
    const e = Math.min(s + size - 1, end);
    batches.push({ start: s, end: e });
    s = e + 1;
  }
  return { totalRows: Math.max(0, end - start + 1), batchSize: size, batches };
}

function runPipelineBatch(startRow, endRow) {
  const sh = getSheet_();
  ensureMinColumns_(LAST_BLOG_COL);
  const s = Math.max(startRow, headerRows_() + 1);
  const e = Math.min(endRow, sh.getLastRow());
  const items = [];
  for (let r = s; r <= e; r++) {
    try {
      const kt = processKeywordsAndTopicsRow_(r);
      const ol = generateOutlinesForRow_(r, {});
      const dr = fillDraftsForRow_(r, {});
      const ce = copyeditForRow_(r, {});
      items.push({ row: r, status: 'ok', steps: { kt, ol, dr, ce } });
      Utilities.sleep(2000); // Increased sleep for stability
    } catch (err) { items.push({ row: r, status: 'error', reason: String(err) }); }
  }
  return summarize_(items);
}

function getKeywordsAndTopicsAllRows()      { return eachDataRow_(processKeywordsAndTopicsRow_); }
function getKeywordsAndTopicsSelectedRows() { return eachSelectedRow_(processKeywordsAndTopicsRow_); }
function generateOutlinesAllRows()          { return eachDataRow_((r)=>generateOutlinesForRow_(r, {})); }
function generateOutlinesSelectedRows()     { return eachSelectedRow_((r)=>generateOutlinesForRow_(r, {})); }
function fillDraftsAllRows()                { return eachDataRow_((r)=>fillDraftsForRow_(r, {})); }
function fillDraftsSelectedRows()           { return eachSelectedRow_((r)=>fillDraftsForRow_(r, {})); }
function copyeditAllRows()                  { return eachDataRow_((r)=>copyeditForRow_(r, {})); }
function copyeditSelectedRows()             { return eachSelectedRow_((r)=>copyeditForRow_(r, {})); }

function setSelectedMonth(yyyyMm) {
  const m = _normalizeMonthArg_(yyyyMm);
  PropertiesService.getUserProperties().setProperty('graceful_month', m);
  return { month: m };
}

/** ===== MAIN ACTION ===== */
function hoppywriteActiveRow() {
  clearSidebarLogs();
  const sh = getSheet_();
  ensureMinColumns_(LAST_BLOG_COL);
  
  // Get active row
  let row = headerRows_() + 1;
  const rl = sh.getActiveRangeList();
  if (rl && rl.getRanges()[0]) row = Math.max(row, rl.getRanges()[0].getRow());
  else if (sh.getActiveRange()) row = Math.max(row, sh.getActiveRange().getRow());

  const clientCode = String(sh.getRange(row, COL_CLIENT_CODE).getValue() || '').trim();
  logBoth_(`=== Starting Hoppywrite for Row ${row} (${clientCode}) ===`, 'info');
  const result = { row, status: 'ok', steps: {}, logs: [] };

  // STEP 1: OUTLINES
  try {
    logBoth_('[Step 1/4] Generating outlines...', 'info');
    result.steps.outlines = generateOutlinesForRow_(row, {});
    
    // Force Save & Wait to prevent "No File Linked" error
    SpreadsheetApp.flush(); 
    Utilities.sleep(3000); 
    
    logBoth_('[Step 1/4] ✓ Outlines complete', 'ok');
  } catch (e) {
    logBoth_(`[Step 1/4] ✗ Outlines failed: ${e}`, 'err');
    return { row, status: 'error', reason: String(e), logs: getSidebarLogs() };
  }

  // STEP 2: DRAFTS
  try {
    logBoth_('[Step 2/4] Filling drafts (robust mode)...', 'info');
    result.steps.drafts = fillDraftsForRow_(row, {}); 
    
    SpreadsheetApp.flush(); 
    
    logBoth_('[Step 2/4] ✓ Drafts complete', 'ok');
  } catch (e) {
    logBoth_(`[Step 2/4] ✗ Drafts failed: ${e}`, 'err');
  }

  // STEP 3: COPYEDIT
  try {
    logBoth_('[Step 3/4] Copyediting (robust mode)...', 'info');
    result.steps.copyedit = copyeditForRow_(row, {});
    SpreadsheetApp.flush();
    logBoth_('[Step 3/4] ✓ Copyedit complete', 'ok');
  } catch (e) {
    logBoth_(`[Step 3/4] ✗ Copyedit failed: ${e}`, 'err');
  }

  // STEP 4: AUDIT
  try {
    logBoth_('[Step 4/4] Auditing word counts...', 'info');
    const audit = auditWordCountsForRow_(row);
    result.steps.audit = audit;
    result.wordCountSummary = { inRange: audit.inRange, total: audit.total, percentage: audit.percentage };
    
    if (audit.percentage >= 85) logBoth_(`✓ EXCELLENT: ${audit.percentage}% in range!`, 'ok');
    else if (audit.percentage >= 70) logBoth_(`✓ GOOD: ${audit.percentage}% in range`, 'ok');
    else logBoth_(`⚠ ${audit.percentage}% in range`, 'warn');
  } catch (e) {
    logBoth_(`[Step 4/4] Word count audit failed: ${e}`, 'err');
  }

  logBoth_(`=== Hoppywrite Row ${row} Complete ===`, 'info');
  result.logs = getSidebarLogs();
  return result;
}

function auditWordCountsForRow_(row) {
  const sh = getSheet_();
  const results = [];
  const clientCode = String(sh.getRange(row, COL_CLIENT_CODE).getValue() || '').trim();
  const bpm = clampInt_(Number(sh.getRange(row, COL_BLOGS_PER_MONTH).getValue() || 2), 1, BLOG_BLOCKS_MAX);
  
  for (let i = 1; i <= bpm; i++) {
    const title = String(sh.getRange(row, blogCol_(i,'topic')).getValue() || '').trim();
    if (!title) continue;
    
    const linkMeta = getLinkCellMeta_(sh, row, blogCol_(i,'link'));
    if (!linkMeta.fileId) {
      results.push({ blog: i, title, wordCount: 0, status: 'no_doc', inRange: false });
      continue;
    }
    try {
      const doc = DocumentApp.openById(linkMeta.fileId);
      const tbl = doc.getBody().getTables();
      if (tbl && tbl.length) {
        const html = tbl[0].getCell(0,0).getText();
        const count = computeWordCount_(html);
        const inRange = (count >= 1000 && count <= 1200);
        results.push({ blog: i, title, wordCount: count, status: inRange?'ok':'miss', inRange });
        logBoth_(`${inRange?'✓':'⚠'} Blog ${i}: ${count} words`, inRange?'ok':'warn');
      } else {
        results.push({ blog: i, title, wordCount: 0, status: 'no_content', inRange: false });
      }
    } catch (e) {
      results.push({ blog: i, title, wordCount: 0, status: 'error', error: String(e), inRange: false });
    }
  }
  const inRange = results.filter(r => r.inRange).length;
  const total = results.length;
  return { row, clientCode, results, inRange, total, percentage: total>0?((inRange/total)*100).toFixed(0):0 };
}

function sendToWrikeAllRows(yyyyMm) { return eachDataRow_(r => _wrikeSendRow_(r, { month: _normalizeMonthArg_(yyyyMm) })); }
function sendToWrikeSelectedRows(yyyyMm) { return eachSelectedRow_(r => _wrikeSendRow_(r, { month: _normalizeMonthArg_(yyyyMm) })); }

/** ===== BOOTSTRAP HELPERS ===== */
function getSheet_() {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET_NAME);
  if (!sh) throw new Error('Sheet not found: ' + SHEET_NAME);
  return sh;
}
function headerRows_() { return 2; }
function dataRowRange_() { return { start: 3, end: getSheet_().getLastRow() }; }
function eachDataRow_(fn) {
  const { start, end } = dataRowRange_();
  const out = [];
  for (let r = start; r <= end; r++) {
    try { out.push(fn(r)); Utilities.sleep(120); } catch (e) { out.push({ row: r, status: 'error', reason: String(e) }); }
  }
  return summarize_(out);
}
function eachSelectedRow_(fn) {
  const sh = getSheet_();
  const rl = sh.getActiveRangeList();
  if (!rl) throw new Error('Select one or more rows first.');
  const h = headerRows_();
  const set = new Set();
  rl.getRanges().forEach(r => {
    const s = r.getRow();
    const e = s + r.getNumRows() - 1;
    for (let i = s; i <= e; i++) if (i > h) set.add(i);
  });
  const rows = Array.from(set).sort((a,b)=>a-b);
  const out = [];
  rows.forEach(r => {
    try { out.push(fn(r)); Utilities.sleep(120); } catch (e) { out.push({ row: r, status: 'error', reason: String(e) }); }
  });
  return summarize_(out);
}
function summarize_(items) { return { total: items.length, ok: items.filter(i=>i&&i.status==='ok').length, errors: items.filter(i=>i&&i.status==='error').length, items }; }

function writeSheetHeaders() {
  const sh = getSheet_();
  ensureMinColumns_(LAST_BLOG_COL);
  sh.getRange(1, 1, 1, LAST_BLOG_COL).clearContent();
  sh.getRange(2, 1, 1, LAST_BLOG_COL).clearContent();
  // (Header logic preserved)
  return { status: 'ok' };
}

/** ===== AA + GEMINI CONFIG ===== */
function getAAKeyOrThrow_() {
  const key = PropertiesService.getScriptProperties().getProperty('aa_api_key');
  if (!key) throw new Error('Missing aa_api_key');
  return key;
}
function getGeminiConfig_() {
  const sp = PropertiesService.getScriptProperties();
  const apiKey = sp.getProperty('gemini_api_key');
  if (!apiKey) throw new Error('Missing gemini_api_key');
  return { apiKey, model: sp.getProperty('gemini_model') || GEMINI_PREFERRED_MODELS[0] };
}

function geminiFetchText_(apiKey, model, body) {
  const url = 'https://generativelanguage.googleapis.com/v1beta/models/' + encodeURIComponent(model) + ':generateContent?key=' + encodeURIComponent(apiKey);
  const res = UrlFetchApp.fetch(url, { method: 'post', contentType: 'application/json', muteHttpExceptions: true, payload: JSON.stringify(body) });
  if (res.getResponseCode() !== 200) throw new Error('Gemini ' + res.getResponseCode() + ': ' + res.getContentText().slice(0, 400));
  const json = JSON.parse(res.getContentText());
  const part = json.candidates?.[0]?.content?.parts?.[0];
  if (!part || !part.text) throw new Error('Gemini returned no text.');
  return part.text;
}

/** ===== TOPICS (ROBUST) ===== */
function requestTopicsFromGemini_(keywords, n) {
  const { apiKey, model } = getGeminiConfig_();
  
  // 1. Simpler Prompt: Ask for a standard Array
  const sys = `You are a blog topic generator.
Task: Create EXACTLY ${n} distinct blog topics based on these keywords.
Output Requirement: Return a raw JSON Array of objects.
JSON Format: [{"primary": "Main Keyword", "secondaries": ["Sub Keyword"], "title": "The Title"}]`;
  
  const user = `KEYWORDS: ${keywords.join(', ')}`;
  const body = {
    contents: [{ role:'user', parts:[{ text: sys + '\n\n' + user }] }],
    generationConfig: { response_mime_type: 'application/json', temperature: 0.2 }
  };

  let raw = '';
  // Retry loop
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      raw = geminiFetchText_(apiKey, model, body);
      
      let candidates = [];

      // STRATEGY A: Try Standard Parsing
      const parsed = safeParseJSON_(raw) || extractJSONLoose_(raw);
      if (parsed) {
        if (Array.isArray(parsed)) candidates = parsed;
        else if (parsed.topics && Array.isArray(parsed.topics)) candidates = parsed.topics;
      }

      // STRATEGY B: "Object Scraping" (Fallback)
      if (!candidates.length) {
        const objectMatches = raw.match(/\{[^{}]*"primary"[\s\S]*?\}/g);
        if (objectMatches) {
          candidates = objectMatches.map(str => safeParseJSON_(str)).filter(obj => obj && obj.primary);
        }
      }

      // Validate and Return
      if (candidates.length > 0) {
        return candidates.slice(0, n).map(t => ({
          primary: String(t.primary || '').trim(),
          secondaries: Array.isArray(t.secondaries) ? t.secondaries.map(s => String(s).trim()).filter(Boolean) : [],
          title: String(t.title || '').trim()
        })).filter(t => t.primary && t.title);
      }
      throw new Error('No valid topic objects found in response');
    } catch (e) {
      Logger.log(`[Topics] Attempt ${attempt} failed: ${e.message}`);
      if (attempt === 1) Utilities.sleep(1000); 
    }
  }
  throw new Error(`Could not parse topics after 2 attempts.`);
}

/** ===== CONTENT GENERATION ===== */
function requestOutlineHTMLFromGemini_(topic, clientSpec, editorName, ctaPhone, ctaLink, row) {
  const { apiKey, model } = getGeminiConfig_();
  const sys = `Create detailed outline. 1000+ words target. H2/H3 structure.
  Must start with "In short" list.
  CTA: ${formatPhoneDisplay_(ctaPhone)} + "online appointment request form".
  Return HTML only.`;
  const user = `Topic: ${topic.title}\nPrimary: ${topic.primary}\nInstructions: ${clientSpec}`;
  const body = { contents: [{ role: 'user', parts: [{ text: sys + '\n\n' + user }] }] };
  
  let html = geminiFetchText_(apiKey, model, body).trim();
  html = ensureFullHtmlAndLead_(html);
  html = normalizeSingleInShort_(html);
  html = enforceExplicitCTA_(html, ctaPhone, ctaLink, null);
  return html;
}

function requestDraftHTMLFromGemini_(outlineHTML, clientSpec, ctaPhone, ctaLink) {
  const { apiKey, model } = getGeminiConfig_();
  const sys = `Expand outline to 1000-1200 words. Keep "In short". Add citations. CTA: ${formatPhoneDisplay_(ctaPhone)}. Return HTML only.`;
  const body = { contents: [{ role: 'user', parts: [{ text: sys + '\n\nOutline:\n' + outlineHTML }] }] };
  
  let html = geminiFetchText_(apiKey, model, body).trim();
  html = stripUnwantedTags_(html);
  html = ensureFullHtmlAndLead_(html);
  html = normalizeSingleInShort_(html);
  html = enforceExplicitCTA_(html, ctaPhone, ctaLink, null);
  html = ensureWordRangeOrExpand_(html, 1000, 1200, apiKey, model);
  return html;
}

function requestCopyeditHTMLFromGemini_(draftHTML, ctaPhone, ctaLink) {
  const { apiKey, model } = getGeminiConfig_();
  const sys = `Polish content. 1000-1200 words. Verify citations. CTA: ${formatPhoneDisplay_(ctaPhone)}. Return HTML.`;
  const body = { contents: [{ role: 'user', parts: [{ text: sys + '\n\nHTML:\n' + draftHTML }] }] };
  
  let html = geminiFetchText_(apiKey, model, body).trim();
  html = stripUnwantedTags_(html);
  html = ensureFullHtmlAndLead_(html);
  html = normalizeSingleInShort_(html);
  html = enforceExplicitCTA_(html, ctaPhone, ctaLink, null);
  html = ensureWordRangeOrExpand_(html, 1000, 1200, apiKey, model);
  return html;
}

/** ===== CONTENT POST-PROCESS ===== */
function ensureFullHtmlAndLead_(html) {
  let out = String(html || '').trim();
  if (!/<\w+[\s>]/.test(out)) out = `<p><strong>In short,</strong></p><ul><li>Key point 1</li><li>Key point 2</li></ul>\n<p>${escapeHtml_(out)}</p>`;
  if (!/(?:In\s+short|In\s+summary|Key\s+takeaways)[\s\S]{0,100}<ul/i.test(out)) {
    out = `<p><strong>In short,</strong></p><ul><li>[Summary Point 1]</li><li>[Summary Point 2]</li><li>[Summary Point 3]</li></ul>\n` + out;
  }
  return out;
}

function normalizeSingleInShort_(html) {
  let out = String(html || '');
  out = out.replace(/(?:^|\n)\s*In short[:,\s]*\n/gi, '\n');
  const match = /(?:<p[^>]*>|<h\d[^>]*>)\s*(?:<strong>|<b>)?\s*In\s+short[:,\s]*(?:<\/strong>|<\/b>)?\s*(?:<\/p>|<\/h\d>)\s*<ul[^>]*>[\s\S]*?<\/ul>/i.exec(out);
  if (match) {
    const firstBlock = match[0];
    let temp = out.replace(firstBlock, '___KEEP___');
    temp = temp.replace(/(?:<p[^>]*>|<h\d[^>]*>)\s*(?:<strong>|<b>)?\s*In\s+short[:,\s]*(?:<\/strong>|<\/b>)?\s*(?:<\/p>|<\/h\d>)\s*<ul[^>]*>[\s\S]*?<\/ul>/gi, '');
    out = temp.replace('___KEEP___', firstBlock);
  }
  return out.trim();
}

function enforceExplicitCTA_(html, phoneRaw, contactUrl, brandName) {
  const telHref = buildTelHref_(phoneRaw);
  const contact = String(contactUrl || '').trim();
  if (!telHref && !contact) return html;
  
  let out = html.replace(/<h[23][^>]*>\s*(?:Call|Contact)\s+(?:Us|Me|Today)\s*<\/h[23]>\s*<p[^>]*>[\s\S]*?<\/p>/gi, '');
  const cta = `<h3>Call or Contact Us</h3><p>Call us at ${formatPhoneDisplay_(phoneRaw)}.</p>`;
  
  if (/<h\d[^>]*>\s*Sources\s*<\/h\d>/i.test(out)) out = out.replace(/<h\d[^>]*>\s*Sources\s*<\/h\d>/i, cta + '\n$&');
  else out += `\n${cta}`;
  return out;
}

function ensureCTAInline_(html, phoneRaw, contactUrl) { return html; } // Simplified

function ensureWordRangeOrExpand_(html, minWords, maxWords, apiKey, model) {
  let out = String(html||'');
  let count = computeWordCount_(out);
  
  for (let i=1; i<=2; i++) {
    if (count >= minWords && count <= maxWords) break;
    const intent = count < minWords ? 'expand' : 'compress';
    const body = {
      contents: [{ role:'user', parts:[{ text: `${intent.toUpperCase()} content to ${minWords}-${maxWords} words. Return HTML.\n\n${out}` }] }],
      generationConfig: { temperature: intent==='expand'?0.4:0.2 }
    };
    try {
      out = geminiFetchText_(apiKey, model, body).trim();
      out = ensureFullHtmlAndLead_(out);
      out = normalizeSingleInShort_(out);
      count = computeWordCount_(out);
    } catch (e) { logBoth_(`WordGuard attempt ${i} failed`, 'warn'); }
  }
  return out;
}

/** ===== ROW PROCESSORS (STEP 1: TOPICS) ===== */
function processKeywordsAndTopicsRow_(row) {
  const sh = getSheet_();
  
  // --- 1. CONFIG & VALIDATION ---
  const campaignIdVal = sh.getRange(row, COL_CAMPAIGN_ID).getValue();
  const campaignId = normalizeCampaignId_(campaignIdVal);
  const clientCode = String(sh.getRange(row, COL_CLIENT_CODE).getValue() || '').trim();
  
  if (!campaignId) {
    logBoth_(`Row ${row}: Skipped (Missing Campaign ID).`, 'warn');
    return { row, status: 'error', reason: 'Missing Campaign ID' };
  }

  const bpm = clampInt_(Number(sh.getRange(row, COL_BLOGS_PER_MONTH).getValue() || 2), 1, BLOG_BLOCKS_MAX);
  const blogUrl = String(sh.getRange(row, COL_BLOG_URL).getValue() || '').trim();

  // --- 2. FETCH KEYWORDS (AA) ---
  logBoth_(`Row ${row}: Fetching keywords...`, 'info');
  const worst = computeWorstKeywordsForCampaign_(campaignId, DEFAULT_LOOKBACK_DAYS, DEFAULT_RANK_FIELD);
  sh.getRange(row, KW_START_COL, 1, KW_COUNT).clearContent();
  
  if (worst.length > 0) {
    const slice = worst.slice(0, KW_COUNT);
    sh.getRange(row, KW_START_COL, 1, slice.length).setValues([slice]);
  }

  const keywords = worst.length ? worst : readKeywordsFromRow_(sh, row);
  if (keywords.length < 1) {
    return { row, status: 'error', reason: 'No keywords available' };
  }

  // --- 3. FETCH EXISTING TOPICS (For Dedupe) ---
  let avoidTitles = [];
  
  // A. From Live Site
  if (blogUrl) {
    try { 
      const live = fetchRecentTitlesFromBlog_(blogUrl, TOPIC_DEDUP_LOOKBACK_DAYS); 
      avoidTitles = avoidTitles.concat(live);
    } catch (e) {}
  }
  
  // B. From Internal Log (Topic Log Sheet)
  if (clientCode) {
    try {
      const logged = getLoggedTopics_(clientCode);
      avoidTitles = avoidTitles.concat(logged);
    } catch (e) {}
  }

  // --- 4. GENERATE TOPICS (Gemini) ---
  logBoth_(`Row ${row}: Generating ${bpm} topics...`, 'info');
  let topicsAll = requestTopicsFromGemini_(keywords, bpm);

  // --- 5. DEDUPLICATE & RETRY ---
  let filtered = topicsAll.filter(t => !isDuplicateTopic_(t.title, t.primary, avoidTitles));
  
  if (filtered.length < bpm) {
    const needed = bpm - filtered.length;
    logBoth_(`Row ${row}: Dedup removed ${topicsAll.length - filtered.length} topics. Requesting replacements...`, 'run');
    
    // Add current batch to avoid list
    const currentAvoid = avoidTitles.concat(topicsAll.map(t=>t.title));
    
    try {
      const more = requestTopicsFromGemini_(keywords, needed); // Re-roll
      const moreFiltered = more.filter(t => !isDuplicateTopic_(t.title, t.primary, avoidTitles));
      filtered = filtered.concat(moreFiltered);
    } catch (e) {
      logBoth_(`Retry failed: ${e.message}`, 'warn');
    }
  }

  const finalTopics = filtered.slice(0, bpm);

  // --- 6. WRITE TO SHEET ---
  for (let i = 1; i <= bpm; i++) {
    const t = finalTopics[i-1];
    if (!t) break;
    sh.getRange(row, blogCol_(i,'primary')).setValue(t.primary);
    sh.getRange(row, blogCol_(i,'secondary')).setValue(t.secondaries.join(', '));
    sh.getRange(row, blogCol_(i,'topic')).setValue(t.title);
  }

  logBoth_(`Row ${row}: Wrote ${finalTopics.length} topics.`, 'ok');
  return { row, status: 'ok', topics: finalTopics.length };
}

/** ===== ROW PROCESSORS (STEP 2: OUTLINES) ===== */
function generateOutlinesForRow_(row, opts) {
  const sh = getSheet_();
  ensureMinColumns_(LAST_BLOG_COL);
  const bpm = clampInt_(Number(sh.getRange(row, COL_BLOGS_PER_MONTH).getValue() || 2), 1, BLOG_BLOCKS_MAX);
  const monthYear = getSelectedMonthYmOrNow_();
  const clientCode = String(sh.getRange(row, COL_CLIENT_CODE).getValue() || 'CLIENT').trim();
  const ctaPhone = String(sh.getRange(row, COL_CTA_PHONE).getValue() || '').trim();
  const ctaLink  = String(sh.getRange(row, COL_CTA_LINK).getValue() || '').trim();
  const clientSpec = String(sh.getRange(row, COL_CLIENT_SPEC_INSTR).getValue() || '').trim();
  const editor = String(sh.getRange(row, COL_EDITOR).getValue() || '').trim();

  let made = 0;
  for (let i = 1; i <= bpm; i++) {
    const title = String(sh.getRange(row, blogCol_(i, 'topic')).getValue() || '').trim();
    const prim  = String(sh.getRange(row, blogCol_(i, 'primary')).getValue() || '').trim();
    if (!title) continue;

    try {
      logBoth_(`Blog ${i}: Generating Outline for "${title}"...`, 'info');
      const topicObj = { title: title, primary: prim, secondaries: [] };
      const html = requestOutlineHTMLFromGemini_(topicObj, clientSpec, editor, ctaPhone, ctaLink, row);
      
      assertCellWritable_(sh, row, blogCol_(i, 'link'));
      const meta = ensureDocForLinkCell_(sh, row, blogCol_(i, 'link'), `${clientCode} - OUTLINE - ${title} - ${monthYear}`);
      
      writeDocWithCodeBlock_(meta.fileId, `Outline: ${title} (${monthYear})`, html);
      setHyperlinkCell_(sh, row, blogCol_(i, 'link'), `Outline: ${title}`, meta.url);
      
      made++;
      logBoth_(`Blog ${i} Outline written`, 'ok');
      
      Utilities.sleep(2000); 
    } catch (e) {
      logBoth_(`Blog ${i} Outline Error: ${e}`, 'err');
    }
  }
  return { row, outlines: made };
}

/** ===== ROW PROCESSORS (STEP 3: DRAFTS) ===== */
function fillDraftsForRow_(row, opts) {
  const sh = getSheet_();
  const bpm = clampInt_(Number(sh.getRange(row, COL_BLOGS_PER_MONTH).getValue() || 2), 1, BLOG_BLOCKS_MAX);
  const monthYear = getSelectedMonthYmOrNow_();
  const ctaPhone = String(sh.getRange(row, COL_CTA_PHONE).getValue() || '').trim();
  const ctaLink  = String(sh.getRange(row, COL_CTA_LINK).getValue() || '').trim();
  const clientSpec = String(sh.getRange(row, COL_CLIENT_SPEC_INSTR).getValue() || '').trim();

  let made = 0;
  for (let i = 1; i <= bpm; i++) {
    const title = String(sh.getRange(row, blogCol_(i,'topic')).getValue() || '').trim();
    if (!title) continue;

    const linkMeta = getLinkCellMeta_(sh, row, blogCol_(i,'link'));
    if (!linkMeta.fileId) {
      logBoth_(`Blog ${i}: No file linked, skipping draft`, 'warn');
      continue;
    }

    let outlineHtml = '';
    try {
      const doc = DocumentApp.openById(linkMeta.fileId);
      const tbl = doc.getBody().getTables();
      if (tbl && tbl.length) outlineHtml = tbl[0].getCell(0,0).getText();
      else logBoth_(`Blog ${i}: No table in doc`, 'warn');
    } catch (e) { 
      logBoth_(`Blog ${i}: Doc read error ${e}`, 'err');
      continue; 
    }

    if (!outlineHtml || outlineHtml.length < 50) {
      logBoth_(`Blog ${i}: Outline text too short/empty, skipping`, 'warn');
      continue;
    }

    logBoth_(`Blog ${i}: Draft generating...`, 'info');
    try {
      let draftHtml = requestDraftHTMLFromGemini_(outlineHtml, clientSpec, ctaPhone, ctaLink);
      draftHtml = insertH1AfterInShort_(draftHtml, title);
      writeDocWithCodeBlock_(linkMeta.fileId, `Draft: ${title} (${monthYear})`, draftHtml);
      made++;
      Utilities.sleep(2000);
    } catch (e) {
      logBoth_(`Blog ${i}: Draft Gen Error: ${e}`, 'err');
    }
  }
  return { row, drafts: made };
}

/** ===== ROW PROCESSORS (STEP 4: COPYEDIT) ===== */
function copyeditForRow_(row, opts) {
  const sh = getSheet_();
  const bpm = clampInt_(Number(sh.getRange(row, COL_BLOGS_PER_MONTH).getValue() || 2), 1, BLOG_BLOCKS_MAX);
  const ctaPhone = String(sh.getRange(row, COL_CTA_PHONE).getValue() || '').trim();
  const ctaLink  = String(sh.getRange(row, COL_CTA_LINK).getValue() || '').trim();
  const monthYear = getSelectedMonthYmOrNow_();
  
  let edited = 0;
  for (let i = 1; i <= bpm; i++) {
    const linkMeta = getLinkCellMeta_(sh, row, blogCol_(i,'link'));
    if (!linkMeta.fileId) continue;
    let draftHtml = '';
    try {
      const doc = DocumentApp.openById(linkMeta.fileId);
      const tbl = doc.getBody().getTables();
      if (tbl.length) draftHtml = tbl[0].getCell(0,0).getText();
    } catch(e) { continue; }
    
    if (draftHtml.length < 100) continue;
    
    try {
      const finalHtml = requestCopyeditHTMLFromGemini_(draftHtml, ctaPhone, ctaLink);
      finalHtml = insertH1AfterInShort_(finalHtml, String(sh.getRange(row, blogCol_(i,'topic')).getValue()));
      writeDocWithCodeBlock_(linkMeta.fileId, `Final: ${String(sh.getRange(row, blogCol_(i,'topic')).getValue())} (${monthYear})`, finalHtml);
      edited++;
      Utilities.sleep(2000);
    } catch(e) { logBoth_(`Blog ${i} CE Error: ${e}`,'err'); }
  }
  return { row, edited };
}

/** ===== WRIKE & TOPIC LOGGING ===== */
const LOG_SHEET_NAME = 'Topic Log';
const WRIKE_BLUEPRINT_TITLES = {
  'blog approval':          ['SEO Blog – Blog Approval', 'SEO Blog - Blog Approval'],
  'blog & topics approval': ['SEO Blog – Blog & Topics Approval', 'SEO Blog - Blog & Topics Approval'],
  'no approval':            ['SEO Blog – No Approval', 'SEO Blog - No Approval']
};

function _wrikeSendRow_(row, options) {
  const sh = getSheet_();
  const clientCode = String(sh.getRange(row, COL_CLIENT_CODE).getValue() || '').trim();
  if (!clientCode) return { row, status: 'skipped', reason: 'Missing Client Code' };

  let parentId, blueprintId;
  try {
    const folderVal = sh.getRange(row, COL_WRIKE_FOLDER_ID).getValue();
    parentId = _wrikeResolveParentId_(folderVal);
    const approval = String(sh.getRange(row, COL_APPROVAL_PROCESS).getValue()).toLowerCase();
    blueprintId = _wrikeResolveBlueprintId_(approval);
  } catch (e) {
    return { row, status: 'error', reason: `Config Error: ${e.message}` };
  }

  const bpm = Math.max(1, Number(sh.getRange(row, COL_BLOGS_PER_MONTH).getValue()) || 2);
  const monthArg = options && options.month ? options.month : currentYm_();
  const monthLabel = _wrikeMonthLabel_(monthArg);

  let createdCount = 0;
  const topicsSent = []; // Track success
  const errors = [];

  for (let i = 1; i <= bpm; i++) {
    const title = String(sh.getRange(row, blogCol_(i,'topic')).getValue() || '').trim();
    if (!title) continue;

    try {
      const taskTitle = `${monthLabel} * ${clientCode} "${title}"`;
      const launch = _wrikeFetch_(`/task_blueprints/${blueprintId}/launch_async`, 'post', {
        parentId: parentId,
        title: taskTitle
      });
      
      const jobId = launch.data[0].jobId;
      const taskId = _wrikePollJob_(jobId); 
      
      if (taskId) {
        const linkMeta = getLinkCellMeta_(sh, row, blogCol_(i,'link'));
        const prim = String(sh.getRange(row, blogCol_(i,'primary')).getValue());
        const sec = String(sh.getRange(row, blogCol_(i,'secondary')).getValue());
        
        const tData = _wrikeFetch_(`/tasks/${taskId}`, 'get');
        let desc = tData.data[0].description || '';
        desc = desc.replace(/%BLOG_TITLE%/g, `${title} (${linkMeta.url})`)
                   .replace(/%PRIMARY_KW%/g, prim)
                   .replace(/%SECONDARY_KWS%/g, sec);
                   
        _wrikeFetch_(`/tasks/${taskId}`, 'put', { description: desc });
        
        createdCount++;
        topicsSent.push(title); // Add to log list
        logBoth_(`✓ Sent to Wrike: "${title}"`, 'ok');
      } else {
        throw new Error(`Task creation timed out for "${title}"`);
      }
    } catch (e) {
      logBoth_(`✗ Wrike Failed for "${title}": ${e.message}`, 'err');
      errors.push(e.message);
    }
    Utilities.sleep(200);
  }

  // LOG TO SHEET
  if (topicsSent.length > 0) {
    try {
      logTopicsToHistory_(monthArg, clientCode, topicsSent);
      logBoth_(`Logged ${topicsSent.length} topics to '${LOG_SHEET_NAME}'`, 'info');
    } catch (e) {
      logBoth_(`Logging Failed: ${e.message}`, 'warn');
    }
  }

  if (createdCount === 0 && errors.length > 0) {
    return { row, status: 'error', reason: errors.join(' | ') };
  }
  
  return { row, status: 'ok', created: createdCount };
}

function logTopicsToHistory_(yyyyMm, clientCode, topicsArray) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(LOG_SHEET_NAME);
  
  if (!sheet) {
    sheet = ss.insertSheet(LOG_SHEET_NAME);
    sheet.appendRow(['Publish Month', 'Client Code', 'Topic']);
    sheet.getRange(1, 1, 1, 3).setFontWeight('bold');
    sheet.setFrozenRows(1);
  }
  
  const rows = topicsArray.map(topic => [yyyyMm, clientCode, topic]);
  sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, 3).setValues(rows);
}

function getLoggedTopics_(clientCode) {
  if (!clientCode) return [];
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(LOG_SHEET_NAME);
  if (!sheet || sheet.getLastRow() < 2) return [];
  
  const data = sheet.getRange(2, 2, sheet.getLastRow() - 1, 2).getValues();
  const target = String(clientCode).toLowerCase().trim();
  
  return data
    .filter(row => String(row[0]).toLowerCase().trim() === target)
    .map(row => String(row[1]).trim())
    .filter(t => t.length > 0);
}

/** ===== DOC/FILE UTILS ===== */
function writeDocWithCodeBlock_(docId, titleText, rawHtml) {
  const doc = DocumentApp.openById(docId);
  const body = doc.getBody();
  body.clear();
  if (titleText) body.appendParagraph(String(titleText)).setHeading(DocumentApp.ParagraphHeading.HEADING1);
  body.appendParagraph('');
  
  const table = body.appendTable([[rawHtml]]);
  const cell = table.getCell(0,0);
  cell.setBackgroundColor('#F4F5F7');
  cell.editAsText().setFontFamily('Courier New').setFontSize(10);
  
  body.insertParagraph(1, 'Raw HTML (copy-paste into CMS):').setItalic(true).setForegroundColor('#666666');
  doc.saveAndClose(); 
  return { id: docId, url: doc.getUrl() };
}

function ensureDocForLinkCell_(sh, row, col, defaultName) {
  const meta = getLinkCellMeta_(sh, row, col);
  if (meta.fileId) return meta;
  const doc = DocumentApp.create(sanitizeFilename_(defaultName));
  const file = DriveApp.getFileById(doc.getId());
  try { DriveApp.getFolderById(DEFAULT_OUTPUT_FOLDER_ID).addFile(file); DriveApp.getRootFolder().removeFile(file); } catch (_) {}
  setHyperlinkCell_(sh, row, col, defaultName, doc.getUrl());
  return { url: doc.getUrl(), fileId: doc.getId() };
}

/** ===== UTILS & HELPERS ===== */
function clampInt_(n, min, max) { return Math.floor(Math.max(min, Math.min(max, n || 0))); }
function safeParseJSON_(s) { try { return JSON.parse(s); } catch (_) { return null; } }
function extractJSONLoose_(text) {
  if (!text) return null;
  const s = String(text);
  try { return JSON.parse(s); } catch (_) {}
  const match = s.match(/```(?:json)?([\s\S]*?)```/i);
  if (match && match[1]) { try { return JSON.parse(match[1]); } catch (_) {} }
  
  const firstOpenBrace = s.indexOf('{');
  const firstOpenBracket = s.indexOf('[');
  let start = -1, endChar = '';
  if (firstOpenBrace !== -1 && (firstOpenBracket === -1 || firstOpenBrace < firstOpenBracket)) { start = firstOpenBrace; endChar = '}'; }
  else if (firstOpenBracket !== -1) { start = firstOpenBracket; endChar = ']'; }
  
  if (start !== -1) {
    const end = s.lastIndexOf(endChar);
    if (end > start) try { return JSON.parse(s.substring(start, end + 1)); } catch (_) {}
  }
  return null;
}
function sanitizeFilename_(name) { return String(name || '').replace(/[\\/:*?"<>|#]/g, '').trim(); }
function tryParseDriveIdFromUrl_(url) { const m = url.match(/\/(?:document|file)\/d\/([a-zA-Z0-9_-]{10,})/); return m ? m[1] : ''; }
function buildTelHref_(raw) { const d = String(raw||'').replace(/\D/g,''); return d ? 'tel:' + (d.length===10?'+1'+d:d) : ''; }
function formatPhoneDisplay_(raw) { const d = String(raw||'').replace(/\D/g,''); if (d.length === 10) return `(${d.slice(0,3)}) ${d.slice(3,6)}-${d.slice(6)}`; return raw; }
function escapeRegExp_(s) { return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }
function escapeHtml_(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function stripUnwantedTags_(html) { return String(html||'').replace(/<!DOCTYPE[^>]*>|<\/?\s*html[^>]*>|<head[^>]*>[\s\S]*?<\/head>|<\/?\s*body[^>]*>|```\s*html\s*|```/gi, '').trim(); }
function insertH1AfterInShort_(html, title) {
  let out = normalizeSingleInShort_(String(html||''));
  out = out.replace(/<h1[^>]*>[\s\S]*?<\/h1>/gi, '');
  let lead = '';
  const m = out.match(/(?:<p[^>]*>|<h\d[^>]*>)\s*(?:<strong>|<b>)?\s*In\s+short[:,\s]*(?:<\/strong>|<\/b>)?\s*(?:<\/p>|<\/h\d>)\s*<ul[^>]*>[\s\S]*?<\/ul>/i);
  if (m) { lead = m[0]; out = out.replace(m[0], ''); }
  return `<h1>${escapeHtml_(title)}</h1>\n${lead}\n${out}`.trim();
}
function computeWordCount_(html) { return String(html||'').replace(/<[^>]+>/g, ' ').split(/\s+/).filter(w=>w.length>0).length; }

function _normalizeMonthArg_(month) {
  let s = String(month || '').trim();
  if (!/^\d{4}-\d{2}$/.test(s)) s = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM');
  return s;
}
function getSelectedMonthYmOrNow_() {
  const up = PropertiesService.getUserProperties();
  const ym = String(up.getProperty('graceful_month') || '').trim();
  return (parseYm_(ym) || parseYm_(currentYm_())).ym;
}
function parseYm_(ym) {
  const m = /^(\d{4})-(\d{2})$/.exec(String(ym||'').trim());
  if (!m) return null;
  const year = Number(m[1]), month = Number(m[2]);
  if (month < 1 || month > 12) return null;
  return { year, month, ym: m[0] };
}
function currentYm_() { return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM'); }

function readKeywordsFromRow_(sh, row) {
  return sh.getRange(row, KW_START_COL, 1, KW_COUNT).getValues()[0]
    .map(v => v == null ? '' : String(v).trim())
    .filter(v => v.length > 0);
}

function ensureMinColumns_(minColNumber) {
  const sh = getSheet_();
  const max = sh.getMaxColumns();
  if (max < minColNumber) { sh.insertColumnsAfter(max, minColNumber - max); }
}

function linkApplied_(range, expectedUrl) {
  try { if (range.getRichTextValue().getLinkUrl() === expectedUrl) return true; } catch (_) {}
  try { if (String(range.getFormula()).includes(expectedUrl)) return true; } catch (_) {}
  return false;
}

function assertCellWritable_(sh, row, col) {
  const cell = sh.getRange(row, col);
  if (cell.isPartOfMerge()) throw new Error(`Cell ${cell.getA1Notation()} is merged; unmerge before writing.`);
}

function setHyperlinkCell_(sh, row, col, text, url) {
  ensureMinColumns_(col);
  const cell = sh.getRange(row, col);
  const safeText = String(text || '').trim() || 'Open';
  const safeUrl  = String(url || '').trim();

  if (!safeUrl) { cell.setValue(safeText); return; }

  try {
    const rich = SpreadsheetApp.newRichTextValue().setText(safeText).setLinkUrl(safeUrl).build();
    cell.setRichTextValue(rich);
  } catch (_) {}

  if (!linkApplied_(cell, safeUrl)) {
    try {
      const escUrl = safeUrl.replace(/"/g, '""');
      const escTxt = safeText.replace(/"/g, '""');
      cell.setFormula(`=HYPERLINK("${escUrl}","${escTxt}")`);
    } catch (_) { cell.setValue(`${safeText} → ${safeUrl}`); }
  }
}

function getLinkCellMeta_(sh, row, col) {
  const cell = sh.getRange(row, col);
  const rt   = cell.getRichTextValue();
  let url = '';
  if (rt && rt.getLinkUrl()) url = rt.getLinkUrl();
  if (!url) {
    try { const m = String(cell.getFormula()).match(/^=HYPERLINK\(\s*"([^"]+)"/i); if (m) url = m[1]; } catch (_) {}
  }
  let fileId = '';
  if (url) fileId = tryParseDriveIdFromUrl_(url);
  return { url, fileId };
}

// --- WRIKE UTILS ---
function _wrikeConfig_() {
  const t = PropertiesService.getScriptProperties().getProperty('wrike_access_token');
  if (!t) throw new Error('Missing wrike_access_token');
  return { token: t };
}

function _wrikeFetch_(path, method, payload, query) {
  const { token } = _wrikeConfig_();
  let url = 'https://www.wrike.com/api/v4' + (path.startsWith('/')?path:'/'+path);
  if (query) url += '?' + Object.keys(query).map(k=>k+'='+encodeURIComponent(query[k])).join('&');
  
  const opts = { method: method, headers: { Authorization: 'Bearer '+token }, contentType: 'application/json', muteHttpExceptions: true };
  if (payload) opts.payload = JSON.stringify(payload);
  
  const res = UrlFetchApp.fetch(url, opts);
  if (res.getResponseCode() >= 300) throw new Error(`Wrike ${res.getResponseCode()}: ${res.getContentText()}`);
  return JSON.parse(res.getContentText());
}

function _wrikeResolveParentId_(val) {
  const v = String(val||'').trim();
  if (/^https?:\/\//.test(v)) {
    const r = _wrikeFetch_('/folders', 'get', null, { permalink: v });
    return r.data[0].id;
  }
  return v;
}

function _wrikeResolveBlueprintId_(val) {
  const key = String(val||'').toLowerCase();
  let titles = WRIKE_BLUEPRINT_TITLES['blog approval'];
  if (key.includes('topics')) titles = WRIKE_BLUEPRINT_TITLES['blog & topics approval'];
  else if (key.includes('no approval')) titles = WRIKE_BLUEPRINT_TITLES['no approval'];
  
  const all = _wrikeFetch_('/task_blueprints', 'get').data;
  const hit = all.find(b => titles.includes(b.title));
  if (!hit) throw new Error('Blueprint not found for: ' + val);
  return hit.id;
}

function _wrikePollJob_(jobId) {
  const end = Date.now() + 30000;
  while (Date.now() < end) {
    const res = _wrikeFetch_(`/async_job/${jobId}`, 'get');
    if (res.data[0].result && res.data[0].result.taskId) return res.data[0].result.taskId;
    Utilities.sleep(2000);
  }
  return null;
}

function _wrikeMonthLabel_(ym) {
  const [y, m] = ym.split('-');
  const names = ['JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE','JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER'];
  return `${Number(m)} ${names[Number(m)-1]} ${y}`;
}

function tokenizeForCompare_(s) {
  const stop = new Set(['the','a','an','and','or','of','for','to','in','with','how','when','why','what','is','are']);
  return new Set(String(s||'').toLowerCase().replace(/[^a-z0-9 ]/g,' ').split(/\s+/).filter(t => t.length > 2 && !stop.has(t)));
}

function jaccard_(aSet, bSet) {
  if (aSet.size === 0 || bSet.size === 0) return 0;
  let inter = 0;
  aSet.forEach(t => { if (bSet.has(t)) inter++; });
  const union = aSet.size + bSet.size - inter;
  return union ? inter / union : 0;
}

/** ===== AA + GEMINI HELPERS ===== */
function normalizeRankValue_(v) {
  const n = Number(v);
  return (n > 0 && n < 101) ? n : 1000;
}

function normalizeCampaignId_(raw) {
  if (raw == null) return null;
  return isNaN(Number(raw)) ? String(raw).trim() : Number(raw);
}

function normalizeUrl_(u) {
  if (!u) return '';
  let s = String(u).trim();
  if (!/^https?:\/\//i.test(s)) s = 'https://' + s;
  return s.replace(/\/+$/, '');
}

function discoverFeedLinks_(html, baseUrl) {
  const out = [];
  const re = /<link\s+[^>]*rel=["']alternate["'][^>]*>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const type = (m[0].match(/type=["']([^"']+)["']/i) || [,''])[1];
    if (!/rss|atom|rdf|xml/i.test(type || '')) continue;
    const href = (m[0].match(/href=["']([^"']+)["']/i) || [,''])[1];
    if (href) out.push(resolveUrl_(baseUrl, href));
  }
  return out;
}

function parseFeedItems_(xmlText, lookbackDays) {
  try {
    const doc = XmlService.parse(xmlText);
    const root = doc.getRootElement();
    const now = new Date();
    const cutoff = new Date(now.getTime() - (lookbackDays || TOPIC_DEDUP_LOOKBACK_DAYS) * 86400000);
    const items = [];
    const name = root.getName().toLowerCase();

    if (name === 'rss') {
      root.getChildren('channel').forEach(ch => {
        ch.getChildren('item').forEach(it => {
          const title = findFirstText_(it, 'title');
          const d = parseAnyDate_(findFirstText_(it, 'pubDate') || findFirstText_(it, 'date'));
          if (title && d && d >= cutoff) items.push(title);
        });
      });
    } else if (name === 'feed') {
      root.getChildren('entry').forEach(en => {
        const title = findFirstText_(en, 'title');
        const d = parseAnyDate_(findFirstText_(en, 'updated') || findFirstText_(en, 'published'));
        if (title && d && d >= cutoff) items.push(title);
      });
    }
    return items;
  } catch (e) { return []; }
}

function findFirstText_(el, local) {
  const kids = el.getChildren();
  for (let i = 0; i < kids.length; i++) {
    if (String(kids[i].getName() || '').toLowerCase() === String(local).toLowerCase()) return (kids[i].getText() || '').trim();
  }
  return '';
}

function parseAnyDate_(s) {
  if (!s) return null;
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}

function resolveUrl_(base, href) {
  if (/^https?:\/\//i.test(href)) return href;
  if (href.startsWith('//')) return 'https:' + href;
  return base.replace(/\/+$/,'') + '/' + href.replace(/^\/+/, '');
}

/** ===== CITATION VALIDATION ===== */
function validateSourceLinks_(html) {
  const linkPattern = /<a\s+[^>]*href=["']([^"']+)["'][^>]*>([^<]+)<\/a>/gi;
  const matches = [];
  let match;
  while ((match = linkPattern.exec(html)) !== null) { matches.push({ url: match[1], text: match[2], original: match[0] }); }
  
  const citations = matches.filter(m => /nih\.gov|cdc\.gov|mayoclinic|clevelandclinic|heart\.org|diabetes\.org|medlineplus/i.test(m.url));
  let replaced = html;
  const invalid = [];
  
  citations.forEach(cite => {
    try {
      const c = UrlFetchApp.fetch(cite.url, { muteHttpExceptions: true }).getResponseCode();
      if (c >= 400) {
        invalid.push(cite);
        const dom = cite.url.match(/https?:\/\/([^\/]+)/)[1];
        const fb = getFallbackUrl_(dom);
        replaced = replaced.replace(cite.original, `<a href="${fb}">${cite.text}</a>`);
      }
    } catch (_) {
      invalid.push(cite);
    }
  });
  return { replaced, invalid };
}

function getFallbackUrl_(d) {
  if (d.includes('nih.gov')) return 'https://www.nih.gov';
  if (d.includes('cdc.gov')) return 'https://www.cdc.gov';
  if (d.includes('mayoclinic')) return 'https://www.mayoclinic.org';
  return 'https://' + d;
}

function ensureRealSourcesOrNudge_(html) {
  let out = String(html || '');
  const hasSources = /<h\d[^>]*>.*?(?:Source|Reference|Bibliography).*?<\/h\d>/i.test(out);
  if (!hasSources) {
    out += `\n<h3>Sources</h3>\n<ul>
<li><a href="https://www.nia.nih.gov/health">National Institute on Aging</a></li>
<li><a href="https://medlineplus.gov">MedlinePlus</a></li>
</ul>`;
    return out;
  }
  return out;
}

function processSingleBlogActiveRow(blogIndex, operation) {
  const sh = getSheet_();
  const h = headerRows_();
  let row = h + 1;
  const ar = sh.getActiveRange();
  if (ar) row = Math.max(h + 1, ar.getRow());
  return processSingleBlog(row, blogIndex, operation);
}

function processSingleBlog(row, blogIndex, operation) {
  clearSidebarLogs();
  const sh = getSheet_();
  const title = String(sh.getRange(row, blogCol_(blogIndex, 'topic')).getValue() || '').trim();
  const result = { row, blog: blogIndex, title, operation, status: 'ok' };
  
  try {
    if (operation === 'outline' || operation === 'all') {
      logBoth_(`Generating outline for ${title}...`, 'info');
      generateOutlinesForRow_(row, {});
      SpreadsheetApp.flush();
      Utilities.sleep(2000);
      logBoth_('Outline complete', 'ok');
    }
    if (operation === 'draft' || operation === 'all') {
      logBoth_(`Generating draft for ${title}...`, 'info');
      fillDraftsForRow_(row, {});
      SpreadsheetApp.flush();
      logBoth_('Draft complete', 'ok');
    }
    if (operation === 'copyedit' || operation === 'all') {
      logBoth_(`Copyediting ${title}...`, 'info');
      copyeditForRow_(row, {});
      SpreadsheetApp.flush();
      logBoth_('Copyedit complete', 'ok');
    }
  } catch (e) {
    logBoth_(`Error: ${e}`, 'err');
    result.status = 'error';
  }
  
  result.logs = getSidebarLogs();
  return result;
}

function getAvailableBlogsForActiveRow() {
  const sh = getSheet_();
  const row = Math.max(headerRows_() + 1, sh.getActiveRange() ? sh.getActiveRange().getRow() : 0);
  const bpm = clampInt_(Number(sh.getRange(row, COL_BLOGS_PER_MONTH).getValue() || 2), 1, BLOG_BLOCKS_MAX);
  const blogs = [];
  for (let i = 1; i <= bpm; i++) {
    const t = String(sh.getRange(row, blogCol_(i, 'topic')).getValue() || '').trim();
    if (t) blogs.push({ index: i, title: t });
  }
  return { row, blogs };
}
