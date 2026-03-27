const appRoot = document.getElementById('app');

const TABS = [
  { id: 'structure', label: 'Structure' },
  { id: 'master-items', label: 'Master Items' },
  { id: 'usage', label: 'Usage' },
  { id: 'field-usage', label: 'Field Usage' },
  { id: 'design', label: 'Design' },
  { id: 'tables', label: 'Tables' },
  { id: 'script', label: 'Script' },
  { id: 'assets', label: 'Assets' },
];
const TAB_IDS = new Set(TABS.map((tab) => tab.id));

const VIEWS = [
  { id: 'apps', label: 'Apps' },
];

const state = {
  jobsById: {},
  jobOrder: [],
  activeJobId: null,
  activeView: 'apps',
  activeTab: 'overview',
  busy: false,
  importsBusy: false,
  error: '',
  statusText: 'Ready to analyze',
  serverJobs: [],
  ui: {
    showUploadTray: false,
    includeTables: false,
    topbarAppMenuOpen: false,
    filters: {
      tableStatus: 'all',
    },
    selectedTableByJob: {},
    selectedScriptTabByJob: {},
  },
};

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderMenuIcon(iconId) {
  const icons = {
    apps: 'fa-table-cells-large',
    overview: 'fa-house',
    structure: 'fa-diagram-project',
    'master-items': 'fa-layer-group',
    usage: 'fa-chart-column',
    'field-usage': 'fa-table',
    design: 'fa-pen-ruler',
    tables: 'fa-database',
    script: 'fa-code',
    assets: 'fa-image',
    default: 'fa-circle',
  };

  const iconClass = icons[iconId] || icons.default;
  return `<span class="menu-icon" aria-hidden="true"><i class="fa-solid ${iconClass}"></i></span>`;
}

function formatNumber(value) {
  if (value === null || value === undefined || value === '') return '—';
  return new Intl.NumberFormat('en-US').format(Number(value));
}

function formatPercent(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return `${Math.round(Number(value) * 100)}%`;
}

function formatDate(value) {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return escapeHtml(value);
  return new Intl.DateTimeFormat('en-GB', {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
}

function formatBytes(value) {
  if (!value && value !== 0) return '—';
  const units = ['B', 'KB', 'MB', 'GB'];
  let size = Number(value);
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(size >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}

function truncate(value, max = 180) {
  const text = String(value ?? '');
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function sortNaturalStrings(values) {
  return [...(Array.isArray(values) ? values : [])]
    .map((value) => String(value ?? '').trim())
    .filter(Boolean)
    .sort((left, right) =>
      left.localeCompare(right, undefined, {
        numeric: true,
        sensitivity: 'base',
      })
    );
}

function renderDownloadLink(url, label = 'Download ZIP', className = 'ghost-button') {
  if (!url) {
    return `<span class="${escapeHtml(`${className} ghost-button--disabled`)}">${escapeHtml(label)}</span>`;
  }
  return `<a class="${escapeHtml(className)}" href="${escapeHtml(url)}">${escapeHtml(label)}</a>`;
}

function getTopbarJobOptions() {
  const seen = new Set();
  const options = [];

  for (const serverJob of state.serverJobs || []) {
    if (!serverJob?.jobId || seen.has(serverJob.jobId)) continue;
    seen.add(serverJob.jobId);
    options.push({
      jobId: serverJob.jobId,
      label: serverJob.appLabel || serverJob.sourceFileName || serverJob.jobId,
    });
  }

  for (const jobId of state.jobOrder || []) {
    if (!jobId || seen.has(jobId)) continue;
    const envelope = state.jobsById?.[jobId];
    seen.add(jobId);
    options.push({
      jobId,
      label: envelope?.appLabel || envelope?.analysis?.meta?.sourceFileName || jobId,
    });
  }

  return options;
}

function renderTopbar(showAppsView, job) {
  const options = getTopbarJobOptions();
  const activeJobId = state.activeJobId || '';
  const activeOption = options.find((option) => option.jobId === activeJobId) || options[0] || null;

  return `
    <header class="portal-topbar">
      <div class="portal-topbar__brand">
        <div class="portal-brand">
          <span class="portal-brand__mark" aria-hidden="true"><i class="fa-solid fa-database"></i></span>
          <div class="portal-brand__copy">
            <strong>QVDMetadataAnalyzer</strong>
          </div>
        </div>
        <div class="portal-topbar__context">
          ${
            options.length
              ? `
                <div class="topbar-app-switch ${state.ui.topbarAppMenuOpen ? 'is-open' : ''}">
                  <button type="button" class="topbar-app-switch__trigger" data-action="toggle-app-switch" aria-haspopup="listbox" aria-expanded="${state.ui.topbarAppMenuOpen ? 'true' : 'false'}">
                    <span>${escapeHtml(activeOption?.label || 'Select app')}</span>
                    <i class="fa-solid fa-chevron-down" aria-hidden="true"></i>
                  </button>
                  ${
                    state.ui.topbarAppMenuOpen
                      ? `
                        <div class="topbar-app-switch__menu" role="listbox" aria-label="Imported apps">
                          ${options
                            .map(
                              (option) => `
                                <button
                                  type="button"
                                  class="topbar-app-switch__option ${option.jobId === activeJobId ? 'is-active' : ''}"
                                  data-select-job="${escapeHtml(option.jobId)}"
                                  role="option"
                                  aria-selected="${option.jobId === activeJobId ? 'true' : 'false'}"
                                >
                                  <span class="topbar-app-switch__option-icon" aria-hidden="true"><i class="fa-solid fa-cubes"></i></span>
                                  <span class="topbar-app-switch__option-text">${escapeHtml(option.label)}</span>
                                </button>
                              `
                            )
                            .join('')}
                        </div>
                      `
                      : ''
                  }
                </div>
              `
              : `<span>No active app</span>`
          }
        </div>
      </div>
    </header>
  `;
}

function getActiveJob() {
  return state.activeJobId ? state.jobsById[state.activeJobId] : null;
}

function getActiveAnalysis() {
  return getActiveJob()?.analysis || null;
}

function getTabBadge(tabId, analysis) {
  if (!analysis) return '';
  const masterItems = getMasterItemsData(analysis);
  switch (tabId) {
    case 'structure':
      return analysis.overview?.counts?.sheets || 0;
    case 'master-items':
      return masterItems.counts.total || 0;
    case 'usage':
      return getMasterItemUsageData(analysis).counts?.unusedTotal || 0;
    case 'field-usage':
      return analysis.fieldUsage?.counts?.unusedFields || 0;
    case 'design':
      return analysis.flags?.length || 0;
    case 'tables':
      return analysis.tables?.summary?.tableCount || analysis.tables?.tables?.length || 0;
    case 'script':
      return analysis.script?.tabs?.length || 1;
    case 'assets':
      return analysis.assets?.count || 0;
    default:
      return '';
  }
}

function getMasterItemsData(analysis) {
  const topLevel = analysis?.masterItems || {};
  const fallback = analysis?.structure?.masterItems || {};
  const measures = Array.isArray(topLevel.measures) ? topLevel.measures : Array.isArray(fallback.measures) ? fallback.measures : [];
  const dimensions = Array.isArray(topLevel.dimensions)
    ? topLevel.dimensions
    : Array.isArray(fallback.dimensions)
      ? fallback.dimensions
      : [];
  const objects = Array.isArray(topLevel.objects) ? topLevel.objects : Array.isArray(fallback.objects) ? fallback.objects : [];

  const counts = topLevel.counts || {
    measures: measures.length,
    dimensions: dimensions.length,
    objects: objects.length,
    total: measures.length + dimensions.length + objects.length,
  };

  return {
    counts,
    measures,
    dimensions,
    objects,
  };
}

function getFieldUsageData(analysis) {
  const data = analysis?.fieldUsage || {};
  const rows = Array.isArray(data.rows) ? data.rows : [];
  const unusedFields = rows.filter((row) => row.unused).map((row) => row.name);
  return {
    counts: data.counts || {
      fields: rows.length,
      usedFields: rows.length - unusedFields.length,
      unusedFields: unusedFields.length,
    },
    rows,
    dropFieldSuggestion: data.dropFieldSuggestion || {
      fields: unusedFields,
      statement: unusedFields.length
        ? `DROP FIELD\n  ${unusedFields.map((fieldName) => `[${String(fieldName).replace(/\]/g, ']]')}]`).join(',\n  ')};`
        : '',
      note: unusedFields.length
        ? 'Review these fields before applying the statement in the load script.'
        : 'No fully unused fields were detected by this front-end usage scan.',
    },
  };
}

function getMasterItemUsageData(analysis) {
  const masterItems = getMasterItemsData(analysis);
  const providedUsage = analysis?.masterItemUsage;

  if (providedUsage && Array.isArray(providedUsage.dimensions) && Array.isArray(providedUsage.measures) && Array.isArray(providedUsage.objects)) {
    return providedUsage;
  }

  const dimensions = masterItems.dimensions.map((item) => ({
    id: item.id,
    title: item.title || 'Untitled dimension',
    usedObjects: [],
    usedObjectTitles: [],
    usedInObjects: 0,
    unused: true,
  }));
  const measures = masterItems.measures.map((item) => ({
    id: item.id,
    title: item.title || item.label || 'Untitled expression',
    expression: item.expression || '',
    usedObjects: [],
    usedObjectTitles: [],
    usedInObjects: 0,
    unused: true,
  }));
  const objects = masterItems.objects.map((item) => ({
    id: item.id,
    title: item.title || 'Untitled master object',
    visualization: item.visualization || 'unknown',
    usedInSheets: 0,
    sheetTitles: [],
    unused: true,
  }));

  return {
    version: 0,
    counts: {
      dimensions: dimensions.length,
      measures: measures.length,
      objects: objects.length,
      unusedDimensions: dimensions.length,
      unusedMeasures: measures.length,
      unusedObjects: objects.length,
      unusedTotal: dimensions.length + measures.length + objects.length,
    },
    dimensions,
    measures,
    objects,
  };
}

function renderUsageDetailsPopover({ ariaLabel, title, items, emptyMessage }) {
  const sortedItems = sortNaturalStrings(items);
  return `
    <span class="chart-usage-details">
      <button type="button" class="chart-usage-details__trigger" aria-label="${escapeHtml(ariaLabel)}">
        <i class="fa-solid fa-circle-info" aria-hidden="true"></i>
      </button>
      <div class="chart-usage-details__popover" role="tooltip">
        <p class="chart-usage-details__title">${escapeHtml(title)}</p>
        ${
          sortedItems.length
            ? `
              <ul class="chart-usage-details__list">
                ${sortedItems.map((item) => `<li>${escapeHtml(item)}</li>`).join('')}
              </ul>
            `
            : `<p class="chart-usage-details__empty">${escapeHtml(emptyMessage)}</p>`
        }
      </div>
    </span>
  `;
}

function toUsageObjectDisplayNames(item) {
  if (Array.isArray(item?.usedObjects) && item.usedObjects.length) {
    return item.usedObjects.map((entry) =>
      entry && entry.isMasterItem ? `${entry.name} (Master Item)` : entry?.name || ''
    );
  }
  return Array.isArray(item?.usedObjectTitles) ? item.usedObjectTitles : [];
}

function normalizeView(view) {
  return view === 'apps' || view === 'imports' ? 'apps' : 'analysis';
}

function normalizeTab(tab) {
  return TAB_IDS.has(tab) ? tab : 'overview';
}

function toShortJobToken(jobId) {
  return String(jobId || '').slice(0, 8);
}

function collectKnownJobIds() {
  const ids = new Set();
  for (const id of Object.keys(state.jobsById || {})) {
    if (id) ids.add(id);
  }
  for (const job of state.serverJobs || []) {
    if (job?.jobId) ids.add(job.jobId);
  }
  return [...ids];
}

function resolveJobTokenToId(token) {
  if (!token) return null;
  const normalizedToken = String(token).toLowerCase();
  const knownIds = collectKnownJobIds();
  const exact = knownIds.find((id) => id.toLowerCase() === normalizedToken);
  if (exact) return exact;
  const matches = knownIds.filter((id) => id.toLowerCase().startsWith(normalizedToken));
  if (matches.length === 1) return matches[0];
  return null;
}

function isUuidLike(value) {
  return /^[a-f0-9-]{36}$/i.test(String(value || ''));
}

function readUrlState() {
  const hash = String(window.location.hash || '').replace(/^#\/?/, '');
  const hashParts = hash
    .split('/')
    .map((part) => part.trim())
    .filter(Boolean);

  if (hashParts[0] === 'apps' || hashParts[0] === 'imports') {
    return {
      jobToken: null,
      view: 'apps',
      tab: normalizeTab(hashParts[1]),
    };
  }

  if (hashParts[0] === 'analysis') {
    return {
      jobToken: hashParts[1] || null,
      view: 'analysis',
      tab: normalizeTab(hashParts[2]),
    };
  }

  const params = new URLSearchParams(window.location.search);
  return {
    jobToken: params.get('job'),
    view: normalizeView(params.get('view')),
    tab: normalizeTab(params.get('tab')),
  };
}

function syncUrl() {
  const url = new URL(window.location.href);
  const view = normalizeView(state.activeView);
  const tab = normalizeTab(state.activeTab);
  url.search = '';
  if (view === 'apps' || !state.activeJobId) {
    url.hash = '#/apps';
  } else {
    url.hash = `#/analysis/${encodeURIComponent(toShortJobToken(state.activeJobId))}/${encodeURIComponent(tab)}`;
  }
  window.history.replaceState({}, '', url);
}

function setStatus(text) {
  state.statusText = text;
  renderApp();
}

function setError(message) {
  state.error = message || '';
  renderApp();
}

function addJob(jobEnvelope, { preserveView = false, preserveTab = false } = {}) {
  if (!jobEnvelope?.jobId || !jobEnvelope?.analysis) {
    throw new Error('The analysis response was incomplete.');
  }
  state.jobsById[jobEnvelope.jobId] = jobEnvelope;
  state.jobOrder = [jobEnvelope.jobId, ...state.jobOrder.filter((jobId) => jobId !== jobEnvelope.jobId)];
  state.activeJobId = jobEnvelope.jobId;
  if (!preserveView) {
    state.activeView = 'analysis';
  }
  if (!preserveTab) {
    state.activeTab = 'overview';
  }
  state.activeView = normalizeView(state.activeView);
  state.activeTab = normalizeTab(state.activeTab);
  const firstTable = jobEnvelope.analysis?.tables?.tables?.[0];
  if (firstTable) {
    state.ui.selectedTableByJob[jobEnvelope.jobId] = firstTable.id;
  }
  if (state.ui.selectedScriptTabByJob[jobEnvelope.jobId] === undefined) {
    state.ui.selectedScriptTabByJob[jobEnvelope.jobId] = 0;
  }
  syncUrl();
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || 'Request failed.');
  }
  return payload;
}

async function loadJobFromUrl(jobId, options = {}) {
  if (!jobId || state.jobsById[jobId]) {
    syncUrl();
    return;
  }

  state.busy = true;
  setStatus('Loading saved analysis');
  renderApp();

  try {
    const envelope = await fetchJson(`/api/jobs/${encodeURIComponent(jobId)}/analysis`);
    addJob(envelope, options);
    setStatus('Analysis loaded');
    setError('');
  } catch (error) {
    setError(error.message);
    state.activeJobId = null;
    syncUrl();
    setStatus('Ready to analyze');
  } finally {
    state.busy = false;
    renderApp();
  }
}

async function loadImports() {
  state.importsBusy = true;
  renderApp();

  try {
    const payload = await fetchJson('/api/jobs');
    state.serverJobs = payload.jobs || [];
  } catch (error) {
    setError(error.message);
  } finally {
    state.importsBusy = false;
    renderApp();
  }
}

async function openJob(jobId) {
  state.activeView = 'analysis';
  if (state.jobsById[jobId]) {
    state.activeJobId = jobId;
    state.activeTab = normalizeTab(state.activeTab);
    syncUrl();
    renderApp();
    return;
  }

  await loadJobFromUrl(jobId);
  state.activeView = 'analysis';
  syncUrl();
  renderApp();
}

async function deleteJob(jobId) {
  state.busy = true;
  setStatus('Removing app');
  renderApp();

  try {
    await fetchJson(`/api/jobs/${encodeURIComponent(jobId)}`, { method: 'DELETE' });
    delete state.jobsById[jobId];
    state.jobOrder = state.jobOrder.filter((id) => id !== jobId);
    state.serverJobs = state.serverJobs.filter((job) => job.jobId !== jobId);

    if (state.activeJobId === jobId) {
      state.activeJobId = state.jobOrder[0] || null;
      state.activeView = state.activeJobId ? 'analysis' : 'apps';
      syncUrl();
    }

    setStatus('App removed');
    setError('');
  } catch (error) {
    setError(error.message);
    setStatus('Remove failed');
  } finally {
    state.busy = false;
    renderApp();
  }
}

async function handleUpload(form) {
  const fileInput = form.querySelector('input[type="file"]');
  const includeTablesInput = form.querySelector('input[name="includeTables"]');
  const [file] = fileInput.files;

  if (!file) {
    setError('Select a .qvf file first.');
    return;
  }

  if (!file.name.toLowerCase().endsWith('.qvf')) {
    setError('Only .qvf files are supported.');
    return;
  }

  state.busy = true;
  state.error = '';
  state.ui.includeTables = Boolean(includeTablesInput?.checked);
  setStatus('Extracting metadata and building the workspace');
  renderApp();

  try {
    const formData = new FormData();
    formData.append('qvf', file);
    formData.append('includeTables', state.ui.includeTables ? 'true' : 'false');

    const envelope = await fetchJson('/api/analyze', {
      method: 'POST',
      body: formData,
    });

    addJob(envelope);
    await loadImports();
    state.ui.showUploadTray = false;
    if (fileInput) {
      fileInput.value = '';
    }
    setStatus('Analysis ready');
  } catch (error) {
    setError(error.message);
    setStatus('Processing failed');
  } finally {
    state.busy = false;
    renderApp();
  }
}

function renderUploadForm({ compact = false } = {}) {
  return `
    <form class="upload-card ${compact ? 'compact' : ''}" data-role="upload-form" novalidate>
      <div class="upload-card__intro">
        <p class="eyebrow">QVF Analysis</p>
        <h2>${compact ? 'Upload another QVF' : 'Start with a QVF and inspect what is inside.'}</h2>
        <p class="lede">
          ${compact
            ? 'Add another app and keep its discoveries available in the workspace.'
            : 'This workspace is built for discovery first: inspect structure, expressions, script, confidence, and assets before you ever download a ZIP.'}
        </p>
      </div>
      <label class="upload-field">
        <span>QVF file</span>
        <input name="qvf" type="file" accept=".qvf" required />
      </label>
      <label class="toggle">
        <input name="includeTables" type="checkbox" value="true" ${state.ui.includeTables ? 'checked' : ''} />
        <span>Include table reconstruction in the analysis and ZIP</span>
      </label>
      <div class="upload-card__actions">
        <button type="submit" ${state.busy ? 'disabled' : ''}>Analyze QVF</button>
        <div class="status-pill ${state.busy ? 'busy' : 'idle'}">${escapeHtml(state.statusText)}</div>
      </div>
      ${state.error ? `<p class="error-message">${escapeHtml(state.error)}</p>` : ''}
    </form>
  `;
}

function renderLandingState() {
  return `
    <main class="shell shell--landing">
      ${renderTopbar(true, null)}
      <section class="landing-hero">
        <div class="landing-copy panel">
          <p class="eyebrow">Discovery workspace</p>
          <h1>Strip a QVF open and follow the discoveries.</h1>
          <p class="lede">
            Upload a Qlik app, inspect its structure, design signals, script sources, table confidence, and embedded assets,
            then download the ZIP only when you need the raw package.
          </p>
          <div class="preview-grid">
            <article class="preview-card">
              <p class="preview-card__title">Overview</p>
              <p>Landing summary, risks, biggest tables, and confidence snapshot.</p>
            </article>
            <article class="preview-card">
              <p class="preview-card__title">Design</p>
              <p>Expression complexity, duplicate logic, and possible unused fields.</p>
            </article>
            <article class="preview-card">
              <p class="preview-card__title">Script</p>
              <p>Browse script tabs directly from <code>///$tab</code> markers and inspect each tab as a focused block.</p>
            </article>
            <article class="preview-card">
              <p class="preview-card__title">Tables</p>
              <p>Exact, partial, and heuristic export confidence per table and per column.</p>
            </article>
          </div>
        </div>
        ${renderUploadForm()}
      </section>
    </main>
  `;
}

function renderKpiCards(analysis) {
  const entries = [
    ['Sheets', analysis.overview.counts.sheets],
    ['Measures', analysis.overview.counts.measures],
    ['Dimensions', analysis.overview.counts.dimensions],
    ['Variables', analysis.overview.counts.variables],
    ['Assets', analysis.overview.counts.assets],
    ['Sources', analysis.overview.counts.dataSources],
  ];

  return entries
    .map(
      ([label, value]) => `
        <article class="metric-card">
          <span class="metric-card__label">${escapeHtml(label)}</span>
          <strong class="metric-card__value">${formatNumber(value)}</strong>
        </article>
      `
    )
    .join('');
}

function renderFlagList(flags, limit = flags.length) {
  if (!flags.length) {
    return `<div class="empty-card">No flags were generated for this analysis.</div>`;
  }

  return `
    <div class="stack-list">
      ${flags
        .slice(0, limit)
        .map(
          (flag) => `
            <article class="flag-card flag-card--${escapeHtml(flag.severity)}">
              <div class="flag-card__header">
                <span class="mini-pill">${escapeHtml(toSentence(flag.category))}</span>
                <span class="mini-pill mini-pill--ghost">${escapeHtml(flag.severity)}</span>
              </div>
              <h3>${escapeHtml(flag.title)}</h3>
              <p>${escapeHtml(flag.description)}</p>
            </article>
          `
        )
        .join('')}
    </div>
  `;
}

function toSentence(value) {
  return String(value || '')
    .replace(/[-_]+/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function renderOverviewTab(analysis) {
  return `
    <section class="tab-grid tab-grid--script">
      <article class="panel hero-card hero-card--wide">
        <div>
          <p class="eyebrow">Active analysis</p>
          <h2>${escapeHtml(analysis.app.title || analysis.meta.appTitle)}</h2>
          <p class="lede">${escapeHtml(analysis.app.description || 'No app description was found in the extracted metadata.')}</p>
        </div>
        <div class="hero-meta">
          <div><span>Source file</span><strong>${escapeHtml(analysis.meta.sourceFileName)}</strong></div>
          <div><span>Reload</span><strong>${formatDate(analysis.app.lastReload)}</strong></div>
          <div><span>Version</span><strong>${escapeHtml(analysis.app.version || 'Unknown')}</strong></div>
          <div><span>Theme</span><strong>${escapeHtml(analysis.app.theme || 'Default')}</strong></div>
        </div>
      </article>
      <section class="metrics-grid">${renderKpiCards(analysis)}</section>
      <article class="panel section-card">
        <div class="section-card__head">
          <h3>Top discoveries</h3>
        </div>
        <div class="chips-row">
          ${analysis.overview.discoveries.cards
            .map(
              (item) => `
                <div class="insight-chip insight-chip--${escapeHtml(item.tone)}">
                  <span>${escapeHtml(item.label)}</span>
                  <strong>${escapeHtml(String(item.value))}</strong>
                </div>
              `
            )
            .join('')}
        </div>
        <ul class="bullet-list">
          ${analysis.overview.discoveries.highlights.map((item) => `<li>${escapeHtml(item)}</li>`).join('')}
        </ul>
      </article>
      <article class="panel section-card">
        <div class="section-card__head">
          <h3>Largest tables</h3>
        </div>
        <div class="stack-list">
          ${analysis.overview.largestTables
            .map(
              (table) => `
                <div class="table-row">
                  <div>
                    <strong>${escapeHtml(table.name)}</strong>
                    <p>${escapeHtml(table.status)} export</p>
                  </div>
                  <div class="table-row__meta">
                    <span>${formatNumber(table.rows)} rows</span>
                    <span>${formatNumber(table.columns)} cols</span>
                  </div>
                </div>
              `
            )
            .join('')}
        </div>
      </article>
      <article class="panel section-card section-card--wide section-card--script">
        <div class="section-card__head">
          <h3>Top flags</h3>
        </div>
        ${renderFlagList(analysis.overview.topFlags, 6)}
      </article>
    </section>
  `;
}

function renderBarList(items, formatter = (item) => `${item.count}`) {
  const max = Math.max(...items.map((item) => item.count), 1);
  return `
    <div class="bar-list">
      ${items
        .map(
          (item) => `
            <div class="bar-list__row">
              <div class="bar-list__label">${escapeHtml(item.type || item.name)}</div>
              <div class="bar-list__track"><span style="width:${(item.count / max) * 100}%"></span></div>
              <div class="bar-list__value">${escapeHtml(formatter(item))}</div>
            </div>
          `
        )
        .join('')}
    </div>
  `;
}

function renderStructureTab(analysis) {
  return `
    <section class="tab-grid">
      <article class="panel section-card">
        <div class="section-card__head">
          <h3>Visualization mix</h3>
        </div>
        ${renderBarList(analysis.structure.visualizations.slice(0, 10))}
      </article>
      <article class="panel section-card">
        <div class="section-card__head">
          <h3>Object inventory</h3>
        </div>
        ${renderBarList(analysis.structure.objects.slice(0, 10))}
      </article>
      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Sheets</h3>
        </div>
        <div class="sheet-grid">
          ${analysis.structure.sheets
            .map(
              (sheet) => `
                <article class="sheet-card">
                  <div class="sheet-card__head">
                    <h4>${escapeHtml(sheet.title)}</h4>
                    <span>${formatNumber(sheet.objectCount)} objects</span>
                  </div>
                  <div class="sheet-card__meta">
                    <span>${formatNumber(sheet.layout.columns)} cols</span>
                    <span>${formatNumber(sheet.layout.rows)} rows</span>
                    <span>${escapeHtml(sheet.layout.mobileLayout || 'Default mobile')}</span>
                  </div>
                  <div class="chips-row">
                    ${sheet.visualizationMix
                      .slice(0, 5)
                      .map((item) => `<span class="mini-pill">${escapeHtml(item.type)} · ${formatNumber(item.count)}</span>`)
                      .join('')}
                  </div>
                </article>
              `
            )
            .join('')}
        </div>
      </article>
      <article class="panel section-card">
        <div class="section-card__head">
          <h3>Shared fields</h3>
        </div>
        <div class="stack-list compact">
          ${analysis.structure.sharedFields.length
            ? analysis.structure.sharedFields
                .map(
                  (field) => `
                    <div class="key-value">
                      <strong>${escapeHtml(field.name)}</strong>
                      <span>${escapeHtml(field.sourceTables.join(' · '))}</span>
                    </div>
                  `
                )
                .join('')
            : `<div class="empty-card">No shared fields were detected.</div>`}
        </div>
      </article>
      <article class="panel section-card">
        <div class="section-card__head">
          <h3>High-cardinality fields</h3>
        </div>
        <div class="stack-list compact">
          ${analysis.structure.majorFields
            .map(
              (field) => `
                <div class="key-value">
                  <strong>${escapeHtml(field.name)}</strong>
                  <span>${formatNumber(field.cardinality)} cardinality</span>
                </div>
              `
            )
            .join('')}
        </div>
      </article>
    </section>
  `;
}

function renderMasterItemsTab(analysis) {
  const masterItems = getMasterItemsData(analysis);

  return `
    <section class="tab-grid">
      <section class="metrics-grid">
        <article class="metric-card">
          <span class="metric-card__label">Master dimensions</span>
          <strong class="metric-card__value">${formatNumber(masterItems.counts.dimensions || 0)}</strong>
        </article>
        <article class="metric-card">
          <span class="metric-card__label">Master expressions</span>
          <strong class="metric-card__value">${formatNumber(masterItems.counts.measures || 0)}</strong>
        </article>
        <article class="metric-card">
          <span class="metric-card__label">Master charts</span>
          <strong class="metric-card__value">${formatNumber(masterItems.counts.objects || 0)}</strong>
        </article>
        <article class="metric-card">
          <span class="metric-card__label">Total master items</span>
          <strong class="metric-card__value">${formatNumber(masterItems.counts.total || 0)}</strong>
        </article>
      </section>

      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Master dimensions</h3>
          <span class="mono-text">${formatNumber(masterItems.dimensions.length)} item(s)</span>
        </div>
        ${
          masterItems.dimensions.length
            ? `
              <div class="table-lite">
                <div class="table-lite__head">
                  <span>Dimension</span>
                  <span>Field definitions</span>
                  <span>Tags</span>
                </div>
                ${masterItems.dimensions
                  .map(
                    (dimension) => `
                      <div class="table-lite__row">
                        <strong>${escapeHtml(dimension.title || 'Untitled dimension')}</strong>
                        <span>${escapeHtml((dimension.fieldDefinitions || []).join(', ') || '—')}</span>
                        <span>${escapeHtml((dimension.tags || []).join(', ') || '—')}</span>
                      </div>
                    `
                  )
                  .join('')}
              </div>
            `
            : `<div class="empty-card">No master dimensions were found in this app.</div>`
        }
      </article>

      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Master expressions</h3>
          <span class="mono-text">${formatNumber(masterItems.measures.length)} item(s)</span>
        </div>
        ${
          masterItems.measures.length
            ? `
              <div class="stack-list compact">
                ${masterItems.measures
                  .map(
                    (measure) => `
                      <div class="expression-item">
                        <div class="expression-item__meta">
                          <strong>${escapeHtml(measure.title || measure.label || 'Untitled expression')}</strong>
                          <span>Definition</span>
                          <span>${formatNumber(String(measure.expression || '').length)} chars</span>
                        </div>
                        <code>${escapeHtml((measure.expression || '').trim() || 'No definition found for this master expression.')}</code>
                      </div>
                    `
                  )
                  .join('')}
              </div>
            `
            : `<div class="empty-card">No master expressions were found in this app.</div>`
        }
      </article>

      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Master charts</h3>
          <span class="mono-text">${formatNumber(masterItems.objects.length)} item(s)</span>
        </div>
        ${
          masterItems.objects.length
            ? `
              <div class="table-lite">
                <div class="table-lite__head">
                  <span>Chart</span>
                  <span>Visualization</span>
                  <span>Children</span>
                </div>
                ${masterItems.objects
                  .map(
                    (item) => `
                      <div class="table-lite__row">
                        <strong>${escapeHtml(item.title || 'Untitled master object')}</strong>
                        <span>${escapeHtml(item.visualization || 'unknown')}</span>
                        <span>${formatNumber(item.childCount ?? 0)}</span>
                      </div>
                    `
                  )
                  .join('')}
              </div>
            `
            : `<div class="empty-card">No master charts were found in this app.</div>`
        }
      </article>
    </section>
  `;
}

function renderMasterItemUsageTab(analysis) {
  const usage = getMasterItemUsageData(analysis);

  return `
    <section class="tab-grid">
      <section class="metrics-grid">
        <article class="metric-card">
          <span class="metric-card__label">Dimensions on 0</span>
          <strong class="metric-card__value">${formatNumber(usage.counts.unusedDimensions || 0)}</strong>
        </article>
        <article class="metric-card">
          <span class="metric-card__label">Expressions on 0</span>
          <strong class="metric-card__value">${formatNumber(usage.counts.unusedMeasures || 0)}</strong>
        </article>
        <article class="metric-card">
          <span class="metric-card__label">Master charts on 0</span>
          <strong class="metric-card__value">${formatNumber(usage.counts.unusedObjects || 0)}</strong>
        </article>
        <article class="metric-card">
          <span class="metric-card__label">Total on 0</span>
          <strong class="metric-card__value">${formatNumber(usage.counts.unusedTotal || 0)}</strong>
        </article>
      </section>

      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Master dimensions usage</h3>
          <span class="mono-text">${formatNumber(usage.dimensions.length)} item(s)</span>
        </div>
        ${
          usage.dimensions.length
            ? `
              <div class="table-lite table-lite--usage-details">
                <div class="table-lite__head">
                  <span>Dimension</span>
                  <span>Used in objects</span>
                  <span>Status</span>
                  <span>Details</span>
                </div>
                ${usage.dimensions
                  .map(
                    (item) => `
                      <div class="table-lite__row ${item.unused ? 'is-unused' : ''}">
                        <strong>${escapeHtml(item.title || 'Untitled dimension')}</strong>
                        <span>${formatNumber(item.usedInObjects || 0)}</span>
                        <span>${item.unused ? 'Unused' : 'Used'}</span>
                        ${renderUsageDetailsPopover({
                          ariaLabel: `Details for ${item.title || 'master dimension'}`,
                          title: 'Used in objects',
                          items: toUsageObjectDisplayNames(item),
                          emptyMessage: 'Not used in any object.',
                        })}
                      </div>
                    `
                  )
                  .join('')}
              </div>
            `
            : `<div class="empty-card">No master dimensions were found in this app.</div>`
        }
      </article>

      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Master expressions usage</h3>
          <span class="mono-text">${formatNumber(usage.measures.length)} item(s)</span>
        </div>
        ${
          usage.measures.length
            ? `
              <div class="table-lite table-lite--usage-details">
                <div class="table-lite__head">
                  <span>Expression</span>
                  <span>Used in objects</span>
                  <span>Status</span>
                  <span>Details</span>
                </div>
                ${usage.measures
                  .map(
                    (item) => `
                      <div class="table-lite__row ${item.unused ? 'is-unused' : ''}">
                        <strong>${escapeHtml(item.title || 'Untitled expression')}</strong>
                        <span>${formatNumber(item.usedInObjects || 0)}</span>
                        <span>${item.unused ? 'Unused' : 'Used'}</span>
                        ${renderUsageDetailsPopover({
                          ariaLabel: `Details for ${item.title || 'master expression'}`,
                          title: 'Used in objects',
                          items: toUsageObjectDisplayNames(item),
                          emptyMessage: 'Not used in any object.',
                        })}
                      </div>
                    `
                  )
                  .join('')}
              </div>
            `
            : `<div class="empty-card">No master expressions were found in this app.</div>`
        }
      </article>

      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Master charts usage</h3>
          <span class="mono-text">${formatNumber(usage.objects.length)} item(s)</span>
        </div>
        ${
          usage.objects.length
            ? `
              <div class="table-lite table-lite--usage-details table-lite--chart-usage">
                <div class="table-lite__head">
                  <span>Chart</span>
                  <span>Used in sheets</span>
                  <span>Visualization</span>
                  <span>Details</span>
                </div>
                ${usage.objects
                  .map(
                    (item) => `
                      <div class="table-lite__row ${item.unused ? 'is-unused' : ''}">
                        <strong>${escapeHtml(item.title || 'Untitled master chart')}</strong>
                        <span>${formatNumber(item.usedInSheets || 0)}</span>
                        <span>${escapeHtml(item.visualization || 'unknown')}</span>
                        ${renderUsageDetailsPopover({
                          ariaLabel: `Details for ${item.title || 'master chart'}`,
                          title: 'Used in sheets',
                          items: item.sheetTitles || [],
                          emptyMessage: 'Not used on any sheet.',
                        })}
                      </div>
                    `
                  )
                  .join('')}
              </div>
            `
            : `<div class="empty-card">No master charts were found in this app.</div>`
        }
      </article>
    </section>
  `;
}

function renderFieldUsageTab(analysis) {
  const fieldUsage = getFieldUsageData(analysis);

  return `
    <section class="tab-grid">
      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Field usage matrix</h3>
          <span class="mono-text">${formatNumber(fieldUsage.counts.fields || 0)} field(s)</span>
        </div>
        <div class="field-usage-meta">
          <span class="mini-pill">Used: ${formatNumber(fieldUsage.counts.usedFields || 0)}</span>
          <span class="mini-pill mini-pill--warn">Unused: ${formatNumber(fieldUsage.counts.unusedFields || 0)}</span>
        </div>
        <div class="field-usage-table-wrap">
          <table class="field-usage-table">
            <thead>
              <tr>
                <th>Veldnaam</th>
                <th>Objecten</th>
                <th>Master Items</th>
                <th>Variabelen</th>
                <th>Totaal</th>
              </tr>
            </thead>
            <tbody>
              ${fieldUsage.rows
                .map(
                  (row) => `
                    <tr class="${row.unused ? 'is-unused' : ''}">
                      <td><strong>${escapeHtml(row.name)}</strong></td>
                      <td>${formatNumber(row.objects)}</td>
                      <td>${formatNumber(row.masterItems)}</td>
                      <td>${formatNumber(row.variables)}</td>
                      <td>${formatNumber(row.total)}</td>
                    </tr>
                  `
                )
                .join('')}
            </tbody>
          </table>
        </div>
      </article>

      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Suggested DROP FIELD statement</h3>
          <span class="mono-text">${formatNumber(fieldUsage.dropFieldSuggestion.fields?.length || 0)} field(s)</span>
        </div>
        ${
          fieldUsage.dropFieldSuggestion.statement
            ? `<pre class="drop-field-viewer"><code>${escapeHtml(fieldUsage.dropFieldSuggestion.statement)}</code></pre>`
            : `<div class="empty-card">No DROP FIELD statement suggested. ${escapeHtml(fieldUsage.dropFieldSuggestion.note || '')}</div>`
        }
      </article>
    </section>
  `;
}

function renderHistogram(items) {
  const max = Math.max(...items.map((item) => item.count), 1);
  return `
    <div class="histogram">
      ${items
        .map(
          (item) => `
            <div class="histogram__bar">
              <span class="histogram__value">${formatNumber(item.count)}</span>
              <div class="histogram__track"><i style="height:${(item.count / max) * 100}%"></i></div>
              <span class="histogram__label">${escapeHtml(item.key)}</span>
            </div>
          `
        )
        .join('')}
    </div>
  `;
}

function renderDesignTab(analysis) {
  const masterMeasures = analysis.design.masterItemAdoption.measures;
  const masterDimensions = analysis.design.masterItemAdoption.dimensions;
  return `
    <section class="tab-grid">
      <article class="panel section-card">
        <div class="section-card__head">
          <h3>Measure adoption</h3>
        </div>
        <div class="adoption-card">
          <div><span>Library</span><strong>${formatNumber(masterMeasures.library)}</strong></div>
          <div><span>Ad hoc</span><strong>${formatNumber(masterMeasures.adhoc)}</strong></div>
        </div>
      </article>
      <article class="panel section-card">
        <div class="section-card__head">
          <h3>Dimension adoption</h3>
        </div>
        <div class="adoption-card">
          <div><span>Library</span><strong>${formatNumber(masterDimensions.library)}</strong></div>
          <div><span>Ad hoc</span><strong>${formatNumber(masterDimensions.adhoc)}</strong></div>
        </div>
      </article>
      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Expression length histogram</h3>
          <span class="mono-text">${formatNumber(analysis.design.expressionTotals.unique)} unique expressions</span>
        </div>
        ${renderHistogram(analysis.design.expressionHistogram)}
      </article>
      <article class="panel section-card">
        <div class="section-card__head">
          <h3>Longest expressions</h3>
        </div>
        <div class="stack-list compact">
          ${analysis.design.longestExpressions
            .map(
              (expression) => `
                <div class="expression-item">
                  <div class="expression-item__meta">
                    <span>${formatNumber(expression.length)} chars</span>
                  </div>
                  <code>${escapeHtml(truncate(expression.expression, 220))}</code>
                </div>
              `
            )
            .join('')}
        </div>
      </article>
      <article class="panel section-card">
        <div class="section-card__head">
          <h3>Duplicate expressions</h3>
        </div>
        <div class="stack-list compact">
          ${
            analysis.design.duplicateExpressions.length
              ? analysis.design.duplicateExpressions
                  .map(
                    (item) => `
                      <div class="expression-item">
                        <div class="expression-item__meta">
                          <span>${formatNumber(item.count)}x</span>
                          <span>${formatNumber(item.length)} chars</span>
                        </div>
                        <code>${escapeHtml(truncate(item.expression, 180))}</code>
                      </div>
                    `
                  )
                  .join('')
              : `<div class="empty-card">No duplicate expression groups were detected.</div>`
          }
        </div>
      </article>
      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Possible unused fields</h3>
        </div>
        <div class="warning-banner">${escapeHtml(analysis.design.possibleUnusedFields.caveat)}</div>
        <div class="table-lite">
          <div class="table-lite__head">
            <span>Field</span>
            <span>Source tables</span>
            <span>Cardinality</span>
          </div>
          ${analysis.design.possibleUnusedFields.fields
            .slice(0, 24)
            .map(
              (field) => `
                <div class="table-lite__row">
                  <strong>${escapeHtml(field.name)}</strong>
                  <span>${escapeHtml(field.sourceTables.join(', ') || '—')}</span>
                  <span>${formatNumber(field.cardinality)}</span>
                </div>
              `
            )
            .join('')}
        </div>
      </article>
    </section>
  `;
}

function getVisibleTables(analysis) {
  const tables = analysis.tables.tables || [];
  const filter = state.ui.filters.tableStatus;
  return filter === 'all' ? tables : tables.filter((table) => table.status === filter);
}

function getSelectedTable(analysis, visibleTables) {
  const selectedId = state.ui.selectedTableByJob[state.activeJobId];
  return visibleTables.find((table) => table.id === selectedId) || visibleTables[0] || null;
}

function renderTablesTab(analysis) {
  if (analysis.tables.skipped) {
    return `
      <section class="tab-grid">
        <article class="panel empty-card empty-card--large">
          <h3>Tables were skipped for this analysis</h3>
          <p>Re-run the upload with table reconstruction enabled to inspect exact, partial, and heuristic table confidence.</p>
        </article>
      </section>
    `;
  }

  const visibleTables = getVisibleTables(analysis);
  const selectedTable = getSelectedTable(analysis, visibleTables);

  return `
    <section class="tab-grid">
      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Table confidence</h3>
          <div class="filter-row">
            ${['all', 'exact', 'partial', 'heuristic']
              .map(
                (status) => `
                  <button
                    type="button"
                    class="filter-chip ${state.ui.filters.tableStatus === status ? 'is-active' : ''}"
                    data-table-status="${escapeHtml(status)}"
                  >
                    ${escapeHtml(toSentence(status))}
                  </button>
                `
              )
              .join('')}
          </div>
        </div>
        <div class="tables-layout">
          <div class="table-selector">
            ${visibleTables
              .map(
                (table) => `
                  <button
                    type="button"
                    class="table-selector__item ${selectedTable?.id === table.id ? 'is-active' : ''}"
                    data-table-id="${escapeHtml(table.id)}"
                  >
                    <div>
                      <strong>${escapeHtml(table.name)}</strong>
                      <p>${escapeHtml(table.status)} · ${formatNumber(table.exportedRowCount)} rows</p>
                    </div>
                    <span>${formatNumber(table.exportedColumns)} cols</span>
                  </button>
                `
              )
              .join('')}
          </div>
          <div class="table-detail">
            ${
              selectedTable
                ? `
                  <div class="table-detail__hero">
                    <div>
                      <p class="eyebrow">Selected table</p>
                      <h3>${escapeHtml(selectedTable.name)}</h3>
                      <p class="lede">${escapeHtml(selectedTable.status)} export with ${formatNumber(selectedTable.exportedRowCount)} exported rows.</p>
                    </div>
                    <div class="hero-meta">
                      <div><span>Expected rows</span><strong>${formatNumber(selectedTable.expectedRowCount)}</strong></div>
                      <div><span>Exported rows</span><strong>${formatNumber(selectedTable.exportedRowCount)}</strong></div>
                      <div><span>Columns</span><strong>${formatNumber(selectedTable.exportedColumns)}</strong></div>
                    </div>
                  </div>
                  <div class="table-lite">
                    <div class="table-lite__head">
                      <span>Column</span>
                      <span>Status</span>
                      <span>Coverage</span>
                    </div>
                    ${selectedTable.columns
                      .map(
                        (column) => `
                          <div class="table-lite__row">
                            <strong>${escapeHtml(column.name)}</strong>
                            <span>${escapeHtml(column.status)}</span>
                            <span>${formatPercent(column.coverageRatio)}</span>
                          </div>
                        `
                      )
                      .join('')}
                  </div>
                `
                : `<div class="empty-card">No tables match the current filter.</div>`
            }
          </div>
        </div>
      </article>
    </section>
  `;
}

function renderScriptTab(analysis) {
  const sections = splitScriptSections(analysis.script.text);
  const selectedIndex = getSelectedScriptTabIndex(sections.length);
  const selectedSection = sections[selectedIndex] || sections[0];

  return `
    <section class="tab-grid">
      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Script</h3>
          <span class="mono-text">${formatNumber(sections.length)} tab(s)</span>
        </div>
        <div class="script-tab-nav">
          <button type="button" class="script-tab-scroll" data-script-tab-scroll="left" aria-label="Scroll script tabs left">
            <i class="fa-solid fa-chevron-left" aria-hidden="true"></i>
          </button>
          <div class="script-tab-row" role="tablist" aria-label="Script tabs">
            ${sections
              .map(
                (section, index) => `
                  <button
                    type="button"
                    class="script-tab-button ${selectedIndex === index ? 'is-active' : ''}"
                    data-script-tab-index="${index}"
                    role="tab"
                    aria-selected="${selectedIndex === index ? 'true' : 'false'}"
                    title="${escapeHtml(section.name)}"
                  >
                    ${escapeHtml(section.name)}
                  </button>
                `
              )
              .join('')}
          </div>
          <button type="button" class="script-tab-scroll" data-script-tab-scroll="right" aria-label="Scroll script tabs right">
            <i class="fa-solid fa-chevron-right" aria-hidden="true"></i>
          </button>
        </div>
        <div class="script-viewer-shell">
          <pre class="script-viewer"><code>${escapeHtml(selectedSection?.content || '')}</code></pre>
        </div>
      </article>
    </section>
  `;
}

function splitScriptSections(scriptText) {
  const text = String(scriptText || '');
  const lines = text.split(/\r?\n/);
  const markerPattern = /^\s*\/\/\/\s*\$tab\s+(.+?)\s*$/i;
  const sections = [];
  let current = null;

  for (const line of lines) {
    const match = line.match(markerPattern);
    if (match) {
      if (current) {
        sections.push(current);
      }
      current = {
        name: match[1].trim() || `Tab ${sections.length + 1}`,
        lines: [],
      };
      continue;
    }
    if (!current) {
      current = { name: 'Script', lines: [] };
    }
    current.lines.push(line);
  }

  if (current) {
    sections.push(current);
  }

  if (!sections.length) {
    sections.push({ name: 'Script', lines: [text] });
  }

  return sections.map((section) => ({
    name: section.name,
    content: section.lines.join('\n').trimEnd(),
  }));
}

function getSelectedScriptTabIndex(sectionCount) {
  if (!state.activeJobId) return 0;
  const storedIndex = Number(state.ui.selectedScriptTabByJob[state.activeJobId]);
  if (!Number.isFinite(storedIndex) || storedIndex < 0 || storedIndex >= sectionCount) {
    return 0;
  }
  return storedIndex;
}

function syncScriptTabScroller() {
  const nav = appRoot.querySelector('.script-tab-nav');
  if (!nav) return;
  const row = nav.querySelector('.script-tab-row');
  const leftButton = nav.querySelector('[data-script-tab-scroll="left"]');
  const rightButton = nav.querySelector('[data-script-tab-scroll="right"]');
  if (!row || !leftButton || !rightButton) return;

  const maxOffset = Math.max(0, row.scrollWidth - row.clientWidth);
  const atStart = row.scrollLeft <= 1;
  const atEnd = row.scrollLeft >= maxOffset - 1 || maxOffset <= 1;

  leftButton.disabled = atStart;
  rightButton.disabled = atEnd;
}

function initializeScriptTabScroller() {
  const nav = appRoot.querySelector('.script-tab-nav');
  if (!nav) return;
  const row = nav.querySelector('.script-tab-row');
  if (!row) return;

  row.addEventListener(
    'scroll',
    () => {
      syncScriptTabScroller();
    },
    { passive: true }
  );

  window.requestAnimationFrame(() => {
    syncScriptTabScroller();
  });
}

function applyScriptViewerLayout() {
  const scriptShell = appRoot.querySelector('.script-viewer-shell');
  const scriptViewer = appRoot.querySelector('.script-viewer');
  if (!scriptShell || !scriptViewer) {
    return;
  }

  const viewportHeight = window.visualViewport?.height || window.innerHeight || document.documentElement.clientHeight;
  const shellTop = scriptShell.getBoundingClientRect().top;
  const bottomGap = 18;
  const availableHeight = Math.floor(viewportHeight - shellTop - bottomGap);

  if (availableHeight > 180) {
    scriptShell.style.height = `${availableHeight}px`;
    scriptViewer.style.height = '100%';
  } else {
    scriptShell.style.height = '';
    scriptViewer.style.height = '';
  }
}

function resetScriptViewerLayout() {
  const scriptShell = appRoot.querySelector('.script-viewer-shell');
  const scriptViewer = appRoot.querySelector('.script-viewer');
  if (scriptShell) {
    scriptShell.style.height = '';
  }
  if (scriptViewer) {
    scriptViewer.style.height = '';
  }
}

function renderAssetsTab(analysis) {
  return `
    <section class="tab-grid">
      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Extracted assets</h3>
          <span class="mono-text">${formatNumber(analysis.assets.count)} files</span>
        </div>
        ${
          analysis.assets.items.length
            ? `
              <div class="asset-grid">
                ${analysis.assets.items
                  .map(
                    (asset) => `
                      <article class="asset-card">
                        <img src="${escapeHtml(asset.url)}" alt="${escapeHtml(asset.filename)}" loading="lazy" />
                        <div class="asset-card__body">
                          <strong>${escapeHtml(asset.filename)}</strong>
                          <div class="chips-row">
                            <span class="mini-pill">${escapeHtml(asset.type)}</span>
                            <span class="mini-pill">${formatBytes(asset.size)}</span>
                            ${asset.sourceType ? `<span class="mini-pill">${escapeHtml(asset.sourceType)}</span>` : ''}
                          </div>
                        </div>
                      </article>
                    `
                  )
                  .join('')}
              </div>
            `
            : `<div class="empty-card empty-card--large"><h3>No assets were extracted</h3><p>This QVF did not expose gallery-ready binary assets in the current extraction path.</p></div>`
        }
      </article>
    </section>
  `;
}

function renderActiveTab(analysis) {
  switch (state.activeTab) {
    case 'structure':
      return renderStructureTab(analysis);
    case 'master-items':
      return renderMasterItemsTab(analysis);
    case 'usage':
      return renderMasterItemUsageTab(analysis);
    case 'field-usage':
      return renderFieldUsageTab(analysis);
    case 'design':
      return renderDesignTab(analysis);
    case 'tables':
      return renderTablesTab(analysis);
    case 'script':
      return renderScriptTab(analysis);
    case 'assets':
      return renderAssetsTab(analysis);
    case 'overview':
    default:
      return renderOverviewTab(analysis);
  }
}

function renderPortalNav() {
  return `
    <nav class="portal-nav" aria-label="Workspace pages">
      ${VIEWS.map((view) => {
        return `
          <button
            type="button"
            class="portal-nav__item ${state.activeView === view.id ? 'is-active' : ''}"
            data-view="${escapeHtml(view.id)}"
          >
            <span class="nav-item-main">${renderMenuIcon(view.id)}<span>${escapeHtml(view.label)}</span></span>
          </button>
        `;
      }).join('')}
    </nav>
  `;
}

function renderSidebar(analysis) {
  return `
    <aside class="workspace-sidebar">
      <div class="workspace-sidebar__section">
        <div class="workspace-sidebar__group">
          <p class="sidebar-label">Workspace</p>
          ${renderPortalNav()}
        </div>
      </div>
      <div class="workspace-sidebar__section">
        <div class="workspace-sidebar__group">
        <p class="sidebar-label">Explore</p>
        <nav class="sidebar-nav" aria-label="Analysis sections">
          <button
            type="button"
            class="sidebar-nav__item ${state.activeView === 'analysis' && state.activeTab === 'overview' ? 'is-active' : ''}"
            data-view="analysis"
            data-tab="overview"
            ${!analysis ? 'disabled' : ''}
          >
            <span class="nav-item-main">${renderMenuIcon('overview')}<span>Overview</span></span>
          </button>
          ${TABS.map((tab) => {
            const badge = getTabBadge(tab.id, analysis);
            const disabled = !analysis;
            return `
              <button type="button" class="sidebar-nav__item ${state.activeTab === tab.id ? 'is-active' : ''}" data-tab="${escapeHtml(tab.id)}" ${disabled ? 'disabled' : ''}>
                <span class="nav-item-main">${renderMenuIcon(tab.id)}<span>${escapeHtml(tab.label)}</span></span>
                ${badge !== '' ? `<span class="nav-badge">${formatNumber(badge)}</span>` : ''}
              </button>
            `;
          }).join('')}
        </nav>
        </div>
      </div>
    </aside>
  `;
}

function renderAppsPage() {
  return `
    <section class="tab-grid">
      <article class="panel section-card section-card--wide">
        <div class="section-card__head">
          <h3>Apps</h3>
          <div class="apps-head-actions">
            <span class="mono-text">${formatNumber(state.serverJobs.length)} app(s)</span>
            <button type="button" class="secondary-button" data-action="toggle-upload">Upload QVF</button>
          </div>
        </div>
        ${
          state.importsBusy
            ? `<div class="empty-card empty-card--large"><h3>Loading apps</h3><p>Refreshing analyzed apps from the server.</p></div>`
            : state.serverJobs.length
              ? `
                <div class="imports-list">
                  ${state.serverJobs
                    .map(
                      (job) => `
                        <article class="import-card ${state.activeJobId === job.jobId ? 'is-active' : ''}">
                          <div class="import-card__main">
                            <div class="import-card__title">
                              <strong>${escapeHtml(job.appLabel || 'Untitled analysis')}</strong>
                              <span>${escapeHtml(job.sourceFileName || 'Unknown source file')}</span>
                            </div>
                            <div class="import-card__meta">
                              <span>${formatDate(job.analyzedAt)}</span>
                              <span>${job.includeTables ? 'Tables included' : 'Metadata only'}</span>
                              <span>${formatNumber(job.counts?.dataSources || 0)} sources</span>
                              <span>${formatNumber(job.counts?.tables || 0)} tables</span>
                            </div>
                          </div>
                          <div class="import-card__actions">
                            <button type="button" class="secondary-button" data-open-job="${escapeHtml(job.jobId)}">Open</button>
                            ${renderDownloadLink(job?.downloadUrl, 'Download ZIP', 'secondary-button')}
                            <button type="button" class="danger-button" data-delete-job="${escapeHtml(job.jobId)}">Remove</button>
                          </div>
                        </article>
                      `
                    )
                    .join('')}
                </div>
              `
              : `
                <div class="empty-card empty-card--large apps-empty-state">
                  <h3>No apps yet</h3>
                  <p>Upload a QVF to analyze an app and manage it from this page.</p>
                  <div class="apps-empty-upload">${renderUploadForm()}</div>
                </div>
              `
        }
      </article>
    </section>
  `;
}

function renderWorkspace() {
  const analysis = getActiveAnalysis();
  const job = getActiveJob();
  const showAppsView = state.activeView === 'apps' || !analysis;
  const isScriptFocus = !showAppsView && state.activeTab === 'script';

  return `
    <main class="shell shell--workspace ${isScriptFocus ? 'shell--script-focus' : ''}">
      ${renderTopbar(showAppsView, job)}
      <div class="workspace-frame panel">
        ${renderSidebar(analysis)}
        <section class="workspace-content ${isScriptFocus ? 'workspace-content--script-focus' : ''}">
          ${
            showAppsView && state.ui.showUploadTray
              ? `<section class="workspace-upload-tray panel">${renderUploadForm({ compact: true })}</section>`
              : ''
          }
          <section class="workspace-main ${isScriptFocus ? 'workspace-main--script-focus' : ''}">
            ${state.error ? `<p class="workspace-error">${escapeHtml(state.error)}</p>` : ''}
            ${showAppsView ? renderAppsPage() : renderActiveTab(analysis)}
          </section>
        </section>
      </div>
    </main>
  `;
}

function renderApp() {
  const lockMainScroll = state.activeView === 'analysis' && state.activeTab === 'script' && Boolean(getActiveAnalysis());
  document.body.classList.toggle('page-scroll-locked', lockMainScroll);
  appRoot.innerHTML = renderWorkspace();
  initializeScriptTabScroller();
  if (lockMainScroll) {
    window.requestAnimationFrame(() => {
      applyScriptViewerLayout();
      syncScriptTabScroller();
    });
  } else {
    resetScriptViewerLayout();
  }
}

function handleViewportResize() {
  const lockMainScroll = state.activeView === 'analysis' && state.activeTab === 'script' && Boolean(getActiveAnalysis());
  if (!lockMainScroll) {
    return;
  }
  window.requestAnimationFrame(() => {
    applyScriptViewerLayout();
    syncScriptTabScroller();
  });
}

document.addEventListener('submit', async (event) => {
  const form = event.target.closest('[data-role="upload-form"]');
  if (!form) return;
  event.preventDefault();
  await handleUpload(form);
});

document.addEventListener('click', (event) => {
  const toggleAppSwitchButton = event.target.closest('[data-action="toggle-app-switch"]');
  if (toggleAppSwitchButton) {
    state.ui.topbarAppMenuOpen = !state.ui.topbarAppMenuOpen;
    renderApp();
    return;
  }

  const selectJobButton = event.target.closest('[data-select-job]');
  if (selectJobButton) {
    state.ui.topbarAppMenuOpen = false;
    openJob(selectJobButton.dataset.selectJob);
    return;
  }

  let shouldRenderForMenuClose = false;
  if (state.ui.topbarAppMenuOpen && !event.target.closest('.topbar-app-switch')) {
    state.ui.topbarAppMenuOpen = false;
    shouldRenderForMenuClose = true;
  }

  const viewButton = event.target.closest('[data-view]');
  if (viewButton) {
    state.ui.topbarAppMenuOpen = false;
    state.activeView = normalizeView(viewButton.dataset.view);
    if (state.activeView === 'analysis' && viewButton.dataset.tab) {
      state.activeTab = normalizeTab(viewButton.dataset.tab);
    }
    syncUrl();
    renderApp();
    return;
  }

  const tabButton = event.target.closest('[data-tab]');
  if (tabButton) {
    state.ui.topbarAppMenuOpen = false;
    state.activeView = 'analysis';
    state.activeTab = normalizeTab(tabButton.dataset.tab);
    syncUrl();
    renderApp();
    return;
  }

  const toggleUploadButton = event.target.closest('[data-action="toggle-upload"]');
  if (toggleUploadButton) {
    state.ui.topbarAppMenuOpen = false;
    state.ui.showUploadTray = !state.ui.showUploadTray;
    renderApp();
    return;
  }

  const tableStatusButton = event.target.closest('[data-table-status]');
  if (tableStatusButton) {
    state.ui.filters.tableStatus = tableStatusButton.dataset.tableStatus;
    renderApp();
    return;
  }

  const tableButton = event.target.closest('[data-table-id]');
  if (tableButton && state.activeJobId) {
    state.ui.selectedTableByJob[state.activeJobId] = tableButton.dataset.tableId;
    renderApp();
    return;
  }

  const scriptTabButton = event.target.closest('[data-script-tab-index]');
  if (scriptTabButton && state.activeJobId) {
    state.ui.selectedScriptTabByJob[state.activeJobId] = Number(scriptTabButton.dataset.scriptTabIndex) || 0;
    renderApp();
    return;
  }

  const scriptTabScrollButton = event.target.closest('[data-script-tab-scroll]');
  if (scriptTabScrollButton) {
    const nav = scriptTabScrollButton.closest('.script-tab-nav');
    const row = nav?.querySelector('.script-tab-row');
    if (!row) {
      return;
    }
    const direction = scriptTabScrollButton.dataset.scriptTabScroll === 'left' ? -1 : 1;
    const step = Math.max(140, Math.floor(row.clientWidth * 0.48));
    row.scrollBy({
      left: direction * step,
      behavior: 'smooth',
    });
    window.setTimeout(() => {
      syncScriptTabScroller();
    }, 180);
    return;
  }

  const openJobButton = event.target.closest('[data-open-job]');
  if (openJobButton) {
    state.ui.topbarAppMenuOpen = false;
    openJob(openJobButton.dataset.openJob);
    return;
  }

  const deleteJobButton = event.target.closest('[data-delete-job]');
  if (deleteJobButton) {
    state.ui.topbarAppMenuOpen = false;
    deleteJob(deleteJobButton.dataset.deleteJob);
    return;
  }

  if (shouldRenderForMenuClose) {
    renderApp();
  }
});

document.addEventListener('change', (event) => {
  const includeTablesToggle = event.target.closest('input[name="includeTables"]');
  if (includeTablesToggle) {
    state.ui.includeTables = includeTablesToggle.checked;
    renderApp();
    return;
  }
});

window.addEventListener('resize', handleViewportResize, { passive: true });
if (window.visualViewport) {
  window.visualViewport.addEventListener('resize', handleViewportResize, { passive: true });
}

async function boot() {
  await loadImports();
  const { jobToken, view, tab } = readUrlState();
  state.activeView = view;
  state.activeTab = tab;
  if (jobToken) {
    const resolvedJobId = resolveJobTokenToId(jobToken) || (isUuidLike(jobToken) ? jobToken : null);
    if (!resolvedJobId) {
      state.activeView = 'apps';
      syncUrl();
      renderApp();
      return;
    }
    await loadJobFromUrl(resolvedJobId, { preserveView: true, preserveTab: true });
    if (!state.activeJobId) {
      state.activeView = 'apps';
      syncUrl();
      renderApp();
    }
    return;
  }
  if (state.serverJobs[0]?.jobId) {
    await loadJobFromUrl(state.serverJobs[0].jobId);
    state.activeView = 'apps';
    syncUrl();
    renderApp();
    return;
  }
  state.activeView = 'apps';
  syncUrl();
  renderApp();
}

boot();
