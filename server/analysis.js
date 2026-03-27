const fsp = require('node:fs/promises');
const path = require('node:path');

async function exists(targetPath) {
  try {
    await fsp.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function readJson(filePath, fallback) {
  if (!(await exists(filePath))) {
    return fallback;
  }

  try {
    const text = await fsp.readFile(filePath, 'utf8');
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

async function readText(filePath, fallback = '') {
  if (!(await exists(filePath))) {
    return fallback;
  }

  try {
    return await fsp.readFile(filePath, 'utf8');
  } catch {
    return fallback;
  }
}

async function readJsonLines(filePath) {
  const text = await readText(filePath, '');
  if (!text) {
    return [];
  }

  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function slugify(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 80);
}

function toTitleCase(value) {
  return String(value || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function normalizeExpression(expression) {
  return String(expression || '').replace(/\s+/g, ' ').trim();
}

function compareNatural(left, right) {
  return String(left || '').localeCompare(String(right || ''), undefined, {
    numeric: true,
    sensitivity: 'base',
  });
}

function makeExpressionHistogram(expressions) {
  const buckets = [
    { key: '0-30', min: 0, max: 30, count: 0 },
    { key: '31-60', min: 31, max: 60, count: 0 },
    { key: '61-90', min: 61, max: 90, count: 0 },
    { key: '91-120', min: 91, max: 120, count: 0 },
    { key: '121-180', min: 121, max: 180, count: 0 },
    { key: '181+', min: 181, max: Infinity, count: 0 },
  ];

  for (const expression of expressions) {
    const length = expression.length;
    const bucket = buckets.find((entry) => length >= entry.min && length <= entry.max);
    if (bucket) {
      bucket.count += 1;
    }
  }

  return buckets;
}

function parseScriptTabs(scriptText) {
  const matches = [...String(scriptText || '').matchAll(/^\s*\/\/\/\$tab\s+(.+)$/gm)];
  return matches.map((match, index) => ({
    id: slugify(`${index + 1}-${match[1]}`),
    name: match[1].trim(),
  }));
}

function parseInlineTables(scriptText) {
  const tables = [];
  const regex = /([A-Za-z0-9_$% -]+):\s*[\r\n]+Load\s+\*\s+Inline\s+\[/gim;
  let match;
  let index = 0;

  while ((match = regex.exec(scriptText))) {
    index += 1;
    tables.push({
      id: slugify(`${index}-${match[1]}`),
      name: match[1].trim(),
    });
  }

  return tables;
}

function collectVisualizationMetrics(decodedObjects) {
  const objectTypeCounts = new Map();
  const visualizationCounts = new Map();
  const expressions = [];
  const fieldRefs = new Set();
  const variableRefs = new Set();
  const measureLibraryObjectRefs = new Map();
  const dimensionLibraryObjectRefs = new Map();
  const objectCatalog = new Map();
  let libraryMeasurePlacements = 0;
  let adhocMeasurePlacements = 0;
  let libraryDimensionPlacements = 0;
  let adhocDimensionPlacements = 0;

  const bracketFieldPattern = /\[([^\]]+)\]/g;
  const variablePattern = /\$\(([A-Za-z0-9_]+)\)/g;

  function bump(map, key) {
    if (!key) return;
    map.set(key, (map.get(key) || 0) + 1);
  }

  function addLibraryObjectRef(targetMap, libraryId, objectId) {
    if (!libraryId || !objectId) return;
    if (!targetMap.has(libraryId)) {
      targetMap.set(libraryId, new Set());
    }
    targetMap.get(libraryId).add(objectId);
  }

  function extractObjectTitle(node) {
    const candidates = [node?.qMetaDef?.title, node?.title, node?.qData?.title, node?.qProperty?.qMetaDef?.title];
    for (const candidate of candidates) {
      if (typeof candidate === 'string' && candidate.trim()) {
        return candidate.trim();
      }
    }
    return '';
  }

  function upsertObjectCatalog(objectId, node) {
    if (!objectId) return;
    const existing = objectCatalog.get(objectId) || {};
    const nextTitle = extractObjectTitle(node);
    const nextType = typeof node?.qInfo?.qType === 'string' ? node.qInfo.qType : existing.type || null;
    objectCatalog.set(objectId, {
      title: existing.title || nextTitle || objectId,
      type: nextType,
    });
  }

  function walk(node, objectId) {
    if (Array.isArray(node)) {
      for (const item of node) {
        walk(item, objectId);
      }
      return;
    }

    if (!node || typeof node !== 'object') {
      return;
    }

    let currentObjectId = objectId;
    if (typeof node?.qInfo?.qId === 'string' && node.qInfo.qId.trim()) {
      currentObjectId = node.qInfo.qId.trim();
    }
    upsertObjectCatalog(currentObjectId, node);

    if (node.qInfo?.qType) {
      bump(objectTypeCounts, node.qInfo.qType);
    }

    if (typeof node.visualization === 'string') {
      const value = node.visualization === '/visualization' ? 'layout-container-child' : node.visualization;
      bump(visualizationCounts, value);
    }

    if (Array.isArray(node.qMeasures)) {
      for (const measure of node.qMeasures) {
        if (measure?.qLibraryId) {
          libraryMeasurePlacements += 1;
          addLibraryObjectRef(measureLibraryObjectRefs, measure.qLibraryId, currentObjectId);
        } else {
          adhocMeasurePlacements += 1;
        }
      }
    }

    if (Array.isArray(node.qDimensions)) {
      for (const dimension of node.qDimensions) {
        if (dimension?.qLibraryId) {
          libraryDimensionPlacements += 1;
          addLibraryObjectRef(dimensionLibraryObjectRefs, dimension.qLibraryId, currentObjectId);
        } else {
          adhocDimensionPlacements += 1;
        }
      }
    }

    if (Array.isArray(node.qFieldDefs)) {
      for (const fieldDef of node.qFieldDefs) {
        if (typeof fieldDef === 'string' && fieldDef.trim()) {
          fieldRefs.add(fieldDef.trim());
        }
      }
    }

    if (typeof node.qDef === 'string') {
      const normalized = normalizeExpression(node.qDef);
      if (normalized) {
        expressions.push(normalized);
      }

      for (const match of normalized.matchAll(bracketFieldPattern)) {
        fieldRefs.add(match[1]);
      }

      for (const match of normalized.matchAll(variablePattern)) {
        variableRefs.add(match[1]);
      }
    }

    for (const value of Object.values(node)) {
      walk(value, currentObjectId);
    }
  }

  for (let index = 0; index < decodedObjects.length; index += 1) {
    const entry = decodedObjects[index] || {};
    const objectId = entry.object?.qInfo?.qId || `object-${entry.marker_offset || index + 1}`;
    walk(entry.object, objectId);
  }

  return {
    objectTypeCounts: [...objectTypeCounts.entries()]
      .map(([type, count]) => ({ type, count }))
      .sort((a, b) => b.count - a.count || a.type.localeCompare(b.type)),
    visualizationCounts: [...visualizationCounts.entries()]
      .map(([type, count]) => ({ type, count }))
      .sort((a, b) => b.count - a.count || a.type.localeCompare(b.type)),
    expressions,
    fieldRefs,
    variableRefs,
    masterItemUsage: {
      measures: {
        library: libraryMeasurePlacements,
        adhoc: adhocMeasurePlacements,
      },
      dimensions: {
        library: libraryDimensionPlacements,
        adhoc: adhocDimensionPlacements,
      },
    },
    libraryObjectUsage: {
      measures: measureLibraryObjectRefs,
      dimensions: dimensionLibraryObjectRefs,
    },
    objectCatalog,
  };
}

function createFieldResolver(dataModel) {
  const fields = (dataModel.qfields || [])
    .map((field) => field.qname)
    .filter((name) => name && !String(name).startsWith('$'));
  const byLower = new Map(fields.map((name) => [String(name).toLowerCase(), name]));
  const hasKnownFields = byLower.size > 0;

  return {
    fields: [...new Set(fields)].sort((a, b) => a.localeCompare(b)),
    resolve(name) {
      if (!name) return null;
      const trimmed = String(name).trim();
      if (!trimmed) return null;
      const matched = byLower.get(trimmed.toLowerCase()) || null;
      if (matched) {
        return matched;
      }
      return hasKnownFields ? null : trimmed;
    },
  };
}

function extractBracketFieldRefs(text, resolveFieldName) {
  const refs = new Set();
  const source = String(text || '');
  const bracketFieldPattern = /\[([^\]]+)\]/g;

  for (const match of source.matchAll(bracketFieldPattern)) {
    const resolved = resolveFieldName(match[1]);
    if (resolved) {
      refs.add(resolved);
    }
  }

  return refs;
}

function addPotentialFieldRef(rawValue, resolveFieldName, targetSet) {
  if (typeof rawValue !== 'string') {
    return;
  }

  const trimmed = rawValue.trim();
  if (!trimmed) {
    return;
  }

  const looksLikeSimpleFieldName = /^[A-Za-z0-9_$%.\- ]{1,160}$/.test(trimmed);
  if (looksLikeSimpleFieldName) {
    const direct = resolveFieldName(trimmed);
    if (direct) {
      targetSet.add(direct);
    }
  }

  const bracketRefs = extractBracketFieldRefs(trimmed, resolveFieldName);
  for (const field of bracketRefs) {
    targetSet.add(field);
  }
}

function collectFieldRefsFromNode(node, resolveFieldName, refs) {
  if (Array.isArray(node)) {
    for (const item of node) {
      collectFieldRefsFromNode(item, resolveFieldName, refs);
    }
    return;
  }

  if (!node || typeof node !== 'object') {
    if (typeof node === 'string' && node.includes('[')) {
      addPotentialFieldRef(node, resolveFieldName, refs);
    }
    return;
  }

  if (Array.isArray(node.qFieldDefs)) {
    for (const fieldDef of node.qFieldDefs) {
      addPotentialFieldRef(fieldDef, resolveFieldName, refs);
    }
  }

  if (typeof node.qDef === 'string') {
    addPotentialFieldRef(node.qDef, resolveFieldName, refs);
  }

  if (typeof node.qExpression === 'string') {
    addPotentialFieldRef(node.qExpression, resolveFieldName, refs);
  }

  if (typeof node.qField === 'string') {
    addPotentialFieldRef(node.qField, resolveFieldName, refs);
  }

  for (const value of Object.values(node)) {
    collectFieldRefsFromNode(value, resolveFieldName, refs);
  }
}

function quoteFieldName(name) {
  return `[${String(name || '').replace(/\]/g, ']]')}]`;
}

function buildFieldUsage({ dataModel, decodedObjects, measures, dimensions, variables }) {
  const fieldResolver = createFieldResolver(dataModel);
  const canScanObjects = fieldResolver.fields.length > 0;
  const usage = new Map();

  function ensureUsageEntry(fieldName) {
    if (!fieldName) return;
    if (!usage.has(fieldName)) {
      usage.set(fieldName, {
        objects: new Set(),
        masterItems: new Set(),
        variables: new Set(),
      });
    }
  }

  for (const fieldName of fieldResolver.fields) {
    ensureUsageEntry(fieldName);
  }

  if (canScanObjects) {
    for (let index = 0; index < decodedObjects.length; index += 1) {
      const entry = decodedObjects[index] || {};
      const refs = new Set();
      collectFieldRefsFromNode(entry.object, fieldResolver.resolve, refs);
      if (!refs.size) {
        continue;
      }
      const objectId = entry.object?.qInfo?.qId || `object-${entry.marker_offset || index + 1}`;
      for (const fieldName of refs) {
        ensureUsageEntry(fieldName);
        usage.get(fieldName)?.objects.add(objectId);
      }
    }
  }

  for (let index = 0; index < measures.length; index += 1) {
    const measure = measures[index] || {};
    const refs = new Set();
    addPotentialFieldRef(measure.expression || '', fieldResolver.resolve, refs);
    if (!refs.size) {
      continue;
    }
    const itemId = `measure-${measure.id || index + 1}`;
    for (const fieldName of refs) {
      ensureUsageEntry(fieldName);
      usage.get(fieldName)?.masterItems.add(itemId);
    }
  }

  for (let index = 0; index < dimensions.length; index += 1) {
    const dimension = dimensions[index] || {};
    const refs = new Set();
    for (const fieldDef of dimension.field_definitions || []) {
      addPotentialFieldRef(fieldDef, fieldResolver.resolve, refs);
    }
    if (!refs.size) {
      continue;
    }
    const itemId = `dimension-${dimension.id || index + 1}`;
    for (const fieldName of refs) {
      ensureUsageEntry(fieldName);
      usage.get(fieldName)?.masterItems.add(itemId);
    }
  }

  for (let index = 0; index < variables.length; index += 1) {
    const variable = variables[index] || {};
    const refs = new Set();
    collectFieldRefsFromNode(variable, fieldResolver.resolve, refs);
    if (!refs.size) {
      continue;
    }
    const variableId = `variable-${variable.id || variable.name || index + 1}`;
    for (const fieldName of refs) {
      ensureUsageEntry(fieldName);
      usage.get(fieldName)?.variables.add(variableId);
    }
  }

  const rows = [...usage.keys()].sort((a, b) => a.localeCompare(b)).map((fieldName) => {
    const entry = usage.get(fieldName);
    const objects = entry?.objects.size || 0;
    const masterItems = entry?.masterItems.size || 0;
    const variablesCount = entry?.variables.size || 0;
    const total = objects + masterItems + variablesCount;
    return {
      name: fieldName,
      objects,
      masterItems,
      variables: variablesCount,
      total,
      unused: total === 0,
    };
  });

  const unusedFields = rows.filter((row) => row.unused).map((row) => row.name);
  const dropFieldStatement = unusedFields.length
    ? `DROP FIELD\n  ${unusedFields.map((fieldName) => quoteFieldName(fieldName)).join(',\n  ')};`
    : '';

  return {
    version: 3,
    counts: {
      fields: rows.length,
      usedFields: rows.length - unusedFields.length,
      unusedFields: unusedFields.length,
    },
    rows,
    dropFieldSuggestion: {
      fields: unusedFields,
      statement: dropFieldStatement,
      note: unusedFields.length
        ? 'Review these fields before applying the statement in the load script.'
        : 'No fully unused fields were detected by this front-end usage scan.',
    },
  };
}

function buildSheetStructure(sheets) {
  return sheets.map((sheet) => {
    const types = new Map();
    const cells = Array.isArray(sheet.cells) ? sheet.cells : [];
    const children = Array.isArray(sheet.children) ? sheet.children : [];

    for (const cell of cells) {
      const type = cell.type || 'unknown';
      types.set(type, (types.get(type) || 0) + 1);
    }

    return {
      id: sheet.id,
      title: sheet.title || 'Untitled sheet',
      description: sheet.description || '',
      objectCount: children.length || cells.length,
      cellCount: cells.length,
      visualizationMix: [...types.entries()]
        .map(([type, count]) => ({ type, count }))
        .sort((a, b) => b.count - a.count || a.type.localeCompare(b.type)),
      layout: {
        columns: sheet.grid?.columns || 0,
        rows: sheet.grid?.rows || 0,
        mobileLayout: sheet.layout_options?.mobileLayout || null,
      },
    };
  });
}

function buildPossibleUnusedFields(dataModel, fieldRefs) {
  const fields = Array.isArray(dataModel.qfields) ? dataModel.qfields : [];
  const availableFieldNames = fields
    .map((field) => field.qname)
    .filter((name) => name && !name.startsWith('$'));
  const usedFieldNames = new Set();

  for (const ref of fieldRefs) {
    if (availableFieldNames.includes(ref)) {
      usedFieldNames.add(ref);
    }
  }

  const possibleUnused = availableFieldNames
    .filter((name) => !usedFieldNames.has(name))
    .map((name) => {
      const field = fields.find((entry) => entry.qname === name) || {};
      return {
        name,
        sourceTables: field.qsrc_tables || [],
        cardinality: field.qcardinal ?? null,
        tags: field.qtags || [],
      };
    })
    .sort((a, b) => a.name.localeCompare(b.name));

  return {
    usedFieldCount: usedFieldNames.size,
    possibleUnusedCount: possibleUnused.length,
    caveat:
      'Front-end only. Dynamic dimensions/expressions, section access, and field-name ambiguity may produce false positives.',
    fields: possibleUnused,
  };
}

function buildTableSummaries(tableManifest, tableConfidence) {
  const summaries = Array.isArray(tableManifest) ? tableManifest : [];
  const statusCounts = { exact: 0, partial: 0, heuristic: 0, missing: 0 };
  const tableEntries = summaries.map((entry) => {
    const confidenceByField = tableConfidence?.[entry.table] || {};
    const columns = Object.entries(confidenceByField).map(([name, meta]) => ({
      name,
      status: meta.status || 'unknown',
      coverageRatio: meta.coverage_ratio ?? null,
      inferredType: meta.inferred_type || null,
      notes: meta.notes || '',
      sourceBlockOffsets: meta.source_block_offsets || [],
    }));

    statusCounts[entry.status] = (statusCounts[entry.status] || 0) + 1;

    return {
      id: slugify(entry.table),
      name: entry.table,
      status: entry.status,
      expectedRowCount: entry.expected_row_count ?? null,
      exportedRowCount: entry.exported_row_count ?? null,
      totalColumns: entry.total_columns ?? 0,
      exportedColumns: entry.exported_columns ?? 0,
      files: entry.files || {},
      columns,
      heuristicColumnCount: columns.filter((column) => column.status === 'heuristic').length,
      partialColumnCount: columns.filter((column) => column.status === 'partial').length,
      lowCoverageColumnCount: columns.filter((column) => typeof column.coverageRatio === 'number' && column.coverageRatio < 1).length,
    };
  });

  return {
    totals: {
      tableCount: tableEntries.length,
      exact: statusCounts.exact || 0,
      partial: statusCounts.partial || 0,
      heuristic: statusCounts.heuristic || 0,
    },
    tables: tableEntries.sort((a, b) => (b.exportedRowCount || 0) - (a.exportedRowCount || 0)),
  };
}

function buildFlags({ tableSummary, possibleUnusedFields, expressions, masterItemUsage, dataModel }) {
  const flags = [];
  const addFlag = (severity, category, title, description, relatedEntity = null) => {
    flags.push({
      id: slugify(`${category}-${title}-${relatedEntity || flags.length + 1}`),
      severity,
      category,
      title,
      description,
      relatedEntity,
    });
  };

  for (const table of tableSummary.tables) {
    if (table.status === 'heuristic') {
      addFlag(
        'high',
        'table-confidence',
        'Heuristic table detected',
        `Table "${table.name}" is exported heuristically and should be reviewed before treating values as authoritative.`,
        table.name
      );
    }

    if (table.lowCoverageColumnCount > 0) {
      addFlag(
        table.lowCoverageColumnCount > 3 ? 'high' : 'medium',
        'column-coverage',
        'Columns with incomplete coverage',
        `Table "${table.name}" has ${table.lowCoverageColumnCount} column(s) with coverage below 100%.`,
        table.name
      );
    }
  }

  if (possibleUnusedFields.possibleUnusedCount > 0) {
    addFlag(
      possibleUnusedFields.possibleUnusedCount >= 25 ? 'high' : 'medium',
      'field-usage',
      'Possible unused fields',
      `${possibleUnusedFields.possibleUnusedCount} field(s) were not detected in front-end usage analysis.`,
      'possible-unused-fields'
    );
  }

  const longExpressions = expressions.filter((expression) => expression.length > 180);
  if (longExpressions.length > 0) {
    addFlag(
      longExpressions.length >= 5 ? 'high' : 'medium',
      'expressions',
      'Long expressions detected',
      `${longExpressions.length} expression(s) exceed 180 characters and may be harder to maintain.`,
      'expressions'
    );
  }

  const measureTotal = masterItemUsage.measures.library + masterItemUsage.measures.adhoc;
  if (measureTotal > 0 && masterItemUsage.measures.adhoc > masterItemUsage.measures.library) {
    addFlag(
      'medium',
      'master-items',
      'Low measure library adoption',
      `${masterItemUsage.measures.adhoc} ad-hoc measure placement(s) were found versus ${masterItemUsage.measures.library} library placement(s).`,
      'measure-adoption'
    );
  }

  const highCardinalityFields = (dataModel.qfields || [])
    .filter((field) => !String(field.qname || '').startsWith('$'))
    .filter((field) => (field.qcardinal || 0) >= 5000);

  if (highCardinalityFields.length > 0) {
    addFlag(
      highCardinalityFields.length >= 5 ? 'medium' : 'low',
      'model',
      'High-cardinality fields present',
      `${highCardinalityFields.length} field(s) have cardinality of 5,000 or more.`,
      'field-cardinality'
    );
  }

  return flags;
}

function buildDiscoveries({ app, sheets, measures, dimensions, variables, assets, dataSources, tableSummary, flags }) {
  const discoveries = [];

  discoveries.push({
    label: 'Sheets',
    value: sheets.length,
    tone: 'neutral',
  });
  discoveries.push({
    label: 'Visual assets',
    value: assets.length,
    tone: assets.length ? 'positive' : 'neutral',
  });
  discoveries.push({
    label: 'Data sources',
    value: (dataSources.lib_references || []).length,
    tone: 'neutral',
  });
  discoveries.push({
    label: 'Table confidence',
    value: `${tableSummary.totals.exact}/${tableSummary.totals.tableCount} exact`,
    tone: tableSummary.totals.heuristic ? 'warning' : 'positive',
  });

  return {
    cards: discoveries,
    highlights: [
      `${app.qTitle || 'App'} contains ${measures.length} measure(s) and ${dimensions.length} dimension(s).`,
      `${variables.length} variable(s) were extracted from script and app metadata.`,
      `${flags.length} flag(s) were generated from confidence, usage, and expression signals.`,
    ],
  };
}

function buildMasterItems({ measures, dimensions, masterObjects }) {
  const normalizedMeasures = (Array.isArray(measures) ? measures : []).map((measure) => ({
    id: measure.id,
    title: measure.title || measure.label || 'Untitled measure',
    label: measure.label || null,
    description: measure.description || '',
    expression: measure.expression || '',
    numberFormat: measure.number_format || null,
    tags: measure.tags || [],
  }));

  const normalizedDimensions = (Array.isArray(dimensions) ? dimensions : []).map((dimension) => ({
    id: dimension.id,
    title: dimension.title || dimension.alias || 'Untitled dimension',
    alias: dimension.alias || null,
    description: dimension.description || '',
    fieldDefinitions: dimension.field_definitions || [],
    fieldLabels: dimension.field_labels || [],
    grouping: dimension.grouping || null,
    tags: dimension.tags || [],
  }));

  const normalizedObjects = (Array.isArray(masterObjects) ? masterObjects : []).map((item) => ({
    id: item.id,
    title: item.title || 'Untitled master object',
    description: item.description || '',
    visualization: item.visualization || 'unknown',
    extendsId: item.extends_id || null,
    childCount: item.child_count ?? 0,
  }));

  return {
    counts: {
      measures: normalizedMeasures.length,
      dimensions: normalizedDimensions.length,
      objects: normalizedObjects.length,
      total: normalizedMeasures.length + normalizedDimensions.length + normalizedObjects.length,
    },
    measures: normalizedMeasures,
    dimensions: normalizedDimensions,
    objects: normalizedObjects,
  };
}

function buildMasterItemUsageDetails({ masterItems, sheets, libraryObjectUsage, objectCatalog }) {
  const measureUsageMap = libraryObjectUsage?.measures instanceof Map ? libraryObjectUsage.measures : new Map();
  const dimensionUsageMap = libraryObjectUsage?.dimensions instanceof Map ? libraryObjectUsage.dimensions : new Map();
  const objectCatalogMap = objectCatalog instanceof Map ? objectCatalog : new Map();
  const masterObjectSheetUsageMap = new Map();
  const sheetTitleById = new Map();

  for (const item of masterItems.objects) {
    masterObjectSheetUsageMap.set(item.id, new Set());
  }

  for (const sheet of Array.isArray(sheets) ? sheets : []) {
    if (!sheet?.id) continue;
    sheetTitleById.set(sheet.id, sheet.title || 'Untitled sheet');
    for (const child of Array.isArray(sheet.children) ? sheet.children : []) {
      const candidateIds = [child?.extends_id, child?.id].filter(Boolean);
      for (const candidateId of candidateIds) {
        if (masterObjectSheetUsageMap.has(candidateId)) {
          masterObjectSheetUsageMap.get(candidateId).add(sheet.id);
        }
      }
    }
  }

  function resolveObjectTitle(objectId) {
    const candidate = objectCatalogMap.get(objectId)?.title;
    return typeof candidate === 'string' && candidate.trim() ? candidate.trim() : String(objectId || 'Unknown object');
  }

  const dimensions = masterItems.dimensions.map((dimension) => {
    const usedObjectIds = [...(dimensionUsageMap.get(dimension.id) || new Set())];
    const usedObjects = usedObjectIds
      .map((objectId) => ({
        id: objectId,
        name: resolveObjectTitle(objectId),
        isMasterItem: objectCatalogMap.get(objectId)?.type === 'masterobject',
      }))
      .sort((left, right) => compareNatural(left.name, right.name));
    const usedObjectTitles = usedObjects.map((item) => item.name);
    const usedInObjects = usedObjectIds.length;
    return {
      id: dimension.id,
      title: dimension.title,
      usedObjects,
      usedObjectTitles,
      usedInObjects,
      unused: usedInObjects === 0,
    };
  });

  const measures = masterItems.measures.map((measure) => {
    const usedObjectIds = [...(measureUsageMap.get(measure.id) || new Set())];
    const usedObjects = usedObjectIds
      .map((objectId) => ({
        id: objectId,
        name: resolveObjectTitle(objectId),
        isMasterItem: objectCatalogMap.get(objectId)?.type === 'masterobject',
      }))
      .sort((left, right) => compareNatural(left.name, right.name));
    const usedObjectTitles = usedObjects.map((item) => item.name);
    const usedInObjects = usedObjectIds.length;
    return {
      id: measure.id,
      title: measure.title,
      expression: measure.expression || '',
      usedObjects,
      usedObjectTitles,
      usedInObjects,
      unused: usedInObjects === 0,
    };
  });

  const objects = masterItems.objects.map((item) => {
    const usedSheetIds = [...(masterObjectSheetUsageMap.get(item.id) || new Set())];
    const sheetTitles = usedSheetIds.map((sheetId) => sheetTitleById.get(sheetId) || sheetId).sort(compareNatural);
    return {
      id: item.id,
      title: item.title,
      visualization: item.visualization || 'unknown',
      usedInSheets: usedSheetIds.length,
      sheetTitles,
      unused: usedSheetIds.length === 0,
    };
  });

  const unusedDimensions = dimensions.filter((item) => item.unused).length;
  const unusedMeasures = measures.filter((item) => item.unused).length;
  const unusedObjects = objects.filter((item) => item.unused).length;

  return {
    version: 3,
    counts: {
      dimensions: dimensions.length,
      measures: measures.length,
      objects: objects.length,
      unusedDimensions,
      unusedMeasures,
      unusedObjects,
      unusedTotal: unusedDimensions + unusedMeasures + unusedObjects,
    },
    dimensions,
    measures,
    objects,
  };
}

async function buildAnalysisPayload({ jobId, extractDir, sourceFileName, includeTables, downloadUrl }) {
  const [
    app,
    sheets,
    measures,
    dimensions,
    variables,
    masterObjects,
    dataModel,
    dataSources,
    assets,
    tableManifest,
    tableConfidence,
    scriptText,
    decodedObjects,
  ] = await Promise.all([
    readJson(path.join(extractDir, 'app.json'), {}),
    readJson(path.join(extractDir, 'sheets.json'), []),
    readJson(path.join(extractDir, 'measures.json'), []),
    readJson(path.join(extractDir, 'dimensions.json'), []),
    readJson(path.join(extractDir, 'variables.json'), []),
    readJson(path.join(extractDir, 'masterobjects.json'), []),
    readJson(path.join(extractDir, 'data-model.json'), {}),
    readJson(path.join(extractDir, 'data-sources.json'), {}),
    readJson(path.join(extractDir, 'assets.json'), []),
    readJson(path.join(extractDir, 'tables', '_manifest.json'), []),
    readJson(path.join(extractDir, 'tables', '_confidence.json'), {}),
    readText(path.join(extractDir, 'script.qvs'), ''),
    readJsonLines(path.join(extractDir, 'raw', 'decoded-objects.jsonl')),
  ]);

  const visualizationMetrics = collectVisualizationMetrics(decodedObjects);
  const possibleUnusedFields = buildPossibleUnusedFields(dataModel, visualizationMetrics.fieldRefs);
  const expressionCounts = new Map();

  for (const expression of visualizationMetrics.expressions) {
    expressionCounts.set(expression, (expressionCounts.get(expression) || 0) + 1);
  }

  const uniqueExpressions = [...expressionCounts.keys()];
  const duplicateExpressions = [...expressionCounts.entries()]
    .filter(([, count]) => count > 1)
    .map(([expression, count]) => ({
      expression,
      count,
      length: expression.length,
    }))
    .sort((a, b) => b.count - a.count || b.length - a.length);

  const longestExpressions = [...uniqueExpressions]
    .sort((a, b) => b.length - a.length)
    .slice(0, 12)
    .map((expression) => ({
      id: slugify(expression.slice(0, 40)),
      expression,
      length: expression.length,
    }));

  const tableSummary = buildTableSummaries(tableManifest, tableConfidence);
  const masterItems = buildMasterItems({
    measures,
    dimensions,
    masterObjects,
  });
  const masterItemUsageDetails = buildMasterItemUsageDetails({
    masterItems,
    sheets,
    libraryObjectUsage: visualizationMetrics.libraryObjectUsage,
    objectCatalog: visualizationMetrics.objectCatalog,
  });
  const fieldUsage = buildFieldUsage({
    dataModel,
    decodedObjects,
    measures,
    dimensions,
    variables,
  });
  const flags = buildFlags({
    tableSummary,
    possibleUnusedFields,
    expressions: uniqueExpressions,
    masterItemUsage: visualizationMetrics.masterItemUsage,
    dataModel,
  });

  const majorFields = (dataModel.qfields || [])
    .filter((field) => !String(field.qname || '').startsWith('$'))
    .sort((a, b) => (b.qcardinal || 0) - (a.qcardinal || 0))
    .slice(0, 10)
    .map((field) => ({
      name: field.qname,
      cardinality: field.qcardinal ?? null,
      sourceTables: field.qsrc_tables || [],
      tags: field.qtags || [],
    }));

  const sharedFields = (dataModel.qfields || [])
    .filter((field) => !String(field.qname || '').startsWith('$'))
    .filter((field) => Array.isArray(field.qsrc_tables) && field.qsrc_tables.length > 1)
    .map((field) => ({
      name: field.qname,
      sourceTables: field.qsrc_tables,
      tags: field.qtags || [],
    }))
    .sort((a, b) => a.name.localeCompare(b.name));

  const assetsWithUrls = assets.map((asset, index) => ({
    id: slugify(`${asset.filename}-${index + 1}`),
    filename: asset.filename,
    type: asset.type || 'asset',
    size: asset.size ?? null,
    sourceType: asset.source_type || null,
    url: `/api/jobs/${encodeURIComponent(jobId)}/assets/${encodeURIComponent(asset.filename)}`,
    sourceHeaderFilenames: asset.source_header_filenames || [],
  }));

  const analysis = {
    meta: {
      jobId,
      sourceFileName,
      appTitle: app.qTitle || path.basename(sourceFileName, path.extname(sourceFileName)),
      analyzedAt: new Date().toISOString(),
      includeTables,
      downloadUrl,
    },
    overview: {
      counts: {
        sheets: sheets.length,
        measures: measures.length,
        dimensions: dimensions.length,
        variables: variables.length,
        masterObjects: masterObjects.length,
        assets: assets.length,
        dataSources: (dataSources.lib_references || []).length,
        tables: tableSummary.totals.tableCount,
      },
      discoveries: buildDiscoveries({
        app,
        sheets,
        measures,
        dimensions,
        variables,
        assets,
        dataSources,
        tableSummary,
        flags,
      }),
      topFlags: flags.slice(0, 6),
      largestTables: tableSummary.tables.slice(0, 6).map((table) => ({
        id: table.id,
        name: table.name,
        status: table.status,
        rows: table.exportedRowCount,
        columns: table.exportedColumns,
      })),
      confidenceSnapshot: tableSummary.totals,
    },
    app: {
      title: app.qTitle || '',
      description: app.description || '',
      lastReload: app.qLastReloadTime || null,
      version: app.qSavedInProductVersion || null,
      published: Boolean(app.published),
      encrypted: Boolean(app.encrypted),
      hasSectionAccess: Boolean(app.hassectionaccess),
      theme: app.app_properties?.qRoot?.qProperty?.theme || null,
      createdDate: app.createdDate || null,
      modifiedDate: app.modifiedDate || null,
      usage: app.qUsage || null,
    },
    structure: {
      sheets: buildSheetStructure(sheets),
      visualizations: visualizationMetrics.visualizationCounts,
      objects: visualizationMetrics.objectTypeCounts,
      masterItems: {
        measures: masterItems.measures,
        dimensions: masterItems.dimensions,
        objects: masterItems.objects,
      },
      sharedFields,
      majorFields,
      majorTables: tableSummary.tables.slice(0, 8).map((table) => ({
        id: table.id,
        name: table.name,
        rows: table.exportedRowCount,
        columns: table.exportedColumns,
        status: table.status,
      })),
    },
    design: {
      masterItemAdoption: {
        measures: visualizationMetrics.masterItemUsage.measures,
        dimensions: visualizationMetrics.masterItemUsage.dimensions,
      },
      expressionTotals: {
        total: visualizationMetrics.expressions.length,
        unique: uniqueExpressions.length,
        duplicateGroups: duplicateExpressions.length,
      },
      expressionHistogram: makeExpressionHistogram(uniqueExpressions),
      longestExpressions,
      duplicateExpressions: duplicateExpressions.slice(0, 12),
      possibleUnusedFields,
      caveats: [
        'Possible unused fields only reflect front-end usage analysis.',
      ],
    },
    fieldUsage,
    tables: {
      skipped: !includeTables,
      summary: tableSummary.totals,
      tables: tableSummary.tables,
    },
    script: {
      text: scriptText,
      tabs: parseScriptTabs(scriptText),
      sources: dataSources.lib_references || [],
      inlineTables: parseInlineTables(scriptText),
      variableReferences: [...visualizationMetrics.variableRefs].sort((a, b) => a.localeCompare(b)),
    },
    assets: {
      count: assetsWithUrls.length,
      items: assetsWithUrls,
    },
    masterItems,
    masterItemUsage: masterItemUsageDetails,
    flags,
  };

  return analysis;
}

module.exports = {
  buildAnalysisPayload,
};
