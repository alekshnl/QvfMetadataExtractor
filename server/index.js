const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');

const express = require('express');
const multer = require('multer');
const dotenv = require('dotenv');

const { buildAnalysisPayload } = require('./analysis');

dotenv.config();

const execFileAsync = promisify(execFile);
const app = express();

const rootDir = path.resolve(__dirname, '..');
const publicDir = path.join(rootDir, 'public');
const tmpRoot = path.resolve(rootDir, process.env.TMP_ROOT || './runtime/tmp');
const incomingDir = path.join(tmpRoot, 'incoming');
const jobsRoot = path.join(tmpRoot, 'jobs');
const host = process.env.HOST || '0.0.0.0';
const port = Number(process.env.PORT || 5165);
const defaultPythonBin = fs.existsSync(path.join(rootDir, '.venv', 'bin', 'python'))
  ? path.join(rootDir, '.venv', 'bin', 'python')
  : 'python3';
const pythonBin = process.env.PYTHON_BIN || defaultPythonBin;
const extractorScript = path.resolve(rootDir, process.env.EXTRACTOR_SCRIPT || './scripts/extract_qvf.py');
const maxUploadMb = Number(process.env.MAX_UPLOAD_MB || 512);
const jobTtlMinutes = Number(process.env.JOB_TTL_MINUTES || 30);
const keepFailedJobs = String(process.env.KEEP_FAILED_JOBS || 'false').toLowerCase() === 'true';
const uploadLimitBytes = maxUploadMb * 1024 * 1024;

let activeJobId = null;

const upload = multer({
  dest: incomingDir,
  limits: {
    fileSize: uploadLimitBytes,
    files: 1,
  },
});

async function ensureRuntimeDirectories() {
  await Promise.all([
    fsp.mkdir(incomingDir, { recursive: true }),
    fsp.mkdir(jobsRoot, { recursive: true }),
  ]);
}

function sanitizeBaseName(filename) {
  return (
    path
      .basename(filename, path.extname(filename))
      .replace(/[^a-zA-Z0-9-_]+/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-|-$/g, '')
      .slice(0, 80) || 'app'
  );
}

function isSafeJobId(jobId) {
  return /^[a-f0-9-]{36}$/i.test(jobId);
}

async function pathExists(targetPath) {
  try {
    await fsp.access(targetPath, fs.constants.R_OK);
    return true;
  } catch {
    return false;
  }
}

async function safeUnlink(filePath) {
  if (!filePath) return;
  try {
    await fsp.unlink(filePath);
  } catch (error) {
    if (error.code !== 'ENOENT') {
      throw error;
    }
  }
}

async function safeRm(targetPath) {
  if (!targetPath) return;
  try {
    await fsp.rm(targetPath, { recursive: true, force: true });
  } catch (error) {
    if (error.code !== 'ENOENT') {
      throw error;
    }
  }
}

async function cleanupExpiredJobs() {
  await ensureRuntimeDirectories();
  const threshold = Date.now() - jobTtlMinutes * 60 * 1000;
  const entries = await fsp.readdir(jobsRoot, { withFileTypes: true }).catch(() => []);

  await Promise.all(
    entries
      .filter((entry) => entry.isDirectory())
      .map(async (entry) => {
        const target = path.join(jobsRoot, entry.name);
        const stats = await fsp.stat(target).catch(() => null);
        if (stats && stats.mtimeMs < threshold) {
          await safeRm(target);
        }
      })
  );
}

async function writeFailureReport(jobDir, details) {
  const reportPath = path.join(jobDir, 'failure-report.txt');
  const lines = Object.entries(details)
    .filter(([, value]) => value !== null && value !== undefined && value !== '')
    .map(([key, value]) => `${key}: ${value}`);

  await fsp.writeFile(reportPath, `${lines.join('\n')}\n`, 'utf8');
}

async function runExtractor(inputFile, extractDir, zipPath, includeTables, createZip = true) {
  const args = [extractorScript, inputFile, '--output-dir', extractDir];
  if (createZip) {
    args.push('--zip', zipPath);
  }
  if (!includeTables) {
    args.push('--skip-tables');
  }

  return execFileAsync(pythonBin, args, {
    cwd: rootDir,
    maxBuffer: 20 * 1024 * 1024,
    env: {
      ...process.env,
      HOME: process.env.HOME,
    },
  });
}

async function buildZipFromDirectory(sourceDir, targetZip) {
  const zipScript = [
    'from pathlib import Path',
    'from zipfile import ZIP_DEFLATED, ZipFile',
    'source_dir = Path(__import__("sys").argv[1]).resolve()',
    'target_zip = Path(__import__("sys").argv[2]).resolve()',
    'target_zip.parent.mkdir(parents=True, exist_ok=True)',
    'if target_zip.exists():',
    '    target_zip.unlink()',
    'with ZipFile(target_zip, "w", compression=ZIP_DEFLATED) as archive:',
    '    for file_path in sorted(source_dir.rglob("*")):',
    '        if file_path.is_dir() or file_path == target_zip:',
    '            continue',
    '        archive.write(file_path, file_path.relative_to(source_dir))',
  ].join('\n');

  await execFileAsync(pythonBin, ['-c', zipScript, sourceDir, targetZip], {
    cwd: rootDir,
    maxBuffer: 20 * 1024 * 1024,
    env: {
      ...process.env,
      HOME: process.env.HOME,
    },
  });
}

async function sendDownload(res, absolutePath, downloadName) {
  return new Promise((resolve, reject) => {
    res.download(absolutePath, downloadName, (error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

function createJobPaths(jobId, originalName = 'app.qvf') {
  const baseName = sanitizeBaseName(originalName);
  const jobDir = path.join(jobsRoot, jobId);
  const uploadDir = path.join(jobDir, 'upload');
  const extractDir = path.join(jobDir, 'extract');
  const resultDir = path.join(jobDir, 'result');
  const uploadedFile = path.join(uploadDir, `${baseName}.qvf`);
  const zipPath = path.join(resultDir, `${baseName}-extract.zip`);
  const analysisPath = path.join(resultDir, 'analysis.json');

  return {
    baseName,
    jobDir,
    uploadDir,
    extractDir,
    resultDir,
    uploadedFile,
    zipPath,
    analysisPath,
  };
}

function createPathsForExistingJob(jobId) {
  const jobDir = path.join(jobsRoot, jobId);
  const uploadDir = path.join(jobDir, 'upload');
  const extractDir = path.join(jobDir, 'extract');
  const resultDir = path.join(jobDir, 'result');

  return {
    jobDir,
    uploadDir,
    extractDir,
    resultDir,
    analysisPath: path.join(resultDir, 'analysis.json'),
  };
}

async function resolveExistingJobPaths(jobId) {
  const partialPaths = createPathsForExistingJob(jobId);
  const uploadEntries = await fsp.readdir(partialPaths.uploadDir).catch(() => []);
  const uploadedName = uploadEntries.find((entry) => entry.toLowerCase().endsWith('.qvf'));
  if (!uploadedName) {
    return null;
  }

  return createJobPaths(jobId, uploadedName);
}

async function prepareJobDirectories(paths) {
  await Promise.all([
    fsp.mkdir(paths.uploadDir, { recursive: true }),
    fsp.mkdir(paths.extractDir, { recursive: true }),
    fsp.mkdir(paths.resultDir, { recursive: true }),
  ]);
}

function uploadMiddleware(req, res, next) {
  upload.single('qvf')(req, res, (error) => {
    if (!error) {
      next();
      return;
    }

    if (error instanceof multer.MulterError && error.code === 'LIMIT_FILE_SIZE') {
      res.status(400).json({ error: `The file is larger than the ${maxUploadMb} MB limit.` });
      return;
    }

    res.status(400).json({ error: error.message || 'Unable to process the uploaded file.' });
  });
}

async function runJobFromUpload(req, { mode }) {
  const jobId = crypto.randomUUID();
  const includeTables = String(req.body?.includeTables ?? 'true').toLowerCase() !== 'false';
  const paths = createJobPaths(jobId, req.file.originalname);
  const originalName = req.file.originalname;
  let extractorStdout = '';
  let extractorStderr = '';

  await prepareJobDirectories(paths);
  await fsp.rename(req.file.path, paths.uploadedFile);

  try {
    const createZip = mode !== 'analyze';
    const result = await runExtractor(paths.uploadedFile, paths.extractDir, paths.zipPath, includeTables, createZip);
    extractorStdout = (result.stdout || '').trim();
    extractorStderr = (result.stderr || '').trim();
    if (createZip) {
      await fsp.access(paths.zipPath, fs.constants.R_OK);
    }

    let analysisEnvelope = null;
    if (mode === 'analyze') {
      const downloadUrl = `/api/jobs/${encodeURIComponent(jobId)}/download`;
      const analysis = await buildAnalysisPayload({
        jobId,
        extractDir: paths.extractDir,
        sourceFileName: originalName,
        includeTables,
        downloadUrl,
      });

      analysisEnvelope = {
        jobId,
        appLabel: analysis.app.title || analysis.meta.appTitle,
        downloadUrl,
        analysis,
      };

      await fsp.writeFile(paths.analysisPath, JSON.stringify(analysisEnvelope, null, 2), 'utf8');
    }

    return {
      jobId,
      includeTables,
      originalName,
      paths,
      extractorStdout,
      extractorStderr,
      analysisEnvelope,
    };
  } catch (error) {
    extractorStdout = (error.stdout || extractorStdout || '').trim();
    extractorStderr = (error.stderr || extractorStderr || '').trim();

    await writeFailureReport(paths.jobDir, {
      jobId,
      uploadedFile: paths.uploadedFile,
      extractDir: paths.extractDir,
      zipPath: paths.zipPath,
      includeTables,
      pythonBin,
      extractorScript,
      stdout: extractorStdout,
      stderr: extractorStderr,
      error: error.message,
    }).catch(() => {});

    error.extractorStdout = extractorStdout;
    error.extractorStderr = extractorStderr;
    error.paths = paths;
    throw error;
  }
}

async function readAnalysisEnvelope(jobId) {
  if (!isSafeJobId(jobId)) {
    return null;
  }

  const paths = createPathsForExistingJob(jobId);
  if (!(await pathExists(paths.analysisPath))) {
    return null;
  }

  try {
    const text = await fsp.readFile(paths.analysisPath, 'utf8');
    const envelope = JSON.parse(text);
    return await refreshLegacyAnalysisIfNeeded(envelope, paths, jobId);
  } catch {
    return null;
  }
}

async function refreshLegacyAnalysisIfNeeded(envelope, paths, jobId) {
  const analysis = envelope?.analysis;
  if (!analysis || typeof analysis !== 'object') {
    return envelope;
  }

  const currentMeasures = analysis.masterItems?.measures;
  const hasMasterDefinitions =
    Array.isArray(currentMeasures) && currentMeasures.some((measure) => typeof measure?.expression === 'string' && measure.expression.trim());
  const hasFieldUsage =
    Number(analysis.fieldUsage?.version || 0) >= 3 && Array.isArray(analysis.fieldUsage?.rows) && analysis.fieldUsage.rows.length > 0;
  const hasMasterItemUsage =
    Number(analysis.masterItemUsage?.version || 0) >= 3 &&
    Array.isArray(analysis.masterItemUsage?.dimensions) &&
    Array.isArray(analysis.masterItemUsage?.measures) &&
    Array.isArray(analysis.masterItemUsage?.objects);

  if (hasMasterDefinitions && hasFieldUsage && hasMasterItemUsage) {
    return envelope;
  }

  try {
    const downloadUrl = envelope.downloadUrl || `/api/jobs/${encodeURIComponent(jobId)}/download`;
    const refreshedAnalysis = await buildAnalysisPayload({
      jobId,
      extractDir: paths.extractDir,
      sourceFileName: analysis.meta?.sourceFileName || 'app.qvf',
      includeTables: Boolean(analysis.meta?.includeTables),
      downloadUrl,
    });

    envelope.analysis = refreshedAnalysis;
    envelope.downloadUrl = downloadUrl;
    envelope.appLabel = refreshedAnalysis.app?.title || refreshedAnalysis.meta?.appTitle || envelope.appLabel || 'Untitled analysis';
    await fsp.writeFile(paths.analysisPath, JSON.stringify(envelope, null, 2), 'utf8');
  } catch {
    return envelope;
  }

  return envelope;
}

function toJobListItem(envelope) {
  const analysis = envelope?.analysis || {};
  return {
    jobId: envelope.jobId,
    appLabel: envelope.appLabel || analysis.app?.title || analysis.meta?.appTitle || 'Untitled analysis',
    sourceFileName: analysis.meta?.sourceFileName || null,
    analyzedAt: analysis.meta?.analyzedAt || null,
    includeTables: Boolean(analysis.meta?.includeTables),
    downloadUrl: envelope.downloadUrl || null,
    counts: analysis.overview?.counts || {},
  };
}

async function listAnalysisJobs() {
  const entries = await fsp.readdir(jobsRoot, { withFileTypes: true }).catch(() => []);
  const jobs = [];

  for (const entry of entries) {
    if (!entry.isDirectory() || !isSafeJobId(entry.name)) {
      continue;
    }
    const envelope = await readAnalysisEnvelope(entry.name);
    if (!envelope) {
      continue;
    }
    jobs.push(toJobListItem(envelope));
  }

  jobs.sort((a, b) => String(b.analyzedAt || '').localeCompare(String(a.analyzedAt || '')));
  return jobs;
}

function jsonError(res, status, error, extra = {}) {
  res.status(status).json({ error, ...extra });
}

app.use(
  express.static(publicDir, {
    setHeaders(res, filePath) {
      if (/\.(html|js|css)$/i.test(filePath)) {
        res.setHeader('Cache-Control', 'no-store');
      }
    },
  })
);

app.get('/healthz', async (_req, res) => {
  res.json({
    ok: true,
    busy: Boolean(activeJobId),
    port,
    pythonBin,
    extractorScript,
  });
});

app.get('/api/jobs', async (_req, res) => {
  const jobs = await listAnalysisJobs();
  res.json({ jobs });
});

app.get('/api/jobs/:jobId/analysis', async (req, res) => {
  const { jobId } = req.params;
  const analysisEnvelope = await readAnalysisEnvelope(jobId);
  if (!analysisEnvelope) {
    jsonError(res, 404, 'Analysis job not found or expired.');
    return;
  }

  res.json(analysisEnvelope);
});

app.get('/api/jobs/:jobId/download', async (req, res) => {
  const { jobId } = req.params;
  if (!isSafeJobId(jobId)) {
    jsonError(res, 404, 'Analysis job not found or expired.');
    return;
  }

  const paths = await resolveExistingJobPaths(jobId);
  if (!paths) {
    jsonError(res, 404, 'ZIP result not found or expired.');
    return;
  }

  try {
    if (!(await pathExists(paths.zipPath))) {
      await buildZipFromDirectory(paths.extractDir, paths.zipPath);
    }
    await sendDownload(res, paths.zipPath, path.basename(paths.zipPath));
  } catch (error) {
    if (!res.headersSent) {
      jsonError(res, 500, error.message || 'Unable to download the ZIP result.');
    }
  }
});

app.get('/api/jobs/:jobId/assets/:filename', async (req, res) => {
  const { jobId } = req.params;
  if (!isSafeJobId(jobId)) {
    jsonError(res, 404, 'Asset not found.');
    return;
  }

  const filename = path.basename(req.params.filename || '');
  if (!filename) {
    jsonError(res, 404, 'Asset not found.');
    return;
  }

  const assetPath = path.join(jobsRoot, jobId, 'extract', 'assets', filename);
  if (!(await pathExists(assetPath))) {
    jsonError(res, 404, 'Asset not found.');
    return;
  }

  res.sendFile(assetPath);
});

app.delete('/api/jobs/:jobId', async (req, res) => {
  const { jobId } = req.params;
  if (!isSafeJobId(jobId)) {
    jsonError(res, 404, 'Analysis job not found or expired.');
    return;
  }

  if (activeJobId === jobId) {
    jsonError(res, 409, 'This analysis is currently being processed and cannot be removed yet.');
    return;
  }

  const jobDir = path.join(jobsRoot, jobId);
  if (!(await pathExists(jobDir))) {
    jsonError(res, 404, 'Analysis job not found or expired.');
    return;
  }

  await safeRm(jobDir);
  res.json({ ok: true, jobId });
});

app.post('/api/analyze', uploadMiddleware, async (req, res) => {
  if (activeJobId) {
    if (req.file?.path) {
      await safeUnlink(req.file.path);
    }
    jsonError(res, 423, 'The service is already processing another file. Please try again shortly.');
    return;
  }

  if (!req.file) {
    jsonError(res, 400, 'Please upload a QVF file.');
    return;
  }

  if (!req.file.originalname.toLowerCase().endsWith('.qvf')) {
    await safeUnlink(req.file.path);
    jsonError(res, 400, 'Only .qvf files are supported.');
    return;
  }

  activeJobId = crypto.randomUUID();

  try {
    const result = await runJobFromUpload(req, { mode: 'analyze' });
    activeJobId = null;
    res.json(result.analysisEnvelope);
  } catch (error) {
    activeJobId = null;
    if (!keepFailedJobs && error.paths?.jobDir) {
      await safeRm(error.paths.jobDir);
    }
    jsonError(res, 500, error.extractorStderr || error.message || 'The file could not be processed.');
  }
});

app.post('/api/extract', uploadMiddleware, async (req, res) => {
  if (activeJobId) {
    if (req.file?.path) {
      await safeUnlink(req.file.path);
    }
    jsonError(res, 423, 'The service is already processing another file. Please try again shortly.');
    return;
  }

  if (!req.file) {
    jsonError(res, 400, 'Please upload a QVF file.');
    return;
  }

  if (!req.file.originalname.toLowerCase().endsWith('.qvf')) {
    await safeUnlink(req.file.path);
    jsonError(res, 400, 'Only .qvf files are supported.');
    return;
  }

  activeJobId = crypto.randomUUID();

  try {
    const result = await runJobFromUpload(req, { mode: 'legacy-extract' });
    activeJobId = null;
    await sendDownload(res, result.paths.zipPath, path.basename(result.paths.zipPath));
  } catch (error) {
    activeJobId = null;
    if (!keepFailedJobs && error.paths?.jobDir) {
      await safeRm(error.paths.jobDir);
    }
    if (!res.headersSent) {
      jsonError(res, 500, error.extractorStderr || error.message || 'The file could not be processed.');
    }
  }
});

async function start() {
  await ensureRuntimeDirectories();
  await cleanupExpiredJobs();

  app.listen(port, host, () => {
    console.log(`Server listening on http://${host}:${port}`);
  });
}

start().catch((error) => {
  console.error(error);
  process.exit(1);
});
