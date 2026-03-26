const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');

const express = require('express');
const multer = require('multer');
const dotenv = require('dotenv');

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
const pythonBin = process.env.PYTHON_BIN || 'python3';
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

async function runExtractor(inputFile, extractDir, zipPath, includeTables) {
  const args = [extractorScript, inputFile, '--output-dir', extractDir, '--zip', zipPath];
  if (!includeTables) {
    args.push('--skip-tables');
  }

  return execFileAsync(
    pythonBin,
    args,
    {
      cwd: rootDir,
      maxBuffer: 20 * 1024 * 1024,
      env: {
        ...process.env,
        HOME: process.env.HOME,
      },
    }
  );
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

function createJobPaths(jobId, originalName) {
  const baseName = sanitizeBaseName(originalName);
  const jobDir = path.join(jobsRoot, jobId);
  const uploadDir = path.join(jobDir, 'upload');
  const extractDir = path.join(jobDir, 'extract');
  const resultDir = path.join(jobDir, 'result');
  const uploadedFile = path.join(uploadDir, `${baseName}.qvf`);
  const zipPath = path.join(resultDir, `${baseName}-extract.zip`);

  return {
    baseName,
    jobDir,
    uploadDir,
    extractDir,
    resultDir,
    uploadedFile,
    zipPath,
  };
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

app.use(express.static(publicDir));

app.get('/healthz', async (_req, res) => {
  res.json({
    ok: true,
    busy: Boolean(activeJobId),
    port,
    pythonBin,
    extractorScript,
  });
});

app.post('/api/extract', uploadMiddleware, async (req, res) => {
  if (activeJobId) {
    if (req.file?.path) {
      await safeUnlink(req.file.path);
    }
    res.status(423).json({ error: 'The service is already processing another file. Please try again shortly.' });
    return;
  }

  if (!req.file) {
    res.status(400).json({ error: 'Please upload a QVF file.' });
    return;
  }

  if (!req.file.originalname.toLowerCase().endsWith('.qvf')) {
    await safeUnlink(req.file.path);
    res.status(400).json({ error: 'Only .qvf files are supported.' });
    return;
  }

  const jobId = crypto.randomUUID();
  const includeTables = String(req.body?.includeTables ?? 'true').toLowerCase() !== 'false';
  const paths = createJobPaths(jobId, req.file.originalname);
  let extractorStdout = '';
  let extractorStderr = '';
  let statusCode = 500;
  let errorMessage = 'The file could not be processed.';
  let completed = false;

  activeJobId = jobId;

  try {
    await prepareJobDirectories(paths);
    await fsp.rename(req.file.path, paths.uploadedFile);

    const result = await runExtractor(paths.uploadedFile, paths.extractDir, paths.zipPath, includeTables);
    extractorStdout = (result.stdout || '').trim();
    extractorStderr = (result.stderr || '').trim();

    await fsp.access(paths.zipPath, fs.constants.R_OK);
    await sendDownload(res, paths.zipPath, path.basename(paths.zipPath));
    completed = true;
  } catch (error) {
    extractorStdout = (error.stdout || extractorStdout || '').trim();
    extractorStderr = (error.stderr || extractorStderr || '').trim();
    errorMessage = extractorStderr || error.message || errorMessage;

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

    if (!res.headersSent) {
      res.status(statusCode).json({ error: errorMessage, jobId });
    }
  } finally {
    activeJobId = null;
    if (!completed && !keepFailedJobs) {
      await safeRm(paths.jobDir);
    }
    if (completed) {
      await safeRm(paths.jobDir);
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
