const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');

const express = require('express');
const multer = require('multer');
const archiver = require('archiver');
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
const engineUrl = process.env.ENGINE_URL || '127.0.0.1:9076';
const qlikBin = path.resolve(rootDir, process.env.QLIK_BIN || './bin/qlik');
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
  return path
    .basename(filename, path.extname(filename))
    .replace(/[^a-zA-Z0-9-_]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 80) || 'app';
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

async function runQlik(args) {
  const { stdout, stderr } = await execFileAsync(qlikBin, args, {
    cwd: rootDir,
    maxBuffer: 20 * 1024 * 1024,
    env: {
      ...process.env,
      HOME: process.env.HOME,
    },
  });

  return {
    stdout: stdout.trim(),
    stderr: stderr.trim(),
  };
}

function explainQlikError(error, args) {
  const stderr = (error.stderr || '').trim();
  const command = args.join(' ');

  if (command.includes('app import') && stderr.includes('500 Internal Error')) {
    return 'The Qlik engine returned a generic import error. This usually means the engine container cannot read the uploaded QVF path from the host runtime folder.';
  }

  return stderr || error.message;
}

function buildEngineArgs(commandArgs) {
  return [...commandArgs, '--server', engineUrl, '--server-type', 'engine'];
}

function parseImportedAppId(output) {
  if (!output) {
    throw new Error('Qlik CLI did not return an app identifier.');
  }

  const lastToken = output.split(/\s+/).pop();
  if (!lastToken) {
    throw new Error('Unable to parse the imported app identifier.');
  }

  return lastToken.trim();
}

async function zipDirectory(sourceDir, targetZipPath) {
  await fsp.mkdir(path.dirname(targetZipPath), { recursive: true });

  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(targetZipPath);
    const archive = archiver('zip', { zlib: { level: 9 } });

    output.on('close', resolve);
    output.on('error', reject);
    archive.on('error', reject);

    archive.pipe(output);
    archive.directory(sourceDir, false);
    archive.finalize();
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

function createJobPaths(jobId, originalName) {
  const baseName = sanitizeBaseName(originalName);
  const jobDir = path.join(jobsRoot, jobId);
  const uploadDir = path.join(jobDir, 'upload');
  const extractDir = path.join(jobDir, 'extract');
  const resultDir = path.join(jobDir, 'result');
  const uploadedFile = path.join(uploadDir, `${baseName}.qvf`);
  const zipPath = path.join(resultDir, `${baseName}-metadata.zip`);

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

async function assertReadableFile(filePath) {
  await fsp.access(filePath, fs.constants.R_OK);
}

async function removeImportedApp(appId) {
  if (!appId) return;
  try {
    await runQlik(buildEngineArgs(['app', 'rm', appId]));
  } catch (error) {
    console.error(`Failed to remove imported app ${appId}:`, error.stderr || error.message);
  }
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
    engineUrl,
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

  if (path.extname(req.file.originalname).toLowerCase() !== '.qvf') {
    await safeUnlink(req.file.path);
    res.status(400).json({ error: 'Only .qvf files are supported.' });
    return;
  }

  const jobId = crypto.randomUUID();
  const paths = createJobPaths(jobId, req.file.originalname);
  const appName = `${paths.baseName}-${jobId.slice(0, 8)}`;
  let appId = null;
  let lastQlikArgs = null;
  let failed = false;

  activeJobId = jobId;

  try {
    await prepareJobDirectories(paths);
    await fsp.rename(req.file.path, paths.uploadedFile);
    await assertReadableFile(paths.uploadedFile);

    lastQlikArgs = buildEngineArgs(['app', 'import', '--quiet', '--name', appName, '--file', paths.uploadedFile]);
    const importResult = await runQlik(lastQlikArgs);
    appId = parseImportedAppId(importResult.stdout);

    await safeUnlink(paths.uploadedFile);

    lastQlikArgs = buildEngineArgs(['app', 'unbuild', '-a', appId, '--dir', paths.extractDir]);
    await runQlik(lastQlikArgs);

    await zipDirectory(paths.extractDir, paths.zipPath);
    await sendDownload(res, paths.zipPath, path.basename(paths.zipPath));
  } catch (error) {
    failed = true;
    const details = explainQlikError(error, lastQlikArgs || []);

    if (keepFailedJobs) {
      await writeFailureReport(paths.jobDir, {
        jobId,
        uploadedFile: paths.uploadedFile,
        extractDir: paths.extractDir,
        zipPath: paths.zipPath,
        appId,
        qlikCommand: (lastQlikArgs || []).join(' '),
        details,
      }).catch((reportError) => {
        console.error('Failed to write failure report:', reportError);
      });
    }

    console.error(error);
    if (!res.headersSent) {
      res.status(500).json({
        error: 'The QVF file could not be processed.',
        details,
        jobId: keepFailedJobs ? jobId : undefined,
      });
    }
  } finally {
    await removeImportedApp(appId);
    await safeUnlink(req.file?.path);
    if (!(failed && keepFailedJobs)) {
      await safeRm(paths.jobDir);
    }
    activeJobId = null;
  }
});

app.get('*', (_req, res) => {
  res.sendFile(path.join(publicDir, 'index.html'));
});

async function start() {
  await ensureRuntimeDirectories();
  await cleanupExpiredJobs();

  const cleanupTimer = setInterval(() => {
    cleanupExpiredJobs().catch((error) => {
      console.error('Failed to clean up expired jobs:', error);
    });
  }, 10 * 60 * 1000);

  cleanupTimer.unref?.();

  app.listen(port, host, () => {
    console.log(`Server listening on http://${host}:${port}`);
  });
}

start().catch((error) => {
  console.error('Failed to prepare runtime directories:', error);
  process.exit(1);
});
