const form = document.getElementById('upload-form');
const fileInput = document.getElementById('qvf-file');
const submitButton = document.getElementById('submit-button');
const statusElement = document.getElementById('status');
const errorElement = document.getElementById('error-message');

function setStatus(text, state) {
  statusElement.textContent = text;
  statusElement.className = `status ${state}`;
}

function setError(message) {
  if (!message) {
    errorElement.hidden = true;
    errorElement.textContent = '';
    return;
  }

  errorElement.hidden = false;
  errorElement.textContent = message;
}

async function triggerDownload(blob, filename) {
  const blobUrl = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = blobUrl;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(blobUrl), 1000);
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  setError('');

  const [file] = fileInput.files;
  if (!file) {
    setStatus('Ready for upload', 'idle');
    setError('Select a .qvf file first.');
    return;
  }

  if (!file.name.toLowerCase().endsWith('.qvf')) {
    setStatus('Ready for upload', 'idle');
    setError('Only .qvf files are supported.');
    return;
  }

  submitButton.disabled = true;

  try {
    setStatus('Uploading file', 'busy');
    const formData = new FormData();
    formData.append('qvf', file);

    const response = await fetch('/api/extract', {
      method: 'POST',
      body: formData,
    });

    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      const detail = payload.jobId ? ` Job ID: ${payload.jobId}` : '';
      throw new Error((payload.error || 'The file could not be processed.') + detail);
    }

    setStatus('Preparing download', 'busy');
    const blob = await response.blob();
    const disposition = response.headers.get('Content-Disposition') || '';
    const match = disposition.match(/filename="?([^";]+)"?/i);
    const filename = match ? match[1] : 'metadata.zip';

    await triggerDownload(blob, filename);
    setStatus('Download started', 'success');
    form.reset();
  } catch (error) {
    setStatus('Processing failed', 'error');
    setError(error.message);
    return;
  } finally {
    submitButton.disabled = false;
  }

  window.setTimeout(() => {
    setStatus('Ready for upload', 'idle');
  }, 1800);
});
