// Drag & Drop functionality
const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');

dropZone.addEventListener('dragover', (e) => {
  e.preventDefault();
  dropZone.classList.add('border-primary', 'bg-light');
});

dropZone.addEventListener('dragleave', () => {
  dropZone.classList.remove('border-primary', 'bg-light');
});

dropZone.addEventListener('drop', (e) => {
  e.preventDefault();
  dropZone.classList.remove('border-primary', 'bg-light');
  
  const files = e.dataTransfer.files;
  if (files.length > 0) {
    handleFileSelect(files[0]);
  }
});

fileInput.addEventListener('change', (e) => {
  if (e.target.files.length > 0) {
    handleFileSelect(e.target.files[0]);
  }
});

function handleFileSelect(file) {
  // Validate file
  if (!file.name.endsWith('.xlsx') && !file.name.endsWith('.xls')) {
    alert('Please select an Excel file (.xlsx or .xls)');
    return;
  }
  
  if (file.size > 10 * 1024 * 1024) { // 10MB
    alert('File size exceeds 10MB limit');
    return;
  }
  
  // Display file info
  document.getElementById('fileName').textContent = file.name;
  document.getElementById('fileSize').textContent = formatFileSize(file.size);
  document.getElementById('fileInfo').classList.remove('d-none');
  document.getElementById('uploadBtn').disabled = false;
}

async function uploadFile() {
  // Validate project info
  const form = document.getElementById('projectInfoForm');
  if (!form.checkValidity()) {
    form.reportValidity();
    return;
  }
  
  const formData = new FormData(form);
  formData.append('file', fileInput.files[0]);
  
  // Show progress
  document.getElementById('progressCard').classList.remove('d-none');
  const progressBar = document.getElementById('progressBar');
  const statusMessages = document.getElementById('statusMessages');
  
  try {
    const xhr = new XMLHttpRequest();
    
    // Progress tracking
    xhr.upload.addEventListener('progress', (e) => {
      if (e.lengthComputable) {
        const percent = (e.loaded / e.total) * 100;
        progressBar.style.width = percent + '%';
        progressBar.textContent = Math.round(percent) + '%';
      }
    });
    
    // Response handling
    xhr.addEventListener('load', () => {
      if (xhr.status === 200) {
        const response = JSON.parse(xhr.responseText);
        displaySuccess(response);
      } else {
        displayError(xhr.responseText);
      }
    });
    
    xhr.open('POST', '/upload');
    xhr.send(formData);
    
    // Update status messages
    updateStatus('⏳ Uploading file...');
    
  } catch (error) {
    displayError(error.message);
  }
}

function updateStatus(message) {
  const statusMessages = document.getElementById('statusMessages');
  const li = document.createElement('li');
  li.textContent = message;
  statusMessages.appendChild(li);
}

function displaySuccess(response) {
  updateStatus('✓ File validated');
  updateStatus('✓ Extracted 4 tables');
  updateStatus('✓ Data inserted into database');
  updateStatus(`✓ Project ID: ${response.projectId}`);
  
  setTimeout(() => {
    window.location.href = '/dashboard';
  }, 2000);
}
