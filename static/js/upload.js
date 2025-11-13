// ==================== GLOBAL VARIABLES ====================
const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');
let selectedFile = null;
let parsedStartDate = null;

// ==================== EVENT LISTENERS ====================

// Drag and drop event handlers
dropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropZone.classList.add('dragover');
});

dropZone.addEventListener('dragleave', () => {
    dropZone.classList.remove('dragover');
});

dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('dragover');
    
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

// ==================== FILE HANDLING ====================

function handleFileSelect(file) {
    // Validate file type - CSV, XLSX, or XLS
    const validExtensions = ['.csv', '.xlsx', '.xls'];
    const hasValidExtension = validExtensions.some(ext => file.name.toLowerCase().endsWith(ext));
    
    if (!hasValidExtension) {
        showAlert('Please select a CSV or Excel file (.csv, .xlsx, .xls)', 'danger');
        return;
    }
    
    // Validate file size (10MB max)
    if (file.size > 10 * 1024 * 1024) {
        showAlert('File size exceeds 10MB limit', 'danger');
        return;
    }
    
    // Store file blob in memory (not saved anywhere)
    selectedFile = file;
    
    // Display file info
    document.getElementById('fileName').textContent = file.name;
    document.getElementById('fileSize').textContent = formatFileSize(file.size);
    document.getElementById('fileInfo').style.display = 'block';
    
    // Automatically parse the file
    parseFile(file);
}

async function parseFile(file) {
    try {
        // Show loading overlay
        document.getElementById('loadingOverlay').style.display = 'flex';
        
        // Create FormData with file blob (kept in memory)
        const formData = new FormData();
        formData.append('file', file);
        
        // Send to backend for parsing
        const response = await fetch('/api/parse-file', {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (result.success) {
            // Extract start date from timeline columns
            // The timeline has columns like ['Project Start Date:', '2025-11-10 00:00:00']
            // The date is in the second column name
            if (result.columns.timeline && result.columns.timeline.length > 1) {
                // Find the column that contains a date pattern (YYYY-MM-DD or datetime)
                const dateColumn = result.columns.timeline.find(col => 
                    col.match(/\d{4}-\d{2}-\d{2}/)
                );
                
                if (dateColumn) {
                    // Extract just the date part (YYYY-MM-DD) from the column name
                    const dateMatch = dateColumn.match(/(\d{4}-\d{2}-\d{2})/);
                    if (dateMatch) {
                        parsedStartDate = dateMatch[1];
                        // Auto-populate the Project Start Date field
                        document.getElementById('projectStartDate').value = parsedStartDate;
                    }
                }
            }
            
            // Show project info form and review section
            document.getElementById('projectInfoSection').classList.remove('d-none');
            document.getElementById('reviewSection').classList.remove('d-none');
            
            // Populate tables with parsed data
            populateTable('cost', result.columns.costAnalysis, result.data.costAnalysis);
            populateTable('hours', result.columns.hoursAnalysis, result.data.hoursAnalysis);
            
            // For timeline, restructure to use "Phase" and "Duration (Weeks)" as columns
            const { columns: timelineColumns, data: timelineData } = restructureTimeline(
                result.columns.timeline, 
                result.data.timeline
            );
            
            populateTable('timeline', timelineColumns, timelineData);
            populateTable('rate', result.columns.rateCalculation, result.data.rateCalculation);
            
            showAlert('File parsed successfully!', 'success');
        } else {
            showAlert(result.error || 'Error parsing file', 'danger');
        }
        
    } catch (error) {
        showAlert('Error: ' + error.message, 'danger');
    } finally {
        document.getElementById('loadingOverlay').style.display = 'none';
    }
}

// Restructure timeline data to use "Phase" and "Duration (Weeks)" as columns
function restructureTimeline(columns, data) {
    if (!data || data.length === 0) {
        return { columns: ['Phase', 'Duration (Weeks)'], data: [] };
    }
    
    // The first row contains the actual column headers (Phase, Duration)
    const firstRow = data[0];
    
    // Find which column contains "Phase" and which contains "Duration"
    let phaseColumnName = null;
    let durationColumnName = null;
    
    // Check the first row values to find the header row
    for (const [key, value] of Object.entries(firstRow)) {
        if (value === 'Phase') {
            phaseColumnName = key;
        } else if (value && value.toString().includes('Duration')) {
            durationColumnName = key;
        }
    }
    
    if (!phaseColumnName || !durationColumnName) {
        // Fallback: use original columns and data
        return { columns: columns, data: data };
    }
    
    // Remove the first row (header row) and restructure remaining rows
    const restructuredData = data.slice(1).map(row => ({
        'Phase': row[phaseColumnName] || '',
        'Duration (Weeks)': row[durationColumnName] || ''
    }));
    
    return {
        columns: ['Phase', 'Duration (Weeks)'],
        data: restructuredData
    };
}

function clearFile() {
    selectedFile = null;
    parsedStartDate = null;
    fileInput.value = '';
    document.getElementById('fileInfo').style.display = 'none';
    document.getElementById('projectInfoSection').classList.add('d-none');
    document.getElementById('reviewSection').classList.add('d-none');
    // Clear project info fields
    document.getElementById('customerName').value = '';
    document.getElementById('projectName').value = '';
    document.getElementById('projectStartDate').value = '';
}

function resetUpload() {
    if (confirm('Are you sure you want to start over? All changes will be lost.')) {
        clearFile();
    }
}

// ==================== TABLE POPULATION ====================

function populateTable(tablePrefix, columns, data) {
    const headerRow = document.getElementById(`${tablePrefix}TableHeader`);
    const tbody = document.getElementById(`${tablePrefix}TableBody`);
    
    // Clear existing content
    headerRow.innerHTML = '';
    tbody.innerHTML = '';
    
    // Create header
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col;
        
        // Set wider width for specific columns
        if ((tablePrefix === 'hours' && col === 'HCM Modules') ||
            (tablePrefix === 'timeline' && col === 'Phase') ||
            (tablePrefix === 'cost' && col === 'Payment Milestone') ||
            (tablePrefix === 'rate' && col === 'Module')) {
            th.style.minWidth = '200px';
        }
        
        headerRow.appendChild(th);
    });
    
    // Create rows with editable inputs
    data.forEach((row, rowIndex) => {
        const tr = document.createElement('tr');
        
        columns.forEach(col => {
            const td = document.createElement('td');
            const input = document.createElement('input');
            
            let value = row[col] !== null && row[col] !== undefined ? row[col] : '';
            
            // Check if this is the "Start Date" row in Hours Analysis
            const isStartDateRow = (tablePrefix === 'hours' && row['HCM Modules'] === 'Start Date');
            const isDataColumn = col !== 'HCM Modules' && col !== 'Weight' && col !== 'Weeks/Hours';
            
            // Determine input type
            if (isStartDateRow && isDataColumn) {
                input.type = 'date';
                input.value = formatDateForInput(value);
            } else if (isNumberValue(value) && !isLabelColumn(tablePrefix, col)) {
                input.type = 'number';
                input.step = 'any';
                input.value = value;
            } else {
                input.type = 'text';
                input.value = value;
            }
            
            input.dataset.column = col;
            input.dataset.row = rowIndex;
            input.className = 'form-control form-control-sm';
            
            // Check if this column should be locked (read-only)
            const isLocked = isColumnLocked(tablePrefix, col, row);
            if (isLocked) {
                input.readOnly = true;
                input.style.backgroundColor = '#e9ecef';
                input.style.cursor = 'not-allowed';
                input.style.fontWeight = '500';
            }
            
            td.appendChild(input);
            tr.appendChild(td);
        });
        
        tbody.appendChild(tr);
    });
}

// ==================== TABLE HELPERS ====================

// Helper function to determine if a column should be locked
function isColumnLocked(tablePrefix, columnName, rowData) {
    switch(tablePrefix) {
        case 'cost':
            return columnName === 'Payment Milestone';
            
        case 'hours':
            return columnName === 'HCM Modules';
            
        case 'timeline':
            // Lock "Phase" column only
            return columnName === 'Phase';
            
        case 'rate':
            return columnName === 'Module';
            
        default:
            return false;
    }
}

// Helper function to check if column is a label/text column (not numeric)
function isLabelColumn(tablePrefix, columnName) {
    const labelColumns = {
        'cost': ['Payment Milestone'],
        'hours': ['HCM Modules'],
        'timeline': ['Phase'],
        'rate': ['Module']
    };
    
    return labelColumns[tablePrefix]?.includes(columnName);
}

// Helper function to check if a value is numeric
function isNumberValue(value) {
    return !isNaN(value) && value !== '' && value !== null;
}

// Helper function to format date for input field
function formatDateForInput(value) {
    if (!value || value === '') return '';
    
    try {
        // If already in YYYY-MM-DD format
        if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
            return value;
        }
        
        // Parse and format date
        const date = new Date(value);
        if (isNaN(date)) return '';
        
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        
        return `${year}-${month}-${day}`;
    } catch (e) {
        return '';
    }
}

// ==================== DATABASE SUBMISSION ====================

async function submitToDatabase() {
    try {
        // Validate project info
        const customerName = document.getElementById('customerName').value;
        const projectName = document.getElementById('projectName').value;
        const projectStartDate = document.getElementById('projectStartDate').value;
        
        if (!customerName || !projectName || !projectStartDate) {
            showAlert('Please fill in all project information fields', 'warning');
            return;
        }
        
        // Collect edited data from tables
        const projectData = {
            projectInfo: {
                customerName,
                projectName,
                projectStartDate
            },
            costAnalysis: getTableData('cost'),
            hoursAnalysis: getTableData('hours'),
            timeline: getTableData('timeline'),
            rateCalculation: getTableData('rate')
        };
        
        // Show loading
        document.getElementById('loadingOverlay').style.display = 'flex';
        
        // Submit to database
        const response = await fetch('/api/submit-project', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(projectData)
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('Project uploaded successfully!', 'success');
            setTimeout(() => {
                window.location.href = '/dashboard';
            }, 1500);
        } else {
            showAlert(result.error || 'Error submitting project', 'danger');
        }
        
    } catch (error) {
        showAlert('Error: ' + error.message, 'danger');
    } finally {
        document.getElementById('loadingOverlay').style.display = 'none';
    }
}

function getTableData(tablePrefix) {
    const tbody = document.getElementById(`${tablePrefix}TableBody`);
    const rows = tbody.querySelectorAll('tr');
    const data = [];
    
    rows.forEach(row => {
        const inputs = row.querySelectorAll('input');
        const rowData = {};
        
        inputs.forEach(input => {
            rowData[input.dataset.column] = input.value;
        });
        
        data.push(rowData);
    });
    
    return data;
}

// ==================== UTILITY FUNCTIONS ====================

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

function showAlert(message, type) {
    const alertContainer = document.getElementById('alertContainer');
    const alert = document.createElement('div');
    alert.className = `alert alert-${type} alert-dismissible fade show`;
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    alertContainer.appendChild(alert);
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        alert.remove();
    }, 5000);
}