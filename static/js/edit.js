let currentProjectId = null;
let originalData = {}; // Store original data for change tracking

// Load project list on page load
document.addEventListener('DOMContentLoaded', async () => {
    try {
        const response = await fetch('/api/projects');
        const projects = await response.json();
        
        console.log('Projects received:', projects);  // ← DEBUG
        
        const selector = document.getElementById('projectSelector');
        
        projects.forEach(proj => {
            // Fixed: Use camelCase properties that match the Python response
            console.log('Adding project:', proj.projectId, proj.customerName);  // ← DEBUG
            
            const option = document.createElement('option');
            option.value = proj.projectId;  // Changed from proj.project_id
            option.textContent = `${proj.customerName} - ${proj.projectName}`;  // Changed from snake_case
            selector.appendChild(option);
        });
        
        console.log('Dropdown populated, total options:', selector.options.length);  // ← DEBUG
    } catch (error) {
        console.error('Error loading projects:', error);
    }
});

async function loadProjectData() {
  const projectId = document.getElementById('projectSelector').value;
  console.log('Selected project ID:', projectId);
  if (!projectId) {
    alert('Please select a project');
    return;
  }
  
  currentProjectId = projectId;
  
  // Fetch complete project data
  const response = await fetch(`/api/project/${projectId}`);
  const data = await response.json();
  
  // Store original data
  originalData = JSON.parse(JSON.stringify(data));
  
  // Populate forms
  populateProjectDetails(data.project);
  populateCostTable(data.costanalysis);
  populateHoursTable(data.hoursanalysis);
  populateTimelineTable(data.timeline);
  populateRateTable(data.ratecalculation);
  
  // Show edit form
  document.getElementById('editForm').classList.remove('d-none');
}

function populateProjectDetails(project) {
    document.getElementById('edit-customerName').value = project.customer_name;
    document.getElementById('edit-projectName').value = project.project_name;
    document.getElementById('edit-startDate').value = project.project_start_date;
}

function populateCostTable(costData) {
    console.log('Populating cost table:', costData);
    const tbody = document.querySelector('#costTable tbody');
    tbody.innerHTML = '';
    
    costData.forEach(row => {
        console.log('Cost row:', row);
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td><input type="text" class="form-control" value="${row.payment_milestone || ''}"></td>
            <td><input type="number" class="form-control" value="${row.weight || ''}"></td>
            <td><input type="number" class="form-control" value="${row.cost || ''}"></td>
            <td><button class="btn btn-sm btn-danger" onclick="this.closest('tr').remove()">Delete</button></td>
        `;
        tbody.appendChild(tr);
    });
}

function populateHoursTable(hoursData) {
    console.log('Populating hours table (pivoting long to wide):', hoursData);
    const tbody = document.querySelector('#hoursTable tbody');
    tbody.innerHTML = '';
    
    // Step 1: Group by module and sum hours by phase
    const moduleMap = {};
    
    hoursData.forEach(row => {
        const moduleKey = row.module_code || row.module_name;
        
        if (!moduleMap[moduleKey]) {
            moduleMap[moduleKey] = {
                module: moduleKey,
                weight: row.module_weight || '',
                'P+M': 0,
                'Plan': 0,
                'A+C': 0,
                'Testing': 0,
                'Deploy': 0,
                'Post Go Live': 0
            };
        }
        
        // Map phase names to column headers
        const phaseMap = {
            'PM': 'P+M',
            'Project Management': 'P+M',
            'PLAN': 'Plan',
            'Planning': 'Plan',
            'AC': 'A+C',
            'Analysis & Configuration': 'A+C',
            'TESTING': 'Testing',
            'Testing': 'Testing',
            'DEPLOY': 'Deploy',
            'Deployment': 'Deploy',
            'POST_GO_LIVE': 'Post Go Live',
            'Post Go-Live Support': 'Post Go Live'
        };
        
        const columnName = phaseMap[row.phase_code] || phaseMap[row.phase_name];
        if (columnName) {
            moduleMap[moduleKey][columnName] += parseFloat(row.planned_hours) || 0;
        }
        
        // Keep the last weight value (they should all be the same per module)
        if (row.module_weight != null) {
            moduleMap[moduleKey].weight = row.module_weight;
        }
    });
    
    // Step 2: Create table rows from pivoted data
    Object.values(moduleMap).forEach(moduleData => {
        console.log('Creating row for module:', moduleData);
        
        const totalHours = 
            moduleData['P+M'] + 
            moduleData['Plan'] + 
            moduleData['A+C'] + 
            moduleData['Testing'] + 
            moduleData['Deploy'] + 
            moduleData['Post Go Live'];
        
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td><input type="text" class="form-control" value="${moduleData.module || ''}"></td>
            <td><input type="number" step="0.01" class="form-control" value="${moduleData.weight || ''}"></td>
            <td><input type="number" step="0.01" class="form-control" value="${moduleData['P+M'] || 0}"></td>
            <td><input type="number" step="0.01" class="form-control" value="${moduleData['Plan'] || 0}"></td>
            <td><input type="number" step="0.01" class="form-control" value="${moduleData['A+C'] || 0}"></td>
            <td><input type="number" step="0.01" class="form-control" value="${moduleData['Testing'] || 0}"></td>
            <td><input type="number" step="0.01" class="form-control" value="${moduleData['Deploy'] || 0}"></td>
            <td><input type="number" step="0.01" class="form-control" value="${moduleData['Post Go Live'] || 0}"></td>
            <td><input type="number" step="0.01" class="form-control" value="${totalHours.toFixed(2)}" readonly></td>
            <td><button class="btn btn-sm btn-danger" onclick="this.closest('tr').remove()">Delete</button></td>
        `;
        tbody.appendChild(tr);
    });
    
    console.log('Hours table populated with', Object.keys(moduleMap).length, 'modules');
}

function populateTimelineTable(timelineData) {
    console.log('Populating timeline table:', timelineData);
    const tbody = document.querySelector('#timelineTable tbody');
    tbody.innerHTML = '';
    
    timelineData.forEach(row => {
        console.log('Timeline row:', row);
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td><input type="text" class="form-control" value="${row.phase_name || ''}"></td>
            <td><input type="number" class="form-control" value="${row.duration_weeks || ''}"></td>
            <td><button class="btn btn-sm btn-danger" onclick="this.closest('tr').remove()">Delete</button></td>
        `;
        tbody.appendChild(tr);
    });
}

function populateRateTable(rateData) {
    console.log('Populating rate table:', rateData);
    const tbody = document.querySelector('#rateTable tbody');
    tbody.innerHTML = '';
    
    rateData.forEach(row => {
        console.log('Rate row:', row);
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td><input type="text" class="form-control" value="${row.module_code || row.module_name || ''}"></td>
            <td><input type="number" class="form-control" value="${row.budgeted_hours || ''}"></td>
            <td><input type="number" class="form-control" value="${row.hourly_rate || ''}"></td>
            <td><input type="number" class="form-control" value="${row.total_cost || ''}" readonly></td>
            <td><button class="btn btn-sm btn-danger" onclick="this.closest('tr').remove()">Delete</button></td>
        `;
        tbody.appendChild(tr);
    });
}

async function saveAllChanges() {
  if (!confirm('Save all changes to this project?')) {
    return;
  }
  
  // Collect all changed data
  const changes = {
    projectId: currentProjectId,
    projectDetails: {
      customername: document.getElementById('edit-customerName').value,
      projectname: document.getElementById('edit-projectName').value,
      projectstartdate: document.getElementById('edit-startDate').value
    },
    costAnalysis: collectTableData('costTable'),
    hoursAnalysis: collectTableData('hoursTable'),
    timeline: collectTableData('timelineTable'),
    rateCalculation: collectTableData('rateTable')
  };
  
  // Send to backend
  try {
    const response = await fetch(`/api/update-project/${currentProjectId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(changes)
    });
    
    if (response.ok) {
      alert('Changes saved successfully!');
      window.location.href = '/dashboard';
    } else {
      alert('Error saving changes');
    }
  } catch (error) {
    alert('Error: ' + error.message);
  }
}

function collectTableData(tableId) {
  const rows = document.querySelectorAll(`#${tableId} tbody tr`);
  const data = [];
  
  rows.forEach(row => {
    const rowData = { id: row.dataset.id };
    row.querySelectorAll('td[contenteditable]').forEach(cell => {
      const field = cell.dataset.field;
      rowData[field] = cell.textContent.trim();
    });
    data.push(rowData);
  });
  
  return data;
}

function cancelEdit() {
  if (confirm('Discard all changes?')) {
    window.location.href = '/dashboard';
  }
}
