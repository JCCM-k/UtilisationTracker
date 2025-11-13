let currentProjectId = null;
let originalData = {}; // Store original data for change tracking

// Load project list on page load
document.addEventListener('DOMContentLoaded', async () => {
  const response = await fetch('/api/projects');
  const projects = await response.json();
  
  const selector = document.getElementById('projectSelector');
  projects.forEach(proj => {
    const option = document.createElement('option');
    option.value = proj.projectid;
    option.textContent = `${proj.customername} - ${proj.projectname}`;
    selector.appendChild(option);
  });
});

async function loadProjectData() {
  const projectId = document.getElementById('projectSelector').value;
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
  document.getElementById('edit-customerName').value = project.customername;
  document.getElementById('edit-projectName').value = project.projectname;
  document.getElementById('edit-startDate').value = project.projectstartdate;
}

function populateCostTable(costData) {
  const tbody = document.querySelector('#costTable tbody');
  tbody.innerHTML = '';
  
  costData.forEach(row => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td contenteditable="true" data-field="paymentmilestone">${row.paymentmilestone}</td>
      <td contenteditable="true" data-field="weight">${row.weight || ''}</td>
      <td contenteditable="true" data-field="cost">${row.cost}</td>
    `;
    tr.dataset.id = row.costanalysisid;
    tbody.appendChild(tr);
  });
}

function populateHoursTable(hoursData) {
  const tbody = document.querySelector('#hoursTable tbody');
  tbody.innerHTML = '';
  
  hoursData.forEach(row => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td contenteditable="true" data-field="hcmmodule">${row.hcmmodule}</td>
      <td contenteditable="true" data-field="weight">${row.weight || ''}</td>
      <td contenteditable="true" data-field="pmhours">${row.pmhours || ''}</td>
      <td contenteditable="true" data-field="planhours">${row.planhours || ''}</td>
      <td contenteditable="true" data-field="achours">${row.achours || ''}</td>
      <td contenteditable="true" data-field="testinghours">${row.testinghours || ''}</td>
      <td contenteditable="true" data-field="deployhours">${row.deployhours || ''}</td>
      <td contenteditable="true" data-field="postgolivehours">${row.postgolivehours || ''}</td>
      <td contenteditable="true" data-field="totalweekshours">${row.totalweekshours || ''}</td>
    `;
    tr.dataset.id = row.hoursanalysisid;
    tbody.appendChild(tr);
  });
}

function populateTimelineTable(timelineData) {
  const tbody = document.querySelector('#timelineTable tbody');
  tbody.innerHTML = '';
  
  timelineData.forEach(row => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td contenteditable="true" data-field="phase">${row.phase}</td>
      <td contenteditable="true" data-field="durationweeks">${row.durationweeks}</td>
    `;
    tr.dataset.id = row.timelineid;
    tbody.appendChild(tr);
  });
}

function populateRateTable(rateData) {
  const tbody = document.querySelector('#rateTable tbody');
  tbody.innerHTML = '';
  
  rateData.forEach(row => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td contenteditable="true" data-field="module">${row.module}</td>
      <td contenteditable="true" data-field="hours">${row.hours}</td>
      <td contenteditable="true" data-field="hourlyrate">${row.hourlyrate || ''}</td>
      <td contenteditable="true" data-field="totalcost">${row.totalcost}</td>
    `;
    tr.dataset.id = row.ratecalcid;
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
