let customers = [], projects = [];
let cModal, pModal;

document.addEventListener('DOMContentLoaded', () => {
    cModal = new bootstrap.Modal(document.getElementById('cModal'));
    pModal = new bootstrap.Modal(document.getElementById('pModal'));
    load();
    document.getElementById('cSearch').oninput = () => renderCustomers(
        customers.filter(c => c.customerName.toLowerCase().includes(document.getElementById('cSearch').value.toLowerCase()))
    );
    document.getElementById('pSearch').oninput = () => renderProjects(
        projects.filter(p => 
            p.projectName.toLowerCase().includes(document.getElementById('pSearch').value.toLowerCase()) ||
            p.customerName.toLowerCase().includes(document.getElementById('pSearch').value.toLowerCase())
        )
    );
});

async function load() {
    customers = await fetch('/api/customers').then(r => r.json());
    projects = await fetch('/api/projects').then(r => r.json());
    renderCustomers(customers);
    renderProjects(projects);
    
    const sel = document.getElementById('pCustomer');
    sel.innerHTML = '<option value="">Select Customer</option>';
    customers.forEach(c => {
        sel.innerHTML += `<option value="${c.customerId}">${c.customerName}</option>`;
    });
}

function renderCustomers(list) {
    const counts = projects.reduce((acc, p) => {
        acc[p.customerId] = (acc[p.customerId] || 0) + 1; // ← Changed
        return acc;
    }, {});
    
    document.getElementById('cList').innerHTML = list.map(c => `
        <li class="list-group-item d-flex justify-content-between align-items-center">
            <div><strong>${c.customerName}</strong> <span class="badge bg-secondary">${counts[c.customerId] || 0}</span></div>
            <div>
                <button class="btn btn-sm btn-warning" onclick="editCustomer(${c.customerId}, '${c.customerName}')">Edit</button>
                <button class="btn btn-sm btn-danger" onclick="delCustomer(${c.customerId}, '${c.customerName}')">Delete</button>
            </div>
        </li>
    `).join('');
}

function renderProjects(list) {
    document.getElementById('pList').innerHTML = list.map(p => `
        <li class="list-group-item d-flex justify-content-between align-items-center">
            <div>
                <strong>${p.projectName}</strong><br>
                <small class="text-muted">${p.customerName} | ${p.project_status || p.status}</small>
            </div>
            <div>
                <button class="btn btn-sm btn-warning" onclick="editProject(${p.projectId || p.project_id})">Edit</button>
                <button class="btn btn-sm btn-danger" onclick="delProject(${p.projectId || p.project_id})">Delete</button>
            </div>
        </li>
    `).join('');
}

function addCustomer() {
    document.getElementById('cTitle').textContent = 'Add Customer';
    document.getElementById('cOld').value = '';
    document.getElementById('cName').value = '';
    cModal.show();
}

function editCustomer(id, name) {
    document.getElementById('cTitle').textContent = 'Edit Customer';
    document.getElementById('cId').value = id;
    document.getElementById('cName').value = name;
    cModal.show();
}

async function saveCustomer() {
    const id = document.getElementById('cId').value;
    const name = document.getElementById('cName').value.trim();
    if (!name) return alert('Name required');
    
    const res = await fetch(id ? `/api/customers/${id}` : '/api/customers', {
        method: id ? 'PUT' : 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({customerName: name})
    }).then(r => r.json());
    
    if (res.success) {
        cModal.hide();
        load();
    } else alert(res.error);
}

async function delCustomer(id, name) {
    if (!confirm(`Delete "${name}" and ALL projects/data? PERMANENT!`)) return;
    
    const res = await fetch(`/api/customers/${id}`, {method: 'DELETE'}).then(r => r.json());
    if (res.success) load();
    else alert(res.error);
}

function addProject() {
    document.getElementById('pTitle').textContent = 'Add Project';
    document.getElementById('pId').value = '';
    document.getElementById('pCustomer').value = '';
    document.getElementById('pCustomer').disabled = false;
    document.getElementById('pName').value = '';
    document.getElementById('pDate').value = '';
    document.getElementById('pStatus').value = 'Active';
    pModal.show();
}

function editProject(id) {
    const p = projects.find(x => (x.projectId || x.project_id) === id);
    if (!p) return;
    
    document.getElementById('pTitle').textContent = 'Edit Project';
    document.getElementById('pId').value = id;
    document.getElementById('pCustomer').value = p.customerId;
    document.getElementById('pCustomer').disabled = true;
    document.getElementById('pName').value = p.projectName;
    document.getElementById('pDate').value = (p.projectStartDate || p.project_start_date || p.startDate).split('T')[0];
    document.getElementById('pStatus').value = p.project_status || p.status;
    pModal.show();
}

async function saveProject() {
    const id = document.getElementById('pId').value;
    const customerIdRaw = document.getElementById('pCustomer').value;
    const projectNameRaw = document.getElementById('pName').value;
    const startDateRaw = document.getElementById('pDate').value;
    const statusRaw = document.getElementById('pStatus').value;
    
    // DEBUG: Log all raw values
    console.log('=== DEBUG: saveProject ===');
    console.log('Project ID:', id);
    console.log('Customer ID (raw):', customerIdRaw);
    console.log('Project Name (raw):', projectNameRaw);
    console.log('Start Date (raw):', startDateRaw);
    console.log('Status (raw):', statusRaw);
    
    const data = {
        customerId: parseInt(customerIdRaw),
        projectName: projectNameRaw.trim(),
        startDate: startDateRaw,
        status: statusRaw
    };
    
    // DEBUG: Log processed data
    console.log('Processed data:', data);
    console.log('customerId type:', typeof data.customerId);
    console.log('customerId isNaN:', isNaN(data.customerId));
    console.log('projectName empty:', !data.projectName);
    console.log('startDate empty:', !data.startDate);
    
    // Validation
    if (!data.customerId || isNaN(data.customerId) || !data.projectName || !data.startDate) {
        console.log('VALIDATION FAILED');
        console.log('customerId check:', !data.customerId || isNaN(data.customerId));
        console.log('projectName check:', !data.projectName);
        console.log('startDate check:', !data.startDate);
        return alert('All fields required');
    }
    
    console.log('Validation passed, making API call...');
    
    try {
        const url = id ? `/api/projects/${id}` : '/api/projects';
        const method = id ? 'PUT' : 'POST';
        
        console.log('API URL:', url);
        console.log('API Method:', method);
        console.log('API Payload:', JSON.stringify(data));
        
        const res = await fetch(url, {
            method: method,
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(data)
        });
        
        console.log('Response status:', res.status);
        const result = await res.json();
        console.log('Response data:', result);
        
        if (result.success) {
            pModal.hide();
            document.getElementById('pCustomer').disabled = false;
            load();
        } else {
            alert(result.error || 'Unknown error');
        }
    } catch (error) {
        console.error('Error saving project:', error);
        alert('Failed to save project: ' + error.message);
    }
}

async function delProject(id) {
    const p = projects.find(x => (x.projectId || x.project_id) === id);
    if (!confirm(`Delete "${p.projectName}" and ALL data? PERMANENT!`)) return;
    
    const res = await fetch(`/api/projects/${id}`, {method: 'DELETE'}).then(r => r.json());
    if (res.success) load();
    else alert(res.error);
}
