// ============================================================================
// DASHBOARD.JS - Main dashboard logic for HCM Utilization Dashboard
// ============================================================================

// Global variables
let timelineInstance = null;
let stackedBarChartInstance = null;
let lineChartInstance = null;
let weekDetailChartInstance = null;
let allProjects = [];
let allTimelineData = null;

// Color scheme for phases
const PHASE_COLORS = {
    'P+M': '#FF6B6B',
    'Plan': '#4ECDC4',
    'A+C': '#45B7D1',
    'Testing': '#FFA07A',
    'Deploy': '#98D8C8',
    'Post Go Live': '#A8E6CF'
};

// Color palette for projects
const PROJECT_COLORS = [
    '#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', 
    '#98D8C8', '#A8E6CF', '#F06292', '#AED581',
    '#FFD54F', '#4DD0E1', '#9575CD', '#FF8A65'
];

// ============================================================================
// INITIALIZATION
// ============================================================================

document.addEventListener('DOMContentLoaded', async () => {
    console.log('Dashboard initializing...');
    
    // Set default date range (current year)
    const today = new Date();
    const startOfYear = new Date(today.getFullYear(), 0, 1);
    const endOfYear = new Date(today.getFullYear(), 11, 31);
    
    document.getElementById('startDate').valueAsDate = startOfYear;
    document.getElementById('endDate').valueAsDate = endOfYear;
    
    // Attach event listeners
    attachEventListeners();
    
    // Load all data
    await loadDashboardData();
});

function attachEventListeners() {
    // Time granularity change
    document.querySelectorAll('input[name="timeGranularity"]').forEach(radio => {
        radio.addEventListener('change', () => {
            if (timelineInstance) {
                timelineInstance.setGranularity(radio.value);
            }
            refreshCharts();
        });
    });
    
    // Project filter change
    document.getElementById('projectFilter').addEventListener('change', (e) => {
        filterTimelineByProject(e.target.value);
        refreshCharts();
    });
    
    // Date range change
    document.getElementById('startDate').addEventListener('change', () => {
        if (timelineInstance) {
            timelineInstance.updateDateRange();
        }
    });
    
    document.getElementById('endDate').addEventListener('change', () => {
        if (timelineInstance) {
            timelineInstance.updateDateRange();
        }
    });
    
    // Project search
    document.getElementById('projectSearch').addEventListener('input', (e) => {
        filterProjectsTable(e.target.value);
    });
}

// ============================================================================
// DATA LOADING
// ============================================================================

async function loadDashboardData() {
    try {
        // Fetch all data in parallel for better performance
        const [timelineResponse, projectsResponse, metricsResponse] = await Promise.all([
            fetch('/api/timeline-data'),
            fetch('/api/projects'),
            fetch('/api/dashboard-metrics')
        ]);

        if (!timelineResponse.ok) throw new Error(`Timeline API Error: ${timelineResponse.statusText}`);
        if (!projectsResponse.ok) throw new Error(`Projects API Error: ${projectsResponse.statusText}`);
        if (!metricsResponse.ok) throw new Error(`Metrics API Error: ${metricsResponse.statusText}`);

        allTimelineData = await timelineResponse.json();
        const projectsList = await projectsResponse.json();
        const metrics = await metricsResponse.json();

        // Update metrics cards
        updateMetrics(metrics);

        // Populate projects table with the CORRECT data (projectsList, not timeline data)
        populateProjectsTable(projectsList);
        populateProjectFilter(projectsList);

        // Try to create timeline only if we have valid data
        if (allTimelineData && 
            allTimelineData.projects && 
            allTimelineData.projects.length > 0 &&
            allTimelineData.dateRange &&
            allTimelineData.dateRange.minDate) {
            
            try {
                timelineInstance = new ProjectTimeline(
                    'timelineCanvas',
                    allTimelineData,
                    {
                        granularity: 'weekly',
                        onProjectClick: (project, phase) => {
                            console.log('Project clicked:', project, phase);
                        }
                    }
                );
            } catch (timelineError) {
                console.error("Timeline initialization failed:", timelineError);
                const canvas = document.getElementById('timelineCanvas');
                if (canvas) {
                    const ctx = canvas.getContext('2d');
                    ctx.clearRect(0, 0, canvas.width, canvas.height);
                    ctx.textAlign = 'center';
                    ctx.fillStyle = '#888';
                    ctx.fillText('Timeline could not be displayed', canvas.width / 2, canvas.height / 2);
                }
            }
        } else {
            console.warn("No valid timeline data. Skipping timeline rendering.");
        }

        // Load charts
        await loadCharts();

    } catch (error) {
        console.error("Fatal error loading dashboard:", error);
        const tableBody = document.querySelector('#projectsTable tbody');
        if (tableBody) {
            tableBody.innerHTML = '<tr><td colspan="7" class="text-center text-danger">Error loading data. Please refresh.</td></tr>';
        }
    }
}

function updateMetrics(metrics) {
    // Update Total Projects
    const totalProjectsEl = document.getElementById('totalProjects');
    if (totalProjectsEl) {
        totalProjectsEl.textContent = metrics.totalProjects || 0;
    }

    // Update Active Modules
    const activeModulesEl = document.getElementById('activeModules');
    if (activeModulesEl) {
        activeModulesEl.textContent = metrics.activeModules || 0;
    }

    // Update Average Utilization
    const avgUtilizationEl = document.getElementById('averageUtilization');
    if (avgUtilizationEl) {
        avgUtilizationEl.textContent = `${metrics.averageUtilization || 0}%`;
    }

    // Update Total Hours
    const totalHoursEl = document.getElementById('totalHours');
    if (totalHoursEl) {
        totalHoursEl.textContent = Math.round(metrics.totalHours || 0);
    }
}

async function fetchProjects() {
    const response = await fetch('/api/projects');
    if (!response.ok) throw new Error('Failed to fetch projects');
    return await response.json();
}

async function fetchTimelineData() {
    const response = await fetch('/api/timeline-data');
    if (!response.ok) throw new Error('Failed to fetch timeline data');
    return await response.json();
}

// ============================================================================
// METRICS CARDS
// ============================================================================

function populateMetrics(projects) {
    // Total projects
    document.getElementById('metric-total-projects').textContent = projects.length;
    
    // Calculate active modules (unique modules across all projects)
    const uniqueModules = new Set();
    let totalHours = 0;
    
    projects.forEach(project => {
        if (project.modules) {
            project.modules.forEach(mod => uniqueModules.add(mod.name));
            totalHours += project.totalHours || 0;
        }
    });
    
    document.getElementById('metric-active-modules').textContent = uniqueModules.size;
    
    // Average utilization (placeholder - you'll need actual capacity data)
    const avgUtil = calculateAverageUtilization(projects);
    document.getElementById('metric-avg-util').textContent = avgUtil + '%';
    
    // Total hours
    document.getElementById('metric-total-hours').textContent = 
        totalHours.toLocaleString() + ' hrs';
}

function calculateAverageUtilization(projects) {
    // This is a simplified calculation
    // In production, you'd compare against actual capacity
    if (projects.length === 0) return 0;
    
    const totalHours = projects.reduce((sum, p) => sum + (p.totalHours || 0), 0);
    const avgHoursPerProject = totalHours / projects.length;
    
    // Assume 2000 hours per year as baseline capacity per project
    const utilization = Math.min(100, (avgHoursPerProject / 2000) * 100);
    
    return Math.round(utilization);
}

// ============================================================================
// PROJECT FILTER & TABLE
// ============================================================================

function populateProjectFilter(projects) {
    const select = document.getElementById('projectFilter');
    
    // Clear existing options except "All Projects"
    select.innerHTML = '<option value="all">All Projects</option>';
    
    projects.forEach(project => {
        const option = document.createElement('option');
        option.value = project.projectid;
        option.textContent = `${project.customerName} - ${project.projectName}`;
        select.appendChild(option);
    });
}

function populateProjectsTable(projects) {
    const tbody = document.querySelector('#projectsTable tbody');
    tbody.innerHTML = '';
    
    if (projects.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="text-center text-muted">
                    No projects found. <a href="/upload">Upload your first project</a>
                </td>
            </tr>
        `;
        return;
    }
    
    projects.forEach(project => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${escapeHtml(project.customerName || 'N/A')}</td>
            <td>${escapeHtml(project.projectName || 'N/A')}</td>
            <td>${formatDate(project.projectStartDate)}</td>
            <td><span class="badge bg-primary">${project.moduleCount || 0}</span></td>
            <td>${(project.totalHours || 0).toLocaleString()} hrs</td>
            <td>${getProjectStatus(project)}</td>
            <td>
                <button class="btn btn-sm btn-outline-primary" 
                        onclick="viewProjectDetails(${project.projectid})">
                    <i class="bi bi-eye"></i> View
                </button>
            </td>
        `;
        tbody.appendChild(tr);
    });
}

function getProjectStatus(project) {
    const today = new Date();
    const startDate = new Date(project.projectstartdate);
    const endDate = project.endDate ? new Date(project.endDate) : null;
    
    if (startDate > today) {
        return '<span class="utilization-badge util-low">Planned</span>';
    } else if (endDate && endDate < today) {
        return '<span class="utilization-badge util-medium">Completed</span>';
    } else {
        return '<span class="utilization-badge util-high">Active</span>';
    }
}

function filterProjectsTable(searchTerm) {
    const tbody = document.querySelector('#projectsTable tbody');
    const rows = tbody.querySelectorAll('tr');
    
    rows.forEach(row => {
        const text = row.textContent.toLowerCase();
        if (text.includes(searchTerm.toLowerCase())) {
            row.style.display = '';
        } else {
            row.style.display = 'none';
        }
    });
}

function filterTimelineByProject(projectId) {
    if (!timelineInstance) return;
    
    if (projectId === 'all') {
        timelineInstance.showAllProjects();
    } else {
        timelineInstance.filterByProject(parseInt(projectId));
    }
}

// ============================================================================
// CHARTS
// ============================================================================

async function loadCharts() {
    await Promise.all([
        loadStackedBarChart(),
        loadLineChart()
    ]);
}

function loadStackedBarChart(data) {
    const canvas = document.getElementById('stackedBarChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    if (stackedBarChartInstance) {
        stackedBarChartInstance.destroy();
    }

    // Guard clause: If data or its periods are missing/empty, show a message and stop.
    if (!data || !data.periods || data.livedata.periods.length === 0) {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        ctx.textAlign = 'center';
        ctx.fillStyle = '#888';
        ctx.fillText('No workload data available', canvas.width / 2, canvas.height / 2);
        return;
    }

    const labels = data.periods.map(p => {
        const startDate = new Date(p.weekStart);
        return `${startDate.getDate().toString().padStart(2, '0')}/${(startDate.getMonth() + 1).toString().padStart(2, '0')}`;
    });

    stackedBarChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Active Projects',
                    data: data.datasets.activeProjects,
                    backgroundColor: 'rgba(75, 192, 192, 0.7)'
                },
                {
                    label: 'Active Modules',
                    data: data.datasets.activeModules,
                    backgroundColor: 'rgba(255, 159, 64, 0.7)'
                }
            ]
        },
        options: {
            scales: {
                x: { stacked: true },
                y: { stacked: true, beginAtZero: true }
            },
            responsive: true,
            maintainAspectRatio: false
        }
    });
}

async function loadLineChart() {
    try {
        const granularity = document.querySelector('input[name="timeGranularity"]:checked').value;
        const response = await fetch(`/api/module-utilization?granularity=${granularity}&view=by-module`);
        const data = await response.json();
        
        if (lineChartInstance) {
            lineChartInstance.destroy();
        }
        
        const ctx = document.getElementById('lineChart').getContext('2d');
        lineChartInstance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.periods || [],
                datasets: (data.modules || []).map((mod, idx) => ({
                    label: mod.moduleName,
                    data: mod.hours,
                    borderColor: PROJECT_COLORS[idx % PROJECT_COLORS.length],
                    backgroundColor: PROJECT_COLORS[idx % PROJECT_COLORS.length] + '20',
                    fill: false,
                    tension: 0.1
                }))
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        title: { display: true, text: 'Time Period' }
                    },
                    y: {
                        beginAtZero: true,
                        title: { display: true, text: 'Total Hours' }
                    }
                },
                plugins: {
                    legend: { 
                        position: 'bottom',
                        labels: { boxWidth: 12 }
                    },
                    tooltip: {
                        callbacks: {
                            label: (context) => {
                                return `${context.dataset.label}: ${context.parsed.y} hours`;
                            }
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error loading line chart:', error);
    }
}

async function refreshCharts() {
    await loadCharts();
}

// ============================================================================
// DRILL-DOWN MODAL
// ============================================================================

async function showModuleDrillDown(date, weekNum) {
    try {
        const response = await fetch(`/api/week-details?date=${date.toISOString()}`);
        const data = await response.json();
        
        // Update modal title
        document.getElementById('modalTitle').textContent = 
            `Module Utilization: Week ${weekNum} (${formatDate(date)})`;
        
        // Check for conflicts
        const alerts = document.getElementById('modalAlerts');
        alerts.innerHTML = '';
        
        const highUtilModules = data.modules.filter(m => m.utilizationPercent > 70);
        if (highUtilModules.length > 0) {
            alerts.innerHTML = `
                <div class="alert alert-warning">
                    <strong><i class="bi bi-exclamation-triangle"></i> High Utilization Alert:</strong>
                    ${highUtilModules.length} module(s) with utilization above 70%
                </div>
            `;
        }
        
        // Populate table
        const tbody = document.getElementById('modalTableBody');
        tbody.innerHTML = '';
        
        data.modules.forEach(module => {
            const tr = document.createElement('tr');
            const utilClass = module.utilizationPercent > 70 ? 'table-danger' : 
                            module.utilizationPercent > 50 ? 'table-warning' : '';
            
            tr.className = utilClass;
            tr.innerHTML = `
                <td><strong>${escapeHtml(module.moduleName)}</strong></td>
                <td>${module.totalHours} hrs</td>
                <td>
                    ${module.projects.map(p => 
                        `<span class="badge bg-primary me-1">${escapeHtml(p.name)} (${p.hours}h)</span>`
                    ).join('')}
                </td>
                <td>
                    <div class="progress" style="min-width: 100px;">
                        <div class="progress-bar ${module.utilizationPercent > 70 ? 'bg-danger' : 'bg-success'}" 
                             style="width: ${module.utilizationPercent}%">
                            ${module.utilizationPercent}%
                        </div>
                    </div>
                </td>
            `;
            tbody.appendChild(tr);
        });
        
        // Create detail chart
        createWeekDetailChart(data.modules);
        
        // Show modal
        const modal = new bootstrap.Modal(document.getElementById('moduleDrillDownModal'));
        modal.show();
        
    } catch (error) {
        console.error('Error loading drill-down data:', error);
        alert('Failed to load details: ' + error.message);
    }
}

function createWeekDetailChart(modules) {
    if (weekDetailChartInstance) {
        weekDetailChartInstance.destroy();
    }
    
    const ctx = document.getElementById('weekDetailChart').getContext('2d');
    weekDetailChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: modules.map(m => m.moduleName),
            datasets: [{
                label: 'Hours',
                data: modules.map(m => m.totalHours),
                backgroundColor: modules.map(m => 
                    m.utilizationPercent > 70 ? '#dc3545' : '#0d6efd'
                )
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Hours' }
                }
            },
            plugins: {
                legend: { display: false }
            }
        }
    });
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function viewProjectDetails(projectId) {
    window.location.href = `/edit?project=${projectId}`;
}

function refreshDashboard() {
    location.reload();
}

function exportToCSV() {
    // TODO: Implement CSV export
    alert('CSV export will be implemented in a future update');
}

function showLoading(show) {
    const overlay = document.getElementById('loadingOverlay');
    overlay.style.display = show ? 'flex' : 'none';
}

function showError(message) {
    alert('Error: ' + message);
}

function showNoDataMessage() {
    const canvas = document.getElementById('timelineCanvas');
    const ctx = canvas.getContext('2d');
    
    ctx.font = '16px sans-serif';
    ctx.fillStyle = '#6c757d';
    ctx.textAlign = 'center';
    ctx.fillText('No project data available. Please upload project data to get started.', 
                 canvas.width / 2, canvas.height / 2);
}

function formatDate(dateString) {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { 
        year: 'numeric', 
        month: 'short', 
        day: 'numeric' 
    });
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

console.log('Dashboard.js loaded successfully');
