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
let currentProjectFilter = 'all';

let currentDateRange = {
    startDate: null,
    endDate: null
};

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
    
    document.getElementById('projectFilter').addEventListener('change', async (e) => {
        currentProjectFilter = e.target.value;
        console.log('Project filter changed to:', currentProjectFilter);
        
        // Filter timeline visualization
        filterTimelineByProject(currentProjectFilter);
        
        // ✓ NEW: Refresh charts and table with filter
        await refreshCharts();
        await loadWeeklyModuleTable();
    });
    
    // Date range change - UPDATED to reload all data
    document.getElementById('startDate').addEventListener('change', updateDateRange);
    document.getElementById('endDate').addEventListener('change', updateDateRange);
    
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
        console.log('=== loadDashboardData START ===');
        
        // Get current date range from inputs
        currentDateRange.startDate = document.getElementById('startDate').value;
        currentDateRange.endDate = document.getElementById('endDate').value;
        
        // Build query parameters for date filtering
        const params = new URLSearchParams({
            start_date: currentDateRange.startDate,
            end_date: currentDateRange.endDate
        });
        
        // Fetch all data in parallel for better performance
        const [timelineResponse, projectsResponse, metricsResponse] = await Promise.all([
            fetch(`/api/timeline-data?${params}`),
            fetch('/api/projects'),  // Active Projects - NO date filtering
            fetch(`/api/dashboard-metrics?${params}`)
        ]);
        
        console.log('API responses received');
        console.log('Timeline response OK?', timelineResponse.ok);
        console.log('Projects response OK?', projectsResponse.ok);
        console.log('Metrics response OK?', metricsResponse.ok);
        
        if (!timelineResponse.ok) throw new Error(`Timeline API Error: ${timelineResponse.statusText}`);
        if (!projectsResponse.ok) throw new Error(`Projects API Error: ${projectsResponse.statusText}`);
        if (!metricsResponse.ok) throw new Error(`Metrics API Error: ${metricsResponse.statusText}`);
        
        // Parse all responses
        allTimelineData = await timelineResponse.json();
        const projectsList = await projectsResponse.json();
        const metrics = await metricsResponse.json();
        
        console.log('=== RAW API DATA ===');
        console.log('allTimelineData:', allTimelineData);
        console.log('Timeline projects count:', allTimelineData?.projects?.length);
        console.log('projectsList count:', projectsList?.length);
        console.log('metrics:', metrics);
        
        // Update dashboard components
        updateMetrics(metrics);
        populateProjectsTable(projectsList);  // Active Projects table (no date filter)
        populateProjectFilter(projectsList);
        
        // Initialize timeline visualization with date-filtered data
        if (allTimelineData && allTimelineData.projects && allTimelineData.projects.length > 0) {
            console.log('✓ Validation passed. Creating timeline...');
            const transformedData = transformTimelineData(allTimelineData);
            
            timelineInstance = new ProjectTimeline(
                'timelineCanvas',
                transformedData,
                {
                    granularity: 'weekly',
                    onProjectClick: (project, phase) => {
                        console.log('Project clicked:', project, phase);
                    }
                }
            );
            console.log('✓ ProjectTimeline created successfully');
        } else {
            console.warn('✗ No valid timeline data for selected date range');
        }
        
        // Load charts with date filtering
        await loadCharts();
        
        console.log('=== loadDashboardData END ===');
    } catch (error) {
        console.error('✗ Fatal error loading dashboard:', error);
        console.error('Error stack:', error.stack);
        
        // Display error message to user
        const tableBody = document.querySelector('#projectsTable tbody');
        if (tableBody) {
            tableBody.innerHTML = '<tr><td colspan="7" class="text-center text-danger">Error loading dashboard data</td></tr>';
        }
    }
}

function updateMetrics(metrics) {
    // Get all metric-value elements (they appear in order: Total Projects, Active Modules, Avg Utilization, Total Hours)
    const metricValues = document.querySelectorAll('.metric-value');
    
    if (metricValues.length >= 4) {
        // Total Projects (first metric card)
        metricValues[0].innerHTML = metrics.totalProjects || 0;
        
        // Active Modules (second metric card)
        metricValues[1].innerHTML = metrics.activeModules || 0;
        
        // Average Utilization (third metric card)
        metricValues[2].innerHTML = `${metrics.averageUtilization || 0}%`;
        
        // Total Hours (fourth metric card)
        metricValues[3].innerHTML = Math.round(metrics.totalHours || 0);
    } else {
        console.error('Could not find all metric value elements');
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

/**
 * Load and render all chart visualizations with date filtering
 */
async function loadCharts() {
    try {
        console.log('Loading charts with date range:', currentDateRange);
        
        const granularity = document.querySelector('input[name="timeGranularity"]:checked')?.value || 'weekly';
        
        const params = new URLSearchParams({
            granularity: granularity,
            view: 'by-module',  // ← ENSURE THIS IS SET
            start_date: currentDateRange.startDate,
            end_date: currentDateRange.endDate,
            project_id: currentProjectFilter || 'all'
        });
        
        const moduleResponse = await fetch(`/api/module-utilization?${params}`);
        if (!moduleResponse.ok) {
            throw new Error(`Module utilization API error: ${moduleResponse.statusText}`);
        }
        const moduleData = await moduleResponse.json();
        
        console.log('Module data received:', moduleData);
        console.log('Dataset count:', moduleData.datasets?.length);
        
        await Promise.all([
            loadStackedBarChart(allTimelineData.workload),
            loadLineChart(moduleData),
            loadWeeklyModuleTable()
        ]);
        
        console.log('✓ All charts loaded successfully');
    } catch (error) {
        console.error('Error loading charts:', error);
    }
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

function loadLineChart(moduleData) {
    try {
        console.log('Loading line chart with module data:', moduleData);
        console.log('Labels:', moduleData.labels);
        console.log('Datasets:', moduleData.datasets?.length);
        
        const ctx = document.getElementById('lineChart');
        if (!ctx) {
            console.error('Line chart canvas not found');
            return;
        }
        
        // Destroy existing chart
        if (lineChartInstance) {
            lineChartInstance.destroy();
        }
        
        // Log the actual data values
        console.log('📊 Chart Debug Info:');
        moduleData.datasets.forEach((dataset, i) => {
            const nonZeroValues = dataset.data.filter(v => v > 0);
            const totalHours = dataset.data.reduce((a, b) => a + b, 0);
            console.log(`  ${dataset.label}:`);
            console.log(`    - Total hours: ${totalHours.toFixed(1)}`);
            console.log(`    - Non-zero points: ${nonZeroValues.length}/${dataset.data.length}`);
            console.log(`    - Data array:`, dataset.data);
        });

        // Create chart with module data
        lineChartInstance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: moduleData.labels || [],
                datasets: moduleData.datasets || []
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                plugins: {
                    title: {
                        display: true,
                        text: 'Module Hours Over Time',
                        font: {
                            size: 16,
                            weight: 'bold'
                        }
                    },
                    legend: {
                        display: true,
                        position: 'top',
                        labels: {
                            usePointStyle: true,
                            padding: 15
                        }
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        callbacks: {
                            label: function(context) {
                                let label = context.dataset.label || '';
                                if (label) {
                                    label += ': ';
                                }
                                if (context.parsed.y !== null) {
                                    label += context.parsed.y.toFixed(1) + ' hours';
                                }
                                return label;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Week Starting',
                            font: {
                                size: 12,
                                weight: 'bold'
                            }
                        },
                        ticks: {
                            maxRotation: 45,
                            minRotation: 45
                        }
                    },
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Hours',
                            font: {
                                size: 12,
                                weight: 'bold'
                            }
                        },
                        ticks: {
                            callback: function(value) {
                                return value.toFixed(0);
                            }
                        }
                    }
                }
            }
        });
        
        console.log('✓ Line chart created successfully');
        
    } catch (error) {
        console.error('Error in loadLineChart:', error);
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

async function updateDateRange() {
    currentDateRange.startDate = document.getElementById('startDate').value;
    currentDateRange.endDate = document.getElementById('endDate').value;
    
    console.log('Date range updated:', currentDateRange);
    
    // ✓ RELOAD ALL DATE-DEPENDENT COMPONENTS
    await loadDashboardData();
    await loadWeeklyModuleTable();  // ✓ ADD THIS LINE
}

function transformTimelineData(apiData) {
    console.log('=== transformTimelineData START ===');
    console.log('Input apiData:', apiData);
    console.log('Input projects count:', apiData?.projects?.length);
    
    const projectMap = new Map();
    
    // Map phase codes to standard names for the timeline
    const phaseMapping = {
        'PM': 'P+M',
        'PLAN': 'Plan',
        'AC': 'A+C',
        'TESTING': 'Testing',
        'DEPLOY': 'Deploy',
        'POST_GO_LIVE': 'Post Go Live'
    };
    
    const phaseOrder = ['P+M', 'Plan', 'A+C', 'Testing', 'Deploy', 'Post Go Live'];
    
    // Group by project, then by phase (ignoring module)
    apiData.projects.forEach((row, index) => {
        if (index < 3) console.log(`Processing row ${index}:`, row);
        
        const projKey = row.project_id;
        
        if (!projectMap.has(projKey)) {
            console.log(`Creating new project: ${row.project_name} (ID: ${projKey})`);
            projectMap.set(projKey, {
                projectId: row.project_id,
                projectName: row.project_name,
                customerName: row.customer_name,
                projectStatus: row.project_status,
                phaseMap: new Map()
            });
        }
        
        const project = projectMap.get(projKey);
        
        // Normalize phase code
        const normalizedPhase = phaseMapping[row.phase_code] || row.phase_code;
        
        if (index < 3) console.log(`  Phase: ${row.phase_code} -> ${normalizedPhase}`);
        
        // Track min/max dates for each phase across ALL modules
        if (!project.phaseMap.has(normalizedPhase)) {
            console.log(`  Creating new phase for project ${projKey}: ${normalizedPhase}`);
            project.phaseMap.set(normalizedPhase, {
                phase: normalizedPhase,
                phaseName: row.phase_name,
                startDate: row.week_start_date,
                endDate: row.week_end_date
            });
        } else {
            const phase = project.phaseMap.get(normalizedPhase);
            
            // Expand the phase date range if this module-week extends it
            if (new Date(row.week_start_date) < new Date(phase.startDate)) {
                console.log(`  Extending ${normalizedPhase} start: ${phase.startDate} -> ${row.week_start_date}`);
                phase.startDate = row.week_start_date;
            }
            if (new Date(row.week_end_date) > new Date(phase.endDate)) {
                console.log(`  Extending ${normalizedPhase} end: ${phase.endDate} -> ${row.week_end_date}`);
                phase.endDate = row.week_end_date;
            }
        }
    });
    
    console.log('ProjectMap after processing:', projectMap);
    console.log('Number of projects in map:', projectMap.size);
    
    // Convert to array format
    const projects = Array.from(projectMap.values()).map(proj => {
        console.log(`Converting project ${proj.projectName}:`);
        const phases = Array.from(proj.phaseMap.values());
        console.log(`  Has ${phases.length} phases:`, phases.map(p => p.phase));
        
        // Sort phases by predefined order
        phases.sort((a, b) => {
            const indexA = phaseOrder.indexOf(a.phase);
            const indexB = phaseOrder.indexOf(b.phase);
            return indexA - indexB;
        });
        
        console.log(`  Sorted phases:`, phases.map(p => p.phase));
        
        // Calculate duration in weeks for each phase
        phases.forEach(phase => {
            const start = new Date(phase.startDate);
            const end = new Date(phase.endDate);
            phase.durationWeeks = Math.ceil((end - start) / (7 * 24 * 60 * 60 * 1000)) + 1;
            console.log(`    ${phase.phase}: ${phase.startDate} to ${phase.endDate} (${phase.durationWeeks} weeks)`);
        });
        
        return {
            projectId: proj.projectId,
            projectName: proj.projectName,
            customerName: proj.customerName,
            projectStatus: proj.projectStatus,
            phases: phases
        };
    });
    
    const result = {
        dateRange: apiData.dateRange,
        projects: projects,
        workload: apiData.workload
    };
    
    console.log('=== transformTimelineData END ===');
    console.log('Output projects count:', result.projects.length);
    console.log('Output structure:', result);
    
    return result;
}


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

// ============================================================================
// WEEKLY MODULE TABLE
// ============================================================================

async function loadWeeklyModuleTable() {
    try {
        console.log('Loading weekly module table...');
        
        const params = new URLSearchParams({
            start_date: currentDateRange.startDate,
            end_date: currentDateRange.endDate,
            project_id: currentProjectFilter || 'all'
        });
        
        const response = await fetch(`/api/weekly-module-hours?${params}`);
        if (!response.ok) {
            throw new Error(`Weekly module hours API error: ${response.statusText}`);
        }
        
        const apiData = await response.json();
        
        console.log('Weekly data received:', apiData);
        console.log('Data structure:', JSON.stringify(apiData, null, 2));
        
        const tableContainer = document.getElementById('weeklyModuleTableContainer');
        if (!tableContainer) {
            console.error('Weekly module table container not found');
            return;
        }
        
        // Validate data structure
        if (!apiData.weeks || apiData.weeks.length === 0) {
            tableContainer.innerHTML = '<div class="alert alert-info">No weeks available for the selected date range</div>';
            return;
        }
        
        if (!apiData.modules || apiData.modules.length === 0) {
            tableContainer.innerHTML = '<div class="alert alert-info">No modules found for the selected date range</div>';
            return;
        }
        
        if (!apiData.data || apiData.data.length === 0) {
            tableContainer.innerHTML = '<div class="alert alert-info">No hour data available for the selected date range</div>';
            return;
        }
        
        // Build table HTML
        let html = '<div class="table-responsive" style="max-height: 600px; overflow-y: auto;">';
        html += '<table class="table table-sm table-bordered table-hover mb-0">';
        html += '<thead class="table-light" style="position: sticky; top: 0; z-index: 10;"><tr>';
        html += '<th class="sticky-col" style="min-width: 150px;">Module</th>';
        
        // Week headers - use the label from weeks array
        apiData.weeks.forEach((week, index) => {
            html += `<th class="text-center text-nowrap" style="min-width: 80px;" title="${week.label}">${formatWeekHeaderShort(week.start)}</th>`;
        });
        html += '<th class="text-center bg-light" style="min-width: 80px;"><strong>Total</strong></th></tr></thead>';
        html += '<tbody>';
        
        // Module rows
        apiData.modules.forEach(module => {
            html += `<tr><td class="sticky-col"><strong>${module}</strong></td>`;
            
            let moduleTotal = 0;
            
            // Iterate through each week's data
            apiData.data.forEach((weekData, weekIndex) => {
                const hours = weekData[module] || 0;
                moduleTotal += hours;
                
                const cellClass = hours > 0 ? 'table-success text-center' : 'text-center text-muted';
                html += `<td class="${cellClass}">${hours > 0 ? hours.toFixed(1) : '-'}</td>`;
            });
            
            html += `<td class="text-center bg-light"><strong>${moduleTotal.toFixed(1)}</strong></td></tr>`;
        });
        
        // Weekly totals row
        html += '<tr class="table-secondary"><td class="sticky-col"><strong>Week Total</strong></td>';
        
        let grandTotal = 0;
        
        // Calculate total for each week
        apiData.data.forEach((weekData, weekIndex) => {
            let weekTotal = 0;
            
            // Sum all module hours for this week
            apiData.modules.forEach(module => {
                weekTotal += weekData[module] || 0;
            });
            
            grandTotal += weekTotal;
            html += `<td class="text-center"><strong>${weekTotal > 0 ? weekTotal.toFixed(1) : '-'}</strong></td>`;
        });
        
        html += `<td class="text-center bg-secondary text-white"><strong>${grandTotal.toFixed(1)}</strong></td></tr>`;
        html += '</tbody></table></div>';
        
        tableContainer.innerHTML = html;
        
        console.log('✓ Weekly module table rendered successfully');
        console.log(`  - ${apiData.modules.length} modules`);
        console.log(`  - ${apiData.weeks.length} weeks`);
        console.log(`  - Grand total: ${grandTotal.toFixed(1)} hours`);
        
    } catch (error) {
        console.error('Error loading weekly module table:', error);
        console.error('Error stack:', error.stack);
        
        const tableContainer = document.getElementById('weeklyModuleTableContainer');
        if (tableContainer) {
            tableContainer.innerHTML = `
                <div class="alert alert-danger">
                    <strong>Error loading weekly module data</strong><br>
                    ${error.message}
                </div>
            `;
        }
    }
}

function formatWeekHeaderShort(weekStart) {
    try {
        const date = new Date(weekStart + 'T00:00:00');
        const month = date.toLocaleString('en-US', { month: 'short' });
        const day = date.getDate();
        return `${month} ${day}`;
    } catch (e) {
        console.error('Error formatting week header:', weekStart, e);
        return weekStart;
    }
}


function renderWeeklyModuleTable(tableData) {
    const container = document.getElementById('weeklyModuleTableContainer');
    if (!container) return;
    
    const { weeks, modules, data } = tableData;
    
    if (!weeks || weeks.length === 0) {
        container.innerHTML = '<div class="alert alert-info">No weekly data available</div>';
        return;
    }
    
    const today = new Date();
    let currentWeekIndex = weeks.findIndex(w => {
        const start = new Date(w.start);
        const end = new Date(w.end);
        return today >= start && today <= end;
    });
    
    if (currentWeekIndex === -1) currentWeekIndex = 0;
    
    let html = `
        <div class="d-flex justify-content-between align-items-center mb-3">
            <button class="btn btn-sm btn-outline-secondary" id="prevWeeks">
                <i class="fas fa-chevron-left"></i> Previous
            </button>
            <span><strong>Viewing weeks ${currentWeekIndex + 1}-${Math.min(currentWeekIndex + 10, weeks.length)} of ${weeks.length}</strong></span>
            <button class="btn btn-sm btn-outline-secondary" id="nextWeeks">
                Next <i class="fas fa-chevron-right"></i>
            </button>
        </div>
        <div class="table-responsive" style="max-height: 500px; overflow-y: auto;">
            <table class="table table-sm table-bordered table-hover">
                <thead class="table-light sticky-top">
                    <tr>
                        <th>Week</th>
                        ${modules.map(m => `<th class="text-end">${m}</th>`).join('')}
                        <th class="text-end"><strong>Total</strong></th>
                    </tr>
                </thead>
                <tbody id="weeklyTableBody"></tbody>
            </table>
        </div>
    `;
    
    container.innerHTML = html;
    container.dataset.currentIndex = currentWeekIndex;
    container.dataset.totalWeeks = weeks.length;
    
    renderWeekRows(weeks, modules, data, currentWeekIndex);
    
    document.getElementById('prevWeeks').addEventListener('click', () => scrollWeeks(-10));
    document.getElementById('nextWeeks').addEventListener('click', () => scrollWeeks(10));
}

function renderWeekRows(weeks, modules, data, startIdx) {
    const tbody = document.getElementById('weeklyTableBody');
    if (!tbody) return;
    
    const today = new Date();
    const endIdx = Math.min(startIdx + 10, weeks.length);
    let html = '';
    
    for (let i = startIdx; i < endIdx; i++) {
        const week = weeks[i];
        const weekData = data[i];
        const weekStart = new Date(week.start);
        const weekEnd = new Date(week.end);
        const isCurrentWeek = today >= weekStart && today <= weekEnd;
        
        const rowTotal = Object.values(weekData).reduce((sum, val) => sum + val, 0);
        
        html += `
            <tr ${isCurrentWeek ? 'class="table-primary"' : ''}>
                <td>${week.label}</td>
                ${modules.map(m => `<td class="text-end">${(weekData[m] || 0).toFixed(1)}</td>`).join('')}
                <td class="text-end"><strong>${rowTotal.toFixed(1)}</strong></td>
            </tr>
        `;
    }
    
    tbody.innerHTML = html;
}

function scrollWeeks(delta) {
    const container = document.getElementById('weeklyModuleTableContainer');
    if (!container) return;
    
    let currentIndex = parseInt(container.dataset.currentIndex);
    const totalWeeks = parseInt(container.dataset.totalWeeks);
    
    currentIndex = Math.max(0, Math.min(currentIndex + delta, totalWeeks - 1));
    container.dataset.currentIndex = currentIndex;
    
    fetch('/api/weekly-module-hours')
        .then(r => r.json())
        .then(tableData => {
            const { weeks, modules, data } = tableData;
            renderWeekRows(weeks, modules, data, currentIndex);
            
            document.querySelector('.d-flex span').innerHTML = 
                `<strong>Viewing weeks ${currentIndex + 1}-${Math.min(currentIndex + 10, weeks.length)} of ${weeks.length}</strong>`;
            
            document.getElementById('prevWeeks').disabled = currentIndex === 0;
            document.getElementById('nextWeeks').disabled = currentIndex >= weeks.length - 10;
        });
}
