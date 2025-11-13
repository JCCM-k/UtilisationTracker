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
    showLoading(true);
    
    try {
        // Load all data in parallel
        const [projects, timelineData] = await Promise.all([
            fetchProjects(),
            fetchTimelineData()
        ]);
        
        allProjects = projects;
        allTimelineData = timelineData;
        
        // Populate UI components
        populateMetrics(projects);
        populateProjectFilter(projects);
        populateProjectsTable(projects);
        
        // Initialize timeline
        if (timelineData && timelineData.projects.length > 0) {
            timelineInstance = new ProjectTimeline('timelineCanvas', timelineData);
        } else {
            showNoDataMessage();
        }
        
        // Load charts
        await loadCharts();
        
        console.log('Dashboard loaded successfully');
    } catch (error) {
        console.error('Error loading dashboard:', error);
        showError('Failed to load dashboard data: ' + error.message);
    } finally {
        showLoading(false);
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
        option.textContent = `${project.customername} - ${project.projectname}`;
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
            <td>${escapeHtml(project.customername || 'N/A')}</td>
            <td>${escapeHtml(project.projectname || 'N/A')}</td>
            <td>${formatDate(project.projectstartdate)}</td>
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
// TIMELINE VISUALIZATION CLASS
// ============================================================================

class ProjectTimeline {
    constructor(canvasId, data) {
        this.canvas = document.getElementById(canvasId);
        this.ctx = this.canvas.getContext('2d');
        this.data = data;
        this.granularity = 'weekly';
        this.filteredProjectIds = null;
        
        this.setupCanvas();
        this.calculateDimensions();
        this.render();
        this.attachEventListeners();
    }
    
    setupCanvas() {
        const container = this.canvas.parentElement;
        this.canvas.width = container.offsetWidth - 40;
        this.canvas.height = Math.max(400, this.data.projects.length * 50 + 150);
    }
    
    calculateDimensions() {
        this.padding = { top: 60, right: 20, bottom: 40, left: 250 };
        this.plotWidth = this.canvas.width - this.padding.left - this.padding.right;
        this.plotHeight = this.canvas.height - this.padding.top - this.padding.bottom;
        this.rowHeight = 40;
        
        // Calculate time range
        this.startDate = new Date(this.data.dateRange.minDate);
        this.endDate = new Date(this.data.dateRange.maxDate);
        this.totalDays = Math.ceil((this.endDate - this.startDate) / (1000 * 60 * 60 * 24));
    }
    
    render() {
        this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
        
        this.drawTimeAxis();
        this.drawProjectBars();
        this.drawGridLines();
    }
    
    drawTimeAxis() {
        const periods = this.getTimePeriods();
        const periodWidth = this.plotWidth / periods.length;
        
        this.ctx.font = 'bold 14px sans-serif';
        this.ctx.fillStyle = '#333';
        this.ctx.textAlign = 'center';
        
        periods.forEach((period, index) => {
            const x = this.padding.left + (index * periodWidth) + (periodWidth / 2);
            const y = this.padding.top - 10;
            
            this.ctx.fillText(period.label, x, y);
        });
        
        // Draw axis line
        this.ctx.beginPath();
        this.ctx.moveTo(this.padding.left, this.padding.top);
        this.ctx.lineTo(this.padding.left + this.plotWidth, this.padding.top);
        this.ctx.strokeStyle = '#333';
        this.ctx.lineWidth = 2;
        this.ctx.stroke();
    }
    
    getTimePeriods() {
        const periods = [];
        let currentDate = new Date(this.startDate);
        
        if (this.granularity === 'weekly') {
            while (currentDate <= this.endDate) {
                const weekNum = this.getWeekNumber(currentDate);
                periods.push({
                    label: `W${weekNum}`,
                    date: new Date(currentDate)
                });
                currentDate.setDate(currentDate.getDate() + 7);
            }
        } else if (this.granularity === 'monthly') {
            while (currentDate <= this.endDate) {
                periods.push({
                    label: currentDate.toLocaleDateString('en-US', { month: 'short' }),
                    date: new Date(currentDate)
                });
                currentDate.setMonth(currentDate.getMonth() + 1);
            }
        } else if (this.granularity === 'quarterly') {
            while (currentDate <= this.endDate) {
                const quarter = Math.floor(currentDate.getMonth() / 3) + 1;
                periods.push({
                    label: `Q${quarter} ${currentDate.getFullYear()}`,
                    date: new Date(currentDate)
                });
                currentDate.setMonth(currentDate.getMonth() + 3);
            }
        }
        
        return periods;
    }
    
    drawGridLines() {
        const periods = this.getTimePeriods();
        const periodWidth = this.plotWidth / periods.length;
        
        this.ctx.strokeStyle = '#E0E0E0';
        this.ctx.lineWidth = 1;
        
        for (let i = 0; i <= periods.length; i++) {
            const x = this.padding.left + (i * periodWidth);
            
            this.ctx.beginPath();
            this.ctx.moveTo(x, this.padding.top);
            this.ctx.lineTo(x, this.padding.top + this.plotHeight);
            this.ctx.stroke();
        }
    }
    
    drawProjectBars() {
        const projects = this.getFilteredProjects();
        
        projects.forEach((project, index) => {
            const y = this.padding.top + (index * this.rowHeight) + 10;
            
            // Draw project label
            this.ctx.font = '13px sans-serif';
            this.ctx.fillStyle = '#333';
            this.ctx.textAlign = 'left';
            this.ctx.fillText(
                this.truncateText(project.projectName, 30),
                10,
                y + 20
            );
            
            // Draw phase bars
            if (project.phases) {
                project.phases.forEach(phase => {
                    this.drawPhaseBar(phase, y);
                });
            }
        });
    }
    
    drawPhaseBar(phase, y) {
        const startX = this.dateToX(new Date(phase.startDate));
        const endX = this.dateToX(new Date(phase.endDate));
        const width = endX - startX;
        
        if (width < 1) return; // Skip if too small
        
        // Draw bar
        this.ctx.fillStyle = PHASE_COLORS[phase.phase] || '#CCCCCC';
        this.ctx.fillRect(startX, y, width, 30);
        
        // Draw border
        this.ctx.strokeStyle = 'rgba(0,0,0,0.2)';
        this.ctx.lineWidth = 1;
        this.ctx.strokeRect(startX, y, width, 30);
        
        // Draw phase label if wide enough
        if (width > 40) {
            this.ctx.fillStyle = '#FFF';
            this.ctx.font = 'bold 11px sans-serif';
            this.ctx.textAlign = 'center';
            this.ctx.fillText(phase.phase, startX + (width / 2), y + 19);
        }
    }
    
    dateToX(date) {
        const daysSinceStart = Math.ceil((date - this.startDate) / (1000 * 60 * 60 * 24));
        const ratio = daysSinceStart / this.totalDays;
        return this.padding.left + (ratio * this.plotWidth);
    }
    
    xToDate(x) {
        const relativeX = x - this.padding.left;
        const ratio = relativeX / this.plotWidth;
        const days = ratio * this.totalDays;
        const date = new Date(this.startDate);
        date.setDate(date.getDate() + Math.floor(days));
        return date;
    }
    
    getFilteredProjects() {
        if (this.filteredProjectIds) {
            return this.data.projects.filter(p => 
                this.filteredProjectIds.includes(p.projectId)
            );
        }
        return this.data.projects;
    }
    
    attachEventListeners() {
        this.canvas.addEventListener('click', (e) => {
            const rect = this.canvas.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;
            
            // Check if click is in plot area
            if (x >= this.padding.left && x <= this.padding.left + this.plotWidth &&
                y >= this.padding.top && y <= this.padding.top + this.plotHeight) {
                
                const clickedDate = this.xToDate(x);
                this.onDateClick(clickedDate);
            }
        });
        
        // Tooltip on hover
        this.canvas.addEventListener('mousemove', (e) => {
            const rect = this.canvas.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;
            
            this.canvas.style.cursor = 
                (x >= this.padding.left && x <= this.padding.left + this.plotWidth) 
                ? 'pointer' : 'default';
        });
    }
    
    async onDateClick(date) {
        console.log('Timeline clicked:', date);
        const weekNum = this.getWeekNumber(date);
        await showModuleDrillDown(date, weekNum);
    }
    
    setGranularity(granularity) {
        this.granularity = granularity;
        this.render();
    }
    
    filterByProject(projectId) {
        this.filteredProjectIds = [projectId];
        this.render();
    }
    
    showAllProjects() {
        this.filteredProjectIds = null;
        this.render();
    }
    
    updateDateRange() {
        const startDate = document.getElementById('startDate').valueAsDate;
        const endDate = document.getElementById('endDate').valueAsDate;
        
        if (startDate && endDate) {
            this.startDate = startDate;
            this.endDate = endDate;
            this.totalDays = Math.ceil((this.endDate - this.startDate) / (1000 * 60 * 60 * 24));
            this.render();
        }
    }
    
    getWeekNumber(date) {
        const d = new Date(date);
        d.setHours(0, 0, 0, 0);
        d.setDate(d.getDate() + 4 - (d.getDay() || 7));
        const yearStart = new Date(d.getFullYear(), 0, 1);
        return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
    }
    
    truncateText(text, maxLength) {
        return text.length > maxLength ? text.substring(0, maxLength) + '...' : text;
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

async function loadStackedBarChart() {
    try {
        const granularity = document.querySelector('input[name="timeGranularity"]:checked').value;
        const response = await fetch(`/api/module-utilization?granularity=${granularity}&view=by-project`);
        const data = await response.json();
        
        if (stackedBarChartInstance) {
            stackedBarChartInstance.destroy();
        }
        
        const ctx = document.getElementById('stackedBarChart').getContext('2d');
        stackedBarChartInstance = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.periods || [],
                datasets: (data.projects || []).map((proj, idx) => ({
                    label: proj.projectName,
                    data: proj.hours,
                    backgroundColor: PROJECT_COLORS[idx % PROJECT_COLORS.length]
                }))
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: { 
                        stacked: true,
                        title: { display: true, text: 'Time Period' }
                    },
                    y: { 
                        stacked: true,
                        beginAtZero: true,
                        title: { display: true, text: 'Hours' }
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
        console.error('Error loading stacked bar chart:', error);
    }
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
