// ============================================================================
// TIMELINE.JS - Standalone Timeline Visualization Component
// ============================================================================

/**
 * ProjectTimeline - Interactive timeline visualization for HCM projects
 * 
 * Features:
 * - Multiple time granularities (weekly, monthly, quarterly)
 * - Phase-based project visualization with color coding
 * - Click interactions for drill-down
 * - Project filtering
 * - Date range customization
 * - Responsive canvas rendering
 * 
 * @author HCM Utilization Dashboard Team
 * @version 1.0.0
 */

class ProjectTimeline {
    constructor(canvasId, data, options = {}) {
        // Core properties
        this.canvas = document.getElementById(canvasId);
        if (!this.canvas) {
            throw new Error(`Canvas element with id '${canvasId}' not found`);
        }
        
        this.ctx = this.canvas.getContext('2d');
        this.data = data;
        this.options = {
            granularity: options.granularity || 'weekly',
            rowHeight: options.rowHeight || 40,
            padding: options.padding || { top: 60, right: 20, bottom: 40, left: 250 },
            colors: options.colors || this.getDefaultColors(),
            onDateClick: options.onDateClick || null,
            onProjectClick: options.onProjectClick || null,
            showTooltips: options.showTooltips !== false,
            ...options
        };
        
        // State
        this.filteredProjectIds = null;
        this.hoveredElement = null;
        this.isDragging = false;
        this.dragStartX = 0;
        this.scrollOffsetX = 0;
        
        // Initialize
        this.setupCanvas();
        this.calculateDimensions();
        this.attachEventListeners();
        this.render();
    }
    
    // ========================================================================
    // INITIALIZATION
    // ========================================================================
    
    setupCanvas() {
        const container = this.canvas.parentElement;
        const containerWidth = container.offsetWidth - 40;
        
        // Set canvas size
        this.canvas.width = containerWidth;
        this.canvas.height = Math.max(
            400, 
            this.data.projects.length * this.options.rowHeight + 150
        );
        
        // Enable high DPI support
        const dpr = window.devicePixelRatio || 1;
        if (dpr > 1) {
            const rect = this.canvas.getBoundingClientRect();
            this.canvas.width = rect.width * dpr;
            this.canvas.height = rect.height * dpr;
            this.canvas.style.width = rect.width + 'px';
            this.canvas.style.height = rect.height + 'px';
            this.ctx.scale(dpr, dpr);
        }
    }
    
    calculateDimensions() {
        this.padding = this.options.padding;
        this.plotWidth = this.canvas.width - this.padding.left - this.padding.right;
        this.plotHeight = this.canvas.height - this.padding.top - this.padding.bottom;
        this.rowHeight = this.options.rowHeight;
        
        // Calculate time range
        this.startDate = new Date(this.data.dateRange.minDate);
        this.endDate = new Date(this.data.dateRange.maxDate);
        this.totalDays = this.calculateDaysBetween(this.startDate, this.endDate);
    }
    
    getDefaultColors() {
        return {
            'P+M': '#FF6B6B',
            'Plan': '#4ECDC4',
            'A+C': '#45B7D1',
            'Testing': '#FFA07A',
            'Deploy': '#98D8C8',
            'Post Go Live': '#A8E6CF',
            'default': '#CCCCCC'
        };
    }
    
    // ========================================================================
    // RENDERING
    // ========================================================================
    
    render() {
        // Clear canvas
        this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
        
        // Draw components in order
        this.drawBackground();
        this.drawGridLines();
        this.drawTimeAxis();
        this.drawProjectBars();
        this.drawToday();
        
        // Draw hover tooltip if applicable
        if (this.hoveredElement && this.options.showTooltips) {
            this.drawTooltip(this.hoveredElement);
        }
    }
    
    drawBackground() {
        // Canvas background
        this.ctx.fillStyle = '#FFFFFF';
        this.ctx.fillRect(0, 0, this.canvas.width, this.canvas.height);
        
        // Plot area background
        this.ctx.fillStyle = '#F8F9FA';
        this.ctx.fillRect(
            this.padding.left,
            this.padding.top,
            this.plotWidth,
            this.plotHeight
        );
    }
    
    drawGridLines() {
        const periods = this.getTimePeriods();
        const periodWidth = this.plotWidth / periods.length;
        
        this.ctx.strokeStyle = '#E0E0E0';
        this.ctx.lineWidth = 1;
        this.ctx.setLineDash([]);
        
        // Vertical grid lines
        for (let i = 0; i <= periods.length; i++) {
            const x = this.padding.left + (i * periodWidth);
            
            this.ctx.beginPath();
            this.ctx.moveTo(x, this.padding.top);
            this.ctx.lineTo(x, this.padding.top + this.plotHeight);
            this.ctx.stroke();
        }
        
        // Horizontal grid lines (between projects)
        const projects = this.getFilteredProjects();
        projects.forEach((_, index) => {
            const y = this.padding.top + ((index + 1) * this.rowHeight);
            
            this.ctx.beginPath();
            this.ctx.moveTo(this.padding.left, y);
            this.ctx.lineTo(this.padding.left + this.plotWidth, y);
            this.ctx.stroke();
        });
    }
    
    drawTimeAxis() {
        const periods = this.getTimePeriods();
        const periodWidth = this.plotWidth / periods.length;
        
        // Axis line
        this.ctx.beginPath();
        this.ctx.moveTo(this.padding.left, this.padding.top);
        this.ctx.lineTo(this.padding.left + this.plotWidth, this.padding.top);
        this.ctx.strokeStyle = '#333';
        this.ctx.lineWidth = 2;
        this.ctx.setLineDash([]);
        this.ctx.stroke();
        
        // Period labels
        this.ctx.font = 'bold 12px sans-serif';
        this.ctx.fillStyle = '#333';
        this.ctx.textAlign = 'center';
        
        periods.forEach((period, index) => {
            const x = this.padding.left + (index * periodWidth) + (periodWidth / 2);
            const y = this.padding.top - 15;
            
            this.ctx.fillText(period.label, x, y);
        });
        
        // Axis title
        this.ctx.font = 'bold 14px sans-serif';
        this.ctx.fillText(
            this.getAxisTitle(),
            this.padding.left + (this.plotWidth / 2),
            this.padding.top - 35
        );
    }
    
    drawProjectBars() {
        const projects = this.getFilteredProjects();
        
        projects.forEach((project, index) => {
            const y = this.padding.top + (index * this.rowHeight) + 5;
            
            // Draw project label
            this.drawProjectLabel(project, y);
            
            // Draw phase bars
            if (project.phases && Array.isArray(project.phases)) {
                project.phases.forEach(phase => {
                    this.drawPhaseBar(project, phase, y);
                });
            }
        });
    }
    
    drawProjectLabel(project, y) {
        this.ctx.font = '12px sans-serif';
        this.ctx.fillStyle = '#333';
        this.ctx.textAlign = 'left';
        this.ctx.textBaseline = 'middle';
        
        const labelText = this.truncateText(project.projectName, 30);
        const labelY = y + (this.rowHeight / 2) - 5;
        
        // Background for label
        const textWidth = this.ctx.measureText(labelText).width;
        this.ctx.fillStyle = 'rgba(255, 255, 255, 0.9)';
        this.ctx.fillRect(10, labelY - 10, textWidth + 10, 20);
        
        // Text
        this.ctx.fillStyle = '#333';
        this.ctx.fillText(labelText, 15, labelY);
    }
    
    drawPhaseBar(project, phase, y) {
        const phaseStartDate = new Date(phase.startDate);
        const phaseEndDate = new Date(phase.endDate);
        
        const startX = this.dateToX(phaseStartDate);
        const endX = this.dateToX(phaseEndDate);
        const width = Math.max(endX - startX, 2); // Minimum 2px width
        const height = 30;
        
        if (width < 1) return; // Skip if too small
        
        // Get color
        const color = this.options.colors[phase.phase] || this.options.colors.default;
        
        // Draw bar with gradient
        const gradient = this.ctx.createLinearGradient(startX, y, startX, y + height);
        gradient.addColorStop(0, color);
        gradient.addColorStop(1, this.darkenColor(color, 0.8));
        
        this.ctx.fillStyle = gradient;
        this.ctx.fillRect(startX, y, width, height);
        
        // Draw border
        this.ctx.strokeStyle = this.darkenColor(color, 0.6);
        this.ctx.lineWidth = 1;
        this.ctx.strokeRect(startX, y, width, height);
        
        // Store element for interaction
        if (!this.elements) this.elements = [];
        this.elements.push({
            type: 'phase',
            project: project,
            phase: phase,
            bounds: { x: startX, y: y, width: width, height: height }
        });
    }
    
    drawToday() {
        const today = new Date();
        
        // Only draw if today is within range
        if (today >= this.startDate && today <= this.endDate) {
            const x = this.dateToX(today);
            
            // Draw line
            this.ctx.strokeStyle = '#DC3545';
            this.ctx.lineWidth = 2;
            this.ctx.setLineDash([5, 5]);
            
            this.ctx.beginPath();
            this.ctx.moveTo(x, this.padding.top);
            this.ctx.lineTo(x, this.padding.top + this.plotHeight);
            this.ctx.stroke();
            
            this.ctx.setLineDash([]);
            
            // Draw label
            this.ctx.fillStyle = '#DC3545';
            this.ctx.font = 'bold 11px sans-serif';
            this.ctx.textAlign = 'center';
            this.ctx.fillText('TODAY', x, this.padding.top - 5);
        }
    }
    
    drawTooltip(element) {
        if (!element || !element.bounds) return;
        
        const padding = 10;
        const lineHeight = 16;
        let lines = [];
        
        if (element.type === 'phase') {
            lines = [
                `Project: ${element.project.projectName}`,
                `Phase: ${element.phase.phase}`,
                `Start: ${this.formatDate(element.phase.startDate)}`,
                `End: ${this.formatDate(element.phase.endDate)}`,
                `Duration: ${element.phase.durationWeeks} weeks`
            ];
        }
        
        // Calculate tooltip dimensions
        const maxWidth = Math.max(...lines.map(l => this.ctx.measureText(l).width));
        const tooltipWidth = maxWidth + (padding * 2);
        const tooltipHeight = (lines.length * lineHeight) + (padding * 2);
        
        // Position tooltip
        let tooltipX = element.bounds.x + (element.bounds.width / 2) - (tooltipWidth / 2);
        let tooltipY = element.bounds.y - tooltipHeight - 10;
        
        // Keep tooltip in bounds
        if (tooltipX < 10) tooltipX = 10;
        if (tooltipX + tooltipWidth > this.canvas.width - 10) {
            tooltipX = this.canvas.width - tooltipWidth - 10;
        }
        if (tooltipY < 10) {
            tooltipY = element.bounds.y + element.bounds.height + 10;
        }
        
        // Draw tooltip background
        this.ctx.fillStyle = 'rgba(0, 0, 0, 0.9)';
        this.ctx.fillRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight);
        
        // Draw tooltip border
        this.ctx.strokeStyle = '#FFF';
        this.ctx.lineWidth = 1;
        this.ctx.strokeRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight);
        
        // Draw tooltip text
        this.ctx.fillStyle = '#FFF';
        this.ctx.font = '12px sans-serif';
        this.ctx.textAlign = 'left';
        this.ctx.textBaseline = 'top';
        
        lines.forEach((line, index) => {
            this.ctx.fillText(
                line,
                tooltipX + padding,
                tooltipY + padding + (index * lineHeight)
            );
        });
    }
    
    // ========================================================================
    // TIME CALCULATIONS
    // ========================================================================
    
    getTimePeriods() {
        const periods = [];
        let currentDate = new Date(this.startDate);
        
        switch (this.options.granularity) {
            case 'weekly':
                while (currentDate <= this.endDate) {
                    const weekNum = this.getWeekNumber(currentDate);
                    const year = currentDate.getFullYear();
                    periods.push({
                        label: `W${weekNum}`,
                        date: new Date(currentDate),
                        fullLabel: `Week ${weekNum}, ${year}`
                    });
                    currentDate.setDate(currentDate.getDate() + 7);
                }
                break;
                
            case 'monthly':
                while (currentDate <= this.endDate) {
                    periods.push({
                        label: currentDate.toLocaleDateString('en-US', { 
                            month: 'short',
                            year: '2-digit'
                        }),
                        date: new Date(currentDate),
                        fullLabel: currentDate.toLocaleDateString('en-US', { 
                            month: 'long',
                            year: 'numeric'
                        })
                    });
                    currentDate.setMonth(currentDate.getMonth() + 1);
                }
                break;
                
            case 'quarterly':
                while (currentDate <= this.endDate) {
                    const quarter = Math.floor(currentDate.getMonth() / 3) + 1;
                    const year = currentDate.getFullYear();
                    periods.push({
                        label: `Q${quarter} '${year.toString().slice(-2)}`,
                        date: new Date(currentDate),
                        fullLabel: `Q${quarter} ${year}`
                    });
                    currentDate.setMonth(currentDate.getMonth() + 3);
                }
                break;
        }
        
        return periods;
    }
    
    getAxisTitle() {
        const year = this.startDate.getFullYear();
        switch (this.options.granularity) {
            case 'weekly':
                return `Weekly Timeline - ${year}`;
            case 'monthly':
                return `Monthly Timeline - ${year}`;
            case 'quarterly':
                return `Quarterly Timeline - ${year}`;
            default:
                return 'Project Timeline';
        }
    }
    
    dateToX(date) {
        const daysSinceStart = this.calculateDaysBetween(this.startDate, date);
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
    
    calculateDaysBetween(date1, date2) {
        const msPerDay = 1000 * 60 * 60 * 24;
        return Math.ceil((date2 - date1) / msPerDay);
    }
    
    getWeekNumber(date) {
        const d = new Date(date);
        d.setHours(0, 0, 0, 0);
        d.setDate(d.getDate() + 4 - (d.getDay() || 7));
        const yearStart = new Date(d.getFullYear(), 0, 1);
        return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
    }
    
    // ========================================================================
    // EVENT HANDLING
    // ========================================================================
    
    attachEventListeners() {
        // Click events
        this.canvas.addEventListener('click', this.handleClick.bind(this));
        
        // Mouse move for tooltips
        this.canvas.addEventListener('mousemove', this.handleMouseMove.bind(this));
        
        // Mouse leave to clear tooltips
        this.canvas.addEventListener('mouseleave', this.handleMouseLeave.bind(this));
        
        // Window resize
        window.addEventListener('resize', this.debounce(() => {
            this.setupCanvas();
            this.calculateDimensions();
            this.render();
        }, 250));
    }
    
    handleClick(event) {
        const rect = this.canvas.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;
        
        // Check if clicked on a phase
        const clickedElement = this.findElementAtPosition(x, y);
        
        if (clickedElement) {
            if (clickedElement.type === 'phase' && this.options.onProjectClick) {
                this.options.onProjectClick(clickedElement.project, clickedElement.phase);
            }
        } else if (this.isInPlotArea(x, y)) {
            // Clicked on timeline background
            const clickedDate = this.xToDate(x);
            if (this.options.onDateClick) {
                this.options.onDateClick(clickedDate, this.getWeekNumber(clickedDate));
            }
        }
    }
    
    handleMouseMove(event) {
        const rect = this.canvas.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;
        
        const hoveredElement = this.findElementAtPosition(x, y);
        
        // Update cursor
        this.canvas.style.cursor = (hoveredElement || this.isInPlotArea(x, y)) 
            ? 'pointer' : 'default';
        
        // Update tooltip
        if (hoveredElement !== this.hoveredElement) {
            this.hoveredElement = hoveredElement;
            this.render();
        }
    }
    
    handleMouseLeave() {
        if (this.hoveredElement) {
            this.hoveredElement = null;
            this.render();
        }
        this.canvas.style.cursor = 'default';
    }
    
    findElementAtPosition(x, y) {
        if (!this.elements) return null;
        
        // Iterate in reverse order (top elements first)
        for (let i = this.elements.length - 1; i >= 0; i--) {
            const el = this.elements[i];
            if (x >= el.bounds.x && x <= el.bounds.x + el.bounds.width &&
                y >= el.bounds.y && y <= el.bounds.y + el.bounds.height) {
                return el;
            }
        }
        
        return null;
    }
    
    isInPlotArea(x, y) {
        return x >= this.padding.left && 
               x <= this.padding.left + this.plotWidth &&
               y >= this.padding.top && 
               y <= this.padding.top + this.plotHeight;
    }
    
    // ========================================================================
    // PUBLIC API
    // ========================================================================
    
    setGranularity(granularity) {
        if (['weekly', 'monthly', 'quarterly'].includes(granularity)) {
            this.options.granularity = granularity;
            this.render();
        }
    }
    
    filterByProject(projectId) {
        this.filteredProjectIds = [projectId];
        this.render();
    }
    
    showAllProjects() {
        this.filteredProjectIds = null;
        this.render();
    }
    
    updateDateRange(startDate, endDate) {
        if (startDate) this.startDate = new Date(startDate);
        if (endDate) this.endDate = new Date(endDate);
        
        this.totalDays = this.calculateDaysBetween(this.startDate, this.endDate);
        this.render();
    }
    
    updateData(newData) {
        this.data = newData;
        this.calculateDimensions();
        this.setupCanvas();
        this.render();
    }
    
    getFilteredProjects() {
        if (this.filteredProjectIds && this.filteredProjectIds.length > 0) {
            return this.data.projects.filter(p => 
                this.filteredProjectIds.includes(p.projectId)
            );
        }
        return this.data.projects;
    }
    
    refresh() {
        this.elements = [];
        this.render();
    }
    
    destroy() {
        // Remove event listeners
        this.canvas.removeEventListener('click', this.handleClick);
        this.canvas.removeEventListener('mousemove', this.handleMouseMove);
        this.canvas.removeEventListener('mouseleave', this.handleMouseLeave);
        
        // Clear canvas
        this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
    }
    
    // ========================================================================
    // UTILITY FUNCTIONS
    // ========================================================================
    
    truncateText(text, maxLength) {
        if (!text) return '';
        return text.length > maxLength 
            ? text.substring(0, maxLength) + '...' 
            : text;
    }
    
    formatDate(dateString) {
        const date = new Date(dateString);
        return date.toLocaleDateString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric'
        });
    }
    
    darkenColor(color, factor) {
        // Convert hex to RGB
        const hex = color.replace('#', '');
        const r = parseInt(hex.substring(0, 2), 16);
        const g = parseInt(hex.substring(2, 4), 16);
        const b = parseInt(hex.substring(4, 6), 16);
        
        // Darken
        const newR = Math.floor(r * factor);
        const newG = Math.floor(g * factor);
        const newB = Math.floor(b * factor);
        
        // Convert back to hex
        return '#' + [newR, newG, newB]
            .map(x => x.toString(16).padStart(2, '0'))
            .join('');
    }
    
    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }
}

// ============================================================================
// EXPORT
// ============================================================================

// Export for use in modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ProjectTimeline;
}

// Make available globally
if (typeof window !== 'undefined') {
    window.ProjectTimeline = ProjectTimeline;
}

console.log('Timeline.js loaded successfully');
