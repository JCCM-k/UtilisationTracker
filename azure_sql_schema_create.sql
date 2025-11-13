
-- ============================================================================
-- COMPLETE DATABASE SCHEMA CREATION SCRIPT
-- For Microsoft Azure SQL Database
-- Project Cost & Hours Tracking System
-- ============================================================================

-- Execute this script in order - dependencies are organized correctly
-- ============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ============================================================================
-- STEP 1: CREATE DIMENSION TABLES
-- ============================================================================

-- 1. Master Project Dimension
CREATE TABLE dim_project (
    project_id INT IDENTITY(1,1) PRIMARY KEY,
    customer_name NVARCHAR(255) NOT NULL,
    project_name NVARCHAR(255) NOT NULL,
    project_start_date DATE NOT NULL,
    project_status NVARCHAR(50) NOT NULL DEFAULT 'Draft',
    created_date DATETIME2 DEFAULT GETDATE(),
    modified_date DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT chk_project_status CHECK (project_status IN ('Draft', 'Active', 'On Hold', 'Completed', 'Cancelled'))
);

CREATE INDEX idx_customer_name ON dim_project(customer_name);
CREATE INDEX idx_project_name ON dim_project(project_name);
CREATE INDEX idx_project_status ON dim_project(project_status);
CREATE INDEX idx_project_start_date ON dim_project(project_start_date);
GO

-- 2. Module Dimension (Standardized HCM Modules)
CREATE TABLE dim_module (
    module_id INT IDENTITY(1,1) PRIMARY KEY,
    module_code NVARCHAR(50) NOT NULL UNIQUE,
    module_name NVARCHAR(255) NOT NULL,
    default_hourly_rate DECIMAL(10,2),
    standard_duration_weeks INT,
    is_active BIT NOT NULL DEFAULT 1,
    created_date DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT chk_default_rate_positive CHECK (default_hourly_rate IS NULL OR default_hourly_rate > 0),
    CONSTRAINT chk_duration_positive CHECK (standard_duration_weeks IS NULL OR standard_duration_weeks > 0)
);

CREATE INDEX idx_module_code ON dim_module(module_code);
CREATE INDEX idx_module_active ON dim_module(is_active);
GO

-- 3. Phase Dimension (Project Lifecycle Phases)
CREATE TABLE dim_phases (
    phase_id INT IDENTITY(1,1) PRIMARY KEY,
    phase_code NVARCHAR(50) NOT NULL UNIQUE,
    phase_name NVARCHAR(100) NOT NULL,
    default_sequence INT NOT NULL,
    created_date DATETIME2 DEFAULT GETDATE()
);

CREATE INDEX idx_phase_sequence ON dim_phases(default_sequence);
GO

-- Insert seed data for phases
INSERT INTO dim_phases (phase_code, phase_name, default_sequence) VALUES
('PM', 'Project Management', 1),
('PLAN', 'Planning', 2),
('AC', 'Analysis & Configuration', 3),
('TESTING', 'Testing', 4),
('DEPLOY', 'Deployment', 5),
('POST_GO_LIVE', 'Post Go-Live Support', 6);
GO

-- ============================================================================
-- STEP 2: CREATE FACT TABLES
-- ============================================================================

-- 4. Payment Milestone Cost Analysis
CREATE TABLE fact_cost_analysis_by_step (
    cost_analysis_id INT IDENTITY(1,1) PRIMARY KEY,
    project_id INT NOT NULL,
    payment_milestone NVARCHAR(255) NOT NULL,
    weight DECIMAL(5,4) NOT NULL,
    cost DECIMAL(18,4) NOT NULL,
    created_date DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT fk_cost_project FOREIGN KEY (project_id) 
        REFERENCES dim_project(project_id) ON DELETE CASCADE,
    CONSTRAINT chk_weight_range CHECK (weight BETWEEN 0 AND 1),
    CONSTRAINT chk_cost_positive CHECK (cost >= 0)
);

CREATE INDEX idx_project_cost ON fact_cost_analysis_by_step(project_id);
GO

-- 5. Module-Level Rate Card & Budget
CREATE TABLE fact_rate_calculation (
    rate_calc_id INT IDENTITY(1,1) PRIMARY KEY,
    project_id INT NOT NULL,
    module_id INT NOT NULL,
    budgeted_hours DECIMAL(10,2) NOT NULL,
    hourly_rate DECIMAL(10,2) NOT NULL,
    total_cost AS (budgeted_hours * hourly_rate) PERSISTED,
    created_date DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT fk_rate_project FOREIGN KEY (project_id) 
        REFERENCES dim_project(project_id) ON DELETE CASCADE,
    CONSTRAINT fk_rate_module FOREIGN KEY (module_id) 
        REFERENCES dim_module(module_id),
    CONSTRAINT chk_hours_positive CHECK (budgeted_hours > 0),
    CONSTRAINT chk_rate_positive CHECK (hourly_rate > 0),
    CONSTRAINT uq_project_module_rate UNIQUE (project_id, module_id)
);

CREATE INDEX idx_project_rate ON fact_rate_calculation(project_id);
CREATE INDEX idx_module_rate ON fact_rate_calculation(module_id);
GO

-- 6. Detailed Weekly Hours by Module and Phase
CREATE TABLE fact_module_phase_hours (
    hours_id INT IDENTITY(1,1) PRIMARY KEY,
    project_id INT NOT NULL,
    module_id INT NOT NULL,
    phase_id INT NOT NULL,
    week_number INT NOT NULL,
    module_start_date DATE NOT NULL,
    planned_hours DECIMAL(10,2) NOT NULL DEFAULT 0,
    module_weight DECIMAL(10,2),
    created_date DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT fk_hours_project FOREIGN KEY (project_id) 
        REFERENCES dim_project(project_id) ON DELETE CASCADE,
    CONSTRAINT fk_hours_module FOREIGN KEY (module_id) 
        REFERENCES dim_module(module_id),
    CONSTRAINT fk_hours_phase FOREIGN KEY (phase_id) 
        REFERENCES dim_phases(phase_id),
    CONSTRAINT chk_week_positive CHECK (week_number > 0),
    CONSTRAINT chk_planned_hours_nonnegative CHECK (planned_hours >= 0),
    CONSTRAINT uq_project_module_phase_week UNIQUE (project_id, module_id, phase_id, week_number)
);

CREATE INDEX idx_project_hours ON fact_module_phase_hours(project_id);
CREATE INDEX idx_module_hours ON fact_module_phase_hours(module_id);
CREATE INDEX idx_phase_hours ON fact_module_phase_hours(phase_id);
CREATE INDEX idx_week_number ON fact_module_phase_hours(week_number);
CREATE INDEX idx_module_start_date ON fact_module_phase_hours(module_start_date);
GO

-- 7. Project Timeline (Phase Duration Planning)
CREATE TABLE fact_project_timeline (
    timeline_id INT IDENTITY(1,1) PRIMARY KEY,
    project_id INT NOT NULL,
    phase_id INT NOT NULL,
    duration_weeks INT NOT NULL,
    start_date DATE,
    end_date DATE,
    created_date DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT fk_timeline_project FOREIGN KEY (project_id) 
        REFERENCES dim_project(project_id) ON DELETE CASCADE,
    CONSTRAINT fk_timeline_phase FOREIGN KEY (phase_id) 
        REFERENCES dim_phases(phase_id),
    CONSTRAINT chk_duration_positive CHECK (duration_weeks > 0),
    CONSTRAINT chk_timeline_dates CHECK (end_date IS NULL OR start_date IS NULL OR end_date >= start_date),
    CONSTRAINT uq_project_phase_timeline UNIQUE (project_id, phase_id)
);

CREATE INDEX idx_project_timeline ON fact_project_timeline(project_id);
CREATE INDEX idx_phase_timeline ON fact_project_timeline(phase_id);
GO

-- ============================================================================
-- STEP 3: CREATE UTILITY FUNCTION
-- ============================================================================

-- Function: Convert week number to calendar date range (excluding weekends)
CREATE FUNCTION dbo.fn_GetWeekDateRange(
    @project_start_date DATE,
    @week_number INT
)
RETURNS TABLE
AS
RETURN
(
    WITH WorkingDays AS (
        SELECT 
            working_day_number = ROW_NUMBER() OVER (ORDER BY date_value),
            date_value
        FROM (
            SELECT TOP (3650)
                date_value = DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1, @project_start_date)
            FROM sys.all_objects a CROSS JOIN sys.all_objects b
        ) dates
        WHERE DATEPART(WEEKDAY, date_value) NOT IN (1, 7)
    )
    SELECT 
        week_number = @week_number,
        week_start_date = MIN(date_value),
        week_end_date = MAX(date_value)
    FROM WorkingDays
    WHERE working_day_number BETWEEN ((@week_number - 1) * 5 + 1) AND (@week_number * 5)
);
GO

-- ============================================================================
-- STEP 4: CREATE ANALYTICAL VIEWS
-- ============================================================================

-- View 1: Weekly hours with calendar dates
CREATE VIEW vw_module_phase_hours_calendar AS
SELECT 
    h.hours_id,
    h.project_id,
    p.customer_name,
    p.project_name,
    p.project_status,
    h.module_id,
    m.module_code,
    m.module_name,
    h.phase_id,
    ph.phase_code,
    ph.phase_name,
    h.week_number,
    h.module_start_date,
    wd.week_start_date,
    wd.week_end_date,
    h.planned_hours,
    h.module_weight,
    h.created_date
FROM fact_module_phase_hours h
INNER JOIN dim_project p ON h.project_id = p.project_id
INNER JOIN dim_module m ON h.module_id = m.module_id
INNER JOIN dim_phases ph ON h.phase_id = ph.phase_id
CROSS APPLY dbo.fn_GetWeekDateRange(h.module_start_date, h.week_number) wd;
GO

-- View 2: Total hours across all projects per module (Resource Utilization)
CREATE VIEW vw_resource_utilization_by_module AS
SELECT 
    m.module_id,
    m.module_code,
    m.module_name,
    p.project_id,
    p.project_name,
    p.project_status,
    SUM(h.planned_hours) AS total_hours,
    COUNT(DISTINCT h.phase_id) AS phases_involved,
    MIN(h.module_start_date) AS earliest_start,
    MAX(wd.week_end_date) AS latest_end
FROM fact_module_phase_hours h
INNER JOIN dim_project p ON h.project_id = p.project_id
INNER JOIN dim_module m ON h.module_id = m.module_id
CROSS APPLY dbo.fn_GetWeekDateRange(h.module_start_date, h.week_number) wd
GROUP BY m.module_id, m.module_code, m.module_name, p.project_id, p.project_name, p.project_status;
GO

-- View 3: Concurrent project workload by calendar week
CREATE VIEW vw_concurrent_project_workload AS
SELECT 
    calendar_week_start = wd.week_start_date,
    calendar_week_end = wd.week_end_date,
    active_projects = COUNT(DISTINCT h.project_id),
    active_modules = COUNT(DISTINCT h.module_id),
    total_weekly_hours = SUM(h.planned_hours)
FROM fact_module_phase_hours h
CROSS APPLY dbo.fn_GetWeekDateRange(h.module_start_date, h.week_number) wd
INNER JOIN dim_project p ON h.project_id = p.project_id
WHERE p.project_status = 'Active'
GROUP BY wd.week_start_date, wd.week_end_date;
GO

-- View 4: Budget vs Actual comparison per module
CREATE VIEW vw_module_budget_summary AS
SELECT 
    p.project_id,
    p.customer_name,
    p.project_name,
    p.project_status,
    m.module_id,
    m.module_code,
    m.module_name,
    r.budgeted_hours,
    r.hourly_rate,
    r.total_cost AS budgeted_cost,
    actual_hours = ISNULL(SUM(h.planned_hours), 0),
    actual_cost = ISNULL(SUM(h.planned_hours), 0) * r.hourly_rate,
    hours_variance = r.budgeted_hours - ISNULL(SUM(h.planned_hours), 0),
    cost_variance = r.total_cost - (ISNULL(SUM(h.planned_hours), 0) * r.hourly_rate)
FROM fact_rate_calculation r
INNER JOIN dim_project p ON r.project_id = p.project_id
INNER JOIN dim_module m ON r.module_id = m.module_id
LEFT JOIN fact_module_phase_hours h ON r.project_id = h.project_id AND r.module_id = h.module_id
GROUP BY 
    p.project_id, p.customer_name, p.project_name, p.project_status,
    m.module_id, m.module_code, m.module_name,
    r.budgeted_hours, r.hourly_rate, r.total_cost;
GO

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Verify all tables were created
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME IN (
    'dim_project', 
    'dim_module', 
    'dim_phases',
    'fact_cost_analysis_by_step',
    'fact_rate_calculation',
    'fact_module_phase_hours',
    'fact_project_timeline'
)
ORDER BY TABLE_NAME;
GO

-- Verify all views were created
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_NAME LIKE 'vw_%'
ORDER BY TABLE_NAME;
GO

-- Verify phase seed data
SELECT * FROM dim_phases ORDER BY default_sequence;
GO

-- ============================================================================
-- SCHEMA CREATION COMPLETE
-- ============================================================================

PRINT 'Database schema created successfully!';
PRINT 'Tables created: 7';
PRINT 'Views created: 4';
PRINT 'Functions created: 1';
PRINT 'Phases seeded: 6';
GO
