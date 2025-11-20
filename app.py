import os
from werkzeug.utils import secure_filename
from flask import Flask, render_template, request, jsonify, session
from db_ops import AzureSQLDBManager
from excel_parser import ExcelTableExtractor
import pandas as pd
import io 
from datetime import datetime, timedelta
import numpy as np

app = Flask(__name__)
global db_manager
db_manager = AzureSQLDBManager(
    server="project-utilisation.database.windows.net",
    database="Utilisation_tracker_db",
    username="kliqtek-tester",
    password="cl@r1tythr0ughkn0wl3dg3",
    pool_size=15,
    max_overflow=5,
    pool_timeout=60,
    log_level='INFO',
    authentication_method='SqlPassword'
)

app.config['UPLOAD_FOLDER'] = '/tmp/uploads'
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024  # 10MB max file size
app.config['SECRET_KEY'] = 'your-secret-key-here'  # Required for session management

# Ensure upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def validate_and_convert_data(df, table_type):
    """
    Validate and convert DataFrame columns to correct types based on SQL schema.
    """
    if df is None or df.empty:
        return df
    
    df_clean = df.copy()
    
    try:
        if table_type == 'cost':
            # Convert to numeric
            df_clean['weight'] = pd.to_numeric(df_clean['weight'], errors='coerce')
            df_clean['cost'] = pd.to_numeric(df_clean['cost'], errors='coerce')
            
            # Remove rows with NaN values in REQUIRED fields
            df_clean = df_clean.dropna(subset=['weight', 'cost'])
            
            if df_clean.empty:
                raise ValueError("No valid numeric data for cost analysis")
            
            # Validate constraints
            if (df_clean['weight'] < 0).any() or (df_clean['weight'] > 1).any():
                invalid_rows = df_clean[(df_clean['weight'] < 0) | (df_clean['weight'] > 1)]
                raise ValueError(f"Weight must be between 0 and 1. Invalid values: {invalid_rows['weight'].tolist()}")
            
            if (df_clean['cost'] < 0).any():
                raise ValueError("Cost must be non-negative")
            
            df_clean['payment_milestone'] = df_clean['payment_milestone'].astype(str)
            
        elif table_type == 'hours':
            # Log initial state
            app.logger.info(f"Hours data before filtering: {len(df_clean)} rows")
            app.logger.info(f"Hours sample row: {df_clean.iloc[0].to_dict() if len(df_clean) > 0 else 'empty'}")
            
            # Filter out summary/total rows BEFORE validation
            if 'module_name' in df_clean.columns:
                df_clean['module_name'] = df_clean['module_name'].astype(str)
                app.logger.info(f"Module names: {df_clean['module_name'].tolist()}")
                
                # Remove summary rows
                summary_keywords = ['SUMS', 'TOTAL', 'WEEKS EFFORT', 'SUBTOTAL']
                df_clean = df_clean[~df_clean['module_name'].str.upper().isin(summary_keywords)]
                app.logger.info(f"After filtering summary rows: {len(df_clean)} rows remaining")
            
            if df_clean.empty:
                raise ValueError("No valid data rows (only summary rows found)")
            
            # Convert numeric columns
            numeric_cols = ['weight', 'p_plus_m', 'plan', 'a_plus_c', 'testing', 'deploy', 'post_go_live', 'total_hours']
            
            for col in numeric_cols:
                if col in df_clean.columns:
                    app.logger.info(f"Converting column {col}, sample values before: {df_clean[col].head(3).tolist()}")
                    # Replace empty strings with NaN before conversion
                    df_clean[col] = df_clean[col].replace('', np.nan)
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                    app.logger.info(f"After conversion, non-null count for {col}: {df_clean[col].notna().sum()}")
            
            # Check if we have at least SOME numeric data
            hour_cols = [c for c in numeric_cols if c in df_clean.columns]
            has_data = df_clean[hour_cols].notna().any().any()
            
            app.logger.info(f"Hour columns checked: {hour_cols}")
            app.logger.info(f"Has any non-null data: {has_data}")
            app.logger.info(f"DataFrame after conversion:\n{df_clean.head()}")
            
            if not has_data:
                raise ValueError("No valid numeric data for hours analysis")
            
            # Validate hours are non-negative (only check non-NaN values)
            for col in hour_cols:
                non_null_values = df_clean[col].dropna()
                if len(non_null_values) > 0 and (non_null_values < 0).any():
                    raise ValueError(f"{col} must be non-negative")
                    
        elif table_type == 'timeline':
            df_clean['duration_weeks'] = pd.to_numeric(df_clean['duration_weeks'], errors='coerce')
            df_clean = df_clean.dropna(subset=['duration_weeks'])
            
            if df_clean.empty:
                raise ValueError("No valid numeric data for timeline")
            
            df_clean['duration_weeks'] = df_clean['duration_weeks'].astype(int)
            
            if (df_clean['duration_weeks'] <= 0).any():
                invalid_rows = df_clean[df_clean['duration_weeks'] <= 0]
                raise ValueError(f"Duration weeks must be greater than 0. Invalid values: {invalid_rows['duration_weeks'].tolist()}")
            
            if 'phase_name' in df_clean.columns:
                df_clean['phase_name'] = df_clean['phase_name'].astype(str)
                
        elif table_type == 'rate':
            # Map hours to budgeted_hours
            if 'hours' in df_clean.columns and 'budgeted_hours' not in df_clean.columns:
                df_clean['budgeted_hours'] = df_clean['hours']
            
            df_clean['budgeted_hours'] = pd.to_numeric(df_clean['budgeted_hours'], errors='coerce')
            df_clean['hourly_rate'] = pd.to_numeric(df_clean['hourly_rate'], errors='coerce')
            
            df_clean = df_clean.dropna(subset=['budgeted_hours', 'hourly_rate'])
            
            if df_clean.empty:
                raise ValueError("No valid numeric data for rate calculation")
            
            # Validate > 0
            if (df_clean['budgeted_hours'] <= 0).any():
                invalid_rows = df_clean[df_clean['budgeted_hours'] <= 0]
                raise ValueError(f"Budgeted hours must be greater than 0. Invalid values: {invalid_rows['budgeted_hours'].tolist()}")
            
            if (df_clean['hourly_rate'] <= 0).any():
                invalid_rows = df_clean[df_clean['hourly_rate'] <= 0]
                raise ValueError(f"Hourly rate must be greater than 0. Invalid values: {invalid_rows['hourly_rate'].tolist()}")
            
            df_clean['total_cost'] = df_clean['budgeted_hours'] * df_clean['hourly_rate']
            
            if 'module_name' in df_clean.columns:
                df_clean['module_name'] = df_clean['module_name'].astype(str)
        
        else:
            raise ValueError(f"Unknown table type: {table_type}")
        
        return df_clean
        
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Data validation failed for {table_type}: {str(e)}")

def calculate_week_number(date):
    """
    Calculate ISO 8601 week number (1-53).
    Week 1 contains first Thursday; weeks start Monday.
    
    Args:
        date: datetime or string (YYYY-MM-DD)
    Returns:
        int: ISO week number (1-53)
    """
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d')
    return date.isocalendar()[1]


def get_phase_color(phase_code):
    """
    Map phase code to hex color matching frontend PHASE_COLORS.
    
    Args:
        phase_code: str (e.g., 'PM', 'Plan', 'AC')
    Returns:
        str: Hex color code with # prefix
    """
    PHASE_COLOR_MAP = {
        'PM': '#FF6B6B',
        'Plan': '#4ECDC4',
        'AC': '#45B7D1',
        'Testing': '#FFA07A',
        'Deploy': '#98D8C8',
        'Post Go Live': '#A8E6CF'
    }
    return PHASE_COLOR_MAP.get(phase_code, '#CCCCCC')


def calculate_date_range(df):
    """
    Calculate min/max dates from timeline DataFrame with buffer.
    
    Args:
        df: DataFrame with 'projectstartdate', 'startdate', 'enddate' columns
    Returns:
        dict: {'minDate': 'YYYY-MM-DD', 'maxDate': 'YYYY-MM-DD'}
    """
    if df.empty:
        today = datetime.now()
        return {
            'minDate': today.strftime('%Y-%m-%d'),
            'maxDate': (today + timedelta(weeks=4)).strftime('%Y-%m-%d')
        }
    
    # Find earliest and latest dates
    min_date = pd.to_datetime(
        df[['projectstartdate', 'startdate']].min().min()
    )
    max_date = pd.to_datetime(df['enddate'].max())
    
    # Add 1-week buffers for visualization padding
    min_date -= timedelta(weeks=1)
    max_date += timedelta(weeks=1)
    
    return {
        'minDate': min_date.strftime('%Y-%m-%d'),
        'maxDate': max_date.strftime('%Y-%m-%d')
    }

def get_date_range_from_request():
    """
    Extract and validate date range from request parameters.
    Defaults to current year if not provided.
    """
    
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    # Default to current year if not provided
    if not start_date:
        start_date = datetime.now().replace(month=1, day=1).strftime('%Y-%m-%d')
    if not end_date:
        end_date = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
    
    return start_date, end_date

def aggregate_hours_by_module(granularity='weekly', start_date=None, end_date=None, project_id = "all"):
    """
    Aggregate module phase hours by time period within date range.
    Returns data suitable for Chart.js line chart.
    """
    try:
        from datetime import datetime, timedelta
        
        # Handle default dates
        if not start_date:
            start_date = datetime.now().date()
        if not end_date:
            end_date = (datetime.now() + timedelta(days=365)).date()
        
        # Convert string dates to date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        app.logger.info(f"===== AGGREGATE_HOURS_BY_MODULE =====")
        app.logger.info(f"Project ID parameter: {project_id}")
        
        # Build project filter clause
        project_filter = ""
        if project_id != 'all':
            try:
                project_id_int = int(project_id)
                project_filter = f"AND project_id = {project_id_int}"
                app.logger.info(f"Project filter clause: {project_filter}")
            except ValueError:
                app.logger.warning(f"Invalid project_id: {project_id}")
        else:
            app.logger.info("No project filter - showing all projects")

        app.logger.info(f"Aggregating by module {granularity}, {start_date} to {end_date}")

        # Build query based on granularity
        if granularity == 'weekly':
            query = f"""
            SELECT
                module_name,
                week_start_date,
                week_end_date,
                SUM(planned_hours) AS total_hours
            FROM vw_module_phase_hours_calendar
            WHERE week_start_date BETWEEN '{start_date}' AND '{end_date}'
                AND planned_hours IS NOT NULL
                AND project_status = 'Active'
                {project_filter}
                AND module_name NOT IN ('SUMS', 'Sums', 'sums',
                    'TOTAL', 'Total', 'total',
                    'WEEKS EFFORT', 'Weeks Effort', 'weeks effort',
                    'WEEK EFFORT', 'Week Effort', 'week effort',
                    'SUBTOTAL', 'Subtotal', 'subtotal')
                AND UPPER(module_name) NOT LIKE '%SUM%'
                AND UPPER(module_name) NOT LIKE '%TOTAL%'
                AND UPPER(module_name) NOT LIKE '%EFFORT%'
            GROUP BY module_name, week_start_date, week_end_date
            ORDER BY week_start_date, module_name
            """
        elif granularity == 'monthly':
            query = f"""
            SELECT
                module_name,
                FORMAT(week_start_date, 'yyyy-MM') AS period,
                SUM(planned_hours) AS total_hours
            FROM vw_module_phase_hours_calendar
            WHERE week_start_date BETWEEN '{start_date}' AND '{end_date}'
                AND planned_hours IS NOT NULL
                AND project_status = 'Active'
                {project_filter}
                AND module_name NOT IN ('SUMS', 'TOTAL', 'WEEKS EFFORT', 'WEEK EFFORT', 'SUBTOTAL')
                AND UPPER(module_name) NOT LIKE '%SUM%'
                AND UPPER(module_name) NOT LIKE '%TOTAL%'
                AND UPPER(module_name) NOT LIKE '%EFFORT%'
            GROUP BY module_name, FORMAT(week_start_date, 'yyyy-MM')
            ORDER BY FORMAT(week_start_date, 'yyyy-MM'), module_name
            """
        else:  # quarterly
            query = f"""
            SELECT
                module_name,
                CONCAT(YEAR(week_start_date), '-Q', DATEPART(QUARTER, week_start_date)) AS period,
                SUM(planned_hours) AS total_hours
            FROM vw_module_phase_hours_calendar
            WHERE week_start_date BETWEEN '{start_date}' AND '{end_date}'
                AND planned_hours IS NOT NULL
                AND project_status = 'Active'
                {project_filter}
                AND module_name NOT IN ('SUMS', 'TOTAL', 'WEEKS EFFORT', 'WEEK EFFORT', 'SUBTOTAL')
                AND UPPER(module_name) NOT LIKE '%SUM%'
                AND UPPER(module_name) NOT LIKE '%TOTAL%'
                AND UPPER(module_name) NOT LIKE '%EFFORT%'
            GROUP BY module_name, CONCAT(YEAR(week_start_date), '-Q', DATEPART(QUARTER, week_start_date))
            ORDER BY CONCAT(YEAR(week_start_date), '-Q', DATEPART(QUARTER, week_start_date)), module_name
            """

        df = db_manager.execute_custom_query(query)

        if df.empty:
            app.logger.warning("No data returned from aggregate_hours_by_module query")
            return {
                'labels': [],
                'datasets': [],
                'dateRange': {'startDate': str(start_date), 'endDate': str(end_date)}
            }

        # ✓ FIXED: Generate Monday-aligned weeks instead of using raw database dates
        if granularity == 'weekly':
            all_periods = generate_week_labels(start_date, end_date)
        elif granularity == 'monthly':
            all_periods = generate_month_labels(start_date, end_date)
        else:
            all_periods = generate_quarter_labels(start_date, end_date)

        # Get unique modules
        modules = sorted(df['module_name'].unique())
        app.logger.info(f"Found {len(modules)} modules: {modules}")
        app.logger.info(f"Found {len(all_periods)} periods")

        # ✓ HELPER FUNCTION: Normalize any date to its Monday
        def normalize_to_monday(date_obj):
            """Convert any date (Wed, Thu, etc.) to the Monday of that week"""
            if isinstance(date_obj, str):
                date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
            days_since_monday = date_obj.weekday()
            monday = date_obj - timedelta(days=days_since_monday)
            return monday.strftime('%Y-%m-%d')

        # Create dataset for each module
        datasets = []
        for module in modules:
            module_data = df[df['module_name'] == module]
            
            # ✓ FIXED: Normalize dates to Monday when creating the mapping
            hours_by_period = {}
            for _, row in module_data.iterrows():
                if granularity == 'weekly':
                    # Normalize Wednesday (or any day) to Monday
                    period_key = normalize_to_monday(row['week_start_date'])
                else:
                    period_key = row['period']
                
                # Accumulate hours if multiple rows map to same Monday
                if period_key in hours_by_period:
                    hours_by_period[period_key] += float(row['total_hours'])
                else:
                    hours_by_period[period_key] = float(row['total_hours'])

            # Fill in all periods with zeros for missing data
            data_values = [hours_by_period.get(period, 0) for period in all_periods]

            # Log the actual data for debugging
            non_zero = sum(1 for v in data_values if v > 0)
            total = sum(data_values)
            app.logger.info(f"Module '{module}': {non_zero}/{len(data_values)} non-zero weeks, total {total:.1f} hours")

            datasets.append({
                'label': module,
                'data': data_values,
                'borderColor': get_module_color(module),
                'backgroundColor': get_module_color(module),
                'fill': False,
                'tension': 0.1
            })

        app.logger.info(f"Generated {len(datasets)} datasets")

        return {
            'labels': all_periods,
            'datasets': datasets,
            'dateRange': {'startDate': str(start_date), 'endDate': str(end_date)}
        }

    except Exception as e:
        app.logger.error(f"Error in aggregate_hours_by_module: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'labels': [],
            'datasets': [],
            'dateRange': {'startDate': str(start_date) if start_date else '', 'endDate': str(end_date) if end_date else ''}
        }


def aggregate_hours_by_project(granularity='weekly', start_date=None, end_date=None, project_id="all"):
    """
    Aggregate module phase hours by project and time period within date range.
    Always returns a dictionary with labels and datasets.
    """
    try:
        from datetime import datetime, timedelta
        
        # Use provided dates or default to today onwards
        if not start_date:
            start_date = datetime.now().date()
        if not end_date:
            end_date = (datetime.now() + timedelta(days=365)).date()
        
        # Convert to datetime objects if they're strings
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        project_filter = ""
        if project_id != 'all':
            try:
                project_id_int = int(project_id)
                project_filter = f"AND project_id = {project_id_int}"
                app.logger.info(f"Filtering by project_id: {project_id_int}")
            except ValueError:
                app.logger.warning(f"Invalid project_id: {project_id}")

        print(f"Aggregating by project with granularity: {granularity}, dates: {start_date} to {end_date}")
        
        # Use the calendar view with date filtering
        if granularity == 'weekly':
            query = f"""
            SELECT
                project_name,
                week_start_date,
                week_end_date,
                SUM(planned_hours) AS total_hours
            FROM vw_module_phase_hours_calendar
            WHERE week_start_date BETWEEN '{start_date}' AND '{end_date}'
            AND planned_hours IS NOT NULL
            AND project_status = 'Active'
            {project_filter}
            GROUP BY project_name, week_start_date, week_end_date
            ORDER BY week_start_date, project_name
            """
        
        elif granularity == 'monthly':
            query = f"""
            SELECT
                project_name,
                FORMAT(week_start_date, 'yyyy-MM') AS period,
                MIN(week_start_date) as period_start,
                MAX(week_end_date) as period_end,
                SUM(planned_hours) AS total_hours
            FROM vw_module_phase_hours_calendar
            WHERE week_start_date BETWEEN '{start_date}' AND '{end_date}'
            AND planned_hours IS NOT NULL
            AND project_status = 'Active'
            {project_filter}
            GROUP BY project_name, FORMAT(week_start_date, 'yyyy-MM')
            ORDER BY FORMAT(week_start_date, 'yyyy-MM'), project_name
            """
        
        else:  # quarterly
            query = f"""
            SELECT
                project_name,
                CONCAT(YEAR(week_start_date), '-Q', DATEPART(QUARTER, week_start_date)) AS period,
                MIN(week_start_date) as period_start,
                MAX(week_end_date) as period_end,
                SUM(planned_hours) AS total_hours
            FROM vw_module_phase_hours_calendar
            WHERE week_start_date BETWEEN '{start_date}' AND '{end_date}'
            AND planned_hours IS NOT NULL
            AND project_status = 'Active'
            {project_filter}
            GROUP BY project_name, CONCAT(YEAR(week_start_date), '-Q', DATEPART(QUARTER, week_start_date))
            ORDER BY CONCAT(YEAR(week_start_date), '-Q', DATEPART(QUARTER, week_start_date)), project_name
            """
        
        # Execute query
        df = db_manager.execute_custom_query(query)
        
        if df.empty:
            print("No data returned from query")
            return {
                'labels': [],
                'datasets': [],
                'dateRange': {'startDate': str(start_date), 'endDate': str(end_date)}
            }
        
        print(f"Query returned {len(df)} rows")
        
        # Generate complete period list
        if granularity == 'weekly':
            all_periods = generate_week_labels(start_date, end_date)
        elif granularity == 'monthly':
            all_periods = generate_month_labels(start_date, end_date)
        else:
            all_periods = generate_quarter_labels(start_date, end_date)
        
        # Get unique projects
        projects = sorted(df['project_name'].unique())
        
        # Create dataset for each project
        datasets = []
        for project in projects:
            project_data = df[df['project_name'] == project]
            
            # Map hours to periods
            hours_by_period = {}
            for _, row in project_data.iterrows():
                if granularity == 'weekly':
                    period_key = row['week_start_date'].strftime('%Y-%m-%d')
                else:
                    period_key = row['period']
                hours_by_period[period_key] = float(row['total_hours'])
            
            # Fill in all periods (including zeros)
            data_values = [hours_by_period.get(period, 0) for period in all_periods]
            
            datasets.append({
                'label': project,
                'data': data_values,
                'backgroundColor': get_project_color(project),
                'borderColor': get_project_color(project),
                'fill': False
            })
        
        print(f"Generated {len(datasets)} datasets with {len(all_periods)} periods each")
        
        return {
            'labels': all_periods,
            'datasets': datasets,
            'dateRange': {'startDate': str(start_date), 'endDate': str(end_date)}
        }
        
    except Exception as e:
        print(f"Error in aggregate_hours_by_project: {str(e)}")
        import traceback
        traceback.print_exc()
        # ALWAYS return a dictionary, even on error
        return {
            'labels': [],
            'datasets': [],
            'dateRange': {'startDate': str(start_date) if start_date else '', 'endDate': str(end_date) if end_date else ''}
        }


def get_project_color(project_name):
    """Return consistent color for a project"""
    colors = [
        '#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8',
        '#A8E6CF', '#F06292', '#AED581', '#FFD54F', '#4DD0E1',
        '#9575CD', '#FF8A65'
    ]
    # Hash project name to get consistent color
    hash_val = sum(ord(c) for c in project_name)
    return colors[hash_val % len(colors)]
    
def get_enriched_projects(filter=True):
    """
    Get projects with aggregated module counts and total hours.
    Joins dim_customer to get customer_name since dim_project only has customer_id.
    
    Args:
        filter (bool): If True, only return Active projects. If False, return all projects.
    
    Returns DataFrame with enriched project data.
    """
    try:
        # Add WHERE clause to filter by Active status if filter=True
        where_clause = "WHERE p.project_status = 'Active'" if filter else ""
        
        query = f"""
            SELECT
                p.project_id AS "projectId",
                p.customer_id AS "customerId",
                c.customer_name AS "customerName",
                p.project_name AS "projectName",
                p.project_start_date AS "projectStartDate",
                p.project_status AS "status",
                COUNT(DISTINCT h.module_id) as "moduleCount",
                SUM(h.planned_hours) as "totalHours"
            FROM dim_project p
            INNER JOIN dim_customer c ON p.customer_id = c.customer_id
            LEFT JOIN fact_module_phase_hours h ON p.project_id = h.project_id
            {where_clause}
            GROUP BY p.project_id, p.customer_id, c.customer_name, p.project_name, p.project_start_date, p.project_status
            ORDER BY p.project_start_date DESC
        """
        
        df = db_manager.execute_custom_query(query)
        
        # SUM(NULL) results in NULL, so we replace it with 0
        # COUNT(DISTINCT NULL) correctly results in 0
        df['totalHours'] = df['totalHours'].fillna(0)
        
        # Format date for consistent JSON output
        df['projectStartDate'] = pd.to_datetime(df['projectStartDate']).dt.strftime('%Y-%m-%d')
        
        return df
    except Exception as e:
        app.logger.error(f"Error in get_enriched_projects: {str(e)}")
        return pd.DataFrame()


# ===== PAGE ROUTES =====
@app.route('/')
@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.route('/upload')
def upload_page():
    return render_template('upload.html')

@app.route('/edit')
def edit_page():
    return render_template('edit.html')

@app.route('/manage')
def manage_page():
    return render_template('manage.html')

# ===== DATA API ROUTES =====
@app.route('/api/customers', methods=['POST'])
def add_customer():
    customer_name = request.json.get('customerName', '').strip()
    if not customer_name:
        return jsonify({'success': False, 'error': 'Name required'}), 400
    
    if db_manager.execute_custom_query(
        f"SELECT 1 FROM dim_customer WHERE customer_name='{customer_name}'"
    ).shape[0] > 0:
        return jsonify({'success': False, 'error': 'Customer exists'}), 400
    
    db_manager.execute_custom_command(
        f"INSERT INTO dim_customer (customer_name) VALUES ('{customer_name}')"
    )
    return jsonify({'success': True})

@app.route('/api/customers/<int:customer_id>', methods=['PUT'])
def update_customer(customer_id):
    new_name = request.json.get('customerName', '').strip()
    if not new_name:
        return jsonify({'success': False, 'error': 'Name required'}), 400
    
    if db_manager.execute_custom_query(
        f"SELECT 1 FROM dim_customer WHERE customer_name='{new_name}' AND customer_id!={customer_id}"
    ).shape[0] > 0:
        return jsonify({'success': False, 'error': 'Customer exists'}), 400
    
    db_manager.execute_custom_command(
        f"UPDATE dim_customer SET customer_name='{new_name}', modified_date=GETDATE() WHERE customer_id={customer_id}"
    )
    return jsonify({'success': True})

@app.route('/api/customers/<int:customer_id>', methods=['DELETE'])
def delete_customer(customer_id):
    project_ids = db_manager.execute_custom_query(
        f"SELECT project_id FROM dim_project WHERE customer_id={customer_id}"
    )['project_id'].tolist()
    
    if project_ids:
        ids = ','.join(map(str, project_ids))
        for table in ['fact_cost_analysis_by_step', 'fact_module_phase_hours', 
                      'fact_project_timeline', 'fact_rate_calculation']:
            db_manager.execute_custom_command(f"DELETE FROM {table} WHERE project_id IN ({ids})")
    
    # Delete projects and customer (FK cascade handles this automatically if set up)
    db_manager.execute_custom_command(f"DELETE FROM dim_customer WHERE customer_id={customer_id}")
    return jsonify({'success': True})

@app.route('/api/projects', methods=['POST'])
def add_project():
    """Create new project"""
    d = request.json
    
    # DEBUG: Log incoming data
    app.logger.info(f"Received data: {d}")
    app.logger.info(f"customerId type: {type(d.get('customerId'))}, value: {d.get('customerId')}")
    
    # Validate customerId is an integer
    try:
        customer_id = int(d['customerId'])
    except (ValueError, KeyError):
        return jsonify({'success': False, 'error': 'Invalid customer ID'}), 400
    
    # Validate unique name
    if db_manager.execute_custom_query(
        f"SELECT 1 FROM dim_project WHERE project_name='{d['projectName']}'"
    ).shape[0] > 0:
        return jsonify({'success': False, 'error': 'Project name exists'}), 400
    
    db_manager.execute_custom_command(
        f"INSERT INTO dim_project (customer_id, project_name, project_start_date, project_status, created_date) "
        f"VALUES ({customer_id}, '{d['projectName']}', '{d['startDate']}', '{d['status']}', GETDATE())"
    )
    
    return jsonify({'success': True})

@app.route('/api/update-project/<int:project_id>', methods=['POST'])
def update_project(project_id):
    """Update project data"""
    try:
        data = request.json
        
        # Log incoming data
        app.logger.info(f"Received update request for project {project_id}")
        app.logger.info(f"Data keys: {data.keys() if data else 'None'}")
        
        # Extract table data
        cost_data = data.get('costAnalysis', [])
        hours_data = data.get('hoursAnalysis', [])
        timeline_data = data.get('timeline', [])
        rate_data = data.get('rateCalculation', [])
        
        app.logger.info(f"Cost rows: {len(cost_data)}, Hours rows: {len(hours_data)}, Timeline rows: {len(timeline_data)}, Rate rows: {len(rate_data)}")
        
        # Convert to DataFrames
        df_cost = pd.DataFrame(cost_data)
        df_hours = pd.DataFrame(hours_data)
        df_timeline = pd.DataFrame(timeline_data)
        df_rate = pd.DataFrame(rate_data)
        
        # Log DataFrame info
        app.logger.info(f"Cost DataFrame columns: {df_cost.columns.tolist()}")
        app.logger.info(f"Cost DataFrame sample: {df_cost.head(1).to_dict('records')}")
        app.logger.info(f"Hours DataFrame columns: {df_hours.columns.tolist()}")
        app.logger.info(f"Timeline DataFrame columns: {df_timeline.columns.tolist()}")
        app.logger.info(f"Rate DataFrame columns: {df_rate.columns.tolist()}")
        
        # Validate and convert data
        try:
            app.logger.info("Starting validation for cost data...")
            df_cost = validate_and_convert_data(df_cost, 'cost')
            app.logger.info(f"Cost validation passed, {len(df_cost)} rows")
            
            app.logger.info("Starting validation for hours data...")
            df_hours = validate_and_convert_data(df_hours, 'hours')
            app.logger.info(f"Hours validation passed, {len(df_hours)} rows")
            
            app.logger.info("Starting validation for timeline data...")
            df_timeline = validate_and_convert_data(df_timeline, 'timeline')
            app.logger.info(f"Timeline validation passed, {len(df_timeline)} rows")
            
            app.logger.info("Starting validation for rate data...")
            df_rate = validate_and_convert_data(df_rate, 'rate')
            app.logger.info(f"Rate validation passed, {len(df_rate)} rows")
            
        except ValueError as ve:
            app.logger.error(f"Validation error: {str(ve)}")
            return jsonify({
                'success': False,
                'error': f'Data validation failed: {str(ve)}'
            }), 400
        
        # Delete existing data
        app.logger.info(f"Deleting existing data for project {project_id}")
        db_manager.delete_cost_analysis(project_id)
        db_manager.delete_hours_analysis(project_id)
        db_manager.delete_timeline(project_id)
        db_manager.delete_rate_calculation(project_id)
        
        # Insert new data
        app.logger.info(f"Inserting new data for project {project_id}")
        cost_count = db_manager.bulk_insert_cost_analysis(project_id, df_cost)
        hours_count = db_manager.bulk_insert_hours_analysis(project_id, df_hours)
        timeline_count = db_manager.bulk_insert_timeline(project_id, df_timeline)
        rate_count = db_manager.bulk_insert_rate_calculation(project_id, df_rate)
        
        app.logger.info(f"Successfully updated project {project_id}: cost={cost_count}, hours={hours_count}, timeline={timeline_count}, rate={rate_count}")
        
        return jsonify({
            'success': True,
            'message': 'Project updated successfully',
            'recordsUpdated': {
                'costAnalysis': cost_count,
                'hoursAnalysis': hours_count,
                'timeline': timeline_count,
                'rateCalculation': rate_count
            }
        })
        
    except Exception as e:
        app.logger.error(f"Error updating project {project_id}: {str(e)}")
        import traceback
        app.logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/projects/<int:project_id>', methods=['DELETE'])
def delete_project(project_id):
    """Hard delete project and cascade"""
    for table in ['fact_cost_analysis_by_step', 'fact_module_phase_hours', 
                  'fact_project_timeline', 'fact_rate_calculation']:
        db_manager.execute_custom_command(f"DELETE FROM {table} WHERE project_id={project_id}")
    
    db_manager.execute_custom_command(f"DELETE FROM dim_project WHERE project_id={project_id}")
    return jsonify({'success': True})

@app.route('/api/projects')
def get_projects():
    """Get all active projects for the dashboard"""
    df = get_enriched_projects(False)
    return jsonify(df.to_dict('records'))

@app.route('/api/project/<int:project_id>')
def get_project(project_id):
    """Get complete project data for editing"""
    try:
        data = db_manager.get_complete_project_data(project_id)
        
        # Convert DataFrames to JSON-compatible format
        # Replace NaN with None (becomes null in JSON)
        return jsonify({
            'success': True,
            'project': data['project'].fillna('').to_dict('records')[0] if not data['project'].empty else {},
            'costanalysis': data['costanalysis'].fillna('').to_dict('records'),
            'hoursanalysis': data['hoursanalysis'].replace({float('nan'): None}).to_dict('records'),  # ← FIXED
            'timeline': data['timeline'].fillna('').to_dict('records'),
            'ratecalculation': data['ratecalculation'].replace({float('nan'): None}).to_dict('records')  # ← FIXED
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/weekly-module-hours')
def get_weekly_module_hours():
    """
    Get aggregated weekly hours by module for all active projects.
    Returns data structured for a scrollable weekly table with Monday-aligned weeks.
    """
    try:
        start_date, end_date = get_date_range_from_request()
        
        # ✓ NEW: Get project filter parameter
        project_id = request.args.get('project_id', 'all')
        
        # Convert to date objects if strings
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # ✓ NEW: Build project filter clause
        project_filter = ""
        if project_id != 'all':
            try:
                project_id_int = int(project_id)
                project_filter = f"AND project_id = {project_id_int}"
            except ValueError:
                app.logger.warning(f"Invalid project_id: {project_id}")
        
        query = f"""
        SELECT
            week_start_date,
            week_end_date,
            module_name,
            SUM(planned_hours) AS total_hours
        FROM vw_module_phase_hours_calendar
        WHERE project_status = 'Active'
            AND week_start_date BETWEEN '{start_date}' AND '{end_date}'
            AND week_start_date IS NOT NULL
            {project_filter}  -- ✓ NEW: Apply project filter
            AND module_name NOT IN ('SUMS', 'Sums', 'sums',
                'TOTAL', 'Total', 'total',
                'WEEKS EFFORT', 'Weeks Effort', 'weeks effort',
                'WEEK EFFORT', 'Week Effort', 'week effort',
                'SUBTOTAL', 'Subtotal', 'subtotal')
            AND UPPER(module_name) NOT LIKE '%SUM%'
            AND UPPER(module_name) NOT LIKE '%TOTAL%'
            AND UPPER(module_name) NOT LIKE '%EFFORT%'
        GROUP BY week_start_date, week_end_date, module_name
        ORDER BY week_start_date, module_name
        """
        
        df = db_manager.execute_custom_query(query)
        df = df.replace({np.nan: None})
        
        if df.empty:
            return jsonify({
                'weeks': [],
                'modules': [],
                'data': []
            })
        
        # ✓ FIXED: Generate Monday-aligned weeks
        week_labels = generate_week_labels(start_date, end_date)
        
        # ✓ HELPER: Normalize any date to its Monday
        def normalize_to_monday(date_obj):
            """Convert any date to the Monday of that week"""
            if isinstance(date_obj, str):
                date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
            days_since_monday = date_obj.weekday()
            monday = date_obj - timedelta(days=days_since_monday)
            return monday
        
        # Get unique modules
        modules = sorted(df['module_name'].unique().tolist())
        
        # ✓ FIXED: Build weeks array with Monday alignment
        weeks = []
        for week_start_str in week_labels:
            week_start = datetime.strptime(week_start_str, '%Y-%m-%d').date()
            week_end = week_start + timedelta(days=6)
            week_label = f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}"
            weeks.append({
                'label': week_label,
                'start': week_start.isoformat(),
                'end': week_end.isoformat()
            })
        
        # ✓ FIXED: Build data array with Monday normalization
        data = []
        for week_start_str in week_labels:
            week_start = datetime.strptime(week_start_str, '%Y-%m-%d').date()
            
            # Accumulate hours for all database rows that belong to this Monday
            module_hours = {module: 0 for module in modules}  # Initialize with zeros
            
            for _, row in df.iterrows():
                # Normalize database date to Monday
                row_monday = normalize_to_monday(row['week_start_date'])
                
                # If this database row belongs to current Monday week
                if row_monday == week_start:
                    module_name = row['module_name']
                    hours = float(row['total_hours']) if pd.notna(row['total_hours']) else 0
                    module_hours[module_name] += hours  # Accumulate
            
            data.append(module_hours)
        
        return jsonify({
            'weeks': weeks,
            'modules': modules,
            'data': data
        })
        
    except Exception as e:
        app.logger.error(f"Error in get_weekly_module_hours: {str(e)}")
        import traceback
        app.logger.error(traceback.format_exc())
        return jsonify({
            'weeks': [],
            'modules': [],
            'data': []
        }), 500

@app.route('/api/timeline-data')
def get_timeline_data():
    """
    Provides consolidated data for timeline and workload charts.
    NOW GROUPED BY MODULE instead of phase.
    """
    try:
        # Get date range from request parameters
        start_date, end_date = get_date_range_from_request()

        # 1. Fetch data from vw_module_phase_hours_calendar - GROUP BY MODULE
        project_timeline_query = f"""
        SELECT
            project_id,
            customer_name,
            project_name,
            project_status,
            module_id,              -- CHANGED: from phase_id
            module_code,            -- CHANGED: from phase_code (add this column)
            module_name,            -- CHANGED: from phase_name
            MIN(week_start_date) AS week_start_date,   -- CHANGED: aggregate
            MAX(week_end_date) AS week_end_date,       -- CHANGED: aggregate
            SUM(planned_hours) AS planned_hours        -- CHANGED: aggregate
        FROM vw_module_phase_hours_calendar
        WHERE project_status = 'Active'
            AND week_start_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 
            project_id,
            customer_name,
            project_name,
            project_status,
            module_id,              -- CHANGED: group by module instead of phase
            module_code,            -- CHANGED: add to GROUP BY
            module_name             -- CHANGED: already here, keep it
        ORDER BY project_id, module_code, week_start_date
        """
        
        app.logger.info("Executing timeline query...")
        timeline_df = db_manager.execute_custom_query(project_timeline_query)
        app.logger.info(f"Query returned {len(timeline_df)} rows")
        
        if len(timeline_df) > 0:
            app.logger.info(f"Columns: {timeline_df.columns.tolist()}")
            app.logger.info(f"First row: {timeline_df.iloc[0].to_dict()}")
        else:
            app.logger.warning("Query returned 0 rows!")
        
        timeline_df = timeline_df.replace({np.nan: None})
        projects = timeline_df.to_dict('records')

        # 2. Fetch workload data (UNCHANGED)
        workload_query = f"""
        SELECT
            calendar_week_start AS "weekStart",
            active_projects AS "activeProjects",
            active_modules AS "activeModules"
        FROM vw_concurrent_project_workload
        WHERE calendar_week_start BETWEEN '{start_date}' AND '{end_date}'
            AND calendar_week_start IS NOT NULL
        ORDER BY calendar_week_start;
        """
        workload_df = db_manager.execute_custom_query(workload_query)
        workload_df = workload_df.replace({np.nan: None})

        if workload_df.empty:
            workload_data = {"periods": [], "datasets": {"activeProjects": [], "activeModules": []}}
        else:
            workload_data = {
                "periods": workload_df[['weekStart']].to_dict('records'),
                "datasets": {
                    "activeProjects": workload_df['activeProjects'].tolist(),
                    "activeModules": workload_df['activeModules'].tolist()
                }
            }

        # 3. Calculate dateRange from timeline data (UNCHANGED)
        date_range = {}
        if not timeline_df.empty and 'week_start_date' in timeline_df.columns:
            min_date = timeline_df['week_start_date'].min()
            max_date = timeline_df['week_end_date'].max()
            
            date_range = {
                "minDate": min_date.isoformat() if pd.notna(min_date) else None,
                "maxDate": max_date.isoformat() if pd.notna(max_date) else None
            }
        else:
            from datetime import datetime
            today = datetime.now()
            date_range = {
                "minDate": today.isoformat(),
                "maxDate": today.isoformat()
            }

        app.logger.info(f"Returning {len(projects)} project records")
        app.logger.info(f"Date range: {date_range}")

        return jsonify({
            "projects": projects,
            "workload": workload_data,
            "dateRange": date_range
        })

    except Exception as e:
        app.logger.error(f"Error in get_timeline_data: {str(e)}")
        import traceback
        app.logger.error(traceback.format_exc())
        from datetime import datetime
        today = datetime.now()
        return jsonify({
            "projects": [],
            "workload": {"periods": [], "datasets": {"activeProjects": [], "activeModules": []}},
            "dateRange": {"minDate": today.isoformat(), "maxDate": today.isoformat()}
        }), 500
    
@app.route('/api/customers')
def get_customers():
    query = """
        SELECT customer_id AS customerId, customer_name AS customerName
        FROM dim_customer 
        ORDER BY customer_name
    """
    df = db_manager.execute_custom_query(query)
    return jsonify(df.to_dict('records'))

@app.route('/api/customers/<int:customer_id>/projects')
def get_customer_projects(customer_id):
    """Get projects for a specific customer"""
    query = f"""
        SELECT project_id AS "projectId", project_name AS "projectName", project_start_date AS "projectStartDate"
        FROM dim_project 
        WHERE customer_id = {customer_id}
        ORDER BY project_name
    """
    df = db_manager.execute_custom_query(query)
    return jsonify(df.to_dict('records'))

@app.route('/api/dashboard-metrics')
def get_dashboard_metrics():
    """
    Get summary metrics for the dashboard cards filtered by date range overlap.
    
    Logic:
    - Total Projects: Count projects that have module work weeks during the date range
    - Active Modules: Count modules that have work weeks overlapping the date range
    - Total Hours: Sum ALL planned hours from weeks that overlap the date range
    
    Uses vw_module_phase_hours_calendar which has week-level granularity.
    A week overlaps if: week_start_date <= end_date AND week_end_date >= start_date
    """
    try:
        # Get date range from request parameters
        start_date, end_date = get_date_range_from_request()
        
        app.logger.info(f"Dashboard metrics requested for date range: {start_date} to {end_date}")
        
        # Use the calendar view which has week_start_date and week_end_date
        # Check if any weeks overlap with the selected date range
        query = f"""
        SELECT
            COUNT(DISTINCT project_id) as total_projects,
            COUNT(DISTINCT module_id) as total_modules,
            COALESCE(SUM(planned_hours), 0) as total_hours
        FROM vw_module_phase_hours_calendar
        WHERE project_status = 'Active'
        AND week_start_date <= '{end_date}'
        AND week_end_date >= '{start_date}'
        AND planned_hours IS NOT NULL
        """
        
        app.logger.info(f"Executing metrics query for date range: {start_date} to {end_date}")
        df = db_manager.execute_custom_query(query)
        
        if df.empty:
            app.logger.warning("No data returned from metrics query")
            return jsonify({
                'totalProjects': 0,
                'activeModules': 0,
                'totalHours': 0,
                'averageUtilization': 0
            })
        
        # Extract metrics with null safety
        total_projects = int(df['total_projects'].iloc[0]) if pd.notna(df['total_projects'].iloc[0]) else 0
        total_modules = int(df['total_modules'].iloc[0]) if pd.notna(df['total_modules'].iloc[0]) else 0
        total_hours = float(df['total_hours'].iloc[0]) if pd.notna(df['total_hours'].iloc[0]) else 0
        
        # Calculate average utilization (placeholder logic)
        # You may want to adjust this based on your business rules
        expected_hours = total_modules * 40 * 12 if total_modules > 0 else 1
        avg_utilization = round((total_hours / expected_hours * 100), 1) if expected_hours > 0 else 0
        
        app.logger.info(f"Metrics calculated - Projects: {total_projects}, Modules: {total_modules}, Hours: {total_hours:.2f}")
        
        return jsonify({
            'totalProjects': total_projects,
            'activeModules': total_modules,
            'totalHours': round(total_hours, 2),
            'averageUtilization': avg_utilization
        })
        
    except Exception as e:
        app.logger.error(f"Error in get_dashboard_metrics: {str(e)}")
        import traceback
        app.logger.error(traceback.format_exc())
        return jsonify({
            'totalProjects': 0,
            'activeModules': 0,
            'totalHours': 0,
            'averageUtilization': 0
        }), 500

    
@app.route('/api/module-utilization')
def get_module_utilization():
    """Module hours aggregation for charts with complete date range"""
    try:
        granularity = request.args.get('granularity', 'weekly')
        view = request.args.get('view', 'by-project')
        project_id = request.args.get('project_id', 'all')
        # Get date range from request
        start_date, end_date = get_date_range_from_request()
        
        app.logger.info(f"===== MODULE UTILIZATION REQUEST =====")
        app.logger.info(f"Granularity: {granularity}")
        app.logger.info(f"View: {view}")
        app.logger.info(f"Project ID: {project_id}")
        app.logger.info(f"Full request args: {dict(request.args)}")
        app.logger.info(f"Module utilization requested: granularity={granularity}, view={view}, dates={start_date} to {end_date}")
        
        # Custom aggregation based on parameters
        if view == 'by-project':
            data = aggregate_hours_by_project(granularity, start_date, end_date, project_id)
        else:
            data = aggregate_hours_by_module(granularity, start_date, end_date, project_id)
        
        # Ensure data is a dictionary (handle None case)
        if data is None:
            app.logger.warning("Aggregation function returned None, using empty dataset")
            data = {
                'labels': [],
                'datasets': [],
                'dateRange': {
                    'startDate': start_date,
                    'endDate': end_date
                }
            }
        else:
            # Add date range to response for frontend to use
            if 'dateRange' not in data:
                data['dateRange'] = {
                    'startDate': start_date,
                    'endDate': end_date
                }
        
        return jsonify(data)
        
    except Exception as e:
        app.logger.error(f"Error in get_module_utilization: {str(e)}")
        import traceback
        app.logger.error(traceback.format_exc())
        return jsonify({
            'error': str(e),
            'labels': [],
            'datasets': [],
            'dateRange': {'startDate': '', 'endDate': ''}
        }), 500

# ===== HELPER FUNCTIONS =====

def transform_to_timeline_format(df):
    """Transform SQL results to frontend timeline structure"""
    projects = {}
    
    for _, row in df.iterrows():
        proj_id = row['projectid']
        if proj_id not in projects:
            projects[proj_id] = {
                'projectId': proj_id,
                'projectName': f"{row['customername']} - {row['projectname']}",
                'startDate': str(row['projectstartdate']),
                'phases': []
            }
        
        projects[proj_id]['phases'].append({
            'phase': row['phase'],
            'startWeek': calculate_week_number(row['startdate']),
            'durationWeeks': row['durationweeks'],
            'color': get_phase_color(row['phase'])
        })
    
    return {
        'projects': list(projects.values()),
        'dateRange': calculate_date_range(df)
    }

@app.route('/api/projects/<int:project_id>', methods=['PUT'])
def update_project_metadata(project_id):
    """Update project metadata (customer, name, start date, status)"""
    try:
        data = request.json
        app.logger.info(f"Updating project {project_id} with data: {data}")
        
        # Extract and validate data
        customer_id = int(data.get('customerId'))
        project_name = data.get('projectName', '').strip()
        start_date = data.get('startDate')
        status = data.get('status', 'Active')
        
        # Validate required fields
        if not all([customer_id, project_name, start_date]):
            return jsonify({'success': False, 'error': 'Missing required fields'}), 400
        
        # Check for duplicate project name (excluding current project)
        existing = db_manager.execute_custom_query(
            f"SELECT 1 FROM dim_project WHERE project_name='{project_name}' AND project_id!={project_id}"
        )
        if existing.shape[0] > 0:
            return jsonify({'success': False, 'error': 'Project name already exists'}), 400
        
        # Update the project
        db_manager.execute_custom_command(
            f"UPDATE dim_project SET "
            f"customer_id={customer_id}, "
            f"project_name='{project_name}', "
            f"project_start_date='{start_date}', "
            f"project_status='{status}', "
            f"modified_date=GETDATE() "
            f"WHERE project_id={project_id}"
        )
        
        return jsonify({'success': True, 'message': 'Project updated successfully'})
        
    except ValueError:
        return jsonify({'success': False, 'error': 'Invalid customer ID'}), 400
    except Exception as e:
        app.logger.error(f"Error updating project {project_id}: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload():
    try:
        # Get form data
        customer_name = request.form.get('customerName')
        project_name = request.form.get('projectName')
        project_start_date = request.form.get('projectStartDate')
        
        # Get uploaded file
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
            
        file = request.files['file']
        
        # Save temporarily
        temp_path = f"/tmp/{file.filename}"
        file.save(temp_path)
        
        # Parse Excel using existing parser
        extractor = ExcelTableExtractor(temp_path)
        extractor.extract_all_tables()
        
        # Convert to DataFrames (matching db_ops.py structure)
        df_cost = extractor.tables['Cost Analysis by Step']
        df_hours = extractor.tables['Hours Analysis by Module']
        df_timeline = extractor.tables['Project Timeline']
        df_rate = extractor.tables['Rate Calculation']
        
        # Insert into database
        project_info = {
            'customername': customer_name,
            'projectname': project_name,
            'projectstartdate': project_start_date
        }
        
        project_id = db_manager.insert_project_from_dataframes(
            project_info, df_cost, df_hours, df_timeline, df_rate
        )
        
        return jsonify({
            'success': True,
            'projectId': project_id,
            'message': 'Project uploaded successfully'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/parse-file', methods=['POST'])
def parse_file():
    """
    Parse uploaded CSV/Excel file in-memory without saving to disk
    """
    try:
        # Validate file upload
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400
        
        # Validate file extension
        if not (file.filename.endswith('.csv') or file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
            return jsonify({'success': False, 'error': 'Invalid file format. Please upload CSV or Excel file.'}), 400
        
        # Read file content into memory (BytesIO object)
        file_content = io.BytesIO(file.read())
        
        # Parse file directly from memory using modified extractor
        extractor = ExcelTableExtractor.from_bytes(file_content, file.filename)
        tables = extractor.extract_all()
        
        # Helper function to clean DataFrame before converting to JSON
        def clean_dataframe(df):
            """Clean DataFrame: handle NaN, duplicate columns, datetime, and convert to records"""
            if df.empty:
                return []
            
            # Make column names unique by appending suffixes
            cols = pd.Series(df.columns)
            for dup in cols[cols.duplicated()].unique():
                cols[cols == dup] = [f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))]
            df.columns = cols
            
            # Convert datetime objects to string format
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].astype(str)
            
            # Replace NaN/NA with empty string (or None for null, or 0 for numeric)
            # Using fillna('') will replace with empty string
            # Using fillna(0) will replace with 0
            df = df.fillna('')  # Change to df.fillna(0) if you prefer zeros
            
            # Convert to records
            records = df.to_dict('records')
            
            # Additional cleanup: convert any remaining numpy types to native Python types
            import numpy as np
            for record in records:
                for key, value in record.items():
                    # Handle numpy types
                    if isinstance(value, (np.integer, np.int64)):
                        record[key] = int(value)
                    elif isinstance(value, (np.floating, np.float64)):
                        # Check if it's NaN
                        if np.isnan(value):
                            record[key] = ''  # or 0 or None
                        else:
                            record[key] = float(value)
                    elif isinstance(value, np.bool_):
                        record[key] = bool(value)
                    elif pd.isna(value):
                        record[key] = ''  # or 0 or None
            
            return records
        
        # Convert DataFrames to JSON-serializable format with cleaned data
        parsed_data = {
            'costAnalysis': clean_dataframe(tables.get('Cost Analysis by Step', pd.DataFrame())),
            'hoursAnalysis': clean_dataframe(tables.get('Hours Analysis by Module', pd.DataFrame())),
            'timeline': clean_dataframe(tables.get('Project Timeline', pd.DataFrame())),
            'rateCalculation': clean_dataframe(tables.get('Rate Calculation', pd.DataFrame()))
        }
        
        # Also send column names for table headers (after cleaning)
        def get_columns(df):
            """Get column names after making them unique"""
            if df.empty:
                return []
            cols = pd.Series(df.columns)
            for dup in cols[cols.duplicated()].unique():
                cols[cols == dup] = [f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))]
            return cols.tolist()
        
        parsed_columns = {
            'costAnalysis': get_columns(tables.get('Cost Analysis by Step', pd.DataFrame())),
            'hoursAnalysis': get_columns(tables.get('Hours Analysis by Module', pd.DataFrame())),
            'timeline': get_columns(tables.get('Project Timeline', pd.DataFrame())),
            'rateCalculation': get_columns(tables.get('Rate Calculation', pd.DataFrame()))
        }
        
        return jsonify({
            'success': True,
            'data': parsed_data,
            'columns': parsed_columns,
            'message': 'File parsed successfully'
        })
        
    except Exception as e:
        import traceback
        print(traceback.format_exc())  # Print full traceback for debugging
        return jsonify({
            'success': False,
            'error': f'Error parsing file: {str(e)}'
        }), 500

@app.route('/api/submit-project', methods=['POST'])
def submit_project():
    """
    Submit reviewed/edited project data to database.
    Uses existing project and inserts fact table data.
    """
    try:
        data = request.json
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        # Extract project information
        project_info = data.get('projectInfo', {})
        customer_id = project_info.get('customerId')
        project_id = project_info.get('projectId')
        project_start_date = project_info.get('projectStartDate')
        
        # Validate required fields
        if not all([customer_id, project_id, project_start_date]):
            return jsonify({
                'success': False,
                'error': 'Missing required project information (customer ID, project ID, or start date)'
            }), 400
        
        # Validate IDs are integers
        try:
            customer_id = int(customer_id)
            project_id = int(project_id)
        except (ValueError, TypeError):
            return jsonify({
                'success': False,
                'error': 'Invalid customer ID or project ID'
            }), 400
        
        # Extract table data and convert to DataFrames
        cost_analysis_data = data.get('costAnalysis', [])
        hours_analysis_data = data.get('hoursAnalysis', [])
        timeline_data = data.get('timeline', [])
        rate_calculation_data = data.get('rateCalculation', [])
        
        df_cost = pd.DataFrame(cost_analysis_data)
        df_hours = pd.DataFrame(hours_analysis_data)
        df_timeline = pd.DataFrame(timeline_data)
        df_rate = pd.DataFrame(rate_calculation_data)
        
        # Validate DataFrames are not empty
        if df_cost.empty or df_hours.empty or df_timeline.empty or df_rate.empty:
            return jsonify({
                'success': False,
                'error': 'One or more required tables are empty'
            }), 400
        
        # Delete existing fact table data for this project before inserting new data
        app.logger.info(f"Clearing existing data for project {project_id}")
        try:
            db_manager.delete_cost_analysis(project_id)
        except:
            app.logger.info("No cost analysis data to remove")

        try:
            db_manager.delete_hours_analysis(project_id)
        except:
            app.logger.info("No hours analysis data to remove")

        try:
            db_manager.delete_timeline(project_id)
        except:
            app.logger.info("No timeline data to remove")
        
        try:
            db_manager.delete_rate_calculation(project_id)
        except:
            app.logger.info("No rate calculation data to remove")
        
        # Insert new data using existing methods
        app.logger.info(f"Inserting new data for project {project_id}")
        cost_count = db_manager.bulk_insert_cost_analysis(project_id, df_cost)
        hours_count = db_manager.bulk_insert_hours_analysis(project_id, df_hours)
        timeline_count = db_manager.bulk_insert_timeline(project_id, df_timeline)
        rate_count = db_manager.bulk_insert_rate_calculation(project_id, df_rate)
        
        app.logger.info(
            f"Successfully uploaded project {project_id}: "
            f"{cost_count} cost, {hours_count} hours, "
            f"{timeline_count} timeline, {rate_count} rate records"
        )
        
        return jsonify({
            'success': True,
            'projectId': project_id,
            'message': 'Project data uploaded successfully',
            'recordsInserted': {
                'costAnalysis': cost_count,
                'hoursAnalysis': hours_count,
                'timeline': timeline_count,
                'rateCalculation': rate_count
            }
        })
        
    except Exception as e:
        app.logger.error(f"Error in submit_project: {str(e)}")
        import traceback
        app.logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': f'Error submitting project: {str(e)}'
        }), 500

@app.route('/api/validate-data', methods=['POST'])
def validate_data():
    """
    Optional: Validate edited data before submission
    Checks for required fields, data types, and business logic
    """
    try:
        data = request.json
        errors = []
        
        # Validate Cost Analysis
        cost_analysis = data.get('costAnalysis', [])
        if not cost_analysis:
            errors.append('Cost Analysis table is empty')
        else:
            for idx, row in enumerate(cost_analysis):
                if not row.get('Payment Milestone'):
                    errors.append(f'Cost Analysis row {idx+1}: Missing Payment Milestone')
                if not isinstance(row.get('Weight'), (int, float)):
                    errors.append(f'Cost Analysis row {idx+1}: Weight must be a number')
        
        # Validate Hours Analysis
        hours_analysis = data.get('hoursAnalysis', [])
        if not hours_analysis:
            errors.append('Hours Analysis table is empty')
        else:
            for idx, row in enumerate(hours_analysis):
                if not row.get('HCM Modules'):
                    errors.append(f'Hours Analysis row {idx+1}: Missing HCM Module')
        
        # Validate Timeline
        timeline = data.get('timeline', [])
        if not timeline:
            errors.append('Timeline table is empty')
        
        # Validate Rate Calculation
        rate_calc = data.get('rateCalculation', [])
        if not rate_calc:
            errors.append('Rate Calculation table is empty')
        
        if errors:
            return jsonify({
                'valid': False,
                'errors': errors
            }), 400
        
        return jsonify({
            'valid': True,
            'message': 'Data validation passed'
        })
        
    except Exception as e:
        return jsonify({
            'valid': False,
            'error': f'Validation error: {str(e)}'
        }), 500

def generate_week_labels(start_date, end_date):
    """Generate list of all week start dates between start and end"""
    from datetime import timedelta
    
    weeks = []
    current = start_date
    
    # Find the Monday of the week containing start_date
    days_since_monday = current.weekday()
    current = current - timedelta(days=days_since_monday)
    
    while current <= end_date:
        weeks.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=7)
    
    return weeks

def generate_month_labels(start_date, end_date):
    """Generate list of all months between start and end"""
    from datetime import datetime
    
    months = []
    current = datetime(start_date.year, start_date.month, 1).date()
    end_month = datetime(end_date.year, end_date.month, 1).date()
    
    while current <= end_month:
        months.append(current.strftime('%Y-%m'))
        # Move to next month
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1).date()
        else:
            current = datetime(current.year, current.month + 1, 1).date()
    
    return months

def generate_quarter_labels(start_date, end_date):
    """Generate list of all quarters between start and end"""
    quarters = []
    
    start_quarter = (start_date.year, (start_date.month - 1) // 3 + 1)
    end_quarter = (end_date.year, (end_date.month - 1) // 3 + 1)
    
    current = start_quarter
    while current <= end_quarter:
        quarters.append(f"{current[0]}-Q{current[1]}")
        
        # Move to next quarter
        if current[1] == 4:
            current = (current[0] + 1, 1)
        else:
            current = (current[0], current[1] + 1)
    
    return quarters

def get_module_color(module_name):
    """Return consistent color for a module"""
    colors = [
        '#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8',
        '#A8E6CF', '#F06292', '#AED581', '#FFD54F', '#4DD0E1'
    ]
    # Hash module name to get consistent color
    hash_val = sum(ord(c) for c in module_name)
    return colors[hash_val % len(colors)]

# ===== HELPER FUNCTION FOR CSV SUPPORT =====
def modify_excel_parser_for_csv(file_path):
    """
    Helper function to modify ExcelTableExtractor to support CSV files
    This can be added to excel_parser.py or used here
    """
    if file_path.endswith('.csv'):
        # For CSV files, you may need custom parsing logic
        # depending on how your CSV is structured
        df = pd.read_csv(file_path, header=None)
        # Use same extraction logic as Excel
        extractor = ExcelTableExtractor.__new__(ExcelTableExtractor)
        extractor.df = df
        extractor.tables = {}
        return extractor
    else:
        return ExcelTableExtractor(file_path)

if __name__ == '__main__':
    app.run(debug=True)
