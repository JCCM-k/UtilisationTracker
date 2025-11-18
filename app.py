import os
from werkzeug.utils import secure_filename
from flask import Flask, render_template, request, jsonify, session
from db_ops import AzureSQLDBManager
from excel_parser import ExcelTableExtractor
import pandas as pd
import io 
from datetime import datetime, timedelta

app = Flask(__name__)
global db_manager
db_manager = AzureSQLDBManager(
    server="project-utilisation.database.windows.net",
    database="Utilisation_tracker_db",
    username="kliqtek-tester",
    password="cl@r1tythr0ughkn0wl3dg3",
    pool_size=5,
    max_overflow=2,
    pool_timeout=30,
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

from datetime import datetime, timedelta
import pandas as pd

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

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


def aggregate_hours_by_module(granularity='weekly'):
    """
    Aggregate module phase hours by time period.
    Uses: fact_module_phase_hours, dim_module (lowercase with underscores)
    
    Args:
        granularity: str ('weekly', 'monthly', 'quarterly')
    Returns:
        dict: {'periods': ['W1', ...], 'modules': [{'moduleName': 'X', 'hours': [...]}, ...]}
    """
    try:
        # SQL query based on granularity
        if granularity == 'weekly':
            period_expr = "DATEPART(WEEK, h.module_start_date)"
            period_format = "CONCAT('W', DATEPART(WEEK, h.module_start_date))"
        elif granularity == 'monthly':
            period_expr = "FORMAT(h.module_start_date, 'yyyy-MM')"
            period_format = "FORMAT(h.module_start_date, 'yyyy-MM')"
        else:  # quarterly
            period_expr = "CONCAT(YEAR(h.module_start_date), '-Q', DATEPART(QUARTER, h.module_start_date))"
            period_format = "CONCAT(YEAR(h.module_start_date), '-Q', DATEPART(QUARTER, h.module_start_date))"
        
        query = f"""
        SELECT 
            m.module_name,
            {period_format} AS period,
            SUM(h.planned_hours) AS totalhours
        FROM fact_module_phase_hours h
        INNER JOIN dim_module m ON h.module_id = m.module_id
        WHERE h.module_start_date IS NOT NULL 
            AND h.planned_hours IS NOT NULL
        GROUP BY m.module_name, {period_expr}, {period_format}
        ORDER BY m.module_name, {period_expr}
        """
        
        df = db_manager.execute_custom_query(query)
        
        if df.empty:
            return {'periods': [], 'modules': []}
        
        # Transform to chart format
        periods = sorted(df['period'].unique().tolist())
        modules = []
        
        for module_name in df['module_name'].unique():
            module_df = df[df['module_name'] == module_name]
            hours_dict = dict(zip(module_df['period'], module_df['totalhours']))
            hours = [hours_dict.get(p, 0) for p in periods]
            
            modules.append({
                'moduleName': module_name,
                'hours': hours
            })
        
        return {'periods': periods, 'modules': modules}
        
    except Exception as e:
        app.logger.error(f"Error in aggregate_hours_by_module: {str(e)}")
        return {'periods': [], 'modules': []}


def aggregate_hours_by_project(granularity='weekly'):
    """
    Aggregate hours by project over time.
    Uses: fact_module_phase_hours, dim_project (lowercase with underscores)
    
    Args:
        granularity: str ('weekly', 'monthly', 'quarterly')
    Returns:
        dict: {'periods': ['W1', ...], 'projects': [{'projectName': 'X', 'hours': [...]}, ...]}
    """
    try:
        # SQL query based on granularity
        if granularity == 'weekly':
            period_expr = "DATEPART(WEEK, h.module_start_date)"
            period_format = "CONCAT('W', DATEPART(WEEK, h.module_start_date))"
        elif granularity == 'monthly':
            period_expr = "FORMAT(h.module_start_date, 'yyyy-MM')"
            period_format = "FORMAT(h.module_start_date, 'yyyy-MM')"
        else:  # quarterly
            period_expr = "CONCAT(YEAR(h.module_start_date), '-Q', DATEPART(QUARTER, h.module_start_date))"
            period_format = "CONCAT(YEAR(h.module_start_date), '-Q', DATEPART(QUARTER, h.module_start_date))"
        
        query = f"""
        SELECT 
            p.project_name,
            {period_format} AS period,
            SUM(h.planned_hours) AS totalhours
        FROM fact_module_phase_hours h
        INNER JOIN dim_project p ON h.project_id = p.project_id
        WHERE h.module_start_date IS NOT NULL 
            AND h.planned_hours IS NOT NULL
        GROUP BY p.project_name, {period_expr}, {period_format}
        ORDER BY p.project_name, {period_expr}
        """
        
        df = db_manager.execute_custom_query(query)
        
        if df.empty:
            return {'periods': [], 'projects': []}
        
        # Transform to chart format
        periods = sorted(df['period'].unique().tolist())
        projects = []
        
        for project_name in df['project_name'].unique():
            project_df = df[df['project_name'] == project_name]
            hours_dict = dict(zip(project_df['period'], project_df['totalhours']))
            hours = [hours_dict.get(p, 0) for p in periods]
            
            projects.append({
                'projectName': project_name,
                'hours': hours
            })
        
        return {'periods': periods, 'projects': projects}
        
    except Exception as e:
        app.logger.error(f"Error in aggregate_hours_by_project: {str(e)}")
        return {'periods': [], 'projects': []}
    
def get_enriched_projects():
    """
    Get all active projects with aggregated module counts and total hours.
    This version starts from dim_project to ensure all active projects are included.
    Returns DataFrame with enriched project data for the dashboard.
    """
    try:
        # This query starts from dim_project and LEFT JOINs to get module data,
        # ensuring that all 'Active' projects are included, even if they have no modules yet.
        query = """
        SELECT
            p.project_id AS "projectId",
            p.customer_name AS "customerName",
            p.project_name AS "projectName",
            p.project_start_date AS "projectStartDate",
            p.project_status AS "status",
            COUNT(DISTINCT h.module_id) as "moduleCount",
            SUM(h.planned_hours) as "totalHours"
        FROM dim_project p
        LEFT JOIN fact_module_phase_hours h ON p.project_id = h.project_id
        WHERE p.project_status = 'Active'
        GROUP BY p.project_id, p.customer_name, p.project_name, p.project_start_date, p.project_status
        ORDER BY p.project_start_date DESC
        """
        df = db_manager.execute_custom_query(query)

        # SUM(NULL) results in NULL, so we replace it with 0. 
        # COUNT(DISTINCT NULL) correctly results in 0.
        df['totalHours'] = df['totalHours'].fillna(0)

        # Format date for consistent JSON output.
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

# ===== DATA API ROUTES =====

@app.route('/api/projects')
def get_projects():
    """Get all active projects for the dashboard"""
    df = get_enriched_projects()
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

@app.route('/api/timeline-data')
def get_timeline_data():
    """Get timeline data for Gantt chart visualization"""
    try:
        query = """
        SELECT
            p.project_id,
            p.customer_name,
            p.project_name,
            p.project_start_date,
            ph.phase_name,
            ph.phase_code,
            t.duration_weeks,
            t.start_date,
            t.end_date
        FROM dim_project p
        JOIN fact_project_timeline t ON p.project_id = t.project_id
        JOIN dim_phases ph ON t.phase_id = ph.phase_id
        ORDER BY p.project_id, ph.default_sequence
        """
        
        df = db_manager.execute_custom_query(query)
        
        # Replace NaN with None for JSON compatibility
        result = df.where(pd.notna(df), None).to_dict('records')
        
        return jsonify({
            'success': True,
            'data': result
        })
    except Exception as e:
        app.logger.error(f"Error fetching timeline data: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/module-utilization')
def get_module_utilization():
    """Module hours aggregation for charts"""
    granularity = request.args.get('granularity', 'weekly')
    view = request.args.get('view', 'by-project')
    
    # Custom aggregation based on parameters
    if view == 'by-project':
        data = aggregate_hours_by_project(granularity)
    else:
        data = aggregate_hours_by_module(granularity)
    
    return jsonify(data)

@app.route('/api/conflicts')
def get_conflicts():
    """Detect module utilization conflicts"""
    week = request.args.get('week')
    # Logic to identify high-utilization modules
    conflicts = detect_weekly_conflicts(week)
    return jsonify(conflicts)

# ===== WRITE OPERATIONS =====

@app.route('/upload', methods=['POST'])
def upload_data():
    # (Already detailed in section 3.2)
    pass

@app.route('/api/update-project/<int:project_id>', methods=['POST'])
def update_project_data(project_id):
    # (Already detailed in section 3.3)
    pass

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

def aggregate_hours_by_project(granularity):
    """Aggregate module hours by project over time"""
    # Complex aggregation query based on granularity
    pass

def detect_weekly_conflicts(week_number):
    """Identify modules with >70% utilization in specific week"""
    # Analysis logic
    pass

@app.route('/api/update-project/<int:project_id>', methods=['POST'])
def update_project(project_id):
    try:
        changes = request.json
        
        # Update project details
        db_manager.execute_custom_command(
            "UPDATE dimproject SET customer_name=?, project_name=?, project_start_date=? WHERE project_id=?",
            params=(
                changes['projectDetails']['customername'],
                changes['projectDetails']['projectname'],
                changes['projectDetails']['projectstartdate'],
                project_id
            )
        )
        
        # Replace cost analysis
        df_cost = pd.DataFrame(changes['costAnalysis'])
        db_manager.replace_cost_analysis(project_id, df_cost)
        
        # Replace hours analysis
        df_hours = pd.DataFrame(changes['hoursAnalysis'])
        db_manager.replace_hours_analysis(project_id, df_hours)
        
        # Replace timeline
        df_timeline = pd.DataFrame(changes['timeline'])
        db_manager.replace_timeline(project_id, df_timeline)
        
        # Rate calculation would use similar replace method
        
        return jsonify({'success': True})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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
    Step 2: Submit reviewed/edited project data to database
    This route accepts the edited data and project info, then inserts into DB
    """
    try:
        # Get JSON data from request
        data = request.json
        
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        # Extract project information
        project_info = data.get('projectInfo', {})
        customer_name = project_info.get('customerName')
        project_name = project_info.get('projectName')
        project_start_date = project_info.get('projectStartDate')
        
        # Validate required fields
        if not all([customer_name, project_name, project_start_date]):
            return jsonify({
                'success': False,
                'error': 'Missing required project information'
            }), 400
        
        # Extract table data
        cost_analysis_data = data.get('costAnalysis', [])
        hours_analysis_data = data.get('hoursAnalysis', [])
        timeline_data = data.get('timeline', [])
        rate_calculation_data = data.get('rateCalculation', [])
        
        # Convert JSON arrays back to pandas DataFrames
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
        
        # Prepare project info dictionary
        project_dict = {
            'customer_name': customer_name,
            'project_name': project_name,
            'project_start_date': project_start_date
        }
        
        # Insert project data into database
        project_id = db_manager.insert_project_from_dataframes(
            project_dict,
            df_cost,
            df_hours,
            df_timeline,
            df_rate
        )
        
        # Clean up uploaded file from session
        if 'uploaded_file_path' in session:
            file_path = session['uploaded_file_path']
            if os.path.exists(file_path):
                os.remove(file_path)
            session.pop('uploaded_file_path', None)
            session.pop('parsed_columns', None)
        
        return jsonify({
            'success': True,
            'projectId': project_id,
            'message': f'Project "{project_name}" uploaded successfully'
        })
        
    except Exception as e:
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
