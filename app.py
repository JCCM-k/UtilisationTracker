import os
from werkzeug.utils import secure_filename
from flask import Flask, render_template, request, jsonify, session
from db_ops import AzureSQLDBManager
from excel_parser import ExcelTableExtractor
import pandas as pd
import io 

app = Flask(__name__)
db_manager = ''
#db_manager = AzureSQLDBManager('','')

app.config['UPLOAD_FOLDER'] = '/tmp/uploads'
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024  # 10MB max file size
app.config['SECRET_KEY'] = 'your-secret-key-here'  # Required for session management

# Ensure upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

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
    """Get all projects for dropdowns and lists"""
    df = db_manager.get_all_projects()
    return jsonify(df.to_dict('records'))

@app.route('/api/project/<int:project_id>')
def get_project(project_id):
    """Get complete project data for editing"""
    data = db_manager.get_complete_project_data(project_id)
    return jsonify({
        'project': data['project'].to_dict('records')[0] if not data['project'].empty else {},
        'costanalysis': data['costanalysis'].to_dict('records'),
        'hoursanalysis': data['hoursanalysis'].to_dict('records'),
        'timeline': data['timeline'].to_dict('records'),
        'ratecalculation': data['ratecalculation'].to_dict('records')
    })

@app.route('/api/timeline-data')
def get_timeline_data():
    """Aggregated timeline data for visualization"""
    # Custom query to aggregate timeline across all projects
    query = """
    SELECT 
        p.projectid,
        p.customername,
        p.projectname,
        p.projectstartdate,
        t.phase,
        t.durationweeks,
        t.startdate,
        t.enddate
    FROM dimproject p
    JOIN factprojecttimeline t ON p.projectid = t.projectid
    ORDER BY p.projectid, t.startdate
    """
    df = db_manager.execute_custom_query(query)
    
    # Transform into timeline structure
    timeline_data = transform_to_timeline_format(df)
    return jsonify(timeline_data)

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
        db_manager = AzureSQLDBManager()
        
        # Update project details
        db_manager.execute_custom_command(
            "UPDATE dimproject SET customername=?, projectname=?, projectstartdate=? WHERE projectid=?",
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
        db_manager = AzureSQLDBManager()
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
        
        # Initialize database manager
        db_manager = AzureSQLDBManager()  # Add your connection params
        
        # Prepare project info dictionary
        project_dict = {
            'customername': customer_name,
            'projectname': project_name,
            'projectstartdate': project_start_date
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
