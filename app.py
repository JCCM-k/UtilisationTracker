# Flask app.py structure

from flask import Flask, render_template, request, jsonify
from db_ops import AzureSQLDBManager
from excel_parser import ExcelTableExtractor
import pandas as pd

app = Flask(__name__)
db_manager = ''
#db_manager = AzureSQLDBManager('','')

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



if __name__ == '__main__':
    app.run(debug=True)
