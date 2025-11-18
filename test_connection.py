from db_ops import AzureSQLDBManager

# Replace with your actual values
db = AzureSQLDBManager(
        server="project-utilisation.database.windows.net",
        database="Utilisation_tracker_db",
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        log_level='INFO',
        authentication_method='SqlPassword',
        username='kliqtek-tester',
        password='cl@r1tythr0ughkn0wl3dg3'
    )

# Test query
print("Testing connection...")
projects = db.get_all_projects()
print(f"Found {len(projects)} projects")
print(projects)

# Check phases were seeded
import pandas as pd
with db._connection_context() as conn:
    phases = pd.read_sql("SELECT * FROM dim_phases", conn)
    print("\nPhases in database:")
    print(phases)

db.close_all_connections()
print("\nConnection test successful!")
