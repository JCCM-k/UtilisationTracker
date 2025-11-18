"""
Azure SQL Database Manager - Connection Management Module
Provides robust connection pooling with Entra ID authentication for Azure SQL Database
"""
# ==================== IMPORTS ====================
import pyodbc
import threading
from contextlib import contextmanager
from queue import Queue, Empty
from typing import Optional, Any, List, Dict, Tuple
from datetime import datetime, timedelta
import pandas as pd
import logging

# ==================== CUSTOM EXCEPTION HIERARCHY ====================

class AzureSQLDBError(Exception):
    """Base exception for database operations"""
    pass

class DatabaseConnectionError(AzureSQLDBError):
    """Connection failures"""
    pass

class DatabaseTransactionError(AzureSQLDBError):
    """Transaction failures"""
    pass

class DataValidationError(AzureSQLDBError):
    """Validation failures"""
    pass

class DataTransformationError(AzureSQLDBError):
    """Transformation failures"""
    pass

class RecordNotFoundError(AzureSQLDBError):
    """Record not found"""
    pass

class ConnectionPoolExhaustedError(AzureSQLDBError):
    """Pool exhausted"""
    pass
    
class ConnectionPool:
    """Thread-safe connection pool for pyodbc connections"""
    
    def __init__(self, connection_string: str, pool_size: int, max_overflow: int, pool_timeout: int):
        self.connection_string = connection_string
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self._pool = Queue(maxsize=pool_size + max_overflow)
        self._active_connections = 0
        self._lock = threading.Lock()
        self._logger = logging.getLogger(__name__)
        
        # Pre-populate pool with initial connections
        for _ in range(pool_size):
            try:
                conn = pyodbc.connect(self.connection_string)
                self._pool.put(conn)
                self._active_connections += 1
                self._logger.debug(f"Created initial connection. Pool size: {self._active_connections}")
            except pyodbc.Error as e:
                self._logger.exception("Failed to create initial connection")
                raise DatabaseConnectionError("Connection pool initialization failed") from e

    
    def acquire(self) -> pyodbc.Connection:
        """Acquire a connection from the pool"""
        try:
            # Try to get existing connection from pool
            conn = self._pool.get(timeout=self.pool_timeout)
            
            # Validate connection is still alive
            if not self._is_connection_alive(conn):
                self._logger.warning("Dead connection detected, creating new one")
                conn.close()
                conn = pyodbc.connect(self.connection_string)
            
            self._logger.debug(f"Connection acquired. Available: {self._pool.qsize()}")
            return conn
            
        except Empty:
            # Pool is empty, try to create overflow connection
            with self._lock:
                if self._active_connections < (self.pool_size + self.max_overflow):
                    try:
                        conn = pyodbc.connect(self.connection_string)
                        self._active_connections += 1
                        self._logger.info(f"Created overflow connection. Total: {self._active_connections}")
                        return conn
                    except pyodbc.Error as e:
                        self._logger.exception("Failed to create overflow connection")
                        raise ConnectionPoolExhaustedError("Unable to create database connection") from e
                else:
                    raise ConnectionError(
                        f"Connection pool exhausted. Max connections ({self.pool_size + self.max_overflow}) reached."
                    )
    
    def release(self, conn: pyodbc.Connection) -> None:
        """Return a connection to the pool"""
        if conn is None:
            return
        
        try:
            # Only return connection to pool if it's alive and pool isn't full
            if self._is_connection_alive(conn) and self._pool.qsize() < self.pool_size:
                self._pool.put(conn)
                self._logger.debug(f"Connection released. Available: {self._pool.qsize()}")
            else:
                # Close overflow or dead connections
                conn.close()
                with self._lock:
                    self._active_connections -= 1
                self._logger.debug(f"Connection closed. Total: {self._active_connections}")
        except Exception as e:
            self._logger.exception("Error releasing connection")
    
    def close_all(self) -> None:
        """Close all connections in the pool"""
        closed_count = 0
        
        # Close pooled connections
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
                closed_count += 1
            except Empty:
                break
            except Exception as e:
                self._logger.exception("Error closing pooled connection")
        
        with self._lock:
            self._active_connections = 0
        
        self._logger.info(f"Closed {closed_count} connections")
    
    def _is_connection_alive(self, conn: pyodbc.Connection) -> bool:
        """Check if connection is still valid"""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except:
            return False
    
    def get_stats(self) -> dict:
        """Get current pool statistics"""
        return {
            'pool_size': self.pool_size,
            'max_overflow': self.max_overflow,
            'available_connections': self._pool.qsize(),
            'active_connections': self._active_connections,
            'total_capacity': self.pool_size + self.max_overflow
        }


class AzureSQLDBManager:
    """
    Azure SQL Database Manager with connection pooling and Entra ID authentication.
    Provides comprehensive CRUD operations for project management database.
    """
    
    def __init__(
        self,
        server: str,
        database: str,
        username: str = None,
        password: str = None,
        pool_size: int = 10,
        max_overflow: int = 5,
        pool_timeout: int = 30,
        log_level: str = 'INFO',
        authentication_method: str = 'ActiveDirectoryInteractive',
        auto_initialize_phases: bool = False
    ):
        """
        Initialize Azure SQL Database Manager with connection pooling.
        
        Args:
            server: Azure SQL server name (e.g., 'myserver.database.windows.net')
            database: Database name
            pool_size: Number of persistent connections to maintain (default: 10)
            max_overflow: Additional connections allowed beyond pool_size (default: 5)
            pool_timeout: Seconds to wait for available connection (default: 30)
            log_level: Logging level - DEBUG, INFO, WARNING, ERROR (default: INFO)
            authentication_method: Entra ID auth method - ActiveDirectoryInteractive, 
                                   ActiveDirectoryMsi, or ActiveDirectoryServicePrincipal
        """
        # Configure logging
        self._setup_logging(log_level)
        self._logger = logging.getLogger(__name__)
        
        # Store connection parameters
        self.server = server
        self.database = database
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.authentication_method = authentication_method
        self.username = username
        self.password = password

        # Initialize connection pool
        self._logger.info(f"Initializing connection pool for {server}/{database}")
        self._connection_pool = self._initialize_pool()
        
        # Test initial connection
        self._test_connection()

        if auto_initialize_phases:
            try:
                self.initialize_phases()
            except Exception as e:
                self._logger.warning(f"Phase auto-initialization skipped: {e}")

        self._logger.info("Database manager initialized successfully")
    
    def _setup_logging(self, log_level: str) -> None:
        """Configure logging with specified level"""
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)
        logging.basicConfig(
            level=numeric_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    def _create_connection_string(self) -> str:
        """
        Build connection string for Azure SQL Database.
        For SQL authentication (not Entra ID).
        """
        # For SQL Password authentication
        if self.authentication_method == 'SqlPassword':
            connection_string = (
                f"Driver={{ODBC Driver 18 for SQL Server}};"
                f"Server=tcp:{self.server},1433;"
                f"Database={self.database};"
                f"Uid={self.username};"
                f"Pwd={self.password};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=yes;"
                f"Connection Timeout=30;"
            )
        else:
            # Original Entra ID method
            connection_string = (
                f"Driver={{ODBC Driver 18 for SQL Server}};"
                f"Server={self.server};"
                f"Database={self.database};"
                f"Authentication={self.authentication_method};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
        
        self._logger.debug(f"Connection string created for server: {self.server}")
        return connection_string
    
    def _initialize_pool(self) -> ConnectionPool:
        """
        Set up connection pool with mssql-python (pyodbc) driver.
        
        Returns:
            Configured ConnectionPool instance
            
        Raises:
            ConnectionError: If unable to establish initial connections
        """
        try:
            connection_string = self._create_connection_string()
            pool = ConnectionPool(
                connection_string=connection_string,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_timeout=self.pool_timeout
            )
            self._logger.info(
                f"Connection pool initialized: {self.pool_size} base + "
                f"{self.max_overflow} overflow = {self.pool_size + self.max_overflow} max"
            )
            return pool
        except Exception as e:
            self._logger.exception("Failed to initialize connection pool")
            raise ConnectionPoolExhaustedError("Connection pool initialization failed") from e
    
    def _get_connection(self) -> pyodbc.Connection:
        """
        Acquire a connection from the pool.
        
        Returns:
            Active database connection from pool
            
        Raises:
            ConnectionError: If unable to acquire connection within timeout
        """
        try:
            conn = self._connection_pool.acquire()
            self._logger.debug("Connection acquired from pool")
            return conn
        except Exception as e:
            self._logger.exception("Failed to acquire connection")
            raise ConnectionPoolExhaustedError("Unable to get database connection") from e
    
    @contextmanager
    def _connection_context(self):
        """
        Context manager for automatic connection acquisition and release.
        
        Yields:
            Database connection that is automatically released on exit
        """
        conn = self._get_connection()
        try:
            yield conn
        finally:
            self._connection_pool.release(conn)
            self._logger.debug("Connection released to pool")
    
    def close_all_connections(self) -> None:
        """
        Gracefully close all connections in the pool.
        Should be called during application shutdown.
        """
        self._logger.info("Closing all database connections...")
        self._connection_pool.close_all()
        self._logger.info("All connections closed successfully")
    
    def _test_connection(self) -> None:
        """
        Test database connectivity on initialization.
        
        Raises:
            ConnectionError: If unable to connect to database
        """
        try:
            with self._connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT @@VERSION")
                version = cursor.fetchone()[0]
                self._logger.info(f"Database connection verified: {version[:50]}...")
                cursor.close()
        except Exception as e:
            self._logger.exception("Connection test failed")
            raise DatabaseConnectionError("Unable to connect to database") from e

    # ==================== CREATE OPERATIONS ====================
    
    def create_project(
        self, 
        customer_name: str, 
        project_name: str, 
        project_start_date: Optional[str] = None
    ) -> int:
        """
        Create a new project in dim_project table.
        
        Args:
            customer_name: Name of the customer
            project_name: Name of the project
            project_start_date: Project start date (YYYY-MM-DD format). Defaults to current date.
        
        Returns:
            Generated project_id
        
        Raises:
            ValueError: If required parameters are missing or invalid
            RuntimeError: If database operation fails
        """
        # Validate inputs
        if not customer_name or not customer_name.strip():
            raise ValueError("customer_name cannot be empty")
        if not project_name or not project_name.strip():
            raise ValueError("project_name cannot be empty")
        
        # Default to current date if not provided
        if project_start_date is None:
            project_start_date = datetime.now().strftime('%Y-%m-%d')
        
        query = """
        INSERT INTO dim_project (customer_name, project_name, project_start_date)
        VALUES (?, ?, ?)
        """
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query, (customer_name, project_name, project_start_date))
                
                # Get the generated project_id
                cursor.execute("SELECT @@IDENTITY")
                project_id = int(cursor.fetchone()[0])
                
                conn.commit()
                self._logger.info(
                    f"Created project_id={project_id}: {customer_name} - {project_name}"
                )
                cursor.close()
                return project_id
                
            except pyodbc.Error as e:
                conn.rollback()
                self._logger.exception("Failed to create project")
                raise DatabaseConnectionError("Failed to create project") from e
            except Exception as e:
                conn.rollback()
                self._logger.exception("Unexpected error creating project")
                raise

    
    def insert_project_from_dataframes(
        self,
        project_info: Dict[str, Any],
        df_cost: pd.DataFrame,
        df_hours: pd.DataFrame,
        df_timeline: pd.DataFrame,
        df_rate: pd.DataFrame
    ) -> int:
        """
        Insert complete project with all related data in a single atomic transaction.
        
        Args:
            project_info: Dict with keys: customer_name, project_name, project_start_date (optional)
            df_cost: DataFrame matching tbl_cost_analysis_by_step.csv structure
            df_hours: DataFrame matching tbl_hours_analysis_by_module.csv structure
            df_timeline: DataFrame matching tbl_project_timeline.csv structure
            df_rate: DataFrame matching tbl_rate_calculation.csv structure
        
        Returns:
            Generated project_id
        
        Raises:
            ValueError: If project_info is invalid or DataFrames are empty
            RuntimeError: If any database operation fails (all changes rolled back)
        """
        # Validate project_info
        if not isinstance(project_info, dict):
            raise ValueError("project_info must be a dictionary")
        if 'customer_name' not in project_info or 'project_name' not in project_info:
            raise ValueError("project_info must contain 'customer_name' and 'project_name'")
        
        # Validate DataFrames
        if df_cost.empty or df_hours.empty or df_timeline.empty or df_rate.empty:
            raise ValueError("All DataFrames must contain data")
        
        with self._connection_context() as conn:
            try:
                # Begin transaction
                cursor = conn.cursor()
                self._logger.debug("Starting atomic transaction for complete project insert")
                
                # Step 1: Create project
                project_start_date = project_info.get('project_start_date')
                query = """
                INSERT INTO dim_project (customer_name, project_name, project_start_date)
                VALUES (?, ?, ?)
                """
                cursor.execute(
                    query,
                    (
                        project_info['customer_name'],
                        project_info['project_name'],
                        project_start_date if project_start_date else datetime.now().strftime('%Y-%m-%d')
                    )
                )
                
                cursor.execute("SELECT @@IDENTITY")
                project_id = int(cursor.fetchone()[0])
                self._logger.info(f"Created project_id={project_id}")
                
                # Step 2: Insert cost analysis
                cost_count = self._bulk_insert_cost_analysis_internal(cursor, project_id, df_cost)
                self._logger.debug(f"Inserted {cost_count} cost analysis records")
                
                # Step 3: Insert hours analysis
                hours_count = self._bulk_insert_hours_analysis_internal(cursor, project_id, df_hours)
                self._logger.debug(f"Inserted {hours_count} hours analysis records")
                
                # Step 4: Insert timeline
                timeline_count = self._bulk_insert_timeline_internal(cursor, project_id, df_timeline)
                self._logger.debug(f"Inserted {timeline_count} timeline records")
                
                # Step 5: Insert rate calculation
                rate_count = self._bulk_insert_rate_calculation_internal(cursor, project_id, df_rate)
                self._logger.debug(f"Inserted {rate_count} rate calculation records")
                
                # Commit transaction
                conn.commit()
                self._logger.info(
                    f"Successfully inserted complete project {project_id}: "
                    f"{cost_count} cost, {hours_count} hours, {timeline_count} timeline, {rate_count} rate records"
                )
                cursor.close()
                return project_id
                
            except Exception as e:
                conn.rollback()
                self._logger.exception("Failed to insert project from DataFrames, rolled back")
                raise DatabaseTransactionError("Transaction failed and rolled back") from e
    
    def bulk_insert_cost_analysis(self, project_id: int, df: pd.DataFrame) -> int:
        """
        Insert cost analysis data for a project.
        
        Args:
            project_id: Target project ID
            df: DataFrame with columns: Payment Milestone, Weight, Cost
        
        Returns:
            Number of rows inserted
        
        Raises:
            ValueError: If DataFrame structure is invalid
            RuntimeError: If database operation fails
        """
        # Validate DataFrame
        required_columns = ['Payment Milestone', 'Weight', 'Cost']
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                count = self._bulk_insert_cost_analysis_internal(cursor, project_id, df)
                conn.commit()
                cursor.close()
                self._logger.info(f"Inserted {count} cost analysis records for project_id={project_id}")
                return count
            except Exception as e:
                conn.rollback()
                self._logger.exception(f"Failed to insert cost analysis")
                raise RuntimeError(f"Cost analysis insert failed: {e}")
    
    def bulk_insert_hours_analysis(self, project_id: int, df: pd.DataFrame) -> int:
        """
        Insert hours analysis data for a project.
        
        Args:
            project_id: Target project ID
            df: DataFrame with columns: HCM Modules, Weight, P+M, Plan, Plan.1, A+C, 
                A+C.1, A+C.2, Testing, Deploy, Post Go Live, Weeks/Hours
        
        Returns:
            Number of rows inserted
        
        Raises:
            ValueError: If DataFrame structure is invalid
            RuntimeError: If database operation fails
        """
        required_columns = ['HCM Modules', 'Weight', 'P+M', 'Plan', 'A+C', 'Testing', 
                           'Deploy', 'Post Go Live', 'Weeks/Hours']
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                count = self._bulk_insert_hours_analysis_internal(cursor, project_id, df)
                conn.commit()
                cursor.close()
                self._logger.info(f"Inserted {count} hours analysis records for project_id={project_id}")
                return count
            except Exception as e:
                conn.rollback()
                self._logger.exception(f"Failed to insert hours analysis")
                raise RuntimeError(f"Hours analysis insert failed: {e}")
    
    def bulk_insert_timeline(self, project_id: int, df: pd.DataFrame) -> int:
        """
        Insert timeline data for a project.
        
        Args:
            project_id: Target project ID
            df: DataFrame with columns based on tbl_project_timeline.csv
                First column is header, actual data starts from row 1
        
        Returns:
            Number of rows inserted
        
        Raises:
            ValueError: If DataFrame structure is invalid
            RuntimeError: If database operation fails
        """
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                count = self._bulk_insert_timeline_internal(cursor, project_id, df)
                conn.commit()
                cursor.close()
                self._logger.info(f"Inserted {count} timeline records for project_id={project_id}")
                return count
            except Exception as e:
                conn.rollback()
                self._logger.exception(f"Failed to insert timeline")
                raise RuntimeError(f"Timeline insert failed: {e}")
    
    def bulk_insert_rate_calculation(self, project_id: int, df: pd.DataFrame) -> int:
        """
        Insert rate calculation data for a project.
        
        Args:
            project_id: Target project ID
            df: DataFrame with columns: Module, Hours, Hourly Rate, Total Cost
        
        Returns:
            Number of rows inserted
        
        Raises:
            ValueError: If DataFrame structure is invalid
            RuntimeError: If database operation fails
        """
        required_columns = ['Module', 'Hours', 'Hourly Rate', 'Total Cost']
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                count = self._bulk_insert_rate_calculation_internal(cursor, project_id, df)
                conn.commit()
                cursor.close()
                self._logger.info(f"Inserted {count} rate calculation records for project_id={project_id}")
                return count
            except Exception as e:
                conn.rollback()
                self._logger.exception(f"Failed to insert rate calculation")
                raise RuntimeError(f"Rate calculation insert failed: {e}")
    
    # ==================== INTERNAL HELPER METHODS ====================
    
    def _unpivot_hours_data(self, df: pd.DataFrame, project_id: int) -> pd.DataFrame:
        """
        Transform wide format (Plan, Plan_1, A+C, A+C_1) to narrow format.
        
        Returns DataFrame with columns:
        [project_id, hcm_module, module_weight, phase_code, week_number, planned_hours]
        """
        records = []
        
        for _, row in df.iterrows():
            module = row['HCM Modules']
            weight = row.get('Weight')
            
            # Group columns by phase
            for col in df.columns:
                if col in ['HCM Modules', 'Weight', 'Weeks/Hours']:
                    continue
                    
                # Parse column name: "Plan" or "Plan_1" or "A+C_2"
                base_phase = col.split('_')[0]  # "Plan", "A+C"
                week_num = 1 if '_' not in col else int(col.split('_')[1]) + 1
                
                hours = self._safe_float(row.get(col))
                if hours is not None:
                    records.append({
                        'project_id': project_id,
                        'hcm_module': module,
                        'module_weight': weight,
                        'phase_code': base_phase,
                        'week_number': week_num,
                        'planned_hours': hours
                    })
        
        return pd.DataFrame(records)

    def _get_phase_mapping(self, cursor: pyodbc.Cursor) -> Dict[str, int]:
        """Get phase_code -> phase_id mapping"""
        cursor.execute("SELECT phase_code, phase_id FROM dim_phases")
        return {row[0]: row[1] for row in cursor.fetchall()}

    def _bulk_insert_cost_analysis_internal(
        self,
        cursor: pyodbc.Cursor,
        project_id: int,
        df: pd.DataFrame
    ) -> int:
        """Insert cost analysis by step records"""
        
        query = """
        INSERT INTO fact_cost_analysis_by_step
        (project_id, payment_milestone, weight, cost)
        VALUES (?, ?, ?, ?)
        """
        
        # Filter out total/summary rows
        df_clean = df[~df.iloc[:, 0].str.upper().isin(['TOTAL', 'SUMS'])].copy()
        
        records = [
            (
                project_id,
                str(row.iloc[0])[:255],  # ← ADD [:255] to truncate to 255 chars
                self._safe_float(row.iloc[1], 0.0),
                self._safe_float(row.iloc[2], 0.0)
            )
            for _, row in df_clean.iterrows()
            if pd.notna(row.iloc[0])
        ]
        
        if records:
            cursor.fast_executemany = True
            cursor.executemany(query, records)
        
        return len(records)
    
    def _bulk_insert_hours_analysis_internal(
        self,
        cursor: pyodbc.Cursor,
        project_id: int,
        df: pd.DataFrame
    ) -> int:
        """Insert hours in NEW normalized format with module_id and phase_id"""
        
        # Step 1: Get or create modules in dim_module
        module_ids = {}
        module_column = df.columns[0]  # First column should be module names
        
        for module_name in df[module_column].unique():
            if pd.notna(module_name) and str(module_name).strip():
                module_code = str(module_name).strip()[:50]  # Truncate to 50 chars for code
                module_name_full = str(module_name).strip()[:255]  # Truncate to 255 for name
                
                # Check if module exists
                cursor.execute(
                    "SELECT module_id FROM dim_module WHERE module_code = ?",
                    (module_code,)
                )
                result = cursor.fetchone()
                
                if result:
                    module_ids[module_code] = result[0]
                else:
                    # Insert new module
                    cursor.execute(
                        "INSERT INTO dim_module (module_code, module_name) VALUES (?, ?)",
                        (module_code, module_name_full)
                    )
                    cursor.execute("SELECT @@IDENTITY")
                    module_ids[module_code] = int(cursor.fetchone()[0])
        
        # Step 2: Get phase mappings
        cursor.execute("SELECT phase_code, phase_id FROM dim_phases")
        phase_map = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Step 3: Map DataFrame columns to phase codes
        phase_column_map = {
            'P+M': 'PM',
            'P+M': 'PM',
            'Plan': 'PLAN',
            'PLAN': 'PLAN',
            'A+C': 'AC',
            'AC': 'AC',
            'Testing': 'TESTING',
            'TESTING': 'TESTING',
            'Deploy': 'DEPLOY',
            'DEPLOY': 'DEPLOY',
            'Post Go Live': 'POST_GO_LIVE',
            'POST_GO_LIVE': 'POST_GO_LIVE'
        }
        
        # Step 4: Insert query
        query = """
        INSERT INTO fact_module_phase_hours
        (project_id, module_id, phase_id, week_number, module_start_date, planned_hours, module_weight)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        # Step 5: Build records
        records = []
        current_date = datetime.now().strftime('%Y-%m-%d')  # Default start date
        
        for _, row in df.iterrows():
            module_name = str(row[module_column]).strip()
            if not module_name or pd.isna(row[module_column]):
                continue
                
            module_code = module_name[:50]
            module_id = module_ids.get(module_code)
            
            if not module_id:
                continue
            
            # Get weight if exists
            weight = None
            if 'Weight' in df.columns:
                weight = self._safe_float(row.get('Weight'))
            
            # Process each phase column
            week_counter = 1
            for col in df.columns[1:]:  # Skip first column (module name)
                col_upper = str(col).strip()
                
                # Skip non-phase columns
                if col_upper in ['Weight', 'WEIGHT', 'Total', 'TOTAL']:
                    continue
                
                # Map column to phase code
                phase_code = phase_column_map.get(col_upper)
                if not phase_code:
                    continue
                
                phase_id = phase_map.get(phase_code)
                if not phase_id:
                    continue
                
                # Get hours value
                hours = self._safe_float(row.get(col), 0.0)
                
                # Only insert if hours > 0
                if hours and hours > 0:
                    records.append((
                        project_id,
                        module_id,
                        phase_id,
                        week_counter,
                        current_date,
                        hours,
                        weight
                    ))
                    week_counter += 1
        
        # Step 6: Execute batch insert
        if records:
            cursor.fast_executemany = True
            cursor.executemany(query, records)
        
        return len(records)

    def _bulk_insert_timeline_internal(
        self,
        cursor: pyodbc.Cursor,
        project_id: int,
        df: pd.DataFrame
    ) -> int:
        """Insert timeline with phase_id lookup"""
        
        # Guard against empty DataFrame
        if df.empty:
            self._logger.warning("Timeline DataFrame is empty, skipping timeline insert")
            return 0
        
        # Get phase mappings from dim_phases
        cursor.execute("SELECT phase_code, phase_id FROM dim_phases")
        phase_map = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Also create reverse map with common variations
        phase_name_map = {
            'P+M': 'PM',
            'Project Management': 'PM',
            'PLAN': 'PLAN',
            'Plan': 'PLAN',
            'Planning': 'PLAN',
            'AC': 'AC',
            'A+C': 'AC',
            'Analysis & Configuration': 'AC',
            'TESTING': 'TESTING',
            'Testing': 'TESTING',
            'DEPLOY': 'DEPLOY',
            'Deploy': 'DEPLOY',
            'Deployment': 'DEPLOY',
            'POST_GO_LIVE': 'POST_GO_LIVE',
            'Post Go Live': 'POST_GO_LIVE',
            'Post Go-Live Support': 'POST_GO_LIVE'
        }
        
        query = """
        INSERT INTO fact_project_timeline
        (project_id, phase_id, duration_weeks)
        VALUES (?, ?, ?)
        """
        
        records = []
        
        # Iterate through DataFrame rows
        for idx, row in df.iterrows():
            # Skip header row if it exists
            if idx == 0:
                first_val = str(row.iloc[0]).strip().lower()
                if 'phase' in first_val or 'step' in first_val:
                    continue
            
            phase_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
            duration = self._safe_float(row.iloc[1]) if len(row) > 1 else None
            
            if not phase_name or not duration or duration <= 0:
                continue
            
            # Map phase name to phase_code
            phase_code = phase_name_map.get(phase_name)
            if not phase_code:
                self._logger.warning(f"Unknown phase name: {phase_name}, skipping")
                continue
            
            # Get phase_id
            phase_id = phase_map.get(phase_code)
            if not phase_id:
                self._logger.warning(f"Phase code {phase_code} not found in dim_phases")
                continue
            
            records.append((
                project_id,
                phase_id,
                int(duration)
            ))
        
        # Only execute if we have records
        if records:
            cursor.fast_executemany = True
            cursor.executemany(query, records)
            return len(records)
        else:
            self._logger.warning("No valid timeline records to insert")
            return 0
    
    def _bulk_insert_rate_calculation_internal(
        self,
        cursor: pyodbc.Cursor,
        project_id: int,
        df: pd.DataFrame
    ) -> int:
        """Insert rate calculation with module_id lookup"""
        
        if df.empty:
            self._logger.warning("Rate calculation DataFrame is empty")
            return 0
        
        # Get or create modules
        module_ids = {}
        module_column = df.columns[0]  # First column should be module names
        
        for module_name in df[module_column].unique():
            if pd.notna(module_name) and str(module_name).strip():
                module_name_str = str(module_name).strip()
                module_code = module_name_str[:50]
                module_name_full = module_name_str[:255]
                
                # Check if module exists
                cursor.execute(
                    "SELECT module_id FROM dim_module WHERE module_code = ?",
                    (module_code,)
                )
                result = cursor.fetchone()
                
                if result:
                    module_ids[module_code] = result[0]
                else:
                    # Insert new module (2 parameters, not 3!)
                    cursor.execute(
                        "INSERT INTO dim_module (module_code, module_name) VALUES (?, ?)",
                        (module_code, module_name_full)
                    )
                    cursor.execute("SELECT @@IDENTITY")
                    module_ids[module_code] = int(cursor.fetchone()[0])
        
        query = """
        INSERT INTO fact_rate_calculation
        (project_id, module_id, budgeted_hours, hourly_rate)
        VALUES (?, ?, ?, ?)
        """
        
        # Filter out total/summary rows
        df_clean = df[~df[module_column].astype(str).str.upper().isin(['TOTAL', 'SUMS', 'SUMMARY'])].copy()
        
        records = []
        for _, row in df_clean.iterrows():
            module_name_raw = row[module_column]
            if pd.isna(module_name_raw):
                continue
                
            module_code = str(module_name_raw).strip()[:50]
            module_id = module_ids.get(module_code)
            
            if not module_id:
                continue
            
            # Assuming columns are: Module, Hours, Hourly Rate
            hours = self._safe_float(row.iloc[1], 0.0)
            rate = self._safe_float(row.iloc[2], 0.0)
            
            if hours and hours > 0:
                records.append((
                    project_id,
                    module_id,
                    hours,
                    rate
                ))
        
        if records:
            cursor.fast_executemany = True
            cursor.executemany(query, records)
            return len(records)
        else:
            self._logger.warning("No valid rate calculation records to insert")
            return 0
    
    def _safe_float(self, value: Any, default: float = None) -> Optional[float]:
        """Safely convert value to float, return default if invalid"""
        if pd.isna(value):
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default


    # ==================== READ OPERATIONS ====================
    
    def get_project_by_id(self, project_id: int) -> pd.DataFrame:
        """
        Retrieve a single project by its ID.
        
        Args:
            project_id: Project identifier
        
        Returns:
            DataFrame with single row containing project data, or empty DataFrame if not found
        """
        query = """
        SELECT project_id, customer_name, project_name, project_start_date, 
               created_date, modified_date
        FROM dim_project
        WHERE project_id = ?
        """
        
        try:
            with self._connection_context() as conn:
                df = pd.read_sql(query, conn, params=(project_id,))
                
                if df.empty:
                    self._logger.warning(f"Project with ID {project_id} not found")
                else:
                    self._logger.info(f"Retrieved project_id={project_id}")
                
                return df
                
        except Exception as e:
            self._logger.exception(f"Failed to retrieve project {project_id}")
            return pd.DataFrame()
    
    def get_all_projects(self) -> pd.DataFrame:
        """
        Retrieve all projects from the database.
        
        Returns:
            DataFrame with columns: project_id, customer_name, project_name, project_start_date
        """
        query = """
        SELECT project_id, customer_name, project_name, project_start_date,
               created_date, modified_date
        FROM dim_project
        ORDER BY created_date DESC
        """
        
        try:
            with self._connection_context() as conn:
                df = pd.read_sql(query, conn)
                self._logger.info(f"Retrieved {len(df)} projects")
                return df
                
        except Exception as e:
            self._logger.exception(f"Failed to retrieve all projects")
            return pd.DataFrame()
    
    def get_projects_by_customer(self, customer_name: str) -> pd.DataFrame:
        """
        Retrieve all projects for a specific customer (case-insensitive).
        
        Args:
            customer_name: Customer name to search for (case-insensitive)
        
        Returns:
            DataFrame containing all matching projects
        """
        query = """
        SELECT project_id, customer_name, project_name, project_start_date,
               created_date, modified_date
        FROM dim_project
        WHERE LOWER(customer_name) = LOWER(?)
        ORDER BY created_date DESC
        """
        
        try:
            with self._connection_context() as conn:
                df = pd.read_sql(query, conn, params=(customer_name,))
                self._logger.info(f"Retrieved {len(df)} projects for customer: {customer_name}")
                return df
                
        except Exception as e:
            self._logger.exception(f"Failed to retrieve projects for customer {customer_name}")
            return pd.DataFrame()
    
    def get_complete_project_data(self, project_id: int) -> Dict[str, pd.DataFrame]:
        """
        Retrieve all data for a project from NEW schema.
        Returns dict with DataFrames for each table.
        """
        try:
            with self._connection_context() as conn:
                # 1. Project details
                project_query = """
                SELECT project_id, customer_name, project_name, 
                    project_start_date, project_status, 
                    created_date, modified_date
                FROM dim_project
                WHERE project_id = ?
                """
                df_project = pd.read_sql(project_query, conn, params=(project_id,))
                
                # 2. Cost analysis
                cost_query = """
                SELECT cost_analysis_id, project_id, payment_milestone, 
                    weight, cost, created_date
                FROM fact_cost_analysis_by_step
                WHERE project_id = ?
                ORDER BY cost_analysis_id
                """
                df_cost = pd.read_sql(cost_query, conn, params=(project_id,))
                
                # 3. Hours analysis - NEW NORMALIZED TABLE
                hours_query = """
                SELECT 
                    h.hours_id,
                    h.project_id,
                    m.module_code,
                    m.module_name,
                    ph.phase_code,
                    ph.phase_name,
                    h.week_number,
                    h.module_start_date,
                    h.planned_hours,
                    h.module_weight,
                    h.created_date
                FROM fact_module_phase_hours h
                INNER JOIN dim_module m ON h.module_id = m.module_id
                INNER JOIN dim_phases ph ON h.phase_id = ph.phase_id
                WHERE h.project_id = ?
                ORDER BY m.module_code, ph.default_sequence, h.week_number
                """
                df_hours = pd.read_sql(hours_query, conn, params=(project_id,))
                
                # 4. Timeline
                timeline_query = """
                SELECT 
                    t.timeline_id,
                    t.project_id,
                    ph.phase_code,
                    ph.phase_name,
                    t.duration_weeks,
                    t.start_date,
                    t.end_date,
                    t.created_date
                FROM fact_project_timeline t
                INNER JOIN dim_phases ph ON t.phase_id = ph.phase_id
                WHERE t.project_id = ?
                ORDER BY ph.default_sequence
                """
                df_timeline = pd.read_sql(timeline_query, conn, params=(project_id,))
                
                # 5. Rate calculation
                rate_query = """
                SELECT 
                    r.rate_calc_id,
                    r.project_id,
                    m.module_code,
                    m.module_name,
                    r.budgeted_hours,
                    r.hourly_rate,
                    r.total_cost,
                    r.created_date
                FROM fact_rate_calculation r
                INNER JOIN dim_module m ON r.module_id = m.module_id
                WHERE r.project_id = ?
                ORDER BY m.module_code
                """
                df_rate = pd.read_sql(rate_query, conn, params=(project_id,))
                
                self._logger.info(f"Retrieved complete project data for project {project_id}")
                
                return {
                    'project': df_project,
                    'costanalysis': df_cost,
                    'hoursanalysis': df_hours,
                    'timeline': df_timeline,
                    'ratecalculation': df_rate
                }
                
        except Exception as e:
            self._logger.error(f"Failed to retrieve complete project data for {project_id}")
            self._logger.error(str(e))
            raise DatabaseConnectionError(f"Failed to retrieve project data: {str(e)}")
    
    def get_cost_analysis(self, project_id: int) -> pd.DataFrame:
        """
        Retrieve cost analysis data for a specific project.
        
        Args:
            project_id: Project identifier
        
        Returns:
            DataFrame containing cost analysis records
        """
        query = """
        SELECT cost_analysis_id, project_id, payment_milestone, weight, cost, created_date
        FROM fact_cost_analysis_by_step
        WHERE project_id = ?
        ORDER BY cost_analysis_id
        """
        
        try:
            with self._connection_context() as conn:
                df = pd.read_sql(query, conn, params=(project_id,))
                self._logger.info(f"Retrieved {len(df)} cost analysis records for project_id={project_id}")
                return df
                
        except Exception as e:
            self._logger.exception(f"Failed to retrieve cost analysis for project {project_id}")
            return pd.DataFrame()
    
    def get_hours_analysis(self, project_id: int) -> pd.DataFrame:
        """
        Retrieve hours analysis data for a specific project.
        
        Args:
            project_id: Project identifier
        
        Returns:
            DataFrame containing hours analysis records
        """
        query = """
        SELECT hours_analysis_id, project_id, hcm_module, weight, 
               pm_hours, plan_hours, ac_hours, testing_hours, 
               deploy_hours, post_go_live_hours, total_weeks_hours, created_date
        FROM fact_hours_analysis_by_module
        WHERE project_id = ?
        ORDER BY hours_analysis_id
        """
        
        try:
            with self._connection_context() as conn:
                df = pd.read_sql(query, conn, params=(project_id,))
                self._logger.info(f"Retrieved {len(df)} hours analysis records for project_id={project_id}")
                return df
                
        except Exception as e:
            self._logger.exception(f"Failed to retrieve hours analysis for project {project_id}")
            return pd.DataFrame()
    
    def get_timeline(self, project_id: int) -> pd.DataFrame:
        """
        Retrieve timeline data for a specific project.
        
        Args:
            project_id: Project identifier
        
        Returns:
            DataFrame containing timeline records
        """
        query = """
        SELECT timeline_id, project_id, phase, duration_weeks, 
               start_date, end_date, created_date
        FROM fact_project_timeline
        WHERE project_id = ?
        ORDER BY timeline_id
        """
        
        try:
            with self._connection_context() as conn:
                df = pd.read_sql(query, conn, params=(project_id,))
                self._logger.info(f"Retrieved {len(df)} timeline records for project_id={project_id}")
                return df
                
        except Exception as e:
            self._logger.exception(f"Failed to retrieve timeline for project {project_id}")
            return pd.DataFrame()
    
    def get_rate_calculation(self, project_id: int) -> pd.DataFrame:
        """
        Retrieve rate calculation data for a specific project.
        
        Args:
            project_id: Project identifier
        
        Returns:
            DataFrame containing rate calculation records
        """
        query = """
        SELECT rate_calc_id, project_id, module, hours, 
               hourly_rate, total_cost, created_date
        FROM fact_rate_calculation
        WHERE project_id = ?
        ORDER BY rate_calc_id
        """
        
        try:
            with self._connection_context() as conn:
                df = pd.read_sql(query, conn, params=(project_id,))
                self._logger.info(f"Retrieved {len(df)} rate calculation records for project_id={project_id}")
                return df
                
        except Exception as e:
            self._logger.exception(f"Failed to retrieve rate calculation for project {project_id}")
            return pd.DataFrame()
    
    def search_projects(self, keyword: str) -> pd.DataFrame:
        """
        Search for projects by keyword in customer_name or project_name (case-insensitive).
        
        Args:
            keyword: Search term to find in customer or project names
        
        Returns:
            DataFrame containing all matching projects
        """
        if not keyword or not keyword.strip():
            self._logger.warning("Empty search keyword provided")
            return pd.DataFrame()
        
        query = """
        SELECT project_id, customer_name, project_name, project_start_date,
               created_date, modified_date
        FROM dim_project
        WHERE LOWER(customer_name) LIKE LOWER(?) 
           OR LOWER(project_name) LIKE LOWER(?)
        ORDER BY created_date DESC
        """
        
        search_pattern = f"%{keyword}%"
        
        try:
            with self._connection_context() as conn:
                df = pd.read_sql(query, conn, params=(search_pattern, search_pattern))
                self._logger.info(f"Found {len(df)} projects matching keyword: '{keyword}'")
                return df
                
        except Exception as e:
            self._logger.exception(f"Failed to search projects with keyword '{keyword}'")
            return pd.DataFrame()
    
    # ==================== ADDITIONAL UTILITY READ METHODS ====================
    
    def get_project_count(self) -> int:
        """
        Get total number of projects in the database.
        
        Returns:
            Total project count
        """
        query = "SELECT COUNT(*) FROM dim_project"
        
        try:
            with self._connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                count = cursor.fetchone()[0]
                cursor.close()
                self._logger.info(f"Total projects in database: {count}")
                return count
        except Exception as e:
            self._logger.exception(f"Failed to get project count")
            return 0
    
    def update_cost_analysis(self, cost_analysis_id: int, **kwargs) -> bool:
        """
        Update specific cost analysis record with dynamic field updates.
        
        Args:
            cost_analysis_id: Cost analysis record identifier
            **kwargs: Fields to update - payment_milestone, weight, cost
        
        Returns:
            True if update successful, False otherwise
        
        Raises:
            ValueError: If no valid fields provided or invalid field names
            RuntimeError: If update fails
        """
        # Define allowed fields
        allowed_fields = {'payment_milestone', 'weight', 'cost'}
        
        # Filter to only allowed fields
        update_fields = []
        params = []
        
        for field, value in kwargs.items():
            if field in allowed_fields:
                update_fields.append(f"{field} = ?")
                params.append(value)
            else:
                self._logger.warning(f"Ignoring invalid field: {field}")
        
        if not update_fields:
            raise ValueError(
                f"No valid fields provided. Allowed fields: {allowed_fields}"
            )
        
        # Add cost_analysis_id to params
        params.append(cost_analysis_id)
        
        # Construct query
        query = f"""
        UPDATE fact_cost_analysis_by_step
        SET {', '.join(update_fields)}
        WHERE cost_analysis_id = ?
        """
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query, params)
                rows_affected = cursor.rowcount
                conn.commit()
                cursor.close()
                
                if rows_affected == 0:
                    self._logger.warning(f"No cost analysis record found with ID {cost_analysis_id}")
                    return False
                
                self._logger.info(
                    f"Updated cost_analysis_id={cost_analysis_id}: {len(update_fields)} fields changed"
                )
                return True
                
            except pyodbc.Error as e:
                conn.rollback()
                self._logger.exception(f"Failed to update cost analysis {cost_analysis_id}")
                raise DatabaseConnectionError("Database error updating cost analysis") from e
            except Exception as e:
                conn.rollback()
                self._logger.exception(f"Unexpected error updating cost analysis {cost_analysis_id}")
                raise
    
    def replace_cost_analysis(self, project_id: int, df: pd.DataFrame) -> int:
        """
        Replace all cost analysis data for a project in atomic transaction.
        Deletes existing records and inserts new DataFrame.
        
        Args:
            project_id: Project identifier
            df: DataFrame with columns: Payment Milestone, Weight, Cost
        
        Returns:
            Number of rows inserted
        
        Raises:
            ValueError: If DataFrame is invalid
            RuntimeError: If transaction fails (all changes rolled back)
        """
        # Validate DataFrame
        required_columns = ['Payment Milestone', 'Weight', 'Cost']
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                
                # Step 1: Delete existing records
                delete_query = "DELETE FROM fact_cost_analysis_by_step WHERE project_id = ?"
                cursor.execute(delete_query, (project_id,))
                deleted_count = cursor.rowcount
                self._logger.debug(f"Deleted {deleted_count} existing cost analysis records")
                
                # Step 2: Insert new records
                inserted_count = self._bulk_insert_cost_analysis_internal(cursor, project_id, df)
                self._logger.debug(f"Inserted {inserted_count} new cost analysis records")
                
                # Commit transaction
                conn.commit()
                cursor.close()
                
                self._logger.info(
                    f"Replaced cost analysis for project_id={project_id}: "
                    f"deleted {deleted_count}, inserted {inserted_count}"
                )
                return inserted_count
                
            except Exception as e:
                conn.rollback()
                self._logger.exception(f"Failed to replace cost analysis for project {project_id}, rolled back")
                raise DatabaseTransactionError("Transaction failed and rolled back") from e
    
    def replace_hours_analysis(self, project_id: int, df: pd.DataFrame) -> int:
        """
        Replace all hours analysis data for a project in atomic transaction.
        Deletes existing records and inserts new DataFrame.
        
        Args:
            project_id: Project identifier
            df: DataFrame with columns: HCM Modules, Weight, P+M, Plan, A+C, Testing, 
                Deploy, Post Go Live, Weeks/Hours
        
        Returns:
            Number of rows inserted
        
        Raises:
            ValueError: If DataFrame is invalid
            RuntimeError: If transaction fails (all changes rolled back)
        """
        required_columns = ['HCM Modules', 'Weight', 'P+M', 'Plan', 'A+C', 'Testing', 
                           'Deploy', 'Post Go Live', 'Weeks/Hours']
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                
                # Step 1: Delete existing records
                delete_query = "DELETE FROM fact_hours_analysis_by_module WHERE project_id = ?"
                cursor.execute(delete_query, (project_id,))
                deleted_count = cursor.rowcount
                self._logger.debug(f"Deleted {deleted_count} existing hours analysis records")
                
                # Step 2: Insert new records
                inserted_count = self._bulk_insert_hours_analysis_internal(cursor, project_id, df)
                self._logger.debug(f"Inserted {inserted_count} new hours analysis records")
                
                # Commit transaction
                conn.commit()
                cursor.close()
                
                self._logger.info(
                    f"Replaced hours analysis for project_id={project_id}: "
                    f"deleted {deleted_count}, inserted {inserted_count}"
                )
                return inserted_count
                
            except Exception as e:
                conn.rollback()
                self._logger.exception(f"Failed to replace hours analysis for project {project_id}, rolled back")
                raise DatabaseTransactionError("Transaction failed and rolled back") from e
    
    def replace_timeline(self, project_id: int, df: pd.DataFrame) -> int:
        """
        Replace all timeline data for a project in atomic transaction.
        Deletes existing records and inserts new DataFrame.
        
        Args:
            project_id: Project identifier
            df: DataFrame matching tbl_project_timeline.csv structure
        
        Returns:
            Number of rows inserted
        
        Raises:
            ValueError: If DataFrame is invalid
            RuntimeError: If transaction fails (all changes rolled back)
        """
        if df.empty:
            raise ValueError("Timeline DataFrame cannot be empty")
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                
                # Step 1: Delete existing records
                delete_query = "DELETE FROM fact_project_timeline WHERE project_id = ?"
                cursor.execute(delete_query, (project_id,))
                deleted_count = cursor.rowcount
                self._logger.debug(f"Deleted {deleted_count} existing timeline records")
                
                # Step 2: Insert new records
                inserted_count = self._bulk_insert_timeline_internal(cursor, project_id, df)
                self._logger.debug(f"Inserted {inserted_count} new timeline records")
                
                # Commit transaction
                conn.commit()
                cursor.close()
                
                self._logger.info(
                    f"Replaced timeline for project_id={project_id}: "
                    f"deleted {deleted_count}, inserted {inserted_count}"
                )
                return inserted_count
                
            except Exception as e:
                conn.rollback()
                self._logger.exception(f"Failed to replace timeline for project {project_id}, rolled back")
                raise DatabaseTransactionError("Transaction failed and rolled back") from e
    
    def replace_rate_calculation(self, project_id: int, df: pd.DataFrame) -> int:
        """
        Replace all rate calculation data for a project in atomic transaction.
        Deletes existing records and inserts new DataFrame.
        
        Args:
            project_id: Project identifier
            df: DataFrame with columns: Module, Hours, Hourly Rate, Total Cost
        
        Returns:
            Number of rows inserted
        
        Raises:
            ValueError: If DataFrame is invalid
            RuntimeError: If transaction fails (all changes rolled back)
        """
        required_columns = ['Module', 'Hours', 'Hourly Rate', 'Total Cost']
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                
                # Step 1: Delete existing records
                delete_query = "DELETE FROM fact_rate_calculation WHERE project_id = ?"
                cursor.execute(delete_query, (project_id,))
                deleted_count = cursor.rowcount
                self._logger.debug(f"Deleted {deleted_count} existing rate calculation records")
                
                # Step 2: Insert new records
                inserted_count = self._bulk_insert_rate_calculation_internal(cursor, project_id, df)
                self._logger.debug(f"Inserted {inserted_count} new rate calculation records")
                
                # Commit transaction
                conn.commit()
                cursor.close()
                
                self._logger.info(
                    f"Replaced rate calculation for project_id={project_id}: "
                    f"deleted {deleted_count}, inserted {inserted_count}"
                )
                return inserted_count
                
            except Exception as e:
                conn.rollback()
                self._logger.exception(f"Failed to replace rate calculation for project {project_id}, rolled back")
                raise DatabaseTransactionError("Transaction failed and rolled back") from e
            
    # ==================== DELETE OPERATIONS ====================
    
    def delete_project(self, project_id: int) -> bool:
        """
        Delete a project and all related fact table records (CASCADE).
        
        Due to foreign key CASCADE constraints defined in the schema, deleting a project
        automatically removes all related records in:
        - fact_cost_analysis_by_step
        - fact_hours_analysis_by_module
        - fact_project_timeline
        - fact_rate_calculation
        
        Args:
            project_id: Project identifier to delete
        
        Returns:
            True if deletion successful, False if project not found or deletion fails
        
        Raises:
            RuntimeError: If database error occurs during deletion
        """
        query = "DELETE FROM dim_project WHERE project_id = ?"
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                
                # Check if project exists before deletion
                check_query = "SELECT COUNT(*) FROM dim_project WHERE project_id = ?"
                cursor.execute(check_query, (project_id,))
                exists = cursor.fetchone()[0] > 0
                
                if not exists:
                    self._logger.warning(f"Project with ID {project_id} not found")
                    cursor.close()
                    return False
                
                # Execute delete (CASCADE will handle child records)
                cursor.execute(query, (project_id,))
                rows_affected = cursor.rowcount
                
                conn.commit()
                cursor.close()
                
                if rows_affected > 0:
                    self._logger.info(
                        f"Deleted project_id={project_id} and related records"
                    )
                    return True
                else:
                    self._logger.warning(f"No project deleted with ID {project_id}")
                    return False
                    
            except pyodbc.Error as e:
                conn.rollback()
                self._logger.exception(f"Failed to delete project {project_id}")
                raise DatabaseConnectionError("Database error deleting project") from e
            except Exception as e:
                conn.rollback()
                self._logger.exception(f"Unexpected error deleting project {project_id}")
                raise
    
    def delete_cost_analysis(self, project_id: int) -> int:
        """
        Delete all cost analysis records for a specific project.
        
        Args:
            project_id: Project identifier
        
        Returns:
            Number of records deleted
        
        Raises:
            DatabaseConnectionError: If database error occurs during deletion
        """
        query = "DELETE FROM fact_cost_analysis_by_step WHERE project_id = ?"
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query, (project_id,))
                rows_deleted = cursor.rowcount
                conn.commit()
                cursor.close()
                
                self._logger.info(
                    f"Deleted {rows_deleted} cost analysis records for project_id={project_id}"
                )
                return rows_deleted
                
            except pyodbc.Error as e:
                conn.rollback()
                self._logger.exception(
                    f"Failed to delete cost analysis for project {project_id}"
                )
                raise DatabaseConnectionError("Database error deleting cost analysis") from e
                
            except Exception as e:
                conn.rollback()
                self._logger.exception(
                    f"Unexpected error deleting cost analysis for project {project_id}"
                )
                raise

    def delete_hours_analysis(self, project_id: int) -> int:
        """
        Delete all hours analysis records for a specific project.
        
        Args:
            project_id: Project identifier
        
        Returns:
            Number of records deleted
        
        Raises:
            DatabaseConnectionError: If database error occurs during deletion
        """
        query = "DELETE FROM fact_hours_analysis_by_module WHERE project_id = ?"
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query, (project_id,))
                rows_deleted = cursor.rowcount
                conn.commit()
                cursor.close()
                
                self._logger.info(
                    f"Deleted {rows_deleted} hours analysis records for project_id={project_id}"
                )
                return rows_deleted
                
            except pyodbc.Error as e:
                conn.rollback()
                self._logger.exception(
                    f"Failed to delete hours analysis for project {project_id}"
                )
                raise DatabaseConnectionError("Database error deleting hours analysis") from e
                
            except Exception as e:
                conn.rollback()
                self._logger.exception(
                    f"Unexpected error deleting hours analysis for project {project_id}"
                )
                raise
    
    def delete_timeline(self, project_id: int) -> int:
        """
        Delete all timeline records for a specific project.
        
        Args:
            project_id: Project identifier
        
        Returns:
            Number of records deleted
        
        Raises:
            DatabaseConnectionError: If database error occurs during deletion
        """
        query = "DELETE FROM fact_project_timeline WHERE project_id = ?"
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query, (project_id,))
                rows_deleted = cursor.rowcount
                conn.commit()
                cursor.close()
                
                self._logger.info(
                    f"Deleted {rows_deleted} timeline records for project_id={project_id}"
                )
                return rows_deleted
                
            except pyodbc.Error as e:
                conn.rollback()
                self._logger.exception(
                    f"Failed to delete timeline for project {project_id}"
                )
                raise DatabaseConnectionError("Database error deleting timeline") from e
                
            except Exception as e:
                conn.rollback()
                self._logger.exception(
                    f"Unexpected error deleting timeline for project {project_id}"
                )
                raise
    
    def delete_rate_calculation(self, project_id: int) -> int:
        """
        Delete all rate calculation records for a specific project.
        
        Args:
            project_id: Project identifier
        
        Returns:
            Number of records deleted
        
        Raises:
            DatabaseConnectionError: If database error occurs during deletion
        """
        query = "DELETE FROM fact_rate_calculation WHERE project_id = ?"
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query, (project_id,))
                rows_deleted = cursor.rowcount
                conn.commit()
                cursor.close()
                
                self._logger.info(
                    f"Deleted {rows_deleted} rate calculation records for project_id={project_id}"
                )
                return rows_deleted
                
            except pyodbc.Error as e:
                conn.rollback()
                self._logger.exception(
                    f"Failed to delete rate calculation for project {project_id}"
                )
                raise DatabaseConnectionError("Database error deleting rate calculation") from e
                
            except Exception as e:
                conn.rollback()
                self._logger.exception(
                    f"Unexpected error deleting rate calculation for project {project_id}"
                )
                raise

    # ==================== TRANSACTION MANAGEMENT ====================
    
    @contextmanager
    def transaction(self):
        """
        Context manager for explicit transaction control.
        Auto-commits on success, auto-rollbacks on exception.
        
        Usage:
            with db_manager.transaction() as txn:
                project_id = txn.create_project(...)
                txn.bulk_insert_cost_analysis(project_id, df)
        
        Yields:
            TransactionManager instance with CRUD methods
        """
        txn = TransactionManager(self)
        try:
            yield txn.__enter__()
        except Exception as e:
            txn.__exit__(type(e), e, e.__traceback__)
            raise
        else:
            txn.__exit__(None, None, None)

    # ==================== HELPER METHODS FOR TRANSACTION CONTEXT ====================
    
    def _create_project_with_cursor(
        self,
        cursor: pyodbc.Cursor,
        customer_name: str,
        project_name: str,
        project_start_date: Optional[str] = None
    ) -> int:
        """Internal method to create project using existing cursor"""
        from datetime import datetime
        
        if project_start_date is None:
            project_start_date = datetime.now().strftime('%Y-%m-%d')
        
        query = """
        INSERT INTO dim_project (customer_name, project_name, project_start_date)
        VALUES (?, ?, ?)
        """
        cursor.execute(query, (customer_name, project_name, project_start_date))
        cursor.execute("SELECT @@IDENTITY")
        project_id = int(cursor.fetchone()[0])
        
        self._logger.debug(f"Created project_id={project_id} in transaction")
        return project_id
    
    # ==================== DATAFRAME TRANSFORMATION HELPERS ====================
    
    def _transform_cost_df(self, df: pd.DataFrame, project_id: int) -> pd.DataFrame:
        """
        Transform cost analysis DataFrame for database insertion.
        Adds project_id, validates columns, removes summary rows, and ensures proper data types.
        
        Args:
            df: Raw DataFrame from CSV with columns: Payment Milestone, Weight, Cost
            project_id: Project identifier to add to each row
        
        Returns:
            Transformed DataFrame ready for insertion with columns:
            [project_id, payment_milestone, weight, cost]
        
        Raises:
            ValueError: If DataFrame structure is invalid
        """
        # Validate required columns
        required_columns = ['Payment Milestone', 'Weight', 'Cost']
        self._validate_dataframe(df, required_columns)
        
        # Create copy to avoid modifying original
        df_clean = df.copy()
        
        # Remove summary rows (TOTAL, Sums, etc.)
        df_clean = df_clean[
            ~df_clean['Payment Milestone'].str.upper().isin(['TOTAL', 'SUMS', 'SUM'])
        ]
        
        # Remove rows with null payment milestones
        df_clean = df_clean[df_clean['Payment Milestone'].notna()]
        
        # Add project_id column
        df_clean['project_id'] = project_id
        
        # Rename columns to match database schema
        df_clean = df_clean.rename(columns={
            'Payment Milestone': 'payment_milestone',
            'Weight': 'weight',
            'Cost': 'cost'
        })
        
        # Ensure proper data types
        df_clean['payment_milestone'] = df_clean['payment_milestone'].astype(str).str.strip()
        df_clean['weight'] = pd.to_numeric(df_clean['weight'], errors='coerce')
        df_clean['cost'] = pd.to_numeric(df_clean['cost'], errors='coerce')
        
        # Validate no null values in critical columns
        if df_clean['payment_milestone'].isna().any():
            raise ValueError("Payment Milestone contains null values after transformation")
        if df_clean['weight'].isna().any():
            raise ValueError("Weight contains null values after transformation")
        if df_clean['cost'].isna().any():
            raise ValueError("Cost contains null values after transformation")
        
        # Select and order columns for database
        df_clean = df_clean[['project_id', 'payment_milestone', 'weight', 'cost']]
        
        self._logger.debug(
            f"Transformed cost DataFrame: {len(df_clean)} rows from {len(df)} original rows"
        )
        
        return df_clean
    
    def _transform_hours_df(self, df: pd.DataFrame, project_id: int) -> pd.DataFrame:
        """
        Transform hours analysis DataFrame for database insertion.
        Maps complex CSV structure with multiple phase columns to database schema.
        
        Args:
            df: Raw DataFrame from CSV with columns: HCM Modules, Weight, P+M, Plan, 
                A+C, Testing, Deploy, Post Go Live, Weeks/Hours
            project_id: Project identifier to add to each row
        
        Returns:
            Transformed DataFrame ready for insertion with columns:
            [project_id, hcm_module, weight, pm_hours, plan_hours, ac_hours, 
             testing_hours, deploy_hours, post_go_live_hours, total_weeks_hours]
        
        Raises:
            ValueError: If DataFrame structure is invalid
        """
        # Validate required columns
        required_columns = ['HCM Modules', 'Weight', 'P+M', 'Plan', 'Testing', 
                           'Deploy', 'Post Go Live', 'Weeks/Hours']
        self._validate_dataframe(df, required_columns)
        
        # Create copy
        df_clean = df.copy()
        
        # Remove header/summary rows
        df_clean = df_clean[
            ~df_clean['HCM Modules'].str.upper().isin(['START DATE', 'WEEKS EFFORT', 'SUMS', 'SUM'])
        ]
        
        # Remove rows with null module names
        df_clean = df_clean[df_clean['HCM Modules'].notna()]
        
        # Add project_id
        df_clean['project_id'] = project_id
        
        # Map phase columns to database columns
        # Hours can be stored in different "Plan" columns (Plan, Plan.1, Plan.2, etc.)
        # Use the first available numeric value from Plan columns
        plan_cols = [col for col in df_clean.columns if col.startswith('Plan')]
        df_clean['plan_hours_combined'] = df_clean[plan_cols].apply(
            lambda row: self._get_first_numeric(row), axis=1
        )
        
        # Similarly for A+C columns
        ac_cols = [col for col in df_clean.columns if col.startswith('A+C')]
        df_clean['ac_hours_combined'] = df_clean[ac_cols].apply(
            lambda row: self._get_first_numeric(row), axis=1
        )
        
        # Rename and convert columns
        df_clean = df_clean.rename(columns={
            'HCM Modules': 'hcm_module',
            'Weight': 'weight',
            'P+M': 'pm_hours',
            'Testing': 'testing_hours',
            'Deploy': 'deploy_hours',
            'Post Go Live': 'post_go_live_hours',
            'Weeks/Hours': 'total_weeks_hours'
        })
        
        # Use combined hours for plan and ac
        df_clean['plan_hours'] = df_clean['plan_hours_combined']
        df_clean['ac_hours'] = df_clean['ac_hours_combined']
        
        # Clean and convert to numeric
        numeric_columns = ['weight', 'pm_hours', 'plan_hours', 'ac_hours', 
                          'testing_hours', 'deploy_hours', 'post_go_live_hours', 'total_weeks_hours']
        
        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        # Clean module names
        df_clean['hcm_module'] = df_clean['hcm_module'].astype(str).str.strip()
        
        # Select final columns
        df_clean = df_clean[[
            'project_id', 'hcm_module', 'weight', 'pm_hours', 'plan_hours', 
            'ac_hours', 'testing_hours', 'deploy_hours', 'post_go_live_hours', 
            'total_weeks_hours'
        ]]
        
        self._logger.debug(
            f"Transformed hours DataFrame: {len(df_clean)} rows from {len(df)} original rows"
        )
        
        return df_clean
    
    def _transform_timeline_df(self, df: pd.DataFrame, project_id: int) -> pd.DataFrame:
        """
        Transform timeline DataFrame for database insertion.
        Handles unusual CSV structure and calculates start/end dates for each phase.
        
        Args:
            df: Raw DataFrame from CSV with unusual structure:
                Columns: ['Project Start Date:', '2025-11-10 00:00:00']
                Data starts from row 1 with phase and duration
            project_id: Project identifier to add to each row
        
        Returns:
            Transformed DataFrame ready for insertion with columns:
            [project_id, phase, duration_weeks, start_date, end_date]
        
        Raises:
            ValueError: If DataFrame structure is invalid
        """
        if df.empty or len(df.columns) < 2:
            raise ValueError("Timeline DataFrame has invalid structure")
        
        # Extract project start date from column header
        project_start_date = None
        if len(df.columns) > 1:
            start_date_str = df.columns[1]
            try:
                project_start_date = pd.to_datetime(start_date_str)
            except:
                # If parsing fails, try to find it in the data
                self._logger.warning(f"Could not parse start date from columns: {start_date_str}")
        
        # Create clean DataFrame from rows (skip row 0 which is the header row)
        records = []
        current_date = project_start_date
        
        for i in range(1, len(df)):
            phase = df.iloc[i, 0]
            duration = df.iloc[i, 1]
            
            # Skip invalid rows
            if pd.isna(phase) or pd.isna(duration):
                continue
            
            try:
                duration_weeks = int(duration)
            except (ValueError, TypeError):
                self._logger.warning(f"Invalid duration for phase {phase}: {duration}")
                continue
            
            # Calculate dates if start date available
            if current_date is not None:
                start_date = current_date
                end_date = current_date + timedelta(weeks=duration_weeks)
                current_date = end_date  # Next phase starts where this one ends
            else:
                start_date = None
                end_date = None
            
            records.append({
                'project_id': project_id,
                'phase': str(phase).strip(),
                'duration_weeks': duration_weeks,
                'start_date': start_date,
                'end_date': end_date
            })
        
        if not records:
            raise ValueError("No valid timeline records found in DataFrame")
        
        df_clean = pd.DataFrame(records)
        
        self._logger.debug(
            f"Transformed timeline DataFrame: {len(df_clean)} rows with date calculations"
        )
        
        return df_clean
    
    def _transform_rate_df(self, df: pd.DataFrame, project_id: int) -> pd.DataFrame:
        """
        Transform rate calculation DataFrame for database insertion.
        Validates numeric types and removes summary rows.
        
        Args:
            df: Raw DataFrame from CSV with columns: Module, Hours, Hourly Rate, Total Cost
            project_id: Project identifier to add to each row
        
        Returns:
            Transformed DataFrame ready for insertion with columns:
            [project_id, module, hours, hourly_rate, total_cost]
        
        Raises:
            ValueError: If DataFrame structure is invalid or numeric validation fails
        """
        # Validate required columns
        required_columns = ['Module', 'Hours', 'Hourly Rate', 'Total Cost']
        self._validate_dataframe(df, required_columns)
        
        # Create copy
        df_clean = df.copy()
        
        # Remove summary rows
        df_clean = df_clean[
            ~df_clean['Module'].str.upper().isin(['TOTAL', 'SUMS', 'SUM'])
        ]
        
        # Remove rows with null modules
        df_clean = df_clean[df_clean['Module'].notna()]
        
        # Add project_id
        df_clean['project_id'] = project_id
        
        # Rename columns
        df_clean = df_clean.rename(columns={
            'Module': 'module',
            'Hours': 'hours',
            'Hourly Rate': 'hourly_rate',
            'Total Cost': 'total_cost'
        })
        
        # Clean module names
        df_clean['module'] = df_clean['module'].astype(str).str.strip()
        
        # Convert numeric columns with validation
        df_clean['hours'] = pd.to_numeric(df_clean['hours'], errors='coerce')
        df_clean['hourly_rate'] = pd.to_numeric(df_clean['hourly_rate'], errors='coerce')
        df_clean['total_cost'] = pd.to_numeric(df_clean['total_cost'], errors='coerce')
        
        # Validate hours and total_cost (hourly_rate can be null)
        if df_clean['hours'].isna().any():
            raise ValueError("Hours column contains invalid numeric values")
        if df_clean['total_cost'].isna().any():
            raise ValueError("Total Cost column contains invalid numeric values")
        
        # Hourly rate can be null, so just log warning
        null_rates = df_clean['hourly_rate'].isna().sum()
        if null_rates > 0:
            self._logger.debug(f"Rate calculation has {null_rates} rows with null hourly_rate")
        
        # Select final columns
        df_clean = df_clean[['project_id', 'module', 'hours', 'hourly_rate', 'total_cost']]
        
        self._logger.debug(
            f"Transformed rate DataFrame: {len(df_clean)} rows from {len(df)} original rows"
        )
        
        return df_clean
    
    def _validate_dataframe(self, df: pd.DataFrame, required_columns: List[str]) -> None:
        """
        Validate DataFrame has required columns and is not empty.
        
        Args:
            df: DataFrame to validate
            required_columns: List of column names that must exist
        
        Raises:
            ValueError: If validation fails
        """
        if df is None:
            raise ValueError("DataFrame cannot be None")
        
        if df.empty:
            raise ValueError("DataFrame cannot be empty")
        
        # Check for required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(
                f"DataFrame missing required columns: {missing_columns}. "
                f"Available columns: {list(df.columns)}"
            )
        
        # Check if DataFrame has any data rows
        if len(df) == 0:
            raise ValueError("DataFrame has no data rows")
        
        self._logger.debug(f"DataFrame validation passed: {len(df)} rows, {len(df.columns)} columns")
    
    # ==================== UTILITY METHODS ====================
    
    def initialize_phases(self) -> int:
        """
        Initialize dim_phases table with standard project phases.
        Should be called once during database setup or after schema creation.
        
        Returns:
            Number of phases inserted
            
        Raises:
            DatabaseConnectionError: If insertion fails
        """
        phases = [
            ('P+M', 'Planning & Management', 1),
            ('Plan', 'Planning', 2),
            ('A+C', 'Analysis & Configuration', 3),
            ('Testing', 'Testing', 4),
            ('Deploy', 'Deployment', 5),
            ('Post Go Live', 'Post Go Live Support', 6)
        ]
        
        query = """
            INSERT INTO dim_phases (phase_code, phase_name, default_sequence)
            VALUES (?, ?, ?)
        """
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                cursor.fast_executemany = True
                cursor.executemany(query, phases)
                conn.commit()
                rows_inserted = len(phases)
                cursor.close()
                self._logger.info(f"Initialized {rows_inserted} phases in dim_phases")
                return rows_inserted
            except pyodbc.Error as e:
                conn.rollback()
                self._logger.exception("Failed to initialize phases")
                raise DatabaseConnectionError("Phase initialization failed") from e
            
    def get_connection_pool_stats(self) -> Dict[str, Any]:
        """
        Get current connection pool statistics for monitoring and debugging.
        
        Returns:
            Dictionary containing:
            - pool_size: Base pool size configuration
            - max_overflow: Maximum overflow connections allowed
            - available_connections: Currently available connections in pool
            - active_connections: Total active connections
            - total_capacity: Maximum possible connections (pool_size + max_overflow)
            - utilization_percent: Percentage of pool being used
        
        Example:
            >>> stats = db_manager.get_connection_pool_stats()
            >>> print(f"Pool utilization: {stats['utilization_percent']:.1f}%")
        """
        if not hasattr(self, '_connection_pool'):
            self._logger.warning("Connection pool not initialized")
            return {}
        
        stats = self._connection_pool.get_stats()
        
        # Calculate utilization percentage
        if stats['total_capacity'] > 0:
            in_use = stats['active_connections'] - stats['available_connections']
            utilization = (in_use / stats['total_capacity']) * 100
        else:
            utilization = 0.0
        
        stats['utilization_percent'] = round(utilization, 2)
        stats['connections_in_use'] = stats['active_connections'] - stats['available_connections']
        
        self._logger.debug(f"Connection pool statistics: {stats}")
        
        return stats
    
    def execute_custom_query(self, query: str) -> pd.DataFrame:
        """
        Execute a read-only query and return results as DataFrame.
        Supports SELECT statements and CTEs (WITH clause).
        """
        
        # Validate query is read-only (SELECT or CTE)
        query_stripped = query.strip().upper()
        
        # Allow SELECT and WITH (CTEs) - both are read-only
        allowed_starts = ('SELECT', 'WITH')
        
        if not query_stripped.startswith(allowed_starts):
            raise ValueError(
                "Only read-only queries (SELECT, WITH) are allowed. "
                "Use execute_custom_command() for INSERT/UPDATE/DELETE."
            )
        
        # Additional safety check - detect write operations anywhere in query
        write_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'TRUNCATE', 'ALTER', 'CREATE']
        
        # Only check for write keywords NOT inside string literals or comments
        # Simple check: look for these keywords followed by whitespace/end
        for keyword in write_keywords:
            # Check if keyword appears as a standalone word
            import re
            if re.search(rf'\b{keyword}\b', query_stripped):
                raise ValueError(
                    f"Write operation detected ({keyword}). "
                    f"Use execute_custom_command() for data modifications."
                )
        
        # Rest of your existing code...
        try:
            with self._get_connection() as conn:
                self._logger.info(f"Executing custom query...")
                df = pd.read_sql(query, conn)
                self._logger.info(f"Custom query executed successfully, rows: {len(df)}, columns: {len(df.columns)}")
                return df
        except Exception as e:
            self._logger.error(f"Unexpected error executing custom query")
            raise
    
    def execute_custom_command(
        self, 
        query: str, 
        params: Optional[Tuple] = None
    ) -> int:
        """
        Execute INSERT, UPDATE, or DELETE command with SQL injection prevention.
        Uses parameterized queries for security.
        
        Args:
            query: SQL command with ? placeholders for parameters
            params: Tuple of parameters to substitute (prevents SQL injection)
        
        Returns:
            Number of rows affected
        
        Raises:
            ValueError: If query is a SELECT statement or contains unsafe patterns
            RuntimeError: If command execution fails
        
        Security:
            - Blocks SELECT statements (use execute_custom_query instead)
            - Uses parameterized queries (? placeholders)
            - Automatic transaction management with rollback on error
        
        Example:
            >>> # Safe: Parameterized UPDATE
            >>> rows_affected = db_manager.execute_custom_command(
            ...     "UPDATE dim_project SET customer_name = ? WHERE project_id = ?",
            ...     params=("New Name", 123)
            ... )
        """
        # Validate query is NOT a SELECT statement
        query_stripped = query.strip().upper()
        if query_stripped.startswith('SELECT'):
            raise ValueError(
                "SELECT queries are not allowed. "
                "Use execute_custom_query() for SELECT statements."
            )
        
        # Log command execution
        query_preview = self._sanitize_query_for_logging(query)
        self._logger.debug(
            f"Executing custom command",
            query_preview=query_preview,
            params_count=len(params) if params else 0
        )
        
        with self._connection_context() as conn:
            try:
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                rows_affected = cursor.rowcount
                conn.commit()
                cursor.close()
                
                self._logger.info(
                    f"Custom command executed successfully",
                    rows_affected=rows_affected
                )
                
                return rows_affected
                
            except pyodbc.Error as e:
                conn.rollback()
                self._logger.exception(f"Custom command failed, rolled back")
                raise DatabaseConnectionError("Command execution failed") from e
            except Exception as e:
                conn.rollback()
                self._logger.exception("Unexpected error executing custom command, rolled back")
                raise

    # ==================== MISC ====================
    def _get_first_numeric(self, row: pd.Series) -> Optional[float]:
        """
        Extract first non-null numeric value from a pandas Series.
        Used for handling multi-column hour data (Plan, Plan.1, Plan.2, etc.)
        """
        for val in row:
            if pd.notna(val):
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue
        return None
    
    def _sanitize_query_for_logging(self, query: str) -> str:
        """Truncate query for safe logging (prevents log overflow)"""
        query_clean = ' '.join(query.split())  # Collapse whitespace
        return query_clean[:200] + ('...' if len(query_clean) > 200 else '')
        
class TransactionManager:
    """
    Transaction context manager for explicit transaction control.
    Provides auto-commit on success and auto-rollback on exception.
    """
    
    def __init__(self, db_manager: 'AzureSQLDBManager'):
        """
        Initialize transaction manager.
        
        Args:
            db_manager: Parent AzureSQLDBManager instance
        """
        self.db_manager = db_manager
        self.connection = None
        self.cursor = None
        self._transaction_active = False
    
    def __enter__(self):
        """Enter transaction context - acquire connection and begin transaction"""
        self.connection = self.db_manager._get_connection()
        self.cursor = self.connection.cursor()
        
        # pyodbc transactions begin implicitly with first query
        # Set autocommit to False to ensure transaction control
        self.connection.autocommit = False
        self._transaction_active = True
        
        self.db_manager._logger.debug("Transaction started")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit transaction context - commit on success, rollback on exception"""
        try:
            if exc_type is None:
                # No exception occurred - commit transaction
                self.connection.commit()
                self.db_manager._logger.info("Transaction committed successfully")
            else:
                # Exception occurred - rollback transaction
                self.connection.rollback()
                self.db_manager._logger.warning(
                    f"Transaction rolled back due to exception: {exc_type.__name__}: {exc_val}"
                )
        finally:
            # Clean up resources
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.db_manager._connection_pool.release(self.connection)
            self._transaction_active = False
        
        # Propagate exception if one occurred
        return False
    
    # Expose all CRUD methods through transaction context
    def create_project(self, customer_name: str, project_name: str, 
                      project_start_date: Optional[str] = None) -> int:
        """Create project within transaction"""
        return self.db_manager._create_project_with_cursor(
            self.cursor, customer_name, project_name, project_start_date
        )
    
    def bulk_insert_cost_analysis(self, project_id: int, df) -> int:
        """Insert cost analysis within transaction"""
        return self.db_manager._bulk_insert_cost_analysis_internal(
            self.cursor, project_id, df
        )
    
    def bulk_insert_hours_analysis(self, project_id: int, df) -> int:
        """Insert hours analysis within transaction"""
        return self.db_manager._bulk_insert_hours_analysis_internal(
            self.cursor, project_id, df
        )
    
    def bulk_insert_timeline(self, project_id: int, df) -> int:
        """Insert timeline within transaction"""
        return self.db_manager._bulk_insert_timeline_internal(
            self.cursor, project_id, df
        )
    
    def bulk_insert_rate_calculation(self, project_id: int, df) -> int:
        """Insert rate calculation within transaction"""
        return self.db_manager._bulk_insert_rate_calculation_internal(
            self.cursor, project_id, df
        )
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute custom query within transaction"""
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        return self.cursor
     
# Example usage
if __name__ == "__main__":
    # Initialize database manager
    db_manager = AzureSQLDBManager(
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
    
    # Check pool stats
    stats = db_manager.get_connection_pool_stats()
    print(f"Connection Pool Stats: {stats}")
    
    # Test connection acquisition
    with db_manager._connection_context() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT GETDATE()")
        result = cursor.fetchone()
        print(f"Current database time: {result[0]}")
        cursor.close()
    
    # Cleanup on shutdown
    db_manager.close_all_connections()