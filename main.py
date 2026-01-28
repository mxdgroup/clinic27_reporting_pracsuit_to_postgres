from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import logging
import json
from datetime import datetime
from pathlib import Path
import re
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
import pandas as pd
import base64
import io
import time
import uuid
import traceback
from contextlib import contextmanager

# Load environment variables
load_dotenv()

# Configure logging with UTF-8 encoding
import sys

# Use JSON-like structured logging for Railway
LOG_FORMAT = '%(asctime)s | %(levelname)s | %(name)s | request_id=%(request_id)s | %(message)s'

class RequestIdFilter(logging.Filter):
    """Add request_id to log records"""
    def filter(self, record):
        if not hasattr(record, 'request_id'):
            record.request_id = 'no-request'
        return True

# Set UTF-8 encoding on stdout to handle Unicode characters
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass  # Ignore if reconfigure not available

# Console handler (always available - this is what Railway captures)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
console_handler.addFilter(RequestIdFilter())

handlers = [console_handler]

# File handler - optional, only if filesystem is writable (not on Railway)
try:
    file_handler = logging.FileHandler('email_logs.log', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    file_handler.addFilter(RequestIdFilter())
    handlers.append(file_handler)
except (OSError, PermissionError) as e:
    # On Railway or read-only filesystems, skip file logging
    print(f"Note: File logging disabled ({type(e).__name__}), using console only", file=sys.stderr)

logging.basicConfig(
    level=logging.DEBUG,
    handlers=handlers,
    force=True  # Override any existing config
)

logger = logging.getLogger(__name__)

# Context variable for request ID
_request_id_var = None

def get_request_id():
    """Get current request ID or generate a new one"""
    global _request_id_var
    return _request_id_var or 'no-request'

def set_request_id(request_id: str):
    """Set the current request ID"""
    global _request_id_var
    _request_id_var = request_id

def log_with_request_id(level, message, **kwargs):
    """Log with request ID in extra"""
    extra = {'request_id': get_request_id()}
    if level == 'debug':
        logger.debug(message, extra=extra, **kwargs)
    elif level == 'info':
        logger.info(message, extra=extra, **kwargs)
    elif level == 'warning':
        logger.warning(message, extra=extra, **kwargs)
    elif level == 'error':
        logger.error(message, extra=extra, **kwargs)
    elif level == 'critical':
        logger.critical(message, extra=extra, **kwargs)

@contextmanager
def log_timing(operation_name: str):
    """Context manager for timing operations"""
    start_time = time.time()
    log_with_request_id('debug', f"[TIMING] Starting: {operation_name}")
    try:
        yield
    finally:
        elapsed_ms = (time.time() - start_time) * 1000
        log_with_request_id('info', f"[TIMING] Completed: {operation_name} | elapsed_ms={elapsed_ms:.2f}")

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', ''),
    'admin_db': os.getenv('POSTGRES_ADMIN_DB', 'postgres')
}

# Log startup configuration (mask sensitive data)
def log_startup_config():
    """Log startup configuration for debugging"""
    logger.info("=" * 80, extra={'request_id': 'startup'})
    logger.info("APPLICATION STARTUP", extra={'request_id': 'startup'})
    logger.info("=" * 80, extra={'request_id': 'startup'})
    logger.info(f"Python version: {sys.version}", extra={'request_id': 'startup'})
    logger.info(f"Working directory: {os.getcwd()}", extra={'request_id': 'startup'})
    logger.info(f"POSTGRES_HOST: {DB_CONFIG['host']}", extra={'request_id': 'startup'})
    logger.info(f"POSTGRES_PORT: {DB_CONFIG['port']}", extra={'request_id': 'startup'})
    logger.info(f"POSTGRES_USER: {DB_CONFIG['user']}", extra={'request_id': 'startup'})
    logger.info(f"POSTGRES_PASSWORD: {'***SET***' if DB_CONFIG['password'] else '***NOT SET***'}", extra={'request_id': 'startup'})
    logger.info(f"POSTGRES_ADMIN_DB: {DB_CONFIG['admin_db']}", extra={'request_id': 'startup'})
    logger.info(f"ONLINE_APPOINTMENTS_DB: {os.getenv('ONLINE_APPOINTMENTS_DB', 'appointments_online')}", extra={'request_id': 'startup'})
    logger.info(f"PORT env var: {os.getenv('PORT', 'not set')}", extra={'request_id': 'startup'})
    logger.info("=" * 80, extra={'request_id': 'startup'})

# Online appointments configuration
ONLINE_APPOINTMENTS_DB = os.getenv('ONLINE_APPOINTMENTS_DB', 'appointments_online')
ONLINE_APPOINTMENTS_KEYWORD = os.getenv(
    'ONLINE_APPOINTMENTS_KEYWORD',
    'Saved filters: Appointments - Online (Last Week)'
)


def extract_clinic_name(email_address: str) -> str:
    """
    Extract clinic name from email address between + and @
    Example: developers.mxd+supertest@gmail.com -> supertest
    """
    match = re.search(r'\+([^@]+)@', email_address)
    if match:
        clinic_name = match.group(1).lower()
        # Sanitize database name (only alphanumeric and underscore)
        clinic_name = re.sub(r'[^a-z0-9_]', '_', clinic_name)
        return clinic_name
    return None


def extract_table_name(filename: str) -> str:
    """
    Extract table name from filename
    Example: 
      "Appointment Report 281025_1151PM.xlsx" -> "appointments"
      "Client List Report 291025_0710PM.xlsx" -> "clients"
    """
    filename_lower = filename.lower()
    
    # Check for known report types
    if filename_lower.startswith('appointment'):
        return 'appointments'
    elif filename_lower.startswith('client list'):
        return 'clients'
    else:
        # Fallback: get first word and pluralize
        first_word = filename.split()[0] if filename else ""
        table_name = first_word.lower()
        if table_name and not table_name.endswith('s'):
            table_name += 's'
        return table_name


def is_online_appointments_email(email_data: dict) -> bool:
    """Determine if the email relates to the weekly online appointment export"""
    keyword = (ONLINE_APPOINTMENTS_KEYWORD or '').strip().lower()
    if not keyword:
        return False
    # Search through available body fields for the configured keyword
    for field in ('body', 'bodyHtml'):
        content = email_data.get(field)
        if isinstance(content, str) and keyword in content.lower():
            return True
    return False


def get_db_connection(database_name: str = None, max_retries: int = 3, retry_delay: float = 1.0):
    """Get a connection to the specified database or admin database with retry logic"""
    db = database_name or DB_CONFIG['admin_db']
    
    for attempt in range(1, max_retries + 1):
        try:
            log_with_request_id('debug', f"[DB] Attempting connection to database '{db}' (attempt {attempt}/{max_retries})")
            log_with_request_id('debug', f"[DB] Connection params: host={DB_CONFIG['host']}, port={DB_CONFIG['port']}, user={DB_CONFIG['user']}")
            
            start_time = time.time()
            conn = psycopg2.connect(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                database=db,
                connect_timeout=10  # 10 second connection timeout
            )
            elapsed_ms = (time.time() - start_time) * 1000
            
            log_with_request_id('info', f"[DB] Connected to database '{db}' successfully | elapsed_ms={elapsed_ms:.2f}")
            return conn
            
        except psycopg2.OperationalError as e:
            log_with_request_id('error', f"[DB] Connection failed to '{db}' (attempt {attempt}/{max_retries}): {str(e)}")
            if attempt < max_retries:
                log_with_request_id('info', f"[DB] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                log_with_request_id('critical', f"[DB] All {max_retries} connection attempts failed for database '{db}'")
                raise
        except Exception as e:
            log_with_request_id('error', f"[DB] Unexpected error connecting to '{db}': {type(e).__name__}: {str(e)}")
            log_with_request_id('error', f"[DB] Traceback: {traceback.format_exc()}")
            raise


def database_exists(database_name: str) -> bool:
    """Check if database exists"""
    log_with_request_id('debug', f"[DB] Checking if database '{database_name}' exists")
    try:
        with log_timing(f"database_exists check for '{database_name}'"):
            conn = get_db_connection()
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (database_name,)
            )
            exists = cursor.fetchone() is not None
            
            cursor.close()
            conn.close()
            
            log_with_request_id('info', f"[DB] Database '{database_name}' exists: {exists}")
            return exists
    except Exception as e:
        log_with_request_id('error', f"[DB] Error checking database existence for '{database_name}': {type(e).__name__}: {str(e)}")
        log_with_request_id('error', f"[DB] Traceback: {traceback.format_exc()}")
        return False


def create_database(database_name: str):
    """Create a new database if it doesn't exist"""
    log_with_request_id('info', f"[DB] create_database called for '{database_name}'")
    try:
        if database_exists(database_name):
            log_with_request_id('info', f"[DB] Database '{database_name}' already exists, skipping creation")
            return True
        
        log_with_request_id('info', f"[DB] Creating new database '{database_name}'")
        with log_timing(f"create database '{database_name}'"):
            conn = get_db_connection()
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # Create database
            cursor.execute(f'CREATE DATABASE {database_name}')
            log_with_request_id('info', f"[DB] Database '{database_name}' created successfully")
            
            cursor.close()
            conn.close()
        return True
    except Exception as e:
        log_with_request_id('error', f"[DB] Error creating database '{database_name}': {type(e).__name__}: {str(e)}")
        log_with_request_id('error', f"[DB] Traceback: {traceback.format_exc()}")
        return False


def create_appointments_table(database_name: str):
    """Create appointments table in the specified database"""
    log_with_request_id('info', f"[TABLE] Creating appointments table in database '{database_name}'")
    try:
        with log_timing(f"create appointments table in '{database_name}'"):
            conn = get_db_connection(database_name)
            cursor = conn.cursor()
            
            # Create appointments table with schema matching Excel structure
            create_table_query = """
            CREATE TABLE IF NOT EXISTS appointments (
                id SERIAL PRIMARY KEY,
                appointment_date TIMESTAMP,
                client VARCHAR(255),
                appointment_type VARCHAR(255),
                profession VARCHAR(255),
                client_duration NUMERIC,
                practitioner VARCHAR(255),
                business VARCHAR(255),
                appointment_status VARCHAR(100),
                column_number NUMERIC,
                billed_status VARCHAR(100),
                clinical_note TEXT,
                client_id BIGINT,
                appointment_id BIGINT UNIQUE,
                address_1 VARCHAR(255),
                address_2 VARCHAR(255),
                address_3 VARCHAR(255),
                address_4 VARCHAR(255),
                suburb VARCHAR(100),
                state VARCHAR(100),
                postcode VARCHAR(20),
                country VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            log_with_request_id('debug', f"[TABLE] Executing CREATE TABLE for appointments")
            cursor.execute(create_table_query)
            
            # Create indexes for common queries
            log_with_request_id('debug', f"[TABLE] Creating indexes for appointments table")
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_appointments_date 
                ON appointments(appointment_date)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_appointments_client_id 
                ON appointments(client_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_appointments_appointment_id 
                ON appointments(appointment_id)
            """)
            
            conn.commit()
            log_with_request_id('info', f"[TABLE] Appointments table created successfully in database '{database_name}'")
            
            cursor.close()
            conn.close()
        return True
    except Exception as e:
        log_with_request_id('error', f"[TABLE] Error creating appointments table in '{database_name}': {type(e).__name__}: {str(e)}")
        log_with_request_id('error', f"[TABLE] Traceback: {traceback.format_exc()}")
        return False


def create_clients_table(database_name: str):
    """Create clients table in the specified database"""
    log_with_request_id('info', f"[TABLE] Creating clients table in database '{database_name}'")
    try:
        with log_timing(f"create clients table in '{database_name}'"):
            conn = get_db_connection(database_name)
            cursor = conn.cursor()
            
            # Create clients table with schema matching Excel structure
            create_table_query = """
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                title VARCHAR(50),
                first_name VARCHAR(255),
                preferred_name VARCHAR(255),
                middle VARCHAR(255),
                surname VARCHAR(255),
                date_of_birth VARCHAR(50),
                address_line_1 VARCHAR(255),
                address_line_2 VARCHAR(255),
                address_line_3 VARCHAR(255),
                address_line_4 VARCHAR(255),
                country VARCHAR(100),
                state VARCHAR(100),
                suburb VARCHAR(100),
                postcode VARCHAR(20),
                preferred_phone VARCHAR(50),
                work_phone VARCHAR(50),
                home_phone VARCHAR(50),
                mobile VARCHAR(50),
                fax VARCHAR(50),
                email VARCHAR(255),
                file_no BIGINT,
                gender VARCHAR(50),
                pronouns VARCHAR(50),
                sex VARCHAR(50),
                archived VARCHAR(10),
                notes TEXT,
                warnings TEXT,
                fee_category VARCHAR(255),
                practitioner VARCHAR(255),
                medicare_no VARCHAR(50),
                medicare_irn VARCHAR(50),
                medicare_expiry VARCHAR(50),
                dva_no VARCHAR(50),
                dva_type VARCHAR(50),
                concession_no VARCHAR(50),
                concession_expiry VARCHAR(50),
                health_fund VARCHAR(255),
                health_fund_member_no VARCHAR(50),
                ndis_no VARCHAR(50),
                created_date TIMESTAMP,
                consent_date TIMESTAMP,
                privacy_policy_date TIMESTAMP,
                client_id BIGINT UNIQUE,
                gp_name VARCHAR(255),
                db_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                db_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            log_with_request_id('debug', f"[TABLE] Executing CREATE TABLE for clients")
            cursor.execute(create_table_query)
            
            # Create indexes for common queries
            log_with_request_id('debug', f"[TABLE] Creating indexes for clients table")
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_clients_client_id 
                ON clients(client_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_clients_email 
                ON clients(email)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_clients_surname 
                ON clients(surname)
            """)
            
            conn.commit()
            log_with_request_id('info', f"[TABLE] Clients table created successfully in database '{database_name}'")
            
            cursor.close()
            conn.close()
        return True
    except Exception as e:
        log_with_request_id('error', f"[TABLE] Error creating clients table in '{database_name}': {type(e).__name__}: {str(e)}")
        log_with_request_id('error', f"[TABLE] Traceback: {traceback.format_exc()}")
        return False


def parse_excel_from_base64(base64_data: str, filename: str):
    """Parse Excel file from base64 encoded data"""
    log_with_request_id('info', f"[EXCEL] Parsing Excel file: {filename}")
    try:
        # Check if file is actually an Excel file
        if not filename.lower().endswith(('.xlsx', '.xls')):
            log_with_request_id('warning', f"[EXCEL] Skipping non-Excel file: {filename}")
            return None
        
        # Log base64 data size
        base64_size = len(base64_data) if base64_data else 0
        log_with_request_id('debug', f"[EXCEL] Base64 data size: {base64_size} characters")
        
        if not base64_data:
            log_with_request_id('error', f"[EXCEL] Empty base64 data for file: {filename}")
            return None
        
        with log_timing(f"parse Excel file '{filename}'"):
            # Decode base64 data
            log_with_request_id('debug', f"[EXCEL] Decoding base64 data for: {filename}")
            excel_bytes = base64.b64decode(base64_data)
            decoded_size = len(excel_bytes)
            log_with_request_id('debug', f"[EXCEL] Decoded size: {decoded_size} bytes")
            
            # Read Excel file into pandas DataFrame
            log_with_request_id('debug', f"[EXCEL] Reading Excel into DataFrame: {filename}")
            df = pd.read_excel(io.BytesIO(excel_bytes))
            
            log_with_request_id('info', f"[EXCEL] Successfully parsed '{filename}': {len(df)} rows, {len(df.columns)} columns")
            log_with_request_id('debug', f"[EXCEL] Columns: {list(df.columns)}")
        return df
    except base64.binascii.Error as e:
        log_with_request_id('error', f"[EXCEL] Base64 decode error for '{filename}': {str(e)}")
        return None
    except Exception as e:
        log_with_request_id('error', f"[EXCEL] Error parsing Excel file '{filename}': {type(e).__name__}: {str(e)}")
        log_with_request_id('error', f"[EXCEL] Traceback: {traceback.format_exc()}")
        return None


def map_appointments_columns_to_db(df: pd.DataFrame):
    """Map Appointment Excel column names to database column names"""
    # Column mapping from Excel to database
    column_mapping = {
        'Appointment Date': 'appointment_date',
        'Client': 'client',
        'Appointment Type': 'appointment_type',
        'Profession': 'profession',
        'ClientDuration': 'client_duration',
        'Practitioner': 'practitioner',
        'Business': 'business',
        'Appointment Status': 'appointment_status',
        'Column #': 'column_number',
        'Billed Status': 'billed_status',
        'Clinical Note': 'clinical_note',
        'Client ID': 'client_id',
        'Appointment ID': 'appointment_id',
        'Address 1': 'address_1',
        'Address 2': 'address_2',
        'Address 3': 'address_3',
        'Address 4': 'address_4',
        'Suburb': 'suburb',
        'State': 'state',
        'Postcode': 'postcode',
        'Country': 'country'
    }
    
    # Rename columns
    df_mapped = df.rename(columns=column_mapping)
    
    # Convert NaN to None for proper NULL handling in PostgreSQL
    df_mapped = df_mapped.where(pd.notna(df_mapped), None)
    
    return df_mapped


def map_clients_columns_to_db(df: pd.DataFrame):
    """Map Client List Excel column names to database column names"""
    # Column mapping from Excel to database
    column_mapping = {
        'Title': 'title',
        'First Name': 'first_name',
        'Preferred Name': 'preferred_name',
        'Middle': 'middle',
        'Surname': 'surname',
        'Date of Birth': 'date_of_birth',
        'Address Line 1': 'address_line_1',
        'Address Line 2': 'address_line_2',
        'Address Line 3': 'address_line_3',
        'Address Line 4': 'address_line_4',
        'Country': 'country',
        'State': 'state',
        'Suburb': 'suburb',
        'Postcode': 'postcode',
        'Preferred Phone': 'preferred_phone',
        'Work Phone': 'work_phone',
        'Home Phone': 'home_phone',
        'Mobile': 'mobile',
        'Fax': 'fax',
        'Email': 'email',
        'File No': 'file_no',
        'Gender': 'gender',
        'Pronouns': 'pronouns',
        'Sex': 'sex',
        'Archived': 'archived',
        'Notes': 'notes',
        'Warnings': 'warnings',
        'Fee Category': 'fee_category',
        'Practitioner': 'practitioner',
        'Medicare No': 'medicare_no',
        'Medicare IRN': 'medicare_irn',
        'Medicare Expiry': 'medicare_expiry',
        'DVA No': 'dva_no',
        'DVA Type': 'dva_type',
        'Concession No': 'concession_no',
        'Concession Expiry': 'concession_expiry',
        'Health Fund': 'health_fund',
        'Health Fund Member No': 'health_fund_member_no',
        'NDIS No': 'ndis_no',
        'Created Date': 'created_date',
        'Consent Date': 'consent_date',
        'Privacy Policy Date': 'privacy_policy_date',
        'Client ID': 'client_id',
        'GP Name': 'gp_name'
    }
    
    # Rename columns
    df_mapped = df.rename(columns=column_mapping)
    
    # Convert NaN to None for non-date columns first
    df_mapped = df_mapped.where(pd.notna(df_mapped), None)
    
    # Handle date columns LAST - after all NaN conversions
    date_columns = ['created_date', 'consent_date', 'privacy_policy_date']
    for col in date_columns:
        if col in df_mapped.columns:
            # Convert values to datetime, handling None values
            converted_dates = []
            for val in df_mapped[col]:
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    # Already None or NaN - keep as None
                    converted_dates.append(None)
                else:
                    # Try to convert to datetime
                    try:
                        dt = pd.to_datetime(val, errors='coerce')
                        if pd.notna(dt):
                            converted_dates.append(dt.to_pydatetime())
                        else:
                            converted_dates.append(None)
                    except:
                        converted_dates.append(None)
            
            df_mapped[col] = converted_dates
    
    return df_mapped


def clean_db_value(value):
    """Normalize values before inserting into the database"""
    if value is None:
        return None
    # Handle pandas NaT/NaN generically
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    # Handle pandas NaT / NaN
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, str) and value.strip().lower() == 'nat':
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def insert_appointments_data(database_name: str, df: pd.DataFrame):
    """Insert appointment data into the database with upsert to avoid duplicates"""
    log_with_request_id('info', f"[INSERT] Starting insert_appointments_data for database '{database_name}'")
    log_with_request_id('info', f"[INSERT] Input DataFrame: {len(df)} rows")
    try:
        with log_timing(f"insert appointments into '{database_name}'"):
            conn = get_db_connection(database_name)
            cursor = conn.cursor()
            
            # Map columns
            log_with_request_id('debug', f"[INSERT] Mapping columns for appointments")
            df_mapped = map_appointments_columns_to_db(df)
            
            # Remove duplicates based on appointment_id (keep last occurrence)
            # This prevents "ON CONFLICT DO UPDATE command cannot affect row a second time" error
            if 'appointment_id' in df_mapped.columns:
                original_count = len(df_mapped)
                df_mapped = df_mapped.drop_duplicates(subset=['appointment_id'], keep='last')
                duplicates_removed = original_count - len(df_mapped)
                log_with_request_id('info', f"[INSERT] After deduplication: {len(df_mapped)} unique appointments (removed {duplicates_removed} duplicates)")
            
            # Prepare columns list (excluding id, created_at, updated_at which are auto-generated)
            db_columns = [
                'appointment_date', 'client', 'appointment_type', 'profession',
                'client_duration', 'practitioner', 'business', 'appointment_status',
                'column_number', 'billed_status', 'clinical_note', 'client_id',
                'appointment_id', 'address_1', 'address_2', 'address_3', 'address_4',
                'suburb', 'state', 'postcode', 'country'
            ]
            
            # Prepare data tuples
            log_with_request_id('debug', f"[INSERT] Preparing {len(df_mapped)} data tuples")
            data_tuples = []
            for idx, (_, row) in enumerate(df_mapped.iterrows()):
                try:
                    row_data = tuple(clean_db_value(row[col]) if col in row else None for col in db_columns)
                    data_tuples.append(row_data)
                except Exception as row_error:
                    log_with_request_id('error', f"[INSERT] Error preparing row {idx}: {str(row_error)}")
                    continue
            
            log_with_request_id('info', f"[INSERT] Prepared {len(data_tuples)} tuples for insert")
            
            # Prepare INSERT ... ON CONFLICT (UPSERT) query
            # This will insert new records or update existing ones based on appointment_id
            columns_str = ', '.join(db_columns)
            
            # Build update columns for ON CONFLICT (excluding appointment_id which is the conflict target)
            update_columns = [col for col in db_columns if col != 'appointment_id']
            update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            
            upsert_query = f"""
            INSERT INTO appointments ({columns_str})
            VALUES %s
            ON CONFLICT (appointment_id) 
            DO UPDATE SET 
                {update_str},
                updated_at = CURRENT_TIMESTAMP
            """
            
            # Execute batch insert with execute_values for better performance
            log_with_request_id('debug', f"[INSERT] Executing upsert query")
            execute_values(cursor, upsert_query, data_tuples)
            
            conn.commit()
            
            rows_affected = cursor.rowcount
            log_with_request_id('info', f"[INSERT] Successfully inserted/updated {rows_affected} appointments in database '{database_name}'")
            
            cursor.close()
            conn.close()
            
            return {
                'success': True,
                'rows_processed': len(data_tuples),
                'rows_affected': rows_affected
            }
        
    except Exception as e:
        log_with_request_id('error', f"[INSERT] Error inserting appointments data into '{database_name}': {type(e).__name__}: {str(e)}")
        log_with_request_id('error', f"[INSERT] Traceback: {traceback.format_exc()}")
        return {
            'success': False,
            'error': str(e)
        }


def insert_clients_data(database_name: str, df: pd.DataFrame):
    """Insert client data into the database with upsert to avoid duplicates"""
    log_with_request_id('info', f"[INSERT] Starting insert_clients_data for database '{database_name}'")
    log_with_request_id('info', f"[INSERT] Input DataFrame: {len(df)} rows")
    try:
        with log_timing(f"insert clients into '{database_name}'"):
            conn = get_db_connection(database_name)
            cursor = conn.cursor()
            
            # Map columns
            log_with_request_id('debug', f"[INSERT] Mapping columns for clients")
            df_mapped = map_clients_columns_to_db(df)
            
            # Remove duplicates based on client_id (keep last occurrence)
            # This prevents "ON CONFLICT DO UPDATE command cannot affect row a second time" error
            if 'client_id' in df_mapped.columns:
                original_count = len(df_mapped)
                df_mapped = df_mapped.drop_duplicates(subset=['client_id'], keep='last')
                duplicates_removed = original_count - len(df_mapped)
                log_with_request_id('info', f"[INSERT] After deduplication: {len(df_mapped)} unique clients (removed {duplicates_removed} duplicates)")
            
            # Prepare columns list (excluding id, db_created_at, db_updated_at which are auto-generated)
            db_columns = [
                'title', 'first_name', 'preferred_name', 'middle', 'surname', 'date_of_birth',
                'address_line_1', 'address_line_2', 'address_line_3', 'address_line_4',
                'country', 'state', 'suburb', 'postcode', 'preferred_phone', 'work_phone',
                'home_phone', 'mobile', 'fax', 'email', 'file_no', 'gender', 'pronouns',
                'sex', 'archived', 'notes', 'warnings', 'fee_category', 'practitioner',
                'medicare_no', 'medicare_irn', 'medicare_expiry', 'dva_no', 'dva_type',
                'concession_no', 'concession_expiry', 'health_fund', 'health_fund_member_no',
                'ndis_no', 'created_date', 'consent_date', 'privacy_policy_date', 'client_id',
                'gp_name'
            ]
            
            # Prepare data tuples
            log_with_request_id('debug', f"[INSERT] Preparing {len(df_mapped)} data tuples")
            data_tuples = []
            for idx, (_, row) in enumerate(df_mapped.iterrows()):
                try:
                    row_data = tuple(clean_db_value(row[col]) if col in row else None for col in db_columns)
                    data_tuples.append(row_data)
                except Exception as row_error:
                    log_with_request_id('error', f"[INSERT] Error preparing row {idx}: {str(row_error)}")
                    continue
            
            log_with_request_id('info', f"[INSERT] Prepared {len(data_tuples)} tuples for insert")
            
            # Prepare INSERT ... ON CONFLICT (UPSERT) query
            # This will insert new records or update existing ones based on client_id
            columns_str = ', '.join(db_columns)
            
            # Build update columns for ON CONFLICT (excluding client_id which is the conflict target)
            update_columns = [col for col in db_columns if col != 'client_id']
            update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            
            upsert_query = f"""
            INSERT INTO clients ({columns_str})
            VALUES %s
            ON CONFLICT (client_id) 
            DO UPDATE SET 
                {update_str},
                db_updated_at = CURRENT_TIMESTAMP
            """
            
            # Execute batch insert with execute_values for better performance
            log_with_request_id('debug', f"[INSERT] Executing upsert query")
            execute_values(cursor, upsert_query, data_tuples)
            
            conn.commit()
            
            rows_affected = cursor.rowcount
            log_with_request_id('info', f"[INSERT] Successfully inserted/updated {rows_affected} clients in database '{database_name}'")
            
            cursor.close()
            conn.close()
            
            return {
                'success': True,
                'rows_processed': len(data_tuples),
                'rows_affected': rows_affected
            }
        
    except Exception as e:
        log_with_request_id('error', f"[INSERT] Error inserting clients data into '{database_name}': {type(e).__name__}: {str(e)}")
        log_with_request_id('error', f"[INSERT] Traceback: {traceback.format_exc()}")
        return {
            'success': False,
            'error': str(e)
        }


def process_attachment_and_store(email_data: dict):
    """
    Process email attachments and store data in appropriate database
    """
    log_with_request_id('info', f"[PROCESS] Starting process_attachment_and_store")
    try:
        with log_timing("process_attachment_and_store"):
            online_weekly_email = is_online_appointments_email(email_data)
            if online_weekly_email:
                database_name = ONLINE_APPOINTMENTS_DB
                log_with_request_id('info', f"[PROCESS] Detected weekly online appointment report email. Target database: {database_name}")
            else:
                # Extract clinic name from 'to' field for standard clinic-specific emails
                to_email = email_data.get('to', '')
                log_with_request_id('debug', f"[PROCESS] Extracting clinic name from: {to_email}")
                database_name = extract_clinic_name(to_email)
                if not database_name:
                    log_with_request_id('warning', f"[PROCESS] Could not extract clinic name from email: {to_email}")
                    return {"status": "error", "message": "Invalid email format"}
            
            log_with_request_id('info', f"[PROCESS] Processing email for database: {database_name}")
            
            # Create database for clinic/online storage
            if not create_database(database_name):
                log_with_request_id('error', f"[PROCESS] Failed to create database: {database_name}")
                return {"status": "error", "message": "Failed to create database"}
            
            # Process attachments
            attachments = email_data.get('attachments', [])
            log_with_request_id('info', f"[PROCESS] Found {len(attachments)} attachments")
            
            if not attachments:
                log_with_request_id('info', f"[PROCESS] No attachments found in email")
                return {"status": "warning", "message": "No attachments to process"}
            
            results = []
            for idx, attachment in enumerate(attachments):
                filename = attachment.get('name', '')
                table_name = extract_table_name(filename)
                
                log_with_request_id('info', f"[PROCESS] Processing attachment {idx + 1}/{len(attachments)}: {filename} -> table: {table_name}")
                
                # Process Appointments
                if table_name == 'appointments':
                    log_with_request_id('info', f"[PROCESS] Processing as appointments file")
                    
                    # Create the appointments table
                    if not create_appointments_table(database_name):
                        log_with_request_id('error', f"[PROCESS] Failed to create appointments table for: {filename}")
                        results.append({
                            "filename": filename,
                            "table": table_name,
                            "database": database_name,
                            "status": "error",
                            "message": "Failed to create table"
                        })
                        continue
                    
                    # Check if attachment has data
                    attachment_data = attachment.get('data', '')
                    data_size = len(attachment_data) if attachment_data else 0
                    log_with_request_id('debug', f"[PROCESS] Attachment data size: {data_size} characters")
                    
                    if not attachment_data:
                        log_with_request_id('warning', f"[PROCESS] No data found in attachment: {filename}")
                        results.append({
                            "filename": filename,
                            "table": table_name,
                            "database": database_name,
                            "status": "warning",
                            "message": "No attachment data found"
                        })
                        continue
                    
                    # Parse Excel file
                    df = parse_excel_from_base64(attachment_data, filename)
                    if df is None or df.empty:
                        log_with_request_id('error', f"[PROCESS] Failed to parse Excel file or file is empty: {filename}")
                        results.append({
                            "filename": filename,
                            "table": table_name,
                            "database": database_name,
                            "status": "error",
                            "message": "Failed to parse Excel file or file is empty"
                        })
                        continue
                    
                    # Insert data into database
                    insert_result = insert_appointments_data(database_name, df)
                    
                    if insert_result['success']:
                        results.append({
                            "filename": filename,
                            "table": table_name,
                            "database": database_name,
                            "status": "success",
                            "rows_processed": insert_result['rows_processed'],
                            "rows_affected": insert_result['rows_affected']
                        })
                        log_with_request_id('info', f"[PROCESS] Successfully imported {insert_result['rows_affected']} rows from {filename}")
                    else:
                        log_with_request_id('error', f"[PROCESS] Insert failed for {filename}: {insert_result.get('error', 'Unknown error')}")
                        results.append({
                            "filename": filename,
                            "table": table_name,
                            "database": database_name,
                            "status": "error",
                            "message": insert_result.get('error', 'Unknown error')
                        })
                
                # Process Clients
                elif table_name == 'clients':
                    log_with_request_id('info', f"[PROCESS] Processing as clients file")
                    
                    # Create the clients table
                    if not create_clients_table(database_name):
                        log_with_request_id('error', f"[PROCESS] Failed to create clients table for: {filename}")
                        results.append({
                            "filename": filename,
                            "table": table_name,
                            "database": database_name,
                            "status": "error",
                            "message": "Failed to create table"
                        })
                        continue
                    
                    # Check if attachment has data
                    attachment_data = attachment.get('data', '')
                    data_size = len(attachment_data) if attachment_data else 0
                    log_with_request_id('debug', f"[PROCESS] Attachment data size: {data_size} characters")
                    
                    if not attachment_data:
                        log_with_request_id('warning', f"[PROCESS] No data found in attachment: {filename}")
                        results.append({
                            "filename": filename,
                            "table": table_name,
                            "database": database_name,
                            "status": "warning",
                            "message": "No attachment data found"
                        })
                        continue
                    
                    # Parse Excel file
                    df = parse_excel_from_base64(attachment_data, filename)
                    if df is None or df.empty:
                        log_with_request_id('error', f"[PROCESS] Failed to parse Excel file or file is empty: {filename}")
                        results.append({
                            "filename": filename,
                            "table": table_name,
                            "database": database_name,
                            "status": "error",
                            "message": "Failed to parse Excel file or file is empty"
                        })
                        continue
                    
                    # Insert data into database
                    insert_result = insert_clients_data(database_name, df)
                    
                    if insert_result['success']:
                        results.append({
                            "filename": filename,
                            "table": table_name,
                            "database": database_name,
                            "status": "success",
                            "rows_processed": insert_result['rows_processed'],
                            "rows_affected": insert_result['rows_affected']
                        })
                        log_with_request_id('info', f"[PROCESS] Successfully imported {insert_result['rows_affected']} rows from {filename}")
                    else:
                        log_with_request_id('error', f"[PROCESS] Insert failed for {filename}: {insert_result.get('error', 'Unknown error')}")
                        results.append({
                            "filename": filename,
                            "table": table_name,
                            "database": database_name,
                            "status": "error",
                            "message": insert_result.get('error', 'Unknown error')
                        })
                
                # Skip other files
                else:
                    log_with_request_id('info', f"[PROCESS] Skipping unsupported file: {filename}")
                    results.append({
                        "filename": filename,
                        "table": table_name,
                        "status": "skipped",
                        "reason": "File type not supported (only Appointment and Client List reports)"
                    })
            
            log_with_request_id('info', f"[PROCESS] Completed processing. Results: {len(results)} attachments processed")
            
            return {
                "status": "success",
                "clinic": database_name,
                "mode": "online_weekly" if online_weekly_email else "clinic_specific",
                "results": results
            }
        
    except Exception as e:
        log_with_request_id('error', f"[PROCESS] Error processing attachment: {type(e).__name__}: {str(e)}")
        log_with_request_id('error', f"[PROCESS] Traceback: {traceback.format_exc()}")
        return {"status": "error", "message": str(e)}


app = FastAPI(title="Email Forwarding Logger", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Global exception handler to catch any unhandled errors
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch all unhandled exceptions and log them"""
    logger.error(f"[GLOBAL_ERROR] Unhandled exception: {type(exc).__name__}: {str(exc)}", extra={'request_id': 'error'})
    logger.error(f"[GLOBAL_ERROR] Path: {request.url.path}", extra={'request_id': 'error'})
    logger.error(f"[GLOBAL_ERROR] Traceback: {traceback.format_exc()}", extra={'request_id': 'error'})
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "error": str(exc),
            "error_type": type(exc).__name__,
            "path": str(request.url.path),
            "timestamp": datetime.now().isoformat()
        }
    )


@app.on_event("startup")
async def startup_event():
    """Log startup information"""
    log_startup_config()
    
    # Test database connectivity at startup
    logger.info("Testing database connectivity...", extra={'request_id': 'startup'})
    try:
        conn = get_db_connection()
        conn.close()
        logger.info("Database connectivity test: SUCCESS", extra={'request_id': 'startup'})
    except Exception as e:
        logger.error(f"Database connectivity test: FAILED - {type(e).__name__}: {str(e)}", extra={'request_id': 'startup'})


@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    """Middleware to log all requests with request ID tracking"""
    # Generate unique request ID
    request_id = str(uuid.uuid4())[:8]
    set_request_id(request_id)
    
    # Log request details
    start_time = time.time()
    log_with_request_id('info', f"[REQUEST] {request.method} {request.url.path}")
    log_with_request_id('debug', f"[REQUEST] Client: {request.client.host if request.client else 'unknown'}")
    log_with_request_id('debug', f"[REQUEST] Headers: {dict(request.headers)}")
    
    # Get content length if available
    content_length = request.headers.get('content-length', 'unknown')
    log_with_request_id('debug', f"[REQUEST] Content-Length: {content_length}")
    
    try:
        response = await call_next(request)
        
        # Log response details
        elapsed_ms = (time.time() - start_time) * 1000
        log_with_request_id('info', f"[RESPONSE] status={response.status_code} | elapsed_ms={elapsed_ms:.2f}")
        
        # Add request ID to response headers for tracing
        response.headers["X-Request-ID"] = request_id
        
        return response
        
    except Exception as e:
        elapsed_ms = (time.time() - start_time) * 1000
        log_with_request_id('error', f"[RESPONSE] Exception during request: {type(e).__name__}: {str(e)} | elapsed_ms={elapsed_ms:.2f}")
        log_with_request_id('error', f"[RESPONSE] Traceback: {traceback.format_exc()}")
        raise
    finally:
        set_request_id(None)


@app.get("/")
async def root():
    log_with_request_id('info', "[ENDPOINT] GET / called")
    return {"message": "Email Logger API is running", "status": "active"}


@app.post("/test")
async def test_post(request: Request):
    """Simple test endpoint to verify POST works"""
    log_with_request_id('info', "[TEST] POST /test called")
    try:
        body = await request.body()
        body_size = len(body) if body else 0
        log_with_request_id('info', f"[TEST] Body size: {body_size} bytes")
        return {
            "status": "ok",
            "body_size": body_size,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        log_with_request_id('error', f"[TEST] Error: {type(e).__name__}: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "type": type(e).__name__}
        )


@app.post("/webhook/email")
async def receive_email(request: Request):
    """
    Endpoint to receive email data from Google Apps Script
    Logs email metadata and processes attachments
    """
    log_with_request_id('info', "[WEBHOOK] POST /webhook/email - Starting email processing")
    
    try:
        # Get the raw body and log its size
        # NOTE: Body can only be read once, so we parse JSON from it directly
        body = await request.body()
        body_size = len(body) if body else 0
        log_with_request_id('info', f"[WEBHOOK] Request body size: {body_size} bytes")
        
        if body_size == 0:
            log_with_request_id('warning', "[WEBHOOK] Empty request body received")
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": "Empty request body",
                    "timestamp": datetime.now().isoformat()
                }
            )
        
        # Try to parse as JSON from the body bytes (not request.json() since body was already consumed)
        try:
            log_with_request_id('debug', "[WEBHOOK] Parsing JSON body")
            email_data = json.loads(body.decode('utf-8'))
            
            log_with_request_id('info', "=" * 80)
            log_with_request_id('info', "NEW EMAIL RECEIVED")
            log_with_request_id('info', "=" * 80)
            
            # Log specific fields if they exist
            if isinstance(email_data, dict):
                log_with_request_id('info', f"From: {email_data.get('from', 'N/A')}")
                log_with_request_id('info', f"To: {email_data.get('to', 'N/A')}")
                log_with_request_id('info', f"Subject: {email_data.get('subject', 'N/A')}")
                log_with_request_id('info', f"Date: {email_data.get('date', 'N/A')}")
                
                if 'attachments' in email_data:
                    attachments = email_data.get('attachments', [])
                    log_with_request_id('info', f"Attachments count: {len(attachments)}")
                    for idx, att in enumerate(attachments):
                        att_name = att.get('name', 'N/A')
                        att_size = att.get('size', 'N/A')
                        att_data_len = len(att.get('data', '')) if att.get('data') else 0
                        log_with_request_id('info', f"  Attachment {idx + 1}: {att_name} (reported size: {att_size} bytes, data length: {att_data_len} chars)")
                
                # Log additional fields that might be useful for debugging
                log_with_request_id('debug', f"Email data keys: {list(email_data.keys())}")
            else:
                log_with_request_id('warning', f"[WEBHOOK] Unexpected email_data type: {type(email_data)}")
            
            log_with_request_id('info', "=" * 80)
            
            # Also save to a JSON file for inspection
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                json_file = Path(f"emails/email_{timestamp}.json")
                json_file.parent.mkdir(exist_ok=True)
                
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(email_data, f, indent=2, default=str)
                
                log_with_request_id('info', f"[WEBHOOK] Email data saved to: {json_file}")
            except Exception as file_error:
                log_with_request_id('warning', f"[WEBHOOK] Could not save email to file: {type(file_error).__name__}: {str(file_error)}")
                json_file = None
            
            # Process attachments and create database/tables
            log_with_request_id('info', "[WEBHOOK] Starting attachment processing")
            db_result = process_attachment_and_store(email_data)
            log_with_request_id('info', f"[WEBHOOK] Attachment processing complete. Result status: {db_result.get('status', 'unknown')}")
            
            response_content = {
                "status": "success",
                "message": "Email logged successfully",
                "timestamp": datetime.now().isoformat(),
                "saved_to": str(json_file) if json_file else None,
                "database_result": db_result
            }
            
            log_with_request_id('info', f"[WEBHOOK] Returning success response")
            return JSONResponse(status_code=200, content=response_content)
            
        except json.JSONDecodeError as json_error:
            # If not JSON, log a warning without the body content
            log_with_request_id('warning', f"[WEBHOOK] Received non-JSON data: {type(json_error).__name__}: {str(json_error)}")
            log_with_request_id('debug', f"[WEBHOOK] First 200 chars of body: {body[:200] if body else 'empty'}")
            
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": f"Invalid JSON: {str(json_error)}",
                    "timestamp": datetime.now().isoformat()
                }
            )
            
    except Exception as e:
        log_with_request_id('error', f"[WEBHOOK] Unhandled exception: {type(e).__name__}: {str(e)}")
        log_with_request_id('error', f"[WEBHOOK] Traceback: {traceback.format_exc()}")
        
        # Return 500 with detailed error info
        raise HTTPException(
            status_code=500, 
            detail={
                "error": str(e),
                "error_type": type(e).__name__,
                "message": "Error processing email"
            }
        )


@app.get("/health")
async def health_check():
    """Basic health check endpoint"""
    log_with_request_id('debug', "[HEALTH] Health check called")
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.get("/health/db")
async def db_health_check():
    """Database health check endpoint"""
    log_with_request_id('info', "[HEALTH] Database health check called")
    
    try:
        start_time = time.time()
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        elapsed_ms = (time.time() - start_time) * 1000
        
        log_with_request_id('info', f"[HEALTH] Database connection healthy | elapsed_ms={elapsed_ms:.2f}")
        
        return {
            "status": "healthy",
            "database": "connected",
            "response_time_ms": round(elapsed_ms, 2),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        log_with_request_id('error', f"[HEALTH] Database health check failed: {type(e).__name__}: {str(e)}")
        
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "database": "disconnected",
                "error": str(e),
                "error_type": type(e).__name__,
                "timestamp": datetime.now().isoformat()
            }
        )


@app.get("/debug/config")
async def debug_config():
    """Debug endpoint to check configuration (no sensitive data)"""
    log_with_request_id('info', "[DEBUG] Config check called")
    
    return {
        "postgres_host": DB_CONFIG['host'],
        "postgres_port": DB_CONFIG['port'],
        "postgres_user": DB_CONFIG['user'],
        "postgres_password_set": bool(DB_CONFIG['password']),
        "postgres_admin_db": DB_CONFIG['admin_db'],
        "online_appointments_db": ONLINE_APPOINTMENTS_DB,
        "online_appointments_keyword": ONLINE_APPOINTMENTS_KEYWORD,
        "port_env": os.getenv('PORT', 'not set'),
        "python_version": sys.version,
        "working_directory": os.getcwd(),
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    
    # Get port from environment (Railway sets this)
    port = int(os.getenv('PORT', 8000))
    
    logger.info(f"Starting server on port {port}", extra={'request_id': 'startup'})
    uvicorn.run(app, host="0.0.0.0", port=port)
