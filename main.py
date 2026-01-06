from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
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

# Load environment variables
load_dotenv()

# Configure logging with UTF-8 encoding
import sys

# File handler with UTF-8 encoding
file_handler = logging.FileHandler('email_logs.log', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Console handler with UTF-8 encoding and error handling for Windows
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
# Set UTF-8 encoding on stdout to handle Unicode characters
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', ''),
    'admin_db': os.getenv('POSTGRES_ADMIN_DB', 'postgres')
}

# Online appointments database (shared across all clinics)
ONLINE_APPOINTMENTS_DB = 'appointments_online'


def is_online_appointments_email(email_data: dict) -> bool:
    """
    Check if the email is a weekly online appointments email from Pracsuite
    These emails are sent every Monday with online-only booked appointments
    Identified by 'Saved filters: Appointments - Online (Last Week)' in the body
    """
    body = email_data.get('body', '')
    body_html = email_data.get('bodyHtml', '')
    
    # Check both plain text and HTML body
    online_indicator = 'Saved filters: Appointments - Online (Last Week)'
    return online_indicator in body or online_indicator in body_html


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


def get_db_connection(database_name: str = None):
    """Get a connection to the specified database or admin database"""
    db = database_name or DB_CONFIG['admin_db']
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=db
    )


def database_exists(database_name: str) -> bool:
    """Check if database exists"""
    try:
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
        return exists
    except Exception as e:
        logger.error(f"Error checking database existence: {e}")
        return False


def create_database(database_name: str):
    """Create a new database if it doesn't exist"""
    try:
        if database_exists(database_name):
            logger.info(f"Database '{database_name}' already exists")
            return True
        
        conn = get_db_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Create database
        cursor.execute(f'CREATE DATABASE {database_name}')
        logger.info(f"Database '{database_name}' created successfully")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error creating database: {e}")
        return False


def create_appointments_table(database_name: str):
    """Create appointments table in the specified database"""
    try:
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
        
        cursor.execute(create_table_query)
        
        # Create indexes for common queries
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
        logger.info(f"Appointments table created successfully in database '{database_name}'")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error creating appointments table: {e}")
        return False


def create_clients_table(database_name: str):
    """Create clients table in the specified database"""
    try:
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
        
        cursor.execute(create_table_query)
        
        # Create indexes for common queries
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
        logger.info(f"Clients table created successfully in database '{database_name}'")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error creating clients table: {e}")
        return False


def parse_excel_from_base64(base64_data: str, filename: str):
    """Parse Excel file from base64 encoded data"""
    try:
        # Check if file is actually an Excel file
        if not filename.lower().endswith(('.xlsx', '.xls')):
            logger.warning(f"Skipping non-Excel file: {filename}")
            return None
        
        # Decode base64 data
        excel_bytes = base64.b64decode(base64_data)
        
        # Read Excel file into pandas DataFrame
        df = pd.read_excel(io.BytesIO(excel_bytes))
        
        logger.info(f"Successfully parsed Excel file '{filename}': {len(df)} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"Error parsing Excel file '{filename}': {e}")
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
    try:
        conn = get_db_connection(database_name)
        cursor = conn.cursor()
        
        # Map columns
        df_mapped = map_appointments_columns_to_db(df)
        
        # Remove duplicates based on appointment_id (keep last occurrence)
        # This prevents "ON CONFLICT DO UPDATE command cannot affect row a second time" error
        if 'appointment_id' in df_mapped.columns:
            df_mapped = df_mapped.drop_duplicates(subset=['appointment_id'], keep='last')
            logger.info(f"After deduplication: {len(df_mapped)} unique appointments")
        
        # Prepare columns list (excluding id, created_at, updated_at which are auto-generated)
        db_columns = [
            'appointment_date', 'client', 'appointment_type', 'profession',
            'client_duration', 'practitioner', 'business', 'appointment_status',
            'column_number', 'billed_status', 'clinical_note', 'client_id',
            'appointment_id', 'address_1', 'address_2', 'address_3', 'address_4',
            'suburb', 'state', 'postcode', 'country'
        ]
        
        # Prepare data tuples
        data_tuples = []
        for _, row in df_mapped.iterrows():
            row_data = tuple(clean_db_value(row[col]) if col in row else None for col in db_columns)
            data_tuples.append(row_data)
        
        # Prepare INSERT ... ON CONFLICT (UPSERT) query
        # This will insert new records or update existing ones based on appointment_id
        columns_str = ', '.join(db_columns)
        placeholders = ', '.join(['%s'] * len(db_columns))
        
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
        execute_values(cursor, upsert_query, data_tuples)
        
        conn.commit()
        
        rows_affected = cursor.rowcount
        logger.info(f"Successfully inserted/updated {rows_affected} appointments in database '{database_name}'")
        
        cursor.close()
        conn.close()
        
        return {
            'success': True,
            'rows_processed': len(data_tuples),
            'rows_affected': rows_affected
        }
        
    except Exception as e:
        logger.error(f"Error inserting appointments data: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


def insert_clients_data(database_name: str, df: pd.DataFrame):
    """Insert client data into the database with upsert to avoid duplicates"""
    try:
        conn = get_db_connection(database_name)
        cursor = conn.cursor()
        
        # Map columns
        df_mapped = map_clients_columns_to_db(df)
        
        # Remove duplicates based on client_id (keep last occurrence)
        # This prevents "ON CONFLICT DO UPDATE command cannot affect row a second time" error
        if 'client_id' in df_mapped.columns:
            df_mapped = df_mapped.drop_duplicates(subset=['client_id'], keep='last')
            logger.info(f"After deduplication: {len(df_mapped)} unique clients")
        
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
        data_tuples = []
        for _, row in df_mapped.iterrows():
            row_data = tuple(clean_db_value(row[col]) if col in row else None for col in db_columns)
            data_tuples.append(row_data)
        
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
        execute_values(cursor, upsert_query, data_tuples)
        
        conn.commit()
        
        rows_affected = cursor.rowcount
        logger.info(f"Successfully inserted/updated {rows_affected} clients in database '{database_name}'")
        
        cursor.close()
        conn.close()
        
        return {
            'success': True,
            'rows_processed': len(data_tuples),
            'rows_affected': rows_affected
        }
        
    except Exception as e:
        logger.error(f"Error inserting clients data: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


def process_attachment_and_store(email_data: dict):
    """
    Process email attachments and store data in appropriate database
    Routes to either:
    - appointments_online database for weekly online appointments emails
    - clinic-specific database for regular appointment/client reports
    """
    try:
        # Check if this is a weekly online appointments email
        if is_online_appointments_email(email_data):
            database_name = ONLINE_APPOINTMENTS_DB
            logger.info(f"Detected weekly online appointments email - routing to '{database_name}' database")
        else:
            # Extract clinic name from 'to' field for regular reports
            to_email = email_data.get('to', '')
            database_name = extract_clinic_name(to_email)
            
            if not database_name:
                logger.warning(f"Could not extract clinic name from email: {to_email}")
                return {"status": "error", "message": "Invalid email format"}
            
            logger.info(f"Processing email for clinic: {database_name}")
        
        # Create database
        if not create_database(database_name):
            return {"status": "error", "message": "Failed to create database"}
        
        # Process attachments
        attachments = email_data.get('attachments', [])
        if not attachments:
            logger.info("No attachments found in email")
            return {"status": "warning", "message": "No attachments to process"}
        
        results = []
        for attachment in attachments:
            filename = attachment.get('name', '')
            table_name = extract_table_name(filename)
            
            logger.info(f"Processing attachment: {filename} -> table: {table_name}")
            
            # Process Appointments
            if table_name == 'appointments':
                # Create the appointments table
                if not create_appointments_table(database_name):
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
                if not attachment_data:
                    logger.warning(f"No data found in attachment: {filename}")
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
                    logger.info(f"Successfully imported {insert_result['rows_affected']} rows from {filename}")
                else:
                    results.append({
                        "filename": filename,
                        "table": table_name,
                        "database": database_name,
                        "status": "error",
                        "message": insert_result.get('error', 'Unknown error')
                    })
            
            # Process Clients
            elif table_name == 'clients':
                # Create the clients table
                if not create_clients_table(database_name):
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
                if not attachment_data:
                    logger.warning(f"No data found in attachment: {filename}")
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
                    logger.info(f"Successfully imported {insert_result['rows_affected']} rows from {filename}")
                else:
                    results.append({
                        "filename": filename,
                        "table": table_name,
                        "database": database_name,
                        "status": "error",
                        "message": insert_result.get('error', 'Unknown error')
                    })
            
            # Skip other files
            else:
                logger.info(f"Skipping unsupported file: {filename}")
                results.append({
                    "filename": filename,
                    "table": table_name,
                    "status": "skipped",
                    "reason": "File type not supported (only Appointment and Client List reports)"
                })
        
        return {
            "status": "success",
            "database": database_name,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error processing attachment: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


app = FastAPI(title="Email Forwarding Logger", version="1.0.0")


@app.get("/")
async def root():
    return {"message": "Email Logger API is running", "status": "active"}


@app.post("/webhook/email")
async def receive_email(request: Request):
    """
    Endpoint to receive email data from Google Apps Script
    Logs email metadata and processes attachments
    """
    try:
        # Get the raw body
        body = await request.body()
        
        # Try to parse as JSON
        try:
            email_data = await request.json()
            logger.info("=" * 80)
            logger.info("NEW EMAIL RECEIVED")
            logger.info("=" * 80)
            
            # Log specific fields if they exist
            if isinstance(email_data, dict):
                logger.info(f"From: {email_data.get('from', 'N/A')}")
                logger.info(f"To: {email_data.get('to', 'N/A')}")
                logger.info(f"Subject: {email_data.get('subject', 'N/A')}")
                logger.info(f"Date: {email_data.get('date', 'N/A')}")
                
                if 'attachments' in email_data:
                    logger.info(f"Attachments: {len(email_data.get('attachments', []))}")
                    for idx, att in enumerate(email_data.get('attachments', [])):
                        logger.info(f"  Attachment {idx + 1}: {att.get('name', 'N/A')} ({att.get('size', 'N/A')} bytes)")
            
            logger.info("=" * 80)
            
            # Also save to a JSON file for inspection
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_file = Path(f"emails/email_{timestamp}.json")
            json_file.parent.mkdir(exist_ok=True)
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(email_data, f, indent=2, default=str)
            
            logger.info(f"Email data saved to: {json_file}")
            
            # Process attachments and create database/tables
            db_result = process_attachment_and_store(email_data)
            
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": "Email logged successfully",
                    "timestamp": datetime.now().isoformat(),
                    "saved_to": str(json_file),
                    "database_result": db_result
                }
            )
            
        except json.JSONDecodeError:
            # If not JSON, log a warning without the body content
            logger.warning("Received non-JSON data (body not logged for privacy)")
            
            return JSONResponse(
                status_code=200,
                content={
                    "status": "warning",
                    "message": "Received non-JSON data",
                    "timestamp": datetime.now().isoformat()
                }
            )
            
    except Exception as e:
        logger.error(f"Error processing email: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing email: {str(e)}")


@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

