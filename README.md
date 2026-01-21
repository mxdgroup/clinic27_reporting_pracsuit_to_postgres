# Email Logger with PostgreSQL Integration

This project forwards emails from Gmail to a FastAPI endpoint, logs the complete email structure, and automatically creates clinic-specific PostgreSQL databases with appointment tables based on email attachments.

## Features

- **Automatic Database Creation**: Creates a separate PostgreSQL database for each clinic based on email address
- **Dynamic Table Creation**: Automatically creates appointment tables from Excel file attachments
- **Clinic Extraction**: Extracts clinic name from email address (e.g., `developers.mxd+supertest@gmail.com` → `supertest` database)
- **Comprehensive Logging**: Logs all email data to both files and console
- **Railway PostgreSQL Support**: Configured for remote Railway database hosting
- **Weekly Online Appointments Support**: Detects emails containing `Saved filters: Appointments - Online (Last Week)` and writes them to the dedicated `appointments_online` database for long-term tracking

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Database Connection

Update the `.env` file with your PostgreSQL credentials:

```env
# Railway PostgreSQL Configuration
POSTGRES_HOST=switchyard.proxy.rlwy.net
POSTGRES_PORT=YOUR_RAILWAY_TCP_PROXY_PORT  # Get this from Railway dashboard
POSTGRES_USER=postgres
POSTGRES_PASSWORD=qktaqSHIazfnIgTwdVmWoJWRHvFxtPVS
POSTGRES_ADMIN_DB=railway
```

**Finding your Railway TCP Proxy Port:**
1. Go to your Railway project dashboard
2. Click on your PostgreSQL service
3. Go to the "Connect" tab
4. Look for `RAILWAY_TCP_PROXY_PORT` or the public connection string
5. The port is typically a 4-5 digit number (e.g., 12345)

### 3. Test Database Connection

Run the test script to verify your database setup:

```bash
python test_db_setup.py
```

This will test:
- PostgreSQL connection
- Clinic name extraction
- Table name extraction
- Database creation
- Table creation

### 4. Run the FastAPI Server

```bash
python main.py
```

Or with uvicorn:
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at: `http://localhost:8000`

### 5. Configure Google Apps Script

1. Go to https://script.google.com/
2. Create a new project
3. Copy the code from `google_script.js`
4. Update the `FASTAPI_ENDPOINT` variable with your public URL (use ngrok for local testing)
5. Run the `manualTrigger()` function to authorize the script
6. Set up a time-driven trigger:
   - Click on the clock icon (Triggers)
   - Add Trigger
   - Choose function: `checkEmails`
   - Select event source: Time-driven
   - Select type of time based trigger: Minutes timer
   - Select minute interval: Every 5 minutes (or your preference)

### 6. For Local Testing with ngrok

```bash
ngrok http 8000
```

Copy the ngrok URL and update it in the Google Apps Script.

## How It Works

### Clinic Database Creation

1. **Email Reception**: When an email is received at the webhook
2. **Clinic Extraction**: The system extracts the clinic name from the "to" field
   - Example: `developers.mxd+supertest@gmail.com` → clinic name: `supertest`
   - Sanitized to database-safe format (alphanumeric and underscores only)
3. **Database Creation**: A new PostgreSQL database is created with the clinic name
4. **Attachment Processing**: Excel attachments are processed to determine table structure
5. **Table Creation**: Tables are created based on the first word of the filename
   - Example: `Appointment Report 281025_1151PM.xlsx` → `appointments` table

### Weekly Online Appointments Database

- Emails that include the text `Saved filters: Appointments - Online (Last Week)` in their body are treated as the weekly online-only export from PracSuite
- Those emails are automatically routed to the shared `appointments_online` database (configurable via the `ONLINE_APPOINTMENTS_DB` environment variable)
- The standard `appointments` table schema is created inside that database and populated just like the clinic-specific exports
- You can override the matching text with the `ONLINE_APPOINTMENTS_KEYWORD` environment variable if PracSuite changes the email wording

### Appointments Table Schema

The appointments table is automatically created with the following structure:

```sql
CREATE TABLE appointments (
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
);
```

**Indexes are created on:**
- `appointment_date` - for date-based queries
- `client_id` - for client lookups
- `appointment_id` - for unique appointment identification

## API Endpoints

- `GET /` - Root endpoint, returns API status
- `POST /webhook/email` - Receives email data from Google Scripts, processes attachments, and creates databases/tables
- `GET /health` - Health check endpoint

## Logging

Emails are logged in two ways:
1. **Console & Log File**: All emails are logged to `email_logs.log`
2. **JSON Files**: Each email is saved as a JSON file in the `emails/` directory

## Email Structure

The Google Script sends the following data structure:

```json
{
  "messageId": "string",
  "threadId": "string",
  "from": "sender@example.com",
  "to": "developers.mxd+supertest@gmail.com",
  "cc": "cc@example.com",
  "bcc": "bcc@example.com",
  "replyTo": "reply@example.com",
  "subject": "Appointment Report",
  "date": "2024-01-01T12:00:00.000Z",
  "body": "Plain text body",
  "bodyHtml": "HTML body",
  "isUnread": true,
  "isStarred": false,
  "isDraft": false,
  "isInInbox": true,
  "isInTrash": false,
  "labels": ["Label1", "Label2"],
  "attachments": [
    {
      "name": "Appointment Report 281025_1151PM.xlsx",
      "size": 12345,
      "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }
  ]
}
```

## API Response Example

When an email with attachments is processed, the API returns:

```json
{
  "status": "success",
  "message": "Email logged successfully",
  "timestamp": "2025-10-28T23:55:21.000Z",
  "saved_to": "emails/email_20251028_235521.json",
  "database_result": {
    "status": "success",
    "clinic": "supertest",
    "results": [
      {
        "filename": "Appointment Report 281025_1151PM.xlsx",
        "table": "appointments",
        "database": "supertest",
        "status": "success"
      }
    ]
  }
}
```

## Database Naming Convention

- **Database Name**: Extracted from email address between `+` and `@`
  - `developers.mxd+supertest@gmail.com` → database: `supertest`
  - `clinic+demo-clinic@example.com` → database: `demo_clinic`
  
- **Table Name**: First word of the Excel filename (pluralized)
  - `Appointment Report.xlsx` → table: `appointments`
  - `Invoice Summary.xlsx` → table: `invoices`
  - `Client List.xlsx` → table: `clients`

## Logging

Emails are logged in two ways:
1. **Console & Log File**: All emails are logged to `email_logs.log`
2. **JSON Files**: Each email is saved as a JSON file in the `emails/` directory

## Data Import Features ✅

The system now automatically imports appointment data from Excel attachments:

- ✅ **Parses Excel files** from email attachments
- ✅ **Maps columns** automatically to database schema
- ✅ **Inserts data** with batch processing for efficiency
- ✅ **Prevents duplicates** using `appointment_id` as unique identifier
- ✅ **Updates existing records** (UPSERT logic)
- ✅ **Handles NULL values** properly
- ✅ **Tracks changes** with `created_at` and `updated_at` timestamps

### How Data Import Works

1. Email received with Excel attachment
2. Clinic database created (e.g., `supertest`)
3. Appointments table created/verified
4. Excel file parsed (base64 → pandas DataFrame)
5. Data inserted/updated in bulk
6. Response includes row counts and status

**Example Response:**
```json
{
  "status": "success",
  "database_result": {
    "clinic": "supertest",
    "results": [{
      "filename": "Appointment Report.xlsx",
      "rows_processed": 68,
      "rows_affected": 68
    }]
  }
}
```

See **`DATA_IMPORT_GUIDE.md`** for complete documentation.

## Current Limitations

- Only `appointments` tables are currently processed
- Future expansion will support additional report types (invoices, clients, etc.)
- Excel files must have standard column names (see DATA_IMPORT_GUIDE.md) 
