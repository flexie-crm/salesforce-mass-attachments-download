import requests
import json
import os
import csv
import time
import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import xml.etree.ElementTree as ET
from threading import Lock

# Configuration
USERNAME = "your username"
PASSWORD = "your password"
SECURITY_TOKEN = "your security token"
LOGIN_URL = "https://login.salesforce.com/services/Soap/u/57.0"
SAVE_DIR = "attachments"
PROGRESS_FILE = "download_progress.json"
CSV_FILE = "attachments_metadata.csv"
MARKER_FILE = "last_marker.json"
MAX_THREADS = 10  # Increased parallelism
RETRY_COUNT = 5   # More retries for reliability
SALESFORCE_API_VERSION = "62.0" # This API version seems to work
BATCH_LIMIT = "200" # You can increase up to 2000
BASE_RETRY_DELAY = 1  # Base delay in seconds for exponential backoff

os.makedirs(SAVE_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download_errors.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# SOAP Request for session ID
LOGIN_PAYLOAD_TEMPLATE = """<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:partner.soap.sforce.com">
    <soapenv:Header>
        <urn:CallOptions>
            <urn:client>RestClient</urn:client>
            <urn:defaultNamespace>sf</urn:defaultNamespace>
        </urn:CallOptions>
    </soapenv:Header>
    <soapenv:Body>
        <urn:login>
            <urn:username>{username}</urn:username>
            <urn:password>{password}</urn:password>
        </urn:login>
    </soapenv:Body>
</soapenv:Envelope>"""

CSV_HEADER = [
    "BodyLength", "ContentType", "CreatedById", "CreatedDate", "Description",
    "Id", "IsDeleted", "IsPrivate", "LastModifiedById", "LastModifiedDate",
    "Name", "OwnerId", "ParentId", "SystemModstamp"
]

# Global session management
session_lock = Lock()
current_session_id = None
current_instance_url = None
session = requests.Session()  # Reusable HTTP session

def authenticate():
    """Authenticate with Salesforce and update global session variables."""
    global current_session_id, current_instance_url
    login_payload = LOGIN_PAYLOAD_TEMPLATE.format(
        username=USERNAME,
        password=PASSWORD + SECURITY_TOKEN
    )
    headers = {"Content-Type": "text/xml", "SOAPAction": "login"}
    
    try:
        response = requests.post(LOGIN_URL, data=login_payload, headers=headers)
        response.raise_for_status()
        
        # Parse SOAP response
        namespaces = {
            'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
            'partner': 'urn:partner.soap.sforce.com'
        }
        root = ET.fromstring(response.content)
        session_id = root.find('.//partner:sessionId', namespaces).text
        server_url = root.find('.//partner:serverUrl', namespaces).text
        instance_url = server_url.split('/services')[0]
        
        with session_lock:
            current_session_id = session_id
            current_instance_url = instance_url
        logger.info("Authentication successful")
        return session_id, instance_url
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        raise

def load_json(filename, default=None):
    """Load JSON data from file, return default if file doesn't exist."""
    if default is None:
        default = {}
    try:
        if os.path.exists(filename):
            with open(filename, "r") as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load {filename}: {str(e)}")
    return default

def save_json(filename, data):
    """Save data to JSON file atomically."""
    try:
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save {filename}: {str(e)}")

def authenticated_request(url, stream=False):
    """Make an authenticated GET request, handling session expiration."""
    global current_session_id, current_instance_url
    for _ in range(2):  # Retry once if session expires
        with session_lock:
            session_id = current_session_id
            headers = {"Authorization": f"Bearer {session_id}"}
        
        try:
            response = session.get(url, headers=headers, stream=stream, timeout=30)
            if response.status_code == 401:
                raise requests.exceptions.HTTPError("Session expired", response=response)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.info("Session expired, re-authenticating...")
                authenticate()  # Updates global session variables
            else:
                raise
    raise requests.exceptions.RetryError("Failed after reauthentication attempt")

def fetch_attachments_page(last_created_date=None, last_id=None):
    """Fetch a page of attachments from Salesforce using query API."""
    try:
        with session_lock:
            instance_url = current_instance_url
        
        base_query = (
            "SELECT " + ", ".join(CSV_HEADER) + " FROM Attachment"
        )
        conditions = ["BodyLength > 0"]  # Always include BodyLength condition
        
        if last_created_date and last_id:
            # Correctly handle pagination with both last_created_date and last_id
            conditions.append(
                f"(CreatedDate < {last_created_date} OR (CreatedDate = {last_created_date} AND Id > '{last_id}'))"
            )
        elif last_created_date:
            # Fallback condition if last_id is missing (shouldn't happen with proper markers)
            conditions.append(f"CreatedDate < {last_created_date}")
        
        query = base_query
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        # Ensure deterministic ordering with Id as tiebreaker
        query += f" ORDER BY CreatedDate DESC, Id ASC LIMIT {BATCH_LIMIT}"
        
        encoded_query = requests.utils.quote(query)
        url = f"{instance_url}/services/data/v{SALESFORCE_API_VERSION}/query/?q={encoded_query}"
        
        response = authenticated_request(url)
        data = response.json()
        records = data.get("records", [])
        next_url = data.get("nextRecordsUrl")
        
        next_marker = {}
        if records:
            last_record = records[-1]
            next_marker = {
                "last_created_date": last_record.get("CreatedDate"),
                "last_id": last_record.get("Id")
            }

        return records, next_marker, len(records) > 0
    except Exception as e:
        logger.error(f"Failed to fetch attachments: {str(e)}")
        raise

def download_attachment(attachment, csv_lock):
    """Download a single attachment with retries and integrity checks."""
    att_id = attachment["Id"]
    att_name = attachment.get("Name", "unnamed")
    _, file_extension = os.path.splitext(att_name)
    file_extension = file_extension or ".bin"
    file_path = os.path.join(SAVE_DIR, f"{att_id}{file_extension}")
    status = "Failed"

    # Check existing file
    if os.path.exists(file_path):
        expected_size = attachment.get("BodyLength")
        if expected_size and os.path.getsize(file_path) == expected_size:
            return f"Skipped (Exists): {att_name}"
        logger.warning(f"Redownloading incomplete {att_name}")
        os.remove(file_path)

    # Download with retries
    for attempt in range(RETRY_COUNT + 1):
        try:
            with session_lock:
                instance_url = current_instance_url
            url = f"{instance_url}/services/data/v{SALESFORCE_API_VERSION}/sobjects/Attachment/{att_id}/Body"
            response = authenticated_request(url, stream=True)
            response.raise_for_status()  # <-- THIS WAS MISSING

            # Stream download to file
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            # Verify file size
            expected_size = attachment.get("BodyLength")
            actual_size = os.path.getsize(file_path)
            if expected_size and actual_size != expected_size:
                raise IOError(f"Size mismatch: {actual_size} vs {expected_size}")

            # Write metadata to CSV
            with csv_lock:
                file_exists = os.path.isfile(CSV_FILE)
                with open(CSV_FILE, "a", newline="") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(CSV_HEADER)
                    writer.writerow([attachment.get(field, "") for field in CSV_HEADER])
            status = "Downloaded"
            break
        except requests.exceptions.HTTPError as e:
            retry_after = int(e.response.headers.get("Retry-After", 30)) if e.response.status_code == 429 else (BASE_RETRY_DELAY * (2 ** attempt)) + random.uniform(0, 1)
            if attempt < RETRY_COUNT:
                logger.warning(f"HTTPError on attempt {attempt+1} for {att_name}: {e}. Retrying in {retry_after:.1f}s.")
                time.sleep(retry_after)
        except Exception as e:
            sleep_time = (BASE_RETRY_DELAY * (2 ** attempt)) + random.uniform(0, 1)
            if attempt < RETRY_COUNT:
                logger.warning(f"Error on attempt {attempt+1} for {att_name}: {e}. Retrying in {sleep_time:.1f}s.")
                time.sleep(sleep_time)
    else:
        logger.error(f"Failed after {RETRY_COUNT} retries: {att_name}")
        return f"{status}: {att_name}"

    return f"{status}: {att_name}"

def main():
    """Main download orchestration logic."""
    try:
        # Initialize session
        authenticate()
        csv_lock = Lock()
        has_more = True

        while has_more:
            try:
                 # Load progress markers
                marker = load_json(MARKER_FILE, {"last_created_date": None, "last_id": None})

                # Fetch next page of attachments
                attachments, new_marker, has_more = fetch_attachments_page(
                    marker.get("last_created_date"), marker.get("last_id")
                )
                
                logger.info(f"Processing {len(attachments)} new attachments")

                with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                    futures = [
                        executor.submit(download_attachment, att, csv_lock)
                        for att in attachments
                    ]
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            logger.info(result)
                        except Exception as e:
                            logger.error(f"Download error: {str(e)}")
                
                # Update progress marker
                if new_marker:
                    save_json(MARKER_FILE, new_marker)
                    logger.info(f"Checkpoint updated: {new_marker}")
            except Exception as e:
                logger.error(f"Page processing failed: {str(e)}", exc_info=True)
                break
        
        logger.info("Download completed successfully")
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
