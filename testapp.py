import os
import requests
import boto3
import pandas as pd
import time
import logging
from io import StringIO
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Veracore Configuration
LOGIN_URL = "https://wms.3plwinner.com/VeraCore/Public.Api"
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
SYSTEM_ID = os.getenv("SYSTEM_ID")
W_TOKEN = os.getenv("W_TOKEN")
OUTPUT_FOLDER = 'csvs'

# AWS Configuration
S3_BUCKET = os.getenv("S3_BUCKET")
ACCESS_KEY = os.getenv("S3_ACCESS")
SECRET_KEY = os.getenv("S3_SECRET")
AWS_REGION = os.getenv("AWS_REGION")


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s',
    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(f'veracore_s3_pipeline_{datetime.now().strftime("%Y%m%d")}.log')
    ]  
)

logger = logging.getLogger(__name__)


# S3 Client Initialization
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=AWS_REGION
    )

    logger.info("S3 client initialized successfully.")

except Exception as e:
    logger.error(f"Failed to initialize S3 client: {str(e)}")
    s3_client = None



# Login and get token
def get_auth_token():
    """Login and generate authentication token"""
    login_url = f"{LOGIN_URL}/api/login"
    payload = {
        "userName": USERNAME,
        "password": PASSWORD,
        "systemId": SYSTEM_ID
    }
    response = requests.post(login_url, json=payload)
    if response.status_code == 200:
        token_data = response.json()
        print(f"Token expires: {token_data.get('UtcExpirationDate')}")
        return token_data.get('Token')
    else:
        print(f"Login failed: {response.status_code} {response.text}")
        return None


# Check token status
def check_token_status(token):
    """Check if token is still valid"""
    status_url = f"{LOGIN_URL}/api/token"
    headers = {"Authorization": f"bearer {token}"}
    response = requests.get(status_url, headers=headers)
    if response.status_code == 200:
        status = response.json()
        print(f"Token status: {status}")
        return "valid" in status.lower()
    return False


# Move LPN/Unit to location
def move_unit(unit_id, location_id, auth_header):
    move_url = f"{LOGIN_URL}/api/inventory/lpns/{unit_id}/move"
    params = {"locationId": location_id}
    
    response = requests.put(move_url, params=params, headers=auth_header)
    
    if response.status_code == 200:
        print(f"✓ Unit {unit_id} moved to location {location_id}")
        return True
    else:
        print(f"✗ Failed to move unit {unit_id}: {response.status_code} - {response.text}")
        return False


# Start Task - POST
def get_report_task(report_name, filters, auth_header):
    api_task_run = f"{LOGIN_URL}/api/reports"
    payload = {
        "reportName": report_name,
        "filters": filters
    }
    response = requests.post(api_task_run, json=payload, headers=auth_header)
    if response.status_code == 200:
        response_data = response.json()
        task_id = response_data["TaskId"]
        return task_id


# Run Task ID - GET & GET Report Task once Done
def run_report(report_name, filters, auth_header, output_csv_name):
    task_id = get_report_task(report_name, filters, auth_header)
    if not task_id:
        print("Failed to start report task.")
        return False
    status_url = f"{LOGIN_URL}/api/reports/{task_id}/status"
    attempts = 20
    for attempt in range(attempts):
        status_response = requests.get(status_url, headers=auth_header)
        if status_response.status_code == 200:
            status = status_response.json().get("Status")
            if status == "Done":
                print("Report completed")
                break
            elif status == "Request too Large":
                print(f"Report Request too large: {status_response.status_code} {status_response.text}")
                return False
            elif status == "Processing":
                print("Report is Processing")
            else:
                if attempt % 5 == 0:
                    print(f"Report Status: {status} (attempt {attempt + 1})")
        else:
            print("Report did not process.")
            return False
        time.sleep(2)
    
    report_url = f"{LOGIN_URL}/api/reports/{task_id}"
    report_response = requests.get(report_url, headers=auth_header)
    if report_response.status_code == 200:
        report_data = report_response.json()["Data"]
        df = pd.DataFrame(report_data)
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        output_path = os.path.join(OUTPUT_FOLDER, output_csv_name)
        df.to_csv(output_path, index=False)
        print(f"Report Data saved to {output_csv_name}")
        return True
    else:
        print("Failed to fetch report data.")
        return False


def filter_unlocated_units(units_csv_path, output_csv_name):
    """
    Filter units to only include those without a location.
    A unit is unlocated if Building, Zone, Aisle, Rack, and Level are all empty.
    """
    df = pd.read_csv(units_csv_path)
    
    # Define location columns
    location_cols = ['Building', 'Zone', 'Aisle', 'Rack', 'Level']
    
    # Filter for unlocated units (all location columns are empty/null)
    unlocated_df = df[
        df[location_cols].isna().all(axis=1) | 
        (df[location_cols].astype(str).replace('', pd.NA).isna().all(axis=1))
    ]
    
    # Save filtered results
    output_path = os.path.join(OUTPUT_FOLDER, output_csv_name)
    unlocated_df.to_csv(output_path, index=False)
    
    print(f"\n--- Unlocated Units Summary ---")
    print(f"Total units: {len(df)}")
    print(f"Unlocated units: {len(unlocated_df)}")
    print(f"Unlocated units saved to {output_csv_name}\n")
    
    return unlocated_df

#Alcohol Product Owners that need to be placed in only West Zone
    #Premier Beverage Consortium
    #Knobel Spirits LLC
    #Wise Caldwell Distillers, LLC.

def find_best_location(product_id, product_owner, locations_df, units_df):
    """
    Optimized putaway logic with hierarchy:
    1. Same product
    2. Same product owner
    3. General front/back optimization
    """

    valid_zones = ['Racks']
    locations_df = locations_df[locations_df['Zone ID'].isin(valid_zones)].copy()
    locations_df = locations_df[locations_df['Level'].str.contains('B|F', case=False, na=False)]
    
    # Get occupied location IDs
    located_units = units_df[
        (units_df['Zone'].notna()) & (units_df['Aisle'].notna()) &
        (units_df['Rack'].notna()) & (units_df['Level'].notna())
    ].copy()
    
    occupied_locations = set()
    for _, unit in located_units.iterrows():
        loc_key = f"{unit['Zone']}_{unit['Aisle']}_{unit['Rack']}_{unit['Level']}"
        occupied_locations.add(loc_key)
    
    # Only get OPEN locations that are NOT occupied
    def is_location_available(row):
        loc_key = f"{row['Zone ID']}_{row['Aisle']}_{row['Rack']}_{row['Level']}"
        return loc_key not in occupied_locations
    
    available_locations = locations_df[
        (locations_df['Location Status'] == 'OPEN') &
        locations_df.apply(is_location_available, axis=1)
    ].copy()

    if available_locations.empty:
        return {
            'Location ID': None,
            'Decision Rule': 'No available locations',
            'Reason': 'All locations are occupied or none are OPEN',
            'Alternatives Considered': 0,
            'Confidence': 0.0
        }
    

    # Helper: applies front/back rules to a group of slots
    def choose_from_group(zone, aisle, rack, group, section_units, rule_name, priority):
        front = group[group['Level'].str[1] == 'F']
        back  = group[group['Level'].str[1] == 'B']
        front_occupied = not section_units[section_units['Level'].str[1] == 'F'].empty
        back_occupied  = not section_units[section_units['Level'].str[1] == 'B'].empty

        fifo_logic = None

        # Rule 1: both empty → back
        if not front_occupied and not back_occupied and not back.empty:
            best = back.iloc[0]
            fifo_logic = 'BOTH_EMPTY_USE_BACK'
            confidence = 0.95

        # Rule 2: front empty + back full → front
        elif not front_occupied and back_occupied and not front.empty:
            best = front.iloc[0]
            fifo_logic = 'BACK_FULL_USE_FRONT'
            confidence = 0.90


        # Default: prefer front if available
        elif not front.empty:
            best = front.iloc[0]
            fifo_logic = 'DEFAULT_USE_FRONT'
            confidence = 0.75
        
        # Last resort: back
        elif not back.empty:
            best = back.iloc[0]
            fifo_logic = 'LAST_RESORT_BACK'
            confidence = 0.70
        else:
            return None
        
        return {
            'Location ID': best['Location ID'],
            'Zone': best['Zone ID'],
            'Aisle': best['Aisle'],
            'Rack': best['Rack'],
            'Level': best['Level'],
            'Decision Rule': rule_name,
            'FIFO Logic': fifo_logic,
            'Priority': priority,
            'Confidence': confidence,
            'Reason': f"{rule_name} - {fifo_logic} - Zone:{zone}, Aisle:{aisle}, Rack:{rack}, Level:{best['Level']}",
            'Front Occupied': front_occupied,
            'Back Occupied': back_occupied,
            'Alternatives Considered': len(group)
        }

    # Priority 1 - Try same product
    same_product_units = located_units[located_units['Product ID'] == product_id]
    if not same_product_units.empty:
        for _, unit in same_product_units.iterrows():
            group = available_locations[
                (available_locations['Zone ID'] == unit['Zone']) &
                (available_locations['Aisle'] == unit['Aisle']) &
                (available_locations['Rack'] == unit['Rack'])
            ]
            if not group.empty:
                section_units = located_units[
                    (located_units['Zone'] == unit['Zone']) &
                    (located_units['Aisle'] == unit['Aisle']) &
                    (located_units['Rack'] == unit['Rack'])
                ]
                choice = choose_from_group(unit['Zone'], unit['Aisle'], unit['Rack'], group, section_units, 'SAME_PRODUCT', priority=1)
                if choice:
                    choice['Existing Units Same Product'] = len(same_product_units)
                    return choice

    # Priority 2 - Try same product owner (FIX: use correct column name)
    same_owner_units = located_units[located_units['Product Owner Name'] == product_owner]
    if not same_owner_units.empty:
        for _, unit in same_owner_units.iterrows():
            group = available_locations[
                (available_locations['Zone ID'] == unit['Zone']) &
                (available_locations['Aisle'] == unit['Aisle']) &
                (available_locations['Rack'] == unit['Rack'])
            ]
            if not group.empty:
                section_units = located_units[
                    (located_units['Zone'] == unit['Zone']) &
                    (located_units['Aisle'] == unit['Aisle']) &
                    (located_units['Rack'] == unit['Rack'])
                ]
                choice = choose_from_group(unit['Zone'], unit['Aisle'], unit['Rack'], group, section_units, 'SAME_OWNER', priority=2)
                if choice:
                    choice['Existing Units Same Owner'] = len(same_owner_units)
                    return choice

    # Priority 3 - General warehouse (fallback)
    aisle_rack_groups = available_locations.groupby(['Zone ID','Aisle','Rack'])
    for (zone, aisle, rack), group in aisle_rack_groups:
        section_units = located_units[
            (located_units['Zone'] == zone) & (located_units['Aisle'] == aisle) & (located_units['Rack'] == rack)
        ]
        choice = choose_from_group(zone, aisle, rack, group, section_units, 'GENERAL_WAREHOUSE', priority=3)
        if choice:
            return choice

    return {
        'Location ID': None,
        'Decision Rule': 'No suitable location found',
        'Reason': 'No suitable location found after checking all rules',
        'Alternatives Considered': len(available_locations),
        'Confidence': 0.0
    }



def move_unlocated_units_fifo(unlocated_units_df, locations_df, units_df, auth_header):
    results = {
        "success": 0, 
        "failed": 0, 
        "no_location": 0,
        "errors": [],
        "placements": [],
        "detailed_logs": []
    }
    
    print(f"\n--- Starting FIFO-Based Placement ---")
    print(f"Processing {len(unlocated_units_df)} unlocated units...\n")
    
    for idx, row in unlocated_units_df.iterrows():
        unit_id = str(row['Unit ID'])
        product_id = row['Product ID']
        receipt_date = row.get('Receipt Date', '')
        product_description = row.get('Product Description', '')
        
        # Remove 'N' prefix if present
        if unit_id.startswith('N'):
            unit_id = unit_id[1:]
        
        timestamp = datetime.now().isoformat()

        product_owner = row.get('Product Owner Name', '')
        # Find best location using FIFO algorithm
        best_location = find_best_location(
            product_id,
            product_owner,
            locations_df, 
            units_df
        )

        log_entry = {
            'timestamp': timestamp,
            "Unit ID": unit_id,
            "Product ID": product_id,
            "Product Description": product_description,
            "Product Owner": product_owner,
            "Receipt Date": receipt_date,
            'Decision Rule': best_location.get('Decision Rule'),
            'FIFO Logic': best_location.get('FIFO Logic'),
            'Priority': best_location.get('Priority'),
            'Confidence Score': best_location.get('Confidence'),
            "Assigned Location ID": best_location.get('Location ID'),
            "Assigned Zone": best_location.get('Zone'),
            "Assigned Aisle": best_location.get('Aisle'),
            "Assigned Rack": best_location.get('Rack'),
            "Assigned Level": best_location.get('Level'),
            'Front Occupied': best_location.get('Front Occupied'),
            'Back Occupied': best_location.get('Back Occupied'),
            'Alternatives Considered': best_location.get('Alternatives Considered', 0),
            'Existing Units Same Product': best_location.get('Existing Units Same Product', 0),
            'Existing Units Same Owner': best_location.get('Existing Units Same Owner', 0),
            "Placement Reason": best_location.get('Reason', 'No reason provided'),
            "Move Status": 'pending',
            "API Response": None,
            "Error Message": None,
            'Total Available Locations': len(locations_df[locations_df['Location Status'] == 'OPEN']),
            'Total Occupied Locations': len(locations_df[locations_df['Location Status'] != 'OPEN']),
        }
        
        if best_location and best_location.get('Location ID'):
            location_id = str(best_location['Location ID'])
            print(f"Unit {unit_id} ({product_id[:30]}...)")
            print(f"  → {best_location.get('Decision Rule')} ({best_location.get('FIFO Logic')})")
            print(f"  → Location {location_id} ({best_location.get('Zone')}-{best_location.get('Aisle')}-{best_location.get('Rack')}-{best_location.get('Level')})")
            print(f"  → Confidence: {best_location.get('Confidence', 0):.2f}")
            
            success = move_unit(unit_id, location_id, auth_header)
            
            if success:
                log_entry['Move Status'] = 'success'
                log_entry['API Response'] = 200
                results["success"] += 1
                results["placements"].append({
                    'Unit ID': unit_id,
                    'Product ID': product_id,
                    'Location ID': location_id,
                    'Physical Location': f"{best_location.get('Zone')}-{best_location.get('Aisle')}-{best_location.get('Rack')}-{best_location.get('Level')}",
                    'Decision Rule': best_location.get('Decision Rule'),
                    'Confidence': best_location.get('Confidence'),
                })
            else:
                log_entry['Move Status'] = 'API failed'
                log_entry['Error Message'] = 'API Move Failed'
                results["failed"] += 1
                results["errors"].append(unit_id)
            
            # Rate limiting
            time.sleep(0.5)
        else:
            log_entry['Move Status'] = 'no_location'
            log_entry['Error Message'] = 'No suitable location found'
            print(f"✗ No suitable location found for Unit {unit_id} ({product_id})")
            results["no_location"] += 1

        results["detailed_logs"].append(log_entry)

    # Save placement report
    if results["placements"]:
        placements_df = pd.DataFrame(results["placements"])
        output_path = os.path.join(OUTPUT_FOLDER, "placement_report.csv")
        placements_df.to_csv(output_path, index=False)
        print(f"\nPlacement report saved to placement_report.csv")
    
    if results['detailed_logs']:
        logs_df = pd.DataFrame(results['detailed_logs'])
        logs_df['date'] = pd.to_datetime(logs_df['timestamp']).dt.date
        logs_df['year'] = pd.to_datetime(logs_df['timestamp']).dt.year
        logs_df['month'] = pd.to_datetime(logs_df['timestamp']).dt.month
        logs_df['day'] = pd.to_datetime(logs_df['timestamp']).dt.day
        output_path = os.path.join(OUTPUT_FOLDER, f"putaway_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        logs_df.to_csv(output_path, index=False)
        print(f"Detailed logs saved to {output_path}")

    print(f"\n--- FIFO Placement Results ---")
    print(f"Successfully placed: {results['success']}")
    print(f"Failed to move: {results['failed']}")
    print(f"No location found: {results['no_location']}")
    if results["errors"]:
        print(f"Failed Unit IDs: {results['errors']}")

    return results

# S3 Upload Function
def upload_to_s3(df, report_name):
    """
    Uploads a DataFrame to an S3 bucket as a JSON file by converting pandas df to json in memory
    """
    if not s3_client:
        logger.error("S3 client is not initialized.")
        return False
    
    logger.info(f"Uploading report {report_name} to S3 bucket {S3_BUCKET}")
    
    # Convert df to json in memory
    json_buffer = StringIO()
    df.to_json(json_buffer, orient="records", lines=True)
    json_content = json_buffer.getvalue()

    # Create s3 file path w/date partitioning
    today = datetime.now()
    timestamp = today.strftime("%Y%m%d_%H%M%S")
    basename = Path(report_name).stem

    s3_key = f'veracore-reports/report-type={basename}/year={today.year}/month={today.month:02d}/day={today.day:02d}/{basename}_{timestamp}.json'

    s3_client.put_object(
        Bucket = S3_BUCKET,
        Key = s3_key,
        Body = json_content,
        ContentType = 'application/json'
    )

    logger.info(f"Uploaded Successfully")
    logger.info(f"Location: s3://{S3_BUCKET}/{s3_key}")
    logger.info(f" Rows: {len(df)}, Columns: {len(df.columns)}")

    return True

def main():
    # Use existing token or get new one
    token = W_TOKEN if W_TOKEN else get_auth_token()
    
    if not token:
        print("Failed to obtain authentication token.")
        return False
    
    auth_header = {
        "Authorization": f"bearer {token}"
    }
    
    # Check token status
    if not check_token_status(token):
        print("Token is invalid. Getting new token...")
        token = get_auth_token()
        if not token:
            return False
        auth_header["Authorization"] = f"bearer {token}"
    
    # Pull reports
    print("\n--- Pulling Reports ---")
    success = run_report("west-locations", [], auth_header, "locations.csv")
    success2 = run_report("unit-details-ALL", [], auth_header, "units.csv")
    
    if success and success2:
        print("Successfully pulled locations and units reports.")
        
        # Load data
        units_path = os.path.join(OUTPUT_FOLDER, "units.csv")
        locations_path = os.path.join(OUTPUT_FOLDER, "locations.csv")
        
        units_df = pd.read_csv(units_path)
        locations_df = pd.read_csv(locations_path)
        
        # Filter for unlocated units
        unlocated_df = filter_unlocated_units(units_path, "unlocated_units.csv")
        
        if len(unlocated_df) > 0:
            # Use FIFO algorithm to place unlocated units
            move_unlocated_units_fifo(
                unlocated_df, 
                locations_df, 
                units_df, 
                auth_header
            )
        else:
            print("No unlocated units to process.")
        
        return True
    else:
        print("Failed to pull reports.")
        return False


if __name__ == "__main__":
    success = main()