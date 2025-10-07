import os
import requests
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

LOGIN_URL = "https://wms.3plwinner.com/VeraCore/Public.Api"
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
SYSTEM_ID = os.getenv("SYSTEM_ID")
W_TOKEN = os.getenv("W_TOKEN")
OUTPUT_FOLDER = 'csvs'

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
    """
    Move a unit (LPN) to a specific location
    
    Args:
        unit_id: The Unit ID (without 'N' prefix)
        location_id: The destination Location ID (not Zone*Aisle*Rack*Level)
        auth_header: Authorization header with bearer token
    
    Returns:
        bool: True if successful, False otherwise
    """
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

#Alcohol Product Owners that need to be placed in only Aisle 7
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

    valid_zones = ['East', 'West']
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
        return None
    

    # Helper: applies front/back rules to a group of slots
    def choose_from_group(zone, aisle, rack, group, section_units, reason_prefix):
        front = group[group['Level'].str.contains('F', case=False, na=False)]
        back  = group[group['Level'].str.contains('B', case=False, na=False)]
        front_occupied = not section_units[section_units['Level'].str.contains('F', case=False, na=False)].empty
        back_occupied  = not section_units[section_units['Level'].str.contains('B', case=False, na=False)].empty

        # Rule 1: both empty → back
        if not front_occupied and not back_occupied and not back.empty:
            best = back.iloc[0]
            return {
                'Location ID': best['Location ID'],
                'Zone': best['Zone ID'], 'Aisle': best['Aisle'], 'Rack': best['Rack'], 'Level': best['Level'],
                'Reason': f'{reason_prefix} - back chosen (both empty)'
            }

        # Rule 2: front empty + back full → front
        if not front_occupied and back_occupied and not front.empty:
            best = front.iloc[0]
            return {
                'Location ID': best['Location ID'],
                'Zone': best['Zone ID'], 'Aisle': best['Aisle'], 'Rack': best['Rack'], 'Level': best['Level'],
                'Reason': f'{reason_prefix} - front chosen (back full)'
            }

        # Rule 3: back empty + front full → back
        if front_occupied and not back_occupied and not back.empty:
            best = back.iloc[0]
            return {
                'Location ID': best['Location ID'],
                'Zone': best['Zone ID'], 'Aisle': best['Aisle'], 'Rack': best['Rack'], 'Level': best['Level'],
                'Reason': f'{reason_prefix} - back chosen (front full)'
            }

        # Default: prefer front if available
        if not front.empty:
            best = front.iloc[0]
            return {
                'Location ID': best['Location ID'],
                'Zone': best['Zone ID'], 'Aisle': best['Aisle'], 'Rack': best['Rack'], 'Level': best['Level'],
                'Reason': f'{reason_prefix} - front (default)'
            }
        
        # Last resort: back
        if not back.empty:
            best = back.iloc[0]
            return {
                'Location ID': best['Location ID'],
                'Zone': best['Zone ID'], 'Aisle': best['Aisle'], 'Rack': best['Rack'], 'Level': best['Level'],
                'Reason': f'{reason_prefix} - back (only option)'
            }
        
        return None

    # --- 1. Try same product
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
                choice = choose_from_group(unit['Zone'], unit['Aisle'], unit['Rack'], group, section_units, "Same product")
                if choice:
                    return choice

    # --- 2. Try same product owner (FIX: use correct column name)
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
                choice = choose_from_group(unit['Zone'], unit['Aisle'], unit['Rack'], group, section_units, "Same owner")
                if choice:
                    return choice

    # --- 3. General warehouse (fallback)
    aisle_rack_groups = available_locations.groupby(['Zone ID','Aisle','Rack'])
    for (zone, aisle, rack), group in aisle_rack_groups:
        section_units = located_units[
            (located_units['Zone'] == zone) & (located_units['Aisle'] == aisle) & (located_units['Rack'] == rack)
        ]
        choice = choose_from_group(zone, aisle, rack, group, section_units, "General")
        if choice:
            return choice

    return None



def move_unlocated_units_fifo(unlocated_units_df, locations_df, units_df, auth_header):
    """
    Move all unlocated units to optimal FIFO locations
    
    Args:
        unlocated_units_df: DataFrame of unlocated units
        locations_df: DataFrame of all warehouse locations
        units_df: DataFrame of all units
        auth_header: Authorization header with bearer token
    
    Returns:
        dict: Results summary with success/failure counts
    """
    results = {
        "success": 0, 
        "failed": 0, 
        "no_location": 0,
        "errors": [],
        "placements": []
    }
    
    print(f"\n--- Starting FIFO-Based Placement ---")
    print(f"Processing {len(unlocated_units_df)} unlocated units...\n")
    
    for idx, row in unlocated_units_df.iterrows():
        unit_id = str(row['Unit ID'])
        product_id = row['Product ID']
        receipt_date = row.get('Receipt Date', '')
        
        # Remove 'N' prefix if present
        if unit_id.startswith('N'):
            unit_id = unit_id[1:]
        
        product_owner = row.get('Product Owner Name', '')
        # Find best location using FIFO algorithm
        best_location = find_best_location(
            product_id,
            product_owner,
            locations_df, 
            units_df
        )
        
        if best_location and best_location.get('Location ID'):
            location_id = str(best_location['Location ID'])
            
            print(f"Unit {unit_id} ({product_id[:30]}...)")
            print(f"  → Location {location_id} ({best_location.get('Zone')}-{best_location.get('Aisle')}-{best_location.get('Rack')}-{best_location.get('Level')})")
            print(f"  → {best_location.get('Reason')}")
            
            success = move_unit(unit_id, location_id, auth_header)
            
            if success:
                results["success"] += 1
                results["placements"].append({
                    'Unit ID': unit_id,
                    'Product ID': product_id,
                    'Location ID': location_id,
                    'Physical Location': f"{best_location.get('Zone')}-{best_location.get('Aisle')}-{best_location.get('Rack')}-{best_location.get('Level')}",
                    'Reason': best_location.get('Reason')
                })
            else:
                results["failed"] += 1
                results["errors"].append(unit_id)
            
            # Rate limiting
            time.sleep(0.5)
        else:
            print(f"✗ No suitable location found for Unit {unit_id} ({product_id})")
            results["no_location"] += 1
    
    # Save placement report
    if results["placements"]:
        placements_df = pd.DataFrame(results["placements"])
        output_path = os.path.join(OUTPUT_FOLDER, "placement_report.csv")
        placements_df.to_csv(output_path, index=False)
        print(f"\nPlacement report saved to placement_report.csv")
    
    print(f"\n--- FIFO Placement Results ---")
    print(f"Successfully placed: {results['success']}")
    print(f"Failed to move: {results['failed']}")
    print(f"No location found: {results['no_location']}")
    if results["errors"]:
        print(f"Failed Unit IDs: {results['errors']}")
    
    return results


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