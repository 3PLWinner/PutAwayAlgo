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



def find_best_location(product_id, locations_df, units_df):
    """
    Find the best location for a product using these rules:
    1. Only use East and West zones
    2. Only use levels containing 'B' (back) or 'F' (front)
    3. Place near same product if it exists
    4. Use BACK if front is empty
    5. Use FRONT if back is full
    
    Args:
        product_id: The Product ID to place
        locations_df: DataFrame of all warehouse locations
        units_df: DataFrame of all units (located and unlocated)
    
    Returns:
        dict: Best location info or None if no suitable location found
    """
    
    # Filter to East and West zones only
    valid_zones = ['East', 'West']
    locations_df = locations_df[locations_df['Zone ID'].isin(valid_zones)].copy()
    
    # Filter to only levels with 'B' or 'F' in them
    locations_df = locations_df[
        locations_df['Level'].str.contains('B|F', case=False, na=False)
    ].copy()
    
    # Filter to only OPEN locations
    available_locations = locations_df[locations_df['Location Status'] == 'OPEN'].copy()
    
    if len(available_locations) == 0:
        return None
    
    # Get all LOCATED units (units with Zone, Aisle, Rack, Level filled)
    located_units = units_df[
        (units_df['Zone'].notna()) & (units_df['Zone'] != '') &
        (units_df['Aisle'].notna()) & (units_df['Aisle'] != '') &
        (units_df['Rack'].notna()) & (units_df['Rack'] != '') &
        (units_df['Level'].notna()) & (units_df['Level'] != '')
    ].copy()
    
    occupied_location_ids = set(located_units['Location ID'].dropna().astype(str))

    available_locations = locations_df[
        (locations_df['Location Status'] == 'OPEN') &
        (~locations_df['Location ID'].astype(str).isin(occupied_location_ids))
    ].copy()

    if len(available_locations) == 0:
        return None
    
    # Find where this product currently exists
    same_product_units = located_units[located_units['Product ID'] == product_id]
    
    if len(same_product_units) == 0:
        # New product - just pick first available BACK location, then FRONT
        back_locations = available_locations[
            available_locations['Level'].str.contains('B', case=False, na=False)
        ]
        if len(back_locations) > 0:
            best = back_locations.iloc[0]
            return {
                'Location ID': best['Location ID'],
                'Zone': best['Zone ID'],
                'Aisle': best['Aisle'],
                'Rack': best['Rack'],
                'Level': best['Level'],
                'Reason': 'New product - back location'
            }
        
        front_locations = available_locations[
            available_locations['Level'].str.contains('F', case=False, na=False)
        ]
        if len(front_locations) > 0:
            best = front_locations.iloc[0]
            return {
                'Location ID': best['Location ID'],
                'Zone': best['Zone ID'],
                'Aisle': best['Aisle'],
                'Rack': best['Rack'],
                'Level': best['Level'],
                'Reason': 'New product - front location'
            }
        
        return None
    
    # Product exists - find best aisle-rack sections where it lives
    aisle_rack_combos = same_product_units.groupby(['Zone', 'Aisle', 'Rack']).size().reset_index(name='count')
    aisle_rack_combos = aisle_rack_combos.sort_values('count', ascending=False)
    
    # Try each aisle-rack where product exists
    for _, ar in aisle_rack_combos.iterrows():
        zone = ar['Zone']
        aisle = ar['Aisle']
        rack = ar['Rack']
        
        # Get available locations in this aisle-rack
        section_available = available_locations[
            (available_locations['Zone ID'] == zone) &
            (available_locations['Aisle'] == aisle) &
            (available_locations['Rack'] == rack)
        ]
        
        if len(section_available) == 0:
            continue
        
        # Get ALL units (occupied) in this aisle-rack section
        section_occupied = located_units[
            (located_units['Zone'] == zone) &
            (located_units['Aisle'] == aisle) &
            (located_units['Rack'] == rack)
        ]
        
        # Count how many front and back locations are occupied
        front_occupied = len(section_occupied[
            section_occupied['Level'].str.contains('F', case=False, na=False)
        ])
        
        back_occupied = len(section_occupied[
            section_occupied['Level'].str.contains('B', case=False, na=False)
        ])
        
        # Get available front and back locations
        front_available = section_available[
            section_available['Level'].str.contains('F', case=False, na=False)
        ]
        
        back_available = section_available[
            section_available['Level'].str.contains('B', case=False, na=False)
        ]
        
        # Apply logic: back if front empty, front if back full
        if front_occupied == 0 and len(back_available) > 0:
            # Front is empty, use back
            best = back_available.iloc[0]
            return {
                'Location ID': best['Location ID'],
                'Zone': best['Zone ID'],
                'Aisle': best['Aisle'],
                'Rack': best['Rack'],
                'Level': best['Level'],
                'Reason': f'Back location (front empty in {zone}-{aisle}-{rack})'
            }
        
        elif back_occupied > 0 and len(front_available) > 0:
            # Back is occupied, use front
            best = front_available.iloc[0]
            return {
                'Location ID': best['Location ID'],
                'Zone': best['Zone ID'],
                'Aisle': best['Aisle'],
                'Rack': best['Rack'],
                'Level': best['Level'],
                'Reason': f'Front location (back occupied in {zone}-{aisle}-{rack})'
            }
        
        elif len(front_available) > 0:
            # Default to front
            best = front_available.iloc[0]
            return {
                'Location ID': best['Location ID'],
                'Zone': best['Zone ID'],
                'Aisle': best['Aisle'],
                'Rack': best['Rack'],
                'Level': best['Level'],
                'Reason': f'Front location (default in {zone}-{aisle}-{rack})'
            }
        
        elif len(back_available) > 0:
            # Only back available
            best = back_available.iloc[0]
            return {
                'Location ID': best['Location ID'],
                'Zone': best['Zone ID'],
                'Aisle': best['Aisle'],
                'Rack': best['Rack'],
                'Level': best['Level'],
                'Reason': f'Back location (only option in {zone}-{aisle}-{rack})'
            }
    
    # No locations available in same aisle-rack as product, find any available location
    back_locations = available_locations[
        available_locations['Level'].str.contains('B', case=False, na=False)
    ]
    if len(back_locations) > 0:
        best = back_locations.iloc[0]
        return {
            'Location ID': best['Location ID'],
            'Zone': best['Zone ID'],
            'Aisle': best['Aisle'],
            'Rack': best['Rack'],
            'Level': best['Level'],
            'Reason': 'Fallback - back location (no space near existing product)'
        }
    
    front_locations = available_locations[
        available_locations['Level'].str.contains('F', case=False, na=False)
    ]
    if len(front_locations) > 0:
        best = front_locations.iloc[0]
        return {
            'Location ID': best['Location ID'],
            'Zone': best['Zone ID'],
            'Aisle': best['Aisle'],
            'Rack': best['Rack'],
            'Level': best['Level'],
            'Reason': 'Fallback - front location (no space near existing product)'
        }
    
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
        
        # Find best location using FIFO algorithm
        best_location = find_best_location(
            product_id, 
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