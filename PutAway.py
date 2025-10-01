import os
import requests
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
load_dotenv()

LOGIN_URL = "https://wms.3plwinner.com/VeraCore/Public.Api"
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
SYSTEM_ID = os.getenv("SYSTEM_ID")
W_TOKEN = os.getenv("W_TOKEN")
OUTPUT_FOLDER = 'csvs'

# To get locations dynamic report
# 1) Run the POST Reports operation to create task of running the report (need report name)
# 2) Run the GET Task Status operation to check on status of task running the report (need Task ID from prev response)
# 3) When task status is "Done", run the GET report task operation to get report (need Task ID)

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
    
    print(f"Total units: {len(df)}")
    print(f"Unlocated units: {len(unlocated_df)}")
    print(f"Unlocated units saved to {output_csv_name}")
    
    return unlocated_df


def main():
    auth_header = {
        "Authorization": "bearer " + W_TOKEN
    }
    
    # Pull reports
    success = run_report("locations", [], auth_header, "locations.csv")
    success2 = run_report("unit-details-ALL", [], auth_header, "units.csv")
    
    if success and success2:
        print("Successfully pulled locations and units reports.")
        
        # Filter for unlocated units only
        units_path = os.path.join(OUTPUT_FOLDER, "units.csv")
        filter_unlocated_units(units_path, "unlocated_units.csv")
        
        return True
    else:
        print("Failed to pull reports.")
        return False


if __name__ == "__main__":
    success = main()



