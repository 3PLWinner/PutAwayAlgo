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