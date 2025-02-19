import requests
import time
import json
import os
import mysql.connector
from mysql.connector import Error


# Define your Google OAuth credentials
CLIENT_ID = ''; # Replace with your OAuth client ID
CLIENT_SECRET = ''; # Replace with your OAuth client secret
REDIRECT_URI = 'https://www.atomicxterra.com/googleCallback'  # Replace with your redirect URI
SCOPE = 'https://www.googleapis.com/auth/sdm.service'
TOKEN_URL = 'https://www.googleapis.com/oauth2/v4/token'

# API URL for Smart Device Management
SDM_API_URL = 'https://smartdevicemanagement.googleapis.com/v1/enterprises/7ab17f6b-d1d0-437f-acde-84d3d8a89c3a/devices'  # Replace with your enterprise ID
TOKEN_FILE = '/home/trichard/projects/tokens.json'  # File to store access and refresh tokens

# Step 1: Get Authorization Code (this needs to be done manually once)
def get_authorization_url():
    auth_url = f'https://accounts.google.com/o/oauth2/v2/auth?client_id={CLIENT_ID}&response_type=code&scope={SCOPE}&redirect_uri={REDIRECT_URI}&access_type=offline&prompt=consent'
    return auth_url

# Step 2: Exchange authorization code for tokens
def exchange_code_for_tokens(authorization_code):
    data = {
        'code': authorization_code,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code'
    }

    response = requests.post(TOKEN_URL, data=data)
    response_data = response.json()

    if response.status_code != 200:
        print(f"Error: {response_data}")
        return None

    access_token = response_data.get('access_token')
    refresh_token = response_data.get('refresh_token')
    expires_in = response_data.get('expires_in')

    return access_token, refresh_token, expires_in

# Step 3: Refresh the access token using the refresh token
def refresh_access_token(refresh_token):
    data = {
        'refresh_token': refresh_token,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'refresh_token'
    }

    response = requests.post(TOKEN_URL, data=data)
    response_data = response.json()

    if response.status_code != 200:
        print(f"Error: {response_data}")
        return None

    new_access_token = response_data.get('access_token')
    new_expires_in = response_data.get('expires_in')

    return new_access_token, new_expires_in

# Step 4: Get the list of devices from Google Smart Device Management API
def get_devices(access_token):
    try:

        connection = mysql.connector.connect(host='162.144.13.179',
            database='mutlizte_trichard',
            user='mutlizte_trichard_w',
            password='VfnWunjyCgusVBYu')
        
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
        }
    
        response = requests.get(SDM_API_URL, headers=headers)
    
        if response.status_code == 200:
            devices = response.json()
            #print("Devices:", json.dumps(devices, indent=2))

            for device in devices["devices"]:
                print(f"Device Name: {device['name']}")
                #print(f"Device Type: {device['type']}")
                #print(f"Assignee: {device['assignee']}")
    
                #print(f"Humidity: {device['traits']['sdm.devices.traits.Humidity']['ambientHumidityPercent']}")
                #print(f"Connectivity Status: {device['traits']['sdm.devices.traits.Connectivity']['status']}")
                #print(f"Thermo Status: {device['traits']['sdm.devices.traits.ThermostatHvac']['status']}")
                #print(f"Temp : {device['traits']['sdm.devices.traits.Temperature']['ambientTemperatureCelsius']}")
                #print(f"Heat To: {device['traits']['sdm.devices.traits.ThermostatTemperatureSetpoint']['heatCelsius']}")
                #print(f"Temp Scale: {device['traits']['sdm.devices.traits.Settings']['temperatureScale']}")
                #print(f"ThermostatMode: {device['traits']['sdm.devices.traits.ThermostatMode']['mode']}")
                #print("\n---\n")
    
                unique_name = device['name']
                custom_name = device['traits']['sdm.devices.traits.Info']['customName']
                humidity = device['traits']['sdm.devices.traits.Humidity']['ambientHumidityPercent']
                connectivity_status = device['traits']['sdm.devices.traits.Connectivity']['status']
                thermo_status = device['traits']['sdm.devices.traits.ThermostatHvac']['status']
                temperature = device['traits']['sdm.devices.traits.Temperature']['ambientTemperatureCelsius']
                temp_setpoint = device['traits']['sdm.devices.traits.ThermostatTemperatureSetpoint']['heatCelsius']
                temp_scale = device['traits']['sdm.devices.traits.Settings']['temperatureScale']
                mode = device['traits']['sdm.devices.traits.ThermostatMode']['mode']

                mySql_insert_query = """INSERT INTO nest_data (unique_name, custom_name, mode, temperature, humidity, temp_setpoint, temp_scale, thermo_status) values (%s, %s, %s, %s, %s, %s, %s, %s)"""
                record = (unique_name, custom_name, mode, temperature, humidity, temp_setpoint, temp_scale, thermo_status)
                print(record)
                cursor.execute(mySql_insert_query, record)
                connection.commit()
                print(cursor.rowcount, "Record inserted successfully into nest_data table")
        
        else:
            print(f"Error fetching devices: {response.status_code}")
            print(response.json())

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Step 5: Load tokens from a file
def load_tokens():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'r') as f:
            tokens = json.load(f)
        return tokens
    return None

# Step 6: Save tokens to a file
def save_tokens(access_token, refresh_token, expires_in):
    tokens = {
        'access_token': access_token,
        'refresh_token': refresh_token,
        'expires_in': expires_in,
        'expires_at': int(time.time()) + expires_in  # Store expiration time
    }
    with open(TOKEN_FILE, 'w') as f:
        json.dump(tokens, f)

# Step 7: Check if the token has expired and refresh if needed
def check_and_refresh_token(tokens):
    current_time = int(time.time())
    if current_time >= tokens['expires_at']:
        print("Access token expired, refreshing...")
        new_access_token, new_expires_in = refresh_access_token(tokens['refresh_token'])
        if new_access_token:
            new_expires_at = current_time + new_expires_in
            print(f"New access token: {new_access_token}")
            print(f"New token expires at: {new_expires_at}")
            save_tokens(new_access_token, tokens['refresh_token'], new_expires_in)
            return new_access_token
        else:
            print("Failed to refresh access token.")
            return None

    return tokens['access_token']

# Step 8: Run the authentication check and device fetch logic
def authenticate_and_fetch_devices():
    # Load stored tokens
    tokens = load_tokens()

    if not tokens:
        print("No tokens found. Please authenticate.")
        # 1. Get authorization URL and ask user to authenticate
        print("Visit this URL to authenticate and get the authorization code:")
        print(get_authorization_url())
        
        # 2. Prompt user for the authorization code
        authorization_code = input("Enter the authorization code from the URL: ")

        # 3. Exchange the code for tokens
        access_token, refresh_token, expires_in = exchange_code_for_tokens(authorization_code)

        if access_token and refresh_token:
            print(f"Access Token: {access_token}")
            print(f"Refresh Token: {refresh_token}")
            print(f"Access Token Expires In: {expires_in} seconds")

            # 4. Save the tokens to file
            save_tokens(access_token, refresh_token, expires_in)
        else:
            print("Error obtaining tokens.")
            return

        # Use the access token to fetch devices
        get_devices(access_token)
    else:
        # Check if the token is expired and refresh if needed
        access_token = check_and_refresh_token(tokens)

        if access_token:
            # Fetch devices from the Smart Device Management API
            get_devices(access_token)
        else:
            print("Could not authenticate, token refresh failed.")

# Main Function to run the task
if __name__ == "__main__":
    authenticate_and_fetch_devices()

