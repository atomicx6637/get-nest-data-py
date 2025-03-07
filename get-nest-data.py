import requests
import time
import json
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Load environment variables
load_dotenv()

# Load configuration
CONFIG_PATH = os.getenv("CONFIG_PATH", "config.json")
if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, 'r') as config_file:
        CONFIG = json.load(config_file)
else:
    logger.error("Configuration file not found.")
    exit(1)

# Access the environment variables
CLIENT_ID = CONFIG.get("client_id")
CLIENT_SECRET = CONFIG.get("client_secret")
EMAIL_SETTINGS = CONFIG.get("email_settings")
DATABASE_SETTINGS = CONFIG.get("database_settings")
LOGGING_FILE_PATH = CONFIG.get("logging_file_path")
TOKEN_FILE = CONFIG.get("token_file")
REDIRECT_URI = CONFIG.get("redirect_uri")
SDM_API_ENDPOINT = CONFIG.get("sdm_api_endpoint")
GOOGLE_API_SCOPE = CONFIG.get("google_api_scope")
TOKEN_API_ENDPOINT = CONFIG.get("token_api_endpoint")

# Set up logger with rotating file handler
log_file = LOGGING_FILE_PATH + 'get-nest-data.log'  # Replace with your desired log file path

# Create a logger
logger = logging.getLogger('nest_data_logger')
logger.setLevel(logging.INFO)

# Create a rotating file handler
handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)  # 5 MB file size, 3 backup files
handler.setLevel(logging.INFO)

# Create a formatter and set it for the handler
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

def send_failure_email(error_message):
    """Send an email notification when a failure occurs."""
    try:
        subject = "⚠️ Google Nest API Failure Notification"
        body = f"""
        <html>
        <body>
            <h2 style="color: red;">Google Nest API Authentication Failed</h2>
            <p><strong>Error Details:</strong></p>
            <pre>{error_message}</pre>
            <p>Please investigate the issue.</p>
        </body>
        </html>
        """

        # Create the email
        msg = MIMEMultipart()
        msg["From"] = EMAIL_SETTINGS["EMAIL_SENDER"]
        msg["To"] = EMAIL_SETTINGS["EMAIL_RECEIVER"]
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))

        # Connect to SMTP server
        context = ssl.create_default_context()
        with smtplib.SMTP(EMAIL_SETTINGS["SMTP_SERVER"], EMAIL_SETTINGS["SMTP_PORT"]) as server:
            server.starttls(context=context)
            server.login(EMAIL_SETTINGS["EMAIL_SENDER"], EMAIL_SETTINGS["EMAIL_PASSWORD"])
            server.sendmail(EMAIL_SETTINGS["EMAIL_SENDER"], EMAIL_SETTINGS["EMAIL_RECEIVER"], msg.as_string())

        print("✅ Failure notification email sent successfully!")

    except Exception as e:
        print(f"❌ Failed to send email: {e}")

# Step 1: Get Authorization Code (this needs to be done manually once)
def get_authorization_url():
    auth_url = f'https://accounts.google.com/o/oauth2/v2/auth?client_id={CLIENT_ID}&response_type=code&scope={GOOGLE_API_SCOPE}&redirect_uri={REDIRECT_URI}&access_type=offline&prompt=consent'
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

    response = requests.post(f"{TOKEN_API_ENDPOINT}/token", data=data)
    response_data = response.json()

    if response.status_code != 200:
        logger.error(f"Error: {response_data}")
        send_failure_email(f"Error in exchange_code_for_tokens: {response_data}.")
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

    response = requests.post(f"{TOKEN_API_ENDPOINT}/token", data=data)
    response_data = response.json()

    if response.status_code != 200:
        logger.error(f"Error: {response_data}")
        send_failure_email(f"Error in refresh_access_token: {response_data}.")
        return None

    new_access_token = response_data.get('access_token')
    new_expires_in = response_data.get('expires_in')

    return new_access_token, new_expires_in

# Step 4: Get the list of devices from Google Smart Device Management API
def get_devices(access_token):
    try:
        connection = mysql.connector.connect(
            host=DATABASE_SETTINGS["host"],
            database=DATABASE_SETTINGS["database"],
            user=DATABASE_SETTINGS["user"],
            password=DATABASE_SETTINGS["password"]
		)

        if connection.is_connected():
            db_Info = connection.get_server_info()
            logger.info(f"Connected to MySQL Server version {db_Info}")
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            logger.info(f"You're connected to database: {record}")

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
        }

        response = requests.get(f"{SDM_API_ENDPOINT}/devices", headers=headers)

        if response.status_code == 200:
            devices = response.json()

            for device in devices["devices"]:
                logger.info(f"Device Name: {device['name']}")

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
                logger.info(f"Record to insert: {record}")
                cursor.execute(mySql_insert_query, record)
                connection.commit()
                logger.info(f"{cursor.rowcount} Record inserted successfully into nest_data table")

        else:
            logger.error(f"Error fetching devices: {response.status_code}")
            send_failure_email(f"Error fetching devices: {resopnse.status_code}.")
            logger.error(response.json())

    except Error as e:
        logger.error(f"Error while connecting to MySQL: {e}")
        send_failure_email(f"Error while connecting to MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("MySQL connection is closed")

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
        logger.info("Access token expired, refreshing...")
        new_access_token, new_expires_in = refresh_access_token(tokens['refresh_token'])
        if new_access_token:
            new_expires_at = current_time + new_expires_in
            logger.info(f"New access token: {new_access_token}")
            logger.info(f"New token expires at: {new_expires_at}")
            save_tokens(new_access_token, tokens['refresh_token'], new_expires_in)
            return new_access_token
        else:
            logger.error("Failed to refresh access token.")
            send_failure_email(f"Failed to refresh access token.")
            return None

    return tokens['access_token']

# Step 8: Run the authentication check and device fetch logic
def authenticate_and_fetch_devices():
    # Load stored tokens
    tokens = load_tokens()

    if not tokens:
        logger.info("No tokens found. Please authenticate.")
        send_failure_email("No tokens found. Please authenticate.")
        # 1. Get authorization URL and ask user to authenticate
        print("Visit this URL to authenticate and get the authorization code:")
        print(get_authorization_url())

        # 2. Prompt user for the authorization code
        authorization_code = input("Enter the authorization code from the URL: ")

        # 3. Exchange the code for tokens
        access_token, refresh_token, expires_in = exchange_code_for_tokens(authorization_code)

        if access_token and refresh_token:
            logger.info(f"Access Token: {access_token}")
            logger.info(f"Refresh Token: {refresh_token}")
            logger.info(f"Access Token Expires In: {expires_in} seconds")

            # 4. Save the tokens to file
            save_tokens(access_token, refresh_token, expires_in)
        else:
            logger.error("Error obtaining tokens.")
            send_failure_email("Error obtaining tokens.")
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
            logger.error("Could not authenticate, token refresh failed.")
            send_failure_email("Could not authenticate, token refresh failed.")

# Main Function to run the task
if __name__ == "__main__":
    authenticate_and_fetch_devices()
