import requests
import json
import mysql.connector
from mysql.connector import Error


# Replace these with your actual credentials
CLIENT_ID = '124412772589-15kitj5tj7v867jb0rf7ovbom8n8ob2p.apps.googleusercontent.com'
CLIENT_SECRET = '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'
REFRESH_TOKEN = '1//040g7qg6eBxAsCgYIARAAGAQSNwF-L9Ird7sbMqc3pCmXdcfV5YPF_glFi9Swhngp9z0wFjsJjkBgLvhOsNZZZIJFWDU8Ky8nEgU'

# Get an access token using the refresh token
def get_access_token(client_id, client_secret, refresh_token):
    url = 'https://www.googleapis.com/oauth2/v4/token'
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }
    response = requests.post(url, data=payload)
    response_data = response.json()
    return response_data['access_token']

# Fetch thermostat data
def get_thermostat_data(access_token):
    url = 'https://smartdevicemanagement.googleapis.com/v1/enterprises/7ab17f6b-d1d0-437f-acde-84d3d8a89c3a/devices'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    return response.json()

# Main function
def main():
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


        access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
        thermostat_data = get_thermostat_data(access_token)
    
        for device in thermostat_data["devices"]:
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
    
    
            # Loop through and print device traits
            #print("Traits:")
            #for trait, details in device["traits"].items():
            #    print(f"  {trait}:")
            #    for key, value in details.items():
            #        print(f"    {key}: {value}")
    
            # Loop through and print parent relations
            #print("Parent Relations:")
            #for relation in device["parentRelations"]:
            #    print(f"  Parent: {relation['parent']}")
            #    print(f"  Display Name: {relation['displayName']}")
            print("\n---\n")


    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

        #print(json.dumps(thermostat_data, indent=2))

if __name__ == '__main__':
    main()
