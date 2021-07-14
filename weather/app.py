import json
import pandas as pd
import requests
import boto3

# import requests

def lambda_handler(event, context):
    # Read csv file with city names and latitudes and longitudes
    df = pd.read_csv('worldcities.csv')
    city = 'Tokyo'
    if df[df['city_ascii'] == city]['city_ascii'].any():

        lat = df[df['city_ascii'] == city]['lat'] 
        lon = df[df['city_ascii'] == city]['lng']

        url = "https://climacell-microweather-v1.p.rapidapi.com/weather/realtime"

        # Query ClimaCell Weather API using latidude and longitude
        querystring = {"unit_system": "si",
        "fields": ["precipitation", "precipitation_type", "temp", "cloud_cover", "wind_speed",
        "weather_code"], "lat": lat, "lon": lon}

        # Host and key found in ClimaCell Weather API Documentation
        headers = {
        'x-rapidapi-host': "climacell-microweather-v1.p.rapidapi.com",
        'x-rapidapi-key': "64606952fcmsh4e754c5afa969d3p101001jsn250893a3dd43"
        }

        response = requests.request("GET", url, headers=headers, params=querystring).json()

        # Add response to a dictionary
        context = {'city_name': city,
        'temp': response['temp']['value'],
        'weather_code': response['weather_code']['value'],
        'wind_speed': response['wind_speed']['value'],
        'precipitation_type': response['precipitation_type']['value'] }

        # Connnect to S3 default profile
        s3 = boto3.client('s3')

        serializedMyData = json.dumps(context)

        # Write to S3 using unique key - Weather
        s3.put_object(Body=serializedMyData, Bucket='weatherdatabucket',Key='weather.json')

        return {
        "statusCode": 200,
        "body": json.dumps(context),
    }
    else:
        context = None