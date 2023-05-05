import argparse
import os
from datetime import datetime, timedelta, timezone

import mysql.connector
import requests
from dotenv import load_dotenv


def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--end", type=str, required=True, help="Ending date (YYYY-MM-DD)"
    )
    return parser


load_dotenv()

db_name = os.environ["DB_NAME"]
db_host = os.environ["DB_HOST"]
db_password = os.environ["DB_PASSWORD"]
db_user = os.environ["DB_USER"]

api_key = os.environ["API_KEY"]


parser = init_argparse()
args = parser.parse_args()

end_date = args.end

api_endpoint = "https://api.nasa.gov/neo/rest/v1/feed"
access_token = api_key

db_config = {
    "host": db_host,
    "user": db_user,
    "database": db_host,
    "password": db_password,
}

db = mysql.connector.connect(**db_config)
cursor = db.cursor()

date_format = "%Y-%m-%d"


def insert_neo(neo):
    epoch_timestamp_second = (
        neo["close_approach_data"][0]["epoch_date_close_approach"] / 1000
    )
    neo_date = datetime.fromtimestamp(epoch_timestamp_second, timezone.utc)

    neo_distance = float(neo["close_approach_data"][0]["miss_distance"]["miles"])
    neo_diameter = float(neo["estimated_diameter"]["feet"]["estimated_diameter_max"])

    neo_reference_id = neo["neo_reference_id"][-4:]
    print(neo_date, neo_distance, neo_diameter, neo_reference_id)
    sql = "INSERT INTO neo (date, distance, diameter, reference_id) VALUES (%s, %s, %s, %s)"
    val = (neo_date, neo_distance, neo_diameter, neo_reference_id)
    cursor.execute(sql, val)
    db.commit()


def get_neos(start_date, end_date):
    end_date_obj = datetime.strptime(end_date, date_format)
    
    while True:
        
        start_obj = datetime.strptime(start_date, date_format)
        next_week_days_obj = start_obj + timedelta(days=7)
        next_week_days = next_week_days_obj.strftime(date_format)

        if next_week_days_obj >= end_date_obj:
            break

        params = {
            "start_date": start_date,
            "end_date": next_week_days,
            "api_key": access_token,
        }

        response = requests.get(api_endpoint, params=params)
        print(start_date, next_week_days, end_date, response)

        neos = response.json()["near_earth_objects"][start_date]
        for neo in neos:
            insert_neo(neo)
        
        start_date = (
            datetime.strptime(next_week_days, date_format) + timedelta(days=1)
        ).strftime(date_format)


get_neos("1982-12-10", end_date)

db.close()
