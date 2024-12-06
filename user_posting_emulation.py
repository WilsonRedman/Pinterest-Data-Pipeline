import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import yaml
import sqlalchemy
import datetime
from sqlalchemy import text


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        with open("db_creds.yaml", "r") as stream:
            creds = yaml.safe_load(stream)

        self.HOST = creds["HOST"]
        self.USER = creds["USER"]
        self.PASSWORD = creds["PASSWORD"]
        self.DATABASE = creds["DATABASE"]
        self.PORT = creds["PORT"]
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)

            invoke_url = "https://804gwh33gk.execute-api.us-east-1.amazonaws.com/dev/topics/0e59bc5e89eb.pin"
            post_data(invoke_url, pin_selected_row)
            

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)

            invoke_url = "https://804gwh33gk.execute-api.us-east-1.amazonaws.com/dev/topics/0e59bc5e89eb.geo"
            post_data(invoke_url, geo_selected_row)


            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)

            invoke_url = "https://804gwh33gk.execute-api.us-east-1.amazonaws.com/dev/topics/0e59bc5e89eb.user"
            post_data(invoke_url, user_selected_row)


def serialize_datetime(obj):
    
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    else:
        raise TypeError(f"Type not serializable: {type(obj)}")


def post_data(invoke_url, selected_rows):

    records = []
    for row in selected_rows:
        result = dict(row._mapping)
        records.append({"value": result})
    
    payload = json.dumps({"records": records}, default=serialize_datetime)
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.request("POST", invoke_url, headers=headers, data=payload)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


