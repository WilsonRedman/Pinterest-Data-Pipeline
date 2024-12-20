import requests
from time import sleep
import random
import json
import yaml
import sqlalchemy
import datetime
from sqlalchemy import text


random.seed(100)

class AWSDBConnector:
    '''
    Class to connect to an database instance

    Attributes:
        HOST: Hostname of the database
        USER: Username of the database
        PASSWORD: Password of the database
        DATABASE: Name of the database
        PORT: Port of the database
    '''
    def __init__(self):
        '''
        Constructor to initialize the database credentials
        '''
        with open("db_creds.yaml", "r") as stream:
            creds = yaml.safe_load(stream)

        self.HOST = creds["HOST"]
        self.USER = creds["USER"]
        self.PASSWORD = creds["PASSWORD"]
        self.DATABASE = creds["DATABASE"]
        self.PORT = creds["PORT"]
        
    def create_db_connector(self):
        '''
        Function to create a database connector

        Returns:
            engine: Database engine
        '''
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def put_data_loop():
    '''
    Function to put data to the Kinesis stream for each of the tables
    '''
    sleep(random.randrange(0, 2))
    random_row = random.randint(0, 11000)
    engine = new_connector.create_db_connector()

    with engine.connect() as connection:
        invoke_url = "https://804gwh33gk.execute-api.us-east-1.amazonaws.com/dev/streams/Kinesis-Prod-Stream/record"
        headers = {'Content-Type': 'application/json'}

        pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
        pin_selected_row = connection.execute(pin_string)

        put_data(invoke_url, pin_selected_row, "pin")
            

        geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
        geo_selected_row = connection.execute(geo_string)

        put_data(invoke_url, geo_selected_row, "geo")


        user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
        user_selected_row = connection.execute(user_string)

        put_data(invoke_url, user_selected_row, "user")


def serialize_datetime(obj):
    '''
    Function to serialize datetime object

    Args:
        obj: datetime object

    Returns:
        isoformat of the datetime object
    '''
    
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    else:
        raise TypeError(f"Type not serializable: {type(obj)}")


def put_data(invoke_url, selected_rows, partitionKey):
    '''
    Function to put data to the Kinesis stream

    Args:
        invoke_url: string: URL to invoke the API
        selected_rows: rows selected from the database
        partitionKey: string: partition key for the Kinesis stream
    '''

    for row in selected_rows:
        ## Convert to correct dictionary format
        record = dict(row._mapping)
    
    payload = json.dumps({
        "StreamName": "Kinesis-Prod-Stream",
        "Data": record,
        "PartitionKey": partitionKey
        }, 
        default=serialize_datetime)
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    except requests.exceptions.RequestException as e:
        print(e)


if __name__ == "__main__":
    while True:
        put_data_loop()
        print('Working')