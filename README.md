# Pinterest Data Pipeline

## Description
This project looks at creating ETL pipelines using pinterest data. One part looks at processing the data in bulk, using Kafka to move data, then cleaning and storing using databricks. The other part looks at streaming from python, and accessing the stream in databricks to clean and save data.

## Installation
Required modules:
- PyYAML
- Requests
- SQLAlchemy

Install by running `pip install -r requirements.txt` from inside of the directory.

All setup mentioned in [How To Run](#how-to-run) is required for this project to run correctly.


## How To Run
### Batch Processing
#### Setup
- Setup MSK cluster, connecting it to an S3 bucket.
- Configure an EC2 instance with Kafka, and connect to the MSK cluster.
- Configure an API to pass data to the EC2 Kafka instance.
- Mount the MSK S3 bucket to a databricks instance.
- Create Airflow environment, and connect it to databricks.
- Update db_creds.yaml, and update API links in `user_posting_emulation.py`.
- Upload `Databricks` files to databricks.
- Upload `Airflow` file to airflow (setup to run daily).

#### Running
Use `user_posting_emulation.py` to pass data from a database through the MSK cluster. `Databricks/clean_data.ipynb` will then be run daily, cleaning the data currently stored in the MSK S3 bucket, and saving it to a database. You can use `Databricks/data_querying` to then query some areas of the stored data.

### Streaming
#### Setup
- Setup Kinesis stream.
- Configure an API to interact with Kinesis stream.
- Setup a databricks instance permissions to access the Kinesis stream.
- Upload `Databricks` files to databricks.

#### Running
Use `user_posting_emulation_streaming.py` to periodically send data into the kinesis stream. Run `Databricks/data_streaming.ipynb` to take the data, clean it, and save it whilst streaming.


## Usage
### Batch Processing
Data is retrieved from an external database in [user_posting_emulation.py](Batch%20Processing/user_posting_emulation.py), and the function `post_data_loop` is used to emulate data streaming. It selects data from three different tables in a database and sends each to a different kafka topic via a created API.

[s3_cleaning_functions.ipynb](Batch%20Processing/Databricks/s3_cleaning_functions.ipynb) provides functions for loading S3 data into a dataframe via `read_s3`. These dataframes can then be cleaned dependent on the topic via `clean_pin_data`, `clean_geo_data`, and `clean_user_data`.

[clean_data.ipynb](Batch%20Processing/Databricks/clean_data.ipynb) is a single script using other functions to retrieve, clean and store all data currently available.

[data_querying.ipynb](Batch%20Processing/Databricks/data_querying.ipynb) provides several functions for querying dataframes using PySpark.


### Streaming
[user_posting_emulation_streaming.py](Streaming/user_posting_emulation_streaming.py) works similarly to [user_posting_emulation.py](Batch%20Processing/Databricks/user_posting_emulation.py), having slight changes in the API call being made (sending to Kinesis stream), and the category for the data is included in the payload rather than the API call.

[data_streaming.ipynb](Streaming/Databricks/data_streaming.ipynb) is a single script that accesses the Kinesis stream, separates data based on partition keys, cleans data using [s3_cleaning_functions.ipynb](Streaming/Databricks/s3_cleaning_functions.ipynb), and continuously writes the cleaned stream to a database.



## Structure
### Project Structure
#### Batch Processing
An EC2 instance has Kafka setup, connected to an MSK instance. Three topics `pin`, `geo`, and `user` are created for the three emulated data streams.

#### Streaming
Data is read from tables at random time intervals to act like streaming data. This is sent to the kinesis stream via an API, using 
different partitions for each topic. The data is streamed into databricks, cleaned, and then saved in tables for each topic.


### Repository Structure
#### Batch Processing
The `Batch Processing` directory is used for uploading, cleaning, and storing data as a single batch. 
The `Airflow` directory contains the python file `0e59bc5e89eb_dag` creating a DAG to automatically retrieve, clean, 
and save the kafka data inside of a databricks database. The `Databricks` directory contains all of the python notebooks 
used to load data from the MSK S3 bucket, and clean it via `s3_cleaning_functions.ipynb`. 
`data_querying.ipynb` contains example transformations of the data using pyspark.

#### Streaming
The `Streaming` directory is used for sending data to a kinesis stream, then cleaning and storing the data whilst it is being streamed. 
The `Databricks` directory  contains the notebook with the cleaning functionality, as well as the one to stream, clean, and store the data.