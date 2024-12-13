# Pinterest Data Pipeline

## Description
This project looks at creating an ETL pipeline using Amazon MSK.

## Installation


## How To Run
Setup EC2 instance with Kafka, connected to an MSK cluster.

Connect MSK cluster to S3 Bucket.

Configure API to retrieve data and split into topics.

Mount S3 bucket to databricks

## Usage
Data is retrieved from an external database in `user_posting_emulation.py`, and the function `run_infinite_post_data_loop` is used to emulate data streaming. It selects data from three different tables in a database and sends each to a different kafka topic via a created API.

`Databircks/s3_cleaning_functions.ipynb` provides functions for loading S3 data into a dataframe via `read_s3`. These dataframes can then be cleaned dependent on the topic via `clean_pin_data`, `clean_geo_data`, and `clean_user_data`.



## Structure

### Project Structure
An EC2 instance has Kafka setup, connected to an MSK instance. Three topics `pin`, `geo`, and `user` are created for the three emulated data streams.

-- Diagram of overall structure


### Repository Structure
The `Airflow` directory contains the python file `0e59bc5e89eb_dag` creating a DAG to automatically retrieve, clean, and save the kafka data inside of a databricks database.

The `Databricks` directory contains all of the python notebooks used to load data from the MSK S3 bucket, and clean it via `s3_cleaning_functions.ipynb`. `data_querying.ipynb` contains example transformations of the data using pyspark.


## Notes
Created custom plugin using S3 bucket.

Created connector.

Created API.

Connected API to Kafka.

Setup databricks to load S3 data, clean, and query.

Created apache airflow DAG to automatically clean and save batch data daily.