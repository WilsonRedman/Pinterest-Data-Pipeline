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

## Structure
An EC2 instance has Kafka setup, connected to an MSK instance. Three topics `pin`, `geo`, and `user` are created for the three emulated data streams.

-- Diagram of overall structure


## Notes
Created custom plugin using S3 bucket.

Created connector.

Created API

Connected API to Kafka