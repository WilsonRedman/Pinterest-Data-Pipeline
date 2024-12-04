# Pinterest Data Pipeline

## Description
This project looks at creating an ETL pipeline using Amazon MSK.

## Installation

## How To Run

## Usage
Data is retrieved from an external database in `user_posting_emulation.py`, and the function `run_infinite_post_data_loop` is used to emulate data streaming

## Structure
An EC2 instance has Kafka setup, connected to an MSK instance. Three topics `pin`, `geo`, and `user` are created for the three emulated data streams.

