# Seismic Streaming

A simple end-to-end Kafka streaming application. This consists of three elements:

1. A producer that sends EU seismic data to a Kafka topic.
2. A consumer that stores Kafka log entries into a flat CSV file.
3. A Dash/Plotly display interface that maps recent seismic data.

This application assumes you have a Kafka endpoint ready. To run Kafka locally using
Docker, see [this repository](https://github.com/uvasds-systems/learn-kafka).

## Run this Solution

To run this solution, there are three pre-requisites:

1. Start your local Kafka cluster using Docker.
2. Create/start your Python virtual environment.
3. Install all Python dependencies.

Next, you will need to run three applications simultaneously. You can either

- Run three terminal windows, each with the same virtual environment activated; OR
- Run this single command, which executes three scripts together (recommended):

```
python display.py & python producer.py & python consumer.py
```

## Enhancements for Production

The wise student will quickly realize there are numerous issues with running this application
at scale. A smarter approach would not use a single text file as "data storage" but would 
instead use a database (Postgres,MySQL,Mongo) fronted by an API for both data retrieval AND
data submission by producers and consumers.

Then, GUI applications like the Plotly/Dash application here could scale out to handle more
traffic, and users would not be interfacing directly with your database.

