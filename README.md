# Project Summary
- Launched an AWS EMR cluster with Spark preinstalled
- Created an ETL pipeline with PySpark
- Created 1 fact and 4 dimension tables stored as parquet files in S3

#### ETL pipeline
The ETL pipeline `etl.py` uses Python (PySpark) and Spark SQL:
- Extracts json files from s3
- Transforms them into PySpark DataFrames
- Loads them back into s3 as parquet files for analytical purposes

#### Running the script

##### ETL-job
- Open a new Ipython notebook and run the command
    - `%run etl.py`

## Data for analytical puropses are loaded back into a separate S3-bucket
#### Purpose
The Parquet files that are loaded back into an S3 bucket are designed for analysing song plays from Sparkify app

#### Schema design
- Fact table 
    - `songplays` are records in log data associated with song plays i.e. records with page NextSong
- Dimension tables
    - `users` are users in the app
    - `songs`  are songs from music metadata database
    - `artists` are artists from music metadata database
    - `time` are timestamps of records in songplays broken down into specific units