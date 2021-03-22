# Project Summary
- Launched a new AWS EMR cluster with Spark preinstalled
- Created 1 fact and 4 dimension tables stored as parquet files in s3
- Created an ETL pipeline that 
    - Extracts data from S3
    - Transforms the data into analytics tables using Spark
    - Loads them back into S3 as parquet files

#### ETL pipeline
The ETL pipeline `etl.py` uses Python (PySpark) and Spark SQL:
    - Extracts json files from s3
    - Transforms them into PySpark DataFrames
    - Loads them back into s3 as parquet files for analytical purposes

#### Running the script

##### ETL-job
- Open a new Ipython notebook and run the command
    - `%run etl.py`

# aletje s3-bucket
#### Purpose
This s3-bucket is designed for analysing song plays from Sparkify app

#### Schema design
- Fact table 
    - `songplays` are records in log data associated with song plays i.e. records with page NextSong
- Dimension tables
    - `users` are users in the app
    - `songs`  are songs from music metadata database
    - `artists` are artists from music metadata database
    - `time` are timestamps of records in songplays broken down into specific units