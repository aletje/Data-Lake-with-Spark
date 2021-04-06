## Sample project building a Data Lake with Spark
- Launched an AWS EMR cluster with Spark preinstalled
- Created an ETL pipeline with PySpark
- Created 1 fact and 4 dimension tables loaded back into S3 as parquet files. <br> <br>
The `Song metadata` is a subset originally from `http://millionsongdataset.com/`. <br>
The `Log data` set is simulating user activity on a fictional music streaming app called 'Sparkify'. <br> 
Both datasets resides in Udacity's S3 bucket. 

#### ETL pipeline
The ETL pipeline `etl.py` uses Python (PySpark) and Spark SQL:
- Extracts json files from s3
- Transforms them into PySpark DataFrames
- Loads them back into s3 as parquet files for analytical purposes

##### Running the ETL-job
- Open a new Ipython notebook and run the command
    - `%run etl.py`

#### Data for analytical puropses are loaded back into a separate S3-bucket
The Parquet files that are loaded back into an S3 bucket are designed for analysing song plays from Sparkify app

#### Schema design
- Fact table 
    - `songplays` are records in log data associated with song plays i.e. records with page NextSong
- Dimension tables
    - `users` are users in the app
    - `songs`  are songs from song metadata database
    - `artists` are artists from song metadata database
    - `time` are timestamps of records in songplays broken down into specific units