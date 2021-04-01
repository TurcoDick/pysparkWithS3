#### Doing ETL with Pyspark and saving to S3
   
Introduction
Pyspark is an interface for using Apache Spark. Spark is a very fast technology for using parallel computing. It is used to ETL large amounts of data.

The dimension tables are:
- `users`, has the data of each user;
- `songs`, contains the data for each song;
- `artists`, has the data of each artist;
- `time`, has the time data of each song;
- `log_data`, contains the raw data that comes from the application;
- `song_data`, contains the raw data of the songs.


The fact table is:
- `songplays`, contains specific data that is used by analysts to analyze customers' musical preferences.

## Instructions for running the application.
#### Menu:
- ##### `Requirements`
To run the application, you must first have an AWS account, have your access credentials and configure the EMR environment in version 5.20.0, choose the advanced configuration option, choose version 5.20.0, among the technology options that the environment EMR offers choice, spark, livy and hadoop. In the Edit software settings field, place the following configuration:
```
[{'classification':'livy-conf','Properties':{'livy.server.session.timeout':'5h'}}]
```

Then just click next on everything.

Edit software settings
in the dl.cfg file, you need to enter your credentials.


##### Commands to run the application.

##### 1 - create a bucket in the us-west-2 region with the name project-spark-datalake 

##### 2 - Open the terminal and use the command below to run the application:
        python3 etl.py

Observation:
The use of AWS services generates costs in your account and if you run with the complete database it will take more than sixty minutes. I suggest that you reduce the database, to do this:

Go to the main function and change

`input_log_data = "s3://udacity-dend/log_data/*/*/*"` 

`input_song_data = "s3://udacity-dend/song-data/*/*/*/"`

per:

`input_log_data = "s3://udacity-dend/log_data/A/A/*"`

`input_song_data = "s3://udacity-dend/song-data/2018/*/*/"`
