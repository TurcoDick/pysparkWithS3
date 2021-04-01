import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
import pyspark.sql.functions as psf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col


config = configparser.ConfigParser()
config.read('dl.cfg')

key = config['AWS']['AWS_ACCESS_KEY_ID']
secret = config['AWS']['AWS_SECRET_ACCESS_KEY']

print('AWS_ACCESS_KEY_ID={} AWS_SECRET_ACCESS_KEY={}'.format(key, secret))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    spark configuration.
    """
    print("Entrou em create_spark_session")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    return spark


#Scheme of the song raw data table.
schema_song_data = StructType([\
                         StructField("num_songs", StringType(), False),\
                         StructField("artist_id", StringType(),  True),\
                         StructField("artist_latitude", StringType(), False),\
                         StructField("artist_longitude", StringType(), False),\
                         StructField("artist_location", StringType(), False),\
                         StructField("artist_name", StringType(), False),\
                         StructField("song_id", StringType(), False),\
                         StructField("title", StringType(), False),\
                         StructField("duration", StringType(), False),\
                         StructField("year", StringType(), False) \
                         ])

#Scheme of the log raw data table.
schema_log_data = StructType([\
                        StructField("artist", StringType(), False), \
                        StructField("auth", StringType(), False), \
                        StructField("firstName", StringType(), False), \
                        StructField("gender", StringType(), False), \
                        StructField("itemInSession", StringType(), False), \
                        StructField("lastName", StringType(), False), \
                        StructField("length", StringType(), False), \
                        StructField("level", StringType(), False), \
                        StructField("location", StringType(), False), \
                        StructField("method", StringType(), False), \
                        StructField("page", StringType(), False), \
                        StructField("registration", StringType(), False), \
                        StructField("sessionId", StringType(), False), \
                        StructField("song", StringType(), False), \
                        StructField("status", StringType(), False), \
                        StructField("ts", StringType(), False), \
                        StructField("userAgent", StringType(), False), \
                        StructField("userId", StringType(), True) \
                        ])

def get_song_table(df_song_data):
    """
    extract columns to create songs table
    @param df_song_data = raw song data dataframe.
    """
    df_song = df_song_data.dropDuplicates((['song_id'])).select("song_id", "title", "artist_id", "year", "duration")
    
    return df_song.withColumn("year", col("year").cast(IntegerType()))\
                  .withColumn("duration", col("duration").cast(DoubleType()))\
                  .repartition("year", "artist_id")

def get_artists(df_song_data):
    """
    creating the artist table.
    @param df_song_data = dataframe with raw song data.
    """
    return df_song_data\
        .dropDuplicates((['artist_id']))\
        .select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")\
        .withColumnRenamed("artist_name", "name")\
        .withColumnRenamed("artist_location", "location")\
        .withColumnRenamed("artist_latitude", "latitude")\
        .withColumnRenamed("artist_longitude", "longitude")\
        .repartition("artist_id")



def process_song_data(spark, input_song_data, output_data):
    """
    processing song data.
    @param spark = spark context
    @param input_data = Address of the original data.
    @param output_data = Address where the treated tables are to be saved.
    """
    print("entrou em process_song_data")
    # get filepath to song data file
    song_data = input_song_data
    
    # read song data file  //
    df_song_data = spark\
    .read\
    .format('org.apache.spark.sql.json')\
    .option('header', True)\
    .schema(schema_song_data)\
    .load(song_data)\
    .cache()
        
    # extract columns to create songs table
    songs_table = get_song_table(df_song_data)
    
    # extract columns to create artists table
    artists_table = get_artists(df_song_data)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs/")
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists/") 
    
    spark.catalog.clearCache()
    

    
def convert_time(string, format_time):
    """
    convert the string ts to timestamp, date, day, hour, month and year.
    @param string = string ts
    @param format_time = new time format that the ts will be converted.
    """
    dt = datetime.fromtimestamp(int(string, 16) // 1000000000)
    return dt.strftime(format_time)


def process_log_data(spark, input_song_data, input_log_data ,output_data):
    """
    processing raw log data
    @param spark = spark context
    @param input_data = Address of the original data.
    @param output_data = Address where the treated tables are to be saved.
    """
    
    # get filepath to log data file
    log_data = input_log_data
    song_data = input_song_data
    
    # read log data file
    df_log_data = spark\
    .read\
    .format('org.apache.spark.sql.json')\
    .option('header', True)\
    .schema(schema_log_data)\
    .load(log_data).cache()
        
    # filter by actions for song plays
    df_log_data_filter = df_log_data.filter(df_log_data.page == "NextSong").cache()

    # extract columns for users table    
    users_table =  df_log_data_filter\
                                    .dropDuplicates((["userId"]))\
                                    .select("userId", "firstName", "lastName", "gender", "level")\
                                    .withColumnRenamed("userId", "user_id")\
                                    .withColumnRenamed("firstName", "first_name")\
                                    .withColumnRenamed("lastName", "last_name")
        

    # create timestamp column from original timestamp column
    get_timestamp     = udf(lambda item : convert_time(item, '%Y-%m-%d %H:%M:%S'))
    get_start_time    = udf(lambda item : convert_time(item, '%H:%M:%S'))
    get_hour          = udf(lambda item : convert_time(item, '%H'))
    get_day           = udf(lambda item : convert_time(item, '%d'))
    get_month         = udf(lambda item : convert_time(item, '%m'))
    get_year          = udf(lambda item : convert_time(item, '%Y'))
    
    
    df = df_log_data_filter\
    .withColumn("timestamp", get_start_time(col("ts")))\
    .withColumn("start_time", get_start_time(col("ts")))\
    .withColumn("hour", get_hour(col("ts")))\
    .withColumn("day", get_day(col("ts")))\
    .withColumn("month", get_month(col("ts")))\
    .withColumn("year", get_year(col("ts")))\
    .withColumn("week", date_format('timestamp', 'u'))\
    .withColumn("weekday", date_format('timestamp' , 'E'))\
    .repartition("year")
    
    # extract columns to create time table
    time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
   

    # read in song data to use for songplays table
    song_df = spark\
    .read\
    .format('org.apache.spark.sql.json')\
    .option('header', True)\
    .schema(schema_song_data)\
    .load(song_data)\
    .cache()
        

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_log_data.join(song_df, df_log_data.length == song_df.duration)\
                                 .filter(\
                                         (df_log_data.song == song_df.title)\
                                         & (df_log_data.artist == song_df.artist_name)\
                                         & (df_log_data.page == "NextSong")\
                                        )\
                                 .withColumn("songplay_id", psf.monotonically_increasing_id() + 10)\
                                 .withColumnRenamed("userId", "user_id")\
                                 .withColumn("start_time", get_start_time(col("ts")))\
                                 .withColumn("year", get_year(col("ts")))\
                                 .withColumn("month", get_month(col("ts")))\
                                 .withColumnRenamed("sessionId", "session_id")\
                                 .withColumnRenamed("userAgent", "user_agent")\
                                 .select("start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "year", "month")
                                 
                                

    # write users table to parquet files
    users_table.write.mode('overwrite').partitionBy("gender").parquet(output_data + "users/")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time/")
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table\
    .write\
    .mode('overwrite')\
    .partitionBy("year", "month")\
    .parquet(output_data + "songplays/")
    
    spark.catalog.clearCache()
           
   
 
def main():
    spark = create_spark_session()
    input_log_data = "s3://udacity-dend/log_data/*/*/*"
    input_song_data = "s3://udacity-dend/song-data/*/*/*/"
    output_data = 's3://project-spark-datalake/'
    
    process_song_data(spark, input_song_data, output_data) 
    
    process_log_data(spark, input_log_data, input_song_data, output_data)


    if __name__ == "__main__":
            main()