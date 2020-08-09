# AWS DataLake


## project into 
Sparkify is a music streaming platform, has grown their user base and song database even more and want to move their data warehouse to a data lake.
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

the Task here is to build an ETL pipeline, extract the data/json files from S3, transform them using spark into dimensional tables then loads them back into S3 

## project Datasets
### Song Dataset

Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by 
the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.  
`song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json`

### Log Dataset 
These simulate app activity logs from an imaginary music streaming app based on configuration settings.
The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.   
`log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json`

## Table Schema 
### * Fact Table 
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### *  Dimension Tables 
1-  users - users in the app user_id, first_name, last_name, gender, level  
2-  songs - songs in music database song_id, title, artist_id, year, duration  
3-  artists - artists in music database artist_id, name, location, lattitude, longitude  
4-  time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

## explanation of the files in the repository
`data.zip` is the data from Sparkify that is placed in S3 for transformation.     
`dl.cfg` configuration and settings file to place aws access key id and aws secret access key.    
`etl.py`  load data from S3 transform then using spark then loads them again into S3 for the analytics team.   

## to run the Python script
1- Add aws credentials  in `dl.cfg`   
2- run in terminal `python etl.py`  





