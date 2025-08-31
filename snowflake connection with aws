create or replace database spotify_top_100

create or replace storage integration s3_spotify
type= external_stage 
storage_provider= s3
enabled =true
storage_aws_role_arn='arn:aws:iam::694450007209:role/snowflake-spotify-connection'
storage_allowed_locations=('s3://spotify-project-kothavinaygoud/transformed_data/')

desc integration s3_spotify


// song data
create or replace table song_data(
song_id varchar(60),
song_name varchar(30),
song_duration int,
song_url string,
song_popularity int,
song_added TIMESTAMP_TZ,
album_id string,
artist_id string
)

create or replace file format new_course_db.file_formats.spotify_csv
type=csv
field_delimiter=','
skip_header=1
null_if = ('NULL', 'null')
empty_field_as_null = TRUE;

create or replace schema spotify_stage

create or replace stage spotify_top_100.spotify_stage.csv_stage
url='s3://spotify-project-kothavinaygoud/transformed_data/song_data/'
storage_integration=s3_spotify
file_format=new_course_db.file_formats.spotify_csv

list @spotify_top_100.spotify_stage.csv_stage

copy into  song_data
from @spotify_top_100.spotify_stage.csv_stage
file_format=(type=csv field_delimiter=',' skip_header=1)
ON_ERROR = 'CONTINUE';

select * from song_data


// album data
create or replace table album_data(
album_id string,
album_name string,
album_release_date date,
album_total_tracks int,
album_url string
)

create or replace stage spotify_top_100.spotify_stage.csv_album_stage
url='s3://spotify-project-kothavinaygoud/transformed_data/album_data/'
storage_integration=s3_spotify
file_format=new_course_db.file_formats.spotify_csv

copy into  album_data
from @spotify_top_100.spotify_stage.csv_album_stage
file_format=(type=csv field_delimiter=',' skip_header=1)
ON_ERROR = 'CONTINUE';

select * from album_data;

create or replace table song_data(

)
// artist data
create or replace table artist_data(
artist_id string,
artist_name string,
external_urls string
)

create or replace stage spotify_top_100.spotify_stage.csv_artist_stage
url='s3://spotify-project-kothavinaygoud/transformed_data/artist_data/'
storage_integration=s3_spotify
file_format=new_course_db.file_formats.spotify_csv

copy into  artist_data
from @spotify_top_100.spotify_stage.csv_artist_stage
file_format=(type=csv field_delimiter=',' skip_header=1)
ON_ERROR = 'CONTINUE';

select * from artist_data

// creating snowpipe

create or replace schema spotify_pipe

create or replace pipe spotify_top_100.spotify_pipe.artist_pipe
auto_ingest=true
as 
copy into  artist_data
from @spotify_top_100.spotify_stage.csv_artist_stage

desc pipe spotify_top_100.spotify_pipe.artist_pipe


select * from artist_data
