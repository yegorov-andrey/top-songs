# Project Description

## Technologies
  1) Minio S3 - storage for raw data (Data Lake) and transformed data (via Spark)
  2) Spark - load raw data to Minio S3 (ingestion), validate via spark libraries, extract and save primary fields (transformation) [`top-songs`]
  3) Hive - aggregate data and persist in new table in hdfs cluster [`song_count_per_year_below_2000.hql`]
Minio launched within `docker-compose.yaml` (`minio` docker container).
Spark launched within `top-songs` application.
Hive launched within `docker-compose.yaml` (`cloudera` docker container).

## How to Run
 1. run `sbt docker:publishLocal` to build docker image of an application and push it to local registry
 2. run `docker-compose up` to start minio, spark master, spark worker and application docker containers

## Workflow
 1. Spark driver program
   <br>1.1. read all CSV files in s3a://ayegorov/datasets/Top_1_000_Songs_To_Hear_Before_You_Die/
   <br>1.2. process data (parse, validate, clean and transform)
   <br>1.3. save processed data to s3a://ayegorov/data/Top_1_000_Songs_To_Hear_Before_You_Die/
 2. Add new 'batch_id' partition to Hive
 3. Count top songs per year before year 2000 in Hive

## Dataset
Name: Top 1000 Songs To Hear Before You Die
Source: https://opendata.socrata.com/Fun/Top-1-000-Songs-To-Hear-Before-You-Die/ed74-c6ni (included in `top-songs` project)
Format: CSV
Delimiter: comma
Schema:
  - THEME - Plain Text
  - TITLE - Plain Text
  - ARTIST - Plain Text
  - YEAR - Number
  - SPOTIFY_URL - Website URL
Description: contains information about selected songs, including artist, publish ygit push -u origin masterear, theme (category) and optionally audio url on Spotify.

## Data Processing
  1. `resources/Top_1_000_Songs_To_Hear_Before_You_Die.csv` is parsed, cleaned and validated by Spark project.
    <p>1.1. Parsing (done by Spark library): each line is parsed in separate row; each row is split by comma; each value is set to each column; all values initially assumed as nullable strings.
    <p>1.2. Cleansing: YEAR cleaned from non-numeric symbols.
    <p>1.3. Validation: TITLE and ARTIST should not be null; YEAR should be in range `[1000, <current_year>]`.
  2. Data extracted on step 1 are stored to minio S3.
  3. Extracted data are added as new Hive partition.
Also, was added Hive aggregation, as an example, how it may be implemented.be implemented.
