# Sparkify migration to AWS DataLake

## 1) Goal
### The purpose of this project is to read Sparkify data from amazon S3 process it on spark without any need to create a database and write them back to S3


## 2) Design
### a) The work plan was to first load the data from S3 into spark, using schema-on-read , we don't need to create a database or tables, which is really convenient
### b) Then we select columns of interest to form our tables which consist of one fact table (song_plays), and many dimension tables each focusing on a specific entity ( users, songs, time, etc.)
### c) Last thing here is to write these new tables in parquet form and send them back to S3


## 3) Files used in the project
###   a) dataLake.ipynb : This file contains all development steps on a small subset of the dataset for quick debugging and trial of code 
###   b) dl.cfg : This file contains parameters used for our IAM role to access S3 bucket, we load it in our script using [ConfigParser](https://docs.python.org/3/library/configparser.html) 
###   c) etl.py : This script automates the ETL process, then the data processing, then writing it back to S3, it should be run from the terminal 
###   d) data : This folder contains the small subset of data used in dataLake.ipynb
###   e) data_parquet : This folder contains the written parquet tables form the dataLake.ipynb


## 4) Conclusion
### migrating to Data Lakes is expected to be a good move for sparkify as it is easier to work with data using powerful tools like spark, but most importantly it opens the door for other opportunities to benefit from our data like using ML to have better insights and predictions for specific users and so on
