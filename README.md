# Project 3 - Data Warehouse - Purpose
As we know, Sparkify has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Database schema design
The database design is based on star schema concept. songplays table is the FACT table which connects with the dimension tables songs, users, artists and time. 

# ETL pipeline
The ETL pipeline implemented in the etl.py will make sure the data transformaiton from S3 JSON files to Redshift staging tables become easier and makes it easy to debug as well. The ETL pipeline has 2 steps.
- Step 1 - Copy data from S3 to Redshift staging tables(staging_events and staging_songs)
- Step 2 - Copy data from Redshift staging tables to the other Redshift fact and dimension tables(songplays, songs, artists, users and time)

# Project Files
- dwh.cfg - Project configuration file containing Redshift cluster details and S3 log file location details.
- sql_queries.py - contains the SQL queries for creating the Redshift tables and also INSERT statements
- create_tables.py - this file reads the sql_queries.py and takes care of creating the Redshift tables. You can invoke it by going to the terminal and use the command 'python create_tables.py'
- etl.py - The ETL pipeline to transform the JSON files present in S3 to the Redshift Staging tables and then copy data from thise staging tables to the fact and dimension tables in Redshift. You can invoke it by going to the terminal and use the command 'python etl.py'

# Example queries and results for song play analysis.
- Analyze songplays table records using Redshift Query editor
select * from public."songplays" limit 10;
