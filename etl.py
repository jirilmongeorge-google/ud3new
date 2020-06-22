import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
"""
Description: This function can be used to load the log files from AWS S3 to Redshift staging tables
Arguments:
    cur: the cursor object. 
    conn: DB connection. 
Returns:
    None
"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

        
def insert_tables(cur, conn):
"""
Description: This function can be used to load the data from Redshift staging tables to the Fact and Dimension tables
Arguments:
    cur: the cursor object. 
    conn: DB connection. 
Returns:
    None
"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
"""
Description: The main function controls the execution of this ETL pipeline. 
This funciton creates a database connection with AWS Redshift, invokes the load_staging_tables fucniton to load data into Redshift staging tables, invokes the insert_tables function to load data from staging tables to fact&dimension tables, 
and also closes the database connection atlast.
Arguments:
     None
Returns:
    None
"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()