import pandas as pd
import psycopg2
import pymongo
from datetime import datetime
import os
from sqlalchemy import create_engine

class DatabaseQuery:
    def __init__(self, postgres_settings, mongodb_settings=None):
        self.postgres_settings = postgres_settings
        self.mongodb_settings = mongodb_settings
        self.pg_conn = None
        self.mongo_client = None
        
        # Initialize connections
        self.connect_postgres()
        if mongodb_settings:
            self.connect_mongodb()
    
    def connect_postgres(self):
        try:
            self.pg_conn = psycopg2.connect(
                dbname=self.postgres_settings['database'],
                user=self.postgres_settings['user'],
                password=self.postgres_settings['password'],
                host=self.postgres_settings['host'],
                port=self.postgres_settings['port']
            )
            print("PostgreSQL connection successful")
        except Exception as e:
            print(f"PostgreSQL connection error: {str(e)}")
            raise
    
    def connect_mongodb(self):
        if not self.mongodb_settings:
            print("No MongoDB settings provided")
            return
            
        try:
            self.mongo_client = pymongo.MongoClient(
                host=self.mongodb_settings['host'],
                port=self.mongodb_settings['port'],
                username=self.mongodb_settings['username'],
                password=self.mongodb_settings['password']
            )
            # Test the connection
            self.mongo_client.admin.command('ping')
            print("MongoDB connection successful")
        except Exception as e:
            print(f"MongoDB connection error: {str(e)}")
            self.mongo_client = None
    
    def get_postgres_data(self):
        if not self.pg_conn:
            self.connect_postgres()
            
        # Create SQLAlchemy engine from existing connection
        engine = create_engine(
            f'postgresql://{self.postgres_settings["user"]}:{self.postgres_settings["password"]}@'
            f'{self.postgres_settings["host"]}:{self.postgres_settings["port"]}/'
            f'{self.postgres_settings["database"]}'
        )
        
        query = """
            SELECT 
                _id,
                companyName,
                correctDate,
                jobKey,
                jobPageUrl,
                annualSalaryAvg,
                city,
                zipcode
            FROM jobs
            ORDER BY correctDate DESC
        """
        
        return pd.read_sql_query(query, engine)
    
    def get_mongodb_data(self):
        if not self.mongo_client:
            self.connect_mongodb()
        
        if not self.mongo_client:
            print("No MongoDB connection available")
            return None
            
        try:
            db = self.mongo_client[self.mongodb_settings['database']]
            collection = db['jobs']
            
            cursor = collection.find({})
            data = list(cursor)
            
            print(f"Found {len(data)} documents in MongoDB")
            
            if data:
                return pd.DataFrame(data)
            print("No data found in MongoDB")
            return None
            
        except Exception as e:
            print(f"Error retrieving MongoDB data: {str(e)}")
            return None
    
    def export_to_csv(self, output_dir='exports'):
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Create timestamp for filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Get PostgreSQL data
        pg_df = self.get_postgres_data()
        if not pg_df.empty:
            pg_filename = f"{output_dir}/postgres_jobs_{timestamp}.csv"
            pg_df.to_csv(pg_filename, index=False)
            print(f"PostgreSQL data exported to {pg_filename}")
            print(f"Total PostgreSQL records: {len(pg_df)}")
        
        # Get MongoDB data if available
        mongo_df = self.get_mongodb_data()
        if mongo_df is not None and not mongo_df.empty:
            # Keep the _id field as it matches PostgreSQL's primary key
            mongo_filename = f"{output_dir}/mongodb_jobs_{timestamp}.csv"
            mongo_df.to_csv(mongo_filename, index=False)
            print(f"MongoDB data exported to {mongo_filename}")
            print(f"Total MongoDB records: {len(mongo_df)}")
    
    def close_connections(self):
        if self.pg_conn:
            self.pg_conn.close()
            print("PostgreSQL connection closed")
            
        if self.mongo_client:
            self.mongo_client.close()
            print("MongoDB connection closed")

if __name__ == "__main__":
    # Settings from settings.py
    postgres_settings = {
        'host': 'postgres',
        'port': 5432,
        'database': 'jobs_db',
        'user': 'user',
        'password': 'password'
    }
    
    mongodb_settings = {
        'host': 'mongodb',
        'port': 27017,
        'username': 'root',
        'password': 'example',
        'database': 'jobs_db'
    }
    
    try:
        # Initialize query object
        db_query = DatabaseQuery(postgres_settings, mongodb_settings)
        
        # Export data to CSV
        db_query.export_to_csv()
        
    finally:
        # Ensure connections are closed
        db_query.close_connections()
