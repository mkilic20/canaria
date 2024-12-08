from infra.postgresql_connector import PostgresConnector
from infra.redis_connector import RedisConnector
import json
import pymongo
import time

class PostgresPipeline:
    def __init__(self, postgres_settings):
        self.postgres_settings = postgres_settings
        self.connector = None
        self.items_processed = 0
        
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get('POSTGRES_SETTINGS'))
    
    def open_spider(self, spider):
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                spider.logger.info(f"Attempting PostgreSQL connection (attempt {attempt + 1}/{max_retries})")
                self.connector = PostgresConnector(
                    dbname=self.postgres_settings['database'],
                    user=self.postgres_settings['user'],
                    password=self.postgres_settings['password'],
                    host=self.postgres_settings['host'],
                    port=self.postgres_settings['port']
                )
                
                # Test the connection
                conn = self.connector.get_connection()
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                self.connector.return_connection(conn)
                
                spider.logger.info("PostgreSQL connection successful")
                break
                
            except Exception as e:
                spider.logger.error(f"PostgreSQL connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    spider.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    spider.logger.error("All PostgreSQL connection attempts failed")
                    self.connector = None
        
        # Create table if it doesn't exist
        if self.connector:
            conn = self.connector.get_connection()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    _id VARCHAR(36) PRIMARY KEY,
                    companyName VARCHAR(255),
                    correctDate TIMESTAMP,
                    jobKey VARCHAR(255),
                    jobPageUrl TEXT,
                    annualSalaryAvg DECIMAL(12,2),
                    city VARCHAR(100),
                    zipcode INTEGER
                )
            """)
            conn.commit()
            self.connector.return_connection(conn)
    
    def process_item(self, item, spider):
        if not self.connector:
            spider.logger.warning("PostgreSQL connector not initialized, skipping item")
            return item
            
        conn = None
        try:
            # Get connection and verify it's still valid
            conn = self.connector.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT 1")  # Test connection
            
            # Log item details before insert
            spider.logger.info(f"Attempting to save item with _id: {item.get('_id')}")
            
            cur.execute("""
                INSERT INTO jobs (_id, companyName, correctDate, jobKey, 
                                jobPageUrl, annualSalaryAvg, city, zipcode)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (_id) DO UPDATE SET
                    companyName = EXCLUDED.companyName,
                    correctDate = EXCLUDED.correctDate,
                    jobKey = EXCLUDED.jobKey,
                    jobPageUrl = EXCLUDED.jobPageUrl,
                    annualSalaryAvg = EXCLUDED.annualSalaryAvg,
                    city = EXCLUDED.city,
                    zipcode = EXCLUDED.zipcode
                RETURNING _id
            """, (
                item.get('_id'),
                item.get('companyName'),
                item.get('correctDate'),
                item.get('jobKey'),
                item.get('jobPageUrl'),
                item.get('annualSalaryAvg'),
                item.get('city'),
                item.get('zipcode')
            ))
            
            result = cur.fetchone()
            conn.commit()
            
            self.items_processed += 1
            spider.logger.info(f"Successfully saved item {result[0]}. Total items processed: {self.items_processed}")
            
        except Exception as e:
            if conn:
                conn.rollback()
            spider.logger.error(f"Error inserting item: {e}")
            spider.logger.error(f"Failed item data: {dict(item)}")
        finally:
            if conn:
                cur.close()
                self.connector.return_connection(conn)
        
        return item
    
    def close_spider(self, spider):
        if self.connector:
            spider.logger.info(f"Total items processed by PostgreSQL pipeline: {self.items_processed}")
            self.connector.close_all()

class RedisPipeline:
    def __init__(self, redis_settings):
        self.redis_settings = redis_settings
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get('REDIS_SETTINGS'))
    
    def open_spider(self, spider):
        self.connector = RedisConnector(
            host=self.redis_settings['host'],
            port=self.redis_settings['port'],
            db=self.redis_settings['db']
        )
        self.redis_client = self.connector.get_client()
    
    def process_item(self, item, spider):
        try:
            if not item.get('_id'):
                spider.logger.error("Missing _id field in item")
                return item
                
            # Convert item to dict and ensure all fields are serializable
            item_dict = dict(item)
            
            # Cache item in Redis using job ID as key
            self.redis_client.setex(
                f"job:{item['_id']}", 
                3600,  # Cache for 1 hour
                json.dumps(item_dict)
            )
        except Exception as e:
            spider.logger.error(f"Redis error: {str(e)}")
        return item

class MongoDBPipeline:
    def __init__(self, mongo_settings):
        self.mongo_settings = mongo_settings
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get('MONGODB_SETTINGS'))
    
    def open_spider(self, spider):
        self.client = pymongo.MongoClient(
            host=self.mongo_settings['host'],
            port=self.mongo_settings['port'],
            username=self.mongo_settings['username'],
            password=self.mongo_settings['password']
        )
        self.db = self.client[self.mongo_settings['database']]
        self.collection = self.db['jobs']
    
    def process_item(self, item, spider):
        try:
            if not item.get('_id'):
                spider.logger.error("Missing _id field in item")
                return item
                
            item_dict = dict(item)
            
            if 'zipcode' in item_dict and item_dict['zipcode']:
                try:
                    item_dict['zipcode'] = int(item_dict['zipcode'])
                except (ValueError, TypeError):
                    item_dict['zipcode'] = None
            
            self.collection.update_one(
                {'_id': item_dict['_id']},
                {'$set': item_dict},
                upsert=True
            )
        except Exception as e:
            spider.logger.error(f"MongoDB error: {str(e)}")
        return item
    
    def close_spider(self, spider):
        self.client.close()
