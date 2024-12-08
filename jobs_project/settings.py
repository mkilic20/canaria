import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BOT_NAME = 'jobs_project'

SPIDER_MODULES = ['jobs_project.spiders']
NEWSPIDER_MODULE = 'jobs_project.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure item pipelines
ITEM_PIPELINES = {
    'jobs_project.pipelines.PostgresPipeline': 300,
    'jobs_project.pipelines.RedisPipeline': 400,
    'jobs_project.pipelines.MongoDBPipeline': 500,
}

# Database settings
POSTGRES_SETTINGS = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

REDIS_SETTINGS = {
    'host': os.getenv('REDIS_HOST'),
    'port': int(os.getenv('REDIS_PORT', 6379)),
    'db': int(os.getenv('REDIS_DB', 0))
}

MONGODB_SETTINGS = {
    'host': os.getenv('MONGO_HOST'),
    'port': int(os.getenv('MONGO_PORT', 27017)),
    'username': os.getenv('MONGO_INITDB_ROOT_USERNAME'),
    'password': os.getenv('MONGO_INITDB_ROOT_PASSWORD'),
    'database': os.getenv('MONGO_DB')
}

# Logging settings
LOG_ENABLED = True
LOG_FILE = '/app/logs/spider.log'
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'
