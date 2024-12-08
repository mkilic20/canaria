import redis

class RedisConnector:
    def __init__(self, host, port, db=0):
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True
        )

    def get_client(self):
        return self.redis_client
