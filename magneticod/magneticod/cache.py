import logging

from redis import StrictRedis as Redis
from lru import LRU


class Cache:
    def __init__(self):
        self.cache = None
        self.hit = 0
        self.miss = 0

    def exists(self, info_hash: bytes):
        raise NotImplementedError

    def put(self, info_hash: bytes):
        raise NotImplementedError

    def ret(self, cache_hit):
        if cache_hit:
            self.hit += 1
        else:
            self.miss +=1

        return cache_hit

    def stats(self):
        return self.hit, self.miss, self.hit/(self.hit+self.miss)*100


class RedisLRUCache(Cache):
    # redis implementation
    def __init__(self):
        super().__init__()
        self.redis = None
        self.connect()

    def connect(self):
        self.redis = Redis()
        logging.info("redis connected")
        try:
            logging.info("redis holds %d infohashes" % self.redis.info()['db0']['keys'])
        except KeyError:
            pass

    def exists(self, info_hash: bytes):
        return self.ret(self.redis.exists(info_hash))

    def put(self, info_hash):
        self.redis.set(info_hash, b'')


class LRUDictCache(Cache):
    # lru-dict implementation
    def __init__(self):
        super().__init__()
        self.cache = LRU(2**18)
        logging.info("using lru-dict for infohash caching")

    def exists(self, info_hash):
        return self.ret(info_hash in self.cache)

    def put(self, info_hash):
        self.cache[info_hash] = b''
