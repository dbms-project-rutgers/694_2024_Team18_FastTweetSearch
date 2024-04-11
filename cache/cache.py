from collections import OrderedDict
from functools import wraps

class LRUCache:
    def __init__(self, capacity: int = 10):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key: str):
        if key not in self.cache:
            value = self.fetch_from_couchbase(key)
            self.put(key, value)
            return value
        value = self.cache.pop(key)
        self.cache[key] = value  # Set key as the newest one
        return value

    def put(self, key: str, value):
        if key in self.cache:
            self.cache.pop(key)
        elif len(self.cache) >= self.capacity:
            self.cache.popitem(last=False)
        self.cache[key] = value

    def fetch_from_couchbase(self, key: str):
        # Placeholder for Couchbase fetching logic
        # TODO: would use Couchbase SDK here to fetch the data.
        # just returning a mock value for flow
        return f"Data for {key} from Couchbase"

def cache_decorator(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        # logic for checking cache before calling the function
        # And updating the cache with the function's result if not present.
        # This is a simple implementation; will adjust based on needs"
        cache_key = f"{function.__name__}_{args}_{kwargs}"
        if cache_key in cache.cache:
            return cache.get(cache_key)
        result = function(*args, **kwargs)
        cache.put(cache_key, result)
        return result
    return wrapper

# Example config
cache = LRUCache(capacity=5)

@cache_decorator
def expensive_function(param):
    # An expensive or I/O bound operation that we want to cache - A DB query in our case
    return f"Computed {param}"

# Example operation
print(expensive_function("test"))
