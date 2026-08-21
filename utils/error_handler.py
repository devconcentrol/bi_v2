from functools import wraps

from utils.logger import Logger


def error_handler(func):
    @wraps(func)
    def try_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            Logger().exception(f"{func.__name__} failed: {e}")
            raise

    return try_function
