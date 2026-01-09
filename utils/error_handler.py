from utils.logger import Logger


def error_handler(func):
    def try_function(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            Logger().error(f"{func.__name__} con error --> {e}")

    return try_function