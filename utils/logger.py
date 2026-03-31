import logging
import sys


class Logger:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialize_logger()
        return cls._instance

    def _initialize_logger(self):
        self.logger = logging.getLogger("BI_Logger")
        self.logger.setLevel(logging.INFO)

        # Prevent adding multiple handlers if instantiated multiple times.
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                fmt="%(asctime)s %(levelname)s %(message)s",
                datefmt="%m/%d/%Y %I:%M:%S %p"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def info(self, message: str, *args):
        self.logger.info(message, *args)

    def error(self, message: str, *args):
        self.logger.error(message, *args)

    def warning(self, message: str, *args):
        self.logger.warning(message, *args)

    def exception(self, message: str, *args):
        self.logger.exception(message, *args)
