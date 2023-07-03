import time
import coloredlogs, logging
from coloredlogs import ColoredFormatter

class CustomFormatter(ColoredFormatter):
    """
    A custom log message formatter that supports colorizing messages.

    This class extends `ColoredFormatter` from the `coloredlogs` package, and overrides the `format` 
    method to add support for a `color` attribute on log records.

    This `color` attribute can be added to a log record to specify the color in which the level name 
    and message of that log record should be printed.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the specified log record.

        If the record has a `color` attribute, this method colorizes the level name and message of the 
        log record with that color. Otherwise, it defaults to white.

        :param record: The log record to format.
        :type record: logging.LogRecord

        :return: The formatted log record.
        :rtype: str
        """

        color = 'white'
        if hasattr(record, 'color'):
            color = record.color.lower()

        record.levelname = coloredlogs.ansi_wrap(record.levelname, color=color)
        record.msg = coloredlogs.ansi_wrap(record.msg, color=color)
        
        return super(CustomFormatter, self).format(record)


def create_custom_logger(logger: logging.Logger) -> None:
    """
    Configures the specified logger to use a custom handler and formatter.

    This function removes all handlers from the root logger, adds a new StreamHandler to the specified 
    logger, and configures this handler to use the `CustomFormatter`. It also sets the log level of the 
    logger to INFO.

    :param logger: The logger to configure.
    :type logger: logging.Logger

    :return: None
    """

    logging.getLogger().handlers = []
    handler = logging.StreamHandler()
    handler.setFormatter(CustomFormatter('%(levelname)s:%(name)s:%(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def retry_on_exception(max_retries=3, delay=1, backoff=2, exceptions=(Exception,)):
    """
    A decorator for retrying a function call with a specified delay in case of a specific set of exceptions

    Args:
        max_retries (int): The maximum number of retries before giving up. Default is 3.
        delay (int/float): The initial delay between retries in seconds. Default is 1.
        backoff (int): The multiplier applied to delay between retries. Default is 2.
        exceptions (tuple): A tuple of exceptions on which to retry. Default is (Exception,), i.e., all exceptions.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            mtries, mdelay = max_retries, delay
            while mtries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as error:
                    msg = f"{str(error)}, Retrying in {mdelay} seconds..."
                    logging.warning(msg, extra={'color': 'magenta'})
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return func(*args, **kwargs)  # retry one final time, if it fails again let the exception bubble up
        return wrapper
    return decorator