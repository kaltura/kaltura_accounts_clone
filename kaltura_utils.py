import time
import coloredlogs, logging
from coloredlogs import ColoredFormatter
from KalturaClient.Base import IKalturaLogger, KalturaConfiguration
from KalturaClient.Plugins.Core import KalturaSessionType
from KalturaClient import KalturaClient
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


def create_custom_logger(logger: logging.Logger, file_path:str = None) -> logging.Logger:
    """
    Configures the specified logger to use a custom handler and formatter.

    This function removes all handlers from the root logger, adds a new StreamHandler to the specified 
    logger, and configures this handler to use the `CustomFormatter`. It also sets the log level of the 
    logger to INFO.

    :param logger: The logger to configure.
    :type logger: logging.Logger
    :param file_path: If passed, the log will be saved into a file at the path indicated in file_path
    :type file_path: str

    :return: The configured logger
    :rtype: logging.Logger
    """

    logging.getLogger().handlers = []
    logger.handlers = []
    if file_path is not None:
        handler = logging.FileHandler(file_path) # save log into a file
    else:
        handler = logging.StreamHandler() # print log to console
    
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
                    logging.critical(f'retrying function due to error: {msg}', extra={'color': 'red'})
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return func(*args, **kwargs)  # retry one final time, if it fails again let the exception bubble up
        return wrapper
    return decorator

class KalturaClientsManager:
    _source_client = None
    _dest_client = None
    _source_user_client = None
    _dest_user_client = None
        
    def __init__(self, should_log, kaltura_user_id, session_duration, session_privileges, source_client_params, dest_client_params):
        self.source_client_params = source_client_params
        self.dest_client_params = dest_client_params
        self.default_session_duration = session_duration
        self.default_session_privileges = session_privileges
        # source account clients (ADMIN and USER) -
        self._source_client = self.get_kaltura_client(should_log, KalturaSessionType.ADMIN, kaltura_user_id,
                                self.source_client_params['service_url'], self.source_client_params['partner_id'], self.source_client_params['partner_secret'],
                                self.default_session_duration, self.default_session_privileges)
        self._source_user_client = self.get_kaltura_client(should_log, KalturaSessionType.USER, kaltura_user_id,
                                self.source_client_params['service_url'], self.source_client_params['partner_id'], self.source_client_params['partner_secret'],
                                self.default_session_duration, self.default_session_privileges)
        # destination account clients (ADMIN and USER) -
        self._dest_client = self.get_kaltura_client(should_log, KalturaSessionType.ADMIN, kaltura_user_id,
                                self.dest_client_params['service_url'], self.dest_client_params['partner_id'], self.dest_client_params['partner_secret'],
                                self.default_session_duration, self.default_session_privileges)
        self._dest_user_client = self.get_kaltura_client(should_log, KalturaSessionType.USER, kaltura_user_id,
                                self.dest_client_params['service_url'], self.dest_client_params['partner_id'], self.dest_client_params['partner_secret'],
                                self.default_session_duration, self.default_session_privileges)
    
    @property
    def source_client(self):
        if self._source_client is None:
            raise ValueError("source_client has not been initialized")
        return self._source_client
    
    @property
    def dest_client(self):
        if self._dest_client is None:
            raise ValueError("dest_client has not been initialized")
        return self._dest_client
    
    def get_source_client_with_user_session(self, user_id, session_duration:int = None, session_privileges:str = None)->KalturaClient:
        """
        Creates a new USER type Kaltura Session and assigns to the source client.

        :param user_id: the KalturaClient to set a new KS on.
        :param session_duration: Seconds this session should live.
        :param session_privileges: Special privileges for this session https://developer.kaltura.com/api-docs/VPaaS-API-Getting-Started/Kaltura_API_Authentication_and_Security.html.
        
        :return: Kaltura client instance connected to the source account with the new KS set to it.
        """
        if session_duration is None:
            session_duration = self.default_session_duration
        if session_privileges is None:
            session_privileges = self.default_session_privileges
        client = self.set_kaltura_client_ks(self._source_user_client, KalturaSessionType.USER, user_id, self.source_client_params['partner_id'], self.source_client_params['partner_secret'], session_duration, session_privileges)
        return client
    
    def get_dest_client_with_user_session(self, user_id, session_duration:int = None, session_privileges:str = None)->KalturaClient:
        """
        Creates a new USER type Kaltura Session and assigns to the destination client.

        :param user_id: the KalturaClient to set a new KS on.
        :param session_duration: Seconds this session should live, if None will use the default.
        :param session_privileges: Special privileges for this session, if None will use the default https://developer.kaltura.com/api-docs/VPaaS-API-Getting-Started/Kaltura_API_Authentication_and_Security.html.
        
        :return: Kaltura client instance connected to the destination account with the new KS set to it.
        """
        if session_duration is None:
            session_duration = self.default_session_duration
        if session_privileges is None:
            session_privileges = self.default_session_privileges
        client = self.set_kaltura_client_ks(self._dest_user_client, KalturaSessionType.USER, user_id, self.dest_client_params['partner_id'], self.dest_client_params['partner_secret'], session_duration, session_privileges)
        return client
        
    def set_kaltura_client_ks(self, client:KalturaClient, type:int, user_id:str, partner_id:int, partner_secret:str, session_duration:int, session_privileges:str)-> KalturaClient:
        """
        Sets a new KS on Kaltura client instance instantiated with a valid Kaltura Session.

        :param client: the KalturaClient to set a new KS on.
        :param type: The KalturaSessionType to use.
        :param user_id: The user ID to associate the KS with.
        :param partner_id: The Kaltura account ID.
        :param partner_secret: The Kaltura account API secret key.
        :param session_duration: Seconds this session should live.
        :param session_privileges: Special privileges for this session https://developer.kaltura.com/api-docs/VPaaS-API-Getting-Started/Kaltura_API_Authentication_and_Security.html.
        
        :return: Kaltura client instance with the new KS set to it.
        """
        ks = client.generateSessionV2(
            partner_secret, 
            user_id, 
            type, 
            partner_id, 
            session_duration, 
            session_privileges
        )
        client.setKs(ks) # type: ignore
        return client
    
    def get_kaltura_client(self, should_log:bool, type:int, user_id:str, service_url:str, partner_id:int, partner_secret:str, session_duration:int, session_privileges:str)-> KalturaClient:
        """
        Get a Kaltura client instance instantiated with a valid Kaltura Session.

        :param should_log: True if this client should log all its calls and responses to a log file kaltura_log.txt.
        :param type: The KalturaSessionType to use.
        :param user_id: The user ID to associate the KS with.
        :param service_url: The Kaltura API service base url.
        :param partner_id: The Kaltura account ID.
        :param partner_secret: The Kaltura account API secret key.
        :param session_duration: Seconds this session should live.
        :param session_privileges: Special privileges for this session https://developer.kaltura.com/api-docs/VPaaS-API-Getting-Started/Kaltura_API_Authentication_and_Security.html.
        
        :return: Kaltura client instance.
        """
        class KalturaLogger(IKalturaLogger):
            def __init__(self):
                with open('kaltura_log.txt', 'w') as f: # clear the contents of the log file before each run
                    pass 
                self.logger = create_custom_logger(logging.getLogger('kaltura_client'), 'kaltura_log.txt')
                
            def log(self, msg):
                self.logger.info(msg)
                
        config = KalturaConfiguration()
        config.serviceUrl = service_url
        if should_log:
            config.setLogger(KalturaLogger())
            
        client = KalturaClient(config, True)
        client = self.set_kaltura_client_ks(client, type, user_id, partner_id, partner_secret, session_duration, session_privileges)
        return client