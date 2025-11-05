import os
from logging.config import dictConfig
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL

class LogSetup(object):
    @staticmethod
    def log_msg(logger, msg, level=INFO, print_=True):
        if print_:
            print(msg)
        if level == DEBUG:
            logger.debug(msg)
        elif level == INFO:
            logger.info(msg)
        elif level == WARNING:
            logger.warning(msg)
        elif level == ERROR:
            logger.error(msg)
        elif level == CRITICAL:
            logger.critical(msg)

    def _make_dir(self, log_dir):
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.log_level = 'INFO'
        self.log_type = 'rotating'
        self.err_log_name = 'err.log'
        self.producer_log_name = 'producer.log'
        self.device_log_name = 'device.log'
        self.log_max_bytes = 100_000_000
        self.log_copies = 5

        self._make_dir(self.log_dir)
        self.err_log = '/'.join([self.log_dir, self.err_log_name])
        self.producer_log = '/'.join([self.log_dir, self.producer_log_name])
        self.device_log = '/'.join([self.log_dir, self.device_log_name])
        self.log_policy = 'logging.handlers.RotatingFileHandler'

        log_config = {
            'version':1,
            'formatters':self.get_std_format().get('formatters'),
            'loggers':self.get_std_logger().get('loggers'),
            'handlers':self.get_logging_handler().get('handlers')
        }
        dictConfig(log_config)

    def get_std_format(self):
        self.std_format = {
            'formatters': {
                'default': {
                    'format': '[%(asctime)s] %(levelname)s: %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S',
                },
                'error': {
                    'format': '[%(asctime)s] %(levelname)s %(name)s:%(funcName)s: %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S',
                },
            }
        }

        return self.std_format
    def get_std_logger(self):
        self.std_logger = {
            'loggers': {
                '': {
                    'level': self.log_level, 
                    'handlers': ['default'], 
                    'propagate': True
                },
                'app.error': {
                    'level': self.log_level,
                    'handlers': ['err_log'],
                    'propagate': False,
                },
                'app.device': {
                    'level': self.log_level,
                    'handlers': ['device_log'],
                    'propagate': False,
                },
                'app.producer': {
                    'level': self.log_level,
                    'handlers': ['producer_log'],
                    'propagate': False,
                },
                'root': {
                    'level': self.log_level, 
                    'handlers': ['default']},
            }
        }

        return self.std_logger
    
    def get_logging_handler(self):

        self.logging_handler = {
            'handlers':{
                'default':{
                    'level':self.log_level,
                    'class':self.log_policy,
                    'filename':self.producer_log,
                    'backupCount': self.log_copies,
                    'maxBytes': self.log_max_bytes,
                    'formatter': 'default',
                    'delay': True,
                },
                'err_log':{
                    'level':self.log_level,
                    'class':self.log_policy,
                    'filename':self.err_log,
                    'backupCount': self.log_copies,
                    'maxBytes': self.log_max_bytes,
                    'formatter': 'error',
                    'delay': True,
                },
                'device_log':{
                    'level':self.log_level,
                    'class':self.log_policy,
                    'filename':self.device_log,
                    'backupCount': self.log_copies,
                    'maxBytes': self.log_max_bytes,
                    'formatter': 'error',
                    'delay': True,
                },
                'producer_log':{
                    'level':self.log_level,
                    'class':self.log_policy,
                    'filename':self.producer_log,
                    'backupCount': self.log_copies,
                    'maxBytes': self.log_max_bytes,
                    'formatter': 'default',
                    'delay': True,
                },
                
            }
        }

        return self.logging_handler