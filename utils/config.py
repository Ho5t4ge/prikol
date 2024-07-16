from datetime import datetime
import warnings as warnings
import logging
import logging.handlers
import os

field_ids = {'NE': 86, 'MI': 78}
search_date = datetime.strptime('2024-05-01 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f')
warnings.simplefilter('ignore')
default_gas_model_value = 0.758
default_gas_factor_value = 74.4
in_work_state = 'В работе'
logging_level = logging.DEBUG


def config_logging():
    current_date = str(datetime.now().date())
    logs_path = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(logs_path):
        os.mkdir(logs_path)
    handler = logging.handlers.RotatingFileHandler(os.path.join(logs_path, f'{current_date}.log'), mode='a',
                                                   maxBytes=5_000_000, backupCount=1000)
    logger_handlers = [handler, logging.StreamHandler()]
    logging.basicConfig(format='%(asctime)s - %(name)12s %(levelname)-7s %(threadName)12s: %(message)s',
                        handlers=logger_handlers, level=logging.getLevelName(logging_level))
