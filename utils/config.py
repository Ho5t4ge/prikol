#  Copyright (c) 2010-2024. LUKOIL-Engineering Limited KogalymNIPINeft Branch Office in Tyumen
#  Данным программным кодом владеет Филиал ООО "ЛУКОЙЛ-Инжиниринг" "КогалымНИПИнефть" в г.Тюмени

from datetime import datetime
import warnings as warnings
import logging
import logging.handlers
import os
import tNavigator_python_API as tnav
import yaml
from typing import Optional


class Config:
    db_user: str
    db_pass: str
    db_host: str
    db_port: int
    db_name: str
    use_external_oracle: bool
    external_oracle_path: str
    field_ids: dict
    search_date: datetime
    search_date_str: str
    default_gas_model_value = 0.758
    default_gas_factor_value = 74.4
    in_work_state: str = 'В работе'
    logging_level: str = logging.DEBUG
    t_nav_exe_path: str
    t_nav_project_path: str
    connection: tnav.Connection
    project: tnav.Project

    def __init__(self, config_path: str):
        self.path = config_path
        with open(self.path, encoding='utf-8') as file:
            config_file = yaml.load(file, Loader=yaml.FullLoader)
            self.set_config_parameter('db_user', config_file, str, 'db', 'user')
            self.set_config_parameter('db_pass', config_file, str, 'db', 'pass')
            self.set_config_parameter('db_host', config_file, str, 'db', 'host')
            self.set_config_parameter('db_port', config_file, int, 'db', 'port')
            self.set_config_parameter('db_name', config_file, str, 'db', 'name')
            self.set_config_parameter('use_external_oracle', config_file, bool, 'db', 'dll', 'use_external')
            self.set_config_parameter('external_oracle_path', config_file, str, 'db', 'dll', 'path')
            self.set_config_parameter('field_ids', config_file, dict, 'field_ids')
            self.set_config_parameter('search_date_str', config_file, str, 'search_date_str')
            self.set_config_parameter('t_nav_exe_path', config_file, str, 't_nav_exe_path')
            self.set_config_parameter('t_nav_project_path', config_file, str, 't_nav_project_path')
            self.set_config_parameter('logging_level', config_file, str, 'log', 'level')
            self.set_config_parameter('default_gas_model_value', config_file, float, 'default', 'gas_model_value')
            self.set_config_parameter('default_gas_factor_value', config_file, float, 'default', 'gas_factor_value')
            self.set_config_parameter('logging_level', config_file, str, 'log', 'level')
            warnings.simplefilter('ignore')
        self.config_logging()
        self.config_connection()
        self.config_search_date()

    def config_logging(self):
        current_date = str(datetime.now().date())
        logs_path = os.path.join(os.getcwd(), 'logs')
        if not os.path.exists(logs_path):
            os.mkdir(logs_path)
        handler = logging.handlers.RotatingFileHandler(os.path.join(logs_path, f'{current_date}.log'), mode='a',
                                                       maxBytes=5_000_000, backupCount=1000)
        logger_handlers = [handler, logging.StreamHandler()]
        logging.basicConfig(format='%(asctime)s - %(name)12s %(levelname)-7s %(threadName)12s: %(message)s',
                            handlers=logger_handlers, level=logging.getLevelName(self.logging_level))

    def config_connection(self):
        if self.t_nav_exe_path:
            self.connection = tnav.Connection(self.t_nav_exe_path)
            if self.t_nav_project_path:
                self.project = self.connection.open_project(self.t_nav_project_path, save_on_close=True)

    def config_search_date(self):
        if self.search_date_str:
            self.search_date = datetime.strptime(self.search_date_str, '%Y-%m-%d %H:%M:%S.%f')

    def set_config_parameter(self, config_parameter, config_file: dict, parameter_type: Optional[type],
                             *parameter_names):
        if len(parameter_names) == 0:
            return
        inner_config = config_file
        for parameter_name in parameter_names:
            if not isinstance(inner_config, dict) or parameter_name not in inner_config.keys():
                return
            inner_config = inner_config[parameter_name]
        if parameter_type is not None:
            self.__setattr__(config_parameter, parameter_type(inner_config))
        else:
            self.__setattr__(config_parameter, inner_config)
