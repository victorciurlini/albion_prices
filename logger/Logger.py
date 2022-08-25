import logging
import sys
import os
from logging import Logger
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime as dt
from pathlib import Path

class etlLogger(Logger): 
    def __init__( 
    self, 
    arq_log=None, 
    project_name="Default", 
    log_formato='{"timestamp": "%(asctime)s", "processo": "%(processName)s", "thread": "%(threadName)s","level": "%(levelname)s", "script": "%(filename)s", "modulo": "%(module)s", "metodo": "%(funcName)s",  "mensagem": "%(message)s"}', 
    *args, 
    **kwargs): 
        self.formatter = logging.Formatter(log_formato) 
        self.path_to_log = self.define_path_log(project_name) 
        self.arq_log = self.path_to_log
        Logger.__init__(self, name=__file__, *args, **kwargs) 
        self.setLevel(logging.DEBUG) 
        self.addHandler(self.get_console_handler()) 
        if self.arq_log: 
            self.addHandler(self.get_file_handler()) 
        
        self.propagate = False
    
    def get_console_handler(self): 
        console_handler = logging.StreamHandler(sys.stdout) 
        console_handler.setFormatter(self.formatter) 
    
        return console_handler
    
    def get_file_handler(self): 
        file_handler = TimedRotatingFileHandler(self.arq_log, when="midnight") 
        file_handler.setFormatter(self.formatter) 
    
        return file_handler
    
    def define_path_log(self, project_name): 
        dir_log = self.verifica_cria_dir_log(project_name) 
        date_file = f"{dt.now().year}{str(dt.now().month).zfill(2)}{str(dt.now().day).zfill(2)}"
        time_file = f"{dt.now().hour}h{dt.now().minute}m{dt.now().second}s"
        extension_file = "log"
        LOG_EXEC_ATUAL = Path(dir_log, 
                              f"{project_name}{date_file}{time_file}.{extension_file}") 
    
        return LOG_EXEC_ATUAL
    
    def verifica_cria_dir_log(self, project_name): 
        path_server = Path("F:/", "logs", project_name) 
        path_local = Path("logs", project_name) 
        path = ""
        try: 
            os.mkdir(path_server) if not Path(path_server).exists() else None
            path = path_server
        except: 
            os.mkdir(path_local) if not path_local.exists() else None
            path = path_local
            return Path(path)