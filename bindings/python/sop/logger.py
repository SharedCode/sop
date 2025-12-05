from enum import Enum
from typing import Optional
from . import call_go

class LogLevel(Enum):
    Debug = 0
    Info = 1
    Warn = 2
    Error = 3

class Logger:
    """
    Logger configuration for the SOP library.
    """
    
    @staticmethod
    def configure(level: LogLevel, log_path: Optional[str] = None):
        """
        Configure the global logger.
        
        Args:
            level (LogLevel): The minimum log level to record.
            log_path (str, optional): Path to the log file. If None or empty, logs to stderr.
        """
        path = log_path if log_path else ""
        call_go.manage_logging(level.value, path)
