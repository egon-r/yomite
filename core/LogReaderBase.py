from abc import abstractmethod

from core.AppConfig import AppConfig


class LogReaderBase:
    def __init__(self, config: AppConfig):
        self.config: AppConfig = config

    @abstractmethod
    def read(self, log_path: str):
        pass
