import logging
import pathlib

from core.AppConfig import AppConfig
from core.FileWatcher import FileWatcher


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    app_config = AppConfig()
    app_config.read_config(pathlib.Path("./config.json"))

    watcher = FileWatcher(app_config)
    watcher.start()
