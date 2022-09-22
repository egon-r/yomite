import importlib
import logging
import pathlib
import threading
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent

from core import AppConfig


class FileWatcher(FileSystemEventHandler):
    """
    Watches files for changes, triggers the read() method of LogReader instances on change and periodically.
    """

    def __init__(self, app_config: AppConfig):
        self.app_config: AppConfig = app_config
        self.running = False
        self.observer = Observer()
        self.reader_classes = {}
        self._init_reader_classes()
        self.file_handler_instances = {}

    def start(self):
        self._start_watcher_thread()

    def __del__(self):
        self.running = False
        self.observer.stop()

    def _init_reader_classes(self):
        for reader_class in set(self.app_config.files_to_watch.values()):
            try:
                readers_module = importlib.import_module(f"readers.{reader_class}")
                class_ref = getattr(readers_module, reader_class)
                self.reader_classes[reader_class] = class_ref
                logging.info(f"{reader_class} -> {self.reader_classes[reader_class]}")
            except:
                logging.warning(f"cannot find class 'readers.{reader_class}.{reader_class}'!")

    def _start_watcher_thread(self):
        self.running = True
        self._watcher_thread = threading.Thread(
            daemon=False,
            target=self._watcher_thread_target
        )
        self._watcher_thread.start()

    def _watcher_thread_target(self):
        self.observer.start()
        while self.running:
            for f in self.app_config.files_to_watch:
                if pathlib.Path(f).exists():
                    self.observer.schedule(self, f)
                    self._read_file(f)
                else:
                    logging.info(f"Failed watching '{f}' because it doesn't exist! Will try again later.")
            time.sleep(self.app_config.file_watcher_interval_s)

    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent):
            self._read_file(event.src_path)

    def _read_file(self, file_path: str):
        if file_path in self.app_config.files_to_watch:
            try:
                reader_class = self.app_config.files_to_watch[file_path]
                if reader_class in self.reader_classes:
                    if file_path not in self.file_handler_instances:
                        logging.info(f"Creating new LogReader for '{file_path}'")
                        self.file_handler_instances[file_path] = self.reader_classes[reader_class](self.app_config)
                    self.file_handler_instances[file_path].read(file_path)
            except Exception as ex:
                logging.warning(ex)
