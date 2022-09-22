from __future__ import annotations

import json
import logging
import pathlib


class AppConfig:
    def __init__(self):
        self.files_to_watch: dict[str, str] = {
            "/var/log/nginx/access.log": "NginxLogReader",
        }
        self.influx_url: str = "http://localhost:8086"
        self.influx_token = "influx_token"
        self.influx_bucket = "yomite"
        self.influx_org = "main_org"
        self.influx_nginx_measurement = "nginx_data"
        self.file_watcher_interval_s = 300

    def read_config(self, config_path: pathlib.Path):
        if config_path.exists():
            config_data = json.load(config_path.open())
            self.__dict__.update(config_data)
        else:
            logging.info(f"Config '{config_path}' not found, creating default configuration and exiting...")
            self.write_config(config_path)
            exit(1)

    def write_config(self, config_path: pathlib.Path):
        json.dump(self.__dict__, config_path.open("w+"), sort_keys=True, indent=4)
