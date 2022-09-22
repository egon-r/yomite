import datetime
import logging
import re
from dataclasses import dataclass
from typing import Optional

from influxdb_client import Point, WritePrecision, InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from core.AppConfig import AppConfig
from core.LogReaderBase import LogReaderBase


@dataclass
class NginxDataFields:
    """
    Holds the fields (frequently changing data) of an influx data point
    """
    time: datetime.datetime = datetime.datetime.utcfromtimestamp(0)
    bytes_recv: int = -1
    bytes_sent: int = -1
    requests: int = -1

    def is_complete(self):
        return not (-1 in self.__dict__.values() and self.time == datetime.datetime.utcfromtimestamp(0))


class NginxLogReader(LogReaderBase):
    """
    Reads nginx access_log in the specified format and writes the result to influxdb.

    /etc/nginx/nginx.conf
        log_format custom '$remote_addr [$msec] '
                           '"$request" $status $bytes_sent $request_length '
                           '"$http_referer" "$http_user_agent" '
                           '"$geoip2_data_country_name" "$geoip2_data_city_name" '
                           '"$geoip2_data_city_lat, $geoip2_data_city_long"';
        access_log /var/log/nginx/access.log custom;
    """

    def __init__(self, config: AppConfig):
        super().__init__(config)
        self.re_pattern = r'(.*) \[([\d.]+)\] "(.*)" (\d*) (\d*) (\d*) "(.*)" "(.*)" "(.*)" "(.*)" "(.*)"'
        self.date_format = "%d/%b/%Y:%H:%M:%S %z"
        self.last_read_line_time: datetime.datetime = datetime.datetime.utcfromtimestamp(0)
        self.influx_client: InfluxDBClient
        self._create_influx_client()

    def __del__(self):
        self.influx_client.close()

    def _create_influx_client(self):
        self.influx_client = InfluxDBClient(
            url=self.config.influx_url, token=self.config.influx_token, org=self.config.influx_org
        )
        self.influx_write = self.influx_client.write_api(write_options=SYNCHRONOUS)
        self.influx_query = self.influx_client.query_api()

    def read(self, log_path: str):
        with open(log_path, "r") as f:
            lines_read = 0
            lines = 0
            for line in f:
                lines += 1
                match = re.match(self.re_pattern, line)
                if match:
                    if self._read_re_match(match):
                        lines_read += 1
                else:
                    logging.warning(f"[NginxLogReader]: line '{lines}' in '{log_path}' didn't match! check nginx access_log format!")
            logging.info(f"[NginxLogReader]: {lines_read}/{lines} new lines")
            logging.info(f"[NginxLogReader]: last line timestamp {self.last_read_line_time}")

    def _fetch_latest_data(self, city: str, country: str, client: str, route: str) -> Optional[NginxDataFields]:
        query = f"""from(bucket: "yomite")
        |> range(start: -30d, stop: now())
        |> filter(fn: (r) => r["_measurement"] == "{self.config.influx_nginx_measurement}")
        |> filter(fn: (r) => r["country"] == "{country}")
        |> filter(fn: (r) => r["city"] == "{city}")
        |> filter(fn: (r) => r["client"] == "{client}")
        |> filter(fn: (r) => r["route"] == "{route}")
        |> last()"""
        try:
            result = NginxDataFields()
            tables = self.influx_query.query(query, org=self.config.influx_org)
            for table in tables:
                for record in table.records:
                    result.time = record["_time"]
                    if record["_field"] == "bytes_recv":
                        result.bytes_recv = record["_value"]
                    elif record["_field"] == "bytes_sent":
                        result.bytes_sent = record["_value"]
                    elif record["_field"] == "requests":
                        result.requests = record["_value"]
            result.time = result.time.astimezone(datetime.timezone.utc)
            result.time = result.time.replace(tzinfo=None)
            if result.is_complete():
                return result
            else:
                logging.error(f"got incomplete result: {result}")
                return None
        except Exception as ex:
            logging.error(f"error reading from database: {ex}")
            return None

    def _read_re_match(self, match: re.Match) -> bool:
        line_time = datetime.datetime.utcfromtimestamp(float(match.group(2)))
        if line_time <= self.last_read_line_time:
            return False
        else:
            self.last_read_line_time = line_time

        # 1 - 90.146.155.214
        # 2 - 1662852430.376
        # 3 - GET /favicon.ico HTTP/1.1
        # 4 status - 502
        # 5 bytes to client - 845
        # 6 bytes from client - 845
        # 7 - https://egonr.dev/test%221!$&!%22%C2%A7
        # 8 - Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36
        # 9 - Country
        # 10 - City
        # 11 - 12.34567, 76.54321
        client_ip = match.group(1)
        client_request = match.group(3)
        client_request_status = match.group(4)
        client_request_url = "None"
        try:
            # only successful requests are recognized as a valid url to reduce data cardinality
            if client_request_status in ["200", "204", "304"]:
                client_request_url = client_request.split(" ")[1]
            else:
                logging.info("'None' Request (Status: " + client_request_status + "): " + client_request.split(" ")[1])
        except Exception as ex:
            logging.info(f"error extracting url from client_request: {client_request}")
        lat_long = match.group(11).split(", ")
        lat = float(lat_long[0])
        long = float(lat_long[1])
        country = match.group(9)
        city = match.group(10)
        bytes_to_client = int(match.group(5))
        bytes_from_client = int(match.group(6))

        latest_data = self._fetch_latest_data(city, country, client_ip, client_request_url)
        if latest_data is None:
            logging.info(f"New client '{client_ip}' from {city}, {country} requested '{client_request_url}'")
            new_location_data = Point(self.config.influx_nginx_measurement) \
                .tag("country", country) \
                .tag("city", city) \
                .tag("lat", lat) \
                .tag("long", long) \
                .tag("client", client_ip) \
                .tag("route", client_request_url) \
                .field("requests", 1) \
                .field("bytes_recv", bytes_to_client) \
                .field("bytes_sent", bytes_from_client) \
                .time(line_time, WritePrecision.NS)
            try:
                self.influx_write.write(self.config.influx_bucket, self.config.influx_org, new_location_data)
            except Exception as ex:
                logging.warning(ex)
                return False
        else:
            if line_time.timestamp() - latest_data.time.timestamp() > 0:
                new_location_data = Point(self.config.influx_nginx_measurement) \
                    .tag("country", country) \
                    .tag("city", city) \
                    .tag("lat", lat) \
                    .tag("long", long) \
                    .tag("client", client_ip) \
                    .tag("route", client_request_url) \
                    .field("requests", latest_data.requests + 1) \
                    .field("bytes_recv", latest_data.bytes_recv + bytes_to_client) \
                    .field("bytes_sent", latest_data.bytes_sent + bytes_from_client) \
                    .time(line_time, WritePrecision.NS)
                try:
                    self.influx_write.write(self.config.influx_bucket, self.config.influx_org, new_location_data)
                except Exception as ex:
                    logging.warning(ex)
                    return False
            else:
                return False
        return True
