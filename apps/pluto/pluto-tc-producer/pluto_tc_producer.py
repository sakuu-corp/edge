import time
import json
import pytz
from datetime import datetime, timezone
import logging
import sys
from pyModbusTCP.client import ModbusClient

from config import CONFIG

from kafka import Producer_Base
import os
from logger import LogSetup as Logs

TOPIC = 'pluto-aemc-dl1081'
CLIENT_IP = '192.168.1.33'
CLIENT_PORT = 502
UNIT_ID=255
STARTING_ADDRESS = 3
COUNT = 2
SCAN_INTERVAL = 0.5

LOG_PATH = os.environ.get('LOG_PATH')
Logs(log_dir=LOG_PATH)
prod_logger = logging.getLogger('app.producer')
err_logger = logging.getLogger('app.error')
device_logger = logging.getLogger('app.device')

class TC_Producer:
    topic = TOPIC
    
    def __init__(self, client_ip, client_port=502, unit_id=255):
        self.producer = Producer_Base(TC_Producer.topic, CONFIG)
        self.client = ModbusClient(host=client_ip, port=client_port, unit_id=unit_id)

    def stream_data(self, starting_address, count, scan_interval):
        if not self.client.open():
            Logs.log_msg(err_logger, f"Unable to connect to Modbus server: {self.client.last_error}", logging.CRITICAL)

        while True: 
            try:
                registers = self.client.read_holding_registers(starting_address, count)
            except Exception as e:
                Logs.log_msg(err_logger, f"Unable to read registers: {e}", logging.CRITICAL)
                continue

            if not registers:
                Logs.log_msg(err_logger, f"Error reading registers: {self.client.last_error}", logging.CRITICAL)
                continue

            try: 
                now = datetime.now()
                data = {
                    'timestamp':now.timestamp(),
                    'tc_1':float(registers[0])/1000,
                    'ts_2':float(registers[1])/1000,
                }
                message_value = json.dumps(data)

                self.producer.produce_data(key='data', value=message_value)
                Logs.log_msg(prod_logger, f"Sucessfully produced data at {now.isoformat()}. Data: {message_value} ", logging.INFO)
            except Exception as e:
                Logs.log_msg(err_logger, f"Unable to extract data. {e}", logging.CRITICAL)
                

            time.sleep(scan_interval)


if __name__ == '__main__':
    
    Logs.log_msg(prod_logger, f"Starting TC Logging", logging.info)
    tcp = TC_Producer(CLIENT_IP, CLIENT_PORT, UNIT_ID)
    tcp.stream_data(STARTING_ADDRESS, COUNT, SCAN_INTERVAL)

