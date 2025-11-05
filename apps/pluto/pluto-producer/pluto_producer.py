import sys
sys.dont_write_bytecode=True 
import time
from pylogix import PLC
import json
import pytz
from datetime import datetime
import logging
from kafka import Producer_Base
from logger import LogSetup
import codecs 
import os

from config import CONFIG

LOG_PATH = os.environ.get('LOG_PATH')
LogSetup(log_dir=LOG_PATH)

prod_logger = logging.getLogger('app.producer')
err_logger = logging.getLogger('app.error')
device_logger = logging.getLogger('app.device')
               
TAGS = ['ST1_0_Scaled', #Speed (RPM)
        'IT1_0_Scaled', #Current (AMPS)
        'W1_0_Scaled',  #Power (KW) 
        'TT1_Scaled', #Inner Temp (°C)
        # 'PRX2_DB_OUT',
        # 'PRX4_DB_OUT',
        # 'REQUIREING_RAW_MATERIAL_PROCESS_BIT'
        ]
IP_ADDR = os.environ.get("IPADDR")
MIN_RPM = 50
TIMEZONE = pytz.timezone('America/Los_Angeles')
SLEEP_TIME = 0.5

class ProducerError(Exception):
    def __init__(self):
        super().__init__()

class Nob_Producer(Producer_Base):
    topic = 'pluto'
    alarm_dict = {
        '12':'NOB-130 MOTOR CURRENT HIGH ALARM (IT1.0)',
        '13':'NOB-130 MOTOR SPEED DEVIATION HIGH ALARM (ST-1.0)',
        '14':'NOB-130 MOTOR SPEED DEVIATION HIGH ALARM (ST-1.0)',
        '16':'EMERGENCY PUSH BOTTON FAULT (ILPB2)',
        '17':'SAFETY STATUS FAULT (ESR)',
        '18':'END COVER CLOSE FAULT (PS1)',
        '20':'INLET ACCESS HOLE  CLOSE FAULT (PS3)',
        '25':'NOB-130 VFD FAULT (M1.0)',
        '11':'COOLING FAN MOTOR FAULT (M2)',
    }

    def __init__(self, ip_addr, tags):
        super().__init__(Nob_Producer.topic, CONFIG)
        print(self.topic)
        print(CONFIG)
        self.ip_addr = ip_addr
        self.tags = tags
    
    @staticmethod
    def get_alarm_(alarm_code):
        codes = [str(i+1) for i,ltr in enumerate(str(alarm_code)[::-1]) if ltr == '1']
        return [Nob_Producer.alarm_dict[key] for key in Nob_Producer.alarm_dict.keys() if key in codes]

    def stream_data(self, min_rpm:int=50):
        self.flush()
        while True:
            try:
                with PLC(self.ip_addr) as conn:
                    LogSetup.log_msg(prod_logger, f"Connected to PLC!", logging.INFO)
                    while True:
                        try:
                            alarm_code = str(bin(conn.Read('Alarms[2]').Value))
                            if alarm_code != '0b0':
                                alarms = Nob_Producer.get_alarm_(alarm_code)
                                LogSetup.log_msg(device_logger, f"ALARMS: {alarms}", logging.ERROR)
                                
                            self.rpm = conn.Read('ST1_0_Scaled')
                            
                            LogSetup.log_msg(prod_logger, f"RPM - {self.rpm}", logging.DEBUG)
                            reading_dt = datetime.now() 
                            
                        
                            if int(self.rpm.Value) > min_rpm:
                                value_dict = {}
                                value_dict['timestamp'] = reading_dt.timestamp()

                                for tag in self.tags:
                                    try:
                                        data = conn.Read(tag)
                                        tag_value_dict = {
                                            'value':data.Value,
                                            'status':data.Status,
                                        }
                                        value_dict[tag] = tag_value_dict
                                    except Exception as e:
                                        LogSetup.log_msg(err_logger, f"Could not read {tag} data. | {e}", logging.CRITICAL)
                                        break

                                if value_dict:
                                    prod_values = json.dumps(value_dict)
                                    try_count = 0
                                    while try_count < 2:
                                        try: 
                                            LogSetup.log_msg(prod_logger, f"Produced - {prod_values} @ {reading_dt}.", logging.INFO)
                                            self.produce_data(key='data', value=prod_values)
                                            break
                                        except Exception as e:
                                            print(e)
                                            time.sleep(3)
                                            try_count+=1
                                    else:
                                        LogSetup.log_msg(err_logger, f"Could not produce data for {prod_values} @ {reading_dt}.", logging.ERROR)
                                # else:
                                #     LogSetup.log_msg(err_logger, f'did not produce {value_dict}', logging.ERROR) 
                        except Exception as e:
                            LogSetup.log_msg(err_logger, f"Could not read RPM data. | {e}", logging.CRITICAL)
                            raise ProducerError(f"Could not read RPM data. | {e}")

                        time.sleep(SLEEP_TIME)
            except ProducerError as pe:
                LogSetup.log_msg(err_logger, f"Resetting connection in 3 seconds. | {pe}", logging.CRITICAL)
            except Exception as e:
                LogSetup.log_msg(err_logger, f"An unknown error ocurred. | {e}", logging.CRITICAL)
                
            time.sleep(3)



            


if __name__=='__main__':
    
    Nob_Producer(IP_ADDR, TAGS).stream_data(MIN_RPM)
