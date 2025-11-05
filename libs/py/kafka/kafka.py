import sys
sys.dont_write_bytecode=True 

from confluent_kafka import Producer, Consumer, OFFSET_BEGINNING
from .config import CONFIG
import time



class Producer_Base(Producer):
    '''
    BASE CLASS which is inherited by all other Kafka Producers. Provides boiler-plate code for producing data to Kafka

    Inherit this class and use produce_data function to send { key, value, and topic } to kafka
    '''
    config = CONFIG

    def __init__(self, topic:str, config:dict=None):
        if config:
            self.config = config 
        else:
            self.config = Producer_Base.config

        super().__init__(self.config)
        self.topic = topic
        self._poll_interval = 0

        print('Successfully connected to kafka topic:', self.topic)

    def delivery_callback(self, err, msg):
        if err:
            print(f'Error: {err}')


    def _do_poll(self, timeout: float = 0.0):
        # Process IO & delivery reports; critical for freeing the internal queue
        try:
            self.poll(timeout)
        except Exception as e:
            # Poll should be harmless; log and continue
            print(f"[poll] exception: {e}")
    
    def produce_data(self, key, value, topic:str=None):
        try:
            key_enc = str(key).encode()
            val_enc = str(value).encode()
            topic = topic if topic else self.topic
            max_retry_sleep = 0.5
            backoff = 0.1

            while True:
                try:
                    self.produce(topic, 
                                 key=key_enc, 
                                 value=val_enc, 
                                 callback=self.delivery_callback)
                    # Give librdkafka a chance to send and process DRs
                    self._do_poll(0.0)
                    if self._poll_interval > 0:
                        self._do_poll(self._poll_interval)
                    return
                except BufferError:
                    # Local: Queue full — drain and back off a hair
                    self._do_poll(0.05)
                    time.sleep(backoff)
                    backoff = min(max_retry_sleep, backoff * 2.0)
                except Exception as err:
                    # Other synchronous errors (e.g., bad topic name)
                    print("[produce] Not produced:", err)
                    # Optional: re-raise if you want callers to handle
                    return
  
            
            # print("produced!")
        except Exception as err:
            self._do_poll(0.1)
            print("Not produced", err)
        # while try_count<3:
        #     try:
        #         if topic: 
        #             self.produce(topic, key=key_enc, value=val_enc, callback=self.delivery_callback)
        #         else:
        #             self.produce(self.topic, key=key_enc, value=val_enc, callback=self.delivery_callback)
        #         break
        #     except Exception as e:
        #         print(e)
        #         time.sleep(3)
        #         try_count+=1

    
    def kill_producer(self, timeout: float = 10.0):
        """
        Flush outstanding messages before exit. Do NOT purge before flush; purge drops queued messages.
        """
        try:
            remaining = self.flush(timeout)
            if remaining > 0:
                print(f"[flush] {remaining} message(s) not delivered before timeout")
        except Exception as e:
            print(f"[flush] exception: {e}")
        finally:
            print("closing")
