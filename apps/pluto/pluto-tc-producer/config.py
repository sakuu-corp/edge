import sys
sys.dont_write_bytecode=True 

CONFIG = {
    'bootstrap.servers':'kafka:29092,kafka:39092,kafka:49092',
    'acks': 'all',                    
    'enable.idempotence': True,
    'request.timeout.ms': 60000,
    # 'metadata.fetch.timeout.ms': 60000,
    'retries': 10000,
    "delivery.timeout.ms": 120000,        # upper bound before failing
    "queue.buffering.max.kbytes": 131072, # 128 MB producer buffer
    "queue.buffering.max.messages": 200000, # raises queue size limit
}