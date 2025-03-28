# producer.py
import time
import random
import json
from kafka import KafkaProducer

# pip install kafka-python

TOPIC = "traffic_events"
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

'''
{ 
    "sensor_id": "S123", 
    "timestamp": "2025-03-15T14:25:10", 
    "vehicle_count": 15, 
    "average_speed": 45.6, 
    "congestion_level": "LOW" 
}
'''

sensors = [None, "S123", "S124", "S200", "S201", "S350"]
timestamp_range = [None, time.time()]
vehicle_count_range = [-10, 30]
average_speed_range = [-5, 300]
congestion_levels = ["LOW", "MEDIUM", "HIGH"]


while True:
    event = {
        "sensor_id": random.choice(sensors),
        "timestamp": random.choice(timestamp_range),
        "vehicle_count": random.randint(vehicle_count_range[0], vehicle_count_range[1]),
        "average_speed": round(random.uniform(average_speed_range[0], average_speed_range[1]),1),
        "congestion_level": random.choice(congestion_levels)
    }
    producer.send(TOPIC, event)
    print(f"Sent event: {event}")
    time.sleep(random.uniform(0.5, 2.0))  # random interval
