import json
import numpy as np
import cv2
from confluent_kafka import SerializingProducer
import time

frame_no = 1
cap = cv2.VideoCapture(0)

def capture_image():
    # fps = cap.get(cv2.CAP_PROP_FPS)
    # skip_frames = int(fps*5)
    ret, frame = cap.read()
    return frame


def image_to_producer(producer):
    img = capture_image()
    #success, encoded_image = cv2.imencode('.jpeg', img)
    success, encoded_image = cv2.imencode('.jpg',img)
    producer.produce(
        'image-data',
        key=str(frame_no),
        value=encoded_image.tobytes())
    producer.flush()



if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'error_cb': lambda err: print(f'Kafka Error {err}')
    }
    producer = SerializingProducer(producer_config)
    try:
        while True:
            image_to_producer(producer)
            time.sleep(5)
            frame_no = frame_no+1
    except KeyboardInterrupt:
        print('Video Streaming Ended by User')
    except Exception as e:
        print(f'Error encounterd: {e}')