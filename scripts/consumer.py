from confluent_kafka import DeserializingConsumer, KafkaError
import numpy as np
import cv2

# Setting up the Video Config Details:
output_file = 'from_kafka_live_stream.avi'
fps = 30
frame_width = 640
frame_height = 480  # Set your frame height
fourcc = cv2.VideoWriter_fourcc(*'XVID')  # Codec for AVI
out = cv2.VideoWriter(output_file, fourcc, fps, (frame_width, frame_height))

# Setting up the Consumer Details:
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest'
}

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(['image-data'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f'Error: {msg.error()}')
                break

        image_data = msg.value()
        image_array = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
        #cv2.imwrite('output_image.jpg', img)
        if img is not None:
            out.write(img)

finally:
    consumer.close()
    cv2.destroyAllWindows()


