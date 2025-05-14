import pika
import cv2
import pickle  # To deserialize and serialize frames
import struct  # To handle frame size unpacking
from ultralytics import YOLO
import cv2
import os
import datetime
import logging
import time
import numpy as np

# Load the YOLO model
model = YOLO("license_plate_detector.pt")

polygon_points = [(275, 206), (1003, 401), (924, 675), (147, 317)]

# Function to send logs to RabbitMQ
def send_log_to_rabbitmq(log_message):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',heartbeat=600))
        channel = connection.channel()
        channel.queue_declare(queue='anpr_logs')  # Declare the queue for logs
        # Serialize the log message as JSON and send it to RabbitMQ
        channel.basic_publish(
            exchange='',
            routing_key='anpr_logs',
            body=pickle.dumps(log_message)
        )
        connection.close()
    except Exception as e:
        print(f"Failed to send log to RabbitMQ: {e}")

# Wrapper functions for logging and sending logs to RabbitMQ
def log_info(message):
    logging.info(message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "log_level" : "INFO",
        "Event_Type":"Numbper Plate detection event",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)

def log_error(message):
    logging.info(message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "log_level" : "ERROR",
        "Event_Type":"Numbper Plate detection event",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)    

def log_exception(message):
    logging.error(message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "log_level" : "EXCEPTION",
        "Event_Type":"Numbper Plate detection event",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)



def setup_rabbitmq_connection(queue_name, rabbitmq_host, retries=5, retry_delay=5):
    """
    Set up a RabbitMQ connection and declare the queue.
    """
    for attempt in range(retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, heartbeat=600))
            channel = connection.channel()
            channel.exchange_declare(exchange=queue_name, exchange_type="fanout")
            log_info(f"Connected to RabbitMQ at {rabbitmq_host}")
            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            log_error(f"RabbitMQ connection failed (attempt {attempt+1}/{retries}): {e}")
            time.sleep(retry_delay)
    raise log_exception(f"Could not connect to RabbitMQ after {retries} attempts")
def process_frame(ch, method, properties, body, processed_queue_name, rabbitmq_host):
    """
    Callback function to process the received frames from RabbitMQ.

    Args:
        ch, method, properties: RabbitMQ parameters.
        body: The serialized frame data received from the queue.
        processed_channel: RabbitMQ channel for sending processed frames.
    """

    processed_connection, processed_channel = setup_rabbitmq_connection(processed_queue_name, rabbitmq_host)
    
    try:
        # Deserialize the frame and metadata
        frame_data = pickle.loads(body)
        camera_id = frame_data.get("camera_id", "Unknown")  # Default to 'Unknown' if not found
        frame = frame_data.get("frame", None)  # Ensure 'frame' is present
        user_id = frame_data.get("user_id", "Unknown") # Default to 'Unknown'
        date_time = frame_data.get("date_time", "Unknown") # Default
        #print("This is payload data :", frame_data)

        if frame is None:
            raise log_error("Frame data is missing from the message")

        # Detect and classify objects in the frame
        results = model(frame)[0]
        for result in results.boxes.data.tolist():
            x1, y1, x2, y2, score, id = result
            cx = int((x1 + x2) / 2)
            cy = int((y1 + y2) / 2)
            # Check if the center point is inside the finalized polygon
            result1d = cv2.pointPolygonTest(np.array(polygon_points, np.int32), (cx, cy), False)
            if result1d > 0 and score > .5: # Object is inside the polygon
            #if score > 0.5:
                # Slice license plate
                license_plate_crop = frame[int(y1):int(y2), int(x1): int(x2), :]
                # Process license plate
                license_plate_crop_resized = cv2.resize(license_plate_crop, (200, 100))
                license_plate_crop_gray = cv2.cvtColor(license_plate_crop_resized, cv2.COLOR_BGR2GRAY)
                _, license_plate_crop_thresh = cv2.threshold(license_plate_crop_gray, 155, 255, cv2.THRESH_BINARY_INV)
                
                # Serialize the processed license plate frame
                processed_frame_data = {
                    "camera_id": camera_id,
                    "frame" : frame,
                    "frame_plate": license_plate_crop_gray,
                    "user_id": user_id,
                    "date_time": date_time
                }
             
                serialized_frame = pickle.dumps(processed_frame_data)
                if not processed_connection or not processed_channel.is_open:
                    log_error(f"Error: Could not open RabbitMQ connection for {processed_queue_name}")
                    processed_connection, processed_channel = setup_rabbitmq_connection(processed_queue_name, rabbitmq_host)

                # Send the processed frame to the 'processed_frames' queue
                processed_channel.basic_publish(
                    exchange=processed_queue_name,
                    routing_key="",
                    body=serialized_frame
                )
               
            else:
                log_error(f"No license plate detected from camera id {camera_id}")    

        # Print metadata (camera ID)
        log_info(f"Received frame from Camera ID: {camera_id}")

    except Exception as e:
        log_exception(f"Error processing frame --=: {e}")

def main(receive_queue_name="all_frame_media", processed_queue_name="detect_plate", rabbitmq_host="localhost"):
    """
    Main function to set up RabbitMQ connections for receiving and sending frames.

    Args:
        queue_name (str): The RabbitMQ queue to consume frames from. Defaults to 'video_frames'.
        processed_queue_name (str): The RabbitMQ queue to send processed frames to. Defaults to 'processed_frames'.
    """
    # Set up RabbitMQ connection and channel for receiving frames
    receiver_connection, receiver_channel = setup_rabbitmq_connection(receive_queue_name, rabbitmq_host)
    queue_name = "detected_vehicle"

    while True:
        try:
            if not receiver_channel.is_open:
                log_error("Receiver channel is closed. Attempting to reconnect.")
                time.sleep(25)
                receiver_connection, receiver_channel = setup_rabbitmq_connection(receive_queue_name, rabbitmq_host)
            receiver_channel.queue_declare(queue=queue_name, durable=True)
            receiver_channel.queue_bind(exchange=receive_queue_name, queue=queue_name)
            # Start consuming frames from the 'video_frames' queue
            receiver_channel.basic_consume(
                queue=queue_name, 
                on_message_callback=lambda ch, method, properties, body: process_frame(
                    ch, method, properties, body, processed_queue_name,rabbitmq_host 
                ),
                auto_ack=True
            )
            log_info("Waiting for video frames...")
            receiver_channel.start_consuming()
        except pika.exceptions.ConnectionClosedByBroker as e:
            log_error("Connection closed by broker, reconnecting...")
            time.sleep(25)
            receiver_connection, receiver_channel = setup_rabbitmq_connection(receive_queue_name, rabbitmq_host)

        except Exception as e:
            log_exception(f"Unexpected error: {e}")
            time.sleep(25)
            continue
    

if __name__ == "__main__":
    # Start the receiver and sender
    main()

