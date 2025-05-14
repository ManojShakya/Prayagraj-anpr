
import pika
import os
import pickle  # To deserialize and serialize frames
import struct  # To handle frame size unpacking
from ultralytics import YOLO
import datetime
import cv2
import logging
import time
import requests


#url = "https://vmsapi3.ajeevi.in/api/VehicleDetection/Post"
url = os.getenv('API_URL')  # Read API URL from environment variable

# Load the YOLO model
model = YOLO("yolov8m.pt")
vehicle = [2, 3, 5, 7]

# Function to send logs to RabbitMQ
def send_log_to_rabbitmq(log_message):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', heartbeat=600))
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
        "Event_Type":"Start threads for send frames",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)

def log_error(message):
    logging.info(message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "log_level" : "ERROR",
        "Event_Type":"Start threads for send frames",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)    

def log_exception(message):
    logging.error(message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "log_level" : "EXCEPTION",
        "Event_Type":"Start threads for send frames",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)


# Directory to save frames 
vehicle_frame = "vehicle_frame"
os.makedirs(vehicle_frame, exist_ok=True)


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

def process_frame(ch, method, properties, body ,processed_queue_name, rabbitmq_host):
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
        camera_id = frame_data["camera_id"]
        frame = frame_data["frame"]
        user_id = frame_data["user_id"]
        date_time = frame_data["date_time"]
        
        # Detect and classify objects in the frame
        results = model(frame)[0]
        for result in results.boxes.data.tolist():
            x1, y1, x2, y2, score, id = result
            if int(id) in vehicle and score > 0.5:
                # Serialize the frame and send it to the 'processed_frames' queue
                vehicle_type = results.names[int(id)]
                processed_frame_data = {
                    "camera_id": camera_id,
                    "frame": frame,
                    "user_id": user_id,
                    "date_time": date_time
                }
                now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Create a directory with the camera ID as its name
                camera_dir = os.path.join(vehicle_frame, str(camera_id))
                os.makedirs(camera_dir, exist_ok=True)

                # Save the original frame in the corresponding camera folder
                frame_path = os.path.join(camera_dir, f'{now}.jpg')

                # Get the current working directory and construct the full path
                # full_frame_path = os.path.join(os.getcwd(), camera_dir, f'{now}.jpg')
                # cv2.imwrite(full_frame_path, frame)
                # print("full frame paht", full_frame_path)
                
                serialized_frame = pickle.dumps(processed_frame_data)
                if not processed_connection or not processed_channel.is_open:
                    log_error(f"Error: Could not open RabbitMQ connection for {processed_queue_name}")
                    processed_connection, processed_channel = setup_rabbitmq_connection(processed_queue_name, rabbitmq_host)


                # Send the frame to the 'processed_frames' queue
                processed_channel.basic_publish(
                    exchange=processed_queue_name,
                    routing_key="",
                    body=serialized_frame
                )

                vehicledata = {
                    "cameraId": camera_id,
                    "framePath": None,
                    "vehicleType": vehicle_type,
                }
                # Send POST request with JSON data
                # response = requests.post(url, json=vehicledata)

                # # Check response status
                # if response.status_code == 200:
                #     log_info(f"Data sent successfully: {response.json()}")
                # else:
                #     log_error(f"Failed to send data. Status code: {response.status_code}")
                # #log_info(f"Vehicle detected with camera id {camera_id}: {results.names[int(id)]}")
            else:
                log_error(f"No vehicle detected with camera id {camera_id}")
                # Set up RabbitMQ connection and channel for sending processed frames
                processed_connection, processed_channel = setup_rabbitmq_connection(processed_queue_name, rabbitmq_host)

        
        

        # Print metadata (camera ID and timestamp)
        log_info(f"Received frame from Camera ID: {camera_id}")
        

    except Exception as e:
        log_exception(f"Error processing frame: {e}")

def main(receive_queue_name="all_frame_media", processed_queue_name="detect_vehicle", rabbitmq_host="localhost"):
    """
    Main function to set up RabbitMQ connections for receiving and sending frames.

    Args:
        queue_name (str): The RabbitMQ queue to consume frames from. Defaults to 'video_frames'.
        processed_queue_name (str): The RabbitMQ queue to send processed frames to. Defaults to 'processed_frames'.
    """
    # Set up RabbitMQ connection and channel for receiving frames
    receiver_connection, receiver_channel = setup_rabbitmq_connection(receive_queue_name,rabbitmq_host)
    queue_name_frame = "frame_for_detect_vehicle"
    
    while True:
        try:
            if not receiver_channel.is_open:
                log_error("Receiver channel is closed. Attempting to reconnect.")
                time.sleep(25)
                receiver_connection, receiver_channel = setup_rabbitmq_connection(receive_queue_name, rabbitmq_host)
            
            receiver_channel.queue_declare(queue=queue_name_frame, durable=True)
            receiver_channel.queue_bind(exchange=receive_queue_name, queue= queue_name_frame)
            receiver_channel.basic_consume(
                queue=queue_name_frame, 
                on_message_callback=lambda ch, method, properties, body: process_frame(
                    ch, method, properties, body,processed_queue_name, rabbitmq_host
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
