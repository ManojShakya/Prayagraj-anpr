import pika
import pickle  # To deserialize and serialize frames
import struct  # To handle frame size unpacking
import re
from paddleocr import PaddleOCR
import os
import cv2
import datetime
import requests
import logging
import time
import pandas as pd

# API endpoint
# url  = "https://vmsapi3.ajeevi.in/api/NumberPlateReadedData/Post"
url = os.getenv("API_URL")

# Initialize the OCR model
ocr = PaddleOCR(use_angle_cls=True, lang='en')

# Directory to save frames and cropped license plates
save_frame = 'frames'
save_plate = 'plates'
os.makedirs(save_frame, exist_ok=True)
os.makedirs(save_plate, exist_ok=True)

temp = []


def write_plate_data_to_excel(data: dict, filename: str = "plate_data.xlsx"):
    """
    Write or append plate detection data to an Excel file in the current working directory.

    Args:
        data (dict): Dictionary containing user_id, date_time, cameraId, and text.
        filename (str): Name of the Excel file (default: plate_data.xlsx).
    """
    filepath = os.path.join(os.getcwd(), filename)

    # Create a DataFrame from the input dictionary
    new_data = pd.DataFrame([data])

    # Check if the file already exists
    if os.path.exists(filepath):
        # Load existing data and append
        existing_data = pd.read_excel(filepath)
        updated_data = pd.concat([existing_data, new_data], ignore_index=True)
    else:
        # File doesn't exist; use the new data as-is
        updated_data = new_data

    # Save to Excel
    updated_data.to_excel(filepath, index=False)


# Function to send logs to RabbitMQ
def send_log_to_rabbitmq(log_message):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=600))
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
        "Event_Type":"Read Numbper Plate event",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)

def log_error(message):
    logging.info(message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "log_level" : "ERROR",
        "Event_Type":"Read Numbper Plate event",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)    

def log_exception(message):
    logging.error(message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "log_level" : "EXCEPTION",
        "Event_Type":"Read Numbper Plate event",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)


last_temp_update_time = datetime.datetime.now()

# Function to clear the `temp` list after 5 hours
# def clear_temp_after_interval(interval_hours=20):
#     global temp, last_temp_update_time
#     current_time = datetime.datetime.now()
#     if int((current_time - last_temp_update_time).total_seconds()) > interval_hours:
#         temp = []
#         last_temp_update_time = current_time
#         log_info("Temp list cleared after 20 second")

def clear_temp_after_interval(interval_seconds=20):
    global temp, last_temp_update_time
    current_time = datetime.datetime.now()
    if (current_time - last_temp_update_time).total_seconds() > interval_seconds:
        temp = []
        last_temp_update_time = current_time
        log_info("Temp list cleared after 20 seconds")
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

#-------------------------------------------------------------------------------
def process_frame(ch, method, properties, body):
    """
    Callback function to process the received frames from RabbitMQ.

    Args:
        ch, method, properties: RabbitMQ parameters.
        body: The serialized frame data received from the queue.
    """
    global temp, last_temp_update_time  # Track the update time  # Use the global temp variable to track last detected plate
    try:
        # Deserialize the frame and metadata
        frame_data = pickle.loads(body)
        camera_id = frame_data["camera_id"]
        frame = frame_data["frame"]
        frame_plate = frame_data["frame_plate"]
        user_id = frame_data["user_id"]
        date_time = frame_data["date_time"]
       
        # Perform OCR on the license plate region
        result = ocr.ocr(frame_plate, cls=True)

        t = ''  # Initialize the variable before processing the OCR result
        highest_score = 0

        if result:
            for result1 in result:
                if result1:
                    for bbox, (text, score) in result1:
                        t += text
                    # Clean the text
                    text = re.sub(r'[^\w\s]|_', '', t)
                    t = text.upper().replace(" ", "")
                    length = len(t)
                    
                    # Define license plate patterns
                    patterns = [
                    r'[a-zA-Z]{2}\d{1}[a-zA-Z]{1}\d{4}',  # Pattern 1: char, char, digit, char, digit, digit, digit, digit    = 8
                    r'[a-zA-Z]{2}\d{2}[a-zA-Z]{1}\d{4}',  # Pattern 1: char, char, digit, digit, char, digit, digit, digit, digit  = 9
                    r'[a-zA-Z]{2}\d{1}[a-zA-Z]{2}\d{4}',  # Pattern 1: char, char, digit, char, char, digit, digit, digit, digit  = 9
                    r'[a-zA-Z]{2}\d{2}[a-zA-Z]{2}\d{4}',  # Pattern 2: char, char, digit, digit, char, char, digit, digit, digit, digit  = 10
                    r'[a-zA-Z]{2}\d{1}[a-zA-Z]{3}\d{4}',  # Pattern 2: char, char, digit, char, char, char, digit, digit, digit, digit  = 10
                    r'[a-zA-Z]{2}\d{2}[a-zA-Z]{3}\d{4}',  # Pattern 2: char, char, digit, digit, char, char, char, digit, digit, digit, digit  = 11
                    r'[a-zA-Z]{2}\d{1}[a-zA-Z]{4}\d{4}',  # Pattern 2: char, char, digit, char, char, char, char, digit, digit, digit, digit  = 11
                    r'[a-zA-Z]{2}\d{2}[a-zA-Z]{4}\d{4}',  # Pattern 2: char, char, digit, digit, char, char, char, char, digit, digit, digit, digit  = 12
                    r'[a-zA-Z]{2}\d{1}[a-zA-Z]{5}\d{4}',  # Pattern 2: char, char, digit, char, char, char, char, char, digit, digit, digit, digit  = 12
                    r'[a-zA-Z]{2}\d{1}[a-zA-Z]{5}\d{4}',  # Pattern 2: char, char, digit, char, char, char, char, char, digit, digit, digit, digit  = 12
                    r'[a-zA-Z]{2}\d{2}[a-zA-Z]{5}\d{4}',  # Pattern 2: char, char, digit, digit, char, char, char, char, char, digit, digit, digit, digit  = 13
                    r'[a-zA-Z]{2}\d{1}[a-zA-Z]{6}\d{4}',  # Pattern 2: char, char, digit, char, char, char, char, char, char, digit, digit, digit, digit  = 13
                    r'\d{2}[a-zA-Z]{2}\d{4}[a-zA-Z]',      # digit, digit, char, char, digit, digit, digit, digit,char         
                    ]
                    
                    # Only proceed if the length of the detected text is valid
                    if 8 <= length <= 13:
                        # Find matches for the plate pattern
                        all_matches = [match for pattern in patterns for match in re.findall(pattern, t)]
                        
                        if all_matches:
                            for match in all_matches:
                                print("plate text :", match)
                                
                                # Only save if this plate is different from the last detected one
                                match = match.upper()
                                if match not in temp:
                                    temp.append(match)  # Update the temp list with the new plate text
                                    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

                                    frame_dir = os.path.join(save_frame, str(camera_id))
                                    os.makedirs(frame_dir, exist_ok=True)

                                    # Save original frame
                                    full_frame_path = os.path.join(os.getcwd(), frame_dir, f'{now}.jpg')
                                    cv2.imwrite(full_frame_path, frame)
                                    
                                    # Save the cropped license plate
                                    plate_dir = os.path.join(save_plate, str(camera_id))
                                    os.makedirs(plate_dir, exist_ok=True)

                                    # Save original frame
                                    full_plate_path = os.path.join(os.getcwd(), plate_dir, f'{match}.jpg')
                                    cv2.imwrite(full_plate_path, frame_plate)
                                    
                                    # Data to send
                                    platedata = {
                                        "framePath": full_frame_path,
                                        "platePath": full_plate_path,
                                         "cameraId": camera_id,
                                        "text": match
                                    }
                                    platewrite_excel = {
                                        "user_id": user_id,
                                        "date_time": date_time,
                                         "cameraId": camera_id,
                                        "text": match
                                                        }
                                    write_plate_data_to_excel(platewrite_excel)
                                    # Send POST request with JSON data
                                    response = requests.post(url, json=platedata)

                                    # Check response status
                                    if response.status_code == 200:
                                        log_info("Data sent successfully:", response.json())
                                    else:
                                        log_error(f"Failed to send data. Status code: {response.status_code}")
                        else:
                            log_error("No valid patters were found from camera id {camera_id}")
                    else:
                        log_error(f"Invalid length of detected text from camera id {camera_id}")        
        else:
            log_error(f"OCR result is empty, no text readed from camera id {camera_id}")

        # Check if `temp` needs to be cleared after the interval
        clear_temp_after_interval()    
        

    except Exception as e:
        log_exception(f"Error processing frame: {e}")

#----------------------------------------------------------------------------------


def main(queue_name="detect_plate", rabbitmq_host="rabbitmq"):
    """
    Main function to set up RabbitMQ connections for receiving and sending frames.

    Args:
        queue_name (str): The RabbitMQ queue to consume frames from. Defaults to 'video_frames'.
        processed_queue_name (str): The RabbitMQ queue to send processed frames to. Defaults to 'processed_frames'.
    """
    # Set up RabbitMQ connection and channel for receiving frames
    receiver_connection, receiver_channel = setup_rabbitmq_connection(queue_name, rabbitmq_host)
    queue_name_plate = "detected_plate"

    # # Set up RabbitMQ connection and channel for sending processed frames
    while True:
        try:
            # Your main consuming logic
            if not receiver_channel.is_open:
                log_error("Receiver channel is closed. Attempting to reconnect.")
                print("it's ok to reconnect")
                receiver_connection, receiver_channel = setup_rabbitmq_connection(queue_name, rabbitmq_host)
            # Start consuming frames from the 'video_frames' queue
            receiver_channel.queue_declare(queue=queue_name_plate, durable=True)
            receiver_channel.queue_bind(exchange=queue_name, queue=queue_name_plate)
            receiver_channel.basic_consume(
                queue=queue_name_plate, 
                on_message_callback=lambda ch, method, properties, body: process_frame(
                    ch, method, properties, body
                ),
                auto_ack=True
            )
            log_info("Waiting for video frames...")
            receiver_channel.start_consuming()
        except pika.exceptions.ChannelClosedByBroker as e:
            log_error("Channel closed by broker, reconnecting...")
            receiver_connection, receiver_channel = setup_rabbitmq_connection(queue_name, rabbitmq_host)
        except pika.exceptions.ConnectionClosedByBroker as e:
            log_error("Connection closed by broker, reconnecting...")
            receiver_connection, receiver_channel = setup_rabbitmq_connection(queue_name, rabbitmq_host)
        except Exception as e:
            log_exception(f"Unexpected error: {e}")
            time.sleep(25)
            continue
    
if __name__ == "__main__":
    # Start the receiver and sender
    main()
