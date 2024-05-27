import logging
import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
import threading
import signal
import sys
import RPi.GPIO as GPIO
import time
from datetime import datetime, timezone
from google.cloud import storage

# Set up logging
logging.basicConfig(filename='/tmp/sprinkler.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')


### Globals 
##
# Initialize Firebase app (ensure this is done before using Firestore)
cred = credentials.Certificate("/home/cbi/sprinkler/fb-key-ai-datapipes.json")
firebase_admin.initialize_app(cred)
db = firestore.client()
storage_client = storage.Client.from_service_account_json("/home/cbi/sprinkler/fb-key-ai-datapipes.json")
bucket_name = 'aidatapipes.appspot.com'
file_name = 'sprinkler_event.txt'


# Function to turn off the GPIO pin and update Firestore
def turn_off_gpio(relay_pin):
    GPIO.output(relay_pin, GPIO.LOW)
    logging.info("GPIO pin set to LOW.")
    
    # Update the Firestore document to set isOn to false
    try:
        doc_ref = db.collection('sprinkler').document('main')
        doc_ref.set({'isOn': False})
        logging.info("Firestore document updated: isOn set to false.")
    except Exception as e:
        logging.error(f"Error updating Firestore document: {e}")


# Storage Bucket Google Cloud
def append_log_to_gcs(timestamp, status):
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Download the existing log file
        if blob.exists():
            existing_logs = blob.download_as_string().decode('utf-8')
        else:
            existing_logs = ''

        # Append the new log entry
        new_entry = f"Time: {timestamp}\tStatus: {status}\n"
        updated_logs = existing_logs + new_entry

        # Upload the updated log file
        blob.upload_from_string(updated_logs)
        logging.info(f"Appended log entry to {file_name} in bucket {bucket_name}")
    except Exception as e:
        logging.error(f"Error appending log entry to GCS: {e}")

# Wrapper function to handle both actions after delay
def turn_off_and_log(relay_pin):
    turn_off_gpio(relay_pin)
    append_log_to_gcs(int(time.time() * 1000), False)


# Create a callback on_snapshot function to capture changes
def on_snapshot(doc_snapshot, changes, read_time):
    relay_pin = 25
    setup(relay_pin)
    for doc in doc_snapshot:
        document_dict = doc.to_dict()
        if document_dict['isOn']:
            GPIO.output(relay_pin, GPIO.HIGH)
            logging.info("GPIO pin set to HIGH.")
         
            # Schedule turning off the GPIO pin
            if 'offTime' in document_dict:
                off_time = document_dict['offTime']
                if isinstance(off_time, datetime):
                    off_time = off_time
                else:
                    off_time = off_time.to_datetime()

                now = datetime.now(timezone.utc)
                delay = (off_time - now).total_seconds()

                if delay > 0:
                    logging.info(f"Scheduling GPIO pin to set to LOW in {delay} seconds.")
                    threading.Timer(delay, turn_off_and_log, args=[relay_pin]).start()
                else:
                    logging.info("offTime is in the past, setting GPIO pin to LOW immediately.")
                    GPIO.output(relay_pin, GPIO.LOW)
                    
        else:
            GPIO.output(relay_pin, GPIO.LOW)
            logging.info("GPIO pin set to LOW.")
            

        logging.info(f'Received document snapshot: {doc.id}')
        logging.info(f'Document data: {document_dict}')

# Create a thread for Firestore listener
def listen_thread():
    doc_ref = db.collection(u'sprinkler').document(u'main')
    doc_watch = doc_ref.on_snapshot(on_snapshot)

# Signal handler for Ctrl+C
def signal_handler(sig, frame):
    logging.info('Ctrl+C captured, stopping listener...')
    destroy()
    sys.exit(0)

def setup(relay_pin):
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(relay_pin, GPIO.OUT)

def destroy():
    GPIO.cleanup()

# Register signal handler for Ctrl+C
signal.signal(signal.SIGINT, signal_handler)

# Start the listener thread
listener_thread = threading.Thread(target=listen_thread)
listener_thread.start()

# Keep the main thread alive
while True:
    time.sleep(1)
