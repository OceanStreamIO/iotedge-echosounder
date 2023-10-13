import os
import asyncio
import sys
import signal
import threading
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message
from oceanstream.L0_unprocessed_data.raw_reader import file_finder, file_integrity_checking, convert_raw_files, read_processed_files
from oceanstream.L0_unprocessed_data.ensure_time_continuity import fix_time_reversions, check_reversed_time
# Event indicating client stop
stop_event = threading.Event()

# Define the directory to monitor
DIRECTORY_TO_WATCH = "/app/data"  # adjust this to the correct path
all_files = set(os.listdir(DIRECTORY_TO_WATCH))

def create_client():
    client = IoTHubModuleClient.create_from_edge_environment()

    async def receive_message_handler(message):
        if message.input_name == "input1":
            print("Received rawFileAdded event on input1:", message.data)
            
            # Trigger the file checking and processing
            await check_and_process_files(client)

    client.on_message_received = receive_message_handler
    return client

async def check_and_process_files(client):
    global all_files

    current_files = set(os.listdir(DIRECTORY_TO_WATCH))
    new_files = current_files - all_files
    if new_files:
        for file in new_files:
            print(f"New file detected: {file}")
            
            # Check file integrity
            check = file_integrity_checking(file)
            if check["file_integrity"]:
                print(f"File {file} passed integrity check.")
                
                # Process the file using oceanstream package
                path_processed = convert_raw_files([check])
                echodata = read_processed_files(path_processed)
                if check_reversed_time(echodata):
                    echodata_fixed = fix_time_reversions(echodata)
                data = "Success"
                await send_to_hub(client, file, data)
            else:
                print(f"File {file} failed integrity check.")

    # Update the tracked files
    all_files.update(new_files)

async def send_to_hub(client, filename, data):
    # Modify this to send the actual processed data or any relevant information
    msg = Message(f"New file detected: {filename}. Processed data: {data}")
    await client.send_message_to_output(msg, "output1")
    print(f"Sent message for file: {filename}")

def main():
    print("IoT Hub Client for Python")

    client = create_client()

    def module_termination_handler(signal, frame):
        print("IoTHubClient stopped by Edge")
        stop_event.set()

    signal.signal(signal.SIGTERM, module_termination_handler)

    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()  # Keep the client running to listen for the rawFileAdded event
    except Exception as e:
        print("Unexpected error:", e)
        raise
    finally:
        print("Shutting down IoT Hub Client...")
        loop.run_until_complete(client.shutdown())
        loop.close()

if __name__ == "__main__":
    main()