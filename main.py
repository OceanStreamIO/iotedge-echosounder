import os
import asyncio
import sys
import signal
import logging
import threading
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message
from oceanstream.L0_unprocessed_data.raw_reader import (file_finder, 
                                                        file_integrity_checking,
                                                        read_raw_files,
                                                        convert_raw_files, 
                                                        read_processed_files)
from oceanstream.L0_unprocessed_data.ensure_time_continuity import fix_time_reversions, check_reversed_time

from oceanstream.L2_calibrated_data.sv_computation import compute_sv
from oceanstream.L2_calibrated_data.sv_dataset_extension import enrich_sv_dataset
from oceanstream.L2_calibrated_data.sv_interpolation import interpolate_sv
from oceanstream.L2_calibrated_data.noise_masks import create_default_noise_masks_oceanstream
from oceanstream.L2_calibrated_data.processed_data_io import read_processed,write_processed
from oceanstream.L2_calibrated_data.background_noise_remover import apply_remove_background_noise


logging.basicConfig(level=logging.INFO, stream=sys.stdout)
# Event indicating client stop
stop_event = threading.Event()


# Define the directory to monitor
# Configuration
CONFIG = {
    "DIRECTORY_TO_WATCH": os.getenv("DIRECTORY_TO_WATCH", "/app/tmpdata")
}


def create_client(all_files):
    async def receive_message_handler(message):
        if message.input_name == "input1":
            logging.info("Received rawFileAdded event on input1:", message.data)
            # Trigger the file checking and processing
            all_files = await check_and_process_files(client, all_files)

    client = IoTHubModuleClient.create_from_edge_environment()
    client.on_message_received = receive_message_handler
    return client, all_files

async def check_and_process_files(client, all_files):
    logging.info("check_and_process_files")
    current_files = set(os.listdir(DIRECTORY_TO_WATCH))
    new_files = current_files - all_files
    if new_files:
        for file in new_files:
            try:
                data = "Hopes and dreams"
                check = file_integrity_checking(os.path.join(DIRECTORY_TO_WATCH,file))
                file_integrity = check.get("file_integrity", False)
                if file_integrity:
                    # Process the file using oceanstream package
                    echodata = read_raw_files([check])
                    sv_dataset = compute_sv(echodata[0])
                    if check["sonar_model"] == "EK60":
                        encode_mode="power"
                    elif check["sonar_model"] == "EK80":
                        encode_mode="complex"
                    sv_enriched = enrich_sv_dataset(sv_dataset,
                                                    echodata[0],
                                                    waveform_mode="CW",
                                                    encode_mode=encode_mode
                                                    )
                    create_default_noise_masks_oceanstream(sv_enriched)
                    data = "Success!"
            except FileNotFoundError as e:
                logging.error(f"File {file} could not be read!" + str(e))
            except ValueError as e:
                logging.error(f"Could not compute SV for file {file} due to: {e}")
            finally:
                try:
                    await send_to_hub(client, file, data)
                except Exception as e:
                    logging.error(f"Failed to send data for file {file}: {e}")

    # Update the tracked files
    return all_files

async def send_to_hub(client, filename, data):
    # Modify this to send the actual processed data or any relevant information
    msg = Message(f"New file detected: {filename}. Processed data: {data}")
    await client.send_message_to_output(msg, "output1")
    logging.info(f"Sent message for file: {filename}")

def main():
    logging.info("IoT Hub Client for Python")
    all_files = set(os.listdir(CONFIG["DIRECTORY_TO_WATCH"]))
    client,all_files = create_client(all_files)

    def signal_handler(signal, frame):
        logging.info(f"Received signal {signal}. Shutting down...")
        stop_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logging.exception("Unexpected error occurred." + str(e))
        raise
    finally:
        logging.info("Shutting down IoT Hub Client...")
        try:
            loop.run_until_complete(client.shutdown())
        except Exception as e:
            logging.error(f"Error during client shutdown: {e}")
        finally:
            loop.close()

if __name__ == "__main__":
    main()