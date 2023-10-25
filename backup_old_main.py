import asyncio
import json
import logging
import os
import signal
import sys
import threading

from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message

from oceanstream.L0_unprocessed_data import (file_finder, 
                                             file_integrity_checking,
                                             read_raw_files,
                                             convert_raw_files, 
                                             read_processed_files,
                                             fix_time_reversions,
                                             check_reversed_time)
from oceanstream.L2_calibrated_data import (compute_sv,
                                            enrich_sv_dataset,
                                            interpolate_sv,
                                            create_noise_masks_oceanstream,
                                            read_processed,
                                            write_processed,
                                            apply_remove_background_noise)
from oceanstream.L3_regridded_data import (
    apply_mask_organisms_in_order,
    apply_selected_noise_masks_and_or_noise_removal as apply_selected_masks
)

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
# Event indicating client stop
stop_event = threading.Event()


# Define the directory to monitor
# Configuration
DIRECTORY_TO_WATCH = "/app/tmpdata"


def create_client(connection_string=None):
    """Create an IoT Hub client, either for Edge or standalone."""

    async def receive_message_handler(message):
        if message.input_name == "input1":
            logging.info("Received rawFileAdded event on input1:", message.data)
            # Trigger the file checking and processing
            await check_and_process_files(client, message.data)

    # If a connection string is provided, create a standalone device client
    if connection_string:
        client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    # Otherwise, assume we're running as an IoT Edge module
    else:
        client = IoTHubModuleClient.create_from_edge_environment()

    client.on_message_received = receive_message_handler
    return client

async def check_and_process_files(client, filename):
    logging.info("Check and process files")
    try:
        check = file_integrity_checking(filename)
        file_integrity = check.get("file_integrity", False)
        if file_integrity:
            # Process the file using oceanstream package
            echodata = read_raw_files([check])
            sv_dataset = compute_sv(echodata[0])
            print("Got raw data")
            if check["sonar_model"] == "EK60":
                encode_mode="power"
            elif check["sonar_model"] == "EK80":
                encode_mode="complex"
            else:
                encode_mode="power"
            sv_enriched = enrich_sv_dataset(sv_dataset,
                                            echodata[0],
                                            waveform_mode="CW",
                                            encode_mode=encode_mode
                                            )
            print("Enriched data")
            sv_with_masks = create_noise_masks_oceanstream(sv_enriched)
            print("Added masks to data")
            process_parameters ={
                                "mask_transient": {"var_name": "Sv"},
                                "mask_impulse": {"var_name": "Sv"},
                                "mask_attenuation": {"var_name": "Sv"}
                                }
            ds_processed = apply_selected_masks(
                                                sv_with_masks, 
                                                process_parameters
                                                )
            print("Applied masks to data")
            ds_interpolated = interpolate_sv(ds_processed)
            print("Interpolated nans on data")
            data_dict = {
                        "filename":filename,
                        "% samples": float('nan'),  # Using float('nan') for NaN values
                        "ping_numbers":float('nan'),
                        "FREQ (kHz)": float('nan'),
                        "Latitude": float('nan'),
                        "Longitude": float('nan'),
                        "Miles": float('nan'),
                        "NASC": float('nan'),
                        "Seabed": float('nan'),  # numeric values in meters nan if no seabed
                        "Start range (m)": float('nan'),
                        "End range (m)": float('nan'),
                        "Time": float('nan'),
                        "Transect": float('nan'),
                        "Type": "watercolumn"
                        }
    except FileNotFoundError as e:
        logging.error(f"File {file} could not be read!" + str(e))
    except ValueError as e:
        logging.error(f"Could not compute SV for file {file} due to: {e}")
    finally:
        try:
            await send_to_hub(client, data_dict)
        except Exception as e:
            logging.error(f"Failed to send data for file {file}: {e}")

async def send_to_hub(client, data_dict):
    # Convert the dictionary to a JSON string
    print(data_dict)
    json_data = json.dumps(data_dict)
    # Prepare the message
    msg = Message(json_data)
    await client.send_message_to_output(msg, "output1")
    logging.info(f"Sent message for file: {data_dict['filename']}")

def main():
    logging.info("IoT Hub Client for Python")
    all_files = set(os.listdir(DIRECTORY_TO_WATCH))
    if len(sys.argv) > 1:
        connection_string = sys.argv[1]
        client = create_client(connection_string)
    else:
        client = create_client()

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