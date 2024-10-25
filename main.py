import asyncio
import json
import logging
import os
import signal
import threading
from dotenv import load_dotenv
from azure_handler import create_client
from threading import Lock

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('oceanstream')

# Event indicating client stop
stop_event = threading.Event()

twin_properties = {}
twin_properties_lock = Lock()


# Signal handler for interruptions and closing
def signal_handler(signal, frame):
    logger.info(f"Received signal {signal}. Shutting down...")
    stop_event.set()


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def get_initial_module_twin(client):
    """
    Retrieve the initial module twin properties.
    """
    # Get the twin properties
    twin_properties = client.get_twin()

    # Access the desired properties for this module
    desired_properties = twin_properties.get("desired", {})

    return desired_properties


def handle_input_message(client, message):
    """
    Handle incoming messages from IoT Hub.

    Parameters:
    - client: IoTHubModuleClient
        The IoT Hub client.
    - message: Message
        The received message from IoT Hub.
    """
    from process.workflow import process_raw_file
    global twin_properties

    try:
        logger.info(f'Received message from IoT Hub: {message}')

        with twin_properties_lock:
            current_twin_properties = twin_properties.copy()

        if message.input_name == "rawfileadded":
            logger.info('Processing message from input "rawfileadded".')
            byte_str = message.data
            dict_obj = json.loads(byte_str.decode("utf-8"))
            message_type = dict_obj.get("event", None)

            logger.info(f'Message content: {dict_obj}')
            logger.info(f'Message type: {message_type}')

            if message_type == "fileadd":
                result = process_raw_file(client, dict_obj["file_added_path"], current_twin_properties)
                logger.info(f'Processing result: {result}')

    except Exception as e:
        logger.error(f"Error handling input message: {e}", exc_info=True)


def main():
    global twin_properties
    client = create_client()

    initial_properties = get_initial_module_twin(client)
    logger.info(f"Initial twin desired properties: {initial_properties}")

    with twin_properties_lock:
        logger.info(f"1. twin properties initial update: {initial_properties}")
        sonar_model = initial_properties.get("sonar_model", "EK60")
        waveform_mode = initial_properties.get("waveform_mode", "CW")
        encode_mode = initial_properties.get("encode_mode", "power")
        depth_offset = initial_properties.get("depth_offset", "")
        survey_id = initial_properties.get("survey_id", "")
        survey_name = initial_properties.get("survey_name", "")
        platform_type = initial_properties.get("platform_type", "")
        platform_name = initial_properties.get("platform_name", "")
        platform_code_ICES = initial_properties.get("platform_code_ICES", "")

        properties = {
            "sonar_model": sonar_model,
            "waveform_mode": waveform_mode,
            "encode_mode": encode_mode,
            "depth_offset": depth_offset,
            "survey_id": survey_id,
            "survey_name": survey_name,
            "platform_type": platform_type,
            "platform_name": platform_name,
            "platform_code_ICES": platform_code_ICES
        }

        twin_properties.update(properties)

    def twin_update_callback(update):
        """
        Callback function to handle updates to module twin properties.
        """
        global twin_properties
        logger.info(f"Received module twin update: {update}")

        # Access the desired properties from the twin update
        with twin_properties_lock:
            desired_properties = update.get('desired', {})
            logger.info(f"Updated twin desired properties: {desired_properties}")

            twin_properties.update(desired_properties)
            client.patch_twin_reported_properties(desired_properties)

    # Set the input message handler and pass the client instance
    client.on_message_received = lambda message: handle_input_message(client, message)

    client.on_twin_desired_properties_patch_received = twin_update_callback

    # Set up signal handler for module termination
    def module_termination_handler(signal, frame):
        logger.info("IoTHubClient sample stopped by Edge")
        stop_event.set()

    signal.signal(signal.SIGTERM, module_termination_handler)

    # Run the sample
    loop = asyncio.get_event_loop()
    try:
        logger.info("Starting IoT Hub client...")
        loop.run_forever()
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise
    finally:
        logger.info("Shutting down IoT Hub Client...")
        loop.run_until_complete(client.shutdown())
        loop.close()


if __name__ == "__main__":
    main()
