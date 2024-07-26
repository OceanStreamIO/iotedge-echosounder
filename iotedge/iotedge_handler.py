import os
import json
import logging
import asyncio
import signal
from dotenv import load_dotenv
from azure.iot.device import IoTHubModuleClient, ProvisioningDeviceClient, Message
from azure.iot.device.aio import IoTHubModuleClient as AsyncIoTHubModuleClient

# from echosounder_processor import process_file
load_dotenv()
logger = logging.getLogger(__name__)


DEVICE_CONNECTION_STRING = os.getenv('DEVICE_CONNECTION_STRING')
LOCAL_ENV = os.getenv('LOCAL_ENV')
IOT_CENTRAL_SYMMETRIC_KEY = os.getenv('IOT_CENTRAL_SYMMETRIC_KEY')
IOT_CENTRAL_ID_SCOPE = os.getenv('IOT_CENTRAL_ID_SCOPE')
IOT_CENTRAL_REGISTRATION_ID = os.getenv('IOT_CENTRAL_REGISTRATION_ID')

ProvisioningHost = 'global.azure-devices-provisioning.net'

async def provision_iot_central_device():
    provisioning_device_client = ProvisioningDeviceClient.create_from_symmetric_key(
        symmetric_key=IOT_CENTRAL_SYMMETRIC_KEY,
        registration_id=IOT_CENTRAL_REGISTRATION_ID,
        id_scope=IOT_CENTRAL_ID_SCOPE,
        provisioning_host=ProvisioningHost
    )
    provisioning_device_client.provisioning_payload = {"a": "b"}

    try:
        result = provisioning_device_client.register()
        logger.info('Registration on IoT Central succeeded: %s', result)
        return f'HostName={result.registration_state.assigned_hub};DeviceId={IOT_CENTRAL_REGISTRATION_ID};SharedAccessKey={IOT_CENTRAL_SYMMETRIC_KEY}'
    except AttributeError as e:
        logger.error('Error registering device: %s', e)
        raise


async def connect_fn(conn_str=None):
    if LOCAL_ENV:
        print('Running in local environment using device connection string.')

        if IOT_CENTRAL_REGISTRATION_ID and IOT_CENTRAL_SYMMETRIC_KEY and IOT_CENTRAL_ID_SCOPE:
            logger.info('Using IoT Central connection...')
            conn_str = await provision_iot_central_device()
        else:
            if not conn_str:
                raise ValueError('Missing DEVICE_CONNECTION_STRING environment variable.')
        client = IoTHubModuleClient.create_from_connection_string(conn_str)
    else:
        client = await AsyncIoTHubModuleClient.create_from_environment()

    if client is None:
        raise ValueError('Failed to create IoTHubModuleClient. The client is None.')

    try:
        logger.info('IoT Hub module client initialized')
        return client
    except Exception as e:
        logger.error('Could not connect: %s', e)
        raise


async def create_client(connection_string=None):
    """Create an IoT Hub client, either for Edge or standalone."""
    async def receive_message_handler(message):
        if message.input_name == "raw_file_added":
            logging.info("Received rawFileAdded event:", message.data)
            byte_str = message.data
            dict_obj = json.loads(byte_str.decode("utf-8"))
            message_type = dict_obj.get("event", None)

            if message_type == "fileadd":
                message_list = await process_file(dict_obj["file_added_path"])

                for message in message_list:
                    await send_to_hub(client, message)
            elif message_type == "user_request":
                await handle_user_request(client, dict_obj)
            else:
                await handle_unhandled(message)

    client = await connect_fn(connection_string)
    client.on_message_received = receive_message_handler
    return client


async def process_file(filename):
    # Do something with the file
    print(f"Processing file: {filename}")
    return [{"filename": filename, "event": "file_processed"}]


async def handle_raw_file_added(client, dict_obj):
    filename = dict_obj["filename"]
    await process_file(client, filename)


async def handle_user_request(client, dict_obj):
    new_config = dict_obj
    logging.info(f"Received new user request: {new_config}")


async def handle_unhandled(message):
    print("Wrong message type")
    logging.warn(f"No capability for this message type: {message.data}")


async def send_to_hub(client, data_dict):
    # Convert the dictionary to a JSON string
    print(data_dict)
    json_data = json.dumps(data_dict)
    # Prepare the message
    msg = Message(json_data)
    await client.send_message_to_output(msg, "output1")
    logging.info(f"Sent message for file: {data_dict['filename']}")
