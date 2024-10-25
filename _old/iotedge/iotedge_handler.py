import os
import json
import logging
import asyncio
import signal
from pathlib import Path


from oceanstream.settings import load_config
from dotenv import load_dotenv
from azure.iot.device import IoTHubModuleClient, ProvisioningDeviceClient, Message
from azure.iot.device.aio import IoTHubModuleClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# from echosounder_processor import process_file
load_dotenv()
logger = logging.getLogger('echosounder')

DEVICE_CONNECTION_STRING = os.getenv('DEVICE_CONNECTION_STRING')
EDGE_STORAGE_CONNECTION_STRING = os.getenv('EDGE_STORAGE_CONNECTION_STRING')
EDGE_STORAGE_CONTAINER = os.getenv('EDGE_STORAGE_CONTAINER')
LOCAL_ENV = os.getenv('LOCAL_ENV')
IOT_CENTRAL_SYMMETRIC_KEY = os.getenv('IOT_CENTRAL_SYMMETRIC_KEY')
IOT_CENTRAL_ID_SCOPE = os.getenv('IOT_CENTRAL_ID_SCOPE')
IOT_CENTRAL_REGISTRATION_ID = os.getenv('IOT_CENTRAL_REGISTRATION_ID')

ProvisioningHost = 'global.azure-devices-provisioning.net'


def provision_iot_central_device():
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


def connect_fn(conn_str=None):
    try:
        if LOCAL_ENV:
            print('Running in local environment using device connection string.')

            if IOT_CENTRAL_REGISTRATION_ID and IOT_CENTRAL_SYMMETRIC_KEY and IOT_CENTRAL_ID_SCOPE:
                logger.info('Using IoT Central connection...')
                conn_str = provision_iot_central_device()
            else:
                if not conn_str:
                    raise ValueError('Missing DEVICE_CONNECTION_STRING environment variable.')
            client = IoTHubModuleClient.create_from_connection_string(conn_str)
        else:
            print("Creating IoTHubModuleClient from edge environment.")
            print(f"IOTEDGE_IOTHUBHOSTNAME: {os.getenv('IOTEDGE_IOTHUBHOSTNAME')}")
            print(f"IOTEDGE_DEVICEID: {os.getenv('IOTEDGE_DEVICEID')}")
            print(f"IOTEDGE_MODULEID: {os.getenv('IOTEDGE_MODULEID')}")
            print(f"IOTEDGE_GATEWAYHOSTNAME: {os.getenv('IOTEDGE_GATEWAYHOSTNAME')}")

            client = IoTHubModuleClient.create_from_edge_environment()

        if client is None:
            raise ValueError('Failed to create IoTHubModuleClient. The client is None.')

        logger.info('IoT Hub module client initialized')
        return client
    except Exception as e:
        logger.error('Could not connect: %s', e)
        raise


def create_client():
    client = IoTHubModuleClient.create_from_edge_environment()

    # Define function for handling received messages
    def receive_message_handler(message):
        print('Received message from IoT Hub', message)

        if message.input_name == "rawfileadded":
            print('Received message from raw_file_added')
            byte_str = message.data
            dict_obj = json.loads(byte_str.decode("utf-8"))
            message_type = dict_obj.get("event", None)

            print('dict_obj', dict_obj)
            print('message_type', message_type)

            if message_type == "fileadd":
                result = process_raw_file(dict_obj["file_added_path"])
                print('message_list', result)

                # for message in message_list:
                #     await send_to_hub(client, message)
            # elif message_type == "user_request":
            #     await handle_user_request(client, dict_obj)
            # else:
            #     await handle_unhandled(message)

    try:
        # Set handler on the client
        client.on_message_received = receive_message_handler
    except e:
        logger.error(f"Error setting message handler: {e}")
        client.shutdown()
        raise

    return client


def init_config(settings, file_path):
    config_data = load_config(settings["config"])
    config_data["raw_path"] = file_path

    if 'sonar_model' in settings and settings["sonar_model"] is not None:
        config_data["sonar_model"] = settings["sonar_model"]

    return config_data


def process_raw_file(source, config_file, twin_properties=None):
    logger.info(f"Processing file: {source}")
    settings = {
        "config": config_file
    }

    filePath = Path(source)
    configData = init_config(settings, filePath)
    configData["sonar_model"] = "EK80"
    configData["output_folder"] = Path(configData["output_folder"]) / 'raw_data'
    configData["cloud_storage"] = {
        "container_name": EDGE_STORAGE_CONTAINER,
        "storage_type": "azure",
        "storage_options": {
            "connection_string": EDGE_STORAGE_CONNECTION_STRING
        }
    }

    print(configData)
    # output_path = convert_raw_file(filePath, configData)
    output_path = filePath

    return [{"filename": filePath, "output_path": output_path, "event": "file_processed"}]


def write_to_blob_storage(file_path):
    blob_service_client = BlobServiceClient.from_connection_string(EDGE_STORAGE_CONNECTION_STRING)


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
