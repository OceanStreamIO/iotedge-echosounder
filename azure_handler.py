import json
import logging

from azure.iot.device.aio import IoTHubModuleClient, IoTHubDeviceClient
from azure.iot.device import Message
from echosounder_processor import process_file

def create_client(connection_string=None):
    """Create an IoT Hub client, either for Edge or standalone."""

    async def receive_message_handler(message):
        if message.input_name == "input1":
            logging.info("Received rawFileAdded event on input1:", message.data)
            byte_str = message.data
            dict_obj = json.loads(byte_str.decode("utf-8"))
            message_type = dict_obj.get("event", None)
            if message_type == "fileadd":
                # Trigger the file checking and processing
                message_list = await process_file(dict_obj["filename"])
                for message in message_list:
                    await send_to_hub(client, message)
            elif message_type == "userrequest":
                await handle_user_request(client, dict_obj)
            else:
                await handle_unhandled(message)


    # If a connection string is provided, create a standalone device client
    if connection_string:
        client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    # Otherwise, assume we're running as an IoT Edge module
    else:
        client = IoTHubModuleClient.create_from_edge_environment()

    client.on_message_received = receive_message_handler
    return client


async def handle_raw_file_added(client, dict_obj):
    # Handle rawFileAdded type message
    filename = dict_obj["filename"]
    await process_file(client, filename)

async def handle_user_request(client, dict_obj):
    # Handle configUpdate type message
    # For demonstration purposes, let's just log the new config
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