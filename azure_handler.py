# azure_handler.py
import json
import logging

from azure.iot.device.aio import IoTHubModuleClient, IoTHubDeviceClient


from echosounder_processor import check_and_process_files

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
                await check_and_process_files(client, dict_obj["filename"])
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
# def create_client(connection_string=None):
    # """
    # Create an IoT Hub client, either for Edge or standalone based on a provided connection string.
    # If no connection string is provided, it assumes it's running as an IoT Edge module.
    # """
    # async def receive_message_handler( message):
        # """
        # Handle incoming messages.
        # """
        # if message.input_name == "input1":
            # byte_str = message.data
            # dict_obj = json.loads(byte_str.decode("utf-8"))
            
            # message_type = dict_obj.get("type", None)
            
            # if message_type == "rawFileAdded":
                # await handle_raw_file_added(client, dict_obj)
            # elif message_type == "userRequest":
                # await handle_user_request(client, dict_obj)
            # else:
                # await handle_unhandled(message)
    # # If a connection string is provided, create a standalone device client
    # if connection_string:
        # client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    # else:
        # # Otherwise, assume we're running as an IoT Edge module
        # client = IoTHubModuleClient.create_from_edge_environment()

    # client.on_message_received = receive_message_handler
    # return client

async def handle_raw_file_added(client, dict_obj):
    # Handle rawFileAdded type message
    filename = dict_obj["filename"]
    await check_and_process_files(client, filename)

async def handle_user_request(client, dict_obj):
    # Handle configUpdate type message
    # For demonstration purposes, let's just log the new config
    new_config = dict_obj
    logging.info(f"Received new user request: {new_config}")

async def handle_unhandled(message):
    print("Wrong message type")
    logging.warn(f"No capability for this message type: {message.data}")