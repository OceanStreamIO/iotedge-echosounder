import asyncio
import logging
import signal
import sys
import os
import threading
from azure_handler import create_client

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
# Event indicating client stop
stop_event = threading.Event()


# Signal handler for intreruptions and closing
def signal_handler(signal, frame):
    logging.info(f"Received signal {signal}. Shutting down...")
    stop_event.set()

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

def main():
    logging.info("IoT Hub Client for Python")
    # Create the Azure client
    if len(sys.argv) > 1:
        connection_string = sys.argv[1]
        client = create_client(connection_string)
    else:
        client = create_client()
    loop = asyncio.get_event_loop()
    print("Echosounder loop ready")
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logging.exception("Unexpected error occurred.", e)
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