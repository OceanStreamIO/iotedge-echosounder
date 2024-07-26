import asyncio
import logging
import signal
import sys
import threading
from dotenv import load_dotenv
from iotedge import create_client

load_dotenv()
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
# Event indicating client stop
stop_event = threading.Event()


# Signal handler for interruptions and closing
def signal_handler(signal, frame):
    logging.info(f"Received signal {signal}. Shutting down...")
    stop_event.set()


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def main():
    loop = asyncio.get_event_loop()
    client = loop.run_until_complete(create_client())

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
