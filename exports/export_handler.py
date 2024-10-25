import logging
import os
from pathlib import Path
from typing import Dict, Any, List
from azure.iot.device import IoTHubModuleClient
from azure.storage.blob import BlobServiceClient, ContentSettings
from .plot import plot_sv_data

logger = logging.getLogger('oceanstream')


def send_to_iot_hub(client: IoTHubModuleClient, data=None, properties=None, output_name: str = None):
    """
    Send data to Azure IoT Hub using IoT Edge messages.

    Parameters:
    - client: IoTHubModuleClient
        The IoT Hub client.
    - data: Dict[str, Any]
        The data to send to Azure IoT Hub.
    - output_name: str
        The output route name defined in the IoT Edge deployment manifest.
    """
    from azure_handler.message_handler import send_to_hub
    send_to_hub(client, data, properties, output_name)


def plot_and_upload_echograms(sv_dataset, survey_id: str, file_base_name: str, blob_service_client: BlobServiceClient,
                              container_name: str):
    """
    Plot echograms for Sv data and upload them to Azure Blob Storage.

    Parameters:
    - sv_dataset: xr.Dataset
        The dataset containing Sv data.
    - file_base_name: str
        The base name for output files.
    - blob_service_client: BlobServiceClient
        The Azure Blob Storage client.
    - container_name: str
        The name of the Azure Blob Storage container to store echograms.
    """
    logger.info(f'Plotting Sv data and saving echograms...')
    os.makedirs('echograms', exist_ok=True)
    echogram_files = plot_sv_data(sv_dataset, file_base_name=file_base_name,
                                  output_path='./output', echogram_path='./echograms')

    logger.info('Uploading echograms to Azure Blob Storage...')
    upload_files_to_blob(blob_service_client, survey_id, file_base_name, echogram_files, container_name=container_name)

    uploaded_files = [f"{survey_id}/{file_base_name}/{str(Path(e).name)}" for e in echogram_files]

    return uploaded_files


def upload_files_to_blob(blob_service_client: BlobServiceClient, survey_id: str, file_base_name: str, files: List[str],
                         container_name: str):
    """
    Upload files to the specified Azure Blob Storage container.

    Parameters:
    - blob_service_client: BlobServiceClient
        The Azure Blob Storage client.
    - files: List[str]
        The list of file paths to upload.
    - container_name: str
        The name of the Azure Blob Storage container.
    """
    try:
        for file_path in files:
            blob_name = f"{survey_id}/{file_base_name}/{os.path.basename(file_path)}"
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

            with open(file_path, "rb") as data:
                content_settings = ContentSettings(content_type='image/png')
                blob_client.upload_blob(data, overwrite=True, content_settings=content_settings)

            logger.info(f'File {file_path} uploaded to container {container_name} as blob {blob_name}.')

    except Exception as e:
        logger.error(f"Error uploading files to Azure Blob Storage: {e}", exc_info=True)
