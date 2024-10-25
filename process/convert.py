import logging
import os

from azure.storage.blob import BlobServiceClient
from adlfs import AzureBlobFileSystem
from pathlib import Path

# Initialize the logger
logger = logging.getLogger('oceanstream')


def convert_raw_to_zarr(raw_file_path: str, survey_id: str, container_name: str = None, sonar_model: str = 'EK60',
                        save_path: str = None):
    """
    Convert raw hydroacoustic data to Zarr format and upload to Azure Blob Storage.

    Parameters:
    - blob_service_client: BlobServiceClient
        The Azure Blob Storage client.
    - raw_file_path: str
        Path to the raw file.
    - container_name: str
        The name of the Azure Blob Storage container to store converted data.

    Returns:
    - str: The path to the converted Zarr file.
    """
    from echopype.convert.api import open_raw

    try:
        # Extract the base file name without extension
        base_file_name = Path(raw_file_path).stem
        echodata = open_raw(raw_file_path, sonar_model=sonar_model)  # type: ignore
        converted_zarr_path = f"{base_file_name}.zarr"

        if container_name is not None:
            zarr_path = f"{container_name}/{survey_id}/{converted_zarr_path}"

            azfs = AzureBlobFileSystem(connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'))
            zarr_store = azfs.get_mapper(zarr_path)

            logger.info(f"Saving converted data to Zarr format at: {zarr_path}")
            echodata.to_zarr(save_path=zarr_store, overwrite=True)
            logger.info(f'Converted data uploaded successfully to Azure Blob Storage.')
        elif save_path is not None:
            converted_zarr_path = os.path.join(save_path, converted_zarr_path)
            echodata.to_zarr(save_path=converted_zarr_path, overwrite=True)
            logger.info(f'Converted data saved successfully.')

        return echodata, base_file_name, converted_zarr_path

    except Exception as e:
        logger.error(f"Error converting raw data to Zarr format: {e}", exc_info=True)
        raise


def upload_file_to_blob(blob_service_client: BlobServiceClient, container_name: str, file_path: str,
                        blob_name: str) -> None:
    """
    Upload a file to Azure Blob Storage.

    Parameters:
    - blob_service_client: BlobServiceClient
        The Azure Blob Storage client.
    - container_name: str
        The name of the Azure Blob Storage container.
    - file_path: str
        The local path to the file to upload.
    - blob_name: str
        The name for the blob in Azure Storage.
    """
    try:
        # Create a blob client
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        # Upload the file
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logger.info(f'File {file_path} uploaded to container {container_name} as blob {blob_name}.')

    except Exception as e:
        logger.error(f"Error uploading file to Azure Blob Storage: {e}", exc_info=True)
        raise
