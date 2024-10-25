import os
import logging

from typing import Optional
from adlfs import AzureBlobFileSystem
from azure.storage.blob import BlobServiceClient, ContainerClient

# Initialize the logger
logger = logging.getLogger('oceanstream')


def create_blob_service_client(connect_str=None) -> BlobServiceClient:
    """
    Create an Azure Blob Storage client.

    Returns:
    - BlobServiceClient: The Azure Blob Storage client.
    """
    if connect_str is None:
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    if not connect_str:
        raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING environment variable not set.")

    return BlobServiceClient.from_connection_string(connect_str)


def get_azure_blob_filesystem() -> AzureBlobFileSystem:
    """
    Get Azure Blob FileSystem Mapper for Dask.

    Returns:
    - AzureBlobFileSystem: The Azure Blob FileSystem Mapper.
    """
    connect_str: Optional[str] = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if not connect_str:
        raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING environment variable not set.")

    return AzureBlobFileSystem(connection_string=connect_str)


def ensure_container_exists(blob_service_client: BlobServiceClient, container_name: str):
    """
    Ensure that the specified container exists in Azure Blob Storage.

    Parameters:
    - blob_service_client: BlobServiceClient
        The Azure Blob Storage client.
    - container_name: str
        The name of the container to check or create.
    """
    try:
        # Get the container client
        container_client = blob_service_client.get_container_client(container_name)

        # Check if the container exists
        if not container_client.exists():
            logger.info(f"Container '{container_name}' does not exist. Creating container...")
            container_client.create_container()
            logger.info(f"Container '{container_name}' created successfully.")
        else:
            logger.info(f"Container '{container_name}' already exists.")

    except Exception as e:
        logger.error(f"Error ensuring container '{container_name}' exists: {e}", exc_info=True)
        raise

