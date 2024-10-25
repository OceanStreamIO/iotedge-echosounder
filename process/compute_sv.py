import logging
import os
from adlfs import AzureBlobFileSystem
from xarray import Dataset
from .sv_enrichment import enrich_sv_dataset

# Initialize the logger
logger = logging.getLogger('oceanstream')


def compute_Sv_and_save(echodata, survey_id: str = None, dataset_id: str = None, container_name: str = None,
                        waveform_mode: str = 'CW', encode_mode: str = 'power', depth_offset: float = 0,
                        save_path: str = None) -> tuple[Dataset, str]:
    """
    Compute volume backscattering strength (Sv) from the converted Zarr data and save the result in Azure Blob Storage.

    Parameters:
    - blob_service_client: BlobServiceClient
        The Azure Blob Storage client.
    - echodata: str
        Path to the converted Zarr file.
    - container_name: str
        The name of the Azure Blob Storage container to store processed data.

    Returns:
    - str: The path to the saved Sv Zarr file.
    """
    from echopype.calibrate import compute_Sv

    try:
        # Compute Sv
        logger.info('Computing Sv from converted Zarr data...')
        sv_dataset = compute_Sv(echodata, waveform_mode=waveform_mode, encode_mode=encode_mode).compute()
        sv_dataset = enrich_sv_dataset(sv_dataset, echodata, depth_offset=depth_offset)

        sv_zarr_path = f"{dataset_id}/{dataset_id}_Sv.zarr"

        if container_name is not None:
            logger.info(f'Uploading computed Sv dataset to Azure Blob Storage: {sv_zarr_path}')
            upload_Sv_to_blob(sv_dataset, survey_id, sv_zarr_path, container_name)

            logger.info('Sv data uploaded successfully to Azure Blob Storage.')
        elif save_path is not None:
            sv_zarr_path = os.path.join(save_path, dataset_id, sv_zarr_path)
            sv_dataset.to_zarr(store=sv_zarr_path, mode='w')
            logger.info(f'Saved Sv dataset to Zarr format at: {sv_zarr_path}')

        return sv_dataset, sv_zarr_path

    except Exception as e:
        logger.error(f"Error computing Sv: {e}", exc_info=True)
        raise


def upload_Sv_to_blob(sv_dataset, survey_id: str, sv_zarr_path: str, container_name: str) -> None:
    """
    Upload the computed Sv dataset to Azure Blob Storage.

    Parameters:
    - blob_service_client: BlobServiceClient
        The Azure Blob Storage client.
    - sv_dataset: xarray.Dataset
        The computed Sv dataset.
    - sv_zarr_path: str
        The path to save the Sv Zarr file.
    - container_name: str
        The name of the Azure Blob Storage container.
    """
    try:
        # Initialize the Azure Blob FileSystem Mapper
        azfs = AzureBlobFileSystem(connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'))
        chunk_store = azfs.get_mapper(f"{container_name}/{survey_id}/{sv_zarr_path}")

        # Save Sv dataset to Zarr format on Azure Blob Storage
        sv_dataset.to_zarr(store=chunk_store, mode='w')
        logger.info(f'Sv dataset uploaded as Zarr file to Azure Blob Storage: {sv_zarr_path}.')

    except Exception as e:
        logger.error(f"Error uploading Sv dataset to Azure Blob Storage: {e}", exc_info=True)
        raise
