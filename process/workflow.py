import logging
import os
import time
from typing import Dict, Any, List
from pathlib import Path

import pandas as pd

from azure_handler import create_blob_service_client, ensure_container_exists
from process.convert import convert_raw_to_zarr
from process.compute_sv import compute_Sv_and_save
from azure.iot.device import IoTHubModuleClient
from exports import send_to_iot_hub, plot_and_upload_echograms, select_location_points, \
    create_location_message, create_instrument_metadata, extract_location_data

# Initialize the logger
logger = logging.getLogger('oceanstream')

CONVERTED_CONTAINER_NAME = os.getenv('CONVERTED_CONTAINER_NAME', 'converted')
ECHOGRAM_CONTAINER_NAME = os.getenv('ECHOGRAM_CONTAINER_NAME', 'echograms')
PROCESSED_CONTAINER_NAME = os.getenv('PROCESSED_CONTAINER_NAME', 'processed')


def process_raw_file(client: IoTHubModuleClient, file_path: str, twin_properties) -> Dict[str, Any]:
    """
    Orchestrate the entire processing workflow for a raw hydroacoustic file.

    Parameters:
    - client: IoTHubModuleClient
        The IoT Hub client instance to send messages.
    - file_path: str
        The path to the raw file.

    Returns:
    - List[Dict[str, Any]]: A list of results after processing.
    """
    try:
        logger.info(f"Starting processing for raw file: {file_path}")
        sonar_model = twin_properties.get("sonar_model", "EK60")
        waveform_mode = twin_properties.get("waveform_mode", "CW")
        encode_mode = twin_properties.get("encode_mode", "power")
        depth_offset = twin_properties.get("depth_offset", 0)

        survey_id = twin_properties.get("survey_id", "")
        survey_name = twin_properties.get("survey_name", "")
        platform_type = twin_properties.get("platform_type", "")
        platform_name = twin_properties.get("platform_name", "")
        platform_code_ICES = twin_properties.get("platform_code_ICES", "")

        survey_name = survey_name.get("value", "")
        sonar_model = sonar_model.get("value", "EK60")
        waveform_mode = waveform_mode.get("value", "CW")
        encode_mode = encode_mode.get("value", "power")
        depth_offset = depth_offset.get("value", 0)
        if isinstance(depth_offset, str):
            if depth_offset == "":
                depth_offset = 0
            depth_offset = float(depth_offset)

        survey_id = survey_id.get("value", "")
        platform_type = platform_type.get("value", "")
        platform_name = platform_name.get("value", "")
        platform_code_ICES = platform_code_ICES.get("value", "")

        # Step 1: Create the Azure Blob Storage client
        logger.info('Creating Azure Blob Storage client...')
        blob_service_client = create_blob_service_client()

        ensure_container_exists(blob_service_client, CONVERTED_CONTAINER_NAME)
        ensure_container_exists(blob_service_client, ECHOGRAM_CONTAINER_NAME)
        ensure_container_exists(blob_service_client, PROCESSED_CONTAINER_NAME)

        start_time = time.time()
        echodata, base_file_name, converted_zarr_path = convert_raw_to_zarr(file_path,
                                                                            survey_id=survey_id,
                                                                            sonar_model=sonar_model,
                                                                            container_name=CONVERTED_CONTAINER_NAME)

        echodata['Top-level'].attrs['title'] = f"{survey_name} [{survey_id}], file {base_file_name}"
        echodata['Top-level'].attrs['summary'] = (
            f"EK60 raw file {base_file_name} from the {survey_name} [{survey_id}], converted to a SONAR-netCDF4 file "
            f"using echopype."
        )

        # -- SONAR-netCDF4 Platform Group attributes
        # Per SONAR-netCDF4, for platform_type see https://vocab.ices.dk/?ref=311
        echodata['Platform'].attrs['platform_type'] = platform_type
        echodata['Platform'].attrs['platform_name'] = platform_name
        echodata['Platform'].attrs['platform_code_ICES'] = platform_code_ICES

        dataset_id = base_file_name

        logger.info(f'Computing Sv for converted file')
        sv_dataset, sv_zarr_path = compute_Sv_and_save(echodata,
                                                       dataset_id=base_file_name,
                                                       waveform_mode=waveform_mode,
                                                       survey_id=survey_id,
                                                       encode_mode=encode_mode,
                                                       depth_offset=depth_offset,
                                                       container_name=PROCESSED_CONTAINER_NAME)

        logger.info('Extracting metadata from the processed data...')
        try:
            instrument_metadata = create_instrument_metadata(echodata, sv_dataset)
        except Exception as e:
            logger.error(f"Error extracting instrument metadata: {e}", exc_info=True)
            instrument_metadata = {}

        logger.info('Sending metadata to Azure IoT Hub...')
        for freq_name, freq_data in instrument_metadata.items():
            channel_payload = {freq_name: freq_data, "dataset_id": dataset_id, "campaign_id": survey_id}
            send_to_iot_hub(client, data=channel_payload, output_name="output1")

        file_path_obj = Path(file_path)
        file_base_name = file_path_obj.stem
        gps_data = process_location_data(client, sv_dataset)

        # Calculate processing times
        processing_time_ms = int((time.time() - start_time) * 1000)
        ping_times = sv_dataset.coords['ping_time'].values
        ping_times_index = pd.DatetimeIndex(ping_times)
        day_date = ping_times_index[0].date()
        total_recording_time = (ping_times_index[-1] - ping_times_index[0]).total_seconds()
        first_ping_time = ping_times_index[0].time()

        payload = {
            "file_name": file_path_obj.name,
            "zarr_path_converted": converted_zarr_path,
            "zarr_path_sv": sv_zarr_path,
            "date": day_date.isoformat(),
            "duration": total_recording_time,

            "file_npings": len(sv_dataset["ping_time"].values),
            "file_nsamples": len(sv_dataset["range_sample"].values),
            "file_start_time": str(sv_dataset["ping_time"].values[0]),
            "file_end_time": str(sv_dataset["ping_time"].values[-1]),
            "file_freqs": ",".join(map(str, sv_dataset["frequency_nominal"].values)),
            "file_start_depth": str(sv_dataset["range_sample"].values[0]),
            "file_end_depth": str(sv_dataset["range_sample"].values[-1]),
            "file_start_lat": echodata["Platform"]["latitude"].values[0],
            "file_start_lon": echodata["Platform"]["longitude"].values[0],
            "file_end_lat": echodata["Platform"]["latitude"].values[-1],
            "file_end_lon": echodata["Platform"]["longitude"].values[-1],
            "start_time": first_ping_time.isoformat(),
            "dataset_id": dataset_id,
            "campaign_id": survey_id,
            "processing_time_ms": processing_time_ms,
            "gps_data": gps_data.to_dict(orient="records")
        }

        send_to_iot_hub(client, payload, output_name="output1")

        payload_for_ml = {
            "file_name": file_path_obj.name,
            "sv_zarr_path": sv_zarr_path,
            "campaign_id": survey_id,
            "dataset_id": dataset_id,
            "depth_offset": depth_offset,
            "date": day_date.isoformat()
        }
        send_to_iot_hub(client, payload_for_ml, output_name="outputml")

        echogram_files = generate_and_upload_echograms(blob_service_client,
                                                       file_base_name=file_base_name,
                                                       survey_id=survey_id,
                                                       sv_dataset=sv_dataset)
        echograms_payload = {
            "echogram_files": echogram_files,
            "file_name": file_path_obj.name,
            "dataset_id": dataset_id,
            "campaign_id": survey_id
        }
        send_to_iot_hub(client, echograms_payload, output_name="output1")
        
        logger.info(f'Processing completed for file: {file_path}')

        return {"filename": file_path, "output_path": sv_zarr_path, "event": "file_processed"}

    except Exception as e:
        logger.error(f"Error during processing of file {file_path}: {e}", exc_info=True)


def generate_and_upload_echograms(blob_service_client, file_base_name=None, survey_id=None, sv_dataset=None):
    logger.info('Plotting and uploading echograms...')

    try:
        echogram_files = plot_and_upload_echograms(sv_dataset,
                                                   survey_id=survey_id,
                                                   file_base_name=file_base_name,
                                                   blob_service_client=blob_service_client,
                                                   container_name=ECHOGRAM_CONTAINER_NAME)
    except Exception as e:
        logger.error(f"Error plotting and uploading echograms: {e}", exc_info=True)
        echogram_files = []
    return echogram_files


def process_location_data(client, sv_dataset):
    try:
        gps_data = extract_location_data(sv_dataset)

        selected_points = select_location_points(gps_data, 5)
        for idx, point in selected_points.iterrows():
            message = create_location_message(point)
            send_to_iot_hub(client, message, output_name="output1")
    except Exception as e:
        logger.error(f"Error extracting location metadata: {e}", exc_info=True)
        gps_data = []

    return gps_data

