import json
import os
import sys
import time
import traceback
import logging
import warnings
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from dask.distributed import Client, LocalCluster

from oceanstream.azure_handler.message_handler import default_serializer
from oceanstream.process import enrich_sv_dataset, convert_raw_to_zarr, compute_Sv_and_save
from oceanstream.exports import select_location_points, create_instrument_metadata, \
    extract_location_data, create_location_message, plot_sv_data, generate_processing_report


load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
warnings.filterwarnings("ignore", module="echopype")
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

STANDARD_CHANNEL_NAMES = {
    18000: "18 kHz",
    38000: "38 kHz",
    70000: "70 kHz",
    120000: "120 kHz",
    200000: "200 kHz"
}

CHUNKS = {
    "ping_time": 100,
    "range_sample": 100
}

DB_NAME = os.getenv('DB_NAME', 'downloads.db')
RAW_SOURCE_DIR = os.getenv('RAW_SOURCE_DIR')
SONAR_MODEL = os.getenv('SONAR_MODEL', 'EK60')
CONVERTED_DIR = os.getenv('CONVERTED_DIR')
OUTPUT_DIR = os.getenv('OUTPUT_DIR')
MAX_RETRIES = 3  # Maximum number of retries for each processing
RETRY_DELAY = 5  # Delay between retries in seconds

failed_files = []


def list_files_in_directory(directory):
    raw_files = []
    for root, _, files in os.walk(directory):
        for file_name in files:
            if file_name.endswith('.raw'):
                file_path = os.path.join(root, file_name)
                size = os.path.getsize(file_path)
                last_modified = time.ctime(os.path.getmtime(file_path))
                folder = os.path.dirname(file_path)

                raw_files.append({
                    'name': file_name,
                    'folder': folder,
                    'size': size,
                    'path': file_path,
                    'last_modified': last_modified
                })

    logging.info(f'Total number of raw files: {len(raw_files)}')

    return raw_files


def send_to_iot_hub(client, data=None, properties=None, output_name='output1'):
    print(json.dumps(data, default=default_serializer, indent=2))


def process_file(file_info, client=None):

    try:
        logging.info(f'Starting processing of {file_info["name"]}')

        # Step 1: Read the raw file using echopype
        raw_file_path = file_info['path']
        file_name = file_info['name']
        start_time = time.time()
        survey_id = 'HB1907'
        echodata, base_file_name, converted_zarr_path = convert_raw_to_zarr(raw_file_path, survey_id,
                                                                            save_path=CONVERTED_DIR)

        sv_dataset, sv_zarr_path = compute_Sv_and_save(echodata, dataset_id=base_file_name, save_path=OUTPUT_DIR, depth_offset=5)

        ping_times = sv_dataset.coords['ping_time'].values
        ping_times_index = pd.DatetimeIndex(ping_times)
        day_date = ping_times_index[0].date()
        total_recording_time = (ping_times_index[-1] - ping_times_index[0]).total_seconds()
        first_ping_time = ping_times_index[0].time()

        print(f'File {file_info["name"]} recorded on {day_date} from {first_ping_time} for {total_recording_time}')
        dataset_id = base_file_name
        json.dumps({
            "file_name": file_info["name"],
            "dataset_id": base_file_name,
            "zarr_path_converted": converted_zarr_path,
            "zarr_path_sv": sv_zarr_path,
            "date": day_date.isoformat()
        }, default=default_serializer, indent=2)

        instrument_metadata = create_instrument_metadata(echodata, sv_dataset)

        for freq_name, freq_data in instrument_metadata.items():
            channel_payload = {freq_name: freq_data}
            send_to_iot_hub(client, channel_payload, output_name="output1")

        gps_data = extract_location_data(sv_dataset)
        selected_points = select_location_points(gps_data, 5)

        # Send each selected point to IoT Hub
        for idx, point in selected_points.iterrows():
            message = create_location_message(point)
            send_to_iot_hub(client, message, output_name="output1")

        # Create Sv dataset with depth dimension:
        ds_Sv_with_depth_dim = sv_dataset.copy()
        depth_1d = ds_Sv_with_depth_dim["depth"].isel(channel=0, ping_time=0)
        ds_Sv_with_depth_dim["depth"] = depth_1d
        ds_Sv_with_depth_dim = ds_Sv_with_depth_dim.swap_dims({"range_sample": "depth"})

        echogram_path = os.path.join(OUTPUT_DIR, base_file_name, 'echograms')
        os.makedirs(echogram_path, exist_ok=True)
        echogram_files = plot_sv_data(ds_Sv_with_depth_dim, file_base_name=base_file_name,
                                      output_path='./output', echogram_path=echogram_path)

        uploaded_files = [f"{base_file_name}/{str(Path(e).name)}" for e in echogram_files]

        processing_time_ms = int((time.time() - start_time) * 1000)
        ping_times = sv_dataset.coords['ping_time'].values
        ping_times_index = pd.DatetimeIndex(ping_times)
        day_date = ping_times_index[0].date()
        total_recording_time = (ping_times_index[-1] - ping_times_index[0]).total_seconds()
        first_ping_time = ping_times_index[0].time()

        file_npings = len(sv_dataset["ping_time"].values)
        file_nsamples = len(sv_dataset["range_sample"].values)
        file_start_time = str(sv_dataset["ping_time"].values[0])
        file_end_time = str(sv_dataset["ping_time"].values[-1])
        file_freqs = ",".join(map(str, sv_dataset["frequency_nominal"].values))
        file_start_depth = str(sv_dataset["range_sample"].values[0])
        file_end_depth = str(sv_dataset["range_sample"].values[-1])
        file_start_lat = echodata["Platform"]["latitude"].values[0]
        file_start_lon = echodata["Platform"]["longitude"].values[0]
        file_end_lat = echodata["Platform"]["latitude"].values[-1]
        file_end_lon = echodata["Platform"]["longitude"].values[-1]
        start_time = first_ping_time.isoformat()
        gps_data_records = gps_data.to_dict(orient="records")

        logging.info(f'Finished processing of {file_info["name"]}')

        payload = {
            "file_name": file_name,
            "zarr_path_converted": converted_zarr_path,
            "zarr_path_sv": sv_zarr_path,
            "date": day_date.isoformat(),
            "duration": total_recording_time,

            "file_npings": file_npings,
            "file_nsamples": file_nsamples,
            "file_start_time": file_start_time,
            "file_end_time": file_end_time,
            "file_freqs": file_freqs,
            "file_start_depth": file_start_depth,
            "file_end_depth": file_end_depth,
            "file_start_lat": file_start_lat,
            "file_start_lon": file_start_lon,
            "file_end_lat": file_end_lat,
            "file_end_lon": file_end_lon,
            "start_time": start_time,
            "dataset_id": dataset_id,
            "campaign_id": survey_id,
            "processing_time_ms": processing_time_ms,
            "gps_data": gps_data_records
        }

        payload_for_pdf = {
            "metadata": {
                "Survey ID": survey_id,
                "Sonar Model": 'EK60',
                "Platform Name": 'R/V Falkor',
                "Platform Type": 'Vessel',
                "Platform Code ICES": 'FALKOR',
                "Survey Summary": 'This is a test survey',
            },
            "parameters": {
                "Waveform Mode": 'CW',
                "Encode Mode": 'power',
                "Depth Offset": 5,
            },
            "results": {
                "Converted Zarr Path": converted_zarr_path,
                "Sv Zarr Path": sv_zarr_path,
                "Processing Date": day_date.isoformat(),
                "Total Recording Duration (seconds)": total_recording_time,

                "Total Number of Pings": file_npings,
                "Total Number of Samples per Ping": file_nsamples,
                "Recording Start Time": file_start_time,
                "Recording End Time": file_end_time,
                "Nominal Frequencies (Hz)": file_freqs,
                "Start Depth (meters)": file_start_depth,
                "End Depth (meters)": file_end_depth,
                "Start Latitude": file_start_lat,
                "Start Longitude": file_start_lon,
                "End Latitude": file_end_lat,
                "End Longitude": file_end_lon,
                "First Ping Timestamp": start_time,
                "Dataset ID": dataset_id,
                "Campaign ID": survey_id,
                "Total Processing Time (milliseconds)": processing_time_ms
            },
            "gps_data": gps_data_records,
            "processing_time_ms": 12345,
            "processing_time_ms_echograms": 5000
        }

        generate_processing_report(file_name, payload_for_pdf, 'pdf_reports')

        return file_info['path']

    except Exception as e:
        logging.error(f'Failed to process {file_info["name"]}: {e}')
        traceback.print_exc()
        failed_files.append(file_info['name'])
        return None


def main():
    num_cores = os.cpu_count()
    # Initialize Dask Local Cluster with the default number of cores
    cluster = LocalCluster(n_workers=num_cores, threads_per_worker=1)
    client_dask = Client(cluster)

    # List raw files in the source directory
    files = list_files_in_directory(RAW_SOURCE_DIR)
    if not files:
        return

    # Create Dask bag to parallelize the processing
    # bag = db.from_sequence(files)
    # processing_results = bag.map(lambda file_info: process_file(file_info)).compute()
    process_file(files[0])

    # Display failed files at the end
    if failed_files:
        logging.error(f'The following files failed to process: {failed_files}')
    else:
        logging.info('All files processed successfully.')

    # Shut down the Dask client
    client_dask.close()
    cluster.close()


if __name__ == "__main__":
    main()
