import asyncio
import gc
import datetime
import json
import logging
from pathlib import Path
import os
import sys
import numpy as np
import xarray as xr


from azure_messages import GENERIC_HEARTBEAT_MESSAGE as file_message

from oceanstream.echodata import (
    compute_sv_with_encode_mode,
    enrich_sv_dataset,
    read_file,
    regrid_dataset,
    write_processed,
)


from oceanstream.denoise import (
    apply_background_noise_removal,
    apply_noise_masks,
    apply_seabed_mask,
    create_masks,
)


from oceanstream.denoise import (
    apply_background_noise_removal,
    apply_noise_masks,
    apply_seabed_mask,
    create_masks,
)
from oceanstream.echodata import (
    compute_sv_with_encode_mode,
    enrich_sv_dataset,
    read_file,
    regrid_dataset,
    interpolate_sv,
    write_processed,
)

from oceanstream.exports import (
                            compute_and_write_nasc,
                            write_csv as write_shoals_csv
                            )
from oceanstream.exports.csv import export_raw_csv, export_Sv_csv
from oceanstream.exports.plot import plot_all_channels
from oceanstream.exports.nasc_computation import compute_per_dataset_nasc
from oceanstream.report import display_profiling_and_summary_info
from oceanstream.settings import load_config
from oceanstream.utils import attach_mask_to_dataset


from database_handler import DBHandler
from filter_configs import (false_seabed_params, 
                            seabed_params, 
                            process_parameters,
                            seabed_process_parameters,
                            shoal_process_parameters)

# Configurations
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIRECTORY_TO_PROC = os.path.join(BASE_DIR, "procdata")



def setup_database():
    db = DBHandler()
    db.setup_database()
    return db

def format_message(shoal):
    key_list = ['label','frequency','area',
            'Sv_mean','npings', 'nsamples', 'corrected_length', 
            'mean_range', 'start_range', 'end_range', 
            'start_time', 'end_time', 
            'start_lat', 'end_lat', 
            'start_lon', 'end_lon', 'nasc']
    formated_shoal_message = {}
    for key in shoal.keys():
        if  key == 'filename':
            formated_shoal_message[key] = shoal[key]
        elif key in key_list:
            formated_shoal_message["shoal_"+key.lower()] = shoal[key]
    return formated_shoal_message

def create_enriched_sv(echodata, encode_mode, sv_dataset):
    sv_enriched = enrich_sv_dataset(
        sv_dataset, echodata, waveform_mode="CW", encode_mode=encode_mode
    )
    return sv_enriched

async def process_file(filename):

    # Create a database handler instance and set up the database
    config = load_config()
    config["raw_path"] = Path(filename)
    config["export_csv"] = True
    config["output_folder"] = DIRECTORY_TO_PROC
    profiling_info = {}
    db = setup_database()
    start_processing = datetime.datetime.now()
    raw_path = Path(filename)
    output_dir = Path(DIRECTORY_TO_PROC)
    # Check if the file has been processed before
    if db.file_processed_before(filename):
        logging.info(f"File {filename} has already been processed. Skipping.")
        db.close()
        return {"Processing Warning":f"File {filename} has already been processed"}
    #check = file_integrity_checking(filename)
    #file_integrity = check.get("file_integrity", False)
    #if not file_integrity:
    #    return {"Processing Error":f"File {filename} could not usable!"}
    
    # Process the file using oceanstream package
    echodata, encode_mode =  read_file(
                             profiling_info=profiling_info, 
                             config=config
                             )
    print("Got raw data")
    logging.info("New raw file read")

    # Compute Sv with encode_mode and save to zarr
    sv_dataset = compute_sv_with_encode_mode(
        echodata, 
        encode_mode=encode_mode, 
        profiling_info=profiling_info, 
        config=config
    )
    
    write_processed(
                    sv_dataset,
                    output_dir,
                    raw_path.stem,
                    "zarr"
                    )
    print("Saved SV processed zarr file")
    ## Save some data from raw file to then close
    ## Calibration and metadata
    export_raw_csv(echodata,
                   output_dir,
                   raw_path.stem)
    print("Calibration and metadata exported")

    generic_message = {}
    generic_message["file_start_lat"] = echodata["Platform"]["latitude"].values[0]
    generic_message["file_start_lon"] = echodata["Platform"]["longitude"].values[0]
    generic_message["file_end_lat"] = echodata["Platform"]["latitude"].values[-1]
    generic_message["file_end_lon"] = echodata["Platform"]["longitude"].values[-1]


    logging.info("Saved SV processed file")

    sv_enriched = create_enriched_sv(
                            echodata, 
                            encode_mode, 
                            sv_dataset
                            )
    print("Enriched data")

    # Force memory clear
    del echodata
    gc.collect()

    # Downsample if needed
    sv_enriched_downsampled = regrid_dataset(sv_enriched)
    sv_enriched = sv_enriched_downsampled


    file_start_timestamp = sv_enriched["ping_time"].values[0]
    file_end_timestamp = sv_enriched["ping_time"].values[-1]

    #sv_with_masks = create_noise_masks_oceanstream(sv_enriched)
    # Create noise masks
    print("Creating noise masks...")
    masks, profiling_info = create_masks(
                            sv_enriched, 
                            profiling_info=profiling_info, 
                            config=config)

    mask_keys = []
    sv_with_masks = sv_enriched.copy(deep=True)
    if masks:
        for mask in masks:
            mask_type = mask[0]
            mask_keys.append(mask_type)
            mask_data = mask[1]
            sv_with_masks = attach_mask_to_dataset(
                sv_with_masks, mask=mask_data, mask_type=mask_type
            )

    ds_processed = apply_noise_masks(sv_with_masks, config)

    print(f"Created and applied masks: {mask_keys}")

    ds_interpolated = interpolate_sv(ds_processed)
    print("Interpolated nans on data")
    ds_interpolated = ds_interpolated.rename({"Sv": "Sv_denoised", 
                                              "Sv_interpolated": "Sv"
                                              })
    ds_clean, profiling_info = apply_background_noise_removal(
                               ds_processed, 
                               profiling_info=profiling_info, 
                               config=config
                               )
    print("Removed background noise on data")

    plot_all_channels(ds_clean,
                      save_path = DIRECTORY_TO_PROC,
                      name=raw_path.stem)
    # CSV exporting
    export_Sv_csv(
                  ds_clean,
                  output_dir,
                  raw_path.stem
                  )
    print("SV and GPS exported")

    # Seabed mask
    ds_clean = apply_seabed_mask(ds_clean, config=config)
    print("Applied seabed mask")

    # Shoal detection
    shoal_list, shoal_dataset = write_shoals_csv(
        ds_clean, 
        profiling_info=profiling_info, 
        config=config
    )

    ## NASC
    NASC_dict = compute_per_dataset_nasc(shoal_dataset)
    compute_and_write_nasc(
        shoal_dataset, 
        profiling_info={}, 
        config=config)
    
    print("NASC computed")

    return_message_list = []
    for shoal in shoal_list:
        shoal["filename"] = raw_path.stem+".zarr"
        shoal["mean_range"] = shoal["mean_range"].item() if shoal["mean_range"] else None
        shoal["start_time"] = str(shoal["start_time"])
        shoal["end_time"] = str(shoal["end_time"])
        shoal_message = format_message(shoal)
        return_message_list.append(shoal_message)

    generic_message["filename"] = raw_path.stem+".zarr"
    generic_message["file_npings"] = len(sv_enriched["ping_time"].values)
    generic_message["file_nsamples"] = len(sv_enriched["range_sample"].values)
    generic_message["file_start_time"] = str(sv_enriched["ping_time"].values[0])
    generic_message["file_end_time"] = str(sv_enriched["ping_time"].values[-1])

    generic_message["file_start_depth"] = str(sv_enriched["depth"].values[0,0,0])
    generic_message["file_end_depth"] = str(sv_enriched["depth"].values[0,0,-1])
    generic_message["file_nasc"] = ",".join(map(str,NASC_dict["NASC_dataset"]["NASC"].values.flatten()))
    generic_message["file_freqs"] = ",".join(map(str,sv_enriched["frequency_nominal"].values))


    if shoal_dataset["mask_seabed"].any().values:
        mean_depth_value = shoal_dataset['depth'].where(shoal_dataset['mask_seabed']).mean().values.item()
    else:
        mean_depth_value = None
    generic_message["file_seabed_depth"] = mean_depth_value
    generic_message["file_nshoals"] = len(shoal_list) if shoal_list[0]["label"] else 0
    return_message_list.append(generic_message)

    try:
        additional_info = "Processed without errors"
        db.mark_file_as_processed(
                               filename,
                               filename_processed=os.path.join(
                                                               DIRECTORY_TO_PROC,
                                                               raw_path.stem+".zarr"
                                                               ),
                               start_date=file_start_timestamp,
                               end_date=file_end_timestamp,
                               start_processing=start_processing,
                               additional_info=additional_info
                               )
        print("Succesfully saved to DB")
    except Exception as e:
        logging.error(f"Failed to save data for file {filename} in DB: {e}")
    finally:
        db.close()

    return return_message_list


def main():
    # Check if the argument is provided
    if len(sys.argv) != 2:
        print("Usage: python echosounder_processor.py <directory_path>")
        sys.exit(1)

    directory_path = sys.argv[1]
    # Check if the provided path is a directory
    if not os.path.isdir(directory_path):
        print(f"The provided path '{directory_path}' is not a directory.")
        sys.exit(1)
    # Process all files within the directory
    for root, _, files in os.walk(directory_path):
        for filename in files:
            if "raw" in filename:
                file_path = os.path.join(root, filename)
                try:
                    result = asyncio.run(process_file(file_path))
                    print(result)
                except Exception as e:
                    print(e)
                    continue
if __name__ == "__main__":
    main()