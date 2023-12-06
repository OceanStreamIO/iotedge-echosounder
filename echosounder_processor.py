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
from database_handler import DBHandler

from filter_configs import (false_seabed_params, 
                            seabed_params, 
                            process_parameters,
                            seabed_process_parameters)
from azure_messages import GENERIC_HEARTBEAT_MESSAGE as file_message
from oceanstream.L0_unprocessed_data import (file_finder, 
                                             file_integrity_checking,
                                             read_raw_files,
                                             convert_raw_files, 
                                             read_processed_files,
                                             fix_time_reversions,
                                             check_reversed_time)
from oceanstream.L2_calibrated_data import (compute_sv,
                                            enrich_sv_dataset,
                                            interpolate_sv,
                                            regrid_dataset,
                                            create_noise_masks_oceanstream,
                                            create_seabed_mask,
                                            read_processed,
                                            write_processed,
                                            apply_remove_background_noise)
from oceanstream.L3_regridded_data import (
    apply_mask_organisms_in_order,
    apply_selected_noise_masks_and_or_noise_removal as apply_selected_masks,
    compute_per_dataset_nasc,
    create_shoal_mask_multichannel,
    attach_shoal_mask_to_ds,
    process_shoals,
    write_shoals_to_csv,
    create_calibration,
    create_metadata,
    export_raw_csv,
    create_location,
    create_Sv,
    export_Sv_csv,
    full_nasc_data,
    write_nasc_to_csv
)

from oceanstream.utils import (add_metadata_to_mask, 
                               attach_masks_to_dataset,
                               dict_to_formatted_list,
                               plot_all_channels
                               )


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

async def process_file(filename):

    # Create a database handler instance and set up the database
    db = setup_database()
    start_processing = datetime.datetime.now()
    raw_path = Path(filename)
    output_dir = Path(DIRECTORY_TO_PROC)
    # Check if the file has been processed before
    if db.file_processed_before(filename):
        logging.info(f"File {filename} has already been processed. Skipping.")
        db.close()
        return {"Processing Warning":f"File {filename} has already been processed"}
    check = file_integrity_checking(filename)
    file_integrity = check.get("file_integrity", False)
    if not file_integrity:
        return {"Processing Error":f"File {filename} could not usable!"}
    
    # Process the file using oceanstream package
    echodata = read_raw_files([check])[0]
    print("Got raw data")
    logging.info("New raw file read")
    if check_reversed_time(echodata, "Sonar/Beam_group1", "ping_time"):
        echodata = fix_time_reversions(echodata, {"Sonar/Beam_group1": "ping_time"})

    if check["sonar_model"] == "EK60":
        encode_mode = "power"
        sv_dataset = compute_sv(echodata)
    elif check["sonar_model"] == "EK80":
        encode_mode = "power"
        sv_dataset = compute_sv(echodata,waveform_mode="CW",encode_mode="power")
    else:
        encode_mode="power"
        sv_dataset = compute_sv(echodata)

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

    sv_enriched = enrich_sv_dataset(sv_dataset,
                                    echodata,
                                    waveform_mode="CW",
                                    encode_mode=encode_mode
                                    )
    print("Enriched data")
    ## Force memory clear
    del echodata
    gc.collect()
    sv_enriched_downsampled = regrid_dataset(sv_enriched)

    sv_enriched = sv_enriched_downsampled


    file_start_timestamp = sv_enriched["ping_time"].values[0]
    file_end_timestamp = sv_enriched["ping_time"].values[-1]

    sv_with_masks = create_noise_masks_oceanstream(sv_enriched)
    print("Added noise masks to data")
    ## Real seabed

    seabed_mask = create_seabed_mask(
                  sv_with_masks,
                  method="ariza",
                  parameters=seabed_params,
                  )
    seabed_mask = add_metadata_to_mask(
                  mask=seabed_mask,
                  metadata={
                  "mask_type": "seabed",
                  "method": "ariza",
                  "parameters": dict_to_formatted_list(seabed_params),
                  }
                  )
    ## Fake seabed
    seabed_echo_mask = create_seabed_mask(
                       sv_with_masks,
                       method="blackwell",
                       parameters=false_seabed_params
                       )
    seabed_echo_mask = add_metadata_to_mask(
                       mask=seabed_echo_mask,
                       metadata={
                       "mask_type": "false_seabed",
                       "method": "blackwell",
                       "parameters": dict_to_formatted_list(false_seabed_params),
                       },
                       )
    sv_with_masks = attach_masks_to_dataset(sv_with_masks, [seabed_mask,seabed_echo_mask])

    print("Added seabed masks to data")
    noise_masks ={
                  "mask_transient": {"var_name": "Sv"},
                  "mask_impulse": {"var_name": "Sv"},
                  "mask_attenuation": {"var_name": "Sv"}
                  }
    ds_processed = apply_selected_masks(
                                        sv_with_masks, 
                                        noise_masks
                                        )
    print("Applied noise masks to data")
    ds_interpolated = interpolate_sv(ds_processed)
    print("Interpolated nans on data")
    ds_interpolated = ds_interpolated.rename({"Sv": "Sv_denoised", 
                                              "Sv_interpolated": "Sv"
                                              })
    ds_clean = apply_selected_masks(
                                    ds_interpolated, 
                                    process_parameters
                                    )
    print("Removed background noise on data")

    plot_all_channels(ds_clean,
                      save_path = DIRECTORY_TO_PROC,
                      name=raw_path.stem)
    ### Add CSV making

    export_Sv_csv(ds_clean,
                  output_dir,
                  raw_path.stem)
    print("SV and GPS exported")

    ### Add seabed detection

    ds_clean = apply_selected_masks(
                                    ds_clean, 
                                    seabed_process_parameters
                                    )

    print("Applied seabed and false seabed masks")
    ## Shoal detection
    parameters = {
                  "thr": -55, 
                  "maxvgap": 5, 
                  "maxhgap": 5, 
                  "minvlen": 2, 
                  "minhlen": 2,
                  "dask_chunking": {"ping_time":100,"range_sample":100}
                  }
    shoal_dataset = attach_shoal_mask_to_ds(
                                            ds_clean, 
                                            parameters=parameters, 
                                            method="will"
                                            )
    print("Attaching Shoal masks")
    
    
    shoal_list = process_shoals(shoal_dataset)
    write_shoals_to_csv(shoal_list,
                        os.path.join(DIRECTORY_TO_PROC,
                                     raw_path.stem+"_fish_schools.csv"
                                    )
                       )

    ## NASC
    NASC_dict = compute_per_dataset_nasc(shoal_dataset)
    nasc = full_nasc_data(shoal_dataset)
    write_nasc_to_csv(nasc,
                      os.path.join(DIRECTORY_TO_PROC,
                                     raw_path.stem+"_NASC.csv"
                                    )
                     )
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