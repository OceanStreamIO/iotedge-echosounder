import datetime
import json
import logging
from pathlib import Path
import os
import sys


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
    combine_shoal_masks_multichannel,
    attach_shoal_mask_to_ds,
    process_shoals,
    write_shoals_to_csv,
    create_calibration,
    create_metadata,
    export_raw_csv,
    create_location,
    create_Sv,
    export_Sv_csv
)

from oceanstream.utils import (add_metadata_to_mask, 
                               attach_masks_to_dataset,
                               dict_to_formatted_list
                               )


# Configurations
DIRECTORY_TO_RAW = "/app/tmpdata"
DIRECTORY_TO_PROC = "/app/procdata"




async def process_file(filename):
    logging.info("Check and process files")
    generic_message = file_message.copy()
        # Create a database handler instance and set up the database
    db = DBHandler()
    db.setup_database()
    start_processing = datetime.datetime.now()
    raw_path = Path(filename)
    # Check if the file has been processed before
    if db.file_processed_before(filename):
        logging.info(f"File {filename} has already been processed. Skipping.")
        db.close()
        return {"Data already processed ":filename}
    try:
        check = file_integrity_checking(filename)
        file_integrity = check.get("file_integrity", False)
        if not file_integrity:
            return {"File integrity":"False"}

        # Process the file using oceanstream package
        echodata = read_raw_files([check])[0]
        print("Got raw data")
        sv_dataset = compute_sv(echodata)
        write_processed(
                        sv_dataset,
                        DIRECTORY_TO_PROC,
                        raw_path.stem,
                        "zarr"
                        )
        print("Saved SV processed file")
        if check["sonar_model"] == "EK60":
            encode_mode="power"
        elif check["sonar_model"] == "EK80":
            encode_mode="complex"
        else:
            encode_mode="power"
        sv_enriched = enrich_sv_dataset(sv_dataset,
                                        echodata,
                                        waveform_mode="CW",
                                        encode_mode=encode_mode
                                        )
        print("Enriched data")
        file_start_timestamp = sv_enriched["ping_time"].values[0]
        file_end_timestamp = sv_enriched["ping_time"].values[-1]

        generic_message["filename"] = filename
        generic_message["pings per file"] = len(sv_enriched["ping_time"].values)
        generic_message["Time"] = str(sv_enriched["ping_time"].values[0])
        generic_message["Start latitude"] = echodata["Platform"]["latitude"].values[0]
        generic_message["Start longitude"] = echodata["Platform"]["longitude"].values[0]
        generic_message["Freq. (Hz)"] = ",".join(map(str,echodata["Environment"]["frequency_nominal"].values))


        sv_with_masks = create_noise_masks_oceanstream(sv_enriched)
        print("Added masks to data")
        process_parameters ={
                            "mask_transient": {"var_name": "Sv"},
                            "mask_impulse": {"var_name": "Sv"},
                            "mask_attenuation": {"var_name": "Sv"}
                            }
        ds_processed = apply_selected_masks(
                                            sv_with_masks, 
                                            process_parameters
                                            )
        print("Applied masks to data")
        ds_interpolated = interpolate_sv(ds_processed)
        print("Interpolated nans on data")
        ds_interpolated = ds_interpolated.rename({"Sv": "Sv_denoised", 
                                                  "Sv_interpolated": "Sv"
                                                  })
        #ds_clean = apply_remove_background_noise(ds_interpolated)
        ds_clean = apply_selected_masks(
                                        ds_interpolated, 
                                        process_parameters
                                        )
        ### Add CSV making
        ## NASC
        NASC_dict = compute_per_dataset_nasc(ds_clean)
        generic_message["NASC"] = str(NASC_dict["NASC_dataset"]["NASC"].values.flatten())
        print("NASC computed")

        ## Calibration and metadata
        calibration = create_calibration(echodata)
        metadata = create_metadata(echodata)
        export_raw_csv(echodata,DIRECTORY_TO_PROC,raw_path.stem)
        print("Calibration and metadata exported")
        ## Raw SV and GPS for r shinny
        channel = ds_clean["channel"][0]
        location_speed = create_location(ds_clean)
        SV = create_Sv(ds_clean,channel)
        export_Sv_csv(ds_clean,DIRECTORY_TO_PROC,raw_path.stem)
        print("SV and GPS exported")

        ### Add seabed detection
        ## Real seabed
        seabed_mask = create_seabed_mask(
                      ds_clean,
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
                           ds_clean,
                           method="blackwell_mod",
                           parameters=false_seabed_params
                           )
        seabed_echo_mask = add_metadata_to_mask(
                           mask=seabed_echo_mask,
                           metadata={
                           "mask_type": "false_seabed",
                           "method": "blackwell_mod",
                           "parameters": dict_to_formatted_list(false_seabed_params),
                           },
                           )
        ds_clean = attach_masks_to_dataset(ds_clean, [seabed_mask,seabed_echo_mask])
        ds_clean = apply_selected_masks(
                                        ds_clean, 
                                        seabed_process_parameters
                                        )
        print("Applied seabed and false seabed masks")
        ## Shoal detection
        """
        ds_clean = attach_shoal_mask_to_ds(ds_clean)
        shoal_list = process_shoals(ds_clean)
        write_shoals_to_csv(shoal_list,
                            os.path.join(DIRECTORY_TO_PROC,
                                         raw_path.stem+"_fish_shoals.csv"
                                         )
                            )
        """
    except FileNotFoundError as e:
        logging.error(f"File {filename} could not be read!" + str(e))
    except ValueError as e:
        logging.error(f"Could not compute SV for file {filename} due to: {e}")
    finally:
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
                               additional_info=None)
            print("Succesfully saved to DB")
        except Exception as e:
            logging.error(f"Failed to send data for file {filename}: {e}")
        finally:
            db.close()
            return generic_message

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
                result = process_file(file_path)
                print(result)
if __name__ == "__main__":
    main()