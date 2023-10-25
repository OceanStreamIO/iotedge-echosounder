import json
import logging
from azure.iot.device import Message
from database_handler import DBHandler
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
                                            read_processed,
                                            write_processed,
                                            apply_remove_background_noise)
from oceanstream.L3_regridded_data import (
    apply_mask_organisms_in_order,
    apply_selected_noise_masks_and_or_noise_removal as apply_selected_masks
)

async def check_and_process_files(client, filename):
    logging.info("Check and process files")
    data_dict = {
                "filename":filename,
                "% samples": float('nan'),  # Using float('nan') for NaN values
                "ping_numbers":float('nan'),
                "FREQ (kHz)": float('nan'),
                "Latitude": float('nan'),
                "Longitude": float('nan'),
                "Miles": float('nan'),
                "NASC": float('nan'),
                "Seabed": float('nan'),  # numeric values in meters nan if no seabed
                "Start range (m)": float('nan'),
                "End range (m)": float('nan'),
                "Time": float('nan'),
                "Transect": float('nan'),
                "Type": "watercolumn"
                }
        # Create a database handler instance and set up the database
    db = DBHandler()
    db.setup_database()

    # Check if the file has been processed before
    if db.file_processed_before(filename):
        logging.info(f"File {filename} has already been processed. Skipping.")
        db.close()
        return
    try:
        check = file_integrity_checking(filename)
        file_integrity = check.get("file_integrity", False)
        if file_integrity:
            # Process the file using oceanstream package
            echodata = read_raw_files([check])
            sv_dataset = compute_sv(echodata[0])
            print("Got raw data")
            if check["sonar_model"] == "EK60":
                encode_mode="power"
            elif check["sonar_model"] == "EK80":
                encode_mode="complex"
            else:
                encode_mode="power"
            sv_enriched = enrich_sv_dataset(sv_dataset,
                                            echodata[0],
                                            waveform_mode="CW",
                                            encode_mode=encode_mode
                                            )
            print("Enriched data")
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

    except FileNotFoundError as e:
        logging.error(f"File {filename} could not be read!" + str(e))
    except ValueError as e:
        logging.error(f"Could not compute SV for file {filename} due to: {e}")
    finally:
        try:
            await send_to_hub(client, data_dict)
            # Assuming you have some additional_info to be stored
            additional_info = "Processed without errors"
            print("Succsefully sent message")
            db.mark_file_as_processed(filename, additional_info)
        except Exception as e:
            logging.error(f"Failed to send data for file {filename}: {e}")
        finally:
            db.close()
async def send_to_hub(client, data_dict):
    # Convert the dictionary to a JSON string
    print(data_dict)
    json_data = json.dumps(data_dict)
    # Prepare the message
    msg = Message(json_data)
    await client.send_message_to_output(msg, "output1")
    logging.info(f"Sent message for file: {data_dict['filename']}")