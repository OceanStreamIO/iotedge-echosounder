import numpy as np
import pandas as pd
import logging

logger = logging.getLogger('oceanstream')

STANDARD_CHANNEL_NAMES = {
    18000: "freq_18_kHz",
    38000: "freq_38_kHz",
    70000: "freq_70_kHz",
    120000: "freq_120_kHz",
    200000: "freq_200_kHz"
}


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


def debug_inspect_ds_Sv(ds_Sv) -> None:
    """
    Print detailed debug information about the ds_Sv dataset.

    Parameters:
    - ds_Sv: xr.Dataset
        The standardized dataset containing Sv data.
    """
    print("=== Debugging ds_Sv Dataset ===")

    # Print the entire dataset structure
    print("\nFull Dataset Structure:\n")
    print(repr(ds_Sv))  # Use repr to ensure full content is printed

    # Print the dataset variables
    print("\nVariables in the ds_Sv Dataset:")
    print(list(ds_Sv.variables))

    # Print the dataset dimensions
    print("\nDimensions in the ds_Sv Dataset:")
    print(ds_Sv.dims)

    # Print the dataset coordinates
    print("\nCoordinates in the ds_Sv Dataset:")
    print(list(ds_Sv.coords))

    # Inspect individual variables and their attributes
    for var in ds_Sv.data_vars:
        print(f"\nVariable '{var}':")
        print(f"  Data Type: {ds_Sv[var].dtype}")
        print(f"  Shape: {ds_Sv[var].shape}")
        print(f"  Dimensions: {ds_Sv[var].dims}")
        print(f"  Attributes: {ds_Sv[var].attrs}")
        sample_data = ds_Sv[var].values[:5] if ds_Sv[var].size > 5 else ds_Sv[var].values
        print(f"  Sample Data (first 5 elements): {repr(sample_data)}")  # Using repr to ensure full output

    # Inspect the attributes at the dataset level
    print("\nDataset-Level Attributes:")
    print(ds_Sv.attrs)

    # Inspect sample data for key coordinates
    if 'frequency_nominal' in ds_Sv.coords:
        print(f"\nValues of 'frequency_nominal': {repr(ds_Sv['frequency_nominal'].values)}")

    if 'ping_time' in ds_Sv.coords:
        print(f"\nValues of 'ping_time' (first 5): {repr(ds_Sv['ping_time'].values[:5])}")

    if 'range_sample' in ds_Sv.coords:
        print(f"\nValues of 'range_sample' (first 5): {repr(ds_Sv['range_sample'].values[:5])}")

    print("=== End of Debugging ds_Sv Dataset ===")


def convert_to_native(value):
    """convert_to_native
    Convert non-native types to native Python types.

    Parameters:
    - value: any
        The value to convert.

    Returns:
    - Converted native Python type.
    """
    # Convert numpy types to native Python types
    if isinstance(value, (np.integer, np.int8, np.int16, np.int32, np.int64)):
        return int(value)

    if isinstance(value, float) and np.isnan(value):
        return None

    if isinstance(value, (np.floating, np.float32, np.float64)):
        return float(value)

    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return value.isoformat()

    return value


def process_variable(data, idx=None):
    """
    Process a variable by extracting its value based on the given index.

    Parameters:
    - data: xr.DataArray or np.ndarray
        The data variable to process.
    - idx: int or None
        The index to use for selection, if applicable.

    Returns:
    - dict or scalar: A dictionary with min, max, mean, std if data is an array,
      or a scalar value if the data is uniform or a single value.
    """
    try:
        # Select data by index if provided
        selected_data = data.isel(channel=idx) if idx is not None and 'channel' in data.dims else data

        # Handle scalar values
        if selected_data.size == 1:
            return convert_to_native(selected_data.item())

        if selected_data.ndim == 1 and np.all(selected_data.values == selected_data.values[0]):  # All values are the same
            return convert_to_native(selected_data.values[0])

        if selected_data.ndim == 1:  # Single dimension array, compute range
            return [
                convert_to_native(selected_data.min().item()),
                convert_to_native(selected_data.max().item()),
                convert_to_native(selected_data.mean().item()),
                convert_to_native(selected_data.std().item())
            ]

        # Handle multi-dimensional arrays by computing mean and std
        return [
            convert_to_native(selected_data.mean().item()),
            convert_to_native(selected_data.std().item())
        ]

    except Exception as e:
        logger.warning(f"Error processing variable: {e}")
        return None


def create_instrument_metadata(echodata, ds_Sv) -> dict:
    """
    Extract instrument metadata from the standardized ds_Sv dataset.

    Parameters:
    - ds_Sv: xr.Dataset
        The standardized dataset containing Sv data.

    Returns:
    - dict: A dictionary containing the instrument metadata organized by channel.
    """

    variable_list = [
        "frequency_nominal",
        "beam_type",
        # "beamwidth_twoway_alongship",
        # "beamwidth_twoway_athwartship",
        # "beam_direction_x",
        # "beam_direction_y",
        # "beam_direction_z",
        # "angle_offset_alongship",
        # "angle_offset_athwartship",
        # "angle_sensitivity_alongship",
        # "angle_sensitivity_athwartship",
        # "equivalent_beam_angle",
        # "gain_correction",
        "gpt_software_version",
        "transceiver_software_version",
        "transmit_frequency_start",
        "transmit_frequency_stop",
        "beam_stabilisation",
        "non_quantitative_processing",
        "channel",
        "sample_interval",
        "transmit_bandwidth",
        "transmit_power",
        "sample_time_offset",
        # "angle_athwartship",
        "transmit_duration_nominal",
        # "angle_alongship",
        "slope",
        # "backscatter_r",
        "channel_mode",
        "data_type",
        "transmit_type"
    ]

    # Excluded variables from collection
    exclude_vars = {'ping_time', 'range_sample', 'Sv', 'echo_range', 'latitude', 'longitude', 'depth', 'source_filenames'}

    # Prepare the output metadata dictionary
    metadata = {}

    # Iterate through each channel
    for idx, freq in enumerate(ds_Sv['frequency_nominal'].values):
        freq_metadata = {}

        for var in variable_list:
            if var in echodata.beam:
                freq_metadata[var] = process_variable(echodata.beam[var], idx)
            else:
                freq_metadata[var] = None

        for var in ds_Sv.data_vars:
            # Exclude unwanted variables
            if var in exclude_vars:
                continue

            freq_metadata[var] = process_variable(ds_Sv[var], idx)

        # Convert frequency to standard channel name
        standard_channel_name = get_standard_channel_name(freq)

        # Assign collected metadata to the corresponding channel
        metadata[standard_channel_name] = freq_metadata

    return metadata



def __create_instrument_metadata(echodata, ds_Sv) -> dict:
    """
    Extract instrument metadata from the hydroacoustic dataset and organize it by channel.

    Parameters:
    - echodata: EchoData
        The raw hydroacoustic data to extract metadata from.

    Returns:
    - dict: A dictionary containing the instrument metadata organized by channel.
    """
    # Access the internal xarray dataset for the Beam group


    try:
        beam_data = echodata.beam  # Extracting the beam data from EchoData
    except AttributeError as e:
        logger.error(f"Failed to access 'beam' data: {e}")
        return {}

    # Debug: Print dataset structure
    logger.debug(f"Beam dataset structure:\n{beam_data}")

    # Ensure 'frequency_nominal' is present in the dataset
    if 'frequency_nominal' not in beam_data:
        logger.error("The 'frequency_nominal' variable is missing from the beam dataset.")
        return {}

    # print(f"Variables in the 'beam' dataset: {list(beam_data.variables)}")
    #
    # # Debug: Print available coordinates
    # print(f"Coordinates in the 'beam' dataset: {list(beam_data.coords)}")
    #
    # # Debug: Print the values of 'frequency_nominal'
    # print(
    #     f"Values of 'frequency_nominal': {beam_data['frequency_nominal'].values if 'frequency_nominal' in beam_data else 'Not found'}")

    variable_list = [
        "frequency_nominal",
        "beam_type",
        "beamwidth_twoway_alongship",
        "beamwidth_twoway_athwartship",
        "beam_direction_x",
        "beam_direction_y",
        "beam_direction_z",
        "angle_offset_alongship",
        "angle_offset_athwartship",
        "angle_sensitivity_alongship",
        "angle_sensitivity_athwartship",
        "equivalent_beam_angle",
        "gain_correction",
        "gpt_software_version",
        "transceiver_software_version",
        "transmit_frequency_start",
        "transmit_frequency_stop",
        "beam_stabilisation",
        "non_quantitative_processing",
        "channel",
        "sample_interval",
        "transmit_bandwidth",
        "transmit_power",
        "sample_time_offset",
        "angle_athwartship",
        "transmit_duration_nominal",
        "angle_alongship",
        "slope",
        "backscatter_r",
        "channel_mode",
        "data_type",
        "transmit_type"
    ]

    metadata_ext = {}
    for idx, freq in enumerate(beam_data["frequency_nominal"].values):
        # Slice the dataset for the current channel
        channel_data = beam_data.isel(channel=idx)

        # Initialize metadata dictionary for the current frequency
        freq_metadata = {}

        # Extract and process each variable
        for var in variable_list:
            if var in channel_data:
                try:
                    values = channel_data[var].values

                    # If it's an array, check for uniqueness or get min/max
                    if isinstance(values, np.ndarray):
                        if np.all(values == values[0]):  # All values are the same
                            freq_metadata[var] = values[0]
                        else:
                            freq_metadata[var] = {
                                "min": values.min(),
                                "max": values.max()
                            }
                    else:
                        freq_metadata[var] = values

                except Exception as e:
                    logger.warning(f"Error processing variable '{var}' for frequency '{freq}': {e}")
                    freq_metadata[var] = None

        # Convert frequency to standard channel name
        standard_channel_name = get_standard_channel_name(freq)

        # Use standard channel name as the key
        metadata_ext[standard_channel_name] = freq_metadata

    print('metadata_ext', metadata_ext)

    metadata = {}

    for idx, freq in enumerate(beam_data["frequency_nominal"].values):
        freq_metadata = {}
        for var in variable_list:
            # Check if the variable is in the dataset
            if var in beam_data:
                # If the variable exists, try to get the value using .isel(); otherwise, set to None
                try:
                    # Use .isel() for positional indexing
                    value = beam_data[var].isel(channel=idx).values

                    # If the value is an array with a single element, extract the scalar
                    if value.size == 1:
                        value = value.item()

                    if isinstance(value, float) and np.isnan(value):
                        value = None

                    freq_metadata[var] = value

                except KeyError:
                    logger.warning(f"Variable '{var}' is missing for frequency '{freq}'. Filling with None.")
                    freq_metadata[var] = None
                except Exception as e:
                    logger.warning(f"Error while extracting '{var}' for frequency '{freq}': {e}")
                    freq_metadata[var] = None
            else:
                logger.warning(f"Variable '{var}' is not declared in the dataset. Filling with None.")
                freq_metadata[var] = None

        # Convert frequency to standard channel name
        standard_channel_name = get_standard_channel_name(freq)

        # Use standard channel name as the key
        metadata[standard_channel_name] = freq_metadata

    return metadata


def get_standard_channel_name(frequency: float) -> str:
    """
    Get the standard channel name for a given frequency.

    Parameters:
    - frequency: float
        The frequency value to match.

    Returns:
    - str: The standard name corresponding to the closest frequency.
    """
    # Find the closest matching frequency in the STANDARD_CHANNEL_NAMES dictionary
    closest_frequency = min(STANDARD_CHANNEL_NAMES.keys(), key=lambda x: abs(x - frequency))
    return STANDARD_CHANNEL_NAMES[closest_frequency]


def aggregate_variable(var_data):
    """Aggregate data using mean, min, max, and standard deviation."""
    return {
        "mean": float(var_data.mean().values),
        "min": float(var_data.min().values),
        "max": float(var_data.max().values),
        "std": float(var_data.std().values)
    }


def aggregate_categorical(var_data):
    """Aggregate categorical data by reporting the most frequent value."""
    # Get the most frequent value (mode)
    value_counts = pd.Series(var_data.values).value_counts()
    most_frequent_value = value_counts.idxmax()
    count = value_counts.max()

    return {
        "most_frequent": most_frequent_value,
        "count": count,
        "unique_values": value_counts.to_dict()
    }
