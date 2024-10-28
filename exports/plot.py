import os
import matplotlib.pyplot as plt
import xarray as xr


def plot_sv_data(ds_Sv: xr.Dataset, file_base_name: str, output_path: str, echogram_path: str, cmap: str = 'ocean_r') -> list:
    """
    Plot Sv data for each channel and save the echogram plots.

    Parameters:
    - ds_Sv: xr.Dataset
        The dataset containing Sv data.
    - file_base_name: str
        The base name for output files.
    - output_path: str
        The path to save the output plots.
    - echogram_path: str
        Path to save individual echogram files.
    - cmap: str
        The colormap for plotting.

    Returns:
    - list: A list of file paths for the saved echograms.
    """
    if not plt.isinteractive():
        plt.switch_backend('Agg')  # Use non-interactive backend for plotting

    echogram_files = []
    for channel in range(ds_Sv.sizes['channel']):
        echogram_file_path = plot_individual_channel_simplified(ds_Sv, channel, file_base_name, echogram_path, cmap)
        echogram_files.append(echogram_file_path)

    return echogram_files


def plot_individual_channel_simplified(ds_Sv: xr.Dataset, channel: int, file_base_name: str,
                                       echogram_path: str, cmap: str) -> str:
    """
    Plot and save echogram for a single channel with optional regions and enhancements.

    Parameters:
    - ds_Sv: xr.Dataset
        The dataset containing Sv data.
    - channel: int
        The channel number to plot.
    - output_path: str
        The output path for the plot.
    - file_base_name: str
        The base file name for the plot.
    - echogram_path: str
        Path to save the echogram file.
    - cmap: str
        Colormap for plotting.

    Returns:
    - str: The path to the saved echogram file.
    """
    full_channel_name = ds_Sv.channel.values[channel]
    channel_name = "_".join(full_channel_name.split()[:3])

    # Extract relevant data and configure axis
    filtered_ds = ds_Sv['Sv']
    if 'beam' in filtered_ds.sizes:
        filtered_ds = filtered_ds.isel(beam=0).drop('beam')

    if 'channel' in filtered_ds.sizes:
        filtered_ds = filtered_ds.assign_coords({'frequency': ds_Sv.frequency_nominal})
        try:
            filtered_ds = filtered_ds.swap_dims({'channel': 'frequency'})
            if filtered_ds.frequency.size == 1:
                filtered_ds = filtered_ds.isel(frequency=0)
        except Exception as e:
            print(f"Error swapping dims while plotting echogram: {e}")

    plt.figure(figsize=(30, 18))
    filtered_ds.isel(frequency=channel).T.plot(
        x='ping_time',
        y='range_sample',
        yincrease=False,
        vmin=-80,
        vmax=-50,
        cmap=cmap,
        cbar_kwargs={'label': 'Volume backscattering strength (Sv re 1 m-1)'}
    )

    plt.grid(True, linestyle='--', alpha=0.5)
    plt.xlabel('Ping time', fontsize=14)
    plt.ylabel('Depth', fontsize=14)
    plt.title(f'{channel_name}', fontsize=16, fontweight='bold')

    echogram_file_name = f"{file_base_name}_{channel_name}.png"
    echogram_output_path = os.path.join(echogram_path, echogram_file_name)
    plt.savefig(echogram_output_path, dpi=300, bbox_inches='tight')
    plt.close()

    return echogram_output_path
