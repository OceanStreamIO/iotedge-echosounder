false_seabed_params = {
    "r0": 10,
    "r1": 1000,
    "tSv": -75,
    "ttheta": 702,
    "tphi": 282,
    "wtheta": 28,
    "wphi": 52,
}

seabed_params = {
    "r0": 10,
    "r1": 1000,
    "roff": 0,
    "thr": -40,
    "ec": 1,
    "ek": (3, 3),
    "dc": 3,
    "dk": (3, 3),
}

process_parameters = {
        "remove_background_noise": {
        "ping_num": 40,
        "noise_max": -125,
        "SNR_threshold": 3,
        },
    }
seabed_process_parameters = {
    'mask_seabed': {
    'var_name': 'Sv',
    },
    'mask_false_seabed': {
    'var_name': 'Sv',
    }
    }
shoal_process_parameters = {
    "thr": -70,
    "maxvgap": 5,
    "maxhgap": 5,
    "minvlen": 5,
    "minhlen": 5,
    "dask_chunking": {"ping_time": 1000, "range_sample": 1000},
}

