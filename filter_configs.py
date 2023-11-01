false_seabed_params = {
    "theta": None,
    "phi": None,
    "r0": 10,
    "r1": 1000,
    "tSv": -75,
    "ttheta": 702,
    "tphi": 282,
    "wtheta": 28,
    "wphi": 52,
    "rlog": None,
    "tpi": None,
    "freq": None,
    "rank": 50,
}

seabed_params = {
    "r0": 10,
    "r1": 1000,
    "roff": 0,
    "thr": -35,
    "ec": 15,
    "ek": (1, 3),
    "dc": 150,
    "dk": (1, 3),
}

process_parameters = {
        "remove_background_noise": {
        "ping_num": 40,
        "range_sample_num": 10,
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
