# iotedge-echosounder
Azure IoT Edge module that handles raw echosounder data in real-time

## Stand-alone usage
Install:

    pip install requirements.txt 
    
It sends all processd files to /procdata (this is from iotedge behavior).
The processing module can be used stand-alone by calling:

    python echosounder_processor.py <directory_path>

Where directory_path points to a folder with raw echosounder files. 
