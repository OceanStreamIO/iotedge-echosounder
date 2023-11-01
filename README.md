# iotedge-echosounder
Azure IoT Edge module that handles raw echosounder data in real-time

## Stand-alone usage
Install:

    pip install requirements.txt 
    
Before running create a folderfor processed data. 

    mkdir /app/procdata
    
It send all processd files there (this is from iotedge behavior).
The processing module can be used standalone by calling:

    python echosounder_processor.py <directory_path>

Where directory_path point to a folder with raw echosounder files. 
