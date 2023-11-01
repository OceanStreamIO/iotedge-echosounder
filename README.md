# iotedge-echosounder
Azure IoT Edge module that handles raw echosounder data in real-time

## Stand-alone usage
Before running create a /app/procdata folder. It send all processd files there(this is from iotegde behavior).
The processing module can be used standalone by calling:
    python echosounder_processor.py <directory_path>
Where directory_path point to a folder with raw echosounder files. 
