# police-data-fetching-service
Service which pulls data from police data API, preprocesses it and stores in a database


### Instructions

Setup:
1. Create a python virtualenv with version 3.11.x
2. `pip install requirements.txt`
3. All scripts must be run from the `service` folder as the working directory.

Initial Batch pipeline to populate historical dataset police_data.csv
 - `cd service`
 - `python batch_pipeline.py`

Daily updates script:
  - `python scheduler.py`
This script uses python's in built `scheduler` module to run daily at same time. 
