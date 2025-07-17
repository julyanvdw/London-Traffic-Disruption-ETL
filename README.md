A data pipeline pulling data from 'Transport for London', transforming it, performing calculations on it and also storing it. Later on, a service layer (via API, interface or CLI) will be made avaialble as well as some applications. 



Dependencies:
1. requests

todo: 
1. Create a setup script for anyone cloning the repo (or just provide a way to manage it)
2. Maybe also add a setup for a virtual env? 
3. For our orchestration, we can provide params which configure how the pipeline operates (rolling window size, fetch times, logs --verbose, etc)
4. abstract away file management with a datalake_manager.py file (to clean up code and simulate S3)

note: I avoided using any of the enterprise technologies as the purpose of this project was to show how I can manipulate files with python (not working with interprise tech). The only exception is the usage of technologies mentioned in the job description (Pydantic, postgreSQL, etc)