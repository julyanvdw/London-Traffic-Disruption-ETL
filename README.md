A data pipeline pulling data from 'Transport for London', transforming it, performing calculations on it and also storing it. Later on, a service layer (via API, interface or CLI) will be made avaialble as well as some applications. 

Dependencies:
1. requests
2. pydantic 
3. postgres - set up a db (if locally)
4. pytest

todo: 
1. Create a setup script for anyones cloning the repo (or just provide a way to manage it)
2. Maybe also add a setup for a virtual env? 
3. For our orchestration, we can provide params which configure how the pipeline operates (rolling window size, fetch times, logs --verbose, etc)
4. abstract away file management with a datalake_manager.py file (to clean up code and simulate S3)

note: I avoided using any of the enterprise technologies as the purpose of this project was to show how I can manipulate files with python (not working with interprise tech). The only exception is the usage of technologies mentioned in the job description (Pydantic, postgreSQL, etc)


stuff done during data transformation
1. strip unecessary data off
2. do necessary type conversions
3. Field format validation
4. Remove duplicates from a given snapshot


plan: 
. get disruptions into db ✅
2. refactor to simulate datalake ✅
4. write tests ✅
5. go and delete old snapshots ✅
6. write logs functionality ✅
7. orchestrate (locally) with orchestration script ✅

8. Complete the TUI ✅
9. Build the Service API layer (RESTful) - guided access to the DB
10. Build an example application using the service layer

11. Host the DB somewhere
12. Wrap up with a virtual env / makefile - make sure thet can clone the pipeline and run it

--- core complete --- now for extras
1. Add in another DS (and alter DB / Application / TUI)
2. host on google
3. Build applet builder platform

small todo:
1) proper docs / comments
2) DB / API credentials must not be hard coded
3) comb through every file - see if the code looks clean



db stuff
-- disruptions_history table
CREATE TABLE disruptions_history (
    id SERIAL PRIMARY KEY,
    tims_id TEXT NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    url TEXT,
    severity TEXT,
    ordinal INT,
    category TEXT,
    subCategory TEXT,
    comments TEXT,
    currentUpdate TEXT,
    currentUpdateDateTime TIMESTAMP,
    corridorIds JSONB,
    startDateTime TIMESTAMP,
    endDateTime TIMESTAMP,
    lastModifiedTime TIMESTAMP,
    levelOfInterest TEXT,
    location TEXT,
    status TEXT,
    geography JSONB,
    geometry JSONB,
    isProvisional BOOLEAN,
    hasClosures BOOLEAN,
    UNIQUE (tims_id, snapshot_time)
);

-- streets table
CREATE TABLE streets (
    id SERIAL PRIMARY KEY,
    tims_id TEXT NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    name TEXT,
    closure TEXT,
    directions TEXT,
    segments JSONB,
    FOREIGN KEY (tims_id, snapshot_time) REFERENCES disruptions_history(tims_id, snapshot_time)
);




