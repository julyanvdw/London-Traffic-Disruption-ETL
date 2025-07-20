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
1. get disruptions into db ✅
2. refactor to simulate datalake ✅
4. write tests ✅
5. go and delete old snapshots ✅
6. write logs functionality ✅
7. orchestrate (locally) with orchestration script ✅

8. build a terminal interface
8. host the DB somewhere
9. build a service layer (RESTful APIs)

12. build an applicaiton (DS / VIS) which interfaces with the API
13. build app creator env

14. Add in another DS
15. prep for easy demoing 
10. try host (google)




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

Things the pipeline interface could show



Monitoring & Status
--------------------

Input Source

Last fetch time
Number of new records available
Source status (reachable/unreachable, API health)
Data source name (e.g., TfL API)
Fetch interval (e.g., every 5 min)
API rate limit info (remaining calls, reset time)
Extraction Step

Number of records extracted (last run, total)
Extraction duration (last run)
Extraction errors (count, last error message)
Current extraction status (idle, running, failed)
Data volume (MB/GB extracted)
Snapshot/file count
Transform Step

Number of records transformed (last run, total)
Transformation duration (last run)
Transformation errors (count, last error message)
Data validation stats (fields fixed, duplicates removed)
Current transform status (idle, running, failed)
Type conversions performed
Unnecessary fields stripped
Load Step

Number of records loaded (last run, total)
Load duration (last run)
Load errors (count, last error message)
Current load status (idle, running, failed)
Database row count (total, new rows)
Database health (connection status, space used)
Last successful load time


Pipeline run status (running, idle, failed, last run time)
Recent logs and errors
Health checks (DB connection, API availability)
Snapshot/file counts and sizes
Database row counts
Database info (space etc)

Control & Operations
--------------------

Start/stop/restart the pipeline
Trigger manual runs (on demand)
Pause/resume scheduled runs
Change scheduling interval (e.g., from 5 min to 10 min)
Enable/disable specific pipeline steps (extract, transform, load)
Clean up old logs or snapshots

Configuration
--------------------

Edit pipeline settings (API keys, DB credentials, retain window size, etc.)
Update environment variables or config files
Manage pipeline parameters (e.g., filter criteria, deduplication rules)

Alerts & Notifications
--------------------

Set up email/SMS/Slack alerts for failures or important events
View and acknowledge alerts

Audit & History
--------------------

View run history (success/failure, duration, data processed)
Download logs or snapshots
Track changes to configuration




