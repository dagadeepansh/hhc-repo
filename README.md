# hhc-repo
specialty_roll_up_transformation.py : Ingested 5 files now instead of 4. Transformation logic has changed accordingly. 
application_setup.py : Only the "Id" is changed for Specialty data ingestion. 
API:
prompt_lib : Prompt is changed, replace entirely.
services.py : Ranking api code has changed to create new id for ranking as this data contains 0 id multiple times and score was coming as null
