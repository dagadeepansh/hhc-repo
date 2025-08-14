import os
import json
import warnings
import pandas as pd
from google.cloud import storage
from google.cloud.storage import Client, transfer_manager

# Suppress all warnings
warnings.filterwarnings('ignore')

def download_bucket_with_transfer_manager(
    bucket_name, prefix, destination_directory="/tmp/", workers=8, max_results=1000
):
    """Download all blobs (files only) from a specific GCS prefix concurrently."""
    print(f"📥 Starting download from bucket: {bucket_name}, prefix: {prefix}")
    print(f"Target local directory: {destination_directory}")
    
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    blobs_to_download = bucket.list_blobs(prefix=prefix, max_results=max_results)
    
    blob_names = [
        blob.name
        for blob in blobs_to_download
        if not blob.name.endswith("/")
    ]

    if not blob_names:
        print(f"⚠️ No files found to download at gs://{bucket_name}/{prefix}")
        return
        
    print(f"Found {len(blob_names)} files to download.")
    
    # Correcting the destination paths to be just the filename in /tmp/
    destination_names = [os.path.join(destination_directory, os.path.basename(name)) for name in blob_names]
    
    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=destination_directory, max_workers=workers
    )

    for name, result in zip(blob_names, results):
        if isinstance(result, Exception):
            print(f"❌ Failed to download {name} due to exception: {result}")
        else:
            print(f"✅ Downloaded {name} to {os.path.join(destination_directory, os.path.basename(name))}.")
    
    print("Download process complete. ✅")

def merge_data_with_pandas(specialty_file, symptom_file, synonym_file):
    """
    Loads data from three JSON files into pandas DataFrames, merges them based
    on 'SpecialtyId', and returns a merged DataFrame.
    """
    print("Merging data with pandas...")
    try:
        df_specialties = pd.read_json(specialty_file)
        df_symptoms = pd.read_json(symptom_file)
        df_synonyms = pd.read_json(synonym_file)
        print("DataFrames loaded successfully.")
    except Exception as e:
        print(f"❌ Error reading JSON files into DataFrames: {e}")
        return None

    df_specialties.rename(columns={'Id': 'SpecialtyId'}, inplace=True)
    df_symptoms_agg = df_symptoms.groupby('SpecialtyId')['SymptomText'].apply(list).reset_index()
    df_synonyms_agg = df_synonyms.groupby('SpecialtyId')['SynonymText'].apply(list).reset_index()

    df_merged = pd.merge(df_specialties, df_symptoms_agg, on='SpecialtyId', how='left')
    df_merged = pd.merge(df_merged, df_synonyms_agg, on='SpecialtyId', how='left')
    
    df_merged['SymptomText'] = df_merged['SymptomText'].apply(lambda x: x if isinstance(x, list) else [])
    df_merged['SynonymText'] = df_merged['SynonymText'].apply(lambda x: x if isinstance(x, list) else [])
    
    print("Data merged successfully. ✅")
    return df_merged

def process_area_of_expertise(input_json_path, output_jsonl_path):
    """
    Reads the AreaOfExpertise JSON file, renames the 'Id' column, and
    appends the data to the existing JSONL file.
    """
    print(f"Processing AreaOfExpertise data from: {input_json_path}")
    try:
        df = pd.read_json(input_json_path)
        df.rename(columns={'Id': 'AreaofExpertiseId'}, inplace=True)
        
        with open(output_jsonl_path, 'a') as f:
            for record in df.to_dict(orient='records'):
                f.write(json.dumps(record) + '\n')

        print(f"✅ Successfully processed '{input_json_path}' and appended to '{output_jsonl_path}'.")

    except FileNotFoundError:
        print(f"❌ Error: The file '{input_json_path}' was not found.")
    except Exception as e:
        print(f"❌ An error occurred: {e}")

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to a Google Cloud Storage bucket."""
    print(f"⬆️ Starting upload of file '{source_file_name}' to GCS bucket '{bucket_name}'...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"✅ Success! File '{source_file_name}' uploaded to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"❌ An error occurred during upload: {e}")
    print("Upload process complete. ✅")

def convert_dataframe_to_jsonl(df: pd.DataFrame, output_filepath: str):
    """Converts a Pandas DataFrame to a JSONL file."""
    print("Converting DataFrame to JSONL format...")
    try:
        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
        df.to_json(output_filepath, orient='records', lines=True, date_format='iso')
        print(f"✅ Success! DataFrame converted and saved to '{output_filepath}'")
    except Exception as e:
        print(f"❌ An error occurred during conversion: {e}")

def process_gcs_data(event, context):
    """
    Background Cloud Function to process data from a GCS bucket.
    """
    print(f"--- Cloud Function triggered ---")
    print(f"Triggered by GCS event: {event['name']} in bucket {event['bucket']}")
    print(f"Event ID: {context.event_id}, Event type: {context.event_type}")

    data_dir = "/tmp/"
    
    bucket_name = "gcf-v2-sources-895480140315-us-central1"
    download_prefix = "SpecialtyData/"
    output_filename = "transformed_specialty_data.jsonl"
    upload_prefix = "transformed-data/"
    
    specialty_filename = "HartfordHealthCare_Specialty.json"
    symptom_filename = "HartfordHealthCare_Symptom.json"
    synonym_filename = "HartfordHealthCare_Synonym.json"
    area_of_expertise_filename = "HartfordHealthCare_AreaOfExpertise.json"

    # Simplify paths to avoid any potential joining issues.
    # All files will be downloaded directly to /tmp/
    download_dir = data_dir
    os.makedirs(download_dir, exist_ok=True)
    
    specialty_path = os.path.join(download_dir, specialty_filename)
    symptom_path = os.path.join(download_dir, symptom_filename)
    synonym_path = os.path.join(download_dir, synonym_filename)
    area_of_expertise_path = os.path.join(download_dir, area_of_expertise_filename)
    output_path = os.path.join(data_dir, output_filename)
    
    download_bucket_with_transfer_manager(
        bucket_name, download_prefix, destination_directory=download_dir
    )
    
    df = merge_data_with_pandas(specialty_path, symptom_path, synonym_path)
    if df is not None:
        convert_dataframe_to_jsonl(df, output_path)
        process_area_of_expertise(area_of_expertise_path, output_path)

        destination_blob_name = os.path.join(upload_prefix, os.path.basename(output_path))
        upload_to_gcs(bucket_name, output_path, destination_blob_name)
    
    print("--- Cloud Function execution complete ---")
