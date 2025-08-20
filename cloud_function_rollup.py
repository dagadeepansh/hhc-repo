import os
import json
import warnings
import pandas as pd
from google.cloud import storage
from google.cloud.storage import Client, transfer_manager
import functions_framework

# Initialize clients globally to reuse connections
storage_client = Client()
warnings.filterwarnings('ignore')

@functions_framework.cloud_event
def process_data(cloud_event):
    """
    Cloud Function triggered by a file upload to GCS.
    Downloads all related files, processes them, and uploads the transformed result.
    """
    # --- Configuration from Event and Environment Variables ---
    data = cloud_event.data
    bucket_name = data['bucket']
    trigger_file = data['name']
    
    # Get config from environment variables with sensible defaults
    download_prefix = os.getenv("DOWNLOAD_PREFIX", "SpecialtyData/")
    upload_prefix = os.getenv("UPLOAD_PREFIX", "TransformedData/")
    output_filename = os.getenv("OUTPUT_FILENAME", "transformed_rollup_data.jsonl")

    # We only care about files in the download_prefix
    if not trigger_file.startswith(download_prefix):
        print(f"File {trigger_file} is not in '{download_prefix}'. Skipping.")
        return

    # Use the /tmp directory, the only writable location in a Cloud Function
    local_temp_dir = "/tmp"
    
    # --- Execution ---
    print(f"Triggered by file: {trigger_file}. Starting data processing pipeline.")
    
    # 1. Download all relevant source files from GCS to the /tmp directory
    if not download_all_source_files(bucket_name, download_prefix, local_temp_dir):
        return # Exit if no files were found or download failed

    # 2. Process and merge the data using the corrected file paths
    df_processed = process_and_merge_data(local_temp_dir, download_prefix)
    if df_processed is None:
        print("Halting execution due to error in data processing.")
        return

    # 3. Write the combined data to a JSONL file in /tmp
    local_output_path = os.path.join(local_temp_dir, output_filename)
    write_dataframe_to_jsonl(df_processed, local_output_path)

    # 4. Upload the final transformed file back to GCS
    destination_blob_name = os.path.join(upload_prefix, output_filename)
    print(f"\n>>> Uploading transformed file to gs://{bucket_name}/{destination_blob_name}...")
    upload_to_gcs(bucket_name, local_output_path, destination_blob_name)
    print("Pipeline finished successfully.")


def download_all_source_files(bucket_name, prefix, destination_directory):
    """Downloads all files from a GCS prefix to a local directory."""
    print(f"📥 Starting download from gs://{bucket_name}/{prefix}")
    bucket = storage_client.bucket(bucket_name)
    blobs_to_download = [blob for blob in bucket.list_blobs(prefix=prefix) if not blob.name.endswith("/")]

    if not blobs_to_download:
        print(f"⚠️ No files found to download at specified prefix.")
        return False
        
    print(f"Found {len(blobs_to_download)} files to download.")
    blob_names = [blob.name for blob in blobs_to_download]
    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=destination_directory, max_workers=8
    )
    # Check for download errors
    for name, result in zip(blob_names, results):
        if isinstance(result, Exception):
            print(f"❌ Failed to download {name} due to exception: {result}")
            return False # Stop execution if a download fails
    return True


def process_and_merge_data(base_dir, prefix):
    """Loads and merges the source data files from the local temp directory."""
    try:
        # --- FIX: Correctly construct path to downloaded files inside /tmp ---
        source_dir = os.path.join(base_dir, prefix)
        
        print("🔍 Loading and processing source data...")
        df_rollup = pd.read_json(os.path.join(source_dir, "HartfordHealthCare_PhysicianRollupSpecialties.json"), encoding='utf-8-sig')
        df_specialties = pd.read_json(os.path.join(source_dir, "HartfordHealthCare_Specialty.json"))
        df_symptoms = pd.read_json(os.path.join(source_dir, "HartfordHealthCare_Symptom.json"))
        df_synonyms = pd.read_json(os.path.join(source_dir, "HartfordHealthCare_Synonym.json"))
        df_expertise = pd.read_json(os.path.join(source_dir, "HartfordHealthCare_AreaOfExpertise.json"))
    except FileNotFoundError as e:
        print(f"\n❌ CRITICAL ERROR: A source JSON file is missing. {e}")
        return None
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR reading a source JSON file: {e}")
        return None

    # (Pandas transformation logic is identical to your original script)
    df_specialties.rename(columns={'Id': 'SpecialtyId', 'Name': 'CanonicalSpecialtyName'}, inplace=True)
    df_merged = pd.merge(df_rollup, df_specialties, on='SpecialtyId', how='left')
    parent_lookup_df = df_rollup[['Id', 'Specialty']].rename(columns={'Id': 'ParentSpecialty', 'Specialty': 'ParentSpecialtyName'})
    df_rolled_up = pd.merge(df_merged, parent_lookup_df, on='ParentSpecialty', how='left')
    df_rolled_up['ParentSpecialtyName'] = df_rolled_up['ParentSpecialtyName'].fillna('')
    df_symptoms_agg = df_symptoms.groupby('SpecialtyId')['SymptomText'].apply(list).reset_index()
    df_synonyms_agg = df_synonyms.groupby('SpecialtyId')['SynonymText'].apply(list).reset_index()
    df_final = pd.merge(df_rolled_up, df_symptoms_agg, on='SpecialtyId', how='left')
    df_final = pd.merge(df_final, df_synonyms_agg, on='SpecialtyId', how='left')
    df_final['SymptomText'] = df_final['SymptomText'].apply(lambda x: x if isinstance(x, list) else [])
    df_final['SynonymText'] = df_final['SynonymText'].apply(lambda x: x if isinstance(x, list) else [])
    
    # Combine with expertise data
    specialty_records = df_final.to_dict(orient='records')
    expertise_records = df_expertise.to_dict(orient='records')
    all_records = specialty_records + expertise_records

    print("✅ Data processing and merging complete.")
    return all_records


def write_dataframe_to_jsonl(records: list, output_filepath: str):
    """Writes a list of dictionary records to a JSONL file."""
    print(f"Writing {len(records)} records to {output_filepath}")
    try:
        with open(output_filepath, 'w') as f:
            for record in records:
                # Convert NaN to None for valid JSON
                record_clean = {k: (None if pd.isna(v) else v) for k, v in record.items()}
                f.write(json.dumps(record_clean) + '\n')
        print(f"✅ Success! Wrote records to '{output_filepath}'")
    except Exception as e:
        print(f"❌ An error occurred during file writing: {e}")

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to a Google Cloud Storage bucket."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"✅ Success! File uploaded to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"❌ An error occurred during upload: {e}")
