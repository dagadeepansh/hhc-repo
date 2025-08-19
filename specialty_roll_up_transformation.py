import os
import json
import warnings
import pandas as pd
from google.cloud import storage
from google.cloud.storage import Client, transfer_manager

# Suppress all warnings
warnings.filterwarnings('ignore')

def download_bucket_with_transfer_manager(
    bucket_name, prefix, destination_directory="", workers=8, max_results=1000
):
    """Download all blobs (files only) from a specific GCS prefix concurrently."""
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    blobs_to_download = bucket.list_blobs(prefix=prefix, max_results=max_results)
    
    blob_names = [
        blob.name
        for blob in blobs_to_download
        if not blob.name.endswith("/")
    ]

    if not blob_names:
        print(f"No files found to download at gs://{bucket_name}/{prefix}")
        return
        
    print(f"Starting download of {len(blob_names)} files from folder '{prefix}'...")
    
    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=destination_directory, max_workers=workers
    )

    for name, result in zip(blob_names, results):
        if isinstance(result, Exception):
            print(f"Failed to download {name} due to exception: {result}")
        else:
            print(f"Downloaded {name} to {os.path.join(destination_directory, name)}.")

def process_and_merge_data(rollup_file, specialty_file, symptom_file, synonym_file):
    """
    Loads and merges the primary specialty, symptom, and synonym data.
    Returns a DataFrame.
    """
    try:
        print("🔍 Loading and processing specialty data...")
        df_rollup = pd.read_json(rollup_file, encoding='utf-8-sig')
        df_specialties = pd.read_json(specialty_file)
        df_symptoms = pd.read_json(symptom_file)
        df_synonyms = pd.read_json(synonym_file)
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR reading a source JSON file: {e}")
        return None

    # Perform Specialty Roll-up
    df_specialties.rename(columns={'Id': 'SpecialtyId', 'Name': 'CanonicalSpecialtyName'}, inplace=True)
    df_merged = pd.merge(df_rollup, df_specialties, on='SpecialtyId', how='left')
    parent_lookup_df = df_rollup[['Id', 'Specialty']].rename(
        columns={'Id': 'ParentSpecialty', 'Specialty': 'ParentSpecialtyName'}
    )
    df_rolled_up = pd.merge(df_merged, parent_lookup_df, on='ParentSpecialty', how='left')
    df_rolled_up['ParentSpecialtyName'].fillna('', inplace=True)

    # Map Symptoms and Synonyms
    df_symptoms_agg = df_symptoms.groupby('SpecialtyId')['SymptomText'].apply(list).reset_index()
    df_synonyms_agg = df_synonyms.groupby('SpecialtyId')['SynonymText'].apply(list).reset_index()
    df_final = pd.merge(df_rolled_up, df_symptoms_agg, on='SpecialtyId', how='left')
    df_final = pd.merge(df_final, df_synonyms_agg, on='SpecialtyId', how='left')
    df_final['SymptomText'] = df_final['SymptomText'].apply(lambda x: x if isinstance(x, list) else [])
    df_final['SynonymText'] = df_final['SynonymText'].apply(lambda x: x if isinstance(x, list) else [])
    print("✅ Specialty data processing complete.")
    return df_final

def write_records_to_jsonl(records: list, output_filepath: str):
    """
    Writes a list of dictionary records to a JSONL file in one operation.
    """
    try:
        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
        with open(output_filepath, 'w') as f:
            for record in records:
                f.write(json.dumps(record) + '\n')
        print(f"\n✅ Success! Wrote {len(records)} total records to '{output_filepath}'")
    except Exception as e:
        print(f"An error occurred during file writing: {e}")

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to a Google Cloud Storage bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"✅ Success! File {source_file_name} uploaded to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"An error occurred during upload: {e}")

def main():
    """
    Main function to define values, download, process, combine, and upload data.
    """
    # Configuration
    bucket_name = "hhc-bucket-dev"
    download_prefix = "specialty-data/"
    data_dir = "data/"
    
    # Input filenames
    rollup_filename = "HartfordHealthCare_PhysicianRollupSpecialties.json"
    specialty_filename = "HartfordHealthCare_Specialty.json"
    symptom_filename = "HartfordHealthCare_Symptom.json"
    synonym_filename = "HartfordHealthCare_Synonym.json"
    area_of_expertise_filename = "HartfordHealthCare_AreaOfExpertise.json"
    
    # Output configuration
    output_filename = "transformed_rollup_data.jsonl"
    output_directory = os.path.join(data_dir, "transformed-data")
    upload_prefix = "transformed-data/"

    # File Paths
    download_dir = os.path.join(data_dir, download_prefix)
    os.makedirs(download_dir, exist_ok=True)
    rollup_path = os.path.join(download_dir, rollup_filename)
    specialty_path = os.path.join(download_dir, specialty_filename)
    symptom_path = os.path.join(download_dir, symptom_filename)
    synonym_path = os.path.join(download_dir, synonym_filename)
    area_of_expertise_path = os.path.join(download_dir, area_of_expertise_filename)
    output_path = os.path.join(output_directory, output_filename)

    # --- Execution ---
    # 1. Download all data from GCS
    download_bucket_with_transfer_manager(
        bucket_name, download_prefix, destination_directory=data_dir
    )
    
    # 2. Process the main specialty data
    df_specialty = process_and_merge_data(rollup_path, specialty_path, symptom_path, synonym_path)
    if df_specialty is None:
        print("Halting execution due to error in specialty data processing.")
        return

    # 3. Process the Area of Expertise data
    try:
        print("🔍 Loading Area of Expertise data...")
        df_expertise = pd.read_json(area_of_expertise_path)
        df_expertise.rename(columns={'Id': 'AreaofExpertiseId'}, inplace=True)
    except Exception as e:
        print(f"❌ CRITICAL ERROR reading Area of Expertise file: {e}")
        print("Halting execution.")
        return

    # 4. Combine all data into a single list of records
    specialty_records = df_specialty.to_dict(orient='records')
    expertise_records = df_expertise.to_dict(orient='records')
    all_records = specialty_records + expertise_records
    
    # 5. Write the combined list to the JSONL file in a single operation
    write_records_to_jsonl(all_records, output_path)

    # 6. Upload the final transformed file to GCS
    destination_blob_name = os.path.join(upload_prefix, os.path.basename(output_path))
    print("\n>>> Uploading transformed file to Google Cloud Storage...")
    upload_to_gcs(bucket_name, output_path, destination_blob_name)

if __name__ == "__main__":
    main()
