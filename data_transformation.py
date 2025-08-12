import os
import json
import warnings
import argparse
import pandas as pd
from google.cloud import storage
from google.cloud.storage import Client, transfer_manager

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
        
    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=destination_directory, max_workers=workers
    )

    print(f"Starting download of {len(blob_names)} files from folder '{prefix}'...")

    for name, result in zip(blob_names, results):
        if isinstance(result, Exception):
            print(f"Failed to download {name} due to exception: {result}")
        else:
            print(f"Downloaded {name} to {os.path.join(destination_directory, os.path.basename(name))}.")

def convert_dataframe_to_jsonl(df: pd.DataFrame, output_filepath: str):
    """Converts a Pandas DataFrame to a JSONL file."""
    try:
        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
        df.to_json(output_filepath, orient='records', lines=True, date_format='iso')
        print(f"\n✅  Success! DataFrame successfully converted and saved to '{output_filepath}'")
    except Exception as e:
        print(f"An error occurred during conversion: {e}")

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to a Google Cloud Storage bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"✅  Success! File {source_file_name} uploaded to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"An error occurred during upload: {e}")

def main():
    """Main function to download, process, transform, and upload physician data."""
    # --- Argument Parser for Command-Line Inputs ---
    parser = argparse.ArgumentParser(description="Download, process, and re-upload physician data to GCS.")
    parser.add_argument(
        "--bucket_name",
        type=str,
        required=True,
        help="The GCS bucket name for both downloading and uploading files."
    )
    args = parser.parse_args()
    
    # --- Centralized Configuration for Paths and Filenames ---
    class Config:
        # Local directories
        LOCAL_DATA_ROOT_DIR = "data/"

        # GCS prefixes (folders)
        GCS_DOWNLOAD_PREFIX = "physician-data/"
        GCS_UPLOAD_PREFIX = "transformed-data/"

        # Output filename
        TRANSFORMED_FILENAME = "transformed_physician_data.jsonl"
        
        # Source filenames to be processed
        PRIMARY_SOURCE_FILE = 'HartfordHealthCare_Physician.json'
        SOURCE_FILENAMES = [
            PRIMARY_SOURCE_FILE, 'HartfordHealthCare_PhysicianPractice.json',
            'HartfordHealthCare_PhysicianLocation.json', 'HartfordHealthCare_PhysicianLanguage.json',
            'HartfordHealthCare_PhysicianInsurance.json', 'HartfordHealthCare_PhysicianFacultyAppointment.json',
            'HartfordHealthCare_PhysicianAreaOfExpertise.json', 'HartfordHealthCare_PhysicianTeamKeyword.json',
            'HartfordHealthCare_PhysicianEducation.json', 'HartfordHealthCare_PhysicianCredential.json',
            'HartfordHealthCare_PhysicianSpecialty.json', 'HartfordHealthCare_Insurance.json',
            'HartfordHealthCare_Location.json', 'HartfordHealthCare_AreaOfExpertise.json',
            'HartfordHealthCare_Specialty.json', 'HartfordHealthCare_Synonym.json',
            'HartfordHealthCare_Symptom.json'
        ]

    # Suppress all warnings
    warnings.filterwarnings('ignore')

    # --- 1. Download Data ---
    print(">>> Step 1: Starting data download from Google Cloud Storage...")
    download_bucket_with_transfer_manager(
        args.bucket_name,
        prefix=Config.GCS_DOWNLOAD_PREFIX,
        destination_directory=Config.LOCAL_DATA_ROOT_DIR,
        workers=8,
        max_results=1000
    )
    print(">>> Download complete.")

    # --- 2. Load Data into DataFrames ---
    print("\n>>> Step 2: Loading JSON files into pandas DataFrames...")
    data_input_path = os.path.join(Config.LOCAL_DATA_ROOT_DIR, Config.GCS_DOWNLOAD_PREFIX)
    
    try:
        physician_df = pd.read_json(os.path.join(data_input_path, Config.PRIMARY_SOURCE_FILE))
    except FileNotFoundError:
        print(f"Error: {Config.PRIMARY_SOURCE_FILE} not found. Exiting.")
        exit()

    dfs = {'physician': physician_df}
    # Iterate over all source files except the primary one which is already loaded
    for file_name in [f for f in Config.SOURCE_FILENAMES if f != Config.PRIMARY_SOURCE_FILE]:
        full_path = os.path.join(data_input_path, file_name)
        try:
            key = file_name.split('.')[0]
            dfs[key] = pd.read_json(full_path)
        except FileNotFoundError:
            print(f"Warning: {full_path} not found, skipping.")
        except ValueError:
            print(f"Warning: {full_path} is empty or not a valid JSON, skipping.")
    print(">>> Loading complete.")

    # --- 3. Process and Merge Data ---
    print("\n>>> Step 3: Processing and merging data...")
    
    # Language
    if 'HartfordHealthCare_PhysicianLanguage' in dfs:
        print("    - Merging Languages...")
        lang_grouped = dfs['HartfordHealthCare_PhysicianLanguage'].groupby('PhysicianId')['Language'].apply(list).reset_index().rename(columns={'Language': 'languages'})
        physician_df = pd.merge(physician_df, lang_grouped, on='PhysicianId', how='left')

    # Insurance
    if 'HartfordHealthCare_PhysicianInsurance' in dfs and 'HartfordHealthCare_Insurance' in dfs:
        print("    - Merging Insurance Plans...")
        merged_insurance_df = pd.merge(dfs['HartfordHealthCare_PhysicianInsurance'], dfs['HartfordHealthCare_Insurance'], left_on='InsuranceId', right_on='Id', how='left')
        ins_grouped = merged_insurance_df.groupby('PhysicianId').apply(lambda x: x[['InsuranceId', 'Name']].to_dict('records')).reset_index(name='insurance')
        physician_df = pd.merge(physician_df, ins_grouped, on='PhysicianId', how='left')

    # Practices
    if 'HartfordHealthCare_PhysicianPractice' in dfs:
        print("    - Merging Practices...")
        prac_grouped = dfs['HartfordHealthCare_PhysicianPractice'].groupby('PhysicianId')['PracticeId'].apply(list).reset_index()
        physician_df = pd.merge(physician_df, prac_grouped, on='PhysicianId', how='left')

    # Locations
    if 'HartfordHealthCare_PhysicianLocation' in dfs and 'HartfordHealthCare_Location' in dfs:
        print("    - Merging Locations...")
        location_details_df = dfs['HartfordHealthCare_Location'].rename(columns={'Id': 'LocationId'})
        merged_locations_df = pd.merge(dfs['HartfordHealthCare_PhysicianLocation'], location_details_df, on='LocationId', how='left')
        loc_grouped = merged_locations_df.groupby('PhysicianId').apply(lambda x: x[location_details_df.columns].to_dict('records')).reset_index(name='location')
        physician_df = pd.merge(physician_df, loc_grouped, on='PhysicianId', how='left')

    # Faculty Appointments
    if 'HartfordHealthCare_PhysicianFacultyAppointment' in dfs:
        print("    - Merging Faculty Appointments...")
        fac_grouped = dfs['HartfordHealthCare_PhysicianFacultyAppointment'].groupby('PhysicianId')['Position'].apply(list).reset_index().rename(columns={'Position': 'Position_faculty_appointments'})
        physician_df = pd.merge(physician_df, fac_grouped, on='PhysicianId', how='left')

    # Area of Expertise
    if 'HartfordHealthCare_PhysicianAreaOfExpertise' in dfs and 'HartfordHealthCare_AreaOfExpertise' in dfs:
        print("    - Merging Areas of Expertise...")
        area_of_expertise_df = dfs['HartfordHealthCare_AreaOfExpertise'].rename(columns={'Id': 'AreaOfExpertiseId'})
        merged_area_of_expertise_df = pd.merge(dfs['HartfordHealthCare_PhysicianAreaOfExpertise'], area_of_expertise_df, on='AreaOfExpertiseId', how='left')
        exp_grouped = merged_area_of_expertise_df.groupby('PhysicianId').apply(lambda x: x[area_of_expertise_df.columns].to_dict('records')).reset_index(name='area_of_expertise')
        physician_df = pd.merge(physician_df, exp_grouped, on='PhysicianId', how='left')

    # Team Keywords
    if 'HartfordHealthCare_PhysicianTeamKeyword' in dfs:
        print("    - Merging Team Keywords...")
        key_grouped = dfs['HartfordHealthCare_PhysicianTeamKeyword'].groupby('PhysicianId')['TeamKeyword'].apply(list).reset_index()
        physician_df = pd.merge(physician_df, key_grouped, on='PhysicianId', how='left')

    # Education
    if 'HartfordHealthCare_PhysicianEducation' in dfs and not dfs['HartfordHealthCare_PhysicianEducation'].empty:
        print("    - Merging Education...")
        edu_grouped = dfs['HartfordHealthCare_PhysicianEducation'].groupby('PhysicianId').apply(lambda x: x[['School', 'SchoolType', 'Degree', 'AreaOfStudy']].to_dict('records')).reset_index().rename(columns={0: 'education'})
        physician_df = pd.merge(physician_df, edu_grouped, on='PhysicianId', how='left')

    # Credentials
    if 'HartfordHealthCare_PhysicianCredential' in dfs:
        print("    - Merging Credentials...")
        cred_grouped = dfs['HartfordHealthCare_PhysicianCredential'].groupby('PhysicianId').apply(lambda x: x[['Facility', 'ShowOnWeb']].to_dict(orient='records')).reset_index(name='facility')
        physician_df = pd.merge(physician_df, cred_grouped, on='PhysicianId', how='left')

    # Specialties
    if ('HartfordHealthCare_PhysicianSpecialty' in dfs and not dfs['HartfordHealthCare_PhysicianSpecialty'].empty):
        print("    - Merging Specialties...")
        physician_specialty_df = dfs['HartfordHealthCare_PhysicianSpecialty']
        if 'HartfordHealthCare_Specialty' in dfs:
            specialty_details_df = dfs['HartfordHealthCare_Specialty'].rename(columns={'Id': 'SpecialtyId'})
            if 'HartfordHealthCare_Synonym' in dfs:
                synonym_grouped = dfs['HartfordHealthCare_Synonym'].groupby('SpecialtyId')['SynonymText'].apply(list).reset_index().rename(columns={'SynonymText': 'SynonymTexts'})
                specialty_details_df = pd.merge(specialty_details_df, synonym_grouped, on='SpecialtyId', how='left')
            if 'HartfordHealthCare_Symptom' in dfs:
                symptom_grouped = dfs['HartfordHealthCare_Symptom'].groupby('SpecialtyId')['SymptomText'].apply(list).reset_index().rename(columns={'SymptomText': 'SymptomTexts'})
                specialty_details_df = pd.merge(specialty_details_df, symptom_grouped, on='SpecialtyId', how='left')
            
            merged_specialties_df = pd.merge(physician_specialty_df, specialty_details_df, on='SpecialtyId', how='left')
            cols_for_dict = [col for col in ['SpecialtyId', 'BoardCertification', 'AcceptingNewPatients', 'Primary', 'Name', 'SynonymTexts', 'SymptomTexts'] if col in merged_specialties_df.columns]
            spec_grouped = merged_specialties_df.groupby('PhysicianId').apply(lambda x: x[cols_for_dict].to_dict('records')).reset_index(name='specialties')
            physician_df = pd.merge(physician_df, spec_grouped, on='PhysicianId', how='left')
    
    print(">>> Data processing complete.")

    # --- 4. Finalize and Save Output ---
    print("\n>>> Step 4: Finalizing and saving output file...")
    if 'AcceptingNewPatients' in physician_df.columns:
        physician_df = physician_df.drop(columns=['AcceptingNewPatients'])
        
    output_file_path = os.path.join(Config.LOCAL_DATA_ROOT_DIR, Config.GCS_UPLOAD_PREFIX, Config.TRANSFORMED_FILENAME)
    convert_dataframe_to_jsonl(physician_df, output_file_path)

    # --- 5. Upload Final File to GCS ---
    print("\n>>> Step 5: Uploading transformed file to Google Cloud Storage...")
    destination_blob_name = os.path.join(Config.GCS_UPLOAD_PREFIX, Config.TRANSFORMED_FILENAME)
    upload_to_gcs(args.bucket_name, output_file_path, destination_blob_name)

if __name__ == "__main__":
    main()
