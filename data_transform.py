"""
This script is designed to be used as a Google Cloud Function, triggered by
a GCS event.
It downloads data from a specified GCS bucket, processes and merges the data,
and uploads the transformed data back to GCS.
"""
import os
import glob
import logging
from typing import Dict, List, Any

import pandas as pd
import functions_framework
from dotenv import load_dotenv
from google.cloud import storage
from google.cloud.storage import Client, transfer_manager

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
load_dotenv()

# --- GCS Helper Functions ---


def _perform_gcs_download(
        bucket: storage.Bucket,
        blob_names: List[str],
        destination: str):
    """Helper to manage the concurrent download of GCS blobs."""
    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=destination, max_workers=8
    )
    for name, result in zip(blob_names, results):
        if isinstance(result, Exception):
            logging.error("Failed to download %s: %s", name, result)
        else:
            logging.info("Successfully downloaded %s.", name)


def download_data_from_gcs(
        bucket_name: str,
        gcs_download_prefix: str,
        local_data_root_dir: str
) -> str:
    """
    Downloads data from a specified GCS prefix.
    """
    logging.info("Step 1: Starting data download from gs://%s/%s...",
                 bucket_name, gcs_download_prefix)
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    local_download_path = os.path.join(
        local_data_root_dir, gcs_download_prefix)
    os.makedirs(local_download_path, exist_ok=True)

    blobs_to_download = [b for b in bucket.list_blobs(
        prefix=gcs_download_prefix) if not b.name.endswith("/")]

    if not blobs_to_download:
        logging.warning("No files found to download at gs://%s/%s",
                        bucket_name, gcs_download_prefix)
        return local_download_path

    _perform_gcs_download(
        bucket, [b.name for b in blobs_to_download], local_data_root_dir)
    logging.info(">>> Download complete.")
    return local_download_path


def upload_to_gcs(
        bucket_name: str,
        source_file_path: str,
        destination_blob_name: str
):
    """
    Uploads a file to a Google Cloud Storage bucket.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        logging.info("✅ Success! File %s uploaded to gs://%s/%s",
                     source_file_path, bucket_name, destination_blob_name)
    except Exception as e:
        logging.error("An error occurred during upload: %s", e, exc_info=True)
        raise

# --- Data Loading & Transformation Functions ---


def load_dataframes(
        data_path: str
) -> Dict[str, pd.DataFrame]:
    """
    Dynamically loads all JSON files from a directory into a dictionary
    of DataFrames.
    """
    logging.info("Step 2: Loading JSON files from '%s'...", data_path)
    dataframes = {}
    for file_path in glob.glob(os.path.join(data_path, "*.json")):
        filename = os.path.basename(file_path)
        key = filename.replace('HartfordHealthCare_', '').replace('.json', '')
        try:
            dataframes[key] = pd.read_json(file_path)
        except ValueError:
            logging.warning(
                "%s is empty or not valid JSON, skipping.", filename)
    logging.info(">>> Loading complete. Loaded %d dataframes.",
                 len(dataframes))
    return dataframes


def get_merge_config() -> List[Dict[str, Any]]:
    """
    Defines the configuration for all data merging operations.
    """
    return [
        {
            "name": "Languages",
            "primary_df_key": "PhysicianLanguage",
            "type": "group_and_list",
            "group_by": "PhysicianId",
            "agg_col": "Language",
            "rename_to": "languages"
        },
        {
            "name": "Practices",
            "primary_df_key": "PhysicianPractice",
            "type": "group_and_list",
            "group_by": "PhysicianId",
            "agg_col": "PracticeId"
        },
        {
            "name": "Team Keywords",
            "primary_df_key": "PhysicianTeamKeyword",
            "type": "group_and_list",
            "group_by": "PhysicianId",
            "agg_col": "TeamKeyword"
        },
        {
            "name": "Faculty Appointments",
            "primary_df_key": "PhysicianFacultyAppointment",
            "type": "group_and_list",
            "group_by": "PhysicianId",
            "agg_col": "Position",
            "rename_to": "Position_faculty_appointments"
        },
        {
            "name": "Insurance",
            "primary_df_key": "PhysicianInsurance",
            "secondary_df_key": "Insurance",
            "type": "group_and_dict",
            "left_on": "InsuranceId",
            "right_on": "Id",
            "group_by": "PhysicianId",
            "dict_cols": ["InsuranceId", "Name"],
            "rename_to": "insurance"
        },
        {
            "name": "Locations",
            "primary_df_key": "PhysicianLocation",
            "secondary_df_key": "Location",
            "type": "group_and_dict",
            "left_on": "LocationId",
            "right_on": "Id",
            "group_by": "PhysicianId",
            "rename_to": "location"
        },
        {
            "name": "Area of Expertise",
            "primary_df_key": "PhysicianAreaOfExpertise",
            "secondary_df_key": "AreaOfExpertise",
            "type": "group_and_dict",
            "left_on": "AreaOfExpertiseId",
            "right_on": "Id",
            "group_by": "PhysicianId",
            "rename_to": "area_of_expertise"
        },
        {
            "name": "Education",
            "primary_df_key": "PhysicianEducation",
            "type": "group_and_dict",
            "group_by": "PhysicianId",
            "dict_cols": [
                "School",
                "SchoolType",
                "Degree",
                "AreaOfStudy"
            ],
            "rename_to": "education"
        },
        {
            "name": "Credentials",
            "primary_df_key": "PhysicianCredential",
            "type": "group_and_dict",
            "group_by": "PhysicianId",
            "dict_cols": [
                "Facility",
                "ShowOnWeb"
            ],
            "rename_to": "facility"
        },
        {
            "name": "Specialties",
            "primary_df_key": "PhysicianSpecialty",
            "type": "special_specialty_merge"
        }
    ]


def _merge_group_and_list(
        physician_df: pd.DataFrame,
        df_to_merge: pd.DataFrame,
        config: Dict[str, Any]
) -> pd.DataFrame:
    """Handles the 'group_and_list' merge strategy."""
    grouped = df_to_merge.groupby(config["group_by"])[
        config["agg_col"]].apply(list).reset_index()
    if "rename_to" in config:
        grouped = grouped.rename(
            columns={config["agg_col"]: config["rename_to"]})
    return pd.merge(physician_df, grouped, on=config["group_by"], how="left")


def _merge_group_and_dict(
        physician_df: pd.DataFrame,
        df_to_merge: pd.DataFrame,
        dfs: Dict[str, pd.DataFrame],
        config: Dict[str, Any]
) -> pd.DataFrame:
    """Handles the 'group_and_dict' merge strategy."""
    secondary_key = config.get("secondary_df_key")
    if secondary_key and secondary_key in dfs:
        secondary_df = dfs[secondary_key].rename(
            columns={config["right_on"]: config["left_on"]})
        df_to_merge = pd.merge(df_to_merge, secondary_df,
                               on=config["left_on"], how="left")
        dict_cols = secondary_df.columns.tolist()
    else:
        dict_cols = config["dict_cols"]

    grouped = df_to_merge.groupby(config["group_by"]).apply(
        lambda x: x[dict_cols].to_dict('records')
    ).reset_index(name=config["rename_to"])
    return pd.merge(physician_df, grouped, on=config["group_by"], how="left")


def _merge_specialties(
        physician_df: pd.DataFrame,
        dfs: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Handles the complex merging logic for specialties."""
    specialty_df = dfs['PhysicianSpecialty']
    if 'Specialty' not in dfs:
        return physician_df

    details_df = dfs['Specialty'].rename(columns={'Id': 'SpecialtyId'})
    if 'Synonym' in dfs:
        synonyms = dfs['Synonym'].groupby('SpecialtyId')['SynonymText'].apply(
            list).reset_index(name='SynonymTexts')
        details_df = pd.merge(details_df, synonyms,
                              on='SpecialtyId', how='left')
    if 'Symptom' in dfs:
        symptoms = dfs['Symptom'].groupby('SpecialtyId')['SymptomText'].apply(
            list).reset_index(name='SymptomTexts')
        details_df = pd.merge(details_df, symptoms,
                              on='SpecialtyId', how='left')

    merged_specialties = pd.merge(
        specialty_df, details_df, on='SpecialtyId', how='left')
    cols_to_group = [
        'SpecialtyId',
        'BoardCertification',
        'AcceptingNewPatients',
        'Primary',
        'Name',
        'SynonymTexts',
        'SymptomTexts'
    ]
    valid_cols = [
        col for col in cols_to_group if col in merged_specialties.columns
    ]

    spec_grouped = merged_specialties.groupby('PhysicianId').apply(
        lambda x: x[valid_cols].to_dict('records')
    ).reset_index(name='specialties')
    return pd.merge(physician_df, spec_grouped, on='PhysicianId', how="left")


def process_and_merge_data(
        dfs: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Processes and merges all dataframes based on a declarative configuration.
    """
    logging.info("Step 3: Processing and merging data...")
    if "Physician" not in dfs:
        raise ValueError(
            "Primary 'Physician' dataframe not found. Cannot proceed.")

    physician_df = dfs.pop("Physician").copy()

    for config in get_merge_config():
        logging.info("    - Merging %s...", config['name'])
        primary_key = config.get("primary_df_key")
        if not primary_key or primary_key not in dfs or dfs[primary_key].empty:
            logging.warning(
                "      Skipping %s: primary dataframe '%s' not found or is empty.", config['name'], primary_key)
            continue

        merge_type = config["type"]
        if merge_type == "group_and_list":
            physician_df = _merge_group_and_list(
                physician_df, dfs[primary_key], config)
        elif merge_type == "group_and_dict":
            physician_df = _merge_group_and_dict(
                physician_df, dfs[primary_key], dfs, config)
        elif merge_type == "special_specialty_merge":
            physician_df = _merge_specialties(physician_df, dfs)

    logging.info(">>> Data processing complete.")
    return physician_df


def save_and_finalize(
        df: pd.DataFrame,
        local_data_root_dir: str,
        gcs_upload_prefix: str,
        transformed_filename: str
) -> str:
    """
    Finalizes the DataFrame and saves it as a JSONL file.
    """
    logging.info("Step 4: Finalizing and saving output file...")
    if 'AcceptingNewPatients' in df.columns:
        df = df.drop(columns=['AcceptingNewPatients'])

    output_dir = os.path.join(local_data_root_dir, gcs_upload_prefix)
    os.makedirs(output_dir, exist_ok=True)
    output_file_path = os.path.join(output_dir, transformed_filename)

    try:
        df.to_json(output_file_path, orient='records',
                   lines=True, date_format='iso')
        logging.info("\n✅ Success! DataFrame saved to '%s'", output_file_path)
        return output_file_path
    except Exception as e:
        logging.error(
            "An error occurred during file conversion: %s", e, exc_info=True)
        raise

# --- Main Execution ---


@functions_framework.cloud_event
def main(cloud_event):
    """Cloud Function entry point. Triggered by a GCS event."""
    bucket_name = cloud_event.data['bucket']
    gcs_download_prefix = os.getenv("GCS_DOWNLOAD_PREFIX")
    gcs_upload_prefix = os.getenv("GCS_UPLOAD_PREFIX")
    transformed_filename = os.getenv("TRANSFORMED_FILENAME")
    local_data_root_dir = os.getenv("LOCAL_DATA_ROOT_DIR")

    if not all(
        [
            gcs_download_prefix,
            gcs_upload_prefix,
            transformed_filename,
            local_data_root_dir
        ]
    ):
        logging.error(
            """
            One or more environment variables are not set.
            Please check your .env file.
            """
        )
        return

    local_data_path = download_data_from_gcs(
        bucket_name, gcs_download_prefix, local_data_root_dir)
    dataframes = load_dataframes(local_data_path)

    if not dataframes:
        logging.info("No dataframes were loaded. Exiting.")
        return

    try:
        final_df = process_and_merge_data(dataframes)
        output_file_path = save_and_finalize(
            final_df,
            local_data_root_dir,
            gcs_upload_prefix,
            transformed_filename
        )

        logging.info(
            "Step 5: Uploading transformed file to Google Cloud Storage...")
        destination_blob = os.path.join(
            gcs_upload_prefix, transformed_filename)
        upload_to_gcs(bucket_name, output_file_path, destination_blob)

    except (ValueError, KeyError) as e:
        logging.error("Failed during data processing: %s", e, exc_info=True)
