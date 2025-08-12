#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A Python script to set up a Google Cloud Discovery Engine environment.

This script performs the following actions in order:
1.  Creates a new Discovery Engine Datastore.
2.  Ingests document data from a JSONL file located in a GCS bucket.
3.  Deletes temporary local JSON files created during the ingestion process.
4.  Creates a new Discovery Engine App (Engine) linked to the datastore.

This script is designed to be run from the command line and requires all configuration
parameters to be provided as arguments.
"""

import argparse
import json
import os
import glob
from typing import List
import sys

# Third-party import for the progress bar
from tqdm import tqdm

from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import AlreadyExists
from google.cloud.exceptions import GoogleCloudError
from google.cloud import discoveryengine_v1beta as discoveryengine
from google.cloud import storage


def get_document_service_client(
    project_id: str,
    location: str,
) -> discoveryengine.DocumentServiceClient:
    """Creates a DocumentServiceClient with an optional regional endpoint."""
    client_options = (
        ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
        if location != "global"
        else None
    )
    return discoveryengine.DocumentServiceClient(client_options=client_options)


def create_document_object(
    project_id: str,
    location: str,
    datastore_name: str,
    document_id: str,
    json_data: str,
    schema_id: str = None,
) -> discoveryengine.Document:
    """Prepares a Discovery Engine Document object."""
    name = f"projects/{project_id}/locations/{location}/collections/default_collection/dataStores/{datastore_name}/documents/{document_id}"
    document = discoveryengine.Document(
        schema_id=schema_id,
        json_data=json_data,
        id=document_id,
        name=name
    )
    return document


def delete_document(
    client: discoveryengine.DocumentServiceClient,
    project_id: str,
    location: str,
    datastore_name: str,
    document_id: str,
) -> None:
    """Deletes a document from a Discovery Engine data store."""
    name = client.document_path(
        project=project_id,
        location=location,
        data_store=datastore_name,
        branch="default_branch",
        document=document_id,
    )
    try:
        client.delete_document(name=name)
    except GoogleCloudError as e:
        # Log only errors
        print(f"Error deleting document {document_id}: {e}")


def create_or_update_document(
    client: discoveryengine.DocumentServiceClient,
    project_id: str,
    location: str,
    datastore_name: str,
    document: discoveryengine.Document,
) -> None:
    """Creates or updates a document in a Discovery Engine data store."""
    name = client.document_path(
        project=project_id,
        location=location,
        data_store=datastore_name,
        branch="default_branch",
        document=document.id,
    )
    try:
        document_exists = False
        try:
            client.get_document(name=name)
            document_exists = True
        except Exception:
            pass  # Document doesn't exist

        if document_exists:
            delete_document(client, project_id, location, datastore_name, document.id)

        parent = client.branch_path(
            project=project_id,
            location=location,
            data_store=datastore_name,
            branch="default_branch",
        )

        client.create_document(
            parent=parent,
            document=document,
            document_id=document.id,
        )
    except GoogleCloudError as e:
        print(f"Error creating or updating document {document.id}: {e}")
        print(f"Failed Document Data: {document.json_data}")


def ingest_data_from_gcs(
    client: discoveryengine.DocumentServiceClient,
    project_id: str,
    location: str,
    datastore_name: str,
    gcs_bucket_name: str,
    gcs_file_name: str,
):
    """Ingests data from a GCS JSONL file into a Discovery Engine Datastore."""
    print(f"Starting ingestion from gs://{gcs_bucket_name}/{gcs_file_name}...")
    os.makedirs("tmp", exist_ok=True)

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_file_name)

    try:
        jsonl_content = blob.download_as_text(encoding="utf-8")
        lines = jsonl_content.strip().split('\n')
        print(f"Found {len(lines)} records to process.")

        # Use tqdm for a progress bar instead of printing each line
        for line in tqdm(lines, desc="Ingesting documents", unit="doc", file=sys.stderr):
            try:
                data = json.loads(line)
                physician_id = data.get("PhysicianId")
                if not physician_id:
                    print(f"Skipping record due to missing 'PhysicianId': {line}")
                    continue

                document_id = str(physician_id)

                with open(f'tmp/output_{physician_id}.json', "w") as f:
                    json.dump(data, f)

                if data.get('is_deleted', '0') == '1':
                    delete_document(client, project_id, location, datastore_name, document_id)
                else:
                    document = create_document_object(
                        project_id=project_id,
                        location=location,
                        datastore_name=datastore_name,
                        document_id=document_id,
                        json_data=json.dumps(data),
                    )
                    create_or_update_document(client, project_id, location, datastore_name, document)

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON line. Skipping. Error: {e}")
                continue

    except GoogleCloudError as e:
        print(f"Error reading file {gcs_file_name} from GCS: {e}")


def create_datastore(
    project_id: str,
    location: str,
    data_store_id: str,
    display_name: str,
) -> None:
    """Creates a Discovery Engine Datastore."""
    client_options = (
        ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
        if location != "global"
        else None
    )
    client = discoveryengine.DataStoreServiceClient(client_options=client_options)

    parent = client.collection_path(
        project=project_id, location=location, collection="default_collection"
    )

    data_store = discoveryengine.DataStore(
        display_name=display_name,
        industry_vertical=discoveryengine.IndustryVertical.GENERIC,
        solution_types=[discoveryengine.SolutionType.SOLUTION_TYPE_SEARCH],
        content_config=discoveryengine.DataStore.ContentConfig.NO_CONTENT,
    )

    request = discoveryengine.CreateDataStoreRequest(
        parent=parent, data_store_id=data_store_id, data_store=data_store
    )

    operation = client.create_data_store(request=request)
    print(f"Waiting for datastore creation operation to complete: {operation.operation.name}")
    response = operation.result()
    print(f"Successfully created datastore: {response.name}")


def create_engine(
    project_id: str,
    location: str,
    engine_id: str,
    display_name: str,
    data_store_ids: List[str]
) -> None:
    """Creates a Discovery Engine App (Engine)."""
    client_options = (
        ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
        if location != "global"
        else None
    )
    client = discoveryengine.EngineServiceClient(client_options=client_options)

    parent = client.collection_path(
        project=project_id, location=location, collection="default_collection"
    )

    engine = discoveryengine.Engine(
        display_name=display_name,
        industry_vertical=discoveryengine.IndustryVertical.GENERIC,
        solution_type=discoveryengine.SolutionType.SOLUTION_TYPE_SEARCH,
        search_engine_config=discoveryengine.Engine.SearchEngineConfig(
            search_tier=discoveryengine.SearchTier.SEARCH_TIER_ENTERPRISE,
            search_add_ons=[discoveryengine.SearchAddOn.SEARCH_ADD_ON_LLM],
        ),
        data_store_ids=data_store_ids,
    )

    request = discoveryengine.CreateEngineRequest(
        parent=parent, engine=engine, engine_id=engine_id
    )

    operation = client.create_engine(request=request)
    print(f"Waiting for engine creation operation to complete: {operation.operation.name}")
    response = operation.result()
    print(f"Successfully created engine: {response.name}")


def delete_local_json_files() -> None:
    """Deletes all temporary 'output_*.json' files in the 'tmp/' directory."""
    pattern = "tmp/output_*.json"
    files_to_delete = glob.glob(pattern)

    if not files_to_delete:
        return

    for file_path in files_to_delete:
        try:
            os.remove(file_path)
        except OSError as e:
            print(f"Error deleting temporary file {file_path}: {e}")
    
    print(f"Cleaned up {len(files_to_delete)} temporary local file(s).")


def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(
        description="Set up Google Cloud Discovery Engine Datastore and App."
    )
    parser.add_argument(
        "--project_id", type=str, required=True, help="Required. Google Cloud Project ID."
    )
    parser.add_argument(
        "--location", type=str, required=True, help="Required. Location for Discovery Engine resources ('global', 'eu', 'us')."
    )
    parser.add_argument(
        "--datastore_name", type=str, required=True, help="Required. Unique ID for the datastore."
    )
    parser.add_argument(
        "--engine_name", type=str, required=True, help="Required. Unique ID for the engine/app."
    )
    parser.add_argument(
        "--gcs_bucket", type=str, required=True, help="Required. GCS bucket containing the source data."
    )
    parser.add_argument(
        "--gcs_file", type=str, required=True, help="Required. Path to the JSONL file within the GCS bucket."
    )
    args = parser.parse_args()

    print("--- Starting Discovery Engine Setup Script ---")
    print(f"  Project ID: {args.project_id}")
    print(f"  Location: {args.location}")
    print("-" * 45)

    # Step 1: Create the Datastore
    print("\n[STEP 1/4] Creating Datastore...")
    try:
        create_datastore(
            args.project_id, args.location, args.datastore_name, args.datastore_name
        )
    except AlreadyExists:
        print(f"Datastore '{args.datastore_name}' already exists. Skipping creation.")
    except Exception as e:
        print(f"An unexpected error occurred during datastore creation: {e}")
        return

    # Step 2: Ingest Data from GCS
    print("\n[STEP 2/4] Ingesting documents from GCS...")
    try:
        doc_client = get_document_service_client(args.project_id, args.location)
        ingest_data_from_gcs(
            client=doc_client,
            project_id=args.project_id,
            location=args.location,
            datastore_name=args.datastore_name,
            gcs_bucket_name=args.gcs_bucket,
            gcs_file_name=args.gcs_file,
        )
        print(f"Completed ingestion for file - {args.gcs_file}")
    except Exception as e:
        print(f"An unexpected error occurred during data ingestion: {e}")

    # Step 3: Delete local temporary JSON files
    print("\n[STEP 3/4] Cleaning up local temporary files...")
    delete_local_json_files()

    # Step 4: Create the Engine (App)
    print("\n[STEP 4/4] Creating Engine/App...")
    try:
        create_engine(
            project_id=args.project_id,
            location=args.location,
            engine_id=args.engine_name,
            display_name=args.engine_name,
            data_store_ids=[args.datastore_name],
        )
    except AlreadyExists:
        print(f"Engine '{args.engine_name}' already exists. Skipping creation.")
    except Exception as e:
        print(f"An unexpected error occurred during engine creation: {e}")

    print("\n--- Script execution finished. ---")


if __name__ == "__main__":
    main()
