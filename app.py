"""Main application module for the Discovery Engine API Service."""

import re
import json
import os
import logging
# from datetime import datetime
from uuid import uuid4
from typing import List, Optional, Any, Dict
from fastapi.responses import JSONResponse

from fastapi import FastAPI, HTTPException, status, Depends
from pydantic import BaseModel, Field
from google.cloud import storage, discoveryengine_v1beta as discoveryengine
from google.cloud.discoveryengine_v1beta import (
    ImportDocumentsRequest,
    AnswerQueryRequest,
    # ClientOptions,
    ConversationalSearchServiceClient,
)

from google.api_core.exceptions import GoogleAPICallError
from dotenv import load_dotenv
import pandas as pd

import prompt_lib

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Discovery Engine API Service",
    description="API to ingest and search in Google Cloud Discovery Engine.",
    version="1.0.0"
)

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_BUCKET_REGION = os.getenv("GCS_BUCKET_REGION", "global")
GCS_INGESTION_FOLDER = os.getenv("GCS_INGESTION_FOLDER", "ingestion-data")
DISCOVERY_ENGINE_LOCATION = os.getenv(
    "DISCOVERY_ENGINE_LOCATION")  # e.g., "global"
DISCOVERY_ENGINE_DATASTORE_ID = os.getenv("DISCOVERY_ENGINE_DATASTORE_ID")
ENGINE_NAME = os.getenv("ENGINE_NAME")
API_ENDPOINT = f"{DISCOVERY_ENGINE_LOCATION}-discoveryengine.googleapis.com"
OUTPUT_FILENAME = "transformed_specialty_data.jsonl"
RANKING_MODEL_NAME = os.getenv(
    "RANKING_MODEL_NAME", "semantic-ranker-default@latest")
GEMINI_MODEL_NAME = os.getenv(
    "GEMINI_MODEL_NAME", "gemini-2.5-flash/answer_gen/v1")
# Basic validation to ensure environment variables are set
if not all(
    [
        GCP_PROJECT_ID,
        GCP_REGION,
        GCS_BUCKET_NAME,
        DISCOVERY_ENGINE_LOCATION,
        DISCOVERY_ENGINE_DATASTORE_ID,
        ENGINE_NAME
    ]
):
    raise RuntimeError(
        """One or more required environment variables are not set.
        Please check your configuration."""
    )

# --- Pydantic Models for Request/Response Schemas ---


class QueryRequest(BaseModel):
    """Request model for the user query."""
    query: str

# Since the LLM response structure is well-defined, we can model it.
# This adds a layer of validation to the data we receive from the LLM.


class FilterModel(BaseModel):
    """Model for the filters in the recommendation response."""
    AcceptingNewPatients: Optional[bool] = None
    FinancialAssistance: Optional[bool] = None
    Gender: Optional[str] = None
    Insurance: Optional[str] = None
    Language: Optional[str] = None
    Location: Optional[str] = None
    OpenScheduling: Optional[bool] = None
    Organization: Optional[str] = None
    Practice: Optional[str] = None
    Rating: Optional[int] = None
    specialty_rollup: Optional[List[str]] = None


class RecommendationModel(BaseModel):
    """Model for each recommendation in the response."""
    id: int
    reason: str
    specialty: str
    type: str
    score: Optional[float] = None  # Added score field for ranking results


class RecommendationResponse(BaseModel):
    """The final JSON response structure."""
    filters: FilterModel
    recommendations: List[RecommendationModel]


class IngestDataRequest(BaseModel):
    """
    Defines the expected structure of the JSON payload for /ingest endpoint.
    """
    data: Dict[str, Any] = Field(
        ...,
        description="A large JSON object with approximately nested sub-JSONs."
    )


class SearchRequest(BaseModel):
    """
    Defines the request body for the /search endpoint.
    """
    query: str = Field(..., description="The search query text.",
                       example="What are the latest product updates?")
    page_size: int = Field(
        10,
        description="Number of search results to return.",
        example=10
    )


class SearchResultDocument(BaseModel):
    """
    Represents a single document in the search results.
    """
    id: str
    data: Dict[str, Any]
    score: float


class SearchApiResponse(BaseModel):
    """
    Defines the successful response structure for the /search endpoint.
    """
    results: List[SearchResultDocument]
    total_size: int
    attribution_token: str

# --- Google Cloud Clients ---


def get_gcs_client():
    """Dependency to get a GCS client."""
    try:
        return storage.Client(project=GCP_PROJECT_ID)
    except Exception as e:
        logger.error("Failed to create GCS client: %s", e)
        raise e


def get_discovery_engine_search_client():
    """Dependency to get a Discovery Engine search client."""
    try:
        client_options = {
            "api_endpoint": API_ENDPOINT
        }
        return discoveryengine.SearchServiceClient(
            client_options=client_options
        )
    except Exception as e:
        logger.error("Failed to create Discovery Engine Search client: %s", e)
        raise e


def get_discovery_engine_document_client():
    """Dependency to get a Discovery Engine document client for ingestion."""
    try:
        client_options = {
            "api_endpoint": API_ENDPOINT
        }
        return discoveryengine.DocumentServiceClient(
            client_options=client_options
        )
    except Exception as e:
        logger.error(
            "Failed to create Discovery Engine Document client: %s", e
        )
        raise e


# --- Core Logic Functions ---

def transform_data_to_jsonl(raw_data: Dict[str, Any]) -> str:
    """
    Transforms the nested dictionary into a JSONL string.

    Args:
        raw_data: The input dictionary from the request.

    Returns:
        A string where each line is a JSON object.
    """
    df_rollup = pd.DataFrame(raw_data['PhysicianRollupSpecialties'])
    df_specialties = pd.DataFrame(raw_data['Specialty'])
    df_symptoms = pd.DataFrame(raw_data['Symptom'])
    df_synonyms = pd.DataFrame(raw_data['Synonym'])
    df_area_of_expertise = pd.DataFrame(raw_data['AreaOfExpertise'])

    # Perform Specialty Roll-up
    df_specialties.rename(
        columns={'Id': 'SpecialtyId', 'Name': 'CanonicalSpecialtyName'},
        inplace=True
    )
    df_merged = pd.merge(df_rollup, df_specialties,
                         on='SpecialtyId', how='left')
    parent_lookup_df = df_rollup[['Id', 'Specialty']].rename(
        columns={'Id': 'ParentSpecialty', 'Specialty': 'ParentSpecialtyName'}
    )
    df_rolled_up = pd.merge(df_merged, parent_lookup_df,
                            on='ParentSpecialty', how='left')
    df_rolled_up['ParentSpecialtyName'].fillna('', inplace=True)

    # Map Symptoms and Synonyms
    df_symptoms_agg = df_symptoms.groupby(
        'SpecialtyId')['SymptomText'].apply(list).reset_index()
    df_synonyms_agg = df_synonyms.groupby(
        'SpecialtyId')['SynonymText'].apply(list).reset_index()
    df_final = pd.merge(df_rolled_up, df_symptoms_agg,
                        on='SpecialtyId', how='left')
    df_final = pd.merge(df_final, df_synonyms_agg,
                        on='SpecialtyId', how='left')
    df_final['SymptomText'] = df_final['SymptomText'].apply(
        lambda x: x if isinstance(x, list) else [])
    df_final['SynonymText'] = df_final['SynonymText'].apply(
        lambda x: x if isinstance(x, list) else [])
    
    df_final.fillna(None, inplace=True)

    records = []

    for rec in (
        df_final.to_dict('records') +
        df_area_of_expertise.to_dict('records')
    ):
        rec['_id'] = str(uuid4())
        records.append(str(rec))

    return "\n".join(records)


def upload_to_gcs(
        gcs_client: storage.Client,
        content: str,
        file_name: str
) -> str:
    """
    Uploads a string content to a specified GCS bucket.

    Args:
        gcs_client: The GCS storage client.
        content: The string content to upload (our JSONL data).
        file_name: The name for the file in GCS.

    Returns:
        The GCS URI of the uploaded file
    """
    try:
        bucket = gcs_client.get_bucket(GCS_BUCKET_NAME)
        blob_path = f"{GCS_INGESTION_FOLDER}/{file_name}"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(content, content_type="application/jsonl")
        gcs_uri = f"gs://{GCS_BUCKET_NAME}/{blob_path}"
        logger.info("Successfully uploaded data to %s", gcs_uri)
        return gcs_uri
    except GoogleAPICallError as e:
        logger.error("GCS Upload Failed: %s", e)
        raise e

# --- API Endpoints ---


def purge_all_documents(
    doc_client: discoveryengine.DocumentServiceClient = Depends(
        get_discovery_engine_document_client)
):
    """
    Permanently deletes ALL documents from the Discovery Engine data store.

    This is a destructive and irreversible action. It's typically used before
    a complete re-ingestion of data. The process is asynchronous.
    """
    logger.warning(
        "Received request to PURGE ALL DOCUMENTS from the datastore.")
    try:
        parent = doc_client.branch_path(
            project=GCP_PROJECT_ID,
            location=DISCOVERY_ENGINE_LOCATION,
            data_store=DISCOVERY_ENGINE_DATASTORE_ID,
            branch="default_branch",
        )

        request = discoveryengine.PurgeDocumentsRequest(
            parent=parent,
            filter="*",
            force=True,
        )

        operation = doc_client.purge_documents(request=request)
        logger.info(
            "Started Discovery Engine purge operation: %s",
            operation.operation.name
        )
        return operation.operation.name

    except GoogleAPICallError as e:
        logger.error("Discovery Engine Purge API call failed: %s", e)
        raise e


@app.post("/ingest", status_code=status.HTTP_202_ACCEPTED)
async def ingest_data(
    payload: IngestDataRequest,
    gcs_client: storage.Client = Depends(get_gcs_client),
    doc_client: discoveryengine.DocumentServiceClient = Depends(
        get_discovery_engine_document_client)
):
    """
    Ingests a large JSON object into the Discovery Engine.

    The process involves:
    1.  **Transforming** the input JSON into a JSONL format.
    2.  **Uploading** the JSONL file to a Google Cloud Storage bucket.
    3.  **Initiating** an import job in Discovery Engine from the GCS file.

    This endpoint returns a 202 Accepted response as the ingestion process
    is asynchronous and handled by Google Cloud.
    """
    # 1. Transform the data
    logger.info("Starting data transformation to JSONL format.")
    try:
        jsonl_data = transform_data_to_jsonl(payload.data)
        if not jsonl_data:
            raise HTTPException(
                status_code=400,
                detail="Check input format."
            )
    except Exception as e:
        logger.error("Error during data transformation: %s", e)
        raise e

    logger.info("Data transformation completed successfully.")
    logger.info("Purging all existing documents in the datastore.")
    purge_all_documents(doc_client)
    logger.info("Starting GCS upload of the transformed data.")

    # 2. Upload to GCS

    gcs_uri = upload_to_gcs(gcs_client, jsonl_data, OUTPUT_FILENAME)

    # # # 3. Call Discovery Engine API to ingest from GCS
    try:
        parent = doc_client.branch_path(
            project=GCP_PROJECT_ID,
            location=DISCOVERY_ENGINE_LOCATION,
            data_store=DISCOVERY_ENGINE_DATASTORE_ID,
            branch="default_branch",
        )
        request = discoveryengine.ImportDocumentsRequest(
            parent=parent,
            gcs_source=discoveryengine.GcsSource(
                input_uris=[gcs_uri],
                data_schema="custom"
            ),
            reconciliation_mode=ImportDocumentsRequest.ReconciliationMode.FULL,
        )
        operation = doc_client.import_documents(request=request)
        logger.info(
            "Started Discovery Engine import operation: %s",
            operation.operation.name
        )
        return {
            "message": "Ingestion process started successfully.",
            "gcs_uri": gcs_uri,
            "operation_name": operation.operation.name
        }

    except GoogleAPICallError as e:
        logger.error("Discovery Engine Import API call failed: %s", e)
        raise e


def get_recommendations_from_engine(query: str) -> str:
    """
    Calls the Discovery Engine to get recommendations based on a user query.

    Args:
        query: The user's natural language query.

    Returns:
        A string containing the JSON response from the model.

    Raises:
        GoogleAPICallError: If the API call fails.
    """
    try:

        client = ConversationalSearchServiceClient()
        serving_config = "/".join([
            "projects",
            GCP_PROJECT_ID,
            "locations",
            GCS_BUCKET_REGION,
            "collections",
            "default_collection",
            "engines",
            ENGINE_NAME,
            "servingConfigs",
            "default_serving_config",
        ])

        search_spec = AnswerQueryRequest.SearchSpec(
            search_params=AnswerQueryRequest.SearchSpec.SearchParams(
                max_return_results=20
            )
        )

        answer_generation_spec = AnswerQueryRequest.AnswerGenerationSpec(
            ignore_adversarial_query=False,
            ignore_non_answer_seeking_query=False,
            ignore_low_relevant_content=False,
            model_spec=AnswerQueryRequest.AnswerGenerationSpec.ModelSpec(
                model_version=GEMINI_MODEL_NAME,
            ),
            prompt_spec=AnswerQueryRequest.AnswerGenerationSpec.PromptSpec(
                preamble=prompt_lib.SPECIALTY_RECOMMENDATION_PROMPT
            ),
            include_citations=True,
            answer_language_code="en",
        )

        request = discoveryengine.AnswerQueryRequest(
            serving_config=serving_config,
            query=discoveryengine.Query(text=query),
            search_spec=search_spec,
            session=None,
            answer_generation_spec=answer_generation_spec,
        )

        logger.info("Sending query to Discovery Engine: '%s'", query)
        response = client.answer_query(request)
        logger.info("Successfully received response from Discovery Engine.")

        return response.answer.answer_text

    except GoogleAPICallError as e:
        logger.error("Google API call failed: %s", e)
        raise
    except Exception as e:
        logger.error("An unexpected error occurred in services.py: %s", e)
        raise


def _sanitize_and_load_llm_response(llm_response_str: str) -> dict:
    """
    Cleans the raw LLM response string, fixes common JSON errors,
    parses it, and merges with default values to ensure a valid structure.
    """
    cleaned_str = llm_response_str.strip()
    if cleaned_str.startswith("```json"):
        cleaned_str = cleaned_str.lstrip("```json").rstrip("```").strip()

    pattern = re.compile(r'("[\w_]+"\s*:\s*)(})', re.DOTALL)
    cleaned_str = pattern.sub(r'\1[]\2', cleaned_str)

    try:
        response_json = json.loads(cleaned_str)
    except json.JSONDecodeError as e:
        logger.error(
            "Failed to parse JSON from LLM response even after cleaning: %s",
            e
        )
        raise e

    default_response = {
        "filters": {
            "AcceptingNewPatients": None,
            "FinancialAssistance": None,
            "Gender": None,
            "Insurance": None,
            "Language": None,
            "Location": None,
            "OpenScheduling": None,
            "Organization": None,
            "Practice": None,
            "Rating": None,
            "specialty_rollup": [],
        },
        "recommendations": []
    }

    final_response = default_response.copy()
    if isinstance(response_json.get("filters"), dict):
        final_response["filters"].update(response_json["filters"])

    if isinstance(response_json.get("recommendations"), list):
        final_response["recommendations"] = response_json["recommendations"]

    if final_response.get("filters", {}).get("specialty_rollup") is None:
        final_response["filters"]["specialty_rollup"] = []

    return final_response


def rank_recommendations(query: str, recommendations: List[Dict]) -> List[Dict]:
    """
    Ranks a list of recommendations using the Discovery Engine Rank API.

    Args:
        query: The original user query.
        recommendations: A list of recommendation dictionaries from the first
        LLM call.

    Returns:
        A list of recommendation dictionaries, sorted by the new 'score' field.
    """
    if not recommendations:
        logger.info("No recommendations to rank.")
        return []

    try:
        client = discoveryengine.RankServiceClient()

        ranking_config = client.ranking_config_path(
            project=GCP_PROJECT_ID,
            location=GCS_BUCKET_REGION,
            ranking_config=RANKING_MODEL_NAME,
        )

        records_to_rank = [
            discoveryengine.RankingRecord(
                id=str(item["id"]),
                title=item["specialty"],
                content=item["reason"],
            )
            for item in recommendations
        ]

        request = discoveryengine.RankRequest(
            ranking_config=ranking_config,
            model=RANKING_MODEL_NAME,
            top_n=len(records_to_rank),
            query=query,
            records=records_to_rank,
        )

        logger.info(
            "Sending %d records to Rank API for query: '%s'",
            len(records_to_rank),
            query
        )
        response = client.rank(request=request)
        logger.info("Successfully received response from Rank API.")

        scores_map = {record.id: record.score for record in response.records}

        for item in recommendations:
            item_id_str = str(item['id'])
            item['score'] = scores_map.get(item_id_str, 0.0)

        # Sort the recommendations by score in descending order
        sorted_recommendations = sorted(
            recommendations, key=lambda x: x['score'], reverse=True)

        return sorted_recommendations

    except GoogleAPICallError as e:
        logger.error("Google API call for ranking failed: %s", e)
        # If ranking fails, return the original list without scores to
        # avoid breaking the response
        return recommendations
    except Exception as e:  # @pylint: disable=broad-except
        logger.error("An unexpected error occurred during ranking: %s", e)
        return recommendations


@app.post("/search", response_model=SearchApiResponse)
async def search(
    request: SearchRequest,
):
    """
    Searches the Discovery Engine data store with the given query.

    - Receives a search query.
    - Calls the Discovery Engine Search API.
    - Formats and returns the search results.
    """
    try:
        query_text = request.query
        if not query_text or not query_text.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Query cannot be empty."
            )

        llm_response_str = get_recommendations_from_engine(query_text)
        response_json = _sanitize_and_load_llm_response(llm_response_str)
        initial_response = RecommendationResponse.model_validate(
            response_json)
        if initial_response.recommendations:
            recs_as_dicts = [rec.model_dump()
                             for rec in initial_response.recommendations]
            ranked_recs = rank_recommendations(
                query_text, recs_as_dicts)
            initial_response.recommendations = [
                RecommendationModel(**rec) for rec in ranked_recs]
        return JSONResponse(content=initial_response.model_dump())

    except GoogleAPICallError as e:
        logger.error("Discovery Engine Search API call failed: %s", e)
        raise e

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
