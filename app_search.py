"""Main application module for the Discovery Engine API Service."""

import re
import json
import os
import logging
from uuid import uuid4
from typing import List, Optional, Any, Dict
from fastapi.responses import JSONResponse

from fastapi import FastAPI, HTTPException, status, Depends
from pydantic import BaseModel, Field
from google.cloud import storage, discoveryengine_v1beta as discoveryengine
from google.cloud.discoveryengine_v1beta import (
    ImportDocumentsRequest,
    AnswerQueryRequest,
    ConversationalSearchServiceClient,
)
from google.api_core.client_options import ClientOptions  # <-- ADDED IMPORT
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

# --- Environment Variable Loading and Configuration ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_BUCKET_REGION = os.getenv("GCS_BUCKET_REGION", "us")
GCS_INGESTION_FOLDER = os.getenv("GCS_INGESTION_FOLDER", "Transformed_Data/specialty-data")
DISCOVERY_ENGINE_LOCATION = os.getenv("DISCOVERY_ENGINE_LOCATION")
DISCOVERY_ENGINE_DATASTORE_ID = os.getenv("DISCOVERY_ENGINE_DATASTORE_ID")
ENGINE_NAME = os.getenv("ENGINE_NAME")
OUTPUT_FILENAME = "transformed_specialty_data.jsonl"
RANKING_MODEL_NAME = os.getenv("RANKING_MODEL_NAME", "semantic-ranker-default@latest")
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash/answer_gen/v1")

# --- CORRECTED ENDPOINT LOGIC ---
if DISCOVERY_ENGINE_LOCATION == "global":
    API_ENDPOINT = "discoveryengine.googleapis.com"
else:
    API_ENDPOINT = f"{DISCOVERY_ENGINE_LOCATION}-discoveryengine.googleapis.com"

# Basic validation to ensure environment variables are set
if not all(
    [
        GCP_PROJECT_ID, GCP_REGION, GCS_BUCKET_NAME,
        DISCOVERY_ENGINE_LOCATION, DISCOVERY_ENGINE_DATASTORE_ID, ENGINE_NAME
    ]
):
    raise RuntimeError("One or more required environment variables are not set.")

# --- Pydantic Models ---
class QueryRequest(BaseModel):
    query: str

class FilterModel(BaseModel):
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
    id: int
    reason: str
    specialty: str
    type: str
    score: Optional[float] = None

class RecommendationResponse(BaseModel):
    filters: FilterModel
    recommendations: List[RecommendationModel]

class IngestDataRequest(BaseModel):
    data: Dict[str, Any]

class SearchRequest(BaseModel):
    query: str
    page_size: int = 10

class SearchResultDocument(BaseModel):
    id: str
    data: Dict[str, Any]
    score: float

class SearchApiResponse(BaseModel):
    results: List[SearchResultDocument]
    total_size: int
    attribution_token: str

# --- Google Cloud Clients ---
def get_gcs_client():
    try:
        return storage.Client(project=GCP_PROJECT_ID)
    except Exception as e:
        logger.error("Failed to create GCS client: %s", e)
        raise

def get_discovery_engine_search_client():
    try:
        client_options = ClientOptions(api_endpoint=API_ENDPOINT)
        return discoveryengine.SearchServiceClient(client_options=client_options)
    except Exception as e:
        logger.error("Failed to create Discovery Engine Search client: %s", e)
        raise

def get_discovery_engine_document_client():
    try:
        client_options = ClientOptions(api_endpoint=API_ENDPOINT)
        return discoveryengine.DocumentServiceClient(client_options=client_options)
    except Exception as e:
        logger.error("Failed to create Discovery Engine Document client: %s", e)
        raise

# --- Core Logic Functions ---
def transform_data_to_jsonl(raw_data: Dict[str, Any]) -> str:
    df_rollup = pd.DataFrame(raw_data['PhysicianRollupSpecialties'])
    df_specialties = pd.DataFrame(raw_data['Specialty'])
    df_symptoms = pd.DataFrame(raw_data['Symptom'])
    df_synonyms = pd.DataFrame(raw_data['Synonym'])
    df_area_of_expertise = pd.DataFrame(raw_data['AreaOfExpertise'])
    df_specialties.rename(columns={'Id': 'SpecialtyId', 'Name': 'CanonicalSpecialtyName'}, inplace=True)
    df_merged = pd.merge(df_rollup, df_specialties, on='SpecialtyId', how='left')
    parent_lookup_df = df_rollup[['Id', 'Specialty']].rename(columns={'Id': 'ParentSpecialty', 'Specialty': 'ParentSpecialtyName'})
    df_rolled_up = pd.merge(df_merged, parent_lookup_df, on='ParentSpecialty', how='left')
    #df_rolled_up['ParentSpecialtyName'].fillna('', inplace=True)
    # In the transform_data_to_jsonl function...
    df_rolled_up['ParentSpecialtyName'] = df_rolled_up['ParentSpecialtyName'].fillna('')
    df_symptoms_agg = df_symptoms.groupby('SpecialtyId')['SymptomText'].apply(list).reset_index()
    df_synonyms_agg = df_synonyms.groupby('SpecialtyId')['SynonymText'].apply(list).reset_index()
    df_final = pd.merge(df_rolled_up, df_symptoms_agg, on='SpecialtyId', how='left')
    df_final = pd.merge(df_final, df_synonyms_agg, on='SpecialtyId', how='left')
    df_final['SymptomText'] = df_final['SymptomText'].apply(lambda x: x if isinstance(x, list) else [])
    df_final['SynonymText'] = df_final['SynonymText'].apply(lambda x: x if isinstance(x, list) else [])
    df_final.fillna(value=None, inplace=True)
    records = []
    for rec in (df_final.to_dict('records') + df_area_of_expertise.to_dict('records')):
        rec['_id'] = str(uuid4())
        records.append(str(rec))
    return "\n".join(records)

def upload_to_gcs(gcs_client: storage.Client, content: str, file_name: str) -> str:
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
        raise

# --- API Endpoints ---
def purge_all_documents(doc_client: discoveryengine.DocumentServiceClient = Depends(get_discovery_engine_document_client)):
    logger.warning("Received request to PURGE ALL DOCUMENTS from the datastore.")
    try:
        parent = doc_client.branch_path(
            project=GCP_PROJECT_ID,
            location=DISCOVERY_ENGINE_LOCATION,
            data_store=DISCOVERY_ENGINE_DATASTORE_ID,
            branch="default_branch",
        )
        request = discoveryengine.PurgeDocumentsRequest(parent=parent, filter="*", force=True)
        operation = doc_client.purge_documents(request=request)
        logger.info("Started Discovery Engine purge operation: %s", operation.operation.name)
        return operation.operation.name
    except GoogleAPICallError as e:
        logger.error("Discovery Engine Purge API call failed: %s", e)
        raise

@app.post("/ingest", status_code=status.HTTP_202_ACCEPTED)
async def ingest_data(
    payload: IngestDataRequest,
    gcs_client: storage.Client = Depends(get_gcs_client),
    doc_client: discoveryengine.DocumentServiceClient = Depends(get_discovery_engine_document_client)
):
    logger.info("Starting data transformation to JSONL format.")
    try:
        jsonl_data = transform_data_to_jsonl(payload.data)
        if not jsonl_data:
            raise HTTPException(status_code=400, detail="Check input format.")
    except Exception as e:
        logger.error("Error during data transformation: %s", e)
        raise
    logger.info("Data transformation completed successfully.")
    logger.info("Purging all existing documents in the datastore.")
    purge_all_documents(doc_client)
    logger.info("Starting GCS upload of the transformed data.")
    gcs_uri = upload_to_gcs(gcs_client, jsonl_data, OUTPUT_FILENAME)
    try:
        parent = doc_client.branch_path(
            project=GCP_PROJECT_ID,
            location=DISCOVERY_ENGINE_LOCATION,
            data_store=DISCOVERY_ENGINE_DATASTORE_ID,
            branch="default_branch",
        )
        request = discoveryengine.ImportDocumentsRequest(
            parent=parent,
            gcs_source=discoveryengine.GcsSource(input_uris=[gcs_uri], data_schema="custom"),
            reconciliation_mode=ImportDocumentsRequest.ReconciliationMode.FULL,
        )
        operation = doc_client.import_documents(request=request)
        logger.info("Started Discovery Engine import operation: %s", operation.operation.name)
        return {"message": "Ingestion process started successfully.", "gcs_uri": gcs_uri, "operation_name": operation.operation.name}
    except GoogleAPICallError as e:
        logger.error("Discovery Engine Import API call failed: %s", e)
        raise

def get_recommendations_from_engine(query: str) -> str:
    try:
        client_options = ClientOptions(api_endpoint=API_ENDPOINT)
        client = ConversationalSearchServiceClient(client_options=client_options)
        serving_config = "/".join([
            "projects", GCP_PROJECT_ID,
            "locations", DISCOVERY_ENGINE_LOCATION,
            "collections", "default_collection",
            "engines", ENGINE_NAME,
            "servingConfigs", "default_serving_config",
        ])
        search_spec = AnswerQueryRequest.SearchSpec(search_params=AnswerQueryRequest.SearchSpec.SearchParams(max_return_results=20))
        answer_generation_spec = AnswerQueryRequest.AnswerGenerationSpec(
            model_spec=AnswerQueryRequest.AnswerGenerationSpec.ModelSpec(model_version=GEMINI_MODEL_NAME),
            prompt_spec=AnswerQueryRequest.AnswerGenerationSpec.PromptSpec(preamble=prompt_lib.SPECIALTY_RECOMMENDATION_PROMPT),
        )
        request = discoveryengine.AnswerQueryRequest(
            serving_config=serving_config,
            query=discoveryengine.Query(text=query),
            search_spec=search_spec,
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
        logger.error("An unexpected error occurred: %s", e)
        raise

def _sanitize_and_load_llm_response(llm_response_str: str) -> dict:
    cleaned_str = llm_response_str.strip()
    if cleaned_str.startswith("```json"):
        cleaned_str = cleaned_str.lstrip("```json").rstrip("```").strip()
    pattern = re.compile(r'("[\w_]+"\s*:\s*)(})', re.DOTALL)
    cleaned_str = pattern.sub(r'\1[]\2', cleaned_str)
    try:
        response_json = json.loads(cleaned_str)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse JSON from LLM response: %s", e)
        raise
    default_response = {"filters": {}, "recommendations": []}
    final_response = default_response.copy()
    if isinstance(response_json.get("filters"), dict):
        final_response["filters"].update(response_json["filters"])
    if isinstance(response_json.get("recommendations"), list):
        final_response["recommendations"] = response_json["recommendations"]
    return final_response

def rank_recommendations(query: str, recommendations: List[Dict]) -> List[Dict]:
    if not recommendations:
        return []
    try:
        client_options = ClientOptions(api_endpoint=API_ENDPOINT)
        client = discoveryengine.RankServiceClient(client_options=client_options)
        ranking_config = client.ranking_config_path(
            project=GCP_PROJECT_ID,
            location=DISCOVERY_ENGINE_LOCATION,
            ranking_config=RANKING_MODEL_NAME,
        )
        records_to_rank = [
            discoveryengine.RankingRecord(id=str(item["id"]), title=item["specialty"], content=item["reason"])
            for item in recommendations
        ]
        request = discoveryengine.RankRequest(
            ranking_config=ranking_config,
            model=RANKING_MODEL_NAME,
            top_n=len(records_to_rank),
            query=query,
            records=records_to_rank,
        )
        response = client.rank(request=request)
        scores_map = {record.id: record.score for record in response.records}
        for item in recommendations:
            item['score'] = scores_map.get(str(item['id']), 0.0)
        return sorted(recommendations, key=lambda x: x['score'], reverse=True)
    except GoogleAPICallError as e:
        logger.error("Google API call for ranking failed: %s", e)
        return recommendations
    except Exception as e:
        logger.error("An unexpected error occurred during ranking: %s", e)
        return recommendations

@app.post("/search", response_model=SearchApiResponse)
async def search(request: SearchRequest):
    try:
        query_text = request.query
        if not query_text or not query_text.strip():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Query cannot be empty.")
        llm_response_str = get_recommendations_from_engine(query_text)
        response_json = _sanitize_and_load_llm_response(llm_response_str)
        initial_response = RecommendationResponse.model_validate(response_json)
        if initial_response.recommendations:
            recs_as_dicts = [rec.model_dump() for rec in initial_response.recommendations]
            ranked_recs = rank_recommendations(query_text, recs_as_dicts)
            initial_response.recommendations = [RecommendationModel(**rec) for rec in ranked_recs]
        return JSONResponse(content=initial_response.model_dump())
    except GoogleAPICallError as e:
        logger.error("Discovery Engine Search API call failed: %s", e)
        raise

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
