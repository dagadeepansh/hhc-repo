import json
import logging
import re
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError

import schemas
import services

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Specialty Recommendation API",
    description="API for getting specialty id recommendations using Google Discovery Engine.",
    version="1.0.0"
)


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
        logger.error(f"Failed to parse JSON from LLM response even after cleaning: {cleaned_str}")
        raise e

    default_response = {
        "filters": {
            "AcceptingNewPatients": None, "FinancialAssistance": None, "Gender": None,
            "Insurance": None, "Language": None, "Location": None, "OpenScheduling": None,
            "Organization": None, "Practice": None, "Rating": None, "specialty_rollup": [],
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


@app.post("/get-specialty-id",
          response_model=schemas.RecommendationResponse,
          tags=["Recommendations"],
          summary="Get Recommendations as a Validated JSON")
async def get_recommendations(request: schemas.QueryRequest):
    """
    Accepts a user query and returns a structured JSON response with
    physician recommendations generated and ranked by the backend model.
    """
    try:
        query_text = request.query
        if not query_text or not query_text.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Query cannot be empty."
            )

        llm_response_str = services.get_recommendations_from_engine(query_text)
        print("----------------------Raw LLM Response: \n", llm_response_str)

        try:
            response_json = _sanitize_and_load_llm_response(llm_response_str)
            print("----------------------Clean LLM Response: \n", response_json)
            
            # Validate the initial structure
            initial_response = schemas.RecommendationResponse.model_validate(response_json)

            # --- RANKING INTEGRATION ---
            # If there are recommendations, proceed to rank them
            if initial_response.recommendations:
                # Convert Pydantic models to dicts for the service function
                recs_as_dicts = [rec.model_dump() for rec in initial_response.recommendations]
                
                # Call the new ranking service
                ranked_recs = services.rank_recommendations(query_text, recs_as_dicts)
                
                # Update the response object with the newly ranked and sorted recommendations
                # Re-validate to ensure the final data is correct
                initial_response.recommendations = [schemas.RecommendationModel(**rec) for rec in ranked_recs]
            # --- END RANKING INTEGRATION ---

            return JSONResponse(content=initial_response.model_dump())

        except (json.JSONDecodeError, ValidationError) as e:
            if isinstance(e, json.JSONDecodeError):
                logger.error(f"Failed to parse JSON from LLM response: {llm_response_str}")
                detail = "Could not parse the response from the recommendation engine. The format was invalid."
            else:  # ValidationError
                logger.error(f"Pydantic validation failed for LLM response. Error: {e}")
                detail = "The response from the recommendation engine was malformed or missing required fields."

            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=detail
            )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.exception(f"An unexpected error occurred while processing query '{request.query}': {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An internal server error occurred."
        )