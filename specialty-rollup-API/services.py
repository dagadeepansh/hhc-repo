import logging
import json
from typing import List, Dict
from google.api_core.client_options import ClientOptions
from google.cloud import discoveryengine_v1 as discoveryengine
from google.api_core.exceptions import GoogleAPICallError
import prompt_lib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---  Project Configuration ---
PROJECT_CONFIG = {
    "project_id": "genai-poc-403304",
    "location": "global",
    "engine_id": "hhc-query-app-specialty-rollup-v1",
    "ranking_config_id": "default_ranking_config",
    "model_version": "gemini-2.5-flash/answer_gen/v1",
    "ranking_model": "semantic-ranker-default@latest"
}
# -----------------------------------------

def get_recommendations_from_engine(query: str) -> str:
    """
    Calls the Google Discovery Engine to get recommendations based on a user query.

    Args:
        query: The user's natural language query.

    Returns:
        A string containing the JSON response from the model.

    Raises:
        GoogleAPICallError: If the API call fails.
    """
    try:
        location = PROJECT_CONFIG["location"]
        client_options = (
            ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
            if location != "global"
            else None
        )

        client = discoveryengine.ConversationalSearchServiceClient(
            client_options=client_options
        )

        serving_config = (f"projects/{PROJECT_CONFIG['project_id']}/locations/{location}/"
                          f"collections/default_collection/engines/{PROJECT_CONFIG['engine_id']}/"
                          f"servingConfigs/default_serving_config")

        search_spec = discoveryengine.AnswerQueryRequest.SearchSpec(
            search_params=discoveryengine.AnswerQueryRequest.SearchSpec.SearchParams(
                max_return_results=20
            )
        )

        answer_generation_spec = discoveryengine.AnswerQueryRequest.AnswerGenerationSpec(
            ignore_adversarial_query=False,
            ignore_non_answer_seeking_query=False,
            ignore_low_relevant_content=False,
            model_spec=discoveryengine.AnswerQueryRequest.AnswerGenerationSpec.ModelSpec(
                model_version=PROJECT_CONFIG["model_version"],
            ),
            prompt_spec=discoveryengine.AnswerQueryRequest.AnswerGenerationSpec.PromptSpec(
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
        
        logger.info(f"Sending query to Discovery Engine: '{query}'")
        response = client.answer_query(request)
        logger.info("Successfully received response from Discovery Engine.")
        
        return response.answer.answer_text

    except GoogleAPICallError as e:
        logger.error(f"Google API call failed: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred in services.py: {e}")
        raise

def rank_recommendations(query: str, recommendations: List[Dict]) -> List[Dict]:
    """
    Ranks a list of recommendations using the Discovery Engine Rank API.

    Args:
        query: The original user query.
        recommendations: A list of recommendation dictionaries from the first LLM call.

    Returns:
        A list of recommendation dictionaries, sorted by the new 'score' field.
    """
    if not recommendations:
        logger.info("No recommendations to rank.")
        return []

    try:
        client = discoveryengine.RankServiceClient()

        ranking_config = client.ranking_config_path(
            project=PROJECT_CONFIG["project_id"],
            location=PROJECT_CONFIG["location"],
            ranking_config=PROJECT_CONFIG["ranking_config_id"],
        )

        # Use the list index 'i' as a guaranteed unique ID for the API call.
        records_to_rank = [
            discoveryengine.RankingRecord(
                id=str(i),
                title=item["specialty"],
                content=item["reason"],
            )
            for i, item in enumerate(recommendations)
        ]

        request = discoveryengine.RankRequest(
            ranking_config=ranking_config,
            model=PROJECT_CONFIG["ranking_model"],
            top_n=len(records_to_rank),
            query=query,
            records=records_to_rank,
        )

        logger.info(f"Sending {len(records_to_rank)} records to Rank API for query: '{query}'")
        response = client.rank(request=request)
        logger.info("Successfully received response from Rank API.")

        # Assign scores back to the original list using the index.
        # The record.id from the response is the string version of the index we sent.
        for record in response.records:
            original_index = int(record.id)
            if original_index < len(recommendations):
                recommendations[original_index]['score'] = record.score

        # Ensure all items have a score key, defaulting to 0.0 if not provided by the API.
        for item in recommendations:
            item.setdefault('score', 0.0)
        
        # Sort the recommendations by score in descending order
        sorted_recommendations = sorted(recommendations, key=lambda x: x['score'], reverse=True)

        return sorted_recommendations

    except GoogleAPICallError as e:
        logger.error(f"Google API call for ranking failed: {e}")
        # If ranking fails, return the original list without scores to avoid breaking the response
        return recommendations
    except Exception as e:
        logger.error(f"An unexpected error occurred during ranking: {e}")
        return recommendations
