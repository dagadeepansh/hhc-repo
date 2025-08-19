"""
Client for interacting with the Discovery Engine API Service on Cloud Run.

This script sends a search query to the /search endpoint of the service,
handling authentication via a Google Cloud service account.

Prerequisites:
1.  A Google Cloud service account with the 'Cloud Run Invoker' role.
2.  The service account key file (JSON) downloaded to your local machine.
3.  The `requests` and `google-auth` libraries installed:
    pip install requests google-auth

Environment Variables:
-   `GOOGLE_APPLICATION_CREDENTIALS`: Set this to the absolute path of your
    service account key JSON file.
-   `CLOUD_RUN_ENDPOINT`: The HTTPS URL of your deployed Cloud Run service.
    (e.g., https://your-service-name-random-hash-uc.a.run.app)

Usage:
    python client.py "Your search query here"
"""
import os
import sys
import json
import requests
from google.oauth2 import id_token
from google.auth.transport import requests as grequests


def get_id_token(audience: str) -> str:
    """
    Generates a Google-signed OIDC ID token for authenticating with Cloud Run.

    Args:
        audience: The URL of the Cloud Run service to be invoked.

    Returns:
        A Google-signed OIDC ID token.

    Raises:
        Exception: If credentials are not found or token generation fails.
    """
    try:
        # The GOOGLE_APPLICATION_CREDENTIALS env var should be set to the
        # path of the service account key file.
        creds, _ = id_token.fetch_id_token(grequests.Request(), audience)
        return creds
    except Exception as e:
        print(
            "Error: Could not obtain Google Cloud credentials. "
            "Is GOOGLE_APPLICATION_CREDENTIALS environment "
            "variable set correctly?",
            file=sys.stderr
        )
        raise e


def search_api(endpoint: str, token: str, query: str) -> dict:
    """
    Calls the /search endpoint of the Discovery Engine API service.

    Args:
        endpoint: The base URL of the Cloud Run service.
        token: The OIDC ID token for authentication.
        query: The search query string.

    Returns:
        The JSON response from the API as a dictionary.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    search_url = f"{endpoint}/search"
    payload = {"query": query}

    print(f"Sending request to: {search_url}")
    print(f"Query: {query}")

    try:
        response = requests.post(
            search_url,
            headers=headers,
            json=payload,
            timeout=300
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error making request to API: {e}", file=sys.stderr)
        if e.response is not None:
            print(
                f"Response status: {e.response.status_code}",
                file=sys.stderr
            )
            print(
                f"Response body: {e.response.text}",
                file=sys.stderr
            )
        sys.exit(1)


def main():
    """
    Main function to run the client.
    """
    # 1. Get Cloud Run endpoint from environment variable
    cloud_run_endpoint = os.getenv("CLOUD_RUN_ENDPOINT")
    if not cloud_run_endpoint:
        print(
            "Error: CLOUD_RUN_ENDPOINT environment variable is not set.",
            file=sys.stderr
        )
        sys.exit(1)

    # 2. Get search query from command-line arguments
    if len(sys.argv) < 2:
        print(
            f"Usage: python {sys.argv[0]} \"Your search query\"", 
            file=sys.stderr
            )
        sys.exit(1)
    query_text = sys.argv[1]

    # 3. Authenticate and get ID token
    try:
        id_token_str = get_id_token(audience=cloud_run_endpoint)
    except Exception:  # @pylint-ignore=broad-except
        # Error is printed within get_id_token
        sys.exit(1)

    # 4. Call the search API
    result = search_api(cloud_run_endpoint, id_token_str, query_text)

    # 5. Print the result
    print("\n--- API Response ---")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
