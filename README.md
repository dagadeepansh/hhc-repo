# Discovery Engine API Service

This is a FastAPI application that provides an API to ingest and search data in Google Cloud Discovery Engine.

## Prerequisites

- Python 3.9+
- Google Cloud SDK
- Docker

## Setup

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/your-repo/hhc-repo.git
    cd hhc-repo
    ```

2.  **Create a virtual environment and install dependencies:**

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Set up environment variables:**

    Copy the sample environment file `.env.sample` to a new file named `.env`:

    ```bash
    cp .env.sample .env
    ```

    Now, open the `.env` file and replace the placeholder values with your actual Google Cloud project details.

## Running Locally

To run the application locally, use the following command:

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

## Deployment to Google Cloud Run

1.  **Build the Docker image:**

    ```bash
    docker build -t gcr.io/your-gcp-project-id/hhc-repo .
    ```

2.  **Push the image to Google Container Registry:**

    ```bash
    docker push gcr.io/your-gcp-project-id/hhc-repo
    ```

3.  **Deploy to Cloud Run:**

    ```bash
    gcloud run deploy hhc-repo --image gcr.io/your-gcp-project-id/hhc-repo --platform managed --region your-gcp-region --allow-unauthenticated
    ```

    **Note:** You will need to set the environment variables in the Cloud Run service as well. You can do this through the Cloud Console or by using the `gcloud` command with the `--set-env-vars` flag.
