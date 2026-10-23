"""
Dashboard Embedding with OAuth Token
Uses Service Principal credentials with dashboard-specific scopes
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()


def get_dashboard_embedding_oauth_token(dashboard_id=None):
    """
    Get OAuth token with dashboard embedding scopes.
    This token is required for embedded dashboard access.
    """
    databricks_host = os.environ.get('DATABRICKS_HOST', 'https://fevm-serverless-stable-mv5gxq.cloud.databricks.com')
    databricks_host = databricks_host.rstrip('/')

    databricks_client_id = os.environ.get('DATABRICKS_CLIENT_ID')
    databricks_client_secret = os.environ.get('DATABRICKS_CLIENT_SECRET')

    if not dashboard_id:
        dashboard_id = os.environ.get('DATABRICKS_DASHBOARD_ID', '01f15bdebe6a12d8b153f14e54703b8e')

    # Dashboard embedding specific OAuth scopes
    oauth_scopes = "dashboards.query-execution dashboards.lakeview-embedded:read sql.redash-config:read settings:read"

    # Make M2M OAuth Request to Databricks server
    token_url = f"{databricks_host}/oidc/v1/token"

    payload = {
        "grant_type": "client_credentials",
        "client_id": databricks_client_id,
        "client_secret": databricks_client_secret,
        "scope": oauth_scopes
    }

    response = requests.post(token_url, data=payload)
    response.raise_for_status()

    token_data = response.json()
    access_token = token_data["access_token"]

    return access_token
