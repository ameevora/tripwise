"""
Genie AI Assistant Integration
Uses Service Principal OAuth (same credentials as Zerobus ingest)
"""

import requests
import json
import time
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

# Use same Service Principal credentials as Zerobus
databricks_host = os.environ.get('DATABRICKS_HOST', 'https://fevm-serverless-stable-mv5gxq.cloud.databricks.com')
client_id = os.environ.get('DATABRICKS_CLIENT_ID')
client_secret = os.environ.get('DATABRICKS_CLIENT_SECRET')


def get_databricks_oauth_token():
    """Get OAuth token using Service Principal credentials (same as Zerobus)"""
    try:
        token_url = f"{databricks_host}/oidc/v1/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "all-apis"
        }
        print(f"Requesting OAuth token from {token_url}")
        response = requests.post(token_url, data=payload)
        response.raise_for_status()

        data = response.json()
        if 'access_token' not in data:
            raise ValueError("No access token in response")

        print("Successfully obtained OAuth token")
        return {'access_token': data["access_token"]}

    except Exception as e:
        print(f"Failed to get OAuth token: {e}")
        raise


def get_genie_space_id():
    """Get the Genie Space ID from environment"""
    return os.environ.get('DATABRICKS_GENIE_SPACE_ID', '01f15d6cfd4a14c29378cffba250e64b')


def new_genie_conversation(space_id, content, token, databricks_host):
    """Start a new conversation with Genie"""
    url = f'{databricks_host}/api/2.0/genie/spaces/{space_id}/start-conversation'

    headers = {
        "Authorization": f"Bearer {token['access_token']}",
        "Content-Type": "application/json"
    }

    payload = {"content": content}
    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        print('Starting Genie conversation...')
        return get_genie_message(
            space_id,
            response.json()['conversation_id'],
            response.json()['message_id'],
            token,
            databricks_host
        )
    else:
        raise Exception(f"API request failed at start_conversation: {response.status_code}, {response.text}")


def get_genie_message(space_id, conversation_id, message_id, token, databricks_host):
    """Retrieve Genie's response to the message we sent"""
    url = f"{databricks_host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"

    headers = {"Authorization": f"Bearer {token['access_token']}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        while True:
            response = requests.get(url, headers=headers).json()
            state = response['status']

            if state == 'COMPLETED':
                print("Genie generation complete.")
                if 'text' in response['attachments'][0]:
                    return {
                        'message_id': message_id,
                        'conversation_id': conversation_id,
                        'attachment_id': response['attachments'][0]['attachment_id'],
                        'content': response['attachments'][0]['text']['content']
                    }
                elif 'query' in response['attachments'][0]:
                    attachment_id = response['attachments'][0]['attachment_id']
                    query_result = get_genie_message_query_result(
                        space_id, conversation_id, message_id, attachment_id, token, databricks_host
                    )
                    return {
                        'message_id': message_id,
                        'conversation_id': conversation_id,
                        'attachment_id': attachment_id,
                        'description': response['attachments'][0]['query']['description'],
                        'query_result': query_result
                    }

            elif state in ["SUBMITTED", 'FILTERING_CONTEXT', 'FETCHING_METADATA', 'ASKING_AI', 'PENDING_WAREHOUSE']:
                print(f"Genie processing: {state}")
                time.sleep(3)

            elif state == 'EXECUTING_QUERY':
                print(f"Executing query: {state}")
                time.sleep(3)

            elif state == 'FAILED':
                raise Exception(f"Genie request failed: {response.get('error', 'Unknown error')}")

            else:
                print(f"Unknown state: {state}")
                return None
    else:
        raise Exception(f"API request failed at get_message: {response.status_code}, {response.text}")


def get_genie_message_query_result(space_id, conversation_id, message_id, attachment_id, token, databricks_host):
    """Retrieve the query result for the SQL query that Genie executed"""
    url = f"{databricks_host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result/{attachment_id}"

    headers = {"Authorization": f"Bearer {token['access_token']}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        while True:
            try:
                response = requests.get(url, headers=headers).json()['statement_response']
            except:
                response = requests.get(url, headers=headers).json()

            state = response['status']['state']

            if state == 'SUCCEEDED':
                data = response.get('result', {})
                rows = data.get('data_array', [])
                meta = response['manifest']
                columns = [column['name'] for column in meta['schema']['columns']]

                if rows == []:
                    df = pd.DataFrame([{"Message": "No data available"}])
                else:
                    df = pd.DataFrame(rows, columns=columns)
                    if df.columns.duplicated().any():
                        df.columns = [
                            f"{col}_{i}" if is_duplicate else col
                            for i, (col, is_duplicate) in enumerate(zip(df.columns, df.columns.duplicated()), 1)
                        ]

                return df.to_json(orient='records')

            elif state in ['RUNNING', 'PENDING']:
                print("Waiting for query result...")
                time.sleep(5)

            else:
                print(f"Query error: {state}")
                return None
    else:
        raise Exception(f"API request failed at get_query_results: {response.status_code}, {response.text}")


def continue_genie_conversation(space_id, content, conversation_id, token, databricks_host):
    """Ask a followup question to an existing conversation"""
    url = f"{databricks_host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages"

    headers = {
        "Authorization": f"Bearer {token['access_token']}",
        "Content-Type": "application/json"
    }

    payload = {"content": content}
    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        return get_genie_message(
            space_id, conversation_id,
            response.json()['message_id'],
            token, databricks_host
        )
    else:
        raise Exception(f"API request failed at continue_conversation: {response.status_code}, {response.text}")
