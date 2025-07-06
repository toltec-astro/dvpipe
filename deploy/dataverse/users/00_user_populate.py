import os
import argparse
import pandas as pd
import requests
from typing import TypedDict
from dotenv import load_dotenv

# --- TypedDict for user metadata ---
class UserMetadata(TypedDict):
    firstName: str
    lastName: str
    email: str
    affiliation: str
    position: str

class GroupMetadata(TypedDict):
    description: str
    displayName: str

def read_credentials(folder: str):
    credentials = []
    for file in os.listdir(folder):
        if file.endswith(".txt"):
            with open(os.path.join(folder, file)) as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) == 2:
                        credentials.append({'username': parts[0], 'password': parts[1]})
    return credentials

def delete_user_if_exists(username: str, base_url: str) -> bool:
    url = f"{base_url}/api/admin/authenticatedUsers/{username}"
    response = requests.delete(url)
    
    if response.status_code == 200:
        print(f"🗑️ Deleted existing user: {username}")
        return True
    elif response.status_code == 404:
        return False
    else:
        print(f"⚠️ Failed to delete user {username}: {response.status_code} {response.text}")
        return False

def create_user(username: str, password: str, metadata: UserMetadata, base_url: str, builtin_key: str) -> bool:
    user_json = {
        "userName": username,
        **metadata
    }
    url = f"{base_url}/api/builtin-users?password={password}&key={builtin_key}&sendEmailNotification=false"
    headers = {"Content-type": "application/json"}
    response = requests.post(url, json=user_json, headers=headers)
    
    if response.status_code == 200:
        print(f"✅ Created user: {username}")
        return True
    elif "Username already exists" in response.text:
        print(f"⚠️ User {username} already exists, continuing.")
        return True
    else:
        print(f"❌ Failed to create user {username}: {response.text}")
        return False

def delete_group_if_exists(groupname: str, base_url: str, dataverse_id: int, su_api_key: str) -> bool:
    url = f"{base_url}/api/dataverses/{dataverse_id}/groups/{groupname}"

    headers = {
            "X-Dataverse-key": su_api_key,
            }

    response = requests.delete(url, headers=headers)
    print(response)
    if response.status_code == 200:
        print(f"🗑️ Deleted existing group: {groupname}")
        return True
    else:
        print(f"⚠️ Failed to delete group {groupname}: {response.status_code} {response.text}")
        return False


def create_group(groupname: str, metadata: GroupMetadata, base_url: str, dataverse_id: int, su_api_key: str) -> bool:
    group_json = {
        "aliasInOwner": groupname,
        **metadata
    }
    url = f"{base_url}/api/dataverses/{dataverse_id}/groups"
    headers = {
            "Content-type": "application/json",
            "X-Dataverse-key": su_api_key,
            }
    response = requests.post(url, json=group_json, headers=headers)
    print(response)

    if response.status_code in [200, 201]:
        print(f"✅ Created group: {groupname}")
        return True
    else:
        print(f"❌ Failed to create group {groupname}: {response.text}")
        return False


def add_user_to_group(username: str, groupname: str, base_url: str, dataverse_id: int, su_api_key: str) -> bool:
    url = f"{base_url}/api/dataverses/{dataverse_id}/groups/{groupname}/roleAssignees/@{username}"
    headers = {
            "X-Dataverse-key": su_api_key,
            }
    response = requests.put(url, headers=headers)
    print(response)

    if response.status_code in [200, 201]:
        print(f"✅ Added user to group: {username} -> {groupname}")
        return True
    else:
        print(f"❌ Failed to add user {username} to group {groupname}: {response.text}")
        return False

def get_api_token(username: str, password: str, base_url: str):
    url = f"{base_url}/api/builtin-users/{username}/api-token?password={password}"
    response = requests.get(url)
    if response.status_code == 200:
        token = response.json()['data']['message']
        print(f"✅ user {username} token: {token}")
        return token
    else:
        print(f"❌ Failed to get API token for {username}: {response.text}")
        return None

def get_token_info(token: str, base_url: str) -> dict:
    url = f"{base_url}/api/users/token"
    headers = {
            "X-Dataverse-key": token,
            }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        info = response.json()['data']['message']
        print(f"✅ token info: {info}")
        return info
    else:
        print(f"⚠️ Could not get token info: {response.text}")
        return {}

def main():
    parser = argparse.ArgumentParser(description="Create Dataverse users from credential files.")
    parser.add_argument("-e", "--env", required=True, help="Path to .env file with BASE_URL and BUILTIN_USERS_KEY")
    args = parser.parse_args()

    # Load environment variables
    load_dotenv(args.env)
    base_url = os.getenv("BASE_URL")
    builtin_key = os.getenv("BUILTIN_USERS_KEY")
    dataverse_id = os.getenv("DATAVERSE_ID")
    su_api_key = os.getenv("SU_API_KEY")

    if not base_url or not builtin_key:
        print("❌ Error: BASE_URL or BUILTIN_USERS_KEY not set in the .env file.")
        return

    creds = read_credentials("pw")
    records = []

    for cred in creds:
        username = cred["username"]
        password = cred["password"]

        metadata: UserMetadata = {
            "firstName": username,
            "lastName": username,
            "email": f"dp+{username}@lmtgtm.org",
            "affiliation": "Large Millimeter Telescope",
            "position": ""
        }

        # delete_user_if_exists(username, base_url)

        user_ok = create_user(username, password, metadata, base_url, builtin_key)

        group_metadata: GroupMetadata = {
             "description": "",
             "displayName": f"{username}",
        }

        # delete_group_if_exists(username, base_url, dataverse_id, su_api_key)
        group_ok = create_group(username, group_metadata, base_url, dataverse_id, su_api_key)

        add_user_to_group(username, username, base_url, dataverse_id, su_api_key)

        token = get_api_token(username, password, base_url)
        token_info = get_token_info(token, base_url)
        expiration = token_info.split("on")[-1].strip()
        records.append({
            "username": username,
            "password": password,
            "api_token": token,
            "expiration": expiration
        })

    df = pd.DataFrame(records)
    df.to_csv("user_tokens.csv", index=False)
    print(f"\n✅ Token info saved to: user_tokens.csv")

if __name__ == "__main__":
    main()

