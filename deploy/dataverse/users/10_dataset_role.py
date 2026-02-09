import os
import argparse
import requests
import pandas as pd
from dotenv import load_dotenv
from typing import List

def get_dataverse_id(base_url: str, subtree: str, api_key: str) -> int:
    url = f"{base_url}/api/dataverses/{subtree}"
    headers = {"X-Dataverse-key": api_key}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Failed to get dataverse ID for subtree '{subtree}': {response.status_code} {response.text}")
        return -1

    return response.json().get("data", {}).get("id", -1)

def search_datasets_by_project_id(base_url: str, project_id: str, api_key: str, subtree: str = None) -> List[str]:
    url = f"{base_url}/api/search"
    params = {
        "q": f"projectID:\"{project_id}\"",
        "type": "dataset",
        "per_page": 1000
    }
    if subtree:
        params["subtree"] = subtree

    headers = {"X-Dataverse-key": api_key}
    response = requests.get(url, params=params, headers=headers)

    if response.status_code != 200:
        print(f"❌ Search failed: {response.status_code} {response.text}")
        return []

    data = response.json()
    items = data.get("data", {}).get("items", [])
    persistent_ids = [item["global_id"] for item in items]
    print(f"✅ Found {len(persistent_ids)} datasets with projectID '{project_id}'")
    return persistent_ids

def assign_group_permission(dataset_pid: str, group_alias: str, role: str, base_url: str, api_key: str) -> bool:
    url = f"{base_url}/api/datasets/:persistentId/assignments"
    params = {"persistentId": dataset_pid}
    payload = {
        "assignee": group_alias,
        "role": role
    }
    headers = {
        "Content-type": "application/json",
        "X-Dataverse-key": api_key
    }

    response = requests.post(url, params=params, json=payload, headers=headers)
    if response.status_code in [200, 201]:
        print(f"✅ Assigned role '{role}' to group '{group_alias}' on dataset '{dataset_pid}'")
        return True
    else:
        resp_text = response.text
        if "already has this role" in resp_text:
            print(f"⏭️  '{group_alias}' already has role '{role}' on dataset '{dataset_pid}', skipping.")
            return True
        print(f"❌ Failed to assign role to group '{group_alias}' on dataset '{dataset_pid}': {resp_text}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Assign group permissions to datasets with projectID metadata.")
    parser.add_argument("-e", "--env", required=True, help="Path to .env file with BASE_URL and SU_API_KEY")
    parser.add_argument("--csv", help="CSV file containing usernames as projectID")
    parser.add_argument("--projects", nargs="*", help="List of project IDs (usernames)")
    parser.add_argument("--role", default="fileDownloader", help="Role to assign (e.g., fileDownloader, editor, admin)")
    parser.add_argument("--subtree", default=None, help="Optional dataverse identifier to limit the search scope")
    args = parser.parse_args()

    load_dotenv(args.env)
    base_url = os.getenv("BASE_URL")
    api_key = os.getenv("SU_API_KEY")

    if not base_url or not api_key:
        print("❌ Error: BASE_URL or SU_API_KEY not set in the .env file.")
        return

    if not args.csv and not args.projects:
        print("❌ Error: Either --csv or --projects must be specified.")
        return

    dataverse_id = get_dataverse_id(base_url, args.subtree, api_key) if args.subtree else -1
    if dataverse_id == -1 and args.subtree:
        return

    project_ids = []
    if args.csv:
        df = pd.read_csv(args.csv)
        project_ids = df['username'].tolist()
    elif args.projects:
        project_ids = args.projects

    for project_id in project_ids:
        group_alias = f"&explicit/{dataverse_id}-{project_id}" if dataverse_id != -1 else f"@{project_id}"
        datasets = search_datasets_by_project_id(base_url, project_id, api_key, subtree=args.subtree)

        for pid in datasets:
            assign_group_permission(pid, group_alias, args.role, base_url, api_key)

if __name__ == "__main__":
    main()

