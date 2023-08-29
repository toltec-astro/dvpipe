import os
import tempfile
import json
import subprocess 
import shlex

def make_github_json():
    client_id = os.environ["DV_GITHUB_CLIENT_ID"]
    client_secret = os.environ["DV_GITHUB_CLIENT_SECRET"]
    return {
        "id":"github",
        "factoryAlias":"oauth2",
        "title":"GitHub",
        "subtitle":"",
        "factoryData": f"type: github | userEndpoint: NONE | clientId: {client_id} | clientSecret: {client_secret}",
        "enabled":True
    }


def make_orcid_json():
    client_id = os.environ["DV_ORCID_CLIENT_ID"]
    client_secret = os.environ["DV_ORCID_CLIENT_SECRET"]
    return {
        "id":"orcid",
        "factoryAlias":"oauth2",
        "title":"ORCID",
        "subtitle":"",
        "factoryData": f"type: orcid | userEndpoint: https://pub.orcid.org/v2.1/{{ORCID}}/person | clientId: {client_id} | clientSecret: {client_secret}",
        "enabled":True
    }


def update_oauth2(data):
    with tempfile.NamedTemporaryFile('w', suffix='.json') as fo:
        content = json.dumps(data, indent=2)
        print(f"content:\n{content}")
        fo.write(content)
        fo.flush()
        cmd = [
            "curl", "-X", "POST",
            "-H", "Content-type: application/json",
            "--upload-file", fo.name,
            "http://localhost:8080/api/admin/authenticationProviders"
            ]
        print("run cmd: {}".format(cmd))
        output = subprocess.check_output(cmd)
        print(f"output:\n{output}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", '-p', choices=["github", "orcid"])
    option = parser.parse_args()
    make_json = {
            "github": make_github_json,
            "orcid": make_orcid_json,
            }.get(option.provider, None)
    if make_json is None:
        raise ValueError(f"invalid provider {option.provider}")

    update_oauth2(make_json())

