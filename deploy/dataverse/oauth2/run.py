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


def update_oauth2(data):
    with tempfile.NamedTemporaryFile('w', suffix='.json') as fo:
        content = json.dumps(data, indent=2)
        print(f"content:\n{content}")
        fo.write(content)
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
    update_oauth2(make_github_json())

