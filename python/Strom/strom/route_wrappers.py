""" get/post wrapper functions for hitting server retrieval routes"""
import requests
import json
host = "172.0.0.1"
port = 5000
url = 'http://localhost:5000/api'

def temp():
    # example get request url
    ret = requests.get("http://localhost:5000/api/retrieve/all?template_id=1")

# TEMPLATE
def post_template(template):
    payload = {"template": json.dumps(template)}
    r = requests.post(f"{url}/define", data=payload)

    return r.status_code, r.text

# DATA
def send_data(dstream):
    # assumes already in dstream format ;D
    r = requests.post(f"{url}/load", data={"data": json.dumps(dstream)})

    return r.status_code, r.text

# ENGINE
def engine_status():
    r = requests.get(f"{url}/engine_status")
    payload = r.json()
    payload['host'] = host
    payload['port'] = port

    return r.status_code, payload

def stop_engine():
    r = requests.get(f"{url}/stop_engine")

    return r.status_code, r.json()