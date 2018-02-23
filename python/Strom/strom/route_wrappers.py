""" get/post wrapper functions for hitting server retrieval routes"""
import requests
host = "172.0.0.1"
port = 5000
url = 'http://localhost:5000/api'

def temp():
    # example get request url
    ret = requests.get("http://localhost:5000/api/retrieve/all?template_id=1")

def post_template(template):
    payload = {"template": template}
    r = requests.post(f"{url}/define", json=payload)

    return r.text

# ENGINE
def engine_status():
    r = requests.get(f"{url}/engine_status")
    payload = r.json()
    payload['host'] = host
    payload['port'] = port

    return payload

def stop_engine():
    r = requests.get(f"{url}/stop_engine")

    return r.text