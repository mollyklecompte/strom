""" get/post wrapper functions for hitting server retrieval routes"""
import requests


def temp():
    # example get request url
    ret = requests.get("http://localhost:5000/api/retrieve/all?template_id=1")