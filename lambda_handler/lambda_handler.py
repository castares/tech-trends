import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timedelta

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("API_KEY")

results = defaultdict(list)


def buildRequest(tag):
    fromdate = int((datetime.utcnow() - timedelta(days=28)).timestamp())
    todate = int(fromdate + timedelta(days=1).total_seconds())
    site = "https://api.stackexchange.com/2.2/"
    endpoint = "questions"
    url = site+endpoint
    params = {
        "tagged": tag,
        "fromdate": str(fromdate),
        "todate": str(todate),
        "pagesize": 100,
        "page": 1,
        "site": "stackoverflow"
    }
    return fetchResponses(url, params)


def fetchResponses(url, params, results=results):
    response = requests.get(url, params)
    response_json = response.json()
    if response.status_code == 200:
        results[params['tagged']] += response_json['items']
        if response_json['has_more']:
            params['page'] += 1
            return fetchResponses(url, params)
        else:
            return response.status_code
    else:
        logging.info(response.status_code)


def lambda_handler(event, context):
    with ProcessPoolExecutor() as executor:
        executor.map(buildRequest, tags)
