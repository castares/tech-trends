import json
import logging
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timedelta

import requests
import s3fs
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("API_KEY")
bucket = os.getenv("STACKOVERFLOW_RAW_BUCKET")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


tags = [
    "javascript",
    "python",
    "java",
    "php",
    "c#",
    "c++",
    "typescript",
    "c",
    "ruby",
    "swift",
    "kotlin",
    "rust",
    "scala",
]
results = defaultdict(list)
s3 = s3fs.S3FileSystem(anon=False)


def buildRequest(tag):
    fromdate = int((datetime.utcnow() - timedelta(days=28)).timestamp())
    todate = int(fromdate + timedelta(days=1).total_seconds())
    site = "https://api.stackexchange.com/2.2/"
    endpoint = "questions"
    url = site + endpoint
    params = {
        "tagged": tag,
        "fromdate": str(fromdate),
        "todate": str(todate),
        "pagesize": 100,
        "page": 1,
        "site": "stackoverflow",
    }

    def fetchResponses(url, params, results=results):
        response = requests.get(url, params)
        response_json = response.json()
        if response.status_code == 200:
            results[params["tagged"]] += response_json["items"]
            if response_json["has_more"]:
                params["page"] += 1
                return fetchResponses(url, params)
            else:
                return logger.info(response.status_code)
        else:
            logger.info(response.status_code)

    return fetchResponses(url, params)


def lambda_handler(event, context):
    with ProcessPoolExecutor() as executor:
        futures = executor.map(buildRequest, tags)
        for future in futures:
            try:
                future
            except Exception as e:
                logger.error(e)
    with s3.open(
        f"{bucket}/{datetime.utcnow().date()}-stackoverflow-questions-by-language.json",
        "w",
    ) as f:
        results_json = json.dumps(results)
        f.write(results_json)
