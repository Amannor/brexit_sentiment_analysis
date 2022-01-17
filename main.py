import math
import os
import requests
import pandas as pd
from credentials import bearer_token
from datetime import datetime
import json
import time
import glob
import numpy as np


# global bearer_token

HEADERS = {"Authorization": "Bearer {}".format(bearer_token)}
DATA_FOLDER = "dataverse_files" #permalink for data: https://drive.google.com/drive/folders/1wKKsfRxJMoDZog_FxBOqeq1-yetn5xPr?usp=sharing
SEARCH_URL = "https://api.twitter.com/2/tweets"
OUT_FOLDER = "out"
MAX_IDS_ALLOWED_BY_TWITTER = 100

def connect_to_endpoint(params):
    response = requests.request("GET", SEARCH_URL, headers=HEADERS, params=params)
    while response.status_code != 200:
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {response.status_code}: {response.text}')
        return response.status_code
    return response.json()

def write_to_json_file(data, fname, add_epoch_suffix=True):
    if add_epoch_suffix:
        no_extension_name, extension = os.path.splitext(fname)
        epoch_time = int(time.time())
        fname = f'{no_extension_name}_{epoch_time}{extension}'
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Writing {len(data)} records to {fname}')
    with open(fname, "w") as write_file:
        json.dump(data, write_file, indent=4)

def get_existing_ids():
    existing_ids = set()
    dir = os.path.join(OUT_FOLDER, "tweet_ids_not_found")
    for filename in os.listdir(dir):
        if filename.startswith("tweet_ids_not_found") and filename.endswith(".json"):
            with open(os.path.join(dir, filename), "r") as f:
                cur_list = json.load(f)
                existing_ids.update(set(cur_list))
    dir = os.path.join(OUT_FOLDER, "tweet_ids_not_authorized")
    for filename in os.listdir(dir):
        if filename.startswith("tweet_ids_not_authorized") and filename.endswith(".json"):
            with open(os.path.join(dir, filename), "r") as f:
                cur_list = json.load(f)
                existing_ids.update(set(cur_list))
    dir = os.path.join(OUT_FOLDER, "tweets_ids_to_creation_time")
    for filename in os.listdir(dir):
        if filename.startswith("tweets_ids_to_creation_time_and_text") and filename.endswith(".json"):
            with open(os.path.join(dir, filename), "r") as f:
                cur_dict = json.load(f)
                existing_ids.update(set(cur_dict.keys()))


    return existing_ids

def write_tweet_ids_not_found_if_not_empty(data):
    if len(data) > 0:
        write_to_json_file(data, os.path.join(OUT_FOLDER, "tweet_ids_not_found", "tweet_ids_not_found.json"))

def write_tweet_ids_not_authorized_if_not_empty(data):
    if len(data) > 0:
        write_to_json_file(data, os.path.join(OUT_FOLDER, "tweet_ids_not_authorized", "tweet_ids_not_authorized.json"))

def write_tweets_ids_to_creation_time_if_not_empty(data):
    if len(data) > 0:
        write_to_json_file(data, os.path.join(OUT_FOLDER, "tweets_ids_to_creation_time", "tweets_ids_to_creation_time_and_text.json"))


def main():

    # data_files = ["tweets_stance_sentiment_1outof4.csv", "tweets_stance_sentiment_2outof4.csv", "tweets_stance_sentiment_3outof4.csv", "tweets_stance_sentiment_4outof4.csv"]
    data_files = ["tweets_stance_sentiment_1outof4.csv"]
    data_files = [os.path.join(DATA_FOLDER, fname) for fname in data_files]
    ids_num_per_request = MAX_IDS_ALLOWED_BY_TWITTER
    records_per_file = 10**5
    existing_ids = get_existing_ids()
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Found {len(existing_ids)} existing tweets ids')
    for data_file in data_files:
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Reading {data_file}')
        tweets_ids_to_creation_time_and_text = {}
        tweet_ids_not_found = []
        tweet_ids_not_authorized = []
        reader = pd.read_csv(data_file, sep='~', header=0, chunksize=10**4)
        for df in reader:
            cur_tweets_ids = []
            for tweet_id in df["ID"]: #TODO: Consider iterating over all the columns to create a single data file (not one mapping to date, another one mapping to something else etc.)
                if str(tweet_id) in existing_ids:
                    continue
                if len(cur_tweets_ids) >= ids_num_per_request:

                    query_params = {'ids': ",".join(cur_tweets_ids),
                                    'tweet.fields': 'created_at,author_id',
                                    # 'max_results': number_of_tweets_from_period,
                                    'expansions': 'author_id',
                                    'user.fields': 'created_at'}
                    json_response = connect_to_endpoint(query_params)
                    while json_response == 429:
                        write_tweets_ids_to_creation_time_if_not_empty(tweets_ids_to_creation_time_and_text)
                        tweets_ids_to_creation_time_and_text = {}
                        write_tweet_ids_not_found_if_not_empty(tweet_ids_not_found)
                        tweet_ids_not_found = []
                        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} sleeping for 15 minutes')
                        time.sleep(60 * 15)
                        json_response = connect_to_endpoint(query_params)
                    if type(json_response) == int:
                        continue #This means we got a response code that isn't 429
                    tweets_that_came_back_count = 0
                    if "data" in json_response:
                        for d_item in json_response["data"]:
                            tweets_ids_to_creation_time_and_text[d_item["id"]] = {"created_at": d_item["created_at"], "text": d_item["text"]}
                            tweets_that_came_back_count += 1
                        if len(tweets_ids_to_creation_time_and_text) >= records_per_file:
                            write_tweets_ids_to_creation_time_if_not_empty(tweets_ids_to_creation_time_and_text)
                            tweets_ids_to_creation_time_and_text = {}
                    if "errors" in json_response:
                        for err_item in json_response["errors"]:
                            if "detail" in err_item and "Could not find tweet with ids" in err_item["detail"] and "resource_id" in err_item:
                                tweet_ids_not_found.append(err_item["resource_id"])
                                tweets_that_came_back_count += 1
                            elif "title" in err_item and err_item["title"] in "Authorization Error":
                                tweet_ids_not_authorized.append(err_item["resource_id"])
                                tweets_that_came_back_count += 1
                        if tweets_that_came_back_count<len(cur_tweets_ids):
                            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Requested {len(cur_tweets_ids)} and got back only {tweets_that_came_back_count}')


                        if len(tweet_ids_not_found) >= records_per_file:
                            write_tweet_ids_not_found_if_not_empty(tweet_ids_not_found)
                            tweet_ids_not_found = []
                        if len(tweet_ids_not_authorized) >= records_per_file:
                            write_tweet_ids_not_authorized_if_not_empty(tweet_ids_not_authorized)
                            tweet_ids_not_authorized = []

                    cur_tweets_ids = []
                else:
                    cur_tweets_ids.append(str(tweet_id))

        write_tweets_ids_to_creation_time_if_not_empty(tweets_ids_to_creation_time_and_text)
        write_tweet_ids_not_found_if_not_empty(tweet_ids_not_found)
        write_tweet_ids_not_authorized_if_not_empty(tweet_ids_not_authorized)

    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} FIN')


def retrieve_tweets_from_file():
    cur_existing_tweets_files = glob.glob('t_ids_for_text_req_1_1_*.json')
    tweets_ids_to_request = set()
    for fname in cur_existing_tweets_files:
        with open(fname, "r") as f:
            cur_list = json.load(f)
            tweets_ids_to_request.update(set(cur_list))

    records_per_file = 10**5
    existing_ids = get_existing_ids()
    tweets_ids_to_request = tweets_ids_to_request - existing_ids
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Found {len(tweets_ids_to_request)} ids to request')
    tweets_ids_to_creation_time_and_text = {}
    tweet_ids_not_found = []
    tweet_ids_not_authorized = []
    num_of_requests = math.ceil(len(tweets_ids_to_request) / MAX_IDS_ALLOWED_BY_TWITTER)
    tweets_ids_per_request = np.array_split(list(tweets_ids_to_request), num_of_requests)
    for i, cur_tweets_in_requests in enumerate(tweets_ids_per_request):
        query_params = {'ids': ",".join(cur_tweets_in_requests),
                        'tweet.fields': 'created_at,author_id,text',
                        'expansions': 'author_id',
                        'user.fields': 'created_at'}
        should_send_req = True
        iteration_counter = 0
        while should_send_req:
            iteration_counter += 1
            try:
                json_response = connect_to_endpoint(query_params)
                should_send_req = False
            except requests.exceptions.ConnectionError as connection_err:
                write_tweets_ids_to_creation_time_if_not_empty(tweets_ids_to_creation_time_and_text)
                tweets_ids_to_creation_time_and_text = {}
                write_tweet_ids_not_found_if_not_empty(tweet_ids_not_found)
                tweet_ids_not_found = []
                write_tweet_ids_not_authorized_if_not_empty(tweet_ids_not_authorized)
                tweet_ids_not_authorized = []
                print(
                    f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Got connection error, going to sleep for 5 minutes (batch {i + 1} out of {num_of_requests}, iteration no. {iteration_counter})')
                time.sleep(60 * 5)
            if json_response in [429, 503]:
                should_send_req = True
                write_tweets_ids_to_creation_time_if_not_empty(tweets_ids_to_creation_time_and_text)
                tweets_ids_to_creation_time_and_text = {}
                write_tweet_ids_not_found_if_not_empty(tweet_ids_not_found)
                tweet_ids_not_found = []
                write_tweet_ids_not_authorized_if_not_empty(tweet_ids_not_authorized)
                tweet_ids_not_authorized = []
                print(
                    f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Sleeping for {"1" if json_response == 429 else ""}5 minutes (batch {i + 1} out of {num_of_requests}, iteration no. {iteration_counter})')
                if json_response == 429:
                    time.sleep(60 * 15)
                else:
                    time.sleep(60 * 5)
                print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Woke up')
        if type(json_response) == int:
            continue  # This means we got a response code that isn't 429 nor 200
        if "data" in json_response:
            for d_item in json_response["data"]:
                tweets_ids_to_creation_time_and_text[d_item["id"]] = {"created_at": d_item["created_at"], "text": d_item["text"]}
            if len(tweets_ids_to_creation_time_and_text) >= records_per_file:
                write_tweets_ids_to_creation_time_if_not_empty(tweets_ids_to_creation_time_and_text)
                tweets_ids_to_creation_time_and_text = {}
        if "errors" in json_response:
            for err_item in json_response["errors"]:
                if "detail" in err_item and "Could not find tweet with ids" in err_item[
                    "detail"] and "resource_id" in err_item:
                    tweet_ids_not_found.append(err_item["resource_id"])
                elif "title" in err_item and err_item["title"] in "Authorization Error":
                    tweet_ids_not_authorized.append(err_item["resource_id"])

            if len(tweet_ids_not_found) >= records_per_file:
                write_tweet_ids_not_found_if_not_empty(tweet_ids_not_found)
                tweet_ids_not_found = []
            if len(tweet_ids_not_authorized) >= records_per_file:
                write_tweet_ids_not_authorized_if_not_empty(tweet_ids_not_authorized)
                tweet_ids_not_authorized = []


    write_tweets_ids_to_creation_time_if_not_empty(tweets_ids_to_creation_time_and_text)
    write_tweet_ids_not_found_if_not_empty(tweet_ids_not_found)
    write_tweet_ids_not_authorized_if_not_empty(tweet_ids_not_authorized)
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} FIN')

if __name__ == "__main__":
    # main()
    retrieve_tweets_from_file()