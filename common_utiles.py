import csv
import datetime
import json
import math
import os
import time
import warnings
from enum import Enum

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests

BASE_OUT_DIR = "out"
DATA_FOLDER = "dataverse_files"
FINAL_REPORT_FOLDER = "final_report"
FINAL_REPORT_DATA_FOLDER = os.path.join(FINAL_REPORT_FOLDER, "data")
DATE_FORMAT = "%Y-%m-%d" #i.e: "YYYY-MM-DD"
MAX_IDS_ALLOWED_BY_TWITTER = 100
RECORDS_PER_FILE = 10 ** 5

PLOTS_DATA_FOLDER = os.path.join("plots", "data_for_plots")
PLOTS_IMG_FOLDER = os.path.join("plots", "images")
REMAIN_COLOR = "blue"
LEAVE_COLOR = "red"
OTHER_STANCE_COLOR = "green"

SEARCH_URL = "https://api.twitter.com/2/tweets"
BOT_SCORES_DF = None

class Sentiment(Enum):
    NEUTRAL = 0
    REMAIN = 1
    LEAVE = 2
    OTHER = 99

def remove_bots_by_threshold(df, bot_score_threshold):
    df.set_index("user_id", inplace=True)
    BOT_SCORES_DF.set_index("user_id", inplace=True)

    df = df.join(BOT_SCORES_DF)

    df.reset_index(inplace=True)
    BOT_SCORES_DF.reset_index(inplace=True)

    df = df[np.logical_or(df["bot_score"].isin([np.nan]), df["bot_score"] <= bot_score_threshold)]
    df.drop(columns=["bot_score"], inplace=True)
    return df

def handle_bots(bot_score_threshold):
    should_filter_bots = False
    bot_msg_suffix = ""
    global BOT_SCORES_DF
    if not bot_score_threshold is None:
        if not 0 <= bot_score_threshold <= 1:
            warnings.warn(f'Bot score should be probability (between 0 and 1) but got {bot_score_threshold} - ignoring it')
        else:
            bot_msg_suffix = f" (bot score threshold {bot_score_threshold})"
            should_filter_bots = True
            if BOT_SCORES_DF is None:
                full_fname = os.path.join(DATA_FOLDER, "users_stance_sentiment_botscore_tweetcounts.csv")
                print(f'{get_cur_formatted_time()} Reading {full_fname}')
                BOT_SCORES_DF = pd.read_csv(full_fname, sep="~", names=["user_id", "user_sentiment", "user_stance", "bot_score", "bot_fetch_time", "tweets_num"])
                BOT_SCORES_DF.drop(columns=["user_sentiment", "user_stance", "bot_fetch_time", "tweets_num"], inplace=True)
                BOT_SCORES_DF = BOT_SCORES_DF[~BOT_SCORES_DF['bot_score'].isin([np.nan])]

    return should_filter_bots, bot_msg_suffix

def df_to_csv_plus_create_dir(df, outdir, file_name):
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    fullname = os.path.join(outdir, file_name)
    df.to_csv(fullname)


def save_fig(f_name, f_format="png"):
    path = os.path.join(PLOTS_IMG_FOLDER, f'{f_name}.{f_format}')
    print(f'{get_cur_formatted_time()} Saving plot {path}')
    plt.savefig(path)
    plt.clf()

def get_cur_formatted_time(format = "%Y-%m-%d %H:%M:%S"):
    return datetime.datetime.now().strftime(format)

def write_to_json_file_if_not_empty(data, fname, add_epoch_suffix=False):
    if len(data) == 0:
        return
    if add_epoch_suffix:
        no_extension_name, extension = os.path.splitext(fname)
        epoch_time = int(time.time())
        fname = f'{no_extension_name}_{epoch_time}{extension}'
    print(f'{get_cur_formatted_time()} Writing {len(data)} records to {fname}')
    with open(fname, "w") as write_file:
        json.dump(data, write_file, indent=4)

def get_trinary_sentiment(cur_stance, cur_sentiment):
    if cur_stance == "remain":
        return Sentiment.REMAIN
    elif cur_stance == "leave":
        return Sentiment.LEAVE
    return Sentiment.OTHER

def connect_to_endpoint(params, headers):
    response = requests.request("GET", SEARCH_URL, headers=headers, params=params)
    if response.status_code != 200:
        print(f'{get_cur_formatted_time()} {response.status_code}: {response.text}')
        return response.status_code
    return response.json()

def get_existing_ids(out_dir):
    existing_ids = set()
    dir = os.path.join(out_dir, "tweet_ids_not_found")
    for filename in os.listdir(dir):
        if filename.startswith("tweet_ids_not_found") and filename.endswith(".json"):
            with open(os.path.join(dir, filename), "r") as f:
                cur_list = json.load(f)
                existing_ids.update(set(cur_list))
    dir = os.path.join(out_dir, "tweet_ids_not_authorized")
    for filename in os.listdir(dir):
        if filename.startswith("tweet_ids_not_authorized") and filename.endswith(".json"):
            with open(os.path.join(dir, filename), "r") as f:
                cur_list = json.load(f)
                existing_ids.update(set(cur_list))
    dir = os.path.join(out_dir, "tweets_ids_to_creation_time")
    for filename in os.listdir(dir):
        if filename.startswith("tweets_ids_to_creation_time") and filename.endswith(".json"):
            with open(os.path.join(dir, filename), "r") as f:
                cur_dict = json.load(f)
                existing_ids.update(set(cur_dict.keys()))

    return existing_ids

def request_tweets_ids_from_csv(data_fname, bearer_token, out_dir, request_text=True, skip_first_line=False):

    data_file = os.path.join(DATA_FOLDER, data_fname)
    existing_ids = get_existing_ids(out_dir)
    print(f'{get_cur_formatted_time()} Found {len(existing_ids)} existing tweets ids (dir {out_dir})')
    print(f'{get_cur_formatted_time()} Reading {data_file}')
    line_count = 0

    with open(data_file) as infile:
        tweets_ids_to_creation_time = {}
        tweet_ids_not_found = []
        tweet_ids_not_authorized = []
        cur_tweets_ids = []
        if skip_first_line:
            infile.readline()
            line_count += 1
        for line in infile:
            line_count += 1
            if line_count % 250000 == 0:
                print(f'{get_cur_formatted_time()} line no. {line_count}')
            tweet_id = line.split('~')[0]
            if str(tweet_id) in existing_ids:
                continue
            cur_tweets_ids.append(str(tweet_id))
            if len(cur_tweets_ids) >= MAX_IDS_ALLOWED_BY_TWITTER:
                headers = {"Authorization": "Bearer {}".format(bearer_token)}
                num_of_requests = math.ceil(len(cur_tweets_ids) / MAX_IDS_ALLOWED_BY_TWITTER)
                tweets_ids_per_request = np.array_split(list(cur_tweets_ids), num_of_requests)

                if request_text:
                    text_req_suffix = ",text"
                    ids_to_creation_fname = os.path.join(out_dir, "tweets_ids_to_creation_time",
                                                         "tweets_ids_to_creation_time_and_text.json")
                else:
                    text_req_suffix = ""
                    ids_to_creation_fname = os.path.join(out_dir, "tweets_ids_to_creation_time",
                                                         "tweets_ids_to_creation_time.json")
                ids_not_found_fname = os.path.join(out_dir, "tweet_ids_not_found", "tweet_ids_not_found.json")
                ids_not_authorized_fname = os.path.join(out_dir, "tweet_ids_not_authorized",
                                                        "tweet_ids_not_authorized.json")



                for i, cur_tweets_in_requests in enumerate(tweets_ids_per_request):
                    query_params = {'ids': ",".join(cur_tweets_in_requests),
                                    'tweet.fields': f'created_at,author_id{text_req_suffix}',
                                    'expansions': 'author_id',
                                    'user.fields': 'created_at'}
                    should_send_req = True
                    iteration_counter = 0
                    while should_send_req:
                        should_send_req = False
                        iteration_counter += 1
                        try:
                            json_response = connect_to_endpoint(query_params, headers)
                            if json_response in [429, 503]:
                                should_send_req = True
                                write_to_json_file_if_not_empty(tweets_ids_to_creation_time, ids_to_creation_fname,
                                                                True)
                                tweets_ids_to_creation_time = {}
                                write_to_json_file_if_not_empty(tweet_ids_not_found, ids_not_found_fname, True)
                                tweet_ids_not_found = []
                                write_to_json_file_if_not_empty(tweet_ids_not_authorized, ids_not_authorized_fname,
                                                                True)
                                tweet_ids_not_authorized = []
                                print(
                                    f'{get_cur_formatted_time()} Sleeping for {"1" if json_response == 429 else ""}5 minutes (batch {i + 1} out of {num_of_requests}, iteration no.: {iteration_counter})')
                                if json_response == 429:
                                    time.sleep(60 * 15)
                                else:
                                    time.sleep(60 * 5)
                                print(f'{get_cur_formatted_time()} Woke up')
                        except requests.exceptions.ConnectionError:
                            should_send_req = True
                            write_to_json_file_if_not_empty(tweets_ids_to_creation_time, ids_to_creation_fname, True)
                            tweets_ids_to_creation_time = {}
                            write_to_json_file_if_not_empty(tweet_ids_not_found, ids_not_found_fname, True)
                            tweet_ids_not_found = []
                            write_to_json_file_if_not_empty(tweet_ids_not_authorized, ids_not_authorized_fname, True)
                            tweet_ids_not_authorized = []
                            print(
                                f'{get_cur_formatted_time()} Got connection error, going to sleep for 10 minutes (iteration no.: {iteration_counter})')
                            time.sleep(60 * 10)

                    if type(json_response) == int:
                        continue  # This means we got a response code that isn't 429 nor 200
                    if "data" in json_response:
                        for d_item in json_response["data"]:
                            if request_text:
                                tweets_ids_to_creation_time[d_item["id"]] = {"created_at": d_item["created_at"],
                                                                             "text": d_item["text"]}
                            else:
                                tweets_ids_to_creation_time[d_item["id"]] = d_item["created_at"]

                        if len(tweets_ids_to_creation_time) >= RECORDS_PER_FILE:
                            write_to_json_file_if_not_empty(tweets_ids_to_creation_time, ids_to_creation_fname, True)
                            tweets_ids_to_creation_time = {}
                    if "errors" in json_response:
                        for err_item in json_response["errors"]:
                            if "detail" in err_item and "Could not find tweet with ids" in err_item[
                                "detail"] and "resource_id" in err_item:
                                tweet_ids_not_found.append(err_item["resource_id"])
                            elif "title" in err_item and err_item["title"] in "Authorization Error":
                                tweet_ids_not_authorized.append(err_item["resource_id"])
                        if len(tweet_ids_not_found) >= RECORDS_PER_FILE:
                            write_to_json_file_if_not_empty(tweet_ids_not_found, ids_not_found_fname, True)
                            tweet_ids_not_found = []
                        if len(tweet_ids_not_authorized) >= RECORDS_PER_FILE:
                            write_to_json_file_if_not_empty(tweet_ids_not_authorized, ids_not_authorized_fname, True)
                            tweet_ids_not_authorized = []
                cur_tweets_ids = []

def write_csv_file_if_data_not_empty(fname, data, header):
    if len(data) == 0:
        return
    print(f'{get_cur_formatted_time()} Writing {len(data)} records to {fname}')
    with open(fname, 'w', newline='', encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(header)
        for record in data:
            writer.writerow(record)