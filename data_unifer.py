import csv
import os
import json
from datetime import datetime
from collections import defaultdict
from collections import Counter
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import glob


DATA_FOLDER = "dataverse_files" #permalink for data: https://drive.google.com/drive/folders/1wKKsfRxJMoDZog_FxBOqeq1-yetn5xPr?usp=sharing
OUT_DATA_FOLDER = "dataverse_files_incl_date"
OUT_FOLDER = "out"
DATE_FORMAT = "%Y-%M-%d" #i.e: "YYYY-MM-DD"
NOT_FOUND_DATE = "1000-01-01"

def write_to_json_file(data, fname, add_epoch_suffix=True):
    if add_epoch_suffix:
        no_extension_name, extension = os.path.splitext(fname)
        epoch_time = int(time.time())
        fname = f'{no_extension_name}_{epoch_time}{extension}'
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Writing {len(data)} records to {fname}')
    with open(fname, "w") as write_file:
        json.dump(data, write_file, indent=4)

def main():
    header = ('t_id', 'user_id', 't_sentiment', 't_stance', 't_date')
    max_tweets_per_file = 10**6
    for i in range(2,5):
        tweets_ids_to_time_dict = {}
        # tweets_ids_not_found = set()
        # tweets_ids_not_requested_yet = set()
        all_tweets_info = []
        out_file_count=1
        out_folder = OUT_FOLDER if i==1 else f'{OUT_FOLDER}{i}'

        dir = os.path.join(f'{out_folder}', "tweets_ids_to_creation_time")
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {dir}')
        for filename in os.listdir(dir):
            if not (filename.startswith("tweets_ids_to_creation_time") and filename.endswith(".json")):
                continue
            full_fname = os.path.join(dir, filename)
            with open(full_fname, "r") as f:
                cur_dict = json.load(f)
                for tweet_id in cur_dict.keys():
                    date_str = cur_dict[tweet_id] if i==1 else cur_dict[tweet_id]["created_at"]
                    date_str = date_str.split("T")[0]
                    try:
                        datetime.strptime(date_str, DATE_FORMAT)
                    except ValueError:
                        print(
                            f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Unexpected date format (id: {tweet_id} file {full_fname} date: {date_str}) (expected {DATE_FORMAT})')
                    tweets_ids_to_time_dict[tweet_id] = date_str

        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Retrieved dates for {len(tweets_ids_to_time_dict)} tweets')

        dir = os.path.join(out_folder, "tweet_ids_not_found")
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {dir}')
        tweets_not_found_count = 0
        for filename in os.listdir(dir):
            if not (filename.startswith("tweet_ids_not_found") and filename.endswith(".json")):
                continue
            full_fname = os.path.join(dir, filename)
            with open(full_fname, "r") as f:
                cur_list = json.load(f)
                for cur_t_id in cur_list:
                    if not cur_t_id in tweets_ids_to_time_dict:
                        tweets_ids_to_time_dict[cur_t_id] = NOT_FOUND_DATE
                        tweets_not_found_count+=1

        print(
            f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {tweets_not_found_count} Tweets with unretrievable date')

        removed_files_count=0
        for filename in os.listdir(OUT_DATA_FOLDER):
            if filename.startswith(f'tweets_stance_sentiment_incl_date_{i}_'):
                os.remove(os.path.join(OUT_DATA_FOLDER, filename))
                removed_files_count+=1
        if removed_files_count>0:
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Deleted {removed_files_count} files from {OUT_DATA_FOLDER}')

        data_file = f'tweets_stance_sentiment_{i}outof4.csv'
        data_file = os.path.join(DATA_FOLDER, data_file)
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Reading {data_file}')

        data_tweets_ids = set()
        tweets_ids_not_yet_requested = set()
        with open(data_file) as infile:
            if i == 1:
                infile.readline()  # Or next(f) - first line is the headers
            for line in infile:
                t_id, user_id, t_sentiment, t_stance = line.split('~')
                t_id, user_id, t_sentiment, t_stance = t_id.strip(), user_id.strip(), t_sentiment.strip(), t_stance.strip()
                data_tweets_ids.add(t_id)
                if t_id in tweets_ids_to_time_dict:
                    t_date = tweets_ids_to_time_dict[t_id]
                    all_tweets_info.append((t_id, user_id, t_sentiment, t_stance, t_date))
                else:
                    tweets_ids_not_yet_requested.add(t_id)

                if len(all_tweets_info)>=max_tweets_per_file:
                    with open(os.path.join(OUT_DATA_FOLDER, f'tweets_stance_sentiment_incl_date_{i}_{out_file_count}_outof4.csv'), 'w', newline='') as csv_file:
                        writer = csv.writer(csv_file)
                        writer.writerow(header)
                        for record in all_tweets_info:
                            writer.writerow(record)
                        out_file_count+=1
                    all_tweets_info = []

            if len(data_tweets_ids)<13*10**6:
                print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Expected 13M records, but only have {len(data_tweets_ids)}')


        cur_existing_tweets_files = glob.glob(f'tweets_ids_not_yet_requested_{i}*.json')
        for fname in cur_existing_tweets_files:
            os.remove(fname)
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Deleted {fname}')

        write_to_json_file(list(tweets_ids_not_yet_requested), f'tweets_ids_not_yet_requested_{i}.json')
        if len(all_tweets_info)>0:
            '''In debug - to see how the *fuck* it's writing more than it read (!!!).
            The leftover that should be written here (which size is len(all_tweets_info)) should be equal to:
            (len(tweets_ids_to_time_dict)+tweets_not_found_count)%1,000,000
            For some reason in the last run it came out that the number of records written here was more than 2000 more (!!!!!!!)
            HOW THE FUCK IS THAT POSSIBLE????
            THERE MUST BE A FUNDAMENTAL BUG HERE!!!
            Last run:
            Retrieved dates for 6488308 tweets
            5668082 Tweets with unretrievable date
            so we get:
            6488308+5668082 = 12156390
            So the number of records that should be written ot the last file is 156390 - but the number was more than 158591 (!!!)
            '''
            fname = os.path.join(OUT_DATA_FOLDER, f'tweets_stance_sentiment_incl_date_{i}_{out_file_count}_outof4.csv')
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Writing {len(all_tweets_info)} records to {fname}')
            with open(fname, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(header)
                for record in all_tweets_info:
                    writer.writerow(record)

        tweets_num_in_dict_and_not_in_data = len(set(tweets_ids_to_time_dict.keys()) - data_tweets_ids)
        if tweets_num_in_dict_and_not_in_data>0:
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Found {tweets_num_in_dict_and_not_in_data} in out folder but not in data')






if __name__ == "__main__":
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Start')
    main()
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} End')
