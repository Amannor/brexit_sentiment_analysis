import os
import json
from datetime import datetime
from collections import defaultdict
from collections import Counter
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from enum import Enum

DATA_FOLDER = "dataverse_files" #permalink for data: https://drive.google.com/drive/folders/1wKKsfRxJMoDZog_FxBOqeq1-yetn5xPr?usp=sharing
OUT_DATA_FOLDER = "dataverse_files_incl_date"
OUT_FOLDER = "out"
DATE_FORMAT = "%Y-%M-%d" #i.e: "YYYY-MM-DD"
NOT_FOUND_DATE = "1000-01-01"

class Sentiment(Enum):
    NEUTRAL = 0
    REMAIN = 1
    LEAVE = 2
    OTHER = 99

def write_to_json_file(data, fname, add_epoch_suffix=False):
    if add_epoch_suffix:
        no_extension_name, extension = os.path.splitext(fname)
        epoch_time = int(time.time())
        fname = f'{no_extension_name}_{epoch_time}{extension}'
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Writing {len(data)} records to {fname}')
    with open(fname, "w") as write_file:
        json.dump(data, write_file, indent=4)

def get_trinary_sentiment(cur_stance, cur_sentiment):
    if (cur_stance == "remain" and cur_sentiment == "positive") or (cur_stance == "leave" and cur_sentiment == "negative"):
        return Sentiment.REMAIN
    elif (cur_stance == "leave" and cur_sentiment == "positive") or (cur_stance == "remain" and cur_sentiment == "negative"):
        return Sentiment.LEAVE
    elif cur_stance in ["remain", "leave"]:
        return Sentiment.NEUTRAL
    return Sentiment.OTHER

def main(use_cache = False):
    output_file = "dates_to_stances_and_sentiments.json"
    dates_to_stances_and_sentiments = {}
    if os.path.isfile(output_file) and use_cache:
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Loading data from existing file {output_file}')
        with open(output_file) as f:
            dates_to_stances_and_sentiments = json.load(f)
    else:
        ################# Data batch 1 #################
        retrieved_file_count=0
        tweet_ids_not_found_to_files = {}
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Loading dates mapping')
        dates_to_tweets_ids = defaultdict(set)
        tweets_ids_to_dates = {}
        dir = os.path.join(OUT_FOLDER, "tweets_ids_to_creation_time")
        for filename in os.listdir(dir):
            if filename.startswith("tweets_ids_to_creation_time") and filename.endswith(".json"):
                full_fname = os.path.join(dir, filename)
                # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {full_fname}')
                with open(full_fname, "r") as f:
                    retrieved_file_count+=1
                    cur_dict = json.load(f)
                    for tweet_id in cur_dict.keys():
                        date_str = cur_dict[tweet_id].split("T")[0]
                        try:
                            datetime.strptime(date_str, DATE_FORMAT)
                        except ValueError:
                            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Unexpected date format (id: {tweet_id} date: {date_str}) (expected {DATE_FORMAT})')
                        dates_to_tweets_ids[date_str].add(tweet_id)
                        tweets_ids_to_dates[tweet_id] = date_str

        unretrievable_tweets = set()
        dir = os.path.join(OUT_FOLDER, "tweet_ids_not_found")
        for filename in os.listdir(dir):
            if filename.startswith("tweet_ids_not_found") and filename.endswith(".json"):
                full_fname = os.path.join(dir, filename)
                with open(full_fname, "r") as f:
                    cur_list = json.load(f)
                    for t_id in cur_list:
                        if not t_id in tweets_ids_to_dates:
                            unretrievable_tweets.add(t_id)

        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Found {len(dates_to_tweets_ids)} distinctive dates for {len(tweets_ids_to_dates)} tweets ({retrieved_file_count} files). {len(unretrievable_tweets)} tweets unretrievable')
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Loading sentiments per tweet')

        data_file = os.path.join(DATA_FOLDER, "tweets_stance_sentiment_1outof4.csv")

        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Reading {data_file}')

        with open(data_file) as infile:
            infile.readline() #Or next(f) - first line is the headers
            for line in infile:
                t_id, user_id, t_sentiment, t_stance = line.split('~')
                t_id, user_id, t_sentiment, t_stance = t_id.strip(), user_id.strip(), t_sentiment.strip(), t_stance.strip()
                if not t_id in tweets_ids_to_dates:
                    continue
                t_date = tweets_ids_to_dates[t_id]
                if not t_date in dates_to_stances_and_sentiments:
                    dates_to_stances_and_sentiments[t_date] = {}
                if not t_stance in dates_to_stances_and_sentiments[t_date]:
                    dates_to_stances_and_sentiments[t_date][t_stance] = Counter()
                dates_to_stances_and_sentiments[t_date][t_stance][t_sentiment] += 1

        ################# Data batch 2-4 #################
        for i in range(2,5):
            retrieved_file_count=0
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Loading dates mapping ({i})')
            dates_to_tweets_ids = defaultdict(set)
            tweets_ids_to_dates = {}
            dir = os.path.join(f'{OUT_FOLDER}{i}', "tweets_ids_to_creation_time")
            for filename in os.listdir(dir):
                if filename.startswith("tweets_ids_to_creation_time") and filename.endswith(".json"):
                    full_fname = os.path.join(dir, filename)
                    # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {full_fname}')
                    with open(full_fname, "r") as f:
                        retrieved_file_count +=1
                        cur_dict = json.load(f)
                        for tweet_id in cur_dict.keys():
                            date_str = cur_dict[tweet_id]["created_at"].split("T")[0]
                            try:
                                datetime.strptime(date_str, DATE_FORMAT)
                            except ValueError:
                                print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Unexpected date format (id: {tweet_id} date: {date_str}) (expected {DATE_FORMAT})')
                            dates_to_tweets_ids[date_str].add(tweet_id)
                            tweets_ids_to_dates[tweet_id] = date_str

            unretrievable_tweets = set()
            dir = os.path.join(f'{OUT_FOLDER}{i}', "tweet_ids_not_found")
            for filename in os.listdir(dir):
                if filename.startswith("tweet_ids_not_found") and filename.endswith(".json"):
                    full_fname = os.path.join(dir, filename)
                    with open(full_fname, "r") as f:
                        cur_set = set(json.load(f))
                        unretrievable_tweets.update(cur_set)
                        t_ids_in_intersection = cur_set.intersection(set(tweets_ids_to_dates.keys()))
                        if len(t_ids_in_intersection)>0:
                            print(f'Found {len(t_ids_in_intersection)} ids that appear as found an unfound! (file {full_fname})')



            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Found {len(dates_to_tweets_ids)} distinctive dates for {len(tweets_ids_to_dates)} tweets ({retrieved_file_count} files). {len(unretrievable_tweets)} tweets unretrievable')
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Loading sentiments per tweet')

            data_file = f'tweets_stance_sentiment_{i}outof4.csv'
            data_file = os.path.join(DATA_FOLDER, data_file)
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Reading {data_file}')

            with open(data_file) as infile:
                for line in infile:
                    t_id, user_id, t_sentiment, t_stance = line.split('~')
                    t_id, user_id, t_sentiment, t_stance = t_id.strip(), user_id.strip(), t_sentiment.strip(), t_stance.strip()
                    if not t_id in tweets_ids_to_dates:
                        continue
                    t_date = tweets_ids_to_dates[t_id]
                    if not t_date in dates_to_stances_and_sentiments:
                        dates_to_stances_and_sentiments[t_date] = {}
                    if not t_stance in dates_to_stances_and_sentiments[t_date]:
                        dates_to_stances_and_sentiments[t_date][t_stance] = Counter()
                    dates_to_stances_and_sentiments[t_date][t_stance][t_sentiment] += 1

        write_to_json_file(dates_to_stances_and_sentiments, output_file)

    dates_to_remain_stance_and_sentiments = Counter()
    dates_to_leave_stance_and_sentiments = Counter()
    dates_to_neutral_stance_and_sentiments = Counter()

    for cur_date in dates_to_stances_and_sentiments.keys():
        for cur_stance in dates_to_stances_and_sentiments[cur_date].keys():
            if not cur_stance in ["remain", "leave"]:
                continue
            for cur_sentiment in dates_to_stances_and_sentiments[cur_date][cur_stance].keys():
                if (cur_stance == "remain" and cur_sentiment == "positive") or (cur_stance == "leave" and cur_sentiment == "negative"):
                    dates_to_remain_stance_and_sentiments[datetime.strptime(cur_date, DATE_FORMAT)]+=dates_to_stances_and_sentiments[cur_date][cur_stance][cur_sentiment]
                elif (cur_stance == "leave" and cur_sentiment == "positive") or (cur_stance == "remain" and cur_sentiment == "negative"):
                    dates_to_leave_stance_and_sentiments[datetime.strptime(cur_date, DATE_FORMAT)]+=dates_to_stances_and_sentiments[cur_date][cur_stance][cur_sentiment]
                else: #Which means the sentiment is neither positive nor negative
                    dates_to_neutral_stance_and_sentiments[datetime.strptime(cur_date, DATE_FORMAT)]+=dates_to_stances_and_sentiments[cur_date][cur_stance][cur_sentiment]

    x, y = zip(*sorted(dates_to_remain_stance_and_sentiments.items()))
    plt.plot(x, y, label="Remain")
    x, y = zip(*sorted(dates_to_leave_stance_and_sentiments.items()))
    plt.plot(x, y, label="Leave")
    x, y = zip(*sorted(dates_to_neutral_stance_and_sentiments.items()))
    plt.plot(x, y, label="Neutral")
    plt.legend(loc="upper left")

    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
    plt.gcf().autofmt_xdate()  # Rotation


    plt.show()


    '''
    In [1]: import matplotlib.pyplot as plt

In [2]: time_dict = {datetime.date(2016, 5, 31): 27, datetime.date(2016, 8, 1): 88, datetime.date(2016, 2, 5): 42,  datetime.date(2016, 9, 1): 87}

In [3]: x,y = zip(*sorted(time_dict.items()))

In [4]: plt.plot(x,y)
    '''


def analyze_from_unified():
    dates_to_stances_and_sentiments = {}
    for filename in os.listdir(OUT_DATA_FOLDER):
        if not filename.startswith('tweets_stance_sentiment_incl_date_'):
            continue
        data_file = os.path.join(OUT_DATA_FOLDER, filename)
        with open(data_file) as infile:
            infile.readline()  # Or next(f) - first line is the headers
            for line in infile:
                t_id, user_id, t_sentiment, t_stance, t_date = line.split(',')
                t_id, user_id, t_sentiment, t_stance, t_date = t_id.strip(), user_id.strip(), t_sentiment.strip(), t_stance.strip(), t_date.strip()
                #TODOD - complete




if __name__ == "__main__":
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Start')
    main()
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} End')
