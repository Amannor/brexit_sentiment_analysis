import csv
import glob
from collections import Counter, defaultdict
import pandas as pd

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from common_utiles import *

DELTA_TIME_IN_DAYS = 14
DEF_CSV_HEADER = ('t_id', 'user_id', 't_sentiment', 't_stance', 't_date') #ID~user_id~t_sentiment~t_stance
CSV_HEADER_INCL_TXT = ('t_id', 'user_id', 't_sentiment', 't_stance', 't_date', 't_text')

PLOTS_DATA_FOLDER = os.path.join("plots", "data_for_plots")
PLOTS_IMG_FOLDER = os.path.join("plots", "images")

def save_fig(f_name):
    path = os.path.join(PLOTS_IMG_FOLDER, f_name)
    print(f'{get_cur_formatted_time()} Saving plot {path}')
    plt.savefig(path)

def get_existing_tweets_per_category(only_files_with_text):
    # tweets_ids_to_creation_time = {}
    tweets_ids_and_creation_time = []
    tweets_ids_not_found = set()
    tweets_ids_not_authorized = set()
    tweets_ids_so_far = set()
    for i in range(1,5):
        out_folder = BASE_OUT_DIR+("" if i == 1 else f'{i}')
        cur_dir = os.path.join(out_folder, "tweets_ids_to_creation_time")
        print(f'{get_cur_formatted_time()} {cur_dir}')
        for filename in os.listdir(cur_dir):
            if filename.startswith("tweets_ids_to_creation_time") and filename.endswith(".json"):
                if filename.startswith("tweets_ids_to_creation_time_and_text") or not only_files_with_text:
                    with open(os.path.join(cur_dir, filename), "r") as f:
                        cur_dict = json.load(f)
                        for key in cur_dict:
                            i_key = int(key)
                            tweets_ids_so_far.add(i_key)
                            if isinstance(cur_dict[key], dict) and only_files_with_text:
                                vals_tuple = tuple(cur_dict[key].values())
                                tweets_ids_and_creation_time.append((i_key, vals_tuple[0], vals_tuple[1]))
                            else:
                                tweets_ids_and_creation_time.append((i_key)+cur_dict.values())

    print(f'{get_cur_formatted_time()} Retrieved dates{" and texts" if only_files_with_text else ""} for {len(tweets_ids_and_creation_time)} tweets')

    for i in range(1, 5):
        out_folder = BASE_OUT_DIR + ("" if i == 1 else f'{i}')
        cur_dir = os.path.join(out_folder, "tweet_ids_not_found")
        print(f'{get_cur_formatted_time()} {cur_dir}')
        for filename in os.listdir(cur_dir):
            if filename.startswith("tweet_ids_not_found") and filename.endswith(".json"):
                with open(os.path.join(cur_dir, filename), "r") as f:
                    cur_list = set(json.load(f))
                    for t_id in cur_list:
                        i_t_id = int(t_id)
                        if not i_t_id in tweets_ids_so_far:
                            tweets_ids_not_found.add(i_t_id)

    print(f'{get_cur_formatted_time()} Unable to find {len(tweets_ids_not_found)} tweets')

    for i in range(1, 5):
        out_folder = BASE_OUT_DIR + ("" if i == 1 else f'{i}')
        cur_dir = os.path.join(out_folder, "tweet_ids_not_authorized")
        print(f'{get_cur_formatted_time()} {cur_dir}')
        for filename in os.listdir(cur_dir):
            if filename.startswith("tweet_ids_not_authorized") and filename.endswith(".json"):
                with open(os.path.join(cur_dir, filename), "r") as f:
                    cur_list = json.load(f)
                    for t_id in cur_list:
                        i_t_id = int(t_id)
                        if not (i_t_id in tweets_ids_so_far or i_t_id in tweets_ids_not_found):
                            tweets_ids_not_authorized.add(i_t_id)

    print(f'{get_cur_formatted_time()} Unauthorized to access {len(tweets_ids_not_authorized)} tweets')

    return tweets_ids_and_creation_time, tweets_ids_not_found, tweets_ids_not_authorized

def final_report_data_generator(only_files_with_text = True):
    max_tweets_per_file = 5*10**6
    tweets_ids_and_creation_time, tweets_ids_not_found, tweets_ids_not_authorized = get_existing_tweets_per_category(only_files_with_text)
    cols = ['t_id', 't_date', 't_text'] if only_files_with_text else ['t_id', 't_date']
    tweets_ids_and_creation_time = pd.DataFrame.from_records(tweets_ids_and_creation_time, columns=cols)


    for i in range(1, 5):
        removed_files_count = 0
        for filename in os.listdir(FINAL_REPORT_DATA_FOLDER):
            if (only_files_with_text and filename.startswith(f'tweets_stance_sentiment_incl_date_and_text_{i}_')) or (filename.startswith(f'tweets_stance_sentiment_incl_date_{i}_') and not only_files_with_text):
                os.remove(os.path.join(FINAL_REPORT_DATA_FOLDER, filename))
                removed_files_count += 1
        if removed_files_count > 0:
            print(f'{get_cur_formatted_time()} Deleted {removed_files_count} files from {FINAL_REPORT_DATA_FOLDER}')

        out_file_count = 1
        data_file = os.path.join(DATA_FOLDER, f'tweets_stance_sentiment_{i}outof4.csv')
        print(f'{get_cur_formatted_time()} Reading {data_file}')
        data_tweets_ids = set()
        duplicate_tweets_ids = Counter()
        tweets_ids_not_requested_yet = set()
        all_tweets_info = []
        tweets_found_count, tweets_not_found_count, tweets_not_authorized_count = 0, 0, 0

        with pd.read_csv(data_file, chunksize=max_tweets_per_file, sep="~") as reader:
            for chunk in reader:
                out_fname = os.path.join(FINAL_REPORT_DATA_FOLDER, f'tweets_stance_sentiment_incl_date_and_text_{i}_{out_file_count}_outof4.csv')
                if i == 1:
                    chunk.rename(columns={'ID': 't_id'}, inplace=True)
                        # .merge(tweets_ids_and_creation_time, how='inner')
                else:
                    chunk.columns = list(DEF_CSV_HEADER)[:4]

                result = chunk.merge(tweets_ids_and_creation_time, how='inner')
                print(f'{get_cur_formatted_time()} Writing {len(result.index)} records to {out_fname}')
                result.to_csv(path_or_buf=out_fname, index=False)
                out_file_count += 1

                unfound_tweets_ids = set(chunk['t_id'].tolist())
                tweets_ids_and_creation_time_set = set(tweets_ids_and_creation_time['t_id'].tolist())
                tweets_ids_not_found -= tweets_ids_and_creation_time_set
                tweets_ids_not_authorized -= tweets_ids_and_creation_time_set
                unfound_tweets_ids = unfound_tweets_ids.difference(tweets_ids_and_creation_time_set, tweets_ids_not_found, tweets_ids_not_authorized)
                tweets_ids_not_requested_yet.update(unfound_tweets_ids)

        cur_existing_tweets_files = glob.glob(os.path.join(FINAL_REPORT_DATA_FOLDER, f'tweets_ids_not_yet_requested_{i}*.json'))
        for fname in cur_existing_tweets_files:
            os.remove(fname)
            print(f'{get_cur_formatted_time()} Deleted {fname}')

        write_to_json_file_if_not_empty(list(tweets_ids_not_requested_yet), os.path.join(FINAL_REPORT_DATA_FOLDER, f'tweets_ids_not_yet_requested_{i}.json'))

    write_to_json_file_if_not_empty(list(tweets_ids_not_found), os.path.join(FINAL_REPORT_DATA_FOLDER, f'tweets_ids_not_found.json'))
    write_to_json_file_if_not_empty(list(tweets_ids_not_authorized), os.path.join(FINAL_REPORT_DATA_FOLDER, f'tweets_ids_not_authorized.json'))

def plot_stances_from_counters(remain_dates_to_stances_count, leave_dates_to_stances_count, neutral_dates_to_stances_count, name):
    x, y = zip(*sorted(remain_dates_to_stances_count.items()))
    plt.scatter(x, y, label="Remain")
    x, y = zip(*sorted(leave_dates_to_stances_count.items()))
    plt.scatter(x, y, label="Leave")
    x, y = zip(*sorted(neutral_dates_to_stances_count.items()))
    plt.scatter(x, y, label="Neutral")
    plt.legend(loc="upper left")

    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter(DATE_FORMAT))
    plt.gcf().autofmt_xdate()  # Rotation
    ax.set_title(f'Brexit tweets analysis - {name}')
    save_fig(name)
    plt.clf()

def add_folder_prefix(fname, folder = PLOTS_DATA_FOLDER):
    return os.path.join(folder, fname)

def get_min_and_max_dates_and_write_to_file(folder = FINAL_REPORT_DATA_FOLDER, read_from_existing_file = True, existing_fname = add_folder_prefix("date_limits.json")):
    if read_from_existing_file and os.path.isfile(existing_fname):
        with open(existing_fname) as json_file:
            dates_limits = json.load(json_file)
            earliest_date = datetime.datetime.strptime(dates_limits["earliest_date"], DATE_FORMAT).replace(tzinfo=None)
            latest_date = datetime.datetime.strptime(dates_limits["latest_date"], DATE_FORMAT).replace(tzinfo=None)
    else:
        earliest_date, latest_date = datetime.datetime.strptime("3000-01-01", DATE_FORMAT).replace(tzinfo=None),  datetime.datetime.strptime("1000-01-01", DATE_FORMAT).replace(tzinfo=None)
        for fname in os.listdir(folder):
            if not (fname.startswith(f'tweets_stance_sentiment_incl_date_and_text') and fname.endswith(".csv")):
                continue
            full_fname = os.path.join(FINAL_REPORT_DATA_FOLDER, fname)
            print(f'{get_cur_formatted_time()} Reading {full_fname} (for date limits calculation)')
            data = pd.read_csv(full_fname)
            datetime_series = pd.to_datetime(data["t_date"], format=DATE_FORMAT)
            earliest_date = min(earliest_date, min(datetime_series).replace(tzinfo=None))
            latest_date = max(latest_date, max(datetime_series).replace(tzinfo=None))

        earliest_date, latest_date = earliest_date.replace(microsecond=0, second=0, minute=0, tzinfo=None), latest_date.replace(microsecond=0, second=0, minute=0, tzinfo=None)
        earliest_date_str, latest_date_str = earliest_date.strftime(DATE_FORMAT), latest_date.strftime(DATE_FORMAT)
        write_to_json_file_if_not_empty({"earliest_date": earliest_date_str, "latest_date": latest_date_str}, add_folder_prefix("date_limits.json"))

    return earliest_date, latest_date


def get_sentiment_counters(pre_calculated_number_of_tweets_per_user = None, limit_user_per_day = False):
    dates_to_remain_stance_and_sentiments = Counter()
    dates_to_leave_stance_and_sentiments = Counter()
    dates_to_neutral_stance_and_sentiments = Counter()

    dates_to_users_tweeted = defaultdict(set)
    number_of_tweets_per_user = Counter()
    earliest_date, latest_date = get_min_and_max_dates_and_write_to_file()

    aggregated_df = None
    for fname in os.listdir(FINAL_REPORT_DATA_FOLDER):
        if not (fname.startswith(f'tweets_stance_sentiment_incl_date_and_text') and fname.endswith(".csv")):
            continue
        full_fname = os.path.join(FINAL_REPORT_DATA_FOLDER, fname)
        print(f'{get_cur_formatted_time()} Parsing {full_fname}')
        df = pd.read_csv(full_fname)
        df['t_date'] = pd.to_datetime(df['t_date'])
        df['t_date'] = df['t_date'].apply(lambda d: d.replace(tzinfo=None))
        df["date_bucket_id"] = (df["t_date"] - earliest_date).apply(lambda t: math.floor(t.days/DELTA_TIME_IN_DAYS))
        df = df.groupby(["date_bucket_id", "t_stance"], as_index=False).size()
        # df = df.groupby(["date_bucket_id", "t_stance"], as_index=False).size().to_frame('size')
        # df = df.groupby(["date_bucket_id", "t_stance"]).size().to_frame('size')
        df.set_index(['date_bucket_id', 't_stance'], inplace=True)
        if aggregated_df is None:
            aggregated_df = df
        else:
            aggregated_df["size"] = aggregated_df["size"].add(df["size"], fill_value=0)



    ax = aggregated_df.plot()
    aggregated_df[aggregated_df["t_stance"] == "other"].plot(x="date_bucket_id", y="size", ax=ax, color='green', label="other")
    aggregated_df[aggregated_df["t_stance"] == "remain"].plot(x="date_bucket_id", y="size", ax=ax, color='blue', label="remain")
    aggregated_df[aggregated_df["t_stance"] == "leave"].plot(x="date_bucket_id", y="size", ax=ax, color='red', label="leave")
    ax.legend()
    plt.show()


    '''
    TODO (after talk with Avrahami:
    0) apply on pandas - then can apply a custom function.
    1) For each row calculate the number of days sine earliest_date - call this field seniority
    2) Divide each seniority by the number of days you want - to get the bucket number
    3) Groupby the bucket number (df.groupby("date_bucket_id").aggregate("stance", np.sum))
    
    
    *I need to od a preprocessing in cae of all the "special" cases (e.g. eliminate according to bot_score threshold, not allowing any user to tweet more than once a day etc.)
    E.g: df[df['botscore']>0.7].copy()

    '''



    ''' Old code (without pandas) - START
    with open(full_fname, encoding="utf8") as infile:
        infile.readline()  # Or next(f) - first line is the headers
        for line in infile:
            # t_id, user_id, t_sentiment, t_stance, t_date, t_text = line.split(',')[0], line.split(',')[1], line.split(',')[2], line.split(',')[3], line.split(',')[4], ("\n").join(line.split(',')[5:])
            t_id, user_id, t_sentiment, t_stance, t_date= line.split(',')[0], line.split(',')[1], line.split(',')[2], line.split(',')[3], line.split(',')[4] #For now not reading t_text
            t_id, user_id, t_sentiment, t_stance, t_date = t_id.strip(), user_id.strip(), t_sentiment.strip(), t_stance.strip(), t_date.strip()
            t_date = t_date.split("T")[0]
            try:
                datetime.datetime.strptime(t_date, DATE_FORMAT)
            except ValueError:
                print(
                    f'{get_cur_formatted_time()} Unexpected date format (id: {t_id} date: {t_date} file {full_fname}) (expected {DATE_FORMAT})')

            earliest_date = min(t_date, earliest_date)
            latest_date = max(t_date, latest_date)

            trinary_sentiment = get_trinary_sentiment(t_stance, t_sentiment)
            tweet_val: int = 1
            if pre_calculated_number_of_tweets_per_user is None:
                if limit_user_per_day:
                    if user_id in dates_to_users_tweeted[t_date]:
                        tweet_val = 0
                    else:
                        dates_to_users_tweeted[t_date].add(user_id)

                else:
                    number_of_tweets_per_user[user_id] += 1
            else:
                tweet_val = 1/pre_calculated_number_of_tweets_per_user[user_id]

            if trinary_sentiment == Sentiment.REMAIN:
                dates_to_remain_stance_and_sentiments[t_date] += tweet_val
            elif trinary_sentiment == Sentiment.LEAVE:
                dates_to_leave_stance_and_sentiments[t_date] += tweet_val
            elif trinary_sentiment == Sentiment.NEUTRAL:
                dates_to_neutral_stance_and_sentiments[t_date] += tweet_val
        Old code (without pandas) - END
        '''
    if pre_calculated_number_of_tweets_per_user is None:
        return dates_to_remain_stance_and_sentiments, dates_to_leave_stance_and_sentiments, dates_to_neutral_stance_and_sentiments, number_of_tweets_per_user, earliest_date, latest_date
    else:
        return dates_to_remain_stance_and_sentiments, dates_to_leave_stance_and_sentiments, dates_to_neutral_stance_and_sentiments, pre_calculated_number_of_tweets_per_user, earliest_date, latest_date


def plot_qualitative_counters(dates_to_remain_stance_and_sentiments, dates_to_leave_stance_and_sentiments, dates_to_neutral_stance_and_sentiments, earliest_date, latest_date, name_suffix=""):
    dates_buckets_to_remain_stance_and_sentiments = Counter()
    dates_buckets_to_leave_stance_and_sentiments = Counter()
    dates_buckets_to_neutral_stance_and_sentiments = Counter()
    start_date, end_date = datetime.datetime.strptime(earliest_date, DATE_FORMAT), datetime.datetime.strptime(
        latest_date, DATE_FORMAT)
    cur_date_iterator = start_date
    while cur_date_iterator <= end_date:
        for i in range(0, DELTA_TIME_IN_DAYS):
            date_str = (cur_date_iterator+datetime.timedelta(days=i)).strftime(DATE_FORMAT)
            dates_buckets_to_remain_stance_and_sentiments[cur_date_iterator] += dates_to_remain_stance_and_sentiments[date_str]
            dates_buckets_to_leave_stance_and_sentiments[cur_date_iterator] += dates_to_leave_stance_and_sentiments[date_str]
            dates_buckets_to_neutral_stance_and_sentiments[cur_date_iterator] += dates_to_neutral_stance_and_sentiments[date_str]
        cur_date_iterator += datetime.timedelta(days=DELTA_TIME_IN_DAYS)

    plot_stances_from_counters(dates_buckets_to_remain_stance_and_sentiments, dates_buckets_to_leave_stance_and_sentiments,
                               dates_buckets_to_neutral_stance_and_sentiments, f'quantitative{name_suffix}')

def plot_percentage_counters(dates_to_remain_stance_and_sentiments, dates_to_leave_stance_and_sentiments, dates_to_neutral_stance_and_sentiments, earliest_date, latest_date, name_suffix=""):
    dates_buckets_to_remain_stance_and_sentiments = Counter()
    dates_buckets_to_leave_stance_and_sentiments = Counter()
    dates_buckets_to_neutral_stance_and_sentiments = Counter()
    start_date, end_date = datetime.datetime.strptime(earliest_date, DATE_FORMAT), datetime.datetime.strptime(
        latest_date, DATE_FORMAT)
    cur_date_iterator = start_date
    while cur_date_iterator <= end_date:
        for i in range(0, DELTA_TIME_IN_DAYS):
            date_str = (cur_date_iterator+datetime.timedelta(days=i)).strftime(DATE_FORMAT)
            dates_buckets_to_remain_stance_and_sentiments[cur_date_iterator] += dates_to_remain_stance_and_sentiments[date_str]
            dates_buckets_to_leave_stance_and_sentiments[cur_date_iterator] += dates_to_leave_stance_and_sentiments[date_str]
            dates_buckets_to_neutral_stance_and_sentiments[cur_date_iterator] += dates_to_neutral_stance_and_sentiments[date_str]
        tot_tweets_count = dates_buckets_to_remain_stance_and_sentiments[cur_date_iterator] + dates_buckets_to_leave_stance_and_sentiments[cur_date_iterator]+dates_buckets_to_neutral_stance_and_sentiments[cur_date_iterator]
        dates_buckets_to_remain_stance_and_sentiments[cur_date_iterator] = round(dates_buckets_to_remain_stance_and_sentiments[cur_date_iterator]/tot_tweets_count,2)
        dates_buckets_to_leave_stance_and_sentiments[cur_date_iterator] = round(dates_buckets_to_leave_stance_and_sentiments[cur_date_iterator]/tot_tweets_count, 2)
        dates_buckets_to_neutral_stance_and_sentiments[cur_date_iterator] = round(dates_buckets_to_neutral_stance_and_sentiments[cur_date_iterator]/tot_tweets_count, 2)

        cur_date_iterator += datetime.timedelta(days=DELTA_TIME_IN_DAYS)

    plot_stances_from_counters(dates_buckets_to_remain_stance_and_sentiments, dates_buckets_to_leave_stance_and_sentiments,
                               dates_buckets_to_neutral_stance_and_sentiments, f'percentage{name_suffix}')




def final_report_plot_generator():

    dates_to_remain_stance_and_sentiments = Counter()
    dates_to_leave_stance_and_sentiments = Counter()
    dates_to_neutral_stance_and_sentiments = Counter()
    dates_to_remain_stance_and_sentiments_normalized = Counter()
    dates_to_leave_stance_and_sentiments_normalized = Counter()
    dates_to_neutral_stance_and_sentiments_normalized = Counter()

    dates_to_remain_stance_and_sentiments_single_tweet_per_user = Counter()
    dates_to_leave_stance_and_sentiments_single_tweet_per_user = Counter()
    dates_to_neutral_stance_and_sentiments_single_tweet_per_user = Counter()

    number_of_tweets_per_user = Counter()
    earliest_date, latest_date = "3000-01-01", "1000-01-01"
    dates_to_remain_stance_and_sentiments_file = add_folder_prefix("dates_to_remain_stance_and_sentiments.json")
    dates_to_leave_stance_and_sentiments_file = add_folder_prefix("dates_to_leave_stance_and_sentiments.json")
    dates_to_neutral_stance_and_sentiments_file = add_folder_prefix("dates_to_neutral_stance_and_sentiments.json")
    dates_to_remain_stance_and_sentiments_normalized_file = add_folder_prefix("dates_to_remain_stance_and_sentiments_normalized.json")
    dates_to_leave_stance_and_sentiments_normalized_file = add_folder_prefix("dates_to_leave_stance_and_sentiments_normalized.json")
    dates_to_neutral_stance_and_sentiments_normalized_file = add_folder_prefix("dates_to_neutral_stance_and_sentiments_normalized.json")
    dates_to_remain_stance_and_sentiments_single_tweet_per_user_file = add_folder_prefix("dates_to_remain_stance_and_sentiments_single_tweet_per_user.json")
    dates_to_leave_stance_and_sentiments_single_tweet_per_user_file = add_folder_prefix("dates_to_leave_stance_and_sentiments_single_tweet_per_user.json")
    dates_to_neutral_stance_and_sentiments_single_tweet_per_user_file = add_folder_prefix("dates_to_neutral_stance_and_sentiments_single_tweet_per_user.json")
    # date_limits_file = add_folder_prefix("date_limits.json")
    number_of_tweets_per_user_file = add_folder_prefix("number_of_tweets_per_user.json")

    if np.all([os.path.isfile(os.path.join(PLOTS_DATA_FOLDER, f)) for f in [dates_to_remain_stance_and_sentiments_file,
                                           dates_to_leave_stance_and_sentiments_file,
                                           dates_to_neutral_stance_and_sentiments_file,
                                           dates_to_remain_stance_and_sentiments_normalized_file,
                                           dates_to_leave_stance_and_sentiments_normalized_file,
                                           dates_to_neutral_stance_and_sentiments_normalized_file,
                                           dates_to_remain_stance_and_sentiments_single_tweet_per_user_file,
                                           dates_to_leave_stance_and_sentiments_single_tweet_per_user_file,
                                           dates_to_neutral_stance_and_sentiments_single_tweet_per_user_file,
                                           number_of_tweets_per_user_file]]):
        with open(dates_to_remain_stance_and_sentiments_file) as json_file:
            cur_dict = json.load(json_file)
            for k in cur_dict.keys():
                dates_to_remain_stance_and_sentiments[k] = cur_dict[k]
        with open(dates_to_leave_stance_and_sentiments_file) as json_file:
            cur_dict = json.load(json_file)
            for k in cur_dict.keys():
                dates_to_leave_stance_and_sentiments[k] = cur_dict[k]
        with open(dates_to_neutral_stance_and_sentiments_file) as json_file:
            cur_dict = json.load(json_file)
            for k in cur_dict.keys():
                dates_to_neutral_stance_and_sentiments[k] = cur_dict[k]

        with open(dates_to_remain_stance_and_sentiments_normalized_file) as json_file:
            cur_dict = json.load(json_file)
            for k in cur_dict.keys():
                dates_to_remain_stance_and_sentiments_normalized[k] = cur_dict[k]
        with open(dates_to_leave_stance_and_sentiments_normalized_file) as json_file:
            cur_dict = json.load(json_file)
            for k in cur_dict.keys():
                dates_to_leave_stance_and_sentiments_normalized[k] = cur_dict[k]
        with open(dates_to_neutral_stance_and_sentiments_normalized_file) as json_file:
            cur_dict = json.load(json_file)
            for k in cur_dict.keys():
                dates_to_neutral_stance_and_sentiments_normalized[k] = cur_dict[k]

        with open(dates_to_remain_stance_and_sentiments_single_tweet_per_user_file) as json_file:
            cur_dict = json.load(json_file)
            for k in cur_dict.keys():
                dates_to_remain_stance_and_sentiments_single_tweet_per_user[k] = cur_dict[k]
        with open(dates_to_leave_stance_and_sentiments_single_tweet_per_user_file) as json_file:
            cur_dict = json.load(json_file)
            for k in cur_dict.keys():
                dates_to_leave_stance_and_sentiments_single_tweet_per_user[k] = cur_dict[k]
        with open(dates_to_neutral_stance_and_sentiments_single_tweet_per_user_file) as json_file:
            cur_dict = json.load(json_file)
            for k in cur_dict.keys():
                dates_to_neutral_stance_and_sentiments_single_tweet_per_user[k] = cur_dict[k]

        with open(number_of_tweets_per_user_file) as json_file:
            cur_dict = json.load(json_file)
            for k in cur_dict.keys():
                number_of_tweets_per_user[k] = cur_dict[k]
        # with open(date_limits_file) as json_file:
        #     dates_limits = json.load(json_file)
        #     earliest_date, latest_date = dates_limits["earliest_date"], dates_limits["latest_date"]

    else:
        dates_to_remain_stance_and_sentiments, dates_to_leave_stance_and_sentiments, dates_to_neutral_stance_and_sentiments, number_of_tweets_per_user, earliest_date, latest_date = get_sentiment_counters()
        write_to_json_file_if_not_empty(dates_to_remain_stance_and_sentiments, dates_to_remain_stance_and_sentiments_file)
        write_to_json_file_if_not_empty(dates_to_leave_stance_and_sentiments, dates_to_leave_stance_and_sentiments_file)
        write_to_json_file_if_not_empty(dates_to_neutral_stance_and_sentiments, dates_to_neutral_stance_and_sentiments_file)
        write_to_json_file_if_not_empty(number_of_tweets_per_user, number_of_tweets_per_user_file)
        # write_to_json_file_if_not_empty({"earliest_date": earliest_date, "latest_date": latest_date}, date_limits_file)

        dates_to_remain_stance_and_sentiments_normalized, dates_to_leave_stance_and_sentiments_normalized, dates_to_neutral_stance_and_sentiments_normalized, number_of_tweets_per_user, earliest_date, latest_date = get_sentiment_counters(number_of_tweets_per_user)
        write_to_json_file_if_not_empty(dates_to_remain_stance_and_sentiments_normalized, dates_to_remain_stance_and_sentiments_normalized_file)
        write_to_json_file_if_not_empty(dates_to_leave_stance_and_sentiments_normalized, dates_to_leave_stance_and_sentiments_normalized_file)
        write_to_json_file_if_not_empty(dates_to_neutral_stance_and_sentiments_normalized, dates_to_neutral_stance_and_sentiments_normalized_file)

        dates_to_remain_stance_and_sentiments_single_tweet_per_user, dates_to_leave_stance_and_sentiments_single_tweet_per_user, dates_to_neutral_stance_and_sentiments_single_tweet_per_user, number_of_tweets_per_user, earliest_date, latest_date = get_sentiment_counters(limit_user_per_day=True)
        write_to_json_file_if_not_empty(dates_to_remain_stance_and_sentiments_single_tweet_per_user, dates_to_remain_stance_and_sentiments_single_tweet_per_user_file)
        write_to_json_file_if_not_empty(dates_to_leave_stance_and_sentiments_single_tweet_per_user, dates_to_leave_stance_and_sentiments_single_tweet_per_user_file)
        write_to_json_file_if_not_empty(dates_to_neutral_stance_and_sentiments_single_tweet_per_user, dates_to_neutral_stance_and_sentiments_single_tweet_per_user_file)

    ### Quantitative ###
    plot_qualitative_counters(dates_to_remain_stance_and_sentiments, dates_to_leave_stance_and_sentiments,
                                dates_to_neutral_stance_and_sentiments, earliest_date, latest_date)

    ### Percentage ###
    plot_percentage_counters(dates_to_remain_stance_and_sentiments, dates_to_leave_stance_and_sentiments,
                             dates_to_neutral_stance_and_sentiments, earliest_date, latest_date)

    ### Quantitative normalized###
    plot_qualitative_counters(dates_to_remain_stance_and_sentiments_normalized, dates_to_leave_stance_and_sentiments_normalized,
                                dates_to_neutral_stance_and_sentiments_normalized, earliest_date, latest_date, "_normalized")

    ### Percentage normalized###
    plot_percentage_counters(dates_to_remain_stance_and_sentiments_normalized, dates_to_leave_stance_and_sentiments_normalized,
                             dates_to_neutral_stance_and_sentiments_normalized, earliest_date, latest_date, "_normalized")

    ### Quantitative single tweet per user###
    plot_qualitative_counters(dates_to_remain_stance_and_sentiments_normalized, dates_to_leave_stance_and_sentiments_normalized,
                                dates_to_neutral_stance_and_sentiments_normalized, earliest_date, latest_date, "_single_tweet_per_user")

    ### Percentage single tweet per user###
    plot_percentage_counters(dates_to_remain_stance_and_sentiments_normalized, dates_to_leave_stance_and_sentiments_normalized,
                             dates_to_neutral_stance_and_sentiments_normalized, earliest_date, latest_date, "_single_tweet_per_user")


if __name__ == "__main__":
    print(f'{get_cur_formatted_time()} Start')
    # final_report_data_generator()
    final_report_plot_generator()
    print(f'{get_cur_formatted_time()} FIN')