import glob
import warnings
from collections import Counter

import pandas as pd

import matplotlib.pyplot as plt

from common_utiles import *

DELTA_TIME_IN_DAYS = 14
DEF_CSV_HEADER = ('t_id', 'user_id', 't_sentiment', 't_stance', 't_date') #ID~user_id~t_sentiment~t_stance
CSV_HEADER_INCL_TXT = ('t_id', 'user_id', 't_sentiment', 't_stance', 't_date', 't_text')

PLOTS_DATA_FOLDER = os.path.join("plots", "data_for_plots")
PLOTS_IMG_FOLDER = os.path.join("plots", "images")

BOT_SCORES_DF = None


def save_fig(f_name, f_format="png"):
    path = os.path.join(PLOTS_IMG_FOLDER, f'{f_name}.{f_format}')
    print(f'{get_cur_formatted_time()} Saving plot {path}')
    plt.savefig(path)
    plt.clf()

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

def plot_stances_from_counters(aggregated_df, y_col_name, earliest_date, latest_date, name, ylabel):
    ax = aggregated_df[aggregated_df["t_stance"] == "other"].plot.scatter(x="Date", y=y_col_name, color='green', label="other")
    aggregated_df[aggregated_df["t_stance"] == "remain"].plot.scatter(x="Date", y=y_col_name, ax=ax, color='blue', label="remain")
    aggregated_df[aggregated_df["t_stance"] == "leave"].plot.scatter(x="Date", y=y_col_name, ax=ax, color='red', label="leave")
    ax.legend(loc="upper right")
    #Todo - Make the x-axis limits as earliest_date, latest_date (respectively) and not min, max of aggregated_df[""Date"] (respectively)

    plt.title(f"Brexit tweets - {name}")
    plt.ylabel(ylabel)

    xticks_num = len(ax.xaxis.get_ticklabels())
    n = 7  # Keeps every nth label
    [l.set_visible(False) for (i, l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0 and i < xticks_num-1]
    plt.xticks(rotation=45, ha="right") #Tilt the x ticks lables
    plt.subplots_adjust(bottom=0.25)

    save_fig(name)

def add_folder_prefix(fname, folder = PLOTS_DATA_FOLDER):
    return os.path.join(folder, fname)

def get_min_and_max_dates_and_write_to_file(folder = FINAL_REPORT_DATA_FOLDER, read_from_existing_file = True):
    existing_fname = add_folder_prefix("date_limits.json")
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

def get_sentiment_aggregated_data(bot_score_threshold=None):
    earliest_date, latest_date = get_min_and_max_dates_and_write_to_file()

    aggregated_df = None
    bot_msg_suffix = ""
    filter_bots = False
    global BOT_SCORES_DF
    if not bot_score_threshold is None:
        if not 0 <= bot_score_threshold <= 1:
            warnings.warn(f'Bot score should be probability (between 0 and 1) but got {bot_score_threshold} - ignoring it')
        else:
            bot_msg_suffix = f" (bot score threshold {bot_score_threshold})"
            filter_bots = True
            if BOT_SCORES_DF is None:
                full_fname = os.path.join(DATA_FOLDER, "users_stance_sentiment_botscore_tweetcounts.csv")
                print(f'{get_cur_formatted_time()} Reading {full_fname}')
                BOT_SCORES_DF = pd.read_csv(full_fname, sep="~", names=["user_id", "user_sentiment", "user_stance", "bot_score", "bot_fetch_time", "tweets_num"])
                BOT_SCORES_DF.drop(columns=["user_sentiment", "user_stance", "bot_fetch_time", "tweets_num"], inplace=True)
                BOT_SCORES_DF = BOT_SCORES_DF[~BOT_SCORES_DF['bot_score'].isin([np.nan])]


    for fname in os.listdir(FINAL_REPORT_DATA_FOLDER):
        if not (fname.startswith(f'tweets_stance_sentiment_incl_date_and_text') and fname.endswith(".csv")):
            continue
        full_fname = os.path.join(FINAL_REPORT_DATA_FOLDER, fname)
        print(f'{get_cur_formatted_time()} Parsing {full_fname}{bot_msg_suffix}')
        df = pd.read_csv(full_fname)
        if filter_bots:
            df.set_index("user_id", inplace=True)
            BOT_SCORES_DF.set_index("user_id", inplace=True)

            df = df.join(BOT_SCORES_DF)

            df.reset_index(inplace=True)
            BOT_SCORES_DF.reset_index(inplace=True)

            df = df[np.logical_or(df["bot_score"].isin([np.nan]), df["bot_score"] <= bot_score_threshold)]
            df.drop(columns=["bot_score"], inplace=True)

        df['t_date'] = pd.to_datetime(df['t_date'])
        df['t_date'] = df['t_date'].apply(lambda d: d.replace(tzinfo=None))
        df["date_bucket_id"] = (df["t_date"] - earliest_date).apply(lambda t: math.floor(t.days/DELTA_TIME_IN_DAYS))
        df = df.groupby(by=["date_bucket_id", "t_stance"], as_index=False).size()
        if aggregated_df is None:
            aggregated_df = df
        else:
            d3 = pd.concat([aggregated_df, df])
            aggregated_df = d3.groupby(by=["date_bucket_id", "t_stance"], as_index=False).sum()


    aggregated_df["Date"] = aggregated_df["date_bucket_id"].apply(lambda i: earliest_date + datetime.timedelta(days=DELTA_TIME_IN_DAYS * i))
    aggregated_df.drop(columns=['date_bucket_id'], inplace=True)
    return aggregated_df, earliest_date, latest_date

def plot_quantitative_counters(sentiment_df, earliest_date, latest_date, name_suffix=""):
    plot_stances_from_counters(sentiment_df, "size", earliest_date, latest_date, f'quantitative{name_suffix}', "Count of tweets")

def plot_percentage_counters(sentiment_df, earliest_date, latest_date, name_suffix=""):

    plot_stances_from_counters(sentiment_df, "percent_per_date", earliest_date, latest_date, f'percentage{name_suffix}',
                               "Percent of tweets")

def get_percentage_df_from_quantitative(quantitative_df):
    percentage_df = quantitative_df.copy(deep=False)
    percentage_df["count_per_date"] = quantitative_df.groupby(by=["Date"]).transform('sum')["size"]
    percentage_df["percent_per_date"] = percentage_df["size"] / percentage_df["count_per_date"]
    percentage_df.drop(columns=['size', "count_per_date"], inplace=True)
    return percentage_df


def final_report_plot_generator():

    quantitative_df = None
    earliest_date, latest_date = "3000-01-01", "1000-01-01"
    bot_score_thresholds = [0.3, 0.5, 0.7, 0.98]  # Probability of an account being a bot (1 is the highest)
    quantitative_df_bots = [None] * len(bot_score_thresholds)

    quantitative_df_fname = add_folder_prefix("quantitative.csv")
    quantitative_df_fname_bots = [add_folder_prefix(f"quantitative_bot_filter_{s}.csv") for s in bot_score_thresholds]

    if np.all([os.path.isfile(f) for f in [quantitative_df_fname]]):
        quantitative_df = pd.read_csv(quantitative_df_fname)
        earliest_date, latest_date = get_min_and_max_dates_and_write_to_file()
    else:
        quantitative_df, earliest_date, latest_date = get_sentiment_aggregated_data()
        quantitative_df.to_csv(quantitative_df_fname)

    for i, bot_score_threshold in enumerate(bot_score_thresholds):
        if os.path.isfile(quantitative_df_fname_bots[i]):
            quantitative_df_bots[i] = pd.read_csv(quantitative_df_fname_bots[i])
        else:
            quantitative_df_bot, earliest_date, latest_date = get_sentiment_aggregated_data(bot_score_threshold)
            quantitative_df_bot.to_csv(quantitative_df_fname_bots[i])
            quantitative_df_bots[i] = quantitative_df_bot

    ### Quantitative ###
    plot_quantitative_counters(quantitative_df, earliest_date, latest_date)

    ### Percentage ###
    percentage_df = get_percentage_df_from_quantitative(quantitative_df)
    plot_percentage_counters(percentage_df, earliest_date, latest_date)

    for i, bot_score_threshold in enumerate(bot_score_thresholds):
        q_df = quantitative_df_bots[i]
        plot_quantitative_counters(q_df, earliest_date, latest_date, f'_botscore_{bot_score_threshold}')
        p_df = get_percentage_df_from_quantitative(q_df)
        plot_percentage_counters(p_df, earliest_date, latest_date, f'_botscore_{bot_score_threshold}')


    '''
    TODO - Need to do analysis for each one of the following counting "policies":
     - Allow every user no more than a single tweet a day (per stance)  
     - Cut off completely users with a botscore higher than [0.3, 0.5, 0.7] (do separate calculation for each threshold)   
     - Count every tweet as one divided by the number of overall tweets that the user tweeted overall in the dataset
     - Count every tweet as one divided by the number of overall tweets that the user tweeted in each timespan separately
    '''




if __name__ == "__main__":
    print(f'{get_cur_formatted_time()} Start')
    # final_report_data_generator()
    final_report_plot_generator()
    print(f'{get_cur_formatted_time()} FIN')