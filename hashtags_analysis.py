import warnings
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import matplotlib.patches as mpatches

from common_utiles import *

BOT_SCORES_DF = None
TAG_FREQ_PERCANT_CUTOFF = [0.1, 0.25, 0.5, 1, 1.5]
DEFAULT_FIGSIZE_VALS = (6.4, 4.8) #See https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.figure.html

#Source: https://ukandeu.ac.uk/how-remain-and-leave-camps-use-hashtags/ (and a bit from https://hashtagify.me/hashtag/brexit)
LEAVE_TAGS = ['no2eu','notoeu','betteroffout', 'voteout','eureform', 'britainout', 'leaveeu', 'voteleave', 'beleave', 'loveeuropeleaveeu']
REMAIN_TAGS = ['yes2eu','yestoeu','betteroffin', 'votein', 'ukineu', 'bremain', 'strongerin','leadnotleave', 'voteremain', 'remain', 'stopbrexit', 'fbpe', 'brexitreality', 'brexitshambles','torybrexitdisaster', 'death2brexit', 'godblesseu']
#For #fbpe look for example at: https://www.markpack.org.uk/153702/fbpe-what-does-it-mean/

TOKENS_TO_REMOVE = [":", ",", ".", ";", "!", "…"]

def reomve_tokens(s, tokens=TOKENS_TO_REMOVE):
    for t in tokens:
        s = s.replace(t, "")
    return s


def extract_hash_tags(s):
    #Source: https://stackoverflow.com/a/2527903
    tags = set(part[1:] for part in s.split() if part.startswith('#'))
    tags = [reomve_tokens(t.lower()) for t in tags]

    return tags

def sum_counters(counter_list):

    '''
    Recursive counter with a O(log(n)) Complexity (Source https://stackoverflow.com/a/62393323)
    '''

    if len(counter_list) > 10:

        counter_0 = sum_counters(counter_list[:int(len(counter_list)/2)])
        counter_1 = sum_counters(counter_list[int(len(counter_list)/2):])

        return sum([counter_0, counter_1], Counter())

    else:

        return sum(counter_list, Counter())

def get_hashtags_counter(bot_score_threshold=None):
    hashtags_counter = Counter()
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

        df["hashtags"] = df["t_text"].apply(extract_hash_tags)
        df = df.drop(df.columns.difference(["hashtags"]), axis="columns")  #Drop all but "hashtags" column
        counters_list = [Counter(x) for x in df["hashtags"].values]
        counters_list.append(hashtags_counter)
        hashtags_counter = sum_counters(counters_list)

    return hashtags_counter

def plot_most_common(counter, n=10, keys_to_ignore=[], title_suffix=""):
    most_common_hashtags = counter.most_common(n)
    x_vals, y_vals = [], []
    colors = []
    if n <= 10:
        plt.figure(figsize=tuple([z * 1.5 for z in DEFAULT_FIGSIZE_VALS]))
    else:
        plt.figure(figsize=tuple([z * 2 for z in DEFAULT_FIGSIZE_VALS]))
    for i, tag in enumerate(most_common_hashtags):
        if not tag[0] in keys_to_ignore:
            x_vals.append(tag[0])
            y_vals.append(tag[1])
            plt.text(x=i, y=tag[1] + 1, s=f"{tag[1]}", fontdict=dict(fontsize=10), ha='center') #Source: https://stackoverflow.com/a/55866275

            if tag[0] in REMAIN_TAGS:
                colors.append(REMAIN_COLOR)
            elif tag[0] in LEAVE_TAGS:
                colors.append(LEAVE_COLOR)
            else:
                colors.append(OTHER_STANCE_COLOR)
    plt.bar(x_vals, y_vals, color=colors)
    plt.title(f'{n} most common tags{title_suffix}')

    #Custom legend (see https://stackoverflow.com/a/39500357)
    legend_item_remain = mpatches.Patch(color=REMAIN_COLOR, label='Remain')
    legend_item_leave = mpatches.Patch(color=LEAVE_COLOR, label='Leave')
    legend_item_other = mpatches.Patch(color=OTHER_STANCE_COLOR, label='Other')
    plt.legend(handles=[legend_item_remain, legend_item_leave, legend_item_other])

    plt.xticks(rotation=45, ha="right") #Tilt the x ticks lables
    plt.subplots_adjust(bottom=0.25)
    save_fig(f'{n}_most_common_tags{title_suffix.lower().replace(" ", "_").replace("-", "_")}')


def analyze_hashtags():
    counter_fname = os.path.join(PLOTS_DATA_FOLDER, "hashtags_counter.json")
    if os.path.isfile(counter_fname):
        print(f'{get_cur_formatted_time()} Reading data from {counter_fname}')
        with open(counter_fname, "r") as f:
            cur_dict = json.load(f)
            hashtags_counter = Counter(cur_dict)
    else:
        print(f'{get_cur_formatted_time()} No existing data file found, calculating hashtags frequency')
        hashtags_counter = get_hashtags_counter()
        write_to_json_file_if_not_empty(hashtags_counter, counter_fname)

    plot_most_common(hashtags_counter)
    plot_most_common(hashtags_counter, keys_to_ignore=["brexit"], title_suffix=" without brexit")

    filtered_counter = Counter({t: hashtags_counter[t] for t in set(LEAVE_TAGS).union(set(REMAIN_TAGS))})
    # dict_you_want = {your_key: old_dict[your_key] for your_key in your_keys} #Source: https://stackoverflow.com/a/3420156
    plot_most_common(filtered_counter,title_suffix=" pure-stance hashtags")
    plot_most_common(filtered_counter,n=len(filtered_counter) ,title_suffix=" pure-stance hashtags")


    '''
    TODO:
     - For each p_cutoff in TAG_FREQ_PERCANT_CUTOFF:
         - Only look at hashtags that appear at least in  (p_cutoff/100) of all tweets that have a tag
    
    '''


if __name__ == "__main__":
    print(f'{get_cur_formatted_time()} Start')
    # final_report_data_generator()
    analyze_hashtags()
    print(f'{get_cur_formatted_time()} FIN')