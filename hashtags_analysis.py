from collections import Counter, OrderedDict

import matplotlib.patches as mpatches

from common_utiles import *

BOT_SCORES_DF = None
TAG_FREQ_PERCANT_CUTOFF = [0.1, 0.25, 0.5, 1, 1.5]
DEFAULT_FIGSIZE_VALS = (6.4, 4.8)  # See https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.figure.html

# Source: https://ukandeu.ac.uk/how-remain-and-leave-camps-use-hashtags/ (and a bit from https://hashtagify.me/hashtag/brexit)
LEAVE_TAGS = ['no2eu', 'notoeu', 'no2europe', 'notoeurope', 'betteroffout', 'voteout', 'eureform', 'britainout',
              'leaveeu', 'voteleave', 'beleave', 'loveeuropeleaveeu', 'leave', 'brexiteer', 'brexiteers',
              'standup4brexit', 'brexiters', 'saynotoeurope']
REMAIN_TAGS = ['yes2eu', 'yestoeu', 'yes2europe', 'yestoeurope', 'betteroffin', 'votein', 'ukineu', 'bremain',
               'strongerin', 'leadnotleave', 'voteremain', 'remain', 'stopbrexit', 'fbpe', 'brexitreality',
               'brexitshambles', 'torybrexitdisaster', 'death2brexit', 'deathtobrexit', 'godblesseu', 'godblesseurope',
               'stopbrexitsavebritain', 'exitfrombrexit', 'revokearticle50', 'revokea50', 'remainer', 'remainers',
               'nobrexit', 'voteleavebrokethelaw', 'brexitstupidity', 'stupidbrexit', 'stupidbrexiteer',
               'stupidbrexiteers', 'brexitisstupid', 'brexitmeansstupid', 'brexitisabloodystupididea']
# For #fbpe look for example at: https://www.markpack.org.uk/153702/fbpe-what-does-it-mean/

TOKENS_TO_REMOVE = [":", ",", ".", ";", "!", "…", '\\', '/']


def reomve_tokens(s, tokens=TOKENS_TO_REMOVE):
    for t in tokens:
        s = s.replace(t, "")
    return s


def extract_hash_tags(s):
    # Source: https://stackoverflow.com/a/2527903
    tags = set(part[1:] for part in s.split() if part.startswith('#'))
    tags = [reomve_tokens(t.lower()) for t in tags]
    tags = list(filter(None, tags))  # Remove empty strings

    return tags


def sum_counters(counter_list):
    '''
    Recursive counter with a O(log(n)) Complexity (Source https://stackoverflow.com/a/62393323)
    '''

    if len(counter_list) > 10:

        counter_0 = sum_counters(counter_list[:int(len(counter_list) / 2)])
        counter_1 = sum_counters(counter_list[int(len(counter_list) / 2):])

        return sum([counter_0, counter_1], Counter())

    else:

        return sum(counter_list, Counter())


def calculate_hashtags_counter(bot_score_threshold=None):
    hashtags_counter = Counter()
    should_filter_bots, bot_msg_suffix = handle_bots(bot_score_threshold)

    for fname in os.listdir(FINAL_REPORT_DATA_FOLDER):
        if not (fname.startswith(f'tweets_stance_sentiment_incl_date_and_text') and fname.endswith(".csv")):
            continue
        full_fname = os.path.join(FINAL_REPORT_DATA_FOLDER, fname)
        print(f'{get_cur_formatted_time()} Parsing {full_fname}{bot_msg_suffix}')
        df = pd.read_csv(full_fname)
        if should_filter_bots:
            df = remove_bots_by_threshold(df, bot_score_threshold)

        df["hashtags"] = df["t_text"].apply(extract_hash_tags)
        df = df.drop(df.columns.difference(["hashtags"]), axis="columns")  # Drop all but "hashtags" column
        counters_list = [Counter(x) for x in df["hashtags"].values]
        counters_list.append(hashtags_counter)
        hashtags_counter = sum_counters(counters_list)

    return Counter(OrderedDict(hashtags_counter.most_common()))  # Sorting the counter


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
            plt.text(x=i, y=tag[1] + 1, s=f"{tag[1]}", fontdict=dict(fontsize=10),
                     ha='center')  # Source: https://stackoverflow.com/a/55866275

            if tag[0] in REMAIN_TAGS:
                colors.append(REMAIN_COLOR)
            elif tag[0] in LEAVE_TAGS:
                colors.append(LEAVE_COLOR)
            else:
                colors.append(OTHER_STANCE_COLOR)
    plt.bar(x_vals, y_vals, color=colors)
    plt.title(f'{n} most common tags{title_suffix}')

    # Custom legend (see https://stackoverflow.com/a/39500357)
    legend_item_remain = mpatches.Patch(color=REMAIN_COLOR, label='Remain')
    legend_item_leave = mpatches.Patch(color=LEAVE_COLOR, label='Leave')
    legend_item_other = mpatches.Patch(color=OTHER_STANCE_COLOR, label='Other')
    plt.legend(handles=[legend_item_remain, legend_item_leave, legend_item_other])

    plt.xticks(rotation=45, ha="right")  # Tilt the x ticks lables
    plt.subplots_adjust(bottom=0.25)
    save_fig(f'{n}_most_common_tags{title_suffix.lower().replace(" ", "_").replace("-", "_")}')


def get_and_write_hashtags_counter():
    counter_fname = os.path.join(PLOTS_DATA_FOLDER, "hashtags_counter.json")
    if os.path.isfile(counter_fname):
        print(f'{get_cur_formatted_time()} Reading data from {counter_fname}')
        with open(counter_fname, "r") as f:
            cur_dict = json.load(f)
            hashtags_counter = Counter(cur_dict)
    else:
        print(f'{get_cur_formatted_time()} No existing data file found, calculating hashtags frequency')
        hashtags_counter = calculate_hashtags_counter()
        write_to_json_file_if_not_empty(hashtags_counter, counter_fname)
    return hashtags_counter


def get_only_specific_keys_from_counter(counter, keys=set(LEAVE_TAGS).union(set(REMAIN_TAGS)), ignore_zero_count=True,
                                        reverse=False):
    res = Counter({k: counter[k] for k in keys})
    if ignore_zero_count:
        res = Counter(el for el in res.elements() if res[el] > 0)
    if reverse:
        res = OrderedDict(res.most_common())
        res = Counter(OrderedDict(reversed(list(res.items()))))

    return res


def create_hashtags_histograms():
    hashtags_counter = get_and_write_hashtags_counter()
    plot_most_common(hashtags_counter)
    plot_most_common(hashtags_counter, keys_to_ignore=["brexit"], title_suffix=" without brexit")

    filtered_counter = get_only_specific_keys_from_counter(hashtags_counter)
    plot_most_common(filtered_counter, title_suffix=" pure-stance hashtags")
    plot_most_common(filtered_counter, n=len(filtered_counter), title_suffix=" pure-stance hashtags")

    '''
    TODO:
     - For each p_cutoff in TAG_FREQ_PERCANT_CUTOFF:
         - Only look at hashtags that appear at least in  (p_cutoff/100) of all tweets that have a tag
    
    '''


def is_quota_met_for_tag(df, num_tweets_required):
    return (not df is None) and len(df.index) >= num_tweets_required


def get_exist_tags_to_tweets_or_default(hashtags_counter):
    tag_to_tweets = {tag: None for tag in hashtags_counter}
    existing_counters_count = 0
    for tag in hashtags_counter:
        fname = os.path.join(PLOTS_DATA_FOLDER, 'hashtag_tweets', f"hashtag_{tag}_tweets.csv")
        if os.path.isfile(fname):
            tag_to_tweets[tag] = pd.read_csv(fname)
            existing_counters_count += 1

    print(f'{get_cur_formatted_time()} Found existing data for {existing_counters_count}/{len(hashtags_counter)} tags')
    return tag_to_tweets

def create_tweets_with_pure_stance_tags(bot_score_threshold=None):
    should_filter_bots, bot_msg_suffix = handle_bots(bot_score_threshold)
    hashtags_counter = get_and_write_hashtags_counter()
    hashtags_counter = get_only_specific_keys_from_counter(hashtags_counter, reverse=True)
    max_tweets_per_tag = 200
    tag_to_tweets = get_exist_tags_to_tweets_or_default(hashtags_counter)
    tag_to_num_tweets_required = {tag: min(hashtags_counter[tag], max_tweets_per_tag) for tag in hashtags_counter}



    for fname in os.listdir(FINAL_REPORT_DATA_FOLDER):
        if all([is_quota_met_for_tag(tag_to_tweets[tag], tag_to_num_tweets_required[tag]) for tag in
                tag_to_num_tweets_required]):
            print(f'{get_cur_formatted_time()} Quotas for all tags met - not checking anymore files')
            break
        if not (fname.startswith(f'tweets_stance_sentiment_incl_date_and_text') and fname.endswith(".csv")):
            continue
        full_fname = os.path.join(FINAL_REPORT_DATA_FOLDER, fname)
        print(f'{get_cur_formatted_time()} Parsing {full_fname}{bot_msg_suffix}')
        df = pd.read_csv(full_fname)
        if should_filter_bots:
            df = remove_bots_by_threshold(df, bot_score_threshold)

        hashtags_set = False

        for cur_tag in hashtags_counter:
            print(f'{get_cur_formatted_time()} Tag {cur_tag}')
            if is_quota_met_for_tag(tag_to_tweets[cur_tag], tag_to_num_tweets_required[cur_tag]):
                print(f'{get_cur_formatted_time()} Tag quota reached')
                continue
            if not hashtags_set:
                df["hashtags"] = df["t_text"].apply(extract_hash_tags)
                df.sort_values(by=['hashtags'], inplace=True)
                hashtags_set = True

            df["is_tag_present"] = df['hashtags'].apply(lambda l: cur_tag in l)
            df_for_tag = df[df["is_tag_present"]]

            print(f'{get_cur_formatted_time()} Found {len(df_for_tag.index)} tweets containing tag')

            if len(df_for_tag.index) == 0:
                continue
            df_for_tag = df_for_tag.drop(["is_tag_present"], axis="columns")
            df_for_tag = df_for_tag.head(tag_to_num_tweets_required[cur_tag])
            if tag_to_tweets[cur_tag] is None:
                tag_to_tweets[cur_tag] = df_for_tag
            else:
                df_for_tag = df_for_tag.head(tag_to_num_tweets_required[cur_tag] - len(tag_to_tweets[cur_tag].index))
                tag_to_tweets[cur_tag] = pd.concat([tag_to_tweets[cur_tag], df_for_tag])

    quotas_not_met = []
    for cur_tag in tag_to_num_tweets_required:
        if tag_to_tweets[cur_tag] is None or len(tag_to_tweets[cur_tag].index) < tag_to_num_tweets_required[cur_tag]:
            quotas_not_met.append(f'{cur_tag}: {0 if tag_to_tweets[cur_tag] is None else len(tag_to_tweets[cur_tag].index)}/{tag_to_num_tweets_required[cur_tag]}')

    if len(quotas_not_met) > 0:
        print(f'Tags not found in enough tweets: {quotas_not_met.join(", ")}')

    return tag_to_tweets


if __name__ == "__main__":
    print(f'{get_cur_formatted_time()} Start')
    create_hashtags_histograms()
    tag_to_tweets_df = create_tweets_with_pure_stance_tags()
    for tag in tag_to_tweets_df:
        df_to_csv_plus_create_dir(tag_to_tweets_df[tag], os.path.join(PLOTS_DATA_FOLDER, 'hashtag_tweets'),
                                  f"hashtag_{tag}_tweets.csv")

    print(f'{get_cur_formatted_time()} FIN')
