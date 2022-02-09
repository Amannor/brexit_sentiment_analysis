import html
import html.parser
import json
import os
import re
from collections import Counter, OrderedDict
from string import punctuation

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd

import common_utiles as cu

BOT_SCORES_DF = None
TAG_FREQ_PERCANT_CUTOFF = [0.1, 0.25, 0.5, 1, 1.5]
DEFAULT_FIGSIZE_VALS = (6.4, 4.8)  # See https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.figure.html

'''
Sources for tag stance assigning:
https://inews.co.uk/news/politics/know-fbpe-pcpeu-guide-remainer-hashtags-127673
https://ukandeu.ac.uk/how-remain-and-leave-camps-use-hashtags/ (and a bit from https://hashtagify.me/hashtag/brexit)
For #fbpe look for example at: https://www.markpack.org.uk/153702/fbpe-what-does-it-mean/
'bollockstobrexit': https://en.wikipedia.org/wiki/Bollocks_to_Brexit
'''
LEAVE_TAGS = set(['no2eu', 'notoeu', 'no2europe', 'notoeurope', 'betteroffout', 'voteout', 'eureform', 'britainout',
                  'leaveeu', 'leaveeurope', 'voteleave', 'beleave', 'loveeuropeleaveeu', 'leave', 'brexiteer',
                  'brexiteers', 'brexiters', 'saynotoeurope', 'britishindependence', 'brexitnow', 'brexitforever',
                  'backthebrexitdeal', 'standup4brexit', 'standupforbrexit', 'standup4bexit', 'standupforbexit',
                  'standup4brexiit', 'standupforbrexiit', 'pro-#brexit', 'pro-brexit', 'probrexit', 'fcukeu',
                  'keepcalmandfcukeu', 'bleave', 'euexit', 'theonlywayisukip', 'makeukipgreatagain', 'backtoukip'
                  'ukipallthewaynow', 'jointheukipfightback', 'voteukip', 'leaveeuofficial', 'leaveeuropeofficial',
                  'fishingforleave', 'fishingforleav', 'supportukip', 'joinukiptoday', 'voteukiptoday', 'ukipisfreedom',
                  'ukip4realbrexit', 'wantbrexitvoteukip', 'ukipfortheunion', 'ukipunited', 'ukvoteukip', 'ukip-forever'])
REMAIN_TAGS = set(['yes2eu', 'yestoeu', 'yes2europe', 'yestoeurope', 'betteroffin', 'votein', 'ukineu', 'bremain',
                   'strongerin', 'leadnotleave', 'voteremain', 'remain', 'stopbrexit', 'fbpe', 'brexitreality',
                   'brexitshambles', 'torybrexitshambles', 'torybrexitdisaster', 'death2brexit', 'deathtobrexit',
                   'godblesseu', 'godblesseurope', 'stopbrexitsavebritain', 'exitfrombrexit', 'revokearticle50',
                   'revokearticle50now', 'revokeart50now', 'revokea50', 'revokea50now', 'revokearticle50petition',
                   'remainer', 'remainers', 'nobrexit', 'voteleavebrokethelaw', 'brexitstupidity', 'stupidbrexit',
                   'stupidbrexiteer', 'stupidbrexiteers', 'brexitisstupid', 'brexitmeansstupid',
                   'brexitisabloodystupididea', 'stopbrexit2018', 'stopbrexitsaveournhs', 'stopbrexitsavenhs',
                   'stopbrexitfixbritain', 'killbrexitnow', 'brexitwontwork', 'brexitsucks', 'stopbrexitnow',
                   'remainineu', 'cancelbrexit', 'stopbrexitsavedemocracy', 'brexshit', 'bollockstobrexit',
                   'iameuropean', 'toryshambles', 'getbrexitgone', 'stopthebrexitcoup', 'rejoineu', 'proeu',
                   'brexitbluff', 'brrrexshit', 'brexitfraud', 'fuckbrexit', 'brexitfail', 'standup2brexit',
                   'standuptobrexit', 'standupagainstbrexit', 'standup2brexitnonsense', 'standuptobrexitnonsense',
                   'standupstopbrexit', 'stopbrexitsaturday', 'mpstopbrexit', 'brexitleavesbritainnaked', 'binbrexit',
                   'pcpeu', 'waton', 'fuckukip', 'fukip', 'ukipout', 'fckukip', 'ukipfraud', 'banukip', 'ukipscum',
                   'nevervoteukip', 'bringdowntoryukipgov', 'dontjoinukip', 'ukipisajoke', 'ukiparepoop', 'ukipfascists',
                   'ukipnonsense', 'ukipracists', 'racistukip', 'stukipid', 'ukipinsheepsclothing',
                   'ukiptoryfascistsout', 'ofoc', 'saytostay'])

ALL_TAGS = LEAVE_TAGS.union(REMAIN_TAGS)

TOKENS_TO_REMOVE = f'{punctuation.replace("?", "")}…'

def process_tokens(s):
    unescaped = ""
    while unescaped != s:  # E.g. when s = '&amp#8216brexit&amp#8217' (in this case the "real" string is html.unescape(html.unescape(s))).
        # Code inspired by: https://stackoverflow.com/a/58739826
        unescaped = html.unescape(s)
        s = html.unescape(unescaped)

    tmp = s
    if s.isascii() and s.isprintable():
        # The reason for this if is a string like "지민" that doesn't throw exception on s.encode().decode('unicode_escape') - but comes out invalid from it.
        # See: https://stackoverflow.com/a/51141941
        '''
        General note for this section: A LOT of time was spent trying to come up with a reasonable solution for dealing with weird, edgde-cases strings.
        There's a good chance there isn't a one-size-fits-all solution and every approach that'll "fix" some strings would "screw" others 
            (which make sense given the myriad sources of the strings - hashtags from tweets regarding brexit that were tweeted by people in pretty much any language in the OECD countries)
        HOWEVER, there's one approach that hasn't been tried yet and it's worth a shot (it's just a lot of tedious work, with no clear ROI)
        That is: one can iterate over all possible encoding values for both decode and encode*, and compare the resulting artifcats (mainly the resulting Counter) to choose the best ones

        *Both decode and encode methods get an 'encoding' param. For all possibilities of it see:  Encode: https://docs.python.org/3.9/library/codecs.html#standard-encodings  
        '''
        try:
            s = s.encode().decode('unicode_escape')  # E.g. when s = "sunse\\u2026" and html.unescape doesn't fix it
                                                    # (see: https://stackoverflow.com/a/55889036)
        except UnicodeDecodeError:
            # Example for a string that causes this exception: 'good/#bad?:o\\'
            s = tmp

    s = s.strip(f'{punctuation.replace("?", "")}’')

    tmp = s
    for t in TOKENS_TO_REMOVE:
        tmp = tmp.replace(t, "")
    return tmp if (tmp == "" or tmp in ALL_TAGS) else s


def extract_hash_tags(s):

    s = s.replace("@", "@ ") #To make sure no user-taggings will be in any hashtag

    # Based on: https://stackoverflow.com/a/2527903
    tags = set(part[1:] for part in s.split() if part.startswith('#'))
    tags_to_remove = set()
    tags_to_add = set()
    for cur_t in tags:
        if any(x in cur_t for x in ["www.", "http:", "https:"]):
            continue

        for token in ["_","-"]:
            if f'{token}#' in cur_t:
                tags_to_remove.add(cur_t)
                tags_to_add.add((cur_t.replace(f'{token}#', token)))

    tags -= tags_to_remove
    tags.update(tags_to_add)
    tags_to_remove = set()
    tags_to_add = set()

    for cur_t in tags:
        if "#" in cur_t:
            tags_to_remove.add(cur_t)
            tags_to_add.update(set(cur_t.split("#")))

    tags -= tags_to_remove
    tags.update(tags_to_add)

    '''
    TODO (optional): there are tags that contain the sequence '"\' - not sure what to do with (some cases require
    splitting to separate tags, some just deleting the sequence from the tag (and not splitting). It's pretty rare so
    no big deal
    '''


    tags = [process_tokens(t.lower()) for t in tags]
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
    should_filter_bots, bot_msg_suffix = cu.handle_bots(bot_score_threshold)

    for fname in os.listdir(cu.FINAL_REPORT_DATA_FOLDER):
        if not (fname.startswith(f'tweets_stance_sentiment_incl_date_and_text') and fname.endswith(".csv")):
            continue
        full_fname = os.path.join(cu.FINAL_REPORT_DATA_FOLDER, fname)
        print(f'{cu.get_cur_formatted_time()} Parsing {full_fname}{bot_msg_suffix}')
        df = pd.read_csv(full_fname)
        if should_filter_bots:
            df = cu.remove_bots_by_threshold(df, bot_score_threshold)

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
                colors.append(cu.REMAIN_COLOR)
            elif tag[0] in LEAVE_TAGS:
                colors.append(cu.LEAVE_COLOR)
            else:
                colors.append(cu.OTHER_STANCE_COLOR)
    plt.bar(x_vals, y_vals, color=colors)
    plt.title(f'{n} most common tags{title_suffix}')

    # Custom legend (see https://stackoverflow.com/a/39500357)
    legend_item_remain = mpatches.Patch(color=cu.REMAIN_COLOR, label='Remain')
    legend_item_leave = mpatches.Patch(color=cu.LEAVE_COLOR, label='Leave')
    legend_item_other = mpatches.Patch(color=cu.OTHER_STANCE_COLOR, label='Other')
    plt.legend(handles=[legend_item_remain, legend_item_leave, legend_item_other])

    plt.xticks(rotation=45, ha="right")  # Tilt the x ticks lables
    plt.subplots_adjust(bottom=0.25)
    cu.save_fig(f'{n}_most_common_tags{title_suffix.lower().replace(" ", "_").replace("-", "_").replace(",", "")}')


def get_and_write_hashtags_counter():
    counter_fname = os.path.join(cu.PLOTS_DATA_FOLDER, "hashtags_counter.json")
    if os.path.isfile(counter_fname):
        print(f'{cu.get_cur_formatted_time()} Reading data from {counter_fname}')
        with open(counter_fname, "r", encoding="utf-8") as f:
            cur_dict = json.load(f)
            hashtags_counter = Counter(cur_dict)
    else:
        print(f'{cu.get_cur_formatted_time()} No existing data file found, calculating hashtags frequency')
        hashtags_counter = calculate_hashtags_counter()
        cu.write_to_json_file_if_not_empty(hashtags_counter, counter_fname, escape_html_chars=False)
    return hashtags_counter


def get_only_specific_keys_from_counter(counter, keys=ALL_TAGS, reverse=False, threshold_count=0):
    res = Counter({k: counter[k] for k in keys})
    if threshold_count is not None:
        res = Counter(el for el in res.elements() if res[el] > threshold_count)

    if reverse:
        res = OrderedDict(res.most_common())
        res = Counter(OrderedDict(reversed(list(res.items()))))

    return res

def create_hashtags_histograms():
    hashtags_counter = get_and_write_hashtags_counter()
    plot_most_common(hashtags_counter)
    tags_to_ignore = ["brexit", "uk", "eu", "news"]
    plot_most_common(hashtags_counter, keys_to_ignore=tags_to_ignore, title_suffix=f" without {', '.join(tags_to_ignore)}")

    filtered_counter = get_only_specific_keys_from_counter(hashtags_counter)
    plot_most_common(filtered_counter, title_suffix=" pure-stance hashtags")
    filtered_counter_remain = get_only_specific_keys_from_counter(hashtags_counter, keys = LEAVE_TAGS)
    filtered_counter_remain = filtered_counter_remain.most_common(20)
    filtered_counter_remain = Counter({i[0]: i[1] for i in filtered_counter_remain})
    filtered_counter_leave = get_only_specific_keys_from_counter(hashtags_counter, keys = REMAIN_TAGS)
    filtered_counter_leave = filtered_counter_leave.most_common(20)
    filtered_counter_leave = Counter({i[0]: i[1] for i in filtered_counter_leave})
    filtered_counter = sum_counters([filtered_counter_remain, filtered_counter_leave])
    plot_most_common(filtered_counter, n=len(filtered_counter), title_suffix=" top 20 from each pure-stance hashtags")

    '''
    TODO:
     - For each p_cutoff in TAG_FREQ_PERCANT_CUTOFF:
         - Only look at hashtags that appear at least in  (p_cutoff/100) of all tweets that have a tag
    
    '''


def is_quota_met_for_tag(df, num_tweets_required):
    return (not df is None) and len(df.index) >= num_tweets_required


def get_exist_tags_to_tweets_or_default(hashtags_counter, dir = os.path.join(cu.PLOTS_DATA_FOLDER, 'hashtag_tweets')):
    tag_to_tweets = {tag: None for tag in hashtags_counter}
    existing_counters_count = 0
    print(f'{cu.get_cur_formatted_time()} Checking for existing tags_to_tweets in {dir}')
    for tag in hashtags_counter:
        fname = os.path.join(dir, f"hashtag_{tag}_tweets.csv")
        if os.path.isfile(fname):
            tag_to_tweets[tag] = pd.read_csv(fname)
            existing_counters_count += 1

    print(f'{cu.get_cur_formatted_time()} Found existing data for {existing_counters_count}/{len(hashtags_counter)} tags (in {dir})')
    return tag_to_tweets


def write_tags_to_tweets_least_common_arbitrator(dir_to_check_existing, bot_score_threshold=None):
    should_filter_bots, bot_msg_suffix = cu.handle_bots(bot_score_threshold)
    hashtags_counter = get_and_write_hashtags_counter()
    hashtags_counter = get_only_specific_keys_from_counter(hashtags_counter, reverse=True, threshold_count=200)

    for fname in os.listdir(cu.FINAL_REPORT_DATA_FOLDER):
        if not (fname.startswith(f'tweets_stance_sentiment_incl_date_and_text') and fname.endswith(".csv")):
            continue
        full_fname = os.path.join(cu.FINAL_REPORT_DATA_FOLDER, fname)
        print(f'{cu.get_cur_formatted_time()} Parsing {full_fname}{bot_msg_suffix}')
        df = pd.read_csv(full_fname)
        if should_filter_bots:
            df = cu.remove_bots_by_threshold(df, bot_score_threshold)

        df["hashtags"] = df["t_text"].apply(extract_hash_tags)

        main_file_id = re.search(r"\d", fname).start()
        sub_file_id = re.search(r"\d", fname[main_file_id + 1:]).start() + main_file_id + 1
        main_file_id, sub_file_id = fname[main_file_id], fname[sub_file_id]

        for cur_tag in hashtags_counter:
            print(f'{cu.get_cur_formatted_time()} Tag {cur_tag}')
            full_tag_fname = os.path.join(dir_to_check_existing, f"hashtag_{cur_tag}_tweets_{main_file_id}_{sub_file_id}.csv")
            if os.path.isfile(full_tag_fname):
                df_for_tag = pd.read_csv(full_tag_fname)
                if len(df_for_tag.index) == 0:
                    print(f'{cu.get_cur_formatted_time()} found {full_tag_fname} - skipping this tag')
                    continue



            df["is_tag_present"] = df['hashtags'].apply(lambda l: cur_tag in l)
            df_for_tag = df[df["is_tag_present"]]
            print(f'{cu.get_cur_formatted_time()} Found {len(df_for_tag.index)} tweets containing tag')

            if len(df_for_tag.index) == 0:
                continue

            df_for_tag["hashtags"] = df_for_tag["hashtags"].apply(lambda l: choose_least_common_tag(hashtags_counter, l))
            df_for_tag["is_tag_present"] = df_for_tag['hashtags'].apply(lambda l: cur_tag in l)
            df_for_tag = df_for_tag[df_for_tag["is_tag_present"]]
            print(f'{cu.get_cur_formatted_time()} Found {len(df_for_tag.index)} tweets that passed arbitrator')

            if len(df_for_tag.index) == 0:
                continue

            df_for_tag = df_for_tag.drop(["is_tag_present"], axis="columns")

            cu.df_to_csv_plus_create_dir(df_for_tag, dir_to_check_existing, tag_fname)


def create_tweets_with_pure_stance_tags(bot_score_threshold=None, max_tweets_per_tag=200):
    should_filter_bots, bot_msg_suffix = cu.handle_bots(bot_score_threshold)
    hashtags_counter = get_and_write_hashtags_counter()
    hashtags_counter = get_only_specific_keys_from_counter(hashtags_counter, reverse=True)
    no_pure_stance_tags_key = "no_stance_tags"
    no_tags_at_all_key = "no_tags_at_all"
    hashtags_counter[no_pure_stance_tags_key] = int(max_tweets_per_tag / 2)
    hashtags_counter[no_tags_at_all_key] = int(max_tweets_per_tag / 2)
    tag_to_num_tweets_required = {tag: min(hashtags_counter[tag], max_tweets_per_tag) for tag in hashtags_counter}
    tag_to_tweets = get_exist_tags_to_tweets_or_default(hashtags_counter)

    for fname in os.listdir(cu.FINAL_REPORT_DATA_FOLDER):
        if all([is_quota_met_for_tag(tag_to_tweets[tag], tag_to_num_tweets_required[tag]) for tag in tag_to_num_tweets_required]):
            print(f'{cu.get_cur_formatted_time()} Quotas for all tags met - not checking anymore files')
            break
        if not (fname.startswith(f'tweets_stance_sentiment_incl_date_and_text') and fname.endswith(".csv")):
            continue
        full_fname = os.path.join(cu.FINAL_REPORT_DATA_FOLDER, fname)
        print(f'{cu.get_cur_formatted_time()} Parsing {full_fname}{bot_msg_suffix}')
        df = pd.read_csv(full_fname)
        if should_filter_bots:
            df = cu.remove_bots_by_threshold(df, bot_score_threshold)

        df["hashtags"] = df["t_text"].apply(extract_hash_tags)
        df.sort_values(by=['hashtags'], inplace=True)

        for cur_tag in hashtags_counter:
            print(f'{cu.get_cur_formatted_time()} Tag {cur_tag}')
            if is_quota_met_for_tag(tag_to_tweets[cur_tag], tag_to_num_tweets_required[cur_tag]):
                print(f'{cu.get_cur_formatted_time()} Tag quota reached')
                continue

            if cur_tag == no_pure_stance_tags_key:
                df["is_tag_present"] = df['hashtags'].apply(
                    lambda l: len(ALL_TAGS.intersection(set(l))) == 0 and len(l) > 0)
                df_for_tag = df[df["is_tag_present"]]
                print(f'{cu.get_cur_formatted_time()} Found {len(df_for_tag.index)} tweets containing no pure-stance tags')
            elif cur_tag == no_tags_at_all_key:
                df["is_tag_present"] = df['hashtags'].apply(
                    lambda l: len(ALL_TAGS.intersection(set(l))) == 0 and len(l) == 0)
                df_for_tag = df[df["is_tag_present"]]
                print(f'{cu.get_cur_formatted_time()} Found {len(df_for_tag.index)} tweets containing no tags at all')
            else:
                df["is_tag_present"] = df['hashtags'].apply(lambda l: cur_tag in l)
                df_for_tag = df[df["is_tag_present"]]
                print(f'{cu.get_cur_formatted_time()} Found {len(df_for_tag.index)} tweets containing tag')

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
            quotas_not_met.append(
                f'{cur_tag}: {0 if tag_to_tweets[cur_tag] is None else len(tag_to_tweets[cur_tag].index)}/{tag_to_num_tweets_required[cur_tag]}')

    if len(quotas_not_met) > 0:
        print(f'Tags not found in enough tweets: {quotas_not_met.join(", ")}')

    return tag_to_tweets

def choose_least_common_tag(counter, tag_list):
    if len(tag_list) == 0:
        return []
    tag_counter_tuple = get_only_specific_keys_from_counter(counter, keys=tag_list)
    tag_counter_tuple = tag_counter_tuple.most_common()[-1]
    if len(tag_counter_tuple) == 0:
        return []
    return [tag_counter_tuple[0]]

if __name__ == "__main__":
    print(f'{cu.get_cur_formatted_time()} Start')
    create_hashtags_histograms()
    tag_to_tweets_df = create_tweets_with_pure_stance_tags()
    for tag in tag_to_tweets_df:
        cu.df_to_csv_plus_create_dir(tag_to_tweets_df[tag], os.path.join(cu.PLOTS_DATA_FOLDER, 'hashtag_tweets'),
                                  f"hashtag_{tag}_tweets.csv")

    write_tags_to_tweets_least_common_arbitrator(dir_to_check_existing=os.path.join(cu.PLOTS_DATA_FOLDER, 'least_common_hashtag_tweets'))

    '''
    TODO - create a tree-like structure that orders (at least some of) the taga in a way that makes semantic sense in
    that a node is more "granularity-fined" then it's parent.
    E.g.:
     - "ukip-manchester" will be a descendant of "ukip"
     - All of the following will be same-level siblings: 'yes2eu', 'yestoeu', 'yes2europe', 'yestoeurope' 
     
    tag_to_tweets_df = create_tweets_with_pure_stance_tags(max_tweets_per_tag=None, tweets_arbitrator=choose_most_accurate_tag)
    for tag in tag_to_tweets_df:
        cu.df_to_csv_plus_create_dir(tag_to_tweets_df[tag], os.path.join(cu.PLOTS_DATA_FOLDER, 'most_accurate_hashtag_tweets'),
                                  f"hashtag_{tag}_tweets.csv")
    '''

    print(f'{cu.get_cur_formatted_time()} FIN')
