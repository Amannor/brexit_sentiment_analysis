from common_utiles import *
from credentials import bearer_token4

def main():
    request_tweets_ids_from_csv("tweets_stance_sentiment_4outof4.csv", bearer_token4, f'{BASE_OUT_DIR}4', False)

if __name__ == "__main__":
    print(f'{get_cur_formatted_time()} Start')
    main()
    print(f'{get_cur_formatted_time()} FIN')