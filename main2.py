from common_utiles import *
from credentials import bearer_token2

def main():
    request_tweets_ids_from_csv("tweets_stance_sentiment_2outof4.csv", bearer_token2, f'{BASE_OUT_DIR}2', False)

if __name__ == "__main__":
    print(f'{get_cur_formatted_time()} Start')
    main()
    print(f'{get_cur_formatted_time()} FIN')