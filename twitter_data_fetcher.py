from common_utiles import *
from credentials import bearer_token

def main():
    request_tweets_ids_from_csv(f"tweets_stance_sentiment_1outof4.csv", bearer_token, f'{BASE_OUT_DIR}', False, skip_first_line=True)
    for i in range(2, 5):
        request_tweets_ids_from_csv(f"tweets_stance_sentiment_{i}outof4.csv", bearer_token, f'{BASE_OUT_DIR}2', False)

if __name__ == "__main__":
    print(f'{get_cur_formatted_time()} Start')
    main()
    print(f'{get_cur_formatted_time()} FIN')