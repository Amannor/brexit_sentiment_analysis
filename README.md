# brexit_sentiment_analysis
Playing with tweets regarding Brexit.

 - I took the [Harvard Dataverst Twitter dataset about brexit](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KP4XRP) that's annotated with sentiments and stances towards the Brexit.
 - I later used Twitter's API to fetch additional fields of each tweet (posting date and text).
 - Finally, I did several attempts at showing temporal trends (using aggregations of the sentiments) along with other possible insights. These can be seen in the "plots" folder


## The code files:


### Part 0 - Getting and organizing the data

1) twitter_data_fetcher.py (aka twitter_fetcher.py). Writes data the tweets's date and text to out folders: 'out', 'out2', 'out3', 'out4'
2) final_report_generator.py (function final_report_data_generator()) - Aggregates the added columns from prevoious point + the original data (in the folder 'dataverse_files') to one series of csv files (in folder 'final_report\data')

### Part 1 - Doing stuff with the data
 - final_report_generator.py (function final_report_plot_generator()) - Plots several plots based on data from the folder 'final_report\data'. Outputs to 'plots' folder
 - hashtags_analysis.py - Various anaylsis, processing and statistics on hashtags in the tweets


**In the plots folder:
 - The 'images' foler holds the pictures
 - 'data_for_plots' holds the data the plots are generated from**
