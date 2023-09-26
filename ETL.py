import snscrape.modules.twitter as sntwitter
import pandas as pd
from transform import transform_data
from airflow.hooks.postgres_hook import PostgresHook

# Creating list to append tweet data to
def extract_data():
  
    # scrape tweets and append to a list
  for i,tweet in enumerate(sntwitter.TwitterSearchScraper('Chatham House since:2023-01-14').get_items()):
    if i>1000:
      break
    tweets_list.append([tweet.date, tweet.user.username, tweet.rawContent, 
                          tweet.sourceLabel,tweet.user.location
                          ])
  
      # convert tweets into a dataframe
  tweets_df = pd.DataFrame(tweets_list, columns=['datetime', 'username', 'text', 'source', 'location'])

      # save tweets as csv file
  
  transform_data(tweets_df)



# Load clean data into postgres database
def task_data_upload(data):
  print(data.head() )
  
  data = data.to_csv(index=None, header=None)
  
  postgres_sql_upload = PostgresHook(postgres_conn_id="postgres_connection")
  postgres_sql_upload.bulk_load('twitter_etl_table', data)
  
  return True
  
## perform data cleaning and transformation
def transform_data(tweets_df):
  print(tweets_df.info() )
	### Transformation happens here	
  
  # load transformed data into database
  task_data_upload(tweets_df)


  
