from twitter_scraper import get_tweets

tweets = get_tweets('maxhumber', pages=1)

for tweet in tweets:
    print(tweet['text'])

list(tweets)

for tweet in get_tweets('maxhumber', pages=1):
    print(tweet['text'])
