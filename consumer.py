from kafka import KafkaConsumer
import umsgpack
import sys
import json
from datetime import datetime

consumer = KafkaConsumer('tweets', bootstrap_servers=['localhost:9092'], value_deserializer=umsgpack.unpackb,
                         consumer_timeout_ms=1000, auto_offset_reset='earliest')

values = []
for message in consumer:
    # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    #                                       message.offset, message.key,
    #                                       message.value))
    values.append(message.value)


#1
def all_accounts(values):
    accounts = set()

    for val in values:
        accounts.add(val['user'])

    accounts = {'accounts': list(accounts)}

    return accounts


# print(all_accounts(values))


#2
def latest_tweets_top_accounts(values):
    accounts_tweets = {}
    datetime_now = datetime.now()

    for val in reversed(values):
        if (datetime_now - datetime.strptime(val['date'], '%d-%m-%Y %H:%M:%S')).total_seconds() > 10800:
            break
        if val['user'] not in accounts_tweets:
            accounts_tweets[val['user']] = [{'id': val['id'], 'date': val['date'], 'text': val['text']}]
        else:
            accounts_tweets[val['user']].append({'id': val['id'], 'date': val['date'], 'text': val['text']})

    top_accounts = sorted(accounts_tweets.items(), key=lambda x: len(x[1]), reverse=True)[:10]

    top_tweets_dict = {}
    for acc in top_accounts:
        top_tweets_dict[acc[0]] = acc[1][:10]

    return top_tweets_dict


# print(latest_tweets_top_accounts(values))


#3
def hourly_statistics(values):
    tweets_each_hour = {'first_hour': 0, 'second_hour': 0, 'third_hour': 0}
    datetime_now = datetime.now()

    for val in reversed(values):
        if (datetime_now - datetime.strptime(val['date'], '%d-%m-%Y %H:%M:%S')).total_seconds() > 10800:
            break

        if (datetime_now - datetime.strptime(val['date'], '%d-%m-%Y %H:%M:%S')).total_seconds() < 3600:
            tweets_each_hour['first_hour'] += 1
        elif 3600 <= (datetime_now - datetime.strptime(val['date'], '%d-%m-%Y %H:%M:%S')).total_seconds() < 7200:
            tweets_each_hour['second_hour'] += 1
        elif 7200 <= (datetime_now - datetime.strptime(val['date'], '%d-%m-%Y %H:%M:%S')).total_seconds() < 10800:
            tweets_each_hour['third_hour'] += 1

    return tweets_each_hour


# print(hourly_statistics(values))


#4
def top_accounts(values, hours):
    accounts_post_count = {}
    datetime_now = datetime.now()

    for val in reversed(values):
        if (datetime_now - datetime.strptime(val['date'], '%d-%m-%Y %H:%M:%S')).total_seconds() > hours * 3600:
            break
        if val['user'] not in accounts_post_count:
            accounts_post_count[val['user']] = 1
        else:
            accounts_post_count[val['user']] += 1

    top_accounts = sorted(accounts_post_count.items(), key=lambda x: x[1], reverse=True)[:20]
    # print(top_accounts)
    top_accounts = {'top_accounts': [i[0] for i in top_accounts]}

    return top_accounts


# print(top_accounts(values, 1))


#5
def top_hashtags(values, hours):
    hashtags_count = {}
    datetime_now = datetime.now()

    for val in reversed(values):
        if (datetime_now - datetime.strptime(val['date'], '%d-%m-%Y %H:%M:%S')).total_seconds() > hours * 3600:
            break

        hashtags = [ht for ht in val['text'].split() if ht.startswith('#')]
        
        for ht in hashtags:
            if ht not in hashtags_count:
                hashtags_count[ht] = 1
            else:
                hashtags_count[ht] += 1

    top_hts = sorted(hashtags_count.items(), key=lambda x: x[1], reverse=True)[:20]
    # print(top_hts)
    top_hts = {'top_hashtags': [i[0] for i in top_hts]}

    return top_hts


# print(top_hashtags(values, 1))


if int(sys.argv[1]) == 1:
    result = all_accounts(values)
elif int(sys.argv[1]) == 2:
    result = latest_tweets_top_accounts(values)
elif int(sys.argv[1]) == 3:
    result = hourly_statistics(values)
elif int(sys.argv[1]) == 4:
    result = top_accounts(values, int(sys.argv[2]))
elif int(sys.argv[1]) == 5:
    result = top_hashtags(values, int(sys.argv[2]))

with open('{}.json'.format(sys.argv[1]), 'w+') as f:
    json.dump(result, f)
