import os
from pprint import pprint

import redis


def sort_dict(_dict):
    _data = [(k, v) for k, v in _dict.items()]
    _data.sort(key=lambda tup: tup[1], reverse=True)
    _sorted_dict = {}
    for _t in _data:
        _sorted_dict[_t[0]] = format(_t[1], '.4f')
    return _sorted_dict


if __name__ == '__main__':
    REDIS_HOST = os.getenv('REDIS_HOST')
    SUBREDDIT = os.getenv('SUBREDDIT')

    REDIS_HOST = 'localhost'
    SUBREDDIT = 'wallstreetbets'

    print('Connecting to Redis...')
    r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
    r.ping()

    keys = r.keys(f'{SUBREDDIT}:data:*')
    results = {}
    for key in keys:
        ticker = r.hget(key, 'ticker')
        sentiment = r.hget(key, 'sentiment')
        if ticker in results:
            results[ticker] = results[ticker] + float(sentiment)
        else:
            results[ticker] = float(sentiment)
    results = sort_dict(results)
    print(results)
