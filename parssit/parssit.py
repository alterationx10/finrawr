import csv
import json
import os
import time
import pika
import redis
from typing import List
from nltk import tokenize
from nltk.sentiment import SentimentIntensityAnalyzer


def parse_tickers(r, comment):
    stop_tickers: List[str] = ['FOR', 'CAN', 'REAL', 'ON', 'ARE', 'LOVE', 'IT', 'SEE', 'GO', 'BE', 'AT', 'OPEN', 'CEO',
                               'DD', 'BIG', 'ALL', 'ONE', 'NOW', 'TV', 'PSA', 'RH']
    tickers = []
    tokens = tokenize.word_tokenize(comment)
    # Remove duplicates
    tokens = set(tokens)
    # Keep only upper case
    tokens = [t for t in tokens if t.isupper()]
    # Remove single letters...
    tokens = [t for t in tokens if not len(t) == 1]
    # Remove some common words that are also tickers
    tokens = [t for t in tokens if t not in stop_tickers]
    # Only feed in stocks we know about
    for token in tokens:
        if token[0] == '$':
            if r.exists(token[1:]) == 1:
                tickers.append(token[1:])
        else:
            if r.exists(token) == 1:
                tickers.append(token)
    return tickers


def analyze_comment(comment):
    return sia.polarity_scores(comment)['compound']


if __name__ == '__main__':

    SUBREDDIT = os.getenv('SUBREDDIT')
    RMQ_HOST = os.getenv('RMQ_HOST')
    REDIS_HOST = os.getenv('REDIS_HOST')

    sia = SentimentIntensityAnalyzer()

    while True:
        try:
            print('Connecting to Redis...')
            r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
            r.ping()
            if str(r.get('tickers')) != '1':
                print('Loading stock tickers...')
                n_tickers = 0
                reader = csv.reader(open('resources/stocks.csv'))
                for row in reader:
                    n_tickers += 1
                    r.hset(f'{row[0]}', 'name', row[1])
                    r.hset(f'{row[0]}', 'sector', row[9])
                    r.hset(f'{row[0]}', 'industry', row[10])
                r.set('tickers', '1')
                print(f'Imported {n_tickers} tickers')
            else:
                print('Tickers already imported...')

            print('Connecting to RMQ...')
            connection = pika.BlockingConnection(pika.ConnectionParameters(RMQ_HOST))
            channel = connection.channel()
            # Set up in/out queues
            queue_in = f'{SUBREDDIT}-comments'
            queue_out = f'{SUBREDDIT}-parsed'
            # set up our channels
            channel.queue_declare(queue=queue_in, durable=True)
            channel.queue_declare(queue=queue_out, durable=True)
            # Don't get greedy
            channel.basic_qos(prefetch_count=1)


            def callback(ch, method, properties, body):
                msg = json.loads(body)
                msg['tickers'] = parse_tickers(r, msg['body'])
                msg['nTickers'] = len(msg['tickers'])
                if msg['nTickers'] > 0:
                    msg['sentiment'] = analyze_comment(msg['body'])
                    channel.basic_publish(exchange='',
                                          routing_key=queue_out,
                                          body=json.dumps(msg),
                                          properties=pika.BasicProperties(
                                              delivery_mode=2,  # make message persistent
                                          ))
                ch.basic_ack(delivery_tag=method.delivery_tag)


            channel.basic_consume(queue=queue_in, on_message_callback=callback)
            print('Consuming from RMQ...')
            channel.start_consuming()

        # Maybe we just restarted rmq...
        except pika.exceptions.ConnectionClosedByBroker:
            print('ConnectionClosedByBroker... retry in 5...')
            time.sleep(5)
            continue
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            print("Caught a channel error: {}, stopping...".format(err))
            break
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            print("Connection was closed, retrying in 3...")
            time.sleep(3)
            continue
        except redis.ConnectionError:
            print("Redis ConnectionError, retying in 3...")
            time.sleep(3)
            continue
        except redis.exceptions.RedisError:
            print("Redis Error, retying in 3...")
            time.sleep(3)
            continue
