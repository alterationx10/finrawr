import json
import os
import time
import pika
import redis

if __name__ == '__main__':

    SUBREDDIT = os.getenv('SUBREDDIT')
    RMQ_HOST = os.getenv('RMQ_HOST')
    REDIS_HOST = os.getenv('REDIS_HOST')

    # One day, in seconds
    one_day = 86400

    while True:
        try:
            print('Connecting to Redis...')
            r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
            r.ping()

            print('Connecting to RMQ...')
            connection = pika.BlockingConnection(pika.ConnectionParameters(RMQ_HOST))
            channel = connection.channel()
            queue_out = f'{SUBREDDIT}-parsed'
            channel.queue_declare(queue=queue_out, durable=True)
            channel.basic_qos(prefetch_count=1)


            def callback(ch, method, properties, body):
                msg = json.loads(body)
                ttl = int(msg['created']) + one_day
                comment_id = msg['id']
                # Cache the whole object
                msg_key = f'{SUBREDDIT}:{comment_id}'
                for k, v in msg.items():
                    r.hset(msg_key, k, str(v))
                r.expireat(msg_key, ttl)
                # Cache data for calc
                tickers = msg['tickers']
                data_key = f'{SUBREDDIT}:data:{comment_id}'
                for ticker in tickers:
                    r.hset(data_key, 'ticker', ticker)
                    r.hset(data_key, 'sentiment', msg['sentiment'])
                r.expireat(data_key, ttl)
                ch.basic_ack(delivery_tag=method.delivery_tag)


            channel.basic_consume(queue=queue_out, on_message_callback=callback)
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
        except redis.exceptions.RedisError as err:
            print("Redis Error, retying in 3...")
            print("Caught a redis error: {}, stopping...".format(err))
            time.sleep(3)
            continue
