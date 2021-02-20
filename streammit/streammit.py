import json
import os
import time
import pika
import praw

if __name__ == '__main__':

    REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
    REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
    REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')
    SUBREDDIT = os.getenv('SUBREDDIT')
    RMQ_HOST = os.getenv('RMQ_HOST')

    while True:
        try:
            print('Connecting to RMQ...')
            connection = pika.BlockingConnection(pika.ConnectionParameters(RMQ_HOST))
            channel = connection.channel()

            queue_name = f'{SUBREDDIT}-comments'

            channel.queue_declare(queue=queue_name, durable=True)

            print('Subscribing to Reddit Comment stream')
            reddit = praw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent=REDDIT_USER_AGENT
            )
            reddit.read_only = True

            for comment in reddit.subreddit(SUBREDDIT).stream.comments():
                author = 'unknown'
                if comment.author is not None:
                    if comment.author.name is not None:
                        author = comment.author.name
                msg = {
                    'id': comment.id,
                    'body': comment.body,
                    'created': comment.created_utc,
                    'author': author,
                    'score': comment.score,
                    'submissionId': comment.submission.id
                }

                channel.basic_publish(exchange='',
                                      routing_key=queue_name,
                                      body=json.dumps(msg),
                                      properties=pika.BasicProperties(
                                          delivery_mode=2,  # make message persistent
                                      ))

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
