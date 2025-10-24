import json, time
from kafka.admin import KafkaAdminClient, NewTopic
import argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--bootstrap', default='kafka:9092')
    ap.add_argument('--config', default='configs/topics.json')
    args = ap.parse_args()
    cfg = json.load(open(args.config))
    admin = KafkaAdminClient(bootstrap_servers=args.bootstrap)
    topics = [NewTopic(t['name'], num_partitions=t['partitions'], replication_factor=t['replication']) for t in cfg['topics']]
    try:
        admin.create_topics(new_topics=topics, validate_only=False)
        print('Topics created.')
    except Exception as e:
        print('Create topics result:', e)

if __name__ == '__main__':
    main()
