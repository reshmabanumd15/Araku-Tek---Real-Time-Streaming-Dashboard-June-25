import argparse, json, time, uuid, random
from datetime import datetime, timezone
from kafka import KafkaProducer

pages = ['/home','/product','/cart','/checkout','/search','/help']
referrers = ['direct','ads','email','social']
devices = ['desktop','mobile','tablet']

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--bootstrap', default='kafka:9092')
    ap.add_argument('--topic', default='clicks')
    ap.add_argument('--tps', type=int, default=400)
    ap.add_argument('--customers', type=int, default=50000)
    args = ap.parse_args()
    prod = KafkaProducer(bootstrap_servers=args.bootstrap, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print('Producing to', args.topic)
    while True:
        start = time.time()
        for _ in range(args.tps):
            evt = {
                'event_id': str(uuid.uuid4()),
                'customer_id': random.randint(1, args.customers),
                'page': random.choice(pages),
                'referrer': random.choice(referrers),
                'device': random.choice(devices),
                'event_ts': datetime.now(timezone.utc).isoformat()
            }
            prod.send(args.topic, evt)
        prod.flush()
        elapsed = time.time()-start
        time.sleep(max(0, 1 - elapsed))

if __name__ == '__main__':
    main()
