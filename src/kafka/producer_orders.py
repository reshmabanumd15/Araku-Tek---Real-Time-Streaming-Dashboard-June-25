import argparse, json, time, uuid, random
from datetime import datetime, timezone
from kafka import KafkaProducer

channels = ['web','ios','android','store']
currencies = ['USD','EUR','GBP','CAD','AUD']

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--bootstrap', default='kafka:9092')
    ap.add_argument('--topic', default='orders')
    ap.add_argument('--tps', type=int, default=200)
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
                'order_id': str(uuid.uuid4()),
                'amount': round(random.uniform(5, 500), 2),
                'currency': random.choice(currencies),
                'channel': random.choice(channels),
                'event_ts': datetime.now(timezone.utc).isoformat()
            }
            prod.send(args.topic, evt)
        prod.flush()
        elapsed = time.time()-start
        sleep = max(0, 1 - elapsed)
        time.sleep(sleep)

if __name__ == '__main__':
    main()
