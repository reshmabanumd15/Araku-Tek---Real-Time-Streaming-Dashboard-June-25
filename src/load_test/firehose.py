import argparse, json, time, uuid, random, threading
from datetime import datetime, timezone
from kafka import KafkaProducer

def produce(producer, topic, tps, make_event):
    while True:
        start = time.time()
        for _ in range(tps):
            producer.send(topic, make_event())
        producer.flush()
        time.sleep(max(0, 1-(time.time()-start)))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--bootstrap', default='kafka:9092')
    ap.add_argument('--orders-tps', type=int, default=500)
    ap.add_argument('--clicks-tps', type=int, default=1000)
    ap.add_argument('--threads', type=int, default=4)
    args = ap.parse_args()
    prod = KafkaProducer(bootstrap_servers=args.bootstrap, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def evt_order():
        return {
            'event_id': str(uuid.uuid4()),
            'customer_id': random.randint(1, 50000),
            'order_id': str(uuid.uuid4()),
            'amount': round(random.uniform(5, 500), 2),
            'currency': 'USD',
            'channel': random.choice(['web','ios','android','store']),
            'event_ts': datetime.now(timezone.utc).isoformat()
        }

    def evt_click():
        return {
            'event_id': str(uuid.uuid4()),
            'customer_id': random.randint(1, 50000),
            'page': random.choice(['/home','/product','/cart','/checkout','/search','/help']),
            'referrer': random.choice(['direct','ads','email','social']),
            'device': random.choice(['desktop','mobile','tablet']),
            'event_ts': datetime.now(timezone.utc).isoformat()
        }

    threads = []
    for _ in range(args.threads):
        threads.append(threading.Thread(target=produce, args=(prod,'orders',args.orders_tps//args.threads,evt_order)))
        threads.append(threading.Thread(target=produce, args=(prod,'clicks',args.clicks_tps//args.threads,evt_click)))
    [t.start() for t in threads]
    [t.join() for t in threads]

if __name__ == '__main__':
    main()
