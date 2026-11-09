"""Generador de sensores simulados; un evento por segundo por defecto."""
import argparse
import json
import random
import time
from kafka import KafkaProducer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--events", type=int, default=0, help="0: hasta Ctrl+C")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--seed", type=int, default=28)
    args = ap.parse_args()
    rng = random.Random(args.seed)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap,
        acks="all", retries=5,
        value_serializer=lambda obj: json.dumps(obj).encode("utf-8"))
    sent = 0
    try:
        while args.events == 0 or sent < args.events:
            event = {"sensor_id": rng.randint(1, 10),
                "temperature": round(rng.uniform(20, 30), 2),
                "humidity": round(rng.uniform(30, 70), 2),
                "timestamp": int(time.time())}
            metadata = producer.send("sensor_data", value=event).get(timeout=20)
            print(json.dumps({"event": event, "partition": metadata.partition,
                              "offset": metadata.offset}), flush=True)
            sent += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(timeout=20)
        producer.close(timeout=20)


if __name__ == "__main__":
    main()
