"""Stream weekly stock prices from Yahoo Finance into a Kafka topic.

Usage:
    pip install -r requirements.txt
    # start Kafka locally (see README), then:
    python kafka_producer.py --bootstrap localhost:9092 --topic yfData
The KafkaConsumer.ipynb notebook reads from the same topic.
"""
import argparse
from json import dumps

import yfinance as yf
from kafka import KafkaProducer

STOCKS = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "TSLA", "BABA", "JNJ", "V", "JPM", "PG", "KO", "INTC",
    "BAC", "DIS", "IBM", "GE", "CSCO", "PFE", "XOM", "CVX", "CCEP", "VZ", "PEP", "T", "NFLX",
    "GS", "HON", "ORCL", "MMM", "MRK", "HD", "CAT", "MCD", "SBUX", "NVDA", "BA", "ACN",
    "ADBE", "MA", "GM", "F",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="yfData")
    parser.add_argument("--start", default="2023-11-30")
    parser.add_argument("--end", default="2023-12-31")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap],
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )
    for stock in STOCKS:
        data = yf.download(stock, start=args.start, end=args.end, interval="1wk", progress=False)
        producer.send(args.topic, value={"stocks": stock, "StockData": data.to_json()})
        print(f"sent {stock}: {len(data)} rows")
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
