import argparse
import json
import time
from pathlib import Path
from typing import List, Dict, Any

import requests
import yaml
from confluent_kafka import Producer

# ===== Kafka config =====
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "tweets"

# ===== Paths =====
ROOT_DIR = Path(__file__).resolve().parents[1]
SECRETS_PATH = ROOT_DIR / "config" / "twitter_secrets.yml"


# ===== Helpers =====

def read_file_lines(path: Path):
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def load_twitter_secrets() -> Dict[str, Any]:
    with SECRETS_PATH.open() as f:
        data = yaml.safe_load(f) or {}
    # Support either {"twitter": {...}} or flat keys
    if "twitter" in data:
        return data["twitter"]
    return data


def make_kafka_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    }
    return Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f" Produced record to {msg.topic()} "
            f"partition [{msg.partition()}] @ offset {msg.offset()}"
        )


# ===== Twitter API (search/recent) =====

TWITTER_SEARCH_RECENT_URL = "https://api.twitter.com/2/tweets/search/recent"


def fetch_recent_tweets(bearer_token: str, query: str, max_results: int = 10) -> List[Dict[str, Any]]:
    """
    Call GET /2/tweets/search/recent once.
    On free tier we must be extremely conservative: one request, up to 10 tweets.
    """
    headers = {
        "Authorization": f"Bearer {bearer_token}",
    }
    params = {
        "query": query,
        "max_results": max_results,
        "tweet.fields": "created_at,author_id,entities,referenced_tweets",
    }
    print(f"Calling Twitter search/recent with query: {query!r}")
    resp = requests.get(TWITTER_SEARCH_RECENT_URL, headers=headers, params=params, timeout=10)
    if resp.status_code != 200:
        print(f"Twitter API error {resp.status_code}: {resp.text}")
        return []
    data = resp.json()
    return data.get("data", [])


def normalize_tweet_for_pipeline(raw_tweet: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Twitter API tweet JSON into the schema expected by stream_job.get_tweet_schema():
      id: string
      created_at: string
      text: string
      author_id: string
      in_reply_to_user_id: string or null
      referenced_tweets: array of {type, id}
      mentions: array of usernames (strings)
      raw_entities: map<string, array<string>>
      source: string (we'll use 'twitter_api')
    """
    entities = raw_tweet.get("entities", {}) or {}
    mentions = [m.get("username", "") for m in entities.get("mentions", []) if m.get("username")]
    hashtags = [h.get("tag", "") for h in entities.get("hashtags", []) if h.get("tag")]
    urls = [u.get("expanded_url", "") for u in entities.get("urls", []) if u.get("expanded_url")]

    normalized = {
        "id": raw_tweet.get("id"),
        "created_at": raw_tweet.get("created_at"),
        "text": raw_tweet.get("text"),
        "author_id": raw_tweet.get("author_id"),
        "in_reply_to_user_id": None,
        "referenced_tweets": raw_tweet.get("referenced_tweets", []) or [],
        "mentions": mentions,
        "raw_entities": {
            "hashtags": hashtags,
            "urls": urls,
        },
        "source": "twitter_api",
    }
    return normalized


# ===== Producers for each source =====

def produce_from_file(producer: Producer, file_path: Path, interval: float):
    print(f"Streaming from {file_path} to Kafka topic '{KAFKA_TOPIC}'...")
    for record in read_file_lines(file_path):
        # Assume record already matches normalized schema
        payload = json.dumps(record).encode("utf-8")
        producer.produce(
            KAFKA_TOPIC,
            value=payload,
            callback=delivery_report
        )
        producer.poll(0)
        if interval > 0:
            time.sleep(interval)
    producer.flush()
    print("Finished streaming from file.")


def produce_from_twitter(producer: Producer, query: str, max_results: int = 10):
    secrets = load_twitter_secrets()
    bearer = secrets.get("bearer_token")
    if not bearer:
        print("No bearer_token in twitter_secrets.yml; cannot call Twitter API.")
        return

    tweets = fetch_recent_tweets(bearer, query=query, max_results=max_results)
    print(f"Fetched {len(tweets)} tweets from Twitter API.")
    for t in tweets:
        normalized = normalize_tweet_for_pipeline(t)
        payload = json.dumps(normalized).encode("utf-8")
        producer.produce(
            KAFKA_TOPIC,
            value=payload,
            callback=delivery_report
        )
        producer.poll(0)
    producer.flush()
    print("Finished sending live Twitter tweets to Kafka.")


# ===== CLI =====

def parse_args():
    parser = argparse.ArgumentParser(description="Kafka tweet producer")
    parser.add_argument(
        "--source",
        choices=["file", "twitter"],
        default="file",
        help="Where to read tweets from"
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Path to JSONL file (for --source file)"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Sleep interval between messages in file mode"
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Twitter search query (for --source twitter)"
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=10,
        help="Max tweets to fetch in twitter mode (free tier: keep <= 10)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    producer = make_kafka_producer()

    if args.source == "file":
        if not args.file:
            raise SystemExit("You must pass --file in file mode.")
        file_path = Path(args.file)
        if not file_path.exists():
            raise SystemExit(f"File not found: {file_path}")
        produce_from_file(producer, file_path, args.interval)

    elif args.source == "twitter":
        if not args.query:
            # Simple default: you can edit this to match your entities.yml handles
            default_query = "taylorswift13 OR elonmusk OR rihanna"
            print(f"No --query provided, using default: {default_query!r}")
            query = default_query
        else:
            query = args.query
        produce_from_twitter(producer, query=query, max_results=args.max_results)


if __name__ == "__main__":
    main()
