"""
This scripts constantly listens to the same topic on Kafka and writes them in micro batches to s3.
"""

import json
import os
from kafka import KafkaConsumer
from datetime import datetime
import boto3
from dotenv import load_dotenv

load_dotenv()

# ---------- Configuration ----------
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "spotify-s3-consumer")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10)) 


# ---------- Connect to AWS S3 ----------

s3 = boto3.client(
    "s3",
    region_name=AWS_DEFAULT_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)


try:
    s3.head_bucket(Bucket=S3_BUCKET_NAME)
    print(f"Bucket {S3_BUCKET_NAME} already exists.")

except Exception:
    if AWS_DEFAULT_REGION == "us-east-1":
        s3.create_bucket(Bucket=S3_BUCKET_NAME)
    else:
        s3.create_bucket(
            Bucket=S3_BUCKET_NAME,
            CreateBucketConfiguration={'LocationConstraint': AWS_DEFAULT_REGION}
        )
    print(f"Bucket {S3_BUCKET_NAME} created.")



# ---------- Kafka Consumer ----------
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    auto_offset_reset="earliest",
    group_id=KAFKA_GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)


print(f"Listening for events on Kafka topic: {KAFKA_TOPIC} ...")


batch = []

for message in consumer:
    event = message.value
    batch.append(event)

    if len(batch) >= BATCH_SIZE:
        now = datetime.utcnow()
        date_path = now.strftime("date=%Y-%m-%d/hour=%H")
        file_name = f"spotify_events_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        file_path = f"bronze/{date_path}/{file_name}"

        json_data = "\n".join([json.dumps(e) for e in batch])

        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_path,
            Body=json_data.encode("utf-8")
        )

        print(f"Wrote {len(batch)} events to s3://{S3_BUCKET_NAME}/{file_path}")

        batch = []