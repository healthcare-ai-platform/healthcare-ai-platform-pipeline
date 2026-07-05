import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topics — must match what the backend publishes to
TOPIC_REPORT_RECEIVED = "healthai.report.received"

# Consumer group — one group per pipeline service so offsets are tracked independently
CONSUMER_GROUP = "pipeline-ingestion"

# How many messages to buffer before writing to S3 / warehouse
BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "100"))

# Max seconds to wait before flushing even if BATCH_SIZE not reached
BATCH_TIMEOUT_SECONDS = int(os.getenv("KAFKA_BATCH_TIMEOUT", "60"))
