import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topics — must match what the backend publishes to
TOPIC_REPORT_RECEIVED = "healthai.report.received"

# Dead-letter topic — a message that still fails after MAX_PROCESSING_RETRIES
# lands here instead of blocking its partition or being silently dropped.
TOPIC_REPORT_RECEIVED_DLQ = f"{TOPIC_REPORT_RECEIVED}.dlq"

# Consumer group — one group per pipeline service so offsets are tracked independently
CONSUMER_GROUP = "pipeline-ingestion"

# Redeliveries allowed before a message is routed to the DLQ. Kept low by default
# since a PDF retry re-runs the paid Anthropic OCR call.
MAX_PROCESSING_RETRIES = int(os.getenv("KAFKA_MAX_RETRIES", "1"))

# How many messages to buffer before writing to S3 / warehouse
BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "100"))

# Max seconds to wait before flushing even if BATCH_SIZE not reached
BATCH_TIMEOUT_SECONDS = int(os.getenv("KAFKA_BATCH_TIMEOUT", "60"))
