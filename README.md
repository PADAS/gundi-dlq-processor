# gundi-dlq-processor

A Python tool for processing messages from Google Cloud Pub/Sub dead letter queues (DLQs) in the Gundi system. This tool allows you to reprocess failed messages or purge them from the queue.

## What it does

The `gundi-dlq-processor` is designed to handle messages that have failed processing and ended up in dead letter queues. It provides two main operations:

1. **Reprocess**: Pull messages from a DLQ subscription and republish them to a target topic for reprocessing
2. **Purge**: Remove messages from a DLQ subscription without reprocessing them

The tool includes filtering capabilities to selectively process messages based on various criteria like message type, connection ID, system ID, gundi ID, and source ID.

## Features

- **Batch Processing**: Process messages in configurable batch sizes
- **Message Filtering**: Filter messages by multiple criteria
- **Safe Operations**: Confirmation prompts for destructive operations
- **Continuous Processing**: Option to continuously process messages until interrupted
- **Error Handling**: Automatic retry on errors
- **Detailed Logging**: Clear output showing which messages are processed or excluded

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd gundi-dlq-processor

# Install dependencies (if using Poetry)
poetry install

# Or install with pip
pip install -r requirements.txt
```

## Usage

### Basic Commands

```bash
# Reprocess messages from DLQ to a target topic
python gundi_dlq.py --from-sub <subscription-id> --reprocess --to-topic <topic-id>

# Purge messages from DLQ (with confirmation)
python gundi_dlq.py --from-sub <subscription-id> --purge

# Process with custom batch size
python gundi_dlq.py --from-sub <subscription-id> --reprocess --to-topic <topic-id> --batch-size 50
```

### Command Line Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--from-sub` | Yes | - | Subscription ID to pull messages from |
| `--to-topic` | No* | - | Topic ID to publish messages to (required with --reprocess) |
| `--project` | No | `cdip-prod1-78ca` | GCP Project ID |
| `--reprocess` | No* | False | Reprocess messages from the source subscription |
| `--purge` | No* | False | Purge messages from the source subscription |
| `--batch-size` | No | 100 | Number of messages to pull per batch iteration |
| `--continue` | No | False | Continue processing messages until interrupted |
| `--msg-type` | No | - | Message types to include in reprocessing (can specify multiple) |
| `--msg-type-exclude` | No | - | Message types to exclude from reprocessing (can specify multiple) |
| `--connection` | No | - | Connection ID to filter messages by |
| `--system-id` | No | - | System Event ID to filter messages by |
| `--gundi-id` | No | - | Gundi ID to filter messages by |
| `--source-id` | No | - | Source ID to filter messages by |

*Either `--reprocess` or `--purge` must be specified, but not both.

### Examples

#### Reprocess all messages from a DLQ
```bash
python gundi_dlq.py --from-sub my-dlq-subscription --reprocess --to-topic my-target-topic
```

#### Purge all messages from a DLQ
```bash
python gundi_dlq.py --from-sub my-dlq-subscription --purge
```

#### Reprocess only specific message types
```bash
python gundi_dlq.py --from-sub my-dlq-subscription --reprocess --to-topic my-target-topic --msg-type observation --msg-type alert
```

#### Exclude specific message types
```bash
python gundi_dlq.py --from-sub my-dlq-subscription --reprocess --to-topic my-target-topic --msg-type-exclude error --msg-type-exclude debug
```

#### Filter by connection ID
```bash
python gundi_dlq.py --from-sub my-dlq-subscription --reprocess --to-topic my-target-topic --connection connection-123
```

#### Filter by multiple criteria
```bash
python gundi_dlq.py --from-sub my-dlq-subscription --reprocess --to-topic my-target-topic --gundi-id gundi-456 --source-id source-789 --batch-size 50
```

#### Continuous processing
```bash
python gundi_dlq.py --from-sub my-dlq-subscription --reprocess --to-topic my-target-topic --continue
```

## Message Filtering

The tool supports filtering messages based on several criteria:

- **Message Type**: Include or exclude specific event types
- **Connection ID**: Filter by data provider connection
- **System ID**: Filter by system event ID
- **Gundi ID**: Filter by gundi identifier
- **Source ID**: Filter by external source ID

When filters are applied, messages that don't match the criteria are left in the queue and not processed.

## Safety Features

- **Purge Confirmation**: When using `--purge`, the tool prompts for confirmation before deleting messages
- **Batch Processing**: Messages are processed in configurable batches to avoid overwhelming the system
- **Error Recovery**: The tool automatically retries on errors and continues processing
- **Detailed Output**: Clear logging shows which messages are processed, excluded, or discarded

## Output Format

The tool provides detailed output showing:
- Number of messages pulled from the subscription
- Which messages are being processed or excluded
- Filtering criteria applied
- Final count of processed messages (e.g., "5/10 messages reprocessed")

## Dependencies

- `click`: Command line interface
- `gcloud.aio.pubsub`: Google Cloud Pub/Sub async client
- `asyncio`: Asynchronous programming support

## Authentication

The tool uses Google Cloud authentication. Ensure you have:
1. Google Cloud SDK installed and configured
2. Appropriate permissions to access the Pub/Sub resources
3. Application Default Credentials set up

```bash
# Set up authentication
gcloud auth application-default login
```

## Error Handling

The tool includes robust error handling:
- Automatic retry on connection errors
- Graceful handling of malformed messages
- Clear error messages for configuration issues
- Safe exit on user interruption

## Contributing

When contributing to this project:
1. Follow the existing code style
2. Add tests for new features
3. Update documentation for any new options
4. Ensure error handling is comprehensive
