import asyncio
import json

import click
from gcloud.aio.pubsub import SubscriberClient, PublisherClient, PubsubMessage


async def publish_messages(publisher_client: PublisherClient, topic_path, messages):
    publish_result = await publisher_client.publish(topic_path, messages)
    return publish_result


async def process_messages(
        subscriber_client, publisher_client, subscription_path, target_topic_path,
        cont=False, purge=False, msg_type=None, msg_type_exclude=None,
        connection=None, system_id=None, gundi_id=None, source_id=None, batch_size=100
):
    if purge and input(f"Using --purge may cause data loss, are you sure? [y/n]: ").strip().lower() != 'y':
        print("Exiting..")
        exit(0)
    # Pulls messages from a given subscription, publishes them to another topic, and acknowledges them.
    total_messages_acknowledged = 0
    total_messages_processed = 0
    while True:
        print(f"Pulling messages from {subscription_path}...")
        received_messages = await subscriber_client.pull(subscription_path, max_messages=batch_size)
        filtered_messages = []
        ack_ids = []
        message_count = 0
        for received_message in received_messages:
            message_count += 1
            decoded_message = json.loads(received_message.data.decode("utf-8"))
            attributes = received_message.attributes
            connection_id = attributes.get("data_provider_id")
            msg_gundi_id = attributes.get("gundi_id")
            msg_source_id = attributes.get("source_id") or decoded_message.get("payload", {}).get("external_source_id")
            msg_system_event_id = decoded_message.get("event_id")
            message_event_type = decoded_message.get("event_type")
            print(f"Reprocessing message:{message_event_type} (gundi_id {msg_gundi_id}, system_id {msg_system_event_id}) - Connection {connection_id}")
            # Filter messages
            if system_id and system_id != msg_system_event_id:
                print(f"Message {message_event_type} (system_id {system_id}) excluded. Left in queue.")
                continue
            if gundi_id and msg_gundi_id != gundi_id:
                print(f"Message {message_event_type} (gundi_id {msg_gundi_id}) excluded. Left in queue.")
                continue
            if source_id and msg_source_id != source_id:
                print(f"Message {message_event_type} (source_id {msg_source_id}) excluded. Left in queue.")
                continue
            if connection and connection_id != connection:
                print(f"Message {message_event_type} (gundi_id {msg_gundi_id}) excluded. Left in queue.")
                continue
            if msg_type_exclude and message_event_type in msg_type_exclude:
                print(f"Message {message_event_type} (gundi_id {msg_gundi_id}) excluded. Left in queue.")
                continue
            if purge:
                ack_ids.append(received_message.ack_id)
                await subscriber_client.acknowledge(subscription_path, [received_message.ack_id])
                print(f"Message {message_event_type} (gundi_id {msg_gundi_id}) discarded.")
                continue
            if msg_type:
                if message_event_type in msg_type:
                    filtered_messages.append(received_message)
                else:
                    print(f"Message {message_event_type} (gundi_id {msg_gundi_id}) excluded. Left in queue.")
            else:
                filtered_messages.append(received_message)

        # Process the resultant messages
        if filtered_messages:
            # Re-publish a copy and then ACK the original messages
            new_messages = [
                PubsubMessage(data=original_message.data, **original_message.attributes)
                for original_message in filtered_messages
            ]
            await publish_messages(
                publisher_client=publisher_client,
                topic_path=target_topic_path,
                messages=new_messages
            )
            ack_ids.extend([m.ack_id for m in filtered_messages])
        if ack_ids:
            # Acknowledges the processed messages
            await subscriber_client.acknowledge(subscription_path, ack_ids)

        total_messages_acknowledged += len(ack_ids)
        total_messages_processed += message_count
        print(f"Total acknowledged/processed: ({total_messages_acknowledged}/{total_messages_processed}). This batch: ({len(ack_ids)}/{message_count})")

        # Ask the user if want to continue when not more messages are found, read user input [y/n]
        if len(ack_ids) == 0 and not cont and input(f"Continue? [y/n]: ").strip().lower() == 'n':
            print("Exiting..")
            exit(0)
        print("Continuing..")
        await asyncio.sleep(2)  # Pause to avoid hitting GCP API rate limits


async def main_async(
        from_sub, to_topic, project, cont=False, reprocess=True, purge=False, msg_type=None, msg_type_exclude=None,
        connection=None, system_id=None, gundi_id=None, source_id=None, batch_size=100
):
    subscription_path = f"projects/{project}/subscriptions/{from_sub}"
    target_topic_path = f"projects/{project}/topics/{to_topic}"
    while True:
        try:
            async with SubscriberClient() as subscriber_client, PublisherClient() as publisher_client:
                await process_messages(
                    subscriber_client=subscriber_client,
                    publisher_client=publisher_client,
                    subscription_path=subscription_path,
                    target_topic_path=target_topic_path,
                    cont=cont,
                    purge=purge,
                    msg_type=msg_type,
                    msg_type_exclude=msg_type_exclude,
                    connection=connection,
                    system_id=system_id,
                    gundi_id=gundi_id,
                    source_id=source_id,
                    batch_size=batch_size
                )
        except Exception as e:
            print(f"An error occurred: {type(e).__name__} {e}. Restarting..")


@click.command()
@click.option('--from-sub', required=True,  help="Subscription (id) to pull messages from")
@click.option('--to-topic', required=False, help="Topic (id) to publish messages to")
@click.option('--project', default='cdip-prod1-78ca', help="GCP Project ID")
@click.option('--continue', 'cont', is_flag=True, default=False, help="Continue processing messages until interrupted")
@click.option('--reprocess', is_flag=True, help="Reprocess messages from the source subscription")
@click.option('--purge', is_flag=True, default=False, help="Purge messages from the source subscription")
@click.option('--msg-type', multiple=True, help="Message types to include in reprocessing.")
@click.option('--msg-type-exclude', multiple=True, help="Message types to exclude from reprocessing.")
@click.option('--connection', help="Connection ID to filter messages by")
@click.option('--system-id', help="System Event ID to filter messages by")
@click.option('--gundi-id', help="Gundi ID to filter messages by")
@click.option('--source-id', help="Source ID to filter messages by")
@click.option('--batch-size', default=100, type=int, help="Number of messages to pull per batch iteration (default: 100)")
def main(
        from_sub, to_topic, project, cont, reprocess, purge,
        msg_type, msg_type_exclude, connection, system_id, gundi_id, source_id, batch_size
):
    if reprocess and purge:
        print("Cannot use --reprocess and --purge together")
        exit(1)
    if not reprocess and not purge:
        print("Must use either --reprocess or --purge")
        exit(1)
    if reprocess and not to_topic:
        print("Must provide a target topic with --reprocess")
        exit(1)
    asyncio.run(
        main_async(
            from_sub, to_topic, project, cont, reprocess, purge,
            msg_type, msg_type_exclude, connection, system_id, gundi_id, source_id, batch_size
        )
    )


if __name__ == '__main__':
    main()

