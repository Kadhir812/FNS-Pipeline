from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer
import time

KAFKA_BROKER = 'localhost:29092'
TOPIC = 'Financenews-raw'

def clear_topic_data():
    """
    Clear topic data without destroying the container
    """
    try:
        # Method 1: Consume all messages to clear data
        print(f"🧹 Clearing data from topic: {TOPIC}")
        
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=10000,  # Stop after 10 seconds of no new messages
            group_id='cleanup_group'
        )
        
        message_count = 0
        try:
            for message in consumer:
                message_count += 1
                if message_count % 100 == 0:
                    print(f"Cleared {message_count} messages...")
        except Exception:
            pass  # Timeout is expected when no more messages
        
        consumer.close()
        print(f"✅ Cleared {message_count} messages from topic: {TOPIC}")
        print("🔄 Topic structure preserved, container safe")
        
    except Exception as e:
        print(f"❌ Error clearing topic data: {e}")

def safe_topic_reset():
    """
    Safely reset topic by recreating it (safer approach)
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            client_id='safe_reset_script'
        )
        
        print(f"� Safely resetting topic: {TOPIC}")
        
        # Delete the topic
        admin_client.delete_topics([TOPIC], timeout_ms=10000)
        print(f"⏳ Waiting for topic deletion...")
        time.sleep(3)
        
        # Recreate the topic immediately
        topic = NewTopic(
            name=TOPIC,
            num_partitions=1,
            replication_factor=1
        )
        admin_client.create_topics([topic], timeout_ms=10000)
        print(f"✅ Topic {TOPIC} recreated successfully")
        print("🔄 Container safe, topic ready for new data")
        
    except Exception as e:
        print(f"❌ Error resetting topic: {e}")

if __name__ == "__main__":
    print("Safe Kafka Topic Cleanup")
    print("=" * 30)
    print("1. Clear data only (consume messages)")
    print("2. Safe reset (delete + immediate recreate)")
    
    choice = input("Choose method (1 or 2): ").strip()
    
    if choice == "1":
        clear_topic_data()
    elif choice == "2":
        safe_topic_reset()
    else:
        print("Invalid choice, using safe clear method")
        clear_topic_data()
