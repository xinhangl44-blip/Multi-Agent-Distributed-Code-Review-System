import time
import json
import random
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

def run_chaos_test():
    brokers = ['localhost:9092']
    topic = 'task.events'
    test_task_id = f"CHAOS-TRACK-{random.randint(1000, 9999)}"
    producer = KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    payload = {
        "metadata": {
            "request_id": f"req-{random.randint(10000, 99999)}",
            "timestamp": time.time(),
            "retry_count": 0
        },
        "payload": {
            "task_id": test_task_id,
            "status": "PlannerWorker_SUCCESS",
            "data": {
                "bug_report": "Panic inside simulator.go: fractional VRAM schedule panic.",
                "steps": [{"step": 1, "action": "Reproduce via custom fractional requests."}]
            }
        }
    }
    temp_consumer = KafkaConsumer(bootstrap_servers=brokers)
    tp = TopicPartition(topic, 0)
    temp_consumer.assign([tp])
    temp_consumer.seek_to_end(tp)
    before_offset = temp_consumer.position(tp)
    temp_consumer.close()
    print("=" * 60)
    print(f"[Chaos Test] Injecting test task into pipeline: {test_task_id}")
    print(f"[Chaos Test] Target message offset: {before_offset}")
    print("=" * 60)
    producer.send(topic, value=payload)
    producer.flush()
    print("\n[Chaos Test] Task successfully submitted to Kafka queue!")
    print("   Monitor your active Leader node terminal.")
    print("   Prepare to terminate the process...\n")
    for i in range(7, 0, -1):
        print(f"   Approximately {i} seconds before the previous Leader polls and commits... Prepare to press Ctrl+C!")
        time.sleep(1.0)
    print("\n[Chaos Test] If you successfully terminated the Leader process prior to commit:")
    print("   1. The previous Leader state machine failed, leaving the offset uncommitted.")
    print("   2. The newly elected Follower completes failover/rebalance.")
    print(f"   3. The new Leader calls committed(), detects the uncommitted offset, and executes a seek replay.")
    print(f"   Check the new Leader logs; it should re-poll and process Task ID: {test_task_id}")
    print("=" * 60)

if __name__ == "__main__":
    run_chaos_test()
