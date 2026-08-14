import time
import json
import random
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

def run_chaos_test():
    brokers = ['localhost:9092']
    topic = 'task.events'
    
    # 1. 产生一个随机的专属 Task ID，方便在日志里一眼认出
    test_task_id = f"CHAOS-TRACK-{random.randint(1000, 9999)}"
    
    producer = KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # 2. 构造契合生产状态机的真实触发 Payload
    payload = {
        "metadata": {
            "request_id": f"req-{random.randint(10000,99999)}",
            "timestamp": time.time(),
            "retry_count": 0
        },
        "payload": {
            "task_id": test_task_id,
            "status": "PlannerWorker_SUCCESS", # 💥 触发状态机的金钥匙
            "data": {
                "bug_report": "Panic inside simulator.go: fractional VRAM schedule panic.",
                "steps": [{"step": 1, "action": "Reproduce via custom fractional requests."}]
            }
        }
    }
    
    # 获取发送前的最新水位线（供测试直观对比）
    temp_consumer = KafkaConsumer(bootstrap_servers=brokers)
    tp = TopicPartition(topic, 0)
    temp_consumer.assign([tp])
    temp_consumer.seek_to_end(tp)
    before_offset = temp_consumer.position(tp)
    temp_consumer.close()
    
    print("=" * 60)
    print(f"🚀 [Chaos Test] 正在向管道注入黄金测试任务: {test_task_id}")
    print(f"📌 [Chaos Test] 该消息预计将写入到 Offset: {before_offset}")
    print("=" * 60)
    
    producer.send(topic, value=payload)
    producer.flush()
    
    # 3. 开启人性化手动 Kill 倒计时提示
    print("\n⏱️  [时空减速中] 任务已进入 Kafka 队列！")
    print("   现在请盯紧你的 Leader 节点终端。")
    print("   你拥有充足的时间准备执行【斩首行动】...\n")
    
    for i in range(7, 0, -1):
        print(f"   ⏳ 距离旧 Leader 轮询并潜在提交该位点还有大约 {i} 秒... 准备按 Ctrl+C！")
        time.sleep(1.0)
        
    print("\n🔥 [时间到] 如果你刚才在旧 Leader 提交前成功将其 Kill 掉：")
    print("   1. 旧 Leader 的内存状态机崩溃，该 Offset 未执行手动 commit。")
    print("   2. 新上任的 Follower 触发 Raft 选主登基。")
    print(f"   3. 新 Leader 将调用 committed() 发现最新位点仍处于旧状态，随后执行 seek() 回流。")
    print(f"   👉 检查新 Leader 终端，它应该会重新拉取并打印出任务 ID: {test_task_id} ！")
    print("=" * 60)

if __name__ == "__main__":
    run_chaos_test()
