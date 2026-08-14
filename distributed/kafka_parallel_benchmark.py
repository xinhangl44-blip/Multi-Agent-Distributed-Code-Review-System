import json
import time
import uuid
import logging
import multiprocessing
from typing import List, Dict, Any
import requests
from openai import OpenAI
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, TopicPartition

# ==========================================
# 1. 统一分布式日志与 Tracing 配置
# ==========================================
class TraceLogFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, "request_id"):
            record.request_id = "SYSTEM"
        return super().format(record)

logger = logging.getLogger("Week6ParallelCluster")
handler = logging.StreamHandler()
formatter = TraceLogFormatter('[%(asctime)s] [%(levelname)s] [TRACE-%(request_id)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ==========================================
# 2. 全局拓扑网关配置
# ==========================================
VLLM_ENDPOINT = "http://localhost:8000/v1"
MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct-AWQ"
KAFKA_BOOTSTRAP = "127.0.0.1:9092"
RAG_SERVICE_URL = "http://127.0.0.1:8001/retrieve"

vllm_client = OpenAI(base_url=VLLM_ENDPOINT, api_key="vllm-shared-token")

# ==========================================
# 3. 生产级持久化 Kafka 事件总线
# ==========================================
class KafkaEventBus:
    def __init__(self):
        self._producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(2, 5, 0),
            acks=1,
            linger_ms=0,
        )

    def get_consumer(self, topic: str, group_id_version: str = "v6-group") -> KafkaConsumer:
        group_id = f"production-agent-group-{topic.replace('.', '-')}-{group_id_version}" 
        return KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            api_version=(2, 5, 0)
        )

    def publish(self, topic: str, message: Dict[str, Any], key: str = None):
        req_id = message["metadata"]["request_id"]
        try:
            # 🌟 配合多分区，显式支持传入 Key 进行 Hash 分流
            kafka_key = key.encode('utf-8') if key else None
            future = self._producer.send(topic, key=kafka_key, value=message)
            future.get(timeout=10)
        except Exception as e:
            logger.error(f"💥 [Kafka Pub 失败] Topic: {topic} | 原因: {str(e)}", extra={"request_id": req_id})

    def flush(self):
        self._producer.flush()

# ==========================================
# 4. 常驻多进程 Worker 基类（精准剥离冷启动开销）
# ==========================================
class BaseParallelWorker:
    def __init__(self, input_topic: str, next_topic: str, worker_id: int = 0):
        self.input_topic = input_topic
        self.next_topic = next_topic
        self.worker_id = worker_id
        self.bus = KafkaEventBus()
        self.consumer = self.bus.get_consumer(self.input_topic, group_id_version="v6-exec-final")

    def start_polling(self):
        for msg_block in self.consumer:
            msg = msg_block.value
            meta = msg["metadata"]
            payload = msg["payload"]
            req_id = meta["request_id"]
            
            # 🌟 [用户硬核修正] 精准拆解真正的 Broker 队列内排队延迟，秒杀进程冷启动误差
            now_ms = time.time()
            broker_timestamp = msg_block.timestamp / 1000.0  # 毫秒转秒
            queue_time = now_ms - broker_timestamp
            
            compute_start = time.time()
            try:
                updated_payload = self.business_logic(payload, req_id, meta)
                msg["payload"] = updated_payload
                
                compute_time = time.time() - compute_start
                
                # 记录真正的物理 Benchmark 报表数据
                meta["metrics_report"].append({
                    "worker": f"{self.__class__.__name__}_{self.worker_id}",
                    "partition": msg_block.partition,
                    "queue_time": queue_time,
                    "compute_time": compute_time
                })
                msg["metadata"] = meta
                
                self.bus.publish(self.next_topic, msg)
            except Exception as e:
                logger.error(f"❌ [Worker 进程异常] 失败: {str(e)}", extra={"request_id": req_id})
        
        self.consumer.close()

    def call_vllm(self, role: str, messages: List[Dict[str, str]], max_tokens: int, req_id: str) -> str:
        response = vllm_client.chat.completions.create(
            model=MODEL_NAME, messages=messages, temperature=0.0, max_tokens=max_tokens
        )
        return response.choices[0].message.content

# ==========================================
# 5. 业务 Worker 实体实现
# ==========================================
class ParallelCoderWorker(BaseParallelWorker):
    def business_logic(self, payload: Dict[str, Any], req_id: str, meta: Dict[str, Any]) -> Dict[str, Any]:
        bug_report = payload["data"]["bug_report"]
        resp = requests.post(RAG_SERVICE_URL, json={"query": bug_report, "limit": 1}, timeout=10)
        hits = resp.json().get("data", [])
        snippet = hits[0].get('code_snippet', '')[:300] if hits else "No context"
        
        messages = [
            {"role": "system", "content": "Automated patch engine."},
            {"role": "user", "content": f"Context: {snippet}\nFix: {bug_report[:100]}. Index: {meta.get('candidate_index', 0)}"}
        ]
        patch_code = self.call_vllm(f"coder_{self.worker_id}", messages, 48, req_id)
        
        payload["status"] = "PATCH_READY"
        payload["data"]["patch"] = {"content": patch_code, "candidate_index": meta.get('candidate_index', 0)}
        return payload

def launch_coder_process(worker_id: int, input_topic: str, next_topic: str):
    logger.info(f"🚀 [Cluster Process] Coder常驻实例 #{worker_id} 正在完成冷启动并监听 Partition 分配...")
    worker = ParallelCoderWorker(input_topic, next_topic, worker_id=worker_id)
    worker.start_polling()

# ==========================================
# 6. 压测控制面与主程序
# ==========================================
if __name__ == "__main__":
    multiprocessing.set_start_method('spawn', force=True)
    
    CONCURRENT_TASKS = 5   
    CANDIDATES_PER_TASK = 3 
    expected_count = CONCURRENT_TASKS * CANDIDATES_PER_TASK

    bus = KafkaEventBus()
    
    print("\n" + "═"*25 + f" ⚡ Week 6 并行吞吐级压力测试 (并发任务: {CONCURRENT_TASKS}) " + "═"*25)
    
    # 🌟 1. 先把 3 个并行进程拉起来，让他们先完成冷启动，抢占好分区蹲点
    processes = []
    for i in range(3):
        p = multiprocessing.Process(
            target=launch_coder_process, 
            args=(i, "agent.coder.batch", "agent.coder.results")
        )
        processes.append(p)
        p.start()
        
    logger.info("⏳ 正在等待 3 秒，确保消费者实例群完全越过 Python Spawn 冷启动物理黑洞...")
    time.sleep(3.0)

    # 2. 批量多任务 + 显式 Partition Key 哈希分流投递
    logger.info(f"📈 骨干网开闸！正在向多分区 Topic 砸入事件流... 预期请求数: {expected_count} 个")
    for t_idx in range(CONCURRENT_TASKS):
        task_id = f"TASK-W6-{t_idx:02d}"
        bug_report = f"Concurrency race condition error in scheduler_mesh.go block #{t_idx}."
        
        for c_idx in range(CANDIDATES_PER_TASK):
            request_id = f"req-{uuid.uuid4().hex[:8]}"
            task_packet = {
                "metadata": {
                    "request_id": request_id, 
                    "candidate_index": c_idx,
                    "metrics_report": []
                },
                "payload": {
                    "task_id": task_id, "status": "CANDIDATE_REQUESTED",
                    "data": {"bug_report": bug_report, "patch": {}}
                }
            }
            # 🌟 显式传入 task_id 作为 Key，强行在 Broker 侧将消息根据 3 分区打散
            bus.publish("agent.coder.batch", task_packet, key=task_id)

    bus.flush()

    # 3. 结果集精准收拢
    print("\n" + "═"*30 + " 📊 Week 6 压力测试 Benchmark 报表 ════════════════")
    results_consumer = bus.get_consumer("agent.coder.results", group_id_version="v6-report-real-final")
    
    report_summary = []
    logger.info(f"📥 主线程开始收拢结果流，精准倒计时计数: {expected_count} 个...")
    
    for msg_block in results_consumer:
        report_summary.append(msg_block.value)
        current_count = len(report_summary)
        if current_count % 3 == 0 or current_count == expected_count:
            logger.info(f"⏳ 进度同步: 已回收 {current_count}/{expected_count} 个候选 Patch 战报...")
        if current_count >= expected_count:
            break
            
    # 释放子进程
    for p in processes:
        if p.is_alive():
            p.terminate()

    print("\n" + "─"*90)
    print(f"| {'任务ID':<11} | {'候选编号':<6} | {'承载 Worker 实例':<18} | {'物理分区':<6} | {'真实排队 (Queue)':<14} | {'纯推理耗时 (Compute)':<16} |")
    print("-" * 90)
    
    total_q, total_c = 0.0, 0.0
    for item in report_summary:
        meta = item["metadata"]
        metrics = meta["metrics_report"][0]
        print(f"| {item['payload']['task_id']:<11} | {meta['candidate_index']:<8} | {metrics['worker']:<22} | {metrics['partition']:<8} | {metrics['queue_time']:11.4f}s | {metrics['compute_time']:13.4f}s |")
        total_q += metrics['queue_time']
        total_c += metrics['compute_time']

    print("-" * 90)
    if report_summary:
        avg_q = total_q / len(report_summary)
        avg_c = total_c / len(report_summary)
        print(f"💡 真实压测总结：平均物理排队时间 (Queue Time): {avg_q:.4f}s | 平均大模型推理耗时 (Compute Time): {avg_c:.4f}s")
    
    results_consumer.close()
    print("👋 [System] Week 6 并行核心压测圆满收官。")
