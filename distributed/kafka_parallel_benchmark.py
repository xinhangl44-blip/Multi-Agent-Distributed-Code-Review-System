import json
import time
import uuid
import logging
import multiprocessing
from typing import List, Dict, Any
import requests
from openai import OpenAI
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, TopicPartition

class TraceLogFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, "request_id"):
            record.request_id = "SYSTEM"
        return super().format(record)

logger = logging.getLogger("ParallelCluster")
handler = logging.StreamHandler()
formatter = TraceLogFormatter('[%(asctime)s] [%(levelname)s] [TRACE-%(request_id)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
VLLM_ENDPOINT = "http://localhost:8000/v1"
MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct-AWQ"
KAFKA_BOOTSTRAP = "127.0.0.1:9092"
RAG_SERVICE_URL = "http://127.0.0.1:8001/retrieve"
vllm_client = OpenAI(base_url=VLLM_ENDPOINT, api_key="vllm-shared-token")

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
            kafka_key = key.encode('utf-8') if key else None
            future = self._producer.send(topic, key=kafka_key, value=message)
            future.get(timeout=10)
        except Exception as e:
            logger.error(f"Kafka publish failed on Topic: {topic} | Error: {str(e)}", extra={"request_id": req_id})

    def flush(self):
        self._producer.flush()

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
            now_ms = time.time()
            broker_timestamp = msg_block.timestamp / 1000.0
            queue_time = now_ms - broker_timestamp
            compute_start = time.time()
            try:
                updated_payload = self.business_logic(payload, req_id, meta)
                msg["payload"] = updated_payload
                compute_time = time.time() - compute_start
                meta["metrics_report"].append({
                    "worker": f"{self.__class__.__name__}_{self.worker_id}",
                    "partition": msg_block.partition,
                    "queue_time": queue_time,
                    "compute_time": compute_time
                })
                msg["metadata"] = meta
                self.bus.publish(self.next_topic, msg)
            except Exception as e:
                logger.error(f"Worker process failure: {str(e)}", extra={"request_id": req_id})
        self.consumer.close()

    def call_vllm(self, role: str, messages: List[Dict[str, str]], max_tokens: int, req_id: str) -> str:
        response = vllm_client.chat.completions.create(
            model=MODEL_NAME, messages=messages, temperature=0.0, max_tokens=max_tokens
        )
        return response.choices[0].message.content

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
    logger.info(f"Coder worker instance #{worker_id} initialized and waiting for partition assignment...")
    worker = ParallelCoderWorker(input_topic, next_topic, worker_id=worker_id)
    worker.start_polling()

if __name__ == "__main__":
    multiprocessing.set_start_method('spawn', force=True)
    CONCURRENT_TASKS = 5    
    CANDIDATES_PER_TASK = 3 
    expected_count = CONCURRENT_TASKS * CANDIDATES_PER_TASK
    bus = KafkaEventBus()
    print("\n" + "="*25 + f" Parallel Throughput Load Test (Concurrent Tasks: {CONCURRENT_TASKS}) " + "="*25)
    processes = []
    for i in range(3):
        p = multiprocessing.Process(
            target=launch_coder_process, 
            args=(i, "agent.coder.batch", "agent.coder.results")
        )
        processes.append(p)
        p.start()
    logger.info("Waiting 3 seconds for consumer instance pool initialization...")
    time.sleep(3.0)
    logger.info(f"Publishing event streams to multi-partition topic... Total expected requests: {expected_count}")
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
            bus.publish("agent.coder.batch", task_packet, key=task_id)
    bus.flush()
    print("\n" + "="*30 + " Load Test Benchmark Report " + "="*30)
    results_consumer = bus.get_consumer("agent.coder.results", group_id_version="v6-report-real-final")
    report_summary = []
    logger.info(f"Main thread collecting results... Remaining: {expected_count}")
    for msg_block in results_consumer:
        report_summary.append(msg_block.value)
        current_count = len(report_summary)
        if current_count % 3 == 0 or current_count == expected_count:
            logger.info(f"Progress update: Collected {current_count}/{expected_count} candidate patch reports...")
        if current_count >= expected_count:
            break
    for p in processes:
        if p.is_alive():
            p.terminate()
    print("\n" + "-"*90)
    print(f"| {'Task ID':<11} | {'Candidate':<9} | {'Worker Instance':<22} | {'Partition':<9} | {'Queue Time':<14} | {'Compute Time':<14} |")
    print("-" * 90)
    total_q, total_c = 0.0, 0.0
    for item in report_summary:
        meta = item["metadata"]
        metrics = meta["metrics_report"][0]
        print(f"| {item['payload']['task_id']:<11} | {meta['candidate_index']:<9} | {metrics['worker']:<22} | {metrics['partition']:<9} | {metrics['queue_time']:11.4f}s | {metrics['compute_time']:11.4f}s |")
        total_q += metrics['queue_time']
        total_c += metrics['compute_time']
    print("-" * 90)
    if report_summary:
        avg_q = total_q / len(report_summary)
        avg_c = total_c / len(report_summary)
        print(f"Benchmark Summary: Avg Queue Time: {avg_q:.4f}s | Avg LLM Compute Time: {avg_c:.4f}s")
    results_consumer.close()
    print("Parallel core load test completed successfully.")
