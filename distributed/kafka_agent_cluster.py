import json
import time
import uuid
import logging
from typing import List, Dict, Any
import requests
from openai import OpenAI
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

class TraceLogFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, "request_id"):
            record.request_id = "SYSTEM"
        return super().format(record)

logger = logging.getLogger("ProductionAgentCluster")
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
        logger.info("Connecting to Kafka backbone...")
        self._producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(2, 5, 0),
            acks=1,
            linger_ms=0,
        )

    def get_consumer(self, topic: str, timeout_ms: int = 5000) -> KafkaConsumer:
        group_id = f"production-agent-group-{topic.replace('.', '-')}-v7" 
        return KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            api_version=(2, 5, 0),
            consumer_timeout_ms=timeout_ms,
        )

    def publish(self, topic: str, message: Dict[str, Any]):
        req_id = message["metadata"]["request_id"]
        try:
            future = self._producer.send(topic, value=message)
            future.get(timeout=10)
            logger.info(f"Topic: {topic.ljust(25)} | Event: {message['payload']['status']}", extra={"request_id": req_id})
        except KafkaError as e:
            logger.error(f"Kafka publish failed on Topic: {topic} | Error: {str(e)}", extra={"request_id": req_id})
            raise e
    def flush(self):
        self._producer.flush()
    def close(self):
        self._producer.close()
kafka_bus = KafkaEventBus()

class BaseAgentWorker:
    def __init__(self, input_topic: str, next_topic: str, max_retries: int = 1):
        self.input_topic = input_topic
        self.next_topic = next_topic
        self.max_retries = max_retries
        self.consumer = kafka_bus.get_consumer(self.input_topic)

    def consume_and_process(self):
        for msg_block in self.consumer:
            msg = msg_block.value
            meta = msg["metadata"]
            payload = msg["payload"]
            req_id = meta["request_id"]
            try:
                updated_payload = self.business_logic(payload, req_id, meta)
                msg["payload"] = updated_payload
                kafka_bus.publish(self.next_topic, msg)
                msg["payload"]["status"] = f"{self.__class__.__name__}_SUCCESS"
                kafka_bus.publish("task.events", msg)
            except Exception as e:
                logger.error(f"Worker failure in {self.__class__.__name__}: {str(e)}", extra={"request_id": req_id})
                if meta["retry_count"] < self.max_retries:
                    meta["retry_count"] += 1
                    msg["metadata"] = meta
                    logger.warning(f"Routing to retry topic. Count: {meta['retry_count']}", extra={"request_id": req_id})
                    kafka_bus.publish(f"{self.input_topic}.retry", msg)
                else:
                    msg["payload"]["status"] = f"{self.__class__.__name__}_DEAD_LETTER"
                    kafka_bus.publish(f"{self.input_topic}.dlq", msg)
                    kafka_bus.publish("task.events", msg)

    def call_vllm_with_limit(self, role: str, messages: List[Dict[str, str]], max_tokens: int, req_id: str) -> str:
        start_time = time.time()
        response = vllm_client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            temperature=0.0,
            max_tokens=max_tokens,
            extra_body={"agent_role": role, "request_trace_id": req_id}
        )
        duration = time.time() - start_time
        logger.info(f"vLLM Call | Role: {role.ljust(8)} | Duration: {duration:.4f}s", extra={"request_id": req_id})
        return response.choices[0].message.content

class PlannerWorker(BaseAgentWorker):
    def business_logic(self, payload: Dict[str, Any], req_id: str, meta: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("Executing planner worker logic...", extra={"request_id": req_id})
        messages = [
            {"role": "system", "content": "KubeGPU expert."},
            {"role": "user", "content": f"Bug: {payload['data']['bug_report'][:200]}"}
        ]
        llm_out = self.call_vllm_with_limit("planner", messages, 48, req_id)
        payload["status"] = "PLAN_READY"
        payload["data"]["steps"] = [{"step": 1, "action": llm_out[:30] + "..."}]
        return payload

class RetrieverWorker(BaseAgentWorker):
    def business_logic(self, payload: Dict[str, Any], req_id: str, meta: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("Querying RAG service for context...", extra={"request_id": req_id})
        if "division by zero" in payload["data"]["bug_report"].lower() and meta["retry_count"] == 0:
            raise ConnectionError("Qdrant gRPC Cluster connection timeout on port 6334")
        query = payload["data"]["bug_report"]
        resp = requests.post(RAG_SERVICE_URL, json={"query": query, "limit": 1}, timeout=10)
        resp.raise_for_status()
        hits = resp.json().get("data", [])
        if not hits:
            raise RuntimeError("RAG returned no matching code snippets")
        MAX_SNIPPET_CHARS = 300
        context_blocks = "\n\n".join([
            f"// {h.get('function_name')} ({h.get('file_path')})\n{h.get('code_snippet', '')[:MAX_SNIPPET_CHARS]}"
            for h in hits
        ])
        payload["status"] = "CONTEXT_READY"
        payload["data"]["shared_system"] = "Automated patch engine."
        payload["data"]["shared_context"] = f"Context:\n{context_blocks}"
        return payload

class CoderWorker(BaseAgentWorker):
    def business_logic(self, payload: Dict[str, Any], req_id: str, meta: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("Executing coder worker logic...", extra={"request_id": req_id})
        full_user_content = (
            f"{payload['data']['shared_context']}\n\n"
            f"Fix: {payload['data']['bug_report'][:100]}. Output fix only."
        )
        messages = [
            {"role": "system", "content": payload["data"]["shared_system"]},
            {"role": "user", "content": full_user_content}
        ]
        patch_code = self.call_vllm_with_limit("coder", messages, 48, req_id)
        payload["status"] = "PATCH_READY"
        payload["data"]["patch"] = {"content": patch_code}
        return payload

class VerifierWorker(BaseAgentWorker):
    def business_logic(self, payload: Dict[str, Any], req_id: str, meta: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("Executing verifier worker logic...", extra={"request_id": req_id})
        full_user_content = (
            f"{payload['data']['shared_context']}\n\n"
            f"Patch:\n{payload['data']['patch']['content'][:150]}\nPASS or FAIL?"
        )
        messages = [
            {"role": "system", "content": payload["data"]["shared_system"]},
            {"role": "user", "content": full_user_content}
        ]
        review_result = self.call_vllm_with_limit("verifier", messages, 32, req_id)
        payload["status"] = "VERIFIED"
        payload["data"]["verification"] = {"result": review_result.strip()}
        return payload

class EventDrivenOrchestrator:
    def submit_task(self, bug_report: str):
        request_id = f"req-{uuid.uuid4().hex[:8]}"
        task_packet = {
            "metadata": {"request_id": request_id, "timestamp": time.time(), "retry_count": 0},
            "payload": {
                "task_id": f"TASK-GPU-{uuid.uuid4().hex[:4].upper()}", "status": "SUBMITTED",
                "data": {"bug_report": bug_report, "steps": [], "patch": {}, "verification": {}}
            }
        }
        kafka_bus.publish("agent.planner.in", task_packet)

if __name__ == "__main__":
    orchestrator = EventDrivenOrchestrator()
    kubegpu_bug_dataset = [
        "Panic inside simulator.go:fetchNodes when remote active GPU metrics return nil pointer.",
        "VRAM division by zero when scheduling fractional GPU requests."
    ]
    print("\nKafka event bus cluster ready.")
    for bug in kubegpu_bug_dataset:
        orchestrator.submit_task(bug)
    planner = PlannerWorker("agent.planner.in", "agent.retriever.in")
    retriever = RetrieverWorker("agent.retriever.in", "agent.coder.in")
    coder = CoderWorker("agent.coder.in", "agent.verifier.in")
    verifier = VerifierWorker("agent.verifier.in", "task.events")
    print("\nStarting task processing workflow...\n")
    planner.consume_and_process()
    retriever.consume_and_process()
    logger.info("Scanning retry queue (agent.retriever.in.retry)...")
    original_topic = retriever.input_topic
    main_retriever_consumer = retriever.consumer
    retriever.consumer = kafka_bus.get_consumer("agent.retriever.in.retry", timeout_ms=5000)
    retriever.consume_and_process()
    retriever.consumer.close() 
    retriever.consumer = main_retriever_consumer  
    retriever.input_topic = original_topic
    time.sleep(1)
    coder.consume_and_process()
    verifier.consume_and_process()
    print("\nDisconnecting from Kafka event bus...")
    planner.consumer.close()
    retriever.consumer.close()
    coder.consumer.close()
    verifier.consumer.close()
    kafka_bus.flush()
    kafka_bus.close()
    print("Kafka resources released successfully.")
