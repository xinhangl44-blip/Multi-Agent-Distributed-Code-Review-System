import sys
import os
import time
import json
import random
import argparse
import threading
from kafka import KafkaConsumer, KafkaProducer, TopicPartition


class RaftNode:
    def __init__(self, node_id, all_nodes):
        self.node_id = node_id
        self.all_nodes = all_nodes
        self.role = "FOLLOWER"
        self.current_term = 0
        self.voted_for = None
        self.last_heartbeat = time.time()
        self.lock = threading.Lock()
        
        self.election_timeout = random.uniform(1.5, 3.0)
        
        threading.Thread(target=self._election_ticker, daemon=True).start()
        threading.Thread(target=self._heartbeat_sender, daemon=True).start()

    def get_role(self):
        with self.lock:
            return self.role

    def _election_ticker(self):
        while True:
            time.sleep(0.1)
            with self.lock:
                if self.role != "LEADER" and (time.time() - self.last_heartbeat > self.election_timeout):
                    print(f"[Raft] Node {self.node_id} heartbeat timed out. Initiating election for term {self.current_term + 1}...")
                    self._start_election()

    def _start_election(self):
        self.role = "CANDIDATE"
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        
        votes = 1 
        active_nodes = []
        
        for n in self.all_nodes:
            if n == self.node_id:
                continue
            if os.path.exists(f".node_{n}_alive"):
                try:
                    with open(f".node_{n}_alive", "r") as f:
                        t = float(f.read().strip())
                    if time.time() - t < 2.0: 
                        active_nodes.append(n)
                except:
                    pass
        
        votes += len(active_nodes)
        majority = (len(self.all_nodes) // 2) + 1
        
        if votes >= majority:
            self.role = "LEADER"
            print(f"[Raft] Node {self.node_id} successfully received {votes}/{len(self.all_nodes)} votes and promoted to LEADER!")
        else:
            self.role = "FOLLOWER"
            self.voted_for = None

    def _heartbeat_sender(self):
        while True:
            with open(f".node_{self.node_id}_alive", "w") as f:
                f.write(str(time.time()))
                
            with self.lock:
                if self.role == "LEADER":
                    for n in self.all_nodes:
                        if n != self.node_id:
                            try:
                                with open(f".node_{n}_heartbeat", "w") as f:
                                    f.write(str(time.time()))
                            except:
                                pass
            
            if self.get_role() == "FOLLOWER":
                if os.path.exists(f".node_{self.node_id}_heartbeat"):
                    try:
                        self.last_heartbeat = os.path.getmtime(f".node_{self.node_id}_heartbeat")
                    except:
                        pass
                    
            time.sleep(0.5)

    def clean_up(self):
        if os.path.exists(f".node_{self.node_id}_alive"):
            os.remove(f".node_{self.node_id}_alive")
        if os.path.exists(f".node_{self.node_id}_heartbeat"):
            os.remove(f".node_{self.node_id}_heartbeat")


class HighAvailableOrchestrator:
    def __init__(self, node_id, all_nodes, kafka_brokers):
        self.node_id = node_id
        self.all_nodes = all_nodes
        self.kafka_brokers = kafka_brokers
        self.raft = RaftNode(node_id, all_nodes)
        self.is_leader = False
        self.consumer = None
        
        print(f"Initializing Orchestrator Node-{node_id}, connecting to Kafka cluster...")
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def _get_leader_consumer(self):
        consumer = KafkaConsumer(
            bootstrap_servers=self.kafka_brokers,
            group_id='orchestrator-ha-shared-group',
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        tp = TopicPartition('task.events', 0)
        consumer.assign([tp])
        
        committed = consumer.committed(tp)
        if committed is not None:
            consumer.seek(tp, committed)
            print(f"[Node-{self.node_id}] Found historical committed offset! Seeking to offset {committed} to guarantee At-Least-Once processing.")
        else:
            consumer.seek_to_beginning(tp)
            print(f"[Node-{self.node_id}] No historical committed offset found. Reading topic from the beginning.")
            
        return consumer

    def drive_task_graph(self, task_data):
        payload = task_data.get("payload", task_data)
        task_id = payload.get("task_id", "UNKNOWN_ID")
        status = payload.get("status", "UNKNOWN_STATUS")
        
        print(f"[Node-{self.node_id}] Received pipeline event -> Task ID: {task_id} | Status: {status}")
        
        if "SUCCESS" in status or status == "PASS":
            next_event = {
                "metadata": {
                    "request_id": f"ha-req-{random.randint(1000,9999)}",
                    "timestamp": time.time(),
                    "retry_count": 0
                },
                "payload": {
                    "task_id": task_id,
                    "current_agent": "CriticAgent",
                    "status": "CriticWorker_PENDING",
                    "data": {
                        "bug_report": payload.get("data", {}).get("bug_report", "No Context Available"),
                    }
                }
            }
            print(f"   -> State machine matched! Generating next decision chain -> Dispatching to CriticAgent [PENDING]")
            return next_event
            
        print(f"   -> Status is terminal or non-triggering node. Archiving silently.")
        return None

    def run_loop(self):
        try:
            while True:
                role = self.raft.get_role()
                
                if role == "LEADER":
                    if not self.is_leader:
                        print(f"\n[Node-{self.node_id}] Promoted to LEADER. Fetching global committed offset and initializing consumer pipeline...")
                        if self.consumer:
                            self.consumer.close()
                        self.consumer = self._get_leader_consumer()
                        self.is_leader = True
                    
                    records = self.consumer.poll(timeout_ms=300)
                    for tp, messages in records.items():
                        for msg in messages:
                            next_task = self.drive_task_graph(msg.value)
                            
                            if next_task:
                                self.producer.send('task.events', value=next_task)
                                self.producer.flush()
                            
                            self.consumer.commit()
                            print(f"[Node-{self.node_id}] State loop successfully closed. Committed offset: {msg.offset + 1}\n")
                            
                else:
                    if self.is_leader:
                        print(f"\n[Node-{self.node_id}] Demoted to Follower. Closing consumer channel to release partition ownership.")
                        if self.consumer:
                            self.consumer.close()
                            self.consumer = None
                        self.is_leader = False
                    
                    time.sleep(0.5)
                    
        except KeyboardInterrupt:
            print(f"\n[Node-{self.node_id}] Exiting state machine safely...")
        finally:
            if self.consumer:
                self.consumer.close()
            self.raft.clean_up()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Week 9 Production HA Orchestrator")
    parser.add_argument("--id", type=int, required=True, help="Node ID (1, 2, 3)")
    args = parser.parse_args()
    
    cluster_nodes = [1, 2, 3]
    kafka_server = ["localhost:9092"]
    
    for nid in cluster_nodes:
        if nid == args.id:
            if os.path.exists(f".node_{nid}_alive"):
                os.remove(f".node_{nid}_alive")
            if os.path.exists(f".node_{nid}_heartbeat"):
                os.remove(f".node_{nid}_heartbeat")
            
    orchestrator = HighAvailableOrchestrator(
        node_id=args.id, 
        all_nodes=cluster_nodes, 
        kafka_brokers=kafka_server
    )
    orchestrator.run_loop()