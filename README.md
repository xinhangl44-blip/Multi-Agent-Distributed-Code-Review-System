# Multi-Agent Distributed Code Review System

A production-grade multi-agent system for automated code review and patch generation, built on Kafka, Qdrant, vLLM, and a Raft-based HA orchestrator. Part of a three-project portfolio: **KubeGPU Scheduler → LLM Inference Platform → this system**.

---

## Architecture Overview

```
                    ┌─────────────────────────────────────────────┐
                    │           Raft HA Orchestrator (×3)          │
                    │  Node-1 (Leader) │ Node-2 (Follower) │ Node-3 │
                    │         File-based heartbeat + term          │
                    └──────────────────┬──────────────────────────┘
                                       │ drives task graph via
                                       ▼
┌──────────────────────────────── Kafka Backbone ─────────────────────────────────────┐
│                                                                                      │
│  agent.planner.in ──► agent.retriever.in ──► agent.coder.batch ──► agent.verifier.in│
│       │                      │  .retry            │ (3 partitions)        │          │
│       │                      │  .dlq              │ (3 worker processes)  │          │
│       └──────────────────────┴────────────────────┴───────────────────────┘          │
│                                   task.events (audit log)                            │
└──────────────────────────────────────────────────────────────────────────────────────┘
         │                   │                   │                   │
         ▼                   ▼                   ▼                   ▼
    PlannerWorker    RetrieverWorker      CoderWorker          VerifierWorker
    (vLLM 7B-AWQ)   (Qdrant hybrid RAG)  (vLLM 7B-AWQ ×3)    (vLLM 7B-AWQ)
                          │
                          ▼
                 ┌─────────────────┐
                 │  Qdrant Cluster │  ← Hybrid retrieval
                 │  Dense (BGE)    │     BM25 + dense + RRF
                 │  Sparse (BM25)  │
                 └─────────────────┘
                          │
                   search_service.py
                   (FastAPI :8001)
```

---

## Key Design Decisions

**Why Kafka instead of direct function calls between agents**

Each agent role maps to a dedicated topic (`agent.planner.in`, `agent.coder.batch`, etc.). This gives persistent message history for free, enables retry/DLQ patterns without extra infrastructure, and lets the HA orchestrator recover mid-flight tasks by replaying from the last committed offset.

**Why Raft for the orchestrator, not just a single process**

The orchestrator is the single point of failure in most multi-agent frameworks. Using Raft leader election means if the leader crashes mid-task (verified via chaos test), the new leader picks up from the last committed Kafka offset rather than losing the task silently.

**Why RAG before every Coder/Verifier call**

Coder and Verifier share the same `shared_system + shared_context` prefix in their vLLM requests. This is intentional: when the same Go function is retrieved for both agents, vLLM's prefix KV cache hits on that prefix, reducing TTFT for the second request. Measured: Verifier TTFT ~28% lower than Coder on the same task.

**Why QLoRA on a 1.5B Critic instead of prompting the 7B**

The Critic's job (PASS/FAIL classification with chain-of-thought) is narrow and repetitive. A fine-tuned 1.5B model handles it with lower latency and VRAM cost, freeing the 7B for generation tasks. Validated: +5% accuracy over base model on 20 held-out samples (limited by training data size, not architecture).

---

## Benchmark Results

| Week | Component | Metric | Result |
|------|-----------|--------|--------|
| 2 | Hybrid RAG (BM25 + dense + RRF) | Recall@10 on 15 Go function queries | **100%** |
| 2 | search_service (FastAPI) | P50 / P99 latency | **17ms / 55ms** |
| 5 | Cross-agent prefix cache sharing | Verifier TTFT vs Coder | **−28%** |
| 6 | Parallel Coder (3 workers, 3 partitions) | Wall-clock time vs serial estimate | **1.43× speedup** |
| 6 | Kafka queue time (broker-side, corrected) | Average consumer lag | **~3.7s** (incl. spawn overhead) |
| 8 | QLoRA fine-tuned Critic vs base | Accuracy on 20 held-out samples | **65% vs 60% (+5%)** |

**On the Week 6 speedup**: the 1.43× figure (vs theoretical 3×) reflects vLLM serializing requests on a single GPU. Kafka partitioning and worker parallelism eliminate I/O and RAG wait time overlap; the remaining bottleneck is GPU compute. With multi-GPU deployment the speedup would scale linearly with worker count.

**On the Week 8 +5%**: training data was <10 synthetic samples. The main contribution of Week 8 is validating the full QLoRA pipeline (data synthesis → fine-tuning → A/B eval), not the final accuracy number.

---

## Stack

| Layer | Technology |
|-------|-----------|
| Message bus | Apache Kafka (Strimzi on k3s) |
| Vector store | Qdrant 1.18 (HTTP REST, 6333) |
| Embedding | BGE-base-en-v1.5 (dense) + Qdrant/BM25 (sparse) |
| Fusion | Reciprocal Rank Fusion (RRF) via Qdrant prefetch |
| Code chunking | tree-sitter (Python + Go, function-level AST) |
| Inference | vLLM + Qwen2.5-7B-Instruct-AWQ |
| Fine-tuning | QLoRA (PEFT) on Qwen2.5-1.5B-Instruct |
| HA orchestration | Raft leader election (file-based heartbeat) |
| Retrieval service | FastAPI (port 8001) |
| Cluster | k3s, WSL2, RTX 5060 Ti 16GB |

---

## Project Structure

```
multiagent/
├── pipline.py                   # Kafka ingestion pipeline (scan → chunk → embed → upsert)
├── search_service.py            # FastAPI hybrid retrieval service (port 8001)
├── evaluate_hybrid_rag.py       # Recall@K evaluation harness
├── kafka_agent_cluster.py       # Week 4/5: 4-agent pipeline (Planner→Retriever→Coder→Verifier)
├── kafka_parallel_benchmark.py  # Week 6: parallel Coder workers + partition benchmark
├── orchestrator_ha.py           # Week 9: Raft HA orchestrator (run with --id 1/2/3)
├── chaos_test.py                # Week 9: fault injection test
├── finetune_critic.py           # Week 8: QLoRA fine-tuning script
├── eval_ab_test.py              # Week 8: base vs fine-tuned A/B evaluation
├── critic_train_data.jsonl      # Synthetic training data (PASS/FAIL labels)
└── critic_qlora_local_adapter/  # Saved LoRA weights
```

---

## Setup

**Prerequisites**

- k3s cluster with Strimzi Kafka and Qdrant deployed
- `kubectl port-forward svc/my-cluster-kafka-bootstrap -n kafka 9092:9092`
- `kubectl port-forward svc/qdrant -n qdrant 6333:6333`
- vLLM serving Qwen2.5-7B-Instruct-AWQ on port 8000

```bash
pip install kafka-python qdrant-client sentence-transformers fastembed \
            tree-sitter tree-sitter-python tree-sitter-go \
            fastapi uvicorn openai peft transformers bitsandbytes \
            accelerate datasets requests
```

**Kafka topic setup**

```bash
for topic in \
  agent.planner.in agent.retriever.in agent.coder.in agent.verifier.in task.events \
  agent.retriever.in.retry agent.retriever.in.dlq \
  agent.coder.in.retry agent.coder.in.dlq \
  agent.verifier.in.retry agent.verifier.in.dlq
do
  kubectl exec -it my-cluster-kafka-node-pool-0 -n kafka -- \
    bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic $topic --partitions 1 --replication-factor 1 --if-not-exists
done

# Parallel benchmark topic (3 partitions)
kubectl exec -it my-cluster-kafka-node-pool-0 -n kafka -- \
  bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic agent.coder.batch --partitions 3 --replication-factor 1 --if-not-exists
kubectl exec -it my-cluster-kafka-node-pool-0 -n kafka -- \
  bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic agent.coder.results --partitions 3 --replication-factor 1 --if-not-exists
```

**Run ingestion**

```bash
# Point to your real repo
python pipline.py  # edit REAL_REPO_PATH inside the script

# Start retrieval service
uvicorn search_service:app --host 0.0.0.0 --port 8001

# Verify recall
python evaluate_hybrid_rag.py
```

**Run agent pipeline**

```bash
python kafka_agent_cluster.py
```

**Run HA orchestrator (3 terminals)**

```bash
python orchestrator_ha.py --id 1
python orchestrator_ha.py --id 2
python orchestrator_ha.py --id 3

# Chaos test (4th terminal, after a leader is elected)
python chaos_test.py
```

**Run parallel benchmark**

```bash
python kafka_parallel_benchmark.py
```

---

## Fault Recovery Demo

The chaos test validates at-least-once semantics across leader failover:

1. Three orchestrator nodes elect a leader via Raft
2. `chaos_test.py` injects a task and counts down 7 seconds
3. Kill the leader before it commits the Kafka offset
4. A follower detects heartbeat timeout, wins election, reads `committed()` offset
5. New leader seeks to the uncommitted offset and reprocesses the same task


---

## Relation to Other Projects

This system sits at the top of a three-layer portfolio:

**KubeGPU Scheduler** provides the codebase used as RAG corpus (gang scheduling, VRAM-aware bin-packing, deadlock detection). The agent pipeline reviews patches against this codebase.

**LLM Inference Platform** established the vLLM deployment and prefix cache experiments (27× TTFT improvement). This project reuses that inference backend and extends the prefix cache finding to a multi-agent setting.
