import numpy as np
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

embedding_model = SentenceTransformer("BAAI/bge-base-en-v1.5")
qdrant_client = QdrantClient(host="127.0.0.1", port=6333, prefer_grpc=False)
COLLECTION_NAME = "test_code_collection"
eval_dataset = [
    {"query": "How to allocate GPU for a specified pod?", "expected": "def allocate_gpu"},
    {"query": "How to assign graphics cards to a requested container pod?", "expected": "def allocate_gpu"},
    {"query": "How are graphics card resources allocated for container scheduling in the cluster?", "expected": "def allocate_gpu"},
    {"query": "Where is the logic that decides if a pod can get accelerator hardware?", "expected": "def allocate_gpu"},
    {"query": "How does the GPU scheduler set up its initial cluster topology?", "expected": "def __init__"},
    {"query": "Show me the implement of mean reciprocal rank calculation.", "expected": "def calculate_mrr_score"},
    {"query": "Which function processes rank lists and sums their reciprocal values?", "expected": "def calculate_mrr_score"}
]
hit_count = 0
print("=== Starting Retrieval Quality Automated Evaluation ===")
for idx, eval_item in enumerate(eval_dataset):
    q_vec = embedding_model.encode(eval_item["query"]).tolist()
    res = qdrant_client.query_points(
        collection_name=COLLECTION_NAME,
        query=q_vec,
        limit=1
    )
    if res.points:
        top_hit = res.points[0]
        code_snippet = top_hit.payload["code_snippet"]
        score = top_hit.score
        is_hit = eval_item["expected"] in code_snippet
        if is_hit:
            hit_count += 1
            status = "PASS"
        else:
            status = "FAIL"
        print(f"[{idx+1:02d}] Query: '{eval_item['query']}' -> Top1 Similarity: {score:.4f} | {status}")
    else:
        print(f"[{idx+1:02d}] Query: '{eval_item['query']}' -> ❌ No data retrieved")
hit_rate_at_1 = (hit_count / len(eval_dataset)) * 100
print("=" * 50)
print(f"Final Benchmark Score Hit Rate@1: {hit_rate_at_1:.2f}%")
print("=" * 50)
