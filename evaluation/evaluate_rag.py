import numpy as np
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# 初始化
embedding_model = SentenceTransformer("BAAI/bge-base-en-v1.5")
qdrant_client = QdrantClient(host="127.0.0.1", port=6333, prefer_grpc=False)
COLLECTION_NAME = "test_code_collection"

# 1. 注入手写的评测集
eval_dataset = [
    {"query": "How to allocate GPU for a specified pod?", "expected": "def allocate_gpu"},
    {"query": "How to assign graphics cards to a requested container pod?", "expected": "def allocate_gpu"},
    {"query": "集群中如何为容器调度分配显卡资源？", "expected": "def allocate_gpu"},
    {"query": "Where is the logic that decides if a pod can get accelerator hardware?", "expected": "def allocate_gpu"},
    {"query": "How does the GPU scheduler set up its initial cluster topology?", "expected": "def __init__"},
    {"query": "Show me the implement of mean reciprocal rank calculation.", "expected": "def calculate_mrr_score"},
    {"query": "Which function processes rank lists and sums their reciprocal values?", "expected": "def calculate_mrr_score"}
    # ... 可自由追加其余表格项
]

hit_count = 0

print("=== 开始检索质量自动化评测 ===")
for idx, eval_item in enumerate(eval_dataset):
    q_vec = embedding_model.encode(eval_item["query"]).tolist()
    
    # 检索 Top 1
    res = qdrant_client.query_points(
        collection_name=COLLECTION_NAME,
        query=q_vec,
        limit=1
    )
    
    if res.points:
        top_hit = res.points[0]
        code_snippet = top_hit.payload["code_snippet"]
        score = top_hit.score
        
        # 验证预期的函数特征名是否包含在召回的代码块内
        is_hit = eval_item["expected"] in code_snippet
        if is_hit:
            hit_count += 1
            status = "✅ PASS"
        else:
            status = "❌ FAIL"
            
        print(f"[{idx+1:02d}] 查询: '{eval_item['query']}' -> Top1 相似度: {score:.4f} | {status}")
    else:
        print(f"[{idx+1:02d}] 查询: '{eval_item['query']}' -> ❌ 未检索到任何数据")

# 2. 计算最终硬核指标
hit_rate_at_1 = (hit_count / len(eval_dataset)) * 100
print("=" * 50)
print(f"📊 评测最终得分 Hit Rate@1: {hit_rate_at_1:.2f}%")
print("=" * 50)
