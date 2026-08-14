import time
import os
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, Prefetch, SparseVector, FusionQuery, Fusion
from sentence_transformers import SentenceTransformer
from fastembed import SparseTextEmbedding

app = FastAPI(title="Multi-Agent RAG Hybrid Retrieval Service (Go Version)")

# 1. 基础模型初始化与预热
os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"
embedding_model = SentenceTransformer("BAAI/bge-base-en-v1.5")
sparse_model = SparseTextEmbedding(model_name="Qdrant/bm25")

# 🦾 战略同步：放弃 6334 gRPC，通过 url 参数完全锁死 6333 纯 HTTP 通道
print("正在连接 Qdrant 纯 HTTP 稳定通道 (127.0.0.1:6333)...")
qdrant_client = QdrantClient(
    url="http://127.0.0.1:6333",
    check_compatibility=False
)
COLLECTION_NAME = "test_code_collection"

# 消除冷启动长尾延迟
for _ in range(3):
    _ = embedding_model.encode("warmup")
    _ = list(sparse_model.embed(["warmup"]))

query_latencies = []

class SearchRequest(BaseModel):
    query: str
    limit: Optional[int] = 5
    file_path: Optional[str] = None
    # 🦾 关键对齐：默认漏斗条件全部切换为真实落库的 Go 属性，允许传 None 进行松绑测试
    language: Optional[str] = "go"
    repo_source: Optional[str] = "kube-gpu-scheduler"

@app.middleware("http")
async def record_latency_baseline(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000
    if request.url.path == "/retrieve":
        query_latencies.append(process_time)
    return response

@app.post("/retrieve")
async def retrieve_code(req: SearchRequest):
    try:
        # 生成双路向量
        dense_vector = embedding_model.encode(req.query).tolist()
        sparse_res = list(sparse_model.embed([req.query]))[0]
        sparse_vector = SparseVector(
            indices=sparse_res.indices.tolist(),
            values=sparse_res.values.tolist()
        )

        # 🦾 坚守元数据过滤：当参数不为 None 时执行硬过滤，毫米级精确定位 Go 函数
        filter_conditions = []
        if req.language:
            filter_conditions.append(FieldCondition(key="language", match=MatchValue(value=req.language)))
        if req.repo_source:
            filter_conditions.append(FieldCondition(key="repo_source", match=MatchValue(value=req.repo_source)))
        if req.file_path:
            filter_conditions.append(FieldCondition(key="file_path", match=MatchValue(value=req.file_path)))

        query_filter = Filter(must=filter_conditions) if filter_conditions else None

        # 双路混合检索 (RRF 融合)
        search_response = qdrant_client.query_points(
            collection_name=COLLECTION_NAME,
            prefetch=[
                Prefetch(query=dense_vector, using="dense", filter=query_filter, limit=req.limit * 2),
                Prefetch(query=sparse_vector, using="sparse", filter=query_filter, limit=req.limit * 2),
            ],
            query=FusionQuery(fusion=Fusion.RRF),
            limit=req.limit
        )

        results = []
        for hit in search_response.points:
            results.append({
                "id": hit.id,
                "score": hit.score,
                "file_path": hit.payload.get("file_path"),
                "function_name": hit.payload.get("function_name"),
                "code_snippet": hit.payload.get("code_snippet")
            })
        return {"status": "success", "data": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
async def get_latency_metrics():
    if not query_latencies:
        return {"message": "No query recorded yet."}
    latencies = np.array(query_latencies)
    return {
        "total_queries": len(latencies),
        "P50_latency_ms": float(np.percentile(latencies, 50)),
        "P99_latency_ms": float(np.percentile(latencies, 99)),
    }
