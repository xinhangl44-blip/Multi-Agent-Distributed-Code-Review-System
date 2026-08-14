from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, SparseVectorParams, SparseIndexParams

def init_hybrid_collection():
    # 1. 连接本地 Qdrant
    client = QdrantClient(host="127.0.0.1", port=6333)
    collection_name = "test_code_collection"
    
    print(f"正在重构 Qdrant 集合: {collection_name} 为混合检索模式...")
    
    # 2. 如果存在旧集合则删除
    if client.collection_exists(collection_name):
        client.delete_collection(collection_name)
        
    # 3. 创建支持双路向量的新集合
    client.create_collection(
        collection_name=collection_name,
        # A. 配置密集向量通道 (BGE-base-en-v1.5 是 768 维)
        vectors_config={
            "dense": VectorParams(
                size=768,
                distance=Distance.COSINE
            )
        },
        # B. 配置稀疏向量通道 (开启 BM25 文本匹配)
        sparse_vectors_config={
            "sparse": SparseVectorParams(
                index=SparseIndexParams(
                    on_disk=False  # 测试环境数据量小，直接放内存提速
                )
            )
        }
    )
    print(f"✅ 混合检索集合 {collection_name} 初始化成功！")

if __name__ == "__main__":
    init_hybrid_collection()
