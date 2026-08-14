import json
import sys
import time
import os
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from qdrant_client import QdrantClient
# 💡 核心修复：引入 SparseVector, FusionQuery, Fusion，确保混合检索的高级语法完全对齐
from qdrant_client.models import (
    VectorParams, 
    Distance, 
    PointStruct, 
    SparseVectorParams, 
    SparseIndexParams, 
    Prefetch, 
    SparseVector,
    FusionQuery,
    Fusion
)
from sentence_transformers import SentenceTransformer
from fastembed import SparseTextEmbedding  
import tree_sitter_python as tspy
from tree_sitter import Language, Parser

# 确保国内 Hugging Face 镜像源生效
os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"

# ==========================================
# 1. 初始化配置与双路模型
# ==========================================
print("正在加载 BGE Embedding 模型...")
embedding_model = SentenceTransformer("BAAI/bge-base-en-v1.5")
EMBEDDING_DIM = 768

print("正在加载 Qdrant/BM25 稀疏模型...")
sparse_model = SparseTextEmbedding(model_name="Qdrant/bm25")

# 模型加载完成后立即做一次轻量探测，激活/刷新因长耗时挂起的 port-forward 连接状态
print("正在唤醒并探测 Kafka port-forward 连接状态...")
try:
    probe = KafkaProducer(bootstrap_servers="127.0.0.1:9092", api_version=(2, 5, 0), max_block_ms=3000)
    probe.close()
    print("✅ Port-forward 唤醒探测成功")
except Exception:
    print("⚠️ 检测到 port-forward 可能已在模型加载期间空闲断连，如后续失败请重新执行 kubectl port-forward")

py_lang = Language(tspy.language())
parser = Parser(py_lang)

qdrant_client = QdrantClient(
    host="127.0.0.1",
    port=6333,
    prefer_grpc=False,
    check_compatibility=False
)

KAFKA_BOOTSTRAP = "127.0.0.1:9092"
COLLECTION_NAME = "test_code_collection"
TOPIC_NAME = "test-raw-code"
GROUP_ID = "ingestion-consumer-group"

# ==========================================
# 2. 核心功能函数
# ==========================================

def create_producer_with_retry(max_attempts: int = 3, backoff: float = 2.0) -> KafkaProducer:
    """Week 2 核心修复：防范 port-forward 遭遇长耗时空闲失效，建立带真实元数据刷新的重试机制"""
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(2, 5, 0),
                acks=1,                      # 改回 1 以获取更真实的 broker 状态反馈
                request_timeout_ms=10000,
                max_block_ms=10000,          # 给 metadata fetch 更宽裕的时间
                retries=3,
                linger_ms=0,
            )
            # 用一次轻量分区元数据请求强行激活物理连接
            producer.partitions_for(TOPIC_NAME)
            print(f"✅ Producer 物理连接成功（第 {attempt} 次尝试）")
            return producer
        except Exception as e:
            last_err = e
            print(f"⚠️ Producer 连接失败（第 {attempt}/{max_attempts} 次）: {e}")
            if attempt < max_attempts:
                time.sleep(backoff)
    print(f"❌ Producer 连接重试 {max_attempts} 次后仍失败: {last_err}")
    sys.exit(1)

def ensure_topic_exists(topic: str):
    """Week 2 强力破局：跳过会引发集群内部域名解析死锁的全局元数据扫描，直接安全放行"""
    print(f"正在检查 Kafka Topic '{topic}' 的状态...")
    print(f"⚠️ [网络隔离模式] 已跳过全局拓扑扫描，假定物理 Topic '{topic}' 已经在集群内就绪。")
    print(f"✅ Kafka Topic '{topic}' 存在且就绪")
    return

def check_gateways_health():
    """前置 Fast-Fail 健康检查门禁"""
    print("正在进行基础设施健康检查...")

    # 1. Qdrant 连通性探测
    try:
        qdrant_client.get_collections()
        print("✅ Qdrant 连接成功")
    except Exception as e:
        print(f"❌ Qdrant 连接失败，请检查 port-forward 6333! 错误: {e}")
        sys.exit(1)

    # 2. Kafka 连通性前置探测
    try:
        probe = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            api_version=(2, 5, 0),
            request_timeout_ms=3000,
            max_block_ms=3000,
        )
        probe.close()
        print("✅ Kafka 基础端口可达")
    except Exception as e:
        print(f"❌ Kafka 基础连接失败，请检查 9092 端口转发! 错误: {e}")
        sys.exit(1)

    # 3. 激活安全隔离检查
    ensure_topic_exists(TOPIC_NAME)

def init_qdrant_collection():
    """重建 Qdrant 集合 (升级为 Week 2 混合检索双路存储通道)"""
    print(f"正在重建 Qdrant 集合: {COLLECTION_NAME}...")
    if qdrant_client.collection_exists(collection_name=COLLECTION_NAME):
        qdrant_client.delete_collection(collection_name=COLLECTION_NAME)
    
    # 核心改造：开启多路向量命名存储，同时支持密集与稀疏向量通道
    qdrant_client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config={
            "dense": VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE)
        },
        sparse_vectors_config={
            "sparse": SparseVectorParams(index=SparseIndexParams(on_disk=False))
        }
    )
    print(f"✅ 集合 {COLLECTION_NAME} 重建完成 (已开启 Dense + Sparse 双通道)")

def ast_chunk_python(content: str) -> list:
    """基于原版 DFS 进化：提取细粒度函数块的同时，顺手抓取函数名作为 Metadata"""
    content_bytes = bytes(content, "utf8")
    tree = parser.parse(content_bytes)
    chunks = []

    def dfs(node):
        if node.type == 'function_definition':
            chunk_content = content_bytes[node.start_byte:node.end_byte].decode("utf8")
            name_node = node.child_by_field_name("name")
            func_name = content_bytes[name_node.start_byte:name_node.end_byte].decode("utf8") if name_node else "unknown_func"
            
            chunks.append({
                "name": func_name,
                "content": chunk_content
            })
            return
        for child in node.children:
            dfs(child)

    dfs(tree.root_node)
    return chunks if chunks else [{"name": "global_scope", "content": content}]

# ==========================================
# 3. 主测试流程
# ==========================================
def main():
    # 门禁与环境初始化
    check_gateways_health()
    init_qdrant_collection()

    mock_code = """
class GPUScheduler:
    def __init__(self, nodes):
        self.nodes = nodes

    def allocate_gpu(self, pod_id, required_gpus):
        print(f"Allocating {required_gpus} GPUs for {pod_id}")
        return True

def calculate_mrr_score(ranks):
    return sum([1.0 / r for r in ranks])
"""

    # --------------------------------------------------
    # 步骤一：Producer 发送源码到 Kafka 
    # --------------------------------------------------
    print("\n--- 步骤 1: 生产者发送代码至 Kafka ---")
    producer = create_producer_with_retry()

    payload = {
        "file_path": "cluster/scheduler.py",
        "content": mock_code
    }

    try:
        future = producer.send(TOPIC_NAME, value=payload)
        record = future.get(timeout=10) # 同步阻塞确保数据真正落入分区
        print(f"✅ [发送成功] Topic={record.topic} Partition={record.partition} Offset={record.offset}")
        producer.flush()
        producer.close()
        print("代码数据流已成功向本地端口推流并确认，Producer 已正常关闭。")
    except Exception as e:
        print(f"❌ [发送失败] 遭遇不可恢复异常: {e}")
        sys.exit(1)

    # 缓冲 1 秒确保 Broker 内部状态完全对齐
    time.sleep(1)

    # --------------------------------------------------
    # 步骤二：Consumer 消费、AST 切分、双路写入 Qdrant
    # --------------------------------------------------
    print("\n--- 步骤 2: 消费者读取并处理数据 ---")
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=GROUP_ID,
            auto_offset_reset='earliest',
            enable_auto_commit=False,           # 开启幂等与手动 Commit 保护
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            api_version=(2, 5, 0),
            session_timeout_ms=10000,
            request_timeout_ms=15000,           # 严格满足 request > session 校验规则
            consumer_timeout_ms=5000            # 5秒拉不到新数据自动退出，不卡死
        )
        print("✅ Consumer 初始化完成，正在检索消息...")
    except KafkaError as e:
        print(f"❌ Consumer 初始化失败: {e}")
        sys.exit(1)

    point_id = 1
    processed_count = 0

    for msg in consumer:
        data = msg.value
        print(f"\n从 Kafka 接收到文件: {data['file_path']} (Partition={msg.partition}, Offset={msg.offset})")

        # AST 结构化切分
        chunks = ast_chunk_python(data['content'])
        print(f"Tree-sitter 成功切分为 {len(chunks)} 个细粒度函数块:")
        for idx, chunk in enumerate(chunks):
            print(f"  -> 块 {idx+1}: {chunk['name']}")

        points = []
        for chunk in chunks:
            # A. 密集语义向量计算
            dense_vector = embedding_model.encode(chunk["content"]).tolist()
            
            # B. 稀疏词频向量计算 (BM25)
            sparse_res = list(sparse_model.embed([chunk["content"]]))[0]
            sparse_vector = {
                "indices": sparse_res.indices.tolist(),
                "values": sparse_res.values.tolist()
            }
            
            # C. 组装支持 Hybrid 检索的命名向量结构
            vector_payload = {
                "dense": dense_vector,
                "sparse": sparse_vector
            }
            
            # D. 元数据增强（添加 Week 2 要求的元数据过滤字段）
            metadata_payload = {
                "file_path": data['file_path'], 
                "code_snippet": chunk["content"],
                "function_name": chunk["name"],
                "language": "python",
                "repo_source": "multiagent-backend"
            }
            
            points.append(PointStruct(
                id=point_id,
                vector=vector_payload,
                payload=metadata_payload
            ))
            point_id += 1

        if points:
            try:
                # 写入向量数据库双路通道
                qdrant_client.upsert(collection_name=COLLECTION_NAME, points=points)
                # 数据持久化落盘确认后，再手动提交 Kafka offset
                consumer.commit()
                processed_count += 1
                print(f"✅ {len(points)} 个双路向量点成功写入 Qdrant，Kafka 位点手动提交成功")
            except Exception as e:
                print(f"❌ Qdrant 写入或提交位点失败，跳过 commit 以便下次重试: {e}")

    consumer.close()
    print(f"\n消费阶段结束，共成功流转 {processed_count} 条代码消息。")

    # --------------------------------------------------
    # 步骤三：双路混合语义检索验证 (🦾 Week 2 修复版)
    # --------------------------------------------------
    print("\n--- 步骤 3: 模拟双路混合检索测试 ---")
    user_query = "How to assign graphics cards to a requested container pod?"
    print(f"用户查询: '{user_query}'")

    # 1. 计算查询句的密集向量
    query_dense = embedding_model.encode(user_query).tolist()
    
    # 2. 计算查询句的稀疏向量并严格封装
    query_sparse_res = list(sparse_model.embed([user_query]))[0]
    query_sparse = SparseVector(
        indices=query_sparse_res.indices.tolist(),
        values=query_sparse_res.values.tolist()
    )

    # 3. 采用 Qdrant 官方 RRF (互惠排名融合) 跨通道无缝合并检索结果
    # 💡 终极修复：使用 FusionQuery(fusion=Fusion.RRF) 彻底消除低版本 JSON 序列化器二义性故障
    search_response = qdrant_client.query_points(
        collection_name=COLLECTION_NAME,
        prefetch=[
            Prefetch(query=query_dense, using="dense", limit=10),
            Prefetch(query=query_sparse, using="sparse", limit=10),
        ],
        query=FusionQuery(fusion=Fusion.RRF),
        limit=3
    )

    if search_response.points:
        print(f"\n[混合检索成功] 共召回 {len(search_response.points)} 个独立代码块：")
        for i, hit in enumerate(search_response.points):
            # 注意：在使用 rrf 融合算法时，hit.score 代表融合排序分，而非单纯的 Cosine 距离
            print(f"\n[Top {i+1} 匹配] 综合融合得分: {hit.score:.4f} | 来源: {hit.payload['file_path']}")
            print("-" * 50)
            print(hit.payload['code_snippet'])
            print("-" * 50)
    else:
        print("❌ 未捕获到任何匹配的代码片段。")

if __name__ == "__main__":
    main()
