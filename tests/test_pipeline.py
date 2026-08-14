import json
import sys
import time
import os
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from qdrant_client import QdrantClient
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

os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"

embedding_model = SentenceTransformer("BAAI/bge-base-en-v1.5")
EMBEDDING_DIM = 768
sparse_model = SparseTextEmbedding(model_name="Qdrant/bm25")

try:
    probe = KafkaProducer(bootstrap_servers="127.0.0.1:9092", api_version=(2, 5, 0), max_block_ms=3000)
    probe.close()
except Exception:
    pass

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

def create_producer_with_retry(max_attempts: int = 3, backoff: float = 2.0) -> KafkaProducer:
    for attempt in range(1, max_attempts + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(2, 5, 0),
                acks=1,
                request_timeout_ms=10000,
                max_block_ms=10000,
                retries=3,
                linger_ms=0,
            )
            producer.partitions_for(TOPIC_NAME)
            return producer
        except Exception:
            if attempt < max_attempts:
                time.sleep(backoff)
    sys.exit(1)

def check_gateways_health():
    try:
        qdrant_client.get_collections()
    except Exception:
        sys.exit(1)

    try:
        probe = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            api_version=(2, 5, 0),
            request_timeout_ms=3000,
            max_block_ms=3000,
        )
        probe.close()
    except Exception:
        sys.exit(1)

def init_qdrant_collection():
    if qdrant_client.collection_exists(collection_name=COLLECTION_NAME):
        qdrant_client.delete_collection(collection_name=COLLECTION_NAME)
    
    qdrant_client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config={
            "dense": VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE)
        },
        sparse_vectors_config={
            "sparse": SparseVectorParams(index=SparseIndexParams(on_disk=False))
        }
    )

def ast_chunk_python(content: str) -> list:
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

def main():
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

    producer = create_producer_with_retry()
    payload = {
        "file_path": "cluster/scheduler.py",
        "content": mock_code
    }

    try:
        future = producer.send(TOPIC_NAME, value=payload)
        future.get(timeout=10)
        producer.flush()
        producer.close()
    except Exception:
        sys.exit(1)

    time.sleep(1)

    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=GROUP_ID,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            api_version=(2, 5, 0),
            session_timeout_ms=10000,
            request_timeout_ms=15000,
            consumer_timeout_ms=5000
        )
    except KafkaError:
        sys.exit(1)

    point_id = 1
    for msg in consumer:
        data = msg.value
        chunks = ast_chunk_python(data['content'])
        points = []
        for chunk in chunks:
            dense_vector = embedding_model.encode(chunk["content"]).tolist()
            sparse_res = list(sparse_model.embed([chunk["content"]]))[0]
            sparse_vector = {
                "indices": sparse_res.indices.tolist(),
                "values": sparse_res.values.tolist()
            }
            vector_payload = {
                "dense": dense_vector,
                "sparse": sparse_vector
            }
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
                qdrant_client.upsert(collection_name=COLLECTION_NAME, points=points)
                consumer.commit()
            except Exception:
                pass

    consumer.close()

    user_query = "How to assign graphics cards to a requested container pod?"
    query_dense = embedding_model.encode(user_query).tolist()
    query_sparse_res = list(sparse_model.embed([user_query]))[0]
    query_sparse = SparseVector(
        indices=query_sparse_res.indices.tolist(),
        values=query_sparse_res.values.tolist()
    )

    search_response = qdrant_client.query_points(
        collection_name=COLLECTION_NAME,
        prefetch=[
            Prefetch(query=query_dense, using="dense", limit=10),
            Prefetch(query=query_sparse, using="sparse", limit=10),
        ],
        query=FusionQuery(fusion=Fusion.RRF),
        limit=3
    )

    for i, hit in enumerate(search_response.points):
        print(f"Top {i+1} [{hit.score:.4f}] {hit.payload['file_path']}")
        print(hit.payload['code_snippet'])

if __name__ == "__main__":
    main()
