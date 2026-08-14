from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, SparseVectorParams, SparseIndexParams

def init_hybrid_collection():
    client = QdrantClient(host="127.0.0.1", port=6333)
    collection_name = "test_code_collection"
    print(f"Rebuilding Qdrant collection: {collection_name} for hybrid search mode...")
    if client.collection_exists(collection_name):
        client.delete_collection(collection_name)
    client.create_collection(
        collection_name=collection_name,
        vectors_config={
            "dense": VectorParams(
                size=768,
                distance=Distance.COSINE
            )
        },
        sparse_vectors_config={
            "sparse": SparseVectorParams(
                index=SparseIndexParams(
                    on_disk=False
                )
            )
        }
    )
    print(f"✅ Hybrid search collection {collection_name} initialized successfully!")

if __name__ == "__main__":
    init_hybrid_collection()
