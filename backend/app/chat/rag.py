"""
RAG Module — OpenAI Embeddings + Qdrant Vector Search

This adds semantic search on top of SOQL:
- SOQL: "show students with Verbal Confirmation" → exact WHERE filter
- RAG:  "find students like Adithya who are doing well" → semantic similarity

Uses OpenAI text-embedding-3-small (fast, cheap, 1536 dimensions).
Cost: $0.02 per 1M tokens (~$0.001 to embed 1000 records).
"""
import json, logging, pickle
from pathlib import Path
from app.config import settings

logger = logging.getLogger(__name__)

_qdrant_client = None
COLLECTION = "salesforce_rag"


def _get_openai():
    from openai import OpenAI
    return OpenAI(api_key=settings.openai_api_key)


def _get_qdrant():
    global _qdrant_client
    if _qdrant_client is None:
        from qdrant_client import QdrantClient
        persist = Path(settings.data_dir) / "qdrant"
        persist.mkdir(parents=True, exist_ok=True)
        _qdrant_client = QdrantClient(path=str(persist))
        logger.info(f"Qdrant initialized at {persist}")
    return _qdrant_client


def embed_texts(texts, batch_size=100):
    """Embed texts using OpenAI API. Fast, cheap, high quality."""
    client = _get_openai()
    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        response = client.embeddings.create(
            model=settings.embedding_model,
            input=batch,
        )
        all_embeddings.extend([r.embedding for r in response.data])
        if (i + batch_size) % 500 == 0:
            logger.info(f"  Embedded {i + batch_size}/{len(texts)}")
    return all_embeddings


def embed_query(text):
    """Embed a single query."""
    client = _get_openai()
    response = client.embeddings.create(
        model=settings.embedding_model,
        input=[text],
    )
    return response.data[0].embedding


def index_records(records_by_object):
    """
    Index all records into Qdrant.
    records_by_object: {"Student__c": [{"Id": ..., "Name": ..., ...}, ...]}
    """
    from qdrant_client.models import Distance, VectorParams, PointStruct

    client = _get_qdrant()

    # Reset collection
    try:
        client.delete_collection(COLLECTION)
    except:
        pass

    client.create_collection(
        collection_name=COLLECTION,
        vectors_config=VectorParams(size=settings.embedding_dimensions, distance=Distance.COSINE),
    )

    total_indexed = 0

    for obj_name, records in records_by_object.items():
        if not records:
            continue

        # Convert records to text
        texts = []
        for r in records:
            parts = [f"{obj_name}:"]
            for k, v in r.items():
                if k == "attributes" or not v or str(v).strip() == "":
                    continue
                clean = k.replace("__c", "").replace("_", " ")
                parts.append(f"{clean}: {v}")
            texts.append(", ".join(parts)[:1000])  # Truncate long records

        logger.info(f"  Embedding {len(texts)} {obj_name} records...")
        embeddings = embed_texts(texts)

        # Upsert into Qdrant
        points = []
        for i, (text, embedding, record) in enumerate(zip(texts, embeddings, records)):
            points.append(PointStruct(
                id=total_indexed + i,
                vector=embedding,
                payload={
                    "text": text,
                    "sf_object": obj_name,
                    "sf_id": record.get("Id", ""),
                    "name": record.get("Name", ""),
                },
            ))

        # Batch upsert
        for j in range(0, len(points), 100):
            client.upsert(collection_name=COLLECTION, points=points[j:j + 100])

        total_indexed += len(records)
        logger.info(f"  Indexed {len(records)} {obj_name} records")

    logger.info(f"RAG index complete: {total_indexed} total records")
    return total_indexed


def search(query, top_k=10, object_filter=None):
    """Semantic search — find records similar to the query."""
    client = _get_qdrant()

    query_vector = embed_query(query)

    filter_condition = None
    if object_filter:
        from qdrant_client.models import Filter, FieldCondition, MatchValue
        filter_condition = Filter(must=[
            FieldCondition(key="sf_object", match=MatchValue(value=object_filter))
        ])

    results = client.query_points(
        collection_name=COLLECTION,
        query=query_vector,
        limit=top_k,
        query_filter=filter_condition,
        with_payload=True,
    )

    return [
        {
            "text": r.payload.get("text", ""),
            "score": r.score,
            "sf_object": r.payload.get("sf_object", ""),
            "name": r.payload.get("name", ""),
        }
        for r in results.points
    ]


def is_indexed():
    """Check if RAG index exists."""
    try:
        client = _get_qdrant()
        info = client.get_collection(COLLECTION)
        return info.points_count > 0
    except:
        return False
