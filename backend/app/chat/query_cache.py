"""
Smart Query Cache — Embedding-based semantic cache for SQL queries.

When a question is asked:
1. Embed the question using OpenAI text-embedding-3-small
2. Search Qdrant for similar past questions (cosine similarity)
3. If match > threshold, reuse the proven SQL instead of generating new

This makes repeated/similar questions instant and 100% accurate.
"""
import logging
import time
from pathlib import Path
from app.config import settings

logger = logging.getLogger(__name__)

COLLECTION = "query_cache"
_initialized = False
SIMILARITY_THRESHOLD_VERIFIED = 0.92
SIMILARITY_THRESHOLD_UNVERIFIED = 0.95
MAX_CACHE_SIZE = 5000


def _get_qdrant():
    from qdrant_client import QdrantClient
    persist = Path(settings.data_dir) / "qdrant"
    persist.mkdir(parents=True, exist_ok=True)
    return QdrantClient(path=str(persist))


def _embed(text):
    from openai import OpenAI
    client = OpenAI(api_key=settings.openai_api_key)
    response = client.embeddings.create(
        model=settings.embedding_model,
        input=[text],
    )
    return response.data[0].embedding


def _patch_qdrant_models():
    try:
        from qdrant_client.http.models import CreateCollection
        if hasattr(CreateCollection, "model_config"):
            CreateCollection.model_config["extra"] = "ignore"
    except Exception:
        pass


def init_cache():
    """Create the query_cache collection if it doesn't exist."""
    global _initialized
    if _initialized:
        return
    _patch_qdrant_models()
    try:
        from qdrant_client.models import Distance, VectorParams
        client = _get_qdrant()
        collections = [c.name for c in client.get_collections().collections]
        if COLLECTION not in collections:
            client.create_collection(
                collection_name=COLLECTION,
                vectors_config=VectorParams(
                    size=settings.embedding_dimensions,
                    distance=Distance.COSINE,
                ),
            )
            logger.info("Query cache collection created")
        else:
            info = client.get_collection(COLLECTION)
            logger.info(f"Query cache loaded: {info.points_count} cached queries")
        _initialized = True
    except Exception as e:
        logger.warning(f"Query cache init failed: {e}")


def cache_query(question, sql, feedback=None):
    """Save a successful question→SQL pair to the embedding cache."""
    try:
        init_cache()
        from qdrant_client.models import PointStruct
        client = _get_qdrant()

        vector = _embed(question.strip().lower())

        existing = client.query_points(
            collection_name=COLLECTION,
            query=vector,
            limit=1,
            with_payload=True,
        )
        if existing.points and existing.points[0].score >= 0.98:
            point = existing.points[0]
            old_count = point.payload.get("used_count", 0)
            old_feedback = point.payload.get("feedback")
            new_feedback = feedback or old_feedback
            client.set_payload(
                collection_name=COLLECTION,
                payload={
                    "sql": sql,
                    "feedback": new_feedback,
                    "used_count": old_count + 1,
                    "updated_at": time.time(),
                },
                points=[point.id],
            )
            logger.debug(f"Query cache UPDATED: '{question[:50]}' (count={old_count + 1})")
            return

        info = client.get_collection(COLLECTION)
        point_id = info.points_count

        client.upsert(
            collection_name=COLLECTION,
            points=[PointStruct(
                id=point_id,
                vector=vector,
                payload={
                    "question": question.strip(),
                    "sql": sql,
                    "feedback": feedback,
                    "used_count": 1,
                    "created_at": time.time(),
                    "updated_at": time.time(),
                },
            )],
        )
        logger.info(f"Query cache SAVED: '{question[:60]}' (id={point_id})")
    except Exception as e:
        logger.warning(f"Query cache save failed: {e}")


def find_cached_query(question):
    """
    Search for a semantically similar cached query.
    Returns (sql, score, cached_question) or (None, 0, None).
    """
    try:
        init_cache()
        client = _get_qdrant()

        info = client.get_collection(COLLECTION)
        if info.points_count == 0:
            return None, 0, None

        vector = _embed(question.strip().lower())

        results = client.query_points(
            collection_name=COLLECTION,
            query=vector,
            limit=3,
            with_payload=True,
        )

        if not results.points:
            return None, 0, None

        for point in results.points:
            score = point.score
            sql = point.payload.get("sql", "")
            feedback = point.payload.get("feedback")
            cached_q = point.payload.get("question", "")
            used_count = point.payload.get("used_count", 0)

            if not sql or not sql.strip().upper().startswith("SELECT"):
                continue

            if feedback not in ("good", "corrected"):
                continue

            threshold = SIMILARITY_THRESHOLD_VERIFIED

            if used_count >= 3:
                threshold -= 0.02

            if score >= threshold:
                client.set_payload(
                    collection_name=COLLECTION,
                    payload={"used_count": used_count + 1, "updated_at": time.time()},
                    points=[point.id],
                )
                logger.info(
                    f"Query cache HIT (score={score:.3f}, fb={feedback}, uses={used_count}): "
                    f"'{cached_q[:50]}' → reused for '{question[:50]}'"
                )
                return sql, score, cached_q

        best = results.points[0]
        logger.debug(
            f"Query cache MISS (best={best.score:.3f}, need>={SIMILARITY_THRESHOLD_VERIFIED}): "
            f"'{best.payload.get('question', '')[:50]}'"
        )
        return None, 0, None

    except Exception as e:
        logger.warning(f"Query cache lookup failed: {e}")
        return None, 0, None


def update_feedback(question, feedback):
    """Update feedback for the closest matching cached query."""
    try:
        init_cache()
        client = _get_qdrant()
        vector = _embed(question.strip().lower())

        results = client.query_points(
            collection_name=COLLECTION,
            query=vector,
            limit=1,
            with_payload=True,
        )
        if results.points and results.points[0].score >= 0.95:
            point = results.points[0]
            client.set_payload(
                collection_name=COLLECTION,
                payload={"feedback": feedback, "updated_at": time.time()},
                points=[point.id],
            )
            logger.info(f"Query cache feedback '{feedback}' for: '{question[:50]}'")
    except Exception as e:
        logger.warning(f"Query cache feedback update failed: {e}")


async def seed_from_learning_memory():
    """Seed the embedding cache from existing good queries in learning_memory."""
    try:
        init_cache()
        client = _get_qdrant()
        info = client.get_collection(COLLECTION)
        if info.points_count > 0:
            logger.info(f"Query cache already has {info.points_count} entries, skipping seed")
            return 0

        from app.database.engine import async_session
        from sqlalchemy import text
        async with async_session() as session:
            result = await session.execute(text("""
                SELECT DISTINCT ON (question) question, sql_query, feedback
                FROM learning_memory
                WHERE sql_query IS NOT NULL AND sql_query != ''
                AND (feedback = 'good' OR feedback = 'corrected')
                ORDER BY question, created_at DESC
                LIMIT 500
            """))
            rows = result.fetchall()

        if not rows:
            logger.info("No good queries in learning_memory to seed cache")
            return 0

        count = 0
        for row in rows:
            question, sql, feedback = row[0], row[1], row[2]
            if sql and sql.strip().upper().startswith("SELECT"):
                cache_query(question, sql, feedback=feedback)
                count += 1

        logger.info(f"Seeded query cache with {count} proven queries from learning_memory")
        return count
    except Exception as e:
        logger.warning(f"Query cache seeding failed: {e}")
        return 0


def get_stats():
    """Get cache statistics."""
    try:
        init_cache()
        client = _get_qdrant()
        info = client.get_collection(COLLECTION)
        return {
            "total_cached": info.points_count,
            "collection": COLLECTION,
        }
    except Exception:
        return {"total_cached": 0, "collection": COLLECTION}
