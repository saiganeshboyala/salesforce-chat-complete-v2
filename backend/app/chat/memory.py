"""
Learning Memory — stored in PostgreSQL.

Shared SQL learning pool + per-user chat history, all in the database.
Tables are auto-created on first use.
"""
import logging
from datetime import datetime
from sqlalchemy import text
from app.database.engine import async_session

logger = logging.getLogger(__name__)

MAX_MEMORY = 2000
MAX_USER_HISTORY = 500


# ── Table Creation (auto-migrate) ──────────────────

_tables_ensured = False

async def _ensure_tables():
    global _tables_ensured
    if _tables_ensured:
        return
    async with async_session() as session:
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS learning_memory (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT NOW(),
                question TEXT NOT NULL,
                sql_query TEXT,
                answer TEXT,
                route VARCHAR(20) DEFAULT 'SQL',
                feedback VARCHAR(20),
                username VARCHAR(100),
                used_count INTEGER DEFAULT 0
            )
        """))
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS chat_history (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT NOW(),
                username VARCHAR(100) NOT NULL,
                question TEXT NOT NULL,
                answer TEXT,
                sql_query TEXT,
                route VARCHAR(20)
            )
        """))
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_learning_feedback ON learning_memory(feedback)
        """))
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_chat_history_user ON chat_history(username, created_at DESC)
        """))
        await session.commit()
    _tables_ensured = True


# ── Sync versions for non-async contexts (test runner) ──

def _get_sync_connection():
    """Get a sync psycopg2 connection for non-async contexts."""
    import os
    try:
        import psycopg2
    except ImportError:
        return None

    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("DATABASE_URL="):
                        db_url = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
    if not db_url:
        db_url = "postgresql://postgres:postgres@localhost:5432/fyxo"

    db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")
    try:
        return psycopg2.connect(db_url)
    except Exception:
        return None


def _ensure_tables_sync():
    """Create tables using sync connection (for test runner)."""
    conn = _get_sync_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS learning_memory (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT NOW(),
                question TEXT NOT NULL,
                sql_query TEXT,
                answer TEXT,
                route VARCHAR(20) DEFAULT 'SQL',
                feedback VARCHAR(20),
                username VARCHAR(100),
                used_count INTEGER DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chat_history (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT NOW(),
                username VARCHAR(100) NOT NULL,
                question TEXT NOT NULL,
                answer TEXT,
                sql_query TEXT,
                route VARCHAR(20)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_learning_feedback ON learning_memory(feedback)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_chat_history_user ON chat_history(username, created_at DESC)")
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        conn.close()
        return False


# ── Shared Learning (SQL examples) ──────────────────

async def load_memory():
    """Load all learning memory entries from DB."""
    await _ensure_tables()
    async with async_session() as session:
        result = await session.execute(text(
            "SELECT question, sql_query, answer, route, feedback, username, used_count, created_at "
            "FROM learning_memory ORDER BY created_at DESC LIMIT :limit"
        ), {"limit": MAX_MEMORY})
        rows = result.fetchall()
        return [
            {
                "question": r[0],
                "soql": r[1],
                "answer": r[2],
                "route": r[3],
                "feedback": r[4],
                "username": r[5],
                "used_count": r[6],
                "timestamp": r[7].isoformat() if r[7] else None,
            }
            for r in rows
        ]


async def save_interaction(question, soql, answer, route, username=None, feedback=None):
    """Save a question→SQL interaction to the learning DB."""
    await _ensure_tables()
    async with async_session() as session:
        await session.execute(text("""
            INSERT INTO learning_memory (question, sql_query, answer, route, feedback, username)
            VALUES (:question, :sql_query, :answer, :route, :feedback, :username)
        """), {
            "question": question,
            "sql_query": soql,
            "answer": (answer or "")[:500],
            "route": route,
            "feedback": feedback,
            "username": username,
        })
        await session.commit()

    # Also save to user's chat history
    if username:
        async with async_session() as session:
            await session.execute(text("""
                INSERT INTO chat_history (username, question, answer, sql_query, route)
                VALUES (:username, :question, :answer, :sql_query, :route)
            """), {
                "username": username,
                "question": question,
                "answer": (answer or "")[:1000],
                "sql_query": soql,
                "route": route,
            })
            await session.commit()

    # Trim if over max
    async with async_session() as session:
        count_result = await session.execute(text("SELECT COUNT(*) FROM learning_memory"))
        count = count_result.scalar()
        if count > MAX_MEMORY:
            await session.execute(text("""
                DELETE FROM learning_memory
                WHERE id IN (
                    SELECT id FROM learning_memory
                    WHERE feedback IS NULL OR feedback NOT IN ('good', 'corrected')
                    ORDER BY created_at ASC
                    LIMIT :to_delete
                )
            """), {"to_delete": count - MAX_MEMORY})
            await session.commit()


def save_interaction_sync(question, soql, answer, route, username=None, feedback=None):
    """Sync version of save_interaction for test runner."""
    _ensure_tables_sync()
    conn = _get_sync_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO learning_memory (question, sql_query, answer, route, feedback, username) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (question, soql, (answer or "")[:500], route, feedback, username)
        )
        if username:
            cur.execute(
                "INSERT INTO chat_history (username, question, answer, sql_query, route) "
                "VALUES (%s, %s, %s, %s, %s)",
                (username, question, (answer or "")[:1000], soql, route)
            )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"save_interaction_sync failed: {e}")
        conn.close()
        return False


async def update_feedback(question, feedback):
    """Update feedback for the most recent matching question."""
    await _ensure_tables()
    async with async_session() as session:
        await session.execute(text("""
            UPDATE learning_memory SET feedback = :feedback
            WHERE id = (
                SELECT id FROM learning_memory WHERE question = :question
                ORDER BY created_at DESC LIMIT 1
            )
        """), {"question": question, "feedback": feedback})
        await session.commit()
    logger.info(f"Feedback '{feedback}' saved for: {question[:50]}")


_STOP_WORDS = {
    "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
    "for", "of", "in", "on", "at", "to", "by", "with", "from", "and",
    "or", "but", "if", "then", "so", "as", "it", "its", "me", "my",
    "we", "our", "i", "you", "your", "do", "does", "did", "has", "have",
    "had", "will", "would", "can", "could", "should", "may", "might",
}

_NEGATION_WORDS = {"no", "not", "without", "zero", "none", "never", "dont", "doesn't", "isn't", "aren't"}

async def find_similar_past_queries(question, top_k=5):
    """Find similar past queries using word overlap scoring with negation awareness."""
    await _ensure_tables()
    q_lower = question.lower()
    q_words = set(q_lower.split()) - _STOP_WORDS
    q_has_negation = bool(q_words & _NEGATION_WORDS)

    async with async_session() as session:
        result = await session.execute(text("""
            SELECT question, sql_query, feedback FROM learning_memory
            WHERE sql_query IS NOT NULL AND sql_query != ''
            AND (feedback IS NULL OR feedback != 'bad')
            ORDER BY created_at DESC LIMIT 500
        """))
        rows = result.fetchall()

    scored = []
    for row in rows:
        past_q, past_sql, fb = row[0], row[1], row[2]
        past_words = set(past_q.lower().split()) - _STOP_WORDS
        overlap = len(q_words & past_words)
        if overlap > 0:
            boost = 2.0 if fb == "good" else (1.5 if fb == "corrected" else 1.0)
            past_has_negation = bool(past_words & _NEGATION_WORDS)
            past_has_not_in = "not in" in (past_sql or "").lower()
            if q_has_negation and not past_has_negation and not past_has_not_in:
                boost *= 0.3
            elif not q_has_negation and (past_has_negation or past_has_not_in):
                boost *= 0.3
            scored.append((overlap * boost, {"past_question": past_q, "past_soql": past_sql, "feedback": fb or "none"}))

    scored.sort(key=lambda x: -x[0])
    examples = [item for _, item in scored[:top_k]]
    if examples:
        logger.info(f"Found {len(examples)} similar past queries for learning")
    return examples


async def get_learning_examples_prompt(question):
    """Build a prompt section with similar past queries for the AI."""
    examples = await find_similar_past_queries(question)
    if not examples:
        return ""
    lines = ["\nPAST SUCCESSFUL QUERIES (learn from these):"]
    for ex in examples:
        fb = " [verified correct]" if ex["feedback"] == "good" else ""
        lines.append(f"  Q: {ex['past_question']}")
        lines.append(f"  SQL: {ex['past_soql']}{fb}")
        lines.append("")
    return "\n".join(lines)


async def get_stats():
    """Get learning memory statistics."""
    await _ensure_tables()
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE feedback = 'good') as good,
                COUNT(*) FILTER (WHERE feedback = 'bad') as bad,
                COUNT(*) FILTER (WHERE feedback = 'corrected') as corrected
            FROM learning_memory
        """))
        row = result.fetchone()
        return {
            "total_interactions": row[0],
            "good_feedback": row[1],
            "bad_feedback": row[2],
            "corrected": row[3],
            "neutral": row[0] - row[1] - row[2] - row[3],
        }


# ── Per-User Data ────────────────────────────────────

async def get_user_history(username, limit=50):
    """Get recent chat history for a user."""
    await _ensure_tables()
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT question, answer, sql_query, route, created_at
            FROM chat_history
            WHERE username = :username
            ORDER BY created_at DESC LIMIT :limit
        """), {"username": username, "limit": limit})
        rows = result.fetchall()
        return [
            {
                "question": r[0],
                "answer": r[1],
                "soql": r[2],
                "route": r[3],
                "timestamp": r[4].isoformat() if r[4] else None,
            }
            for r in reversed(rows)
        ]


async def get_user_stats(username):
    """Get user statistics."""
    await _ensure_tables()
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT COUNT(*), MIN(created_at), MAX(created_at)
            FROM chat_history WHERE username = :username
        """), {"username": username})
        row = result.fetchone()
        return {
            "total_questions": row[0],
            "first_question": row[1].isoformat() if row[1] else None,
            "last_question": row[2].isoformat() if row[2] else None,
        }
