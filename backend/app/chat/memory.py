"""
Learning Memory — per-user data + shared learning pool.

Each user has their own chat history.
SOQL learning is shared across all users (everyone benefits from good queries).
"""
import json, logging
from datetime import datetime
from pathlib import Path
from app.config import settings

logger = logging.getLogger(__name__)

SHARED_MEMORY_FILE = "learning_memory.json"
MAX_MEMORY = 1000


def _user_dir(username=None):
    if not username:
        return Path(settings.data_dir)
    d = Path(settings.data_dir) / "users" / username
    d.mkdir(parents=True, exist_ok=True)
    return d


def _load_json(path):
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except:
            return []
    return []


def _save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


# ── Shared Learning (SOQL examples) ──────────────────

def load_memory():
    return _load_json(Path(settings.data_dir) / SHARED_MEMORY_FILE)


def save_interaction(question, soql, answer, route, username=None, feedback=None):
    # Save to shared learning pool
    memory = load_memory()
    entry = {
        "timestamp": datetime.now().isoformat(),
        "question": question,
        "soql": soql,
        "answer": answer[:500],
        "route": route,
        "feedback": feedback,
        "username": username,
        "used_count": 0,
    }
    memory.append(entry)
    if len(memory) > MAX_MEMORY:
        good = [m for m in memory if m.get("feedback") == "good"]
        recent = [m for m in memory if m.get("feedback") != "good"][-500:]
        memory = good + recent
    _save_json(Path(settings.data_dir) / SHARED_MEMORY_FILE, memory)

    # Save to user's chat history
    if username:
        history = _load_json(_user_dir(username) / "chat_history.json")
        history.append({
            "timestamp": datetime.now().isoformat(),
            "question": question,
            "answer": answer[:1000],
            "soql": soql,
            "route": route,
        })
        if len(history) > 500:
            history = history[-500:]
        _save_json(_user_dir(username) / "chat_history.json", history)


def update_feedback(question, feedback):
    memory = load_memory()
    for m in reversed(memory):
        if m["question"] == question:
            m["feedback"] = feedback
            break
    _save_json(Path(settings.data_dir) / SHARED_MEMORY_FILE, memory)
    logger.info(f"Feedback '{feedback}' saved for: {question[:50]}")


def find_similar_past_queries(question, top_k=5):
    memory = load_memory()
    if not memory:
        return []
    q_words = set(question.lower().split())
    scored = []
    for m in memory:
        if not m.get("soql") or m.get("feedback") == "bad":
            continue
        past_words = set(m["question"].lower().split())
        overlap = len(q_words & past_words)
        if overlap > 0:
            boost = 2.0 if m.get("feedback") == "good" else 1.0
            scored.append((overlap * boost, m))
    scored.sort(key=lambda x: -x[0])
    examples = []
    for score, m in scored[:top_k]:
        examples.append({"past_question": m["question"], "past_soql": m["soql"], "feedback": m.get("feedback", "none")})
    if examples:
        logger.info(f"Found {len(examples)} similar past queries for learning")
    return examples


def get_learning_examples_prompt(question):
    examples = find_similar_past_queries(question)
    if not examples:
        return ""
    lines = ["\nPAST SUCCESSFUL QUERIES (learn from these):"]
    for ex in examples:
        fb = " [user liked this]" if ex["feedback"] == "good" else ""
        lines.append(f"  Q: {ex['past_question']}")
        lines.append(f"  SOQL: {ex['past_soql']}{fb}")
        lines.append("")
    return "\n".join(lines)


def get_stats():
    memory = load_memory()
    total = len(memory)
    good = sum(1 for m in memory if m.get("feedback") == "good")
    bad = sum(1 for m in memory if m.get("feedback") == "bad")
    return {"total_interactions": total, "good_feedback": good, "bad_feedback": bad, "neutral": total - good - bad}


# ── Per-User Data ────────────────────────────────────

def get_user_history(username, limit=50):
    history = _load_json(_user_dir(username) / "chat_history.json")
    return history[-limit:]


def get_user_stats(username):
    history = _load_json(_user_dir(username) / "chat_history.json")
    return {
        "total_questions": len(history),
        "first_question": history[0]["timestamp"] if history else None,
        "last_question": history[-1]["timestamp"] if history else None,
    }
