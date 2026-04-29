import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
AI_ENGINE = ROOT / "app" / "chat" / "ai_engine.py"
SEMANTIC = ROOT / "app" / "chat" / "semantic.py"
MEMORY = ROOT / "app" / "chat" / "memory.py"
QUERY_CACHE = ROOT / "app" / "chat" / "query_cache.py"


def _load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _compile_file(path: Path) -> None:
    compile(_load_text(path), str(path), "exec")


def _load_ai_helpers():
    source = _load_text(AI_ENGINE)
    module = ast.parse(source, filename=str(AI_ENGINE))
    needed = {
        "_detect_primary_entity",
        "_build_count_answer",
        "_build_group_template_answer",
    }
    selected = [node for node in module.body if isinstance(node, ast.FunctionDef) and node.name in needed]
    helper_module = ast.Module(body=selected, type_ignores=[])
    namespace = {"re": re}
    exec(compile(helper_module, str(AI_ENGINE), "exec"), namespace)
    return namespace


def test_files_compile():
    for path in (AI_ENGINE, SEMANTIC, MEMORY, QUERY_CACHE):
        _compile_file(path)


def test_placed_maps_to_project_started():
    semantic_text = _load_text(SEMANTIC)
    assert '"got placed": "Project Started"' in semantic_text
    assert '"placements": "Project Started"' in semantic_text
    assert '"placement": "Project Started"' in semantic_text


def test_learning_and_cache_are_verified_only():
    memory_text = _load_text(MEMORY)
    cache_text = _load_text(QUERY_CACHE)
    assert "feedback IN ('good', 'corrected')" in memory_text
    assert 'if feedback not in ("good", "corrected")' in cache_text


def test_count_answer_does_not_invent_in_market():
    helpers = _load_ai_helpers()
    answer = helpers["_build_count_answer"]("How many students do we have?", 7790)
    assert answer == "**7,790 students** match your query."
    assert "in market" not in answer.lower()


def test_count_answer_preserves_requested_filter():
    helpers = _load_ai_helpers()
    answer = helpers["_build_count_answer"]("How many students in market this month?", 42)
    assert "in market" in answer.lower()
    assert "this month" in answer.lower()


def test_group_template_uses_entity_total():
    helpers = _load_ai_helpers()
    answer = helpers["_build_group_template_answer"](
        "submissions by bu this month",
        [{"BU_Name": "Divya", "cnt": 3}, {"BU_Name": "Ravi", "cnt": 2}],
    )
    assert answer is not None
    assert "**5 submissions** across **2 groups**." in answer
    assert "| **Total** | **5** |" in answer


if __name__ == "__main__":
    test_files_compile()
    test_placed_maps_to_project_started()
    test_learning_and_cache_are_verified_only()
    test_count_answer_does_not_invent_in_market()
    test_count_answer_preserves_requested_filter()
    test_group_template_uses_entity_total()
    print("All regression tests passed.")
