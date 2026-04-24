from typing import Dict, Any
from graphrag.models.contracts import QueryInput, TaskType


TASK_RELATIONS = {
    TaskType.TEST_GENERATION: {
        "forward": ["SUPPORTED_BY", "PARENT_OF", "DECOMPOSED_TO", "TESTS"],
        "reverse": ["TESTS"]
    },
    TaskType.DEBUG: {
        "forward": ["TESTS", "SUPPORTED_BY", "EXECUTED_AS", "RAISED_AS"],
        "reverse": ["TESTS", "AFFECTS"]
    },
    TaskType.IMPACT: {
        "forward": ["DECOMPOSED_TO", "SUPPORTED_BY"],
        "reverse": ["TESTS", "AFFECTS"]
    },
    # FIX: acceptance_validation was missing — caused ValueError before reaching query_graph.py
    TaskType.ACCEPTANCE_VALIDATION: {
        "forward": ["SUPPORTED_BY", "PARENT_OF", "TESTS", "EVIDENCE_FOR"],
        "reverse": ["TESTS"]
    },
}


def route_query(payload: Dict[str, Any]) -> QueryInput:
    """Route incoming query to typed QueryInput."""
    task_str = payload.get("task")
    if not task_str:
        raise ValueError("task is required")

    task = TaskType(task_str)
    if task not in TASK_RELATIONS:
        raise ValueError(f"Unsupported task: {task}")

    req_id = payload.get("req_id")
    query_text = payload.get("query_text")

    if not req_id and not query_text:
        raise ValueError("Either req_id or query_text must be provided")

    return QueryInput(
        task=task,
        req_id=req_id,
        query_text=query_text,
        filters=payload.get("filters", {}),
        k_evidence=int(payload.get("k_evidence", 8)),
        k_parent=int(payload.get("k_parent", 3))
    )