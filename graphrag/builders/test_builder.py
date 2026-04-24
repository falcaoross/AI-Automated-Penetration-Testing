"""TEST node builder with full error handling."""
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict

from graphrag.storage.graph_store import GraphStore


def build_test_nodes(graph_store: GraphStore, test_file_path: str) -> List[Dict]:
    print(f"[TEST] Loading: {test_file_path}")
    
    try:
        with open(test_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[TEST ERROR] Failed to load {test_file_path}: {e}")
        return []
    
    # Parse ALL phases
    all_tests = []
    for phase_key in ['phase1_test_cases', 'phase2_test_cases']:
        if phase_key in data:
            phase_tests = data[phase_key]
            print(f"[TEST] {phase_key}: {len(phase_tests)} tests")
            all_tests.extend(phase_tests)
    
    print(f"[TEST] Total tests to process: {len(all_tests)}")
    
    if not all_tests:
        print("[TEST] No tests found!")
        return []
    
    # Sample first test for debugging
    sample = all_tests[0]
    print(f"[TEST DEBUG] Sample: {sample.get('test_id')} -> {sample.get('requirement_id')}")
    
    inserted = 0
    skipped = 0
    
    for i, test in enumerate(all_tests):
        test_id = test.get('test_id')
        req_id = test.get('requirement_id')
        
        if not test_id or not req_id:
            print(f"[TEST SKIP {i}] Missing test_id/req_id: {test}")
            skipped += 1
            continue
        
        # Bulletproof minimal node - ALL required fields
        node = {
            "node_id": test_id,
            "node_type": "TEST",
            "title": test.get("test_title", f"Test {test_id}"),
            "text": test.get("description", ""),
            "module": "SRS",
            "version": "20260403",
            "doc_id": Path(test_file_path).name,
            "doc_type": "TESTS",
            "section_path": test.get("generation_phase", "fast_batch"),  # REQUIRED
            "source_locator_json": json.dumps({  # REQUIRED
                "file": test_file_path,
                "test_id": test_id,
                "req_id": req_id
            }),
            "extra_json": json.dumps({
                "priority": test.get("priority"),
                "test_type": test.get("test_type"),
                "steps": test.get("test_steps", []),
                "expected": test.get("expected_result", "")
            }),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        
        try:
            graph_store.insert_node(node)
            inserted += 1
        except Exception as e:
            print(f"[TEST INSERT ERROR {i}] {test_id}: {e}")
            skipped += 1
    
    print(f"[TEST SUMMARY] Inserted: {inserted}, Skipped: {skipped}")
    return all_tests


