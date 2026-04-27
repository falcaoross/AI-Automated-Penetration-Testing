# linker.py - AI Pentest CAU Layer
# Deterministic linking: CAU -> CRU -> generated test cases.
# No LLM, no semantic search, no fuzzy matching.
#
# Gap fix (v1.1): After linkage, comparator.py is called to produce
# per-CRU-CAU verdicts (MATCH / PARTIAL / MISSING / CONFLICT).
# This is purely additive - existing linkage logic is unchanged.

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_crus(source: Union[str, Path, bytes]) -> list[dict]:
    """Load cru_units.json from a file path or raw bytes."""
    return _load_json(source)


def load_test_cases(source: Union[str, Path, bytes]) -> list[dict]:
    """Load optimized_test_cases.json from a file path or raw bytes."""
    return _load_json(source)


def build_indexes(
    crus: list[dict],
    test_cases: list[dict],
) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    """
    Build two in-memory lookup indexes:

    req_to_crus  : { parent_requirement_id -> [cru, ...] }
    cru_to_tests : { cru_id -> [test_case, ...] }
    """
    req_to_crus: dict[str, list[dict]] = {}
    for cru in crus:
        parent = _normalise_parent_req_id(cru.get('parent_requirement_id', '') or '')
        if parent:
            req_to_crus.setdefault(parent, []).append(cru)

    cru_to_tests: dict[str, list[dict]] = {}
    for tc in test_cases:
        cru_id = tc.get('requirement_id', '') or tc.get('cru_id', '')
        cru_id = cru_id.upper().strip()
        if cru_id:
            cru_to_tests.setdefault(cru_id, []).append(tc)

    logger.info(
        "Indexes built - req_to_crus keys: %d, cru_to_tests keys: %d",
        len(req_to_crus),
        len(cru_to_tests),
    )
    return req_to_crus, cru_to_tests


def build_dependency_index(crus: list[dict]) -> dict[str, set[str]]:
    """
    Build a requirement-level dependency graph from the CRU list.

    Returns:
        req_deps : { req_id (upper) -> set of req_ids it directly depends on }
    """
    req_deps: dict[str, set[str]] = {}
    for cru in crus:
        parent = _normalise_parent_req_id(cru.get('parent_requirement_id') or '')
        if not parent:
            continue
        for dep in (cru.get('dependencies') or []):
            dep_upper = dep.upper().strip()
            if dep_upper:
                req_deps.setdefault(parent, set()).add(dep_upper)
    logger.debug("Dependency index built - %d req_ids have declared deps", len(req_deps))
    return req_deps


def infer_coverage(
    cau_units: list[dict],
    crus: list[dict],
    req_to_crus: dict[str, list[dict]],
    cru_to_tests: dict[str, list[dict]],
    req_deps: dict[str, set[str]],
) -> list[dict]:
    """
    Dependency-propagation pass - Option B inference.

    For every CRU whose parent_requirement_id has no direct UAT entry,
    inspect its declared dependency chain. If every direct dependency of
    that requirement resolves to a passing UAT entry, the CRU is eligible
    for INFERRED_PARTIAL coverage.

    Returns a list of synthetic CAU dicts - one per qualifying uncovered
    requirement group. Never mutates any input structure.
    """
    import config  # local import - keeps linker importable standalone

    pass_statuses: set[str] = {s.upper() for s in getattr(config, 'PASS_STATUSES', ['PASS'])}

    directly_tested: set[str] = set()
    for cau in cau_units:
        if (cau.get('status') or '').upper() in pass_statuses:
            for req_id in cau.get('req_ids', []):
                directly_tested.add(req_id.upper())

    logger.debug("Directly tested req_ids (%d): %s", len(directly_tested), sorted(directly_tested))

    _cache: dict[str, bool | None] = {}

    def _is_satisfied(req_id: str, visiting: frozenset[str]) -> bool:
        if req_id in directly_tested:
            return True
        if req_id in _cache:
            cached = _cache[req_id]
            return bool(cached) if cached is not None else False
        if req_id in visiting:
            return False
        deps = req_deps.get(req_id)
        if not deps:
            _cache[req_id] = False
            return False
        _cache[req_id] = None
        result = all(_is_satisfied(d, visiting | {req_id}) for d in deps)
        _cache[req_id] = result
        return result

    uat_req_ids: set[str] = set()
    for cau in cau_units:
        uat_req_ids.update(r.upper() for r in cau.get('req_ids', []))

    from collections import defaultdict
    req_to_cru_list: dict[str, list[dict]] = defaultdict(list)
    for cru in crus:
        parent = _normalise_parent_req_id(cru.get('parent_requirement_id') or '')
        if parent and parent not in uat_req_ids:
            req_to_cru_list[parent].append(cru)

    synthetic_caus: list[dict] = []
    for req_id, cru_list in sorted(req_to_cru_list.items()):
        if not req_deps.get(req_id):
            logger.debug("INFER_SKIP %s - no declared dependencies", req_id)
            continue

        if not _is_satisfied(req_id, frozenset()):
            logger.debug("INFER_SKIP %s - not all dependencies are satisfied", req_id)
            continue

        linked_crus: list[dict] = []
        linked_test_cases: list[dict] = []
        for cru in cru_list:
            cru_id = (cru.get('cru_id') or '').upper().strip()
            if not cru_id:
                continue
            entry = _build_cru_entry(cru)
            entry['match_method'] = 'inferred_dep'
            linked_crus.append(entry)
            for tc in cru_to_tests.get(cru_id, []):
                tc_entry = _build_tc_entry(tc, cru_id)
                if not any(t['test_id'] == tc_entry['test_id'] for t in linked_test_cases):
                    linked_test_cases.append(tc_entry)

        satisfied_deps = sorted(
            d for d in req_deps[req_id] if _is_satisfied(d, frozenset())
        )

        inferred_coverage = _classify_coverage(
            status='INFERRED',
            linked_crus=linked_crus,
            linked_test_cases=linked_test_cases,
            unmatched_req_ids=[],
        )
        # Preserve inferred-specific fields that _classify_coverage doesn't produce
        inferred_coverage['inferred_from'] = satisfied_deps
        inferred_coverage['summary'] = (
            f'INFERRED. No UAT entry; all deps ({", ".join(satisfied_deps)}) '
            f'are tested. {len(linked_crus)} CRU(s), '
            f'{len(linked_test_cases)} test case(s) surfaced.'
        )

        synthetic_caus.append({
            'cau_id':              f'CAU-INFERRED-{req_id}',
            'uat_id':              f'INFERRED-{req_id}',
            'title':               f'Inferred coverage for {req_id}',
            'actor_class':         '',
            'status':              'INFERRED',
            'req_ids':             [req_id],
            'description':         (
                f'No direct UAT entry exists for {req_id}. '
                f'Coverage inferred because all declared dependencies '
                f'({", ".join(satisfied_deps)}) are directly or transitively tested.'
            ),
            'preconditions':       [],
            'test_steps':          [],
            'expected_result':     '',
            'actual_result':       '',
            'tester_observations': '',
            'linked_requirements': [_build_req_entry(req_id, cru_list)],
            'linked_crus':         linked_crus,
            'linked_test_cases':   linked_test_cases,
            'cru_verdicts':        [],
            'coverage':            inferred_coverage,
        })
        logger.info(
            "INFERRED_PARTIAL for %s - deps=%s, crus=%d, tcs=%d",
            req_id, satisfied_deps, len(linked_crus), len(linked_test_cases),
        )

    logger.info(
        "Inference pass complete - %d requirement(s) promoted to INFERRED_PARTIAL",
        len(synthetic_caus),
    )
    return synthetic_caus


def link_cau(
    raw_cau: dict,
    req_to_crus: dict[str, list[dict]],
    cru_to_tests: dict[str, list[dict]],
    cru_meta: dict[str, dict],
) -> dict:
    """
    Resolve a single raw CAU dict into a fully linked CAU object.

    Linking is strictly:
      req_id  ->  CRU
      cru_id  ->  test case

    After linkage, comparator.compare_cau_all_crus() is called to produce
    per-CRU-CAU verdicts (MATCH / PARTIAL / MISSING / CONFLICT).
    Verdicts are attached to each cru_entry and to the top-level cru_verdicts
    list. Existing coverage classification is NOT replaced.

    Returns a new dict - does not mutate the input.
    """
    req_ids: list[str] = [r.upper() for r in raw_cau.get('req_ids', [])]

    linked_requirements: list[dict] = []
    linked_crus: list[dict] = []
    linked_test_cases: list[dict] = []
    unmatched_req_ids: list[str] = []

    for req_id in req_ids:
        matched_crus = req_to_crus.get(req_id, [])
        if not matched_crus:
            unmatched_req_ids.append(req_id)
            logger.debug("NO_CRU_MATCH for req_id=%s in CAU %s", req_id, raw_cau.get('uat_id'))
            continue

        req_entry = _build_req_entry(req_id, matched_crus)
        if not any(r['req_id'] == req_id for r in linked_requirements):
            linked_requirements.append(req_entry)

        for cru in matched_crus:
            cru_id = (cru.get('cru_id') or '').upper().strip()
            if not cru_id:
                continue

            cru_entry = _build_cru_entry(cru)
            if not any(c['cru_id'] == cru_id for c in linked_crus):
                linked_crus.append(cru_entry)

            for tc in cru_to_tests.get(cru_id, []):
                tc_entry = _build_tc_entry(tc, cru_id)
                if not any(t['test_id'] == tc_entry['test_id'] for t in linked_test_cases):
                    linked_test_cases.append(tc_entry)

    # -- Coverage classification (unchanged) ------------------------------
    coverage = _classify_coverage(
        status=raw_cau.get('status', ''),
        linked_crus=linked_crus,
        linked_test_cases=linked_test_cases,
        unmatched_req_ids=unmatched_req_ids,
    )

    # -- Gap fix: comparator verdict layer (additive) ----------------------
    # Import locally to keep linker importable standalone without comparator.
    cru_verdicts: list[dict] = []
    try:
        import comparator as _comparator
        cru_verdicts = _comparator.compare_cau_all_crus(raw_cau, linked_crus, cru_meta)

        # Attach verdict fields directly onto each linked_cru entry
        verdict_map = {v['cru_id']: v for v in cru_verdicts}
        for cru_entry in linked_crus:
            cru_id_key = cru_entry.get('cru_id', '').upper()
            v = verdict_map.get(cru_id_key, {})
            cru_entry['verdict']       = v.get('verdict', 'MISSING')
            cru_entry['overlap_ratio'] = v.get('overlap_ratio', 0.0)

    except ImportError:
        logger.warning("comparator.py not found - skipping verdict layer")
    except Exception as exc:
        logger.warning("comparator raised an unexpected error - skipping: %s", exc)

    cau_id = 'CAU-' + raw_cau.get('uat_id', 'UNKNOWN').replace('UAT-', '', 1)

    return {
        'cau_id':              cau_id,
        'uat_id':              raw_cau.get('uat_id', ''),
        'title':               raw_cau.get('title', ''),
        'actor_class':         raw_cau.get('actor_class', ''),
        'status':              raw_cau.get('status', ''),
        'req_ids':             req_ids,
        'description':         raw_cau.get('description', ''),
        'preconditions':       raw_cau.get('precondition', []),
        'test_steps':          raw_cau.get('test_steps', []),
        'expected_result':     raw_cau.get('expected_result', ''),
        'actual_result':       raw_cau.get('actual_result', ''),
        'tester_observations': raw_cau.get('observations', ''),
        'linked_requirements': linked_requirements,
        'linked_crus':         linked_crus,
        'linked_test_cases':   linked_test_cases,
        'cru_verdicts':        cru_verdicts,
        'coverage':            coverage,
    }


def compute_gap_report(
    cau_units: list[dict],
    crus: list[dict],
    req_to_crus: dict[str, list[dict]],
    inferred_req_ids: set[str] | None = None,
) -> dict:
    """
    Build the traceability gap report.

    uncovered_crus   - CRUs whose parent_requirement_id never appears in any UAT
                       and were NOT promoted by the inference pass.
    missing_req_ids  - req_ids referenced in UAT but not found in any CRU.
    """
    inferred: set[str] = {r.upper() for r in (inferred_req_ids or set())}

    uat_req_ids: set[str] = set()
    for cau in cau_units:
        if cau.get('status') == 'INFERRED':
            continue
        uat_req_ids.update(r.upper() for r in cau.get('req_ids', []))

    uncovered_crus: list[dict] = []
    inferred_crus: list[dict] = []

    for cru in crus:
        parent = _normalise_parent_req_id(cru.get('parent_requirement_id') or '')
        if not parent or parent in uat_req_ids:
            continue

        entry = {
            'cru_id':                cru.get('cru_id', ''),
            'parent_requirement_id': parent,
            'actor':                 cru.get('actor', ''),
            'action':                cru.get('action', ''),
        }

        if parent in inferred:
            entry['reason'] = 'No direct UAT entry; promoted to INFERRED_PARTIAL via dependency chain'
            inferred_crus.append(entry)
        else:
            entry['reason'] = 'No UAT test case references this requirement'
            uncovered_crus.append(entry)

    missing_req_ids: list[dict] = []
    seen: set[str] = set()
    for cau in cau_units:
        if cau.get('status') == 'INFERRED':
            continue
        for req_id in cau.get('req_ids', []):
            req_id_upper = req_id.upper()
            if req_id_upper not in req_to_crus and req_id_upper not in seen:
                seen.add(req_id_upper)
                missing_req_ids.append({
                    'req_id': req_id_upper,
                    'referenced_in_uat': cau.get('uat_id', ''),
                    'reason': 'Requirement referenced in UAT but not found in CRU file',
                })

    logger.info(
        "Gap report - uncovered_crus: %d, inferred_crus: %d, missing_req_ids: %d",
        len(uncovered_crus),
        len(inferred_crus),
        len(missing_req_ids),
    )
    return {
        'uncovered_crus':  uncovered_crus,
        'inferred_crus':   inferred_crus,
        'missing_req_ids': missing_req_ids,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalise_parent_req_id(raw: str) -> str:
    """
    Normalise a CRU parent_requirement_id using the CRU_TAG_ALIASES map in
    config.py (Bug 2 fix).

    Some CRU extractors emit raw PLanguage TAG values (e.g. "SystemReliability")
    instead of the formal requirement ID (e.g. "QR9").  The alias map is
    checked case-insensitively so operators don't need to worry about
    capitalisation.  If no alias matches, the uppercased original is returned
    unchanged - fully domain-agnostic.
    """
    import config as _config
    aliases: dict[str, str] = getattr(_config, 'CRU_TAG_ALIASES', {})
    upper = raw.upper().strip()
    if not upper:
        return upper
    # Build a normalised lookup key (uppercase) for the alias table.
    for raw_tag, canonical_id in aliases.items():
        if raw_tag.upper() == upper:
            logger.debug(
                "CRU TAG alias: '%s' -> '%s'", raw, canonical_id.upper()
            )
            return canonical_id.upper().strip()
    return upper


def _load_json(source: Union[str, Path, bytes]) -> list[dict]:
    if isinstance(source, (str, Path)):
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"JSON file not found: {path}")
        with open(path, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
    elif isinstance(source, bytes):
        data = json.loads(source.decode('utf-8'))
    else:
        raise TypeError(f"Unsupported source type: {type(source)}")

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ('cru_units', 'test_cases', 'test_case_units', 'optimized_test_cases',
                    'requirements', 'items', 'data', 'results', 'units'):
            if key in data and isinstance(data[key], list):
                logger.info("Loaded list from wrapper key '%s'", key)
                return data[key]

        all_lists = [(k, v) for k, v in data.items() if isinstance(v, list) and v]
        if len(all_lists) > 1:
            merged = []
            for k, v in all_lists:
                merged.extend(v)
            logger.info("Merged %d lists (%s) -> %d total items",
                        len(all_lists), ', '.join(k for k, _ in all_lists), len(merged))
            return merged

        if len(all_lists) == 1:
            k, v = all_lists[0]
            logger.info("Loaded list from auto-detected key '%s' (%d items)", k, len(v))
            return v

        return [data]
    raise ValueError(f"Unexpected JSON structure: {type(data)}")


def _build_req_entry(req_id: str, crus: list[dict]) -> dict:
    first = crus[0]
    return {
        'req_id':       req_id,
        'title':        first.get('title', ''),
        'section_path': first.get('section_path', first.get('section', '')),
    }


def _build_cru_entry(cru: dict) -> dict:
    return {
        'cru_id':                (cru.get('cru_id') or '').upper(),
        'parent_requirement_id': _normalise_parent_req_id(cru.get('parent_requirement_id') or ''),
        'actor':                 cru.get('actor', ''),
        'action':                cru.get('action', ''),
        'type':                  cru.get('type', ''),
        'match_method':          'direct_req_id',
        'verdict':               '',      # populated by comparator after linkage
        'overlap_ratio':         0.0,     # populated by comparator after linkage
    }


def _build_tc_entry(tc: dict, cru_id: str) -> dict:
    return {
        'test_id':    tc.get('test_id', ''),
        'cru_id':     cru_id,
        'test_type':  tc.get('test_type', ''),
        'test_title': tc.get('test_title', tc.get('title', '')),
    }


def _classify_coverage(
    status: str,
    linked_crus: list[dict],
    linked_test_cases: list[dict],
    unmatched_req_ids: list[str],
) -> dict:
    """Apply coverage rules from config.COVERAGE_RULES."""
    import config

    proxy = {
        'status':            status,
        'linked_crus':       linked_crus,
        'linked_test_cases': linked_test_cases,
    }

    classification = 'UNKNOWN'
    for condition_fn, label in config.COVERAGE_RULES:
        try:
            if condition_fn(proxy):
                classification = label
                break
        except Exception:
            continue

    cru_count = len(linked_crus)
    tc_count  = len(linked_test_cases)
    summary_parts = [f"UAT {status or 'UNKNOWN'}."]
    if cru_count:
        summary_parts.append(f"{cru_count} CRU(s) and {tc_count} test case(s) linked.")
    else:
        summary_parts.append("No CRUs matched.")
    if unmatched_req_ids:
        summary_parts.append(f"Unmatched req_ids: {', '.join(unmatched_req_ids)}.")

    return {
        'classification':    classification,
        'cru_count':         cru_count,
        'test_case_count':   tc_count,
        'unmatched_req_ids': unmatched_req_ids,
        'summary':           ' '.join(summary_parts),
    }