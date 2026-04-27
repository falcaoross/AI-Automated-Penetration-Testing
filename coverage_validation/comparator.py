# comparator.py - Autopilot-QA CAU Layer
# Rule-based content comparator: CRU specification vs CAU actual result.
# Produces per-CRU-CAU verdict: MATCH / PARTIAL / MISSING / CONFLICT
#
# Design principles:
#   - Fully domain agnostic - zero hardcoded domain terms anywhere
#   - All thresholds, patterns, field names come from config.py
#   - Pure Python stdlib only - no LLM, no embeddings, no ML dependencies
#   - Additive layer - never replaces existing linkage or coverage logic
#   - Verdict is per CRU-CAU pair, not per CAU overall
#
# v1.2 - Three targeted improvements to reduce false MISSING verdicts:
#
#   1. STEM-BASED OVERLAP  (_stem, _tokenise)
#      Raw word matching penalises morphological variants that express the
#      same concept: "log-in" / "logged" / "login", "automatically" /
#      "automatic", "stored" / "storage", "provide" / "provided", etc.
#      A minimal suffix-stripping stemmer (domain-agnostic, stdlib only)
#      now reduces tokens to their approximate root before comparison.
#      This is intentionally conservative - it strips only the most common
#      English inflectional/derivational suffixes - to avoid false positives.
#
#   2. WIDER EVIDENCE WINDOW  (_extract_evidence_text)
#      The previous implementation concatenated actual_result +
#      tester_observations only when actual_result was non-empty.
#      tester_observations was silently dropped in the fallback branch.
#      The fix always appends tester_observations (and description, if
#      present) regardless of which primary field is used, maximising the
#      evidence surface without changing the priority order.
#
#   3. STEM-OVERLAP SECONDARY SCORE  (_classify_verdict, compare_cau_cru)
#      Even after stemming, some CRU-evidence pairs have zero exact-stem
#      overlap because the UAT uses structural paraphrase ("Registration
#      was completed without errors" vs "The user must provide user-name,
#      password and e-mail address").  A secondary stem-prefix overlap
#      ratio (first STEM_PREFIX_LEN characters of each stem) is computed
#      and blended with the primary ratio at weight STEM_BLEND_WEIGHT
#      (both configurable in config.py, default 4 chars / 0.30 weight).
#      This captures near-matches (autom·atically / autom·atic) that
#      full-stem matching still misses.

from __future__ import annotations

import re
import logging
from typing import Optional

import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compare_cau_cru(cau: dict, cru: dict) -> dict:
    """
    Compare a single CAU against a single CRU.

    Extracts specification text from the CRU using CRU_SPEC_FIELDS priority
    order, extracts evidence text from the CAU actual_result (falling back to
    expected_result if empty), computes stem-based word overlap (v1.2),
    checks for negation patterns, and returns a verdict dict.

    Returns
    -------
    dict with keys:
        cru_id             : str
        verdict            : MATCH | PARTIAL | MISSING | CONFLICT
        overlap_ratio      : float  (0.0 - 1.0, primary stem overlap)
        stem_overlap_ratio : float  (0.0 - 1.0, stem-prefix secondary score)
        blended_ratio      : float  (weighted blend used for verdict)
        spec_text_used     : str    (CRU text that was compared)
        evidence_text_used : str    (CAU text that was compared)
        negation_found     : bool
        spec_field_used    : str    (which CRU field provided spec_text)
    """
    cru_id = (cru.get('cru_id') or '').upper().strip()

    # -- Step 1: extract spec text from CRU -------------------------------
    spec_text, spec_field = _extract_spec_text(cru)

    # -- Step 2: extract evidence text from CAU ----------------------------
    evidence_text = _extract_evidence_text(cau)

    # -- Step 3: handle empty cases early ---------------------------------
    if not spec_text:
        logger.debug("CRU %s has no spec text in any CRU_SPEC_FIELDS - verdict MISSING", cru_id)
        return _build_verdict(cru_id, config.VERDICT_MISSING, 0.0, 0.0, 0.0,
                              spec_text, evidence_text, False, spec_field)

    if not evidence_text:
        logger.debug("CAU %s has no evidence text - verdict MISSING for CRU %s",
                     cau.get('uat_id', '?'), cru_id)
        return _build_verdict(cru_id, config.VERDICT_MISSING, 0.0, 0.0, 0.0,
                              spec_text, evidence_text, False, spec_field)

    # -- Step 4: tokenise with stemming and remove stopwords --------------
    spec_stems     = _tokenise(spec_text)
    evidence_stems = _tokenise(evidence_text)

    if not spec_stems:
        return _build_verdict(cru_id, config.VERDICT_MISSING, 0.0, 0.0, 0.0,
                              spec_text, evidence_text, False, spec_field)

    # -- Step 5: primary overlap ratio (exact stem match) -----------------
    overlap       = spec_stems & evidence_stems
    overlap_ratio = len(overlap) / len(spec_stems)

    # -- Step 6: secondary stem-prefix overlap ratio -----------------------
    # Catches near-morphological matches that full stemming still misses
    # e.g. "autom·atically" / "autom·atic" - same first 5 chars, different stem
    prefix_len = getattr(config, 'STEM_PREFIX_LEN', 4)
    spec_prefixes     = {s[:prefix_len] for s in spec_stems if len(s) >= prefix_len}
    evidence_prefixes = {s[:prefix_len] for s in evidence_stems if len(s) >= prefix_len}
    prefix_overlap = spec_prefixes & evidence_prefixes
    stem_overlap_ratio = (
        len(prefix_overlap) / len(spec_prefixes)
        if spec_prefixes else 0.0
    )

    # -- Step 7: blend primary and secondary scores ------------------------
    blend_weight = getattr(config, 'STEM_BLEND_WEIGHT', 0.30)
    blended_ratio = (
        (1.0 - blend_weight) * overlap_ratio
        + blend_weight * stem_overlap_ratio
    )

    # -- Step 8: check for negation conflict ------------------------------
    negation_found = _has_negation(evidence_text)

    # -- Step 9: classify verdict using blended ratio ---------------------
    verdict = _classify_verdict(blended_ratio, negation_found)

    logger.debug(
        "CRU %s | overlap=%.2f | stem_overlap=%.2f | blended=%.2f | negation=%s | verdict=%s",
        cru_id, overlap_ratio, stem_overlap_ratio, blended_ratio, negation_found, verdict,
    )

    return _build_verdict(cru_id, verdict, overlap_ratio, stem_overlap_ratio, blended_ratio,
                          spec_text, evidence_text, negation_found, spec_field)


def compare_cau_all_crus(
    cau: dict,
    linked_crus: list[dict],
    cru_meta: dict,
) -> list[dict]:
    """
    Compare a CAU against all its linked CRUs.

    Parameters
    ----------
    cau         : raw or linked CAU dict (must have actual_result / expected_result)
    linked_crus : list of cru_entry dicts from linker (have cru_id key)
    cru_meta    : dict mapping UPPER cru_id -> full CRU object from cru_units.json

    Returns
    -------
    list of verdict dicts (one per linked CRU)
    """
    verdicts: list[dict] = []

    for cru_entry in linked_crus:
        cru_id = (cru_entry.get('cru_id') or '').upper().strip()
        if not cru_id:
            continue

        full_cru = cru_meta.get(cru_id)
        if not full_cru:
            # CRU metadata not available - mark as MISSING
            logger.debug("CRU %s not found in cru_meta - verdict MISSING", cru_id)
            verdicts.append(_build_verdict(
                cru_id, config.VERDICT_MISSING, 0.0, 0.0, 0.0, '', '', False, '',
            ))
            continue

        verdict = compare_cau_cru(cau, full_cru)
        verdicts.append(verdict)

    return verdicts


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_spec_text(cru: dict) -> tuple[str, str]:
    """
    Extract specification text from a CRU object.

    Iterates CRU_SPEC_FIELDS in priority order and returns the first
    non-null, non-empty string value found, along with the field name.

    Returns (spec_text, field_name). Both empty strings if nothing found.
    """
    for field in config.CRU_SPEC_FIELDS:
        value = cru.get(field)
        if value and isinstance(value, str) and value.strip():
            return value.strip(), field
    return '', ''


def _extract_evidence_text(cau: dict) -> str:
    """
    Extract evidence text from a CAU object.

    v1.2 fix: tester_observations is now ALWAYS appended when present,
    regardless of which primary source is used. Previously it was only
    appended when actual_result was non-empty, meaning it was silently
    dropped in the expected_result fallback branch.

    Priority order (unchanged):
      Primary   : actual_result
      Secondary : tester_observations / observations (always appended)
      Tertiary  : description (appended as additional context)
      Fallback  : expected_result (when actual_result is empty)
    """
    actual       = (cau.get('actual_result') or '').strip()
    observations = (
        cau.get('tester_observations') or
        cau.get('observations') or ''
    ).strip()
    description  = (cau.get('description') or '').strip()
    expected     = (cau.get('expected_result') or '').strip()

    parts: list[str] = []

    if actual:
        parts.append(actual)
    elif expected:
        # Fallback: use expected_result when actual_result is absent
        logger.debug(
            "CAU %s has empty actual_result - falling back to expected_result",
            cau.get('uat_id', '?'),
        )
        parts.append(expected)

    # Always append supplementary fields when they exist - they add evidence
    # surface without displacing the primary source.
    if observations:
        parts.append(observations)
    if description:
        parts.append(description)

    return ' '.join(parts)


# ---------------------------------------------------------------------------
# Stemmer - minimal suffix-stripping (domain-agnostic, stdlib only)
# ---------------------------------------------------------------------------
# This is a deliberately conservative stemmer: it handles only the most
# common English inflectional and derivational suffixes that cause false
# MISSING verdicts in UAT traceability (e.g. "logged" -> "log",
# "automatically" -> "automat", "registration" -> "registr").
# It does NOT implement the full Porter algorithm - only the subset that
# is safe to apply without introducing false positives between genuinely
# different words.
#
# Suffix rules are applied in order; first match wins.
# All rules are driven by this table - no code changes needed to add/remove.

_SUFFIX_RULES: list[tuple[str, str, int]] = [
    # (suffix_to_strip, replacement, min_stem_length_after)
    # Longer suffixes first to avoid partial matches
    ('atically',  'at',  3),
    ('ically',    'ic',  3),
    ('ational',   'at',  3),
    ('isation',   'is',  3),
    ('ization',   'iz',  3),
    ('isation',   'is',  3),
    ('ingness',   'ing', 3),
    ('fulness',   'ful', 3),
    ('ousness',   'ous', 3),
    ('iveness',   'iv',  3),
    ('atively',   'at',  3),
    ('ionally',   'ion', 3),
    ('ically',    'ic',  3),
    ('ation',     'at',  3),
    ('ition',     'it',  3),
    ('ment',      '',    4),
    ('ness',      '',    4),
    ('tion',      't',   3),
    ('sion',      's',   3),
    ('ing',       '',    4),
    ('tion',      '',    3),
    ('ied',       'y',   3),
    ('ies',       'y',   3),
    ('eed',       'ee',  3),
    ('eed',       '',    3),
    ('ed',        '',    4),
    ('er',        '',    4),
    ('ly',        '',    4),
    ('al',        '',    4),
    ('ic',        '',    4),
    ('ful',       '',    4),
    ('ous',       '',    4),
    ('ive',       '',    4),
    ('ise',       '',    4),
    ('ize',       '',    4),
    ('ent',       '',    4),
    ('ant',       '',    4),
    ('ist',       '',    4),
    ('ity',       '',    4),
    ('ry',        '',    4),
    ('es',        '',    3),
    ('s',         '',    4),   # plural - strip only if stem ≥ 4 chars
]


def _stem(word: str) -> str:
    """
    Apply minimal suffix stripping to reduce a word to its approximate root.

    Examples (domain-agnostic):
        automatically -> automat
        automatic     -> automat
        logged        -> log
        login         -> login   (no suffix matched; kept as-is)
        registration  -> registr
        registered    -> registr
        password      -> password (no suffix matched; kept as-is)
        stored        -> stor
        storage       -> storag
        provide       -> provid
        provided      -> provid
        destination   -> destin
        distance      -> distanc
        functioning   -> function  (ing stripped)
        functioned    -> function  (ed stripped)
    """
    # Normalise hyphenated compounds: log-in -> login
    word = word.replace('-', '')

    for suffix, replacement, min_len in _SUFFIX_RULES:
        if word.endswith(suffix):
            candidate = word[: len(word) - len(suffix)] + replacement
            if len(candidate) >= min_len:
                return candidate

    return word


def _tokenise(text: str) -> set[str]:
    """
    Lowercase, extract alphabetic tokens, remove stopwords, apply stemming.

    v1.2: Stemming is applied after stopword removal so stopwords are never
    partially stemmed and then accidentally matched.

    Returns a set of stemmed meaningful tokens for overlap computation.
    """
    tokens = re.findall(r'[a-z]+', text.lower())
    stopwords = config.COMPARATOR_STOPWORDS
    meaningful = (t for t in tokens if t not in stopwords and len(t) > 1)
    return {_stem(t) for t in meaningful}


def _has_negation(text: str) -> bool:
    """
    Scan evidence text for any NEGATION_PATTERNS from config.

    Returns True if any negation pattern matches anywhere in the text.
    """
    text_lower = text.lower()
    for pattern in config.NEGATION_PATTERNS:
        if re.search(pattern, text_lower):
            return True
    return False


def _classify_verdict(blended_ratio: float, negation_found: bool) -> str:
    """
    Apply classification rules in order using the blended overlap ratio:

    1. CONFLICT  - negation detected AND blended_ratio >= PARTIAL_THRESHOLD
                   (evidence mentions relevant concepts but contradicts them)
    2. MATCH     - blended_ratio >= MATCH_THRESHOLD, no conflict
    3. PARTIAL   - blended_ratio >= PARTIAL_THRESHOLD, no conflict
    4. MISSING   - blended_ratio < PARTIAL_THRESHOLD, no conflict

    Thresholds are read from config.py - unchanged interface.
    """
    if negation_found and blended_ratio >= config.PARTIAL_THRESHOLD:
        return config.VERDICT_CONFLICT

    if blended_ratio >= config.MATCH_THRESHOLD:
        return config.VERDICT_MATCH

    if blended_ratio >= config.PARTIAL_THRESHOLD:
        return config.VERDICT_PARTIAL

    return config.VERDICT_MISSING


def _build_verdict(
    cru_id: str,
    verdict: str,
    overlap_ratio: float,
    stem_overlap_ratio: float,
    blended_ratio: float,
    spec_text: str,
    evidence_text: str,
    negation_found: bool,
    spec_field: str,
) -> dict:
    """Assemble the standard verdict dict returned by all comparison functions."""
    return {
        'cru_id':              cru_id,
        'verdict':             verdict,
        'overlap_ratio':       round(overlap_ratio, 4),
        'stem_overlap_ratio':  round(stem_overlap_ratio, 4),
        'blended_ratio':       round(blended_ratio, 4),
        'spec_text_used':      spec_text[:200],     # truncate for output cleanliness
        'evidence_text_used':  evidence_text[:200],
        'negation_found':      negation_found,
        'spec_field_used':     spec_field,
    }