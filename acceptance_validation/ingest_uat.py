# ingest_uat.py — Autopilot-QA CAU Layer
# Parses a UAT PDF into a list of raw CAU dicts.
# Fully domain-agnostic: all patterns come from config.py.

from __future__ import annotations

import re
import io
import logging
from pathlib import Path
from typing import Union

from pdfminer.high_level import extract_text_to_fp
from pdfminer.layout import LAParams

import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ingest_uat_pdf(source: Union[str, Path, bytes]) -> tuple[list[dict], str]:
    """
    Parse a UAT PDF and return:
      (raw_cau_list, detected_req_id_pattern)

    `source` can be:
      - a filesystem path (str or Path)
      - raw PDF bytes (for UI / upload mode)
    """
    raw_text = _extract_text(source)
    req_id_pattern = _detect_req_id_pattern(raw_text)
    logger.info("Detected req_id pattern: %s", req_id_pattern)

    raw_caus = _parse_cau_blocks(raw_text, req_id_pattern)
    logger.info("Extracted %d CAU objects from UAT PDF", len(raw_caus))
    return raw_caus, req_id_pattern


# ---------------------------------------------------------------------------
# Step 1: PDF → plain text
# ---------------------------------------------------------------------------

def _extract_text(source: Union[str, Path, bytes]) -> str:
    """Extract plain text from a PDF file path or raw bytes."""
    laparams = LAParams(line_margin=0.5, word_margin=0.1)
    buf = io.StringIO()

    if isinstance(source, (str, Path)):
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"UAT PDF not found: {path}")
        with open(path, 'rb') as fh:
            extract_text_to_fp(fh, buf, laparams=laparams, output_type='text', codec='utf-8')
    elif isinstance(source, bytes):
        extract_text_to_fp(io.BytesIO(source), buf, laparams=laparams, output_type='text', codec='utf-8')
    else:
        raise TypeError(f"Unsupported source type for UAT PDF: {type(source)}")

    text = buf.getvalue()
    logger.debug("Extracted %d characters from UAT PDF", len(text))
    return text


# ---------------------------------------------------------------------------
# Step 2: Auto-detect req_id pattern from the PDF text
# ---------------------------------------------------------------------------

def _detect_req_id_pattern(text: str) -> str:
    """
    Scan the text for all capitalised-ID prefixes that appear in a
    'Requirement IDs' context and build a matching regex that captures
    every prefix found there.  Falls back to a frequency-based heuristic,
    then to config.REQ_ID_PATTERN.

    BUG FIX: The old implementation promoted a single 'dominant' prefix
    (≥50 % of ALL token occurrences in the document) to an exclusive
    pattern.  In practice the most frequent prefix (e.g. 'FR') dominated
    the count even though 'QR', 'UC', etc. also appear as legitimate
    requirement IDs in the Requirement IDs field.  Any prefix that didn't
    win the 50 % threshold was silently dropped, so QR-prefixed IDs were
    never extracted.

    Fix strategy
    ────────────
    1. First, scan *only* the lines that immediately follow a
       'Requirement IDs' label (or equivalent from config.FIELD_LABEL_MAP).
       Every distinct prefix found there is a confirmed req_id prefix.
       Build the pattern from these and return early.
    2. If step 1 yields nothing (label not present / unusual layout),
       fall back to the top-N prefixes by frequency across the whole
       document — but keep ALL prefixes whose count is at least
       MIN_PREFIX_SHARE of the leading prefix (default 10 %), rather
       than discarding everything below 50 %.
    3. Final fallback: config.REQ_ID_PATTERN.

    This is still fully domain-agnostic: the 'Requirement IDs' label is
    read from config.FIELD_LABEL_MAP, not hardcoded here.
    """
    # ── Step 1: context-aware scan around the req_ids label ───────────────
    # Find the canonical label text for the req_ids field from config.
    req_ids_labels = [
        label for label, field in config.FIELD_LABEL_MAP.items()
        if field == 'req_ids'
    ]

    if req_ids_labels:
        label_re = re.compile(
            r'(?:' + '|'.join(re.escape(l) for l in req_ids_labels) + r')\s*:?\s*(.+)',
            re.IGNORECASE,
        )
        candidate_re = re.compile(r'\b([A-Z]{1,5}\d+)\b')
        context_prefixes: set[str] = set()

        for line in text.splitlines():
            m = label_re.search(line)
            if m:
                for tok in candidate_re.findall(m.group(1)):
                    prefix = re.match(r'[A-Z]+', tok).group()
                    context_prefixes.add(prefix)

        if context_prefixes:
            alternation = '|'.join(re.escape(p) for p in sorted(context_prefixes))
            pattern = r'\b((?:' + alternation + r')\d+)\b'
            logger.info(
                "Context-derived req_id prefixes %s — pattern: %s",
                sorted(context_prefixes), pattern,
            )
            return pattern

    # ── Step 2: frequency-based heuristic with a generous inclusion bar ───
    candidate_re = re.compile(r'\b([A-Z]{1,5}\d+)\b')
    tokens = candidate_re.findall(text)

    if not tokens:
        logger.warning("No req_id candidates found — using default pattern")
        return config.REQ_ID_PATTERN

    prefix_counts: dict[str, int] = {}
    for tok in tokens:
        prefix = re.match(r'[A-Z]+', tok).group()
        prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1

    max_count = max(prefix_counts.values())
    # Keep any prefix that appears at least MIN_PREFIX_SHARE of the top prefix.
    # Default 0.10 — generous enough to capture secondary prefixes like QR
    # alongside a dominant FR, but still filters out noise (page numbers, etc.).
    min_share = getattr(config, 'MIN_PREFIX_SHARE', 0.10)
    kept = sorted(
        [p for p, c in prefix_counts.items() if c / max_count >= min_share],
        key=lambda p: -prefix_counts[p],
    )[:10]  # cap at 10 to keep the regex sane

    if not kept:
        logger.warning("Prefix filter removed all candidates — using default pattern")
        return config.REQ_ID_PATTERN

    alternation = '|'.join(re.escape(p) for p in kept)
    pattern = r'\b((?:' + alternation + r')\d+)\b'
    logger.info("Frequency-based req_id prefixes %s — pattern: %s", kept, pattern)
    return pattern


# ---------------------------------------------------------------------------
# Step 3: Parse CAU blocks from text
# ---------------------------------------------------------------------------

def _parse_cau_blocks(text: str, req_id_pattern: str) -> list[dict]:
    """
    Split the text into per-UAT-test-case blocks and parse each one into a
    raw CAU dict using the configurable FIELD_LABEL_MAP.
    """
    header_re = re.compile(config.UAT_HEADER_PATTERN, re.IGNORECASE)
    lines = text.splitlines()

    # Identify header line indices
    header_positions: list[tuple[int, str, str]] = []  # (line_idx, uat_id, title)
    for idx, line in enumerate(lines):
        m = header_re.match(line.strip())
        if m:
            header_positions.append((idx, m.group(1).upper(), m.group(2).strip()))

    if not header_positions:
        logger.warning("No UAT test-case headers found — check UAT_HEADER_PATTERN")
        return []

    # Slice text between consecutive headers
    raw_caus: list[dict] = []
    for i, (start_idx, uat_id, title) in enumerate(header_positions):
        end_idx = header_positions[i + 1][0] if i + 1 < len(header_positions) else len(lines)
        block_lines = lines[start_idx:end_idx]
        cau = _parse_single_block(block_lines, uat_id, title, req_id_pattern)
        raw_caus.append(cau)

    return raw_caus


def _parse_single_block(
    lines: list[str],
    uat_id: str,
    title: str,
    req_id_pattern: str,
) -> dict:
    """
    Parse one UAT test-case block into a raw CAU dict.

    Handles TWO layout styles produced by pdfminer from table-formatted UAT docs:

    Style A (inline)  — label and value on the same line:
        Requirement IDs: FR1, FR3

    Style B (split)   — label on one line, value on the next non-empty line(s):
        Requirement IDs
        <blank>
        FR1, FR3

    Also handles the pdfminer two-column burst pattern where all labels appear
    first before any values:
        Requirement IDs
        Description
        Pre-condition
        Test Steps
        ...
        <values follow in order>

    BUG FIX (field boundary / description+precondition bleed)
    ──────────────────────────────────────────────────────────
    The old STOP_PATTERNS regex only matched document-level section headings
    (e.g. "4. Identified Changes").  It did NOT recognise the per-test-case
    section headings that pdfminer emits between test cases
    (e.g. "3.2 User Class 2 – Restaurant Owner", "3.3 User Class 3 –
    Administrator").  When such a heading appeared inside the trailing lines
    of a block — typically right after "No issues were reported." — it was
    treated as ordinary content and appended to the tester_observations field.

    Fix: extend STOP_PATTERNS to also halt on numbered sub-section headings
    of the form "N.N <Title>" that match config.SECTION_HEADING_PATTERN (if
    defined) or the built-in fallback pattern.  This is still fully
    domain-agnostic because the pattern matches structure, not words.
    """
    label_map = {k.lower().strip(): v for k, v in config.FIELD_LABEL_MAP.items()}

    # Regex: label optionally followed by colon+value (Style A) or alone (Style B)
    label_line_re = re.compile(
        r'^\s*(' + '|'.join(re.escape(k) for k in label_map) + r')\s*(?::(.*))?$',
        re.IGNORECASE,
    )

    # ── BUG FIX: extended stop patterns ───────────────────────────────────
    # The original pattern only caught document-level section starts.
    # We now also stop at any numbered sub-section heading that looks like
    # "3.2 Something" or "3.3 Something" — i.e. the inter-test-case dividers
    # pdfminer picks up when the PDF uses section headings between table rows.
    #
    # config.SECTION_HEADING_PATTERN (optional) lets operators override this
    # for unusual numbering schemes.  The built-in fallback matches the
    # common "N.N[.N] <Word>" form and is conservative enough not to
    # accidentally swallow content lines.
    _section_heading_fallback = r'^\d+\.\d+(?:\.\d+)?\s+\S'
    _section_heading_pat = getattr(
        config, 'SECTION_HEADING_PATTERN', _section_heading_fallback
    )

    STOP_PATTERNS = re.compile(
        r'(?:'
        + r'^(?:\d+\.\d*\s+(?:identified|summary|sign|change|new req)|'
        + r'table\s+\d+|4\.\s|5\.\s)'
        + r'|'
        + _section_heading_pat
        + r')',
        re.IGNORECASE,
    )

    cau: dict = {
        'uat_id':          uat_id,
        'title':           title,
        'actor_class':     _infer_actor(lines),
        'req_ids':         [],
        'description':     '',
        'precondition':    [],
        'test_steps':      [],
        'expected_result': '',
        'actual_result':   '',
        'status':          '',
        'observations':    '',
    }

    # ── Pass 1: collect label-burst order and inline values ────────────────
    # Walk lines, record (line_index, canonical_field, inline_value_or_None)
    label_hits: list[tuple[int, str, str]] = []  # (idx, field, inline_value)
    content_lines = lines[1:]  # skip header

    for idx, line in enumerate(content_lines):
        if STOP_PATTERNS.match(line.strip()):
            content_lines = content_lines[:idx]
            break
        m = label_line_re.match(line.strip())
        if m:
            field = label_map[m.group(1).lower().strip()]
            inline = (m.group(2) or '').strip()
            label_hits.append((idx, field, inline))

    if not label_hits:
        return cau

    # ── Pass 2: assign values ──────────────────────────────────────────────
    # For each label hit, collect non-empty lines between it and the next label.
    # If the label had an inline value, use that directly.
    #
    # BUG FIX (description + precondition bleed into test_steps)
    # ──────────────────────────────────────────────────────────
    # In the pdfminer two-column burst layout, ALL label names are emitted
    # consecutively BEFORE any of their values appear.  The old code handled
    # this correctly for most fields, but when a label had NO inline value
    # AND NO lines between it and the next label (because the values hadn't
    # appeared yet in the burst sequence), the buffer was empty and nothing
    # was assigned — correct so far.
    #
    # The problem arose for 'description' and 'precondition' specifically:
    # pdfminer sometimes emits the Description and Pre-condition label names
    # in the left column, then immediately starts the Test Steps label — and
    # then emits ALL values for description, precondition, AND test_steps
    # together in the right column, underneath the Test Steps label.  The old
    # code assigned ALL of that combined value blob to test_steps (the last
    # label it encountered), leaving description and precondition empty.
    #
    # Fix: after Pass 2, detect the specific burst-column misassignment
    # signature — description is empty, precondition is empty, and
    # test_steps starts with text that looks like a description sentence
    # (no leading bullet or step number, substantially long) — and re-split
    # the test_steps content using the structural field separator logic.
    # The split is driven purely by the known field order from the label_hits
    # sequence, NOT by domain keywords, so it remains generic.

    for hit_pos, (idx, field, inline) in enumerate(label_hits):
        next_idx = label_hits[hit_pos + 1][0] if hit_pos + 1 < len(label_hits) else len(content_lines)

        if inline:
            buffer = [inline]
        else:
            buffer = [
                l.strip()
                for l in content_lines[idx + 1: next_idx]
                if l.strip()
            ]

        if buffer:
            _assign_field(cau, field, buffer, req_id_pattern)

    # ── Pass 3: repair burst-column misassignment ──────────────────────────
    _repair_burst_misassignment(cau, label_hits)

    cau['status'] = _normalise_status(cau.get('status', ''))
    return cau


def _repair_burst_misassignment(cau: dict, label_hits: list[tuple[int, str, str]]) -> None:
    """
    Detect and repair TWO distinct pdfminer burst-column misassignment patterns.

    ── Pattern A: description / precondition text swept into test_steps ──────
    Detection (generic):
      - One or more of {description, precondition} is empty.
      - test_steps is non-empty.
      - The first item in test_steps does NOT look like a step (no leading
        digit/bullet) — it appears to be prose that was swept in by the burst.
      - The empty fields appear BEFORE test_steps in the label_hits sequence.
    Repair:
      Walk test_steps items.  Use the first genuinely step-like item (leading
      digit or bullet) as the boundary.  Items before that boundary are
      redistributed to the preceding empty fields in label_hits order.

    ── Pattern B: status / actual_result swept into a later field ───────────
    Detection (generic):
      - status is empty (or falsy) after the standard pass.
      - actual_result is empty, OR actual_result contains what looks like
        test_steps text (i.e. it starts with a step-like token or is very long
        and the real test_steps field is also populated from the same burst).
      - Another non-empty text field that appears AFTER status in label_hits
        starts with a valid STATUS_VALUES token — meaning the burst pushed
        status text into that later field's buffer.
    Repair:
      Scan non-empty string fields that appear after the status label in
      label_hits order.  If one starts with a STATUS token, extract that token
      as the status and strip it from the field it landed in.  Then check
      whether actual_result is empty or contains test_steps text and
      redistribute accordingly.

    Both repairs are driven by label_hits order and config.STATUS_VALUES —
    zero domain-specific terms are hardcoded here.
    """
    STEP_START = re.compile(r'^\s*(?:\d+[.)]\s|[•\-*]\s)')

    fields_in_order = [f for (_, f, _) in label_hits]

    # ── Pattern A ─────────────────────────────────────────────────────────
    if 'test_steps' in fields_in_order:
        empty_before_steps = [
            f for f in fields_in_order
            if f in ('description', 'precondition')
            and (not cau.get(f) or cau[f] == [] or cau[f] == '')
            and fields_in_order.index(f) < fields_in_order.index('test_steps')
        ]

        if empty_before_steps:
            steps = cau.get('test_steps', [])
            if steps:
                first_step_idx = next(
                    (i for i, item in enumerate(steps) if STEP_START.match(item)),
                    None,
                )
                if first_step_idx is not None and first_step_idx > 0:
                    preamble = steps[:first_step_idx]
                    cau['test_steps'] = steps[first_step_idx:]
                    for field in empty_before_steps:
                        if not preamble:
                            break
                        value = preamble.pop(0)
                        if field == 'precondition':
                            cau['precondition'] = [value] if value else []
                        else:
                            cau[field] = value
                    if preamble:
                        logger.warning(
                            "burst-column repair (Pattern A): %d preamble item(s) "
                            "could not be redistributed and were returned to "
                            "test_steps for uat_id=%s",
                            len(preamble), cau.get('uat_id', '?'),
                        )
                        cau['test_steps'] = preamble + cau['test_steps']

    # ── Pattern B: status empty, status token stranded in a later field ───
    # Only attempt if status is currently empty/unknown.
    current_status = cau.get('status', '')
    if current_status and current_status.upper() in config.STATUS_VALUES:
        return  # status already correctly populated; nothing to do

    if 'status' not in fields_in_order:
        return

    status_pos = fields_in_order.index('status')

    # Build an ordered list of string fields that appear AFTER the status
    # label in the document and currently hold non-empty string content.
    status_token_re = re.compile(
        r'(?:^|\s)(' + '|'.join(re.escape(sv) for sv in config.STATUS_VALUES) + r')(?:\s|$)',
        re.IGNORECASE,
    )

    candidate_fields_after_status = [
        f for f in fields_in_order[status_pos + 1:]
        if f not in ('test_steps', 'precondition', 'req_ids')
        and isinstance(cau.get(f), str)
        and cau.get(f, '').strip()
    ]

    for field in candidate_fields_after_status:
        field_text = cau[field].strip()
        m = status_token_re.search(field_text)
        if not m:
            continue

        # Found a status token in this field — extract it.
        found_status = m.group(1).upper()
        # Strip the matched token from the field's text.
        remaining = (field_text[:m.start()] + field_text[m.end():]).strip()
        cau['status'] = found_status
        cau[field] = remaining

        logger.debug(
            "burst-column repair (Pattern B): status='%s' extracted from "
            "field '%s' for uat_id=%s; remaining text='%s'",
            found_status, field, cau.get('uat_id', '?'), remaining[:80],
        )

        # Secondary check: if actual_result is empty but test_steps text
        # appears to have been swept into expected_result (or vice-versa),
        # attempt to recover actual_result from whichever text field
        # immediately follows status in the label_hits sequence.
        actual = cau.get('actual_result', '').strip()
        if not actual:
            # Look for the field that immediately follows 'status' and is
            # non-empty — treat it as the real actual_result if it appears
            # BEFORE any other content field in the post-status sequence.
            for candidate in candidate_fields_after_status:
                candidate_val = cau.get(candidate, '').strip()
                if candidate_val and candidate != field:
                    # Only reassign if actual_result label appears before
                    # this candidate in the document order.
                    if ('actual_result' in fields_in_order and
                            fields_in_order.index('actual_result') <
                            fields_in_order.index(candidate)):
                        cau['actual_result'] = candidate_val
                        cau[candidate] = ''
                        logger.debug(
                            "burst-column repair (Pattern B): actual_result "
                            "recovered from field '%s' for uat_id=%s",
                            candidate, cau.get('uat_id', '?'),
                        )
                    break

        break  # only process the first field that contains a status token


def _assign_field(cau: dict, field: str, buffer: list[str], req_id_pattern: str) -> None:
    """Write buffer content into the correct cau field."""
    text = ' '.join(buffer).strip()

    if field == 'req_ids':
        ids = re.findall(req_id_pattern, text, re.IGNORECASE)
        cau['req_ids'] = list(dict.fromkeys(i.upper() for i in ids))

    elif field in ('precondition', 'test_steps'):
        items = _split_list_items(buffer)
        if isinstance(cau[field], list):
            cau[field].extend(items)
        else:
            cau[field] = items

    elif field == 'status':
        cau['status'] = _normalise_status(text)

    else:
        cau[field] = text


def _split_list_items(lines: list[str]) -> list[str]:
    """Split numbered/bulleted lines into individual items."""
    items: list[str] = []
    bullet_re = re.compile(r'^\s*(?:\d+[.)]\s*|[•\-*]\s*)')
    current: list[str] = []

    for line in lines:
        if bullet_re.match(line):
            if current:
                items.append(' '.join(current).strip())
            current = [bullet_re.sub('', line).strip()]
        else:
            current.append(line.strip())

    if current:
        items.append(' '.join(current).strip())

    return [i for i in items if i]


def _normalise_status(raw: str) -> str:
    """Return the canonical STATUS token or empty string.

    Fix: verbose strings like "NOT TESTED – SCHEDULED FOR NEXT RELEASE CYCLE"
    were not matching the clean 'NOT_TESTED' token because the loop checks
    sv IN upper — 'NOT_TESTED' is not a substring of 'NOT TESTED – SCHEDULED...'.
    Added explicit check for 'NOT TESTED' (space variant) before the fallback.
    """
    upper = raw.strip().upper()
    # Check clean token membership first (handles PASS, FAIL, PARTIAL, NOT_TESTED)
    for sv in config.STATUS_VALUES:
        if sv in upper:
            return sv
    # Catch verbose NOT TESTED strings: "NOT TESTED – ...", "NOT TESTED (SCHEDULED...)"
    if 'NOT TESTED' in upper or 'NOT_TESTED' in upper:
        return 'NOT_TESTED'
    return upper if upper else ''


def _infer_actor(lines: list[str]) -> str:
    """
    Try to infer actor_class from lines preceding the first field label.
    Looks for lines that match ACTOR_BLOCK_PATTERN or contain 'actor' keyword.
    Returns empty string if not found.
    """
    actor_re = re.compile(config.ACTOR_BLOCK_PATTERN, re.IGNORECASE)
    for line in lines[:10]:
        m = actor_re.search(line.strip())
        if m:
            return m.group(1).strip()
        if 'actor' in line.lower():
            parts = re.split(r'[:\-]', line, maxsplit=1)
            if len(parts) == 2:
                return parts[1].strip()
    return ''