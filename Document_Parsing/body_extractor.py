"""
body_extractor.py — PDF body extraction for the Autopilot-QA Ingestion Engine.

Public API: extract_body(pdf_path, document_skeleton, page_offset, doc_meta) -> dict

Iterates every page of the PDF body (pages from page_offset onward), extracts
all text and table blocks, assigns each block to its active section via
SectionTracker, and emits a flat ordered list of typed block dicts.

Block types emitted: heading, paragraph, list_item, table
Image blocks emitted separately to: result["images"]

Does NOT classify blocks semantically — that is block_classifier.py.
Does NOT write any output file — caller (runner.py) handles file I/O.
Does NOT infer section hierarchy from fonts — uses SectionTracker exclusively.

Imports: utils.clean_text, section_tracker.SectionTracker
"""

import re
from collections import defaultdict

import pdfplumber

from utils import clean_text
from section_tracker import SectionTracker


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

# Minimum text length to emit a paragraph block (shorter = noise)
MIN_PARA_LEN = 3

# Bullet characters that signal a list_item
BULLET_CHARS = frozenset('- ·\u2023\u2043\u25b8\u25b9\u25ba\u25bb\u25e6\u2010\u002d\u2013\u2014*')

# Labeled requirement field prefixes — each starts a new block unconditionally
FIELD_LABEL_RE = re.compile(r'^(ID|TITLE|DESC|RAT|DEP|FIT)\s*:')

# Appendix heading pattern — matches "Appendix I: Title", "Appendix A. Title" etc.
APPENDIX_PREFIX_RE = re.compile(r"^Appendix", re.IGNORECASE)

# Ordered list prefix pattern: "1.", "1)", "a.", "(1)", etc.
ORDERED_LIST_RE = re.compile(r'^\s*(\d+|[a-zA-Z])[.)]\s+')

# Numeric section prefix — used to detect candidate headings
# Requires at least one space after the number. Matches "3.2.1 Title" or "3." etc.
NUMERIC_PREFIX_RE = re.compile(r'^(\d+(?:\.\d+)*\.?)\s+\S')

# Image suppression gates
IMG_MIN_DIMENSION    = 40      # px — suppress images narrower or shorter than this
IMG_MIN_AREA         = 2000    # px²
IMG_MAX_ASPECT       = 20.0    # suppress extremely elongated images (thin lines)
IMG_MAX_PAGE_COVERAGE = 0.8   # suppress full-page backgrounds
IMG_MIN_PAGE_COVERAGE = 0.005 # suppress tiny dots/icons


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _table_to_text(rows: list) -> str:
    """Convert table rows to pipe-delimited text for downstream processing."""
    lines = []
    for row in rows:
        cells = [str(c).strip() if c is not None else "" for c in row]
        lines.append(" | ".join(cells))
    return "\n".join(lines)


def _inside_table(block_bbox, table_bboxes):
    """Return True if block_bbox is fully contained within any table bbox."""
    bx0, btop, bx1, bbottom = block_bbox
    for tx0, ttop, tx1, tbottom in table_bboxes:
        if bx0 >= tx0 and bx1 <= tx1 and btop >= ttop and bbottom <= tbottom:
            return True
    return False


def _group_words_into_blocks(words):
    """Group pdfplumber word dicts into paragraph block objects.

    Returns a list of block dicts:
        {"lines": [{"text": str, "bbox": (x0, top, x1, bottom)}, ...],
         "bbox": (x0, top, x1, bottom)}

    Algorithm:
      1. Round each word's `top` to the nearest 2px to bucket same-line words.
      2. Sort lines by vertical position.
      3. Group consecutive lines into blocks when the gap between them is <= 8px.
    """
    if not words:
        return []

    # Step 1 — bucket words into lines
    lines_dict = defaultdict(list)
    for word in words:
        line_key = round(word["top"] / 2) * 2
        lines_dict[line_key].append(word)

    # Step 2 — build sorted line objects
    line_objects = []
    for key in sorted(lines_dict.keys()):
        line_words = sorted(lines_dict[key], key=lambda w: w["x0"])
        line_text = " ".join(w["text"] for w in line_words)
        line_bbox = (
            min(w["x0"]     for w in line_words),
            min(w["top"]    for w in line_words),
            max(w["x1"]     for w in line_words),
            max(w["bottom"] for w in line_words),
        )
        line_objects.append({"text": line_text, "bbox": line_bbox})

    # Step 3 — group lines into paragraph blocks (gap threshold: 8px)
    paragraph_blocks = []
    current_lines = [line_objects[0]]

    for i in range(1, len(line_objects)):
        prev_bottom = current_lines[-1]["bbox"][3]   # bottom of last accumulated line
        curr_top    = line_objects[i]["bbox"][1]      # top of next line
        
        next_line_text = line_objects[i]["text"].strip()
        is_field_label = bool(FIELD_LABEL_RE.match(next_line_text))

        if (curr_top - prev_bottom <= 8) and not is_field_label:
            current_lines.append(line_objects[i])
        else:
            paragraph_blocks.append(current_lines)
            current_lines = [line_objects[i]]
    paragraph_blocks.append(current_lines)

    # Compute block-level bbox as union of all line bboxes
    result = []
    for block_lines in paragraph_blocks:
        block_bbox = (
            min(ln["bbox"][0] for ln in block_lines),
            min(ln["bbox"][1] for ln in block_lines),
            max(ln["bbox"][2] for ln in block_lines),
            max(ln["bbox"][3] for ln in block_lines),
        )
        result.append({"lines": block_lines, "bbox": block_bbox})
    return result


def _make_block(
    block_counter, block_type, logical_page, para_index,
    section_path, section_entry, low_confidence, text, doc_meta,
    table_data=None,
):
    """Assemble a complete block dict with all required fields."""
    toc_confirmed  = section_entry.get("toc_confirmed",  False) if section_entry else False
    body_confirmed = section_entry.get("body_confirmed", False) if section_entry else False
    section_title  = section_entry["title"] if section_entry else "Document Preamble"
    sec_id         = f"SEC-{section_path}"

    block = {
        "block_id":   f"B-{block_counter:04d}",
        "block_type": block_type,
        "page":       logical_page,
        "para_index": para_index,
        "section_path":           section_path,
        "section_title":          section_title,
        "section_semantic_type":  None,   # filled by block_classifier
        "section_id":             sec_id,
        "structural_role":        None,   # filled by block_classifier
        "candidate_req_id":       None,   # filled by block_classifier
        "candidate_dependencies": [],     # filled by block_classifier
        "skip":                   None,   # filled by block_classifier
        "toc_confirmed":          toc_confirmed,
        "body_confirmed":         body_confirmed,
        "low_confidence_confirmation": low_confidence,
        "text": text,
        "source_locator": {"page": logical_page, "para": para_index},
        "doc_id":   doc_meta["doc_id"],
        "doc_type": doc_meta["doc_type"],
        "module":   doc_meta["module"],
        "version":  doc_meta["version"],
    }
    if table_data is not None:
        block["table"] = table_data
    return block


def _current_state(tracker):
    """Return normalised section-state variables from the tracker."""
    state = tracker.get_current_section()
    sec_path  = state["section_path"] or "PREAMBLE"
    sec_entry = state["section_entry"]   # may be None (PREAMBLE)
    low_conf  = state["low_confidence"]
    return sec_path, sec_entry, low_conf


def _classify_text_type(text: str) -> str:
    """Return 'list_item' or 'paragraph' for a non-heading block."""
    first_char = text.lstrip()[:1]
    if first_char in BULLET_CHARS:
        return "list_item"
    first_line = text.split("\n")[0]
    if ORDERED_LIST_RE.match(first_line):
        return "list_item"
    return "paragraph"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_body(pdf_path: str, document_skeleton: dict, page_offset: int,
                 doc_meta: dict) -> dict:
    """Extract all body blocks from a PDF and return a structured result dict.

    Args:
        pdf_path:          Path to the PDF file.
        document_skeleton: Skeleton dict from toc_parser; passed to SectionTracker
                           which mutates it in place (Tier-3 body-confirmed sections).
        page_offset:       0-based PDF page index where body content starts.
                           Pages [0, page_offset) are TOC/front-matter and are skipped.
        doc_meta:          Dict with keys: doc_id, doc_type, module, version, source_file.

    Returns:
        {
            "blocks": list[dict],   # heading / paragraph / list_item / table blocks
            "images": list[dict],   # image blocks (separate list)
            "stats": dict
        }
    """
    tracker = SectionTracker(document_skeleton, page_offset)

    blocks        = []
    images        = []
    block_counter = 0
    image_counter = 1
    type_counts   = {"heading": 0, "paragraph": 0, "list_item": 0, "table": 0, "image": 0}

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)

        for page_idx in range(page_offset, total_pages):
            page         = pdf.pages[page_idx]
            logical_page = page_idx - page_offset + 1  # 1-based logical page number
            para_index   = 0                            # reset per page

            # ------------------------------------------------------------------
            # Step 1-2 — Extract tables and collect their bounding boxes
            # ------------------------------------------------------------------
            found_tables = page.find_tables()
            extracted_tables = page.extract_tables()
            paired_tables = list(zip(found_tables, extracted_tables))

            table_bboxes = [ft.bbox for ft, _ in paired_tables]

            # ------------------------------------------------------------------
            # Step 3-4 — Extract and process text blocks
            # ------------------------------------------------------------------
            words = page.extract_words(x_tolerance=3, y_tolerance=3)
            para_blocks = _group_words_into_blocks(words)

            for pb in para_blocks:
                block_lines = pb["lines"]
                block_bbox  = pb["bbox"]
                raw_text    = "\n".join(ln["text"] for ln in block_lines)
                cleaned     = clean_text(raw_text)

                if len(cleaned) < MIN_PARA_LEN:
                    continue   # noise

                first_line   = block_lines[0]["text"]
                in_table     = _inside_table(block_bbox, table_bboxes)
                is_candidate = (not in_table) and (
                    bool(NUMERIC_PREFIX_RE.match(first_line)) or
                    bool(APPENDIX_PREFIX_RE.match(first_line))
                )

                if is_candidate:
                    result = tracker.try_confirm(first_line, logical_page)
                    if result is not None:
                        # ── Confirmed heading ──────────────────────────────
                        sec_path  = result["section_path"]
                        sec_entry = result["section_entry"]
                        low_conf  = result["low_confidence"]
                        heading_text = clean_text(first_line)

                        block = _make_block(
                            block_counter, "heading", logical_page, para_index,
                            sec_path, sec_entry, low_conf, heading_text, doc_meta,
                        )
                        blocks.append(block)
                        type_counts["heading"] += 1
                        block_counter += 1
                        para_index    += 1

                        # Remaining lines after the heading → separate paragraph
                        if len(block_lines) > 1:
                            remainder = clean_text(
                                "\n".join(ln["text"] for ln in block_lines[1:])
                            )
                            if len(remainder) >= MIN_PARA_LEN:
                                btype = _classify_text_type(remainder)
                                block2 = _make_block(
                                    block_counter, btype, logical_page, para_index,
                                    sec_path, sec_entry, low_conf, remainder, doc_meta,
                                )
                                blocks.append(block2)
                                type_counts[btype] += 1
                                block_counter += 1
                                para_index    += 1
                        continue  # move to next block

                # ── Not a heading (or heading not confirmed) ───────────────
                sec_path, sec_entry, low_conf = _current_state(tracker)
                btype = _classify_text_type(cleaned)
                block = _make_block(
                    block_counter, btype, logical_page, para_index,
                    sec_path, sec_entry, low_conf, cleaned, doc_meta,
                )
                blocks.append(block)
                type_counts[btype] += 1
                block_counter += 1
                para_index    += 1

            # ------------------------------------------------------------------
            # Step 5 — Emit table blocks (use current section at time of emission)
            # ------------------------------------------------------------------
            for ft, rows in paired_tables:
                if not rows:       # skip empty tables
                    continue
                sec_path, sec_entry, low_conf = _current_state(tracker)
                headers = [str(c).strip() if c is not None else "" for c in rows[0]]
                body_rows = [
                    [str(c).strip() if c is not None else "" for c in row]
                    for row in rows[1:]
                ]
                table_data = {"headers": headers, "rows": body_rows}
                text = _table_to_text(rows)

                block = _make_block(
                    block_counter, "table", logical_page, para_index,
                    sec_path, sec_entry, low_conf, text, doc_meta,
                    table_data=table_data,
                )
                blocks.append(block)
                type_counts["table"] += 1
                block_counter += 1
                para_index    += 1

            # ------------------------------------------------------------------
            # Step 6 — Image detection
            # ------------------------------------------------------------------
            page_area = page.width * page.height
            for img in page.images:
                w = img.get("width",  0)
                h = img.get("height", 0)
                area = w * h

                # 6-gate suppression filter
                if w < IMG_MIN_DIMENSION:                          continue  # gate 1
                if h < IMG_MIN_DIMENSION:                          continue  # gate 2
                if area < IMG_MIN_AREA:                            continue  # gate 3
                short_side = max(min(w, h), 1)
                if max(w, h) / short_side > IMG_MAX_ASPECT:        continue  # gate 4
                coverage = area / page_area if page_area else 0
                if coverage > IMG_MAX_PAGE_COVERAGE:               continue  # gate 5
                if coverage < IMG_MIN_PAGE_COVERAGE:               continue  # gate 6

                sec_path, _, _ = _current_state(tracker)
                img_id = f"IMG-{image_counter:03d}"
                images.append({
                    "block_id":   img_id,
                    "block_type": "image",
                    "image_id":   img_id,
                    "image_path": f"01_output/images/{img_id}.png",
                    "page":       logical_page,
                    "section_path": sec_path,
                    "skip":       None,
                    "source_locator": {"page": logical_page, "para": 0},
                    "doc_id":  doc_meta["doc_id"],
                    "module":  doc_meta["module"],
                    "version": doc_meta["version"],
                })
                type_counts["image"] += 1
                image_counter += 1

    body_confirmed_count = len(tracker.get_body_confirmed_sections())

    return {
        "blocks": blocks,
        "images": images,
        "stats": {
            "total_pages_scanned":     total_pages - page_offset,
            "total_blocks":            len(blocks),
            "blocks_by_type":          type_counts,
            "body_confirmed_sections": body_confirmed_count,
        },
    }
