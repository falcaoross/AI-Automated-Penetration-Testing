"""
toc_parser.py — Table of Contents parser for the Autopilot-QA Ingestion Engine.

Public API: parse_toc(pdf_path: str) -> dict

Reads a PDF, identifies TOC pages, parses all entries into a document_skeleton,
and detects the page_offset between TOC logical page numbers and PDF physical indices.

Returns:
    {
        "document_skeleton": dict[str, dict],  # section_path → entry metadata
        "page_offset": int,                    # add to (toc_page-1) for pdf index
        "toc_warnings": list[dict]             # blocking warnings, empty if clean
    }

Raises:
    ValueError if no TOC is found in the first 10 pages.
    pdfplumber exceptions propagate for unreadable or missing files.

Imports from utils: normalize_section_path, clean_text
Does NOT classify sections — that is block_classifier.py's responsibility.
"""

import re
import pdfplumber

from utils import normalize_section_path, clean_text


# ---------------------------------------------------------------------------
# Compiled regex patterns
# ---------------------------------------------------------------------------

# Pattern 1 — Appendix TOC entry (checked BEFORE the general pattern)
APPENDIX_RE = re.compile(
    r"^Appendix\s+([IVXivxA-Za-z]+)[:\.\ s]+(.+?)\s*[.\ s\u2026]{2,}\s*(\d+)\s*$"
)

# Pattern 2 — Regular TOC entry
TOC_ENTRY_RE = re.compile(
    r"^(.+?)\s*[.\ s\u2026]{2,}\s*(\d+)\s*$"
)

# Pattern 3 — Numeric section number at start of left-hand TOC text
SECTION_NUM_RE = re.compile(
    r"^(\d+(?:\.\d+)*\.?)\s+(.*)"
)


# ---------------------------------------------------------------------------
# page_offset semantics:
#   pdf_page_index_0based = (toc_page_number - 1) + page_offset
#
# Example (ALI SRS):
#   Cover = pdf.pages[0], TOC = pdf.pages[1], body page 1 = pdf.pages[2]
#   TOC says "Introduction" is on page 1.
#   pdf_index = (1 - 1) + 2 = 2  ✓  → page_offset = 2
# ---------------------------------------------------------------------------


def _count_toc_lines(lines: list) -> int:
    """Return the number of lines that match either TOC pattern."""
    count = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if APPENDIX_RE.match(line) or TOC_ENTRY_RE.match(line):
            count += 1
    return count


def _compute_level(section_path: str) -> int:
    """Return depth level for a normalized section path."""
    if section_path.startswith("A."):
        return 1
    return len(section_path.split("."))


def parse_toc(pdf_path: str) -> dict:
    """
    Parse the Table of Contents from a PDF and return a document_skeleton dict.

    Args:
        pdf_path: Path to the PDF file.

    Returns:
        {
            "document_skeleton": dict[str, dict],
            "page_offset": int,
            "toc_warnings": list[dict]
        }

    Raises:
        ValueError: If no TOC is found in the first 10 pages.
        pdfplumber exceptions propagate for unreadable/missing files.
    """
    document_skeleton = {}
    toc_warnings = []

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)

        # ------------------------------------------------------------------
        # Step 1 — Detect TOC pages (scan first 10 pages)
        # ------------------------------------------------------------------
        first_toc_page_idx = None
        scan_limit = min(10, total_pages)

        for i in range(scan_limit):
            page_text = pdf.pages[i].extract_text() or ""
            lines = page_text.split("\n")
            if _count_toc_lines(lines) >= 3:
                first_toc_page_idx = i
                break

        if first_toc_page_idx is None:
            raise ValueError(
                "TOC_NOT_FOUND: No Table of Contents detected in first 10 pages."
            )

        # ------------------------------------------------------------------
        # Step 2 — Consume all consecutive TOC pages
        # ------------------------------------------------------------------
        toc_lines = []
        last_toc_page_idx = first_toc_page_idx

        for i in range(first_toc_page_idx, total_pages):
            page_text = pdf.pages[i].extract_text() or ""
            lines = page_text.split("\n")
            match_count = _count_toc_lines(lines)

            if i == first_toc_page_idx:
                # Always include the first confirmed TOC page
                toc_lines.extend(lines)
                last_toc_page_idx = i
            elif match_count >= 1:
                toc_lines.extend(lines)
                last_toc_page_idx = i
            else:
                # First page with 0 matching lines → body content starts here
                break

        # ------------------------------------------------------------------
        # Step 3 — Parse each TOC line
        # ------------------------------------------------------------------
        for raw_line in toc_lines:
            line = raw_line.strip()
            if not line:
                continue

            # Try Appendix pattern first
            m_app = APPENDIX_RE.match(line)
            if m_app:
                numeral, title_raw, page_str = m_app.group(1), m_app.group(2), m_app.group(3)
                section_path = normalize_section_path(numeral.strip())
                title = clean_text(title_raw.strip())
                page = int(page_str)
                level = 1
                if section_path not in document_skeleton:
                    document_skeleton[section_path] = {
                        "title": title,
                        "page": page,
                        "level": level,
                        "toc_confirmed": True,
                        "body_confirmed": False,
                    }
                continue

            # Try regular TOC entry pattern
            m_toc = TOC_ENTRY_RE.match(line)
            if m_toc:
                left_text, page_str = m_toc.group(1), m_toc.group(2)
                m_num = SECTION_NUM_RE.match(left_text.strip())
                if m_num:
                    num_raw, title_raw = m_num.group(1), m_num.group(2)
                    num_clean = num_raw.strip().rstrip(".")
                    section_path = normalize_section_path(num_clean)
                    title = clean_text(title_raw.strip())
                    page = int(page_str)
                    level = _compute_level(section_path)
                    if section_path not in document_skeleton:
                        document_skeleton[section_path] = {
                            "title": title,
                            "page": page,
                            "level": level,
                            "toc_confirmed": True,
                            "body_confirmed": False,
                        }
                # If SECTION_NUM_RE does not match: skip silently

        # ------------------------------------------------------------------
        # Step 4 — Validate entry count
        # ------------------------------------------------------------------
        if len(document_skeleton) < 3:
            toc_warnings.append({
                "code": "WARN-TOC-TOO-FEW-ENTRIES",
                "message": (
                    f"TOC produced only {len(document_skeleton)} entries. "
                    "Possible parse failure."
                ),
                "impact": (
                    "document_skeleton may be incomplete. "
                    "Downstream section grouping will be unreliable."
                ),
            })

        # ------------------------------------------------------------------
        # Step 5 — Detect page_offset
        # ------------------------------------------------------------------
        page_offset = 0
        offset_resolved = False

        # Take up to first 3 entries sorted by page number ascending
        sorted_entries = sorted(
            document_skeleton.items(), key=lambda kv: kv[1]["page"]
        )[:3]

        search_start = last_toc_page_idx + 1
        search_end = min(last_toc_page_idx + 6, total_pages - 1)

        for candidate_path, entry in sorted_entries:
            toc_page = entry["page"]
            for j in range(search_start, search_end + 1):
                page_text = pdf.pages[j].extract_text() or ""
                page_lines = page_text.split("\n")
                for pline in page_lines:
                    pline_stripped = pline.strip()
                    if (
                        pline_stripped.startswith(candidate_path + ".")
                        or pline_stripped.startswith(candidate_path + " ")
                    ):
                        computed_offset = j - (toc_page - 1)
                        page_offset = computed_offset
                        offset_resolved = True
                        break
                if offset_resolved:
                    break
            if offset_resolved:
                break

        if not offset_resolved:
            toc_warnings.append({
                "code": "WARN-TOC-OFFSET-UNRESOLVED",
                "message": (
                    "Could not determine page offset between TOC page numbers "
                    "and PDF page indices. Using offset 0."
                ),
                "impact": (
                    "Section page numbers in document_skeleton may be inaccurate. "
                    "Verify manually."
                ),
            })

        # ------------------------------------------------------------------
        # Step 6 — Return result
        # ------------------------------------------------------------------
        return {
            "document_skeleton": document_skeleton,
            "page_offset": page_offset,
            "toc_warnings": toc_warnings,
        }
