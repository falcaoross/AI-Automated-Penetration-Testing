"""
section_tracker.py — Active section state manager for the Autopilot-QA Ingestion Engine.

Public API: SectionTracker(document_skeleton, page_offset)
  .try_confirm(heading_text, page) -> dict | None
  .get_current_section()           -> dict
  .get_body_confirmed_sections()   -> dict

Receives the document_skeleton from toc_parser and mutates it in place as
body-confirmed sub-sections are discovered during the body scan.

Confirmation tiers:
  Tier 1 — TOC path + title match      (high confidence)
  Tier 2 — TOC path match, title drift (low_confidence=True)
  Tier 3 — Body-only sub-section       (added to skeleton, high confidence)
  Tier 4 — No match                    (returns None, state unchanged)

Does NOT detect headings — that is body_extractor.py's responsibility.
Does NOT open or read any PDF file.
Does NOT classify blocks semantically.
"""

import re

from utils import normalize_section_path


# ---------------------------------------------------------------------------
# Compiled regex patterns
# ---------------------------------------------------------------------------

# Extracts a numeric section prefix (with optional trailing period) from
# the start of a heading string.
# Group 1: section number, e.g. "3.2.1" or "3." or "3.2.1.1"
# Group 2: remainder title text
PREFIX_RE = re.compile(r"^(\d+(?:\.\d+)*\.?)\s+(.*)")

APPENDIX_BODY_RE = re.compile(
    r"^Appendix\s+([IVXivxA-Za-z]+)[:\.\s]+(.*)", re.IGNORECASE
)

def _norm_title(t: str) -> str:
    """Normalize a title string for fuzzy Tier 1 comparison.

    Lowercases, removes all characters except a-z, 0-9, and space, then strips.
    This handles punctuation differences, hyphens, and minor OCR truncation.
    Note: 'abbreviation' vs 'abbreviations' will NOT match — no stemming is
    applied. Such cases fall through to Tier 2 (low_confidence=True), which
    is expected and documented behaviour.
    """
    return re.sub(r'[^a-z0-9 ]', '', t.lower()).strip()


class SectionTracker:
    """Tracks the active section during a sequential body scan.

    Holds a reference to the document_skeleton and mutates it in place when
    body-only sections (Tier 3) are discovered. The caller (body_extractor)
    must pass the same dict object so that runner.py sees the final augmented
    skeleton.
    """

    def __init__(self, document_skeleton: dict, page_offset: int):
        """Initialise the tracker.

        Args:
            document_skeleton: The skeleton dict produced by toc_parser.
                               Held by reference and mutated in place.
            page_offset:       Stored for body_extractor access; not used
                               internally by the tracker itself.
        """
        self.skeleton = document_skeleton          # reference — NOT a copy
        self.page_offset = page_offset
        self.current_section_path = None
        self.current_section_entry = None
        self.low_confidence = False
        self._path_set = set(document_skeleton.keys())  # O(1) lookup

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def try_confirm(self, heading_text: str, page: int):
        """Attempt to confirm a candidate heading against the skeleton.

        heading_text must already be cleaned (body_extractor calls clean_text
        before passing it here — do NOT call clean_text again).

        Args:
            heading_text: Cleaned text of the candidate heading block.
            page:         Logical page number (already offset-corrected by
                          body_extractor).

        Returns:
            A result dict on Tier 1/2/3 confirmation, or None (Tier 4).
        """
        if not heading_text or not heading_text.strip():
            return None
        
        # ── Appendix heading fast-path ─────────────────────────────────
        m_app = APPENDIX_BODY_RE.match(heading_text.strip())
        if m_app:
            numeral = m_app.group(1).strip()
            section_path = normalize_section_path(numeral)
            if section_path in self._path_set:
                return self._confirm(section_path, tier=1, low_confidence=False)
            return None

        # Step 1 — Extract numeric prefix
        m = PREFIX_RE.match(heading_text.strip())
        if not m:
            return None  # Tier 4 immediately — no numeric prefix

        num_raw = m.group(1).rstrip(".")
        title_raw = m.group(2).strip()

        # Step 2 — Normalize prefix to a section path
        section_path = normalize_section_path(num_raw)

        # Step 3 — Tier 1: path in skeleton AND title matches
        if section_path in self._path_set:
            known_title = self.skeleton[section_path]["title"]
            if _norm_title(title_raw) == _norm_title(known_title):
                return self._confirm(section_path, tier=1, low_confidence=False)

            # Step 4 — Tier 2: path in skeleton but title does not match
            # (OCR drift, truncation, or minor wording difference)
            return self._confirm(section_path, tier=2, low_confidence=True)

        # Step 5 — Tier 3: path NOT in skeleton, but num_raw contains a dot
        # (sub-section only — bare numbers like "5" are NOT added)
        if "." in num_raw:
            entry = {
                "title": title_raw,
                "page": page,
                "level": len(section_path.split(".")),
                "toc_confirmed": False,
                "body_confirmed": True,
            }
            self.skeleton[section_path] = entry
            self._path_set.add(section_path)
            return self._confirm(section_path, tier=3, low_confidence=False)

        # Step 6 — Tier 4: no match
        return None

    def get_current_section(self) -> dict:
        """Return the current active section state.

        Returns a dict with section_path=None and section_entry=None if no
        heading has been confirmed yet. body_extractor assigns those blocks to
        a special PREAMBLE section.
        """
        return {
            "section_path": self.current_section_path,
            "section_entry": self.current_section_entry,
            "low_confidence": self.low_confidence,
        }

    def get_body_confirmed_sections(self) -> dict:
        """Return only the body-confirmed sections added during the scan.

        Used by runner.py for audit reporting. Excludes all TOC-only entries.
        """
        return {
            k: v for k, v in self.skeleton.items()
            if v.get("body_confirmed") is True
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _confirm(self, section_path: str, tier: int, low_confidence: bool) -> dict:
        """Update internal state and return a confirmation result dict."""
        self.current_section_path = section_path
        self.current_section_entry = self.skeleton[section_path]
        self.low_confidence = low_confidence
        return {
            "section_path": section_path,
            "section_entry": self.skeleton[section_path],
            "tier": tier,
            "low_confidence": low_confidence,
        }
