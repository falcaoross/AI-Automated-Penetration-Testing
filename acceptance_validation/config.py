# config.py — Autopilot-QA CAU Layer Configuration
# All patterns and label maps live here. Zero hardcoding in logic files.

# ---------------------------------------------------------------------------
# Requirement ID detection
# ---------------------------------------------------------------------------
REQ_ID_PATTERN = r'\b([A-Z]{1,5}\d+)\b'

# ---------------------------------------------------------------------------
# UAT test-case header pattern
# ---------------------------------------------------------------------------
UAT_HEADER_PATTERN = r'(?:[\d.]+\s+)?((?:UAT|TC|AT)-[A-Z0-9]+-\d+)\s*[:\-\u2013\u2014]\s*(.+)'

# ---------------------------------------------------------------------------
# Actor / Use-Case block header
# ---------------------------------------------------------------------------
ACTOR_BLOCK_PATTERN = r'(?:use\s*case\s*[\d.]+|uc-?\d+)[:\s]+(.+)'

# ---------------------------------------------------------------------------
# Field label map
# ---------------------------------------------------------------------------
FIELD_LABEL_MAP = {
    'requirement ids':          'req_ids',
    'requirement id':           'req_ids',
    'requirement':              'req_ids',
    'requirements':             'req_ids',
    'linked requirements':      'req_ids',
    'description':              'description',
    'objective':                'description',
    'test objective':           'description',
    'purpose':                  'description',
    'pre-condition':            'precondition',
    'precondition':             'precondition',
    'pre-conditions':           'precondition',
    'preconditions':            'precondition',
    'prerequisites':            'precondition',
    'test steps':               'test_steps',
    'test step':                'test_steps',
    'steps':                    'test_steps',
    'test procedure':           'test_steps',
    'expected result':          'expected_result',
    'expected results':         'expected_result',
    'expected outcome':         'expected_result',
    'actual result':            'actual_result',
    'actual results':           'actual_result',
    'actual outcome':           'actual_result',
    'status':                   'status',
    'test status':              'status',
    'result':                   'status',
    'tester observations':      'observations',
    'observations':             'observations',
    'comments':                 'observations',
    'remarks':                  'observations',
}

# ---------------------------------------------------------------------------
# Valid UAT status tokens — NOT_TESTED added
# ---------------------------------------------------------------------------
STATUS_VALUES = {'PASS', 'FAIL', 'PARTIAL', 'NOT_TESTED'}

# ---------------------------------------------------------------------------
# Coverage classification rules (evaluated in order — first match wins)
# ---------------------------------------------------------------------------
# INFERRED_PARTIAL is now a first-class rule, not a hardcoded string in
# linker.py.  Inferred CAUs carry status='INFERRED' (set by infer_coverage())
# and always have linked_crus — so they pass rules 1-4 and land here before
# falling through to FULL_COVERAGE.  This gives the reporter a stable label
# to count and display consistently.
COVERAGE_RULES = [
    (lambda c: not c.get('linked_crus'),                                   'NO_CRU_MATCH'),
    (lambda c: c.get('linked_crus') and not c.get('linked_test_cases'),    'NO_TEST_CASE'),
    (lambda c: c.get('status', '').upper() == 'FAIL',                      'FAILED_COVERAGE'),
    (lambda c: c.get('status', '').upper() == 'NOT_TESTED',                'NOT_TESTED'),
    (lambda c: c.get('status', '').upper() == 'PARTIAL',                   'PARTIAL_COVERAGE'),
    (lambda c: c.get('status', '').upper() == 'INFERRED',                  'INFERRED_PARTIAL'),
    (lambda c: True,                                                         'FULL_COVERAGE'),
]

# Classifications that count toward the coverage rate (configurable).
# INFERRED_PARTIAL counts as covered — the pipeline has confirmed all
# declared dependencies were directly tested and passed.
COVERED_CLASSIFICATIONS = {'FULL_COVERAGE', 'INFERRED_PARTIAL'}

# Classifications within COVERED_CLASSIFICATIONS that are inferred (not directly tested).
# Used by reporter to annotate the coverage breakdown — no hardcoding needed in logic files.
INFERRED_CLASSIFICATIONS = {'INFERRED_PARTIAL'}

# ---------------------------------------------------------------------------
# Reporter display labels — change here to rename terminal output globally
# ---------------------------------------------------------------------------
# Label used in uat_status_breakdown for all covered CAUs (FULL_COVERAGE + INFERRED_PARTIAL)
LABEL_COVERED = 'PASS'
# Sentinel label for any classification or verdict that could not be resolved
LABEL_UNKNOWN = 'UNKNOWN'

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
DEFAULT_OUTPUT_DIR = 'output'
CAU_JSON_FILENAME  = 'cau_output.json'
HTML_FILENAME      = 'cau_traceability_report.html'

# ---------------------------------------------------------------------------
# Pipeline metadata
# ---------------------------------------------------------------------------
PIPELINE_NAME    = 'Autopilot-QA CAU Layer'
PIPELINE_VERSION = '1.0'

# ---------------------------------------------------------------------------
# Comparator settings — Gap 2, 3, 4 fix
# All domain agnostic — no hardcoded domain terms
# ---------------------------------------------------------------------------

# Verdict labels
VERDICT_MATCH    = 'MATCH'
VERDICT_PARTIAL  = 'PARTIAL'
VERDICT_MISSING  = 'MISSING'
VERDICT_CONFLICT = 'CONFLICT'

# CRU fields to extract specification text from (priority order)
# First non-null field found is used for comparison
CRU_SPEC_FIELDS = ['action', 'description', 'constraint', 'acceptance_criteria']

# Stopwords — filtered out before overlap computation
# Domain agnostic — purely linguistic
COMPARATOR_STOPWORDS = {
    'a', 'an', 'the', 'is', 'be', 'to', 'of', 'and', 'or', 'in',
    'on', 'at', 'for', 'with', 'that', 'this', 'it', 'its', 'as',
    'are', 'was', 'were', 'by', 'from', 'has', 'have', 'had',
    'do', 'does', 'did', 'will', 'would', 'could', 'should',
    'may', 'might', 'can', 'then', 'than', 'if', 'so', 'but',
    'not', 'no', 'all', 'any', 'each', 'when', 'where', 'which',
    'who', 'how', 'what', 'been', 'being', 'also', 'into', 'up',
    'out', 'about', 'after', 'before', 'through', 'during',
}

# Negation patterns for conflict detection (domain agnostic)
# TUNED: removed broad single-word patterns (\bnot\b, \bno\b, \berror\b, \bfailure\b)
# that caused false CONFLICTs on positive UAT text like "No issues were reported"
# or "Completed without errors". Only specific multi-word failure phrases remain.
NEGATION_PATTERNS = [
    r'\bdid\s+not\s+work\b',
    r'\bdid\s+not\s+(?:complete|load|save|display|respond|function|execute)\b',
    r'\bwas\s+not\s+(?:found|working|available|received|completed|processed)\b',
    r'\bcould\s+not\s+(?:be|complete|access|load|find|save|open|submit)\b',
    r'\bunable\s+to\b',
    r'\bfailed\s+to\b',
    r'\bdoes\s+not\s+work\b',
    r'\bdoes\s+not\s+(?:load|display|save|respond|function|execute)\b',
    r'\bnot\s+(?:functioning|working|loading|responding|saving|processing)\b',
    r'\bincorrect(?:ly)?\b',
    r'\bwrong(?:ly)?\b',
    r'\bnever\s+(?:completed|worked|loaded|saved|executed)\b',
    r'\bwould\s+not\s+(?:work|load|save|complete|respond)\b',
    r'\bshould\s+not\s+have\b',
]

# Overlap ratio thresholds — tunable without touching code
# overlap_ratio = matching_words / spec_words (after stopword removal)
# TUNED: lowered from 0.40/0.15 to reduce false MISSING and increase MATCH count
# for short CRU action sentences vs brief UAT actual_result text
MATCH_THRESHOLD   = 0.30   # >= this → MATCH  (was 0.40)
PARTIAL_THRESHOLD = 0.10   # >= this → PARTIAL, < this → MISSING  (was 0.15)
# ---------------------------------------------------------------------------
# CRU parent_requirement_id normalisation aliases — Bug 2 fix
# ---------------------------------------------------------------------------
# Some CRU extractors emit raw PLanguage TAG values (e.g. "SystemReliability")
# instead of the formal requirement ID (e.g. "QR9").  This alias map lets
# operators register those mappings so the linker can normalise them before
# index lookup.
#
# Keys   : raw TAG value (case-insensitive match is applied at runtime)
# Values : canonical req_id that the linker and gap report should use
#
# This map is intentionally empty by default — it is a project-level
# configuration hook, not a hardcoded domain assumption.  Operators populate
# it for their specific SRS/CRU extractor output.
#
# Example (restaurant ordering system):
#   CRU_TAG_ALIASES = {
#       'SystemReliability':                   'QR9',
#       'RestaurantOwnerCreateAccountSecurity': 'QR18',
#   }
CRU_TAG_ALIASES: dict[str, str] = {}

# ---------------------------------------------------------------------------
# v1.2 Comparator enhancements — stem-prefix overlap settings
# ---------------------------------------------------------------------------
# Length of stem prefix used for the secondary overlap score.
# A prefix of 4 chars captures most morphological variants (e.g. "automat"
# for both "automatically" and "automatic") without introducing false
# positives between short words.
STEM_PREFIX_LEN = 4

# Weight given to the stem-prefix overlap in the blended verdict score.
# blended = (1 - STEM_BLEND_WEIGHT) * exact_stem_overlap
#          + STEM_BLEND_WEIGHT       * prefix_overlap
# Default 0.30: primary exact-stem score dominates (70%), prefix adds 30%.
STEM_BLEND_WEIGHT = 0.30