"""
constants.py — Named constants and configuration for Internal Audit Makes Easy.
"""

from pathlib import Path

# ── App identity ──────────────────────────────────────────────────────────────
APP_TITLE = "Internal Audit Makes Easy"
APP_VERSION = "2.0.0"

# ── Persistence ───────────────────────────────────────────────────────────────
PERSISTENT_DIR: Path = Path.home() / ".audit_tool_v2"
PERSISTENT_DIR.mkdir(exist_ok=True)
MAPPING_PATH: Path = PERSISTENT_DIR / "stock_mapping.json"

# ── Matching thresholds ───────────────────────────────────────────────────────
DEFAULT_FUZZY_THRESHOLD: float = 60.0   # % — minimum score to accept a fuzzy match
AUTO_ACCEPT_THRESHOLD: float   = 90.0   # % — score at which a match is auto-accepted
HIGH_CONFIDENCE_MIN: float     = 80.0   # % — labelled "High"
MED_CONFIDENCE_MIN: float      = 60.0   # % — labelled "Medium"

DEFAULT_BANK_TOLERANCE: float = 0.0     # absolute amount tolerance

# ── File constraints ──────────────────────────────────────────────────────────
MAX_FILE_ROWS: int   = 100_000          # warn above this many rows
LARGE_FILE_ROWS: int = 10_000           # show progress hint above this

# ── Excel styling ─────────────────────────────────────────────────────────────
COL_MIN_WIDTH: int = 12
COL_MAX_WIDTH: int = 50

# Header colours (hex, no #)
COLOR_BLUE_DARK   = "2F5496"
COLOR_BLUE_NAVY   = "1A5276"
COLOR_BLUE_MED    = "2471A3"
COLOR_GREEN_DARK  = "1E8449"
COLOR_GREEN_TEAL  = "117A65"
COLOR_RED         = "C0392B"
COLOR_ORANGE      = "E67E22"
COLOR_PURPLE      = "7D3C98"
COLOR_PURPLE_DARK = "6C3483"
COLOR_RECON_CHECK = "1E8449"

# Row fill colours
FILL_BLUE_LIGHT   = "DCE6F1"
FILL_RED_LIGHT    = "FADBD8"
FILL_ORANGE_LIGHT = "FDEBD0"
FILL_GREEN_LIGHT  = "D5F5E3"
FILL_PURPLE_LIGHT = "E8DAEF"
FILL_YELLOW_HI    = "FEF9E7"
FILL_GREY_EVEN    = "F2F3F4"
FILL_WHITE        = "FFFFFF"
FILL_NEAR_WHITE   = "FDFEFE"

# ── Noise words stripped during text normalisation ────────────────────────────
NOISE_WORDS: list[str] = [
    "ltd", "limited", "pvt", "private", "inc", "incorporated",
    "llc", "llp", "co", "company", "corp", "corporation",
    "trading", "enterprises", "& co", "and co",
]

# ── Unmatched reason labels ───────────────────────────────────────────────────
REASON_NO_CANDIDATES   = "No similar values found in reference file"
REASON_BELOW_THRESHOLD = "Best fuzzy score below threshold"
REASON_EMPTY_SOURCE    = "Source value is empty"
REASON_CACHED          = "Matched via saved mapping"
REASON_FUZZY           = "Fuzzy text match"
REASON_AUTO_ACCEPTED   = "Auto-accepted (high confidence)"
