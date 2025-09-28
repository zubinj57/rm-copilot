"""
Month normalizer: English-only, numeric-aware, fuzzy-typo tolerant.

Public API:
- normalize_month_str(value) -> str
- month_num_for_sort(value) -> Optional[int]
- parse_month(value, *, allow_fuzzy=True, fuzzy_cutoff=0.84) -> MonthParseResult
"""

from __future__ import annotations
import re
import difflib
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

# Optional pandas: used only for NaN detection if present.
try:
    import pandas as pd  # type: ignore
    _HAS_PD = True
except Exception:
    _HAS_PD = False

# ---------------- Canonical months ----------------
MONTHS = [
    "January","February","March","April","May","June",
    "July","August","September","October","November","December"
]
MONTH_TO_NUM = {m: i+1 for i, m in enumerate(MONTHS)}
NUM_TO_MONTH = {v: k for k, v in MONTH_TO_NUM.items()}

# ---------------- Aliases & common misspellings ----------------
ALIASES = {
    # full/short/dotted
    "january":"January","jan":"January","jan.":"January",
    "february":"February","feb":"February","feb.":"February","febr":"February",
    "march":"March","mar":"March","mar.":"March",
    "april":"April","apr":"April","apr.":"April",
    "may":"May",
    "june":"June","jun":"June","jun.":"June",
    "july":"July","jul":"July","jul.":"July",
    "august":"August","aug":"August","aug.":"August",
    "september":"September","sep":"September","sep.":"September","sept":"September","sept.":"September",
    "october":"October","oct":"October","oct.":"October",
    "november":"November","nov":"November","nov.":"November",
    "december":"December","dec":"December","dec.":"December",
    # typos/truncations often seen in exports / manual inputs
    "janaury":"January","januray":"January",
    "febuary":"February","feburary":"February",
    "marhc":"March","apirl":"April",
    "augest":"August","agust":"August",
    "septembr":"September","septemeber":"September","setember":"September",
    "novemeber":"November","decemebr":"December",
}

FUZZY_KEYS = sorted(set(list(ALIASES.keys()) + [m.lower() for m in MONTHS]))
DEFAULT_FUZZY_CUTOFF = 0.84  # conservative to avoid May↔Mar errors

# Precompiled regex for perf
_RE_ISO = re.compile(r"\b(\d{4})[-/](\d{1,2})\b")
_RE_TOKEN_1_12 = re.compile(r"\b(0?[1-9]|1[0-2])\b")
_RE_ORDINAL = re.compile(r"\b(\d{1,2})(st|nd|rd|th)\s+month\b", flags=re.IGNORECASE)
_RE_LETTERS_OR_DOT = re.compile(r"[^A-Za-z.]")

@dataclass(frozen=True)
class MonthParseResult:
    original: str
    normalized_name: Optional[str]  # "January".. "December" or None
    month_num: Optional[int]        # 1..12 or None
    confidence: float               # 0..1
    source: str                     # "numeric" | "alias" | "fuzzy" | "fallback" | "nan"

def _is_nan_like(x) -> bool:
    if x is None:
        return True
    if _HAS_PD:
        try:
            return bool(pd.isna(x))
        except Exception:
            return False
    return False

def _coerce_numeric_month(s: str) -> Optional[int]:
    """
    Numeric detection:
      - "1", "01", "12"
      - "YYYY-MM" / "YYYY/MM"
      - standalone tokens 1..12 (rightmost bias)
      - ordinals like "2nd month"
    """
    t = s.strip()

    # Pure digits
    if re.fullmatch(r"\d{1,2}", t):
        v = int(t)
        if 1 <= v <= 12:
            return v

    # ISO-ish YYYY-MM / YYYY/MM
    m = _RE_ISO.search(t)
    if m:
        mm = int(m.group(2))
        if 1 <= mm <= 12:
            return mm

    # tokens 1..12 (prefer the last one)
    toks = _RE_TOKEN_1_12.findall(t)
    if toks:
        return int(toks[-1])

    # "2nd month", "11th month"
    m2 = _RE_ORDINAL.search(t)
    if m2:
        v = int(m2.group(1))
        if 1 <= v <= 12:
            return v

    return None

@lru_cache(maxsize=4096)
def parse_month(value, *, allow_fuzzy: bool = True, fuzzy_cutoff: float = DEFAULT_FUZZY_CUTOFF) -> MonthParseResult:
    """
    Best-effort month parser (English only).
    Priority: numeric -> alias -> fuzzy -> fallback.
    """
    if _is_nan_like(value):
        return MonthParseResult(str(value), None, None, 0.0, "nan")

    raw = str(value).strip()

    # 1) numeric signals
    mnum = _coerce_numeric_month(raw)
    if mnum:
        return MonthParseResult(raw, NUM_TO_MONTH[mnum], mnum, 0.98, "numeric")

    # 2) alias/direct text (keep dotted abbrevs first; then without dots)
    letters = _RE_LETTERS_OR_DOT.sub("", raw).lower()
    if letters in ALIASES:
        name = ALIASES[letters]
        return MonthParseResult(raw, name, MONTH_TO_NUM[name], 0.97, "alias")

    letters_nodot = letters.replace(".", "")
    if letters_nodot in ALIASES:
        name = ALIASES[letters_nodot]
        return MonthParseResult(raw, name, MONTH_TO_NUM[name], 0.965, "alias")

    # 3) fuzzy (conservative)
    if allow_fuzzy:
        match = difflib.get_close_matches(letters_nodot, FUZZY_KEYS, n=1, cutoff=fuzzy_cutoff)
        if match:
            key = match[0]
            name = ALIASES.get(key, key.title())  # if canonical month lowercased
            ratio = difflib.SequenceMatcher(None, letters_nodot, key).ratio()
            conf = 0.88 + 0.1*(ratio - fuzzy_cutoff)/(1.0 - fuzzy_cutoff)  # ~[0.88,0.98]
            conf = max(0.88, min(0.98, conf))
            return MonthParseResult(raw, name, MONTH_TO_NUM[name], conf, "fuzzy")

    # 4) fallback (non-guessy; only returns a month_num if it happens to match exactly)
    fb = raw.title()
    mnum = MONTH_TO_NUM.get(fb)
    return MonthParseResult(raw, fb if mnum else None, mnum, 0.30, "fallback")

# -------- Thin wrappers (drop-in replacements for your code) --------
def normalize_month_str(value) -> str:
    """Return canonical month name or 'Unknown' if not confidently resolved."""
    res = parse_month(value)
    return res.normalized_name or "Unknown"

def month_num_for_sort(value) -> Optional[int]:
    """Return 1..12 for sortable month, else None."""
    res = parse_month(value)
    return res.month_num

__all__ = [
    "MONTHS", "MONTH_TO_NUM", "NUM_TO_MONTH",
    "normalize_month_str", "month_num_for_sort",
    "parse_month", "MonthParseResult",
]

