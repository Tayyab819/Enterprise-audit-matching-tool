"""
stock_engine.py — Stock Reconciliation matching engine.

All state is encapsulated in StockReconState (a dataclass).
No global variables — Gradio state objects hold instances of this class.

Improvements over v1:
  - Class-based state (no globals)
  - Text normalisation before fuzzy matching
  - Fuzzy-match cache keyed on (normalised_value, frozenset_of_choices, threshold)
  - Auto-accept configurable threshold
  - Richer confidence scoring with reason field
  - Duplicate-aware: multiple source rows sharing a value all get same result
  - Detailed unmatched reasons
  - Full type hints and docstrings
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd
from rapidfuzz import fuzz, process

from constants import (
    AUTO_ACCEPT_THRESHOLD,
    DEFAULT_FUZZY_THRESHOLD,
    HIGH_CONFIDENCE_MIN,
    MED_CONFIDENCE_MIN,
    MAPPING_PATH,
    REASON_AUTO_ACCEPTED,
    REASON_BELOW_THRESHOLD,
    REASON_CACHED,
    REASON_EMPTY_SOURCE,
    REASON_FUZZY,
    REASON_NO_CANDIDATES,
)
from utils import log_exception, normalise_text, normalise_series

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Confidence scoring
# ──────────────────────────────────────────────────────────────────────────────

def confidence_label(score: float) -> str:
    """Return a human-readable confidence label based on fuzzy score."""
    if score >= HIGH_CONFIDENCE_MIN:
        return "High"
    if score >= MED_CONFIDENCE_MIN:
        return "Medium"
    return "Low"


def confidence_score_adjusted(
    raw_score: float,
    method: str,
    source_len: int,
    match_len: int,
) -> float:
    """
    Adjust raw fuzzy score with heuristics for a more robust confidence value.

    Penalties:
      - Large length ratio difference (very short vs very long strings)
      - Low-match method
    Bonuses:
      - Cached (mapping) matches always return 100.
    """
    if method == REASON_CACHED:
        return 100.0
    score = raw_score
    # Penalise extreme length disparity
    if source_len > 0 and match_len > 0:
        ratio = min(source_len, match_len) / max(source_len, match_len)
        if ratio < 0.4:
            score *= 0.85
    return round(min(score, 100.0), 2)


# ──────────────────────────────────────────────────────────────────────────────
# Mapping persistence
# ──────────────────────────────────────────────────────────────────────────────

def load_mapping(path: Path = MAPPING_PATH) -> dict[str, str]:
    """Load saved name mappings from JSON. Returns empty dict on any failure."""
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data: dict[str, str] = json.load(f)
        log.info("Loaded %d mapping entries from %s", len(data), path)
        return data
    except Exception as exc:
        log_exception("Could not read mapping file", exc)
        return {}


def save_mapping(mapping: dict[str, str], path: Path = MAPPING_PATH) -> None:
    """Persist name mappings to JSON atomically (write + rename)."""
    try:
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(mapping, f, indent=2, ensure_ascii=False)
        tmp.replace(path)
        log.info("Saved %d mapping entries to %s", len(mapping), path)
    except Exception as exc:
        log_exception("Could not save mapping file", exc)


# ──────────────────────────────────────────────────────────────────────────────
# State container
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class StockReconState:
    """
    Holds all mutable state for one Stock Reconciliation session.
    Instances live inside a gr.State component — never as module globals.
    """
    df1: Optional[pd.DataFrame]              = None   # Purchase / source file
    df2: Optional[pd.DataFrame]              = None   # Sale / reference file
    col1: Optional[str]                      = None   # Match column in df1
    col2: Optional[str]                      = None   # Match column in df2
    results: Optional[pd.DataFrame]          = None   # Last match results
    unmatched_sales: Optional[pd.DataFrame]  = None   # df2 rows never matched
    mapping: dict[str, str]                  = field(default_factory=load_mapping)
    _fuzzy_cache: dict                       = field(default_factory=dict, repr=False)

    # ── Fuzzy match with caching ──────────────────────────────────────────────

    def _fuzzy(
        self,
        value: str,
        choices: list[str],
        threshold: float,
    ) -> tuple[Optional[str], float, list[tuple[str, float]]]:
        """
        Perform fuzzy matching with an in-memory cache.

        Cache key: (normalised_value, sorted_choices_tuple, threshold)

        Returns:
            (best_match | None, score, top3_list)
        """
        norm_val = normalise_text(value)
        cache_key = (norm_val, tuple(sorted(choices)), threshold)
        if cache_key in self._fuzzy_cache:
            return self._fuzzy_cache[cache_key]

        if not choices or not norm_val:
            result = (None, 0.0, [])
        else:
            norm_choices = [normalise_text(c) for c in choices]
            raw = process.extract(
                norm_val, norm_choices, scorer=fuzz.token_sort_ratio, limit=3
            )
            top3 = [(choices[norm_choices.index(m)], float(s)) for m, s, _ in raw]
            best_val, best_score = top3[0] if top3 else (None, 0.0)
            matched = best_val if best_score >= threshold else None
            result = (matched, best_score, top3)

        self._fuzzy_cache[cache_key] = result
        return result

    # ── Core matching ─────────────────────────────────────────────────────────

    def run_match(
        self,
        col1: str,
        col2: str,
        threshold: float = DEFAULT_FUZZY_THRESHOLD,
        auto_accept_threshold: float = AUTO_ACCEPT_THRESHOLD,
    ) -> tuple[pd.DataFrame, str]:
        """
        Match df1[col1] against df2[col2] using mapping cache + fuzzy search.

        Args:
            col1: Column name in df1 to match from.
            col2: Column name in df2 to match against.
            threshold: Minimum fuzzy score (%) to accept a match.
            auto_accept_threshold: Score at or above which a match is auto-accepted
                                   without requiring user confirmation.

        Returns:
            (results_dataframe, status_message)

        Raises:
            ValueError: if either DataFrame is None or columns are invalid.
        """
        from utils import validate_dataframe, ValidationError
        validate_dataframe(self.df1, [col1], "Purchase file")
        validate_dataframe(self.df2, [col2], "Sale/Reference file")

        self.col1 = col1
        self.col2 = col2

        source_vals: list[str] = (
            self.df1[col1].fillna("").astype(str).str.strip().tolist()
        )
        choices_series: pd.Series = (
            self.df2[col2].fillna("").astype(str).str.strip()
        )
        # Deduplicate choices for faster matching
        unique_choices: list[str] = [c for c in choices_series.drop_duplicates() if c]

        matched_sale_vals: set[str] = set()
        newly_learned: int = 0
        rows: list[dict] = []

        # Pre-deduplicate source values so identical rows share one fuzzy call
        unique_source: dict[str, tuple] = {}   # normalised → result tuple

        for raw_val in source_vals:
            val = raw_val.strip()
            key = val.lower()

            if not val:
                row = {
                    "Source Value":      val,
                    "Best Match":        "No Match",
                    "Score (%)":         0.0,
                    "Confidence":        "Low",
                    "Auto Accepted":     False,
                    "Method":            "—",
                    "Match Reason":      REASON_EMPTY_SOURCE,
                    "Top 3 Suggestions": "",
                }
                rows.append(row)
                continue

            # Check mapping cache first
            if key in self.mapping:
                matched   = self.mapping[key]
                score     = 100.0
                method    = REASON_CACHED
                reason    = REASON_CACHED
                top3_str  = matched
                auto      = True
                matched_sale_vals.add(matched.lower())

            elif key in unique_source:
                # Reuse previously computed result for duplicate source values
                matched, score, method, reason, top3_str, auto = unique_source[key]
                if matched:
                    matched_sale_vals.add(matched.lower())

            else:
                fz_match, fz_score, fz_top3 = self._fuzzy(val, unique_choices, threshold)

                top3_str = ", ".join(
                    f"{m} ({round(s, 1)}%)" for m, s in fz_top3
                )

                if fz_match:
                    adj_score = confidence_score_adjusted(
                        fz_score, REASON_FUZZY, len(val), len(fz_match)
                    )
                    auto   = adj_score >= auto_accept_threshold
                    method = REASON_AUTO_ACCEPTED if auto else REASON_FUZZY
                    reason = method
                    matched = fz_match
                    score  = adj_score
                    matched_sale_vals.add(matched.lower())
                    if key not in self.mapping:
                        self.mapping[key] = matched
                        newly_learned += 1
                else:
                    matched = None
                    score   = fz_score
                    auto    = False
                    method  = "Low Match"
                    reason  = (
                        REASON_NO_CANDIDATES
                        if not unique_choices
                        else REASON_BELOW_THRESHOLD
                    )

                unique_source[key] = (matched, score, method, reason, top3_str, auto)

            rows.append({
                "Source Value":      val,
                "Best Match":        matched or "No Match",
                "Score (%)":         round(score, 1),
                "Confidence":        confidence_label(score) if matched else "Low",
                "Auto Accepted":     auto,
                "Method":            method,
                "Match Reason":      reason,
                "Top 3 Suggestions": top3_str,
            })

        if newly_learned:
            save_mapping(self.mapping)

        self.results = pd.DataFrame(rows)

        # Identify df2 rows that were never matched
        unmatched_mask = ~choices_series.str.lower().isin(matched_sale_vals)
        self.unmatched_sales = self.df2[unmatched_mask].copy().reset_index(drop=True)

        total    = len(rows)
        high     = sum(1 for r in rows if r["Confidence"] == "High"   and r["Best Match"] != "No Match")
        med      = sum(1 for r in rows if r["Confidence"] == "Medium" and r["Best Match"] != "No Match")
        no_match = sum(1 for r in rows if r["Best Match"] == "No Match")
        auto_cnt = sum(1 for r in rows if r["Auto Accepted"])

        status = (
            f"✅ Matching complete — {total} rows processed | "
            f"High: {high} · Medium: {med} · No match: {no_match} | "
            f"Auto-accepted: {auto_cnt} | Mapping cache: {len(self.mapping)} entries"
        )
        log.info(status)
        return self.results, status

    # ── Mapping management ────────────────────────────────────────────────────

    def confirm_mapping(self, edited_df: pd.DataFrame) -> str:
        """
        Persist user-edited Best Match values back to the mapping cache.

        Args:
            edited_df: The (possibly edited) results table from the UI.

        Returns:
            Status message string.
        """
        if edited_df is None or len(edited_df) == 0:
            return "⚠️ No data to save."
        if "Source Value" not in edited_df.columns or "Best Match" not in edited_df.columns:
            return "⚠️ Table must have 'Source Value' and 'Best Match' columns."

        count = 0
        for _, row in edited_df.iterrows():
            src = str(row["Source Value"]).strip()
            bst = str(row["Best Match"]).strip()
            if src and bst and bst != "No Match":
                self.mapping[src.lower()] = bst
                count += 1

        save_mapping(self.mapping)
        return f"✅ Saved {count} entries. Mapping cache now has {len(self.mapping)} entries."

    def clear_mapping(self) -> str:
        """Remove all saved mappings from memory and disk."""
        self.mapping = {}
        save_mapping(self.mapping)
        self._fuzzy_cache.clear()
        return "🗑️ All mappings cleared."

    # ── Excel export ──────────────────────────────────────────────────────────

    def export_xlsx(self, edited_df: pd.DataFrame) -> Optional[str]:
        """
        Write a multi-sheet reconciliation workbook and return the file path.

        Sheets:
          1. Results
          2. Unmatched Purchase
          3. Unmatched Sale
          4. Mapping
          5. Reconciliation Summary

        Args:
            edited_df: The (possibly user-edited) results DataFrame.

        Returns:
            Path to the generated .xlsx file, or None on failure.
        """
        from utils import make_temp_xlsx, style_sheet, write_summary_section
        from constants import (
            COLOR_BLUE_DARK, COLOR_RED, COLOR_ORANGE, COLOR_GREEN_DARK,
            COLOR_BLUE_NAVY, COLOR_PURPLE,
            FILL_BLUE_LIGHT, FILL_RED_LIGHT, FILL_ORANGE_LIGHT, FILL_GREEN_LIGHT,
        )

        try:
            results_df = pd.DataFrame(edited_df) if not isinstance(edited_df, pd.DataFrame) else edited_df

            no_match_mask = results_df["Best Match"] == "No Match"
            unmatched_purchases = pd.DataFrame()
            if self.df1 is not None and self.col1:
                no_match_vals = set(results_df.loc[no_match_mask, "Source Value"].tolist())
                unmatched_purchases = self.df1[
                    self.df1[self.col1].fillna("").astype(str).str.strip().isin(no_match_vals)
                ].copy().reset_index(drop=True)

            unmatched_sales = self.unmatched_sales if self.unmatched_sales is not None else pd.DataFrame()

            total_pur     = len(results_df)
            total_unm_pur = int(no_match_mask.sum())
            total_unm_sal = len(unmatched_sales)
            matched_pur   = total_pur - total_unm_pur
            recon_rate    = (matched_pur / total_pur) if total_pur else 0.0
            high_cnt = int((results_df["Confidence"] == "High").sum())
            med_cnt  = int((results_df["Confidence"] == "Medium").sum())
            low_cnt  = int((results_df["Confidence"] == "Low").sum())

            out = make_temp_xlsx("stock_recon_")

            with pd.ExcelWriter(out, engine="openpyxl") as writer:

                # Sheet 1: Results
                results_df.to_excel(writer, sheet_name="Results", index=False)
                style_sheet(writer.sheets["Results"], COLOR_BLUE_DARK, FILL_BLUE_LIGHT)

                # Sheet 2: Unmatched Purchase
                (unmatched_purchases if not unmatched_purchases.empty
                 else pd.DataFrame({"Info": ["✅ All purchase entries were matched."]})).to_excel(
                    writer, sheet_name="Unmatched Purchase", index=False
                )
                style_sheet(writer.sheets["Unmatched Purchase"], COLOR_RED, FILL_RED_LIGHT)

                # Sheet 3: Unmatched Sale
                (unmatched_sales if not unmatched_sales.empty
                 else pd.DataFrame({"Info": ["✅ All sale entries were matched."]})).to_excel(
                    writer, sheet_name="Unmatched Sale", index=False
                )
                style_sheet(writer.sheets["Unmatched Sale"], COLOR_ORANGE, FILL_ORANGE_LIGHT)

                # Sheet 4: Mapping
                if self.mapping:
                    pd.DataFrame(
                        list(self.mapping.items()), columns=["Original (normalised)", "Confirmed Match"]
                    ).to_excel(writer, sheet_name="Mapping", index=False)
                    style_sheet(writer.sheets["Mapping"], COLOR_GREEN_DARK, FILL_GREEN_LIGHT)

                # Sheet 5: Reconciliation Summary
                ws = writer.book.create_sheet("Reconciliation Summary")
                ws.sheet_view.showGridLines = False
                ws.column_dimensions["A"].width = 40
                ws.column_dimensions["B"].width = 22

                r = write_summary_section(ws, 1, "📊  Processing Statistics", COLOR_BLUE_DARK, [
                    ("Total Purchase Entries",  total_pur),
                    ("Total Sale Entries",      len(self.df2) if self.df2 is not None else "—"),
                    ("Matched Purchases",        matched_pur),
                ])
                r = write_summary_section(ws, r, "❌  Unmatched Breakdown", COLOR_RED, [
                    ("Unmatched Purchases",  total_unm_pur),
                    ("Unmatched Sales",      total_unm_sal),
                ])
                r = write_summary_section(ws, r, "✅  Reconciliation Rate", COLOR_GREEN_DARK, [
                    ("Reconciliation Rate", recon_rate),
                ])
                ws.cell(row=r - 1, column=2).number_format = "0.00%"
                r = write_summary_section(ws, r, "🎯  Confidence Breakdown", COLOR_PURPLE, [
                    ("High Confidence (≥ 80%)",    high_cnt),
                    ("Medium Confidence (60–79%)", med_cnt),
                    ("Low / No Match (< 60%)",     low_cnt),
                ])
                write_summary_section(ws, r, "🗂️  Mapping Cache", COLOR_BLUE_NAVY, [
                    ("Saved Mapping Entries", len(self.mapping)),
                ])

            log.info("Stock recon report saved: %s", out)
            return out

        except Exception as exc:
            log_exception("Stock export failed", exc)
            return None
