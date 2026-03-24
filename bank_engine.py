"""
bank_engine.py — Bank Reconciliation matching engine.

All state is encapsulated in BankReconState (a dataclass).
No global variables — Gradio state objects hold instances of this class.

Improvements over v1:
  - Class-based state (no globals)
  - Best-match selection (smallest absolute difference, not first hit)
  - Duplicate-amount handling: tracks used bank indices to avoid double-matching
  - Vectorised amount parsing
  - Strict column validation before processing
  - Detailed unmatched reason column
  - Summary stats as a typed dataclass
  - Full type hints and docstrings
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from constants import DEFAULT_BANK_TOLERANCE
from utils import log_exception, parse_amount, validate_dataframe

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Summary statistics container
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class BankSummaryStats:
    """Typed container for all reconciliation summary numbers."""
    total_books_entries: int   = 0
    total_bank_entries: int    = 0
    total_books_dr: float      = 0.0
    total_books_cr: float      = 0.0
    total_bank_dr: float       = 0.0
    total_bank_cr: float       = 0.0
    net_books: float           = 0.0
    net_bank: float            = 0.0
    matched_pairs: int         = 0
    unmatched_books: int       = 0
    unmatched_bank: int        = 0
    recon_rate: float          = 0.0
    unm_books_dr: float        = 0.0
    unm_books_cr: float        = 0.0
    unm_bank_dr: float         = 0.0
    unm_bank_cr: float         = 0.0
    unm_net_books: float       = 0.0
    unm_net_bank: float        = 0.0
    difference: float          = 0.0

    @property
    def is_balanced(self) -> bool:
        return abs(self.difference) < 0.01

    @property
    def status_label(self) -> str:
        return "✅ BALANCED" if self.is_balanced else "⚠️ DIFFERENCE EXISTS — INVESTIGATE"


# ──────────────────────────────────────────────────────────────────────────────
# State container
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class BankReconState:
    """
    Holds all mutable state for one Bank Reconciliation session.
    Instances live inside a gr.State component — never as module globals.
    """
    books_df: Optional[pd.DataFrame]             = None
    bank_df:  Optional[pd.DataFrame]             = None
    matched:          Optional[pd.DataFrame]     = None
    unmatched_books:  Optional[pd.DataFrame]     = None
    unmatched_bank:   Optional[pd.DataFrame]     = None
    stats:            BankSummaryStats           = field(default_factory=BankSummaryStats)

    # ── Matching engine ───────────────────────────────────────────────────────

    def run_reconciliation(
        self,
        books_dr_col: str,
        books_cr_col: str,
        bank_dr_col:  str,
        bank_cr_col:  str,
        tolerance:    float = DEFAULT_BANK_TOLERANCE,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
        """
        Match Books entries against Bank Statement entries by amount.

        Matching logic:
          Pass 1 — Books DR  ↔  Bank CR  (receipts)
          Pass 2 — Books CR  ↔  Bank DR  (payments)

        For each unmatched book entry, selects the *closest* bank candidate
        (smallest |difference|) rather than the first candidate.

        Handles duplicate amounts by tracking used bank-row indices so the
        same bank entry is never matched twice.

        Args:
            books_dr_col: DR column in the Books DataFrame.
            books_cr_col: CR column in the Books DataFrame.
            bank_dr_col:  DR column in the Bank DataFrame.
            bank_cr_col:  CR column in the Bank DataFrame.
            tolerance:    Maximum absolute difference allowed for a near-match.

        Returns:
            (matched_df, unmatched_books_df, unmatched_bank_df, status_message)

        Raises:
            ValidationError: if DataFrames are None or columns are missing.
        """
        from utils import ValidationError

        validate_dataframe(self.books_df, [books_dr_col, books_cr_col], "Books / Cash Book")
        validate_dataframe(self.bank_df,  [bank_dr_col,  bank_cr_col],  "Bank Statement")

        books = self.books_df.copy()
        bank  = self.bank_df.copy()

        # Parse amounts — vectorised, handles symbols / brackets
        books["__DR"] = parse_amount(books[books_dr_col])
        books["__CR"] = parse_amount(books[books_cr_col])
        bank["__DR"]  = parse_amount(bank[bank_dr_col])
        bank["__CR"]  = parse_amount(bank[bank_cr_col])

        # Track match status using index sets (avoids modifying rows in loops)
        matched_books_idx: set[int] = set()
        matched_bank_idx:  set[int] = set()
        matched_rows: list[dict]    = []

        def _best_match_pass(
            books_amt_col: str,
            bank_amt_col:  str,
            label:         str,
        ) -> None:
            """
            Single matching pass: for each unmatched book entry find the
            *best* (closest amount) unmatched bank entry within tolerance.
            """
            # Pre-build arrays of unmatched bank amounts for vectorised lookup
            for bi in books.index:
                if bi in matched_books_idx:
                    continue
                b_amt = books.at[bi, books_amt_col]
                if b_amt <= 0:
                    continue

                # Candidate bank rows: unmatched, positive, within tolerance
                cand_mask = (
                    (~bank.index.isin(matched_bank_idx)) &
                    (bank[bank_amt_col] > 0) &
                    (np.abs(bank[bank_amt_col] - b_amt) <= tolerance)
                )
                candidates = bank.loc[cand_mask, bank_amt_col]
                if candidates.empty:
                    continue

                # Select best match: smallest absolute difference
                diffs  = np.abs(candidates - b_amt)
                best_i = int(diffs.idxmin())
                bk_amt = bank.at[best_i, bank_amt_col]
                diff   = round(float(bk_amt - b_amt), 4)

                matched_books_idx.add(bi)
                matched_bank_idx.add(best_i)

                matched_rows.append({
                    "Match Type":  label,
                    "Books DR":    round(float(books.at[bi, "__DR"]), 2),
                    "Books CR":    round(float(books.at[bi, "__CR"]), 2),
                    "Bank DR":     round(float(bank.at[best_i, "__DR"]), 2),
                    "Bank CR":     round(float(bank.at[best_i, "__CR"]), 2),
                    "Difference":  diff,
                    "Status":      "✅ Exact" if diff == 0 else "⚠️ Near Match",
                })

        _best_match_pass("__DR", "__CR", "Books DR  ↔  Bank CR  (Receipts)")
        _best_match_pass("__CR", "__DR", "Books CR  ↔  Bank DR  (Payments)")

        # ── Compile unmatched ──────────────────────────────────────────────────
        drop_cols = ["__DR", "__CR"]
        unm_books_idx = [i for i in books.index if i not in matched_books_idx]
        unm_bank_idx  = [i for i in bank.index  if i not in matched_bank_idx]

        unm_books = books.loc[unm_books_idx].drop(columns=drop_cols, errors="ignore").copy()
        unm_bank  = bank.loc[unm_bank_idx].drop(columns=drop_cols,  errors="ignore").copy()

        # Add unmatched reason
        def _books_reason(row: pd.Series) -> str:
            dr = row.get(books_dr_col, 0)
            cr = row.get(books_cr_col, 0)
            dr_val = parse_amount(pd.Series([dr])).iloc[0]
            cr_val = parse_amount(pd.Series([cr])).iloc[0]
            if dr_val <= 0 and cr_val <= 0:
                return "Zero / missing amount"
            return "No bank entry within tolerance for this amount"

        def _bank_reason(row: pd.Series) -> str:
            dr = row.get(bank_dr_col, 0)
            cr = row.get(bank_cr_col, 0)
            dr_val = parse_amount(pd.Series([dr])).iloc[0]
            cr_val = parse_amount(pd.Series([cr])).iloc[0]
            if dr_val <= 0 and cr_val <= 0:
                return "Zero / missing amount"
            return "No book entry within tolerance for this amount"

        if not unm_books.empty:
            unm_books["Unmatched Reason"] = unm_books.apply(_books_reason, axis=1)
        if not unm_bank.empty:
            unm_bank["Unmatched Reason"]  = unm_bank.apply(_bank_reason, axis=1)

        self.matched         = pd.DataFrame(matched_rows)
        self.unmatched_books = unm_books.reset_index(drop=True)
        self.unmatched_bank  = unm_bank.reset_index(drop=True)

        # ── Compute summary statistics ─────────────────────────────────────────
        tb_dr = float(books["__DR"].sum())
        tb_cr = float(books["__CR"].sum())
        bk_dr = float(bank["__DR"].sum())
        bk_cr = float(bank["__CR"].sum())

        unm_b_dr = float(books.loc[unm_books_idx, "__DR"].sum()) if unm_books_idx else 0.0
        unm_b_cr = float(books.loc[unm_books_idx, "__CR"].sum()) if unm_books_idx else 0.0
        unm_k_dr = float(bank.loc[unm_bank_idx,   "__DR"].sum()) if unm_bank_idx  else 0.0
        unm_k_cr = float(bank.loc[unm_bank_idx,   "__CR"].sum()) if unm_bank_idx  else 0.0

        net_books = round(tb_dr - tb_cr, 2)
        net_bank  = round(bk_dr - bk_cr, 2)
        recon_rate = len(matched_rows) / max(len(books), 1)

        self.stats = BankSummaryStats(
            total_books_entries = len(books),
            total_bank_entries  = len(bank),
            total_books_dr      = round(tb_dr, 2),
            total_books_cr      = round(tb_cr, 2),
            total_bank_dr       = round(bk_dr, 2),
            total_bank_cr       = round(bk_cr, 2),
            net_books           = net_books,
            net_bank            = net_bank,
            matched_pairs       = len(matched_rows),
            unmatched_books     = len(self.unmatched_books),
            unmatched_bank      = len(self.unmatched_bank),
            recon_rate          = round(recon_rate, 4),
            unm_books_dr        = round(unm_b_dr, 2),
            unm_books_cr        = round(unm_b_cr, 2),
            unm_bank_dr         = round(unm_k_dr, 2),
            unm_bank_cr         = round(unm_k_cr, 2),
            unm_net_books       = round(unm_b_dr - unm_b_cr, 2),
            unm_net_bank        = round(unm_k_dr - unm_k_cr, 2),
            difference          = round(net_books - net_bank, 4),
        )

        s = self.stats
        status = (
            f"{s.status_label} | "
            f"Matched: {s.matched_pairs} pairs | "
            f"Unmatched Books: {s.unmatched_books} | "
            f"Unmatched Bank: {s.unmatched_bank} | "
            f"Net Difference: {s.difference:,.4f} | "
            f"Recon Rate: {s.recon_rate * 100:.1f}%"
        )
        log.info("Bank recon complete. %s", status)

        m_out   = self.matched         if not self.matched.empty         else pd.DataFrame([{"Info": "No matches found."}])
        ub_out  = self.unmatched_books if not self.unmatched_books.empty else pd.DataFrame([{"Info": "✅ All book entries matched."}])
        ubk_out = self.unmatched_bank  if not self.unmatched_bank.empty  else pd.DataFrame([{"Info": "✅ All bank entries matched."}])

        return m_out, ub_out, ubk_out, status

    # ── Excel export ──────────────────────────────────────────────────────────

    def export_xlsx(self) -> Optional[str]:
        """
        Write the full bank reconciliation workbook and return the file path.

        Sheets:
          1. Matched Entries
          2. Unmatched Books
          3. Unmatched Bank Statement
          4. Unmatched Figures
          5. Reconciliation Summary

        Returns:
            Absolute path string, or None on failure.
        """
        from utils import make_temp_xlsx, style_sheet, write_summary_section
        from constants import (
            COLOR_BLUE_DARK, COLOR_BLUE_MED, COLOR_BLUE_NAVY,
            COLOR_GREEN_DARK, COLOR_GREEN_TEAL,
            COLOR_RED, COLOR_ORANGE, COLOR_PURPLE_DARK,
            FILL_BLUE_LIGHT, FILL_RED_LIGHT, FILL_ORANGE_LIGHT, FILL_PURPLE_LIGHT,
        )

        if self.matched is None:
            return None

        try:
            s   = self.stats
            out = make_temp_xlsx("bank_recon_")

            with pd.ExcelWriter(out, engine="openpyxl") as writer:

                # Sheet 1: Matched Entries
                df_m = self.matched if not self.matched.empty else pd.DataFrame([{"Info": "No entries were matched."}])
                df_m.to_excel(writer, sheet_name="Matched Entries", index=False)
                style_sheet(writer.sheets["Matched Entries"], COLOR_BLUE_NAVY, FILL_BLUE_LIGHT)

                # Sheet 2: Unmatched Books
                df_ub = self.unmatched_books if not self.unmatched_books.empty else pd.DataFrame([{"Info": "✅ All book entries matched."}])
                df_ub.to_excel(writer, sheet_name="Unmatched Books", index=False)
                style_sheet(writer.sheets["Unmatched Books"], COLOR_RED, FILL_RED_LIGHT)

                # Sheet 3: Unmatched Bank Statement
                df_ubk = self.unmatched_bank if not self.unmatched_bank.empty else pd.DataFrame([{"Info": "✅ All bank entries matched."}])
                df_ubk.to_excel(writer, sheet_name="Unmatched Bank Statement", index=False)
                style_sheet(writer.sheets["Unmatched Bank Statement"], COLOR_ORANGE, FILL_ORANGE_LIGHT)

                # Sheet 4: Unmatched Figures
                pd.DataFrame([
                    {"Category": "Unmatched Books — DR Total",    "Amount": s.unm_books_dr},
                    {"Category": "Unmatched Books — CR Total",    "Amount": s.unm_books_cr},
                    {"Category": "Net Unmatched Books (DR – CR)", "Amount": s.unm_net_books},
                    {"Category": "—",                              "Amount": ""},
                    {"Category": "Unmatched Bank — DR Total",     "Amount": s.unm_bank_dr},
                    {"Category": "Unmatched Bank — CR Total",     "Amount": s.unm_bank_cr},
                    {"Category": "Net Unmatched Bank (DR – CR)",  "Amount": s.unm_net_bank},
                    {"Category": "—",                              "Amount": ""},
                    {"Category": "Net Difference (Books – Bank)", "Amount": s.difference},
                ]).to_excel(writer, sheet_name="Unmatched Figures", index=False)
                style_sheet(writer.sheets["Unmatched Figures"], COLOR_PURPLE_DARK, FILL_PURPLE_LIGHT)

                # Sheet 5: Reconciliation Summary
                ws = writer.book.create_sheet("Reconciliation Summary")
                ws.sheet_view.showGridLines = False
                ws.column_dimensions["A"].width = 44
                ws.column_dimensions["B"].width = 22
                ws.column_dimensions["C"].width = 4

                r = write_summary_section(ws, 1, "📒  Books / Cash Book Summary", COLOR_BLUE_MED, [
                    ("Total Book Entries",          s.total_books_entries),
                    ("Total Books DR",              s.total_books_dr),
                    ("Total Books CR",              s.total_books_cr),
                    ("Net Books Balance (DR – CR)", s.net_books),
                ])
                r = write_summary_section(ws, r, "🏦  Bank Statement Summary", COLOR_BLUE_NAVY, [
                    ("Total Bank Entries",         s.total_bank_entries),
                    ("Total Bank DR",              s.total_bank_dr),
                    ("Total Bank CR",              s.total_bank_cr),
                    ("Net Bank Balance (DR – CR)", s.net_bank),
                ])
                r = write_summary_section(ws, r, "🔗  Matching Results", COLOR_GREEN_TEAL, [
                    ("Matched Pairs",          s.matched_pairs),
                    ("Unmatched Book Entries", s.unmatched_books),
                    ("Unmatched Bank Entries", s.unmatched_bank),
                    ("Reconciliation Rate",    s.recon_rate),
                ])
                ws.cell(row=r - 1, column=2).number_format = "0.00%"
                r = write_summary_section(ws, r, "❌  Unmatched Figures", COLOR_RED, [
                    ("Unmatched Books DR",          s.unm_books_dr),
                    ("Unmatched Books CR",          s.unm_books_cr),
                    ("Net Unmatched Books (DR–CR)", s.unm_net_books),
                    ("Unmatched Bank DR",           s.unm_bank_dr),
                    ("Unmatched Bank CR",           s.unm_bank_cr),
                    ("Net Unmatched Bank (DR–CR)",  s.unm_net_bank),
                ])
                write_summary_section(ws, r, "✅  Reconciliation Check", COLOR_GREEN_DARK, [
                    ("Net Books Balance",           s.net_books),
                    ("Net Bank Balance",            s.net_bank),
                    ("Net Difference (Books–Bank)", s.difference),
                    ("Reconciliation Status",       s.status_label),
                ])

            log.info("Bank recon report saved: %s", out)
            return out

        except Exception as exc:
            log_exception("Bank export failed", exc)
            return None
