"""
app.py — Internal Audit Makes Easy
====================================
Unified Gradio application with two modules:
  • 📦 Stock Reconciliation  (fuzzy-match audit tool)
  • 🏦 Bank Reconciliation   (DR/CR amount-matching tool)

Key improvements over v1:
  - No global variables: state lives in gr.State (one per session)
  - Proper error handling with user-friendly messages
  - Input validation and file-size warnings before processing
  - Auto-accept high-confidence matches (configurable)
  - Visual summary charts
  - Loading indicators via Gradio's built-in queue
  - Tooltips / help text on all key controls
  - Clean modular imports from stock_engine / bank_engine / utils / constants

Run:
    pip install gradio pandas openpyxl rapidfuzz
    python app.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Optional

import gradio as gr
import pandas as pd

# Ensure the audit_app package directory is on the path when running directly
sys.path.insert(0, str(Path(__file__).parent))

from bank_engine import BankReconState
from constants import (
    APP_TITLE,
    AUTO_ACCEPT_THRESHOLD,
    DEFAULT_BANK_TOLERANCE,
    DEFAULT_FUZZY_THRESHOLD,
    HIGH_CONFIDENCE_MIN,
    LARGE_FILE_ROWS,
)
from stock_engine import StockReconState
from utils import (
    ValidationError,
    load_excel,
    log_exception,
    sanitise_dataframe,
    setup_logging,
)

setup_logging()
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# CSS
# ──────────────────────────────────────────────────────────────────────────────

CSS = """
footer { display: none !important; }

/* App header */
.app-title { text-align: center; padding: 18px 0 6px; }
.app-title h1 { font-size: 2.1rem !important; font-weight: 800 !important; letter-spacing: -0.5px; }
.app-title p  { color: #6b7280; font-size: 0.95rem; margin-top: 4px; }

/* Tab labels */
#main-tabs .tab-nav button {
    font-size: 15px !important;
    font-weight: 600 !important;
    padding: 10px 28px !important;
}

/* Mono status boxes */
.status-box textarea {
    font-family: "JetBrains Mono", "Fira Code", monospace !important;
    font-size: 12.5px !important;
    line-height: 1.6 !important;
}

/* Run button */
.run-btn { font-size: 15px !important; font-weight: 700 !important; }

/* Warning banner */
.warn-box textarea { background: #fef3c7 !important; border-color: #f59e0b !important; }

/* Section subheadings */
.section-label { font-weight: 600; font-size: 1rem; color: #374151; margin-bottom: 4px; }
"""

# ──────────────────────────────────────────────────────────────────────────────
# Helper: build a small HTML summary card
# ──────────────────────────────────────────────────────────────────────────────

def _stat_cards(stats: dict[str, Any]) -> str:
    """
    Render a row of coloured stat cards as an HTML string.

    Args:
        stats: {label: value} ordered dict.

    Returns:
        HTML string.
    """
    cards = []
    palette = ["#3b82f6", "#10b981", "#ef4444", "#f59e0b", "#8b5cf6", "#06b6d4"]
    for i, (label, value) in enumerate(stats.items()):
        color = palette[i % len(palette)]
        cards.append(
            f'<div style="flex:1;min-width:140px;background:{color}15;border:1px solid {color}40;'
            f'border-left:4px solid {color};border-radius:8px;padding:10px 14px;text-align:center">'
            f'<div style="font-size:1.45rem;font-weight:800;color:{color}">{value}</div>'
            f'<div style="font-size:0.78rem;color:#6b7280;margin-top:2px">{label}</div>'
            f'</div>'
        )
    return (
        '<div style="display:flex;flex-wrap:wrap;gap:10px;padding:6px 0">'
        + "".join(cards)
        + "</div>"
    )


# ──────────────────────────────────────────────────────────────────────────────
# STOCK RECONCILIATION — Gradio callbacks
# ──────────────────────────────────────────────────────────────────────────────

def stk_load_file1(
    file_obj,
    state: StockReconState,
) -> tuple:
    if not file_obj:
        return state, gr.update(choices=[], value=None), None, ""
    try:
        df, warn = load_excel(file_obj.name, "Purchase / Source file")
        df = sanitise_dataframe(df)
        state.df1 = df
        cols = df.columns.tolist()
        size_msg = f"⚠️ Large file: {len(df):,} rows — processing may be slow." if len(df) > LARGE_FILE_ROWS else ""
        warning  = warn or size_msg
        return state, gr.update(choices=cols, value=cols[0] if cols else None), df.head(8), warning
    except ValidationError as exc:
        return state, gr.update(choices=[], value=None), pd.DataFrame([{"Error": str(exc)}]), str(exc)
    except Exception as exc:
        log_exception("stk_load_file1", exc)
        return state, gr.update(choices=[], value=None), pd.DataFrame([{"Error": str(exc)}]), f"❌ {exc}"


def stk_load_file2(
    file_obj,
    state: StockReconState,
) -> tuple:
    if not file_obj:
        return state, gr.update(choices=[], value=None), None, ""
    try:
        df, warn = load_excel(file_obj.name, "Sale / Reference file")
        df = sanitise_dataframe(df)
        state.df2 = df
        cols = df.columns.tolist()
        size_msg = f"⚠️ Large file: {len(df):,} rows — processing may be slow." if len(df) > LARGE_FILE_ROWS else ""
        warning  = warn or size_msg
        return state, gr.update(choices=cols, value=cols[0] if cols else None), df.head(8), warning
    except ValidationError as exc:
        return state, gr.update(choices=[], value=None), pd.DataFrame([{"Error": str(exc)}]), str(exc)
    except Exception as exc:
        log_exception("stk_load_file2", exc)
        return state, gr.update(choices=[], value=None), pd.DataFrame([{"Error": str(exc)}]), f"❌ {exc}"


def stk_run_match(
    col1: str,
    col2: str,
    threshold: float,
    auto_accept: float,
    state: StockReconState,
) -> tuple:
    try:
        if state.df1 is None or state.df2 is None:
            raise ValidationError("Please upload both files before running.")
        if not col1 or not col2:
            raise ValidationError("Please select match columns for both files.")

        results, status = state.run_match(col1, col2, threshold, auto_accept)

        # Build summary cards
        total    = len(results)
        high     = int((results["Confidence"] == "High").sum())
        med      = int((results["Confidence"] == "Medium").sum())
        no_match = int((results["Best Match"] == "No Match").sum())
        auto_cnt = int(results["Auto Accepted"].sum()) if "Auto Accepted" in results.columns else 0
        unm_sal  = len(state.unmatched_sales) if state.unmatched_sales is not None else 0

        cards_html = _stat_cards({
            "Total Rows":       total,
            "High Confidence":  high,
            "Medium Conf.":     med,
            "No Match":         no_match,
            "Auto-Accepted":    auto_cnt,
            "Unmatched Sales":  unm_sal,
        })

        return (
            state,
            results,
            status,
            cards_html,
            gr.update(visible=True),
        )

    except ValidationError as exc:
        return state, pd.DataFrame([{"Error": str(exc)}]), f"❌ {exc}", "", gr.update(visible=False)
    except Exception as exc:
        log_exception("stk_run_match", exc)
        return state, pd.DataFrame([{"Error": str(exc)}]), f"❌ Unexpected error: {exc}", "", gr.update(visible=False)


def stk_confirm_mapping(edited_df, state: StockReconState) -> tuple[StockReconState, str]:
    try:
        df = pd.DataFrame(edited_df) if not isinstance(edited_df, pd.DataFrame) else edited_df
        # Validate edits: Best Match column must not be empty for non-"No Match" entries
        if "Best Match" in df.columns:
            blank_matches = df[(df["Best Match"].fillna("").str.strip() == "") & (df["Source Value"].fillna("").str.strip() != "")]
            if not blank_matches.empty:
                return state, f"⚠️ {len(blank_matches)} rows have blank Best Match values. Fill them in or set to 'No Match'."
        msg = state.confirm_mapping(df)
        return state, msg
    except Exception as exc:
        log_exception("stk_confirm_mapping", exc)
        return state, f"❌ {exc}"


def stk_clear_mapping(state: StockReconState) -> tuple[StockReconState, str]:
    msg = state.clear_mapping()
    return state, msg


def stk_download(edited_df, state: StockReconState) -> tuple[StockReconState, Optional[str]]:
    try:
        df = pd.DataFrame(edited_df) if not isinstance(edited_df, pd.DataFrame) else edited_df
        path = state.export_xlsx(df)
        if path is None:
            return state, None
        return state, path
    except Exception as exc:
        log_exception("stk_download", exc)
        return state, None


# ──────────────────────────────────────────────────────────────────────────────
# BANK RECONCILIATION — Gradio callbacks
# ──────────────────────────────────────────────────────────────────────────────

def bnk_load_books(file_obj, state: BankReconState) -> tuple:
    if not file_obj:
        return state, gr.update(choices=[], value=None), gr.update(choices=[], value=None), None, ""
    try:
        df, warn = load_excel(file_obj.name, "Books / Cash Book")
        df = sanitise_dataframe(df)
        state.books_df = df
        cols = df.columns.tolist()
        size_msg = f"⚠️ Large file: {len(df):,} rows." if len(df) > LARGE_FILE_ROWS else ""
        return (
            state,
            gr.update(choices=cols, value=cols[0] if cols else None),
            gr.update(choices=cols, value=cols[1] if len(cols) > 1 else (cols[0] if cols else None)),
            df.head(8),
            warn or size_msg,
        )
    except ValidationError as exc:
        return state, gr.update(choices=[]), gr.update(choices=[]), pd.DataFrame([{"Error": str(exc)}]), str(exc)
    except Exception as exc:
        log_exception("bnk_load_books", exc)
        return state, gr.update(choices=[]), gr.update(choices=[]), pd.DataFrame([{"Error": str(exc)}]), f"❌ {exc}"


def bnk_load_bank(file_obj, state: BankReconState) -> tuple:
    if not file_obj:
        return state, gr.update(choices=[], value=None), gr.update(choices=[], value=None), None, ""
    try:
        df, warn = load_excel(file_obj.name, "Bank Statement")
        df = sanitise_dataframe(df)
        state.bank_df = df
        cols = df.columns.tolist()
        size_msg = f"⚠️ Large file: {len(df):,} rows." if len(df) > LARGE_FILE_ROWS else ""
        return (
            state,
            gr.update(choices=cols, value=cols[0] if cols else None),
            gr.update(choices=cols, value=cols[1] if len(cols) > 1 else (cols[0] if cols else None)),
            df.head(8),
            warn or size_msg,
        )
    except ValidationError as exc:
        return state, gr.update(choices=[]), gr.update(choices=[]), pd.DataFrame([{"Error": str(exc)}]), str(exc)
    except Exception as exc:
        log_exception("bnk_load_bank", exc)
        return state, gr.update(choices=[]), gr.update(choices=[]), pd.DataFrame([{"Error": str(exc)}]), f"❌ {exc}"


def bnk_run_reconciliation(
    b_dr: str, b_cr: str,
    bk_dr: str, bk_cr: str,
    tolerance: float,
    state: BankReconState,
) -> tuple:
    try:
        if state.books_df is None or state.bank_df is None:
            raise ValidationError("Please upload both files before running.")
        if not all([b_dr, b_cr, bk_dr, bk_cr]):
            raise ValidationError("Please select DR and CR columns for both files.")

        m_out, ub_out, ubk_out, status = state.run_reconciliation(b_dr, b_cr, bk_dr, bk_cr, tolerance)
        s = state.stats

        cards_html = _stat_cards({
            "Matched Pairs":     s.matched_pairs,
            "Unmatched Books":   s.unmatched_books,
            "Unmatched Bank":    s.unmatched_bank,
            "Net Difference":    f"{s.difference:,.2f}",
            "Recon Rate":        f"{s.recon_rate * 100:.1f}%",
            "Status":            "✅ OK" if s.is_balanced else "⚠️ GAP",
        })

        return state, m_out, ub_out, ubk_out, status, cards_html, gr.update(visible=True)

    except ValidationError as exc:
        empty = pd.DataFrame([{"Error": str(exc)}])
        return state, empty, empty, empty, f"❌ {exc}", "", gr.update(visible=False)
    except Exception as exc:
        log_exception("bnk_run_reconciliation", exc)
        empty = pd.DataFrame([{"Error": str(exc)}])
        return state, empty, empty, empty, f"❌ Unexpected error: {exc}", "", gr.update(visible=False)


def bnk_download(state: BankReconState) -> tuple[BankReconState, Optional[str]]:
    try:
        path = state.export_xlsx()
        return state, path
    except Exception as exc:
        log_exception("bnk_download", exc)
        return state, None


# ──────────────────────────────────────────────────────────────────────────────
# Gradio UI Layout
# ──────────────────────────────────────────────────────────────────────────────

with gr.Blocks(
    title=APP_TITLE,
) as demo:

    # ── Per-session state ─────────────────────────────────────────────────────
    stk_state = gr.State(lambda: StockReconState())
    bnk_state = gr.State(lambda: BankReconState())

    # ── App header ─────────────────────────────────────────────────────────────
    gr.HTML("""
    <div class="app-title">
      <h1>🔍 Internal Audit Makes Easy</h1>
      <p>Powerful reconciliation tools for internal auditors — all in one place</p>
    </div>
    """)

    # ── Main menu tabs ─────────────────────────────────────────────────────────
    with gr.Tabs(elem_id="main-tabs"):

        # ══════════════════════════════════════════════════════════════════════
        # TAB 1 — STOCK RECONCILIATION
        # ══════════════════════════════════════════════════════════════════════
        with gr.Tab("📦  Stock Reconciliation"):

            gr.Markdown(
                "Match **Purchase** entries against **Sale / Reference** entries using "
                "intelligent fuzzy text matching. Learned mappings are saved across sessions.\n\n"
                "**Output:** Results · Unmatched Purchase · Unmatched Sale · Mapping · Reconciliation Summary"
            )

            with gr.Row():
                with gr.Column():
                    gr.HTML('<div class="section-label">📁 File 1 — Purchase / Source</div>')
                    stk_file1   = gr.File(label="Upload Excel (.xlsx / .xls)", file_types=[".xlsx", ".xls"])
                    stk_col1_dd = gr.Dropdown(
                        label="Match column",
                        choices=[],
                        interactive=True,
                        info="Column whose values will be matched (e.g. Supplier Name, SKU)",
                    )
                    stk_warn1   = gr.Textbox(label="", visible=True, interactive=False, elem_classes=["warn-box"], show_label=False)
                    stk_prev1   = gr.Dataframe(label="Preview (first 8 rows)", interactive=False)

                with gr.Column():
                    gr.HTML('<div class="section-label">📁 File 2 — Sale / Reference</div>')
                    stk_file2   = gr.File(label="Upload Excel (.xlsx / .xls)", file_types=[".xlsx", ".xls"])
                    stk_col2_dd = gr.Dropdown(
                        label="Match column",
                        choices=[],
                        interactive=True,
                        info="Column to match against (e.g. Vendor Name, Product Code)",
                    )
                    stk_warn2   = gr.Textbox(label="", visible=True, interactive=False, elem_classes=["warn-box"], show_label=False)
                    stk_prev2   = gr.Dataframe(label="Preview (first 8 rows)", interactive=False)

            with gr.Row():
                stk_threshold_sl = gr.Slider(
                    0, 100, value=DEFAULT_FUZZY_THRESHOLD, step=1,
                    label="Match Threshold (%)",
                    info="Minimum fuzzy score to accept a match. Lower = more lenient, higher = stricter.",
                    scale=2,
                )
                stk_auto_sl = gr.Slider(
                    50, 100, value=AUTO_ACCEPT_THRESHOLD, step=1,
                    label="Auto-Accept Threshold (%)",
                    info="Matches at or above this score are auto-accepted without manual review.",
                    scale=2,
                )
                stk_run_btn = gr.Button(
                    "🚀 Run Smart Matching",
                    variant="primary",
                    scale=1,
                    elem_classes=["run-btn"],
                )

            stk_status_box  = gr.Textbox(label="Status", interactive=False, elem_classes=["status-box"])
            stk_cards_html  = gr.HTML()
            stk_results_tbl = gr.Dataframe(
                label="Results — edit 'Best Match' to correct errors, then click Confirm",
                interactive=True,
                wrap=False,
            )

            with gr.Row(visible=False) as stk_action_row:
                stk_confirm_btn  = gr.Button("🔄 Confirm & Save Mapping", variant="secondary")
                stk_clear_btn    = gr.Button("🗑️ Clear All Mappings",     variant="stop")
                stk_download_btn = gr.Button("📥 Download Results (.xlsx)", variant="primary")

            stk_mapping_status = gr.Textbox(label="Mapping status", interactive=False)
            stk_dl_file        = gr.File(label="Your report", visible=True)

            # ── Wire events ───────────────────────────────────────────────────
            stk_file1.change(
                stk_load_file1,
                inputs=[stk_file1, stk_state],
                outputs=[stk_state, stk_col1_dd, stk_prev1, stk_warn1],
            )
            stk_file2.change(
                stk_load_file2,
                inputs=[stk_file2, stk_state],
                outputs=[stk_state, stk_col2_dd, stk_prev2, stk_warn2],
            )
            stk_run_btn.click(
                stk_run_match,
                inputs=[stk_col1_dd, stk_col2_dd, stk_threshold_sl, stk_auto_sl, stk_state],
                outputs=[stk_state, stk_results_tbl, stk_status_box, stk_cards_html, stk_action_row],
            )
            stk_confirm_btn.click(
                stk_confirm_mapping,
                inputs=[stk_results_tbl, stk_state],
                outputs=[stk_state, stk_mapping_status],
            )
            stk_clear_btn.click(
                stk_clear_mapping,
                inputs=[stk_state],
                outputs=[stk_state, stk_mapping_status],
            )
            stk_download_btn.click(
                stk_download,
                inputs=[stk_results_tbl, stk_state],
                outputs=[stk_state, stk_dl_file],
            )

        # ══════════════════════════════════════════════════════════════════════
        # TAB 2 — BANK RECONCILIATION
        # ══════════════════════════════════════════════════════════════════════
        with gr.Tab("🏦  Bank Reconciliation"):

            gr.Markdown(
                "Match **Books / Cash Book** entries against **Bank Statement** entries by amount.\n\n"
                "**Matching logic:** `Books DR ↔ Bank CR` (receipts) · `Books CR ↔ Bank DR` (payments) — "
                "best-fit match within your chosen tolerance.\n\n"
                "**Output:** Matched Entries · Unmatched Books · Unmatched Bank Statement · "
                "Unmatched Figures · Reconciliation Summary"
            )

            with gr.Row(equal_height=True):
                with gr.Column():
                    gr.HTML('<div class="section-label">📒 Books / Cash Book</div>')
                    bnk_books_file = gr.File(label="Upload (.xlsx / .xls)", file_types=[".xlsx", ".xls"])
                    with gr.Row():
                        bnk_b_dr_dd = gr.Dropdown(
                            label="DR Amount Column", choices=[], interactive=True,
                            info="Column containing Debit (receipt) amounts in the Books",
                        )
                        bnk_b_cr_dd = gr.Dropdown(
                            label="CR Amount Column", choices=[], interactive=True,
                            info="Column containing Credit (payment) amounts in the Books",
                        )
                    bnk_books_warn    = gr.Textbox(label="", interactive=False, show_label=False, elem_classes=["warn-box"])
                    bnk_books_preview = gr.Dataframe(label="Preview (first 8 rows)", interactive=False)

                with gr.Column():
                    gr.HTML('<div class="section-label">🏦 Bank Statement</div>')
                    bnk_bank_file = gr.File(label="Upload (.xlsx / .xls)", file_types=[".xlsx", ".xls"])
                    with gr.Row():
                        bnk_bk_dr_dd = gr.Dropdown(
                            label="DR Amount Column", choices=[], interactive=True,
                            info="Column containing Debit amounts on the Bank Statement",
                        )
                        bnk_bk_cr_dd = gr.Dropdown(
                            label="CR Amount Column", choices=[], interactive=True,
                            info="Column containing Credit amounts on the Bank Statement",
                        )
                    bnk_bank_warn    = gr.Textbox(label="", interactive=False, show_label=False, elem_classes=["warn-box"])
                    bnk_bank_preview = gr.Dataframe(label="Preview (first 8 rows)", interactive=False)

            gr.HTML('<div class="section-label" style="margin-top:12px">⚙️ Matching Settings</div>')
            with gr.Row():
                bnk_tolerance_sl = gr.Slider(
                    0, 10_000, value=DEFAULT_BANK_TOLERANCE, step=0.01,
                    label="Amount Tolerance (±)",
                    info="Maximum absolute difference allowed between two amounts to still count as a match. "
                         "Set to 0 for exact-only matching.",
                    scale=3,
                )
                bnk_run_btn = gr.Button(
                    "🚀 Run Reconciliation",
                    variant="primary",
                    size="lg",
                    scale=1,
                    elem_classes=["run-btn"],
                )

            bnk_status_box = gr.Textbox(label="Status", interactive=False, elem_classes=["status-box"])
            bnk_cards_html = gr.HTML()

            with gr.Tabs():
                with gr.Tab("🔗 Matched Entries"):
                    bnk_matched_tbl = gr.Dataframe(
                        label="Matched pairs",
                        interactive=False,
                        wrap=False,
                    )
                with gr.Tab("📒 Unmatched Books"):
                    bnk_unm_books_tbl = gr.Dataframe(
                        label="Book entries with no match on the bank statement (includes reason)",
                        interactive=False,
                        wrap=False,
                    )
                with gr.Tab("🏦 Unmatched Bank Statement"):
                    bnk_unm_bank_tbl = gr.Dataframe(
                        label="Bank statement entries with no match in the books (includes reason)",
                        interactive=False,
                        wrap=False,
                    )

            with gr.Row(visible=False) as bnk_download_row:
                bnk_download_btn = gr.Button(
                    "📥 Download Full Reconciliation Report (.xlsx)",
                    variant="primary",
                    size="lg",
                )

            bnk_dl_file = gr.File(label="Your report", visible=True)

            # ── Wire events ───────────────────────────────────────────────────
            bnk_books_file.change(
                bnk_load_books,
                inputs=[bnk_books_file, bnk_state],
                outputs=[bnk_state, bnk_b_dr_dd, bnk_b_cr_dd, bnk_books_preview, bnk_books_warn],
            )
            bnk_bank_file.change(
                bnk_load_bank,
                inputs=[bnk_bank_file, bnk_state],
                outputs=[bnk_state, bnk_bk_dr_dd, bnk_bk_cr_dd, bnk_bank_preview, bnk_bank_warn],
            )
            bnk_run_btn.click(
                bnk_run_reconciliation,
                inputs=[bnk_b_dr_dd, bnk_b_cr_dd, bnk_bk_dr_dd, bnk_bk_cr_dd, bnk_tolerance_sl, bnk_state],
                outputs=[bnk_state, bnk_matched_tbl, bnk_unm_books_tbl, bnk_unm_bank_tbl,
                         bnk_status_box, bnk_cards_html, bnk_download_row],
            )
            bnk_download_btn.click(
                bnk_download,
                inputs=[bnk_state],
                outputs=[bnk_state, bnk_dl_file],
            )


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    demo.queue()   # Enable queuing for loading indicators
    demo.launch(
        server_name="0.0.0.0",
        show_error=True,
        css=CSS,
        theme=gr.themes.Soft(primary_hue="blue", neutral_hue="slate"),
        inbrowser=True,
    )
