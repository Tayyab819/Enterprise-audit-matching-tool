"""
Enterprise Audit Matching Tool — Gradio UI
Install dependencies:
    pip install gradio pandas openpyxl rapidfuzz

Run:
    python audit_tool_gradio_persistent.py
"""

import os
import json
import logging
import pandas as pd
from rapidfuzz import process, fuzz
import gradio as gr
from pathlib import Path

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------- Persistence ----------------
# Persistent folder in user's home directory
PERSISTENT_DIR = Path.home() / ".audit_tool"
PERSISTENT_DIR.mkdir(exist_ok=True)
MAPPING_PATH = PERSISTENT_DIR / "audit_mapping.json"

# ---------------- In-memory state ----------------
df1_global = None
df2_global = None
results_global = None
mapping_global: dict = {}

# ---------------- Mapping helpers ----------------
def _load_mapping() -> dict:
    if MAPPING_PATH.exists():
        try:
            with open(MAPPING_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            log.info(f"Loaded {len(data)} mapping entries from {MAPPING_PATH}")
            return data
        except Exception as e:
            log.warning(f"Could not read mapping file: {e}")
    return {}

def _save_mapping(m: dict) -> None:
    try:
        with open(MAPPING_PATH, "w", encoding="utf-8") as f:
            json.dump(m, f, indent=2, ensure_ascii=False)
        log.info(f"Mapping saved to {MAPPING_PATH}")
    except Exception as e:
        log.error(f"Could not save mapping: {e}")

mapping_global = _load_mapping()

# ---------------- Utility functions ----------------
def _confidence(score: float) -> str:
    if score >= 80: return "High"
    if score >= 60: return "Medium"
    return "Low"

def _fuzzy_match(value: str, choices: list[str], threshold: float):
    if not choices:
        return None, 0.0, []
    top3_raw = process.extract(value, choices, scorer=fuzz.token_sort_ratio, limit=3)
    top3 = [(m, float(s)) for m, s, _ in top3_raw]
    best_val, best_score = top3[0] if top3 else (None, 0.0)
    matched = best_val if best_score >= threshold else None
    return matched, best_score, top3

# ---------------- File loaders ----------------
def load_file1(file_obj):
    global df1_global
    if not file_obj: return gr.update(choices=[], value=None), None
    try:
        df1_global = pd.read_excel(file_obj.name)
        df1_global.columns = df1_global.columns.str.strip()
        cols = df1_global.columns.tolist()
        return gr.update(choices=cols, value=cols[0] if cols else None), df1_global.head(5)
    except Exception as e:
        log.error(e)
        return gr.update(choices=[], value=None), pd.DataFrame([{"Error": str(e)}])

def load_file2(file_obj):
    global df2_global
    if not file_obj: return gr.update(choices=[], value=None), None
    try:
        df2_global = pd.read_excel(file_obj.name)
        df2_global.columns = df2_global.columns.str.strip()
        cols = df2_global.columns.tolist()
        return gr.update(choices=cols, value=cols[0] if cols else None), df2_global.head(5)
    except Exception as e:
        log.error(e)
        return gr.update(choices=[], value=None), pd.DataFrame([{"Error": str(e)}])

# ---------------- Matching engine ----------------
def run_match(col1: str, col2: str, threshold: float):
    global df1_global, df2_global, mapping_global, results_global

    if df1_global is None or df2_global is None:
        return pd.DataFrame([{"Error": "Upload both files first."}]), "❌ Upload both files first.", gr.update(visible=False)
    if col1 not in df1_global.columns or col2 not in df2_global.columns:
        return pd.DataFrame([{"Error": "Invalid column selection."}]), "❌ Invalid column selection.", gr.update(visible=False)

    source_vals = df1_global[col1].fillna("").astype(str).str.strip().tolist()
    choices = [c for c in df2_global[col2].fillna("").astype(str).str.strip().drop_duplicates() if c]

    results, unmapped_idxs, unmapped_vals = [], [], []
    for i, val in enumerate(source_vals):
        key = val.lower()
        if not val:
            results.append({"__idx": i, "matched": None, "score": 0.0, "method": "—", "top3": []})
        elif key in mapping_global:
            cached = mapping_global[key]
            results.append({"__idx": i, "matched": cached, "score": 100.0, "method": "Mapping", "top3": [(cached, 100.0)]})
        else:
            results.append(None)
            unmapped_idxs.append(i)
            unmapped_vals.append(val)

    newly_learned = 0
    for pos, idx in enumerate(unmapped_idxs):
        val = unmapped_vals[pos]
        key = val.lower()
        fz_match, fz_score, fz_top3 = _fuzzy_match(val, choices, threshold)
        matched, score, method = fz_match, fz_score, "Fuzzy" if fz_match else None
        top3 = fz_top3[:3]

        if matched and key not in mapping_global:
            mapping_global[key] = matched
            newly_learned += 1

        results[idx] = {"__idx": idx, "matched": matched, "score": score, "method": method or "Low Match", "top3": top3}

    if newly_learned: _save_mapping(mapping_global)

    rows = []
    for i, val in enumerate(source_vals):
        r = results[i]
        top3_str = ", ".join(f"{m} ({round(s,1)}%)" for m, s in r["top3"])
        rows.append({
            "Source Value": val,
            "Best Match": r["matched"] or "No Match",
            "Score (%)": round(r["score"], 1),
            "Confidence": _confidence(r["score"]),
            "Method": r["method"],
            "Top 3 Suggestions": top3_str,
        })

    results_global = pd.DataFrame(rows)

    total = len(rows)
    high = sum(1 for r in rows if r["Confidence"]=="High" and r["Best Match"]!="No Match")
    med = sum(1 for r in rows if r["Confidence"]=="Medium" and r["Best Match"]!="No Match")
    no_match = sum(1 for r in rows if r["Best Match"]=="No Match")
    summary = f"✔ Done — {total} rows | High: {high} | Medium: {med} | No match: {no_match} | Mapping entries: {len(mapping_global)}"

    return results_global, summary, gr.update(visible=True)

# ---------------- Mapping actions ----------------
def confirm_mapping(edited_df):
    global mapping_global
    if edited_df is None or len(edited_df)==0: return "❌ No data to save."
    try:
        df = pd.DataFrame(edited_df)
        updated = 0
        for _, row in df.iterrows():
            src = str(row.get("Source Value","")).strip()
            match = str(row.get("Best Match","")).strip()
            if src and match and match!="No Match":
                mapping_global[src.lower()] = match
                updated += 1
        _save_mapping(mapping_global)
        return f"✔ {updated} entries confirmed and saved to {MAPPING_PATH}"
    except Exception as e:
        return f"❌ Error: {e}"

def clear_mapping():
    global mapping_global
    mapping_global = {}
    if MAPPING_PATH.exists(): MAPPING_PATH.unlink()
    return "🗑️ All mappings cleared."

def download_results(edited_df):
    if edited_df is None or len(edited_df)==0: return None
    out = "Matched_Results.xlsx"
    df = pd.DataFrame(edited_df)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Results", index=False)
        if mapping_global:
            pd.DataFrame(list(mapping_global.items()), columns=["Original","Confirmed Match"]).to_excel(writer, sheet_name="Mapping", index=False)
    log.info(f"Results saved to {out}")
    return out

# ---------------- Gradio UI ----------------
CSS = """
.stat-box { border: 1px solid #e0e0e0; border-radius: 8px; padding: 10px 16px; background: #fafafa; }
.summary-box textarea { font-family: monospace; font-size: 13px !important; }
footer { display: none !important; }
"""

with gr.Blocks(title="Enterprise Audit Matching Tool") as app:

    gr.Markdown("# 🤖 Enterprise Audit Matching Tool")
    gr.Markdown("Upload two Excel files → select match columns → run fuzzy matching → edit results → confirm mappings → download.")

    with gr.Row():
        with gr.Column():
            gr.Markdown("### 📁 File 1 — Source")
            file1_input = gr.File(label="Upload Excel (.xlsx / .xls)", file_types=[".xlsx", ".xls"])
            col1_dd = gr.Dropdown(label="Match column", choices=[], interactive=True)
            preview1 = gr.Dataframe(label="Preview", interactive=False)
        with gr.Column():
            gr.Markdown("### 📁 File 2 — Reference / Target")
            file2_input = gr.File(label="Upload Excel (.xlsx / .xls)", file_types=[".xlsx", ".xls"])
            col2_dd = gr.Dropdown(label="Match column", choices=[], interactive=True)
            preview2 = gr.Dataframe(label="Preview", interactive=False)

    with gr.Row():
        threshold_sl = gr.Slider(0, 100, value=60, step=1, label="Match Threshold (%)", scale=3)
        run_btn = gr.Button("🚀 Run Smart Matching", variant="primary", scale=1)

    summary_box = gr.Textbox(label="Status", interactive=False, elem_classes=["summary-box"])
    results_table = gr.Dataframe(label="Results — edit 'Best Match' to correct errors", interactive=True, wrap=False)

    with gr.Row(visible=False) as action_row:
        confirm_btn = gr.Button("🔄 Confirm & Save Mapping", variant="secondary")
        clear_btn = gr.Button("🗑️ Clear All Mappings", variant="stop")
        download_btn = gr.Button("📥 Download Results (.xlsx)")

    mapping_status = gr.Textbox(label="Mapping status", interactive=False)
    download_file = gr.File(label="Download", visible=True)

    # ---------------- Wire events ----------------
    file1_input.change(load_file1, inputs=file1_input, outputs=[col1_dd, preview1])
    file2_input.change(load_file2, inputs=file2_input, outputs=[col2_dd, preview2])
    run_btn.click(run_match, inputs=[col1_dd, col2_dd, threshold_sl], outputs=[results_table, summary_box, action_row])
    confirm_btn.click(confirm_mapping, inputs=results_table, outputs=mapping_status)
    clear_btn.click(clear_mapping, outputs=mapping_status)
    download_btn.click(download_results, inputs=results_table, outputs=download_file)

if __name__ == "__main__":
    app.launch(css=CSS, theme=gr.themes.Soft())