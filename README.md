# 🔍 Internal Audit Makes Easy

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue)](https://python.org)
[![Gradio](https://img.shields.io/badge/Gradio-4.0%2B-orange)](https://gradio.app)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**v2.0 — Now with Bank Reconciliation + Professional Architecture**

Unified desktop application for internal auditors with two powerful reconciliation modules.

---

## 🆕 What's New in v2.0

| Feature | v1.0 | v2.0 |
|---------|------|------|
| **Stock Reconciliation** | ✅ Basic fuzzy | ✅ Advanced fuzzy + learning |
| **Bank Reconciliation** | ❌ | ✅ NEW — Amount matching |
| **Session Isolation** | ❌ Globals | ✅ `gr.State` per user |
| **Auto-Accept Matches** | ❌ | ✅ Configurable threshold |
| **Excel Styling** | ❌ Basic | ✅ Professional formatted |
| **Persistent Mapping** | ✅ File-based | ✅ Improved + cached |

---

## 📦 Modules

| Module | Purpose | Matching Type |
|--------|---------|---------------|
| **📦 Stock Reconciliation** | Purchase ↔ Sale entries | Fuzzy text with AI learning |
| **🏦 Bank Reconciliation** | Books ↔ Bank Statement | Amount matching with tolerance |

---

## 🚀 Quick Start

```bash
# Clone
git clone https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
cd YOUR_REPO_NAME

# Install
pip install -r requirements.txt

# Run
python app.py
