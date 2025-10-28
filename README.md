
# Regional Revenue (Lite) — Streamlit App

Super-light Streamlit app that imports your existing `monday_to_df.py` and displays a regional table in the exact order you want.

## What it shows
Rows (in order):
1. **Addition Scholars (2025)** → `amount_2025` where `mapped_class == "Restricted - MD Scholars"`
2. **Addition Global (2025)** → `amount_2025` where `mapped_class == "Restricted - Global Work"`
3. **Pledged but not received** → `balance_total` from `region_balances`
4. **Addition Unrestricted (2025)** → `amount_2025` where `mapped_class == "Unrestricted"`

Columns (in order):
`Africa, Latin America, Central Asia, South Asia, Middle East, Greatest Need, New Regions`

## Expected inputs
This app expects your script to define two DataFrames at import time:
- `region_rev_breakdown` with columns: `region, mapped_class, amount, amount_2025, amount_2024`
- `region_balances` with columns at least: `region, balance_total`

> Put your existing `monday_to_df.py` in the root of this repo so `app.py` can import it.

---

## Local run

```bash
# 1) Clone / download this repo
cd region-rev-lite

# 2) (Optional) create a venv
python -m venv .venv && source .venv/bin/activate   # macOS/Linux
# or: .venv\Scripts\activate                         # Windows

# 3) Install deps
pip install -r requirements.txt

# 4) Make sure your monday_to_df.py is in this folder
#    and that it sets region_rev_breakdown and region_balances.
#    If it needs MONDAY_API_TOKEN etc., export those first.

# 5) Run the app
streamlit run app.py
```

---

## Create a new GitHub repo & push

```bash
# From the folder that contains this README
git init
git add .
git commit -m "Initial commit: regional revenue lite app"
git branch -M main
gh repo create region-rev-lite --public --source=. --remote=origin --push
# If you don't have GitHub CLI, create an empty repo on GitHub named region-rev-lite,
# then do:
#   git remote add origin https://github.com/<you>/region-rev-lite.git
#   git push -u origin main
```

---

## Deploy on Streamlit Community Cloud

1. Go to https://share.streamlit.io
2. Connect your GitHub account (if not already).
3. **New app** → pick your repo (`region-rev-lite`), branch `main`, and main file `app.py`.
4. Add any required secrets (e.g., `MONDAY_API_TOKEN`) under **Advanced settings → Secrets** if your script needs them.
5. Deploy. That’s it.

### Streamlit secrets example
If `monday_to_df.py` reads env vars, you can set them like this in **Secrets**:
```toml
MONDAY_API_TOKEN = "xxx"
```

---

## Tweaks you might want
- Change currency formatting: edit the `column_config` in `app.py`.
- Add CSV export: wrap `table_df.to_csv(index=True)` in a `st.download_button`.
- Hide the debug expander: remove the expander block in `app.py`.
