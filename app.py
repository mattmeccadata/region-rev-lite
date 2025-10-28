
import streamlit as st
import pandas as pd
import importlib


st.set_page_config(page_title="Regional Revenue Snapshot", layout="wide")
st.title("Regional Revenue Snapshot")

# Add a Run / Refresh button
col1, col2 = st.columns([1, 8])
with col1:
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()   # clear any cached results
        importlib.reload(__import__("monday_to_df"))
        st.rerun()

from monday_to_df import region_rev_breakdown, region_balances

st.set_page_config(page_title="Regional Revenue Snapshot", layout="wide")

st.title("Regional Revenue Snapshot")

REGION_ORDER = [
    "Africa",
    "Latin America",
    "Central Asia",
    "South Asia",
    "Middle East",
    "Greatest Need",
    "New Regions",
]

ROW_LABELS = [
    "Addition Scholars (2025)",
    "Addition Global (2025)",
    "Pledged but not received",
    "Addition Unrestricted (2025)",
]


def _safe_num(x):
    try:
        return float(x)
    except Exception:
        return 0.0


@st.cache_data(show_spinner=False)
def build_table(region_rev_breakdown: pd.DataFrame, region_balances: pd.DataFrame) -> pd.DataFrame:
    # Normalize expected columns
    rr = region_rev_breakdown.copy()
    rb = region_balances.copy()

    # Ensure required columns exist
    for col in ["region", "mapped_class", "additions_2025"]:
        if col not in rr.columns:
            rr[col] = None
    for col in ["region", "balance_total"]:
        if col not in rb.columns:
            rb[col] = 0.0

    rr["region"] = rr["region"].astype(str)
    rr["mapped_class"] = rr["mapped_class"].astype(str)

    # Aggregate just in case there are multiple rows per region/class
    agg_rr = (
        rr.groupby(["region", "mapped_class"], dropna=False, as_index=False)["additions_2025"]
        .sum()
    )

    # Helper to get a series (indexed by region) for a given class -> additions_2025
    def values_for_class(class_name: str) -> pd.Series:
        sub = agg_rr[agg_rr["mapped_class"] == class_name]
        s = sub.set_index("region")["additions_2025"].map(_safe_num)
        return s

    # Row 1: Restricted - MD Scholars (2025)
    r1 = values_for_class("Restricted - MD Scholars")

    # Row 2: Restricted - Global Work (2025)
    r2 = values_for_class("Restricted - Global Work")

    # Row 3: Pledged but not received (balance_total from region_balances)
    r3 = rb.set_index("region")["balance_total"].map(_safe_num)

    # Row 4: Unrestricted (2025)
    r4 = values_for_class("Unrestricted")

    # Combine into one DataFrame with our explicit row order
    combined = pd.DataFrame({
        "Addition Scholars (2025)": r1,
        "Addition Global (2025)": r2,
        "Pledged but not received": r3,
        "Addition Unrestricted (2025)": r4,
    }).fillna(0.0)

    # Reindex columns for region order (axis=1), and index (rows) for row labels
    combined = combined.T  # rows -> labels, columns -> regions
    combined = combined.reindex(REGION_ORDER, axis=1)  # enforce region order
    combined = combined.reindex(ROW_LABELS, axis=0)    # enforce row order

    # Ensure numeric
    for col in combined.columns:
        combined[col] = combined[col].apply(_safe_num)

    return combined


table_df = build_table(region_rev_breakdown, region_balances)

st.caption("Copy-paste into https://docs.google.com/spreadsheets/d/1eDJm3Vcy191uTfafAcWBXNmyGNzgCsLT/edit?usp=sharing&ouid=105572649957203637297&rtpof=true&sd=true.")

# Display with nice currency formatting
col_config = {region: st.column_config.NumberColumn(format="%,.2f") for region in table_df.columns}

st.dataframe(
    table_df,
    use_container_width=True,
    column_config=col_config,
)

with st.expander("Debug: preview source DataFrames"):
    st.write("**region_rev_breakdown (head)**")
    st.dataframe(region_rev_breakdown.head(50), use_container_width=True)
    st.write("**region_balances (head)**")
    st.dataframe(region_balances.head(50), use_container_width=True)

st.markdown("---")
