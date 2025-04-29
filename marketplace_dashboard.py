import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pathlib import Path
from typing import Dict, List
import tempfile
import requests
import gdown
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from datetime import date, timedelta
from marketplace_api import get_api

# -----------------------------------------------------------------------------
# Streamlit page configuration & custom CSS
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Rionero Analisi Vendite", layout="wide")
st.markdown("""
    <style>
      [data-testid="stSidebar"] .block-container { padding-top: 0; padding-bottom: 0.5rem; }
      [data-testid="stSidebar"] button { font-size: 0.75rem !important; padding: 0.2rem 0.4rem !important; }
      [data-testid="stSidebar"] hr { border-top: 1px solid #eee; margin: 0.4rem 0; }
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Utility: format numbers as Euro currency
# -----------------------------------------------------------------------------
def format_euro(x) -> str:
    s = str(x).replace("€", "").replace(" ", "").strip()
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        val = float(s)
    except:
        val = 0.0
    out = "{:,.2f}".format(val).replace(",", "X").replace(".", ",").replace("X", ".")
    return f"€ {out}"

# -----------------------------------------------------------------------------
# Database & Excel column mapping
# -----------------------------------------------------------------------------
engine = create_engine("sqlite:///marketplace.db", future=True, echo=False)

COL_MAP: Dict[str,str] = {
    "Data":          "date",
    "Vendita":       "sale",
    "Acquisto":      "purchase_cost",
    "C. Market":     "commission",
    "SKU/EAN":       "sku",
    "Prodotto":      "product_name",
    "Qta":           "quantity",
}
ESSENTIAL = {"date", "sale"}
KEEP_COLS = [
    "order_date", "marketplace", "sheet",
    "sku", "product_name", "quantity",
    "sale", "purchase_cost", "commission",
]

# Create 'sales' table if it does not exist
with engine.begin() as conn:
    conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS sales (
          id INTEGER PRIMARY KEY,
          order_date DATE,
          marketplace TEXT,
          sheet TEXT,
          sku TEXT,
          product_name TEXT,
          quantity INTEGER,
          sale REAL,
          purchase_cost REAL,
          commission REAL,
          UNIQUE(order_date, marketplace, sheet, sku)
        );
    """)

# -----------------------------------------------------------------------------
# Excel helper functions: fetch, parse, clean, import
# -----------------------------------------------------------------------------
def fetch_xlsx(url: str) -> bytes:
    if "drive.google.com" in url:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        gdown.download(url, tmp.name, quiet=True, fuzzy=True)
        data = Path(tmp.name).read_bytes()
        Path(tmp.name).unlink(missing_ok=True)
        return data
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.content


def parse_excel(content: bytes, stem: str) -> List[pd.DataFrame]:
    dfs: List[pd.DataFrame] = []
    sheets = pd.read_excel(content, sheet_name=None, engine="openpyxl")
    for sheet_name, df in sheets.items():
        df = df.rename(columns=COL_MAP)
        if not ESSENTIAL.issubset(df.columns):
            continue
        df["sheet"], df["marketplace"] = sheet_name, stem
        dfs.append(df)
    return dfs


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["order_date"] = pd.to_datetime(df.get("date"), errors="coerce")
    df.drop(columns=["date"], errors="ignore", inplace=True)
    for c in ("sku","product_name","marketplace","sheet"):
        df[c] = df[c].astype(str)
    df["quantity"] = pd.to_numeric(df.get("quantity",1), errors="coerce").fillna(1).astype(int)
    for c in ("sale","purchase_cost","commission"):
        df[c] = pd.to_numeric(df.get(c,0), errors="coerce").fillna(0.0)
    for c in KEEP_COLS:
        if c not in df:
            df[c] = 0 if c in {"quantity","sale","purchase_cost","commission"} else None
    return df[KEEP_COLS]


def import_to_db(dfs: List[pd.DataFrame]) -> int:
    if not dfs:
        return 0
    big = clean(pd.concat(dfs, ignore_index=True))
    big.drop_duplicates(subset=["order_date","marketplace","sheet","sku"], inplace=True)
    with engine.begin() as conn:
        existing = pd.read_sql("SELECT order_date,marketplace,sheet,sku FROM sales", conn, parse_dates=["order_date"])
    if not existing.empty:
        merged = big.merge(existing, on=["order_date","marketplace","sheet","sku"], how="left", indicator=True)
        big = merged[merged["_merge"]=="left_only"].drop(columns=["_merge"])
    if big.empty:
        return 0
    with engine.begin() as conn:
        big.to_sql("sales", conn, if_exists="append", index=False, method="multi")
    return len(big)

@st.cache_data(show_spinner=False)
def drive_to_dfs() -> List[pd.DataFrame]:
    dfs: List[pd.DataFrame] = []
    with tempfile.TemporaryDirectory() as td:
        files = gdown.download_folder(REMOTE_FOLDER, quiet=True, output=td, use_cookies=False)
        for p in files:
            if not str(p).endswith(".xlsx"):
                continue
            try:
                b = fetch_xlsx(p) if str(p).startswith("http") else Path(p).read_bytes()
                dfs.extend(parse_excel(b, Path(p).stem))
            except Exception as e:
                st.error(f"❌ Errore {Path(p).name}: {e}")
    return dfs

# -----------------------------------------------------------------------------
# API section with dynamic cache invalidation
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_orders_api(marketplace_name: str, start_date: date, end_date: date, key: int = 0) -> pd.DataFrame:
    client = get_api(marketplace_name)
    return client.get_orders(start_date, end_date)

# -----------------------------------------------------------------------------
# Main Streamlit app
def main():
    st.title("📊 Rionero Analisi Vendite")

    # Sidebar: Excel upload & filters
    with st.sidebar:
        source = st.selectbox("Sorgente Excel", ["File manuali","Cartella Drive"])
        if st.button("Aggiorna ora"):
            if source == "File manuali":
                uploads = st.file_uploader("Trascina .xlsx", type="xlsx", accept_multiple_files=True)
                if not uploads:
                    st.error("Carica almeno un file.")
                    st.stop()
                dfs = [df for uf in uploads for df in parse_excel(uf.read(), uf.name.split(".")[0])]
                st.success(f"Righe importate: {import_to_db(dfs)}")
            else:
                st.success(f"Righe importate: {import_to_db(drive_to_dfs())}")

        st.markdown("---")
        bounds = pd.read_sql("SELECT MIN(order_date) AS dmin,MAX(order_date) AS dmax FROM sales", engine, parse_dates=["dmin","dmax"]).iloc[0]
        dmin = bounds["dmin"].date() if pd.notna(bounds["dmin"]) else date.today()
        dmax = bounds["dmax"].date() if pd.notna(bounds["dmax"]) else date.today()

        markets = sorted(pd.read_sql("SELECT DISTINCT marketplace FROM sales", engine)["marketplace"])
        sel = st.multiselect("Marketplace Excel", markets, default=markets)

        dates = st.date_input("Intervallo Excel", (dmin, dmax), min_value=dmin, max_value=date.today())
        sd, ed = (dates if isinstance(dates, tuple) else (dates, dates))

        st.markdown("---")
        c1, c2, c3 = st.columns(3)
        c4, c5, c6 = st.columns(3)
        today = date.today()
        if c1.button("30 giorni"): sd, ed = today - timedelta(days=30), today
        if c2.button("Oggi"):       sd, ed = today, today
        if c3.button("Ieri"):       sd, ed = today - timedelta(days=1), today - timedelta(days=1)
        if c4.button("Questa Settimana"):
            mon = today - timedelta(days=today.weekday())
            sd, ed = mon, today
        if c5.button("Mese Corrente"): sd, ed = today.replace(day=1), today
        if c6.button("Questo Anno"):  sd, ed = date(today.year, 1, 1), today

    st.markdown("---")
    st.markdown(f"**Periodo Excel:** {sd} – {ed}")
    df_x = pd.read_sql("SELECT * FROM sales", engine, parse_dates=["order_date"])
    mask_x = df_x["marketplace"].isin(sel) & df_x["order_date"].dt.date.between(sd, ed)
    filt_x = df_x[mask_x]

    if filt_x.empty:
        st.warning("Nessun dato Excel")
    else:
        v1, v2, v3, v4, v5 = st.columns(5)
        v1.metric("Ordini Excel", len(filt_x))
        fatturato = filt_x["sale"].sum()
        costi     = filt_x["purchase_cost"].sum()
        commissioni = filt_x["commission"].sum()
        margine   = fatturato - costi - commissioni
        perc_margine = (margine / fatturato) * 100 if fatturato else 0
        v2.metric("Fatturato",      format_euro(fatturato))
        v3.metric("Costi",          format_euro(costi))
        v4.metric("Commissione",    format_euro(commissioni))
        v5.metric("Margine Lordo Excel", format_euro(margine))
        st.metric("% Margine Lordo Excel", f"{perc_margine:.2f}%")

        st.subheader("Top Prodotti Excel")
        sel_mp2 = st.radio("Marketplace", ["Tutti"] + sel, horizontal=True)
        df2 = filt_x if sel_mp2 == "Tutti" else filt_x[filt_x["marketplace"] == sel_mp2]
        top_n = st.slider("Top N", 5, 50, 10)

        df2 = df2[~df2["sku"].isin(["0", "nan", ""]) & df2["product_name"].notna() & (df2["sale"] > 0)]
        topx = (
            df2
            .groupby(["sku", "marketplace", "product_name"])  
            .agg(
                quantitá=("quantity", "sum"),
                vendite=("sale", "sum"),
                commissione=("commission", "sum"),
                acquisto=("purchase_cost", "sum"),
            )
            .reset_index()
        )
        topx["margine"] = topx["vendite"] - topx["commissione"] - topx["acquisto"]
        topx["% margine"] = (topx["margine"] / topx["vendite"]) * 100
        topx = topx.sort_values("quantitá", ascending=False).head(top_n)
        for c in ("vendite", "commissione", "acquisto", "margine"): topx[c] = topx[c].apply(format_euro)
        topx["% margine"] = topx["% margine"].apply(lambda x: f"{x:.2f}%")
        st.dataframe(topx, use_container_width=True)

    # -------------------------------------------------------------------------
    st.markdown("---")
    st.markdown("## Vendite Estratte via API")
    opts = ["Worten", "Leroy Merlin"]
    api_mp = st.selectbox("Marketplace API", opts)
    api_key = api_mp.strip().lower().replace(" ", "")

    preset = st.radio(
        "Filtra ordini API per",
        ["Oggi", "Ieri", "Ultimi 30 giorni", "Questa Settimana", "Mese Corrente", "Questo Anno", "Personalizzato"],
        horizontal=True
    )
    today = date.today()
    if preset == "Oggi": api_sd, api_ed = today, today
    elif preset == "Ieri": api_sd, api_ed = today - timedelta(days=1), today - timedelta(days=1)
    elif preset == "Ultimi 30 giorni": api_sd, api_ed = today - timedelta(days=29), today
    elif preset == "Questa Settimana":
        mon = today - timedelta(days=today.weekday()); api_sd, api_ed = mon, today
    elif preset == "Mese Corrente": api_sd, api_ed = today.replace(day=1), today
    elif preset == "Questo Anno": api_sd, api_ed = date(today.year, 1, 1), today
    else:
        d = st.date_input("Intervallo personalizzato", value=(today - timedelta(days=7), today), min_value=date(today.year-1,1,1), max_value=today)
        if isinstance(d, tuple) and len(d)==2: api_sd, api_ed = d
        else: api_sd = api_ed = d

    # Bottone per forzare il refresh degli ordini API
    if "reload_key" not in st.session_state: st.session_state["reload_key"] = 0
    if st.button("🔄 Forza aggiornamento API"): st.session_state["reload_key"] += 1

    orders_df = load_orders_api(api_key, api_sd, api_ed, key=st.session_state["reload_key"])

    # Ensure minimal columns
    for col in ("order_id","sku","product_name","order_status","order_date"): orders_df[col] = orders_df.get(col,"")
    for col in ("sale_price","commission","purchase_cost"): orders_df[col] = pd.to_numeric(orders_df.get(col,0), errors="coerce").fillna(0.0)

    status = st.radio("Stato Ordine", ["TUTTI","Ordini Effettivi","Ordini Cancellati"], horizontal=True)
    if status == "Ordini Effettivi":
        orders_df = orders_df[orders_df["order_status"].str.upper().isin(["SHIPPED","SHIPPING","RECEIVED","CLOSED","STAGING"])]
    elif status == "Ordini Cancellati":
        orders_df = orders_df[orders_df["order_status"].str.upper().isin(["CANCELED","CANCELLED"])]

    vendite   = orders_df["sale_price"].sum()
    comm      = orders_df["commission"].sum()
    acquisto  = orders_df["purchase_cost"].sum()
    margine   = vendite - comm - acquisto

    k1,k2,k3,k4 = st.columns(4)
    k1.metric("Ordini (API)", orders_df["order_id"].nunique())
    k2.metric("Vendite", format_euro(vendite))
    k3.metric("Commissione", format_euro(comm))
    k4.metric("Margine Lordo", format_euro(margine))

    st.subheader("Dettaglio Ordini API")
    orders_df["margine_lordo"] = orders_df["sale_price"] - orders_df["commission"] - orders_df["purchase_cost"]
    df_table = orders_df[["order_id","sku","order_date","sale_price","commission","margine_lordo","product_name","order_status"]].copy()
    df_table.columns = ["ID Ordine","SKU","Data","Vendita","Commissione","Margine Lordo","Nome Prodotto","Stato Ordine"]
    for c in ("Vendita","Commissione","Margine Lordo"): df_table[c] = df_table[c].apply(format_euro)
    st.dataframe(df_table, use_container_width=True)

if __name__ == "__main__":
    main()
