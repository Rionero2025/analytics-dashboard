"""
Marketplace Dashboard – v3.5
============================
* Quick‑range periodi: Ultimi 30 giorni, Oggi, Ieri, Settimana, Mese in corso, Anno corrente
* KPI: Fatturato, Costi (acquisto+commissioni), Margine Lordo, %
* Riepilogo marketplace con colonne: Vendite, Acquisto, Commissione Market, Margine Lordo
* Trend giornaliero con serie separate per marketplace
* Prodotti più venduti con filtro marketplace, colonne: SKU, Nome, Qta, Vendite, Acquisto, Commissione, Margine
* Formato europeo € con separatore migliaia "." e decimale ","
"""

from pathlib import Path
from typing import Dict, List
import datetime as dt
import tempfile, requests

import gdown
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

st.set_page_config(page_title="Marketplace Dashboard", layout="wide")

# ─── Helper formato € ──────────────────────────────────────────────
EURO = "€ "

def eur(val: float | int) -> str:
    """Restituisce stringa formattata € 1.234,56"""
    s = f"{val:,.2f}"
    return EURO + s.replace(",", "_").replace(".", ",").replace("_", ".")

# ─── Config ────────────────────────────────────────────────────────
REMOTE_FOLDER = "https://drive.google.com/drive/folders/1y4c1Qo5eE_WdgFmqjXWrGrN0QMkLR0wp?usp=drive_link"
EXTRA_LINKS: List[str] = (
    [l.strip() for l in Path("links.txt").read_text().splitlines() if l.strip()]
    if Path("links.txt").exists() else []
)
engine = create_engine("sqlite:///marketplace.db", future=True, echo=False)

COL_MAP: Dict[str, str] = {
    "Data": "date", "Vendita": "sale", "Acquisto": "purchase_cost",
    "C. Market": "commission", "SKU/EAN": "sku", "Prodotto": "product_name",
    "Qta": "quantity",
}
ESSENTIAL = {"date", "sale"}
KEEP_COLS = [
    "order_date", "marketplace", "sheet", "sku", "product_name",
    "quantity", "sale", "purchase_cost", "commission",
]

# ─── Init DB ──────────────────────────────────────────────────────
with engine.begin() as con:
    con.exec_driver_sql(
        """CREATE TABLE IF NOT EXISTS sales (
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
        );"""
    )

# ─── Funzioni download/parse ──────────────────────────────────────
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
    for s, df in sheets.items():
        df = df.rename(columns=COL_MAP)
        if not ESSENTIAL.issubset(df.columns):
            continue
        df["sheet"], df["marketplace"] = s, stem
        dfs.append(df)
    return dfs


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["order_date"] = pd.to_datetime(df.get("date"), errors="coerce")
    df.drop(columns=[c for c in ("date",) if c in df.columns], inplace=True)
    for col in ("sku", "product_name", "marketplace", "sheet"): df[col] = df.get(col, "").astype(str)
    df["quantity"] = pd.to_numeric(df.get("quantity", 1), errors="coerce").fillna(1).astype(int)
    for col in ("sale", "purchase_cost", "commission"): df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0.0)
    for col in KEEP_COLS:
        if col not in df.columns:
            df[col] = 0 if col in {"sale","purchase_cost","commission","quantity"} else None
    return df[KEEP_COLS]


def import_to_db(dfs: List[pd.DataFrame]) -> int:
    if not dfs: return 0
    big = clean(pd.concat(dfs, ignore_index=True))
    big.drop_duplicates(subset=["order_date","marketplace","sheet","sku"], inplace=True)
    with engine.begin() as con:
        existing = pd.read_sql("SELECT order_date,marketplace,sheet,sku FROM sales", con, parse_dates=["order_date"])
    if not existing.empty:
        big = big.merge(existing, on=["order_date","marketplace","sheet","sku"], how="left", indicator=True)
        big = big[big["_merge"]=="left_only"].drop(columns="_merge")
    if big.empty: return 0
    with engine.begin() as con: big.to_sql("sales", con, if_exists="append", index=False, method="multi")
    return len(big)


def drive_to_dfs() -> List[pd.DataFrame]:
    dfs: List[pd.DataFrame] = []
    with tempfile.TemporaryDirectory() as td:
        files = gdown.download_folder(REMOTE_FOLDER, quiet=True, remaining_ok=True, output=td, use_cookies=False)
        files.extend(EXTRA_LINKS)
        for p in files:
            try:
                content = fetch_xlsx(p) if str(p).startswith("http") else Path(p).read_bytes()
                dfs.extend(parse_excel(content, Path(p).stem))
            except Exception as e:
                st.error(f"Errore {Path(p).name}: {e}")
    return dfs

# ─── App ─────────────────────────────────────────────────────────
def main():
    st.title("📊 Marketplace Dashboard — DB SQLite")

    # Import
    with st.sidebar:
        st.header("Aggiorna DB")
        mode = st.selectbox("Sorgente", ["File manuali","Cartella Drive"])
        if st.button("Aggiorna DB ora"):
            if mode=="File manuali":
                ups = st.file_uploader("Trascina .xlsx", type="xlsx", accept_multiple_files=True)
                if ups: st.success(f"Righe nuove: {import_to_db([df for u in ups for df in parse_excel(u.read(),u.name.split('.')[0])])}")
            else:
                st.success(f"Righe nuove: {import_to_db(drive_to_dfs())}")

    df = pd.read_sql("SELECT * FROM sales", engine, parse_dates=["order_date"])
    if df.empty:
        st.info("DB vuoto: importa dati.")
        st.stop()

    # Filtri
    with st.sidebar:
        st.header("Filtri")
        markets = sorted(df["marketplace"].unique())
        sel = st.multiselect("Marketplace", markets, default=markets)
        dmin, dmax = df["order_date"].min().date(), df["order_date"].max().date()
        today = dt.date.today()
        sd, ed = st.date_input("Intervallo", (dmin, today), min_value=dmin, max_value=today)
        st.markdown("**Dati da Analizzare**")
        c1,c2,c3 = st.columns(3)
        c4,c5,c6 = st.columns(3)
        if c1.button("Ultimi 30 giorni"): sd,ed = today-dt.timedelta(30),today
        if c2.button("Oggi"): sd=ed=today
        if c3.button("Ieri"): sd=ed=today-dt.timedelta(1)
        if c4.button("Settimana"): sd=today-dt.timedelta(today.weekday()); ed=sd+dt.timedelta(6)
        if c5.button("Mese in corso"): sd=today.replace(day=1); nm=sd.replace(day=28)+dt.timedelta(4); ed=nm-dt.timedelta(nm.day)
        if c6.button("Anno corrente"): sd=today.replace(month=1,day=1); ed=today

    filt = df[df["marketplace"].isin(sel) & df["order_date"].between(pd.Timestamp(sd),pd.Timestamp(ed))]
    if filt.empty:
        st.warning("Nessun record.")
        st.stop()

    # KPI
    filt["margine_lordo"] = filt["sale"]-filt["purchase_cost"]-filt["commission"]
    sales = filt["sale"].sum()
    costs = (filt["purchase_cost"]+filt["commission"]).sum()
    margin = round(filt["margine_lordo"].sum(),2)
    k1,k2,k3,k4 = st.columns(4)
    k1.metric("Fatturato",eur(sales))
    k2.metric("Costi (acq+comm)",eur(costs))
    k3.metric("Margine Lordo",eur(margin))
    k4.metric("Margine %",f"{margin/sales*100 if sales else 0:.1f}%")

    # Riepilogo marketplace
    st.subheader("Riepilogo marketplace")
    summary = filt.groupby("marketplace").agg(
        vendite=("sale","sum"),
        acquisto=("purchase_cost","sum"),
        commissione_market=("commission","sum"),
        margine_lordo=("margine_lordo","sum")
    )
    for c in summary.columns: summary[c]=summary[c].apply(eur)
    st.dataframe(summary.reset_index(),use_container_width=True)

    # Trend giornaliero
    st.subheader("Trend giornaliero")
    trend = filt.pivot_table(index=filt["order_date"].dt.date,columns="marketplace",values="sale",aggfunc="sum",fill_value=0)
    st.line_chart(trend)

    # Prodotti più venduti
    st.subheader("Prodotti più venduti")
    opts=["Tutti i marketplace"]+markets
    selm=st.radio("Marketplace",opts,horizontal=True)
    dtop=filt if selm==opts[0] else filt[filt["marketplace"]==selm]
    topn=st.slider("Top N",5,50,10)
    cols=["sku"]+(["product_name"] if "product_name" in dtop.columns else [])
    prod = dtop.groupby(cols).agg(
        qta=("quantity","sum"),
        vendite=("sale","sum"),
        acquisto=("purchase_cost","sum"),
        commissione=("commission","sum"),
    )
    prod["margine"] = prod["vendite"]-prod["acquisto"]-prod["commissione"]
    prod = prod.sort_values("qta",ascending=False).head(topn).reset_index()
    for c in ["vendite","acquisto","commissione","margine"]: prod[c]=prod[c].apply(eur)
    prod.index=prod.index+1
    st.dataframe(prod,use_container_width=True)

    # Export CSV
    st.download_button("Scarica CSV filtrato",filt.to_csv(index=False).encode("utf-8"),"dati_filtrati.csv","text/csv")

if __name__=="__main__":
    main()
