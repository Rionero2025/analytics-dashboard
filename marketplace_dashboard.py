from pathlib import Path
from typing import Dict, List
import tempfile
import requests
import gdown
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

# ───── Page config & custom CSS ─────
st.set_page_config(page_title="Marketplace Dashboard", layout="wide")
st.markdown(
    """
    <style>
      /* sidebar: padding ridotto in alto */
      [data-testid="stSidebar"] .block-container {
        padding-top: 0rem;
        padding-bottom: 0.5rem;
      }
      /* font e bottoni super-compatti */
      [data-testid="stSidebar"] label,
      [data-testid="stSidebar"] .stSelectbox>div,
      [data-testid="stSidebar"] .stMultiSelect>div,
      [data-testid="stSidebar"] .stDateInput>div,
      [data-testid="stSidebar"] button {
        font-size: 0.75rem !important;
      }
      [data-testid="stSidebar"] button {
        padding: 0.2rem 0.4rem !important;
      }
      /* separatore leggero */
      [data-testid="stSidebar"] hr {
        border-top: 1px solid #eee;
        margin: 0.4rem 0;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# ───────────────────── Config base ─────────────────────
REMOTE_FOLDER = "https://drive.google.com/drive/folders/1y4c1Qo5E_WdgFmqjXWrGrN0QMkLR0wp?usp=drive_link"
engine = create_engine("sqlite:///marketplace.db", future=True, echo=False)

COL_MAP: Dict[str, str] = {
    "Data": "date",
    "Vendita": "sale",
    "Acquisto": "purchase_cost",
    "C. Market": "commission",
    "SKU/EAN": "sku",
    "Prodotto": "product_name",
    "Qta": "quantity",
}
ESSENTIAL = {"date", "sale"}
KEEP_COLS = [
    "order_date", "marketplace", "sheet", "sku", "product_name",
    "quantity", "sale", "purchase_cost", "commission",
]

# ───────────────────── Init DB ─────────────────────
with engine.begin() as conn:
    conn.exec_driver_sql(
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

# ───────────────────── Helper download/parse ─────────────────────
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
    if "date" in df.columns:
        df.drop(columns=["date"], inplace=True)
    for col in ("sku", "product_name", "marketplace", "sheet"):
        if col in df.columns:
            df[col] = df[col].astype(str)
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).astype(int)
    else:
        df["quantity"] = 1
    for col in ("sale", "purchase_cost", "commission"):
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0.0)
    for col in KEEP_COLS:
        if col not in df.columns:
            default = 0 if col in {"quantity","sale","purchase_cost","commission"} else None
            df[col] = default
    return df[KEEP_COLS]

def import_to_db(dfs: List[pd.DataFrame]) -> int:
    if not dfs:
        return 0
    big = clean(pd.concat(dfs, ignore_index=True))
    big.drop_duplicates(subset=["order_date","marketplace","sheet","sku"], inplace=True)
    with engine.begin() as conn:
        existing = pd.read_sql(
            "SELECT order_date, marketplace, sheet, sku FROM sales",
            conn, parse_dates=["order_date"]
        )
    if not existing.empty:
        merged = big.merge(existing, on=["order_date","marketplace","sheet","sku"],
                           how="left", indicator=True)
        big = merged[merged["_merge"]=="left_only"].drop(columns=["_merge"])
    if big.empty:
        return 0
    with engine.begin() as conn:
        big.to_sql("sales", conn, if_exists="append", index=False, method="multi")
    return len(big)

def drive_to_dfs() -> List[pd.DataFrame]:
    dfs: List[pd.DataFrame] = []
    with tempfile.TemporaryDirectory() as td:
        files = gdown.download_folder(
            REMOTE_FOLDER, quiet=True, remaining_ok=True,
            output=td, use_cookies=False
        )
        for p in files:
            if not str(p).endswith(".xlsx"):
                continue
            try:
                content = fetch_xlsx(p) if str(p).startswith("http") else Path(p).read_bytes()
                dfs.extend(parse_excel(content, Path(p).stem))
            except Exception as e:
                st.error(f"❌ Errore {Path(p).name}: {e}")
    return dfs

# ───────────────────── Streamlit UI ─────────────────────
def main():
    st.title("📊 Marketplace Dashboard — DB SQLite")

    # ────── Sidebar: Aggiorna DB ──────
    with st.sidebar:
        mode = st.selectbox("Sorgente", ["File manuali", "Cartella Drive"])
        run = st.button("Aggiorna ora")

    if run:
        if mode == "File manuali":
            upl = st.file_uploader(
                "Trascina uno o più .xlsx", type="xlsx", accept_multiple_files=True
            )
            if not upl:
                st.error("Carica almeno un file."); st.stop()
            dfs = [
                df
                for uf in upl
                for df in parse_excel(uf.read(), uf.name.split(".")[0])
            ]
            st.success(f"Righe nuove: {import_to_db(dfs)}")
        else:
            st.success(f"Righe nuove: {import_to_db(drive_to_dfs())}")

    # ────── Carica dati dal DB ──────
    df = pd.read_sql("SELECT * FROM sales", engine, parse_dates=["order_date"])
    if df.empty:
        st.info("DB vuoto: importa dei file."); st.stop()

    # ────── Sidebar: Filtri & Quick Range ──────
    with st.sidebar:
        st.header("Filtri")
        markets = sorted(df["marketplace"].unique())
        sel = st.multiselect("Marketplace", markets, default=markets)

        dmin, dmax = df["order_date"].min(), df["order_date"].max()
        dates = st.date_input(
            "Intervallo",
            value=(dmin.date(), dmax.date()),
            min_value=dmin.date(),
            max_value=dmax.date(),
            key="date_range"
        )
        if isinstance(dates, tuple):
            sd, ed = dates
        else:
            sd = ed = dates
        sd = max(sd, dmin.date())
        ed = min(ed, dmax.date())

        st.markdown("---")
        st.subheader("Dati da analizzare")
        r1 = st.columns(3)
        r2 = st.columns(3)
        import datetime as _dt
        today = _dt.date.today()

        if r1[0].button("30giorni"):
            sd, ed = today - _dt.timedelta(days=30), today
        if r1[1].button("Oggi"):
            sd = ed = today
        if r1[2].button("Ieri"):
            y = today - _dt.timedelta(days=1); sd = ed = y

        if r2[0].button("Settimana"):
            sd = today - _dt.timedelta(days=today.weekday())
            ed = sd + _dt.timedelta(days=6)
        if r2[1].button("Mese corr."):
            sd = today.replace(day=1)
            nm = sd.replace(day=28) + _dt.timedelta(days=4)
            ed = nm - _dt.timedelta(days=nm.day)
        if r2[2].button("Anno"):
            sd = today.replace(month=1, day=1)
            ed = today

    # ────── Applica filtri ──────
    filt = df[
        df["marketplace"].isin(sel) &
        df["order_date"].between(pd.Timestamp(sd), pd.Timestamp(ed))
    ]
    if filt.empty:
        st.warning("Nessun record."); st.stop()

    # ────── KPI ──────
    sales  = filt["sale"].sum()
    costs  = filt["purchase_cost"].sum()
    comm   = filt["commission"].sum()
    margin = sales - (costs + comm)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Fatturato",          f"€ {sales:,.2f}")
    k2.metric("Acquisto",           f"€ {costs:,.2f}")
    k3.metric("Commissione Market", f"€ {comm:,.2f}")
    k4.metric("Margine Lordo",      f"€ {margin:,.2f}")

    # ────── Trend giornaliero ──────
    st.subheader("Trend giornaliero")
    trend = (
        filt
        .groupby([filt["order_date"].dt.date, "marketplace"])
        .agg(vendite=("sale","sum"))
        .unstack(fill_value=0)["vendite"]
    )
    st.line_chart(trend)

    # ────── Riepilogo marketplace ──────
    st.subheader("Riepilogo marketplace")
    summary = (
        filt
        .groupby("marketplace")
        .agg(
            vendite             = ("sale","sum"),
            acquisto            = ("purchase_cost","sum"),
            commissione_market  = ("commission","sum"),
        )
    )
    summary["margine_lordo"] = summary["vendite"] - (
        summary["acquisto"] + summary["commissione_market"]
    )
    summary = summary.reset_index()
    def fmt_eur(x): return f"€ {x:,.2f}"
    for col in ["vendite","acquisto","commissione_market","margine_lordo"]:
        summary[col] = summary[col].apply(fmt_eur)
    st.dataframe(summary, use_container_width=True)

    # ────── Prodotti più venduti ──────
    st.subheader("Prodotti più venduti")
    mp = st.radio("Marketplace", ["Tutti i marketplace"] + markets, horizontal=True)
    dt = filt if mp == "Tutti i marketplace" else filt[filt["marketplace"] == mp]
    top_n = st.slider("Top N", 5, 50, 10)
    grp = ["sku"] + (["product_name"] if "product_name" in dt.columns else [])
    data_top = (
        dt
        .groupby(grp)
        .agg(
            qta                 = ("quantity","sum"),
            vendite             = ("sale","sum"),
            acquisto            = ("purchase_cost","sum"),
            commissione_market  = ("commission","sum"),
        )
        .reset_index()
        .sort_values("qta", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    data_top["margine_lordo"] = (
        data_top["vendite"] - (data_top["acquisto"] + data_top["commissione_market"])
    )
    for col in ["vendite","acquisto","commissione_market","margine_lordo"]:
        data_top[col] = data_top[col].apply(fmt_eur)
    data_top.index += 1
    st.dataframe(data_top, use_container_width=True)

    # ────── Export CSV ──────
    st.download_button(
        "Scarica CSV filtrato",
        filt.to_csv(index=False).encode("utf-8"),
        "dati_filtrati.csv",
        "text/csv"
    )

if __name__ == "__main__":
    main()
