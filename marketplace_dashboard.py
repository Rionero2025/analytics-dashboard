from pathlib import Path
from typing import Dict, List
import tempfile
import requests
import gdown
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from datetime import date, timedelta
from marketplace_api import get_orders_worten

# ───── Config pagina e CSS ───────────────────────────────────────
st.set_page_config(page_title="Rionero Analisi Vendite", layout="wide")
st.markdown("""
    <style>
      [data-testid="stSidebar"] .block-container { padding-top: 0; padding-bottom: 0.5rem; }
      [data-testid="stSidebar"] label,
      [data-testid="stSidebar"] .stSelectbox>div,
      [data-testid="stSidebar"] .stMultiSelect>div,
      [data-testid="stSidebar"] .stDateInput>div,
      [data-testid="stSidebar"] button { font-size: 0.75rem !important; }
      [data-testid="stSidebar"] button { padding: 0.2rem 0.4rem !important; }
      [data-testid="stSidebar"] hr { border-top: 1px solid #eee; margin: 0.4rem 0; }
    </style>
""", unsafe_allow_html=True)

# ───── Utility per formattare valori in Euro ─────────────────────
def format_euro(x: float) -> str:
    s = f"{x:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"€ {s}"

# ───────────────── Config base ────────────────────────────────────
REMOTE_FOLDER = "https://drive.google.com/drive/folders/1y4c1Qo5eE_WdgFmqjXWrGrN0QMkLR0wp?usp=drive_link"
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
    "order_date","marketplace","sheet","sku","product_name",
    "quantity","sale","purchase_cost","commission",
]

# ───────────────── Inizializzazione DB ─────────────────────────────
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

# ───────────────── Helper per Excel ─────────────────────────────────
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
        if c in df: df[c] = df[c].astype(str)
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
        existing = pd.read_sql(
            "SELECT order_date,marketplace,sheet,sku FROM sales",
            conn, parse_dates=["order_date"]
        )
    if not existing.empty:
        m = big.merge(
            existing,
            on=["order_date","marketplace","sheet","sku"],
            how="left",
            indicator=True
        )
        big = m[m["_merge"]=="left_only"].drop(columns=["_merge"])
    if big.empty:
        return 0
    with engine.begin() as conn:
        big.to_sql("sales", conn, if_exists="append", index=False, method="multi")
    return len(big)

def drive_to_dfs() -> List[pd.DataFrame]:
    dfs: List[pd.DataFrame] = []
    with tempfile.TemporaryDirectory() as td:
        files = gdown.download_folder(
            REMOTE_FOLDER, quiet=True, output=td, use_cookies=False
        )
        for p in files:
            if not str(p).endswith(".xlsx"):
                continue
            try:
                if str(p).startswith("http"):
                    content = fetch_xlsx(p)
                else:
                    content = Path(p).read_bytes()
                dfs.extend(parse_excel(content, Path(p).stem))
            except Exception as e:
                st.error(f"❌ Errore {Path(p).name}: {e}")
    return dfs

# ───────────────── Streamlit UI ──────────────────────────────────
def main():
    st.title("📊 Rionero Analisi Vendite")

    # Sidebar: upload / import
    with st.sidebar:
        mode = st.selectbox("Sorgente", ["File manuali","Cartella Drive"])
        if st.button("Aggiorna ora"):
            if mode == "File manuali":
                upl = st.file_uploader(
                    "Trascina .xlsx", type="xlsx", accept_multiple_files=True
                )
                if not upl:
                    st.error("Carica almeno un file.")
                    st.stop()
                dfs = [
                    df
                    for uf in upl
                    for df in parse_excel(uf.read(), uf.name.split(".")[0])
                ]
                st.success(f"Righe nuove: {import_to_db(dfs)}")
            else:
                st.success(f"Righe nuove: {import_to_db(drive_to_dfs())}")

        st.markdown("---")
     # ───────────────── Sidebar: range date ─────────────────────────────────────
bounds = pd.read_sql(
    "SELECT MIN(order_date) AS dmin, MAX(order_date) AS dmax FROM sales",
    engine, parse_dates=["dmin","dmax"]
).iloc[0]

raw_min, raw_max = bounds["dmin"], bounds["dmax"]

# Se il DB è vuoto (NaT), usiamo oggi come default
if pd.isna(raw_min):
    dmin = date.today()
else:
    dmin = raw_min.date()

if pd.isna(raw_max):
    dmax = date.today()
else:
    dmax = raw_max.date()

markets = sorted(
    pd.read_sql("SELECT DISTINCT marketplace FROM sales", engine)["marketplace"]
)
sel = st.multiselect("Marketplace", markets, default=markets)

dates = st.date_input(
    "Intervallo",
    (dmin, dmax),
    min_value=dmin,
    max_value=date.today(),
    key="date_range"
)
if isinstance(dates, tuple) and len(dates) == 2:
    sd, ed = dates
else:
    sd = ed = dates


        st.markdown("---")
        st.subheader("Dati da analizzare rapidi")
        a1,a2,a3 = st.columns(3)
        b1,b2,b3 = st.columns(3)
        today = date.today()
        if a1.button("30giorni"):
            sd, ed = today - timedelta(days=30), today
        if a2.button("Oggi"):
            sd = ed = today
        if a3.button("Ieri"):
            sd, ed = today - timedelta(days=1), today - timedelta(days=1)
        if b1.button("Settimana"):
            sd = today - timedelta(days=today.weekday())
            ed = sd + timedelta(days=6)
        if b2.button("Mese corr."):
            sd = today.replace(day=1)
            nm = sd.replace(day=28) + timedelta(days=4)
            ed = nm - timedelta(days=nm.day)
        if b3.button("Anno"):
            sd = today.replace(month=1, day=1)
            ed = today

    # ─── Excel: titoli e filtri ─────────────────────────────────────
    st.markdown(f"**Periodo Selezionato: {sd} – {ed}**\n")

    df = pd.read_sql("SELECT * FROM sales", engine, parse_dates=["order_date"])

    # --- CORRETTO: conversione a Timestamp per evitare errori di confronto
    left = pd.to_datetime(sd)
    right = pd.to_datetime(ed)
    filt = df[
        df["marketplace"].isin(sel) &
        df["order_date"].between(left, right)
    ]

    if filt.empty:
        st.warning("Nessun record")
        return

    # ─── KPI Excel ──────────────────────────────────────────────────
    n_ordini   = len(filt)
    sales_sum  = filt["sale"].sum()
    cost_sum   = filt["purchase_cost"].sum()
    comm_sum   = filt["commission"].sum()
    margin_l   = sales_sum - (cost_sum + comm_sum)
    c0,c1,c2,c3,c4 = st.columns(5)
    c0.metric("Ordini Excel",   n_ordini)
    c1.metric("Fatturato",     format_euro(sales_sum))
    c2.metric("Acquisto",      format_euro(cost_sum))
    c3.metric("Commissione",   format_euro(comm_sum))
    c4.metric("Margine Lordo", format_euro(margin_l))

    # ─── Trend giornaliero ─────────────────────────────────────────
    st.subheader("Trend giornaliero")
    trend = (
        filt.groupby([filt["order_date"].dt.date, "marketplace"])
            .agg(vendite=("sale","sum"))
            .unstack(fill_value=0)["vendite"]
    )
    st.line_chart(trend)

    # ─── Riepilogo marketplace ────────────────────────────────────
    st.subheader("Riepilogo marketplace")
    summary = filt.groupby("marketplace").agg(
        vendite=("sale","sum"),
        acquisto=("purchase_cost","sum"),
        commissione=("commission","sum")
    )
    summary["margine_lordo"] = (
        summary["vendite"] - (summary["acquisto"] + summary["commissione"])
    )
    summary = summary.reset_index()
    for col in ["vendite","acquisto","commissione","margine_lordo"]:
        summary[col] = summary[col].apply(format_euro)
    st.dataframe(summary, use_container_width=True)

    # ─── Prodotti più venduti ─────────────────────────────────────
    st.subheader("Prodotti più venduti")
    sel_mp = st.radio(
        "Marketplace",
        ["Tutti"] + markets,
        horizontal=True
    )
    df_top = filt if sel_mp == "Tutti" else filt[filt["marketplace"] == sel_mp]
    top_n = st.slider("Top N", 5, 50, 10)
    grp = ["sku"] + (["product_name"] if "product_name" in df_top else [])
    top = (
        df_top.groupby(grp)
              .agg(
                  qta=("quantity","sum"),
                  vendite=("sale","sum"),
                  acquisto=("purchase_cost","sum"),
                  commissione=("commission","sum")
              )
              .reset_index()
              .sort_values("qta", ascending=False)
              .head(top_n)
    )
    top["margine_lordo"] = top["vendite"] - (top["acquisto"] + top["commissione"])
    for col in ["vendite","acquisto","commissione","margine_lordo"]:
        top[col] = top[col].apply(format_euro)
    top.index += 1
    st.dataframe(top, use_container_width=True)

    st.download_button(
        "Scarica CSV Excel filtrato",
        filt.to_csv(index=False).encode("utf-8"),
        "excel_filtrato.csv",
        "text/csv"
    )

    # ───────────────── KPI API ───────────────────────────────────────
    st.markdown("---")
    st.subheader("Vendite Estratte da Worten")

    api_quick = st.radio(
        "Filtra ordini per",
        ["Oggi","Ieri","Ultimi 30 giorni","Questa Settimana","Mese Corrente","Questo Anno","Personalizzato"],
        horizontal=True
    )
    today = date.today()
    if api_quick == "Oggi":
        api_sd = api_ed = today
    elif api_quick == "Ieri":
        api_sd = api_ed = today - timedelta(days=1)
    elif api_quick == "Ultimi 30 giorni":
        api_sd, api_ed = today - timedelta(days=30), today
    elif api_quick == "Questa Settimana":
        api_sd = today - timedelta(days=today.weekday())
        api_ed = api_sd + timedelta(days=6)
    elif api_quick == "Mese Corrente":
        api_sd = today.replace(day=1)
        nm = api_sd.replace(day=28) + timedelta(days=4)
        api_ed = nm - timedelta(days=nm.day)
    elif api_quick == "Questo Anno":
        api_sd = date(today.year,1,1)
        api_ed = today
    else:  # "Personalizzato"
        api_sd, api_ed = st.date_input(
            "Seleziona intervallo personalizzato",
            value=(today - timedelta(days=7), today),
            min_value=date(today.year-1,1,1),
            max_value=today,
            key="api_custom_range"
        )

    st.markdown(f"**Intervallo API:** {api_sd} – {api_ed}")

    orders_df = get_orders_worten(api_sd, api_ed)

    for col in ["sale_price","taxes","commission","shipping"]:
        orders_df[col] = pd.to_numeric(orders_df.get(col,0), errors="coerce").fillna(0)
    total_sales  = orders_df["sale_price"].sum()
    total_taxes  = orders_df["taxes"].sum()
    total_comm   = orders_df["commission"].sum()
    total_margin = total_sales - total_taxes - total_comm

    k1,k2,k3,k4,k5 = st.columns(5)
    k1.metric("Ordini",      orders_df["order_id"].nunique())
    k2.metric("Vendite",     format_euro(total_sales))
    k3.metric("Tasse",       format_euro(total_taxes))
    k4.metric("Commissioni", format_euro(total_comm))
    k5.metric("Margine",     format_euro(total_margin))

    # ─── Ordini Ricevuti API ───────────────────────────────────────
    st.markdown("---")
    st.subheader("Ordini Ricevuti")
    df_orders = orders_df[[
        "order_id","order_date","sale_price","commission",
        "product_name","order_status"
    ]].rename(columns={
        "product_name":"Nome Prodotto",
        "order_status":"Stato Ordine"
    })

    styled = (
        df_orders.style
                 .format({"sale_price": format_euro, "commission": format_euro})
                 .applymap(
                     lambda v: "color: red"
                     if str(v).lower() in ("annullato","cancellato","rimborsato")
                     else "",
                     subset=["Stato Ordine"]
                 )
    )
    st.write(styled)

    # ─── LINEA DI DEMARCAZIONE ─────────────────────────────────────
    st.markdown("---")

    # ─── TOP 10 PRODOTTI VENDUTI (API) ──────────────────────────────
    st.subheader("Top 10 Prodotti Venduti nel Periodo")
    exploded = (
        orders_df
        .assign(product_name=orders_df["product_name"].str.split("; "))
        .explode("product_name")
    )
    top10 = (
        exploded
        .groupby("product_name")
        .agg(
            ordini     = ("order_id",   "count"),
            vendite    = ("sale_price", "sum"),
            commissioni= ("commission", "sum")
        )
        .sort_values("ordini", ascending=False)
        .head(10)
        .reset_index()
    )
    top10["vendite"]     = top10["vendite"].apply(format_euro)
    top10["commissioni"] = top10["commissioni"].apply(format_euro)
    st.dataframe(top10, use_container_width=True)

if __name__ == "__main__":
    main()
