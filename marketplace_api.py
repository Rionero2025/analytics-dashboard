import requests
import pandas as pd
from datetime import date
from typing import List

def get_orders_worten(
    start: date,
    end: date,
    api_key: str = "0635f400-65e8-4c23-8335-03146399c3a0",
    shop_id: int = 13810,
    page_size: int = 100
) -> pd.DataFrame:
    """
    Recupera gli ordini da Mirakl (Worten) nell'intervallo [start, end],
    estrae order_id, order_date, sale_price, commission, taxes, shipping,
    product_name e sku (dalle righe d'ordine).
    """

    base_url = "https://marketplace.worten.pt/api/orders"
    headers = {"Authorization": api_key}
    all_orders: List[dict] = []
    offset = 0

    # Paginazione
    while True:
        params = {
            "start_date": start.isoformat() + "T00:00:00Z",
            "end_date":   end.isoformat()   + "T23:59:59Z",
            "shop_id":    shop_id,
            "offset":     offset,
            "max":        page_size
        }
        resp = requests.get(base_url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("orders", [])
        if not batch:
            break
        all_orders.extend(batch)
        offset += len(batch)
        if offset >= data.get("total_count", 0):
            break

    if not all_orders:
        return pd.DataFrame()

    # Normalizzo l'array principale
    df = pd.json_normalize(all_orders)

    # 1) order_date
    date_cols = [c for c in df.columns if "date" in c.lower()]
    df["order_date"] = pd.to_datetime(df[date_cols[0]]).dt.date

    # 2) sale_price
    price_cols = [c for c in df.columns if c.lower() in ("total_price","sale_price","amount","price")]
    df = df.rename(columns={price_cols[0]: "sale_price"})

    # 3) commission
    comm_cols = [c for c in df.columns if "commission" in c.lower()]
    df = df.rename(columns={comm_cols[0]: "commission"})

    # 4) taxes
    tax_cols = [c for c in df.columns if "tax" in c.lower()]
    if tax_cols:
        df = df.rename(columns={tax_cols[0]: "taxes"})
    else:
        df["taxes"] = 0.0

    # 5) shipping
    ship_cols = [c for c in df.columns if "ship" in c.lower()]
    if ship_cols:
        df = df.rename(columns={ship_cols[0]: "shipping"})
    else:
        df["shipping"] = 0.0

    # 6) order_id
    if "order_id" not in df.columns:
        raise KeyError("order_id non trovato nella risposta API")

    # 7) product_name
    lines = pd.json_normalize(
        all_orders,
        record_path=["order_lines"],
        meta=["order_id"],
        errors="ignore"
    )
    if "product_title" in lines.columns:
        prod_map = (
            lines
            .groupby("order_id")["product_title"]
            .agg(lambda lst: "; ".join(lst))
            .to_dict()
        )
        df["product_name"] = df["order_id"].map(prod_map)
    else:
        df["product_name"] = ""

    # 8) SKU/EAN
    # cerco una colonna che sembri SKU/EAN all'interno di order_lines
    sku_col = None
    for c in lines.columns:
        if c.lower().endswith(("sku","ean")):
            sku_col = c
            break
    if sku_col:
        sku_map = (
            lines
            .groupby("order_id")[sku_col]
            .agg(lambda lst: "; ".join(lst))
            .to_dict()
        )
        df["sku"] = df["order_id"].map(sku_map)
    else:
        df["sku"] = ""

    # restituisco solo le colonne necessarie
    return df[[
        "order_id", "order_date", "sku", "product_name",
        "sale_price", "taxes", "commission", "shipping"
    ]]
