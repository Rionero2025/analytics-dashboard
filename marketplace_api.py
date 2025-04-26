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
    Recupera paginando via GET /api/orders:
      - order_id, order_date
      - sale_price, taxes, commission, shipping
      - order_status (order_state_label o order_state)
      - product_name (concat di tutti i productTitle nelle righe)
    """
    base_url = "https://marketplace.worten.pt/api/orders"
    headers = {"Authorization": api_key}
    all_orders: List[dict] = []
    offset = 0

    # 1) pagino tutti gli ordini
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
        batch = resp.json().get("orders", [])
        if not batch:
            break
        all_orders.extend(batch)
        offset += len(batch)
        if offset >= resp.json().get("total_count", 0):
            break

    if not all_orders:
        return pd.DataFrame()

    # 2) normalizzo l’array principale
    df = pd.json_normalize(all_orders, sep="_")

    # campi chiave
    # 2.a) order_id
    id_key = next((k for k in all_orders[0].keys() 
                   if k.lower() in ("code","order_id","id")), None)
    df["order_id"] = df[id_key]

    # 2.b) order_date
    if "creationDate" in df.columns:
        df["order_date"] = pd.to_datetime(df["creationDate"]).dt.date
    else:
        dcol = next(c for c in df.columns if "date" in c.lower())
        df["order_date"] = pd.to_datetime(df[dcol]).dt.date

    # 2.c) sale_price
    df["sale_price"] = pd.to_numeric(df.get("price",0), errors="coerce").fillna(0.0)

    # 2.d) taxes
    if {"shippingTaxAmount","taxAmount"}.issubset(df.columns):
        df["taxes"] = df["shippingTaxAmount"].fillna(0) + df["taxAmount"].fillna(0)
    else:
        tcol = next((c for c in df.columns if "tax" in c.lower()), None)
        df["taxes"] = pd.to_numeric(df.get(tcol,0), errors="coerce").fillna(0.0)

    # 2.e) commission
    ccol = next((c for c in df.columns if "commission" in c.lower()), None)
    df["commission"] = pd.to_numeric(df.get(ccol,0), errors="coerce").fillna(0.0)

    # 2.f) shipping
    scol = next((c for c in df.columns 
                 if "ship" in c.lower() and "tax" not in c.lower()), None)
    df["shipping"] = pd.to_numeric(df.get(scol,0), errors="coerce").fillna(0.0)

    # 2.g) order_status
    lbl = next((c for c in df.columns if "status_label" in c.lower() or "state_label" in c.lower()), None)
    if lbl:
        df["order_status"] = df[lbl].astype(str)
    else:
        fb = next((c for c in df.columns if "status" in c.lower() or "state" in c.lower()), None)
        df["order_status"] = df[fb].astype(str) if fb else ""

    # 3) estraggo product_name dalle righe ordine
    #   riconosco automaticamente orderLines vs order_lines
    if all("orderLines" in o for o in all_orders):
        lines_key = "orderLines"
    elif all("order_lines" in o for o in all_orders):
        lines_key = "order_lines"
    else:
        lines_key = None

    if lines_key:
        lines = pd.json_normalize(
            all_orders,
            record_path=[lines_key],
            meta=[id_key],
            sep="_"
        )
        if "productTitle" in lines.columns:
            prod_map = (
                lines
                .groupby(id_key)["productTitle"]
                .agg(lambda items: "; ".join(items))
                .to_dict()
            )
            df["product_name"] = df[id_key].map(prod_map)
        else:
            df["product_name"] = ""
    else:
        df["product_name"] = ""

    # 4) ritorno colonne ordinate
    return df[[
        "order_id",
        "order_date",
        "sale_price",
        "taxes",
        "commission",
        "shipping",
        "order_status",
        "product_name"
    ]]
