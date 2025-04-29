import streamlit as st
import requests
import pandas as pd
from datetime import date
from typing import List
from .base import MarketplaceAPI

class LeroyMerlinAPI(MarketplaceAPI):
    def __init__(self):
        self.base    = st.secrets["leroy_base_url"]
        self.shop    = st.secrets["leroy_shop_id"]
        self.key     = st.secrets["leroy_api_key"]
        self.headers = {
            "Authorization": f"Basic {self.key}",
            "Accept":        "application/json",
        }

    def get_orders(self, start_date: date, end_date: date) -> pd.DataFrame:
        url = f"{self.base}/v2/orders"
        params = {
            "from_date": start_date.isoformat() + "T00:00:00Z",
            "to_date":   end_date.isoformat()   + "T23:59:59Z",
            "limit":     100,
        }

        all_orders: List[dict] = []
        while True:
            resp = requests.get(url, headers=self.headers, params=params, timeout=30)
            resp.raise_for_status()
            page = resp.json()
            data = page.get("data", [])
            if not data:
                break
            all_orders.extend(data)

            token = page.get("next_page_token")
            if not token:
                break
            params = {"page_token": token, "limit": 100}

        # se non ho ordini, ritorno DF vuoto con colonne standard
        if not all_orders:
            return pd.DataFrame(columns=[
                "order_id","order_date","sale_price","taxes",
                "commission","shipping","sku","product_name","order_status"
            ])

        rows: List[dict] = []
        for o in all_orders:
            # estraggo la data in modo robusto
            dt = (
                o.get("creation_date")
                or o.get("creationDate")
                or o.get("dateCreated")
                or o.get("date_created")
            )
            # qui prendo la commissione **a livello di ordine**
            comm = o.get("commissionAmount") or o.get("commission_amount") or 0
            for l in o.get("order_lines", []) or o.get("items", []):
                rows.append({
                    "order_id":     o.get("order_id"),
                    "order_date":   dt,
                    "sale_price":   float(o.get("total_price",    0)),
                    "taxes":        float(o.get("tax_amount",      0)),
                    "commission":   float(comm),
                    "shipping":     float(o.get("shipping_amount",  0)),
                    "sku":          l.get("offer_sku") or l.get("product_sku") or l.get("sku", ""),
                    "product_name": l.get("product_name") or l.get("product_title", ""),
                    "order_status": o.get("order_status", ""),
                })

        df = pd.DataFrame(rows)
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        return df
