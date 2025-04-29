import streamlit as st
import requests
import pandas as pd
from datetime import date
from typing import List
from .base import MarketplaceAPI

class LeroyMerlinAPI(MarketplaceAPI):
    def __init__(self):
        self.base = st.secrets["leroy_base_url"]       # Corretto rispetto ai tuoi secrets
        self.shop = st.secrets["leroy_shop_id"]
        self.key = st.secrets["leroy_api_key"]          # Bearer token API

        self.headers = {
            "Authorization": f"Bearer {self.key}",
            "Accept": "application/json",
        }

    def get_orders(self, start_date: date, end_date: date) -> pd.DataFrame:
        url = f"{self.base}/v1/orders"
        params = {
            "start_creation_date": start_date.isoformat() + "T00:00:00Z",
            "end_creation_date": end_date.isoformat() + "T23:59:59Z",
            "max": 100,
        }

        all_orders: List[dict] = []
        while True:
            resp = requests.get(url, headers=self.headers, params=params, timeout=30)
            resp.raise_for_status()
            page = resp.json()
            data = page.get("orders", []) or page.get("data", [])
            if not data:
                break
            all_orders.extend(data)

            next_page_token = page.get("next")
            if not next_page_token:
                break
            params["page_token"] = next_page_token

        if not all_orders:
            return pd.DataFrame(columns=[
                "order_id", "order_date", "sale_price", "taxes",
                "commission", "shipping", "sku", "product_name", "order_status"
            ])

        rows: List[dict] = []
        for o in all_orders:
            dt = (
                o.get("creation_date")
                or o.get("creationDate")
                or o.get("dateCreated")
                or o.get("date_created")
            )
            comm = o.get("commission_total_amount") or o.get("commission_amount") or 0
            total_price = o.get("total_price") or o.get("totalPrice") or 0
            shipping = o.get("shipping_price") or o.get("shippingPrice") or 0

            for l in o.get("order_lines", []) or o.get("items", []):
                rows.append({
                    "order_id": o.get("order_id"),
                    "order_date": dt,
                    "sale_price": float(total_price),
                    "taxes": float(o.get("tax_amount", 0)),
                    "commission": float(comm),
                    "shipping": float(shipping),
                    "sku": l.get("offer_sku") or l.get("product_sku") or l.get("sku", ""),
                    "product_name": l.get("product_name") or l.get("product_title", ""),
                    "order_status": o.get("order_status", ""),
                })

        df = pd.DataFrame(rows)
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        return df
