import streamlit as st
import requests
import pandas as pd
from datetime import date
from typing import List, Dict, Any
from .base import MarketplaceAPI

class WortenAPI(MarketplaceAPI):
    """
    Client sincrono Mirakl (OR11) per Wort​en.
    Restituisce un DataFrame con una riga per riga d'ordine, contenente:
      - order_id
      - order_date
      - order_status
      - sale_price
      - taxes
      - commission
      - shipping
      - sku
      - product_name
    """
    def __init__(self):
        self.base    = st.secrets["worten_api_base"]   # e.g. "https://marketplace.worten.pt/api/orders"
        self.shop    = st.secrets["worten_shop_id"]
        self.key     = st.secrets["worten_api_key"]
        self.headers = {
            "Authorization": self.key,
            "Accept":        "application/json",
        }

    def get_orders(self, start_date: date, end_date: date) -> pd.DataFrame:
        # 1) Paginazione OR11
        all_orders: List[Dict[str, Any]] = []
        offset, page_size = 0, 100
        while True:
            params = {
                "shop_id":    self.shop,
                "start_date": start_date.isoformat() + "T00:00:00Z",
                "end_date":   end_date.isoformat()   + "T23:59:59Z",
                "offset":     offset,
                "max":        page_size,
            }
            resp = requests.get(self.base, headers=self.headers, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            batch = payload.get("orders", []) or payload.get("data", [])
            if not batch:
                break
            all_orders.extend(batch)
            total = payload.get("total_count", len(batch))
            offset += len(batch)
            if offset >= total:
                break

        # 2) Se non ci sono ordini, ritorno DF vuoto con tutte le colonne
        if not all_orders:
            cols = [
                "order_id","order_date","order_status",
                "sale_price","taxes","commission","shipping",
                "sku","product_name"
            ]
            return pd.DataFrame(columns=cols)

        # 3) Costruisco manualmente il DataFrame (una riga per riga d'ordine)
        rows: List[Dict[str, Any]] = []
        for o in all_orders:
            oid = o.get("order_id")
            # data ordine
            dt = (
                o.get("creation_date") or o.get("creationDate")
                or o.get("dateCreated")   or o.get("date_created")
            )
            # stato ordine
            status = o.get("order_state") or o.get("order_status") or o.get("status") or ""
            # vendite totali (articoli)
            sale = o.get("total_price") or o.get("totalPrice") or o.get("price") or 0
            # tasse totali
            taxes = o.get("shipping_price") or o.get("shippingPrice") or o.get("tax_amount") or o.get("taxAmount") or 0
            # commissione totale
            comm_field = (
                o.get("total_commission")
                or o.get("commissionAmount")
                or o.get("commission_amount")
                or o.get("commission")
                or 0
            )
            if isinstance(comm_field, dict):
                comm = comm_field.get("amount", comm_field.get("value", 0))
            else:
                comm = comm_field or 0
            # shipping (spese di spedizione per l'ordine)
            ship = (
                o.get("shipping_price")
                or o.get("shippingPrice")
                or o.get("shipping_amount")
                or o.get("shippingAmount")
                or 0
            )
            # righe d'ordine
            lines = o.get("order_lines", []) or o.get("items", [])
            for line in lines:
                # sku e nome prodotto per riga
                sku = line.get("offer_sku") or line.get("product_sku") or line.get("sku") or ""
                name = line.get("product_title") or line.get("product_name") or ""
                rows.append({
                    "order_id":     oid,
                    "order_date":   dt,
                    "order_status": status,
                    "sale_price":   float(sale),
                    "taxes":        float(taxes),
                    "commission":   float(comm),
                    "shipping":     float(ship),
                    "sku":          sku,
                    "product_name": name,
                })

        # 4) Creo il DataFrame e normalizzo tipi e date
        df = pd.DataFrame(rows)
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        for c in ["sale_price", "taxes", "commission", "shipping"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        return df
