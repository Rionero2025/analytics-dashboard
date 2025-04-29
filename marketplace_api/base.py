from abc import ABC, abstractmethod
from datetime import date
import pandas as pd

class MarketplaceAPI(ABC):
    """
    Interfaccia base per i client Marketplace.
    Ogni implementazione deve fornire get_orders().
    """

    @abstractmethod
    def get_orders(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Ritorna un DataFrame con almeno le colonne:
          - order_id
          - order_date
          - sale_price
          - taxes
          - commission
          - shipping
          - product_name
          - order_status
        """
        raise NotImplementedError
