# marketplace_api/__init__.py

from .base import MarketplaceAPI
from .worten import WortenAPI
from .leroymerlin import LeroyMerlinAPI

__all__ = ["MarketplaceAPI", "WortenAPI", "LeroyMerlinAPI", "get_api"]

APIS = {
    "Worten":       WortenAPI,
    "Leroy Merlin": LeroyMerlinAPI,
}

def get_api(name: str) -> MarketplaceAPI:
    """
    Restituisce l'istanza del client corrispondente al nome
    (ad es. "Worten" o "Leroy Merlin").
    """
    try:
        return APIS[name]()
    except KeyError:
        raise ValueError(f"Nessun client API per marketplace '{name}'")
