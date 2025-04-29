from .base import MarketplaceAPI
from .worten import WortenAPI
from .leroymerlin import LeroyMerlinAPI

# chiavi “canoniche”
APIS = {
    "worten": WortenAPI,
    "leroymerlin": LeroyMerlinAPI,
}

def get_api(name: str) -> MarketplaceAPI:
    """
    Restituisce l'istanza del client corrispondente al nome del marketplace.
    Case- and space-insensitive lookup.
    """
    key = name.strip().lower().replace(" ", "")
    if key not in APIS:
        raise ValueError(f"Nessun client API per marketplace '{name}'")
    return APIS[key]()
