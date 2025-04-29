from .base import MarketplaceAPI
from .worten import WortenAPI
from .leroymerlin import LeroyMerlinAPI

__all__ = ["MarketplaceAPI", "WortenAPI", "LeroyMerlinAPI", "get_api"]

# Dizionario delle API disponibili
APIS = {
    "Worten": WortenAPI,
    "Leroy Merlin": LeroyMerlinAPI,
}

def get_api(name: str) -> MarketplaceAPI:
    """
    Restituisce l'istanza del client corrispondente al nome del marketplace.
    Solleva ValueError se il nome non è presente.
    """
    name = name.strip()
    if name not in APIS:
        raise ValueError(f"API non disponibile per il marketplace: '{name}'")
    return APIS[name]()
