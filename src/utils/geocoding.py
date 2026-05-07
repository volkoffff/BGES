import json
import logging
from pathlib import Path

from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim

logger = logging.getLogger(__name__)


def _load_cache(path: Path) -> dict[str, list[float] | None]:
    """Charger le cache JSON des coordonnées géographiques."""
    if path.exists():
        raw: dict[str, list[float] | None] = json.loads(
            path.read_text(encoding="utf-8")
        )
        return raw
    return {}


def _save_cache(path: Path, cache: dict[str, list[float] | None]) -> None:
    """Sauvegarder le cache JSON des coordonnées géographiques."""
    path.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def geocode_cities(
    city_names: list[str], cache_path: Path
) -> dict[str, tuple[float, float] | None]:
    """Retourner (lat, lon) pour chaque ville, en mettant à jour le cache JSON."""
    cache = _load_cache(cache_path)
    to_fetch = [c for c in city_names if c not in cache]
    if to_fetch:
        geolocator = Nominatim(user_agent="nf26-bges")
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
        for city in to_fetch:
            loc = geocode(city)
            cache[city] = [loc.latitude, loc.longitude] if loc else None
            logger.info("géocodé %s → %s", city, cache[city])
        _save_cache(cache_path, cache)
    result: dict[str, tuple[float, float] | None] = {}
    for city in city_names:
        raw = cache.get(city)
        result[city] = (raw[0], raw[1]) if raw else None
    return result
