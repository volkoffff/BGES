"""City geocoding via Nominatim with a JSON file cache to avoid redundant API calls."""

import json
import logging
from pathlib import Path

from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim

logger = logging.getLogger(__name__)


def _load_cache(path: Path) -> dict[str, list[float] | None]:
    """Load city coordinates from the JSON cache file; return {} if absent."""
    if path.exists():
        raw: dict[str, list[float] | None] = json.loads(
            path.read_text(encoding="utf-8")
        )
        return raw
    return {}


def _save_cache(path: Path, cache: dict[str, list[float] | None]) -> None:
    """Persist the coordinates cache to a JSON file."""
    path.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def geocode_cities(
    city_names: list[str], cache_path: Path
) -> dict[str, tuple[float, float] | None]:
    """Return (lat, lon) for each city name, hitting Nominatim only for cache misses.

    Results are written back to *cache_path* after each new lookup so the file
    acts as a permanent store across runs.  Duplicate names in *city_names* are
    deduplicated before fetching to avoid redundant API calls.
    """
    cache = _load_cache(cache_path)

    # Preserve order while deduplicating so we only call Nominatim once per city.
    to_fetch = list(dict.fromkeys(c for c in city_names if c not in cache))

    if to_fetch:
        geolocator = Nominatim(user_agent="nf26-bges")
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
        for city in to_fetch:
            loc = geocode(city)
            cache[city] = [loc.latitude, loc.longitude] if loc else None
            logger.info("geocoded %s → %s", city, cache[city])
        _save_cache(cache_path, cache)

    result: dict[str, tuple[float, float] | None] = {}
    for city in city_names:
        raw = cache.get(city)
        result[city] = (raw[0], raw[1]) if raw else None
    return result
