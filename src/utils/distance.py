from geopy.distance import geodesic as _geodesic


def geodesic_km(
    lat1: float | None,
    lon1: float | None,
    lat2: float | None,
    lon2: float | None,
) -> float | None:
    """Calculer la distance géodésique en km entre deux points (lat, lon)."""
    if any(v is None for v in (lat1, lon1, lat2, lon2)):
        return None
    return float(_geodesic((lat1, lon1), (lat2, lon2)).km)
