"""Geodesic distance helper suitable for use as a PySpark UDF."""

from geopy.distance import geodesic as _geodesic


def geodesic_km(
    lat1: float | None,
    lon1: float | None,
    lat2: float | None,
    lon2: float | None,
) -> float | None:
    """Return the geodesic distance in kilometres between two (lat, lon) points.

    Returns *None* when any coordinate is missing so the UDF propagates nulls
    cleanly without raising an exception at the worker level.

    Args:
        lat1: Latitude of the origin point in decimal degrees.
        lon1: Longitude of the origin point in decimal degrees.
        lat2: Latitude of the destination point in decimal degrees.
        lon2: Longitude of the destination point in decimal degrees.

    Returns:
        Geodesic distance in kilometres, or None if any coordinate is null.
    """
    if any(v is None for v in (lat1, lon1, lat2, lon2)):
        return None
    return float(_geodesic((lat1, lon1), (lat2, lon2)).km)
