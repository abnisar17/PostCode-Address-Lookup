from pyproj import Transformer

# Single reused Transformer instance: OSGB36 (EPSG:27700) → WGS84 (EPSG:4326)
_transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)


def osgb36_to_wgs84(easting: float, northing: float) -> tuple[float, float]:
    """Convert OSGB36 easting/northing to WGS84 longitude/latitude.

    Returns:
        (latitude, longitude) tuple.
    """
    lon, lat = _transformer.transform(easting, northing)
    return lat, lon
