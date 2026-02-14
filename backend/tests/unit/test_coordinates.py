import pytest

from app.core.utils.coordinates import osgb36_to_wgs84


class TestOsgb36ToWgs84:
    def test_trafalgar_square(self):
        # Trafalgar Square: easting 530034, northing 180381
        # Expected WGS84: ~51.508, ~-0.128
        lat, lon = osgb36_to_wgs84(530034, 180381)
        assert lat == pytest.approx(51.508, abs=0.01)
        assert lon == pytest.approx(-0.128, abs=0.01)

    def test_edinburgh_castle(self):
        # Edinburgh Castle: easting 325178, northing 673512
        # Expected WGS84: ~55.949, ~-3.201
        lat, lon = osgb36_to_wgs84(325178, 673512)
        assert lat == pytest.approx(55.949, abs=0.01)
        assert lon == pytest.approx(-3.201, abs=0.01)

    def test_big_ben(self):
        # Big Ben / Palace of Westminster: easting 530268, northing 179545
        # Expected WGS84: ~51.501, ~-0.125
        lat, lon = osgb36_to_wgs84(530268, 179545)
        assert lat == pytest.approx(51.501, abs=0.01)
        assert lon == pytest.approx(-0.125, abs=0.01)

    def test_returns_float_tuple(self):
        lat, lon = osgb36_to_wgs84(530034, 180381)
        assert isinstance(lat, float)
        assert isinstance(lon, float)
