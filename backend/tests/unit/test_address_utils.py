import pytest

from app.core.utils.address import normalise_city, normalise_street


class TestNormaliseStreet:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("HIGH STREET", "High Street"),
            ("high st", "High Street"),
            ("CHURCH RD", "Church Road"),
            ("VICTORIA AVE", "Victoria Avenue"),
            ("park ln", "Park Lane"),
            ("QUEENS DR", "Queens Drive"),
            ("the  green", "The Green"),
        ],
    )
    def test_normalises_with_abbreviations(self, raw: str, expected: str):
        assert normalise_street(raw) == expected

    @pytest.mark.parametrize("raw", [None, "", "   "])
    def test_empty_input(self, raw: str | None):
        assert normalise_street(raw) is None


class TestNormaliseCity:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("london", "London"),
            ("MANCHESTER", "Manchester"),
            ("  birmingham  ", "Birmingham"),
            ("STOKE-ON-TRENT", "Stoke-On-Trent"),
            ("newcastle  upon  tyne", "Newcastle Upon Tyne"),
        ],
    )
    def test_normalises(self, raw: str, expected: str):
        assert normalise_city(raw) == expected

    @pytest.mark.parametrize("raw", [None, "", "  "])
    def test_empty_input(self, raw: str | None):
        assert normalise_city(raw) is None
