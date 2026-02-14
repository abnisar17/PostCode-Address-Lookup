import pytest

from app.core.utils.postcode import (
    normalise_postcode,
    postcode_no_space,
    validate_postcode,
)


class TestNormalisePostcode:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("SW1A 1AA", "SW1A 1AA"),
            ("sw1a1aa", "SW1A 1AA"),
            ("sw1a  1aa", "SW1A 1AA"),
            ("  SW1A 1AA  ", "SW1A 1AA"),
            ("EC1A 1BB", "EC1A 1BB"),
            ("W1A 0AX", "W1A 0AX"),
            ("M1 1AE", "M1 1AE"),
            ("B33 8TH", "B33 8TH"),
            ("CR2 6XH", "CR2 6XH"),
            ("DN55 1PT", "DN55 1PT"),
        ],
    )
    def test_valid_postcodes(self, raw: str, expected: str):
        assert normalise_postcode(raw) == expected

    @pytest.mark.parametrize(
        "raw",
        [
            "",
            "   ",
            "INVALID",
            "12345",
            "AAA 9AA",
            "A 9AA",
        ],
    )
    def test_invalid_postcodes(self, raw: str):
        assert normalise_postcode(raw) is None


class TestValidatePostcode:
    def test_valid(self):
        assert validate_postcode("SW1A 1AA") is True

    def test_invalid(self):
        assert validate_postcode("NOPE") is False


class TestPostcodeNoSpace:
    def test_removes_space(self):
        assert postcode_no_space("SW1A 1AA") == "SW1A1AA"

    def test_no_space_input(self):
        assert postcode_no_space("M11AE") == "M11AE"
