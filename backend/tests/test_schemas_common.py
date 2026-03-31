import pytest
from pydantic import ValidationError
from schemas.common import PaginationParams, ErrorResponse, validate_request_data


def test_pagination_defaults():
    p = PaginationParams()
    assert p.page == 1
    assert p.page_size == 100


def test_pagination_clamps():
    p = PaginationParams(page=0, page_size=9999)
    assert p.page == 1
    assert p.page_size == 500


def test_pagination_valid():
    p = PaginationParams(page=3, page_size=50)
    assert p.page == 3
    assert p.page_size == 50


def test_error_response():
    e = ErrorResponse(error="something went wrong")
    assert e.error == "something went wrong"


def test_validate_request_data_valid():
    result = validate_request_data(PaginationParams, {"page": 2, "page_size": 50})
    assert result.page == 2


def test_validate_request_data_invalid():
    with pytest.raises(ValueError):
        validate_request_data(PaginationParams, "not a dict")
