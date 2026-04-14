from fastapi import HTTPException
import pytest

from api.routes.dashboard import _validate_pair_scoped_patch


def test_global_pair_param_patch_is_blocked() -> None:
    with pytest.raises(HTTPException) as exc:
        _validate_pair_scoped_patch({"spacing_bps": 35}, None)
    assert exc.value.status_code == 400
    assert "Pair-specific parameters require product_id" in str(exc.value.detail)


def test_pair_param_patch_with_product_id_is_allowed() -> None:
    _validate_pair_scoped_patch({"spacing_bps": 35, "grid_levels": 3}, "SOL-USD")


def test_non_pair_param_patch_without_product_id_is_allowed() -> None:
    _validate_pair_scoped_patch({"local_timezone_iana": "Europe/Paris"}, None)
