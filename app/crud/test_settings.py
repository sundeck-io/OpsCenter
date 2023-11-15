import pytest
from pydantic import ValidationError
from .settings import Setting


def test_none_attrs():
    with pytest.raises(ValidationError):
        _ = Setting(
            key=None,
            value="asdf",
        )
    with pytest.raises(ValidationError):
        _ = Setting(
            key="compute_credit_cost",
            value=None,
        )


def test_unknown_setting():
    with pytest.raises(ValidationError):
        _ = Setting(
            key="some_other_key",
            value="asdf",
        )


def test_timezone_setting():
    _ = Setting(
        key="default_timezone",
        value="America/Los_Angeles",
    )

    with pytest.raises(ValidationError):
        _ = Setting(
            key="default_timezone",
            value="America/DoesNotExist",
        )
    with pytest.raises(ValidationError):
        _ = Setting(
            key="default_timezone",
            value=1.0,
        )


def test_storage_cost():
    _ = Setting(
        key="storage_cost",
        value=40.0,
    )

    with pytest.raises(ValidationError):
        _ = Setting(key="storage_cost", value=-1.0)
    with pytest.raises(ValidationError):
        _ = Setting(key="storage_cost", value=0)


def test_compute_credit_cost():
    _ = Setting(
        key="compute_credit_cost",
        value=2.0,
    )

    with pytest.raises(ValidationError):
        _ = Setting(key="compute_credit_cost", value=-1.0)
    with pytest.raises(ValidationError):
        _ = Setting(key="compute_credit_cost", value=0)
    with pytest.raises(ValidationError):
        _ = Setting(
            key="compute_credit_cost",
            value="asdf",
        )


def test_serverless_credit_cost():
    _ = Setting(
        key="serverless_credit_cost",
        value=3.0,
    )

    with pytest.raises(ValidationError):
        _ = Setting(key="serverless_credit_cost", value=-1.0)
    with pytest.raises(ValidationError):
        _ = Setting(key="serverless_credit_cost", value=0)
    with pytest.raises(ValidationError):
        _ = Setting(
            key="serverless_credit_cost",
            value="asdf",
        )
