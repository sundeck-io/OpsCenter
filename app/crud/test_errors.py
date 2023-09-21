import datetime
import pytest
from pydantic import ValidationError
from .errors import summarize_error
from .probes import Probe


def test_null_name():
    with pytest.raises(ValidationError) as ve:
        _ = Probe(
            name=None,
            condition="1=1",
            probe_created_at=datetime.datetime.now(),
            probe_modified_at=datetime.datetime.now(),
        )

    assert "name cannot be null".lower() in summarize_error("ohnoes", ve.value).lower()


def test_null_condition():
    with pytest.raises(ValidationError) as ve:
        _ = Probe(
            name="no nulls here",
            condition=None,
            probe_created_at=datetime.datetime.now(),
            probe_modified_at=datetime.datetime.now(),
        )

    assert (
        "condition cannot be null".lower()
        in summarize_error("ohnoes", ve.value).lower()
    )
