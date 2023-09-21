import datetime
import pytest
from pydantic import ValidationError
from .probes import NotificationMethod, Probe


def test_basic_probe(session):
    p = Probe.parse_obj(
        {
            "name": "test_probe",
            "condition": "1=1",
            "probe_created_at": datetime.datetime.now(),
            "probe_modified_at": datetime.datetime.now(),
        }
    )

    assert not p.notify_writer
    assert not p.notify_other

    assert len(session._sql) == 1, "Expected 1 sql statement during validation"
    assert (
        _expected_condition_verification_sql(p) in session._sql[0].lower()
    ), "Unexpected probe condition query"


@pytest.mark.parametrize(
    "method", ["EMAIL", NotificationMethod.EMAIL, "SLACK", NotificationMethod.SLACK]
)
def test_probe_notify_writer(session, method):
    # Enum value or enum is allowed
    p = Probe.parse_obj(
        {
            "name": "test_probe",
            "condition": "1=1",
            "notify_writer": True,
            "notify_writer_method": method,
            "probe_created_at": datetime.datetime.now(),
            "probe_modified_at": datetime.datetime.now(),
        }
    )

    assert p.notify_writer
    assert not p.notify_other

    assert len(session._sql) == 1, "Expected 1 sql statement during validation"
    assert (
        _expected_condition_verification_sql(p) in session._sql[0].lower()
    ), "Unexpected probe condition query"


@pytest.mark.parametrize(
    "method", ["EMAIL", NotificationMethod.EMAIL, "SLACK", NotificationMethod.SLACK]
)
def test_probe_notify_other(session, method):
    # Enum value or enum is allowed
    others = "@josh,josh@sundeck.io"
    p = Probe.parse_obj(
        {
            "name": "test_probe",
            "condition": "1=1",
            "notify_other": others,
            "notify_other_method": method,
            "probe_created_at": datetime.datetime.now(),
            "probe_modified_at": datetime.datetime.now(),
        }
    )

    assert not p.notify_writer
    assert p.notify_other == others

    assert len(session._sql) == 1, "Expected 1 sql statement during validation"
    assert (
        _expected_condition_verification_sql(p) in session._sql[0].lower()
    ), "Unexpected probe condition query"


def test_cannot_notify_writer_without_method():
    with pytest.raises(ValidationError):
        _ = Probe.parse_obj(
            {
                "name": "probe1",
                "condition": "1=1",
                "notify_writer": True,
                "probe_created_at": datetime.datetime.now(),
                "probe_modified_at": datetime.datetime.now(),
            }
        )


def test_cannot_notify_others_without_method():
    with pytest.raises(ValidationError):
        _ = Probe.parse_obj(
            {
                "name": "probe1",
                "condition": "1=1",
                "notify_other": "@josh",
                "probe_created_at": datetime.datetime.now(),
                "probe_modified_at": datetime.datetime.now(),
            }
        )


def test_name_validation():
    with pytest.raises(ValidationError):
        _ = Probe.parse_obj(
            {
                "name": None,
                "condition": "1=1",
                "probe_created_at": datetime.datetime.now(),
                "probe_modified_at": datetime.datetime.now(),
            }
        )

    # Must have a non-empty name
    with pytest.raises(ValidationError):
        _ = Probe.parse_obj(
            {
                "name": "",
                "condition": "1=1",
                "probe_created_at": datetime.datetime.now(),
                "probe_modified_at": datetime.datetime.now(),
            }
        )


def test_condition_validation():
    with pytest.raises(ValidationError):
        _ = Probe.parse_obj(
            {
                "name": "probe1",
                "condition": None,
                "probe_created_at": datetime.datetime.now(),
                "probe_modified_at": datetime.datetime.now(),
            }
        )

    # Must have a non-empty condition
    with pytest.raises(ValidationError):
        _ = Probe.parse_obj(
            {
                "name": "probe1",
                "condition": "",
                "probe_created_at": datetime.datetime.now(),
                "probe_modified_at": datetime.datetime.now(),
            }
        )


def test_created_at_validation():
    with pytest.raises(ValidationError):
        _ = Probe.parse_obj(
            {
                "name": "probe1",
                "condition": "",
                "probe_modified_at": datetime.datetime.now(),
            }
        )


def test_modified_at_validation():
    with pytest.raises(ValidationError):
        _ = Probe.parse_obj(
            {
                "name": "probe1",
                "condition": "",
                "probe_created_at": datetime.datetime.now(),
            }
        )


def test_notification_enum():
    for method in ("EMAIL", "SLACK"):
        assert method == Probe.notification_method_is_valid(method)
        assert method == Probe.notification_method_is_valid(method.lower())

    with pytest.raises(AssertionError):
        Probe.notification_method_is_valid("teams")
    with pytest.raises(AssertionError):
        Probe.notification_method_is_valid("carrier_pidgeon")


def _expected_condition_verification_sql(p: Probe) -> str:
    return f"select case when {p.condition} then 1 else 0 end"
