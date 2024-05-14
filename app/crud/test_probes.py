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


@pytest.mark.parametrize("method", ["TEAMS", "Carrier Pidgeon"])
def test_unknown_notification_method(method):
    with pytest.raises(ValidationError):
        _ = Probe.parse_obj(
            {
                "name": "test",
                "condition": "TRUE",
                "notify_writer": True,
                "notify_writer_method": method,
                "probe_created_at": datetime.datetime.now(),
                "probe_modified_at": datetime.datetime.now(),
            }
        )


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
    with pytest.raises(ValidationError):
        _ = Probe.parse_obj(
            {
                "name": "This ain't allowed",
                "condition": "1=1",
                "probe_created_at": datetime.datetime.now(),
                "probe_modified_at": datetime.datetime.now(),
            }
        )

    # Probe names may also be empty
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


@pytest.mark.parametrize(
    "tc",
    [
        {
            "others": " josh@domain.io, josh2@domain.io, josh@domain.io ",
            "duplicates": ["josh@domain.io"],
        },
        # We should detect multiple duplicates
        {
            "others": " josh@domain.io, josh2@domain.io, josh3@domain.io, josh2@domain.io, josh3@domain.io",
            "duplicates": ["josh2@domain.io", "josh3@domain.io"],
        },
        # Email addresses are not case-sensitive
        {
            "others": " josh@domain.io, JOSH@domain.io, Josh@domain.io ",
            "duplicates": ["josh@domain.io"],
        },
    ],
)
def test_disallow_duplicate_email_addresses(tc):
    others = tc["others"]
    duplicates = tc["duplicates"]
    with pytest.raises(ValidationError) as ve:
        _ = Probe.parse_obj(
            {
                "name": "probe1",
                "condition": "1=2",
                "notify_writer": False,
                "notify_other_method": NotificationMethod.EMAIL,
                "notify_other": others,
                "probe_created_at": datetime.datetime.now(),
                "probe_modified_at": datetime.datetime.now(),
            }
        )
    for d in duplicates:
        assert d in str(
            ve.value
        ), "Expected to see the duplicated item in the exception's message"


@pytest.mark.parametrize(
    "tc",
    [
        {"others": "@Josh, @Josh2, @Josh ", "duplicates": ["@Josh"]},
        {"others": "#general, #engineering, #general ", "duplicates": ["#general"]},
        {
            "others": "@Josh2, josh@domain.io, josh@domain.io",
            "duplicates": ["josh@domain.io"],
        },
        # Should detect multiple duplicate elements
        {
            "others": "#general, @Vicky, @Josh, @Vicky, @Josh, #general",
            "duplicates": ["@Vicky", "@Josh", "#general"],
        },
    ],
)
def test_disallow_duplicate_slack_addrs(tc):
    """
    For slack, we should reject duplicate usernames, duplicate channels, and duplicate email addresses (that resolve to users)
    """
    others = tc["others"]
    duplicates = tc["duplicates"]
    with pytest.raises(ValidationError) as ve:
        _ = Probe.parse_obj(
            {
                "name": "probe1",
                "condition": "1=2",
                "notify_writer": False,
                "notify_other_method": NotificationMethod.SLACK,
                "notify_other": others,
                "probe_created_at": datetime.datetime.now(),
                "probe_modified_at": datetime.datetime.now(),
            }
        )
    for d in duplicates:
        assert d in str(
            ve.value
        ), "Expected to see the duplicated item in the exception's message"


def _expected_condition_verification_sql(p: Probe) -> str:
    return f"select {p.condition}"
