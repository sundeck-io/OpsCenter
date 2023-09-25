import datetime
from enum import Enum
from .base import handle_type, unwrap_value


class TestStrEnum(str, Enum):
    __test__ = False
    A = "a"
    B = "b"


class TestIntEnum(int, Enum):
    __test__ = False
    ONE = 1
    TWO = 2


def test_basic_types():
    assert "STRING" == handle_type(type("asdf"))
    assert "NUMBER" == handle_type(type(42))
    assert "NUMBER" == handle_type(type(42.0))
    assert "TIMESTAMP" == handle_type(type(datetime.datetime.now()))
    assert "BOOLEAN" == handle_type(type(True))


def test_handle_enums():
    assert "STRING" == handle_type(type(TestStrEnum.A))
    assert "NUMBER" == handle_type(type(TestIntEnum.TWO))


def test_unwrap_values():
    assert "a" == unwrap_value("a")
    assert 1 == unwrap_value(1)
    assert "a" == unwrap_value(TestStrEnum.A)
    assert 1 == unwrap_value(TestIntEnum.ONE)
