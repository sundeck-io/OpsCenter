from . import Label
from datetime import datetime

def test_label():
    _ = Label(
        name="label1",
        condition="user_name = 'josh@sundeck.io'",
        modified_at=datetime.now(),
        created_at=datetime.now(),
        enabled=True,
        is_dynamic=False,
    )