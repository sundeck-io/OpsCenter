from . import Label

def test_label():
    l = Label(**{
        "name": "label1",
        'condition': "user_name = 'josh@sundeck.io'",
    })