import streamlit as st
from crud.base import BaseOpsCenterModel
from typing import List
import datetime
import string
import random


class Actions:
    def __init__(self, edit_callback, delete_callback, create_callback):
        self.edit_callback = edit_callback
        self.delete_callback = delete_callback
        self.create_callback = create_callback

    def init(self, row, empty=False):
        buttons = st.columns(3)
        if self.edit_callback and not empty:
            buttons[0].button(
                "✏️",
                key=f"{get_random_string(10)}",
                on_click=self.edit_callback,
                args=[row],
            )
        if self.delete_callback and not empty:
            buttons[1].button(
                "🗑️",
                key=f"{get_random_string(10)}",
                on_click=self.delete_callback,
                args=[row],
            )
        if self.create_callback:
            buttons[2].button(
                "➕",
                key=f"{get_random_string(10)}",
                on_click=self.create_callback,
                args=[row],
            )


def build_table(
    cls: BaseOpsCenterModel,
    data: List[BaseOpsCenterModel],
    button_callbacks: Actions,
    has_empty=False,
):
    cols = [v[1] for _, v in cls.col_widths.items()]
    if button_callbacks:
        cols.append(1)
    header = st.columns(cols)
    i = 0
    for name in cls.model_fields.keys():
        if name not in cls.col_widths:
            continue
        header[i].text(cls.col_widths[name][0])
        i += 1
    if button_callbacks:
        header[-1].text("Actions")

    for row in data:
        columns = st.columns(cols)

        i = 0
        for name in cls.model_fields.keys():
            if name not in cls.col_widths:
                continue
            obj = getattr(row, name)
            transform = (
                cls.col_widths[name][2]
                if len(cls.col_widths[name]) > 2
                else (lambda x: x)
            )
            this_type = type(obj)
            _COLS[this_type](columns[i], transform(obj))
            i += 1

        if button_callbacks:
            with columns[-1]:
                button_callbacks.init(row)
    if has_empty:
        columns = st.columns(cols)
        for i in range(len(columns) - 1):
            columns[i].text("")
        if button_callbacks:
            with columns[-1]:
                button_callbacks.init(None, empty=True)


def write_if(column, value):
    if value is not None:
        column.write(str(value))


def checkbox(column, value):
    column.checkbox(
        label="",
        label_visibility="hidden",
        value=value,
        disabled=True,
        key=get_random_string(10),
    )


def write_dt(column, value):
    column.write(value.strftime("%I:%M %p"))


_COLS = {
    str: write_if,
    bool: checkbox,
    datetime.time: write_dt,
    int: write_if,
    type(None): write_if,
}


def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for _ in range(length))
    return result_str
