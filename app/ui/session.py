import datetime
from threading import Lock
from enum import Enum


class ReportSession:
    report_start: datetime
    report_end: datetime
    warehouse_filter: []

    def __init__(self):
        self.report_start = datetime.datetime.now() - datetime.timedelta(days=30)
        self.report_end = datetime.datetime.now()
        self.warehouse_filter = None

    def set_date_range(self, start: datetime, end: datetime):
        self.report_start = start
        self.report_end = end

    def set_date_range_days(self, num):
        self.set_date_range(
            datetime.datetime.now() - datetime.timedelta(days=num),
            datetime.datetime.now(),
        )

    def get_report_start(self):
        return self.report_start

    def get_report_end(self):
        return self.report_end

    def get_warehouse_filter(self):
        return self.warehouse_filter

    def set_warehouse_filter(self, f):
        if f is None:
            raise ValueError("Warehouse filter cannot be None")

        self.warehouse_filter = f


class Mode(Enum):
    LIST = "list"
    CREATE = "create"
    EDIT = "edit"


class Session:
    toast_lock = Lock()

    update: dict
    create: dict

    mode: Mode
    initialized: bool
    report_session: ReportSession

    def __init__(self):
        self.toast = None
        self.mode = Mode.LIST
        self.initialized = False
        self.report_session = ReportSession()

    def do_edit(self, update: dict):
        self.update = update
        self.mode = Mode.EDIT

    def do_list(self):
        self.mode = Mode.LIST

    def do_create(self, create: dict):
        self.create = create
        self.mode = Mode.CREATE

    def set_toast(self, message: str):
        self.toast = message

    def show_toast(self, status):
        if self.toast is not None:
            # For some reason this gets executed twice in quick succession on edit.
            with Session.toast_lock:
                if self.toast is not None:
                    status.success(self.toast)
                    self.toast = None
                    # TODO: figure out how to hide toast after a few seconds (this probably requires a custom component)

    def get_report(self):
        return self.report_session


class Sessions:
    lock = Lock()
    sessions = {}

    @classmethod
    def get(cls, name: str) -> Session:
        if cls.sessions.get(name) is None:
            with cls.lock:
                if cls.sessions.get(name) is None:
                    cls.sessions[name] = Session()
        return cls.sessions[name]


def labels():
    return Sessions.get("label")


def probes():
    return Sessions.get("probe")


def reports():
    return Sessions.get("report")
