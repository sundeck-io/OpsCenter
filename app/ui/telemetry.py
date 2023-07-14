import connection
import config


def action(act: str):
    """
    Called when a given action is performed, e.g. create a label.
    """
    send_telemetry("action", act)


def page_view(page: str):
    """
    Called when a page in the application is loaded.
    """
    send_telemetry("page", page)


def send_telemetry(event: str, data: str):
    """
    Sends usage data to the telemetry external function.
    """
    if not config.is_telemetry_enabled():
        return

    # TODO can we make this asynchronous?
    success = True
    try:
        _ = connection.execute_select(
            "select internal.telemetry(%(event)s, %(data)s)",
            {"event": event, "data": data},
        )
    except Exception:
        success = False
        pass
    print(f"Telemetry: {event} {data}, {'success' if success else 'failed'}")
