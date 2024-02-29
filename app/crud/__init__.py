import snowflake.snowpark
from datetime import datetime, time
from uuid import uuid4

# Make the entrypoints for SQL procedures available
from .common import (  # noqa F401
    create_entity,
    create_table,
    update_entity,
    delete_entity,
)
from .labels import PredefinedLabel
from .session import snowpark_session
from .wh_sched import (
    update_existing_schedule,
    delete_warehouse_schedule,
    verify_and_clean,
    after_schedule_change,
    fetch_schedules_with_defaults,
    WarehouseSchedules,
    merge_new_schedule,
)  # noqa F401
from .account import (
    sundeck_signup_with_snowflake_sso,  # noqa F401
    get_api_gateway_url,  # noqa F401
    OPSCENTER_ROLE_ARN,  # noqa F401
)
from .base import transaction
from .errors import summarize_error
from .settings import Setting
from .tasks import Task


def validate_predefined_labels(sess: snowflake.snowpark.Session):
    with snowpark_session(sess) as txn:
        PredefinedLabel.validate_all(txn)


def create_label(session, name, grp, rank, condition, is_dynamic):
    return create_entity(
        session,
        "LABEL",
        {
            "name": name,
            "group_name": grp,
            "group_rank": rank,
            "condition": condition,
            "is_dynamic": is_dynamic,
            "label_created_at": datetime.now(),
            "label_modified_at": datetime.now(),
            "label_id": str(uuid4()),
        },
    )


def delete_label(session, name):
    return delete_entity(session, "LABEL", name)


def update_label(session, old_name, name, grp, rank, condition, is_dynamic):
    return update_entity(
        session,
        "LABEL",
        old_name,
        {
            "name": name,
            "group_name": grp,
            "group_rank": rank,
            "condition": condition,
            "is_dynamic": is_dynamic,
            "label_created_at": datetime.now(),
            "label_modified_at": datetime.now(),
            "label_id": str(uuid4()),
        },
    )


def create_probe(
    session,
    name,
    condition,
    notify_writer,
    notify_writer_method,
    notify_other,
    notify_other_method,
    cancel,
):
    return create_entity(
        session,
        "QUERY_MONITOR",
        {
            "name": name,
            "condition": condition,
            "notify_writer": notify_writer,
            "notify_writer_method": notify_writer_method,
            "notify_other": notify_other,
            "notify_other_method": notify_other_method,
            "cancel": cancel,
            "probe_created_at": datetime.now(),
            "probe_modified_at": datetime.now(),
        },
    )


def delete_probe(session, name):
    return delete_entity(session, "QUERY_MONITOR", name)


def update_probe(
    session,
    oldname,
    name,
    condition,
    notify_writer,
    notify_writer_method,
    notify_other,
    notify_other_method,
    cancel,
):
    return update_entity(
        session,
        "QUERY_MONITOR",
        oldname,
        {
            "name": name,
            "condition": condition,
            "notify_writer": notify_writer,
            "notify_writer_method": notify_writer_method,
            "notify_other": notify_other,
            "notify_other_method": notify_other_method,
            "cancel": cancel,
            "probe_created_at": datetime.now(),
            "probe_modified_at": datetime.now(),
        },
    )


def update_setting(bare_session, name: str, value: str):
    with transaction(bare_session) as session:
        try:
            setting = Setting(key=name, value=value)
            setting.write(session)
            return ""
        except Exception as ve:
            return summarize_error("Failed to update setting", ve)


def enable_task(session, name: str):
    try:
        task = Task(task_name=name)
        task.enable(session)
        return ""
    except Exception as e:
        return summarize_error("Unable to enable task", e)


def disable_task(session, name: str):
    try:
        task = Task(task_name=name)
        task.disable(session)
        return ""
    except Exception as e:
        return summarize_error("Unable to disable task", e)


def create_warehouse_schedule(
    bare_session,
    name: str,
    size: str,
    start: time,
    finish: time,
    weekday: bool,
    suspend_minutes: int,
    autoscale_mode: str,
    autoscale_min: int,
    autoscale_max: int,
    auto_resume: bool,
    comment: str,
):
    with transaction(bare_session) as session:
        # Make sure the default schedule are created.
        _ = fetch_schedules_with_defaults(session, name)

        current_scheds = WarehouseSchedules.find_all_with_weekday(
            session, name, weekday
        )

        # Figure out if the schedules are enabled or disabled
        is_enabled = (
            all(s.enabled for s in current_scheds) if len(current_scheds) > 0 else False
        )

        new_sched = WarehouseSchedules.parse_obj(
            dict(
                id_val=uuid4().hex,
                name=name,
                size=size,
                start_at=start,
                finish_at=finish,
                suspend_minutes=suspend_minutes,
                warehouse_mode=autoscale_mode,
                scale_min=autoscale_min,
                scale_max=autoscale_max,
                resume=auto_resume,
                weekday=weekday,
                enabled=is_enabled,
                comment=comment,
                user_modified=True,
            )
        )

        # Handles pre-existing schedules or no schedules for this warehouse.
        new_scheds = merge_new_schedule(new_sched, current_scheds)

        err_msg, new_scheds = verify_and_clean(new_scheds)
        if err_msg is not None:
            raise Exception(f"Failed to create schedule for {name}, {err_msg}")

        # Write the new schedule
        new_sched.write(session)
        # Update any schedules that were affected by adding the new schedule
        [i.update(session, i) for i in new_scheds if i.id_val != new_sched.id_val]
        # Twiddle the task state after adding a new schedule
        after_schedule_change(session)


def delete_warehouse_schedule_main(
    bare_session, name: str, start: time, finish: time, is_weekday: bool
):
    with transaction(bare_session) as session:
        # Find the matching schedule
        row = WarehouseSchedules.find_one(session, name, start, finish, is_weekday)
        if not row:
            raise Exception(
                f"Could not find warehouse schedule: {name}, {start}, {finish}, {'weekday' if is_weekday else 'weekend'}"
            )

        to_delete = WarehouseSchedules.construct(id_val=row.id_val)
        current_scheds = WarehouseSchedules.batch_read(
            session,
            sortby="start_at",
            filter=lambda df: ((df.name == name) & (df.weekday == is_weekday)),
        )
        new_scheds = delete_warehouse_schedule(to_delete, current_scheds)

        # Delete that schedule, leaving a hole
        to_delete.delete(session)

        # Run the updates, filling the hole
        [i.update(session, i) for i in new_scheds]

        # Twiddle the task state after adding a new schedule
        after_schedule_change(session)


def update_warehouse_schedule(
    bare_session,
    name: str,
    start: time,
    finish: time,
    is_weekday: bool,
    new_start_at: time,
    new_finish_at: time,
    size: str,
    suspend_minutes: int,
    autoscale_mode: str,
    autoscale_min: int,
    autoscale_max: int,
    auto_resume: bool,
    comment: str,
):
    with transaction(bare_session) as session:
        # Make sure the default schedule are created.
        _ = fetch_schedules_with_defaults(session, name)

        # Find a matching schedule
        old_schedule = WarehouseSchedules.find_one(
            session, name, start, finish, is_weekday
        )
        if not old_schedule:
            raise Exception(
                f"Could not find warehouse schedule: {name}, {start}, {finish}, {'weekday' if is_weekday else 'weekend'}"
            )

        # Make the new version of that schedule with the same id_val
        new_schedule = WarehouseSchedules.parse_obj(
            dict(
                id_val=old_schedule.id_val,
                name=name,
                size=size,
                start_at=new_start_at,
                finish_at=new_finish_at,
                suspend_minutes=suspend_minutes,
                warehouse_mode=autoscale_mode,
                scale_min=autoscale_min,
                scale_max=autoscale_max,
                resume=auto_resume,
                comment=comment,
                enabled=old_schedule.enabled,
                user_modified=True,
            )
        )

        # Read the current schedules
        schedules = WarehouseSchedules.find_all_with_weekday(
            session, name, new_schedule.weekday
        )

        # Update the WarehouseSchedule instance for this warehouse
        schedules_needing_update = update_existing_schedule(
            old_schedule.id_val, new_schedule, schedules
        )

        # Persist all updates to the table
        [i.update(session, i) for i in schedules_needing_update]

        # Twiddle the task state after a schedule has changed
        after_schedule_change(session)


def enable_schedule(session, name: str):
    with transaction(session) as txn:
        # Find a matching schedule
        WarehouseSchedules.enable_scheduling(txn, name, True)


def disable_schedule(session, name: str):
    with transaction(session) as txn:
        # Find a matching schedule
        WarehouseSchedules.enable_scheduling(txn, name, False)


def create_default_schedules(session, name: str):
    fetch_schedules_with_defaults(session, name)
    return ""


def reset_warehouse_schedule(bare_session, warehouse_name: str):
    with transaction(bare_session) as session:
        session.sql(
            "DELETE FROM internal.wh_schedules WHERE name = ?", params=(warehouse_name,)
        ).collect()

        # Re-create the default schedules for this warehouse
        _ = fetch_schedules_with_defaults(session, warehouse_name)

        # Twiddle the task state after adding a new schedule
        after_schedule_change(session)
        return ""


methods = {
    "create_label": create_label,
    "delete_label": delete_label,
    "update_label": update_label,
    "validate_predefined_labels": validate_predefined_labels,
    "create_query_monitor": create_probe,
    "delete_query_monitor": delete_probe,
    "update_query_monitor": update_probe,
    "update_setting": update_setting,
    "enable_task": enable_task,
    "disable_task": disable_task,
    "create_warehouse_schedule": create_warehouse_schedule,
    "update_warehouse_schedule": update_warehouse_schedule,
    "delete_warehouse_schedule": delete_warehouse_schedule_main,
    "enable_warehouse_scheduling": enable_schedule,
    "disable_warehouse_scheduling": disable_schedule,
    "create_default_schedules": create_default_schedules,
    "reset_warehouse_schedule": reset_warehouse_schedule,
}


def main(session, method, **kwargs):
    func = methods.get(method.lower())
    if not func:
        raise ValueError(f"Invalid method: {method}")
    return func(session, **kwargs)
