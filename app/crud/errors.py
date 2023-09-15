import pydantic


def error_to_markdown(base_msg: str, ve: pydantic.ValidationError) -> str:
    errs = []
    for e in ve.errors():
        if e["type"] == "assertion_error" or e["type"] == "value_error":
            errs.append(f"- {e['msg']}")
    # Extra spaces are (allegedly) to get markdown to properly render newlines
    return base_msg + "  \n" + "  \n".join(errs)
