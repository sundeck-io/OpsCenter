from typing import List
import pydantic


def summarize_error(base_msg: str, ve: pydantic.ValidationError) -> str:
    errs = _parse_validation_error(ve)
    return f'{base_msg}: {", ".join(errs)}'


def error_to_markdown(base_msg: str, ve: pydantic.ValidationError) -> str:
    errs = _parse_validation_error(ve)
    # Extra spaces are (allegedly) to get markdown to properly render newlines
    return base_msg + "  \n" + "  \n".join(errs)


def _parse_validation_error(ve: pydantic.ValidationError) -> List:
    errs = []
    for e in ve.errors():
        err_type = e.get("type", "")
        if err_type.startswith("assertion_error") or err_type.startswith("value_error"):
            for attr in e["loc"]:
                if attr == '__root__':
                    errs.append(f"- {e['msg']}")
                else:
                    errs.append(f"- `{attr}`: {e['msg']}")
    return errs
