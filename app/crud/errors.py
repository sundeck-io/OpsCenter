from typing import List, Optional
import pydantic


_custom_errors = {
    "type_error.none.not_allowed": "{} cannot be null",
}


def summarize_error(base_msg: str, ve: pydantic.ValidationError) -> str:
    if not type(ve) == pydantic.ValidationError:
        return f'{base_msg}: {str(ve)}'

    errs = _parse_validation_error(ve)
    return f'{base_msg}: {", ".join(errs)}'


def error_to_markdown(base_msg: str, ve: pydantic.ValidationError) -> str:
    if not type(ve) == pydantic.ValidationError:
        return f'{base_msg}: {str(ve)}'

    errs = _parse_validation_error(ve, as_markdown=True)
    # Extra spaces are (allegedly) to get markdown to properly render newlines
    return base_msg + "  \n" + "  \n".join(errs)


def _parse_validation_error(ve: pydantic.ValidationError, as_markdown=False) -> List:
    errs = []
    formatter = MarkdownFormatter() if as_markdown else Formatter()
    for e in ve.errors():
        err_type = e.get("type", "")
        for attr in e["loc"]:
            if attr == "__root__":
                attr = None

            # Swap out certain error messages for our own.
            if err_type in _custom_errors:
                errs.append(_custom_errors[err_type].format(attr))
            else:
                errs.append(formatter.format(attr, e["msg"]))
    return errs


class Formatter:
    def format(self, attribute: Optional[str], message: str) -> str:
        if attribute:
            return f"{attribute}: {message}"
        else:
            return message


class MarkdownFormatter(Formatter):
    def format(self, attribute: Optional[str], message: str) -> str:
        if attribute:
            return f"- `{attribute}`: {message}"
        else:
            return f"- {message}"
