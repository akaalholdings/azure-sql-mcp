from __future__ import annotations

import re

_QUERY_START_PATTERN = re.compile(r"^(SELECT|WITH)\b", re.IGNORECASE)
_TYPE_NAME_PART = r"(?:\[[^\]]+\]|[A-Za-z_][\w$#]*)"
_DECLARED_PARAMETER_PATTERN = re.compile(
    r"\s*(?P<name>@\w+)\s+"
    rf"(?P<type>{_TYPE_NAME_PART}(?:\s*\.\s*{_TYPE_NAME_PART})?"
    r"(?:\s*\(\s*(?:max|\d{1,5}(?:\s*,\s*\d{1,5})?)\s*\))?)"
    r"(?:\s+(?:OUTPUT|OUT|READONLY))?\s*",
    re.IGNORECASE,
)


def split_parameter_declarations(sql: str) -> tuple[tuple[tuple[str, str], ...], str]:
    """Split Query Store text into its declared parameters and the statement.

    Query Store keeps a parameterized statement of any kind (sp_executesql,
    simple or forced parameterization) as ``(@p1 int,@p2 varchar(20))<statement>``.
    Returns ``((name, declared type), ...)`` in declaration order and the
    statement text. Text with no such prefix comes back unchanged with ``()``:
    every item must read as ``@name type``, so ``(SELECT 1)`` is left alone.
    """

    candidate = sql.lstrip()
    if not candidate.startswith("("):
        return (), sql
    end_index = _find_matching_parenthesis(candidate)
    if end_index < 0:
        return (), sql
    statement = candidate[end_index + 1 :].lstrip()
    if not statement:
        return (), sql
    parameters: list[tuple[str, str]] = []
    for item in _split_top_level_commas(candidate[1:end_index]):
        match = _DECLARED_PARAMETER_PATTERN.fullmatch(item)
        if match is None:
            return (), sql
        parameters.append((match.group("name"), re.sub(r"\s+", "", match.group("type"))))
    return tuple(parameters), statement


def parameterized_query_store_text(statement: str, parameter_definition: str) -> str:
    """Return the text Query Store keeps for an sp_executesql call.

    It is ``(`` + the ``@params`` argument + ``)`` + the ``@stmt`` argument, both
    unchanged, so an exact-text lookup of this form finds the statement an
    application sent with these parameter declarations.
    """

    if not parameter_definition.strip():
        return statement
    return f"({parameter_definition}){statement}"


def _split_top_level_commas(text: str) -> list[str]:
    items: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            items.append(text[start:index])
            start = index + 1
    items.append(text[start:])
    return items


def strip_query_store_parameter_declarations(sql: str) -> str:
    """Remove Query Store's leading parameter declaration block when present.

    Query Store often stores text as:
      (@P1 nvarchar(30), @P2 int)SELECT ...

    This helper strips the declaration prefix so read-only validation/parsing can
    operate on the actual query text.
    """
    candidate = sql.lstrip()
    if not candidate.startswith("("):
        return candidate

    end_index = _find_matching_parenthesis(candidate)
    if end_index < 0:
        return candidate

    declaration_block = candidate[1:end_index]
    query_text = candidate[end_index + 1 :].lstrip()
    if not query_text:
        return candidate
    if "@" not in declaration_block:
        return candidate
    if not _QUERY_START_PATTERN.match(query_text):
        return candidate
    return query_text


def _find_matching_parenthesis(text: str) -> int:
    depth = 0
    in_string = False
    index = 0
    while index < len(text):
        char = text[index]
        if in_string:
            if char == "'":
                # Escaped single quote inside a literal.
                if index + 1 < len(text) and text[index + 1] == "'":
                    index += 2
                    continue
                in_string = False
            index += 1
            continue

        if char == "'":
            in_string = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
            if depth < 0:
                return -1
        index += 1

    return -1
