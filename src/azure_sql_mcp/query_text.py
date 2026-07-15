from __future__ import annotations

import re

_QUERY_START_PATTERN = re.compile(r"^(SELECT|WITH)\b", re.IGNORECASE)


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
