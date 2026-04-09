from __future__ import annotations


def main(argv: list[str] | None = None) -> None:
    from .server import main as server_main

    server_main(argv)


__all__ = ["main"]
