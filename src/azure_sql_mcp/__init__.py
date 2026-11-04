from __future__ import annotations


def main(argv: list[str] | None = None) -> None:
    # Before any import that loads the ODBC driver and before stdio is read:
    # this may re-exec the process so the macOS driver can load OpenSSL.
    from . import driver_openssl

    driver_openssl.ensure_driver_openssl()

    from .server import main as server_main

    server_main(argv)


__all__ = ["main"]
