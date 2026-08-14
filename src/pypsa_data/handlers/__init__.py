# SPDX-License-Identifier: Apache-2.0
"""Handler registration for the pypsa-data domain."""

from __future__ import annotations


def register_all_registry_handlers(runner) -> None:
    from .catalog.catalog_handlers import register_handlers as reg_catalog

    reg_catalog(runner)
