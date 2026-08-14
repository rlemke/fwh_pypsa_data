# SPDX-License-Identifier: Apache-2.0
"""fwh_pypsa_data — PyPSA-Eur's data-retrieval layer as an FFL workflow."""

from __future__ import annotations

from pathlib import Path

from facetwork.domains import DomainPackage

from .handlers import register_all_registry_handlers

domain = DomainPackage(
    name="pypsa-data",
    ffl_dir=Path(__file__).parent / "ffl",
    register_handlers=register_all_registry_handlers,
)
