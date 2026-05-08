"""Project-wide constants and path-resolution helpers for all ETL jobs."""

from dataclasses import dataclass
from pathlib import Path

SITES: list[str] = ["PARIS", "BERLIN", "LONDON", "NEWYORK", "LOSANGELES", "SHANGHAI"]
EU_SITES: frozenset[str] = frozenset({"PARIS", "BERLIN", "LONDON"})
US_SITES: frozenset[str] = frozenset({"NEWYORK", "LOSANGELES"})

@dataclass(frozen=True)
class ETLConfig:
    """Immutable configuration: data root path and all source-file path helpers."""

    data_path: Path = Path("data")

    def personnel_path(self, site: str) -> Path:
        """Path to the PERSONNEL file for a given site (loaded once at startup)."""
        return self.data_path / f"BDD_BGES_{site}" / f"PERSONNEL_{site}.txt"

    def mission_file(self, site: str, date_str: str) -> Path:
        """Path to the daily MISSION file for a site and date (YYYYMMDD)."""
        return (
            self.data_path
            / f"BDD_BGES_{site}"
            / f"BDD_BGES_{site}_MISSION"
            / f"MISSION_{date_str}.txt"
        )

    def equipment_file(self, site: str, date_str: str) -> Path:
        """Path to the daily EQUIPMENT file for a site and date (YYYYMMDD)."""
        return (
            self.data_path
            / f"BDD_BGES_{site}"
            / f"BDD_BGES_{site}_INFORMATIQUE"
            / f"MATERIEL_INFORMATIQUE_{date_str}.txt"
        )

    def co2_ref_path(self) -> Path:
        """Path to the equipment CO2 reference file (loaded once at startup)."""
        return self.data_path / "materiel_informatique_impact.csv"

    def coordinates_path(self) -> Path:
        """Path to the city geographic coordinates cache."""
        return self.data_path / "city_coordinates.json"
