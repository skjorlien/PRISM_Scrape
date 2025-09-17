from dataclasses import dataclass
from pathlib import Path

# for example:
# https://ftp.prism.oregonstate.edu/time_series/us/an/4km/tmean/daily/
#

HOST = "prism.oregonstate.edu"
FTP_PATH = "time_series/us/an/4km"

# I don't use this for this project, but nifty if you want to replace Path.home() with the repo root


def get_repo_root():
    """Find the nearest parent directory containing a .git folder."""
    current = Path.cwd()
    for parent in [current] + list(current.parents):
        if (parent / ".git").is_dir():
            return parent
    raise RuntimeError("No .git directory found in any parent folder.")


SHAPEFILE_PATH = Path.home() / "Data" / "Shapefiles" / "US" / \
    "County" / "cb_2023_us_county_5m.zip"


@dataclass
class Dirs:
    output: Path = Path.home() / "Data" / "PRISM"
    clean: Path = Path.home() / "Data" / "PRISM" / "prism_clean"


Dirs.clean.mkdir(parents=True, exist_ok=True)
