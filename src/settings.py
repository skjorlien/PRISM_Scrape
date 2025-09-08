from dataclasses import dataclass
from pathlib import Path

# for example:
# https://ftp.prism.oregonstate.edu/time_series/us/an/4km/tmax/daily/
#

HOST = "prism.oregonstate.edu"
FTP_PATH = "time_series/us/an/4km"


def get_repo_root():
    """Find the nearest parent directory containing a .git folder."""
    current = Path.cwd()
    for parent in [current] + list(current.parents):
        if (parent / ".git").is_dir():
            return parent
    raise RuntimeError("No .git directory found in any parent folder.")


@dataclass
class Dirs:
    output: Path = Path.home() / "Data" / "PRISM"
