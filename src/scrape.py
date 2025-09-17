from settings import Dirs, HOST, FTP_PATH
import ftplib
from models import Variable, TimeStep
from tqdm import tqdm


def download_prism(var: Variable, scope: TimeStep, year):
    print(f"Downloading {scope.value} {var.value} - {year}")
    ftp = ftplib.FTP(HOST)
    ftp.login()
    remote_dir = FTP_PATH + f"/{var.value}/{scope.value}/{year}"
    ftp.cwd(remote_dir)
    remote_files = ftp.nlst()

    local_path = Dirs.output / "prism_raw" / \
        var.value / scope.value / str(year)
    local_path.mkdir(parents=True, exist_ok=True)

    local_files = [x.name for x in local_path.iterdir()]
    files = [f for f in remote_files if f not in local_files]
    if len(files) == 0:
        print("you have downloaded all of these")
    for fname in tqdm(files):
        output_file = local_path / fname
        with open(output_file, "wb") as f:
            ftp.retrbinary(f"RETR {fname}", f.write)
    ftp.quit()


if __name__ == "__main__":
    years = range(2013, 2026)

    for var in Variable:
        for y in years:
            download_prism(var, TimeStep.DAILY, y)
