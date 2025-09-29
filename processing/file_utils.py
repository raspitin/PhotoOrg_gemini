import shutil
from pathlib import Path

class FileUtils:
    @staticmethod
    def safe_copy(src_path, dest_dir, base_name):
        dest_file = Path(dest_dir) / base_name
        counter = 1
        while dest_file.exists():
            stem, suffix = dest_file.stem, dest_file.suffix
            dest_file = Path(dest_dir) / f"{stem}__{counter}{suffix}"
            counter += 1
        shutil.copy2(src_path, dest_file)
        return dest_file

    @staticmethod
    def available_space(path):
        import os
        stat = os.statvfs(str(path))
        return stat.f_bavail * stat.f_frsize