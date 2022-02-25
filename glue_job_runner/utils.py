import string
from pathlib import Path
from typing import Iterable, List, Union


class JobMisconfigured(Exception):
    pass


class JobFailed(Exception):
    pass


class JobTimedOut(Exception):
    pass


class JobNotStarted(Exception):
    pass


class JobStopped(Exception):
    pass


class JobThrottlingExceeded(Exception):
    pass


class ConflictingJobDefinitionArguments(Exception):
    pass


def get_or_return_path(p: Union[Path, str]):
    """Converts input into a Path if p is a str otherwise return the Path unchanged"""
    return p if isinstance(p, Path) else Path(p)


def filter_directories_by_extensions(
    directories: List[Path], extensions: Iterable[str]
) -> List[Path]:
    """Return paths in all directories that have the matching extensions"""

    filtered_paths = []
    for d in directories:
        if not d.is_dir():
            continue  # Avoids error being raised by iterdir
        for child in d.iterdir():
            if [e for e in extensions if e in child.suffixes]:
                filtered_paths.append(child)
    return filtered_paths


def validate_string(s, allowed_chars="_", allow_upper=False):
    if s != s.lower() and not allow_upper:
        raise ValueError("string provided must be lowercase")

    invalid_chars = string.punctuation

    for a in allowed_chars:
        invalid_chars = invalid_chars.replace(a, "")

    if any(char in invalid_chars for char in s):
        raise ValueError(
            f"punctuation excluding ({allowed_chars}) is not allowed in string"
        )
