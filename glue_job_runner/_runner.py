import copy
import datetime
import os
import re
import shutil
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Iterable, List, Optional, Union
from urllib.request import urlretrieve

import boto3
from botocore.exceptions import ClientError

from glue_job_runner.utils import (
    ConflictingJobDefinitionArguments,
    JobFailed,
    JobMisconfigured,
    JobNotStarted,
    JobStopped,
    JobThrottlingExceeded,
    JobTimedOut,
    filter_directories_by_extensions,
    get_or_return_path,
    validate_string,
)

VALID_GLUE_VERSION = ("3.0", "2.0", "1.0", "0.9")
GLUE_V1_VALID_PYTHON_VERSIONS = ("3", "2")
VALID_WORKER_TYPES = ("Standard", "G.1X", "G.2X")

RESERVED_AWS_PARAMS = [
    "--JOB_NAME",
    "--debug",
    "--mode",
    "--conf",
]


RESERVED_PACKAGE_PARAMS = [
    "--metadata_base_path",
]

ALL_RESERVED_PARAMS = RESERVED_AWS_PARAMS + RESERVED_PACKAGE_PARAMS


def calculate_timeout_based_on_max_cost(
    worker_type: str,
    number_of_workers: int,
    max_cost_in_dollars: float = 20.00,
    dpu_dollars_per_hour: float = 0.44,
) -> int:
    """
    Calculate the max timeout based on a max cost of your glue job
    """
    if worker_type not in VALID_WORKER_TYPES:
        raise ValueError(
            f"worker_type must be one of {VALID_WORKER_TYPES}. Got {worker_type}."
        )

    # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-jobs-job.html
    if worker_type == "G.2X":
        dpus_per_worker = 2
    else:
        dpus_per_worker = 1

    timeout = int(
        60
        * max_cost_in_dollars
        / (dpu_dollars_per_hour * dpus_per_worker * number_of_workers)
    )

    return timeout


# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-jobs-job.html
def _get_default_job_definition():
    job_def = {
        "Name": None,
        "Role": None,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
        "Command": {"Name": "glueetl", "ScriptLocation": None, "PythonVersion": "3"},
        "DefaultArguments": {
            "--TempDir": "",
            "--extra-files": "",
            "--extra-py-files": "",
            "--job-bookmark-option": "job-bookmark-disable",
        },
        "MaxRetries": 0,
        "GlueVersion": "2.0",
        "Tags": {
            "environment-name": "dev",
            "business-unit": "Platforms",
            "application": "Data Engineering",
            "owner": "Data Engineering:dataengineering@digital.justice.gov.uk",
            "is-production": "False",
        },
        "Timeout": None,
        "WorkerType": "Standard",
        "NumberOfWorkers": 2,
    }
    job_def["Timeout"] = calculate_timeout_based_on_max_cost(
        job_def["WorkerType"], job_def["NumberOfWorkers"]
    )
    return job_def


# Decided to make a seperate class to manage editting the data sent to
# the glue API. This could have just been part of the GlueJob but I wanted
# to seperate it out for readabilty and hopefully easier future maintainance
# (The GlueJob class is quite large and convoluted). Hopefully this makes things
# a bit easier. The GlueJob class is now a basic class that you manually set all
# the API settings and you can still use it too
class GlueJob:
    def __init__(
        self,
        job_name: str,
        job_role: str,
        job_arguments: Optional[dict] = None,
        job_def: Optional[dict] = None,
        tags: Optional[dict] = None,
    ):
        """
        Class to manage key arguments for a GlueJob.
        """

        self._job_id = "{:0.0f}".format(time.time())
        # TODO remove hardcoded definition at the bottom
        if job_def is None:
            self.job_def = _get_default_job_definition()
        else:
            self.job_def = copy.deepcopy(job_def)

        # Since this is called after if API Data states something
        # different to these arguments they are overwritten
        self.job_name = job_name
        self.job_role = job_role
        self.job_arguments = job_arguments
        if tags is not None:
            self.tags = tags

    @property
    def job_name(self) -> str:
        return self.job_def.get("Name")

    @job_name.setter
    def job_name(self, job_name) -> None:
        validate_string(job_name, allowed_chars="-_:")
        self.job_def["Name"] = job_name

    @property
    def job_role(self) -> str:
        return self.job_def.get("Role")

    @job_role.setter
    def job_role(self, job_role: str):
        self.job_def["Role"] = job_role

    @property
    def max_concurrent_runs(self) -> int:
        return self.job_def.get("ExecutionProperty", {}).get("MaxConcurrentRuns")

    @max_concurrent_runs.setter
    def max_concurrent_runs(self, max_concurrent_runs: int):
        if "ExecutionProperty" not in self.job_def:
            self.job_def["ExecutionProperty"] = {}

        self.job_def["ExecutionProperty"]["MaxConcurrentRuns"] = max_concurrent_runs

    @property
    def glue_job_type(self) -> str:
        return self.job_def.get("Command", {}).get("Name")

    @glue_job_type.setter
    def glue_job_type(self, glue_job_type: str):
        if glue_job_type.lower().strip() in ["spark", "glueetl"]:
            value = "glueetl"
        elif glue_job_type.lower().strip() in ["python", "pythonshell"]:
            value = "pythonshell"
        elif glue_job_type.lower().strip() == "gluestreaming":
            value = "gluestreaming"
        else:
            raise ValueError(
                "glue_job_type can either be set to 'glueetl' "
                "(or 'spark' as an alias) for Spark Jobs "
                "or 'pythonshell' (or 'python' as an alias) for Python Jobs or "
                "gluestreaming for a Glue Streaming Job. "
                f"Got given ({value})."
            )
        if "Command" not in self.job_def:
            self.job_def["Command"] = {}
        self.job_def["Command"]["Name"] = value

    @property
    def script_location(self) -> str:
        return self.job_def.get("Command", {}).get("ScriptLocation")

    @script_location.setter
    def script_location(self, script_location: str):
        if "Command" not in self.job_def:
            self.job_def["Command"] = {}
        self.job_def["Command"]["ScriptLocation"] = script_location

    @property
    def python_version(self) -> str:
        return self.job_def.get("Command", {}).get("PythonVersion")

    @python_version.setter
    def python_version(self, python_version: str):

        if not self.glue_version == "1.0":
            raise ValueError(
                f"Can only set python version when Glue Version is 1.0. The glue_version is set to {self.glue_version}"
            )

        if not isinstance(python_version, str):
            raise TypeError(
                f"python_version must be of type str (given {type(python_version)})"
            )

        elif python_version not in GLUE_V1_VALID_PYTHON_VERSIONS:
            raise ValueError(
                "python_version must be one of "
                f"{GLUE_V1_VALID_PYTHON_VERSIONS} (given {python_version})"
            )

        if "Command" not in self.job_def:
            self.job_def["Command"] = {}
        self.job_def["Command"]["PythonVersion"] = python_version

    @property
    def glue_job_temp_dir(self) -> str:
        return self.job_def.get("DefaultArguments", {}).get("--TempDir")

    @glue_job_temp_dir.setter
    def glue_job_temp_dir(self, glue_job_temp_dir: str):
        if "DefaultArguments" not in self.job_def:
            self.job_def["DefaultArguments"] = {}

        self.job_def["DefaultArguments"]["--TempDir"] = glue_job_temp_dir

    @property
    def max_retries(self) -> int:
        return self.job_def.get("MaxRetries")

    @max_retries.setter
    def max_retries(self, max_retries: int):
        self.job_def["MaxRetries"] = max_retries

    @property
    def max_capacity(self) -> float:
        return self.job_def.get("MaxCapacity")

    @max_capacity.setter
    def max_capacity(self) -> float:
        if self.glue_version != "1.0":
            raise ConflictingJobDefinitionArguments(
                "You can cannot set set max_capacity when glue_version is not 1.0. "
                "If you want to set max_capacity set glue_version to '1.0' first"
            )
        elif self.worker_type is not None and self.number_of_workers is not None:
            raise ConflictingJobDefinitionArguments(
                "You cannot set max_capacity when the API data has "
                "WorkerType and NumberOfWorkers set. Use "
                "unset_specific_worker_definitions() to drop these values "
                "from the API data."
            )
        self.job_def.get("MaxCapacity")

    @property
    def timeout(self) -> int:
        return self.job_def.get("Timeout")

    @timeout.setter
    def timeout(self, timeout: int):
        self.job_def["Timeout"] = timeout

    def set_timeout_based_on_max_cost(self, max_cost_in_dollars: float):
        self.timeout = calculate_timeout_based_on_max_cost(
            self.worker_type, self.number_of_workers, max_cost_in_dollars
        )

    @property
    def tags(self) -> dict:
        return self.job_def.get("Tags")

    @tags.setter
    def tags(self, tags: dict):
        self.job_def["Tags"] = tags if tags is not None else {}

    @property
    def glue_version(self) -> str:
        return self.job_def.get("GlueVersion")

    @glue_version.setter
    def glue_version(self, glue_version: str):
        if not isinstance(glue_version, str):
            raise TypeError(
                f"glue_version must be of type str (given {type(glue_version)})"
            )

        if glue_version not in VALID_GLUE_VERSION:
            raise ValueError(
                f"glue_version must be one of {VALID_GLUE_VERSION} (give {glue_version})"
            )

        self.job_def["GlueVersion"] = glue_version

    @property
    def number_of_workers(self) -> int:
        return self.job_def.get("NumberOfWorkers")

    @number_of_workers.setter
    def number_of_workers(self, number_of_workers: int):
        self.job_def["NumberOfWorkers"] = number_of_workers

    @property
    def worker_type(self) -> str:
        return self.job_def.get("WorkerType")

    @worker_type.setter
    def worker_type(self, worker_type: str):
        if worker_type not in VALID_WORKER_TYPES:
            raise ValueError(
                f"worker_type must be on of: {VALID_WORKER_TYPES}. "
                f"Got {worker_type}."
            )
        self.job_def["WorkerType"] = worker_type

    @property
    def job_arguments(self) -> dict:
        return self._job_arguments

    @job_arguments.setter
    def job_arguments(self, job_arguments: dict):
        # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html

        if job_arguments is None:
            job_arguments = {}
        elif not isinstance(job_arguments, dict):
            raise TypeError("job_arguments must be a dictionary or None")
        else:
            pass

        errors = []
        for k in job_arguments.keys():
            if k[:2] != "--" or k in RESERVED_AWS_PARAMS:
                errors.append(k)

        if errors:
            raise ValueError(
                (
                    f"Found incorrect AWS job argument ({k}). All "
                    f"arguments should begin with '--' and cannot be "
                    f"one of the following: {RESERVED_AWS_PARAMS}"
                )
            )
        self._job_arguments = job_arguments

    @property
    def tags(self) -> dict:
        return self._tags

    @tags.setter
    def tags(self, tags: dict):
        self._tags.update(tags)

    def unset_specific_worker_definitions(self):
        """
        Removes WorkerType and NumberOfWorkers definitions from
        the current API data.
        """
        if "WorkerType" in self.job_def:
            del self.job_def["WorkerType"]

        if "NumberOfWorkers" in self.job_def:
            del self.job_def["NumberOfWorkers"]

    def unset_max_capacity(self):
        """
        Removes MaxCapacity from the current API data.
        """
        if "MaxCapacity" in self.job_def:
            del self.job_def["MaxCapacity"]

    def run_job(self):

        glue_client = boto3.client("glue")
        self.delete_job()

        glue_client.create_job(**self.job_def)
        response = glue_client.start_job_run(
            JobName=self.job_name, Arguments=self.job_arguments
        )

        self._job_run_id = response["JobRunId"]

    def delete_job(self):
        """
        Delete the glue job definition on AWS Glue
        """
        glue_client = boto3.client("glue")
        if self.job_name is None:
            raise JobMisconfigured('Missing "job_name"')

        glue_client.delete_job(JobName=self.job_name)

    @property
    def job_run_id(self) -> str:
        return self._job_run_id

    @property
    def job_status(self):
        glue_client = boto3.client("glue")
        if self.job_run_id is None:
            raise JobNotStarted('Missing "job_run_id", have you started the job?')

        if self.job_name is None:
            raise JobMisconfigured('Missing "job_name"')

        return glue_client.get_job_run(JobName=self.job_name, RunId=self.job_run_id)

    @property
    def job_run_state(self):
        status = self.job_status
        return status["JobRun"]["JobRunState"]

    @property
    def is_running(self):
        return self.job_run_state == "RUNNING"

    def wait_for_completion(
        self,
        verbose=False,
        wait_seconds=10,
        back_off_retries=5,
        cleanup_if_successful=False,
    ):
        """
        Wait for the job to complete.

        This means it either succeeded or it was manually stopped.

        Raises:
            JobFailed: When the job failed
            JobTimedOut: When the job timed out
        """

        back_off_counter = 0
        while True:
            time.sleep(wait_seconds)

            try:
                status = self.job_status
            except ClientError as e:
                if (
                    "ThrottlingException" in str(e)
                    and back_off_counter < back_off_retries
                ):
                    back_off_counter += 1
                    back_off_wait_time = wait_seconds * (2 ** (back_off_counter))
                    status_code = (
                        f"BOTO_CLIENT_RATE_EXCEEDED (waiting {back_off_wait_time}s)"
                    )
                    time.sleep(back_off_wait_time)
                else:
                    if "ThrottlingException" in str(e):
                        err_str = f"Total number of retries ({back_off_retries}) exceeded - {str(e)}"
                        raise JobThrottlingExceeded(err_str)
                    else:
                        raise e
            else:
                back_off_counter = 0
                status_code = status["JobRun"]["JobRunState"]
                status_error = status["JobRun"].get("ErrorMessage", "n/a")
                exec_time = status["JobRun"].get("ExecutionTime", "n/a")

            if verbose:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(
                    (
                        f"{timestamp}: Job State: {status_code} | "
                        f"Execution Time: {exec_time} (s) | Error: {status_error}"
                    )
                )

            if status_code == "SUCCEEDED":
                break

            if status_code == "FAILED":
                raise JobFailed(status_error)
            if status_code == "TIMEOUT":
                raise JobTimedOut(status_error)
            if status_code == "STOPPED":
                raise JobStopped(status_error)

        if status_code == "SUCCEEDED" and cleanup_if_successful:
            back_off_counter = 0
            if verbose:
                print("JOB SUCCEEDED: Cleaning Up")

            while True:
                try:
                    self.cleanup()
                except ClientError as e:
                    if (
                        "ThrottlingException" in str(e)
                        and back_off_counter < back_off_retries
                    ):
                        back_off_counter += 1
                        back_off_wait_time = wait_seconds * (2 ** (back_off_counter))
                        time.sleep(back_off_wait_time)
                    else:
                        if "ThrottlingException" in str(e):
                            err_str = f"Total number of retries ({back_off_retries}) exceeded - {str(e)}"
                            raise JobThrottlingExceeded(err_str)
                        else:
                            raise e
                else:
                    break


class GlueJobFromFolder(GlueJob):
    """
    Has additional functions to the GlueJob class that allows you to define a job from a local folder structure.

    Folder must be formatted as follows:
    job_folder
      job.py
      glue_py_resources/
        zip and python files
        github_zip_urls.txt <- file containing urls of zip files from github, which will be converted into glue's required format
      glue_resources/
        txt, sql, json, or csv files
      glue_jars/
        jar files

    Can then run jobs on aws glue using this class.

    If include_shared_job_resources is True then glue_py_resources and glue_resources folders inside a special named folder 'shared_glue_resources'
    will also be referenced.
    glue_jobs (parent folder to 'job_folder')
      shared_glue_resources
        glue_py_resources/
          zip, python and zip_urls
        glue_resources/
          txt, sql, json, or csv files
        glue_jars/
            jar files
      job_folder
        etc...
    """

    def __init__(
        self,
        job_folder: Union[Path, str],
        bucket: str,
        job_role: str,
        job_name: Optional[str] = None,
        job_arguments: Optional[dict] = None,
        job_def: Optional[dict] = None,
        tags: Optional[dict] = None,
        include_shared_job_resources: bool = True,
        timeout_override_minutes: int = None,
    ):

        job_folder = get_or_return_path(job_folder)
        if job_name is None:
            job_name = job_folder.name

        super().__init__(
            job_name=job_name,
            job_role=job_role,
            job_arguments=None,  # This class has it's own job_args getter/setter
            job_def=job_def,
            tags=tags,
        )

        self.job_arguments = job_arguments
        self._job_id = "{:0.0f}".format(time.time())

        self.job_folder = job_folder
        if not self.job_path.exists():
            raise ValueError(
                (
                    "Could not find job.py in base directory provided "
                    f"({self.job_folder}), stopping.\nOnly folder allowed to "
                    "have no job.py is a folder named shared_job_resources"
                )
            )

        self.bucket = bucket
        self.include_shared_job_resources = include_shared_job_resources
        self.py_resources = self._get_py_resources()
        self.resources = self._get_resources()
        self.jars = self._get_jars()

        # Set metadata folder name (if one exists)
        self._metadata_folder_name = "metadata"
        for meta_folder_name in ["metadata", "meta_data"]:
            if Path(self.etl_root_folder, meta_folder_name).exists():
                self._metadata_folder_name = meta_folder_name
                break

        # Within a glue job, it's sometimes useful to be able to access the agnostic metdata
        self.all_meta_data_paths = self._get_metadata_paths()
        self.github_zip_urls = self._get_github_resource_list()

        self.timeout_override_minutes = timeout_override_minutes

        # List of URLs we want to install for our AWS Spark Job
        self.github_py_resources: List[Path] = []

        self._reserved_job_arguments = {
            "--metadata_base_path": self.s3_metadata_base_folder_inc_bucket
        }

    @property
    def reserved_job_arguments(self) -> dict:
        return self._reserved_job_arguments

    @property
    def job_arguments(self) -> dict:
        reserved_job_arguments = {
            "--metadata_base_path": self.s3_metadata_base_folder_inc_bucket
        }
        return {**reserved_job_arguments, **self._job_arguments}

    @job_arguments.setter
    def job_arguments(self, job_arguments: dict):
        # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html

        if job_arguments is None:
            job_arguments = {}
        elif not isinstance(job_arguments, dict):
            raise TypeError("job_arguments must be a dictionary or None")
        else:
            pass

        errors = []
        for k in job_arguments.keys():
            if k[:2] != "--" or k in ALL_RESERVED_PARAMS:
                errors.append(k)

        if errors:
            raise ValueError(
                (
                    f"Found incorrect AWS job argument ({k}). All "
                    f"arguments should begin with '--' and cannot be "
                    f"one of the following: {ALL_RESERVED_PARAMS}"
                )
            )
        self._job_arguments = job_arguments

    @property
    def job_folder(self) -> Path:
        return self._job_folder

    @job_folder.setter
    def job_folder(self, job_folder: Union[Path, str]) -> None:
        self._job_folder = get_or_return_path(job_folder)
        self.script_location = str(self.job_folder.joinpath("job.py"))

    @property
    def job_path(self) -> Path:
        return self.job_folder.joinpath("job.py")

    @property
    def s3_job_folder_inc_bucket(self) -> str:
        return f"s3://{self.bucket}/{self.s3_job_folder_no_bucket}"

    @property
    def s3_job_folder_no_bucket(self) -> str:
        return os.path.join("_GlueJobs_", self.job_name, self._job_id, "resources/")

    @property
    def s3_metadata_base_folder_inc_bucket(self) -> str:
        return os.path.join(
            self.s3_job_folder_inc_bucket, self._metadata_folder_name + "/"
        )

    @property
    def s3_metadata_base_folder_no_bucket(self) -> str:
        return os.path.join(
            self.s3_job_folder_no_bucket, self._metadata_folder_name + "/"
        )

    @property
    def job_parent_folder(self) -> Path:
        return self.job_folder.parent

    @property
    def etl_root_folder(self) -> Path:
        return self.job_parent_folder.parent

    @property
    def bucket(self) -> str:
        return self._bucket

    @bucket.setter
    def bucket(self, bucket) -> None:
        validate_string(bucket, "-.")
        self._bucket = bucket

    def _check_nondup_resources(self, resources_list: Union[Path, str]) -> None:

        file_list = [get_or_return_path(r).name for r in resources_list]
        if len(file_list) != len(set(file_list)):
            raise ValueError(
                (
                    "There are duplicate file names in your supplied "
                    "resources. A file in job resources might share the "
                    "same name as a file in the shared resources folders."
                )
            )

    def _get_github_resource_list(self) -> List[str]:
        zip_urls_path = Path(
            self.job_folder, "glue_py_resources", "github_zip_urls.txt"
        )
        shared_zip_urls_path = Path(
            self.job_parent_folder,
            "shared_job_resources",
            "glue_py_resources",
            "github_zip_urls.txt",
        )

        if zip_urls_path.exists():
            with open(zip_urls_path, "r") as f:
                urls = f.readlines()
            f.close()
        else:
            urls = []

        if shared_zip_urls_path.exists() and self.include_shared_job_resources:
            with open(shared_zip_urls_path, "r") as f:
                shared_urls = f.readlines()
            f.close()
            urls = urls + shared_urls

        urls = [url.strip() for url in urls if url.strip()]

        return urls

    def _get_py_resources(self) -> List[Path]:
        """
        Check existence of glue_py_resources folder and upload all
        the .py or .zip files in resources otherwise skip.
        """
        # Upload all the .py or .zip files in resources
        # Check existence of folder, otherwise skip
        extensions = (".py", ".zip")
        resources = self._job_resource_filter_directories_by_extension(
            "glue_py_resources", extensions
        )
        return resources

    def _get_resources(self) -> List[Path]:
        """
        Check existence of glue_resources and upload all the
        sql, json, csv or txt files in resources otherwise skip.
        """
        extensions = (
            ".sql",
            ".json",
            ".csv",
            ".txt",
        )  # TODO: Maybe make this more flexible?
        resources = self._job_resource_filter_directories_by_extension(
            "glue_resources", extensions
        )
        return resources

    def _get_jars(self) -> List[Path]:
        """
        Check existence of glue_jars folder and upload all the
        .jar in resources otherwise skip.
        """
        extensions = (".jar",)
        resources = self._job_resource_filter_directories_by_extension(
            "glue_jars", extensions
        )
        return resources

    def _get_metadata_paths(self) -> List[Path]:
        """
        Enumerate the relative path for all metadata json files
        in a metadata or meta_data folder whichever folder was first when object.__init__()
        is called.

        To force your own metadata folder then set object._metadata_folder_name = "<folder-name>"
        after the object is created but would only recommend if you understand how this
        package internals work!
        """

        if self._metadata_folder_name is not None:
            metadata_base = Path(self.etl_root_folder, self._metadata_folder_name)
            return list(metadata_base.rglob("*.json"))
        else:
            return []

    def _download_github_zipfile_and_rezip_to_glue_file_structure(self, url) -> Path:
        """
        Returns a Path object pointing to a zipfile
        """
        # TODO: Note I do a lot of str(Path) in here that might be pointless here
        # but haven't checked how shutil works with Path objects.
        this_zip_path = Path(f"_{self.job_name}_tmp_zip_files_to_s3_", "github.zip")

        urlretrieve(url, str(this_zip_path))

        original_dir = this_zip_path.parent

        with tempfile.TemporaryDirectory() as td:
            myzip = zipfile.ZipFile(this_zip_path, "r")
            myzip.extractall(td)
            nested_folder_to_unnest = os.listdir(td)[0]
            nested_path = Path(td, nested_folder_to_unnest)
            name = url.split("/")[4]
            output_path = Path(original_dir, name)
            final_output_path = shutil.make_archive(
                str(output_path), "zip", str(nested_path)
            )

        this_zip_path.unlink()

        return Path(final_output_path)

    def sync_job_to_s3_folder(self) -> None:

        s3_client = boto3.client("s3")
        # Test if folder exists and create if not
        temp_zip_folder = Path(f"_{self.job_name}_tmp_zip_files_to_s3_")
        temp_zip_folder.mkdir(exist_ok=True)

        # Download the github urls and rezip them to work with aws glue
        self.github_py_resources = []
        for url in self.github_zip_urls:
            self.github_py_resources.append(
                self._download_github_zipfile_and_rezip_to_glue_file_structure(url)
            )

        # Check if all filenames are unique
        files_to_sync = (
            self.github_py_resources
            + self.py_resources
            + self.resources
            + self.jars
            + [self.job_path]
        )
        self._check_nondup_resources(files_to_sync)

        # delete the tmp folder before uploading new data to it
        self.delete_s3_job_temp_folder()

        # Sync all job resources to the same s3 folder
        # Note s3 paths of Job object are str not Path objects
        for f in files_to_sync:
            s3_file_path = os.path.join(self.s3_job_folder_no_bucket, str(f.name))
            s3_client.upload_file(str(f), self.bucket, s3_file_path)

        # Upload metadata to subfolder
        for f in self.all_meta_data_paths:
            path_within_metadata_folder = re.sub(
                f"^.*/?{self._metadata_folder_name}/", "", str(f)
            )
            s3_file_path = os.path.join(
                self.s3_metadata_base_folder_no_bucket, path_within_metadata_folder
            )
            s3_client.upload_file(str(f), self.bucket, s3_file_path)

        #  Clean up downloaded zip files
        for f in self.github_py_resources:
            f.unlink()

        shutil.rmtree(temp_zip_folder)

        def run_job(self, sync_to_s3_before_run: bool = True):

            glue_client = boto3.client("glue")
            self.delete_job()

            glue_client.create_job(**self.job_def)
            response = glue_client.start_job_run(
                JobName=self.job_name, Arguments=self.job_arguments
            )

            self._job_run_id = response["JobRunId"]

    def delete_job(self, delete_s3_job_temp_folder=True):
        """
        Deletes the Glue Job definition on AWS. Also will delete the temp
        s3 folder for the job if delete_s3_job_temp_folder is set to True.
        """

        glue_client = boto3.client("glue")
        if self.job_name is None:
            raise JobMisconfigured('Missing "job_name"')

        glue_client.delete_job(JobName=self.job_name)
        if delete_s3_job_temp_folder:
            self.delete_s3_job_temp_folder()

    def delete_s3_job_temp_folder(self):
        """
        Deletes the temporary S3 folder created on S3.
        """

        bucket = boto3.resource("s3").Bucket(self.bucket)
        bucket.objects.filter(Prefix=self.s3_job_folder_no_bucket).delete()

    def _job_resource_filter_directories_by_extension(
        self, sub_folder_name: str, extensions: Iterable[str]
    ) -> List[str]:
        """
        Wrapper for utils.filter_directories_by_extensions giving it paths
        paths based on the job properties.
        """
        resource_paths = [Path(self.job_folder, sub_folder_name)]
        if self.include_shared_job_resources:
            resource_paths.append(
                Path(self.job_parent_folder, "shared_job_resources", sub_folder_name)
            )

        return filter_directories_by_extensions(resource_paths, extensions)
