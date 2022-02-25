# Glue Job Runner

A Python wrapper to run glue jobs on AWS.

This is basically a wrapper for setting API components for the glue job defintion (`obj.job_def`) ((docs)[https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-jobs-job.html]) and also commands for Glue Job Arguments (`obj.job_arguments`) ((docs)[https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html]).


## Basic Usage

Use the basic class to just interact with glue jobs.

```python
job = GlueJob(job_name="test", job_role="a-role", job_arguments={"input": "s3://bucket/input", "output": "s3://bucket/output"})

# Define the spark script location
job.script_location = "s3://bucket/scripts/spark_script.py"

# Increase the number of Workers
job.number_of_workers = 4

# run the job, wait for it to comnplete then delete
job.run_job()
job.wait_for_completion()
job.delete_job()
```

## GlueJobFolder: Defining a GlueJob for a folder

This is a subclass of the above GlueJob. It expects a folder path setup for spark jobs (like the etl_manager this is replacing).


The GlueJobFolder class can be used to run PySpark jobs on AWS Glue. It is worth keeping up to date with AWS release notes and general guidance on running Glue jobs. This class is a wrapper function to simplify running glue jobs by using a structured format.

```python
from glue_job_runner import GlueJobFromFolder

my_role = 'aws_role'
bucket = 'bucket-to-store-temp-glue-job-in'

job = GlueJobFromFolder('glue_jobs/simple_etl_job/', bucket=bucket, job_role=my_role, job_arguments={"--test_arg" : 'some_string'})
job.run_job()

print(job.job_status)
```

### Glue Job Folder Structure

Glue jobs have the prescribed folder format as follows:

```
├── glue_jobs/
|   |
│   ├── job1/
|   |   ├── job.py
|   |   ├── glue_resources/
|   |   |   └── my_lookup_table.csv
|   |   └── glue_py_resources/
|   |       ├── my_python_functions.zip
|   |       └── github_zip_urls.txt
|   |   └── glue_jars/
|   |       └── my_jar.jar
|   |
│   ├── job2/
│   |   ├── job.py
│   |   ├── glue_resources/
│   |   └── glue_py_resources/
|   |
|   ├── shared_job_resources/
│   |   ├── glue_resources/
|   |   |   └── meta_data_dictionary.json
│   |   └── glue_py_resources/
|   |   └── glue_jars/
|   |       └── my_other_jar.jar
```

Every glue job folder must have a `job.py` script in that folder. That is the only required file everything else is optional. When you want to create a glue job object you point the GlueJobFromFolder class to the parent folder of the `job.py` script you want to run. There are two additional folders you can add to this parent folder :

#### glue_resources folder

Any files in this folder are uploaded to the working directory of the glue job. This means in your `job.py` script you can get the path to these files by:

```python
path_to_file = os.path.join(os.getcwd(), 'file_in_folder.txt')
```

The GlueJobFromFolder class will only upload files with extensions (.csv, .sql, .json, .txt) to S3 for the glue job to access.

#### glue_py_resources

These are python scripts you can import in your `job.py` script. e.g. if I had a `utils.py` script in my glue_py_resources folder. I could import that script normally e.g.

```python
from utils import *
```

You can also supply zip file which is a group of python functions in the standard python package structure. You can then reference this package as you would normally in python. For example if I had a package zipped as `my_package.zip` in the glue_py_resources folder then you could access that package normally in your job script like:

```python
from my_package.utils import *
```

You can also supply a text file with the special name `github_zip_urls.txt`. This is a text file where each line is a path to a github zip ball. The GlueJobFromFolder class will download the github package rezip it and send it to S3. This github python package can then be accessed in the same way you would the local zip packages. For example if the `github_zip_urls.txt` file had a single line `https://github.com/moj-analytical-services/gluejobutils/archive/master.zip`. The package `gluejobutils` would be accessible in the `job.py` script:

```python
from gluejobutils.s3 import read_json_from_s3
```

#### shared_job_resources folder

This a specific folder (must have the name `shared_job_resources`). This folder has the same structure and restrictions as a normal glue job folder but does not have a `job.py` file. Instead anything in the `glue_resources` or `glue_py_resources` folders will also be used (and therefore uploaded to S3) by any other glue job. Take the example below:

```
├── glue_jobs/
│   ├── job1/
│   |   ├── job.py
│   |   ├── glue_resources/
|   |   |   └── lookup_table.csv
│   |   └── glue_py_resources/
|   |       └── job1_specific_functions.py
|   |
|   ├── shared_job_resources/
│   |   ├── glue_resources/
|   |   |   └── some_global_config.json
│   |   └── glue_py_resources/
|   |       └── utils.py
```

Running the glue job `job1` i.e.

```python
job = GlueJobFromFolder('glue_jobs/job1/', bucket, job_role)
job.run_job()
```

This glue job would not only have access the the python script `job1_specific_functions.py` and file `lookup_table.csv` but also have access to the python script `utils.py` and file `some_global_config.json`. This is because the latter two files are in the `shared_job_resources` folder and accessible to all job folders (in their `glue_jobs` parent folder).

**Note:** Users should make sure there is no naming conflicts between filenames that are uploaded to S3 as they are sent to the same working folder.

### Using the Glue Job class

Returning to the initial example:

```python
from glue_job_runner import GlueJobFromFolder

my_role = 'aws_role'
bucket = 'bucket-to-store-temp-glue-job-in'

job = GlueJobFromFolder('glue_jobs/simple_etl_job/', bucket=bucket, job_role=my_role)
```

Allows you to create a job object. The GlueJobFromFolder class will have a `job_name` which is defaulted to the folder name you pointed it to i.e. in this instance the job is called `simple_etl_job`. To change the job name:

```python
job.job_name = 'new_job_name'
```

In AWS you can only have unique job names.

Other useful function and properties:

```python
# Increase the number of workers on a glue job (default is 2)
job.number_of_workers = 8

# Use more powerful workers
job.worker_type = 'G.2X'

# Set job arguments these are input params that can be accessed by the job.py script
job.job_arguments = {"--test_arg" : 'some_string', "--enable-metrics" : ""}
```

#### job_arguments

These are strings that can be passed to the glue job script. Below is an example of how these are accessed in the `job.py` script. This code snippit is taken from the `simple_etl_job` found in the `example` folder of this repo.

```python
# Example job tests access to all files passed to the job runner class
import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from gluejobutils.s3 import read_json_from_s3

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'metadata_base_path', 'test_arg'])

print "JOB SPECS..."
print "JOB_NAME: ", args["JOB_NAME"]
print "test argument: ", args["test_arg"]

# Read in meta data json
meta_employees = read_json_from_s3(os.path.join(args['metadata_base_path'], "employees.json"))

### etc
```
### Changes from ETL manager functionality

- Use `GlueJobFolder` instead of `GlueJob`
- You can now use this to run Python, Spark (still default) or Spark Streaming jobs by setting the `glue_job_type` to `python`, `spark` or `glstreaming`
- Timeout is now just set a property instead of using the `overwrite_timeout` argument in etl_manager. If you still want to set a timeout based on cost the new Class has a ``
- Instead of `allocated_capacity` the new class defaults to having a `worker_type` and `number_of_workers`. This is to align with the recommendations in the (Glue documentation)[https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-jobs-job.html].

