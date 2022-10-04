# Getting started

Welcome to the introduction to Jobdispatcher! In this page you will find all the information to get you started and launch your first batch of parallel jobs.

This introduction will be structured as follows:
1. [Installation](#installation)
2. [My first job](#my-first-job)
3. [Jobs with arguments and retrieving results](#jobs-with-arguments-and-retrieving-results)
4. [Running multiple jobs](#running-multiple-jobs)
5. [Requesting cores](#requesting-cores) 
 
## Installation
The easiest way to install the latest release of JobDispatcher is through the conda package manager:

```bash
conda install -c greenenergystorage job-dispatcher
```


## My first job
In order to run your first job, first import the library in your python script:
```python
import jobdispatcher as jd
```

JobDispatcher relies on the creation of Job objects which allow to define the job name and the function to be executed. In this minimal example we will not request a specific number of cores, therefore the Job will use only one core.

Lets then create our first Job:

```python
def my_function():
	print("This is my first job")

job = jd.Job(name="first_job", function=my_function)
```

Then, we need to assign the Job to JobDispatcher and invoke the `run` method. In this easy example, we will perform the assignment at the initialization of the JobDispatcher instance by providing to the constructor the job instance, but more advanced assigment methods are available in the [User Guide](user-guide/intro).

```python
dispatcher = jd.JobDispatcher(job)
dispatcher.run()
```

This should print "This is my first job" to the screen.


## Jobs with arguments and retrieving results
You can also create Jobs for functions that accept positional and keyword arguments. Here we show a simple example with positional arguments, while a more complete
explanation can be found in the [User Guide](user-guide/intro).

```python

def my_function(a):
	return a*2

job = jd.Job(name="first job", 
		 function=my_function,  	    
 	     arguments=[2])
```

If the function has a return value, JobDispatcher's `.run()` function will return a dictionary whose entry has the job name as a key and the function result as value.

```python
dispatcher = jd.JobDispatcher(job)
results = dispatcher.run()	
```

## Running multiple jobs
The main purpose of JobDispatcher is to help the user to run multiple jobs in an automated fashion without worrying about how many concurrent jobs can be run on a given system. By default, JobDispatcher allocates 1 core for each Job and determines the number of available cores from a `os.sched_getaffinity(0)` call to the system.

In the following short example we will generate random numbers in different processes. To do so, we will provide multiple Job objects to JobDispatcher:

```python
import jobdispatcher as jd
from random import random

def my_function():
	return random()

# Create some functions and assign them to Job objects
jobs = []

for number in range(100):
	job = jd.Job(name=f"Job {number}", function=my_function)
	
	jobs.append(job)

# Run the provided jobs!
dispatcher = jd.JobDispatcher(jobs)
results = dispatcher.run()

```

Once the jobs are completed, the functions results are collected in the returned `results` dictionary


## Requesting cores
When creating a Job object, you can specify the required number of cores as follows:

```python
import jobdispatcher as jd
from random import random

def my_function():
	return random()

# Create some functions and assign them to Job objects
jobs = []

for number in range(100):
	job = jd.Job(name=f"Job {number}", function=my_function, cores=2)
	
	jobs.append(job)
```

You can also specify the maximum number of cores that will be used by JobDispatcher:

```python
# Run the provided jobs!
dispatcher = jd.JobDispatcher(jobs, maxcores=4)
results = dispatcher.run()
```

More advanced uses of JobDispatcher are detailed in the [User Guide](user-guide/intro).
