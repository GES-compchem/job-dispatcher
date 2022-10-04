# Job definition
Jobs can be defined by using Job instances. Job instances allow the user to allocate a certain amount
of resources to a given function and to retrieve its corresponding result from the output of JobDispatcher.

Job instances can be initialized by providing a unique name, the function to be run, its arguments and
the number of cores to be requested:


```python
def some_function():
	print("Some function")

job = jd.Job(name="some_job", function=some_function, cores=10)
```

Job instances can also represent functions that require positional and keyword arguments:
```
job = jd.Job(name="some_job",
             function=some_function, arguments= [2], keyword_arguments={a: '5'}, cores=10)
```

The positional arguments must be provided as a list, while keyword arguments must be provided as dictionary. No check is performed on the
number and keys of the provided arguments. The user is therefore responsible to make sure the provided arguments match with the function signature. 

Once a job has completed running, the user can retrieve its output from the dictionary returned by JobDispatcher using the job name as key.

# Job dependencies
Not implemented yet.