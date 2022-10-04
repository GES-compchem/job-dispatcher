# Job running
JobDispatcher allows the user to run Jobs either with multithreading or multiprocessing Python paradigms. This can be
achieved by specifying the engine keyword argument when initializing the JobDispatcher object:

```
jd = JobDispatcher(jobs, maxcores=8, engine="multiprocessing", cores_per_job=1)
```

The choice of the preferred engine should be performed based on the type of function to be run.

### Threading engine
If the function relies either on external I/O or on programs running outside of the GIL that do not require navigating
the system folders, then the `threading` engine should be preferred as it is less resource intensive.

### Multiprocessing engine
Otherwise, the `multiprocessing` engine can be used if the function is parallelized within Python or
require the navigation of system folders. Note that multiprocessing works by creating a copy of the
original python process in memory, therefore if a large amount of data is loaded in the system RAM,
then each process will carry all the data. This impacts both the performance of the parallelization,
as the process of copying the memory stack requires longer, and the requirements in terms of memory size.