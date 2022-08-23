# Welcome to your the JobDispatcher documentation

:Release: |0.18|
:Date: ||

**JobDispatcher** is a python job scheduler library that helps the user execute concurrent independent functions in a simple way
without the need to tinker with multiprocessing and threading libraries.

JobDispatcher main application is to run code external to the python interpreter. By providing a Job class, the user can set the number of cores available to external applications, and JobDispatcher will take care of launching the jobs in an order such that the CPU core usage is maximized.

JobDispatcher can also be used to easily perform concurrent I/O operations when operated with its threading engine.

run concurrent indipendent threads or processes

### Installing JobDispatcher
The easiest way to install the latest release of JobDispatcher is through conda package manager:

```
conda install -c greenenergystorage job-dispatcher
```

### Using JobDispatcher
If you're a first time user, please refer to the [Getting Started section](getting_started)
