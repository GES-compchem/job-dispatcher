#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 19 12:40:18 2021

@author: mpalermo
"""
from multiprocessing import Process, current_process, active_children, Manager
import os
import sys
import time
import traceback
from dataclasses import dataclass, field

from typing import Callable, List, Dict, Any

@dataclass
class Job:
    """Represent a job as a function and its arguments.
    
    Parameters
    ----------
    function : Callable
        A function that will be called by JobDispatcher.
    arguments : List[Any]
        A list containing the function positional arguments.
    keyword_arguments : Dict[str, Any]
        A ditionary contained the function keyword arguments,
    
    """
    function : Callable
    arguments : List[Any] = field(default_factory=list)
    keyword_arguments : Dict[str, Any] = field(default_factory=dict)

class JobDispatcher:
    """Queue any number of different functions and execute them in parallel."""

    def __init__(
        self, jobs_list: dict[Callable: list[Any]], maxcores: int = -1, cores_per_job: int = 1
    ):

        # Set maximum total number of cores used. If user does not provide it,
        # just deduce from os settings
        self.maxcores: int

        if maxcores == -1:
            self.maxcores = len(os.sched_getaffinity(0))
        else:
            self.maxcores = maxcores

        self.cores_per_job: int = cores_per_job  # number of cores used per job

        if not all((isinstance(self.maxcores, int), self.maxcores > 0)):
            raise TypeError("maxcores must be a positive integer")
        if not all((isinstance(cores_per_job, int), cores_per_job > 0)):
            raise TypeError("cores_per_job must be a positive integer")

        for job in jobs_list:
            self._is_it_job(job)

        self.jobs_list = jobs_list

        self.number_of_jobs: int = len(jobs_list)

        self._results_queue : Manager().Queue()

    def _job_completion_tracker(self, user_function: Callable, args, kwargs) -> Callable:
        """
        Take user function and decorate it by setting the number of cores
        available to the jobs, gather the results in a common queue and update
        the number of available cores once the functions terminates.

        Parameters
        ----------
        user_function : method
            User function to be run

        Returns
        -------
        decorated_function : method
            Function decorated for the inner working of the program.
            .

        """

        def decorated_function(results_queue) -> None:
            """Decorate function to be returned."""
            t = time.process_time()

            os.environ["OMP_NUM_THREADS"] = str(
                self.cores_per_job
            )  # set maximum cores per job
            result: object = user_function(*args, **kwargs)  # run function and catch output
            results_queue.put(result)  # store the result in queue
            print(
                f"Elapsed time for job {current_process().name}: {(time.process_time() - t)}"
            )

        return decorated_function

    def _is_it_job(self, job: Callable) -> None:
        """Check if the input job is a function."""
        if not isinstance(job, Job):
            raise TypeError(
                f"The provided job \x1b[1;37;34m{job}\x1b[0m is not an instance"
                " of jobdispatcher.Job.\n"
                "Please initialize a Job object as Job(function) and then pass"
                " it to JobDispatcher."
            )

    def run(self) -> List:
        """Run jobs in the job list."""
        # Clean up zombie processes left running from previous Runtime errors
        for process in active_children():
            if "SyncManager" in process.name:
                process.terminate()

        self._results_queue = Manager().Queue()

        if self.number_of_jobs is None:
            print("No jobs to process")
            sys.exit()
            
        if (self.number_of_jobs*self.cores_per_job)+1 > self.maxcores:
            raise RuntimeError(f"The total number of requested cores {(self.number_of_jobs*self.cores_per_job)+1}"
                               f" is exceeding the maxcore limit of {self.maxcores}."
                               "\n Please remember to take into account that "
                               "1 core must be left free for the parent thread.")

        print(
            f"Running {self.number_of_jobs} jobs on {self.maxcores} cores,"
            f" {self.cores_per_job} cores per job"
        )

        job_counter: int = 0

        while True:
            # 1 core is occupied by the main process
            used_cores = 1 + ((len(active_children())-1) * self.cores_per_job)

            # Check if adding a new job would exceed the available num of cores
            if used_cores + self.cores_per_job <= self.maxcores:
                try:
                    job_counter += 1
                    job = self.jobs_list.pop()  # get job from queue
                    function, args, kwargs = job.function, job.arguments, job.keyword_arguments
                    decorated_job: Callable = self._job_completion_tracker(function, args, kwargs)
                    worker: Process = Process(
                        name=str(job_counter),
                        target=decorated_job,
                        args=(self._results_queue,),
                    )
                    worker.start()  # start job
                    print(f"Starting job {job_counter}")

                except IndexError:
                    # Check if there are no more jobs to run
                    if self._results_queue.qsize() == self.number_of_jobs:
                        print("No more jobs to execute")
                        break

                except Exception as error:
                    traceback.print_exc()
                    print("An exception has been caught.")
                    pass

        dump: List = []

        for _ in range(self.number_of_jobs):
            dump.append(self._results_queue.get())
        
        # For some reason, after jobs have finished, the SyncManager object remains
        # active, and this is a problem in an interactive python shell because
        # the active children count is then flawed. This makes sure the Syncmanager
        # object is terminated.        
        
        for process in active_children():
            if "SyncManager" in process.name:
                process.terminate()
            
        return dump
