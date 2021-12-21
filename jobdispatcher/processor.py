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

from typing import Callable, List


class JobDispatcher:
    """Queue any number of different functions and execute them in parallel."""

    def __init__(
        self, jobs_list: List[Callable], maxcores: int = -1, cores_per_job: int = 1
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

        for function in jobs_list:
            self._is_it_function(function)

        self.jobs_list = jobs_list

        self.number_of_jobs: int = len(jobs_list)

        self._results_queue = Manager().Queue()

    def _job_completion_tracker(self, user_function: Callable) -> Callable:
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
            result: object = user_function()  # run function and catch output
            results_queue.put(result)  # store the result in queue
            print(
                f"Elapsed time for job {current_process().name}: {time.process_time() - t}"
            )

        return decorated_function

    def _is_it_function(self, job: Callable) -> None:
        """Check if the input job is a function."""
        if not callable(job):
            raise TypeError(
                f"The provided job \x1b[1;37;34m{job}\x1b[0m is not callable"
                " and cannot be processed."
            )

    def run(self) -> List:
        """Run jobs in the job list."""

        if self.number_of_jobs is None:
            print("No jobs to process")
            sys.exit()

        print(
            f"Running {self.number_of_jobs} jobs on {self.maxcores} cores,"
            f"{self.cores_per_job} cores per job"
        )

        job_counter: int = 0

        while True:
            used_cores = len(active_children()) * self.cores_per_job
            # Check if adding a new job would exceed the available num of cores
            if used_cores + self.cores_per_job <= self.maxcores:
                try:
                    job_counter += 1
                    job: Callable = self.jobs_list.pop(0)  # get job from queue
                    decorated_job: Callable = self._job_completion_tracker(job)
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
                    break

        dump: List = []

        for _ in range(self.number_of_jobs):
            dump.append(self._results_queue.get())

        return dump
