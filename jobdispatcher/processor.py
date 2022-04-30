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
import logging

from typing import Callable, List, Dict, Any

logger = logging.getLogger(__name__)



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
    name : str
    function : Callable
    arguments : List[Any] = field(default_factory=list)
    keyword_arguments : Dict[str, Any] = field(default_factory=dict)

class JobDispatcher:
    """Queue any number of different functions and execute them in parallel."""

    def __init__(
        self, jobs_list: List[Job], maxcores: int = -1, cores_per_job: int = 1
    ):

        # Set maximum total number of cores used. If user does not provide it,
        # just deduce from os settings
        self.maxcores: int

        if maxcores == -1:
            self.maxcores = len(os.sched_getaffinity(0))
            logger.debug(f"Total core count from os.sched_getaffinity(0): {self.maxcores}")
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
            t = time.time()

            os.environ["OMP_NUM_THREADS"] = str(
                self.cores_per_job
            )  # set maximum cores per job
            
            job_name = current_process().name
            
            logger.debug(f"Starting job {job_name}")
            result: object = user_function(*args, **kwargs)  # run function and catch output
            results_queue.put((job_name, result))  # store the result in queue
            logger.info(f"Elapsed time for job {current_process().name}: {time.time() - t} seconds")

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
            
    def add(self, job: Job):
        self._is_it_job(job)
        self.jobs_list.append(job)
        

    def run(self) -> List:
        """Run jobs in the job list."""
        # Clean up zombie processes left running from previous Runtime errors
        for process in active_children():
            if "SyncManager" in process.name:
                logger.debug(f"Rogue SyncManager found. Killing it.")
                process.terminate()

        self._results_queue = Manager().Queue()

        if self.number_of_jobs == 0:
            logger.info(f"No jobs to process. To add a new job, use the add method.")
            sys.exit()
            
        # if (self.number_of_jobs*self.cores_per_job)+1 > self.maxcores:
        #     raise RuntimeError(f"The total number of requested cores {(self.number_of_jobs*self.cores_per_job)+1}"
        #                        f" is exceeding the maxcore limit of {self.maxcores}."
        #                        "\n Please remember to take into account that "
        #                        "1 core must be left free for the parent thread.")

        logger.info(
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
                    job = self.jobs_list.pop()  # get job from queue
                    job_counter += 1
                    logger.debug(f"Adding job n°: {job_counter} when {used_cores} cores were used.")
                    function, args, kwargs = job.function, job.arguments, job.keyword_arguments
                    decorated_job: Callable = self._job_completion_tracker(function, args, kwargs)
                    worker: Process = Process(
                        name=str(job_counter),
                        target=decorated_job,
                        args=(self._results_queue,),
                    )
                    worker.start()  # start job

                except IndexError:
                    # Check if there are no more jobs to run
                    if self._results_queue.qsize() == self.number_of_jobs:
                        logger.info("--- No more jobs to execute ---")
                        break

                except Exception as error:
                    logger.critical(f"Critical error encountered while trying to start job n° {job_counter}")
                    logger.critical(error, exc_info=True)
                    pass

        #dump: List = []

        dump : Dict = {}

        logger.debug("Retrieving results from results queue")
        for _ in range(self.number_of_jobs):
            name, result = self._results_queue.get()
            dump[name] = result
        
        # For some reason, after jobs have finished, the SyncManager object remains
        # active, and this is a problem in an interactive python shell because
        # the active children count is then flawed. This makes sure the Syncmanager
        # object is terminated.        
        
        for process in active_children():
            if "SyncManager" in process.name:
                process.terminate()
            
        return dump
